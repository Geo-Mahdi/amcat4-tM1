"""
Tests for the version 2 changes that come with migration 0002:
- listing without a query over the B-tree index (AMCAT4_BTREE_LISTING)
- the five extra sort slots, filling them in a job for large projects (AMCAT4_SORT_SLOT_JOB_ROWS), Top-K sorting
- the derived quarter key of date fields, and its backfill job for older documents (AMCAT4_QUARTER_KEY)
- vacuum after large changes (AMCAT4_VACUUM_AFTER_ROWS) and prewarming the search indexes (AMCAT4_PREWARM)
- moving a project to a partition of its own, in batches (migration 0003, the move_project job)

The fixtures come from test_fast_path.py: two projects in the same partition, with overlapping words and values.
"""

# ruff: noqa: F811 (the fixtures are imported from test_fast_path)

import re

import pytest
from psycopg import sql

from amcat4.config import get_settings
from amcat4.errors import ConflictError
from amcat4.models import FieldSpec, FilterSpec, ProjectSettings, SortSpec, UpdateDocumentField
from amcat4.postgres import index_aggregate
from amcat4.postgres import search as search_module
from amcat4.postgres.connection import connection, execute, fetch_all
from amcat4.postgres.fields import normalize_date, quarter_sql
from amcat4.postgres.layout import list_partitions, partition_of, partition_table
from amcat4.postgres.projects import project_pk
from amcat4.postgres.search import SearchQuery, compile_search, order_by
from amcat4.projects import aggregate as aggregate_module
from amcat4.projects import jobs, maintenance, partitions
from amcat4.projects.aggregate import Aggregation, Axis, query_aggregate
from amcat4.projects.documents import create_or_update_documents, fetch_document
from amcat4.projects.index import create_project_index, delete_project_index
from amcat4.projects.query import delete_query, query_documents, update_tag_query
from amcat4.systemdata.fields import field_infos, get_fieldset, list_fields, update_fields
from tests.conftest import upload
from tests.test_fast_path import _same_partition, two_projects  # noqa: F401
from tests.tools import amcat_settings


async def _pages(index, per_page=7, **kwargs) -> list[str]:
    """All document ids, page by page with the cursor"""
    ids, after = [], None
    while True:
        r = await query_documents(index, per_page=per_page, after=after, **kwargs)
        ids += [d["_id"] for d in r.data]
        if r.next is None:
            return ids
        after = r.next


LISTINGS = [
    {},
    {"sort": [{"_id": SortSpec(order="desc")}]},
    {"filters": {"_id": FilterSpec(values=["a1", "a5", "a7", "b1"])}},
]


@pytest.mark.anyio
@pytest.mark.parametrize("kwargs", LISTINGS)
async def test_btree_listing_same_results(two_projects, monkeypatch, kwargs):
    """Listing without a query gives the same documents in the same order, with and without the B-tree path"""
    a, b = two_projects
    assert await _same_partition(a, b)
    results = {}
    for on in (True, False):
        monkeypatch.setattr(get_settings(), "btree_listing", on)
        first = await query_documents(a, per_page=5, **kwargs)
        results[on] = (first.total_count, [d["_id"] for d in first.data], await _pages(a, **kwargs))
    assert results[True] == results[False]
    assert all(i.startswith("a") for i in results[True][2])


@pytest.mark.anyio
async def test_btree_listing_only_without_query(two_projects):
    a, _ = two_projects
    fs = await get_fieldset([a])
    assert compile_search(fs, SearchQuery(), fast=True).plain_where is not None
    assert compile_search(fs, SearchQuery(ids=["a1"])).plain_where is not None
    assert compile_search(fs, SearchQuery(queries={"q": "common"}), fast=True).plain_where is None
    assert compile_search(fs, SearchQuery(filters={"pub": FilterSpec(values=["A"])}), fast=True).plain_where is None


@pytest.mark.anyio
async def test_btree_listing_uses_id_index(two_projects):
    """The plain condition can use the (partition_id, project_pk, id) index of migration 0002"""
    a, _ = two_projects
    c = compile_search(await get_fieldset([a]), SearchQuery(), fast=True)
    assert c.plain_where is not None
    async with connection() as conn, conn.transaction():
        # (the test projects are tiny, so the planner would read the table instead: make it show the index plan)
        await conn.execute("SET LOCAL enable_seqscan = off")
        await conn.execute("SET LOCAL enable_bitmapscan = off")
        cur = await conn.execute(
            sql.SQL("EXPLAIN SELECT documents.id FROM {} WHERE {} AND documents.id > 0 ORDER BY documents.id LIMIT 5").format(
                c.table, c.plain_where
            ),
            c.plain_params,
        )
        plan = "\n".join(next(iter(row.values())) for row in await cur.fetchall())  # type: ignore[union-attr]
    # (the index is called documents_p<N>_project_id, or has a generated name on partitions created later)
    assert re.search(r"Index (Only )?Scan using documents_p\d+_(project_id|partition_id_project_pk_id_idx)", plan), plan
    assert "Sort" not in plan, plan


################ sort slots ################

SLOT_FIELDS = {
    "text": "text",
    "date": "date",
    "date2": "date",
    "date3": "date",
    "k1": "keyword",
    "k2": "keyword",
    "k3": "keyword",
    "k4": "keyword",
    "k5": "keyword",
    "link": "url",
    "n": "integer",
    "x": "number",
    "tags": "tag",
}


def _slot_docs(n: int) -> list[dict]:
    docs = []
    for i in range(n):
        doc = {
            "_id": f"d{i}",
            "text": f"word{i % 3} text",
            "date": f"2020-01-{1 + i % 28:02d}",
            "date2": f"2019-{1 + (i * 7) % 12:02d}-01",
            "k1": f"v{(i * 13) % 17}",
            "k2": ["b", "a", "B", "é", "Z", ""][i % 6],  # byte order (COLLATE "C"): "" < B < Z < a < b < é
            "k3": f"v{i % 4}",
            "k4": "x",
            "k5": "y",
            "link": f"http://example.com/{(i * 5) % 9}",
            "n": (i * 37) % 11 - 5,
            "x": (i * 0.7) % 3,
            "tags": ["t"],
        }
        if i % 5 == 0:  # some documents without a value: they sort last
            for name in ("date2", "k1", "n"):
                del doc[name]
        docs.append(doc)
    return docs


@pytest.fixture()
async def slot_project():
    name = "amcat4_unittest_slots"
    await delete_project_index(name, ignore_missing=True)
    await create_project_index(ProjectSettings(id=name))
    await upload(name, _slot_docs(40), fields=SLOT_FIELDS)  # type: ignore[arg-type]
    yield name
    await delete_project_index(name, ignore_missing=True)


async def _order(index: str, field: str, direction: str = "asc", q: str | None = None) -> list[str]:
    r = await query_documents(
        index,
        sort=[{field: SortSpec(order=direction)}],  # type: ignore[arg-type]
        per_page=40,
        queries={"q": q} if q else None,  # type: ignore[arg-type]
    )
    return [d["_id"] for d in r.data]


async def _slot_values(index: str, slot: str) -> dict[str, object]:
    rows = await fetch_all(
        sql.SQL("SELECT doc_id, {} AS v FROM documents WHERE project_pk = %s").format(sql.Identifier(slot)),  # type: ignore[arg-type]
        [await project_pk(index)],
    )
    return {row["doc_id"]: row["v"] for row in rows}


async def _slots(index: str) -> dict[str, str | None]:
    return {name: f.sort_slot for name, f in (await list_fields(index)).items()}


@pytest.mark.anyio
async def test_sort_slot_allocation(slot_project):
    p = slot_project
    slots = await _slots(p)
    assert slots["date"] == "date" and slots["date2"] is None and slots["k1"] is None  # only the standard automatic one
    for name, slot in [("k1", "source"), ("k2", "sort_kw1"), ("link", "sort_kw2"), ("k3", "sort_kw3")]:
        await update_fields(p, {name: UpdateDocumentField(fast_sort=True)})
        assert (await _slots(p))[name] == slot
    with pytest.raises(ValueError, match="No free sort slot"):
        await update_fields(p, {"k4": UpdateDocumentField(fast_sort=True)})
    await update_fields(p, {"date2": UpdateDocumentField(fast_sort=True)})
    assert (await _slots(p))["date2"] == "sort_date2"
    with pytest.raises(ValueError, match="No free sort slot"):
        await update_fields(p, {"date3": UpdateDocumentField(fast_sort=True)})
    await update_fields(p, {"n": UpdateDocumentField(fast_sort=True)})
    assert (await _slots(p))["n"] == "sort_num1"
    with pytest.raises(ValueError, match="No free sort slot"):
        await update_fields(p, {"x": UpdateDocumentField(fast_sort=True)})
    for name in ("tags", "text"):
        with pytest.raises(ValueError, match="cannot be used for fast sorting"):
            await update_fields(p, {name: UpdateDocumentField(fast_sort=True)})

    # freeing a slot clears its column; the slot can then be used by another field
    assert {v for v in (await _slot_values(p, "sort_kw1")).values()} == {"b", "a", "B", "é", "Z", ""}
    await update_fields(p, {"k2": UpdateDocumentField(fast_sort=False)})
    assert (await _slots(p))["k2"] is None
    assert set((await _slot_values(p, "sort_kw1")).values()) == {None}
    await update_fields(p, {"k4": UpdateDocumentField(fast_sort=True)})
    assert (await _slots(p))["k4"] == "sort_kw1"
    assert set((await _slot_values(p, "sort_kw1")).values()) == {"x"}


SLOTTED = [("k1", "source"), ("k2", "sort_kw1"), ("link", "sort_kw2"), ("n", "sort_num1"), ("date2", "sort_date2")]


@pytest.mark.anyio
async def test_sort_slots_same_order(slot_project):
    """Sorting on a slot gives the same order as sorting on the json value (late fetch), also with a query"""
    p = slot_project
    expected = {}
    for name, _ in SLOTTED:
        for direction in ("asc", "desc"):
            for q in (None, "word1"):
                expected[name, direction, q] = await _order(p, name, direction, q)
    for name, _ in SLOTTED:
        await update_fields(p, {name: UpdateDocumentField(fast_sort=True)})
    assert {name: slot for name, slot in SLOTTED} == {name: (await _slots(p))[name] for name, _ in SLOTTED}
    for (name, direction, q), ids in expected.items():
        if name == "k2" and not get_settings().byte_order_sort:
            continue  # mixed case and accents: the database collation differs from the byte order of the slot
        assert await _order(p, name, direction, q) == ids, (name, direction, q)
    # documents without a value come last in both directions
    assert (await _order(p, "k1"))[-8:] == (await _order(p, "k1", "desc"))[-8:]
    # new and updated documents fill the slots
    await upload(p, [{"_id": "new", "k1": "zzz", "n": 100, "date2": "2030-01-01"}])
    for name in ("k1", "n", "date2"):
        assert (await _order(p, name, "desc"))[0] == "new"


@pytest.mark.anyio
async def test_sort_slots_use_topk(slot_project):
    """A sort on a slot column is a Top-K scan inside the search index (not: read every hit, then sort)"""
    p = slot_project
    for name, _ in SLOTTED:
        await update_fields(p, {name: UpdateDocumentField(fast_sort=True)})
    fs = await get_fieldset([p])
    for name, slot in SLOTTED:
        for direction in ("asc", "desc"):
            for queries in (None, {"q": "word1"}):
                c = compile_search(fs, SearchQuery(queries=queries), fast=True)
                ordering = order_by(fs, [(name, direction)], scored=bool(queries))  # type: ignore[list-item]
                async with connection() as conn:
                    cur = await conn.execute(
                        sql.SQL("EXPLAIN SELECT documents.id FROM {} WHERE {} ORDER BY {} LIMIT 10").format(
                            c.table, c.where, ordering
                        ),
                        c.params,
                    )
                    plan = "\n".join(next(iter(row.values())) for row in await cur.fetchall())  # type: ignore[union-attr]
                assert "TopKScanExecState" in plan and f"TopK Order By: {slot}" in plan, plan


@pytest.mark.anyio
async def test_sort_slot_fill_job(slot_project, monkeypatch):
    """For large projects a job fills the slot in batches; until it is done, sorting uses the json value"""
    p = slot_project
    monkeypatch.setattr(get_settings(), "sort_slot_job_rows", 0)
    monkeypatch.setattr(jobs, "FILL_BATCH_SIZE", 7)
    await update_fields(p, {"k1": UpdateDocumentField(fast_sort=True)})
    # the slot is reserved, not shown, and not used for sorting yet; writes already fill it
    assert (await _slots(p))["k1"] is None
    info = (await field_infos(p))["k1"]
    assert info.sort_slot == "source" and not info.sort_ready and info.sort_read_column is None
    assert set((await _slot_values(p, "source")).values()) == {None}
    await create_or_update_documents(p, [{"_id": "d1", "k1": "v1"}], op_type="update")
    assert (await _slot_values(p, "source"))["d1"] == "v1"
    await update_fields(p, {"k5": UpdateDocumentField(fast_sort=False)})  # (no slot: a no-op)
    expected = await _order(p, "k1", "desc")
    assert await _order(p, "k1", "desc", "word1") == [i for i in expected if int(i[1:]) % 3 == 1]
    with pytest.raises(ValueError, match="No free sort slot"):  # reserved slots are not free
        for name in ("k2", "k3", "k4", "k5"):
            await update_fields(p, {name: UpdateDocumentField(fast_sort=True)})

    assert await jobs.run_pending_jobs() >= 1
    [job] = await fetch_all(
        "SELECT status, result FROM jobs WHERE type = 'fill_sort_slot' AND project_pk = %s AND params->>'slot' = 'source'",
        [await project_pk(p)],
    )
    assert job["status"] == "done" and job["result"]["batches"] == 6  # 40 documents in batches of 7
    assert (await _slots(p))["k1"] == "source"
    values = await _slot_values(p, "source")
    assert values["d1"] == "v1" and values["d0"] is None and values["d2"] == "v9"
    assert await _order(p, "k1", "desc") == expected

    # freeing it: a job clears the column
    await update_fields(p, {"k1": UpdateDocumentField(fast_sort=False)})
    assert (await _slots(p))["k1"] is None
    await jobs.run_pending_jobs()
    assert set((await _slot_values(p, "source")).values()) == {None}
    await fetch_all("DELETE FROM jobs WHERE type = 'fill_sort_slot' RETURNING id")


@pytest.mark.anyio
async def test_sort_slot_fill_job_stops_if_slot_moves(slot_project, monkeypatch):
    p = slot_project
    monkeypatch.setattr(get_settings(), "sort_slot_job_rows", 0)
    await update_fields(p, {"k1": UpdateDocumentField(fast_sort=True)})
    await update_fields(p, {"k1": UpdateDocumentField(fast_sort=False)})
    await update_fields(p, {"k3": UpdateDocumentField(fast_sort=True)})  # takes the same slot (source)
    await jobs.run_pending_jobs()
    rows = await fetch_all(
        "SELECT params, result FROM jobs WHERE type = 'fill_sort_slot' AND project_pk = %s ORDER BY created_at",
        [await project_pk(p)],
    )
    assert [("stopped" in r["result"]) for r in rows] == [True, True, False]  # fill k1, clear, fill k3
    assert set((await _slot_values(p, "source")).values()) == {"v0", "v1", "v2", "v3"}
    assert (await _slots(p))["k3"] == "source"
    await fetch_all("DELETE FROM jobs WHERE type = 'fill_sort_slot' RETURNING id")


@pytest.mark.anyio
async def test_sort_slot_type_change(slot_project):
    p = slot_project
    await update_fields(p, {"k1": UpdateDocumentField(fast_sort=True), "n": UpdateDocumentField(fast_sort=True)})
    # keyword -> url and integer -> number keep their slot (same values)
    await update_fields(p, {"k1": UpdateDocumentField(type="url"), "n": UpdateDocumentField(type="number")})
    assert (await _slots(p))["k1"] == "source" and (await _slots(p))["n"] == "sort_num1"
    # integer/number -> keyword: another kind of slot, so the slot is freed and cleared
    await update_fields(p, {"n": UpdateDocumentField(type="keyword")})
    assert (await _slots(p))["n"] is None
    assert set((await _slot_values(p, "sort_num1")).values()) == {None}


################ quarter key ################

QUARTER_DATES = ["2018-01-01", "2018-03-31T23:59:59Z", "2018-04-01", "2018-06-15", "2018-12-31", "0999-11-05", "2020-07-04"]


@pytest.fixture()
async def quarter_project():
    name = "amcat4_unittest_quarters"
    await delete_project_index(name, ignore_missing=True)
    await create_project_index(ProjectSettings(id=name))
    docs = [{"_id": str(i), "date": d, "n": i, "cat": "ab"[i % 2]} for i, d in enumerate(QUARTER_DATES)]
    await upload(
        name, [*docs, {"_id": "nodate", "n": 100, "cat": "a"}], fields={"date": "date", "n": "integer", "cat": "keyword"}
    )  # type: ignore[arg-type]
    yield name
    await delete_project_index(name, ignore_missing=True)


async def _quarter_keys(index: str) -> dict[str, str | None]:
    key = (await field_infos(index))["date"].derived_key("quarter")
    rows = await fetch_all(
        "SELECT doc_id, exact_fields->>%s AS q FROM documents WHERE project_pk = %s", [key, await project_pk(index)]
    )
    return {row["doc_id"]: row["q"] for row in rows}


async def _quarter_aggs(index: str) -> list:
    out = []
    for axes, metrics in [
        ([Axis("date", "quarter")], []),
        ([Axis("date", "quarter"), Axis("cat")], []),
        ([Axis("date", "quarter")], [Aggregation("n", "avg"), Aggregation("n", "max")]),
    ]:
        r = await query_aggregate(index, axes, metrics, limit=1000)  # type: ignore[arg-type]
        out.append(sorted(map(str, r.data)))
    return out


async def _remove_quarter_keys(index: str) -> None:
    """Make the project look like one from before migration 0002: no quarter keys, field not ready"""
    f = (await field_infos(index))["date"]
    pk = await project_pk(index)
    await fetch_all(
        "UPDATE documents SET exact_fields = exact_fields - %s WHERE project_pk = %s RETURNING id",
        [f.derived_key("quarter"), pk],
    )
    await fetch_all("UPDATE fields SET quarter_ready = false WHERE pk = %s RETURNING pk", [f.pk])


@pytest.mark.anyio
async def test_quarter_key_at_upload(quarter_project):
    keys = await _quarter_keys(quarter_project)
    assert keys == {
        "0": "2018-01",
        "1": "2018-01",
        "2": "2018-04",
        "3": "2018-04",
        "4": "2018-10",
        "5": "0999-10",
        "6": "2020-07",
        "nodate": None,
    }
    # the SQL version (used by the backfill job) gives the same keys
    rows = await fetch_all(
        sql.SQL("SELECT {} AS q FROM unnest(%s::text[]) AS v").format(quarter_sql(sql.SQL("v"))),  # type: ignore[arg-type]
        [[normalize_date(d) for d in QUARTER_DATES]],
    )
    assert [row["q"] for row in rows] == [keys[str(i)] for i in range(len(QUARTER_DATES))]


@pytest.mark.anyio
@pytest.mark.parametrize("index_paths_on", [False, True])
async def test_quarter_aggregation_same_results(quarter_project, monkeypatch, index_paths_on):
    """Quarter aggregations give the same results with the key, without it (switch off), and before the backfill"""
    if index_paths_on:
        monkeypatch.setattr(aggregate_module, "SMALL_RESULT", 0)
        monkeypatch.setattr(search_module, "COUNT_STEP", 0)
    p = quarter_project
    with_key = await _quarter_aggs(p)
    assert with_key[0] == [
        "(None, 1)",
        "(datetime.date(2018, 1, 1), 2)",
        "(datetime.date(2018, 10, 1), 1)",
        "(datetime.date(2018, 4, 1), 2)",
        "(datetime.date(2020, 7, 1), 1)",
        "(datetime.date(999, 10, 1), 1)",
    ]
    assert "(datetime.date(2018, 4, 1), 2, 2.5, 3.0)" in with_key[2]
    monkeypatch.setattr(get_settings(), "quarter_key", False)
    assert await _quarter_aggs(p) == with_key
    monkeypatch.setattr(get_settings(), "quarter_key", True)
    await _remove_quarter_keys(p)
    assert not (await field_infos(p))["date"].quarter_ready
    assert await _quarter_aggs(p) == with_key


@pytest.mark.anyio
async def test_quarter_metrics_in_index(quarter_project):
    """With the quarter key, a quarter axis with metrics can be done inside the index (pdb.agg)"""
    fs = await get_fieldset([quarter_project])
    request, _ = index_aggregate.build_request(fs, [Axis("date", "quarter")], [Aggregation("n", "avg")])
    assert request["terms"]["field"].endswith("_quarter")
    await _remove_quarter_keys(quarter_project)
    fs = await get_fieldset([quarter_project])
    with pytest.raises(index_aggregate._NotSupported):
        index_aggregate.build_request(fs, [Axis("date", "quarter")], [Aggregation("n", "avg")])


@pytest.mark.anyio
async def test_quarter_backfill(quarter_project, monkeypatch):
    p = quarter_project
    expected_keys, expected_aggs = await _quarter_keys(p), await _quarter_aggs(p)
    await _remove_quarter_keys(p)
    assert set((await _quarter_keys(p)).values()) == {None}
    monkeypatch.setattr(jobs, "FILL_BATCH_SIZE", 3)
    await jobs.schedule_quarter_backfill({})
    await jobs.schedule_quarter_backfill({})  # (not twice)
    pk = await project_pk(p)
    assert len(await fetch_all("SELECT 1 FROM jobs WHERE type = 'backfill_quarter' AND project_pk = %s", [pk])) == 1
    await jobs.run_pending_jobs()
    [job] = await fetch_all("SELECT status, result FROM jobs WHERE type = 'backfill_quarter' AND project_pk = %s", [pk])
    assert job["status"] == "done" and job["result"]["fields"] == ["date"]
    assert await _quarter_keys(p) == expected_keys
    assert (await field_infos(p))["date"].quarter_ready
    assert await _quarter_aggs(p) == expected_aggs
    await fetch_all("DELETE FROM jobs WHERE type = 'backfill_quarter' RETURNING id")


################ vacuum and prewarm ################


async def _vacuum_jobs() -> list[dict]:
    return await fetch_all("SELECT status, params, result FROM jobs WHERE type = 'vacuum' ORDER BY created_at")


@pytest.mark.anyio
async def test_vacuum_after_large_changes(slot_project, monkeypatch):
    p = slot_project
    await fetch_all("DELETE FROM jobs WHERE type = 'vacuum' RETURNING id")
    monkeypatch.setattr(get_settings(), "vacuum_after_rows", 30)
    async with connection() as conn:
        partition_id = await partition_of(conn, await project_pk(p))
    # a small change: no vacuum (unless the statistics say the partition changed a lot: vacuum it first)
    await execute(sql.SQL("VACUUM {}").format(sql.Identifier(f"documents_p{partition_id}")))  # type: ignore[arg-type]
    await update_tag_query(p, "add", "tags", "small", ids=["d1", "d2"])
    assert await _vacuum_jobs() == []
    # a large one: a vacuum job for the partition, only one while it is pending
    await update_tag_query(p, "add", "tags", "large")
    await delete_query(p, ids=[f"d{i}" for i in range(31)])
    [job] = await _vacuum_jobs()
    assert job["status"] == "pending" and job["params"] == {"partition_id": partition_id}
    await jobs.run_pending_jobs()
    [job] = await _vacuum_jobs()
    assert job["status"] == "done" and job["result"]["table"] == f"documents_p{partition_id}"
    row = await fetch_all(
        "SELECT last_vacuum IS NOT NULL AS v FROM pg_stat_user_tables WHERE relid = to_regclass(%s)",
        [f"documents_p{partition_id}"],
    )
    assert row[0]["v"]
    # switched off: never
    monkeypatch.setattr(get_settings(), "vacuum_after_rows", 0)
    await update_tag_query(p, "add", "tags", "again")
    assert len(await _vacuum_jobs()) == 1
    await fetch_all("DELETE FROM jobs WHERE type = 'vacuum' RETURNING id")


@pytest.mark.anyio
async def test_prewarm(slot_project):
    loaded = await maintenance.prewarm_indexes()
    async with connection() as conn:
        partitions = await list_partitions(conn)
    # (if the server has no pg_prewarm, nothing is loaded, without an error)
    assert loaded in ([], [p["index"] for p in partitions])


################ moving a project to its own partition ################


async def _state(index: str) -> tuple:
    """What readers see of a project: totals, hits, aggregations, single documents, vectors"""
    r = await query_documents(index, fields=[FieldSpec(name="pub")], per_page=100, queries={"q": "common"})
    listing = await _pages(index, per_page=9)
    agg = await query_aggregate(index, [Axis("pub")], [Aggregation("n", "avg")])
    first = listing[0]
    return (
        r.total_count,
        sorted(d["_id"] for d in r.data),
        listing,
        sorted(map(str, agg.data)),
        await fetch_document(index, first),
        await fetch_document(index, listing[-1]),
    )


async def _partition_tables(index: str) -> list[str]:
    rows = await fetch_all(
        "SELECT DISTINCT tableoid::regclass::text AS t FROM documents WHERE project_pk = %s ORDER BY 1",
        [await project_pk(index)],
    )
    return [row["t"] for row in rows]


@pytest.mark.anyio
async def test_move_project_in_batches(two_projects, monkeypatch):
    a, b = two_projects
    await create_or_update_documents(
        a, [{"_id": f"a{i}", "vec": [1.0, i / 10]} for i in range(5)], {"vec": "vector"}, op_type="update"
    )
    before_a, before_b = await _state(a), await _state(b)
    [source] = await _partition_tables(a)
    monkeypatch.setattr(partitions, "MOVE_BATCH_SIZE", 15)

    # run the job, and stop it after the first batch (as if the server stopped)
    original = jobs.Job.save_progress

    async def stop_after_first_batch(self, **progress):
        await original(self, **progress)
        if progress.get("moved"):
            raise jobs.JobCancelled()

    monkeypatch.setattr(jobs.Job, "save_progress", stop_after_first_batch)
    job = await partitions.start_move(a)
    await jobs.run_pending_jobs()
    monkeypatch.setattr(jobs.Job, "save_progress", original)
    progress = (await jobs.get_job(job["id"]))["progress"]
    assert progress["moved"] == 15
    target = partition_table(progress["target"])
    assert await _partition_tables(a) == sorted([source, target])  # in two partitions

    # halfway: readers see every document once, writers are refused, no second move, no new projects in the target
    assert await _state(a) == before_a and await _state(b) == before_b
    with pytest.raises(ConflictError):
        await upload(a, [{"_id": "new", "text": "new"}])
    with pytest.raises(ConflictError):
        await update_tag_query(a, "add", "tags", "z")
    with pytest.raises(ConflictError):
        await delete_query(a, ids=["a1"])
    await upload(b, [{"_id": "bnew", "text": "common b"}])  # other projects can still be written
    await delete_query(b, ids=["bnew"])
    with pytest.raises(ValueError, match="already being moved"):
        await partitions.start_move(a)
    other = "amcat4_unittest_move_other"
    await delete_project_index(other, ignore_missing=True)
    try:
        await create_project_index(ProjectSettings(id=other))
        assert await _project_partition(other) != progress["target"]

        # resume: the job continues where it stopped
        await execute("UPDATE jobs SET status = 'pending' WHERE id = %s", [job["id"]])
        await jobs.run_pending_jobs()
        done = await jobs.get_job(job["id"])
        assert done["status"] == "done" and done["result"]["moved"] == 40 and done["result"]["target"] == target
        assert await _partition_tables(a) == [target]
        assert await _project_partition(a) == progress["target"]
        assert await _state(a) == before_a and await _state(b) == before_b
        rows = await fetch_all(
            """SELECT DISTINCT partition_id FROM document_vectors v JOIN fields f ON f.pk = v.field_pk
               WHERE f.name = 'vec' AND f.project_pk = %s""",
            [await project_pk(a)],
        )
        assert [row["partition_id"] for row in rows] == [progress["target"]]  # the vectors moved along
        # writes work again, and new projects still do not go to the project's own partition
        await upload(a, [{"_id": "new", "text": "common new"}])
        assert (await query_documents(a, queries={"q": "new"})).total_count == 1
        with pytest.raises(ValueError, match="own"):
            await partitions.start_move(a)
        await delete_project_index(other)
        await create_project_index(ProjectSettings(id=other))
        assert await _project_partition(other) != progress["target"]
        with amcat_settings(partition_max_gb=0):  # a full shared partition: a new one, not the project's own
            await delete_project_index(other)
            await create_project_index(ProjectSettings(id=other))
            assert await _project_partition(other) > progress["target"]
    finally:
        await delete_project_index(other, ignore_missing=True)
        await fetch_all("DELETE FROM jobs WHERE type IN ('move_project', 'vacuum') RETURNING id")


async def _project_partition(index: str) -> int:
    async with connection() as conn:
        return await partition_of(conn, await project_pk(index))
