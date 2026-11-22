"""
Tests for the version 2 changes that come with migration 0002:
- listing without a query over the B-tree index (AMCAT4_BTREE_LISTING)
- the five extra sort slots, filling them in a job for large projects (AMCAT4_SORT_SLOT_JOB_ROWS), Top-K sorting

The fixtures come from test_fast_path.py: two projects in the same partition, with overlapping words and values.
"""

# ruff: noqa: F811 (the fixtures are imported from test_fast_path)

import re

import pytest
from psycopg import sql

from amcat4.config import get_settings
from amcat4.models import FilterSpec, ProjectSettings, SortSpec, UpdateDocumentField
from amcat4.postgres.connection import connection, fetch_all
from amcat4.postgres.projects import project_pk
from amcat4.postgres.search import SearchQuery, compile_search, order_by
from amcat4.projects import jobs
from amcat4.projects.documents import create_or_update_documents
from amcat4.projects.index import create_project_index, delete_project_index
from amcat4.projects.query import query_documents
from amcat4.systemdata.fields import field_infos, get_fieldset, list_fields, update_fields
from tests.conftest import upload
from tests.test_fast_path import _same_partition, two_projects  # noqa: F401


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
