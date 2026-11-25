"""
Tests for version 3, phase 3: quick speed wins without schema change (docs/postgres-fast-v3/QUICK_WINS.md).
Every test compares the new path with the old path on the same data.
"""

import pytest

from amcat4.config import get_settings
from amcat4.models import FilterSpec, ProjectSettings
from amcat4.projects import aggregate as aggregate_module
from amcat4.projects.aggregate import Aggregation, Axis, query_aggregate
from amcat4.projects.index import create_project_index, delete_project_index
from tests.conftest import upload
from tests.test_fast_path import index_paths  # noqa: F401

PROJECT = "amcat4_unittest_quick"
FIELDS = {
    "text": "text",
    "title": "text",
    "pub": "keyword",
    "date": "date",
    "tags": "tag",
    "n": "integer",
    "x": "number",
    "flag": "boolean",
    "url": "url",
}
# whole and decimal values in one number field, very large and very small ones, and both zeros
NUMBERS = [1.0, 2.5, 3.0, -0.5, 1e10, 0.1, 17, -3.25, 1e22, 1e-7, 0.0, -0.0, 2.0**53 + 2]


def _docs(prefix: str, n: int, numbers: list) -> list[dict]:
    docs = []
    for i in range(n):
        doc: dict = {"_id": f"{prefix}{i}", "text": f"common word{i % 7}", "pub": "ABC"[i % 3], "url": f"http://x/{i % 4}"}
        if i % 4:
            doc["title"] = f"title {i}" if i % 8 else ""
        if i % 5:
            doc["date"] = f"20{10 + i % 5}-0{1 + i % 9}-1{i % 9}T0{i % 7}:1{i % 5}:00.{i:06d}"
        if i % 6:
            doc["x"] = numbers[i % len(numbers)]
        if i % 7:
            doc["n"] = i % 11 - 4
        if i % 3:
            doc["flag"] = bool(i % 2)
        if i % 5 > 1:
            doc["tags"] = ["x", "y", "z"][: i % 3 + 1]
        docs.append(doc)
    return docs


@pytest.fixture()
async def project():
    """
    One project, uploaded in four parts (four segments of the search index): only whole numbers, only decimals, both,
    and documents with almost no fields
    """
    await delete_project_index(PROJECT, ignore_missing=True)
    await create_project_index(ProjectSettings(id=PROJECT))
    await upload(PROJECT, _docs("a", 40, [1.0, 2.0, 17.0]), fields=FIELDS)  # type: ignore[arg-type]
    await upload(PROJECT, _docs("b", 40, [2.5, 0.1, -3.25]))
    await upload(PROJECT, _docs("c", 80, NUMBERS))
    await upload(PROJECT, [{"_id": f"d{i}", "pub": "D"} for i in range(5)])
    yield PROJECT
    await delete_project_index(PROJECT, ignore_missing=True)


@pytest.fixture()
def index_aggregations(monkeypatch):
    """Records what amcat4.postgres.index_aggregate returned: rows (in the index) or None (GROUP BY)"""
    calls: list = []
    original = aggregate_module.index_aggregate

    async def spy(*args, **kwargs):
        rows = await original(*args, **kwargs)
        calls.append(rows)
        return rows

    monkeypatch.setattr(aggregate_module, "index_aggregate", spy)
    return calls


async def _aggregate(index, axes, aggregations=None, **kwargs):
    r = await query_aggregate(index, axes, aggregations, limit=10000, **kwargs)
    return r.data


async def _old_and_new(monkeypatch, switch: str, run):
    """The result of run() with the original SQL (no fast path), with the switch off, and with the switch on"""
    settings = get_settings()
    monkeypatch.setattr(settings, "fast_path", False)
    original = await run()
    monkeypatch.setattr(settings, "fast_path", True)
    monkeypatch.setattr(settings, switch, False)
    off = await run()
    monkeypatch.setattr(settings, switch, True)
    return original, off, await run()


############ 1. Aggregations inside the index ############

TYPED_AGGREGATIONS = [
    ([Axis("n")], [], {}),
    ([Axis("x")], [], {}),
    ([Axis("flag")], [], {}),
    ([Axis("flag"), Axis("x")], [], {}),
    ([Axis("pub"), Axis("n")], [], {}),
    ([Axis("x"), Axis("date", "year")], [], {}),
    ([Axis("tags"), Axis("flag")], [], {}),
    ([Axis("n")], [], {"queries": {"q": "word1 OR word2"}}),
    ([Axis("x")], [], {"filters": {"flag": FilterSpec(value=True)}}),
    ([Axis("x")], [Aggregation("n", "sum"), Aggregation("n", "min")], {}),
    ([Axis("flag")], [Aggregation("x", "max"), Aggregation("date", "min")], {}),
    ([Axis("pub")], [Aggregation("date", "min"), Aggregation("date", "max")], {}),
    ([Axis("n")], [Aggregation("date", "max")], {"queries": {"q": "common"}}),
    ([Axis("date", "quarter")], [Aggregation("date", "min")], {}),
    ([], [Aggregation("n", "min"), Aggregation("n", "max"), Aggregation("x", "min")], {}),
    ([], [Aggregation("date", "min"), Aggregation("date", "max"), Aggregation("n", "max")], {}),
    ([], [Aggregation("date", "max")], {"filters": {"pub": FilterSpec(value="D")}}),  # no document has a date
    ([], [Aggregation("x", "min")], {"queries": {"q": "word3"}}),
]


def _same_rows(result: list[tuple], expected: list[tuple], n_exact: int):
    """The same rows; the metrics (after the axes and the count) with a float tolerance (sums and averages)"""
    assert [row[:n_exact] for row in result] == [row[:n_exact] for row in expected]
    for row, exp in zip(result, expected):
        for value, e in zip(row[n_exact:], exp[n_exact:]):
            assert value == (pytest.approx(e) if isinstance(e, float) else e)


@pytest.mark.anyio
@pytest.mark.parametrize("axes,aggregations,kwargs", TYPED_AGGREGATIONS)
async def test_typed_aggregation_same_rows(project, index_paths, index_aggregations, monkeypatch, axes, aggregations, kwargs):  # noqa: F811
    """Group by number, integer and boolean, min/max of dates and metrics without axes: the same rows as GROUP BY"""

    async def run():
        return await _aggregate(project, axes, aggregations, **kwargs)

    original, off, new = await _old_and_new(monkeypatch, "index_agg_types", run)
    assert len(original) > 0
    # min, max (of numbers and dates) are exact; no sum or avg of decimals here, so everything is exact
    assert new == original
    assert off == original
    # with the switch on the rows came from the index; with it off these shapes used GROUP BY
    assert [rows is not None for rows in index_aggregations] == [False, True]


@pytest.mark.anyio
@pytest.mark.parametrize("function", ["avg", "sum", "min", "max"])
async def test_typed_metric_same_values(project, index_paths, index_aggregations, monkeypatch, function):  # noqa: F811
    """avg and sum of a number field with whole and decimal values per boolean, and without axes (float tolerance)"""
    for axes in ([Axis("flag")], [Axis("n")], []):

        async def run():
            return await _aggregate(project, axes, [Aggregation("x", function), Aggregation("n", function)])

        original, off, new = await _old_and_new(monkeypatch, "index_agg_types", run)
        _same_rows(new, original, len(axes) + 1)
        _same_rows(off, original, len(axes) + 1)
    assert [rows is not None for rows in index_aggregations] == [False, True] * 3


@pytest.mark.anyio
@pytest.mark.parametrize(
    "axes,aggregations",
    [
        ([Axis("n", "5")], []),  # number interval
        ([Axis("date", "hour")], []),
        ([Axis("url")], []),
        ([Axis("pub")], [Aggregation("date", "avg")]),
        ([], [Aggregation("date", "avg")]),
        ([], []),
    ],
)
async def test_shapes_left_on_group_by(project, index_paths, index_aggregations, monkeypatch, axes, aggregations):  # noqa: F811
    """These stay on GROUP BY (see QUICK_WINS.md), with the same rows as the original SQL"""
    new = await _aggregate(project, axes, aggregations)
    monkeypatch.setattr(get_settings(), "fast_path", False)
    assert new == await _aggregate(project, axes, aggregations)
    assert index_aggregations == [None]


@pytest.mark.anyio
async def test_values_a_float_cannot_hold(project, index_paths, index_aggregations, monkeypatch):  # noqa: F811
    """
    The index computes with 64-bit floats. Integers from 2^53 and dates more than 285 years from 2000 could be
    rounded there, so GROUP BY is used for them, and the rows stay the same.
    """
    await upload(
        project,
        [
            {"_id": "big1", "pub": "E", "n": 2**53 + 1, "date": "1500-03-04T05:06:07.123457"},
            {"_id": "big2", "pub": "E", "n": 2**53 + 2, "date": "1500-03-04T05:06:07.123456"},
            {"_id": "big3", "pub": "E", "n": -(2**53) - 1},
        ],
    )
    for axes, aggregations in [
        ([Axis("n")], []),
        ([Axis("pub")], [Aggregation("date", "min")]),
        ([], [Aggregation("date", "min")]),
    ]:

        async def run():
            return await _aggregate(project, axes, aggregations)

        original, off, new = await _old_and_new(monkeypatch, "index_agg_types", run)
        assert new == original == off
    assert index_aggregations == [None] * 6


############ 2. Field values and field statistics through the index ############


@pytest.fixture()
def index_field_calls(monkeypatch):
    """Records what index_field_values and index_field_stats returned (None: the original statement was used)"""
    from amcat4.systemdata import fields as fields_module

    calls: list = []
    for name in ("index_field_values", "index_field_stats"):
        original = getattr(fields_module, name)

        async def spy(*args, _original=original, **kwargs):
            result = await _original(*args, **kwargs)
            calls.append(result)
            return result

        monkeypatch.setattr(fields_module, name, spy)
    return calls


async def _on_and_off(monkeypatch, switch: str, run):
    monkeypatch.setattr(get_settings(), switch, False)
    off = await run()
    monkeypatch.setattr(get_settings(), switch, True)
    return off, await run()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "field,in_index",
    [("pub", True), ("tags", True), ("url", True), ("flag", True), ("n", True), ("x", False), ("date", False)],
)
@pytest.mark.parametrize("size", [2, 3, 1000])
async def test_field_values_same_list(project, index_field_calls, monkeypatch, field, in_index, size):
    """The same list of values (same order, also for values with the same count) with and without the index"""
    from amcat4.systemdata.fields import field_values

    off, on = await _on_and_off(monkeypatch, "index_field_stats", lambda: field_values(project, field, size))
    assert on == off
    assert 0 < len(on) <= size
    assert [r is not None for r in index_field_calls] == [in_index]


@pytest.mark.anyio
async def test_field_values_odd_values(index, index_field_calls, monkeypatch):
    """
    Empty strings, values that look like numbers, booleans or dates, mixed case and accents. A keyword that looks
    like a full timestamp is a date inside the index (two spellings of one moment become one value there), so
    for such a field the original statement is used.
    """
    from amcat4.systemdata.fields import field_values

    values = ["", "17", "17.0", "true", "a", "A", "é", "b b", "2024-05-01", "null", "Z", "z z"]
    docs = [
        {"kw": values[i % len(values)], "plain": values[i % 7], "tags": [values[i % 5], "t"], "i": i % 3} for i in range(40)
    ]
    await upload(index, docs, fields={"kw": "keyword", "plain": "keyword", "tags": "tag", "i": "integer"})
    await upload(
        index, [{"kw": "2024-05-01T00:00:00Z"}, {"kw": "2024-05-01T00:00:00.000000Z"}, {"i": 2**53 + 1}, {"i": 2**53 + 2}]
    )
    for field, in_index in [("plain", True), ("tags", True), ("kw", False), ("i", False)]:
        off, on = await _on_and_off(monkeypatch, "index_field_stats", lambda: field_values(index, field, 100))
        assert on == off
        assert (index_field_calls.pop() is not None) == in_index
    assert "2024-05-01T00:00:00Z" in await field_values(index, "kw", 100)


@pytest.mark.anyio
async def test_field_values_too_many_values(project, index_field_calls, monkeypatch):
    """More different values than the index returns: the original statement, never a cut list"""
    from amcat4.postgres import index_aggregate
    from amcat4.systemdata.fields import field_values

    monkeypatch.setattr(index_aggregate, "MAX_BUCKETS", 2)
    off, on = await _on_and_off(monkeypatch, "index_field_stats", lambda: field_values(project, "pub", 10))
    assert on == off == ["A", "B", "C", "D"]
    assert index_field_calls == [None]


@pytest.mark.anyio
@pytest.mark.parametrize("field", ["n", "x", "date"])
async def test_field_stats_same_output(project, index_field_calls, monkeypatch, field):
    """Count, minimum and maximum are the same; the average within float precision (dates: within a millisecond)"""
    from datetime import datetime

    from amcat4.systemdata.fields import field_stats

    off, on = await _on_and_off(monkeypatch, "index_field_stats", lambda: field_stats(project, field))
    assert index_field_calls[0] is not None
    assert {k: on[k] for k in ("count", "min", "max")} == {k: off[k] for k in ("count", "min", "max")}
    assert on["count"] > 0
    if field == "date":
        assert abs((datetime.fromisoformat(on["avg"]) - datetime.fromisoformat(off["avg"])).total_seconds()) < 0.001
    else:
        assert on["avg"] == pytest.approx(off["avg"])


@pytest.mark.anyio
async def test_field_stats_no_values_and_old_dates(index, index_field_calls, monkeypatch):
    """A field without any value, an empty project, and dates a float cannot hold exactly (the original statement)"""
    from amcat4.systemdata.fields import create_fields, field_stats

    await create_fields(index, {"n": "integer", "date": "date", "pub": "keyword"})
    for field in ("n", "date"):
        off, on = await _on_and_off(monkeypatch, "index_field_stats", lambda: field_stats(index, field))
        assert on == off == {"count": 0, "min": None, "max": None, "avg": None}
    await upload(index, [{"pub": "a"}, {"pub": "b", "date": "1500-03-04T05:06:07.123457"}, {"pub": "b", "date": "1500-03-05"}])
    index_field_calls.clear()
    for field in ("n", "date"):
        off, on = await _on_and_off(monkeypatch, "index_field_stats", lambda: field_stats(index, field))
        assert on == off
    assert on["count"] == 2
    assert [r is not None for r in index_field_calls] == [True, False]


############ 3. "Text field exists" inside the index ############


async def _text_keys_wrong(index: str) -> int:
    """The number of documents of the project whose list of text keys is not the list of the keys of their text fields"""
    from psycopg import sql

    from amcat4.postgres.connection import fetch_one
    from amcat4.postgres.fields import with_text_keys_sql
    from amcat4.postgres.projects import project_pk

    row = await fetch_one(
        sql.SQL(
            "SELECT count(*) FILTER (WHERE exact_fields IS DISTINCT FROM {}) AS wrong, count(*) AS n "
            "FROM documents WHERE project_pk = %s"
        ).format(with_text_keys_sql()),
        [await project_pk(index)],
    )
    assert row is not None and row["n"] > 0
    return row["wrong"]


async def _hits(index, **kwargs) -> tuple[int, list[str]]:
    from amcat4.projects.query import query_documents

    r = await query_documents(index, per_page=200, **kwargs)
    return r.total_count, sorted(d["_id"] for d in r.data)


EXISTS_QUERIES = [
    {"filters": {"title": FilterSpec(exists=True)}},
    {"filters": {"title": FilterSpec(exists=False)}},
    {"filters": {"text": FilterSpec(exists=False)}},
    {"filters": {"title": FilterSpec(exists=True), "pub": FilterSpec(value="A")}},
    {"filters": {"title": FilterSpec(exists=False)}, "queries": {"q": "word1 OR word2"}},
    {"filters": {"title": FilterSpec(exists=True), "text": FilterSpec(exists=True)}, "queries": {"q": "common"}},
]


async def _exists_results(index) -> list:
    out: list = [await _hits(index, **kwargs) for kwargs in EXISTS_QUERIES]
    out.append(await _aggregate(index, [Axis("pub")], filters={"title": FilterSpec(exists=True)}))
    out.append(await _aggregate(index, [Axis("flag")], filters={"title": FilterSpec(exists=False)}))
    return out


def _exists_in_index(fieldset, name: str = "title") -> bool:
    """Whether an exists filter on the field compiles to a clause of the search index (and no SQL condition)"""
    from amcat4.postgres.filters import compile_filters

    c = compile_filters({name: FilterSpec(exists=True)}, fieldset)
    assert len(c.json_clauses) + len(c.sql_clauses) == 1
    return bool(c.json_clauses)


@pytest.mark.anyio
async def test_text_exists_same_hits(project, index_paths, monkeypatch):  # noqa: F811
    """'exists' on a text field gives the same hits, counts and aggregations inside the index as the SQL condition"""
    from amcat4.systemdata.fields import get_fieldset

    assert await _text_keys_wrong(project) == 0
    assert _exists_in_index(await get_fieldset([project]))
    original, off, new = await _old_and_new(monkeypatch, "text_exists_key", lambda: _exists_results(project))
    assert new == off == original
    # a title that is an empty string is a value too; documents without any text field have none
    assert new[0][0] == sum(1 for prefix, n in [("a", 40), ("b", 40), ("c", 80)] for i in range(n) if i % 4)
    assert new[2] == (5, [f"d{i}" for i in range(5)])
    monkeypatch.setattr(get_settings(), "text_exists_key", False)
    assert not _exists_in_index(await get_fieldset([project]))


@pytest.mark.anyio
async def test_text_keys_follow_every_write(project, index_paths, monkeypatch):  # noqa: F811
    """
    The list of text keys stays right, and the filter gives the same hits as the SQL condition, after every kind of
    write: upload (new, merged, replaced), update and delete by query, deleting a field, changing a field to and from
    text, copying to another project and moving to another partition
    """
    from amcat4.models import UpdateDocumentField
    from amcat4.postgres.projects import project_partitions, project_pk
    from amcat4.projects.documents import create_or_update_documents, update_document
    from amcat4.projects.jobs import create_job, get_job, run_pending_jobs
    from amcat4.projects.partitions import start_move
    from amcat4.projects.query import update_query
    from amcat4.systemdata.fields import delete_fields, update_fields

    async def check(index=project, queries=EXISTS_QUERIES[:3]):
        assert await _text_keys_wrong(index) == 0

        async def run():
            return [await _hits(index, **kwargs) for kwargs in queries]

        original, off, new = await _old_and_new(monkeypatch, "text_exists_key", run)
        assert new == off == original
        return new

    before = await check()
    # merge: a2 gets a title, a1 keeps its title; replace: a3 loses its title, a5 its text
    await create_or_update_documents(project, [{"_id": "a4", "title": "new"}, {"_id": "a1", "pub": "Z"}], op_type="update")
    await create_or_update_documents(project, [{"_id": "a3", "text": "only text"}, {"_id": "a5", "title": "only title"}])
    await update_document(project, "a8", {"title": "another"})
    after = await check()
    assert after[0][0] == before[0][0] + 2 - 1 and "a4" in after[0][1] and "a3" in after[1][1] and "a5" in after[2][1]
    # update by query: set and remove a text field
    await update_query(project, "title", "set by query", filters={"pub": FilterSpec(value="D")})
    await update_query(project, "text", None, filters={"pub": FilterSpec(value="B")})
    after = await check()
    assert {f"d{i}" for i in range(5)} <= set(after[0][1])
    assert after[2][0] > 5
    # a keyword field becomes a text field and back; a text field is deleted
    await update_fields(project, {"pub": UpdateDocumentField(type="text")})
    # (the two replaced documents have no pub any more)
    assert (await check(queries=[{"filters": {"pub": FilterSpec(exists=True)}}]))[0][0] == 163
    await update_fields(project, {"pub": UpdateDocumentField(type="keyword")})
    await check()
    # copy to another project (new field keys) and move the copy to a partition of its own
    copy = "amcat4_unittest_quick_copy"
    await delete_project_index(copy, ignore_missing=True)
    try:
        await create_project_index(ProjectSettings(id=copy))
        await create_job("copy", project, None, {"source": project, "destination": copy})
        await run_pending_jobs()
        assert (await check(copy))[0] == (await check())[0]
        job = await start_move(copy, None)
        await run_pending_jobs()
        assert (await get_job(job["id"]))["status"] == "done"
        partitions = await project_partitions([await project_pk(project), await project_pk(copy)])
        assert len(set(partitions.values())) == 2
        assert (await check(copy))[0] == (await check())[0]
        # documents written in the new partition get the list too
        await upload(copy, [{"_id": "x1", "title": "in the new partition"}, {"_id": "x2", "pub": "A"}])
        assert "x1" in (await check(copy))[0][1]
    finally:
        await delete_project_index(copy, ignore_missing=True)
    await delete_fields(project, ["title"])
    assert await _text_keys_wrong(project) == 0
    assert await _hits(project, filters={"text": FilterSpec(exists=True)}) == (165 - after[2][0], after_ids(after))


def after_ids(after) -> list[str]:
    missing = set(after[2][1])
    return sorted(
        i for i in [f"{p}{i}" for p, n in [("a", 40), ("b", 40), ("c", 80), ("d", 5)] for i in range(n)] if i not in missing
    )


@pytest.mark.anyio
async def test_text_keys_backfill(project, index_paths, monkeypatch):  # noqa: F811
    """
    Documents stored before migration 0004 have no list of text keys: their text fields are not ready, the filter
    uses the SQL condition (same hits), and the background job writes the lists (also correcting a wrong one), in
    batches, once; after it the filter runs inside the index, with the same hits
    """
    from amcat4.postgres.connection import execute, fetch_all
    from amcat4.postgres.projects import project_pk
    from amcat4.projects import jobs
    from amcat4.systemdata.fields import create_fields, field_infos, get_fieldset

    pk = await project_pk(project)
    expected = await _exists_results(project)
    # as after the migration: no lists (this statement does not write text_fields, so the trigger does not fire), a
    # wrong list on some documents, and the text fields not ready
    await execute("UPDATE documents SET exact_fields = exact_fields - '_text' WHERE project_pk = %s", [pk])
    await execute(
        """UPDATE documents SET exact_fields = exact_fields || '{"_text": ["f0"]}' WHERE project_pk = %s AND id %% 9 = 0""",
        [pk],
    )
    await execute("UPDATE fields SET text_exists_ready = false WHERE project_pk = %s AND type = 'text'", [pk])
    assert await _text_keys_wrong(project) > 100
    assert not _exists_in_index(await get_fieldset([project]))
    assert await _exists_results(project) == expected
    # a text field that is created now is ready at once: every document that gets it, gets the list from the trigger
    await create_fields(project, {"summary": "text"})
    await upload(project, [{"_id": "a1", "summary": "new"}, {"_id": "new", "summary": "new"}])
    infos = await field_infos(project)
    assert infos["summary"].text_exists_ready and not infos["title"].text_exists_ready
    assert _exists_in_index(await get_fieldset([project]), "summary")
    assert await _hits(project, filters={"summary": FilterSpec(exists=True)}) == (2, ["a1", "new"])
    expected = await _exists_results(project)

    monkeypatch.setattr(jobs, "FILL_BATCH_SIZE", 25)
    await jobs.schedule_text_keys_backfill({})
    await jobs.schedule_text_keys_backfill({})  # (not twice)
    assert len(await fetch_all("SELECT 1 FROM jobs WHERE type = 'backfill_text_keys' AND project_pk = %s", [pk])) == 1
    await jobs.run_pending_jobs()
    [job] = await fetch_all("SELECT status FROM jobs WHERE type = 'backfill_text_keys' AND project_pk = %s", [pk])
    assert job["status"] == "done"
    assert await _text_keys_wrong(project) == 0
    assert all(f.text_exists_ready for f in (await field_infos(project)).values())
    assert _exists_in_index(await get_fieldset([project]))
    assert await _exists_results(project) == expected
    await jobs.schedule_text_keys_backfill({})  # nothing left to do
    assert len(await fetch_all("SELECT 1 FROM jobs WHERE type = 'backfill_text_keys' AND project_pk = %s", [pk])) == 1
    await fetch_all("DELETE FROM jobs WHERE type = 'backfill_text_keys' RETURNING id")
