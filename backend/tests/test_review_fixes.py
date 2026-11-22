"""
Tests for the fixes after the code review of the postgres-fast-v2 branch (one section per finding).
"""

import pytest

from amcat4.config import get_settings
from amcat4.models import FieldSpec, ProjectSettings, SortSpec, UpdateDocumentField
from amcat4.postgres.search import slot_sort_column
from amcat4.projects.index import create_project_index, delete_project_index
from amcat4.projects.query import query_documents
from amcat4.systemdata.fields import get_fieldset, list_fields, update_fields
from tests.conftest import upload


@pytest.fixture()
async def two():
    names = ["amcat4_unittest_review_a", "amcat4_unittest_review_b"]
    for name in names:
        await delete_project_index(name, ignore_missing=True)
        await create_project_index(ProjectSettings(id=name))
    yield names
    for name in names:
        await delete_project_index(name, ignore_missing=True)


async def _ids(index, **kwargs) -> list[str]:
    r = await query_documents(index, per_page=100, **kwargs)
    return [d["_id"] for d in r.data]


###################### 1. Sort slots in a multi-project query ######################


@pytest.mark.anyio
@pytest.mark.parametrize("fast_path", [True, False])
async def test_slot_sort_other_project_same_slot(two, monkeypatch, fast_path):
    """
    A slot column holds a different field in another project: a sort on one project's field must not order the other
    project's documents by that (maybe hidden) field. They have no value, so they come last (NULLS LAST), by id.
    """
    monkeypatch.setattr(get_settings(), "fast_path", fast_path)
    a, b = two
    await upload(a, [{"_id": f"a{i}", "n": i * 10} for i in range(3)], fields={"n": "integer"})
    await upload(b, [{"_id": f"b{i}", "secret": 100 - i * 10} for i in range(3)], fields={"secret": "integer"})
    await update_fields(a, {"n": UpdateDocumentField(fast_sort=True)})
    await update_fields(b, {"secret": UpdateDocumentField(fast_sort=True)})
    assert (await list_fields(a))["n"].sort_slot == (await list_fields(b))["secret"].sort_slot == "sort_num1"
    fieldset = await get_fieldset([a, b])
    assert slot_sort_column(fieldset, "n") is None
    assert slot_sort_column(await get_fieldset([a]), "n") == "sort_num1"
    asc = await _ids([a, b], sort=[{"n": SortSpec(order="asc")}], fields=[FieldSpec(name="n")])
    assert asc == ["a0", "a1", "a2", "b0", "b1", "b2"]
    desc = await _ids([a, b], sort=[{"n": SortSpec(order="desc")}], fields=[FieldSpec(name="n")])
    assert desc == ["a2", "a1", "a0", "b0", "b1", "b2"]


@pytest.mark.anyio
async def test_slot_sort_auto_mapped_date(two):
    """The automatic date slot: another project's first date field (with another name) is in the same column"""
    a, b = two
    await upload(a, [{"_id": f"a{i}", "date": f"2020-01-0{i + 1}"} for i in range(3)], fields={"date": "date"})
    await upload(b, [{"_id": f"b{i}", "published": f"2019-01-0{3 - i}"} for i in range(3)], fields={"published": "date"})
    assert (await list_fields(a))["date"].sort_slot == (await list_fields(b))["published"].sort_slot == "date"
    assert slot_sort_column(await get_fieldset([a, b]), "date") is None
    assert await _ids([a, b], sort=[{"date": SortSpec(order="asc")}]) == ["a0", "a1", "a2", "b0", "b1", "b2"]
    assert await _ids([a, b], sort=[{"published": SortSpec(order="desc")}]) == ["b0", "b1", "b2", "a0", "a1", "a2"]


@pytest.mark.anyio
async def test_slot_sort_used_when_all_projects_have_it(two):
    """If every project of the query has the field in the same filled slot, the slot column is used"""
    a, b = two
    await upload(a, [{"_id": f"a{i}", "date": f"2020-01-0{i + 1}"} for i in range(3)], fields={"date": "date"})
    await upload(b, [{"_id": f"b{i}", "date": f"2020-01-0{i + 4}"} for i in range(3)], fields={"date": "date"})
    assert slot_sort_column(await get_fieldset([a, b]), "date") == "date"
    assert await _ids([a, b], sort=[{"date": SortSpec(order="desc")}]) == ["b2", "b1", "b0", "a2", "a1", "a0"]


###################### 2, 3, 4. Sorting on _id and _seq, and the page depth limit ######################


async def _all_pages(index, per_page=2, **kwargs) -> list[str]:
    """All documents, following the 'next' cursors"""
    r = await query_documents(index, per_page=per_page, **kwargs)
    ids = [d["_id"] for d in r.data]
    while r.next:
        r = await query_documents(index, per_page=per_page, after=r.next, **kwargs)
        ids += [d["_id"] for d in r.data]
    return ids


ID_ORDER = ["a10", "a9", "b", "c1"]  # the doc_id (text) order of the documents below, which are stored as b, c1, a9, a10


@pytest.mark.anyio
@pytest.mark.parametrize("fast_path", [True, False])
async def test_sort_id_is_doc_id(two, monkeypatch, fast_path):
    """sort on _id is the order of the document ids (as before), with page numbers and offset cursors"""
    monkeypatch.setattr(get_settings(), "fast_path", fast_path)
    a, _ = two
    await upload(a, [{"_id": i, "text": "x"} for i in ["b", "c1", "a9", "a10"]], fields={"text": "text"})
    for order, expected in (("asc", ID_ORDER), ("desc", ID_ORDER[::-1])):
        sort = [{"_id": SortSpec(order=order)}]
        assert await _ids(a, sort=sort) == expected
        p0 = await query_documents(a, sort=sort, page=0, per_page=2)
        p1 = await query_documents(a, sort=sort, page=1, per_page=2)
        assert [d["_id"] for d in p0.data] + [d["_id"] for d in p1.data] == expected
        assert await _all_pages(a, sort=sort) == expected
        # with a query, too
        assert await _all_pages(a, sort=sort, queries={"q": "x"}) == expected


@pytest.mark.anyio
async def test_sort_seq(two):
    """_seq is the storage order with a keyset cursor at page 0 (or with a keyset cursor), and offset paging for page > 0"""
    from amcat4.projects.query import _decode_cursor

    a, _ = two
    stored = ["b", "c1", "a9", "a10", "d", "e"]
    await upload(a, [{"_id": i, "text": "x"} for i in stored], fields={"text": "text"})
    for order, expected in (("asc", stored), ("desc", stored[::-1])):
        sort = [{"_seq": SortSpec(order=order)}]
        r = await query_documents(a, sort=sort, per_page=2, queries={"q": "x"})
        assert r.next is not None and "id" in _decode_cursor(r.next)
        assert await _all_pages(a, sort=sort, queries={"q": "x"}) == expected
        assert await _all_pages(a, sort=sort) == expected
        pages = [await query_documents(a, sort=sort, page=p, per_page=2, queries={"q": "x"}) for p in range(3)]
        assert [d["_id"] for p in pages for d in p.data] == expected
        assert all(p.total_count == 6 for p in pages)
        # the cursor of a page number continues after that page (as a keyset cursor)
        assert pages[1].next is not None and "id" in _decode_cursor(pages[1].next)
        rest = await query_documents(a, sort=sort, per_page=10, after=pages[1].next, queries={"q": "x"})
        assert [d["_id"] for d in rest.data] == expected[4:]


@pytest.mark.anyio
async def test_cursor_beyond_max_offset(two, monkeypatch):
    """The 'next' cursors of a query without sort (offset cursors) are not limited by max_offset; page numbers are"""
    monkeypatch.setattr(get_settings(), "max_offset", 4)
    a, _ = two
    await upload(a, [{"_id": f"d{i}", "text": "word"} for i in range(11)], fields={"text": "text"})
    ids = await _all_pages(a, queries={"q": "word"})
    assert sorted(ids) == sorted(f"d{i}" for i in range(11)) and len(ids) == 11
    ids = await _all_pages(a, queries={"q": "word"}, sort=[{"_id": SortSpec(order="asc")}])
    assert ids == sorted(f"d{i}" for i in range(11))
    with pytest.raises(ValueError, match='"sort": "_seq"'):
        await query_documents(a, queries={"q": "word"}, page=2, per_page=2)
    with pytest.raises(ValueError, match='"sort": "_seq"'):
        await query_documents(a, sort=[{"_seq": SortSpec(order="asc")}], page=2, per_page=2)


###################### 5. pdb.agg metrics of a bucket without values ######################


@pytest.mark.anyio
async def test_index_metrics_empty_bucket(two, monkeypatch):
    """In-index metrics (pdb.agg) of buckets where no document has the field: null, the same as GROUP BY"""
    from amcat4.postgres import search as search_module
    from amcat4.projects import aggregate as aggregate_module
    from amcat4.projects.aggregate import Aggregation, Axis, query_aggregate

    monkeypatch.setattr(aggregate_module, "SMALL_RESULT", 0)
    monkeypatch.setattr(search_module, "COUNT_STEP", 0)
    used: list = []
    original = aggregate_module.index_aggregate

    async def spy(*args, **kwargs):
        rows = await original(*args, **kwargs)
        used.append(rows is not None)
        return rows

    monkeypatch.setattr(aggregate_module, "index_aggregate", spy)
    a, _ = two
    docs = [{"_id": f"d{i}", "pub": "AB"[i // 4], "cat": "xy"[i % 2], **({"n": i - 2} if i < 4 else {})} for i in range(8)]
    await upload(a, docs, fields={"pub": "keyword", "cat": "keyword", "n": "integer"})
    aggs = [Aggregation("n", f) for f in ("sum", "avg", "min", "max")]
    for axes in ([Axis("pub")], [Axis("pub"), Axis("cat")]):
        used.clear()
        fast = sorted((await query_aggregate(a, axes, aggs)).data)
        assert used == [True]  # pdb.agg was used, not the fallback
        monkeypatch.setattr(get_settings(), "fast_path", False)
        slow = sorted((await query_aggregate(a, axes, aggs)).data)
        monkeypatch.setattr(get_settings(), "fast_path", True)
        assert fast == slow
        assert all(row[-4:] == (None, None, None, None) for row in fast if row[0] == "B")
    assert ("A", 4, -2.0 + -1 + 0 + 1, -0.5, -2.0, 1.0) in sorted((await query_aggregate(a, [Axis("pub")], aggs)).data)


###################### 6. No slower fallback after a statement timeout or a connection problem ######################


class _FailingConnection:
    """A connection that raises `error` for statements for which marker(sql) is true (and counts them)"""

    def __init__(self, conn, marker, error: Exception):
        self.conn, self.marker, self.error, self.failed = conn, marker, error, 0

    async def execute(self, stmt, params=None):
        text = stmt if isinstance(stmt, str) else stmt.as_string(self.conn)
        if self.marker(text):
            self.failed += 1
            raise self.error
        return await self.conn.execute(stmt, params)


def _failing(monkeypatch, module, marker, error: Exception) -> list[_FailingConnection]:
    from contextlib import asynccontextmanager

    from amcat4.postgres.connection import connection

    made: list[_FailingConnection] = []

    @asynccontextmanager
    async def failing_connection():
        async with connection() as conn:
            made.append(_FailingConnection(conn, marker, error))
            yield made[-1]

    monkeypatch.setattr(module, "connection", failing_connection)
    return made


@pytest.mark.anyio
async def test_statement_timeout_is_not_a_fallback_error():
    """A real statement timeout is QueryCanceled, which must_not_fall_back recognizes"""
    import psycopg

    from amcat4.postgres.connection import connection, must_not_fall_back

    async with connection() as conn:
        await conn.execute("SET statement_timeout = 10")
        try:
            with pytest.raises(psycopg.errors.QueryCanceled) as e:
                await conn.execute("SELECT pg_sleep(1)")
        finally:
            await conn.execute("RESET statement_timeout")
    assert must_not_fall_back(e.value)
    assert must_not_fall_back(psycopg.OperationalError("connection lost"))
    assert not must_not_fall_back(psycopg.errors.InternalError_("too many buckets"))
    assert not must_not_fall_back(psycopg.errors.FeatureNotSupported("not in the index"))


IN_INDEX = {
    # pdb.agg (a keyword axis)
    "pdb.agg": ("pub", lambda text: "pdb.agg" in text),
    # GROUP BY on the partition table (an integer axis, which pdb.agg does not do)
    "in-index GROUP BY": ("n", lambda text: "GROUP BY" in text and '"documents_p' in text),
}


@pytest.mark.anyio
@pytest.mark.parametrize("path", IN_INDEX.keys())
async def test_aggregate_fallbacks(two, monkeypatch, path):
    """
    The in-index aggregations (pdb.agg; GROUP BY on the partition table) fall back to slower statements after a query
    error, but raise a statement timeout or connection error at once (no second statement)
    """
    import psycopg

    from amcat4.postgres import search as search_module
    from amcat4.projects import aggregate as aggregate_module
    from amcat4.projects.aggregate import Axis, query_aggregate

    monkeypatch.setattr(aggregate_module, "SMALL_RESULT", 0)
    monkeypatch.setattr(search_module, "COUNT_STEP", 0)
    axis, marker = IN_INDEX[path]
    a, _ = two
    await upload(
        a, [{"_id": f"d{i}", "pub": "AB"[i % 2], "n": i % 3} for i in range(9)], fields={"pub": "keyword", "n": "integer"}
    )
    expected = sorted((await query_aggregate(a, [Axis(axis)])).data)
    # a query error: the fallback gives the same result
    made = _failing(monkeypatch, aggregate_module, marker, psycopg.errors.InternalError_("in-index limit"))
    assert sorted((await query_aggregate(a, [Axis(axis)])).data) == expected
    assert made[-1].failed == 1
    # a statement timeout, or a lost connection: raised, without a second (slower) statement
    for error in (psycopg.errors.QueryCanceled("statement timeout"), psycopg.OperationalError("connection lost")):
        made = _failing(monkeypatch, aggregate_module, marker, error)
        with pytest.raises(type(error)):
            await query_aggregate(a, [Axis(axis)])
        assert made[-1].failed == 1
