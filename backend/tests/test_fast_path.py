"""
Tests for the fast read paths (amcat4.config.Settings.fast_path): the partition table, the two-step count,
pdb.agg aggregations, late fetch for sorts, and the correctness fixes that came with them.

The fast paths only kick in above a number of hits (SMALL_RESULT, COUNT_STEP), which the small test sets never
reach. The fixture `index_paths` sets these to 0, so the in-index paths are used for every request.
Run the whole suite that way with AMCAT4_TEST_INDEX_PATHS=1 (see conftest.py).
"""

import pytest

from amcat4.config import get_settings
from amcat4.models import FieldSpec, FilterSpec, ProjectSettings, SortSpec
from amcat4.postgres import search as search_module
from amcat4.postgres.connection import connection
from amcat4.postgres.layout import partition_of
from amcat4.postgres.projects import project_pk
from amcat4.postgres.search import SearchQuery, compile_search
from amcat4.projects import aggregate as aggregate_module
from amcat4.projects.aggregate import Aggregation, Axis, query_aggregate
from amcat4.projects.index import create_project_index, delete_project_index
from amcat4.projects.query import query_documents
from amcat4.systemdata.fields import get_fieldset
from tests.conftest import upload

FIELDS = {"text": "text", "pub": "keyword", "date": "date", "tags": "tag", "n": "integer"}


def _docs(prefix: str, n: int) -> list[dict]:
    return [
        {
            "_id": f"{prefix}{i}",
            "text": f"common {prefix}word{i % 7} " + ("rare" if i == 3 else ""),
            "pub": ["A", "B", "C", ""][i % 4],
            "date": f"20{10 + i % 5}-0{1 + i % 9}-1{i % 9}",
            "tags": [["x"], ["x", "y"], []][i % 3],
            "n": i,
        }
        for i in range(n)
    ]


@pytest.fixture()
def index_paths(monkeypatch):
    monkeypatch.setattr(aggregate_module, "SMALL_RESULT", 0)
    monkeypatch.setattr(search_module, "COUNT_STEP", 0)


@pytest.fixture()
async def two_projects():
    """Two projects in the same partition, with overlapping words and values"""
    names = ["amcat4_unittest_fast_a", "amcat4_unittest_fast_b"]
    for name in names:
        await delete_project_index(name, ignore_missing=True)
        await create_project_index(ProjectSettings(id=name))
    await upload(names[0], _docs("a", 40), fields=FIELDS)  # type: ignore[arg-type]
    await upload(names[1], _docs("b", 25), fields=FIELDS)  # type: ignore[arg-type]
    yield names
    for name in names:
        await delete_project_index(name, ignore_missing=True)


async def _same_partition(a: str, b: str) -> bool:
    async with connection() as conn:
        return await partition_of(conn, await project_pk(a)) == await partition_of(conn, await project_pk(b))


async def _search(index, **kwargs):
    r = await query_documents(index, fields=[FieldSpec(name="pub")], per_page=100, **kwargs)
    return r.total_count, sorted(d["_id"] for d in r.data)


async def _agg(index, axes, **kwargs):
    r = await query_aggregate(index, axes, limit=10000, **kwargs)
    return sorted(map(str, r.data))


@pytest.mark.anyio
async def test_fast_path_uses_partition_table(two_projects):
    """
    The fast read path uses the partition table; writes and document id filters keep the parent table (aggregating on the
    partition table instead of the parent: 37 ms instead of 1,640 ms)
    """
    if not get_settings().fast_path:
        pytest.skip("fast path switched off (AMCAT4_FAST_PATH=false)")
    a, _ = two_projects
    fs = await get_fieldset([a])
    assert compile_search(fs, SearchQuery(queries={"q": "common"}), fast=True).fast
    assert not compile_search(fs, SearchQuery(queries={"q": "common"})).fast  # writes keep the parent table
    assert not compile_search(fs, SearchQuery(ids=["a1"]), fast=True).fast  # doc_id filters keep the B-tree


@pytest.mark.anyio
async def test_project_isolation(two_projects, index_paths):
    """Search, count and aggregate never see documents of another project in the same partition"""
    a, b = two_projects
    assert await _same_partition(a, b)
    for index, prefix, n in [(a, "a", 40), (b, "b", 25)]:
        total, ids = await _search(index, queries={"q": "common"})
        assert total == n and all(i.startswith(prefix) for i in ids)
        total, ids = await _search(index)
        assert total == n and all(i.startswith(prefix) for i in ids)
        total, _ = await _search(index, queries={"q": "rare"})
        assert total == 1
        result = await query_aggregate(index, [Axis("pub")])
        assert sum(row[-1] for row in result.data) == n
        result = await query_aggregate(index, [Axis("tags")])
        assert sum(row[-1] for row in result.data) == sum(len(d["tags"]) for d in _docs(prefix, n))
    # both projects in one request
    total, ids = await _search([a, b], queries={"q": "common"})
    assert total == 65


AGGREGATIONS = [
    ([Axis("pub")], {}),
    ([Axis("tags")], {}),
    ([Axis("date", "year")], {}),
    ([Axis("date", "month")], {}),
    ([Axis("date", "quarter")], {}),
    ([Axis("date", "dayofweek")], {}),
    ([Axis("pub"), Axis("date", "year")], {}),
    ([Axis("tags"), Axis("pub")], {}),
    ([Axis("date", "quarter"), Axis("pub")], {}),
    ([Axis("pub")], {"queries": {"q": "aword1"}}),
    ([Axis("pub")], {"filters": {"date": FilterSpec(gte="2011-01-01", lt="2013-01-01")}}),
    ([Axis("n", "10")], {}),
]


@pytest.mark.anyio
@pytest.mark.parametrize("axes,kwargs", AGGREGATIONS)
async def test_aggregate_parity(two_projects, index_paths, axes, kwargs):
    """The fast aggregation paths give exactly the same rows as the original SQL"""
    a, _ = two_projects
    get_settings().fast_path = False
    try:
        expected = await _agg(a, axes, **kwargs)
    finally:
        get_settings().fast_path = True
    assert await _agg(a, axes, **kwargs) == expected


@pytest.mark.anyio
@pytest.mark.parametrize("function", ["avg", "min", "max", "sum"])
async def test_metric_parity(two_projects, index_paths, function):
    """avg, min, max and sum per bucket from the fast path equal the original GROUP BY (sum and avg with a float tolerance)"""
    a, _ = two_projects
    aggs = [Aggregation("n", function)]
    get_settings().fast_path = False
    try:
        expected = await query_aggregate(a, [Axis("pub")], aggs)
    finally:
        get_settings().fast_path = True
    result = await query_aggregate(a, [Axis("pub")], aggs)
    assert [row[:-1] for row in result.data] == [row[:-1] for row in expected.data]
    assert [row[-1] for row in result.data] == pytest.approx([row[-1] for row in expected.data])


@pytest.mark.anyio
@pytest.mark.parametrize(
    "kwargs",
    [
        {"queries": {"q": "common"}},
        {},
        {"filters": {"pub": FilterSpec(values=["A"])}},
        {"queries": {"q": "aword*"}},
        {"sort": [{"pub": SortSpec(order="desc")}]},
        {"sort": [{"n": SortSpec(order="asc")}], "queries": {"q": "common"}},
    ],
)
async def test_search_parity(two_projects, index_paths, kwargs):
    """Hits, totals and order of searches (query, filter, wildcard, sorts) are the same with the fast path on and off"""
    a, _ = two_projects

    async def run():
        r = await query_documents(a, fields=[FieldSpec(name="pub"), FieldSpec(name="n")], per_page=7, page=1, **kwargs)
        return r.total_count, [(d["_id"], d.get("pub"), d.get("n")) for d in r.data]

    get_settings().fast_path = False
    try:
        expected = await run()
    finally:
        get_settings().fast_path = True
    assert await run() == expected


@pytest.mark.anyio
async def test_wildcard_expands_all_terms(index):
    """A prefix matches every word with that prefix, not only the first 50 (Tantivy's default)"""
    await upload(index, [{"_id": str(i), "text": f"pol{i:03d}"} for i in range(120)], fields={"text": "text"})
    r = await query_documents(index, queries={"q": "pol*"}, per_page=1)
    assert r.total_count == 120


@pytest.mark.anyio
async def test_aggregate_empty_string(two_projects):
    """An empty keyword value is a group of its own (this crashed with HTTP 500 on `section`)"""
    a, _ = two_projects
    result = await query_aggregate(a, [Axis("pub")])
    assert {row[0]: row[1] for row in result.data} == {"": 10, "A": 10, "B": 10, "C": 10}


@pytest.mark.anyio
async def test_scroll_counts_once(two_projects, monkeypatch):
    """The total of a scroll is counted on the first page and carried in the cursor"""
    a, _ = two_projects
    first = await query_documents(a, queries={"q": "common"}, per_page=10)
    calls = []
    original = search_module.count

    async def counting(*args, **kwargs):
        calls.append(1)
        return await original(*args, **kwargs)

    monkeypatch.setattr(search_module, "count", counting)
    second = await query_documents(a, queries={"q": "common"}, per_page=10, after=first.next)
    assert second.total_count == first.total_count == 40
    assert not calls
