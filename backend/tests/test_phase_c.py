"""
Tests for the version 2 changes that come with migration 0002:
- listing without a query over the B-tree index (AMCAT4_BTREE_LISTING)

The fixtures come from test_fast_path.py: two projects in the same partition, with overlapping words and values.
"""

# ruff: noqa: F811 (the fixtures are imported from test_fast_path)

import re

import pytest
from psycopg import sql

from amcat4.config import get_settings
from amcat4.models import FilterSpec, SortSpec
from amcat4.postgres.connection import connection
from amcat4.postgres.search import SearchQuery, compile_search
from amcat4.projects.query import query_documents
from amcat4.systemdata.fields import get_fieldset
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
