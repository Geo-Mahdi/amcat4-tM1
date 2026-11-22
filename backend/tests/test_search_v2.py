"""
Tests for the version 2 read paths:
- no parallel workers for API connections (AMCAT4_API_PARALLEL_WORKERS)
- the project and filters as unscored clauses (AMCAT4_FILTER_CLAUSES)
- paging by id and the offset limit (AMCAT4_MAX_OFFSET)
- one count, next to the page (AMCAT4_CAPPED_COUNT, AMCAT4_PARALLEL_COUNT)
- behaviour under overload: statement timeout and quick 503 (AMCAT4_STATEMENT_TIMEOUT_MS, AMCAT4_POOL_TIMEOUT_S)

The fixtures come from test_fast_path.py: two projects in the same partition, with overlapping words and values.
"""

# ruff: noqa: F811 (the fixtures are imported from test_fast_path)

import asyncio
import time

import pytest

from amcat4.api import index_query as index_query_module
from amcat4.config import Settings, get_settings
from amcat4.connections import CONNECTIONS, _api_options, db, server_defaults, with_server_defaults
from amcat4.models import FieldSpec, FilterSpec, Roles, SortSpec
from amcat4.postgres import search as search_module
from amcat4.postgres.connection import connection, fetch_one, spare_connection
from amcat4.postgres.search import SearchQuery, compile_search
from amcat4.projects.query import _decode_cursor, delete_query, query_documents, update_tag_query
from amcat4.systemdata.fields import get_fieldset
from amcat4.systemdata.roles import create_project_role
from tests.conftest import upload
from tests.test_fast_path import _same_partition, _search, index_paths, two_projects  # noqa: F401
from tests.tools import auth_cookie, post_json


@pytest.mark.anyio
async def test_api_parallel_workers_setting(monkeypatch):
    assert Settings(api_parallel_workers="default").api_parallel_workers is None  # type: ignore[arg-type]
    assert Settings(api_parallel_workers="").api_parallel_workers is None  # type: ignore[arg-type]
    assert Settings(api_parallel_workers="2").api_parallel_workers == 2  # type: ignore[arg-type]
    assert Settings.model_fields["api_parallel_workers"].default == 0
    monkeypatch.setattr(get_settings(), "api_parallel_workers", None)
    assert "max_parallel_workers_per_gather" not in _api_options()
    monkeypatch.setattr(get_settings(), "api_parallel_workers", 3)
    assert "-c max_parallel_workers_per_gather=3" in _api_options()


@pytest.mark.anyio
async def test_parallel_workers_only_for_api_connections():
    """API connections run without parallel workers; jobs, maintenance and CLI (server_defaults) keep the server's"""
    query = "SELECT setting, source FROM pg_settings WHERE name = 'max_parallel_workers_per_gather'"
    api = await fetch_one(query)
    assert api is not None
    if get_settings().api_parallel_workers is not None:  # the suite can run with AMCAT4_API_PARALLEL_WORKERS=default
        assert api["setting"] == str(get_settings().api_parallel_workers) and api["source"] == "client"
    with server_defaults():
        work = await fetch_one(query)
    assert work is not None and work["source"] != "client"


SEARCH_SHAPES = [
    {},
    {"queries": {"q": "common"}},
    {"queries": {"q": "rare"}},
    {"queries": {"q": "aword1 OR aword2"}},
    {"queries": {"q": "common AND NOT aword3"}},
    {"queries": {"q": '"common aword1"'}},
    {"queries": {"q": "aword*"}},
    {"queries": {"q1": "aword1", "q2": "aword2"}},
    {"filters": {"pub": FilterSpec(values=["A", "B"])}},
    {"filters": {"date": FilterSpec(gte="2011-01-01", lt="2013-01-01")}},
    {"filters": {"pub": FilterSpec(exists=False)}},
    {"filters": {"text": FilterSpec(exists=True)}},
    {"filters": {"tags": FilterSpec(values=["y"])}},
    {"queries": {"q": "common"}, "filters": {"pub": FilterSpec(values=["A"]), "n": FilterSpec(gte=10)}},
    {"queries": {"q": "aword*"}, "filters": {"date": FilterSpec(gte="2012-01-01")}},
]


async def _both_forms(monkeypatch, func):
    out = []
    for form in (False, True):
        monkeypatch.setattr(get_settings(), "filter_clauses", form)
        out.append(await func())
    return out


@pytest.mark.anyio
async def test_filter_form_json(two_projects, monkeypatch):
    """The new form puts the project and filters in a const_score clause; pg_search silently ignores a 'filter' key"""
    a, _ = two_projects
    fs = await get_fieldset([a])
    query = SearchQuery(queries={"q": "common"}, filters={"pub": FilterSpec(values=["A"])})
    monkeypatch.setattr(get_settings(), "filter_clauses", True)
    must = compile_search(fs, query, fast=True).json_query["boolean"]["must"]
    assert "const_score" in must[0] and must[0]["const_score"]["score"] == 0
    project = {"term": {"field": "project_pk", "value": fs.project_pks[0]}}
    assert must[0]["const_score"]["query"]["boolean"]["must"][0] == project
    assert "filter" not in compile_search(fs, query, fast=True).json_query["boolean"]
    # without a query string, only the unscored selection remains
    assert "const_score" in compile_search(fs, SearchQuery(), fast=True).json_query


@pytest.mark.anyio
@pytest.mark.parametrize("fast_path", [True, False])
async def test_filter_form_isolation(two_projects, index_paths, monkeypatch, fast_path):
    """With the new form, search and count never see documents of another project in the same partition"""
    monkeypatch.setattr(get_settings(), "filter_clauses", True)
    monkeypatch.setattr(get_settings(), "fast_path", fast_path)
    a, b = two_projects
    assert await _same_partition(a, b)
    for index, prefix, n in [(a, "a", 40), (b, "b", 25)]:
        for kwargs in [{}, {"queries": {"q": "common"}}, {"filters": {"pub": FilterSpec(values=["A", "B", "C", ""])}}]:
            total, ids = await _search(index, **kwargs)
            assert total == n and len(ids) == n and all(i.startswith(prefix) for i in ids)
        # a word that only exists in the other project
        other = "bword1" if prefix == "a" else "aword1"
        assert await _search(index, queries={"q": other}) == (0, [])
    total, ids = await _search([a, b], queries={"q": "common"})
    assert total == 65


@pytest.mark.anyio
@pytest.mark.parametrize("kwargs", SEARCH_SHAPES)
async def test_filter_form_same_hits(two_projects, index_paths, monkeypatch, kwargs):
    """Old and new form give the same counts and hit sets (only scores may differ), for one and for two projects"""
    a, b = two_projects
    for index in (a, [a, b]):
        old, new = await _both_forms(monkeypatch, lambda: _search(index, **kwargs))
        assert old == new

    # paging through all hits gives the same set too
    async def all_ids():
        r = await query_documents(a, fields=[FieldSpec(name="pub")], per_page=7, **kwargs)
        ids = [d["_id"] for d in r.data]
        for page in range(1, 10):
            r = await query_documents(a, fields=[FieldSpec(name="pub")], per_page=7, page=page, **kwargs)
            ids += [d["_id"] for d in r.data]
        return sorted(ids)

    old, new = await _both_forms(monkeypatch, all_ids)
    assert old == new


@pytest.mark.anyio
async def test_filter_form_writes(two_projects, monkeypatch):
    """Update and delete by query only touch the project itself (they keep their SQL conditions)"""
    monkeypatch.setattr(get_settings(), "filter_clauses", True)
    a, b = two_projects
    result = await update_tag_query(a, "add", "tags", "z", queries={"q": "common"})
    assert result["updated"] == 40
    assert (await _search(a, filters={"tags": FilterSpec(values=["z"])}))[0] == 40
    assert (await _search(b, filters={"tags": FilterSpec(values=["z"])}))[0] == 0
    result = await delete_query(a, queries={"q": "aword1"})
    assert result["updated"] == len([i for i in range(40) if i % 7 == 1])
    assert (await _search(b))[0] == 25
    assert (await _search(a))[0] == 40 - result["updated"]


async def _pages(client, user, url, body) -> tuple[list[int], list[dict]]:
    r = await post_json(client, url, user=user, json=body, expected=200)
    seen, metas = [h["i"] for h in r["results"]], [r["meta"]]
    while r["meta"]["next"]:
        r = await post_json(client, url, user=user, json={**body, "after": r["meta"]["next"]}, expected=200)
        seen += [h["i"] for h in r["results"]]
        metas.append(r["meta"])
    return seen, metas


@pytest.mark.anyio
async def test_sort_id_keyset(client, index, user):
    """sort _id pages a query by internal id with an 'after this id' cursor, in both directions"""
    await create_project_role(user, index, Roles.READER)
    await upload(
        index,
        docs=[{"i": i, "text": "even" if i % 2 == 0 else "odd"} for i in range(66)],
        fields={"i": "integer", "text": "text"},
    )
    url = f"/index/{index}/query"
    for direction in ("asc", "desc"):
        body = {"queries": ["even"], "sort": [{"_id": {"order": direction}}], "per_page": 10, "fields": ["i"]}
        seen, metas = await _pages(client, user, url, body)
        assert sorted(seen) == list(range(0, 66, 2)) and len(seen) == 33
        assert all(m["total_count"] == 33 for m in metas)
        assert "id" in _decode_cursor(metas[0]["next"])  # a keyset cursor, not an offset
        # the same order as one big page
        r = await post_json(client, url, user=user, json={**body, "per_page": 1000}, expected=200)
        assert seen == [h["i"] for h in r["results"]]
        if direction == "asc":
            ascending = seen
        else:
            assert seen == list(reversed(ascending))
    # "sort": "_id" as a plain string works too
    seen, _ = await _pages(client, user, url, {"queries": ["odd"], "sort": "_id", "per_page": 10, "fields": ["i"]})
    assert sorted(seen) == list(range(1, 66, 2))


@pytest.mark.anyio
async def test_default_cursors_unchanged(index):
    """Without sort, a query keeps the offset cursor and no query keeps the id cursor"""
    await upload(index, docs=[{"i": i, "text": "word"} for i in range(30)], fields={"i": "integer", "text": "text"})
    r = await query_documents(index, fields=[FieldSpec(name="i")], queries={"q": "word"}, per_page=10)
    assert r.next is not None and "offset" in _decode_cursor(r.next)
    r = await query_documents(index, fields=[FieldSpec(name="i")], per_page=10)
    assert r.next is not None and "id" in _decode_cursor(r.next)


@pytest.mark.anyio
async def test_max_offset(client, index, user, monkeypatch):
    """Offset paging stops at AMCAT4_MAX_OFFSET with a 400 that names sort _id; id paging has no limit"""
    monkeypatch.setattr(get_settings(), "max_offset", 20)
    await create_project_role(user, index, Roles.READER)
    await upload(index, docs=[{"i": i, "text": "word"} for i in range(50)], fields={"i": "integer", "text": "text"})
    url = f"/index/{index}/query"
    body = {"queries": ["word"], "per_page": 10, "fields": ["i"]}
    await post_json(client, url, user=user, json={**body, "page": 1}, expected=200)  # offset 10 + 10 = 20: allowed
    r = await post_json(client, url, user=user, json={**body, "page": 2}, expected=400)
    assert '"sort": "_id"' in r["message"]
    # a cursor that walks past the limit fails the same way
    r = await post_json(client, url, user=user, json=body, expected=200)
    r = await post_json(client, url, user=user, json={**body, "after": r["meta"]["next"]}, expected=200)
    await post_json(client, url, user=user, json={**body, "after": r["meta"]["next"]}, expected=400)
    with pytest.raises(ValueError, match="_id"):
        await query_documents(index, queries={"q": "word"}, sort=[{"i": SortSpec(order="desc")}], page=5, per_page=10)
    # paging by id and paging without query are not limited
    seen, _ = await _pages(client, user, url, {**body, "sort": "_id"})
    assert sorted(seen) == list(range(50))
    seen, _ = await _pages(client, user, url, {"per_page": 10, "fields": ["i"]})
    assert sorted(seen) == list(range(50))
    monkeypatch.setattr(get_settings(), "max_offset", 0)  # 0 = no limit
    await post_json(client, url, user=user, json={**body, "page": 4}, expected=200)


@pytest.mark.anyio
@pytest.mark.parametrize("kwargs", SEARCH_SHAPES)
async def test_single_count_equals_capped_count(two_projects, monkeypatch, kwargs):
    """One count (default) and the two-step capped count (AMCAT4_CAPPED_COUNT) give the same totals"""
    monkeypatch.setattr(search_module, "COUNT_STEP", 3)  # so the capped count needs its second step
    a, b = two_projects
    totals = []
    for capped in (False, True):
        monkeypatch.setattr(get_settings(), "capped_count", capped)
        totals.append([(await _search(index, **kwargs))[0] for index in (a, b, [a, b])])
    assert totals[0] == totals[1]


@pytest.mark.anyio
async def test_parallel_count_uses_second_connection(two_projects, monkeypatch):
    """With a spare connection, the count runs next to the page on another connection; the total is the same"""
    a, _ = two_projects
    fs = await get_fieldset([a])
    used = []
    original = search_module.count

    async def spy(conn, *args, **kwargs):
        used.append(conn)
        return await original(conn, *args, **kwargs)

    monkeypatch.setattr(search_module, "count", spy)
    query = SearchQuery(queries={"q": "common"})
    totals = []
    for parallel in (True, False):
        monkeypatch.setattr(get_settings(), "parallel_count", parallel)
        async with connection() as conn:
            r = await search_module.search(conn, fs, query, ["pub"], per_page=5)
            totals.append(r.total)
            assert len(r.results) == 5
            assert (used[-1] is not conn) == parallel
    assert totals == [40, 40]


@pytest.mark.anyio
async def test_parallel_count_busy_pool(two_projects):
    """When the pool has fewer than two free connections, the count runs after the page (and nothing waits)"""
    a, _ = two_projects
    fs = await get_fieldset([a])
    pool = db()
    async with connection() as conn:
        for keep_free in (0, 1):
            held = [await pool.getconn() for _ in range(pool.max_size - 1 - keep_free)]
            try:
                async with spare_connection() as spare:
                    assert spare is None
                coro = search_module.search(conn, fs, SearchQuery(queries={"q": "common"}), ["pub"], per_page=5)
                r = await asyncio.wait_for(coro, timeout=5)
                assert r.total == 40
            finally:
                for c in held:
                    await pool.putconn(c)


@pytest.mark.anyio
async def test_statement_timeout_only_for_api(monkeypatch):
    """API connections have the statement timeout; jobs, maintenance and long writes (server_defaults) do not"""
    monkeypatch.setattr(get_settings(), "statement_timeout_ms", 0)
    assert "statement_timeout" not in _api_options()
    monkeypatch.setattr(get_settings(), "statement_timeout_ms", 1500)
    assert "-c statement_timeout=1500" in _api_options()
    query = "SELECT source FROM pg_settings WHERE name = 'statement_timeout'"
    with server_defaults():
        assert (await fetch_one(query))["source"] != "client"  # type: ignore[index]

    @with_server_defaults
    async def long_write():
        return db()

    assert await long_write() is not db()  # the decorated function uses the work pool
    assert db() is CONNECTIONS.db


@pytest.mark.anyio
async def test_statement_timeout_gives_503(client, index, user, monkeypatch):
    async def slow_query(*args, **kwargs):
        async with connection() as conn, conn.transaction():
            await conn.execute("SET LOCAL statement_timeout = 50")
            await conn.execute("SELECT pg_sleep(2)")

    await create_project_role(user, index, Roles.READER)
    monkeypatch.setattr(index_query_module, "query_documents", slow_query)
    r = await post_json(client, f"/index/{index}/query", user=user, json={}, expected=503)
    assert "longer than" in r["detail"]


@pytest.mark.anyio
async def test_pool_exhausted_gives_quick_503(client, index, user, monkeypatch):
    """When all API connections are busy, a request gets 503 with Retry-After after pool_timeout_s, not a long wait"""
    await create_project_role(user, index, Roles.READER)
    pool = db()
    monkeypatch.setattr(pool, "timeout", 0.3)
    held = []
    try:
        while (stats := pool.get_stats())["pool_available"] or stats["pool_size"] < pool.max_size:
            held.append(await pool.getconn())
        start = time.perf_counter()
        r = await client.post(f"/index/{index}/query", json={}, cookies=auth_cookie(user, None))
        assert r.status_code == 503 and r.headers["Retry-After"] == "1"
        assert time.perf_counter() - start < 3
    finally:
        for c in held:
            await pool.putconn(c)
    await post_json(client, f"/index/{index}/query", user=user, json={}, expected=200)
