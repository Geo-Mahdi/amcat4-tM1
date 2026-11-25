"""
Tests for the new query API of ParadeDB (amcat4.postgres.pdb_sql, switch AMCAT4_NEW_QUERY_API).

AmCAT builds every search condition as a json query tree. With the switch off it is sent as a legacy json object
(`documents.id @@@ '{...}'::jsonb`), with the switch on as SQL predicates of the new API. Every test here runs the same
search in both formats on the same documents and compares: the hits, the count, the score of every hit and the order
of the first pages. The comparison is exact (scores within float tolerance).

Also here: what the renderer writes for each node of the tree, the plans (EXPLAIN) of the main statements in both
formats, and tests that record why some predicates are written the way they are (pg_search 0.27.0); these fail when a
new pg_search version behaves differently, which is the moment to look at pdb_sql.py again.

The number of random cases is AMCAT4_NEW_API_CASES (default 150).
"""

import json
import os
import random
from contextlib import contextmanager
from typing import Any

import pytest
from psycopg import sql

from amcat4.config import get_settings
from amcat4.models import FieldSpec, FilterSpec, ProjectSettings, SnippetParams, SortSpec
from amcat4.postgres import cutoff_sort, pdb_sql
from amcat4.postgres.connection import connection
from amcat4.postgres.fields import QueryError
from amcat4.postgres.pdb_sql import NotRenderable, render, search_condition
from amcat4.postgres.search import SearchQuery, compile_search, count
from amcat4.projects.aggregate import Aggregation, Axis, query_aggregate
from amcat4.projects.index import create_project_index, delete_project_index, index_size_in_bytes
from amcat4.projects.query import delete_query, query_documents, update_tag_query
from amcat4.systemdata.fields import field_stats, field_values, get_fieldset
from tests.conftest import upload

pytestmark = pytest.mark.anyio

A = "amcat4_unittest_newapi_a"
B = "amcat4_unittest_newapi_b"
N_CASES = int(os.environ.get("AMCAT4_NEW_API_CASES", "150"))
# Two projects with the same field names in another order: every field has another json key in each project
FIELDS = {
    "title": "text",
    "text": "text",
    "kw": "keyword",
    "num": "number",
    "i": "integer",
    "date": "date",
    "tags": "tag",
    "day": "date",
    "flag": "boolean",
    "link": "url",
    "loc": "geo_point",
}
WORDS = [f"w{k}" for k in range(40)] + ["economy", "economic", "economics", "house", "of", "representatives", "represent"]
KEYWORDS = ["alpha", "Beta", "gamma", "delta", "épsilon", "zeta", "Zeta", "10", "9", "a b", ""]
NUMBERS = [17, 17.5, 18, 3.25, 40, -2, -2.5, 0, 1000000, 0.1, -1000.75]
TAGS = ["sport", "economy", "politics", "Art", "zoo", "x"]
DAYS = ["1987-03-04", "1999-12-31T23:59:59.999999", "2000-01-01", "2015-06-07T08:09:10.000001", "2015-06-07T08:09:10"]


def make_docs(n: int, seed: int, prefix: str = "d") -> list[dict[str, Any]]:
    rnd = random.Random(seed)
    docs = []
    for k in range(n):
        # words with very different frequencies and texts of different lengths, so that scores differ
        length = rnd.choice([5, 10, 20, 60])
        words = [WORDS[min(int(rnd.expovariate(0.1)), len(WORDS) - 1)] for _ in range(length)]
        doc: dict[str, Any] = {"_id": f"{prefix}{k}", "text": " ".join(words)}
        if rnd.random() < 0.9:
            doc["title"] = " ".join(rnd.choice(WORDS) for _ in range(rnd.randrange(1, 6)))
        if rnd.random() < 0.9:
            doc["kw"] = rnd.choice(KEYWORDS)
        if rnd.random() < 0.9:
            doc["num"] = rnd.choice(NUMBERS) if rnd.random() < 0.7 else round(rnd.uniform(-50, 50), 3)
        if rnd.random() < 0.9:
            doc["i"] = rnd.randrange(-5, 40)
        if rnd.random() < 0.9:
            doc["date"] = f"20{rnd.randrange(10, 24)}-{rnd.randrange(1, 13):02d}-{rnd.randrange(1, 28):02d}"
        if rnd.random() < 0.8:
            doc["tags"] = rnd.sample(TAGS, rnd.randrange(0, 4))
        if rnd.random() < 0.85:
            doc["day"] = (
                rnd.choice(DAYS)
                if rnd.random() < 0.4
                else f"{rnd.randrange(1995, 2024)}-05-01T{rnd.randrange(0, 24):02d}:30:00"
            )
        if rnd.random() < 0.7:
            doc["flag"] = rnd.random() < 0.5
        if rnd.random() < 0.6:
            doc["link"] = f"https://example.com/{rnd.randrange(50)}"
        if rnd.random() < 0.5:
            doc["loc"] = {"lat": round(rnd.uniform(-80, 80), 4), "lon": round(rnd.uniform(-170, 170), 4)}
        docs.append(doc)
    # a few documents with known values
    docs.append({"_id": f"{prefix}-house", "text": "the House of Representatives met", "date": "2015-03-04", "kw": "alpha"})
    docs.append({"_id": f"{prefix}-west", "text": "w1 far west", "loc": {"lat": 5.5, "lon": -120.25}, "num": 17, "i": 5})
    return docs


@pytest.fixture(scope="module")
async def projects():
    settings = get_settings()
    before = settings.auto_sort_slot
    # (as in test_parity.py: only the standard slots, so that "i", "num" and "day" are sorted on their json value)
    settings.auto_sort_slot = False
    try:
        for name, fields, n, seed in ((A, FIELDS, 1198, 5), (B, dict(reversed(FIELDS.items())), 498, 6)):
            # (document ids start with d in project A and with e in project B)
            await delete_project_index(name, ignore_missing=True)
            await create_project_index(ProjectSettings(id=name))
            docs = make_docs(n, seed, prefix="d" if name == A else "e")
            for start in range(0, n, 400):
                await upload(name, docs[start : start + 400], fields=fields)  # type: ignore[arg-type]
        yield [A, B]
    finally:
        settings.auto_sort_slot = before
        for name in (A, B):
            await delete_project_index(name, ignore_missing=True)


@pytest.fixture(autouse=True)
def no_legacy_fallback(monkeypatch):
    """A search condition that silently falls back to the legacy form would make these tests compare it with itself"""

    def fail(message, *args, **kwargs):
        raise AssertionError(message)

    monkeypatch.setattr(pdb_sql.logging, "warning", fail)


@contextmanager
def query_api(new: bool):
    settings = get_settings()
    before = settings.new_query_api
    settings.new_query_api = new
    try:
        yield
    finally:
        settings.new_query_api = before


def _value(value: Any) -> Any:
    """Filter values are text or whole numbers in the API: a decimal number is sent as text"""
    if isinstance(value, list):
        return [_value(v) for v in value]
    return str(value) if isinstance(value, float) else value


def _specs(filters: dict[str, dict] | None) -> dict[str, FilterSpec] | None:
    return {name: FilterSpec(**{k: _value(v) for k, v in spec.items()}) for name, spec in (filters or {}).items()} or None


async def _all_hits(index, queries, filters, queryable=None, fast=True) -> list[tuple[int, str, float | None]]:
    """Every hit with its score, in id order (materialized: sorted by postgres, without a Top-K scan)"""
    fieldset = await get_fieldset(index, queryable=queryable)
    c = compile_search(fieldset, SearchQuery(queries=queries, filters=_specs(filters)), fast=fast)
    stmt = sql.SQL(
        "WITH hits AS MATERIALIZED (SELECT documents.id, documents.project_pk, documents.doc_id, {} AS s FROM {} WHERE {}) "
        "SELECT project_pk, doc_id, s FROM hits ORDER BY id"
    ).format(pdb_sql.score_sql(), c.table, c.where)
    async with connection() as conn:
        cur = await conn.execute(stmt, c.params)
        return [(r["project_pk"], r["doc_id"], r["s"]) for r in await cur.fetchall()]  # type: ignore[index, call-overload]


async def _count(index, queries, filters, queryable=None) -> int:
    fieldset = await get_fieldset(index, queryable=queryable)
    async with connection() as conn:
        return await count(conn, fieldset, SearchQuery(queries=queries, filters=_specs(filters)))


async def _page(index, queries, filters, sort=None, page=0, per_page=10, queryable=None, **kwargs):
    r = await query_documents(
        index,
        fields=kwargs.pop("fields", [FieldSpec(name="kw")]),
        queries=queries,
        filters=_specs(filters),
        sort=[{name: SortSpec(order=order)} for name, order in sort] if sort else None,  # type: ignore[arg-type]
        page=page,
        per_page=per_page,
        queryable=queryable,
        **kwargs,
    )
    return r.total_count, r.data


async def _run(function, *args, **kwargs):
    """The result of a search in one format, or the error it gives (errors must be the same in both formats too)"""
    try:
        return await function(*args, **kwargs)
    except (QueryError, ValueError) as e:
        return ("error", type(e).__name__, str(e))


# Scores are 32-bit floats. The new format gives the same score as the legacy one, or (a score that is the sum of three
# or more parts, see test_scores_of_three_or_more_parts and NEW_QUERY_API.md) a score that differs in its last digit
TOLERANCE = 2e-6


def _close(a: float | None, b: float | None) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) <= TOLERANCE * max(1.0, abs(a))


def _same_order(ids: list[str], new_ids: list[str], scores: dict[str, float | None], by_score: bool) -> bool:
    """
    The same documents in the same order. In relevance order, two documents may be in each other's place if their
    scores are the same up to the last digit (their order is then decided by rounding, see TOLERANCE)
    """
    if ids == new_ids or not by_score:
        return ids == new_ids
    return len(ids) == len(new_ids) and all(a == b or _close(scores[a], scores[b]) for a, b in zip(ids, new_ids))


async def compare(index, queries=None, filters=None, queryable=None, sorts=((),), count_is_hits=True, exact=False) -> int:
    """
    Run the search in both formats and compare all hits, their scores, the count and the first pages in every given
    sort order (() is the default order: relevance with a query string, else the order of storing).
    count_is_hits=False: do not compare the count with the number of scored hits (see test_sloppy_phrase_of_three_words).
    exact=True: the scores must be the very same numbers, and so the relevance order the very same order.
    Returns the number of hits.
    """
    results = {}
    for new in (False, True):
        with query_api(new):
            hits = await _run(_all_hits, index, queries, filters, queryable)
            n = await _run(_count, index, queries, filters, queryable)
            pages = [
                await _run(_page, index, queries, filters, list(sort), page, 10, queryable)
                for sort in sorts
                for page in (0, 2)
            ]
            results[new] = (hits, n, pages)
    (hits, n, pages), (new_hits, new_n, new_pages) = results[False], results[True]
    what = f"queries={queries} filters={filters}"
    if isinstance(hits, tuple):
        assert new_hits == hits and new_n == n and new_pages == pages, f"{what}: another error"
        return 0
    assert [h[:2] for h in new_hits] == [h[:2] for h in hits], f"{what}: other hits ({len(new_hits)} and {len(hits)})"
    if exact:
        assert new_hits == hits, f"{what}: other scores"
    assert all(_close(a[2], b[2]) for a, b in zip(hits, new_hits)), f"{what}: other scores"
    assert new_n == n, f"{what}: counts {n} (legacy), {new_n} (new)"
    assert n == len(hits) or not count_is_hits, f"{what}: count {n}, {len(hits)} hits"
    scores = {h[1]: h[2] for h in hits}
    page_sorts = [sort for sort in sorts for _ in (0, 2)]
    for sort, (total, data), (new_total, new_data) in zip(page_sorts, pages, new_pages):
        by_score = not exact and bool(queries) and (not sort or any(name == "_score" for name, _ in sort))
        assert new_total == total, f"{what}: page total {total} and {new_total}"
        ids, new_ids = [d["_id"] for d in data], [d["_id"] for d in new_data]
        assert _same_order(ids, new_ids, scores, by_score), f"{what} sort={sort}: another page order: {ids} and {new_ids}"
    return len(hits)


# ---------------------------------------------------------------- query strings

QUERY_STRINGS = [
    # words and boolean operators
    "w1",
    "w1 w2",
    "w3 w17 economy",
    "w1 AND w2",
    "w1 OR w2",
    "w1 AND NOT w2",
    "NOT w1",
    "-w1",
    "w5 -w1",
    "+w1 w2",
    "+w1 +w2 w3 w30",
    "(w1 OR w2) AND (w3 OR NOT w4)",
    "w1 AND (w2 OR (w3 AND NOT (w4 OR w5)))",
    "NOT (w1 OR w2) AND NOT w3",
    # what postgres rewrites: the same word twice, a common part of two branches, a NOT inside a NOT
    "w1 w1",
    "w1 w1 w1 w2",
    "w1 AND w1",
    "(w1 AND w2) OR (w1 AND w3)",
    "(w1 AND w2 AND w4) OR (w2 AND w1 AND w5) OR (w1 AND w6)",
    "(title:w1^2 AND w2) OR (title:w1^2 AND w3) OR title:w1",
    '("w1 w2"~2 AND w3) OR ("w1 w2"~2 AND w4)',
    "(econ* AND w1) OR (econ* AND w2) OR (num:17 AND w1) OR (num:17 AND w2)",
    "w1 AND NOT (w2 AND NOT w3)",
    "w1 AND NOT (w2 AND NOT (w3 AND NOT w4))",
    "NOT (NOT w1)",
    "w5 OR NOT (w1 OR NOT num:[0 TO 18])",
    "+w1 w1 w2",
    "*",
    "nosuchword",
    # fields: one, several, the default (all text fields)
    "title:w5",
    "text:w5",
    "title:(w5 OR w6)",
    "title:w5 AND text:w6",
    "title:w5 OR text:(w6 AND NOT w1)",
    # phrases, with slop
    '"w1 w2"',
    '"w1 w2"~2',
    '"w2 w1"~1',
    'title:"w1 w2"~5',
    # prefix and phrase prefix
    "econ*",
    "w1*",
    "title:econ*",
    "repr* AND w1",
    '"house of repr*"',
    '"w1 w*"',
    'title:"w1 w*"',
    # fuzzy (the second one is a swap of two letters)
    "econom~",
    "ecnoomy~1",
    "w31~1",
    "economy~2",
    "title:represnt~2",
    # boosts
    "w1^3 w2",
    "w1 w2^0.5",
    '"w1 w2"^2 w3',
    '"w1 w2"~2^3 w4',
    "econ*^2 w1",
    "econom~2^3 w1",
    '"w1 w*"^4 w9',
    "title:w1^5 text:w1",
    "(w1^2 AND w2) OR w3^3",
    # keyword, tag and url fields
    "kw:alpha",
    'kw:"a b"',
    "kw:al*",
    "kw:[b TO e]",
    "kw:{Beta TO gamma}",
    "kw:>=delta",
    "kw:<Zeta",
    "tags:sport",
    "tags:sport AND NOT tags:economy",
    "tags:s*",
    "tags:[a TO p]",
    'link:"https://example.com/7"',
    # numbers: whole, decimal, negative, ranges with every kind of bound
    "num:17",
    "num:17.5",
    "num:17.0",
    "num:[-2 TO -2]",
    "num:[0.5 TO 17.5]",
    "num:{17 TO 40}",
    "num:[17 TO 17.5}",
    "num:>17",
    "num:>=17.5",
    "num:<=0",
    "num:<0.1",
    "num:[* TO -2]",
    "num:[-2.5 TO *]",
    "num:[-1000.75 TO -2.5]",
    "num:[40 TO 17]",
    "i:5",
    "i:[3 TO 25]",
    "i:{3 TO 25}",
    "i:{5 TO 6}",
    "i:>30",
    "i:<=0",
    "i:[* TO -2]",
    "i:[9 TO 5]",
    # dates: a day, a moment, ranges
    "date:2015-03-04",
    "date:[2013-01-01 TO 2019-06-15]",
    "date:{2013-01-01 TO 2019-06-15}",
    "date:>2020-01-01",
    "date:<=2012-12-12",
    "date:[2019-01-01 TO 2012-01-01]",
    "day:2015-06-07",
    'day:"2015-06-07T08:09:10"',
    'day:"2015-06-07T08:09:10.000001"',
    "day:[2000-01-01 TO 2015-06-07T08:09:10]",
    'day:{"1999-12-31T23:59:59.999999" TO 2015-06-07T08:09:10.000001}',
    "day:<2000-01-01",
    # booleans and sub-fields
    "flag:true",
    "flag:false",
    "loc.lat:[0 TO 10]",
    "loc.lon:[-180 TO -100.5]",
    # combinations
    "w1 AND kw:alpha AND num:[0 TO 18]",
    "(w1 OR econ*) AND NOT tags:zoo AND date:>2015-01-01",
    'title:w3^2 OR ("w1 w2"~3 AND i:[0 TO 20]) OR kw:Be*',
    "+w1 kw:alpha num:17",
    # errors (the same error in both formats)
    "num:abc",
    "nosuchfield:w1",
    "w1 AND (",
    "te?t",
]


@pytest.mark.parametrize("q", QUERY_STRINGS)
async def test_query_string(projects, q):
    """Every kind of query string: same hits, scores, counts and relevance order, in one project and in two"""
    await compare(A, {"q": q})
    await compare([A, B], {"q": q}, sorts=((), (("_score", "desc"), ("i", "asc"))))


async def test_query_strings_find_something(projects):
    """(so that the test above does not compare empty results)"""
    with query_api(True):
        empty = [q for q in QUERY_STRINGS if not isinstance(n := await _run(_count, A, {"q": q}, None), int) or n == 0]
    assert set(empty) <= {
        "nosuchword",
        "num:[40 TO 17]",
        "i:{5 TO 6}",
        "i:[9 TO 5]",
        "date:[2019-01-01 TO 2012-01-01]",
        "num:abc",
        "nosuchfield:w1",
        "w1 AND (",
        "te?t",
    }


async def test_sloppy_phrase_of_three_words(projects):
    """
    A phrase of three or more words with slop: the same in both formats. Not compared here: the count and the number
    of scored hits. pg_search 0.27.0 finds more documents for such a phrase when it does not compute scores (count,
    sort on a field) than when it does (relevance order), in the legacy format as much as in the new one (seen on
    this test set: 236 counted, 152 with scores). Phrases of two words do not have this.
    """
    for q in ('"w1 w2 w3"~3', '"w3 w1 w2"~2 OR "w4 w5 w6"~4', 'title:"w1 w2 w3"~10'):
        await compare(A, {"q": q}, count_is_hits=False)
        await compare([A, B], {"q": q}, {"num": {"gt": 0}}, sorts=((), (("date", "asc"),)), count_is_hits=False)
    assert await compare(A, {"q": '"w1 w2 w3"~3'}, count_is_hits=False) > 0


EXACT_QUERIES = [
    "text:w1",
    "text:w1 OR text:w2",
    "text:w1 AND title:w2",
    "text:w1^3 OR title:w2",
    'text:"w1 w2"~2',
    'text:"w1 w2" OR title:w3^0.5',
    "+text:w1 text:w2",
    "text:w1 AND NOT title:w2",
    "text:w1 AND kw:alpha",
]


@pytest.mark.parametrize("q", EXACT_QUERIES)
async def test_same_scores_exactly(projects, q):
    """
    A score that is the sum of at most two parts is the very same number in both formats, and so the relevance order
    is the very same order. (Filters are not scored, so they do not matter.)
    """
    assert await compare(A, {"q": q}, sorts=((), (("_score", "asc"),)), exact=True) > 0
    await compare(A, {"q": q}, {"num": {"gt": -3}, "kw": {"values": ["alpha", "Beta", "gamma"]}}, exact=True)
    # the same holds for the default search of a user who can search one text field
    words = q.replace("text:", "").replace("title:", "")
    await compare(A, {"q": words}, queryable={"text", "kw", "num", "i"}, exact=True)


async def test_scores_of_three_or_more_parts(projects):
    """
    A score is a sum of 32-bit floats, one per matching part of the query. The two formats give pg_search the same
    parts in another grouping (postgres flattens an OR inside an OR and an AND inside an AND before pg_search sees
    them, and the filters are separate clauses instead of one), so three or more parts can be added up in another
    order. The score can then differ in its last digit (8.647248 and 8.647249), and two documents with (nearly) the
    same score can change places in the relevance order. Hits and counts are the same.
    """
    queries = {"q": "w3 w2 w1"}
    results = []
    for new in (False, True):
        with query_api(new):
            results.append(await _all_hits(A, queries, None))
    legacy, new_hits = results
    assert [h[:2] for h in legacy] == [h[:2] for h in new_hits] and len(legacy) > 500
    assert all(_close(a[2], b[2]) for a, b in zip(legacy, new_hits))
    differ = sum(a[2] != b[2] for a, b in zip(legacy, new_hits))
    print(f"three words in two text fields: {differ} of {len(legacy)} scores differ in their last digit")
    assert differ < 0.3 * len(legacy)


async def test_several_queries(projects):
    """Several labelled queries are combined with OR"""
    queries = {"a": "w1", "b": "title:w2 AND NOT w3", "c": '"w4 w5"~2^2', "d": "kw:alpha"}
    assert await compare([A, B], queries) > 0
    # queries that share words (each query scores its own copy of the word)
    assert await compare([A, B], {"a": "w1 AND w2", "b": "w1 AND w3", "c": "w1"}, sorts=((), (("_score", "asc"),))) > 0
    assert await compare(A, {"a": "text:w1 AND text:w2", "b": "text:w1 AND text:w3"}) > 0
    assert await compare(A, queries, {"num": {"gte": 0}}) > 0


async def test_queryable(projects):
    """A user who may only search some fields: the default search uses these fields only, in both formats"""
    all_fields = await compare([A, B], {"q": "w5 economy"})
    title_only = await compare([A, B], {"q": "w5 economy"}, queryable={"title", "kw"})
    assert 0 < title_only < all_fields
    assert await compare([A, B], {"q": "w5"}, {"kw": {"values": ["alpha", "Beta"]}}, queryable={"title", "kw"}) > 0
    # a field that may not be searched: the same error
    for new in (False, True):
        with query_api(new):
            with pytest.raises(QueryError, match="Unknown field: text"):
                await _all_hits(A, {"q": "text:w5"}, None, queryable={"title"})
            with pytest.raises(QueryError, match="Unknown field: num"):
                await _all_hits(A, None, {"num": {"gt": 1}}, queryable={"title"})


# ---------------------------------------------------------------- filters

FILTERS: list[dict[str, dict]] = [
    # values: text, keyword, tag, url, boolean
    {"title": {"values": ["w5"]}},
    {"text": {"values": ["w5 w6", "economy"]}},
    {"kw": {"values": ["alpha"]}},
    {"kw": {"values": ["alpha", "Beta", "a b", "nothing"]}},
    {"kw": {"values": [""]}},
    {"kw": {"values": ["it's", '{"all":null}', "a\\b"]}},
    {"tags": {"values": ["sport"]}},
    {"tags": {"values": ["sport", "Art"]}},
    {"link": {"values": ["https://example.com/7", "https://example.com/8"]}},
    {"flag": {"values": [True]}},
    {"flag": {"values": [False]}},
    # numbers: whole, decimal and negative values on one field
    {"num": {"values": [17]}},
    {"num": {"values": ["17"]}},
    {"num": {"values": ["17.0"]}},
    {"num": {"values": [17.5]}},
    {"num": {"values": [-2]}},
    {"num": {"values": [-2.5]}},
    {"num": {"values": [0]}},
    {"num": {"values": [17, 17.5, -2, 1000000, 0.1]}},
    {"num": {"gt": 0, "lt": 18}},
    {"num": {"gte": 17, "lte": 17.5}},
    {"num": {"gt": 17, "lt": 17.5}},
    {"num": {"gte": -2.5}},
    {"num": {"gt": -2.5}},
    {"num": {"lte": 17}},
    {"num": {"lt": 17}},
    {"num": {"lt": -1000}},
    {"num": {"gte": 18, "lte": 17}},
    {"i": {"values": [5]}},
    {"i": {"values": [-3, 0, 39]}},
    {"i": {"gt": 3, "lt": 25}},
    {"i": {"gte": 3, "lte": 25}},
    {"i": {"gte": -5, "lt": -4}},
    {"i": {"gt": 5, "lt": 6}},
    {"i": {"lte": -1}},
    {"i": {"gt": 38}},
    # dates with and without a time, open and closed ranges
    {"date": {"values": ["2015-03-04"]}},
    {"date": {"values": ["2015-03-04", "2020-01-01", "2012-12-12"]}},
    {"date": {"gte": "2013-01-01", "lt": "2019-06-15"}},
    {"date": {"gt": "2013-01-01", "lte": "2019-06-15"}},
    {"date": {"gte": "2020-01-01"}},
    {"date": {"lt": "2012-06-01"}},
    {"date": {"gte": "2019-01-01", "lte": "2012-01-01"}},
    {"day": {"values": ["2015-06-07T08:09:10"]}},
    {"day": {"values": ["2015-06-07T08:09:10.000001", "1987-03-04"]}},
    {"day": {"gte": "1999-12-31T23:59:59.999999", "lte": "2000-01-01"}},
    {"day": {"gt": "1999-12-31T23:59:59.999999", "lt": "2015-06-07T08:09:10.000001"}},
    {"day": {"gt": "2015-06-07T08:09:10", "lt": "2015-06-07T08:09:10.000001"}},
    {"day": {"lte": "1999-12-31T23:59:59.999999"}},
    {"day": {"gte": "2005-05-01T12:30:00+02:00"}},
    {"date": {"monthnr": 3}},
    {"date": {"dayofweek": "Monday"}},
    {"date": {"monthnr": 12, "gte": "2015-01-01"}},
    # text ranges on keywords and tags
    {"kw": {"gte": "b", "lt": "e"}},
    {"kw": {"gt": "Beta"}},
    {"kw": {"lte": "9"}},
    {"tags": {"gte": "economy", "lte": "politics"}},
    {"link": {"gt": "https://example.com/3"}},
    # exists, for every kind of field
    *[
        {name: {"exists": exists}}
        for name in ("title", "kw", "num", "i", "date", "day", "tags", "flag", "link")
        for exists in (True, False)
    ],
    # sub-fields of a geo point
    {"loc.lat": {"gte": 0, "lt": 10}},
    {"loc.lon": {"lt": -100.5}},
    # document ids (a SQL condition next to the search condition)
    {"_id": {"values": ["d1", "d5", "d7", "nothing"]}},
    # several filters
    {"kw": {"values": ["alpha", "Beta"]}, "num": {"gt": 0}, "tags": {"exists": True}},
    {"date": {"gte": "2015-01-01"}, "i": {"lt": 20}, "title": {"exists": False}},
    {"num": {"values": [17, 18]}, "flag": {"values": [True]}, "day": {"exists": True}},
    {"_id": {"values": ["d1", "d5", "d7"]}, "kw": {"exists": True}},
]


@pytest.mark.parametrize("filters", FILTERS, ids=lambda f: json.dumps(f)[:60])
async def test_filter(projects, filters):
    """Every kind of filter: same hits and counts, alone and with a query string, in one project and in two"""
    await compare(A, None, filters, sorts=((), (("date", "desc"),)))
    await compare([A, B], {"q": "w1 w2 title:w3^2"}, filters)


async def test_filters_find_something(projects):
    with query_api(True):
        empty = [f for f in FILTERS if await _count(A, None, f) == 0]
    assert empty == [
        {"kw": {"values": ["it's", '{"all":null}', "a\\b"]}},
        {"num": {"gt": 17, "lt": 17.5}},
        {"num": {"gte": 18, "lte": 17}},
        {"i": {"gt": 5, "lt": 6}},
        {"date": {"gte": "2019-01-01", "lte": "2012-01-01"}},
        {"day": {"gt": "2015-06-07T08:09:10", "lt": "2015-06-07T08:09:10.000001"}},
    ], empty


@pytest.mark.parametrize("switch", ["filter_clauses", "fast_path", "text_exists_key"])
async def test_other_switches_off(projects, monkeypatch, switch):
    """
    The other forms of the search condition: scored filters (AMCAT4_FILTER_CLAUSES=false), the parent table with SQL
    conditions on the project (AMCAT4_FAST_PATH=false), 'exists' on a text field as a SQL condition
    """
    monkeypatch.setattr(get_settings(), switch, False)
    for filters in (
        {"kw": {"values": ["alpha", "Beta"]}, "num": {"gt": 0}},
        {"title": {"exists": True}},
        {"title": {"exists": False}, "i": {"lt": 20}},
        {"date": {"gte": "2015-01-01"}, "tags": {"values": ["sport"]}},
    ):
        assert await compare([A, B], {"q": "w1 w2^2 -w3"}, filters, sorts=((), (("i", "asc"),))) > 0
        assert await compare(A, None, filters, sorts=((), (("kw", "desc"),))) > 0


# ---------------------------------------------------------------- sorting, snippets, aggregations, writes


async def test_sorts(projects, monkeypatch):
    """Pages in every kind of order: relevance, a sort slot column, a json field (cutoff sort and late fetch), id"""
    sorts = (
        (),
        (("_score", "desc"),),
        (("_score", "asc"),),
        (("date", "desc"),),
        (("kw", "asc"), ("date", "asc")),
        (("i", "asc"),),
        (("num", "desc"),),
        (("day", "asc"), ("i", "desc")),
        (("tags", "asc"),),
        (("link", "desc"),),
        (("_seq", "desc"),),
        (("_id", "asc"),),
    )
    calls = []
    original = cutoff_sort.page_ids

    async def recording(*args, **kwargs):
        ids = await original(*args, **kwargs)
        calls.append((get_settings().new_query_api, ids is not None))
        return ids

    monkeypatch.setattr(cutoff_sort, "page_ids", recording)
    for queries, filters in (
        ({"q": "w1 w2"}, None),
        (None, {"num": {"gt": -3}}),
        ({"q": "w1 OR econ*"}, {"kw": {"exists": True}, "date": {"gte": "2012-01-01"}}),
    ):
        assert await compare(A, queries, filters, sorts=sorts) > 30
    # the cutoff sort gave the page as often in the new format as in the legacy one
    assert sum(used for new, used in calls if new) == sum(used for new, used in calls if not new) > 0
    for limits in ({"SPLIT_ROWS": 4, "ZOOM_ROWS": 4, "HIST_BUCKETS": 6}, {}):
        for constant, value in limits.items():
            monkeypatch.setattr(cutoff_sort, constant, value)
        monkeypatch.setattr(get_settings(), "cutoff_sort", bool(limits))
        assert await compare(A, None, {"num": {"gt": -3}}, sorts=sorts) > 30


async def test_highlight_and_snippets(projects):
    """Highlighting and snippets use the match positions of pg_search: the same text in both formats"""
    fields = [FieldSpec(name="title"), FieldSpec(name="text", snippet=SnippetParams(nomatch_words=5, max_matches=2))]
    for q in ("w1 w2", '"w1 w2"~2', "econ* OR title:w5", "ecnoomy~1 w3", '"house of repr*" OR w4^2'):
        results = []
        for new in (False, True):
            with query_api(new):
                results.append(await _page([A, B], {"q": q}, None, per_page=20, fields=fields, highlight=True))
        assert results[0] == results[1], q
        assert any("<em>" in str(d) for d in results[0][1]), q


AGGREGATIONS = [
    ([Axis("kw")], None),
    ([Axis("tags")], None),
    ([Axis("date", interval="year")], None),
    ([Axis("date", interval="quarter"), Axis("kw")], None),
    ([Axis("flag")], None),
    ([Axis("i")], None),
    ([Axis("kw")], [Aggregation("num", "avg"), Aggregation("i", "max")]),
    ([], [Aggregation("num", "sum"), Aggregation("date", "min")]),
    ([Axis("num", interval="10")], None),
    ([Axis("day", interval="hour")], None),
]


@pytest.mark.parametrize("small_result", [0, 10**9])
async def test_aggregations(projects, monkeypatch, small_result):
    """Aggregations inside the index (pdb.agg) and with GROUP BY, over a search with a query string and filters"""
    from amcat4.projects import aggregate

    monkeypatch.setattr(aggregate, "SMALL_RESULT", small_result)
    for index in (A, [A, B]):
        for queries, filters in (
            (None, None),
            ({"q": "w1 OR w2"}, None),
            (None, {"num": {"gte": 0.1, "lt": 18}, "tags": {"exists": True}}),
            (
                {"a": "w1", "b": "title:w3 AND NOT w5"},
                {"date": {"gte": "2012-06-01"}, "kw": {"values": ["alpha", "Beta", "10"]}},
            ),
        ):
            for axes, aggregations in AGGREGATIONS:
                results = []
                for new in (False, True):
                    with query_api(new):
                        r = await query_aggregate(
                            index, list(axes), list(aggregations or []), queries=queries, filters=_specs(filters), limit=10000
                        )
                        results.append(r.data)
                assert results[0] == results[1], (index, queries, filters, axes)
                assert results[0] or filters, (index, queries, axes)


async def test_aggregate_by_query(projects):
    results = []
    for new in (False, True):
        with query_api(new):
            r = await query_aggregate(A, [Axis("_query"), Axis("kw")], queries={"a": "w1", "b": "econ*"})
            results.append(r.data)
    assert results[0] == results[1] and results[0]


async def test_field_values_and_stats(projects):
    results = []
    for new in (False, True):
        with query_api(new):
            results.append(
                (
                    await field_values(A, "kw", 100),
                    await field_values(A, "tags", 100),
                    await field_stats(A, "num"),
                    await field_stats(A, "i"),
                    await field_stats(A, "date"),
                    await index_size_in_bytes(A),
                )
            )
    assert results[0] == results[1]
    assert results[0][0] and results[0][2]["count"] > 0 and results[0][5] > 0


async def test_update_and_delete_by_query():
    """Tags added and documents deleted by query: the same documents in both formats"""
    name = "amcat4_unittest_newapi_write"
    await delete_project_index(name, ignore_missing=True)
    await create_project_index(ProjectSettings(id=name))
    try:
        await upload(name, make_docs(298, 9), fields=FIELDS)  # type: ignore[arg-type]
        queries, filters = {"q": "w1 OR title:w2"}, {"num": {"gt": 0.05}, "kw": {"exists": True}}
        for new in (False, True):
            with query_api(new):
                done = await update_tag_query(name, "add", "tags", "new" if new else "legacy", queries, _specs(filters))
                assert done["updated"] == done["total"] > 0
        with query_api(False):
            legacy = {d["_id"] for d in (await _page(name, None, {"tags": {"values": ["legacy"]}}, per_page=300))[1]}
            new_ids = {d["_id"] for d in (await _page(name, None, {"tags": {"values": ["new"]}}, per_page=300))[1]}
            assert legacy == new_ids and legacy
            assert legacy == {h[1] for h in await _all_hits(name, queries, filters)}
        with query_api(True):
            done = await update_tag_query(name, "remove", "tags", "new", None, _specs({"tags": {"values": ["legacy"]}}))
            assert done["updated"] == len(legacy)
            assert await _count(name, None, {"tags": {"values": ["new"]}}) == 0
            # delete by query: exactly the documents that the legacy format finds
            deleted = await delete_query(name, queries, _specs(filters))
            assert deleted["updated"] == len(legacy)
            assert await _count(name, None, None) == 300 - len(legacy)
        with query_api(False):
            assert await _count(name, queries, filters) == 0
            assert await _count(name, None, None) == 300 - len(legacy)
    finally:
        await delete_project_index(name, ignore_missing=True)


# ---------------------------------------------------------------- random cases


def random_query(rnd: random.Random, depth: int = 0) -> str:
    def word() -> str:
        return rnd.choice(WORDS[: rnd.choice([5, 15, 40])])

    kind = rnd.choice(["word"] * 5 + ["phrase", "slop", "prefix", "fuzzy", "field", "exact", "range", "group", "group"])
    if depth >= 2 and kind == "group":
        kind = "word"
    if kind == "word":
        q = word()
    elif kind == "phrase":
        q = f'"{word()} {word()}"'
    elif kind == "slop":
        # (two words: with three or more, pg_search 0.27.0 finds other documents with scores than without, in both formats)
        q = f'"{word()} {word()}"~{rnd.randrange(1, 6)}'
    elif kind == "prefix":
        q = rnd.choice(["w1*", "w2*", "econ*", "repr*", "ho*"])
    elif kind == "fuzzy":
        q = rnd.choice(["econom~", "ecnoomy~1", "w31~1", "huose~1", "represnt~2"])
    elif kind == "field":
        return f"{rnd.choice(['title', 'text'])}:{word()}"
    elif kind == "exact":
        return rnd.choice(
            ["kw:alpha", "kw:Be*", "tags:sport", "tags:zoo", "num:17", "num:17.5", "i:7", "flag:true", "date:2015-03-04"]
        )
    elif kind == "range":
        return rnd.choice(
            [
                "num:[0.5 TO 17.5]",
                "num:>17",
                "num:{-2.5 TO 18}",
                "i:[3 TO 25]",
                "i:<10",
                "date:>2016-01-01",
                "kw:[b TO e]",
                "day:<2000-01-01",
            ]
        )
    else:
        operator = rnd.choice([" AND ", " OR ", " "])
        parts = []
        for _ in range(rnd.randrange(2, 4)):
            prefix = rnd.choice(["", "", "", "NOT ", "+", "-"] if operator == " " else ["", "", "", "NOT "])
            parts.append(prefix + random_query(rnd, depth + 1))
        return "(" + operator.join(parts) + ")"
    if rnd.random() < 0.2:
        q += f"^{rnd.choice(['2', '0.5', '3.5', '10'])}"
    return q


def random_filters(rnd: random.Random) -> dict[str, dict]:
    filters: dict[str, dict] = {}
    for _ in range(rnd.choice([0, 0, 1, 1, 2])):
        name = rnd.choice(["kw", "num", "i", "date", "day", "tags", "title", "flag"])
        kind = rnd.choice(["values", "range", "exists"])
        if kind == "exists" or name == "title":
            filters[name] = {"exists": rnd.random() < 0.7}
        elif kind == "values" or name == "flag":
            pool: list[Any] = {
                "kw": KEYWORDS,
                "tags": TAGS,
                "num": [*NUMBERS, "17.0", "-2"],
                "i": list(range(-5, 40)),
                "date": ["2015-03-04", "2020-01-01", "2012-12-12"],
                "day": DAYS,
                "flag": [True, False],
            }[name]
            filters[name] = {"values": rnd.sample(pool, rnd.randrange(1, 3))}
        else:
            lo, hi = {
                "num": (rnd.choice([-2.5, 0, 0.1, 17]), rnd.choice([17, 17.5, 18, 40.5])),
                "i": (rnd.randrange(-5, 20), rnd.randrange(10, 40)),
                "date": ("2013-01-01", "2019-06-15"),
                "day": ("1999-12-31T23:59:59.999999", "2015-06-07T08:09:10"),
                "kw": ("Beta", "gamma"),
                "tags": ("b", "t"),
            }[name]
            spec: dict[str, Any] = {}
            if rnd.random() < 0.8:
                spec[rnd.choice(["gt", "gte"])] = lo
            if rnd.random() < 0.8 or not spec:
                spec[rnd.choice(["lt", "lte"])] = hi
            filters[name] = spec
    return filters


async def test_random_searches(projects):
    """Random query strings with random filters, in one or both projects: the same in both formats"""
    rnd = random.Random(2026)
    with_hits = 0
    for case in range(N_CASES):
        queries = {"q": random_query(rnd)} if rnd.random() < 0.85 else None
        if queries and rnd.random() < 0.15:
            queries["q2"] = random_query(rnd)
        filters = random_filters(rnd)
        index = rnd.choice([A, A, [A, B]])
        sort = rnd.choice(
            [(), (), (("_score", "desc"),), (("date", "desc"),), (("i", "asc"),), (("num", "desc"), ("kw", "asc"))]
        )
        try:
            with_hits += await compare(index, queries, filters, sorts=(sort,)) > 0
        except AssertionError as e:
            raise AssertionError(f"case {case}: {e}") from e
    assert with_hits > 0.6 * N_CASES, with_hits


# ---------------------------------------------------------------- the renderer


def _sql(query: dict) -> tuple[str, list]:
    condition, params = render(query)
    return condition.as_string(None).replace('"', ""), params


def test_render_nodes():
    """What each node of the json tree becomes"""
    m = {"match": {"field": "text_fields.f2", "value": "a b"}}
    t = {"term": {"field": "exact_fields.f3", "value": "x"}}
    assert _sql(m) == ("documents.text_fields->>'f2' ||| %s::text", ["a b"])
    assert _sql(t) == ("documents.exact_fields->>'f3' === %s::text", ["x"])
    assert _sql({"term": {"field": "project_pk", "value": 3}}) == ("documents.project_pk @@@ pdb.term(%s::bigint)", [3])
    assert _sql({"term": {"field": "exact_fields.f4", "value": 17.5}})[0].endswith("pdb.term(%s::float8)")
    assert _sql({"term": {"field": "exact_fields.f4", "value": True}})[0].endswith("pdb.term(%s::boolean)")
    assert _sql({"term_set": {"field": "exact_fields.f4", "terms": [1, 2]}}) == (
        "documents.exact_fields->>'f4' @@@ pdb.term_set(%s::bigint[])",
        [[1, 2]],
    )
    assert _sql({"fuzzy_term": {"field": "text_fields.f2", "value": "a", "distance": 1}}) == (
        "documents.text_fields->>'f2' === %s::text::pdb.fuzzy(1, f, t)",
        ["a"],
    )
    phrase = {"phrase": {"field": "text_fields.f2", "phrases": ["a", "b"], "slop": 0}}
    assert _sql(phrase) == ("documents.text_fields->>'f2' ### %s::text[]", [["a", "b"]])
    phrase["phrase"]["slop"] = 2
    assert _sql(phrase)[0] == "documents.text_fields->>'f2' ### %s::text[]::pdb.slop(2)"
    assert _sql({"phrase_prefix": {"field": "text_fields.f2", "phrases": ["a"], "max_expansions": 9}}) == (
        "documents.text_fields->>'f2' @@@ pdb.phrase_prefix(%s::text[], %s::integer)",
        [["a"], 9],
    )
    assert _sql({"exists": {"field": "exact_fields.f5"}}) == ("documents.exact_fields->>'f5' @@@ pdb.exists()", [])
    assert _sql({"all": None}) == ("documents.id @@@ pdb.all()::pdb.const(0)", [])
    # sub-fields and another table name
    assert _sql({"exists": {"field": "exact_fields.f5.lat"}})[0] == "documents.exact_fields->'f5'->>'lat' @@@ pdb.exists()"
    assert render(t, table="d")[0].as_string(None).startswith('"d"."exact_fields"')
    # (a json key is always a quoted constant)
    assert _sql({"exists": {"field": "exact_fields.f5'; DROP TABLE x"}})[0] == (
        "documents.exact_fields->>'f5''; DROP TABLE x' @@@ pdb.exists()"
    )
    # boost and constant score go on the right-hand side; a constant score wins from a boost inside it
    assert _sql({"boost": {"query": m, "factor": 2.5}})[0] == "documents.text_fields->>'f2' ||| %s::text::pdb.boost(2.5)"
    assert _sql({"const_score": {"score": 0.0, "query": {"boost": {"query": t, "factor": 2}}}})[0].endswith(
        "%s::text::pdb.const(0)"
    )
    assert _sql({"boost": {"factor": 2, "query": {"boost": {"factor": 3, "query": t}}}})[0].endswith("::pdb.boost(6)")
    # boolean
    assert _sql({"boolean": {"must": [m, t]}})[0] == f"({_sql(m)[0]} AND {_sql(t)[0]})"
    assert _sql({"boolean": {"should": [m, t]}})[0] == f"({_sql(m)[0]} OR {_sql(t)[0]})"
    # (what is excluded has a constant score of 0)
    not_t = "NOT (documents.exact_fields->>'f3' === %s::text::pdb.const(0))"
    assert _sql({"boolean": {"must": [{"all": None}], "must_not": [t]}})[0] == not_t
    assert _sql({"boolean": {"must": [m], "must_not": [t]}})[0] == f"({_sql(m)[0]} AND {not_t})"
    # the same scored predicate again: marked, so that postgres does not merge the copies (see _Renderer.distinct)
    assert _sql(
        {"boolean": {"should": [m, m, m, {"const_score": {"score": 0.0, "query": {"boolean": {"must": [t, t]}}}}]}}
    ) == (
        "(documents.text_fields->>'f2' ||| %s::text"
        " OR documents.text_fields->>'f2' ||| CASE WHEN extract(epoch FROM now()) = -1 THEN NULL ELSE %s::text END"
        " OR documents.text_fields->>'f2' ||| CASE WHEN extract(epoch FROM now()) = -2 THEN NULL ELSE %s::text END"
        " OR (documents.exact_fields->>'f3' === %s::text::pdb.const(0)"
        " AND documents.exact_fields->>'f3' === %s::text::pdb.const(0)))",
        ["a b", "a b", "a b", "x", "x"],
    )
    # should next to must only adds to the score
    assert _sql({"boolean": {"must": [m], "should": [t]}})[0] == (
        f"({_sql(m)[0]} AND ({_sql(t)[0]} OR documents.id @@@ pdb.all()::pdb.const(0)))"
    )
    assert _sql({"const_score": {"score": 0.0, "query": {"boolean": {"must": [m], "should": [t]}}}})[0] == (
        "documents.text_fields->>'f2' ||| %s::text::pdb.const(0)"
    )
    assert _sql({"boolean": {"must": [{"all": None}]}})[0] == "documents.id @@@ pdb.all()::pdb.const(0)"
    # the search condition of a normal search: the unscored selection and the scored query
    selection = {
        "const_score": {"score": 0.0, "query": {"boolean": {"must": [{"term": {"field": "project_pk", "value": 3}}, t]}}}
    }
    assert _sql({"boolean": {"must": [selection, m]}}) == (
        "((documents.project_pk @@@ pdb.term(%s::bigint)::pdb.const(0)"
        " AND documents.exact_fields->>'f3' === %s::text::pdb.const(0))"
        " AND documents.text_fields->>'f2' ||| %s::text)",
        [3, "x", "a b"],
    )


def test_render_ranges():
    def r(lower=None, upper=None, **extra):
        return {"range": {"field": "exact_fields.f5", "lower_bound": lower, "upper_bound": upper, **extra}}

    key = "documents.exact_fields->>'f5' @@@ "
    # whole numbers and dates: a postgres range
    assert _sql(r({"included": 3}, {"excluded": 9})) == (key + "pdb.range(int8range(%s::bigint, %s::bigint, '[)'))", [3, 9])
    assert _sql(r(None, {"included": 9})) == (key + "pdb.range(int8range(%s::bigint, %s::bigint, '(]'))", [None, 9])
    assert _sql(r({"excluded": "2020-01-01T00:00:00.000000Z"}, None, is_datetime=True)) == (
        key + "pdb.range(tstzrange(%s::timestamptz, %s::timestamptz, '()'))",
        ["2020-01-01T00:00:00.000000Z", None],
    )
    # the lower bound above the upper bound: nothing (postgres refuses such a range)
    assert _sql(r({"included": 9}, {"included": 3})) == (key + "pdb.empty()", [])
    dates = r({"included": "2020-01-02T00:00:00.000000Z"}, {"included": "2020-01-01T00:00:00.000000Z"}, is_datetime=True)
    assert _sql(dates) == (key + "pdb.empty()", [])
    # decimal and text bounds: the json of the range as a pdb.query constant (see pdb_sql._Renderer.leaf_range)
    assert _sql(r({"included": 0.5}, {"excluded": 17.0})) == (
        key + "%s::pdb.query",
        ['{"range": {"lower_bound": {"included": 0.5}, "upper_bound": {"excluded": 17.0}}}'],
    )
    assert _sql(r({"included": "a'b"}, None)) == (
        key + "%s::pdb.query",
        ['{"range": {"lower_bound": {"included": "a\'b"}, "upper_bound": null}}'],
    )
    assert _sql({"const_score": {"score": 0.0, "query": r({"included": 0.5}, None)}})[0] == key + "%s::pdb.query::pdb.const(0)"
    # too large for a bigint: as a decimal
    assert _sql(r({"included": 2**70}, None))[0] == key + "%s::pdb.query"


@pytest.mark.parametrize(
    "query",
    [
        {"regex": {"field": "text_fields.f2", "pattern": "a.*"}},
        {"match": {"field": "text_fields.f2", "value": "a", "distance": 1}},
        {"match": {"field": "text_fields; DROP TABLE x", "value": "a"}},
        {"match": {"field": "other_json.f2", "value": "a"}},
        {"match": {"value": "a"}},
        {"term": {"field": "exact_fields.f2", "value": None}},
        {"term": {"field": "exact_fields.f2", "value": 2**70}},
        {"range": {"field": "exact_fields.f2", "lower_bound": None, "upper_bound": None}},
        {"range": {"field": "exact_fields.f2", "lower_bound": {"included": 1}, "upper_bound": {"included": "a"}}},
        {"range": {"field": "exact_fields.f2", "lower_bound": {"from": 1}, "upper_bound": None}},
        {"boolean": {"must_not": [{"all": None}]}},
        {"boolean": {"should": [{"all": None}], "minimum_should_match": 1}},
        {"const_score": {"score": 2.0, "query": {"boolean": {"must": [{"all": None}]}}}},
        {"boost": {"factor": -1, "query": {"match": {"field": "text_fields.f2", "value": "a"}}}},
        {"parse": {"query_string": "a"}},
        "all",
    ],
)
def test_not_renderable(query, monkeypatch):
    """A json query without a tested form in the new API is sent in the legacy format (and logged)"""
    with pytest.raises(NotRenderable):
        render(query)
    warnings = []
    monkeypatch.setattr(pdb_sql.logging, "warning", warnings.append)
    with query_api(True):
        condition, params = search_condition(query)
    assert condition.as_string(None) == '"documents".id @@@ %s::jsonb' and params[0].obj == query
    assert len(warnings) == 1


def test_switch_off_is_the_legacy_form(monkeypatch):
    query = {"match": {"field": "text_fields.f2", "value": "a"}}
    with query_api(False):
        condition, params = search_condition(query)
        assert condition.as_string(None) == '"documents".id @@@ %s::jsonb' and params[0].obj == query
        assert pdb_sql.score_sql().as_string(None) == 'paradedb.score("documents".id)'
        assert pdb_sql.snippet_positions_sql("text_fields", "f2").as_string(None).startswith("paradedb.snippet_positions(")
    with query_api(True):
        assert search_condition(query)[0].as_string(None) == '"documents"."text_fields"->>\'f2\' ||| %s::text'
        assert pdb_sql.score_sql().as_string(None) == 'pdb.score("documents".id)'
        assert pdb_sql.snippet_positions_sql("text_fields", "f2").as_string(None).startswith("pdb.snippet_positions(")


# ---------------------------------------------------------------- plans


async def _plan(statement: sql.Composable, params: list) -> str:
    async with connection() as conn:
        await conn.execute("SET paradedb.planner_warnings = off")
        try:
            cur = await conn.execute(sql.SQL("EXPLAIN (COSTS OFF) {}").format(statement), params)
            return "\n".join(row["QUERY PLAN"] for row in await cur.fetchall())  # type: ignore[index, call-overload]
        finally:
            await conn.execute("RESET paradedb.planner_warnings")


def _shape(plan: str) -> list[str]:
    """The nodes of a plan and how pg_search runs its scan, without the query itself"""
    keep = ("Scan", "Exec Method", "TopK", "Sort", "Limit", "Aggregate", "Gather", "Append", "Filter")
    return [line.strip() for line in plan.splitlines() if any(k in line for k in keep) and "Tantivy Query" not in line]


async def test_plans(projects):
    """
    The fast scans of pg_search (Top-K scan for ORDER BY ... LIMIT, Aggregate Scan for counts and pdb.agg) are used
    for the same statements in both formats
    """
    fieldset = await get_fieldset(A)
    filters = _specs(
        {"num": {"gt": 0.5}, "kw": {"values": ["alpha", "Beta"]}, "date": {"gte": "2012-01-01"}, "tags": {"exists": False}}
    )
    agg = sql.Literal(json.dumps({"terms": {"field": fieldset.by_name["kw"][0].path, "size": 10}}))
    shapes: dict[str, dict[bool, list[str]]] = {}
    for new in (False, True):
        with query_api(new):
            for label, query in (
                ("query", SearchQuery(queries={"q": 'w1 OR "w2 w3"~2 OR econ*^2'})),
                ("query+filters", SearchQuery(queries={"q": "w1 -w2"}, filters=filters)),
                ("filters", SearchQuery(filters=filters)),
            ):
                c = compile_search(fieldset, query, fast=True)
                by_score = compile_search(fieldset, query, fast=True, by_score=True)
                assert c.fast and c.json_only and by_score.fast
                statements = {
                    "relevance top 10": (
                        sql.SQL("SELECT documents.id, {s} FROM {t} WHERE {w} ORDER BY {s} DESC, documents.id LIMIT 10").format(
                            s=pdb_sql.score_sql(), t=by_score.table, w=by_score.where
                        ),
                        by_score.params,
                    ),
                    "slot column top 10": (
                        sql.SQL(
                            "SELECT documents.id FROM {} WHERE {} "
                            "ORDER BY documents.date DESC NULLS LAST, documents.id LIMIT 10"
                        ).format(c.table, c.where),
                        c.params,
                    ),
                    "id top 10": (
                        sql.SQL("SELECT documents.id FROM {} WHERE {} ORDER BY documents.id LIMIT 10").format(
                            c.table, c.where
                        ),
                        c.params,
                    ),
                    "count": (sql.SQL("SELECT count(*) FROM {} WHERE {}").format(c.table, c.where), c.params),
                    "terms aggregation": (
                        sql.SQL("SELECT pdb.agg({}::jsonb) FROM {} WHERE {}").format(agg, c.table, c.where),
                        c.params,
                    ),
                    "group by": (
                        sql.SQL(
                            "SELECT documents.exact_fields->>{k} AS a0, count(*) AS n FROM {t} WHERE {w} GROUP BY 1"
                        ).format(k=sql.Literal(fieldset.by_name["kw"][0].key), t=c.table, w=c.where),
                        c.params,
                    ),
                }
                for name, (statement, params) in statements.items():
                    shapes.setdefault(f"{label}: {name}", {})[new] = _shape(await _plan(statement, params))
    for name, shape in shapes.items():
        assert shape[True] == shape[False], f"{name}: another plan in the new format:\n{shape[True]}\n{shape[False]}"
        text = " ".join(shape[True])
        if "top 10" in name:
            assert "TopKScanExecState" in text, (name, shape[True])
        if name.endswith(("count", "terms aggregation")):
            assert "ParadeDB Aggregate Scan" in text, (name, shape[True])


# ---------------------------------------------------------------- why some predicates are written as they are


async def _n(condition: str, params: list | None = None) -> int:
    """The number of documents of project A for a condition on its partition table (as "documents")"""
    fieldset = await get_fieldset(A)
    c = compile_search(fieldset, SearchQuery(), fast=True)
    assert c.fast
    async with connection() as conn:
        cur = await conn.execute(
            sql.SQL("SELECT count(*) AS n FROM {} WHERE {} AND " + condition).format(c.table, c.where),  # type: ignore[arg-type]
            [*c.params, *(params or [])],
        )
        return (await cur.fetchone())["n"]  # type: ignore[index, call-overload]


async def test_pg_search_numrange_on_json_numbers(projects):
    """
    pdb.range(numrange) finds nothing on a json number (it sends its bounds as text) and int8range rounds to whole
    numbers: the reason why decimal ranges are a pdb.query constant. If this test fails after a pg_search upgrade,
    pdb.range(numrange(...)) may work, and leaf_range can use it.
    """
    with query_api(True):
        key = (await get_fieldset(A)).by_name["num"][0].key
        want = await _count(A, None, {"num": {"gte": 0.5, "lte": 17.5}})
        assert want > 0
        assert await _n(f"documents.exact_fields->>'{key}' @@@ pdb.range(numrange(0.5, 17.5, '[]'))") == 0
        # int8range(1, 17, '[]') is [1, 18): it also finds 17.5
        whole = await _n(f"documents.exact_fields->>'{key}' @@@ pdb.range(int8range(1, 17, '[]'))")
        assert whole > await _count(A, None, {"num": {"gte": 1, "lte": 17}}) > 0


async def test_pg_search_fuzzy_transposition(projects):
    """
    The legacy fuzzy_term counts a swap of two letters as one edit; ::pdb.fuzzy(1) counts it as two. The renderer
    asks for the old behaviour: ::pdb.fuzzy(1, f, t)
    """
    key = (await get_fieldset(A)).by_name["text"][0].key
    with query_api(False):
        legacy = await _count(A, {"q": "text:ecnoomy~1"}, None)
    assert legacy > 0
    assert await _n(f"documents.text_fields->>'{key}' === 'ecnoomy'::pdb.fuzzy(1, f, t)") == legacy
    assert await _n(f"documents.text_fields->>'{key}' === 'ecnoomy'::pdb.fuzzy(1)") == 0


async def test_text_is_never_read_as_a_query(projects):
    """A search text or filter value that looks like a json query is searched as text"""
    with query_api(True):
        everything = await _count(A, None, None)
        for value in ('{"all":null}', '"all"', '{"term":{"value":"nosuchword"}}', "all"):
            assert await _count(A, None, {"title": {"values": [value]}}) == 0
            assert await _count(A, None, {"kw": {"values": [value]}}) == 0
            assert await _count(A, None, {"kw": {"gte": value, "lte": value}}) == 0
        assert await _count(A, {"q": r"\{\"all\"\:null\}^2"}, None) == 0
        assert 0 < await _count(A, {"q": r"\{w1\}^2"}, None) < everything
