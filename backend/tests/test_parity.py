"""
Same-results test (version 3): many random searches through the normal query code, each compared with a reference.

- Filters and sorts on fields: the reference is computed in Python from the uploaded documents, so it does not depend
  on pg_search at all (no Top-K scan, no index). Every case runs with the cutoff sort (AMCAT4_CUTOFF_SORT) on, on
  with small limits (so that the branches for large results are used on this small test set) and off. Tag sorts are
  only compared with the switch on: then the order is that of Elasticsearch (smallest tag ascending, largest tag
  descending); with the switch off it is the order of postgres-fast-v2 (the json text of the tag list).
- Relevance order: the reference is one SQL statement that first materializes every hit with its score and then
  sorts them in postgres, so pg_search cannot use its Top-K scan.

This test is the gate for every change to the search paths and for every pg_search upgrade.
The number of random cases is AMCAT4_PARITY_CASES (default 120 in the normal suite; the short test plan uses 2000).
Also here: the tests for the two version 3 correctness fixes (number filter on whole numbers, relevance order on the
fast path).
"""

import os
import random
from datetime import datetime
from typing import Any

import pytest
from psycopg import sql

from amcat4.config import get_settings
from amcat4.models import FieldSpec, FilterSpec, ProjectSettings, SortSpec
from amcat4.postgres import cutoff_sort
from amcat4.postgres.connection import connection
from amcat4.postgres.pdb_sql import score_sql
from amcat4.postgres.search import SearchQuery, compile_search
from amcat4.projects.index import create_project_index, delete_project_index
from amcat4.projects.query import query_documents
from amcat4.systemdata.fields import get_fieldset
from tests.conftest import upload

pytestmark = pytest.mark.anyio


@pytest.fixture(scope="module", autouse=True)
def json_sort_fields():
    """
    No automatic sort slots for the fields of this module (AMCAT4_AUTO_SORT_SLOT=false): the tests are about sorting on
    json fields, and a field that gets a slot is sorted on its slot column instead.
    """
    settings = get_settings()
    before = settings.auto_sort_slot
    settings.auto_sort_slot = False
    yield
    settings.auto_sort_slot = before


N_DOCS = 1500
N_CASES = int(os.environ.get("AMCAT4_PARITY_CASES", "120"))
PROJECT = "amcat4_unittest_parity"
FIELDS = {
    "text": "text",
    "kw": "keyword",
    "num": "number",
    "i": "integer",
    "date": "date",
    "tags": "tag",
    "date2": "date",
}
WORDS = [f"w{k}" for k in range(60)]
KEYWORDS = ["alpha", "Beta", "gamma", "delta", "épsilon", "zeta", "Zeta", "10", "9", "a b"]
NUMBERS = [17, 17.5, 18, 3.25, 40, -2, -2.5, 0, 1000000, 0.1]
TAGS = ["sport", "economy", "politics", "Art", "zoo", "x"]


def make_docs(n: int = N_DOCS, seed: int = 7) -> list[dict[str, Any]]:
    rnd = random.Random(seed)
    docs = []
    for k in range(n):
        # words with very different frequencies and texts of different lengths, so that scores differ
        length = rnd.choice([5, 10, 20, 60])
        words = [WORDS[min(int(rnd.expovariate(0.12)), len(WORDS) - 1)] for _ in range(length)]
        doc: dict[str, Any] = {"_id": f"d{k}", "text": " ".join(words)}
        if rnd.random() < 0.9:
            doc["kw"] = rnd.choice(KEYWORDS)
        if rnd.random() < 0.9:
            doc["num"] = rnd.choice(NUMBERS)
        if rnd.random() < 0.9:
            doc["i"] = rnd.randrange(-5, 40)
        if rnd.random() < 0.9:
            doc["date"] = f"20{rnd.randrange(10, 24)}-{rnd.randrange(1, 13):02d}-{rnd.randrange(1, 28):02d}"
        if rnd.random() < 0.8:
            doc["tags"] = rnd.sample(TAGS, rnd.randrange(1, 4))
        if rnd.random() < 0.85:
            doc["date2"] = f"{rnd.randrange(1995, 2024)}-{rnd.randrange(1, 13):02d}-01T{rnd.randrange(0, 24):02d}:00:00"
        docs.append(doc)
    return docs


@pytest.fixture(scope="module")
async def parity_project():
    await delete_project_index(PROJECT, ignore_missing=True)
    await create_project_index(ProjectSettings(id=PROJECT))
    docs = make_docs()
    for start in range(0, len(docs), 500):
        await upload(PROJECT, docs[start : start + 500], fields=FIELDS)  # type: ignore[arg-type]
    yield docs
    await delete_project_index(PROJECT, ignore_missing=True)


# ---------------------------------------------------------------- reference (python)


def _date(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _matches(doc: dict, name: str, spec: dict) -> bool:
    value = doc.get(name)
    if "exists" in spec:
        return (value is not None) == spec["exists"]
    if value is None:
        return False
    values = value if isinstance(value, list) else [value]
    if "values" in spec:
        wanted = spec["values"]
        if name == "date":
            return any(_date(v) == _date(str(w)) for v in values for w in wanted)
        if name in ("num", "i"):
            return any(float(v) == float(w) for v in values for w in wanted)
        return any(v == w for v in values for w in wanted)
    conv = _date if name == "date" else (float if name in ("num", "i") else str)
    for v in values:
        x = conv(v)  # type: ignore[operator]
        ok = True
        for op, bound in spec.items():
            b = conv(bound)  # type: ignore[operator]
            ok = ok and {"gt": x > b, "gte": x >= b, "lt": x < b, "lte": x <= b}[op]
        if ok:
            return True
    return False


def _sort_key(name: str, value: Any, order: str = "asc") -> Any:
    if name == "tags":
        # the order of Elasticsearch: the smallest tag of the document for ascending, the largest for descending
        tags = [str(t).encode("utf-8") for t in value]
        return max(tags) if order == "desc" else min(tags)
    if name in ("date", "date2"):
        return _date(value)
    if name in ("num", "i"):
        return float(value)
    return str(value).encode("utf-8")  # byte order, as the text sort columns and Elasticsearch


def reference(docs: list[dict], filters: dict[str, dict], sort: list[tuple[str, str]]) -> list[str]:
    """The ids of all hits in the expected order (values first, missing values last, ties in upload order)"""
    hits = [(k, d) for k, d in enumerate(docs) if all(_matches(d, name, spec) for name, spec in filters.items())]
    for name, order in reversed(sort):  # stable sorts, last key first
        # (a document without tags, or with an empty list of tags, has no value)
        present = [(k, d) for k, d in hits if d.get(name) not in (None, [])]
        missing = [(k, d) for k, d in hits if d.get(name) in (None, [])]
        # python's sort is stable, also with reverse=True: equal values keep their earlier order
        present.sort(key=lambda kd: _sort_key(name, kd[1][name], order), reverse=(order == "desc"))
        hits = present + missing
    return [d["_id"] for _, d in hits]


# ---------------------------------------------------------------- random cases


def random_filters(rnd: random.Random) -> dict[str, dict]:
    filters: dict[str, dict] = {}
    for _ in range(rnd.choice([0, 1, 1, 2])):
        name = rnd.choice(["kw", "num", "i", "date", "tags"])
        kind = rnd.choice(["values", "range", "exists"])
        if kind == "exists":
            filters[name] = {"exists": rnd.random() < 0.7}
        elif kind == "values" or name in ("kw", "tags"):
            pool: list[Any] = {
                "kw": KEYWORDS,
                "tags": TAGS,
                "num": [17, "17.5", 18, "3.25", 40, -2, "-2.5", 0, 1000000, "0.1"],
                "i": list(range(-5, 40)),
                "date": ["2015-03-04", "2020-01-01", "2012-12-12"],
            }[name]
            filters[name] = {"values": rnd.sample(pool, rnd.randrange(1, 3))}
        else:
            lo, hi = {"num": (0, 18), "i": (3, 25), "date": ("2013-01-01", "2019-06-15")}[name]
            spec: dict[str, Any] = {}
            if rnd.random() < 0.8:
                spec[rnd.choice(["gt", "gte"])] = lo
            if rnd.random() < 0.8 or not spec:
                spec[rnd.choice(["lt", "lte"])] = hi
            filters[name] = spec
    return filters


def random_sort(rnd: random.Random) -> list[tuple[str, str]]:
    # date is the first date field, so it is in a sort slot (a Top-K scan); date2 is sorted on its json value.
    # tags: only compared with the cutoff sort switched on (see the top of this file)
    names = rnd.sample(["kw", "num", "i", "date", "date2", "tags"], rnd.choice([1, 1, 2, 3]))
    return [(name, rnd.choice(["asc", "desc"])) for name in names]


async def _page(filters: dict[str, dict], sort: list[tuple[str, str]], page: int, per_page: int, queries=None):
    return await query_documents(
        PROJECT,
        fields=[FieldSpec(name="kw")],
        queries=queries,
        filters={name: FilterSpec(**spec) for name, spec in filters.items()} or None,
        sort=[{name: SortSpec(order=order)} for name, order in sort] or None,  # type: ignore[arg-type]
        page=page,
        per_page=per_page,
    )


# The limits of the cutoff sort (module constants), as they are and small: with the small ones this test set uses the
# group for many ties at the cutoff, the second histogram for numbers and the fallback for deep pages
CUTOFF_LIMITS = {
    "on": {"SMALL_HITS": 0},
    "on, small limits": {
        "SMALL_HITS": 0,
        "SPLIT_ROWS": 4,
        "ZOOM_ROWS": 4,
        "HIST_BUCKETS": 6,
        "MAX_BUCKETS": 400,
        "MAX_TERMS": 3,
    },
    # results of up to 100 hits are sorted as a whole (as the default limit does for all results of this test set)
    "on, small results": {"SMALL_HITS": 100},
}


async def test_parity_filters_and_sorts(parity_project, monkeypatch):
    """
    Random filters, sorts and pages give exactly the documents of the python reference, in the same order: with the
    cutoff sort on (twice: normal and small limits) and off
    """
    docs = parity_project
    rnd = random.Random(2026)
    mismatches = []
    late_fetches: dict[str, list[bool]] = {mode: [] for mode in CUTOFF_LIMITS}
    mode = "on"
    original = cutoff_sort.page_ids

    async def recording(*args, **kwargs):
        ids = await original(*args, **kwargs)
        late_fetches[mode].append(ids is not None)
        return ids

    monkeypatch.setattr(cutoff_sort, "page_ids", recording)
    for case in range(N_CASES):
        filters, sort = random_filters(rnd), random_sort(rnd)
        per_page = rnd.choice([1, 5, 10, 50])
        expected = reference(docs, filters, sort)
        last_page = max(0, (len(expected) - 1) // per_page)
        page = rnd.choice([0, 0, 1, last_page, rnd.randrange(0, last_page + 1)])
        want = expected[page * per_page : (page + 1) * per_page]
        modes = [*CUTOFF_LIMITS, "off"]
        if any(name == "tags" for name, _ in sort):
            modes.remove("off")  # another order of tags, by design
        for mode in modes:
            with monkeypatch.context() as m:
                m.setattr(get_settings(), "cutoff_sort", mode != "off")
                for constant, value in CUTOFF_LIMITS.get(mode, {}).items():
                    m.setattr(cutoff_sort, constant, value)
                result = await _page(filters, sort, page, per_page)
            got = [d["_id"] for d in result.data]
            if got != want or result.total_count != len(expected):
                mismatches.append(
                    (case, mode, filters, sort, page, per_page, got[:5], want[:5], result.total_count, len(expected))
                )
    assert not mismatches, f"{len(mismatches)} of {N_CASES} cases differ, first: {mismatches[0]}"
    # the cutoff sort really ran: in most sorts on a json field (with the normal limits in nearly all of them)
    used = {mode: sum(calls) for mode, calls in late_fetches.items()}
    assert used["on"] >= 0.9 * len(late_fetches["on"]) > 0.3 * N_CASES, (used, N_CASES)
    assert used["on, small limits"] >= 0.5 * len(late_fetches["on, small limits"]), used
    assert 0 < len(late_fetches["on, small results"]) < len(late_fetches["on"]), late_fetches
    print(f"parity: {N_CASES} cases, late fetches { ({m: len(c) for m, c in late_fetches.items()}) }, by cutoff sort {used}")


async def _reference_by_score(queries: dict[str, str], filters: dict[str, dict], limit: int) -> list[str]:
    """Top documents by score, sorted by postgres over all hits (materialized, so no Top-K scan in pg_search)"""
    fieldset = await get_fieldset([PROJECT])
    c = compile_search(
        fieldset, SearchQuery(queries=queries, filters={k: FilterSpec(**v) for k, v in filters.items()} or None)
    )
    # (score_sql: paradedb.score, or pdb.score with AMCAT4_NEW_QUERY_API=true)
    stmt = sql.SQL(
        "WITH hits AS MATERIALIZED (SELECT documents.id, documents.doc_id, {} AS s "
        "FROM {} WHERE {}) SELECT doc_id FROM hits ORDER BY s DESC, id LIMIT {}"
    ).format(score_sql(), c.table, c.where, sql.Literal(limit))
    async with connection() as conn:
        cur = await conn.execute(stmt, c.params)
        return [row["doc_id"] for row in await cur.fetchall()]  # type: ignore[index, call-overload]


async def test_parity_relevance(parity_project):
    """The first pages in relevance order are the documents with the highest scores"""
    rnd = random.Random(11)
    mismatches = []
    cases = max(20, N_CASES // 4)
    for case in range(cases):
        words = rnd.sample(WORDS[:40], rnd.choice([1, 1, 2, 3]))
        queries = {"q": " ".join(words)}
        filters = random_filters(rnd) if rnd.random() < 0.4 else {}
        per_page = rnd.choice([5, 10, 20])
        page = rnd.choice([0, 0, 1, 3])
        want = (await _reference_by_score(queries, filters, (page + 1) * per_page))[page * per_page :]
        for explicit in (False, True):
            sort = [("_score", "desc")] if explicit else []
            result = await _page(filters, sort, page, per_page, queries=queries)
            got = [d["_id"] for d in result.data]
            if got != want:
                mismatches.append((case, queries, filters, explicit, page, per_page, got[:5], want[:5]))
    assert not mismatches, f"{len(mismatches)} relevance results differ, first: {mismatches[0]}"


# ---------------------------------------------------------------- the two fixes of version 3


@pytest.mark.parametrize(
    "value, expected",
    [
        (17, 17.0),
        ("17", 17.0),
        ("17.0", 17.0),
        ("17.5", 17.5),
        (40, 40.0),
        (-2, -2.0),
        ("-2.5", -2.5),
        (0, 0.0),
        (1000000, 1e6),
    ],
)
async def test_number_filter_whole_and_decimal(parity_project, value, expected):
    """A values filter on a number field finds whole numbers too (it found nothing for them before version 3)"""
    docs = parity_project
    want = sum(1 for d in docs if d.get("num") is not None and float(d["num"]) == expected)
    assert want > 0
    result = await _page({"num": {"values": [value]}}, [], 0, 1)
    assert result.total_count == want


async def test_number_query_string_whole_and_decimal(parity_project):
    docs = parity_project
    for text, expected in [("num:17", 17.0), ("num:17.5", 17.5), ("num:40", 40.0)]:
        want = sum(1 for d in docs if d.get("num") is not None and float(d["num"]) == expected)
        result = await _page({}, [("i", "asc")], 0, 1, queries={"q": text})
        assert result.total_count == want, text


async def test_score_order_keeps_partition_condition(parity_project, monkeypatch):
    """On the fast path a statement ordered by relevance has a plain condition next to the search condition"""
    fieldset = await get_fieldset([PROJECT])
    query = SearchQuery(queries={"q": "w1"})
    plain = compile_search(fieldset, query, fast=True)
    scored = compile_search(fieldset, query, fast=True, by_score=True)
    assert plain.fast and scored.fast
    assert "partition_id" not in plain.where.as_string(None)
    assert "partition_id" in scored.where.as_string(None)
    monkeypatch.setattr(get_settings(), "score_partition_condition", False)
    assert "partition_id" not in compile_search(fieldset, query, fast=True, by_score=True).where.as_string(None)
