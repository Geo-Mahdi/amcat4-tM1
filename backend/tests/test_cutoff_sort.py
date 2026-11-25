"""
Tests for the cutoff sort (amcat4.postgres.cutoff_sort, switch AMCAT4_CUTOFF_SORT): sorting on a json field by first
finding the value of the last document of the page.

Every result is compared with a reference that is computed in Python from the uploaded documents (so it does not
depend on pg_search), and the fixture `cutoff_calls` records whether the cutoff sort gave the ids of the page or the
late fetch did (fallback). The random comparison over many cases is in test_parity.py.
"""

import random
from datetime import datetime
from typing import Any

import pytest
from psycopg import AsyncConnection
from psycopg.pq import TransactionStatus

from amcat4.config import get_settings
from amcat4.models import FieldSpec, FilterSpec, ProjectSettings, SortSpec
from amcat4.postgres import cutoff_sort
from amcat4.projects.documents import delete_document
from amcat4.projects.index import create_project_index, delete_project_index
from amcat4.projects.query import query_documents
from tests.conftest import upload

pytestmark = pytest.mark.anyio


@pytest.fixture(scope="module", autouse=True)
def json_sort_fields():
    """
    No automatic sort slots for the fields of this module (AMCAT4_AUTO_SORT_SLOT=false): the tests are about sorting on
    json fields, and a field that gets a slot is sorted on its slot column instead.
    """
    settings = get_settings()
    before = settings.auto_sort_slot, settings.label_tables
    settings.auto_sort_slot = False
    # ... and tags in json (AMCAT4_LABEL_TABLES=false): a tag field in the label table is not in the search index of
    # documents, so the cutoff sort leaves it to the late fetch (tested in test_v3_together.py)
    settings.label_tables = False
    yield
    settings.auto_sort_slot, settings.label_tables = before


PROJECT = "amcat4_unittest_cutoff"
N_DOCS = 600
# "date" is the first date field, so it gets the date sort slot: sorting on it is a Top-K scan, not a cutoff sort
FIELDS = {
    "text": "text",
    "date": "date",
    "kw": "keyword",
    "link": "url",
    "tags": "tag",
    "i": "integer",
    "num": "number",
    "day": "date",
}
KEYWORDS = ["", "10", "9", "Beta", "alpha", "a b", "zeta", "Zeta", "épsilon", "李明", "x ", "x"]
TAGS = ["Art", "economy", "politics", "sport", "x", "zoo", "épée", "10"]
NUMBERS = [17, 17.5, 18, 3.25, 40, -2, -2.5, 0, 1000000, 0.1, -1000.75]
DAYS = ["1987-03-04", "1999-12-31T23:59:59.999999", "2000-01-01", "2015-06-07T08:09:10.000001", "2015-06-07T08:09:10"]


def make_docs(n: int = N_DOCS, seed: int = 3) -> list[dict[str, Any]]:
    rnd = random.Random(seed)
    docs = []
    for k in range(n):
        doc: dict[str, Any] = {"_id": f"d{k:04d}", "text": " ".join(rnd.choice(["red", "green", "blue"]) for _ in range(4))}
        doc["date"] = f"20{rnd.randrange(10, 24)}-01-{rnd.randrange(1, 28):02d}"
        if rnd.random() < 0.85:
            # one very common value (many ties), the others spread
            doc["kw"] = "common" if rnd.random() < 0.4 else rnd.choice(KEYWORDS)
        if rnd.random() < 0.9:
            doc["link"] = f"https://example.com/{rnd.randrange(10**6):06d}/{k}"  # unique
        if rnd.random() < 0.8:
            doc["tags"] = rnd.sample(TAGS, rnd.randrange(0, 4))  # (also empty lists)
        if rnd.random() < 0.9:
            doc["i"] = rnd.randrange(-5, 40)
        if rnd.random() < 0.85:
            # whole and decimal numbers on one path, some values many times, and many different values
            doc["num"] = rnd.choice(NUMBERS) if rnd.random() < 0.5 else round(rnd.uniform(-50, 50), 3)
        if rnd.random() < 0.9:
            doc["day"] = (
                rnd.choice(DAYS) if rnd.random() < 0.5 else f"20{rnd.randrange(1, 24):02d}-05-{rnd.randrange(1, 28):02d}"
            )
        docs.append(doc)
    return docs


@pytest.fixture(scope="module")
async def project():
    await delete_project_index(PROJECT, ignore_missing=True)
    await create_project_index(ProjectSettings(id=PROJECT))
    docs = make_docs()
    await upload(PROJECT, docs, fields=FIELDS)  # type: ignore[arg-type]
    yield docs
    await delete_project_index(PROJECT, ignore_missing=True)


@pytest.fixture()
def cutoff_calls(monkeypatch):
    """Records for every late fetch whether the cutoff sort gave the ids of the page (True) or not (False: fallback)"""
    calls: list[bool] = []
    original = cutoff_sort.page_ids
    # (the test sets are small: without this, all their hits would simply be sorted, see test_small_results)
    monkeypatch.setattr(cutoff_sort, "SMALL_HITS", 0)

    async def recording(*args, **kwargs):
        ids = await original(*args, **kwargs)
        calls.append(ids is not None)
        return ids

    monkeypatch.setattr(cutoff_sort, "page_ids", recording)
    return calls


@pytest.fixture()
def statements(monkeypatch):
    """Records the text of every statement"""
    seen: list[str] = []
    original = AsyncConnection.execute

    async def execute(self, query, params=None, **kwargs):
        seen.append(query if isinstance(query, str) else query.as_string(None))
        return await original(self, query, params, **kwargs)

    monkeypatch.setattr(AsyncConnection, "execute", execute)
    return seen


@pytest.fixture()
def transactions(monkeypatch):
    """Records for every statement whether it ran inside a transaction: (text, in transaction)"""
    seen: list[tuple[str, bool]] = []
    original = AsyncConnection.execute

    async def execute(self, query, params=None, **kwargs):
        text = query if isinstance(query, str) else query.as_string(None)
        seen.append((text, self.info.transaction_status != TransactionStatus.IDLE))
        return await original(self, query, params, **kwargs)

    monkeypatch.setattr(AsyncConnection, "execute", execute)
    return seen


# ---------------------------------------------------------------- reference (python)


def _missing(name: str, value: Any) -> bool:
    return value is None or (name == "tags" and not value)


def _sort_key(name: str, value: Any, order: str) -> Any:
    if name in ("date", "day"):
        return datetime.fromisoformat(value)
    if name in ("num", "i"):
        return float(value)
    if name == "tags":
        # the order of Elasticsearch: the smallest tag for ascending, the largest for descending
        tags = [t.encode("utf-8") for t in value]
        return max(tags) if order == "desc" else min(tags)
    return str(value).encode("utf-8")  # byte order


def reference(docs: list[dict], sort: list[tuple[str, str]], keep=lambda d: True) -> list[str]:
    """The ids of all hits in the expected order: values first, missing values last, ties in upload order"""
    hits = [d for d in docs if keep(d)]
    for name, order in reversed(sort):  # stable sorts, last key first
        if name == "_seq":  # the order of upload
            hits.sort(key=lambda d: docs.index(d), reverse=(order == "desc"))
            continue
        value = (lambda d: d["_id"]) if name == "_id" else (lambda d: d.get(name))
        present = [d for d in hits if not _missing(name, value(d))]
        missing = [d for d in hits if _missing(name, value(d))]
        present.sort(key=lambda d: _sort_key(name, value(d), order), reverse=(order == "desc"))
        hits = present + missing
    return [d["_id"] for d in hits]


async def _page(sort, page=0, per_page=10, filters=None, queries=None, project=PROJECT):
    result = await query_documents(
        project,
        fields=[FieldSpec(name="kw")],
        queries=queries,
        filters={name: FilterSpec(**spec) for name, spec in (filters or {}).items()} or None,
        sort=[{name: SortSpec(order=order)} for name, order in sort] or None,  # type: ignore[arg-type]
        page=page,
        per_page=per_page,
    )
    return [d["_id"] for d in result.data]


async def _check_pages(docs, sort, per_page=10, pages=None, filters=None, keep=lambda d: True):
    """Pages of this sort are the pages of the reference. Default pages: first, second, a deep one, the last two"""
    expected = reference(docs, sort, keep)
    last = max(0, (len(expected) - 1) // per_page)
    for page in pages if pages is not None else sorted({0, 1, last // 2, max(0, last - 1), last}):
        got = await _page(sort, page, per_page, filters)
        assert got == expected[page * per_page : (page + 1) * per_page], (sort, page, per_page)
    return expected


# ---------------------------------------------------------------- every type, both directions


@pytest.mark.parametrize("name", ["kw", "link", "tags", "i", "num", "day"])
@pytest.mark.parametrize("order", ["asc", "desc"])
async def test_each_type(project, cutoff_calls, name, order):
    """Every supported type, ascending and descending, from the first page to the documents without a value"""
    expected = await _check_pages(project, [(name, order)])
    assert len(expected) == N_DOCS
    assert cutoff_calls and all(cutoff_calls), "the cutoff sort was not used"


@pytest.mark.parametrize("name", ["kw", "link", "tags", "i", "num", "day"])
async def test_small_steps(project, cutoff_calls, monkeypatch, name):
    """
    The same with small limits, so that the small test set reaches the branches for large results: a group for many
    ties at the cutoff, and the second (finer) histogram for numbers
    """
    monkeypatch.setattr(cutoff_sort, "SPLIT_ROWS", 3)
    monkeypatch.setattr(cutoff_sort, "ZOOM_ROWS", 3)
    monkeypatch.setattr(cutoff_sort, "HIST_BUCKETS", 7)
    monkeypatch.setattr(cutoff_sort, "MAX_TERMS", 2)
    for order in ("asc", "desc"):
        await _check_pages(project, [(name, order)], per_page=7)
    assert cutoff_calls and all(cutoff_calls)


async def test_missing_values_last(project, cutoff_calls):
    """Documents without a value come last, ascending and descending, in id order; a page can cross the border"""
    with_value = sum(1 for d in project if d.get("i") is not None)
    without = [d["_id"] for d in project if d.get("i") is None]
    assert len(without) > 20
    for order in ("asc", "desc"):
        # a page that starts in the documents with a value and ends in the documents without
        got = await query_documents(
            PROJECT, sort=[{"i": SortSpec(order=order)}], per_page=10, page=with_value // 10, fields=[FieldSpec(name="i")]
        )
        ids = [d["_id"] for d in got.data]
        assert ids == reference(project, [("i", order)])[with_value // 10 * 10 :][:10]
        assert ids[with_value % 10 :] == without[: 10 - with_value % 10]
        # a page that lies completely in the documents without a value
        page = with_value // 10 + 2
        assert await _page([("i", order)], page) == reference(project, [("i", order)])[page * 10 :][:10]
    assert len(cutoff_calls) == 4 and all(cutoff_calls)


async def test_ties_larger_than_page(project, cutoff_calls, monkeypatch):
    """A value that many documents have (more than several pages): ties are in id order, on every page"""
    common = [d["_id"] for d in project if d.get("kw") == "common"]
    assert len(common) > 150
    for split_rows in (1000, 20):  # all ties sorted, and the ties as a group of their own
        monkeypatch.setattr(cutoff_sort, "SPLIT_ROWS", split_rows)
        expected = reference(project, [("kw", "asc")])
        start = expected.index(common[0])
        for offset_page in (start // 10, start // 10 + 1, start // 10 + 7, (start + len(common)) // 10):
            assert await _page([("kw", "asc")], offset_page) == expected[offset_page * 10 :][:10]
        assert await _page([("kw", "asc")], start // 10 + 3) == common[30 - start % 10 :][:10]
    assert cutoff_calls and all(cutoff_calls)


async def test_deep_pages(project, cutoff_calls):
    """Pages far into the result, with large and small pages"""
    for sort in ([("link", "asc")], [("num", "desc")], [("tags", "asc")]):
        await _check_pages(project, sort, per_page=1, pages=[0, 137, 400, 599])
        await _check_pages(project, sort, per_page=100, pages=[3, 4, 5])
        await _check_pages(project, sort, per_page=700, pages=[0])
    assert all(cutoff_calls)


@pytest.mark.parametrize(
    "sort",
    [
        [("kw", "asc"), ("i", "desc")],
        [("kw", "desc"), ("day", "asc")],
        [("i", "asc"), ("kw", "asc")],
        [("day", "desc"), ("num", "asc")],
        [("num", "asc"), ("tags", "desc")],
        [("tags", "asc"), ("i", "asc")],
        [("tags", "desc"), ("tags", "asc")],
        [("kw", "asc"), ("date", "desc")],  # the second key is a sort slot column
        [("kw", "asc"), ("_seq", "desc")],
        [("i", "desc"), ("_id", "desc")],
        [("kw", "asc"), ("i", "desc"), ("num", "asc")],
    ],
)
async def test_multi_key(project, cutoff_calls, monkeypatch, sort):
    """The cutoff is on the first key; the other keys order ties and the documents without the first key"""
    for split_rows in (1000, 4):
        monkeypatch.setattr(cutoff_sort, "SPLIT_ROWS", split_rows)
        await _check_pages(project, sort, per_page=13)
    assert cutoff_calls and all(cutoff_calls)


async def test_with_filters_and_query(project, cutoff_calls):
    """The cutoff is found among the hits of the search (filters and query string), not among all documents"""
    await _check_pages(project, [("kw", "asc")], filters={"i": {"gte": 10}}, keep=lambda d: d.get("i", -99) >= 10)
    await _check_pages(
        project, [("num", "desc")], filters={"tags": {"values": ["x"]}}, keep=lambda d: "x" in d.get("tags", [])
    )
    expected = reference(project, [("i", "asc")], lambda d: "red" in d["text"])
    assert await _page([("i", "asc")], 2, 10, queries={"q": "red"}) == expected[20:30]
    assert len(cutoff_calls) == 11 and all(cutoff_calls)
    # A second key on the score: such a statement keeps a SQL condition (score_partition_condition), so the cutoff sort
    # is not tried. Same order with the switch on and off
    with_switch = [await _page([("i", "asc"), ("_score", "desc")], p, 10, queries={"q": "red blue"}) for p in (0, 3)]
    assert len(cutoff_calls) == 11
    get_settings().cutoff_sort = False
    try:
        without = [await _page([("i", "asc"), ("_score", "desc")], p, 10, queries={"q": "red blue"}) for p in (0, 3)]
    finally:
        get_settings().cutoff_sort = True
    assert with_switch == without and len(with_switch[1]) == 10


async def test_few_hits(project, cutoff_calls):
    """Fewer hits than the page asks for (also none at all): no range clause without a bound, same result"""
    for name in ("kw", "num", "tags", "day"):
        keep = lambda d: d.get("i") == 7  # noqa: E731
        expected = reference(project, [(name, "asc")], keep)
        assert 0 < len(expected) < 50
        assert await _page([(name, "asc")], 0, 50, {"i": {"values": [7]}}) == expected
        assert await _page([(name, "asc")], 3, 50, {"i": {"values": [7]}}) == []
        assert await _page([(name, "desc")], 0, 50, {"i": {"values": [12345]}}) == []
    assert all(cutoff_calls)


# ---------------------------------------------------------------- small results


@pytest.mark.parametrize(
    "sort",
    [
        [("kw", "asc")],
        [("link", "desc")],
        [("tags", "asc")],
        [("tags", "desc")],
        [("i", "desc")],
        [("num", "asc")],
        [("day", "desc")],
        [("kw", "desc"), ("num", "asc"), ("tags", "asc")],
        [("i", "asc"), ("date", "desc")],
        [("day", "asc"), ("_id", "desc")],
    ],
)
async def test_small_results_are_sorted_as_a_whole(project, statements, sort):
    """Up to SMALL_HITS hits (the default covers this test set): one statement sorts all hits, no aggregation"""
    assert cutoff_sort.SMALL_HITS >= N_DOCS
    await _check_pages(project, sort, per_page=9)
    assert await _page(sort, 100, 9) == []  # a page after the last one
    finding = [s for s in statements if "@@@" in s and "count(*) AS n FROM" not in s and "ANY(%s)" not in s]
    assert finding and all(s.startswith("WITH hits AS MATERIALIZED (SELECT documents.id, ") for s in finding)
    assert 'FROM hits ORDER BY "o0" ' in finding[0]
    assert not any("pdb.agg" in s for s in statements)


async def test_more_hits_than_small_use_the_cutoff(project, statements, monkeypatch):
    """One hit more than SMALL_HITS: the first statement stops there, and the cutoff sort finds the page"""
    hits = sum(1 for d in project if d.get("i", -99) >= 10)
    for small, by_cutoff in ((hits, False), (hits - 1, True)):
        monkeypatch.setattr(cutoff_sort, "SMALL_HITS", small)
        del statements[:]
        await _check_pages(
            project, [("kw", "asc")], filters={"i": {"gte": 10}}, keep=lambda d: d.get("i", -99) >= 10, pages=[2]
        )
        assert sum(s.startswith("WITH hits AS MATERIALIZED") for s in statements) == 1
        assert any("pdb.agg" in s for s in statements) == by_cutoff


async def test_only_late_fetch_sorts_get_a_transaction(project, transactions):
    """
    Searches that never had a late fetch (sort slot columns, relevance, id order, no sort) are untouched: one
    statement, no transaction, no extra statement. Sorts on a json field run their statements in one transaction
    """
    for sort, queries in (
        ([("date", "desc")], None),
        ([("date", "asc"), ("_seq", "desc")], None),
        ([], None),
        ([], {"q": "red"}),
        ([("_score", "desc")], {"q": "red"}),
        ([("date", "desc")], {"q": "red"}),
    ):
        del transactions[:]
        assert len(await _page(sort, 1, 10, queries=queries)) == 10
        searches = [(s, t) for s, t in transactions if "@@@" in s or "FROM documents" in s or 'FROM "documents' in s]
        pages = [s for s, _ in searches if "count(*)" not in s]
        assert len(pages) == 1 and "ORDER BY" in pages[0] and "hits" not in pages[0], (sort, pages)
        assert not any(t for _, t in transactions), sort
        assert not any("pdb.agg" in s for s, _ in transactions)
    for sort in ([("kw", "asc")], [("_id", "asc")]):
        del transactions[:]
        assert len(await _page(sort, 1, 10)) == 10
        pages = [t for s, t in transactions if "@@@" in s and "count(*) AS n FROM" not in s]
        assert len(pages) == 2 and all(pages), sort


async def test_database_error_while_finding_ids(project, monkeypatch):
    """A statement of the cutoff sort that fails (not a timeout): the page comes from the late fetch"""
    failed = []

    async def failing(conn, *args, **kwargs):
        failed.append(True)
        await conn.execute("SELECT 1 / 0")

    for name in ("small_page_ids", "page_ids"):
        with monkeypatch.context() as m:
            m.setattr(cutoff_sort, "SMALL_HITS", N_DOCS if name == "small_page_ids" else 0)
            m.setattr(cutoff_sort, name, failing)
            await _check_pages(project, [("kw", "asc")], pages=[3])
    assert failed == [True, True]


async def test_tag_groups(project, cutoff_calls, statements, monkeypatch):
    """
    Tags with many documents per tag: the documents whose smallest (largest) tag is the cutoff tag are a group that
    is not sorted on the tags; a page can lie in several groups, and before, in and after a group
    """
    monkeypatch.setattr(cutoff_sort, "SPLIT_ROWS", 10)
    for sort in ([("tags", "asc")], [("tags", "desc")], [("tags", "asc"), ("i", "desc")], [("tags", "desc"), ("kw", "asc")]):
        for per_page in (10, 150, 400):
            await _check_pages(project, sort, per_page=per_page)
    assert all(cutoff_calls)
    # the first page of a sort on tags alone: an aggregation, then the first documents with the first tag by id
    del statements[:]
    await _page([("tags", "asc")], 0, 10)
    searches = [s for s in statements if "@@@" in s and "count(*) AS n FROM" not in s]
    assert len(searches) == 3 and "pdb.agg" in searches[0]
    # (the search condition itself is the legacy json object or, with AMCAT4_NEW_QUERY_API, new-API predicates)
    assert searches[1].startswith("SELECT documents.id FROM ")
    assert searches[1].endswith(" ORDER BY documents.id LIMIT %s OFFSET %s") and searches[1].count("ORDER BY") == 1


# ---------------------------------------------------------------- tag order


@pytest.fixture()
async def tag_project():
    name = "amcat4_unittest_cutoff_tags"
    await delete_project_index(name, ignore_missing=True)
    await create_project_index(ProjectSettings(id=name))
    docs = [
        {"_id": "bz", "text": "t", "tags": ["z", "b"]},
        {"_id": "c", "text": "t", "tags": ["c"]},
        {"_id": "none", "text": "t"},
        {"_id": "ay", "text": "t", "tags": ["a", "y"]},
        {"_id": "empty", "text": "t", "tags": []},
        {"_id": "Zb", "text": "t", "tags": ["b", "Z"]},
    ]
    await upload(name, docs, fields={"text": "text", "tags": "tag"})
    yield name
    await delete_project_index(name, ignore_missing=True)


async def test_tag_order_is_elasticsearch_order(tag_project, cutoff_calls, monkeypatch):
    """Ascending: by the smallest tag of the document; descending: by the largest; byte order; no tags = last"""
    asc = ["Zb", "ay", "bz", "c", "none", "empty"]  # smallest tags: Z, a, b, c
    desc = ["bz", "ay", "c", "Zb", "none", "empty"]  # largest tags: z, y, c, b
    assert await _page([("tags", "asc")], project=tag_project) == asc
    assert await _page([("tags", "desc")], project=tag_project) == desc
    assert await _page([("tags", "desc")], 1, 2, project=tag_project) == desc[2:4]
    assert cutoff_calls == [True, True, True]
    # the same order when the cutoff sort is not used: the late fetch, and the original single statement
    monkeypatch.setattr(cutoff_sort, "MAX_BUCKETS", 0)
    assert await _page([("tags", "asc")], project=tag_project) == asc
    assert await _page([("tags", "desc")], project=tag_project) == desc
    assert cutoff_calls[3:] == [False, False]
    monkeypatch.setattr(get_settings(), "fast_path", False)
    assert await _page([("tags", "asc")], project=tag_project) == asc
    assert await _page([("tags", "desc")], project=tag_project) == desc
    assert len(cutoff_calls) == 5


async def test_switch_off_is_old_statements(tag_project, cutoff_calls, statements, monkeypatch):
    """AMCAT4_CUTOFF_SORT=false: the two statements of the late fetch as they were, and the old tag order"""
    monkeypatch.setattr(get_settings(), "cutoff_sort", False)
    monkeypatch.setattr(get_settings(), "parallel_count", False)
    del statements[:]
    # the old order: the json text of the tag list, so in the order the tags were stored
    assert await _page([("tags", "asc")], project=tag_project) == ["ay", "Zb", "c", "bz", "empty", "none"]
    assert cutoff_calls == []
    searches = [s for s in statements if "@@@" in s]
    order = 'ORDER BY "documents"."exact_fields"->>\'f'
    assert len(searches) == 2
    assert searches[0].startswith("SELECT documents.id FROM ") and order in searches[0]
    assert searches[0].endswith('COLLATE "C" ASC NULLS LAST, documents.id LIMIT %s OFFSET %s')
    assert "documents.id = ANY(%s)" in searches[1] and order in searches[1]
    assert not any("pdb.agg" in s or "jsonb_array_elements" in s for s in statements)

    # and with the switch on: an aggregation, then the ids with a range clause, then the rows
    monkeypatch.setattr(get_settings(), "cutoff_sort", True)
    del statements[:]
    await _page([("tags", "asc")], 0, 2, project=tag_project)
    searches = [s for s in statements if "@@@" in s and "count(*)" not in s]
    assert len(searches) == 3 and "pdb.agg" in searches[0]
    assert all("jsonb_array_elements_text" in s for s in searches[1:])


# ---------------------------------------------------------------- fallbacks


@pytest.mark.parametrize("first", ["_score", "_id", "_seq", "?", "date", "text"])
async def test_not_for_other_first_keys(project, cutoff_calls, statements, first):
    """Score, document id, internal id, random order, a sort slot column and a text field: never the cutoff sort"""
    sort = [(first, "asc"), ("kw", "asc")]
    got = await _page(sort, 1, 10, queries={"q": "red"} if first == "_score" else None)
    assert len(got) == 10
    if first in ("_id", "_seq", "date"):
        assert got == reference(project, sort)[10:20]
    assert not any(cutoff_calls)
    assert not any("pdb.agg" in s for s in statements)


async def test_fallback_on_deep_page(project, cutoff_calls, monkeypatch):
    """A page that needs more buckets than a terms aggregation returns: the late fetch, same result"""
    monkeypatch.setattr(cutoff_sort, "MAX_BUCKETS", 100)
    await _check_pages(project, [("link", "asc")], pages=[0, 8, 9, 30])
    assert cutoff_calls == [True, True, False, False]


async def test_fallback_on_sql_conditions(project, cutoff_calls):
    """A search with SQL conditions next to the json query (here: a filter on document ids) is not cut off"""
    ids = [d["_id"] for d in project[:80]]
    expected = reference(project, [("kw", "asc")], lambda d: d["_id"] in ids)
    assert await _page([("kw", "asc")], 1, 10, {"_id": {"values": ids}}) == expected[10:20]
    assert not any(cutoff_calls)
    # also when filters are scored (AMCAT4_FILTER_CLAUSES=false): an extra clause would change the scores
    get_settings().filter_clauses = False
    try:
        await _check_pages(project, [("kw", "asc")], pages=[1])
    finally:
        get_settings().filter_clauses = True
    assert not any(cutoff_calls)


async def test_fallback_without_byte_order(project, cutoff_calls, monkeypatch):
    """AMCAT4_BYTE_ORDER_SORT=false: text is sorted in the database's collation, which the search index cannot do"""
    monkeypatch.setattr(get_settings(), "byte_order_sort", False)
    assert len(await _page([("kw", "asc")], 1)) == 10
    assert len(await _page([("tags", "desc")], 1)) == 10
    assert not any(cutoff_calls)
    await _check_pages(project, [("i", "asc")], pages=[1])  # numbers and dates do not depend on it
    assert cutoff_calls[-1] is True


@pytest.fixture()
async def small_projects():
    names = ["amcat4_unittest_cutoff_a", "amcat4_unittest_cutoff_b"]
    for name in names:
        await delete_project_index(name, ignore_missing=True)
        await create_project_index(ProjectSettings(id=name))
    yield names
    for name in names:
        await delete_project_index(name, ignore_missing=True)


async def test_several_projects(small_projects, cutoff_calls):
    """A field with another json key per project: late fetch. A field of only one of the projects: cutoff sort"""
    a, b = small_projects
    docs_a = [{"_id": f"a{k:02d}", "text": "t", "kw": f"v{k % 7}", "only": f"o{k % 5}"} for k in range(30)]
    docs_b = [{"_id": f"b{k:02d}", "text": "t", "kw": f"v{k % 6}"} for k in range(30)]
    await upload(a, docs_a, fields={"text": "text", "kw": "keyword", "only": "keyword"})
    await upload(b, docs_b, fields={"text": "text", "kw": "keyword"})
    both = docs_a + docs_b

    async def page(sort, page_nr):
        r = await query_documents(
            [a, b], fields=[FieldSpec(name="kw")], sort=[{n: SortSpec(order=o)} for n, o in sort], page=page_nr, per_page=10
        )
        return [d["_id"] for d in r.data]

    assert await page([("kw", "asc")], 1) == reference(both, [("kw", "asc")])[10:20]
    assert cutoff_calls == []  # (not even tried)
    for page_nr in (0, 2, 3, 5):
        assert await page([("only", "desc")], page_nr) == reference(both, [("only", "desc")])[page_nr * 10 :][:10]
    assert cutoff_calls == [True] * 4


async def test_fallback_on_text_that_is_indexed_as_date(small_projects, cutoff_calls):
    """
    pg_search indexes a text value that looks like a timestamp as a date, and the aggregation then lists it after all
    text values, where SQL sorts it as text. With such values among the hits the cutoff sort is not used
    """
    a, _ = small_projects
    values = ["10", "2020-01-01T00:00:00Z", "9", "alpha", "2019-05-05"]
    docs = [{"_id": f"d{k:02d}", "text": "t", "kw": values[k % 5], "tags": [values[k % 5], "m"]} for k in range(40)]
    await upload(a, docs, fields={"text": "text", "kw": "keyword", "tags": "tag"})
    for name in ("kw", "tags"):
        for order in ("asc", "desc"):
            for page in (0, 1, 2):
                assert await _page([(name, order)], page, 6, project=a) == reference(docs, [(name, order)])[page * 6 :][:6]
    assert cutoff_calls and not any(cutoff_calls)
    # hits without such values: cutoff sort
    del cutoff_calls[:]
    keep = lambda d: not d["kw"].startswith("2020")  # noqa: E731
    got = await _page([("kw", "asc")], 1, 6, {"kw": {"values": ["10", "9", "alpha", "2019-05-05"]}}, project=a)
    assert got == reference(docs, [("kw", "asc")], keep)[6:12]
    assert cutoff_calls == [True]


async def test_unexpected_aggregation_result_falls_back(project, cutoff_calls, monkeypatch):
    """Bucket keys that are not in sort order (as pg_search gives for numbers with decimals): not trusted"""
    original = cutoff_sort._Sorter.aggregate

    async def shuffled(self, extra, *requests):
        results = await original(self, extra, *requests)
        if "buckets" in results[0]:
            results[0]["buckets"].reverse()
        return results

    monkeypatch.setattr(cutoff_sort._Sorter, "aggregate", shuffled)
    await _check_pages(project, [("i", "asc")], pages=[0, 5])
    await _check_pages(project, [("kw", "desc")], pages=[2])
    assert cutoff_calls == [False] * 3


async def test_keyset_paging_is_unchanged(project, cutoff_calls, statements):
    """Paging by internal id (sort on _seq, scrolling) does not use the cutoff sort"""
    r = await query_documents(PROJECT, sort=[{"_seq": SortSpec(order="desc")}], per_page=10, fields=[FieldSpec(name="kw")])
    assert [d["_id"] for d in r.data] == [d["_id"] for d in project[::-1][:10]]
    assert cutoff_calls == [] and not any("pdb.agg" in s for s in statements)


# ---------------------------------------------------------------- one snapshot


@pytest.fixture()
async def changing_project():
    name = "amcat4_unittest_cutoff_snapshot"
    await delete_project_index(name, ignore_missing=True)
    await create_project_index(ProjectSettings(id=name))
    docs = [{"_id": f"d{k:02d}", "text": "t", "kw": f"v{k:02d}", "i": k} for k in range(40)]
    await upload(name, docs, fields={"text": "text", "kw": "keyword", "i": "integer"})
    yield name, docs
    await delete_project_index(name, ignore_missing=True)


async def test_delete_between_the_two_statements(changing_project, monkeypatch):
    """A document that is deleted after the ids of the page were found is still on the page (one snapshot)"""
    name, docs = changing_project
    original = cutoff_sort.page_ids
    used = []
    monkeypatch.setattr(cutoff_sort, "SMALL_HITS", 0)

    async def page_ids_then_delete(*args, **kwargs):
        ids = await original(*args, **kwargs)
        used.append(ids is not None)
        await delete_document(name, "d03")  # (on another connection, committed at once)
        return ids

    monkeypatch.setattr(cutoff_sort, "page_ids", page_ids_then_delete)
    assert await _page([("kw", "asc")], 0, 5, project=name) == ["d00", "d01", "d02", "d03", "d04"]
    assert used == [True]
    monkeypatch.setattr(cutoff_sort, "page_ids", original)
    assert await _page([("kw", "asc")], 0, 5, project=name) == ["d00", "d01", "d02", "d04", "d05"]


async def test_upload_and_delete_inside_the_cutoff_sort(changing_project, cutoff_calls, monkeypatch):
    """
    Documents that are uploaded and deleted between the aggregation and the statement that reads the ids do not
    change the page: the cutoff and the rows are of the same moment
    """
    name, docs = changing_project
    original = cutoff_sort._Sorter.fetch
    changed = []

    async def change_then_fetch(self, *args, **kwargs):
        if not changed:
            changed.append(True)
            # five documents that sort before everything (ascending) and after everything (descending), and two gone
            new = [{"_id": f"new{k}", "text": "t", "kw": v, "i": n} for k, (v, n) in enumerate([(" a", -1), ("zz", 99)] * 3)]
            await upload(name, new)
            await delete_document(name, "d01")
            await delete_document(name, "d38")
        return await original(self, *args, **kwargs)

    monkeypatch.setattr(cutoff_sort._Sorter, "fetch", change_then_fetch)
    assert await _page([("kw", "asc")], 0, 5, project=name) == ["d00", "d01", "d02", "d03", "d04"]
    assert changed == [True]
    # the next search sees the new state
    assert await _page([("kw", "asc")], 0, 5, project=name) == ["new0", "new2", "new4", "d00", "d02"]
    assert await _page([("i", "desc")], 0, 5, project=name) == ["new1", "new3", "new5", "d39", "d37"]
    # (also after the deletes the cutoff sort is used: the aggregation does not count deleted documents)
    assert cutoff_calls == [True] * 3


async def test_snapshot_is_released(project):
    """After a search the connection is back in autocommit mode with the default isolation level"""
    from amcat4.postgres.connection import connection

    await _page([("kw", "asc")])
    async with connection() as conn:
        assert conn.isolation_level is None and conn.autocommit
        cur = await conn.execute("SHOW transaction_isolation")
        assert (await cur.fetchone())["transaction_isolation"] == "read committed"  # type: ignore[index, call-overload]
