"""
The features of version 3 together (branch postgres-fast-v3-all): each has its own test module; these tests are about
the places where two of them meet.

- the cutoff sort (cutoff_sort.py) on a project with a release and old row versions (versions.py): the release and
  the current documents each give their own rows, in the right order;
- the same with the new query API (pdb_sql.py), which must render every statement of the cutoff sort and the
  row-version clauses, without falling back to the legacy format;
- a sort on a tag field in the label table (labels.py): not a case for the cutoff sort, and the same order as tags in
  json;
- the quick wins (text-exists key, field values and statistics from the index, automatic sort slots) on a project
  with old row versions: old versions are not counted, every row version has the right list of text keys and the
  right slot values.
"""

import random
from typing import Any

import pytest
from psycopg import AsyncConnection

from amcat4.config import get_settings
from amcat4.models import FieldSpec, FilterSpec, ProjectSettings
from amcat4.postgres import cutoff_sort, pdb_sql
from amcat4.postgres.connection import fetch_one
from amcat4.postgres.fields import with_text_keys_sql
from amcat4.postgres.projects import project_pk
from amcat4.postgres.versions import CURRENT
from amcat4.projects.documents import create_or_update_documents, delete_document, update_document
from amcat4.projects.index import create_project_index, delete_project_index
from amcat4.projects.query import delete_query, query_documents, update_query, update_tag_query
from amcat4.projects.releases import create_release
from amcat4.systemdata.fields import field_infos, field_stats, field_values
from tests.conftest import upload

pytestmark = pytest.mark.anyio

PROJECT = "amcat4_unittest_v3_together"
TWIN = "amcat4_unittest_v3_together_labels"
# "date" takes the standard date slot; "day" is a date in json
FIELDS = {
    "text": "text",
    "title": "text",
    "date": "date",
    "kw": "keyword",
    "i": "integer",
    "num": "number",
    "day": "date",
    "tags": "tag",
}
TAGS = ["art", "economy", "politics", "sport", "zoo", "Ärger", "中文"]
N = 400


def make_docs(start: int, n: int, seed: int = 11) -> list[dict[str, Any]]:
    rnd = random.Random(seed + start)
    docs = []
    for k in range(start, start + n):
        doc: dict[str, Any] = {
            "_id": f"d{k:04d}",
            "text": f"common word{k % 7} number {k}",
            "date": f"2020-01-{1 + k % 28:02d}",
        }
        if k % 3:
            doc["title"] = f"title {k % 5}"
        if rnd.random() < 0.9:
            doc["kw"] = f"cat{rnd.randrange(6)}"
        if rnd.random() < 0.9:
            doc["i"] = rnd.randrange(12)
        if rnd.random() < 0.85:
            doc["num"] = rnd.choice([0, 1, 2.5, -3, 17]) if rnd.random() < 0.5 else round(rnd.uniform(-9, 9), 2)
        if rnd.random() < 0.9:
            doc["day"] = f"20{rnd.randrange(10, 20)}-0{rnd.randrange(1, 9)}-1{rnd.randrange(0, 9)}"
        if rnd.random() < 0.8:
            doc["tags"] = rnd.sample(TAGS, rnd.randrange(1, 4))
        docs.append(doc)
    return docs


@pytest.fixture(params=[False, True], ids=["legacy_api", "new_api"])
def api(request, monkeypatch):
    """Both query formats. A condition that falls back to the legacy format in the new one fails the test"""
    monkeypatch.setattr(get_settings(), "new_query_api", request.param)

    def fail(message, *args, **kwargs):
        raise AssertionError(message)

    monkeypatch.setattr(pdb_sql.logging, "warning", fail)
    return request.param


@pytest.fixture()
def json_sort_fields(monkeypatch):
    """No automatic sort slots (as in test_cutoff_sort.py): the fields are sorted on their json value"""
    monkeypatch.setattr(get_settings(), "auto_sort_slot", False)


@pytest.fixture()
def cutoff_calls(monkeypatch):
    """Per call of the cutoff sort: did it give the ids of the page. Small results are not sorted whole (SMALL_HITS 0)"""
    calls: list[bool] = []
    original = cutoff_sort.page_ids
    monkeypatch.setattr(cutoff_sort, "SMALL_HITS", 0)

    async def recording(*args, **kwargs):
        ids = await original(*args, **kwargs)
        calls.append(ids is not None)
        return ids

    monkeypatch.setattr(cutoff_sort, "page_ids", recording)
    return calls


@pytest.fixture()
def statements(monkeypatch):
    seen: list[str] = []
    original = AsyncConnection.execute

    async def execute(self, query, params=None, **kwargs):
        seen.append(query if isinstance(query, str) else query.as_string(None))
        return await original(self, query, params, **kwargs)

    monkeypatch.setattr(AsyncConnection, "execute", execute)
    return seen


async def _create(index: str, docs: list[dict]) -> None:
    await delete_project_index(index, ignore_missing=True)
    await create_project_index(ProjectSettings(id=index))
    await upload(index, docs, fields=FIELDS)  # type: ignore[arg-type]


@pytest.fixture()
async def project():
    yield PROJECT
    for index in (PROJECT, TWIN):
        await delete_project_index(index, ignore_missing=True)


async def _pages(
    index: str, sort: list[tuple[str, str]], release: int | None = None, per_page: int = 45, **kwargs
) -> list[dict]:
    """All documents of the search in the order of the sort, read page by page"""
    spec = [{name: {"order": order}} for name, order in sort]
    fields = [FieldSpec(name=name) for name in FIELDS]
    docs: list[dict] = []
    for page in range(100):
        r = await query_documents(index, fields=fields, sort=spec, page=page, per_page=per_page, release=release, **kwargs)  # type: ignore[arg-type]
        docs += r.data
        assert r.total_count is not None and len(docs) <= r.total_count
        if len(r.data) < per_page:
            assert len(docs) == r.total_count
            return docs
    raise AssertionError("too many pages")


def _ids(docs: list[dict]) -> list[str]:
    return [d["_id"] for d in docs]


def _sort_value(doc: dict, name: str, desc: bool) -> Any:
    value = doc.get(name)
    if name == "tags":
        # (the order of the cutoff sort: the smallest tag ascending, the largest descending, in byte order)
        tags = [t.encode() for t in value or []]
        return (max(tags) if desc else min(tags)) if tags else None
    return value.encode() if isinstance(value, str) else value


def assert_sorted(docs: list[dict], name: str, order: str) -> None:
    """The documents are in the order of this (first) sort key, documents without a value last"""
    values = [_sort_value(d, name, order == "desc") for d in docs]
    with_value = [v for v in values if v is not None]
    assert values == with_value + [None] * (len(values) - len(with_value)), f"{name}: missing values are not last"
    assert with_value == sorted(with_value, reverse=order == "desc"), f"{name} {order}"


async def _row_versions(index: str) -> tuple[int, int]:
    """(current rows, old row versions) of the project"""
    row = await fetch_one(
        """SELECT count(*) FILTER (WHERE retired_seq = %(c)s) AS live, count(*) FILTER (WHERE retired_seq <> %(c)s) AS old
           FROM documents WHERE project_pk = %(pk)s""",
        {"c": CURRENT, "pk": await project_pk(index)},
    )
    assert row is not None
    return row["live"], row["old"]


async def _change(index: str, start: int) -> None:
    """Writes that make old row versions when the project has a release (and move, add and remove documents)"""
    await update_document(index, "d0001", {"kw": "aaa first", "i": -7, "num": 99.5, "day": "1999-01-01"})
    await update_document(index, "d0002", {"tags": ["!first", "zzz last"]})
    await update_query(index, "kw", "moved", filters={"i": FilterSpec(values=[4])})
    await update_query(index, "num", None, filters={"kw": FilterSpec(values=["cat3"])})
    await update_query(index, "i", 1000, filters={"kw": FilterSpec(values=["cat2"])})
    await update_tag_query(index, "add", "tags", "extra", filters={"kw": FilterSpec(values=["cat5"])})
    await update_tag_query(index, "remove", "tags", "sport", filters={"i": FilterSpec(gte=6)})
    await delete_document(index, "d0003")
    await delete_query(index, filters={"i": FilterSpec(values=[7])})
    await upload(index, make_docs(start, 40))
    await create_or_update_documents(index, [{"_id": "d0005", "kw": "cat0", "day": "2030-12-31"}], op_type="upsert")


SORTS: list[list[tuple[str, str]]] = [
    [("kw", "asc")],
    [("kw", "desc"), ("i", "asc")],
    [("i", "desc")],
    [("num", "asc")],
    [("day", "desc"), ("kw", "asc")],
    [("tags", "asc")],
    [("tags", "desc"), ("kw", "asc")],
]


async def test_cutoff_sort_with_release(project, api, json_sort_fields, cutoff_calls, monkeypatch):
    """
    The cutoff sort on a project with a release and old row versions: the release keeps the pages it had (same
    documents, same order), the current documents are each on the page once and in the order of the late fetch.
    With the new query API: the statements of the cutoff sort with the row-version clauses, no legacy fallback.
    """
    await _create(project, make_docs(0, N))
    before = {str(sort): await _pages(project, sort) for sort in SORTS}
    assert cutoff_calls and all(cutoff_calls)
    for sort in SORTS:
        assert len(before[str(sort)]) == N
        assert_sorted(before[str(sort)], *sort[0])
    await create_release(project)
    await _change(project, N)
    live, old = await _row_versions(project)
    assert old > 50

    async def late_fetch(sort, release=None, **kwargs) -> list[dict]:
        with monkeypatch.context() as m:
            m.setattr(cutoff_sort, "sort_key", lambda *args, **kwargs: None)
            return await _pages(project, sort, release, **kwargs)

    current = await _pages(project, [("_seq", "asc")])
    assert len(current) == live == len(set(_ids(current)))
    for small_hits in (0, 5_000):
        # (5,000: the whole result is sorted by the first statement of the cutoff sort, small_page_ids)
        monkeypatch.setattr(cutoff_sort, "SMALL_HITS", small_hits)
        del cutoff_calls[:]
        for sort in SORTS:
            assert await _pages(project, sort, release=1) == before[str(sort)], (sort, small_hits)
            docs = await _pages(project, sort)
            assert sorted(_ids(docs)) == sorted(_ids(current)), (sort, small_hits)
            assert_sorted(docs, *sort[0])
            assert docs == await late_fetch(sort), (sort, small_hits)
        assert all(cutoff_calls) and bool(cutoff_calls) == (small_hits == 0)
    # with a query string and a filter
    monkeypatch.setattr(cutoff_sort, "SMALL_HITS", 0)
    search: dict[str, Any] = {"queries": {"q": "word1 OR word2 OR word3"}, "filters": {"i": FilterSpec(gte=2)}}
    for sort in SORTS:
        for release in (None, 1):
            docs = await _pages(project, sort, release, **search)
            assert len(docs) > 50 and docs == await late_fetch(sort, release, **search), (sort, release)
    assert all(cutoff_calls)


async def test_label_table_tag_sort(project, api, json_sort_fields, cutoff_calls, monkeypatch):
    """
    A sort on a tag field in the label table is not a case for the cutoff sort (the tags are not in the search index
    of documents): the late fetch sorts it, in the same order as a tag field in json. As a later sort key it orders
    the rows that the cutoff on the first key selects.
    """
    docs = make_docs(0, N)
    monkeypatch.setattr(get_settings(), "label_tables", False)
    await _create(project, docs)
    monkeypatch.setattr(get_settings(), "label_tables", True)
    await _create(TWIN, docs)
    monkeypatch.setattr(get_settings(), "label_tables", False)  # (a field in the label table stays there)
    assert not (await field_infos(project))["tags"].in_labels and (await field_infos(TWIN))["tags"].in_labels

    tag_sorts: list[list[tuple[str, str]]] = [[("tags", "asc")], [("tags", "desc")], [("tags", "asc"), ("i", "desc")]]

    async def compare() -> None:
        for sort in tag_sorts:
            del cutoff_calls[:]
            labels = await _pages(TWIN, sort)
            assert not cutoff_calls, sort  # the cutoff sort was not even tried
            assert_sorted(labels, "tags", sort[0][1])
            json = await _pages(project, sort)
            assert cutoff_calls and all(cutoff_calls)
            # (the documents were stored in the same order, so ties are in the same order too)
            assert _ids(labels) == _ids(json) and len(labels) == len(docs), sort
            # (tags as a set: the label table keeps a value once, in stored order)
            assert [sorted(d.get("tags") or []) for d in labels] == [sorted(d.get("tags") or []) for d in json]
        for sort in ([("kw", "asc"), ("tags", "desc")], [("i", "desc"), ("tags", "asc"), ("kw", "asc")]):
            del cutoff_calls[:]
            labels = await _pages(TWIN, sort)
            assert cutoff_calls and all(cutoff_calls), sort
            assert _ids(labels) == _ids(await _pages(project, sort)), sort

    await compare()
    # tag changes (no release: both projects are written in place, so the internal order stays the same)
    for index in (project, TWIN):
        await update_tag_query(index, "add", "tags", "!first", filters={"i": FilterSpec(values=[3])})
        await update_tag_query(index, "add", "tags", "zzz last", filters={"kw": FilterSpec(values=["cat1"])})
        await update_tag_query(index, "remove", "tags", "art", filters={"i": FilterSpec(gte=5)})
    await compare()

    # with a release: the release keeps its order, also after tag changes (label rows are versioned on their own)
    before = {str(sort): _ids(await _pages(TWIN, sort)) for sort in tag_sorts}
    await create_release(TWIN)
    await update_tag_query(TWIN, "add", "tags", "!!", filters={"kw": FilterSpec(values=["cat2"])})
    await update_tag_query(TWIN, "remove", "tags", "!first")
    await delete_query(TWIN, filters={"i": FilterSpec(values=[7])})
    for sort in tag_sorts:
        assert _ids(await _pages(TWIN, sort, release=1)) == before[str(sort)], sort
        live = await _pages(TWIN, sort)
        assert _ids(live) != before[str(sort)] and len(set(_ids(live))) == len(live)
        assert_sorted(live, "tags", sort[0][1])


def _expected_values(docs: dict[str, dict], field: str) -> list[str]:
    """field_values: the values by the number of documents (descending), then by value"""
    counts: dict[str, int] = {}
    for doc in docs.values():
        value = doc.get(field)
        for v in value if isinstance(value, list) else [] if value is None else [value]:
            counts[v] = counts.get(v, 0) + 1
    return [v for v, _ in sorted(counts.items(), key=lambda item: (-item[1], item[0].encode()))]


async def test_quick_wins_with_old_row_versions(project, api, statements, monkeypatch):
    """
    'Text field exists', field values and field statistics (inside the index and with the original statements) on a
    project with a release: old row versions and deleted documents are not counted. Every row version has the right
    list of text keys (trigger of migration 0004) and the right values in its sort slot columns (automatic slots).
    """
    settings = get_settings()
    model = {d["_id"]: d for d in make_docs(0, N)}
    await _create(project, list(model.values()))
    infos = await field_infos(project)
    # (automatic sort slots are on: kw is sorted on a slot column, which row versions must carry along)
    assert infos["kw"].sort_column and infos["date"].sort_column and infos["title"].text_exists_ready
    pk = await project_pk(project)

    async def with_title(exists: bool, release: int | None = None) -> list[str]:
        """The documents with (without) a title, with the filter inside the index and as a SQL condition: the same"""
        results = []
        for in_index in (True, False):
            monkeypatch.setattr(settings, "text_exists_key", in_index)
            del statements[:]
            docs = await _pages(project, [("_seq", "asc")], release, filters={"title": FilterSpec(exists=exists)})
            # (the SQL condition is `documents.text_fields ? 'f<pk>'`)
            assert any('"text_fields" ?' in s for s in statements) == (not in_index)
            results.append(sorted(_ids(docs)))
        monkeypatch.setattr(settings, "text_exists_key", True)
        assert results[0] == results[1]
        return results[0]

    async def values_and_stats() -> dict[str, Any]:
        """Field values and statistics, from the index and with the original statements: the same"""
        results = []
        for in_index in (True, False):
            monkeypatch.setattr(settings, "index_field_stats", in_index)
            del statements[:]
            r = {
                "kw": await field_values(project, "kw", 100),
                "tags": await field_values(project, "tags", 100),
                "i": await field_stats(project, "i"),
                "day": await field_stats(project, "day"),
            }
            assert any("pdb.agg" in s for s in statements) == in_index
            results.append(r)
        monkeypatch.setattr(settings, "index_field_stats", True)
        a, b = results
        assert a["kw"] == b["kw"] and a["tags"] == b["tags"]
        for field in ("i", "day"):
            assert {k: a[field][k] for k in ("count", "min", "max")} == {k: b[field][k] for k in ("count", "min", "max")}
        assert a["i"]["avg"] == pytest.approx(b["i"]["avg"])
        return a

    def check(result: dict[str, Any]) -> None:
        assert result["kw"] == _expected_values(model, "kw")
        assert result["tags"] == _expected_values(model, "tags")
        numbers = [d["i"] for d in model.values() if d.get("i") is not None]
        assert result["i"]["count"] == len(numbers)
        assert (result["i"]["min"], result["i"]["max"]) == (min(numbers), max(numbers))
        assert result["i"]["avg"] == pytest.approx(sum(numbers) / len(numbers))
        days = sorted(d["day"] for d in model.values() if d.get("day"))
        assert result["day"]["count"] == len(days)
        assert (result["day"]["min"][:10], result["day"]["max"][:10]) == (days[0], days[-1])

    titled = await with_title(True)
    untitled = await with_title(False)
    assert titled == sorted(k for k, d in model.items() if "title" in d) and len(titled) + len(untitled) == N
    check(await values_and_stats())
    by_kw = _ids(await _pages(project, [("kw", "asc")]))

    await create_release(project)
    # The same changes in the database and in the model
    await update_query(project, "title", None, filters={"kw": FilterSpec(values=["cat1"])})
    for doc in model.values():
        if doc.get("kw") == "cat1":
            doc.pop("title", None)
    assert "title" not in model["d0000"] and "title" in model["d0001"]
    await update_document(project, "d0000", {"title": "a new title"})
    model["d0000"]["title"] = "a new title"
    await update_query(project, "kw", "moved", filters={"i": FilterSpec(values=[4])})
    for doc in model.values():
        if doc.get("i") == 4:
            doc["kw"] = "moved"
    await update_query(project, "i", 1000, filters={"kw": FilterSpec(values=["cat2"])})
    for doc in model.values():
        if doc.get("kw") == "cat2":
            doc["i"] = 1000
    await update_tag_query(project, "add", "tags", "extra", filters={"kw": FilterSpec(values=["cat5"])})
    for doc in model.values():
        if doc.get("kw") == "cat5" and "extra" not in doc.get("tags", []):
            doc["tags"] = [*doc.get("tags", []), "extra"]
    await update_tag_query(project, "remove", "tags", "sport", filters={"i": FilterSpec(gte=6)})
    for doc in model.values():
        if doc.get("i", 0) >= 6 and "sport" in doc.get("tags", []):
            doc["tags"] = [t for t in doc["tags"] if t != "sport"]
    await delete_query(project, filters={"i": FilterSpec(values=[7])})
    for key in [k for k, d in model.items() if d.get("i") == 7]:
        del model[key]
    await delete_document(project, "d0001")
    del model["d0001"]
    new = make_docs(N, 40)
    await upload(project, new)
    model.update({d["_id"]: d for d in new})
    # replace: the document loses the fields that are not given (also its title)
    await create_or_update_documents(project, [{"_id": "d0002", "text": "replaced", "kw": "cat0"}])
    model["d0002"] = {"_id": "d0002", "text": "replaced", "kw": "cat0"}

    live, old = await _row_versions(project)
    assert live == len(model) and old > 100

    # the current documents: no old version and no deleted document is counted
    assert await with_title(True) == sorted(k for k, d in model.items() if "title" in d)
    assert await with_title(False) == sorted(k for k, d in model.items() if "title" not in d)
    check(await values_and_stats())
    # the release: as before the changes (the filter on the text keys of the old row versions; the slot column)
    assert await with_title(True, release=1) == titled
    assert await with_title(False, release=1) == untitled
    assert _ids(await _pages(project, [("kw", "asc")], release=1)) == by_kw
    current_by_kw = await _pages(project, [("kw", "asc")])
    assert len(current_by_kw) == len(model)
    assert_sorted(current_by_kw, "kw", "asc")
    assert {d["_id"]: d.get("kw") for d in current_by_kw} == {k: d.get("kw") for k, d in model.items()}

    # every row version (current, old, retired) has the list of its text keys, as the trigger of 0004 writes it
    row = await fetch_one(
        f"""SELECT count(*) FILTER (WHERE exact_fields IS DISTINCT FROM {with_text_keys_sql().as_string(None)}) AS wrong,
                   count(*) FILTER (WHERE exact_fields ? '_text' AND retired_seq <> {CURRENT}) AS old_with_keys
            FROM documents WHERE project_pk = %s""",  # type: ignore[arg-type]
        [pk],
    )
    assert row is not None and row["wrong"] == 0 and row["old_with_keys"] > 100
    # ... and its values in the sort slot columns (a copy of a row for a release must carry them along)
    for name in ("kw", "date"):
        f = infos[name]
        cast = "timestamptz" if name == "date" else "text"
        row = await fetch_one(
            f"""SELECT count(*) FILTER (WHERE {f.sort_column}::{cast} IS DISTINCT FROM (exact_fields->>'{f.key}')::{cast})
                           AS wrong,
                       count(*) FILTER (WHERE {f.sort_column} IS NOT NULL AND retired_seq <> {CURRENT}) AS old_with_value
                FROM documents WHERE project_pk = %s""",  # type: ignore[arg-type]
            [pk],
        )
        assert row is not None and row["wrong"] == 0 and row["old_with_value"] > 100, name
