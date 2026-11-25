"""
Tags in the label table (doc_labels, AMCAT4_LABEL_TABLES; amcat4.postgres.labels).

The gate: two projects get the same documents and the same writes, one with its tags inside the documents (switch off)
and one with its tags in the label table (switch on). Every search, filter, aggregation and returned value must be the
same, on the in-index read paths and on the plain SQL paths.
"""

from typing import Any

import pytest

from amcat4.config import get_settings
from amcat4.models import FieldSpec, FilterSpec, ProjectSettings
from amcat4.postgres.connection import connection, execute, fetch_all, fetch_one
from amcat4.postgres.fields import QueryError
from amcat4.postgres.projects import project_pk
from amcat4.projects import label_jobs
from amcat4.projects.aggregate import Aggregation, Axis, TopHitsAggregation, query_aggregate
from amcat4.projects.documents import create_or_update_documents, delete_document, fetch_document, update_document
from amcat4.projects.index import create_project_index, delete_project_index
from amcat4.projects.jobs import create_job, run_job_now, run_pending_jobs
from amcat4.projects.query import delete_query, query_documents, update_query, update_tag_query
from amcat4.projects.releases import create_release, verify_release
from amcat4.systemdata.fields import field_infos, field_values, update_fields
from tests.conftest import upload

pytestmark = pytest.mark.anyio

IN_JSON, IN_LABELS = "amcat4_unittest_tags_json", "amcat4_unittest_tags_labels"
FIELDS = {"text": "text", "kw": "keyword", "i": "integer", "date": "date", "tags": "tag", "codes": "tag"}
ALL = [FieldSpec(name=name) for name in FIELDS]
N = 300


def make_docs(start: int, n: int) -> list[dict[str, Any]]:
    docs = []
    for k in range(start, start + n):
        doc: dict[str, Any] = {
            "_id": f"d{k}",
            "text": f"common word{k % 7} text number {k}" + (" rare" if k % 40 == 0 else ""),
            "kw": f"cat{k % 4}",
            "i": k % 11,
            "date": f"20{10 + k % 5}-{1 + k % 12:02d}-{1 + k % 28:02d}",
        }
        # (the order of the tags differs per document: it must come back as it was given)
        tags = [t for j, t in enumerate(["sport", "economy", "politics", "art", "Zebra"]) if (k + j) % 3 == 0]
        if tags and k % 7:
            doc["tags"] = tags if k % 2 else tags[::-1]
        if k % 5 == 0:
            doc["codes"] = [f"c{k % 2}"]
        docs.append(doc)
    return docs


async def _make(index: str, label_tables: bool, monkeypatch) -> None:
    monkeypatch.setattr(get_settings(), "label_tables", label_tables)
    await delete_project_index(index, ignore_missing=True)
    await create_project_index(ProjectSettings(id=index))
    await upload(index, make_docs(0, N), fields=FIELDS)  # type: ignore[arg-type]
    infos = await field_infos(index)
    assert infos["tags"].in_labels is label_tables and infos["codes"].in_labels is label_tables


@pytest.fixture(params=["in_index", "plain_sql", "parent_table"])
async def both(request, monkeypatch):
    """
    The same project twice: tags in json, and tags in the label table. Three ways to read: the in-index paths forced
    on (partition table, also for small results), the defaults, and the parent table only (fast path off).
    """
    from amcat4.postgres import search
    from amcat4.projects import aggregate

    if request.param == "in_index":
        monkeypatch.setattr(aggregate, "SMALL_RESULT", 0)
        monkeypatch.setattr(search, "COUNT_STEP", 0)
    elif request.param == "parent_table":
        monkeypatch.setattr(get_settings(), "fast_path", False)
    await _make(IN_JSON, False, monkeypatch)
    await _make(IN_LABELS, True, monkeypatch)
    yield IN_JSON, IN_LABELS
    for index in (IN_JSON, IN_LABELS):
        await delete_project_index(index, ignore_missing=True)


@pytest.fixture
async def pair(monkeypatch):
    """The same project twice (tags in json, tags in the label table), read with the default settings"""
    await _make(IN_JSON, False, monkeypatch)
    await _make(IN_LABELS, True, monkeypatch)
    yield IN_JSON, IN_LABELS
    for index in (IN_JSON, IN_LABELS):
        await delete_project_index(index, ignore_missing=True)


async def _docs(index, per_page=1000, fields=ALL, **kwargs) -> tuple[int | None, list[dict]]:
    result = await query_documents(index, fields=fields, per_page=per_page, **kwargs)
    return result.total_count, result.data


async def _agg(index, axes, aggregations=None, **kwargs) -> list[dict]:
    return list((await query_aggregate(index, axes, aggregations, **kwargs)).as_dicts())


def tag_filter(**kwargs) -> dict[str, FilterSpec]:
    return {"tags": FilterSpec(**kwargs)}


# Results whose order depends on the internal ids of the documents when the sort values are equal
ORDER_BY_ID = ("by_date", "by_kw_i", "sort_on_tags", "sort_on_tags_desc", "query_top", "query_top_missing", "top_hits")
ORDER_BY_ID += ("query_tag_by_date",)
# The relevance order. It is the same for two projects that were filled in the same way. After writes it is not: a
# BM25 score depends on the index segment a document is in, and a tag change in json moves the document to a new
# segment, where a tag change in the label table leaves it where it is
RELEVANCE = ("query_top", "query_top_missing")


def without_relevance(s: dict) -> dict:
    return {key: value for key, value in s.items() if key not in RELEVANCE}


async def everything(index: str | list[str], same_ids: bool = True) -> dict[str, Any]:
    """
    Every way to read tags: returned values, filters (alone and with a search), sorting, aggregations, values.
    same_ids=False: for two projects whose documents do not have the same internal ids in the same order. That is so
    once a project has a release: with tags in json a tag change makes a new row version with a new id, with tags in
    the label table the document row is not touched. Then the lists are ordered by document id (_id) instead of _seq,
    and the results in ORDER_BY_ID are left out.
    """
    s: dict[str, Any] = {}
    ASC = [{"_seq": {"order": "asc"}}] if same_ids else [{"_id": {"order": "asc"}}]
    s["all"] = await _docs(index, sort=ASC)
    s["only_tags"] = await _docs(index, fields=[FieldSpec(name="tags")], sort=ASC, per_page=40)
    s["page_3"] = await _docs(index, sort=ASC, per_page=25, page=3)
    s["value"] = await _docs(index, filters=tag_filter(values=["sport"]), sort=ASC)
    s["values"] = await _docs(index, filters=tag_filter(values=["art", "Zebra", "none"]), sort=ASC, per_page=20)
    s["no_such_value"] = await _docs(index, filters=tag_filter(values=["none"]))
    s["exists"] = await _docs(index, filters=tag_filter(exists=True), sort=ASC)
    s["missing"] = await _docs(index, filters=tag_filter(exists=False), sort=ASC)
    s["range"] = await _docs(index, filters=tag_filter(gte="b", lt="p"), sort=ASC)
    s["range_gt"] = await _docs(index, filters=tag_filter(gt="economy"), sort=ASC)
    s["range_capital"] = await _docs(index, filters=tag_filter(lte="a"), sort=ASC)  # (byte order: Zebra < a)
    s["value_and_exists"] = await _docs(index, filters=tag_filter(values=["sport"], exists=True), sort=ASC)
    s["two_fields"] = await _docs(
        index, filters={"tags": FilterSpec(values=["economy"]), "codes": FilterSpec(values=["c0"])}, sort=ASC
    )
    s["tag_and_other"] = await _docs(index, filters={**tag_filter(values=["art"]), "i": FilterSpec(gte=3, lt=9)}, sort=ASC)
    s["tag_and_ids"] = await _docs(index, filters={**tag_filter(values=["art"]), "_id": FilterSpec(values=["d3", "d4", "d6"])})
    total, hits = await _docs(index, queries={"q": "common"}, filters=tag_filter(values=["politics"]))
    s["query"] = (total, sorted(d["_id"] for d in hits))
    total, hits = await _docs(index, queries={"q": "word3 OR rare"}, filters=tag_filter(exists=False))
    s["query_missing"] = (total, sorted(d["_id"] for d in hits))
    # relevance order, first page: the same documents in the same order (text and scores do not depend on the tags)
    s["query_top"] = await _docs(index, queries={"q": "rare word2"}, filters=tag_filter(values=["sport", "art"]), per_page=7)
    # (documents without the tag: the scores are other numbers than with tags in json, the order must be the same)
    total, hits = await _docs(index, queries={"q": "rare word2 word4"}, filters=tag_filter(exists=False), per_page=12)
    s["query_top_missing"] = (total, [d["_id"] for d in hits])
    s["by_date"] = await _docs(index, per_page=15, sort=[{"date": {"order": "desc"}}], filters=tag_filter(values=["sport"]))
    s["by_kw_i"] = await _docs(
        index, per_page=15, sort=[{"kw": {"order": "asc"}}, {"i": {"order": "desc"}}], filters=tag_filter(values=["sport"])
    )
    s["sort_on_tags"] = await _docs(index, per_page=30, sort=[{"tags": {"order": "asc"}}])
    s["sort_on_tags_desc"] = await _docs(
        index, per_page=30, sort=[{"tags": {"order": "desc"}}], filters={"i": FilterSpec(lt=5)}
    )
    s["count_only"] = (await _docs(index, per_page=1, filters=tag_filter(values=["economy"])))[0]
    s["agg_tags"] = await _agg(index, [Axis("tags")])
    s["agg_codes"] = await _agg(index, [Axis("codes")])
    s["agg_tags_query"] = await _agg(index, [Axis("tags")], queries={"q": "word1 OR word2"})
    s["agg_tags_kw"] = await _agg(index, [Axis("tags"), Axis("kw")], filters={"i": FilterSpec(lte=8)})
    s["agg_kw_tags"] = await _agg(index, [Axis("kw"), Axis("tags")])
    s["agg_codes_tags"] = await _agg(index, [Axis("codes"), Axis("tags")])
    s["agg_tags_year"] = await _agg(index, [Axis("tags"), Axis("date", interval="year")])
    s["agg_tags_metric"] = await _agg(index, [Axis("tags")], [Aggregation("i", "sum"), Aggregation("i", "avg")])
    s["agg_tags_by_tag"] = await _agg(index, [Axis("tags")], filters=tag_filter(values=["sport"]))
    s["agg_tags_by_code"] = await _agg(index, [Axis("tags")], filters={"codes": FilterSpec(exists=True)})
    s["agg_kw_by_tag"] = await _agg(index, [Axis("kw")], filters=tag_filter(values=["sport", "art"]))
    s["agg_kw_by_two"] = await _agg(
        index, [Axis("kw")], filters={"tags": FilterSpec(values=["sport"]), "codes": FilterSpec(exists=False)}
    )
    s["agg_month_by_tag"] = await _agg(index, [Axis("date", interval="month")], filters=tag_filter(exists=False))
    s["agg_metric_by_tag"] = await _agg(index, [], [Aggregation("i", "sum")], filters=tag_filter(values=["art"]))
    s["agg_year_metric_by_tag"] = await _agg(
        index, [Axis("date", interval="year")], [Aggregation("i", "max")], filters=tag_filter(values=["art"])
    )
    s["agg_kw_query_tag"] = await _agg(index, [Axis("kw")], queries={"q": "common"}, filters=tag_filter(values=["sport"]))
    s["agg_tags_query_tag"] = await _agg(
        index, [Axis("tags")], [Aggregation("i", "sum")], queries={"q": "word1 OR word2"}, filters=tag_filter(exists=True)
    )
    s["agg_year_query_no_tag"] = await _agg(
        index,
        [Axis("date", interval="year")],
        [Aggregation("i", "avg")],
        queries={"q": "common"},
        filters=tag_filter(exists=False),
    )
    s["agg_total_query_tag"] = await _agg(
        index, [], [Aggregation("i", "sum")], queries={"q": "word3"}, filters=tag_filter(gte="p")
    )
    s["query_two_tags"] = await _docs(
        index,
        queries={"q": "common"},
        filters={"tags": FilterSpec(values=["economy"]), "codes": FilterSpec(values=["c0", "c1"])},
        sort=ASC,
    )
    s["query_tag_by_date"] = await _docs(
        index, queries={"q": "common"}, filters=tag_filter(values=["art"]), sort=[{"date": {"order": "desc"}}], per_page=12
    )
    s["query_highlight"] = await _docs(
        index, queries={"q": "rare"}, filters=tag_filter(values=["sport"]), highlight=True, sort=ASC
    )
    s["agg_queries"] = await _agg(index, [Axis("_query"), Axis("tags")], queries={"a": "word1", "b": "rare"})
    s["top_hits"] = await _agg(
        index, [Axis("kw")], [TopHitsAggregation(fields=["tags", "i"], sort=[{"i": {"order": "desc"}}], n=2)]
    )
    if isinstance(index, str):
        s["field_values"] = await field_values(index, "tags", 10)
        s["field_values_3"] = await field_values(index, "tags", 3)
        fetched = [await fetch_document(index, f"d{k}") for k in (0, 1, 2, 3, 10)]
        s["fetch"] = [{k: v for k, v in doc.items() if k != "_copied_from"} for doc in fetched]
        s["fetch_tags"] = (await fetch_document(index, "d3", ["tags"])).get("tags")
    if not same_ids:
        for key in ORDER_BY_ID:
            del s[key]
    return s


async def write_everything(index: str, start: int) -> list:
    """Every kind of write that touches tags; returns what the writes return"""
    out: list[Any] = []
    for doc in (
        {"_id": "d1", "text": "a completely new text", "kw": "changed"},
        {"_id": "d2", "tags": ["new", "sport"]},
        {"_id": "d7", "tags": ["politics", "art"]},  # (maybe the same tags, in another order)
    ):
        out.append(await create_or_update_documents(index, [doc], op_type="upsert"))
    out.append(
        await create_or_update_documents(
            index, [{"_id": "d3", "text": "replaced word3", "tags": ["art"]}, {"_id": "d4", "text": "replaced too"}]
        )
    )
    out.append(await create_or_update_documents(index, [{"_id": "d5", "i": 99, "codes": ["c9"]}], op_type="upsert"))
    out.append(await create_or_update_documents(index, [{"_id": "d8", "tags": "single"}], op_type="upsert"))
    out.append(await update_query(index, "kw", "moved", filters={"i": FilterSpec(values=[4])}))
    out.append(await update_query(index, "codes", ["set", "by query"], filters={"kw": FilterSpec(values=["cat3"])}))
    out.append(await update_query(index, "codes", None, filters={"tags": FilterSpec(values=["Zebra"])}))
    out.append(await update_query(index, "tags", "one", ids=["d30", "d31"]))
    out.append(await update_tag_query(index, "add", "tags", "extra", queries={"q": "word1"}))
    out.append(await update_tag_query(index, "add", "tags", "sport", filters={"kw": FilterSpec(values=["cat1"])}))
    out.append(await update_tag_query(index, "add", "tags", "sport", filters={"tags": FilterSpec(values=["art"])}))
    out.append(await update_tag_query(index, "remove", "tags", "economy", filters={"i": FilterSpec(gte=5)}))
    out.append(await update_tag_query(index, "remove", "tags", "sport", filters={"tags": FilterSpec(values=["politics"])}))
    out.append(await update_tag_query(index, "remove", "codes", "c0", ids=["d10", "d20"]))
    out.append(await update_tag_query(index, "add", "codes", "c1", ids=["d10", "d11"]))
    out.append(await update_tag_query(index, "remove", "tags", "no such tag"))
    out.append(await update_tag_query(index, "add", "tags", "extra", queries={"q": "word1"}))  # (all have it already)
    await delete_document(index, "d6")
    out.append(await delete_query(index, queries={"q": "word6"}))
    out.append(await delete_query(index, filters={"tags": FilterSpec(values=["politics"]), "i": FilterSpec(values=[7])}))
    out.append(await upload(index, make_docs(start, 30)))
    out.append(await upload(index, [{"_id": "d6", "text": "common again", "tags": ["art"]}]))
    return out


def assert_same(a: dict, b: dict) -> None:
    assert a.keys() == b.keys()
    for key in a:
        assert a[key] == b[key], key


async def test_label_tables_give_the_same_results(both):
    """The gate for the label table: every read gives the same as tags in json, also after every kind of write"""
    in_json, in_labels = both
    expected = await everything(in_json)
    assert expected["value"][0] and expected["missing"][0] and expected["range"][0] and expected["agg_tags_kw"]
    assert expected["range_capital"][0] and expected["two_fields"][0] and expected["query_top"][0]
    assert_same(await everything(in_labels), expected)

    assert await write_everything(in_labels, N) == await write_everything(in_json, N)
    expected2 = await everything(in_json)
    assert expected2["all"] != expected["all"] and expected2["agg_tags"] != expected["agg_tags"]
    assert_same(without_relevance(await everything(in_labels)), without_relevance(expected2))

    # with a release in between: the label rows are then retired, not deleted
    await create_release(in_json)
    await create_release(in_labels)
    assert await write_everything(in_labels, N + 100) == await write_everything(in_json, N + 100)
    assert_same(await everything(in_labels, same_ids=False), await everything(in_json, same_ids=False))
    # the tags are really in the label table, and not in the documents
    pk = await project_pk(in_labels)
    assert (await fetch_one("SELECT count(*) AS n FROM doc_labels WHERE project_pk = %s", [pk]))["n"] > N  # type: ignore[index]
    infos = await field_infos(in_labels)
    row = await fetch_one(
        "SELECT count(*) AS n FROM documents WHERE project_pk = %s AND exact_fields ?| %s",
        [pk, [infos["tags"].key, infos["codes"].key]],
    )
    assert row and row["n"] == 0
    assert (await verify_release(in_labels, 1))["ok"] and (await verify_release(in_json, 1))["ok"]


async def test_several_projects_with_and_without_label_table(both):
    """A query on two projects, one with its tags in json and one in the label table"""
    in_json, in_labels = both
    one = await everything(in_json)
    mixed = await everything([in_json, in_labels])
    for key in ("value", "values", "exists", "missing", "range", "two_fields", "tag_and_other", "query", "count_only"):
        total = one[key][0] if isinstance(one[key], tuple) else one[key]
        mixed_total = mixed[key][0] if isinstance(mixed[key], tuple) else mixed[key]
        assert mixed_total == 2 * total, key
    for key in ("agg_tags", "agg_tags_kw", "agg_kw_by_tag", "agg_tags_by_tag", "agg_kw_by_two", "agg_tags_metric"):
        doubled = [{**row, "n": 2 * row["n"]} for row in one[key]]
        assert [{k: v for k, v in row.items() if k == "n" or k in ("tags", "kw")} for row in mixed[key]] == [
            {k: v for k, v in row.items() if k == "n" or k in ("tags", "kw")} for row in doubled
        ], key
    total, docs = mixed["all"]
    assert total == 2 * N and sorted(str(d.get("tags")) for d in docs) == sorted(
        2 * [str(d.get("tags")) for d in one["all"][1]]
    )
    # writes by query on both projects
    result = await update_tag_query([in_json, in_labels], "add", "tags", "both", filters=tag_filter(values=["sport"]))
    assert result["updated"] == 2 * one["value"][0]
    assert (await _docs([in_json, in_labels], filters=tag_filter(values=["both"]), per_page=1))[0] == 2 * one["value"][0]
    assert_same(without_relevance(await everything(in_labels)), without_relevance(await everything(in_json)))


async def _rows(index: str) -> list[dict]:
    pk = await project_pk(index)
    return await fetch_all(
        "SELECT id, xmin::text AS xmin, ctid::text AS ctid, updated_at FROM documents WHERE project_pk = %s ORDER BY id", [pk]
    )


async def test_tag_changes_do_not_touch_the_documents(index, monkeypatch):
    """With the label table, adding and removing tags writes label rows only: no document row is rewritten"""
    monkeypatch.setattr(get_settings(), "label_tables", True)
    await upload(index, make_docs(0, 50), fields=FIELDS)  # type: ignore[arg-type]
    before = await _rows(index)
    assert (await update_tag_query(index, "add", "tags", "extra", queries={"q": "common"}))["updated"] == 50
    assert (await update_tag_query(index, "remove", "tags", "sport"))["updated"] > 0
    await update_query(index, "codes", ["x"], filters={"kw": FilterSpec(values=["cat1"])})
    assert await _rows(index) == before
    assert (await _docs(index, filters=tag_filter(values=["extra"]), per_page=1))[0] == 50
    assert (await _docs(index, filters=tag_filter(values=["sport"]), per_page=1))[0] == 0
    # also when the documents are in a release: the release keeps the old tags, and still no document row is written
    await create_release(index)
    await update_tag_query(index, "remove", "tags", "extra", filters={"kw": FilterSpec(values=["cat0"])})
    await update_tag_query(index, "add", "tags", "after", filters={"kw": FilterSpec(values=["cat0"])})
    assert await _rows(index) == before
    released = await query_documents(index, filters=tag_filter(values=["extra"]), per_page=1, release=1)
    assert released.total_count == 50
    assert (await _docs(index, filters=tag_filter(values=["extra"]), per_page=1))[0] < 50
    released = await query_documents(index, filters=tag_filter(values=["after"]), per_page=1, release=1)
    assert released.total_count == 0
    # tags in json: the same change rewrites the documents (what the label table avoids)
    monkeypatch.setattr(get_settings(), "label_tables", False)
    await update_tag_query(index, "add", "old_style", "x", filters={"kw": FilterSpec(values=["cat0"])})
    assert await _rows(index) != before


async def test_label_table_limits(index, monkeypatch):
    """What a tag field in the label table cannot do is refused with a clear error"""
    monkeypatch.setattr(get_settings(), "label_tables", True)
    await upload(index, make_docs(0, 20), fields=FIELDS)  # type: ignore[arg-type]
    assert (await field_infos(index))["tags"].in_labels
    with pytest.raises(QueryError, match="use a filter"):
        await query_documents(index, queries={"q": "tags:sport"})
    from amcat4.models import UpdateDocumentField

    with pytest.raises(ValueError, match="label table"):
        await update_fields(index, {"tags": UpdateDocumentField(type="keyword")})
    # a value occurs once per document, and an empty list is no value
    await upload(index, [{"_id": "x", "text": "t", "tags": ["a", "b", "a"]}, {"_id": "y", "text": "t", "tags": []}])
    assert (await fetch_document(index, "x"))["tags"] == ["a", "b"]
    assert "tags" not in await fetch_document(index, "y")
    # the switch only decides about new fields: this field stays in the label table and keeps working
    monkeypatch.setattr(get_settings(), "label_tables", False)
    await update_tag_query(index, "add", "tags", "later", ids=["x"])
    assert (await fetch_document(index, "x"))["tags"] == ["a", "b", "later"]
    await update_tag_query(index, "add", "new_field", "later", ids=["x"])
    assert not (await field_infos(index))["new_field"].in_labels


async def _copy(source: str, destination: str) -> None:
    job = await create_job("copy", destination, None, {"source": source, "destination": destination})
    assert (await run_job_now(job["id"]))["status"] == "done"


async def test_copy_between_json_and_label_table(pair, monkeypatch):
    """The copy job copies tags from json to the label table, from the label table to json, and between label tables"""
    in_json, in_labels = pair
    expected = await everything(in_json)
    targets = {"amcat4_unittest_copy_a": (in_json, True), "amcat4_unittest_copy_b": (in_labels, False)}
    targets["amcat4_unittest_copy_c"] = (in_labels, True)
    try:
        for target, (source, label_tables) in targets.items():
            monkeypatch.setattr(get_settings(), "label_tables", label_tables)
            await delete_project_index(target, ignore_missing=True)
            await create_project_index(ProjectSettings(id=target))
            await _copy(source, target)
            assert (await field_infos(target))["tags"].in_labels is label_tables
            assert_same(without_relevance(await everything(target)), without_relevance(expected))
            # a second copy, after changes in the source, updates the copies
            await update_tag_query(source, "add", "tags", "copied twice", filters={"kw": FilterSpec(values=["cat2"])})
            await update_document(source, "d1", {"tags": ["only this"]})
            await _copy(source, target)
            assert_same(without_relevance(await everything(target)), without_relevance(await everything(source)))
            expected = await everything(in_json)
            if source == in_json:  # (keep the two sources the same)
                await update_tag_query(in_labels, "add", "tags", "copied twice", filters={"kw": FilterSpec(values=["cat2"])})
                await update_document(in_labels, "d1", {"tags": ["only this"]})
            else:
                await update_tag_query(in_json, "add", "tags", "copied twice", filters={"kw": FilterSpec(values=["cat2"])})
                await update_document(in_json, "d1", {"tags": ["only this"]})
                expected = await everything(in_json)
    finally:
        for target in targets:
            await delete_project_index(target, ignore_missing=True)


async def test_backfill_moves_tags_to_the_label_table(pair, monkeypatch):
    """
    The backfill_labels job moves the tags of an existing project (with old row versions and releases) to the label
    table: the current documents and every release read the same before and after, and the json no longer has the tags
    """
    from tests.test_releases import snapshot as release_snapshot

    in_json, _ = pair
    monkeypatch.setattr(label_jobs, "STRIP_GRACE_SECONDS", 0)
    monkeypatch.setattr("tests.test_releases.PROJECT", in_json)
    pk = await project_pk(in_json)
    await create_release(in_json)
    await write_everything(in_json, N)
    await create_release(in_json)
    await write_everything(in_json, N + 100)
    # as a project from before migration 0005: no doc_pk on its rows
    await execute("ALTER TABLE documents DISABLE TRIGGER USER")
    try:
        await execute("UPDATE documents SET doc_pk = NULL WHERE project_pk = %s AND doc_pk = id", [pk])
    finally:
        await execute("ALTER TABLE documents ENABLE TRIGGER USER")
    await execute("UPDATE projects SET labels_ready = false WHERE pk = %s", [pk])
    live, r1, r2 = await everything(in_json), await release_snapshot(1), await release_snapshot(2)
    # a new tag field of a project that is not ready stays in the json, also with the switch on
    monkeypatch.setattr(get_settings(), "label_tables", True)
    await update_tag_query(in_json, "add", "later", "x", ids=["d1"])
    assert not (await field_infos(in_json))["later"].in_labels
    await update_tag_query(in_json, "remove", "later", "x", ids=["d1"])

    await label_jobs.schedule_label_backfill({})
    jobs = await fetch_all("SELECT id, status FROM jobs WHERE type = 'backfill_labels' AND project_pk = %s", [pk])
    assert len(jobs) == 1
    await run_pending_jobs()
    job = await fetch_one("SELECT status, result, error FROM jobs WHERE id = %s", [jobs[0]["id"]])
    assert job and job["status"] == "done", job
    assert set(job["result"]["fields"]) == {"tags", "codes", "later"}

    infos = await field_infos(in_json)
    assert all(infos[name].in_labels for name in ("tags", "codes", "later"))
    row = await fetch_one("SELECT labels_ready FROM projects WHERE pk = %s", [pk])
    assert row and row["labels_ready"]
    row = await fetch_one(
        "SELECT count(*) FILTER (WHERE doc_pk IS NULL) AS no_pk, count(*) FILTER (WHERE exact_fields ?| %s) AS in_json"
        " FROM documents WHERE project_pk = %s",
        [[infos["tags"].key, infos["codes"].key], pk],
    )
    assert row == {"no_pk": 0, "in_json": 0}
    assert_same(without_relevance(await everything(in_json)), without_relevance(live))
    assert await release_snapshot(1) == r1
    assert await release_snapshot(2) == r2
    assert (await verify_release(in_json, 1))["ok"] and (await verify_release(in_json, 2))["ok"]
    # nothing left to do: no second job
    await label_jobs.schedule_label_backfill({})
    jobs = await fetch_all("SELECT id FROM jobs WHERE type = 'backfill_labels' AND project_pk = %s", [pk])
    assert len(jobs) == 1
    # and the project keeps working: writes after the backfill
    await write_everything(in_json, N + 200)
    assert await release_snapshot(1) == r1


async def test_stale_field_definitions_are_refused(index, monkeypatch):
    """A write that read the field definitions before the backfill moved a tag field does not write tags into the json"""
    from amcat4.errors import ConflictError
    from amcat4.postgres import documents as storage
    from amcat4.systemdata.fields import get_fieldset

    monkeypatch.setattr(get_settings(), "label_tables", False)
    await upload(index, make_docs(0, 10), fields=FIELDS)  # type: ignore[arg-type]
    pk = await project_pk(index)
    stale = await field_infos(index)
    stale_fieldset = await get_fieldset(index)
    monkeypatch.setattr(get_settings(), "label_tables", True)
    monkeypatch.setattr(label_jobs, "STRIP_GRACE_SECONDS", 0)
    job = await create_job("backfill_labels", index, None, {"project_pk": pk})
    assert (await run_job_now(job["id"]))["status"] == "done"
    from amcat4.postgres.search import SearchQuery

    async with connection() as conn:
        with pytest.raises(ConflictError, match="try again"):
            await storage.upload_documents(conn, pk, [{"_id": "d1", "tags": ["lost?"]}], stale, "update")
        with pytest.raises(ConflictError, match="try again"):
            await storage.update_tag_by_query(conn, stale_fieldset, SearchQuery(), "tags", "lost?", "add")
        with pytest.raises(ConflictError, match="try again"):
            await storage.update_by_query(conn, stale_fieldset, SearchQuery(), "tags", "lost?")
    assert (await _docs(index, filters=tag_filter(values=["lost?"]), per_page=1))[0] == 0
