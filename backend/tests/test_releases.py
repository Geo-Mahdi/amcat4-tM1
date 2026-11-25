"""
Frozen releases (migration 0005, amcat4.projects.releases): a release made before changes returns exactly the same
documents, values, tags and counts after updates, deletes, tag changes and new uploads, also after VACUUM and after the
purge for another release. Current ("live") queries never see an old row version.

Every test runs twice: with tags inside the documents (label_tables off, as in the original) and with tags in the
doc_labels table (label_tables on), and with the in-index read paths forced on (see test_fast_path.py).
"""

import asyncio
from typing import Any

import pytest

from amcat4.config import get_settings
from amcat4.errors import ConflictError, NotFoundError
from amcat4.models import CreateDocumentField, FieldSpec, FilterSpec, ProjectSettings, UpdateDocumentField
from amcat4.postgres import documents as storage
from amcat4.postgres.connection import connection, fetch_all, fetch_one
from amcat4.postgres.layout import partition_table
from amcat4.postgres.projects import project_partitions, project_pk
from amcat4.postgres.versions import CURRENT
from amcat4.projects.aggregate import Aggregation, Axis, query_aggregate
from amcat4.projects.documents import (
    create_or_update_documents,
    delete_document,
    fetch_document,
    update_document,
)
from amcat4.projects.index import create_project_index, delete_project_index
from amcat4.projects.jobs import run_pending_jobs
from amcat4.projects.query import delete_query, query_documents, update_query, update_tag_query
from amcat4.projects.releases import (
    create_release,
    delete_release,
    get_release,
    list_releases,
    purge,
    set_release_doi,
    verify_release,
)
from amcat4.systemdata.fields import delete_fields, field_infos, field_values, update_fields
from tests.conftest import upload

pytestmark = pytest.mark.anyio

PROJECT = "amcat4_unittest_releases"
FIELDS = {"text": "text", "kw": "keyword", "i": "integer", "date": "date", "tags": "tag", "codes": "tag"}
ALL = [FieldSpec(name=name) for name in FIELDS]
N = 240


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
        tags = [t for j, t in enumerate(["sport", "economy", "politics", "art"]) if (k + j) % 3 == 0]
        if tags:
            doc["tags"] = tags
        if k % 5 == 0:
            doc["codes"] = [f"c{k % 2}"]
        docs.append(doc)
    return docs


@pytest.fixture(params=[False, True], ids=["tags_in_json", "label_tables"])
async def project(request, monkeypatch):
    """A project with documents and tags, with the label table switched off or on, and the in-index paths forced on"""
    from amcat4.postgres import search
    from amcat4.projects import aggregate

    monkeypatch.setattr(get_settings(), "label_tables", request.param)
    monkeypatch.setattr(aggregate, "SMALL_RESULT", 0)
    monkeypatch.setattr(search, "COUNT_STEP", 0)
    await delete_project_index(PROJECT, ignore_missing=True)
    await create_project_index(ProjectSettings(id=PROJECT))
    await upload(PROJECT, make_docs(0, N), fields=FIELDS)  # type: ignore[arg-type]
    yield PROJECT
    await delete_project_index(PROJECT, ignore_missing=True)


async def _agg(release, axes, aggregations=None, **kwargs) -> list[dict]:
    result = await query_aggregate(PROJECT, axes, aggregations, release=release, **kwargs)
    return list(result.as_dicts())


async def _docs(release, per_page=1000, **kwargs) -> tuple[int | None, list[dict]]:
    result = await query_documents(PROJECT, fields=ALL, per_page=per_page, release=release, **kwargs)
    return result.total_count, result.data


async def snapshot(release: int | None = None) -> dict[str, Any]:
    """Everything a user can ask of the project (or of a release of it): documents, values, tags, counts"""
    s: dict[str, Any] = {}
    asc = [{"_seq": {"order": "asc"}}]
    s["all"] = await _docs(release, sort=asc)
    s["by_date"] = await _docs(release, per_page=50, sort=[{"date": {"order": "desc"}}])
    s["by_kw"] = await _docs(release, per_page=50, sort=[{"kw": {"order": "asc"}}, {"i": {"order": "desc"}}])
    total, hits = await _docs(release, queries={"q": "word3"})
    s["query"] = (total, sorted(d["_id"] for d in hits))
    total, hits = await _docs(release, queries={"q": "rare OR word5"}, filters={"kw": FilterSpec(values=["cat1", "cat2"])})
    s["query_filter"] = (total, sorted(d["_id"] for d in hits))
    s["filter_i"] = await _docs(release, filters={"i": FilterSpec(gte=3, lt=6)}, sort=asc)
    s["filter_tag"] = await _docs(release, filters={"tags": FilterSpec(values=["sport"])}, sort=asc)
    s["filter_tags"] = await _docs(release, filters={"tags": FilterSpec(values=["art", "zzz"])}, sort=asc, per_page=20)
    s["filter_two"] = await _docs(
        release, filters={"tags": FilterSpec(values=["economy"]), "codes": FilterSpec(values=["c0"])}, sort=asc
    )
    s["tag_exists"] = (await _docs(release, filters={"tags": FilterSpec(exists=True)}, per_page=1))[0]
    s["tag_missing"] = (await _docs(release, filters={"tags": FilterSpec(exists=False)}, per_page=1))[0]
    total, hits = await _docs(release, queries={"q": "common"}, filters={"tags": FilterSpec(values=["politics"])})
    s["query_tag"] = (total, sorted(d["_id"] for d in hits))
    s["by_date_tag"] = await _docs(
        release, per_page=15, sort=[{"date": {"order": "desc"}}], filters={"tags": FilterSpec(values=["sport"])}
    )
    s["agg_kw"] = await _agg(release, [Axis("kw")])
    s["agg_tags"] = await _agg(release, [Axis("tags")])
    s["agg_tags_query"] = await _agg(release, [Axis("tags")], queries={"q": "word1 OR word2"})
    s["agg_tags_kw"] = await _agg(release, [Axis("tags"), Axis("kw")], filters={"i": FilterSpec(lte=8)})
    s["agg_codes_tags"] = await _agg(release, [Axis("codes"), Axis("tags")])
    s["agg_kw_by_tag"] = await _agg(release, [Axis("kw")], filters={"tags": FilterSpec(values=["sport", "art"])})
    s["agg_year"] = await _agg(release, [Axis("date", interval="year")], [Aggregation("i", "avg"), Aggregation("i", "max")])
    s["agg_tags_metric"] = await _agg(release, [Axis("tags")], [Aggregation("i", "sum")])
    s["agg_total"] = await _agg(release, [], [Aggregation("i", "sum")])
    return s


def assert_same(a: dict, b: dict) -> None:
    for key in a:
        assert a[key] == b[key], key


async def change_everything(start: int) -> None:
    """Every kind of write: update, replace, update and delete by query, tag changes, deletes, new documents"""
    await update_document(PROJECT, "d1", {"text": "a completely new text", "kw": "changed"})
    await update_document(PROJECT, "d2", {"tags": ["new", "sport"]})
    await create_or_update_documents(
        PROJECT, [{"_id": "d3", "text": "replaced word3", "tags": ["art"]}, {"_id": "d4", "text": "replaced too"}]
    )
    await create_or_update_documents(PROJECT, [{"_id": "d5", "i": 99, "codes": ["c9"]}], op_type="upsert")
    await update_query(PROJECT, "kw", "moved", filters={"i": FilterSpec(values=[4])})
    await update_query(PROJECT, "i", None, filters={"kw": FilterSpec(values=["cat3"])})
    await update_tag_query(PROJECT, "add", "tags", "extra", queries={"q": "word1"})
    await update_tag_query(PROJECT, "add", "tags", "sport", filters={"kw": FilterSpec(values=["cat1"])})
    await update_tag_query(PROJECT, "remove", "tags", "economy", filters={"i": FilterSpec(gte=5)})
    await update_tag_query(PROJECT, "remove", "codes", "c0", ids=["d10", "d20"])
    await update_tag_query(PROJECT, "add", "codes", "c1", ids=["d10", "d11"])
    await delete_document(PROJECT, "d6")
    await delete_query(PROJECT, queries={"q": "word6"})
    await delete_query(PROJECT, filters={"tags": FilterSpec(values=["politics"]), "i": FilterSpec(values=[7])})
    await upload(PROJECT, make_docs(start, 30))
    # a deleted document id can be used again, and is then a new document
    await upload(PROJECT, [{"_id": "d6", "text": "common again", "tags": ["art"]}])


async def assert_live_is_clean() -> None:
    """A query on the current documents never returns an old row version: every document once"""
    total, docs = await _docs(None, per_page=1000)
    ids = [d["_id"] for d in docs]
    assert total == len(ids) == len(set(ids))
    pk = await project_pk(PROJECT)
    row = await fetch_one("SELECT count(*) AS n FROM documents WHERE project_pk = %s AND retired_seq = %s", [pk, CURRENT])
    assert row and row["n"] == total
    for word in ["common", "word3", "rare"]:
        total, hits = await _docs(None, queries={"q": word})
        assert total == len(hits) == len({d["_id"] for d in hits})
    assert sum(r["n"] for r in await _agg(None, [Axis("kw")])) == len(ids)


async def _vacuum() -> None:
    partitions = await project_partitions([await project_pk(PROJECT)])
    async with connection() as conn:
        for pid in partitions.values():
            await conn.execute(f"VACUUM (ANALYZE) {partition_table(pid)}")  # type: ignore[arg-type]
        await conn.execute("VACUUM (ANALYZE) doc_labels")


async def test_release_is_frozen(project):
    """The gate: a release gives the same results after every kind of change, VACUUM, and the purge for another one"""
    before = await snapshot()
    assert before["all"][0] == N and before["filter_tag"][0] > 0 and before["agg_tags"]
    r1 = await create_release(project, created_by="me@example.com")
    assert r1["number"] == 1 and r1["manifest"]["documents"] == N and r1["created_by"] == "me@example.com"
    assert r1["manifest"]["fields"]["text"]["documents"] == N
    assert r1["manifest"]["fields"]["tags"]["documents"] == before["tag_exists"]
    # the release and the current documents are the same until something changes
    assert_same(await snapshot(1), before)

    await change_everything(N)
    after1 = await snapshot()
    assert after1["all"] != before["all"] and after1["agg_tags"] != before["agg_tags"]
    await assert_live_is_clean()
    assert_same(await snapshot(1), before)
    assert (await verify_release(project, 1))["ok"]

    # a second release, and more changes: both stay what they were
    r2 = await create_release(project)
    assert r2["number"] == 2 and r2["seq"] > r1["seq"] and r2["manifest"]["documents"] == after1["all"][0]
    assert_same(await snapshot(2), after1)
    await change_everything(N + 100)
    await update_document(project, "d1", {"text": "changed once more"})
    after2 = await snapshot()
    await assert_live_is_clean()
    assert_same(await snapshot(1), before)
    assert_same(await snapshot(2), after1)

    await _vacuum()
    assert_same(await snapshot(1), before)
    assert_same(await snapshot(2), after1)
    assert_same(await snapshot(), after2)
    for number in (1, 2):
        assert (await verify_release(project, number))["ok"]

    # delete release 1 and purge what only it needed: release 2 and the current documents do not change
    pk = await project_pk(project)
    versions = (await fetch_one("SELECT count(*) AS n FROM documents WHERE project_pk = %s", [pk]))["n"]  # type: ignore[index]
    await delete_release(project, 1)
    assert await run_pending_jobs() == 1
    left = (await fetch_one("SELECT count(*) AS n FROM documents WHERE project_pk = %s", [pk]))["n"]  # type: ignore[index]
    assert left < versions
    with pytest.raises(NotFoundError):
        await snapshot(1)
    assert_same(await snapshot(2), after1)
    assert_same(await snapshot(), after2)
    assert (await verify_release(project, 2))["ok"]
    await _vacuum()
    assert_same(await snapshot(2), after1)
    assert await purge(pk) == {"documents": 0, "labels": 0}  # nothing more to remove
    # every row version that is left is current or needed by release 2
    rows = await fetch_all(
        "SELECT created_seq, retired_seq FROM documents WHERE project_pk = %s AND retired_seq <> %s", [pk, CURRENT]
    )
    assert rows and all(r["created_seq"] <= r2["seq"] < r["retired_seq"] for r in rows)
    rows = await fetch_all("SELECT created_seq, retired_seq FROM doc_labels WHERE project_pk = %s", [pk])
    assert all(r["retired_seq"] == CURRENT or r["created_seq"] <= r2["seq"] < r["retired_seq"] for r in rows)


async def test_no_release_no_versions(project):
    """Without a release every update and delete is in place, as before: the table does not grow"""
    pk = await project_pk(project)
    await change_everything(N)
    rows = await fetch_all("SELECT retired_seq, count(*) AS n FROM documents WHERE project_pk = %s GROUP BY 1", [pk])
    assert [r["retired_seq"] for r in rows] == [CURRENT]
    assert await fetch_all("SELECT 1 FROM doc_labels WHERE project_pk = %s AND retired_seq <> %s", [pk, CURRENT]) == []
    await assert_live_is_clean()


async def test_only_released_rows_are_versioned(project):
    """A document made or already changed after the newest release is changed in place: one old version at most"""
    pk = await project_pk(project)
    await create_release(project)
    await upload(project, [{"_id": "new", "text": "new document"}])
    for k in range(3):
        await update_document(project, "new", {"text": f"new document {k}"})
        await update_document(project, "d1", {"text": f"old document {k}"})
    rows = await fetch_all(
        "SELECT doc_id, count(*) AS n FROM documents WHERE project_pk = %s AND doc_id IN ('new', 'd1') GROUP BY 1", [pk]
    )
    assert {r["doc_id"]: r["n"] for r in rows} == {"new": 1, "d1": 2}
    assert (await fetch_document(project, "d1"))["text"] == "old document 2"
    total, docs = await _docs(1, filters={"_id": FilterSpec(values=["d1", "new"])})
    assert total == 1 and docs[0]["text"].startswith("common word1")
    await delete_document(project, "new")
    await delete_document(project, "d1")
    rows = await fetch_all(
        "SELECT doc_id, retired_seq = %s AS live FROM documents WHERE project_pk = %s AND doc_id IN ('new', 'd1')",
        [CURRENT, pk],
    )
    assert rows == [{"doc_id": "d1", "live": False}]  # (the two versions of d1 that release 1 does not need are gone)
    with pytest.raises(NotFoundError):
        await fetch_document(project, "d1")
    assert (await _docs(1, filters={"_id": FilterSpec(values=["d1"])}))[0] == 1


async def test_unique_fields_and_ids_with_versions(index, monkeypatch):
    """Document ids and unique fields stay unique among current documents; old versions do not block them"""
    await upload(
        index,
        [{"_id": "a", "url": "u1", "text": "one"}, {"_id": "b", "url": "u2", "text": "two"}],
        fields={"url": CreateDocumentField(type="keyword", unique=True), "text": "text"},
    )
    await create_release(index)
    with pytest.raises(storage.UploadError):
        await create_or_update_documents(index, [{"_id": "c", "url": "u1", "text": "x"}], op_type="create")
    await update_document(index, "a", {"url": "u3"})
    # u1 is free again (only an old version has it), and the old id can be used again after a delete
    await create_or_update_documents(index, [{"_id": "c", "url": "u1", "text": "three"}], op_type="create")
    await delete_document(index, "b")
    await create_or_update_documents(index, [{"_id": "b", "url": "u2", "text": "two again"}], op_type="create")
    with pytest.raises(storage.UploadError):
        await create_or_update_documents(index, [{"_id": "d", "url": "u3", "text": "x"}], op_type="create")
    result = await query_documents(index, fields=[FieldSpec(name="url")], per_page=10)
    assert sorted((d["_id"], d["url"]) for d in result.data) == [("a", "u3"), ("b", "u2"), ("c", "u1")]
    result = await query_documents(index, fields=[FieldSpec(name="url"), FieldSpec(name="text")], per_page=10, release=1)
    assert sorted((d["_id"], d["url"], d["text"]) for d in result.data) == [("a", "u1", "one"), ("b", "u2", "two")]
    assert (await verify_release(index, 1))["ok"]


async def test_release_api_and_limits(project, monkeypatch):
    """Listing, DOI, and what is refused for a project with releases"""
    assert await list_releases(project) == []
    with pytest.raises(NotFoundError):
        await get_release(project, 1)
    monkeypatch.setattr(get_settings(), "versioned_writes", False)
    with pytest.raises(ValueError, match="versioned writes"):
        await create_release(project)
    monkeypatch.setattr(get_settings(), "versioned_writes", True)
    await create_release(project)
    await create_release(project, doi="10.1234/test")
    assert [(r["number"], r["doi"]) for r in await list_releases(project)] == [(1, None), (2, "10.1234/test")]
    assert (await set_release_doi(project, 1, "10.1234/one"))["doi"] == "10.1234/one"
    with pytest.raises(ConflictError, match="DOI"):
        await delete_release(project, 1)
    # the field definitions are not versioned: deleting a field or changing its type would change the release
    with pytest.raises(ConflictError):
        await delete_fields(project, ["kw"])
    with pytest.raises(ConflictError):
        await update_fields(project, {"kw": UpdateDocumentField(type="text")})
    # renaming is fine: values are stored by field key
    await update_fields(project, {"kw": UpdateDocumentField(name="category")})
    result = await query_documents(project, fields=[FieldSpec(name="category")], per_page=1, release=1)
    assert result.data[0]["category"].startswith("cat")
    assert (await verify_release(project, 1))["ok"]
    # a release query is for one project, and not for vectors
    with pytest.raises(ValueError, match="one project"):
        await query_documents([project, project], release=1)
    with pytest.raises(NotFoundError):
        await query_documents(project, release=7)
    assert await field_values(project, "tags", 10)


async def test_verify_detects_a_changed_release(project):
    """verify_release notices when rows of a release were removed behind its back"""
    pk = await project_pk(project)
    await create_release(project)
    assert (await verify_release(project, 1))["ok"]
    async with connection() as conn:
        await conn.execute("DELETE FROM documents WHERE project_pk = %s AND doc_id = 'd3'", [pk])
    check = await verify_release(project, 1)
    assert not check["ok"] and "documents" in check["differences"] and "fingerprint" in check["differences"]


async def test_versioned_writes_off_updates_in_place(project, monkeypatch):
    """AMCAT4_VERSIONED_WRITES=false: in place, as in the original (documented: releases are then not protected)"""
    pk = await project_pk(project)
    await create_release(project)
    monkeypatch.setattr(get_settings(), "versioned_writes", False)
    await update_document(project, "d1", {"text": "changed in place"})
    await delete_document(project, "d2")
    row = await fetch_one("SELECT count(*) AS n, max(retired_seq) AS r FROM documents WHERE project_pk = %s", [pk])
    assert row == {"n": N - 1, "r": CURRENT}


async def test_release_with_concurrent_writers(project):
    """
    Two writers and a release at the same time. The release waits for a write that has started (and has a lower
    number), and a write that starts after the release waits for it and gets a higher number: the release contains
    the first write completely and nothing of the second.
    """
    pk = await project_pk(project)
    infos = await field_infos(project)
    first_started, first_may_commit = asyncio.Event(), asyncio.Event()

    async def first_writer():
        async with connection() as conn, conn.transaction():
            await storage.upload_documents(conn, pk, [{"_id": f"first{k}", "text": "first writer"} for k in range(20)], infos)
            await storage.upload_documents(conn, pk, [{"_id": "d1", "text": "changed by the first writer"}], infos, "update")
            first_started.set()
            await first_may_commit.wait()

    async def second_writer():
        await upload(project, [{"_id": f"second{k}", "text": "second writer"} for k in range(20)])
        await update_document(project, "d2", {"text": "changed by the second writer"})

    first = asyncio.create_task(first_writer())
    await first_started.wait()
    release = asyncio.create_task(create_release(project))
    await asyncio.sleep(0.5)
    assert not release.done()  # waits for the first writer, which has not committed
    second = asyncio.create_task(second_writer())
    await asyncio.sleep(0.5)
    assert not second.done() and not release.done()  # the second writer waits behind the release
    first_may_commit.set()
    await asyncio.gather(first, release, second)

    r = release.result()
    assert r["manifest"]["documents"] == N + 20
    total, docs = await _docs(1, per_page=1000)
    ids = {d["_id"] for d in docs}
    assert total == N + 20 and {f"first{k}" for k in range(20)} <= ids and not any(i.startswith("second") for i in ids)
    by_id = {d["_id"]: d for d in docs}
    assert by_id["d1"]["text"] == "changed by the first writer" and by_id["d2"]["text"].startswith("common")
    rows = await fetch_all(
        "SELECT doc_id, created_seq FROM documents"
        " WHERE project_pk = %s AND retired_seq = %s AND doc_id ~ '^(first|second|d2$)'",
        [pk, CURRENT],
    )
    assert all((row["created_seq"] < r["seq"]) == row["doc_id"].startswith("first") for row in rows)
    assert (await _docs(None, per_page=1))[0] == N + 40
    assert (await verify_release(project, 1))["ok"]
    await assert_live_is_clean()


async def test_vectors_follow_the_current_version(index):
    """The current row of a document gets a new id when a released version is updated: its vector goes with it"""
    await upload(
        index,
        [{"_id": "a", "text": "one", "emb": [1.0, 0.0]}, {"_id": "b", "text": "two", "emb": [0.0, 1.0]}],
        fields={"text": "text", "emb": "vector"},
    )
    await create_release(index)
    await update_document(index, "a", {"text": "one changed"})
    result = await query_documents(index, fields=[FieldSpec(name="text")], similar=("emb", [1.0, 0.1]), per_page=2)
    assert [(d["_id"], d["text"]) for d in result.data] == [("a", "one changed"), ("b", "two")]
    assert (await fetch_document(index, "a"))["emb"] == [1.0, 0.0]
    with pytest.raises(ValueError, match="Vectors"):
        await query_documents(index, similar=("emb", [1.0, 0.1]), release=1)
    result = await query_documents(index, fields=[FieldSpec(name="text")], release=1, sort=[{"_seq": {"order": "asc"}}])
    assert [(d["_id"], d["text"]) for d in result.data] == [("a", "one"), ("b", "two")]


async def test_release_scroll_is_stable(project):
    """A download of a release (cursor on _seq) gives every document once, also when documents change during it"""
    await create_release(project)
    seen: list[str] = []
    after = None
    while True:
        result = await query_documents(project, per_page=50, release=1, sort=[{"_seq": {"order": "asc"}}], after=after)
        seen += [d["_id"] for d in result.data]
        await update_query(project, "kw", f"round{len(seen)}", filters={"i": FilterSpec(values=[len(seen) // 50])})
        await delete_document(project, f"d{len(seen) - 1}", ignore_missing=True)
        after = result.next
        if after is None:
            break
    assert seen == [f"d{k}" for k in range(N)]


async def test_releases_http_api(client, project, admin, reader):
    """The endpoints: create (project admin only), list, get, DOI, verify, query and aggregate a release, delete"""
    from tests.tools import adelete, get_json, post_json, put_json

    await post_json(client, f"/index/{project}/releases", expected=403, user=reader)
    r = await post_json(client, f"/index/{project}/releases", expected=201, user=admin, json={})
    assert r["number"] == 1 and r["created_by"] == admin and r["manifest"]["documents"] == N
    await update_document(project, "d1", {"text": "changed"})
    await delete_document(project, "d2")
    assert [x["number"] for x in await get_json(client, f"/index/{project}/releases", user=admin)] == [1]
    assert (await get_json(client, f"/index/{project}/releases/1", user=admin))["seq"] == r["seq"]
    await get_json(client, f"/index/{project}/releases/2", expected=404, user=admin)
    assert (await get_json(client, f"/index/{project}/releases/1/verify", user=admin))["ok"] is True
    body = {"filters": {"_id": ["d1", "d2"]}, "fields": ["text"], "release": 1}
    res = await post_json(client, f"/index/{project}/query", expected=200, user=admin, json=body)
    assert res["meta"]["total_count"] == 2 and all(d["text"].startswith("common") for d in res["results"])
    res = await post_json(client, f"/index/{project}/query", expected=200, user=admin, json={**body, "release": None})
    assert [d["text"] for d in res["results"]] == ["changed"]
    res = await post_json(
        client, f"/index/{project}/aggregate", expected=200, user=admin, json={"axes": [{"field": "kw"}], "release": 1}
    )
    assert sum(row["n"] for row in res["data"]) == N
    await post_json(client, "/query", expected=400, user=admin, json={"projects": [project, project], "release": 1})
    url = f"/index/{project}/releases/1"
    assert (await put_json(client, url, user=admin, json={"doi": "10.1/x"}))["doi"] == "10.1/x"
    await adelete(client, url, expected=409, user=admin)
    await put_json(client, url, user=admin, json={"doi": None})
    assert (await adelete(client, url, expected=200, user=admin))["job"]
    assert await run_pending_jobs() == 1
    assert await get_json(client, f"/index/{project}/releases", user=admin) == []
