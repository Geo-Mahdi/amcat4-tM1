"""
The frozen metadata of a release (migration 0006, amcat4.projects.release_metadata): made once at creation, never
changed by later writes, verifiable (SHA-256 and golden queries), without document text; and the live columns
(public flag, withdrawn, DOI status) with the /metadata and /fields endpoints.
"""

import hashlib
import json
from typing import Any

import pytest

from amcat4.config import AuthOptions, get_settings
from amcat4.models import FieldSpec, FilterSpec, ProjectSettings, Roles
from amcat4.postgres.connection import connection, fetch_one
from amcat4.postgres.projects import project_pk
from amcat4.projects.documents import create_or_update_documents, delete_document, update_document
from amcat4.projects.index import create_project_index, delete_project_index
from amcat4.projects.query import delete_query, query_documents, update_query, update_tag_query
from amcat4.projects.release_metadata import canonical_json, sha256_hex
from amcat4.projects.releases import create_release, get_release_frozen, update_release, verify_release
from amcat4.systemdata.roles import set_project_guest_role
from tests.conftest import upload
from tests.tools import get_json, put_json, set_auth

pytestmark = pytest.mark.anyio

PROJECT = "amcat4_unittest_release_metadata"
FIELDS = {"title": "text", "text": "text", "kw": "keyword", "i": "integer", "date": "date", "tags": "tag"}
SECRET = "zyxwquorbleflux"  # a word that only occurs in a document text


def make_docs(start: int, n: int) -> list[dict[str, Any]]:
    return [
        {
            "_id": f"d{k}",
            "title": f"Title {k}",
            "text": f"some text {k}" + (f" {SECRET}" if k == 3 else ""),
            "kw": f"cat{k % 3}",
            "i": k % 5,
            "date": f"2020-01-{1 + k % 28:02d}",
            **({"tags": ["a", "b"] if k % 2 else ["a"]} if k % 4 else {}),
        }
        for k in range(start, start + n)
    ]


@pytest.fixture(params=[False, True], ids=["tags_in_json", "label_tables"])
async def project(request, monkeypatch):
    monkeypatch.setattr(get_settings(), "label_tables", request.param)
    await delete_project_index(PROJECT, ignore_missing=True)
    await create_project_index(ProjectSettings(id=PROJECT, name="Metadata test", description="A test project"))
    await upload(PROJECT, make_docs(0, 40), fields=FIELDS)  # type: ignore[arg-type]
    yield PROJECT
    await delete_project_index(PROJECT, ignore_missing=True)


async def _frozen(number: int = 1) -> dict:
    row = await get_release_frozen(PROJECT, number)
    return {k: row[k] for k in ("metadata_jsonld", "metadata_sha256", "versions", "field_definitions", "golden_queries")}


async def test_creation_fills_all_columns(project):
    r = await create_release(project, created_by="someone@example.com")
    row = await get_release_frozen(project, r["number"])
    assert row["metadata_public"] is False and row["doi_status"] == "none" and row["withdrawn_at"] is None
    doc = row["metadata_jsonld"]
    assert doc["@type"] == ["Dataset", "dcat:Dataset"]
    assert {"dcat", "dct", "prov"} <= set(doc["@context"])
    assert doc["@id"] == f"{get_settings().host}/index/{project}/releases/1"
    assert doc["version"] == "1" and doc["description"] == "A test project" and doc["alternateName"] == "Metadata test"
    assert doc["isPartOf"]["@id"] == f"{get_settings().host}/index/{project}"
    assert doc["size"]["value"] == 40
    assert "creator" not in doc  # (an email address is left out)
    assert not any(i["propertyID"] == "doi" for i in doc["identifier"])
    variables = {v["name"]: v for v in doc["variableMeasured"]}
    assert set(variables) == set(FIELDS)
    assert variables["kw"]["additionalType"] == "keyword" and variables["kw"]["value"] == 40
    assert variables["tags"]["value"] == 30
    assert doc["conditionsOfAccess"].startswith("The documents are not public")
    v = row["versions"]
    assert v["postgres"].startswith("18") and v["pg_search"] and v["amcat4"] and v["migration"] == "0007"
    defs = {f["name"]: f for f in row["field_definitions"]}
    assert set(defs) == set(FIELDS) and defs["kw"]["type"] == "keyword" and defs["kw"]["identifier"] is False
    assert all("client_settings" not in f and "access" in f["metareader"] for f in defs.values())
    golden = {g["name"]: g for g in row["golden_queries"]}
    assert set(golden) == {"total", "top_values:kw", "top_values:tags", "date_range:date"}
    assert golden["total"]["result"]["count"] == 40 and len(golden["total"]["result"]["first_ids"]) == 10
    assert golden["total"]["query"] == {"release": 1, "sort": [{"_seq": {"order": "asc"}}], "per_page": 10}
    assert golden["top_values:kw"]["result"]["values"] == [
        {"value": "cat0", "n": 14},
        {"value": "cat1", "n": 13},
        {"value": "cat2", "n": 13},
    ]
    assert golden["top_values:tags"]["result"]["values"] == [{"value": "a", "n": 30}, {"value": "b", "n": 20}]
    dr = golden["date_range:date"]["result"]
    assert dr["count"] == 40 and dr["min"].startswith("2020-01-01") and dr["max"].startswith("2020-01-28")
    # first_ids are the doc_pk of the first documents in _seq order
    pk = await project_pk(project)
    first = await fetch_one(
        "SELECT coalesce(doc_pk, id) AS doc_pk FROM documents WHERE project_pk = %s AND doc_id = 'd0'", [pk]
    )
    assert golden["total"]["result"]["first_ids"][0] == first["doc_pk"]  # type: ignore[index]
    # the hash, also in the manifest
    assert row["metadata_sha256"] == hashlib.sha256(canonical_json(doc)).hexdigest() == r["metadata_sha256"]
    assert row["manifest"]["metadata_sha256"] == row["metadata_sha256"]


def test_canonical_json():
    assert canonical_json({"b": 1, "a": [True, None, "é"]}) == '{"a":[true,null,"é"],"b":1}'.encode("utf-8")
    assert sha256_hex({"b": 1, "a": 2}) == sha256_hex({"a": 2, "b": 1})


async def test_no_text_in_metadata(project):
    await create_release(project)
    hits = await query_documents(project, queries={"q": SECRET})
    assert hits.total_count == 1  # (the word is in the release)
    frozen = await _frozen()
    for key in ("metadata_jsonld", "field_definitions", "golden_queries"):
        text = json.dumps(frozen[key])
        assert SECRET not in text and "some text" not in text and "Title 1" not in text, key


async def test_metadata_unchanged_by_writes(project):
    await create_release(project)
    before = await _frozen()
    await upload(project, make_docs(100, 10))
    await update_document(project, "d1", {"text": "changed", "kw": "new"})
    await create_or_update_documents(project, [{"_id": "d2", "text": "replaced"}])
    await update_query(project, "kw", "moved", filters={"i": FilterSpec(values=[2])})
    await update_tag_query(project, "add", "tags", "c", filters={"kw": FilterSpec(values=["cat1"])})
    await update_tag_query(project, "remove", "tags", "a", ids=["d5", "d6"])
    await delete_document(project, "d7")
    await delete_query(project, filters={"i": FilterSpec(values=[4])})
    assert await _frozen() == before
    check = await verify_release(project, 1)
    assert check["ok"], check["differences"]
    assert [g["ok"] for g in check["golden_queries"]] == [True] * 4
    assert check["metadata"]["ok"]
    # a second release sees the changes, the first stays as it was
    await create_release(project)
    second = await _frozen(2)
    assert second["metadata_sha256"] != before["metadata_sha256"]
    assert second["golden_queries"][0]["result"]["count"] != before["golden_queries"][0]["result"]["count"]
    assert await _frozen() == before


async def test_verify_detects_tampering(project):
    await create_release(project)
    assert (await verify_release(project, 1))["ok"]
    pk = await project_pk(project)
    async with connection() as conn:
        await conn.execute(
            "UPDATE releases SET metadata_jsonld = jsonb_set(metadata_jsonld, '{name}', '\"other\"') WHERE project_pk = %s",
            [pk],
        )
    check = await verify_release(project, 1)
    assert not check["ok"] and check["differences"] == ["metadata_sha256"]
    assert check["metadata"]["computed_sha256"] != check["metadata"]["stored_sha256"]
    # a changed golden result: the hash in the manifest no longer matches, and the rerun differs
    async with connection() as conn:
        await conn.execute(
            """UPDATE releases SET metadata_jsonld = metadata_jsonld - 'name' || jsonb_build_object('name', %s::text),
                      golden_queries = jsonb_set(golden_queries, '{0,result,count}', '41')
               WHERE project_pk = %s""",
            ["Metadata test, release 1", pk],
        )
    check = await verify_release(project, 1)
    assert check["differences"] == ["golden_queries_sha256", "golden:total"]
    total = check["golden_queries"][0]
    assert total["stored"]["count"] == 41 and total["computed"]["count"] == 40


async def test_rename_field_keeps_golden_queries(project):
    from amcat4.models import UpdateDocumentField
    from amcat4.systemdata.fields import update_fields

    await create_release(project)
    await update_fields(project, {"kw": UpdateDocumentField(name="category")})
    assert (await verify_release(project, 1))["ok"]


async def test_old_release_without_metadata(project):
    """A release from before migration 0006 (no frozen metadata) verifies as before"""
    await create_release(project)
    pk = await project_pk(project)
    async with connection() as conn:
        await conn.execute(
            """UPDATE releases SET metadata_jsonld = NULL, metadata_sha256 = NULL, versions = NULL,
                      field_definitions = NULL, golden_queries = NULL, manifest = manifest - 'metadata_sha256'
               WHERE project_pk = %s""",
            [pk],
        )
    check = await verify_release(project, 1)
    assert check["ok"] and check["metadata"] is None and check["golden_queries"] == []


async def test_withdraw_keeps_rows(project):
    await create_release(project)
    await update_document(project, "d1", {"text": "changed"})
    r = await update_release(project, 1, withdrawn=True, withdrawn_reason="wrong data")
    assert r["withdrawn_at"] is not None and r["withdrawn_reason"] == "wrong data"
    result = await query_documents(
        project, fields=[FieldSpec(name="text")], release=1, filters={"_id": FilterSpec(values=["d1"])}
    )
    assert result.data[0]["text"] == "some text 1"
    assert (await query_documents(project, release=1)).total_count == 40
    assert (await verify_release(project, 1))["ok"]
    r = await update_release(project, 1, withdrawn=False)
    assert r["withdrawn_at"] is None and r["withdrawn_reason"] is None
    with pytest.raises(ValueError):
        await update_release(project, 1, doi_status="published")


async def test_metadata_api(client, project, admin, reader):
    await create_release(project, created_by="the server")
    url = f"/index/{project}/releases/1"
    with set_auth(AuthOptions.allow_authenticated_guests):
        r = await client.get(f"{url}/metadata")
        assert r.status_code == 401
        await get_json(client, f"{url}/metadata", expected=403, user=reader)  # (reader of the server, not the project)
        await get_json(client, f"{url}/fields", expected=401)
        doc = await get_json(client, f"{url}/metadata", user=admin)
        assert doc["creator"] == {"name": "the server"} and doc["creativeWorkStatus"] == "Published"
        # public: anyone, also without login
        updated = await put_json(client, url, user=admin, json={"metadata_public": True})
        assert updated["metadata_public"] is True and updated["doi"] is None
        r = await client.get(f"{url}/metadata")
        assert r.status_code == 200 and r.headers["content-type"].startswith("application/ld+json")
        public = r.json()
        assert {k: v for k, v in public.items() if k not in ("additionalProperty",)} == {
            k: v for k, v in doc.items() if k not in ("additionalProperty",)
        }
        fields = await get_json(client, f"{url}/fields")
        assert {f["name"]: f["documents"] for f in fields}["tags"] == 30
        # the DOI and its status are live: added to the frozen metadata, which does not change
        sha = (await get_json(client, url, user=admin))["metadata_sha256"]
        await put_json(client, url, user=reader, expected=403, json={"doi_status": "draft"})
        updated = await put_json(client, url, user=admin, json={"doi": "10.1234/abc", "doi_status": "registered"})
        assert updated["doi"] == "10.1234/abc" and updated["doi_status"] == "registered"
        assert updated["metadata_sha256"] == sha and updated["metadata_public"] is True
        await put_json(client, url, user=admin, expected=422, json={"doi_status": "bogus"})
        doc = (await client.get(f"{url}/metadata")).json()
        assert doc["sameAs"] == "https://doi.org/10.1234/abc"
        assert doc["identifier"][0] == {
            "@type": "PropertyValue",
            "propertyID": "doi",
            "value": "10.1234/abc",
            "url": "https://doi.org/10.1234/abc",
        }
        assert {"@type": "PropertyValue", "name": "doiStatus", "value": "registered"} in doc["additionalProperty"]
        await put_json(client, url, user=admin, json={"withdrawn": True, "withdrawn_reason": "duplicate"})
        doc = (await client.get(f"{url}/metadata")).json()
        assert doc["creativeWorkStatus"] == "Withdrawn"
        assert {"@type": "PropertyValue", "name": "withdrawnReason", "value": "duplicate"} in doc["additionalProperty"]
        assert (await get_json(client, url, user=admin))["doi"] == "10.1234/abc"  # (not changed by the withdraw)
        # turtle
        r = await client.get(f"{url}/metadata", params={"format": "turtle"})
        assert r.status_code == 200 and r.headers["content-type"].startswith("text/turtle")
        assert "schema:Dataset" in r.text or "<https://schema.org/Dataset>" in r.text
        assert "dcat:Dataset" in r.text
        # private again
        await put_json(client, url, user=admin, json={"metadata_public": False})
        assert (await client.get(f"{url}/metadata")).status_code == 401
        # a guest role on the project that allows metadata: the usual rule
        await set_project_guest_role(project, Roles.METAREADER)
        with set_auth(AuthOptions.allow_guests):
            assert (await client.get(f"{url}/metadata")).status_code == 200
        # a release that does not exist looks the same as a private one to a user without access
        assert (await client.get(f"/index/{project}/releases/9/metadata")).status_code == 401
        await get_json(client, f"/index/{project}/releases/9/metadata", expected=404, user=admin)
