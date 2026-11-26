"""
The views for the SPARQL endpoint (migration 0007, deploy/ontop): only public releases of existing projects, only fields
that are not text and that metareaders may see, and a role (deploy/ontop/role.sql) that can read these views and
nothing else.
"""

import psycopg
import pytest

from amcat4.connections import db_schema
from amcat4.postgres.connection import fetch_all
from amcat4.projects.index import delete_project_index
from tests.sparql_tools import (
    BASE_IRI,
    PRIVATE,
    PUBLIC,
    SECRET_WORD,
    connect_as,
    create_role,
    drop_projects,
    drop_role,
    make_projects,
    release_row,
)

pytestmark = pytest.mark.anyio

VIEWS = {
    "sparql_projects": ["project_iri", "project_id", "project_name", "release_count"],
    "sparql_releases": [
        "release_iri",
        "project_iri",
        "project_id",
        "project_name",
        "release_number",
        "created_at",
        "doi",
        "doi_iri",
        "doi_status",
        "withdrawn_at",
        "withdrawn",
        "metadata_sha256",
        "document_count",
    ],
    "sparql_release_fields": [
        "release_iri",
        "field_iri",
        "field_name",
        "field_type",
        "is_identifier",
        "description",
        "field_position",
        "document_count",
    ],
    "sparql_field_values": ["release_iri", "field_iri", "value_iri", "value", "document_count", "value_rank"],
}


@pytest.fixture
async def projects():
    await make_projects()
    yield
    await drop_projects()


async def test_views_exist():
    rows = await fetch_all(
        """SELECT table_name, array_agg(column_name::text ORDER BY ordinal_position) AS columns
           FROM information_schema.columns WHERE table_schema = %s AND table_name LIKE 'sparql\\_%%' GROUP BY 1""",
        [db_schema()],
    )
    assert {row["table_name"]: row["columns"] for row in rows} == VIEWS
    # the views read releases and projects only
    rows = await fetch_all(
        """SELECT DISTINCT v.relname AS view, t.relname AS used FROM pg_depend d
           JOIN pg_rewrite rw ON rw.oid = d.objid JOIN pg_class v ON v.oid = rw.ev_class
           JOIN pg_class t ON t.oid = d.refobjid
           WHERE v.relname LIKE 'sparql\\_%%' AND v.relnamespace = %s::regnamespace AND t.oid <> v.oid""",
        [db_schema()],
    )
    assert {row["used"] for row in rows} == {"releases", "projects"}


async def test_public_and_private_releases(projects):
    rows = await fetch_all("SELECT * FROM sparql_releases ORDER BY project_id, release_number")
    assert [(r["project_id"], r["release_number"]) for r in rows] == [(PUBLIC, 1), (PUBLIC, 2)]
    first, second = rows
    assert first["release_iri"] == f"urn:amcat4:projects/{PUBLIC}/releases/1"  # (no base IRI set in this session)
    assert first["project_iri"] == f"urn:amcat4:projects/{PUBLIC}"
    assert first["project_name"] == f"Project {PUBLIC}"
    assert first["document_count"] == (await release_row(PUBLIC, 1))["manifest"]["documents"] == 12
    assert second["document_count"] == 15
    assert first["metadata_sha256"] == "ab" * 32
    assert (first["doi"], first["doi_iri"], first["doi_status"], first["withdrawn"]) == (None, None, "none", False)
    assert second["doi_iri"] == "https://doi.org/10.5555/amcat.test.2"
    assert second["withdrawn"] and second["withdrawn_at"] is not None
    rows = await fetch_all("SELECT project_id, project_name, release_count FROM sparql_projects")
    assert rows == [{"project_id": PUBLIC, "project_name": f"Project {PUBLIC}", "release_count": 2}]
    # the project of a public release is deleted: its releases go with it
    await delete_project_index(PUBLIC)
    assert await fetch_all("SELECT * FROM sparql_releases") == []
    assert await fetch_all("SELECT * FROM sparql_projects") == []


async def test_fields_hide_text_and_hidden_fields(projects):
    rows = await fetch_all("SELECT * FROM sparql_release_fields ORDER BY release_iri, field_name")
    by_release: dict = {}
    for row in rows:
        by_release.setdefault(row["release_iri"], {})[row["field_name"]] = row
    assert set(by_release) == {f"urn:amcat4:projects/{PUBLIC}/releases/1", f"urn:amcat4:projects/{PUBLIC}/releases/2"}
    manifest = (await release_row(PUBLIC, 1))["manifest"]
    fields = by_release[f"urn:amcat4:projects/{PUBLIC}/releases/1"]
    # text and title are text fields, secret_kw cannot be seen by metareaders
    assert set(fields) == {"kw", "date", "tags"}
    assert {name: f["field_type"] for name, f in fields.items()} == {"kw": "keyword", "date": "date", "tags": "tag"}
    for name, f in fields.items():
        assert f["document_count"] == manifest["fields"][name]["documents"]
        assert f["field_iri"] == f"urn:amcat4:projects/{PUBLIC}/releases/1/fields/{name}"
    assert fields["kw"]["document_count"] == 12 and fields["tags"]["document_count"] == 6
    assert not any(f["is_identifier"] for f in fields.values())


async def test_top_values_from_golden_queries(projects):
    rows = await fetch_all(
        "SELECT * FROM sparql_field_values WHERE release_iri LIKE %s ORDER BY field_iri, value_rank", ["%/releases/1"]
    )
    by_field: dict = {}
    for row in rows:
        by_field.setdefault(row["field_iri"].rsplit("/", 1)[1], []).append((row["value"], row["document_count"]))
    # (secret_kw is a keyword field too, but metareaders cannot see it)
    assert by_field == {"kw": [("cat0", 4), ("cat1", 4), ("cat2", 4)], "tags": [("a", 6), ("b", 6)]}
    assert rows[0]["value_iri"] == rows[0]["field_iri"] + "/values/1"


async def test_nothing_secret(projects):
    # nothing in any view contains the word of the texts and of the hidden field
    for view in VIEWS:
        rows = await fetch_all(f"SELECT t::text AS row FROM {view} t")  # type: ignore[arg-type]
        assert rows and not [r for r in rows if SECRET_WORD in r["row"]]


async def test_role_reads_views_only(projects):
    role = create_role(password="test-password")
    try:
        conn = await connect_as(role)
        try:
            cur = await conn.execute("SELECT release_iri FROM sparql_releases ORDER BY 1")
            assert [r["release_iri"] for r in await cur.fetchall()] == [
                f"{BASE_IRI}projects/{PUBLIC}/releases/1",  # (role.sql set the base IRI for the role)
                f"{BASE_IRI}projects/{PUBLIC}/releases/2",
            ]
            cur = await conn.execute("SELECT count(*) AS n FROM sparql_release_fields")
            assert (await cur.fetchone())["n"] == 6  # type: ignore[index]
            for table in ["documents", "documents_p1", "doc_labels", "releases", "projects", "fields", "roles", "api_keys"]:
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    await conn.execute(f"SELECT 1 FROM {table} LIMIT 1")  # type: ignore[arg-type]
            # read only, with a statement timeout
            with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
                await conn.execute("CREATE TEMP TABLE x (i int)")
            cur = await conn.execute("SHOW statement_timeout")
            assert (await cur.fetchone())["statement_timeout"] == "10s"  # type: ignore[index]
            # the private release is not there, also not with a condition that the view must not see through
            cur = await conn.execute("SELECT count(*) AS n FROM sparql_releases WHERE project_id = %s", [PRIVATE])
            assert (await cur.fetchone())["n"] == 0  # type: ignore[index]
        finally:
            await conn.close()
        # applying it again (e.g. after a migration) works
        create_role(password="test-password", role=role)
        await (await connect_as(role)).close()
        # without a password the role cannot log in
        other = create_role()
        try:
            with pytest.raises(psycopg.OperationalError):
                await (await connect_as(other)).close()
        finally:
            await drop_role(other)
    finally:
        await drop_role(role)
