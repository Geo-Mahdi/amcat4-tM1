"""Shared set-up for the tests of the SPARQL views (test_sparql_views.py) and of the Ontop endpoint (test_sparql_ontop.py)"""

import secrets
import subprocess
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from amcat4.config import get_settings
from amcat4.connections import db_schema
from amcat4.models import CreateDocumentField, DocumentFieldMetareaderAccess, ProjectSettings
from amcat4.postgres.connection import connection, fetch_one
from amcat4.postgres.projects import project_pk
from amcat4.projects.index import create_project_index, delete_project_index
from amcat4.projects.releases import create_release
from tests.conftest import upload

ROLE_SQL = Path(__file__).parents[2] / "deploy" / "ontop" / "role.sql"
MAPPING = Path(__file__).parents[2] / "deploy" / "ontop" / "mapping.ttl"
PROPERTIES_EXAMPLE = Path(__file__).parents[2] / "deploy" / "ontop" / "ontop.properties.example"

PUBLIC = "amcat4_unittest_sparql_public"
PRIVATE = "amcat4_unittest_sparql_private"
SECRET_WORD = "zebracrossingsecret"  # occurs only in the texts and in a field that metareaders cannot see
BASE_IRI = "https://amcat.test/"

READ = DocumentFieldMetareaderAccess(access="read")
FIELDS = {
    "text": CreateDocumentField(type="text", metareader=DocumentFieldMetareaderAccess(access="snippet")),
    "title": CreateDocumentField(type="text", metareader=READ),  # text: never shown, also when metareaders can read it
    "kw": CreateDocumentField(type="keyword", metareader=READ),
    "date": CreateDocumentField(type="date", metareader=READ),
    "tags": CreateDocumentField(type="tag", metareader=READ),
    "secret_kw": CreateDocumentField(type="keyword"),  # metareader access none
}
N = 12


def docs(n: int = N) -> list[dict]:
    return [
        {
            "_id": f"d{k}",
            "text": f"a text with {SECRET_WORD} number {k}",
            "title": f"title {SECRET_WORD} {k}",
            "kw": f"cat{k % 3}",
            "date": f"2020-01-{1 + k:02d}",
            "secret_kw": SECRET_WORD,
            **({"tags": ["a", "b"]} if k % 2 else {}),
        }
        for k in range(n)
    ]


async def publish_release(index: str, number: int, public: bool = True, **columns) -> None:
    """
    Set the 0006 metadata of a release (as the release code of 0006 would): public or not, and the field definitions
    from the fields table if the release code did not store them itself
    """
    pk = await project_pk(index)
    sets = ", ".join(f"{k} = %s" for k in columns)
    async with connection() as conn:
        await conn.execute(
            f"""UPDATE releases SET metadata_public = %s, field_definitions = coalesce(field_definitions,
                    (SELECT jsonb_agg(jsonb_build_object('pk', pk, 'name', name, 'type', type, 'identifier', unique_field,
                                                         'metareader', metareader)
                                      ORDER BY name)
                     FROM fields WHERE project_pk = %s)) {", " + sets if sets else ""}
                WHERE project_pk = %s AND number = %s""",  # type: ignore[arg-type]
            [public, pk, *columns.values(), pk, number],
        )


async def make_projects() -> None:
    """Two projects with the same fields and documents: PUBLIC with two public releases (the second withdrawn and
    with a DOI), PRIVATE with a release whose metadata is not public"""
    await drop_projects()
    for index in (PUBLIC, PRIVATE):
        await create_project_index(ProjectSettings(id=index, name=f"Project {index}"))
        await upload(index, docs(), fields=FIELDS)  # type: ignore[arg-type]
    await create_release(PUBLIC)
    await upload(PUBLIC, docs(N + 3)[N:])
    await create_release(PUBLIC)
    await create_release(PRIVATE)
    await publish_release(PUBLIC, 1, metadata_sha256="ab" * 32)
    await publish_release(
        PUBLIC,
        2,
        doi="10.5555/amcat.test.2",
        doi_status="findable",
        withdrawn_at="2026-11-01T12:00:00+00:00",
        withdrawn_reason=f"reason {SECRET_WORD}",
    )
    await publish_release(PRIVATE, 1, public=False)


async def drop_projects() -> None:
    for index in (PUBLIC, PRIVATE):
        await delete_project_index(index, ignore_missing=True)


async def release_row(index: str, number: int) -> dict:
    row = await fetch_one(
        "SELECT number, doi, manifest FROM releases WHERE project_pk = %s AND number = %s", [await project_pk(index), number]
    )
    assert row is not None
    return row


def create_role(password: str | None = None, role: str | None = None) -> str:
    """Apply deploy/ontop/role.sql with psql to the test database, by default with a random role name. Returns it."""
    role = role or f"amcat_sparql_test_{secrets.token_hex(4)}"
    args = ["-v", f"schema={db_schema()}", "-v", f"role={role}", "-v", f"base_iri={BASE_IRI}"]
    if password:
        args += ["-v", f"password={password}"]
    subprocess.run(
        ["psql", get_settings().postgres_url, "-X", "-q", *args, "-f", str(ROLE_SQL)],
        check=True,
        capture_output=True,
        text=True,
    )
    return role


async def drop_role(role: str) -> None:
    async with await psycopg.AsyncConnection.connect(get_settings().postgres_url, autocommit=True) as conn:
        await conn.execute(f'DROP OWNED BY "{role}"')  # (the grants)
        await conn.execute(f'DROP ROLE IF EXISTS "{role}"')


async def connect_as(role: str) -> psycopg.AsyncConnection:
    """A connection as the role, as Ontop makes it (the role's own settings apply: search path, read only, base IRI)"""
    return await psycopg.AsyncConnection.connect(
        get_settings().postgres_url,
        user=role,
        autocommit=True,
        row_factory=dict_row,  # type: ignore[arg-type]
    )
