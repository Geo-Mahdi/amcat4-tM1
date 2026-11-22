"""
Migration 0002 on a database that has data from 0001: rows are kept, the new columns and indexes exist, the search
index is rebuilt with the slot columns, searching still works, and the downgrade gives back the 0001 schema.
Runs in its own schema, next to the test schema.
"""

import asyncio
from typing import Any

import psycopg
import pytest
from alembic import command
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from amcat4.config import get_settings
from amcat4.postgres.layout import create_partition, drop_schema
from amcat4.systemdata.manage import _alembic_config

SCHEMA = "amcat_unittest_migrations"


async def _connect() -> Any:
    return await psycopg.AsyncConnection.connect(
        get_settings().postgres_url,
        autocommit=True,
        row_factory=dict_row,  # type: ignore[arg-type]
        options=f"-c search_path={SCHEMA},public",
    )


def _migrate(direction: str, revision: str) -> None:
    config = _alembic_config()
    config.attributes["schema"] = SCHEMA
    getattr(command, direction)(config, revision)


async def _one(conn, query, params=None):
    cur = await conn.execute(query, params)
    return await cur.fetchone()


async def _bm25(conn, table: str) -> str:
    row = await _one(
        conn,
        """SELECT pg_get_indexdef(ix.indexrelid) AS def FROM pg_index ix JOIN pg_class c ON c.oid = ix.indexrelid
           JOIN pg_am am ON am.oid = c.relam AND am.amname = 'bm25' WHERE ix.indrelid = %s::regclass""",
        [table],
    )
    # without the index name, which differs (auto-generated in 0001, documents_p<N>_bm25 after a migration)
    return row["def"].split(" USING ", 1)[1]


async def _indexdefs(conn, **where) -> list[str]:
    """Definitions of the indexes in the migration schema, e.g. _indexdefs(conn, indexname="x")"""
    conditions = " AND ".join(f"{k} LIKE %s" for k in where)
    cur = await conn.execute(
        f"SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND {conditions}",  # type: ignore[arg-type]
        list(where.values()),
    )
    return [row["indexdef"] for row in await cur.fetchall()]


async def _columns(conn, table: str) -> set[str]:
    cur = await conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s", [SCHEMA, table]
    )
    return {row["column_name"] for row in await cur.fetchall()}


async def _search(conn, field_key: str, word: str) -> int:
    query = Jsonb({"match": {"field": f"text_fields.{field_key}", "value": word}})
    row = await _one(conn, "SELECT count(*) AS n FROM documents_p1 WHERE id @@@ %s::jsonb", [query])
    return row["n"]


@pytest.mark.anyio
async def test_migration_0002_with_data():
    conn = await _connect()
    try:
        await drop_schema(conn, SCHEMA)
        await asyncio.to_thread(_migrate, "upgrade", "0001")
        bm25_0001 = await _bm25(conn, "documents_p1")

        # A project with fields and documents, as 0001 stored them
        project = (await _one(conn, "INSERT INTO projects (id, partition_id) VALUES ('p', 1) RETURNING pk"))["pk"]
        text_pk = (
            await _one(conn, "INSERT INTO fields (project_pk, name, type) VALUES (%s, 'text', 'text') RETURNING pk", [project])
        )["pk"]
        await _one(
            conn,
            "INSERT INTO fields (project_pk, name, type, sort_slot) VALUES (%s, 'date', 'date', 'date') RETURNING pk",
            [project],
        )
        async with conn.cursor() as cur:
            await cur.executemany(
                """INSERT INTO documents (partition_id, project_pk, doc_id, text_fields, date, source)
                   VALUES (1, %s, %s, %s, %s, %s)""",
                [
                    (project, str(i), Jsonb({f"f{text_pk}": f"word{i % 3} text"}), f"2020-0{1 + i % 9}-01", f"s{i % 4}")
                    for i in range(300)
                ],
            )
        assert await _search(conn, f"f{text_pk}", "word1") == 100

        await asyncio.to_thread(_migrate, "upgrade", "head")
        assert (await _one(conn, "SELECT count(*) AS n FROM documents"))["n"] == 300
        assert {"sort_kw1", "sort_kw2", "sort_kw3", "sort_num1", "sort_date2"} <= await _columns(conn, "documents")
        assert {"sort_kw1", "sort_num1"} <= await _columns(conn, "documents_p1")
        assert {"sort_slot_ready", "quarter_ready"} <= await _columns(conn, "fields")
        # the existing date field still needs its quarter keys; other fields do not
        cur = await conn.execute("SELECT name, quarter_ready FROM fields ORDER BY name")
        assert {row["name"]: row["quarter_ready"] for row in await cur.fetchall()} == {"date": False, "text": True}

        bm25 = await _bm25(conn, "documents_p1")
        assert bm25 != bm25_0001
        for column in ["sort_kw1", "sort_kw2", "sort_kw3", "sort_num1", "sort_date2"]:
            assert column in bm25
        # every entry and option of 0001 is still there (the 0002 definition is 0001 plus the slots)
        for entry in bm25_0001.removeprefix("bm25 (").split(") WITH")[0].split(", "):
            assert entry in bm25
        assert bm25_0001.split(" WITH ")[1] == bm25.split(" WITH ")[1]
        assert len(await _indexdefs(conn, indexname="documents_p1_bm25")) == 1
        assert await _search(conn, f"f{text_pk}", "word1") == 100

        # the id index, on the parent and the partition
        [indexdef] = await _indexdefs(conn, indexname="documents_p1_project_id")
        assert "(partition_id, project_pk, id)" in indexdef
        # lz4 for new values of text_fields, also for new partitions
        await create_partition(conn, 2)
        cur = await conn.execute(
            """SELECT attrelid::regclass::text AS t, attcompression AS c FROM pg_attribute
               WHERE attname = 'text_fields' AND attrelid IN ('documents'::regclass, 'documents_p1'::regclass,
                                                              'documents_p2'::regclass)"""
        )
        compression = {row["t"]: row["c"] for row in await cur.fetchall()}
        assert compression == {"documents": "l", "documents_p1": "l", "documents_p2": "l"}
        # a new partition gets the same search index (and the id index)
        assert await _bm25(conn, "documents_p2") == bm25
        assert any("(partition_id, project_pk, id)" in d for d in await _indexdefs(conn, tablename="documents_p2"))

        # down and up again gives the same index (on both partitions)
        await asyncio.to_thread(_migrate, "downgrade", "0001")
        await asyncio.to_thread(_migrate, "upgrade", "head")
        assert await _bm25(conn, "documents_p1") == bm25

        await asyncio.to_thread(_migrate, "downgrade", "0001")
        assert (await _one(conn, "SELECT count(*) AS n FROM documents"))["n"] == 300
        assert not {"sort_kw1", "sort_num1", "sort_date2"} & await _columns(conn, "documents")
        assert not {"sort_slot_ready", "quarter_ready"} & await _columns(conn, "fields")
        assert await _bm25(conn, "documents_p1") == bm25_0001
        assert await _indexdefs(conn, indexname="%project_id") == []
        assert await _search(conn, f"f{text_pk}", "word1") == 100
    finally:
        await drop_schema(conn, SCHEMA)
        await conn.close()
