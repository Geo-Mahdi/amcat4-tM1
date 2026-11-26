"""
Migrations 0002 to 0005 on a database that has data from 0001 or 0003: rows are kept, the new columns and indexes exist, the
search index is rebuilt with the slot columns, searching still works, documents reference project_partitions (0003),
and the downgrade gives back the 0001 schema.
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


async def _references(conn) -> str:
    """The table that the foreign key of documents (project_pk, partition_id) references"""
    row = await _one(
        conn,
        """SELECT confrelid::regclass::text AS t FROM pg_constraint
           WHERE conrelid = 'documents'::regclass AND contype = 'f' AND convalidated""",
    )
    return row["t"]


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
    """
    Migrations 0002 and 0003 on a database with 0001 data: nothing is lost, the new columns and indexes exist, the
    search index is rebuilt, and the downgrade restores the 0001 schema (0001 itself is not edited)
    """
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
        # 0003: documents reference project_partitions, which has a row for the project
        assert await _references(conn) == "project_partitions"
        row = await _one(conn, "SELECT project_pk, partition_id FROM project_partitions")
        assert row == {"project_pk": project, "partition_id": 1}
        assert {"sort_kw1", "sort_kw2", "sort_kw3", "sort_num1", "sort_date2"} <= await _columns(conn, "documents")
        assert {"sort_kw1", "sort_num1"} <= await _columns(conn, "documents_p1")
        assert {"sort_slot_ready", "quarter_ready"} <= await _columns(conn, "fields")
        # the existing date field still needs its quarter keys; other fields do not
        cur = await conn.execute("SELECT name, quarter_ready FROM fields ORDER BY name")
        assert {row["name"]: row["quarter_ready"] for row in await cur.fetchall()} == {"date": False, "text": True}
        # 0004: the existing text field still needs the lists of text keys; new rows get them from the trigger
        cur = await conn.execute("SELECT name, text_exists_ready FROM fields ORDER BY name")
        assert {row["name"]: row["text_exists_ready"] for row in await cur.fetchall()} == {"date": True, "text": False}
        assert (await _one(conn, "SELECT count(*) AS n FROM documents WHERE exact_fields ? '_text'"))["n"] == 0
        row = await _one(
            conn,
            # (created_seq: migration 0005 comes after 0004, and since then every INSERT must give the number)
            """INSERT INTO documents (partition_id, project_pk, doc_id, text_fields, created_seq)
               VALUES (1, %s, 'new', %s, 1) RETURNING exact_fields, id, doc_pk""",
            [project, Jsonb({f"f{text_pk}": "new"})],
        )
        assert row["exact_fields"] == {"_text": [f"f{text_pk}"]}
        assert row["doc_pk"] == row["id"]  # (the trigger of 0005, next to the one of 0004)
        await conn.execute("DELETE FROM documents WHERE doc_id = 'new'")

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
        assert await _references(conn) == "projects"
        assert not {"sort_kw1", "sort_num1", "sort_date2"} & await _columns(conn, "documents")
        assert not {"sort_slot_ready", "quarter_ready", "text_exists_ready"} & await _columns(conn, "fields")
        assert await _one(conn, "SELECT 1 FROM pg_trigger WHERE tgrelid = 'documents'::regclass AND NOT tgisinternal") is None
        assert await _bm25(conn, "documents_p1") == bm25_0001
        assert await _indexdefs(conn, indexname="%project_id") == []
        assert await _search(conn, f"f{text_pk}", "word1") == 100
    finally:
        await drop_schema(conn, SCHEMA)
        await conn.close()


async def _insert_0003_data(conn) -> tuple[int, int]:
    """A project with a text field and 300 documents, as the code before migration 0005 stored them"""
    project = (await _one(conn, "INSERT INTO projects (id, partition_id) VALUES ('p', 1) RETURNING pk"))["pk"]
    text_pk = (
        await _one(conn, "INSERT INTO fields (project_pk, name, type) VALUES (%s, 'text', 'text') RETURNING pk", [project])
    )["pk"]
    async with conn.cursor() as cur:
        await cur.executemany(
            "INSERT INTO documents (partition_id, project_pk, doc_id, text_fields) VALUES (1, %s, %s, %s)",
            [(project, str(i), Jsonb({f"f{text_pk}": f"word{i % 3} text"})) for i in range(300)],
        )
    return project, text_pk


async def _check_0005(conn, project: int, text_pk: int, bm25_0003: str) -> str:
    """The state after migration 0005 on the data of _insert_0003_data. Returns the definition of the search index."""
    current = 9223372036854775807
    # no row was rewritten or lost: old rows are "created before any release", current, and have no doc_pk yet
    row = await _one(
        conn,
        """SELECT count(*) AS n, min(created_seq) AS c0, max(created_seq) AS c1, min(retired_seq) AS r0,
                  count(doc_pk) AS with_pk FROM documents""",
    )
    assert row == {"n": 300, "c0": 0, "c1": 0, "r0": current, "with_pk": 0}
    assert (await _one(conn, "SELECT labels_ready FROM projects WHERE pk = %s", [project]))["labels_ready"] is False
    default = await _one(
        conn,
        """SELECT column_default AS d FROM information_schema.columns
           WHERE table_schema = %s AND table_name = 'documents' AND column_name = 'created_seq'""",
        [SCHEMA],
    )
    assert default["d"] is None
    # the search index: the 0002 entries and options, plus the three columns, once per partition
    bm25 = await _bm25(conn, "documents_p1")
    for entry in bm25_0003.removeprefix("bm25 (").split(") WITH")[0].split(", "):
        assert entry in bm25
    for column in ["doc_pk", "created_seq", "retired_seq"]:
        assert f" {column}," in bm25
    assert bm25_0003.split(" WITH ")[1] == bm25.split(" WITH ")[1]
    assert len(await _indexdefs(conn, indexname="documents_p1_bm25")) == 1
    assert await _indexdefs(conn, indexname="%bm25_new") == []
    assert await _search(conn, f"f{text_pk}", "word1") == 100
    # "current" is found as an INTEGER term, and a release range works
    for query, n in [
        ({"term": {"field": "retired_seq", "value": current}}, 300),
        ({"range": {"field": "created_seq", "lower_bound": None, "upper_bound": {"included": 5}}}, 300),
        ({"range": {"field": "retired_seq", "lower_bound": {"excluded": 5}, "upper_bound": None}}, 300),
        ({"range": {"field": "created_seq", "lower_bound": {"excluded": 0}, "upper_bound": None}}, 0),
    ]:
        row = await _one(conn, "SELECT count(*) AS n FROM documents_p1 WHERE id @@@ %s::jsonb", [Jsonb(query)])
        assert row["n"] == n, query
    # the unique constraint on doc_id is now a unique index over current rows
    assert await _one(conn, "SELECT 1 FROM pg_constraint WHERE conrelid = 'documents'::regclass AND contype = 'u'") is None
    [live] = await _indexdefs(conn, indexname="documents_p1_doc_id_live")
    assert "UNIQUE" in live and "retired_seq = " in live
    with pytest.raises(psycopg.errors.UniqueViolation):
        await conn.execute(
            "INSERT INTO documents (partition_id, project_pk, doc_id, created_seq) VALUES (1, %s, '7', 1)", [project]
        )
    # created_seq has no default any more; a new row gets doc_pk = id, a new version keeps the doc_pk it is given
    with pytest.raises(psycopg.errors.NotNullViolation):
        await conn.execute("INSERT INTO documents (partition_id, project_pk, doc_id) VALUES (1, %s, 'new')", [project])
    async with conn.transaction(force_rollback=True):
        new = await _one(
            conn,
            """INSERT INTO documents (partition_id, project_pk, doc_id, created_seq) VALUES (1, %s, 'new', 1)
               RETURNING id, doc_pk""",
            [project],
        )
        assert new["doc_pk"] == new["id"]
        old = await _one(
            conn,
            """INSERT INTO documents (partition_id, project_pk, doc_id, created_seq, retired_seq, doc_pk)
               VALUES (1, %s, 'new', 0, 1, %s) RETURNING id, doc_pk""",
            [project, new["id"]],
        )
        assert old["doc_pk"] == new["id"] != old["id"]
    # the new tables, the label index and the sequence
    assert {"number", "seq", "doi", "manifest", "created_by"} <= await _columns(conn, "releases")
    assert {"doc_pk", "field_pk", "value", "created_seq", "retired_seq", "created_by"} <= await _columns(conn, "doc_labels")
    [labels] = await _indexdefs(conn, indexname="doc_labels_bm25")
    assert "bm25" in labels and "retired_seq" in labels and "mutable_segment_rows" in labels
    assert "label_table" in await _columns(conn, "fields")
    assert (await _one(conn, "SELECT last_value FROM change_seq"))["last_value"] == 1
    return bm25


@pytest.mark.anyio
async def test_migration_0005_with_data():
    """
    Migration 0005 on a database with documents: no row is rewritten or lost, the search index is rebuilt once with the
    version columns, the new tables exist. Running it again (as after an interruption) changes nothing, also from a
    half-done state. The downgrade gives back the 0003 schema.
    """
    conn = await _connect()
    try:
        await drop_schema(conn, SCHEMA)
        await asyncio.to_thread(_migrate, "upgrade", "0003")
        bm25_0003 = await _bm25(conn, "documents_p1")
        project, text_pk = await _insert_0003_data(conn)
        xmin = (await _one(conn, "SELECT max(xmin::text::bigint) AS x FROM documents"))["x"]

        await asyncio.to_thread(_migrate, "upgrade", "0005")
        bm25 = await _check_0005(conn, project, text_pk, bm25_0003)
        # no document row was written by the migration
        assert (await _one(conn, "SELECT max(xmin::text::bigint) AS x FROM documents"))["x"] == xmin
        index_oid = (await _one(conn, "SELECT 'documents_p1_bm25'::regclass::oid AS oid"))["oid"]

        # Run again, as after an interruption at the very end: nothing changes, and the search index is not rebuilt
        await asyncio.to_thread(_migrate, "stamp", "0003")
        await asyncio.to_thread(_migrate, "upgrade", "0005")
        assert await _check_0005(conn, project, text_pk, bm25_0003) == bm25
        assert (await _one(conn, "SELECT 'documents_p1_bm25'::regclass::oid AS oid"))["oid"] == index_oid

        # Run again from a half-done state: the new search index was being built (a leftover parent without the
        # partition's index), the unique index over current rows is missing, the default is back
        await asyncio.to_thread(_migrate, "stamp", "0003")
        await conn.execute("DROP INDEX documents_bm25")
        await conn.execute(f"CREATE INDEX documents_bm25 ON documents USING {bm25_0003}")
        await conn.execute(f"CREATE INDEX documents_bm25_new ON ONLY documents USING {bm25}")
        await conn.execute("DROP INDEX documents_doc_id_live")
        await conn.execute("ALTER TABLE documents ALTER COLUMN created_seq SET DEFAULT 0")
        await conn.execute("DROP TABLE doc_labels")
        await asyncio.to_thread(_migrate, "upgrade", "0005")
        assert await _check_0005(conn, project, text_pk, bm25_0003) == bm25

        # a new partition gets the same indexes and the doc_pk trigger
        await create_partition(conn, 2)
        assert await _bm25(conn, "documents_p2") == bm25
        assert any("retired_seq = " in d and "UNIQUE" in d for d in await _indexdefs(conn, tablename="documents_p2"))
        await conn.execute("INSERT INTO projects (id, partition_id) VALUES ('q', 2)")
        row = await _one(
            conn,
            """INSERT INTO documents (partition_id, project_pk, doc_id, created_seq)
               VALUES (2, (SELECT pk FROM projects WHERE id = 'q'), 'x', 1) RETURNING id, doc_pk""",
        )
        assert row["doc_pk"] == row["id"]

        # The downgrade is refused while a release exists; without, it gives back the 0003 schema and keeps the documents
        await conn.execute("INSERT INTO releases (project_pk, number, seq) VALUES (%s, 1, 5)", [project])
        with pytest.raises(RuntimeError, match="releases"):
            await asyncio.to_thread(_migrate, "downgrade", "0003")
        await conn.execute("DELETE FROM releases")
        # an old row version that no release needs any more is removed by the downgrade
        await conn.execute(
            """INSERT INTO documents (partition_id, project_pk, doc_id, created_seq, retired_seq, doc_pk)
               VALUES (1, %s, '7', 0, 3, 1)""",
            [project],
        )
        await asyncio.to_thread(_migrate, "downgrade", "0003")
        assert (await _one(conn, "SELECT count(*) AS n FROM documents WHERE project_pk = %s", [project]))["n"] == 300
        assert not {"created_seq", "retired_seq", "doc_pk"} & await _columns(conn, "documents")
        assert await _columns(conn, "releases") == set() and await _columns(conn, "doc_labels") == set()
        assert await _bm25(conn, "documents_p1") == bm25_0003
        assert await _one(conn, "SELECT 1 FROM pg_constraint WHERE conrelid = 'documents'::regclass AND contype = 'u'")
        assert await _indexdefs(conn, indexname="%doc_id_live") == []
        assert await _search(conn, f"f{text_pk}", "word1") == 100
        await asyncio.to_thread(_migrate, "upgrade", "head")
        assert await _bm25(conn, "documents_p1") == bm25
    finally:
        await drop_schema(conn, SCHEMA)
        await conn.close()


@pytest.mark.anyio
async def test_migration_chain_0003_to_0005():
    """
    The chain 0003 -> 0004 (quick wins: text keys) -> 0005 (releases and labels) on a database as the branch
    postgres-fast-v3 left it, with documents; and a fresh database from nothing to the head. The two migrations were
    written on separate branches, both on top of 0003.
    """
    conn = await _connect()
    try:
        # a fresh database: 0001 to the head in one go, which is 0007 (one head, no branches)
        await drop_schema(conn, SCHEMA)
        await asyncio.to_thread(_migrate, "upgrade", "head")
        assert await _one(conn, "SELECT version_num FROM alembic_version") == {"version_num": "0007"}
        cur = await conn.execute("SELECT table_name FROM information_schema.views WHERE table_schema = %s", [SCHEMA])
        assert {row["table_name"] for row in await cur.fetchall()} == {
            "sparql_projects",
            "sparql_releases",
            "sparql_release_fields",
        }
        cur = await conn.execute("SELECT tgname FROM pg_trigger WHERE tgrelid = 'documents'::regclass AND NOT tgisinternal")
        assert {row["tgname"] for row in await cur.fetchall()} == {"documents_text_keys", "documents_doc_pk"}
        assert {"text_exists_ready", "label_table"} <= await _columns(conn, "fields")
        assert {"created_seq", "retired_seq", "doc_pk"} <= await _columns(conn, "documents")

        # a database at 0003 with documents, step by step
        await drop_schema(conn, SCHEMA)
        await asyncio.to_thread(_migrate, "upgrade", "0003")
        bm25_0003 = await _bm25(conn, "documents_p1")
        project, text_pk = await _insert_0003_data(conn)
        key = f"f{text_pk}"
        xmin = (await _one(conn, "SELECT max(xmin::text::bigint) AS x FROM documents"))["x"]

        await asyncio.to_thread(_migrate, "upgrade", "0004")
        assert await _one(conn, "SELECT version_num FROM alembic_version") == {"version_num": "0004"}
        assert await _bm25(conn, "documents_p1") == bm25_0003  # (0004 does not touch the search index)
        assert not {"created_seq", "retired_seq", "doc_pk"} & await _columns(conn, "documents")

        await asyncio.to_thread(_migrate, "upgrade", "0005")
        assert await _one(conn, "SELECT version_num FROM alembic_version") == {"version_num": "0005"}
        await _check_0005(conn, project, text_pk, bm25_0003)
        # neither migration wrote a document row
        assert (await _one(conn, "SELECT max(xmin::text::bigint) AS x FROM documents"))["x"] == xmin
        # 0004 is still in place: the old documents have no list of text keys yet and their field is marked
        row = await _one(conn, "SELECT text_exists_ready FROM fields WHERE pk = %s", [text_pk])
        assert row == {"text_exists_ready": False}
        assert (await _one(conn, "SELECT count(*) AS n FROM documents WHERE exact_fields ? '_text'"))["n"] == 0
        # a write of an old document gets the list from the trigger, and stays a current row without a number
        row = await _one(
            conn,
            "UPDATE documents SET text_fields = text_fields WHERE doc_id = '6' "
            "RETURNING exact_fields, created_seq, retired_seq, doc_pk",
        )
        assert row == {"exact_fields": {"_text": [key]}, "created_seq": 0, "retired_seq": 9223372036854775807, "doc_pk": None}
        async with conn.transaction(force_rollback=True):
            # a new document: both triggers (the list of text keys, and doc_pk = id)
            new = await _one(
                conn,
                """INSERT INTO documents (partition_id, project_pk, doc_id, text_fields, created_seq)
                   VALUES (1, %s, 'new', %s, 5) RETURNING id, doc_pk, exact_fields""",
                [project, Jsonb({key: "word1 new"})],
            )
            assert new["doc_pk"] == new["id"] and new["exact_fields"] == {"_text": [key]}
            # an old version of it, as a versioned write stores it: the given doc_pk is kept, and the row gets the
            # list of its own text fields, whatever exact_fields it was given
            old = await _one(
                conn,
                """INSERT INTO documents (partition_id, project_pk, doc_id, text_fields, exact_fields, created_seq,
                                          retired_seq, doc_pk)
                   VALUES (1, %s, 'new', '{}', %s, 1, 5, %s) RETURNING id, doc_pk, exact_fields""",
                [project, Jsonb({"_text": [key], "f99": "x"}), new["id"]],
            )
            assert old["doc_pk"] == new["id"] != old["id"] and old["exact_fields"] == {"f99": "x"}
            # the term on the text keys and the row-version clauses in one search (as the code sends them)
            query = {
                "boolean": {
                    "must": [
                        {"term": {"field": "exact_fields._text", "value": key}},
                        {"term": {"field": "retired_seq", "value": 9223372036854775807}},
                        {"match": {"field": f"text_fields.{key}", "value": "word1"}},
                    ]
                }
            }
            row = await _one(conn, "SELECT count(*) AS n FROM documents_p1 WHERE id @@@ %s::jsonb", [Jsonb(query)])
            assert row["n"] == 1  # (of the old documents with word1 none has the list; the new document has)

        # down to 0003 again (through 0004) and up: the same schema as a fresh database
        await asyncio.to_thread(_migrate, "downgrade", "0003")
        assert not {"created_seq", "retired_seq", "doc_pk"} & await _columns(conn, "documents")
        assert "text_exists_ready" not in await _columns(conn, "fields")
        assert await _one(conn, "SELECT 1 FROM pg_trigger WHERE tgrelid = 'documents'::regclass AND NOT tgisinternal") is None
        assert (await _one(conn, "SELECT count(*) AS n FROM documents"))["n"] == 300
        await asyncio.to_thread(_migrate, "upgrade", "head")
        assert (
            await _one(
                conn, "SELECT count(*) AS n, count(*) FILTER (WHERE retired_seq = 9223372036854775807) AS live FROM documents"
            )
        ) == {"n": 300, "live": 300}
    finally:
        await drop_schema(conn, SCHEMA)
        await conn.close()
