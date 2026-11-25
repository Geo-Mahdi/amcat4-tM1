"""
Storage layout of AmCAT on PostgreSQL + pg_search: the design of the database, and helpers that inspect it.

The tables themselves are not defined here, but in the (alembic) migrations in amcat4/migrations/versions:
0001_initial.py contains the complete initial schema, and later migrations change it. Migrations are history and
must never be edited once released, so this module is the place for the (current) design and its rationale.

System data (projects, settings, roles, api keys, requests, object storage register) are plain tables.

Documents:
- All documents of all projects live in a single `documents` table. A project owns its documents.
- Document values are stored in jsonb columns, keyed by a stable *field key* (e.g. "f12") rather than the
  field name. Field names are project-level labels in the `fields` table, so renaming a field is a metadata
  update, and two projects can use the same name with different types.
- Values are split over columns by how they need to be indexed:
    - text_fields:   tokenized full-text fields (BM25, with positions for phrase queries). Compressed with lz4 since
                     migration 0002 (lz4 decompresses faster than the default pglz); rows written before keep pglz.
                     Each partition has its own compression setting (see create_partition)
    - exact_fields:  untokenized fields (keyword, tag, url, number, integer, boolean, date, geo_point, multimedia
                     paths), stored as fast (columnar) fields for filtering, sorting and aggregation
    - stored_fields: values that are stored but not indexed (object)
  New values of exact_fields and stored_fields are compressed with lz4 too (AMCAT4_LZ4_JSON_COLUMNS, set at startup
  by ensure_json_compression, not by a migration; postgres only compresses values of about 2 kB and more).
- Vectors are stored in the document_vectors table (pgvector), with a vector index per field.
- A BM25 index covers partition_id, project_pk, the standard columns and sort slots (see below), text_fields and
  exact_fields. New fields are new json keys, so adding a field never requires rebuilding the index.
- A search selects the project twice: in SQL (partition_id and project_pk, so postgres only scans the right
  partitions) and as a term on project_pk in the json query of the BM25 index (amcat4.postgres.search). That term
  and the user's filters are one unscored clause, {"const_score": {"score": 0, "query": ...}} inside "must", so only
  the query string is scored. pg_search's json boolean query has no "filter" list: an unknown key such as "filter"
  is silently ignored (tested on 0.26 and 0.27), so it would drop the project condition. Unlike the original, where
  the project term was a scored "must" clause: top 10 for a very common word went from 34 ms to 3 ms in a shared
  partition of 13 M documents (0.27, no parallel workers, 22 Nov 2026). Counts and hits are the same; only the
  relevance order changes. AMCAT4_FILTER_CLAUSES=false gives the original form.
- The documents table is list partitioned on partition_id, and every partition has its own BM25 index. Every project
  is assigned to a partition when it is created (projects.partition_id): the newest partition, until its BM25 index
  reaches partition_max_gb, after which a new partition is created. So the long tail of small projects shares
  partitions, and a big project fills (most of) a partition. A big project can be moved to a partition of its own
  (amcat4.projects.partitions, in batches; projects.own_partition). During the move its documents are in two
  partitions (projects.moving_to): reads look in both (project_filter), writes are refused (writing).
  Queries on a project only use its own partition, but only if they have a SQL condition on partition_id: postgres
  does not know that a project is in one partition (see project_filter). partition_id is also in the BM25 index:
  pg_search checks SQL conditions on columns outside the index against the table, for every match. A foreign key
  keeps the documents of a project in its partition(s) (project_partitions, migration 0003: one row per project,
  two while it is being moved): updating projects.partition_id moves the documents (a trigger, in one statement).
  Every upload adds segments to the BM25 index, and updates leave deleted documents in it. pg_search 0.26 does not
  merge segments (in our tests), so the number of segments grows with every upload; from 0.27 on (the target version
  of this code), pg_search merges small segments in the background (index option background_layer_sizes; measured:
  20 single-row inserts leave 15 segments, 5 a few seconds later), so it stays low. A periodic task rebuilds
  the index of a partition when it still needs it (many segments or deleted documents), after the partition has been
  quiet for a while (see amcat4.projects.maintenance); manually: amcat4 optimize --reindex [--project ...].
- BM25 scores (how rare a word is) are computed per partition, so other projects in the same partition affect the
  ranking (but not which documents match).
- Standard metadata columns: date and source (e.g. publication date and outlet), which most communication data has.
  pg_search cannot sort on json keys inside the index (Top-K; also not in 0.27), so each project can map one date
  field to the date column and one keyword field to the source column (fields.sort_slot): real columns that are
  also in the index.
  Sorting on these fields is fast; sorting on other fields works but needs to read all matches. The first date
  field, and a keyword field called "source", are mapped automatically. Adding a standard column later means
  rebuilding the BM25 indexes.
- copied_from records the original project and document of copied documents.
- Date fields also get derived keys (f12_year, f12_quarter, f12_month, f12_dayofweek, ...) in exact_fields, so date
  histograms and date-part filters run inside the index. (The quarter key came with migration 0002: older documents
  get it from a background job, see fields.quarter_ready.)
- exact_fields also has the key _text: the list of the keys of the text fields of the document that have a value
  (migration 0004, written by a trigger on documents), so "this text field has a value" is a term inside the index.
  Older documents get it from a background job, see fields.text_exists_ready.
- Sort slots: migration 0002 added five generic slot columns (sort_kw1..3 as text COLLATE "C", sort_num1,
  sort_date2) next to date and source, also in the BM25 index, so sorting on them is a Top-K scan inside the index.
  fast_sort puts a field in a free slot of its kind (see amcat4.postgres.fields). For a large project a background
  job fills the column (fields.sort_slot_ready is false until it is done; sorting does not use the slot meanwhile).
- A B-tree index on (partition_id, project_pk, id) (migration 0002) serves listings without a query string in id
  order with plain SQL, reading only the rows of the page; through the BM25 index, such a listing goes over the ids
  of all projects in the partition (AMCAT4_BTREE_LISTING=false: the original way). Counts stay in the BM25 index.

TODO (later): read-only *reference* projects, that reference documents owned by other projects
(project_references / document_references tables). Querying a reference project filters on the owner
project ids inside the BM25 index and then joins on the referenced document ids. Not implemented yet.
"""

import functools
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from typing import AsyncGenerator, Callable

import psycopg
from psycopg import AsyncConnection, sql
from psycopg.errors import LockNotAvailable
from psycopg.pq import TransactionStatus

from amcat4.config import get_settings
from amcat4.errors import ConflictError

# Keys of the advisory locks that serialize assigning partitions (so two new projects cannot both create one), and
# rebuilding BM25 indexes (one at a time: a rebuild uses many cores).
# _PARTITION_LOCK is the same key as MIGRATION_LOCK (amcat4/migrations/env.py), as in the original code. Kept on
# purpose: while migrations run (they change the documents table and its partitions, e.g. 0002 builds the partition
# indexes and 0003 adds project_partitions), no process creates a partition or starts a move; those wait until the
# migrations are done. Distinct keys would end this. Also for a server of the older version that keeps running while
# a newer one migrates (a rolling upgrade): with a new key on either side, it could create a partition in the middle
# of a migration that changes the partitions. The cost of the shared key: creating a project waits while migrations
# run (minutes for a long one, such as the index build of 0002).
_PARTITION_LOCK = 4_000_001
_REINDEX_LOCK = 4_000_002


def partition_table(partition_id: int) -> str:
    return f"documents_p{partition_id}"


async def partition_of(conn: AsyncConnection, project_pk: int) -> int:
    """The documents partition of a project"""
    cur = await conn.execute("SELECT partition_id FROM projects WHERE pk = %s", [project_pk])
    row = await cur.fetchone()
    if row is None:
        raise ValueError(f"Project {project_pk} does not exist")
    return row["partition_id"]  # type: ignore[index, call-overload]


async def project_filter(conn: AsyncConnection, project_pk: int, table: str = "documents") -> sql.Composable:
    """
    SQL condition that selects the documents of a project. This includes the partition: postgres can only skip the
    other partitions if the query has a condition on the partition key. While the project is being moved to another
    partition (see amcat4.projects.partitions), its documents are in both, so the condition names both (each document
    is in exactly one of them, so it is seen once). Unlike the original, which used partition_of (one partition);
    partition and move target are read in one statement, so a move that ends in between cannot be missed.
    """
    cur = await conn.execute("SELECT partition_id, moving_to FROM projects WHERE pk = %s", [project_pk])
    row: dict | None = await cur.fetchone()  # type: ignore[assignment]
    if row is None:
        raise ValueError(f"Project {project_pk} does not exist")
    if row["moving_to"] is None:
        part = sql.SQL("{t}.partition_id = {p}").format(t=sql.Identifier(table), p=sql.Literal(row["partition_id"]))
    else:
        part = sql.SQL("{t}.partition_id IN ({a}, {b})").format(
            t=sql.Identifier(table), a=sql.Literal(row["partition_id"]), b=sql.Literal(row["moving_to"])
        )
    return sql.SQL("{part} AND {t}.project_pk = {pk}").format(part=part, t=sql.Identifier(table), pk=sql.Literal(project_pk))


# Second key of the advisory lock (project_pk, _WRITE_LOCK) between writes to a project (shared) and moving it
# (exclusive, see lock_writes). The sort slot locks use the second keys 0 to 6 (documents._sort_slot_lock)
_WRITE_LOCK = 100


@asynccontextmanager
async def writing(conn: AsyncConnection, project_pk: int) -> AsyncGenerator[None, None]:
    """
    Wrap every write to the documents of a project. While the project is being moved to another partition, writes are
    refused (ConflictError): uploads look up existing documents in the project's partition, and the move job relies on
    the documents not changing. The shared lock makes the move wait for writes that already started; writes do not
    block each other. New in this version: the original could not move a project in batches, so it had no such check.
    """
    # In a transaction: a transaction lock (released at commit or rollback, also after an error). Otherwise (autocommit
    # statements) a session lock, released afterwards
    in_transaction = conn.info.transaction_status != TransactionStatus.IDLE
    function = "pg_advisory_xact_lock_shared" if in_transaction else "pg_advisory_lock_shared"
    await conn.execute(f"SELECT {function}(%s, %s)", [project_pk, _WRITE_LOCK])  # type: ignore[arg-type]
    try:
        cur = await conn.execute("SELECT moving_to FROM projects WHERE pk = %s", [project_pk])
        row = await cur.fetchone()
        if row is not None and row["moving_to"] is not None:  # type: ignore[index, call-overload]
            raise ConflictError("This project is being moved to another partition; please try again later")
        yield
    finally:
        if not in_transaction:
            await conn.execute("SELECT pg_advisory_unlock_shared(%s, %s)", [project_pk, _WRITE_LOCK])


def guard_writes(project: Callable[..., int | list[int]]):
    """
    Decorator for the write functions of the storage layer (first argument: the connection): run the function inside
    writing() for the project(s) that project(*args, **kwargs) returns.
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(conn, *args, **kwargs):
            pks = project(conn, *args, **kwargs)
            async with AsyncExitStack() as stack:
                for pk in [pks] if isinstance(pks, int) else pks:
                    await stack.enter_async_context(writing(conn, pk))
                return await func(conn, *args, **kwargs)

        return wrapper

    return decorator


async def lock_writes(conn: AsyncConnection, project_pk: int, unlock: bool = False) -> None:
    """
    Wait for the writes to a project that are running (and keep new ones waiting), for the move job: an exclusive
    session lock on the key that writing() takes shared. Call again with unlock=True to release it.
    """
    function = "pg_advisory_unlock" if unlock else "pg_advisory_lock"
    await conn.execute(f"SELECT {function}(%s, %s)", [project_pk, _WRITE_LOCK])  # type: ignore[arg-type]


async def list_partitions(conn: AsyncConnection) -> list[dict]:
    """The documents partitions, with their table, BM25 index and sizes (sizes are cheap: no data is read)"""
    cur = await conn.execute(
        r"""SELECT (regexp_match(pg_get_expr(part.relpartbound, part.oid), 'IN \((\d+)\)'))[1]::int AS partition_id,
                   part.relname AS table, idx.relname AS index,
                   pg_relation_size(idx.oid) AS index_bytes, pg_total_relation_size(part.oid) AS total_bytes,
                   greatest(part.reltuples, 0)::bigint AS estimated_documents
            FROM pg_inherits i
            JOIN pg_class part ON part.oid = i.inhrelid
            JOIN pg_index ix ON ix.indrelid = part.oid
            JOIN pg_class idx ON idx.oid = ix.indexrelid
            JOIN pg_am am ON am.oid = idx.relam AND am.amname = 'bm25'
            WHERE i.inhparent = 'documents'::regclass
              AND ix.indisvalid AND idx.relname !~ '_cc(new|old)[0-9]*$'  -- not the copies made by a reindex
            ORDER BY 1"""
    )
    return await cur.fetchall()  # type: ignore[return-value]


async def index_segments(conn: AsyncConnection, index: str) -> dict:
    """Number of segments, documents and deleted documents (not yet removed from the segments) of a BM25 index"""
    cur = await conn.execute(
        """SELECT count(*) AS segments, coalesce(sum(num_docs), 0)::bigint AS documents,
                  coalesce(sum(num_deleted), 0)::bigint AS deleted
           FROM paradedb.index_info(%s::regclass)""",
        [index],
    )
    return await cur.fetchone()  # type: ignore[return-value]


async def index_merging(conn: AsyncConnection, index: str) -> bool:
    """
    Whether pg_search is merging segments of a BM25 index in the background right now (0.27 and later merge;
    paradedb.is_merging exists on 0.26 too). Used by the maintenance, which waits for a running merge.
    """
    cur = await conn.execute("SELECT paradedb.is_merging(%s::regclass) AS merging", [index])
    return bool((await cur.fetchone())["merging"])  # type: ignore[index]


async def partition_changes(conn: AsyncConnection) -> dict[str, int]:
    """
    Number of inserted, updated and deleted rows per partition table, as counted by postgres since its statistics
    were reset. Only useful to see whether a partition changed since an earlier call.
    """
    cur = await conn.execute(
        """SELECT s.relname AS table, s.n_tup_ins + s.n_tup_upd + s.n_tup_del AS changes
           FROM pg_stat_user_tables s JOIN pg_inherits i ON i.inhrelid = s.relid
           WHERE i.inhparent = 'documents'::regclass"""
    )
    return {row["table"]: row["changes"] for row in await cur.fetchall()}  # type: ignore[index, call-overload]


async def reindex(conn: AsyncConnection, index: str) -> None:
    """
    Rebuild a BM25 index (online: reads and writes continue). Waits until no other rebuild is running.
    The connection must be in autocommit mode.
    """
    await conn.execute("SELECT pg_advisory_lock(%s)", [_REINDEX_LOCK])
    try:
        await conn.execute(sql.SQL("REINDEX INDEX CONCURRENTLY {}").format(sql.Identifier(index)))
    finally:
        await conn.execute("SELECT pg_advisory_unlock(%s)", [_REINDEX_LOCK])


async def drop_reindex_leftovers(conn: AsyncConnection) -> list[str]:
    """
    Drop the invalid index copies left behind by an interrupted REINDEX CONCURRENTLY (only if no rebuild is running).
    Returns the names of the dropped indexes.
    """
    cur = await conn.execute("SELECT pg_try_advisory_lock(%s) AS locked", [_REINDEX_LOCK])
    if not (await cur.fetchone())["locked"]:  # type: ignore[index]
        return []
    try:
        cur = await conn.execute(
            """SELECT idx.relname AS index FROM pg_index ix
               JOIN pg_class idx ON idx.oid = ix.indexrelid
               JOIN pg_inherits i ON i.inhrelid = ix.indrelid
               WHERE i.inhparent = 'documents'::regclass AND NOT ix.indisvalid AND idx.relname ~ '_cc(new|old)[0-9]*$'"""
        )
        names = [row["index"] for row in await cur.fetchall()]  # type: ignore[index, call-overload]
        for name in names:
            await conn.execute(sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {}").format(sql.Identifier(name)))
        return names
    finally:
        await conn.execute("SELECT pg_advisory_unlock(%s)", [_REINDEX_LOCK])


async def partition_overview(conn: AsyncConnection) -> list[dict]:
    """
    The documents partitions with their sizes, and the projects in each partition with their number of documents.
    Counting documents reads the (document id) index of all partitions, so this takes a moment on large databases.
    While a project is being moved, it is listed in both partitions (projects.partition_id and moving_to) with the
    number of its documents in each (before, only those in the old partition were counted, so the documents that had
    been moved were missing from the overview).
    """
    cur = await conn.execute(
        """SELECT pp.partition_id, p.id AS project, coalesce(c.n, 0) AS documents
           FROM projects p
           CROSS JOIN LATERAL (VALUES (p.partition_id), (p.moving_to)) pp(partition_id)
           LEFT JOIN (SELECT partition_id, project_pk, count(*) AS n FROM documents GROUP BY 1, 2) c
             ON c.partition_id = pp.partition_id AND c.project_pk = p.pk
           WHERE pp.partition_id IS NOT NULL
           ORDER BY documents DESC, project"""
    )
    projects: dict[int, list[dict]] = {}
    for row in await cur.fetchall():
        projects.setdefault(row["partition_id"], []).append({"project": row["project"], "documents": row["documents"]})  # type: ignore[index, call-overload]
    result = []
    for partition in await list_partitions(conn):
        members = projects.get(partition["partition_id"], [])
        segments = await index_segments(conn, partition["index"])
        result.append(
            {
                "partition_id": partition["partition_id"],
                "table": partition["table"],
                "index_bytes": partition["index_bytes"],
                "total_bytes": partition["total_bytes"],
                "segments": segments["segments"],
                "deleted_documents": segments["deleted"],
                "documents": sum(p["documents"] for p in members),
                "projects": members,
            }
        )
    return result


async def create_partition(conn: AsyncConnection, partition_id: int, lock_timeout: str = "5s") -> None:
    """
    Create a new (empty) documents partition. Postgres creates its BM25 and other indexes when it is attached, with
    the definitions of the indexes of the partitioned table (so the same columns and options as the migrations gave
    the existing partitions).
    CREATE TABLE ... PARTITION OF would need an exclusive lock on documents (waiting for, and blocking, all
    queries), so the table is created separately and then attached, which does not block queries or uploads.
    Raises LockNotAvailable if the lock cannot be acquired within lock_timeout.
    """
    table = sql.Identifier(partition_table(partition_id))
    async with conn.transaction():
        await conn.execute(sql.SQL("SET LOCAL lock_timeout = {}").format(sql.Literal(lock_timeout)))
        # INCLUDING COMPRESSION: the compression of text_fields (lz4, see migration 0002) is set per partition
        await conn.execute(sql.SQL("CREATE TABLE {} (LIKE documents INCLUDING DEFAULTS INCLUDING COMPRESSION)").format(table))
        await conn.execute(
            sql.SQL("ALTER TABLE documents ATTACH PARTITION {} FOR VALUES IN ({})").format(table, sql.Literal(partition_id))
        )
        await conn.execute("SET LOCAL lock_timeout = DEFAULT")


# The json columns whose compression AMCAT4_LZ4_JSON_COLUMNS sets (text_fields has lz4 since migration 0002)
LZ4_COLUMNS = ("exact_fields", "stored_fields")


async def ensure_json_compression(conn: AsyncConnection, lock_timeout: str = "5s") -> list[str]:
    """
    Set the compression of new values of exact_fields and stored_fields to lz4 (AMCAT4_LZ4_JSON_COLUMNS, the
    default), or back to the server's default (false), on documents and every partition that has another setting.
    Returns the tables that were changed. Called at startup, after the migrations; it only reads the catalog when
    nothing has to change.
    Only new values: SET COMPRESSION does not rewrite existing rows, which keep their compression and stay readable.
    New partitions copy the setting of documents (create_partition, INCLUDING COMPRESSION).
    If a table cannot be changed (the server has no lz4, or the table stays locked for lock_timeout), a warning is
    logged, nothing else changes, and the next start tries again.
    """
    wanted = "l" if get_settings().lz4_json_columns else ""
    cur = await conn.execute(
        """SELECT a.attrelid::regclass::text AS tbl FROM pg_attribute a
           WHERE a.attname = ANY(%s) AND a.attcompression <> %s
             AND a.attrelid IN (SELECT 'documents'::regclass
                                UNION ALL SELECT inhrelid FROM pg_inherits WHERE inhparent = 'documents'::regclass)
           GROUP BY 1 ORDER BY 1""",
        [list(LZ4_COLUMNS), wanted],
    )
    method = sql.SQL("lz4" if wanted else "default")
    changed = []
    for row in await cur.fetchall():
        table = row["tbl"]  # type: ignore[index, call-overload]
        alter = sql.SQL("ALTER TABLE {} {}").format(
            sql.SQL(table),  # (regclass text: quoted where needed, as in the migrations)
            sql.SQL(", ").join(
                sql.SQL("ALTER COLUMN {} SET COMPRESSION {}").format(sql.Identifier(c), method) for c in LZ4_COLUMNS
            ),
        )
        try:
            async with conn.transaction():
                # (an exclusive lock, for a catalog change only; with a timeout, so a long query cannot make every
                # other query on documents wait behind this statement)
                await conn.execute(sql.SQL("SET LOCAL lock_timeout = {}").format(sql.Literal(lock_timeout)))
                await conn.execute(alter)
            changed.append(table)
        except psycopg.Error as e:
            logging.warning(f"Could not set {method.as_string(None)} compression on {table} ({e}); keeping it as it is")
    return changed


async def assign_partition(conn: AsyncConnection) -> int:
    """
    The partition for a new project: the newest partition, or a new one if the BM25 index of the newest partition
    has reached partition_max_gb. Call this in the transaction that creates the project.
    Projects are never moved automatically: a big project can be moved to a partition of its own with the move_project
    job (amcat4.projects.partitions). Partitions that projects have for themselves are not used for new projects.
    """
    await conn.execute("SELECT pg_advisory_xact_lock(%s)", [_PARTITION_LOCK])
    partitions = await list_partitions(conn)
    # not the partitions that a project has (or is getting) for itself
    cur = await conn.execute(
        """SELECT partition_id FROM projects WHERE own_partition
           UNION SELECT moving_to FROM projects WHERE moving_to IS NOT NULL"""
    )
    own = {row["partition_id"] for row in await cur.fetchall()}  # type: ignore[index, call-overload]
    shared = [p for p in partitions if p["partition_id"] not in own]
    # (unlike the original, the newest shared partition, which need not be the newest partition)
    if shared and shared[-1]["index_bytes"] < get_settings().partition_max_gb * 1024**3:
        return shared[-1]["partition_id"]
    partition_id = await next_partition_id(conn)
    try:
        await create_partition(conn, partition_id)
    except LockNotAvailable:
        if not shared:  # every partition belongs to a big project: there is no partition to fall back to
            raise
        logging.warning(f"Could not create documents partition {partition_id} (lock timeout), using the current one")
        return shared[-1]["partition_id"]
    return partition_id


async def next_partition_id(conn: AsyncConnection) -> int:
    """
    The id for a new partition: one more than the highest, also counting partitions without a valid BM25 index (which
    list_partitions leaves out, e.g. one whose index is still being built), so an id is never used twice.
    """
    cur = await conn.execute(
        r"""SELECT coalesce(max((regexp_match(pg_get_expr(c.relpartbound, c.oid), 'IN \((\d+)\)'))[1]::int), 0) AS n
            FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = 'documents'::regclass"""
    )
    return (await cur.fetchone())["n"] + 1  # type: ignore[index]


async def drop_schema(conn: AsyncConnection, schema: str) -> None:
    """Drop the amcat schema and ALL its data. Only for tests and benchmarks!"""
    await conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
