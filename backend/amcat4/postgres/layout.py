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
    - text_fields:   tokenized full-text fields (BM25, with positions for phrase queries)
    - exact_fields:  untokenized fields (keyword, tag, url, number, integer, boolean, date, geo_point, multimedia
                     paths), stored as fast (columnar) fields for filtering, sorting and aggregation
    - stored_fields: values that are stored but not indexed (object)
- Vectors are stored in the document_vectors table (pgvector), with a vector index per field.
- A BM25 index covers partition_id, project_pk, the standard columns, text_fields and exact_fields. New fields are
  new json keys, so adding a field never requires rebuilding the index.
- The documents table is list partitioned on partition_id, and every partition has its own BM25 index. Every project
  is assigned to a partition when it is created (projects.partition_id): the newest partition, until its BM25 index
  reaches partition_max_gb, after which a new partition is created. So the long tail of small projects shares
  partitions, and a big project fills (most of) a partition. A big project can be moved to a partition of its own
  (amcat4.projects.partitions, in batches; projects.own_partition).
  Queries on a project only use its own partition, but only if they have a SQL condition on partition_id: postgres
  does not know that a project is in one partition (see project_filter). partition_id is also in the BM25 index:
  pg_search checks SQL conditions on columns outside the index against the table, for every match. A foreign key
  keeps the documents of a project in its partition(s) (project_partitions, migration 0003: one row per project,
  two while it is being moved): updating projects.partition_id moves the documents (a trigger, in one statement).
  Every upload adds segments to the BM25 index, and updates leave deleted documents in it. pg_search 0.26 does not
  merge segments (in our tests), so the number of segments grows with every upload; from 0.27 on, pg_search merges
  small segments in the background (index option background_layer_sizes), so it stays low. A periodic task rebuilds
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
- Sort slots: migration 0002 added five generic slot columns (sort_kw1..3, sort_num1, sort_date2) next to date and
  source; fast_sort puts a field in a free slot of its kind (see amcat4.postgres.fields). A B-tree index on
  (partition_id, project_pk, id) serves listings without a query in id order.

TODO (later): read-only *reference* projects, that reference documents owned by other projects
(project_references / document_references tables). Querying a reference project filters on the owner
project ids inside the BM25 index and then joins on the referenced document ids. Not implemented yet.
"""

import functools
import logging
import re
from contextlib import AsyncExitStack, asynccontextmanager
from typing import AsyncGenerator, Callable

from psycopg import AsyncConnection, sql
from psycopg.errors import LockNotAvailable
from psycopg.pq import TransactionStatus

from amcat4.config import get_settings
from amcat4.errors import ConflictError

# Keys of the advisory locks that serialize assigning partitions (so two new projects cannot both create one), and
# rebuilding BM25 indexes (one at a time: a rebuild uses many cores)
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
    partition (see amcat4.projects.partitions), its documents are in both.
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
_WRITE_LOCK = 100


@asynccontextmanager
async def writing(conn: AsyncConnection, project_pk: int) -> AsyncGenerator[None, None]:
    """
    Wrap every write to the documents of a project. While the project is being moved to another partition, writes are
    refused (ConflictError): uploads look up existing documents in the project's partition, and the move job relies on
    the documents not changing. The shared lock makes the move wait for writes that already started.
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
    """Wait for the writes to a project that are running (and keep new ones waiting), for the move job"""
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
    """Whether pg_search is merging segments of a BM25 index in the background right now (0.27 and later merge)"""
    cur = await conn.execute("SELECT paradedb.is_merging(%s::regclass) AS merging", [index])
    return bool((await cur.fetchone())["merging"])  # type: ignore[index]


async def pg_search_version(conn: AsyncConnection) -> tuple[int, ...]:
    """The installed version of the pg_search extension (e.g. (0, 27, 0))"""
    cur = await conn.execute("SELECT extversion FROM pg_extension WHERE extname = 'pg_search'")
    row = await cur.fetchone()
    if row is None:
        return ()
    return tuple(int(x) for x in re.findall(r"\d+", row["extversion"]))  # type: ignore[index, call-overload]


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
    """
    cur = await conn.execute(
        """SELECT p.partition_id, p.id AS project, coalesce(c.n, 0) AS documents
           FROM projects p
           LEFT JOIN (SELECT partition_id, project_pk, count(*) AS n FROM documents GROUP BY 1, 2) c
             ON c.partition_id = p.partition_id AND c.project_pk = p.pk
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
    if shared and shared[-1]["index_bytes"] < get_settings().partition_max_gb * 1024**3:
        return shared[-1]["partition_id"]
    partition_id = await next_partition_id(conn)
    try:
        await create_partition(conn, partition_id)
    except LockNotAvailable:
        if not shared:
            raise
        logging.warning(f"Could not create documents partition {partition_id} (lock timeout), using the current one")
        return shared[-1]["partition_id"]
    return partition_id


async def next_partition_id(conn: AsyncConnection) -> int:
    """The id for a new partition (one more than the highest, also counting partitions without a valid index)"""
    cur = await conn.execute(
        r"""SELECT coalesce(max((regexp_match(pg_get_expr(c.relpartbound, c.oid), 'IN \((\d+)\)'))[1]::int), 0) AS n
            FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = 'documents'::regclass"""
    )
    return (await cur.fetchone())["n"] + 1  # type: ignore[index]


async def drop_schema(conn: AsyncConnection, schema: str) -> None:
    """Drop the amcat schema and ALL its data. Only for tests and benchmarks!"""
    await conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
