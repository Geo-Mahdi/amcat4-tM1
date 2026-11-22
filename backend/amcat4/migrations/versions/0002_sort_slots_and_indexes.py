"""Sort slots, an id index per project, and the search index rebuilt with them

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-22

- Five more sort slots in the documents table: sort_kw1, sort_kw2, sort_kw3 (keyword or url fields, like source),
  sort_num1 (number or integer fields) and sort_date2 (a second date field). A project maps a field to a slot with
  fast_sort; sorting on a slot column is a Top-K scan inside the search index (pg_search cannot do that for json
  keys). Adding empty columns does not rewrite the table. fields.sort_slot_ready is false while a background job fills
  a slot for an existing project (until then, sorting on the field uses the slower path).
- A B-tree index on (partition_id, project_pk, id): listing and downloading the documents of a project without a
  query in id order, without walking over the rows of the other projects in the partition.
- The BM25 search index is rebuilt once, with the new slot columns. Every other entry and option of 0001 is kept.
  The rebuild is online: the new index is built next to the old one (per partition, CREATE INDEX CONCURRENTLY), then
  the old one is dropped. This needs disk space for a second copy of the search index while it runs.
- If the database has an older pg_search version than the server's library (after upgrading the docker image of
  an existing database), the extension is updated first, so the new index is built and used by the new version.
- lz4 compression for new values of text_fields (the long texts): faster to read than the default (pglz). Only new
  rows: existing rows keep their compression.
- Date fields get a derived quarter key (see amcat4.postgres.fields.DATE_DERIVED). New values get it at upload;
  existing date fields are marked with fields.quarter_ready = false and filled by a background job (until then,
  quarter aggregations use the slower path, with the same results).

All steps are idempotent, so a migration that was interrupted (e.g. during the index build) can be run again.
"""

import logging
from typing import Sequence

from alembic import op
from sqlalchemy import text

from amcat4.migrations.locking import alter_with_lock_timeout

revision: str = "0002"
down_revision: str | Sequence[str] | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# The same key as amcat4.postgres.layout._REINDEX_LOCK: no automatic rebuild while the migration builds the index
REINDEX_LOCK = 4_000_002

# Text slots in byte order (COLLATE "C"), the order of Elasticsearch, and of the sort on keyword values without a slot
# (AMCAT4_BYTE_ORDER_SORT), so the order does not change when a field gets a slot
SLOT_COLUMNS = {
    "sort_kw1": 'text COLLATE "C"',
    "sort_kw2": 'text COLLATE "C"',
    "sort_kw3": 'text COLLATE "C"',
    "sort_num1": "numeric",
    "sort_date2": "timestamptz",
}

BM25_OPTIONS = "WITH (mutable_segment_rows = 0)"  # as in 0001

# The columns of the BM25 index in 0001
BM25_0001 = """
    id,
    partition_id,
    project_pk,
    date,
    (source::pdb.literal),
    (text_fields::pdb.unicode_words),
    (exact_fields::pdb.literal)
"""

# 0001 plus the slot columns (text slots as literals, like source: exact values, sortable)
BM25_0002 = """
    id,
    partition_id,
    project_pk,
    date,
    (source::pdb.literal),
    (sort_kw1::pdb.literal),
    (sort_kw2::pdb.literal),
    (sort_kw3::pdb.literal),
    sort_num1,
    sort_date2,
    (text_fields::pdb.unicode_words),
    (exact_fields::pdb.literal)
"""

log = logging.getLogger("alembic.runtime.migration")


def _partitions() -> list[str]:
    rows = op.get_bind().execute(
        text("SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = 'documents'::regclass ORDER BY 1")
    )
    # (regclass text can be schema qualified if the schema is not on the search path; it always is here)
    return list(rows.scalars())


def _update_pg_search() -> None:
    """Update the pg_search extension if the server has a newer version than the database (after an image upgrade)"""
    bind = op.get_bind()
    row = bind.execute(
        text("SELECT default_version, installed_version FROM pg_available_extensions WHERE name = 'pg_search'")
    ).one()
    if row.installed_version == row.default_version:
        return
    log.info(f"Updating the pg_search extension from {row.installed_version} to {row.default_version}")
    try:
        with bind.begin_nested():  # a savepoint: a failure (e.g. not the owner of the extension) is not fatal
            bind.execute(text("ALTER EXTENSION pg_search UPDATE"))
    except Exception as e:
        log.warning(f"Could not update pg_search ({e}); run ALTER EXTENSION pg_search UPDATE as its owner")


def _set_compression(method: str) -> None:
    # SET COMPRESSION on the partitioned table is only the default for new partitions; existing partitions need their
    # own (amcat4.postgres.layout.create_partition copies it with LIKE ... INCLUDING COMPRESSION)
    bind = op.get_bind()
    try:
        with bind.begin_nested():
            for table in ["documents", *_partitions()]:
                bind.execute(text(f"ALTER TABLE {table} ALTER COLUMN text_fields SET COMPRESSION {method}"))
    except Exception as e:  # e.g. a postgres built without lz4
        log.warning(f"Could not set {method} compression for text_fields ({e}); keeping the default")


def _build_index(name: str, using: str, partitions: list[str]) -> None:
    """
    Create an index on the partitioned documents table without blocking reads or writes: CREATE INDEX on a partitioned
    table cannot be CONCURRENTLY, so the parent index is created ON ONLY the parent (invalid until every partition
    has one), and each partition's index is built concurrently and attached. Must run in autocommit mode.
    Idempotent: an invalid leftover of an interrupted build is dropped and rebuilt. A partition index with the name
    that is attached to another parent index (e.g. after an interrupted run of _replace_bm25 renamed the parents) is
    left alone, and the partition gets an index under a free name: attaching it would fail, and taking it as done
    would leave the new parent without an index for the partition (and dropping the other parent drops that one).
    """
    bind = op.get_bind()
    bind.execute(text(f"CREATE INDEX IF NOT EXISTS {name} ON ONLY documents {using}"))
    for table in partitions:
        base = f"{table}_{name.removeprefix('documents_')}"
        child, suffix = base, 0
        while True:
            state = bind.execute(
                text(
                    """SELECT ix.indisvalid AS valid,
                              (SELECT i.inhparent = CAST(:parent AS regclass) FROM pg_inherits i
                               WHERE i.inhrelid = ix.indexrelid) AS attached_here
                       FROM pg_index ix JOIN pg_class c ON c.oid = ix.indexrelid
                       WHERE c.relname = :child AND c.relnamespace = current_schema()::regnamespace"""
                ),
                {"child": child, "parent": name},
            ).one_or_none()
            if state is None or state.attached_here is not False:  # free, not attached, or attached to this parent
                break
            suffix += 1
            child = f"{base}_{suffix}"
        if state is not None and not state.valid:
            bind.execute(text(f"DROP INDEX CONCURRENTLY {child}"))
            state = None
        if state is None:
            log.info(f"Building index {child}")
            bind.execute(text(f"CREATE INDEX CONCURRENTLY {child} ON {table} {using}"))
        if state is None or not state.attached_here:
            bind.execute(text(f"ALTER INDEX {name} ATTACH PARTITION {child}"))


def _replace_bm25(columns: str) -> None:
    """
    Rebuild the BM25 index (documents_bm25, with documents_p<N>_bm25 on the partitions) with these columns, online:
    build the new one next to the old one, then drop the old one (which takes a short exclusive lock) and rename.
    Must run in autocommit mode. Takes the reindex lock, so the automatic maintenance does not rebuild meanwhile.
    """
    bind = op.get_bind()
    bind.execute(text("SELECT pg_advisory_lock(:key)"), {"key": REINDEX_LOCK})
    try:
        partitions = _partitions()
        size = bind.execute(
            text(
                """SELECT coalesce(sum(pg_relation_size(c.oid)), 0) AS bytes FROM pg_index ix
                   JOIN pg_class c ON c.oid = ix.indexrelid JOIN pg_am am ON am.oid = c.relam AND am.amname = 'bm25'
                   JOIN pg_inherits i ON i.inhrelid = ix.indrelid WHERE i.inhparent = 'documents'::regclass"""
            )
        ).scalar_one()
        log.info(f"Rebuilding the search index ({size / 1024**3:.1f} GB now: about as much extra disk is needed meanwhile)")
        _build_index("documents_bm25_new", f"USING bm25 ({columns}) {BM25_OPTIONS}", partitions)
        bind.execute(text("DROP INDEX IF EXISTS documents_bm25"))
        bind.execute(text("ALTER INDEX documents_bm25_new RENAME TO documents_bm25"))
        for table in partitions:
            bind.execute(text(f"ALTER INDEX IF EXISTS {table}_bm25_new RENAME TO {table}_bm25"))
    finally:
        bind.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": REINDEX_LOCK})


def upgrade() -> None:
    _update_pg_search()
    columns = ", ".join(f"ADD COLUMN IF NOT EXISTS {name} {definition}" for name, definition in SLOT_COLUMNS.items())
    # (with a lock timeout: waiting for the lock behind a long query would block every other query on documents)
    alter_with_lock_timeout(op.get_bind(), f"ALTER TABLE documents {columns}")
    op.execute("ALTER TABLE fields ADD COLUMN IF NOT EXISTS sort_slot_ready boolean NOT NULL DEFAULT true")
    # existing date fields get their quarter keys from a background job (amcat4.projects.jobs, backfill_quarter)
    op.execute("ALTER TABLE fields ADD COLUMN IF NOT EXISTS quarter_ready boolean")
    # (added without a default first, so existing date fields are marked not ready, and new fields later get true)
    op.execute("UPDATE fields SET quarter_ready = (type <> 'date') WHERE quarter_ready IS NULL")
    op.execute("ALTER TABLE fields ALTER COLUMN quarter_ready SET DEFAULT true, ALTER COLUMN quarter_ready SET NOT NULL")
    _set_compression("lz4")
    # The index builds below cannot run in a transaction; this commits the steps above
    with op.get_context().autocommit_block():
        _build_index("documents_project_id", "(partition_id, project_pk, id)", _partitions())
        _replace_bm25(BM25_0002)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        _replace_bm25(BM25_0001)
        op.get_bind().execute(text("DROP INDEX IF EXISTS documents_project_id"))
    _set_compression("default")
    # job types that the 0001 code does not know (its worker would fail them)
    op.execute("DELETE FROM jobs WHERE type IN ('fill_sort_slot', 'backfill_quarter', 'vacuum', 'move_project')")
    # fields mapped to the new slots lose their slot (0001 only knows date and source)
    op.execute("UPDATE fields SET sort_slot = NULL WHERE sort_slot NOT IN ('date', 'source')")
    # a slot that a job was still filling: 0001 has no readiness flag, so it would sort on a half-filled column
    op.execute("UPDATE fields SET sort_slot = NULL WHERE NOT sort_slot_ready")
    op.execute("ALTER TABLE fields DROP COLUMN IF EXISTS sort_slot_ready, DROP COLUMN IF EXISTS quarter_ready")
    alter_with_lock_timeout(
        op.get_bind(), f"ALTER TABLE documents {', '.join(f'DROP COLUMN IF EXISTS {name}' for name in SLOT_COLUMNS)}"
    )
    # (the derived quarter keys stay in exact_fields: 0001 ignores them)
