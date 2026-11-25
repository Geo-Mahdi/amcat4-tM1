"""Frozen releases (row versions with sequence numbers) and a separate table for tags and annotations

Revision ID: 0005
Revises: 0003
Create Date: 2026-11-25

See docs/postgres-fast-v3/RELEASES_AND_LABELS.md for the design. In short:

- A sequence change_seq: every write to documents or labels takes one number from it.
- documents gets created_seq (the number of the write that made this row version), retired_seq (the number of the
  write that ended it; 9223372036854775807 means "current") and doc_pk (the identity of a document over its row
  versions). A release is one number R: its rows are those with created_seq <= R AND retired_seq > R.
  Adding the columns does not rewrite the table: created_seq is added with the constant default 0 (all existing rows
  are older than any release) and the default is dropped at the end, so the application must always give the number.
  doc_pk stays empty (NULL) for existing rows, which means "the same as id"; a trigger gives every new row doc_pk = id,
  and a background job fills it for the existing rows of a project when its tags move to the label table
  (projects.labels_ready).
- The unique constraint on (partition_id, project_pk, doc_id) becomes a unique index over current rows only, because
  an older row version of a document has the same doc_id.
- The BM25 index is rebuilt ONCE per partition with the three new columns (real columns, not json keys: only real
  columns are pruned per segment and can be used in joins and aggregations over joins). Online, the same way as
  migration 0002: the new index is built next to the old one, per partition, then the old one is dropped. This needs
  disk space for a second copy of the search index while it runs. The index now has 15 of the 32 possible entries.
- releases: one row per release of a project (number, seq, who, when, DOI, manifest with counts and fingerprints).
- doc_labels: values that change after upload (tags now, annotations later), one row per (document, field, value),
  with the same created_seq / retired_seq columns and its own BM25 index. Not partitioned.
  fields.label_table says whether the values of a (tag) field are stored there instead of in documents.exact_fields.

All steps are idempotent, so a migration that was interrupted (e.g. during the index build) can be run again.
After this migration, servers of the older version must not write any more: their uploads fail (created_seq has no
default), which is on purpose, because they do not know about row versions.
"""

import logging
from typing import Sequence

from alembic import op
from sqlalchemy import text

from amcat4.migrations.locking import alter_with_lock_timeout

revision: str = "0005"
down_revision: str | Sequence[str] | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# The same key as amcat4.postgres.layout._REINDEX_LOCK: no automatic rebuild while the migration builds the index
REINDEX_LOCK = 4_000_002

# retired_seq of a current row (the largest bigint). A value, not NULL: pg_search pushes a comparison with a NOT NULL
# column into its index, also in joins
CURRENT = 9223372036854775807

BM25_OPTIONS = "WITH (mutable_segment_rows = 0)"  # as in 0001

# The columns of the BM25 index in 0002 (for the downgrade)
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

# 0002 plus doc_pk, created_seq and retired_seq. (A text column outside json for the main text, if that is decided,
# would be one more entry here, in the same rebuild: see the design document.)
BM25_0005 = """
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
    doc_pk,
    created_seq,
    retired_seq,
    (text_fields::pdb.unicode_words),
    (exact_fields::pdb.literal)
"""

DOC_ID_UNIQUE = "(partition_id, project_pk, doc_id)"
DOC_ID_LIVE_INDEX = "documents_doc_id_live"
OLD_DOC_ID_CONSTRAINT = "documents_partition_id_project_pk_doc_id_key"  # the name postgres gave it in 0001

# Gives a new row doc_pk = id (the identity column is filled before the trigger runs). A new version of an existing
# document is inserted with the doc_pk of the document, and is left alone. SET search_path is not needed: the function
# names no table.
TRIGGER_FUNCTION = """
CREATE OR REPLACE FUNCTION documents_set_doc_pk() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.doc_pk := NEW.id;
    RETURN NEW;
END
$$
"""

TABLES = [
    """
    CREATE TABLE IF NOT EXISTS releases (
        pk integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        project_pk integer NOT NULL REFERENCES projects(pk) ON DELETE CASCADE,
        number integer NOT NULL,  -- 1, 2, ... within the project
        seq bigint NOT NULL,  -- R: the release is every row with created_seq <= R AND retired_seq > R
        created_at timestamptz NOT NULL DEFAULT now(),
        created_by text,
        doi text,
        manifest jsonb NOT NULL DEFAULT '{}',  -- counts and fingerprints, see amcat4.projects.releases
        UNIQUE (project_pk, number),
        UNIQUE (project_pk, seq)
    )
    """,
    # field_pk references fields (and so the project: deleting a project deletes its fields and their labels).
    # project_pk is repeated here for the BM25 index (whole-project counts on this table alone).
    # created_by and created_at are what annotations need on top of tags: who gave the value, and when.
    f"""
    CREATE TABLE IF NOT EXISTS doc_labels (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        project_pk integer NOT NULL,
        doc_pk bigint NOT NULL,  -- documents.doc_pk: the document, whatever its row version
        field_pk integer NOT NULL REFERENCES fields(pk) ON DELETE CASCADE,
        value text COLLATE "C" NOT NULL,
        created_seq bigint NOT NULL,
        retired_seq bigint NOT NULL DEFAULT {CURRENT},
        created_by text,
        created_at timestamptz NOT NULL DEFAULT now()
    )
    """,
    # all versions of the labels of a document (reading the values of a page of results, also for a release)
    "CREATE INDEX IF NOT EXISTS doc_labels_doc ON doc_labels (doc_pk, field_pk)",
    # a document has a value once among its current labels
    f"""
    CREATE UNIQUE INDEX IF NOT EXISTS doc_labels_live ON doc_labels (doc_pk, field_pk, value)
        WHERE retired_seq = {CURRENT}
    """,
    # deleting a field (the foreign key), and plain SQL fallbacks that select on a value
    "CREATE INDEX IF NOT EXISTS doc_labels_field ON doc_labels (field_pk, value)",
    f"""
    CREATE INDEX IF NOT EXISTS doc_labels_bm25 ON doc_labels USING bm25 (
        id,
        project_pk,
        doc_pk,
        field_pk,
        (value::pdb.literal),
        created_seq,
        retired_seq
    ) {BM25_OPTIONS}
    """,
]

log = logging.getLogger("alembic.runtime.migration")


def _partitions() -> list[str]:
    rows = op.get_bind().execute(
        text("SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = 'documents'::regclass ORDER BY 1")
    )
    return list(rows.scalars())


def _build_index(name: str, using: str, partitions: list[str], unique: bool = False) -> None:
    """
    Create an index on the partitioned documents table without blocking reads or writes, as in migration 0002 (copied
    here: a migration does not depend on the code of another one; the only addition is `unique`). The parent index is
    created ON ONLY the parent (invalid until every partition has one), and each partition's index is built
    concurrently and attached. Must run in autocommit mode. Idempotent: an invalid leftover of an interrupted build is
    dropped and rebuilt; a partition index with the name that is attached to another parent index is left alone, and
    the partition gets an index under a free name.
    """
    bind = op.get_bind()
    create = "CREATE UNIQUE INDEX" if unique else "CREATE INDEX"
    bind.execute(text(f"{create} IF NOT EXISTS {name} ON ONLY documents {using}"))
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
            bind.execute(text(f"{create} CONCURRENTLY {child} ON {table} {using}"))
        if state is None or not state.attached_here:
            bind.execute(text(f"ALTER INDEX {name} ATTACH PARTITION {child}"))


def _has_bm25_column(column: str) -> bool:
    """Whether the BM25 index of the documents table (documents_bm25) exists, is complete, and has this column"""
    row = (
        op.get_bind()
        .execute(
            text(
                """SELECT pg_get_indexdef(ix.indexrelid) AS def, ix.indisvalid AS valid FROM pg_index ix
                   JOIN pg_class c ON c.oid = ix.indexrelid
                   WHERE c.relname = 'documents_bm25' AND c.relnamespace = current_schema()::regnamespace"""
            )
        )
        .one_or_none()
    )
    return _has(row, column)


def _has(row, column: str) -> bool:
    """Whether this index (a row with its definition and validity; None: no index) is complete and has the column"""
    if row is None or not row.valid:
        return False
    entries = [e.strip() for e in row[0].split(" USING bm25 (", 1)[1].split(") WITH")[0].split(",")]
    return column in entries


def _replace_bm25(columns: str) -> None:
    """
    Rebuild the BM25 index (documents_bm25, with documents_p<N>_bm25 on the partitions) with these columns, online, as
    in migration 0002: build the new one next to the old one, then drop the old one (a short exclusive lock) and
    rename. Must run in autocommit mode. Takes the reindex lock, so the automatic maintenance does not rebuild meanwhile.
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


def _doc_id_constraint() -> str | None:
    """The name of the unique constraint on (partition_id, project_pk, doc_id) of 0001, or None if it is gone"""
    return (
        op.get_bind()
        .execute(
            text(
                """SELECT conname FROM pg_constraint WHERE conrelid = 'documents'::regclass AND contype = 'u'
                   AND pg_get_constraintdef(oid) = 'UNIQUE (partition_id, project_pk, doc_id)'"""
            )
        )
        .scalar_one_or_none()
    )


def upgrade() -> None:
    bind = op.get_bind()
    op.execute("CREATE SEQUENCE IF NOT EXISTS change_seq")
    # (with a lock timeout: waiting for the lock behind a long query would block every other query on documents)
    alter_with_lock_timeout(
        bind,
        f"""ALTER TABLE documents
                ADD COLUMN IF NOT EXISTS created_seq bigint NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS retired_seq bigint NOT NULL DEFAULT {CURRENT},
                ADD COLUMN IF NOT EXISTS doc_pk bigint""",
    )
    # The id column becomes GENERATED BY DEFAULT (it was ALWAYS; only the catalog changes): when a document that is in
    # a release is updated, the old row version keeps its id (so the order and the cursors of the release never
    # change) and the current row moves to a new id, which an ALWAYS identity does not allow in one UPDATE
    # (see amcat4.postgres.versions.versioned_update)
    alter_with_lock_timeout(bind, "ALTER TABLE documents ALTER COLUMN id SET GENERATED BY DEFAULT")
    op.execute(TRIGGER_FUNCTION)
    alter_with_lock_timeout(
        bind,
        """CREATE OR REPLACE TRIGGER documents_doc_pk BEFORE INSERT ON documents
           FOR EACH ROW WHEN (NEW.doc_pk IS NULL) EXECUTE FUNCTION documents_set_doc_pk()""",
    )
    # Projects that have documents from before this migration: their rows have no doc_pk yet, so their tags cannot
    # move to the label table until a background job has filled it (amcat4.projects.jobs, backfill_labels).
    # (Added without a default first, so existing projects are marked, and new projects later get true.)
    op.execute("ALTER TABLE projects ADD COLUMN IF NOT EXISTS labels_ready boolean")
    op.execute(
        """UPDATE projects p SET labels_ready = NOT EXISTS (
               SELECT 1 FROM documents d WHERE d.project_pk = p.pk
               AND d.partition_id IN (SELECT partition_id FROM project_partitions pp WHERE pp.project_pk = p.pk))
           WHERE labels_ready IS NULL"""
    )
    op.execute("ALTER TABLE projects ALTER COLUMN labels_ready SET DEFAULT true, ALTER COLUMN labels_ready SET NOT NULL")
    op.execute("ALTER TABLE fields ADD COLUMN IF NOT EXISTS label_table boolean NOT NULL DEFAULT false")
    for statement in TABLES:
        op.execute(statement)
    # The index builds below cannot run in a transaction; this commits the steps above
    with op.get_context().autocommit_block():
        partitions = _partitions()
        _build_index(DOC_ID_LIVE_INDEX, f"{DOC_ID_UNIQUE} WHERE retired_seq = {CURRENT}", partitions, unique=True)
        if not _has_bm25_column("retired_seq"):
            _replace_bm25(BM25_0005)
    # Only now that the new unique index exists (so document ids are unique at every moment)
    old = _doc_id_constraint()
    if old:
        alter_with_lock_timeout(op.get_bind(), f"ALTER TABLE documents DROP CONSTRAINT {old}")
    # From here on the application gives every new row its number (amcat4.postgres.versions)
    alter_with_lock_timeout(op.get_bind(), "ALTER TABLE documents ALTER COLUMN created_seq DROP DEFAULT")


def downgrade() -> None:
    bind = op.get_bind()
    # The 0003 code knows no row versions and no label table: it would show old versions as documents and lose the tags
    if bind.execute(text("SELECT count(*) FROM releases")).scalar_one():
        raise RuntimeError("There are releases: delete them (and run the purge job) before downgrading")
    if bind.execute(text("SELECT count(*) FROM fields WHERE label_table")).scalar_one():
        raise RuntimeError("Tag fields are stored in the label table: the older version cannot read them")
    # row versions that no release needs (there are no releases)
    op.execute(f"DELETE FROM documents WHERE retired_seq <> {CURRENT}")
    if not _doc_id_constraint():
        alter_with_lock_timeout(
            op.get_bind(), f"ALTER TABLE documents ADD CONSTRAINT {OLD_DOC_ID_CONSTRAINT} UNIQUE {DOC_ID_UNIQUE}"
        )
    with op.get_context().autocommit_block():
        if _has_bm25_column("retired_seq"):
            _replace_bm25(BM25_0002)
        op.get_bind().execute(text(f"DROP INDEX IF EXISTS {DOC_ID_LIVE_INDEX}"))
    op.execute("DELETE FROM jobs WHERE type IN ('purge_releases', 'backfill_labels')")
    op.execute("DROP TABLE IF EXISTS doc_labels")
    op.execute("DROP TABLE IF EXISTS releases")
    alter_with_lock_timeout(op.get_bind(), "ALTER TABLE documents ALTER COLUMN id SET GENERATED ALWAYS")
    alter_with_lock_timeout(op.get_bind(), "DROP TRIGGER IF EXISTS documents_doc_pk ON documents")
    op.execute("DROP FUNCTION IF EXISTS documents_set_doc_pk()")
    alter_with_lock_timeout(
        op.get_bind(),
        "ALTER TABLE documents DROP COLUMN IF EXISTS created_seq, DROP COLUMN IF EXISTS retired_seq, "
        "DROP COLUMN IF EXISTS doc_pk",
    )
    op.execute("ALTER TABLE projects DROP COLUMN IF EXISTS labels_ready")
    op.execute("ALTER TABLE fields DROP COLUMN IF EXISTS label_table")
    op.execute("DROP SEQUENCE IF EXISTS change_seq")
