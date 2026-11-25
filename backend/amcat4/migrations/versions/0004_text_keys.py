"""The text fields of a document as a small key in exact_fields, for 'exists' filters inside the search index

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-25

- A filter "this text field has a value" was the only filter that could not run inside the search index (pg_search
  'exists' needs a columnar field, and text fields are not): it was a SQL condition that reads the long text json of
  every hit. Now every document has the keys of its text fields that have a value as a short list in exact_fields
  (key _text, e.g. ["f1", "f2"]), which the index stores columnar, so the filter is a term in the index.
- The list is written by a trigger on documents (BEFORE INSERT OR UPDATE OF text_fields), not by the application
  code: it depends on all text fields of the row (also after a merge with the stored row), and every statement that
  writes text (upload, update by query, deleting or converting a field, copy, move) then keeps it right. Statements
  that do not write text_fields do not fire it.
- Documents stored before this migration get the list from a background job (amcat4.projects.jobs,
  backfill_text_keys). Existing text fields are marked with fields.text_exists_ready = false until the job of their
  project is done; until then the filter uses the SQL condition, with the same results (as fields.quarter_ready).
- No change to the documents table or the search index: exact_fields is indexed as a whole, so the new key needs no
  index rebuild.

All steps are idempotent.
"""

from typing import Sequence

from alembic import op

from amcat4.migrations.locking import alter_with_lock_timeout

revision: str = "0004"
down_revision: str | Sequence[str] | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# exact_fields with the list of text keys under _text (no key if the document has no text field). The same expression
# as amcat4.postgres.fields.with_text_keys_sql, which the backfill job uses (checked in a test)
TRIGGER_FUNCTION = """
CREATE OR REPLACE FUNCTION documents_text_keys() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    SELECT CASE WHEN count(k) = 0 THEN NEW.exact_fields - '_text'
                ELSE NEW.exact_fields || jsonb_build_object('_text', jsonb_agg(k)) END
    INTO NEW.exact_fields FROM jsonb_object_keys(NEW.text_fields) AS k;
    RETURN NEW;
END $$
"""


def upgrade() -> None:
    op.execute(TRIGGER_FUNCTION)
    # (added without a default first, so existing text fields are marked not ready, and new fields later get true)
    op.execute("ALTER TABLE fields ADD COLUMN IF NOT EXISTS text_exists_ready boolean")
    op.execute("UPDATE fields SET text_exists_ready = (type <> 'text') WHERE text_exists_ready IS NULL")
    op.execute(
        "ALTER TABLE fields ALTER COLUMN text_exists_ready SET DEFAULT true, ALTER COLUMN text_exists_ready SET NOT NULL"
    )
    # (with a lock timeout: creating a trigger locks documents and its partitions against writes; waiting for that
    # lock behind a long statement would block every other statement on documents)
    alter_with_lock_timeout(op.get_bind(), "DROP TRIGGER IF EXISTS documents_text_keys ON documents")
    alter_with_lock_timeout(
        op.get_bind(),
        "CREATE TRIGGER documents_text_keys BEFORE INSERT OR UPDATE OF text_fields ON documents "
        "FOR EACH ROW EXECUTE FUNCTION documents_text_keys()",
    )


def downgrade() -> None:
    alter_with_lock_timeout(op.get_bind(), "DROP TRIGGER IF EXISTS documents_text_keys ON documents")
    op.execute("DROP FUNCTION IF EXISTS documents_text_keys()")
    op.execute("DELETE FROM jobs WHERE type = 'backfill_text_keys'")  # (the 0003 code does not know this job type)
    op.execute("ALTER TABLE fields DROP COLUMN IF EXISTS text_exists_ready")
    # (the _text keys stay in exact_fields: the 0003 code ignores them. After a later upgrade the backfill job
    # compares every list with the text fields again, so lists that got out of date meanwhile are corrected)
