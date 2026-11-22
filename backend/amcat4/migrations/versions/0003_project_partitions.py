"""Moving a project to another partition in batches

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-22

In 0001, the documents table has a foreign key (project_pk, partition_id) to projects(pk, partition_id), with
ON UPDATE CASCADE: changing projects.partition_id moves all documents of the project in one statement (one huge
transaction for a big project, with every row locked until the end). To move a project in batches, its documents
must be in two partitions for a while, which that foreign key does not allow.

So documents now reference project_partitions (project_pk, partition_id): the partitions a project has documents in.
Normally that is one row per project (kept in sync with projects.partition_id by a trigger); during a move (the
move_project job, see amcat4.projects.partitions) also the target partition (projects.moving_to). The trigger keeps
the old behaviour of changing projects.partition_id directly: the documents are moved in the same statement.
projects.own_partition marks a project that has a partition of its own: new projects are not assigned to it.

Deleting a project still deletes its documents (projects -> project_partitions -> documents, ON DELETE CASCADE).
The new foreign key is added NOT VALID and then validated in a separate transaction: the validation reads all
documents, but does not block reads or writes.
"""

from typing import Sequence

from alembic import op
from sqlalchemy import text

revision: str = "0003"
down_revision: str | Sequence[str] | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

NEW_FK = "documents_project_partition_fkey"
OLD_FK = "documents_project_pk_partition_id_fkey"  # the name postgres gave the foreign key of 0001

TRIGGER_FUNCTION = """
CREATE OR REPLACE FUNCTION project_partition_sync() RETURNS trigger LANGUAGE plpgsql
SET search_path FROM CURRENT AS $$
BEGIN
    INSERT INTO project_partitions (project_pk, partition_id) VALUES (NEW.pk, NEW.partition_id) ON CONFLICT DO NOTHING;
    IF TG_OP = 'UPDATE' AND NEW.partition_id <> OLD.partition_id THEN
        -- Moving directly (as in 0001): all documents in this statement. The move_project job moves them in batches
        -- first, so this then finds none.
        UPDATE documents SET partition_id = NEW.partition_id WHERE partition_id = OLD.partition_id AND project_pk = NEW.pk;
        DELETE FROM project_partitions WHERE project_pk = NEW.pk AND partition_id = OLD.partition_id;
    END IF;
    RETURN NULL;
END
$$
"""


def _fk_name(referenced: str) -> str | None:
    return (
        op.get_bind()
        .execute(
            text(
                """SELECT conname FROM pg_constraint WHERE conrelid = 'documents'::regclass AND contype = 'f'
               AND confrelid = to_regclass(:t)"""
            ),
            {"t": referenced},
        )
        .scalar_one_or_none()
    )


def upgrade() -> None:
    op.execute(
        """CREATE TABLE IF NOT EXISTS project_partitions (
               project_pk integer NOT NULL REFERENCES projects(pk) ON DELETE CASCADE,
               partition_id integer NOT NULL,
               PRIMARY KEY (project_pk, partition_id)
           )"""
    )
    op.execute("INSERT INTO project_partitions SELECT pk, partition_id FROM projects ON CONFLICT DO NOTHING")
    op.execute(
        "ALTER TABLE projects ADD COLUMN IF NOT EXISTS moving_to integer, "
        "ADD COLUMN IF NOT EXISTS own_partition boolean NOT NULL DEFAULT false"
    )
    op.execute(TRIGGER_FUNCTION)
    op.execute("DROP TRIGGER IF EXISTS projects_partition ON projects")
    op.execute(
        "CREATE TRIGGER projects_partition AFTER INSERT OR UPDATE OF partition_id ON projects "
        "FOR EACH ROW EXECUTE FUNCTION project_partition_sync()"
    )
    old = _fk_name("projects")
    if old:
        op.execute(f"ALTER TABLE documents DROP CONSTRAINT {old}")
    if not _fk_name("project_partitions"):
        op.execute(
            f"""ALTER TABLE documents ADD CONSTRAINT {NEW_FK} FOREIGN KEY (project_pk, partition_id)
                REFERENCES project_partitions (project_pk, partition_id) ON DELETE CASCADE NOT VALID"""
        )
    # Validating reads all documents: outside the transaction that changed the table (and holds an exclusive lock)
    with op.get_context().autocommit_block():
        op.execute(f"ALTER TABLE documents VALIDATE CONSTRAINT {NEW_FK}")


def downgrade() -> None:
    moving = op.get_bind().execute(text("SELECT count(*) FROM projects WHERE moving_to IS NOT NULL")).scalar_one()
    if moving:
        raise RuntimeError("A project is being moved to another partition: finish or undo the move first")
    if _fk_name("project_partitions"):
        op.execute(f"ALTER TABLE documents DROP CONSTRAINT {NEW_FK}")
    op.execute(
        f"""ALTER TABLE documents ADD CONSTRAINT {OLD_FK} FOREIGN KEY (project_pk, partition_id)
            REFERENCES projects (pk, partition_id) ON UPDATE CASCADE ON DELETE CASCADE NOT VALID"""
    )
    op.execute("DROP TRIGGER IF EXISTS projects_partition ON projects")
    op.execute("DROP FUNCTION IF EXISTS project_partition_sync()")
    op.execute("DROP TABLE IF EXISTS project_partitions")
    op.execute("ALTER TABLE projects DROP COLUMN IF EXISTS moving_to, DROP COLUMN IF EXISTS own_partition")
    with op.get_context().autocommit_block():
        op.execute(f"ALTER TABLE documents VALIDATE CONSTRAINT {OLD_FK}")
