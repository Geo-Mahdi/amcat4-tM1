"""Frozen metadata of a release (JSON-LD) and the live columns next to it

Revision ID: 0006
Revises: 0005
Create Date: 2026-11-26

See docs/postgres-fast-v3/release-metadata.md. Columns added to releases:

Frozen (written once, when the release is made; NULL for releases made before this migration):
- metadata_jsonld jsonb: the description of the release (schema.org / DCAT JSON-LD). No document text.
- metadata_sha256 text: hex SHA-256 of the canonical JSON of metadata_jsonld (also stored in the manifest).
- versions jsonb: PostgreSQL, pg_search, amcat4 version and the migration revision at creation.
- field_definitions jsonb: the fields of the project at creation (name, type, identifier, metareader access).
- golden_queries jsonb: a few fixed queries with their results, to check that a rerun gives the same answer.

Live (may change after the release is made, shown next to the frozen metadata):
- metadata_public boolean: whether the metadata (never the texts) may be shown to anyone.
- withdrawn_at, withdrawn_reason: a withdrawn release keeps its rows; it is only marked.
- doi_status: none, draft, registered or findable.

Only new nullable columns or columns with a constant default: no table rewrite. Idempotent.
"""

from typing import Sequence

from alembic import op

revision: str = "0006"
down_revision: str | Sequence[str] | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

COLUMNS = [
    ("metadata_jsonld", "jsonb"),
    ("metadata_sha256", "text"),
    ("versions", "jsonb"),
    ("field_definitions", "jsonb"),
    ("golden_queries", "jsonb"),
    ("metadata_public", "boolean NOT NULL DEFAULT false"),
    ("withdrawn_at", "timestamptz"),
    ("withdrawn_reason", "text"),
    ("doi_status", "text NOT NULL DEFAULT 'none'"),
]

CHECK = "releases_doi_status_check"


def upgrade() -> None:
    adds = ", ".join(f"ADD COLUMN IF NOT EXISTS {name} {type}" for name, type in COLUMNS)
    op.execute(f"ALTER TABLE releases {adds}")
    op.execute(f"ALTER TABLE releases DROP CONSTRAINT IF EXISTS {CHECK}")
    op.execute(
        f"ALTER TABLE releases ADD CONSTRAINT {CHECK} CHECK (doi_status IN ('none', 'draft', 'registered', 'findable'))"
    )


def downgrade() -> None:
    op.execute(f"ALTER TABLE releases DROP CONSTRAINT IF EXISTS {CHECK}")
    drops = ", ".join(f"DROP COLUMN IF EXISTS {name}" for name, _ in COLUMNS)
    op.execute(f"ALTER TABLE releases {drops}")
