"""Safe views over the public release metadata, for the SPARQL endpoint (Ontop, deploy/ontop)

Revision ID: 0007
Revises: 0006
Create Date: 2026-11-26

Ontop (a virtual knowledge graph) answers SPARQL by translating it to SQL on these views. It connects as a role that
can read only these views (deploy/ontop/role.sql, run by an administrator: creating roles needs more rights than the
migrations have). See docs/postgres-fast-v3/sparql-ontop.md.

- sparql_projects: one row per project with at least one release whose metadata is public.
- sparql_releases: one row per release whose metadata is public (releases.metadata_public, migration 0006), of a
  project that still exists.
- sparql_release_fields: one row per (public release, field) from releases.field_definitions, only fields that are not
  of type text (or vector) and that metareaders may see completely (metareader access "read": the same rule as the
  API, see amcat4.systemdata.fields._field_access_for_role), with the number of documents with a value from the
  manifest (matched by field pk).
- sparql_field_values: the top values (at most 10) of the keyword and tag fields of sparql_release_fields, with
  their document counts, as the golden queries of the release (migration 0006) stored them. Not computed here: the
  views never read documents.

The views read only the tables releases and projects (never documents, doc_labels or users), and only the columns
listed here. They are security barrier views: a condition that a client adds cannot see the rows that the view leaves
out. They are owned by the migration user, so the SPARQL role needs SELECT on the views only.

The IRIs are built from a base IRI, the setting amcat4.sparql_base_iri of the session (role.sql sets it for the SPARQL
role, or set it in the JDBC url), by default http://localhost:5000. A release is <base>/index/<project id>/releases/<n>,
the same as the @id of its JSON-LD (migration 0006) when the base is AMCAT4_RELEASE_ID_BASE (or the host).
"""

from typing import Sequence

from alembic import op

revision: str = "0007"
down_revision: str | Sequence[str] | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# The same IRIs as the @id of the JSON-LD of a release (amcat4.projects.release_metadata.release_url), when
# amcat4.sparql_base_iri is set to AMCAT4_RELEASE_ID_BASE (or the host); the default is the default host
BASE_IRI = "rtrim(coalesce(nullif(current_setting('amcat4.sparql_base_iri', true), ''), 'http://localhost:5000'), '/')"
PROJECT_IRI = f"{BASE_IRI} || '/index/' || p.id"
RELEASE_IRI = f"{PROJECT_IRI} || '/releases/' || r.number"
# what a release counts as public: its metadata is published, and the project still exists (the join)
PUBLIC = "FROM releases r JOIN projects p ON p.pk = r.project_pk WHERE r.metadata_public"
# the fields of a public release that may be shown (f.def: an entry of field_definitions, see
# amcat4.projects.release_metadata.field_definitions): not text, and metareaders may read it completely
FIELDS = """
    FROM releases r JOIN projects p ON p.pk = r.project_pk
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE jsonb_typeof(r.field_definitions) WHEN 'array' THEN r.field_definitions ELSE '[]'::jsonb END
    ) WITH ORDINALITY AS f(def, position)
    WHERE r.metadata_public
      AND jsonb_typeof(f.def) = 'object'
      AND f.def->>'name' IS NOT NULL
      AND coalesce(f.def->>'type', 'text') NOT IN ('text', 'vector')
      AND f.def->'metareader'->>'access' = 'read'
"""
FIELD_IRI = f"""{RELEASE_IRI} || '/fields/' || CASE WHEN f.def->>'name' ~ '^[A-Za-z0-9_.-]+$' THEN f.def->>'name'
                                                    ELSE 'md5-' || md5(f.def->>'name') END"""

VIEWS = {
    "sparql_projects": f"""
        SELECT {PROJECT_IRI} AS project_iri,
               p.id AS project_id,
               p.name AS project_name,
               count(*) AS release_count
        {PUBLIC}
        GROUP BY p.pk, p.id, p.name
    """,
    "sparql_releases": f"""
        SELECT {RELEASE_IRI} AS release_iri,
               {PROJECT_IRI} AS project_iri,
               p.id AS project_id,
               p.name AS project_name,
               r.number AS release_number,
               r.created_at,
               r.doi,
               CASE WHEN r.doi IS NOT NULL AND r.doi_status IN ('registered', 'findable')
                    THEN CASE WHEN r.doi ~* '^https?://' THEN r.doi ELSE 'https://doi.org/' || r.doi END
               END AS doi_iri,
               r.doi_status,
               r.withdrawn_at,
               r.withdrawn_at IS NOT NULL AS withdrawn,
               r.metadata_sha256,
               CASE WHEN jsonb_typeof(r.manifest->'documents') = 'number' THEN (r.manifest->>'documents')::bigint
               END AS document_count
        {PUBLIC}
    """,
    "sparql_release_fields": f"""
        SELECT {RELEASE_IRI} AS release_iri,
               {FIELD_IRI} AS field_iri,
               f.def->>'name' AS field_name,
               f.def->>'type' AS field_type,
               coalesce(f.def->'identifier' = 'true'::jsonb, false) AS is_identifier,
               CASE WHEN jsonb_typeof(f.def->'description') = 'string' THEN f.def->>'description' END AS description,
               f.position AS field_position,
               'field:' || (f.def->>'pk') AS property_id,
               (SELECT (m.value->>'documents')::bigint FROM jsonb_each(
                    CASE jsonb_typeof(r.manifest->'fields') WHEN 'object' THEN r.manifest->'fields' ELSE '{{}}'::jsonb END
                ) m
                WHERE jsonb_typeof(m.value) = 'object' AND m.value->'pk' = f.def->'pk'
                  AND jsonb_typeof(m.value->'documents') = 'number'
                LIMIT 1) AS document_count
        {FIELDS}
    """,
    # The top values of keyword and tag fields, from the golden queries of the release ("top_values:<field>", with
    # field_pk; see amcat4.projects.release_metadata.golden_query_specs): only for the fields shown above
    "sparql_field_values": f"""
        SELECT fl.release_iri,
               fl.field_iri,
               fl.field_iri || '/values/' || v.rank AS value_iri,
               v.item->>'value' AS value,
               (v.item->>'n')::bigint AS document_count,
               v.rank AS value_rank
        FROM (
            SELECT {RELEASE_IRI} AS release_iri, {FIELD_IRI} AS field_iri, f.def->'pk' AS field_pk,
                   r.golden_queries
            {FIELDS}
              AND f.def->>'type' IN ('keyword', 'tag')
        ) fl
        CROSS JOIN LATERAL jsonb_array_elements(
            CASE jsonb_typeof(fl.golden_queries) WHEN 'array' THEN fl.golden_queries ELSE '[]'::jsonb END
        ) AS g(query)
        CROSS JOIN LATERAL jsonb_array_elements(
            CASE jsonb_typeof(g.query->'result'->'values') WHEN 'array' THEN g.query->'result'->'values'
                 ELSE '[]'::jsonb END
        ) WITH ORDINALITY AS v(item, rank)
        WHERE starts_with(g.query->>'name', 'top_values:')
          AND jsonb_typeof(g.query) = 'object'
          AND g.query->'field_pk' = fl.field_pk
          AND jsonb_typeof(v.item->'n') = 'number'
          AND jsonb_typeof(v.item->'value') IN ('string', 'number', 'boolean')
    """,
}


def upgrade() -> None:
    for name, query in VIEWS.items():
        # (DROP first: CREATE OR REPLACE cannot change the columns of an existing view)
        op.execute(f"DROP VIEW IF EXISTS {name}")
        op.execute(f"CREATE VIEW {name} WITH (security_barrier) AS {query}")
        op.execute(f"COMMENT ON VIEW {name} IS 'Public release metadata for the SPARQL endpoint (deploy/ontop)'")


def downgrade() -> None:
    for name in reversed(list(VIEWS)):
        op.execute(f"DROP VIEW IF EXISTS {name}")
