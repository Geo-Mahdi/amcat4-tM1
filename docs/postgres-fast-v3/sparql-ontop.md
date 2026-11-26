# SPARQL over release metadata with Ontop (phase 6h)

MEDem plans to keep its ATLAS catalogue and Meteor "as a knowledge graph". To take part, AmCAT offers its public
release metadata as RDF, queryable with SPARQL, without copying anything out of PostgreSQL.

## Why a virtual graph (Ontop) and not a triple store

| | Ontop (chosen) | copy into a triple store |
| --- | --- | --- |
| freshness | every answer comes from the current database | stale until the next export |
| what can leak | only what the views select: the boundary is SQL, reviewed in one migration | whatever the export job writes; a second copy to secure, back up and delete from |
| withdrawing / unpublishing | immediate (the row leaves the view) | needs a delete in the store |
| operations | one stateless container, no storage | a second database with its own storage, upgrades and backups |
| query power | SPARQL 1.1 queries, translated to SQL; no reasoning beyond the mapping | full SPARQL, inference, federation |
| cost when unused | nothing (off by default) | the store keeps running |

The metadata is small (a few rows per release), so the translation overhead of Ontop does not matter, and the
security argument dominates: the endpoint can never show more than three views, and the role it uses can read
nothing else.

## What is exposed

Migration 0007 adds three security barrier views. They read the tables `releases` (with the columns of migration
0006) and `projects` only.

`sparql_projects`: one row per project with at least one public release:
`project_iri, project_id, project_name, release_count`.

`sparql_releases`: one row per release with `metadata_public`, of a project that still exists:
`release_iri, project_iri, project_id, project_name, release_number, created_at, doi, doi_iri (only for a
registered or findable DOI), doi_status, withdrawn_at, withdrawn, metadata_sha256, document_count (from the
manifest)`.

`sparql_release_fields`: one row per (public release, field) from `releases.field_definitions`, only fields that
are not of type text and whose metareader access is `read` (the same rule the API uses for metareaders):
`release_iri, field_iri, field_name, field_type, field_identifier, field_position, document_count (from the
manifest)`.

There is no view of field values: the manifest has counts per field, not per value, and computing them would mean
reading documents.

IRIs are `<base>projects/<id>` and `<base>projects/<id>/releases/<n>` (fields: `.../fields/<name>`), with
`<base>` the session setting `amcat4.sparql_base_iri` (set for the role by `role.sql`), by default `urn:amcat4:`.

The mapping (`deploy/ontop/mapping.ttl`) uses DCAT 3 (`dcat:DatasetSeries` for a project, `dcat:Dataset` with
`dcat:inSeries` and `dcat:version` for a release), Dublin Core (`dct:identifier`, `dct:issued`, `dct:title`),
schema.org (`schema:Dataset`, `schema:variableMeasured` with `schema:PropertyValue` nodes for fields,
`schema:sameAs` for the DOI), PROV-O (`prov:generatedAtTime`, `prov:invalidatedAtTime` for a withdrawn release)
and a few `amcat:` terms (`https://amcat.nl/ns/amcat4#`: `documentCount`, `fieldType`, `doiStatus`, `withdrawn`,
`metadataSha256`, `releaseCount`).

Never exposed: documents, texts, values, tags and annotations (the views do not touch `documents` or
`doc_labels`), users and roles, private releases, text fields, fields hidden from metareaders, the free-text reason
of a withdrawal.

## The database role

The migration cannot create roles (that needs more rights than the application has). `deploy/ontop/role.sql`
(run by an administrator with psql) creates `amcat_sparql`: NOLOGIN unless a password is given, no superuser or
create rights, read-only sessions, `statement_timeout` 10 s, USAGE on the schema, SELECT on the `sparql_*` views,
and everything else on the schema revoked. The views run with the rights of their owner, so the role needs no
rights on the tables.

## Switching it on

See `deploy/ontop/README.md`: run `role.sql`, create `ontop.properties`, set `AMCAT4_SPARQL=on`, and start compose
with `--profile sparql`. The `ontop` service uses `ontop/ontop:5.5.0` (pinned). Caddy sends `/sparql` to it only
when `AMCAT4_SPARQL=on`.

## Limits

- Query timeout 10 s in Ontop (`ontop.query.defaultTimeout`) and 10 s in postgres (`statement_timeout`).
- Ontop has no result-size limit setting; the graph is small. Limit the response size in the web server if needed.
- No reasoning beyond the mapping (no ontology is loaded).
- Tested with the Ontop 5.5.0 CLI against the test database (`backend/tests/test_sparql_ontop.py`, run it with
  `AMCAT4_ONTOP_CLI_DIR` set); the Docker image and the Caddy route were not run here.
