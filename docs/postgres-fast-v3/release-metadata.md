# Release metadata (phase 6a and 6b)

Status: 26 November 2026, PostgreSQL 18 with pg_search 0.27.1. Branch `postgres-fast-v3-sparql`, migration
`0006_release_metadata.py`. Builds on the frozen releases of [RELEASES_AND_LABELS.md](RELEASES_AND_LABELS.md).

## Why

A frozen release must be citable in a stable and reproducible way (OPTIMA D3.1: a frozen copy with a URL and a DOI).
A catalogue such as MEDem's ATLAS needs a description of it: the fields, their types and counts (a codebook). And
a user must be able to check that a rerun, from R or from Stata, finds the same cases as the first run.

So the description of a release is made **once**, when the release is made, and stored next to it. It is never
changed afterwards. Anything that may change later is kept apart from it.

## What is stored

New columns of the `releases` table (the column contract; SQL views may rely on these names and types):

| Column | Type | Frozen? | Content |
|---|---|---|---|
| `metadata_jsonld` | jsonb | frozen | The description as JSON-LD (schema.org and DCAT). NULL for releases made before 0006. |
| `metadata_sha256` | text | frozen | Hex SHA-256 of the canonical JSON of `metadata_jsonld`. Also in `manifest.metadata_sha256`. |
| `versions` | jsonb | frozen | `postgres`, `pg_search`, `amcat4` (versions) and `migration` (alembic revision) at creation. |
| `field_definitions` | jsonb | frozen | List of `{pk, name, type, identifier, metareader: {access, queryable}, description?}`. No client settings. |
| `golden_queries` | jsonb | frozen | List of `{name, endpoint, query, field_pk?, result}`: fixed queries and their results (below). |
| `metadata_public` | boolean, not null, default false | live | Whether anyone may see the metadata. Never the texts. |
| `withdrawn_at` | timestamptz, null | live | When the release was withdrawn. |
| `withdrawn_reason` | text, null | live | Why. |
| `doi_status` | text, not null, default `'none'` | live | `none`, `draft`, `registered` or `findable` (a CHECK constraint). |

`doi` (from 0005) is live too. The DOI is never part of the frozen JSON-LD: it is usually registered after the
release is made, and adding it must not change the hash.

### The JSON-LD

- `@context`: schema.org as the vocabulary, with the prefixes `dcat`, `dct` and `prov`. The context is inline, so
  reading it needs no network.
- `@type`: `["Dataset", "dcat:Dataset"]`.
- `@id` and `url`: `{AMCAT4_RELEASE_ID_BASE}/index/{project}/releases/{number}`. Without the setting, the `host` of
  the server is used. Set it to an address that stays valid: it is frozen.
- `name` ("project name, release n"), `alternateName` (project name), `description` (project description),
  `version` (the release number), `dateCreated` and `datePublished` (the moment of the release).
- `creator`: only if the server gave a name. An email address is left out.
- `isPartOf`: the project (its own `@id`).
- `identifier`: internal identifiers (`amcat4:release`, `amcat4:seq`, `amcat4:fingerprint` of the manifest).
- `size`: the number of documents.
- `variableMeasured`: one `PropertyValue` per field: `name`, `propertyID` (`field:<pk>`), `additionalType` (the
  AmCAT type), `value` (documents with a value) and `description` if the field has one.
- `conditionsOfAccess`: a sentence derived from the guest role of the project at the moment of the release.
- `prov:wasGeneratedBy`: the software (AmCAT and its version) and the versions above.

It never contains document text, and no values of any field. (A test checks that a word from a text is absent.)

### Golden queries

A few cheap queries, run on the release right after it is made:

- `total`: all documents in `_seq` order, 10 per page. Result: `count`, and `first_ids` = the `doc_pk` of the first
  10 documents (not in score order).
- `top_values:<field>` for keyword and tag fields (at most 10 fields): the 10 most frequent values, ties ordered by
  value. Result: `count` (number of distinct values seen, at most 1000) and `values` = `[{value, n}]`.
- `date_range:<field>` for date fields (at most 3): `count`, `min`, `max`.

`query` is the request body for `POST /index/{ix}/query` or `/aggregate` (`endpoint`), with `release` set. So
anyone can send the same request, from any client, and compare. `field_pk` lets the check find a renamed field.
The values of keyword and tag fields are in `golden_queries` (not in the JSON-LD). They are only shown by verify,
which needs WRITER role.

## What is live

`GET /index/{ix}/releases/{n}/metadata` returns the frozen JSON-LD with a live overlay:

- if a DOI is set: an extra `identifier` (`propertyID: doi`) and `sameAs` (`https://doi.org/...`);
- `creativeWorkStatus`: `Published` or `Withdrawn`;
- `additionalProperty`: `doiStatus`, and `withdrawnAt` and `withdrawnReason` when withdrawn.

`?format=turtle` gives the same as `text/turtle` (with rdflib, BSD licence).
`GET /index/{ix}/releases/{n}/fields` gives the frozen field definitions with the counts of the manifest.

Access to both: anyone (also without login) when `metadata_public` is true; otherwise METAREADER role on the project
or more (a guest role counts). A release that does not exist looks the same as a private one to a user without access.

`PUT /index/{ix}/releases/{n}` (ADMIN role) changes only the properties in the body: `doi`, `doi_status`,
`metadata_public`, `withdrawn` (true or false) and `withdrawn_reason`. Withdrawing deletes nothing: the release
can still be queried with `release: n`, and it still verifies. It is only marked.

## How to verify

`GET /index/{ix}/releases/{n}/verify` (WRITER role) computes the manifest again from the rows, as before, and for a
release with metadata also:

1. the SHA-256 of the stored JSON-LD, which must equal `metadata_sha256` and `manifest.metadata_sha256`
   (difference: `metadata_sha256`);
2. the SHA-256 of the stored golden queries against `manifest.golden_queries_sha256` (difference:
   `golden_queries_sha256`);
3. every golden query is run again and compared (difference: `golden:<name>`; the stored and new results are in
   `golden_queries`).

Outside AmCAT: take `metadata_jsonld`, serialise it with keys sorted, no whitespace, UTF-8 and non-ASCII
characters as they are (`json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)`), and compare
its SHA-256. The JSON-LD has no floating point numbers, so this is the same as RFC 8785 for it.

Releases made before 0006 have no metadata: verify works as before and `metadata` is null.

## Limits

- What is frozen: which documents are in the release, their values, the counts, and every order on a field or on
  `_seq`. **BM25 scores and the relevance order are not frozen**: scores depend on all rows in the index. The golden
  queries therefore use no scores.
- The manifest uses md5 for its fingerprints (from 0005); only the metadata hash is SHA-256.
- The metadata is computed after the cut, in a second step (as the manifest). If the server stops in between, the
  release has no metadata. The field definitions are read in the same transaction as the cut.
- `top_values` ties at the 1000th row could make a rerun differ in theory; it needs more than 990 values with the
  same count as the 10th.
- The `@id` is built from a setting at creation. If the server moves, the old `@id` stays in the frozen record; the
  DOI (live) is the stable way to cite.
