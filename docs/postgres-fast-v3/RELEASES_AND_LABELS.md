# Frozen releases and the label table (phase 5)

Status: 25 November 2026, PostgreSQL 18.7 with pg_search 0.27.0. Branch `postgres-fast-v3-releases`.
All numbers below are from that date and that setup unless a line says otherwise.

## What this adds

1. **Frozen releases.** A project can be frozen as release 1, 2, ... Later updates, deletes, tag changes and
   uploads do not change what a release returns. Nothing is copied: a release is one number.
2. **The label table.** Tags can be stored as rows of a separate table (`doc_labels`) instead of inside the
   json of the document. A tag change then writes one small row and does not rewrite the document.

Both are in migration `0005_releases_and_labels.py`.

## Switches

| Setting | Default | Meaning |
|---|---|---|
| `AMCAT4_VERSIONED_WRITES` | `true` | A row that is in a release is kept when it is changed or deleted. `false`: every write is in place, as before; creating a release is then refused. |
| `AMCAT4_LABEL_TABLES` | `false` | `true`: new tag fields store their values in `doc_labels`, and a background job moves the tags of existing tag fields. `false`: tags stay in the json, as in the original. |

A project without releases is written in place whatever the first switch says, so it pays one sequence number
per write and nothing else. A tag field that is in the label table stays there when the second switch is turned off
(the switch decides about new fields and about the backfill, not about reading).

## Design: row versions ("shared rows")

Every row of `documents` and of `doc_labels` has two numbers from one sequence (`change_seq`):

- `created_seq`: the number of the write that made this row version;
- `retired_seq`: the number of the write that replaced or deleted it. A current row has the largest bigint
  (9223372036854775807).

A release is one number R. It is the rows with `created_seq <= R AND retired_seq > R`.
Current ("live") queries read the rows with `retired_seq = 9223372036854775807`.

`doc_pk` is the document a row is a version of. It stays the same over all versions. For rows from before the
migration it is NULL, which means "the same as id".

### Writes

- Every write takes its number S after it has the write lock of the project (shared) and before it changes
  anything, and holds the lock until it commits.
- Creating a release takes the same lock exclusively for a moment and then takes R. So every write below R has
  committed and every later write is above R. The lock, not the order of the numbers, makes R a clean cut.
- A row that is in a release (`created_seq <=` the newest release of the project) is never changed:
  - update: one statement. The current row moves to a **new id** with `created_seq = S`; the old row is put back
    with its old id, content and `created_seq`, and `retired_seq = S`. (PostgreSQL 18: `RETURNING old.*`.)
    Vectors follow the current row (foreign key with ON UPDATE CASCADE).
  - delete: the row gets `retired_seq = S`.
- A row that is in no release is updated and deleted in place, as before.

Because a release row keeps its id, the order by `_seq` and the cursors of a release never change. The price:
the current row of a document gets a new id when a released version of it is updated, so in the **live** project
that document moves to the end of the `_seq` order.

Not versioned, on purpose: values that are derived from the content (sort slot columns, date keys, the hash for
unique fields). Jobs fill them in place on all row versions.

### What each query adds

- Live queries: one extra term in the json query, `retired_seq = 9223372036854775807`, inside the same unscored
  block as the project term. Plain SQL reads of documents (listing in id order, field values, statistics, get
  one document) add `AND retired_seq = 9223372036854775807`.
- Release queries: two range clauses instead (`created_seq <= R`, `retired_seq > R`).
- `doc_pk`, `created_seq` and `retired_seq` are real columns in the BM25 index of every partition (the index is
  rebuilt once by the migration).
- The unique index on (partition, project, doc_id) is now a partial unique index over current rows only.

### Releases API

Python (`amcat4.projects.releases`): `create_release`, `list_releases`, `get_release`, `set_release_doi`,
`verify_release`, `delete_release`, `purge`.

HTTP:

| Call | Role | |
|---|---|---|
| `POST /index/{ix}/releases` | ADMIN | create; returns number, seq, manifest |
| `GET /index/{ix}/releases`, `GET .../{number}` | METAREADER | list, get |
| `PUT .../{number}` | ADMIN | set or remove the DOI |
| `GET .../{number}/verify` | WRITER | compute the manifest again and compare |
| `DELETE .../{number}` | ADMIN | refused if it has a DOI; starts the purge job |
| `POST /index/{ix}/query`, `/aggregate` with `"release": n` | as usual | search, count, aggregate the release |

The manifest holds: number of documents, a fingerprint (md5 over the ordered list of doc_pk and created_seq),
per field the number of documents with a value, and for tags the number of values and a fingerprint over them.

The purge job (`purge_releases`) removes old row versions that no release needs any more.

## Design: the label table

`doc_labels(id, project_pk, doc_pk, field_pk, value, created_seq, retired_seq, created_by, created_at)`.
One row per (document, tag field, value). It has B-tree indexes and its own BM25 index. Labels have their own
`created_seq` and `retired_seq`, with the same rules as document rows.

A tag field uses the table if `fields.label_table` is true. That is set:

- for a new tag field, if the switch is on and every row of the project has its `doc_pk` (`projects.labels_ready`);
- by the `backfill_labels` job, for existing tag fields (see below).

### Writes

- Add or remove a tag by query: an INSERT or a retire/delete in `doc_labels`. **No document row is written.**
- Upload, update, update by query: the tags of each (document, field) that the write sets are compared with the
  current labels. The same list in the same order: nothing is written. Otherwise the old labels are removed
  (retired if a release has them) and the new list is stored in the given order.
- Deleting a document removes its labels the same way.
- Copying between projects works in all four combinations (json or label table on either side).

### Reads: which form runs where

| Read | Form | Where it runs |
|---|---|---|
| Filter on a tag (value, values, range, exists, not exists) | `CASE WHEN EXISTS (SELECT 1 FROM doc_labels l WHERE l.doc_pk = documents.doc_pk AND ...) THEN true ELSE false END` | **Falls back to plain SQL.** pg_search finds the hits of the rest of the search inside the index; postgres then checks each hit against `doc_labels` (B-tree, or a hash of the label's documents). No Top-K and no in-index count for such a statement. |
| Aggregation with a tag axis | `JOIN doc_labels` + GROUP BY | **Inside pg_search** on a partition table (Aggregate Scan over the join) when pg_search takes it; otherwise the existing fallback runs the same statement on the parent table (plain postgres join). pdb.agg is not used for a tag axis in the label table. |
| Tag values of a result page | one extra statement for the documents of the page | plain SQL over the B-tree (doc_pk, field_pk) |
| Tag values of one document, top hits | a subselect per row | plain SQL |
| Sort on a tag field | the same subselect | plain SQL sort (as a json tag field: no Top-K) |
| Most frequent values of a field | GROUP BY on `doc_labels` | plain SQL |
| Tag in a query string (`tags:sport`) | **refused** with a clear error | use a filter |

Searches without a tag filter are not changed by the switch: the same plans as with tags in json.

#### Why the filter is not a join inside pg_search

That was the plan, and I tried three forms on 40,000 documents with a tag on 20,000 of them:

| Form | Count | Page of 10 | Result |
|---|---|---|---|
| `EXISTS (...)` at the top of WHERE, partition table: ParadeDB Join Scan | 11 ms | 8 ms | correct in every test I ran, **but** two such conditions in one statement fail (`Failed to build DataFusion aggregate plan`) |
| the same on the parent table: postgres joins above a Top-K scan | 12 ms | 3.5 ms | **wrong page**: 10 asked, 783 match, 7 returned (0 for a rarer tag). No error. The scan stops after LIMIT rows before the join. |
| `doc_pk = ANY(ARRAY(SELECT ...))`: the list of documents, evaluated inside the pg_search scan | 130 ms | 132 ms | correct, keeps Top-K; 5 ms for a tag on 400 documents, but 850 ms for a list of 66,000, and "has no tag" 7-12 s for 66,000 |
| `CASE WHEN EXISTS ...` (used) | 22-28 ms | 8-10 ms | correct; cannot become a join |

The Join Scan is the fastest, but whether pg_search takes the join is its own choice per statement, and when it does
not, the page can be wrong without an error. I could not make that impossible from the application, so the code
uses the form that cannot go wrong. If a later pg_search fixes the Top-K under a join and the two-join error, the
Join Scan form is a change in one function (`amcat4.postgres.labels.exists`).

### Speed (40,000 documents, one project in its partition, 25 Nov 2026, best of 5, one run)

| | tags in json | label table |
|---|---|---|
| upload 40,000 documents | 4.3 s | 6.1 s |
| first page by `_seq`, no filter | 2.5 ms | 2.9 ms |
| search, top 10 by relevance + count (1,563 hits) | 5.1 ms | 4.1 ms |
| tag on 400 documents: count + first page | 3.3 ms | 23 ms |
| tag on 20,000 documents: count + first page | 3.9 ms | 26 ms |
| search + tag, by relevance (783 hits) | 6.3 ms | 9.4 ms |
| search + tag, by date | 4.2 ms | 9.3 ms |
| documents without tags: count + page | 5.3 ms | 48 ms |
| aggregate by tag | 8-9 ms | 10.5 ms |
| aggregate by keyword with a tag filter | 6 ms | 35 ms |
| aggregate by tag with a search | 7.5 ms | 10.7 ms |
| add a tag to 10,000 documents | 483 ms | 266 ms |
| remove a tag from 10,000 documents | 396 ms | 24 ms |
| add a tag to 10,000 documents that are in a release | 1,057-1,216 ms | 265 ms |
| release: tag on 20,000 documents, count + page | 10-17 ms | 20 ms |
| release: aggregate by tag | 20-23 ms | 10 ms |

Read this as: tag writes are 2 to 16 times faster and do not grow the documents table; a tag filter without a
search is 5 to 9 times slower at this size, and its cost grows with the size of the project (every document of the
project is checked), where the json form stays inside the index. Not measured on a large project.

Plans checked with EXPLAIN on the same data: a live count is still a ParadeDB Aggregate Scan, and a live first
page (by `_seq`, by relevance, by date) is still a Top-K scan, in both modes.

### Backfill job (`backfill_labels`)

Scheduled once a minute, only with the switch on, for projects that have a tag field in json. Three steps, each
can be resumed:

- A. give every row of the project its `doc_pk`, in batches; then set `projects.labels_ready`. (Skipped for projects
  made after the migration.) This rewrites every such row once.
- B. per tag field, in one transaction that holds the write lock exclusively: copy the tags of **every row version**
  to `doc_labels` with the `created_seq` and `retired_seq` of that row version, and set `fields.label_table`.
  Releases therefore read the same tags as before.
- C. 60 seconds later, remove the tags from the json, in batches.

A write that read the field definitions before step B and got the lock after it is refused (409, "try again"), so
it cannot write tags into the json that step C would then remove.

## What was tested

`tests/test_migrations.py` (migration), `tests/test_releases.py`, `tests/test_labels.py`.

- Migration 0005 on a database with data: no row rewritten, run again without rebuilding the index, run again from
  a half-done state, new partitions get the same columns and indexes, downgrade refused when releases exist,
  downgrade and upgrade again.
- A release gives the same documents, values, tags, counts and aggregations (26 different reads) after: update,
  replace, upsert, update by query, delete, delete by query, tag add and remove, new uploads, reuse of a deleted
  id, a second release and more changes, VACUUM, the purge for another release. Run twice: tags in json, tags in
  the label table.
- Live queries never return an old row version (every document once; counts equal the current rows).
- A project without releases gets no extra rows; only released rows are versioned.
- Unique fields and document ids keep working with old versions in the table.
- Two writers and a release at the same time: every write is wholly in or wholly out of the release.
- A download of a release with a `_seq` cursor gives every document once while documents change.
- Vectors follow the current row version.
- HTTP endpoints, roles, DOI rule, purge job.
- Label table against json: two projects with the same documents and the same writes give the same result for
  about 55 reads (filters, with and without a search, relevance order, sorts, aggregations, top hits, field
  values, single documents), before and after every kind of write, and with a release in between. Three ways to
  read: in-index paths forced on, default settings, parent table only.
- A query on two projects, one with tags in json and one in the label table.
- Tag changes do not touch the document rows (same xmin, ctid and updated_at), also when a release has them.
- Copy job between json and label table, both ways, and a second copy after changes.
- Backfill job on a project with old row versions and two releases, with `doc_pk` NULL as in an old project: live
  and both releases read the same before and after; the json no longer has the tags; `verify_release` still ok.
- The stale-definition check.

Test results are in the final report of this phase (the numbers change with every merge).

## Known limits

Releases:

- The relevance order (BM25) of a release is not frozen: scores depend on the other rows in the index. Which
  documents match, the counts, and every order on a field or on `_seq` are frozen.
- Field definitions are not versioned. For a project with releases these are refused: delete a field, change the
  type of a field, clear the project. Deleting the whole project is possible (its releases go with it).
- Vectors are not part of a release (a similarity search with `release` is refused).
- A release query on several projects at once is refused.
- The live `_seq` order changes when a released document is updated (see above). A client that pages through
  the **live** project with a `_seq` cursor while such updates happen can see that document again at the end.
- `verify_release` proves that the release has the same row versions and tags. It cannot see a change made in
  place to a row version (possible only with `AMCAT4_VERSIONED_WRITES=false`, or directly in SQL).
- Old row versions make the table and the index larger until the release is deleted and purged.
- After migration 0005 a server with the older code cannot upload (`created_seq` has no default). Upgrade the
  code and the database together.

Label table:

- A value occurs once per document (json keeps duplicates). `tags: []` is the same as no tags (json returns `[]`).
- A tag field in the label table cannot be used in a query string, cannot change type, and cannot be unique.
- After a release, a tag change makes a new document row version in json mode (new id) and none in label mode. So
  the live `_seq` order of the two modes differs from then on. The documents and values are the same.
- A tag filter is checked by postgres for every hit of the rest of the search (see "Reads"). Without a search or
  another filter that is every document of the project: 23-48 ms at 40,000 documents, so seconds for millions. Not
  measured on a large project.
- The relevance order of the same search can differ between a project with tags in json and one with tags in the
  label table after tag changes: a BM25 score depends on the index segment the document is in, and a tag change in
  json moves the document to a new segment.
- Turning the switch on for an old project rewrites every row once (step A) and once more for the rows that had
  tags (step C).
- `doc_labels` has one BM25 index over all projects (as decided). The filters do not use it (they use the B-trees);
  only the aggregation join can. Every label write maintains it. What it costs per write was not measured; if
  the Join Scan form is not taken up again, dropping it is worth a measurement.
- `doc_labels.created_by` exists but is not filled yet.

## A text column outside the json (not built)

Not decided, so not built. The migration leaves room: the BM25 column list is one constant (`BM25_0005`) and
the rebuild step is skipped when the index already has the new columns.

What it would add:

1. Migration: `ALTER TABLE documents ADD COLUMN text text` (no rewrite), and one more entry in the BM25 list,
   `(text::pdb.unicode_words)` or the tokenizer in use. If it goes into 0005 before 0005 is released, the index is
   rebuilt once for both. As a later migration it is a second rebuild of every partition.
2. A backfill job that copies the text out of `text_fields` into the column, in batches, with a ready flag per
   project (the same pattern as `backfill_labels`); and the trigger of migration 0004 on the quick-wins branch
   would have to keep the column in step.
3. Code: the query-string compiler must address the column instead of `text_fields.fN` for that field; snippets
   and highlights read the column; uploads and copies write it; `VERSION_COLUMNS` in `versions.py` gets the column
   so old versions keep it.
4. A project has several text fields and the json key differs per project. One column fits one field per project
   (as the sort slots do), so it needs a mapping like the slot mapping.

## Merge notes

- Migration 0005 has `down_revision = "0003"`. Point it at 0004 when the quick-wins branch is merged. It does not
  depend on 0004.
- `search.py`: the changes are `scope_clauses` (project + row versions) and reading label values for a page.
  A cutoff sort added on another branch must skip fields with `in_labels`.
- New plain SQL reads of `documents` on other branches need the live condition
  (`project_filter(conn, pk, live=True)`).
- New INSERTs into `documents` must give `created_seq`.
