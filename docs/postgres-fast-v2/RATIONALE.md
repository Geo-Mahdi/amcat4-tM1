# Branch `postgres-fast-v2`: design rationale

This document explains why the branch `postgres-fast-v2` changes what it changes. It is written for people who did
not take part in the work: the original author of the PostgreSQL version of AmCAT, other developers, and
researchers who use AmCAT. For a short overview of the same changes, see
[`backend/POSTGRES_FAST_PATHS.md`](../../backend/POSTGRES_FAST_PATHS.md). For the exact code, see
`git log bb35e43a..postgres-fast-v2` and `git diff bb35e43a..postgres-fast-v2`.

## Contents

1. [Purpose and status](#1-purpose-and-status)
2. [Background: how the PostgreSQL version works, and what was slow or wrong](#2-background)
3. [Principles behind the changes](#3-principles-behind-the-changes)
4. [The changes, one by one](#4-the-changes-one-by-one)
5. [What users and client libraries notice](#5-what-users-and-client-libraries-notice)
6. [Results](#6-results)
7. [Known limits and open questions](#7-known-limits-and-open-questions)
8. [How to verify](#8-how-to-verify)
9. [Glossary](#9-glossary)

---

## 1. Purpose and status

### 1.1 What the branch is

The branch `postgres-fast-v2` starts from commit `bb35e43a`, the tip of the branch `postgres-migration`. That branch
(called "the original branch" in this document) moves AmCAT from Elasticsearch to PostgreSQL with the pg_search
extension. It keeps AmCAT's API, adds transactions, a relational store for system data, and field-level access, and
it uses pg_search for full-text search inside PostgreSQL.

Measurements in November 2026 showed that the original branch gives correct results and passes every transaction
check, but is several times slower than the Elasticsearch version on searches, and much slower on counts over large
result sets, on aggregations, on sorting and under load. The measurements also found a few bugs. Most of the slow
cases had their cause in how AmCAT's SQL reaches the pg_search index, not in PostgreSQL itself.

This branch changes those read paths, adds a few indexes and columns through new migrations, and adds maintenance
that fits how pg_search behaves. It keeps the design of the original branch: one documents table, values in JSON
columns under stable field keys, projects that share partitions, AmCAT's own query parser, and field-level access.

### 1.2 Status

- **Experimental.** The branch is a proposal for the original author to look at, take in parts, change or reject.
- **Not reviewed by the AmCAT maintainers.** Nothing on it is agreed with them.
- **Prepared with the help of an AI assistant.** The code, the tests and this document were written with an AI
  coding assistant and checked by running the test suite and measurements. The commit messages carry a co-author
  line for this reason.
- **Everything is behind a switch.** Almost every change has an environment variable that restores the original
  behaviour (see [section 4](#4-the-changes-one-by-one) and `deploy/.env.example`). Correctness fixes have no switch.
- **Migration `0001_initial.py` is not edited.** Schema changes are in two new migrations, `0002` and `0003`.

### 1.3 How the branch grew

The commits fall into four groups. Knowing them helps to read the measurements, because some numbers were taken on
an earlier stage.

| Stage | Commits | What |
|---|---|---|
| 1. First fast paths | `b0d75679` to `b8251fb9` (5 commits) | Ported from an earlier experimental copy of 20 Nov 2026 (called "the first fast-path version" below): bug fixes, custom plans, the partition table read path, in-index aggregations, late fetch for sorts, an estimated project size, tests |
| 2. Second round of read-path changes | `1ec626bd` to `659a70d1` | No parallel workers for API queries, unscored project clause, paging by id, one count next to the page, behaviour under overload |
| 3. Index, schema and maintenance | `79c83985` to `57eaa02d` | pg_search 0.27.0, migration `0002` (sort slots, B-tree index, one index rebuild, lz4), more sort slots, quarter key, VACUUM, prewarm, migration `0003` and moving a project to its own partition |
| 4. Review fixes and documentation | `75c52261` to `f90c0624` | Fixes found by reviewing stages 1 to 3 (isolation of sort slots, `_seq`, null metrics, no fallback after a timeout, move job details, cursor checks, and more), comments, tests, README |

### 1.4 How to read this document

- Section 2 explains the storage design and the measured problems. Read it first if you do not know the
  PostgreSQL version of AmCAT.
- Section 4 has one subsection per change. Each has the same parts: **problem** (with evidence), **change**, **why
  this solution** (with the alternatives that were considered), **measured effect**, **switch**, **risks and
  trade-offs**, and **files and commits**.
- Every number says where, on which code and when it was measured. "Not measured" means there is no measurement.
  "(inferred)" marks reasoning that was not checked by a run.
- All measurements come from one laptop, with authentication switched off (`no_auth`), mostly on one collection
  of 2.68 million Dutch news articles (in some tests next to a copy of 10.7 million documents in the same
  partition), or on synthetic test partitions. They show the direction of an effect, not what a production server
  will do.
- Switches are environment variables with the prefix `AMCAT4_`. Their defaults are in `backend/amcat4/config.py`.

---

## 2. Background

### 2.1 How the PostgreSQL version stores documents

The original author describes the design in `backend/amcat4/postgres/layout.py` and in
`backend/ELASTIC_VS_POSTGRES.md`. In short:

- **One table for all documents.** Every document of every project is a row of the table `documents`.
- **Values in JSON columns.** A document's values are stored in three `jsonb` columns, by how they need to be
  indexed:

  | Column | Holds | Indexed as |
  |---|---|---|
  | `text_fields` | text fields | tokenized full text (BM25, with positions for phrases) |
  | `exact_fields` | keyword, tag, url, number, integer, boolean, date, geo_point, multimedia paths | exact values, stored as "fast" (columnar) fields for filters, sorting and aggregation |
  | `stored_fields` | object fields | not indexed, only stored |

- **Field keys, not field names.** Inside the JSON, a value is stored under a stable key such as `f12`, the primary
  key of the field in the `fields` table. The field name is a label of the project. So renaming a field is one row
  update, and two projects can use the same name with different types.
- **Partitions.** The table is list-partitioned on `partition_id`. Each partition is a table of its own
  (`documents_p1`, `documents_p2`, ...) with its own BM25 index. A new project goes to the newest partition until
  that partition's index reaches `AMCAT4_PARTITION_MAX_GB` (default 5 GB); then a new partition is made. So many
  small projects share a partition, and a big project fills most of one.
- **The BM25 index.** pg_search builds one BM25 index per partition table. In the original branch it covers `id`,
  `partition_id`, `project_pk`, the standard columns `date` and `source`, `text_fields` and `exact_fields`. Adding
  a field adds a JSON key, so it never needs an index rebuild.
- **Standard columns as sort slots.** pg_search cannot sort on JSON keys inside its index. So each project can map
  one date field to the real column `date` and one keyword field to the real column `source`
  (`fields.sort_slot`). Sorting on those is fast; sorting on other fields reads every match.
- **Derived date keys.** Date fields get derived keys (year, month, day of week, ...) in `exact_fields`, so date
  histograms and date filters can run inside the index.
- **AmCAT's own query parser** turns AmCAT's Lucene-style query strings into pg_search's JSON query language, maps
  field names to keys per project, and enforces which fields a user may query.

A search request on the original branch roughly becomes one SQL statement on the parent table:

```sql
SELECT ... FROM documents
WHERE documents.id @@@ '<json query: project term AND query string AND filters>'
  AND documents.partition_id = ANY(...) AND documents.project_pk = ANY(...)
ORDER BY ... LIMIT ... OFFSET ...
```

plus a second statement for the total count. The `@@@` operator hands the JSON query to the BM25 index. The SQL
conditions on `partition_id` and `project_pk` let PostgreSQL skip the other partitions.

A small picture of the layout:

```
documents (partitioned on partition_id)
 |-- documents_p1  (projects A, B, C, ... share it)   -> BM25 index documents_p1_bm25
 |-- documents_p2  (project D, large)                 -> BM25 index documents_p2_bm25
 '-- ...
Each row: id, partition_id, project_pk, doc_id, date, source, text_fields{f12: ...}, exact_fields{f14: ...}, ...
```

### 2.2 The original design goals

In `ELASTIC_VS_POSTGRES.md` the original author ranks what the split of data over BM25 indexes decides:

- "how expensive AmCAT is to host, and how well it scales (very important)"
- "how stable AmCAT is when uploading data (very important)"
- "how fast, safe (and complicated) it is to query across projects (the 'safe' part is important)"
- "how fast queries are (mildly important)"

On sorting he writes: "I do expect that pg_search will address this at some point, so we shouldn't over engineer a
solution", and suggests standard columns such as `date` and `source`, which "would also make sense for data
discovery purposes". On moving projects between partitions: "it would be easy to add a feature for move projects
across partitions". The layout notes add "(Later: moving big projects to their own partition.)".

This branch takes these goals as given (see [section 3](#3-principles-behind-the-changes)). Several of its changes
are features that the original design already foresaw: more standard columns for sorting, and moving a big project
to a partition of its own.

### 2.3 What was measured to be slow or wrong

Four series of measurements were made, all on one laptop, through the AmCAT API unless stated:

| Name in this document | Date | What was compared |
|---|---|---|
| Night test | 15 to 16 Nov 2026 | Elasticsearch version (`master` 4.2.11, Elasticsearch 8.18.4) against the original branch (PostgreSQL 18.6, pg_search 0.26.0), 2.68 M documents, three passes, 100 runs per search |
| First fast-path version | 20 Nov 2026 | The original branch against the first fast-path version, synthetic corpus of 1 M documents plus two small projects, in process (no HTTP) |
| Three-way test | 20 to 21 Nov 2026 | Elasticsearch, the original branch and the first fast-path version, same 2.68 M documents, the original branch's database also holding a 10.7 M document copy in the same partition |
| Speed research | 22 Nov 2026 | SQL timings in `psql` and API timings on a scratch copy of the database (the 2.68 M document collection alone in its partition, running the first fast-path version), plus some timings on the shared partition |

The main findings on the original branch:

| Finding | Measured | Where and when |
|---|---|---|
| Very common words (`de`, `het`) and "no query" took about 0.45 s | 450 to 490 ms (Elasticsearch about 8 ms) | Night test |
| Normal searches 2 to 5 times slower than Elasticsearch | 15 to 60 ms against 5 to 10 ms | Night test |
| Whole-collection aggregations slow | 1.4 to 2.9 s (Elasticsearch 16 to 90 ms) | Night test |
| Sort by publisher slow | 580 ms (Elasticsearch 6 ms) | Night test |
| Load | clean to 100 requests/s, overloaded at 150/s (Elasticsearch 400/s) | Night test |
| Bulk upload unstable | 120 to 460 documents/s in most passes, one pass 1,800 to 12,500 | Night test |
| Broad wildcards miss documents | `pol*` found 2 % of what Elasticsearch found, `ver*` 7 % | Night test |
| Aggregation per `section` | HTTP 500 in every pass | Night test |
| Concurrent uploads of the same documents | 3 of 4 clients got HTTP 500 (data correct) | Short three-way test, 22 Nov 2026 |
| Correctness under transactions | passes all 40 of 40 checks (Elasticsearch 20 of 40) | Night test |

The causes, found by reading the code and by the measurements above:

1. **The parent table blocks the in-index aggregate scan.** pg_search can count and aggregate inside its index (an
   "Aggregate Scan") only when the statement runs on a table that has the BM25 index. The original statements run
   on the partitioned parent table `documents`. Then every count or GROUP BY fetched and unpacked each matching row.
   Measured with the original statement: a whole-project aggregation took 1,640 ms on the parent table and 37 ms on
   the partition table, with the same result (2.68 M document project in a partition of 13.4 M documents,
   pg_search 0.26.0, 16 Nov 2026).
2. **Generic plans after five executions.** psycopg prepares a statement on the server after it has run five times
   on a connection, and PostgreSQL may then switch to a generic plan that does not know the actual project or query.
   For pg_search such a plan is much slower (row estimate 1, no parallel workers). In the night test a very common
   word took 84 ms for the first 5 requests and 469 ms from the 11th request on, in every pass. This explains most
   of the 0.45 s cases.
3. **The project clause was scored.** The project condition is a term inside the JSON query, in a `must` list
   together with the query string, so pg_search computed a score for it for every hit. For very common words this
   is most of the time: top 10 for `de` took 33.7 ms with the scored project clause and 3.0 ms with an unscored one
   (shared partition of 13.4 M documents, no parallel workers, 22 Nov 2026). The scored term also added a score that
   depends on the index segment a document is in, so the relevance order partly depended on the index layout.
4. **A duplicate project term.** The SQL condition `project_pk = ANY(...)` on top of the JSON project term became a
   second term inside the index, which made large counts slower (up to 2 times, according to the code comment in
   `compile_search`). `= ANY(...)` with one value is also evaluated over every row of the partition (about 14 ms at
   13 million rows, according to the code comment in `_equals_any`).
5. **Parallel workers on short queries.** PostgreSQL starts up to four helper processes per statement. For a search
   of a few milliseconds this costs more than it saves: a count of `klimaat` took 10.0 ms with 4 workers and 3.7 ms
   without (`psql`, speed research, 22 Nov 2026).
6. **JSON sub-fields have no Top-K sort.** pg_search can sort inside the index and stop after the top N (Top-K)
   only on real indexed columns, not on JSON keys. Its documentation says so. So a sort on any field that is not in
   `date` or `source` read every match.
7. **Deep offset paging.** With a query, the "next page" cursor was an offset in score order. Page N means sorting
   all earlier hits and throwing them away: one page of 200 took 5.3 ms at offset 0, 29.7 ms at offset 20,000 and
   83.1 ms at offset 100,000, against 3.9 ms by id at any depth (`psql`, speed research, 22 Nov 2026).
8. **Extra count statements.** The original branch counts the total in a separate statement, also again on every
   page of a scroll; the first fast-path version even counted in two steps. Each statement costs about 2.4 ms of
   planning (speed research, 22 Nov 2026).
9. **Wildcard expansion cap of 50.** pg_search (Tantivy) expands a prefix only to the first 50 matching terms of
   each index segment unless told otherwise. The original code set no limit, so `pol*` found 13,678 documents where
   Elasticsearch found 592,319 (night test), and the result changed when segments were merged.
10. **Empty-string sort crash.** Aggregation results are sorted in Python with `row[axis] or 0`. An empty string
    became `0` and was compared with strings: the HTTP 500 of the `section` aggregation.
11. **Concurrent duplicate upload.** Two uploads of the same documents could both pass the duplicate check and then
    collide on the unique index (`UniqueViolation`, HTTP 500).
12. **Unstable uploads.** The staging table of an upload is a fresh temporary table without statistics, so the
    planner guessed its size; in one pass of the night test an upload spent 626 of 632 s in one statement.
13. **Maintenance assumptions of pg_search 0.26.** On 0.26.0 pg_search never merged index segments in the tests, so
    every upload added segments. The original branch handles this with an automatic rebuild. pg_search 0.27.0 does
    merge in the background (section 4.15), which changes what maintenance is needed.

---

## 3. Principles behind the changes

These rules guided every change. The first two come from the original author's notes.

1. **The original priorities set the order.** Hosting cost, scaling and upload stability are "very important",
   safe querying across projects is important, query speed is "mildly important". No change may make the first
   three worse. So: no new services, no new per-project indexes by default, and isolation between projects in a
   shared partition is tested for every new query form.
2. **"We shouldn't over engineer a solution."** The smallest change that works. Existing mechanisms are extended
   (more sort slots like `date` and `source`, a move job as foreseen in `layout.py`) rather than replaced.
3. **Same results first.** Every change to a read path has a test that compares its counts and hit sets with the
   original SQL. The only intended change of results is the relevance order (section 4.2), once.
4. **A switch for every behaviour change.** Each change can be turned off with one environment variable, to compare
   results or to fall back if something looks wrong.
5. **Reads change, writes keep their safety.** Write statements (update and delete by query) keep both SQL
   conditions on partition and project. Fast paths are for read-only statements.
6. **Measure, and say what was measured.** Each number in comments and documents says on which code and data it
   was taken. Ideas that were not measured are marked.
7. **Schema history is not rewritten.** `0001_initial.py` stays as it is; the layout notes say migrations "must
   never be edited once released". New columns and indexes come in `0002` and `0003`, and all index changes are
   bundled so the search index is rebuilt once.
8. **Speed features are not meaning.** A sort slot makes sorting fast. It says nothing about what a field means.

---

## 4. The changes, one by one

The subsections follow the topics of the overview. Each ends with the switch and the commits.

### 4.1 Read paths: the partition table ("fast path")

**Problem.** Counts and aggregations on the parent table cannot use pg_search's in-index Aggregate Scan (cause 1 in
section 2.3). The duplicate SQL project condition added a second term inside the index (cause 4).

**Change.** When all projects of a read-only query (count, search, aggregate) are in one partition, and there is no
filter on document ids, the statement runs on that partition's table, aliased as `documents` so the rest of the SQL
does not change:

```sql
SELECT ... FROM documents_p3 AS documents WHERE documents.id @@@ '<json query>'
```

On the partition table there is no SQL condition on `partition_id` (every row has the same value) and no SQL
condition on `project_pk`. The project is selected only by the project clause inside the JSON query. That clause is
built by the server, first in the list, never from user input, so a query string cannot escape it. Where SQL
conditions on a project remain (the general path and all writes), a single value is written as `= value` instead of
`= ANY(array)`.

**Why this solution.** The statement is the same as before except for the table name, so the results are the same
and the change is small. The alternative of letting PostgreSQL pick the partition from the condition on
`partition_id` is what the original does; it is correct, but the in-index aggregate scan does not work through the
parent. Making the project condition structural (one partition per project, or pg_search's `partition_by` index
option, which is a Beta feature) would remove the need for any project term, but changes the layout for every
project and was not tested; for big projects a move to their own partition is offered instead (section 4.18).

**Not used** while a project of the query is being moved (its documents are then in two partitions), for queries over
projects in several partitions, for similarity (vector) searches, and with a filter on `_id`. These use the original
statement on the parent table.

**Measured effect.** On the original statement, 1,640 ms on the parent against 37 ms on the partition table for a
whole-project aggregation (16 Nov 2026, see section 2.3). Not re-measured on this code alone; its effect is part of
the results in section 6.

**Switch.** `AMCAT4_FAST_PATH` (default `true`). `false` gives the original SQL on the parent table for every read,
and also turns off the late fetch (4.10), the in-index aggregations (4.8) and the estimated project size (4.21).

**Risks and trade-offs.** Isolation now depends on the JSON project clause alone on the partition table. Tests check
isolation in a shared partition with the fast path on and off (`tests/test_fast_path.py`,
`tests/test_search_v2.py`). Whether read queries may name the partition table directly is one of the open questions
for the original author (section 7.2).

**Files and commits.** `backend/amcat4/postgres/search.py` (`compile_search`, `_equals_any`),
`backend/amcat4/projects/aggregate.py`. Commits `b0d75679`, `3f157381`.

### 4.2 The project condition and filters as an unscored clause

**Problem.** The project term was scored for every hit (cause 3). For very common words this was most of the time,
and it made the relevance order depend on index segments.

**Change.** The project clause and the user's filters are put together in one `const_score` clause with score 0,
inside `must`, next to the scored query string:

```json
{"boolean": {"must": [
  {"const_score": {"score": 0.0, "query": <project term and filters>}},
  <query string>
]}}
```

Without a query string only the `const_score` clause remains.

**Why this solution, and the alternatives.**

| Alternative | Result | Verdict |
|---|---|---|
| A `filter` list next to `must`, as in Elasticsearch | pg_search's JSON boolean query has **no** `filter` list. An unknown key is silently ignored (tested on pg_search 0.26.0 and 0.27.0). On a shared partition, a clause that matches documents of two projects gave 12,718,910 documents with the project in `filter` against 2,543,782 with the project in `must`. This form would drop the project condition and return documents of other projects in a shared partition | Rejected: unsafe |
| A boost of 0 on the project term | Correct counts, no speed gain (speed research, 22 Nov 2026) | Rejected |
| `const_score` with score 0 | Correct counts (2,550,435 for `de` and 45,840 for `klimaat`, identical to the `must` form; also 2,543,782 for a clause that matches both projects), and fast | Chosen |

An earlier check had suggested that the `filter` form gave the same counts. That check was wrong by accident: the
other project in that partition used different field keys, so no search word could match its documents. The
mistake was caught and corrected before any code used the `filter` form. The commit message of `1ec626bd` and the
docstring of `unscored()` describe this, so that nobody tries the `filter` key again.

**Measured effect.** Top 10 for `de`: 33.7 ms to 3.0 ms; `klimaat`: 4.2 ms to 1.2 ms (shared partition of 13.4 M
documents, no parallel workers, 22 Nov 2026). In SQL on a synthetic 156,000 document partition with three projects,
no workers: top 10 for `de` 5.8 ms to 2.9 ms, `woord5` 4.0 to 2.9 ms, `de` with a publisher filter 4.8 to 3.9 ms;
counts unchanged in time (commit `1ec626bd`).

**Visible change.** Counts and hit sets do not change. The relevance order changes once, to the plain BM25 order of
the query string (the same order as a search without any project clause). This is the only intended change of
results on the branch.

**Switch.** `AMCAT4_FILTER_CLAUSES` (default `true`). `false` gives the original form, in which every clause is
scored.

**Risks and trade-offs.** If pg_search ever changes how it handles `const_score`, isolation depends on it. The
isolation tests in a shared partition run in the new form. Write paths keep their SQL conditions.

**Files and commits.** `backend/amcat4/postgres/search.py` (`unscored`, `compile_search`). Commit `1ec626bd` (in one
commit with 4.3, because 4.3 alone makes very common words slower).

### 4.3 No parallel workers for API queries

**Problem.** Starting parallel workers costs more than it saves for searches of a few milliseconds (cause 5), and
under load a busy server started up to four extra processes per request.

**Change.** The connections of the API pool start with `max_parallel_workers_per_gather = 0`. Background jobs,
maintenance, CLI commands and long writes keep the server's setting: they use a second connection pool (section
4.20).

**Why this solution.** A per-pool setting affects only the short API queries. A server-wide setting would also take
the workers away from index rebuilds and jobs, which do benefit from them. Workers help only the very common words
(count of `de`: 16.8 ms with 4 workers, 19.7 ms without; top 10 for `de`: 18.9 ms against 34.1 ms; `psql`, speed
research, 22 Nov 2026), and the unscored project clause (4.2) fixes those in a better way. That is why 4.2 and 4.3
were committed together.

**Measured effect.** In `psql` on the scratch copy, 22 Nov 2026, 4 workers against 0: count `klimaat` 10.0 to 3.7 ms,
count `regering` 10.5 to 4.6 ms, whole-project count 9.1 to 1.3 ms, count per publisher 10.2 to 6.2 ms. Through the
API on the first fast-path version, only this setting changed: `klimaat` 21.9 to 9.1 ms, `"tweede kamer"` 46.7 to
26.2 ms, `de` 43.7 to 59.4 ms (worse until 4.2).

**Switch.** `AMCAT4_API_PARALLEL_WORKERS` (default `0`). An empty value or `default` keeps the server's setting, as in
the original. A number sets that number of workers.

**Risks and trade-offs.** A request class that would really benefit from workers (very large projects, queries over
many projects) could get slower. This was not measured.

**Files and commits.** `backend/amcat4/connections.py` (`_api_options`), `backend/amcat4/config.py`. Commit
`1ec626bd`.

### 4.4 Custom plans

**Problem.** Generic plans after five executions (cause 2).

**Change.** API connections run with `plan_cache_mode = force_custom_plan`, so PostgreSQL plans each statement for
its actual values.

**Why this solution.** It keeps psycopg's prepared statements and removes only the switch to a generic plan.
Planning each statement costs 1 to 3 ms (code comment in `connections.py`), much less than the slowdown it prevents.
Disabling prepared statements in psycopg would be another route (inferred); it was not tried.

**Measured effect.** The night test showed the problem (84 ms for the first 5 requests, 469 ms from the 11th on, very
common word, original code, 2.68 M documents, pg_search 0.26.0). The effect of the fix alone was not measured
separately; it is part of all later measurements.

**Switch.** `AMCAT4_CUSTOM_PLANS` (default `true`). `false` gives the PostgreSQL default.

**Files and commits.** `backend/amcat4/connections.py` (`_open_pool`). Commit `b0d75679`.

### 4.5 Counts: one count, next to the page

**Problem.** Each extra statement costs about 2.4 ms of planning (speed research, 22 Nov 2026). The first fast-path
version counted in two steps: first up to 1,000 hits one by one, then, if there were more, a full count in the
index. In the first fast-path version, a search for `klimaat` sent three statements: capped count 5.6 ms, full
count 12.9 ms, page 11.7 ms (speed research, 22 Nov 2026). The original branch also counted the total again on every
page of a scroll.

**Change.**

1. **One count** in place of the two-step count. The two steps paid off when an in-index count had a floor of 7 to
   12 ms with parallel workers; without workers (4.3) a full count takes 2 to 4 ms.
2. **The count runs next to the page**, on a second connection, at the same time (`asyncio.gather`), when the pool
   has one to spare. The request then takes as long as the slower of the two, not the sum.
3. **No deadlock on a busy pool.** The request already holds one connection. A second one is taken only when at
   least two are free (idle, or room to open one) and nobody is waiting, and only with a wait of 0.2 s
   (`SPARE_TIMEOUT`). Otherwise the count runs after the page on the same connection. Both statements are always
   awaited before the second connection goes back to the pool, also after an error.
4. **No count when the first page is not full**: the total is then the number of rows. (Kept from the first fast-path
   version.)
5. **A scroll counts once**, on its first page, and carries the total in the cursor. The code comment says the count
   was about two thirds of the work of a download; this share comes from the commit history and was not re-measured.

**Why this solution, and the alternatives.** One statement that returns both the page and the total would save a
statement too; whether pg_search can do that was not established, and it was not pursued. A cache of counts keyed by
a per-project version number was proposed in the research notes and not built. The two-step count stays available
behind a switch.

**Measured effect.** In process (no HTTP), synthetic 156,000 document partition, 10 results per page, median of 15
(commit `698ccb50`):

| Search | Two-step, one after the other | One count, one after the other | One count, in parallel |
|---|---:|---:|---:|
| `klimaat` (25 hits) | 6.5 ms | 6.8 ms | 6.3 ms |
| `woord5` (58,000 hits) | 9.3 ms | 8.0 ms | 4.9 ms |
| `de` (150,000 hits) | 10.0 ms | 9.0 ms | 8.4 ms |
| `de` with a filter | 12.6 ms | 10.1 ms | 8.8 ms |
| no query | 6.0 ms | 4.4 ms | 3.4 ms |
| phrase (71,000 hits) | 48.6 ms | 45.5 ms | 25.5 ms |

Not measured under concurrent load.

**Switches.** `AMCAT4_CAPPED_COUNT` (default `false`; `true` gives the two-step count). `AMCAT4_PARALLEL_COUNT`
(default `true`; `false` runs the count after the page, as in the original).

**Risks and trade-offs.** A search may use two connections while the pool has room, which could matter at high
request rates; the fallback then makes it sequential. On a first page that is not full, the parallel count has run
for nothing (cheap, as there are few hits). This is left as is on purpose: whether the page is full is only known
when it is fetched, and waiting for that is exactly the time the parallel count saves. `spare_connection` reads
psycopg_pool's statistics keys (`pool_available`, `pool_size`, `requests_waiting`, checked for psycopg_pool 3.3.3);
a missing key means "no spare connection", and a test fails if a key disappears.

**Files and commits.** `backend/amcat4/postgres/search.py`, `backend/amcat4/postgres/connection.py`
(`spare_connection`), `backend/amcat4/projects/query.py`. Commits `b0d75679` (count once per scroll), `3f157381`
(two-step count, no count for a page that is not full), `698ccb50` (one count, in parallel), `5754e81a` (safe
defaults for the pool statistics), `af58819a` (why the wasted parallel count is left).

### 4.6 Paging and the new sort key `_seq`

**Problem.** Deep offset paging (cause 7): a download of all results of a query got slower with every page.

**Change.** A new sort key `"sort": "_seq"` orders by the internal id, the order in which documents were stored. It
is accepted as `"_seq"`, `["_seq"]` or `[{"_seq": {"order": "asc"|"desc"}}]`. Its `next` cursor is "after this
document" (a keyset cursor), which stays fast at any depth, with a query and filters too. A request for a page
number greater than 0 with `_seq` uses offset paging in the same order, and its `next` cursor is a keyset cursor
again. Without sort and without query, paging uses the keyset cursor as before. `"_id"` keeps its original meaning:
the document id (`doc_id`).

**Why this solution, and the alternatives.** The first attempt (commit `aedef146`) gave `"_id"` the meaning of the
internal id with a keyset cursor. That changed the meaning for existing clients and ignored page numbers (page 1
returned page 0). Commit `4b9ebec4` restored `"_id"` and added `_seq` as a new key. A separate key does not change any
existing request. Ordering by the internal id is also a stable order, which repeatable downloads need, and pg_search
can use it for a Top-K scan.

**Measured effect.** In `psql`, speed research, 22 Nov 2026: one page of 200 documents (`regering`, 133,210 hits) by
score at offset 0 / 20,000 / 100,000 took 5.3 / 29.7 / 83.1 ms; by id after a given id 3.9 ms at any depth, 4.4 ms
for 1,000 documents. In process on a synthetic 156,000 document partition with an earlier form of this change
(commit `aedef146`): a whole download of a query with 58,182 hits at 1,000 per page ran at 25,500 documents/s by
offset and 107,600 documents/s by id. In the short three-way test of 22 Nov 2026 (section 6) a download with `_seq`
ran at 13,217 documents/s through the API.

**Files and commits.** `backend/amcat4/postgres/search.py` (`SEQ`, keyset with `descending`),
`backend/amcat4/projects/query.py` (`_seq_order`, cursor), `backend/amcat4/api/index_query.py` (descriptions).
Commits `aedef146`, `4b9ebec4`.

### 4.7 A limit on offset depth, and checked cursors

**Problem.** A request for a deep page number in score or field order is slow and has no good use: all earlier hits
are sorted and thrown away. A hand-made cursor with a wrong value reached the SQL statement (a database error, HTTP
500).

**Change.**

- `AMCAT4_MAX_OFFSET` (default 10,000; 0 = no limit): a request for a page number with `offset + per_page` beyond the
  limit gets HTTP 400 with a message that says to use `"sort": "_seq"`. Keyset paging is never limited.
- The limit applies **only to explicit page numbers**, never to a cursor the server issued. In the first form
  (commit `aedef146`) a download that followed the `next` offset cursors of a query without sort failed halfway with
  400; commit `4b9ebec4` removed that.
- Every cursor value is checked (commit `6f73a9de`): a JSON object with known keys only, exactly one of `id` or
  `offset`, plus `total` and `desc` (keyset only); numbers must be non-negative integers that fit a bigint, `desc`
  a boolean. Anything else is "Invalid cursor" (HTTP 400). Issued cursors are unchanged.

**Why this solution, and the alternatives.** Elasticsearch has the same kind of limit (`max_result_window`), so
clients that work with Elasticsearch already page this way. **Signing cursors** was considered and rejected: a
forged offset cursor can only page deeper than the limit, and the limit is a performance guard, not a security
boundary. What a user may see is decided by fields and roles, as for any page. An issued offset cursor has no
property that a forged one cannot copy, so only signing could tell them apart.

**Measured effect.** None needed: the limit only refuses requests.

**Switch.** `AMCAT4_MAX_OFFSET=0` (no limit, as in the original).

**Risks and trade-offs.** A client that asks for page numbers beyond 10,000 results in score order gets an error. It
should use `_seq`, or follow the `next` cursor.

**Files and commits.** `backend/amcat4/projects/query.py` (`_decode_cursor`, the max_offset check). Commits
`aedef146`, `4b9ebec4`, `6f73a9de`.

### 4.8 Aggregations inside the index with `pdb.agg`

**Problem.** Aggregations were GROUP BY statements on the parent table (cause 1). Even on the partition table,
pg_search pushes a GROUP BY into the index only for some shapes. For tag axes (a lateral join over the tag list),
metrics (casts), quarters (`date_trunc`) and others it fetches every matching row and unpacks its JSON: 0.6 to
0.8 s per million documents (module docstring of `index_aggregate.py`).

**Change.** A new module computes the common shapes with pg_search's `pdb.agg` (Elasticsearch-style aggregation
JSON), which reads the columnar fields of the index directly:

- axes: keyword and tag fields, date fields with an interval that is stored as a derived key (year, quarter, month,
  ...), nested axes as nested terms aggregations;
- metrics: avg, min, max and sum of number and integer fields, through a stats aggregation;
- a single project per field (fields of several projects have different keys; GROUP BY handles that).

The order of paths in `_aggregate`:

1. no fast path, or at most 1,000 hits (`SMALL_RESULT`): the original GROUP BY on the parent table, because the
   in-index paths cost at least 10 to 25 ms whatever the number of hits;
2. `pdb.agg` on the partition table, for the shapes it handles;
3. the same GROUP BY on the partition table, which pg_search can partly do in the index;
4. if 3 fails with a query or feature error: the original GROUP BY on the parent table.

**Correctness rules.** Every terms aggregation asks for all buckets (65,000, pg_search's `max_term_agg_buckets`). If
pg_search reports left-out buckets (`sum_other_doc_count > 0`) or fails, the result is not used and GROUP BY runs.
A bucket without values gives null for sum, as GROUP BY and Elasticsearch do (pg_search's `pdb.agg` gives 0.0;
commit `2b61f36c`). Top hits (a window function) also runs on the partition table, with the same fallback rule
(commit `e130d564`).

**No slower fallback after a timeout.** Before commit `fc50ff83`, every `psycopg.Error` led to the slower fallback
statement, also after a statement timeout: a request that had used its time limit ran a second, slower statement.
Now cancelled statements, other `OperationalError`s (lost connection, shutdown, out of memory, lock timeout) and
`InterfaceError` are raised at once (`must_not_fall_back`). Fallbacks remain for query and feature errors.

**Why this solution.** The original author notes that pg_search "can't aggregate on calendar units like month or
year inside the index", which the derived date keys of the original branch already address. `pdb.agg` uses those
keys and the columnar fields, so no schema change was needed. GROUP BY stays as the general and fallback path, so
every shape still works.

**Measured effect.** On the first fast-path version, synthetic 1 M document corpus, in process, median of 9
(20 Nov 2026), original against new: aggregation per publisher 419.9 to 13.0 ms, per tag 778.6 to 37.8 ms, per
quarter 659.1 to 14.1 ms, tags by publisher 1,658.0 to 48.1 ms, publisher with average word count 775.1 to 18.4 ms;
a small aggregation (25 hits) 5.3 to 6.8 ms (a small loss). In the short three-way test of 22 Nov 2026 the median of
15 aggregation kinds went from 237.8 ms (original branch) to 22.1 ms (this branch); Elasticsearch: 28.9 ms.

**Switch.** `AMCAT4_FAST_PATH=false` (the original GROUP BY). The test setting `AMCAT4_TEST_INDEX_PATHS=1` forces
every test through the in-index paths.

**Risks and trade-offs.** Two code paths must give the same rows; tests check parity of every aggregation, metric and
search shape with the original SQL. More than 65,000 groups falls back to GROUP BY.

**Files and commits.** `backend/amcat4/postgres/index_aggregate.py` (new), `backend/amcat4/projects/aggregate.py`,
`backend/amcat4/postgres/connection.py` (`must_not_fall_back`). Commits `dcb93fa3`, `2b61f36c`, `fc50ff83`,
`e130d564`.

### 4.9 The derived quarter key

**Problem.** Quarter aggregations had no derived key, so GROUP BY used `date_trunc` per row and `pdb.agg` had to
merge month buckets; metrics per quarter were not possible inside the index.

**Change.** Date fields get a derived key `<field>_quarter` (the first month of the quarter, for example `2024-04`,
so it sorts and converts like the month key). It is written at upload, update, conversion and copy like the other
derived keys. Documents stored before migration `0002` have no quarter key: `0002` marks existing date fields with
`fields.quarter_ready = false`, and for those fields the old paths stay in use. A periodic task (every minute)
creates a `backfill_quarter` job per project with fields that are not ready; the job adds the key in batches
(resumable, with SQL that gives the same value as the Python function, checked in a test) and then marks the fields
ready. A failed backfill is retried after 5 minutes, at most 3 failed jobs in all. It waits for a project move
instead of failing (section 4.18).

**Why this solution, and the alternatives.** A backfill job plus a readiness flag was chosen over a fallback at query
time, so the fast path needs no extra statement per aggregation. Results are the same before, during and after the
backfill.

**Measured effect.** Not measured separately.

**Switch.** `AMCAT4_QUARTER_KEY` (default `true`; `false` uses the old paths).

**Files and commits.** `backend/amcat4/postgres/fields.py` (`DATE_DERIVED`, `quarter_sql`, `use_quarter_key`),
`backend/amcat4/projects/jobs.py`, `backend/amcat4/postgres/index_aggregate.py`, migration `0002`. Commits
`44a60d87`, `e889c962`, `9da48b8c`.

### 4.10 Late fetch for sorts on JSON fields

**Problem.** A sort on a JSON value cannot be a Top-K scan (cause 6), so every hit is read and sorted, with all its
columns (text, snippets).

**Change.** When the sort is on a JSON value (or on `_id`, the `doc_id` column, which is not in the index), the hits
are first sorted and paged with only the internal id, and then the full rows of just that page are fetched with the
same statement restricted to those ids. Random order is excluded, as two statements would give different orders.

**Why this solution.** It needs no schema change and gives the same rows in the same order. It does not make such a
sort a Top-K scan; that needs a sort slot (4.11).

**Measured effect.** Not measured against the original separately. With late fetch, such a sort takes about 60 ms at
46,000 hits (2.68 M document project, 22 Nov 2026, code comment in `search.py`).

**Switch.** `AMCAT4_FAST_PATH=false`.

**Files and commits.** `backend/amcat4/postgres/search.py` (`_needs_late_fetch`, `page_rows`). Commit `59aad158`.

### 4.11 More sort slots (`fast_sort`)

**Problem.** Only `date` and `source` could be sorted with a Top-K scan. In the night test a sort by publisher took
580 ms (original branch, no slot used, because the field was not called `source`).

**Change.** Migration `0002` adds five generic slot columns: `sort_kw1`, `sort_kw2`, `sort_kw3` (text, `COLLATE "C"`,
for keyword and url fields), `sort_num1` (numeric, for number and integer fields) and `sort_date2` (timestamptz, a
second date field). They are in the BM25 index. `fast_sort: true` on a field takes the first free slot of its kind
(date, then `sort_date2`; source, then `sort_kw1` to `sort_kw3`; `sort_num1`), or gives an error that names the fields
that use them. Tag fields cannot be put in a slot (several values per document). `fast_sort: false` frees the slot and
clears its column. The automatic mapping is unchanged (first date field to `date`, a keyword field called `source` to
`source`). A type change keeps the slot within the same kind and frees it otherwise. Uploads, updates, update by
query and copies fill all slot columns.

**Why this solution, and the alternatives.**

| Alternative | For | Against | Verdict |
|---|---|---|---|
| Wait for pg_search to sort on JSON keys | The original author expected this ("I do expect that pg_search will address this at some point") | Not available in 0.27 (layout notes) | Not chosen for now; the slots remain useful afterwards (inferred) |
| Index expressions per promoted attribute, for example `(((exact_fields->>'f14') COLLATE "C")::pdb.literal('alias=p_f14'))` (tested in SQL, 22 Nov 2026) | No new table column; no row rewrite | Each change of the list needs an index rebuild of the partition; keys (`f14`) differ per project, so in a shared partition the index definition would name fields of single projects | Rejected for shared partitions; fits a project alone in its partition (inferred) |
| Spare real columns ("slots"), mapped per project by `fields.sort_slot` | Extends what the original branch already does with `date` and `source`; every project maps its own fields to the same columns; no rebuild per promotion | Fixed number of slots, chosen once; filling a slot rewrites the project's rows | Chosen |
| Named standard columns (for example `author`, `language`) instead of generic slots | Would also serve discovery across projects | Less flexible per project | Open question (section 7.2); the default is generic |

Five slots (three keyword, one number, one date) was a choice for this experiment, not a measured optimum.

**Measured effect.** In SQL with promoted index fields (the index expression form, which reads the same kind of
columnar field as a slot; `psql` on the scratch copy, 22 Nov 2026): `klimaat` sorted by publisher, top 10, 48.7 ms
on the JSON attribute against 4.6 ms on the promoted field; sorted by author 2.9 ms. Promoted fields gave **no gain
for filters or aggregations** (count per publisher 4.3 against 4.2 ms; filter count 1.2 against 1.1 ms), so slots are
only for fields people sort on. EXPLAIN in the tests checks that a sort on every slot type is a Top-K scan, with and
without a query. In the short three-way test (section 6) "sorted results" went from 477.2 ms (original branch, no
slot) to 12.4 ms (this branch, `publisher` and `author` in slots).

**Isolation fix (commit `75c52261`).** A slot column holds, per project, whichever field that project put in the
slot. In a query over several projects, sorting on one project's field by its slot column ordered the documents of
the other projects by their own field in the same slot, which may be a hidden field. Now the slot column is used only
when every project of the query has this field in the same filled slot; otherwise the JSON value is sorted on, where
documents of projects without the field are NULL and come last. This holds for `date` and `source` too.

**Switch.** No single switch: `fast_sort` is opt-in per field (except the original automatic mapping).

**Files and commits.** Migration `0002`, `backend/amcat4/postgres/fields.py` (`_SORT_SLOTS`, `SORT_SLOTS`,
`SLOT_TYPES`), `backend/amcat4/systemdata/fields.py`, `backend/amcat4/postgres/documents.py`,
`backend/amcat4/postgres/search.py` (`slot_sort_column`), `backend/amcat4/models.py`. Commits `f157c1bd`, `2bdae391`,
`75c52261`.

### 4.12 Filling sort slots: the fill job

**Problem.** Filling a slot rewrites every document of the project that has a value, too long for one request on a
large project. And pg_search indexes each rewritten row again.

**Change.**

- Projects with more than `AMCAT4_SORT_SLOT_JOB_ROWS` documents (default 10,000) fill the slot in a background job,
  `fill_sort_slot`, in batches of 10,000 documents. The job is resumable; rows that already have the right value are
  not written.
- The slot is reserved at once (`fields.sort_slot_ready = false`): uploads and updates fill it from then on, but
  sorting does not use it, and the API does not show it, until the job has filled every document.
- An advisory lock per project and slot keeps a fill and a clear from overlapping. A job stops if the slot was given
  to another field meanwhile. `fast_sort: true` on a slot that is reserved but has no running fill job (it failed or
  was cancelled) starts the fill again.
- **Slots requested together are filled in one pass** (commit `7d0e44a2`): one UPDATE per batch sets every requested
  slot column, so N slots rewrite each row once, not N times. A new slot is added to a pending fill job of the
  project that has not started yet.
- The job ends with a vacuum of the partition (section 4.14).

**Why this solution.** A job keeps the request short and is resumable. Filling all requested slots in one pass came
from a measurement (below). Filling in the request remains available for small projects and as a switch.

**Measured effect.** On 2.68 M real documents (22 Nov 2026), with one job per slot (before commit `7d0e44a2`):
filling two slots took more than 10 minutes, searches were 2 to 4 times slower meanwhile, and an index rebuild
followed. The one-pass version was not re-measured at that scale; a test checks that three slots are filled in one
pass with every statement setting all three. After a fill of 270,000 documents on a 300,000 document scratch
database, a sort on the slot took 187 ms before VACUUM and 3.4 ms after (section 4.14).

**Switch.** `AMCAT4_SORT_SLOT_JOB_ROWS` (default 10000; `-1` = always in the request, as in the original; `0` = always
a job).

**Risks and trade-offs.** Filling a slot on a large project is a **maintenance operation**: do it when the project is
not in heavy use, and put all fields that need a slot in one request. The `fast_sort` description in the API says so.

**Files and commits.** `backend/amcat4/projects/jobs.py` (`_fill_sort_slot_job`, `queue_fill_sort_slot`),
`backend/amcat4/postgres/documents.py` (`fill_sort_slots`), `backend/amcat4/systemdata/fields.py`. Commits `2bdae391`,
`57eaa02d`, `9da48b8c`, `7d0e44a2`, `5e2a4c58`.

### 4.13 Byte order for text sorts

**Problem.** The text slot columns use `COLLATE "C"` (byte order), which pg_search needs for a Top-K sort. Text values
outside a slot were sorted in the database collation. So the order of mixed-case values would change when a field
gets a slot, or while its slot is being filled.

**Change.** Keyword, url, text and tag values that are not in a slot are sorted with `COLLATE "C"`.

**Why this solution.** The order is then the same with and without a slot. Byte order is also Elasticsearch's order
for keywords. The alternative, a linguistic collation on the slot columns, would prevent the Top-K sort (code comment
in `order_by`).

**Measured effect.** Not a speed change; not measured.

**Switch.** `AMCAT4_BYTE_ORDER_SORT` (default `true`; `false` uses the database collation, as in the original).

**Files and commits.** `backend/amcat4/postgres/search.py` (`order_by`). Commit `2bdae391`.

### 4.14 VACUUM after large changes

**Problem.** pg_search's fast count and other in-index shortcuts skip the visibility check only for pages that the
visibility map marks all-visible, and only VACUUM updates that map. Autovacuum starts late on big tables (by default
after 20 % of the rows have changed), so after a large upload, update by query or slot fill, counts and sorts could
stay slow.

**Change.** After an upload, update by query, tag update by query or delete by query, `vacuum_soon` creates a
`vacuum` job for each affected partition if the change was large: at least `AMCAT4_VACUUM_AFTER_ROWS` documents
(default 10,000), either in this request or, according to PostgreSQL's statistics (`n_ins_since_vacuum + n_dead_tup`),
since the last vacuum, which also catches many small uploads. The job runs `VACUUM (ANALYZE)` on the partition. At
most one pending vacuum job exists per partition: the check and the insert run in one transaction under an advisory
lock per partition (commit `e453f2c0`; eight concurrent calls created eight jobs before, one now). The sort slot fill
and quarter backfill jobs also end with a vacuum. `vacuum_soon` never fails a write.

**Why this solution, and the alternatives.** VACUUM cannot run inside a transaction and takes a while on big
partitions, so it runs in the job worker, not in the request. The alternative is to leave it to autovacuum, as the
original does.

**Measured effect.** After filling a sort slot for 270,000 documents on a 300,000 document scratch database, a sort on
the slot took 187 ms before VACUUM and 3.4 ms after in SQL, 5.9 ms through `query_documents` (commit `57eaa02d`). The
research notes also observed that on a freshly vacuumed table the count skipped the visibility check for all
segments.

**Switch.** `AMCAT4_VACUUM_AFTER_ROWS=0` (off; autovacuum only, as in the original).

**Risks and trade-offs.** A vacuum of a big partition uses I/O. Jobs run one at a time per worker (section 7.1).

**Files and commits.** `backend/amcat4/projects/maintenance.py` (`vacuum_soon`, `vacuum_job`),
`backend/amcat4/projects/query.py`, `backend/amcat4/projects/documents.py`. Commits `e2a4634e`, `57eaa02d`,
`e453f2c0`, `19e5e18d`, `335d5ed9`.

### 4.15 pg_search 0.27.0 and maintenance that fits background merging

**Problem.** The original branch was pinned to pg_search 0.26.0, which in the tests never merged index segments. Its
maintenance rebuilds an index when it has many segments or many deleted documents, once the partition has been quiet
for 15 minutes (or after 24 hours). Newer pg_search versions build faster indexes: on 0.26.6 after a rebuild, a count
of `de` took 2.8 ms against 19.7 ms on 0.26.0 (scratch copy, `psql`, 0 workers, 22 Nov 2026; a rebuild of 2.68 M
documents took 3 min 19 s). The gain comes from "bitmap postings" (from 0.26.2), which exist only in indexes built
on the new version, so an upgrade without a rebuild gives nothing.

**Change.**

- The deploy files, the CI workflow and the README pin `paradedb/paradedb:0.27.0-pg18` (`PARADEDB_VERSION` still
  overrides it). The test suite passes on 0.26.0 and 0.27.0.
- pg_search 0.27.0 merges small segments in the background. Measured: 20 single-row inserts left 15 segments, 5 a
  few seconds later; 0.26.0 kept 20 (commit `79c83985`). So the segment count stays low and the segment trigger of
  the automatic rebuild rarely fires on 0.27; the trigger for deleted documents stays relevant, and the rebuild stays
  as the safety net on both versions.
- A merge that pg_search is still running counts as activity (`paradedb.is_merging`, which exists on both versions),
  so a partition is not rebuilt while the merge may solve the problem by itself.
- Migration `0002` runs `ALTER EXTENSION pg_search UPDATE` first if the database's extension is older than the
  server library (after an image upgrade), in a savepoint, so a failure only logs a warning. Tested: a 0.26.0
  database opened by the 0.27.0 image, then `0002`.

**Why this solution, and the alternatives.** 0.26.6 was the measured fallback if 0.27.0 had failed in tests. 0.27.0
was chosen as the newest release, after the test suite passed on it. The release notes of 0.27.0 list one breaking
change, in the vector part (research notes). The rebuild that brings the bitmap postings is bundled with the
rebuild of migration `0002` (4.16), so the index is rebuilt once.

**Measured effect.** The merge behaviour above. The speed of 0.27.0 itself was not measured separately from the
other changes.

**Switch.** `PARADEDB_VERSION` in the deploy files.

**Files and commits.** `deploy/docker-compose.yml`, `deploy/.env.example`, `.github/workflows/unittests.yml`,
`backend/amcat4/projects/maintenance.py`, `backend/amcat4/postgres/layout.py` (`index_merging`). Commits `79c83985`,
`f068a38e`, `1229c6bf`.

### 4.16 Migration `0002`: slot columns, an id index, one index rebuild, lz4

**Problem.** The slot columns (4.11), a B-tree index for listings (4.17), the rebuild for the new pg_search
version (4.15) and compression all change the table or the index.

**Change.** One migration, `0002_sort_slots_and_indexes.py`:

- `documents` gets `sort_kw1..3` (`text COLLATE "C"`), `sort_num1` (numeric) and `sort_date2` (timestamptz). Adding
  empty columns does not rewrite the table.
- `fields` gets `sort_slot_ready` and `quarter_ready`.
- A B-tree index `(partition_id, project_pk, id)`.
- The BM25 index is rebuilt with the slot columns; every entry and option of `0001` is kept.
- `text_fields` gets lz4 compression for new rows. `SET COMPRESSION` on the parent does not reach existing
  partitions, so each partition gets it, and new partitions are created with `LIKE ... INCLUDING COMPRESSION`.
- `downgrade()` rebuilds the `0001` index the same way, drops what `0002` added, deletes job types the `0001` code
  does not know, and clears slots that `0001` does not know or that were still being filled.

**How the rebuild stays online.** `CREATE INDEX` on a partitioned table cannot be `CONCURRENTLY`. So the parent index
is created `ON ONLY` the parent, each partition's index is built `CONCURRENTLY` and attached, then the old BM25 index
is dropped and the new one renamed. This needs a second copy of the search index on disk meanwhile (the migration
logs the current size). It holds the reindex advisory lock, so the automatic maintenance does not rebuild at the
same time. All steps are idempotent: an interrupted run can be run again. A partition index with the expected name
that is attached to another parent (after an interrupted run) is left alone, and the partition gets an index under a
free name (commit `1fefe694`).

**Lock handling.** `ALTER TABLE documents` needs an exclusive lock. Behind a long query, every new query would wait
behind it. Such statements now run with `lock_timeout` 5 s in a savepoint, up to 6 attempts with short pauses, and
then stop with `TableBusyError`, which says that nothing was changed and that the migration can simply be run again
(`migrations/locking.py`, commit `1fefe694`).

**Why this solution, and the alternatives.** Editing `0001` was rejected: the layout notes say migrations "must never
be edited once released", and an existing database would not see the change. Several smaller migrations would each
rebuild the index; one migration rebuilds it once. A rebuild is online and took 2 to 3.5 minutes per 2.7 M documents,
about 7.5 minutes at 13.4 M (speed research, 22 Nov 2026).

**Measured effect.** lz4: not measured (the gain on downloads is inferred). The index rebuild time above.

**Risks and trade-offs.** Disk space for a second index during the migration. The B-tree index adds a little work per
insert (not measured) and disk (about 400 MB per 13.4 M documents, inferred from the similar primary key in the
research notes).

**Files and commits.** `backend/amcat4/migrations/versions/0002_sort_slots_and_indexes.py`,
`backend/amcat4/migrations/locking.py`, `backend/amcat4/postgres/layout.py` (`create_partition`). Commits `f157c1bd`,
`1fefe694`.

### 4.17 Listing without a query over the B-tree index

**Problem.** Opening a project without a search lists the first documents by id. Through the search index, that
listing goes over the ids of all projects in the partition.

**Change.** A search without a query string and without filters that need the search index, in id order (keyset or
no sort), is plain SQL over the B-tree index `(partition_id, project_pk, id)` of migration `0002`, which reads only
the rows of the page. Counts stay in the search index.

**Why this solution.** The primary key alone is not enough in a shared partition: for a small project it would skip
over the neighbours' rows. An index that starts with the partition and the project solves that.

**Measured effect.** First 10 documents, in `psql`, before migration `0002`, on a copy of the 2.68 M document project
alone in its partition: 13.5 ms through the search index, 0.14 ms over the primary key B-tree (pg_search 0.26.0,
22 Nov 2026); 1,000 documents after a given id 0.26 ms. In the short three-way test "no query, filters only" went
from 99.8 ms to 13.2 ms (with all other changes).

**Switch.** `AMCAT4_BTREE_LISTING` (default `true`; `false` lists through the search index, as in the original).

**Files and commits.** `backend/amcat4/postgres/search.py` (`plain_where`), migration `0002`. Commit `f157c1bd`.

### 4.18 Moving a project to a partition of its own (migration `0003`)

**Problem.** A big project in a shared partition is slower than it needs to be for whole-project counts and
aggregations, its relevance scores depend on the other projects, and its index cannot be rebuilt on its own. The
original layout foresaw a move, as an update of `projects.partition_id`. In `0001`, the foreign key
`documents(project_pk, partition_id) -> projects(pk, partition_id) ON UPDATE CASCADE` makes that one statement: one
huge transaction that locks every document of the project until the end, and that does not allow a project's
documents to be in two partitions.

**Change.** Migration `0003`:

- Documents now reference `project_partitions (project_pk, partition_id)`: one row per project, two while it is being
  moved. A trigger keeps it in sync with `projects.partition_id` and keeps the old behaviour: setting
  `projects.partition_id` directly still moves all documents in that statement (the original test passes unchanged).
- `projects.moving_to` (the target during a move) and `projects.own_partition`.
- The new foreign key is added `NOT VALID` and validated in its own transaction, which reads all documents but blocks
  neither reads nor writes. The downgrade restores the `0001` key the same way (refused while a project is moving).

The job `move_project`, started with `amcat4 move-project --project <id> [--now]` (an admin CLI command):

1. creates a new partition, registers it, sets `moving_to` and `own_partition`, and saves the target in the job's
   progress, in one transaction (so a resumed job never creates a second partition; commit `927e6666`), after waiting
   for running writes to the project;
2. waits a grace period (`MOVE_GRACE_SECONDS`, 60 s), then moves the documents in batches of 10,000 (an UPDATE of
   `partition_id`; each row moves atomically, and its vectors move along through their foreign key); resumable;
3. switches `projects.partition_id` in a short transaction and schedules a vacuum of both partitions.

During the move:

- **Reads are never blocked.** Every read path looks in both partitions and sees each document once. The partition
  and the move target are read in one statement (commit `5b4be135`; before, a move that finished between two reads
  gave 0 documents). The single-partition fast paths are not used.
- **Writes are refused** with a conflict (HTTP 409): uploads look up existing documents in one partition. All write
  functions of the storage layer are wrapped in `writing()`, a shared advisory lock that the move waits for, plus a
  check of `moving_to`.
- Fill, backfill and copy jobs are postponed (`JobPostponed`, retried after 30 s) instead of failing (commits
  `9da48b8c`, `6d5d5664`). A move is refused while the project has unfinished jobs.
- New projects are never assigned to a partition that a project has, or gets, for itself.
- The number of moved documents is counted in the target partition whenever the job (re)starts (commit `b02a0246`).
- Project size and the partition overview count both partitions (commit `d6e46cf7`).
- `--now` runs or waits for only its own job and reports its real result (commit `97552eaf`).

**Why this solution, and the alternatives.**

| Alternative | Verdict |
|---|---|
| One UPDATE of `projects.partition_id` (the original route) | Kept for compatibility, but locks every document of the project until the end |
| One partition per project for all projects | Changes hosting cost for the long tail of small projects, which the original design shares on purpose |
| pg_search's `partition_by` index option | Beta; not tested |
| A shared read lock per request instead of a grace period | Rejected: an extra statement on every read, and reads could hold off a move for as long as they keep coming |
| A grace period before the first batch | Chosen: 60 s covers a pool wait plus two statements at the default API timeout |

**Measured effect.** For 2.68 M documents next to 10.7 M others (`psql`, pg_search 0.26.0, 0 workers, speed research,
22 Nov 2026): whole-project aggregation 14.5 ms shared against 6.2 ms alone; whole-project count 3.4 ms against
1.3 ms; searches gain little (count `klimaat` 4.2 against 3.7 ms). The move itself was run on a scratch database with
3,000 documents through the CLI; its duration on a large project was not measured.

**Switch.** None needed: a move happens only when an administrator starts it.

**Risks and trade-offs.** Writes to the project are refused during the move. Two extra small statements per write for
the move guard (not measured). The grace period is not a guarantee for requests that run longer than 60 s after
reading their partitions. During the move, relevance scores mix the statistics of two indexes. Afterwards the old
partition has deleted documents in its index, which the automatic maintenance cleans up.

**Files and commits.** `backend/amcat4/projects/partitions.py` (new), `backend/amcat4/postgres/layout.py`
(`project_filter`, `writing`, `guard_writes`, `lock_writes`, `assign_partition`, `next_partition_id`),
`backend/amcat4/postgres/projects.py` (`project_layout`), `backend/amcat4/projects/jobs.py` (`JobPostponed`,
`run_job_now`), `backend/amcat4/__main__.py`, migration `0003`. Commits `e886b816`, `5b4be135`, `927e6666`,
`9da48b8c`, `6d5d5664`, `97552eaf`, `b02a0246`, `d6e46cf7`, `2e56d130`.

**The shared lock key.** The advisory lock that serializes creating partitions has the same key as the migration
lock, as in the original code. Kept on purpose (commit `2e56d130`): while migrations run (`0002` and `0003` change the
documents table and its partitions), no process creates a partition or starts a move. Distinct keys would end this,
also for an older server that keeps running during a rolling upgrade. The cost is that creating a project waits while
migrations run.

### 4.19 Optional prewarm

**Problem.** The first searches after a restart read the index from disk.

**Change.** With `AMCAT4_PREWARM=true`, a background task at startup loads the search index of every partition into
PostgreSQL's cache with `pg_prewarm`, creating the extension if possible. Without the extension it logs a warning and
continues.

**Why off by default.** The effect was not measured, and loading large indexes uses memory and I/O at startup.

**Switch.** `AMCAT4_PREWARM` (default `false`, as in the original).

**Files and commits.** `backend/amcat4/projects/maintenance.py` (`prewarm_indexes`), `backend/amcat4/api/__init__.py`.
Commit `e2a4634e`.

### 4.20 Two connection pools, a statement timeout and quick 503s

**Problem.** One pool served everything, so API tuning (4.3, 4.4) would also apply to jobs and maintenance. A runaway
search could hold a connection and a CPU for minutes. An exhausted pool made requests wait 30 s (psycopg_pool's
default) and then fail with HTTP 500 (according to the docstring of `pool_timeout_handler`).

**Change.**

- **Two pools.** `amcat4-api` (min 1, max 20 connections, the tuned options, a statement timeout) for API requests.
  `amcat4-work` (min 0, max 10, the server's own settings, the default 30 s wait) for background jobs, maintenance,
  CLI commands and long writes. The choice is a context variable (`server_defaults()`), so concurrent API requests are
  not affected by a job running at the same time. Long writes through the API (uploads, update or delete by query,
  clearing or deleting a project, creating, deleting and changing fields) use the work pool through the decorator
  `with_server_defaults` (`create_fields` was added in commit `eb2a4519`).
- **Statement timeout.** `AMCAT4_STATEMENT_TIMEOUT_MS` (default 30,000) on API connections. A stopped statement gives
  HTTP 503 with advice to try a more specific query. A statement cancelled for another reason (for example
  `pg_cancel_backend`, a shutdown) gets a 503 that does not claim a timeout (commit `08a8d5b7`).
- **Pool timeout.** `AMCAT4_POOL_TIMEOUT_S` (default 5): after that, HTTP 503 with `Retry-After: 1`, so clients can
  back off.

**Why this solution.** Separate pools keep each kind of work on settings that fit it, without `SET` statements per
job. A 503 is the standard answer for "busy, try again", and answering after 5 s instead of 30 s keeps queues short
(inferred).

**Measured effect.** Tests show that an exhausted pool gives 503 within about the pool timeout and that the server
recovers. Behaviour under real overload was not measured on this code.

**Switches.** `AMCAT4_STATEMENT_TIMEOUT_MS=0` (no limit), `AMCAT4_POOL_TIMEOUT_S=30` (the original wait).

**Risks and trade-offs.** A legitimate long read through the API is stopped after 30 s. The 503 for a timeout has no
`Retry-After`, because retrying the same query would time out again.

**Files and commits.** `backend/amcat4/connections.py`, `backend/amcat4/api/__init__.py`,
`backend/amcat4/postgres/connection.py`, `backend/amcat4/__main__.py`. Commits `1ec626bd`, `659a70d1`, `eb2a4519`,
`08a8d5b7`.

### 4.21 Estimated project size

**Problem.** `GET /index/{ix}` summed the size of every document of the project, which read the whole project: 12 s
for 10.7 M documents (night test, where the benchmark script then judged the server as not started).

**Change.** For projects over 20,000 documents, the size is the average size of a 20,000 document sample times the
number of documents, counted inside the index on the partition table. During a move both partitions are counted and
sampled (commit `d6e46cf7`).

**Measured effect.** On the first fast-path version, synthetic 1 M documents: 344 ms to 42 ms, 0.7 % off (20 Nov 2026).

**Switch.** `AMCAT4_FAST_PATH=false` (the exact sum).

**Risks and trade-offs.** The sample is the first 20,000 rows the index returns, not a random sample.

**Files and commits.** `backend/amcat4/projects/index.py`. Commits `59aad158`, `d6e46cf7`.

### 4.22 Bug fixes

These are correctness fixes. They have no switch.

| Bug | Fix | Evidence | Commit |
|---|---|---|---|
| Wildcard expansion cap of 50 (cause 9) | A single-word prefix is expanded fully (`max_expansions` 2^31 - 1); it costs about as much as a plain word with the same number of hits. A prefix at the end of a phrase (`"tweede ka*"`) keeps a bound of 1,000, because 10,000 expansions took 5 to 20 s | `pol*` 13,678 against 592,319 documents before; in the short three-way test all 7 wildcard counts equal Elasticsearch on this branch, 1 of 7 on the original | `b0d75679` |
| Empty-string sort crash (cause 10) | `sort_key` treats an empty string as a value, not as "no value" | HTTP 500 on the `section` aggregation in every night pass; works on this branch | `b0d75679` |
| Concurrent duplicate upload (cause 11) | On `UniqueViolation`, retry once (the retry sees the other upload's documents); if it fails again, a clear error (HTTP 409) instead of HTTP 500 | 3 of 4 clients got HTTP 500 on the original, all 4 accepted on this branch (short three-way test) | `b0d75679` |
| Unstable uploads (cause 12) | `ANALYZE staging_documents` after the COPY: a few milliseconds for a few thousand rows | One night-test upload spent 626 of 632 s in one statement | `b0d75679` |
| Quarter metrics returned 0.0 for empty buckets | Stats aggregation, null when its count is 0 | Test against GROUP BY | `2b61f36c` |
| Slot sort across projects (4.11) | Slot only if every project has the field in it | Test | `75c52261` |

Partition ids are now never reused: `next_partition_id` also counts partitions without a valid BM25 index (for
example one whose index is still being built), which the original `list_partitions` leaves out (commit `e886b816`).

### 4.23 Review fixes

Stage 4 reviewed the earlier stages. Besides the fixes already named above, it changed:

- the help text of `optimize --project`, which described an old layout with 64 fixed partitions (`f068a38e`);
- an unused helper `pg_search_version()` removed: no code branches on the pg_search version (`f068a38e`);
- type annotations (no new type checker errors) (`6a5c7935`, `6062092a`);
- tests that depended on the state other tests left behind, or on delayed table statistics (`1229c6bf`, `97aef59f`,
  `19e5e18d`, `335d5ed9`), so the whole suite passes in either file order;
- comments that quoted measurements without saying on which code they were taken (`eb58cf1e`), and comments for every
  change since `bb35e43a`, checked by comparing syntax trees so that no code changed (`288e0da1`).

---

## 5. What users and client libraries notice

Nothing is removed from the API. The changes that a user, a client library (such as the R or Python clients) or an
operator can notice:

| Change | Who notices | Details |
|---|---|---|
| Relevance order changes once (4.2) | People comparing the first results before and after | Counts and hit sets are the same; the order becomes the plain BM25 order of the query. Relevance scores still depend on the other projects in a partition, as before |
| New sort key `_seq` (4.6) | Clients that download all results | `"sort": "_seq"` with the `next` cursor is the fast way to download all results of a query. The order is the order of storage |
| `_id` keeps its meaning | Nobody | `"sort": "_id"` still orders by document id |
| Offset limit (4.7) | Clients that request deep page numbers | `page` with `offset + per_page > 10,000` gets HTTP 400 that says to use `_seq`. Following `next` cursors is never limited |
| Invalid cursors (4.7) | Clients that make cursors by hand | HTTP 400 instead of a database error |
| HTTP 503 (4.20) | Scripts under heavy load, or with very slow queries | Busy pool: 503 with `Retry-After: 1` after 5 s. Statement timeout: 503 after 30 s, with advice to narrow the query |
| HTTP 409 during a move (4.18) | Uploads to a project that is being moved | Retry after the move |
| Byte-order sorting (4.13) | People sorting mixed-case text values | Upper case before lower case, as in Elasticsearch and in the slot columns |
| More `fast_sort` slots (4.11) | Project administrators | Up to three keyword or url fields, one number field and two date fields per project. A slot appears in the field's `sort_slot` only once it is filled |
| Cursor carries the total | Clients that scroll | The total of a scroll is counted once and repeated on every page |
| Upgrade of an existing database | Operators | Migrations `0002` and `0003` rebuild the search index once (minutes per few million documents) and need free disk of about the size of the index. Running `amcat4 migrate` before starting the API avoids downtime |

---

## 6. Results

### 6.1 Short three-way test, 22 November 2026

One run per version, one laptop, the same 2.68 million Dutch news articles, requests through the AmCAT API, no
authentication. The two PostgreSQL versions used the same database (PostgreSQL 18, pg_search 0.27.0, the collection
alone in its partition), one after the other; this branch ran second, after migrations `0002` and `0003`, with
`publisher` and `author` in sort slots. This branch was tested at commit `6a5c7935`, before the review fixes of
stage 4 (for example before slots were filled in one pass).

| Median response time (ms) | Elasticsearch (`master` 4.2.11, ES 8.18.4) | Original branch (`bb35e43a`) | This branch |
|---|---:|---:|---:|
| Rare word | 6.0 | 16.2 | 7.6 |
| Medium word | 8.8 | 27.3 | 8.9 |
| Common word | 8.8 | 108.1 | 11.8 |
| Very common word (`de`, `het`) | 8.3 | 490.1 | 16.1 |
| Phrase | 8.7 | 29.1 | 11.8 |
| Boolean | 6.5 | 24.8 | 12.2 |
| Broad wildcard | 8.3 | 22.7 | 12.1 |
| Search with filters | 6.9 | 26.6 | 13.2 |
| No query, filters only | 7.2 | 99.8 | 13.2 |
| Sorted results | 8.0 | 477.2 | 12.4 |
| Snippets | 14.1 | 50.4 | 21.1 |
| Aggregations (15 kinds) | 28.9 | 237.8 | 22.1 |

Examples: sort of `regering` by author 7.1 / 1,354.7 / 14.5 ms; count per day 128.3 / 592.7 / 56.6 ms; count per
`section` 158.1 / HTTP 500 / 49.5 ms.

| Load, closed loop, mixed searches: requests/s (p95) | Elasticsearch | Original branch | This branch |
|---|---:|---:|---:|
| 1 client | 158 (9 ms) | 28 (64 ms) | 89 (20 ms) |
| 4 clients | 544 (10 ms) | 92 (150 ms) | 276 (24 ms) |
| 16 clients | 832 (31 ms) | 130 (373 ms) | 365 (70 ms) |
| 32 clients | 912 (61 ms) | 133 (767 ms) | 370 (147 ms) |

| Download of 20,000 documents, documents/s | Elasticsearch | Original branch | This branch |
|---|---:|---:|---:|
| 200 per page | 7,645 | 1,064 | 1,195 |
| 1,000 per page | not tested | 4,779 | 6,125 |
| 1,000 per page, `"sort": "_seq"` | | | 13,217 |

| Writes and correctness | Elasticsearch | Original branch | This branch |
|---|---|---|---|
| Bulk upload 20,000 documents in batches of 1,000 | 9,741 documents/s | 1,342 documents/s | 8,605 documents/s |
| Same counts as the original branch, 56 non-wildcard searches | | | 56 of 56 |
| Same cells as the original branch, 15 aggregations both can answer | | | 15 of 15 |
| Wildcard counts equal to Elasticsearch, 7 prefixes | | 1 of 7 (`pol*`: 11,560 against 592,319) | 7 of 7 |
| Four clients upload the same 300 documents at once | all accepted | 3 of 4 get HTTP 500 (data correct) | all accepted |
| A reader sees a half-finished upload | yes | no | no |
| Upload interrupted by stopping the API | part is kept | all or nothing | all or nothing |

The upload difference between the two PostgreSQL versions most likely comes from the `ANALYZE` of the staging table
(4.22; inferred, as the three-way test of 21 Nov found the same jump for the first fast-path version, which has that
fix).

**Limits of this test.**

- One run per version, one laptop, no authentication, one collection. Differences of a few milliseconds mean little.
- The original branch is pinned to pg_search 0.26.0 and ran here on 0.27.0. Its 0.26.0 numbers of 21 Nov were similar
  or slower.
- The original branch had no sort slot for `publisher` or `author`; adding slots is part of what this branch does.
- Elasticsearch caches repeated aggregation requests, so its aggregation times are partly cache hits.
- A first run of this branch was invalid, because it ran while the sort slot fill job was still rewriting all
  2.68 M rows. Filling the two slots took more than 10 minutes, followed by an index rebuild. The valid run started
  after all jobs had finished. This led to the one-pass fill (4.12).
- Elasticsearch and the PostgreSQL versions differ slightly on some plain words (`klimaat` 46,649 against 45,840),
  probably because the two engines analyse text differently (inferred, not checked).
- Not tested: larger collections, cold starts, fixed-rate load, queries over many projects.

### 6.2 Earlier measurements, in brief

- **Night test, 15 to 16 Nov 2026** (Elasticsearch against the original branch on pg_search 0.26.0; section 2.3):
  normal searches 2 to 5 times slower, very common words and no query 450 to 490 ms, whole-collection aggregations
  1.4 to 2.9 s, load 100 against 400 requests/s; the original branch passed 40 of 40 transaction checks against 20
  of 40 for Elasticsearch, and loaded 10.7 M documents as fast as Elasticsearch (about 10,500 documents/s).
- **First fast-path version, 20 Nov 2026** (synthetic 1 M documents, in process): very common word 92.2 to 35.4 ms,
  no query 46.4 to 17.8 ms, sort by publisher 2,758.5 to 687.1 ms, aggregations 13 to 47 times faster.
- **Three-way test, 20 to 21 Nov 2026** (Elasticsearch, the original branch and the first fast-path version, the
  2.68 M documents in a partition that also held 10.7 M other documents; night test, median of 3 passes x 100 runs):
  the first fast-path version was 2 to 4 times faster than the original on searches and 30 to 100 times on
  aggregations (whole-collection aggregations 26 to 58 ms against 1.5 to 3.1 s), but still about three times slower
  than Elasticsearch on searches (a floor of about 20 ms). The speed research of 22 Nov traced that floor to parallel
  workers, planning of extra statements and the scored project clause, which led to stage 2 of this branch.

---

## 7. Known limits and open questions

### 7.1 Known limits

These are design limits of this version, each a choice with a cost (from `POSTGRES_FAST_PATHS.md`, "Known limits"):

- **One job at a time.** The job worker of a server process runs one job after the other. A long job (a move, a slot
  fill on a large project) delays the jobs behind it.
- **Two extra statements per write.** Every write takes the move guard: a shared advisory lock and a read of
  `projects.moving_to`. Small statements; the cost is not measured.
- **No `Retry-After` on a statement timeout.** Retrying the same query would time out again.
- **The size sample is not random.** It averages the first 20,000 rows the index returns.
- **BM25 statistics per partition.** How rare a word is, is computed per partition, so other projects in a shared
  partition (and, during a move, the two partitions) affect the relevance order, not which documents match. This was
  already true on the original branch.
- **The grace period of a move is not a guarantee** for requests that run statements more than 60 s after reading
  their partitions.
- **Cursors are not signed.** A hand-made offset cursor pages deeper than `AMCAT4_MAX_OFFSET`; the limit is a
  performance guard.
- **The parallel count can be wasted** on a first page that is not full.
- **Project creation waits for migrations**, because of the shared lock key.
- **A prefix at the end of a phrase** can still miss documents if its last word has more than 1,000 completions in a
  segment.
- **Filling a sort slot on a large project is a maintenance operation**, not something to do during heavy use.

### 7.2 Open questions for the original author

These are questions, not proposals. The branch has a default for each, behind a switch, so every answer can be
followed.

1. Is the unscored `const_score` form of the project clause acceptable, knowing that the relevance order changes
   once?
2. Is a new migration `0002` (and `0003`) the right form, and is `0001` frozen from now on?
3. On read queries, may the partition table be named directly, in place of a condition on `partition_id`? (Writes
   keep the original rule.)
4. Which behaviours are contracts for clients: the paging cursor, the default order, exact totals, the meaning of
   `_id`? Is `_seq` a good name for the download order?
5. Is pg_search 0.27.0 the target version, and when is an index rebuild acceptable on a running server?
6. Sort slots: five, as three keyword, one number and one date? Generic slots, or some as named standard columns with
   a fixed meaning (which would also serve discovery across projects)?
7. Moving a large project to its own partition: wanted now, later, or in another form (for example `partition_by`)?
8. Are the default limits right: a 30 s statement timeout, a 5 s pool wait, an offset limit of 10,000, a vacuum after
   10,000 changed documents, a fill job above 10,000 documents?
9. Which fields do users really sort on, and is there a sample of real user queries to test against?
10. Who adapts the client libraries to `_seq` and to the 503 answers?

### 7.3 Not addressed by this branch

- The cause of the remaining gap on phrases and broad wildcards was not analysed.
- Queries over many projects (several partitions) use the original SQL and were not measured.
- Tags and annotations still live in the document row, so a tag change rewrites the row and indexes its text again.
- A cache for counts and aggregations was not built.
- pg_search's `partition_by` option was not tested.

---

## 8. How to verify

### 8.1 Tests

The test suite went from 125 tests on the original branch to 257 (plus 1 skipped). It passes on pg_search 0.26.0 and
0.27.0 (commit `335d5ed9`: 14 full runs on 0.26.0 and 3 on 0.27.0, all 257 passed), and in reverse file order (commit
`97aef59f`). New or changed test files:

| File | Covers |
|---|---|
| `tests/test_fast_path.py` | Partition table use, isolation in a shared partition, parity of aggregations and searches with the original SQL, wildcards, empty strings, scroll counts once |
| `tests/test_search_v2.py` | Settings, worker setting per pool, the `const_score` form, isolation with the fast path on and off, equal counts and hit sets old against new for 15 query shapes, writes, `_seq`, offset limit, counts |
| `tests/test_phase_c.py` | Listing parity and the id index in the plan, sort slots (Top-K checked with EXPLAIN), fill jobs, quarter key and backfill, vacuum, prewarm, moves |
| `tests/test_migrations.py` | Upgrade of a `0001` schema with data to `0002` and `0003`, down and up again, lock timeout, index attachment |
| `tests/test_maintenance.py` | Segment and deleted-document triggers on both pg_search versions (background merging switched off for the test partition), merging as activity |
| `tests/test_review_fixes.py`, `tests/test_review_fixes2.py` | The fixes of stage 4, each failing on the code before its fix |
| `tests/conftest.py`, `tests/test_querystring.py`, `tests/test_storage.py`, `tests/test_middlecat.py` | Fixtures, wildcard expansion, storage details, a test that now restores the setting it changes |

Run the suite against a PostgreSQL with pg_search (see `backend/README.md`). With `AMCAT4_TEST_INDEX_PATHS=1` the
small-result thresholds are 0, so every count and aggregation goes through the in-index paths. Each test's docstring
says which behaviour or bug it protects.

### 8.2 Switches

Every behaviour change can be turned off to compare with the original:

| Switch | Default | Original behaviour | Section |
|---|---|---|---|
| `AMCAT4_FAST_PATH` | `true` | `false` | 4.1, 4.8, 4.10, 4.21 |
| `AMCAT4_FILTER_CLAUSES` | `true` | `false` | 4.2 |
| `AMCAT4_API_PARALLEL_WORKERS` | `0` | empty or `default` | 4.3 |
| `AMCAT4_CUSTOM_PLANS` | `true` | `false` | 4.4 |
| `AMCAT4_CAPPED_COUNT` | `false` | `true` (the two-step count of the first fast-path version; the original had one count) | 4.5 |
| `AMCAT4_PARALLEL_COUNT` | `true` | `false` | 4.5 |
| `AMCAT4_MAX_OFFSET` | `10000` | `0` | 4.7 |
| `AMCAT4_QUARTER_KEY` | `true` | `false` | 4.9 |
| `AMCAT4_SORT_SLOT_JOB_ROWS` | `10000` | `-1` | 4.12 |
| `AMCAT4_BYTE_ORDER_SORT` | `true` | `false` | 4.13 |
| `AMCAT4_VACUUM_AFTER_ROWS` | `10000` | `0` | 4.14 |
| `AMCAT4_BTREE_LISTING` | `true` | `false` | 4.17 |
| `AMCAT4_PREWARM` | `false` | `false` | 4.19 |
| `AMCAT4_STATEMENT_TIMEOUT_MS` | `30000` | `0` | 4.20 |
| `AMCAT4_POOL_TIMEOUT_S` | `5` | `30` | 4.20 |

All are listed with a comment in `deploy/.env.example`.

### 8.3 Reproducing the measurements

The measurement scripts are not in the repository. To repeat a comparison:

1. Load the same collection into two servers that share one PostgreSQL database, one running the original branch and
   one running this branch (the migration of this branch cannot be undone, so run the original first, or use two
   copies of the database).
2. For each request class, send the same request many times through the API (the tests above used 15 to 100 runs)
   and take the median. Alternate the order of the servers between passes.
3. Compare counts, hit sets and aggregation cells, not only times. Expect identical counts between the two
   PostgreSQL versions and a changed relevance order.
4. For a single change, run the same comparison on this branch with only its switch turned off.
5. For SQL timings, run the statement in `psql` several times (the research used the median of 9), with
   `SET max_parallel_workers_per_gather = 0` to match the API.
6. Run `VACUUM (ANALYZE)` and wait for background merges and jobs to finish before measuring. A measurement during a
   sort slot fill is not valid.

---

## 9. Glossary

| Term | Meaning |
|---|---|
| **Advisory lock** | A lock in PostgreSQL on a number chosen by the application, not on a table. Used here to keep a move, a slot fill or a vacuum from overlapping with other work |
| **Aggregate Scan** | pg_search's way of counting or aggregating inside its index, without fetching the rows. It works only on a table that has the BM25 index, not through the partitioned parent |
| **Autovacuum** | PostgreSQL's background process that vacuums tables when enough rows have changed (by default 20 %) |
| **BM25** | The usual formula to rank text search results by relevance. It uses how often a word occurs in a document and how rare it is in the whole index |
| **B-tree index** | PostgreSQL's ordinary index, sorted by its columns. Good for "the next N rows of this project in id order" |
| **const_score** | A pg_search query clause that selects documents and gives them a fixed score (here 0), so the clause does not affect ranking and is cheap |
| **Cursor (keyset)** | A paging cursor that says "after this id". The next page starts there, so every page costs the same |
| **Cursor (offset)** | A paging cursor that says "skip the first N". Page N costs more than page 1 |
| **Derived key** | An extra value stored with a date field, such as its year or quarter, so aggregations need no date computation |
| **Fast field** | A column-like copy of exact values inside the pg_search index, used for filters, sorting and aggregation |
| **Generic plan** | A query plan that PostgreSQL makes once for a prepared statement and reuses for any values. It cannot use the actual project or query, and for pg_search it is much slower |
| **jsonb** | PostgreSQL's binary JSON type. AmCAT stores document values in jsonb columns under field keys |
| **Keyset paging** | Paging by "after this key" (see cursor) |
| **lz4** | A compression method that PostgreSQL can use for large values. Faster to decompress than the default (pglz) |
| **Partition** | One table of a partitioned table. Here, a group of projects (or one big project) with its own BM25 index |
| **pdb.agg** | pg_search's function for Elasticsearch-style aggregations computed inside the index |
| **Planning** | The step in which PostgreSQL decides how to run a statement. About 2.4 ms per statement in the speed research |
| **Prepared statement** | A statement that is parsed once and run many times. psycopg prepares a statement after five executions on a connection |
| **Prewarm** | Loading an index into memory before it is needed, with the `pg_prewarm` extension |
| **Segment** | A piece of a pg_search (Tantivy) index. Each upload adds segments; merging joins them; a search visits every segment |
| **Sort slot** | A real column of the documents table, in the BM25 index, that a project maps one of its fields to, so sorting on that field is a Top-K scan |
| **Statement timeout** | A limit on how long one SQL statement may run before PostgreSQL stops it |
| **Tantivy** | The search library (in Rust) that pg_search is built on, similar to Lucene |
| **Top-K** | Finding only the best K results in sorted order, without sorting all hits. pg_search can do this inside its index for real indexed columns, not for JSON keys |
| **VACUUM** | PostgreSQL's cleanup of dead rows. It also updates the visibility map |
| **Visibility map** | PostgreSQL's record of which table pages hold only rows that every transaction can see. pg_search's fast counts can skip the visibility check for those pages |
| **Wildcard expansion** | Turning a prefix such as `pol*` into the list of indexed words that start with it. pg_search limits this to 50 words per segment unless told otherwise |
