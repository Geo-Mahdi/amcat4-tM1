# Changes on this branch since bb35e43a

This is an overview of every change made after the original code (bb35e43a), grouped by topic. For each change: what
it does, why (with the measured effect where there is one), the switch to turn it off, and the files. The details and
measurements are in the commit messages (`git log bb35e43a..HEAD`). "Not measured" means there is no measurement for
that point. Timings come from synthetic partitions (156k documents) or from a 2.7 M document collection on one
laptop, with pg_search 0.26 or 0.27 (Nov 2026); they show the direction, not a guarantee for other data.

All settings are environment variables with the prefix `AMCAT4_` (see `amcat4/config.py` and `deploy/.env.example`).
Migrations 0002 and 0003 are new files; `0001_initial.py` is not changed.

## 1. Reads: counts, filters, listing

**Fast read path** (`AMCAT4_FAST_PATH`, default true). When all projects of a query are in one partition, the
statement runs on that partition's table and not on the parent table, without the duplicate project term. Why:
aggregating on the partition table instead of the parent took 37 ms instead of 1,640 ms with the same result (the
original statement on a 2.68 M document project in a partition of 13.4 M documents, pg_search 0.26.0, 16 Nov 2026; not
re-measured on this code). Off: the original SQL. Files: `postgres/search.py`, `projects/aggregate.py`,
`projects/index.py` (estimated project size, see 7).

**Project and filters as unscored clauses** (`AMCAT4_FILTER_CLAUSES`, default true). The project condition and the
filters go in a `const_score` clause, so only the query string is scored. Why: top 10 for a very common word 5.8 ms
to 2.9 ms (synthetic, pg_search 0.27). Counts and hit sets are the same; the relevance order changes once to the plain
BM25 order of the query. Note: pg_search's boolean query has no `filter` list; an unknown `filter` key is silently
ignored (tested on 0.26.0 and 0.27.0) and would have dropped the project condition and leaked documents of other
projects in a shared partition, so `const_score` is used. Off: every clause scored, as in the original. Files:
`postgres/search.py`.

**One count, on a second connection** (`AMCAT4_CAPPED_COUNT`, default false; `AMCAT4_PARALLEL_COUNT`, default true).
The two-step count (up to 1000 hits one by one, then in the index) is replaced by one count, which is cheaper without
parallel workers (a full count takes 2-4 ms). The count runs next to the page on a second connection when the pool has
one to spare (a common word: 8.0 ms to 4.9 ms; not measured under concurrent load). No count when the first page is
not full. A scroll counts once, on the first page, and carries the total in the cursor (the count was about two
thirds of the work of a download; the exact share is from the commit message, not re-measured). Off:
`AMCAT4_CAPPED_COUNT=true` for the two steps, `AMCAT4_PARALLEL_COUNT=false` for one after the other. Files:
`postgres/search.py`, `postgres/connection.py` (`spare_connection`), `projects/query.py`.

**No parallel workers, custom plans** (`AMCAT4_API_PARALLEL_WORKERS`, default 0; `AMCAT4_CUSTOM_PLANS`, default true).
API connections run with `max_parallel_workers_per_gather = 0` (a count of a normal word: 10 ms with 4 workers, 3.7 ms
without) and with `plan_cache_mode = force_custom_plan` (after 5 uses psycopg prepares a statement and postgres may
use a generic plan that is 5-10x slower for pg_search: 84 ms for the first requests, 469 ms from the 11th on, for a very
common word, in the benchmark of the original code on 2.68 M documents, pg_search 0.26.0, 15-16 Nov 2026). Off: empty value for the first (the server's setting), `false` for the second. Files: `connections.py`,
`config.py`.

**Listing without a query over a B-tree index** (`AMCAT4_BTREE_LISTING`, default true). A search without query string
and without index filters, in id order, is plain SQL over the new index (partition_id, project_pk, id) of migration
0002 and reads only the rows of the page (first 10 documents in psql, before migration 0002, on a copy of the 2.68 M
document project alone in its partition: 13.5 ms through the search index, 0.14 ms over the primary key; pg_search
0.26.0, 22 Nov 2026). Counts stay in the search index. Off: through the search index. Files:
`postgres/search.py`, migration 0002.

**Late fetch for sorts on json fields.** When the sort is on a json value (no sort slot), pg_search cannot do a Top-K
scan, so every hit is read and sorted. Now this is done with only the internal id, and the full rows of the page are
fetched afterwards. Not measured separately. Switched off with `AMCAT4_FAST_PATH=false`. Files: `postgres/search.py`.

**Wildcards.** pg_search expands a prefix to only the first 50 terms per segment by default; `pol*` found 2% of the
documents and the result changed when segments merged. A single-word prefix is now expanded fully; a prefix at the
end of a phrase keeps a bound of 1000 expansions (10,000 took 5-20 s). A correctness fix, no switch. Files:
`postgres/querystring.py`.

## 2. Paging and downloads

**New sort key `_seq`, depth limit** (`AMCAT4_MAX_OFFSET`, default 10000, 0 = off). `"sort": "_seq"` orders by the
internal id (the order of storage) with an "after this document" cursor that stays fast at any depth (download by
offset 25,500 documents/s, by id 107,600, query with 58k hits, synthetic 156k document partition, an earlier version
of this branch). `_id` keeps its original meaning (the document id). Offset pages deeper than the limit get HTTP 400
with the advice to use `_seq`; cursors issued by the server are never limited. Cursors are not signed, so a hand-made
offset cursor is not limited either: the limit is a performance guard, not a security boundary. Every cursor value is
checked (known keys, non-negative integers), so a hand-made cursor gives HTTP 400, not a database error. Off: `AMCAT4_MAX_OFFSET=0`. Files: `postgres/search.py`, `projects/query.py`.

## 3. Aggregations

**In-index aggregations with `pdb.agg`.** Tags, quarters, metrics and nested axes are aggregated inside the index
instead of by GROUP BY, which pg_search pushes into the index only for some shapes (0.6-0.8 s per million documents
for the others, 30-50 ms with `pdb.agg`). Small results use plain SQL. A bucket without values gives null for sum, as
GROUP BY. If pg_search reports left-out buckets (more than 65,000 groups) or fails with a query error, the original
GROUP BY runs. Off: `AMCAT4_FAST_PATH=false`. Files: `postgres/index_aggregate.py`, `projects/aggregate.py`.

**No slower fallback after a timeout or connection problem.** Before, every `psycopg.Error` led to a second, slower
statement, also after a statement timeout. Now `must_not_fall_back` (cancelled statements, other OperationalError,
InterfaceError) are raised at once. No switch. Files: `postgres/connection.py`, `postgres/index_aggregate.py`,
`projects/aggregate.py`.

**Derived quarter key** (`AMCAT4_QUARTER_KEY`, default true). Date fields get a derived key `<field>_quarter`. Quarter
aggregations use it, so they stay in the index, also with metrics. Documents stored before migration 0002 have no key;
`fields.quarter_ready` is false for those fields, and the old paths are used until a backfill job (scheduled every
minute, retried after 5 minutes, at most 3 failed jobs) has filled the key. Results are the same in all states. Off:
the old paths. Files: `projects/jobs.py`, `systemdata/fields.py`, `postgres/index_aggregate.py`, migration 0002.

## 4. Sorting

**Five more sort slots (`fast_sort`).** Besides the original date and source columns: `sort_kw1..3` (keyword, url),
`sort_num1` and `sort_date2`. Sorting on a slot is a Top-K scan inside the index (checked with EXPLAIN in the tests);
sorting on a json value reads every hit. Projects with more than `AMCAT4_SORT_SLOT_JOB_ROWS` documents (default
10000; -1 = always in the request, as before) fill the slot in a background job in batches; the slot is not used
for sorting until it is filled. Slots requested together (in one request, or in several requests before the job
starts) are filled by one job per project in one pass: one UPDATE per batch sets every slot column, so N slots
rewrite each row once, not N times; rows where no slot value changes are not written. Filling a slot on a large
project is a maintenance operation: it rewrites every document of the project (and pg_search indexes each one
again), so do it when the project is not in heavy use. Measured on 2.68 M real documents (22 Nov 2026) with one job
per slot, before this change: more than 10 minutes for two slots, searches 2 to 4 times slower meanwhile, and an
index rebuild afterwards (one segment per batch; not re-measured since). Files: `projects/jobs.py`,
`systemdata/fields.py`, `postgres/documents.py` (`fill_sort_slots`), migration 0002.

**Byte order for text sorts** (`AMCAT4_BYTE_ORDER_SORT`, default true). Text values not in a slot are sorted with
`COLLATE "C"`, like the slot columns and Elasticsearch, so the order does not change when a field gets a slot. Off:
the database collation. Files: `postgres/search.py`.

**Slot sort only if every project has the field in it.** A slot column holds, per project, whichever field that project
put in it. In a query over several projects, sorting on one project's field by the slot ordered the other projects by
their own field in the same slot (possibly a hidden field). Now the slot is used only when every project has the field
in the same filled slot. A correctness and isolation fix, no switch. Files: `postgres/search.py`.

## 5. Maintenance

**pg_search 0.27.0 as target.** It merges small segments in the background (20 single-row inserts: 15 segments, 5 a
few seconds later; 0.26.0 kept 20). The automatic rebuild stays as a safety net, and a running merge counts as
activity. Deploy files, CI and README pin `paradedb/paradedb:0.27.0-pg18` (`PARADEDB_VERSION` overrides it). Files:
`projects/maintenance.py`, `postgres/layout.py`, `deploy/`, `.github/workflows/unittests.yml`.

**VACUUM (ANALYZE) after large changes** (`AMCAT4_VACUUM_AFTER_ROWS`, default 10000, 0 = off). In-index counts and
sorts must check every rewritten row until the visibility map is updated; autovacuum starts late on big tables. Measured
after a slot fill of 270k documents on a 300k-document database: a sort on the slot 187 ms before VACUUM, 3.4 ms
after. Runs as a job, at most one pending vacuum job per partition (checked under an advisory lock). Also run after the
slot fill and quarter backfill jobs. Files: `projects/maintenance.py`.

**Optional pg_prewarm** (`AMCAT4_PREWARM`, default false). Loads the search indexes into the cache at startup. Not
measured. Files: `projects/maintenance.py`, `config.py`.

**Migration 0002 and 0003 lock handling.** `ALTER TABLE documents` runs with `lock_timeout` 5 s, up to 6 attempts, and
then an error that says nothing was changed, so a waiting lock does not block every query. The partition index build
checks which parent an existing index belongs to. Files: `migrations/locking.py`, migrations 0002, 0003.

## 6. Projects in their own partition

**Move a project to a partition of its own** (migration 0003, `amcat4 move-project`). Done in batches of 10000
documents, resumable; reads are never blocked (every read path looks in both partitions during the move); writes are
refused with a conflict during the move. Setting `projects.partition_id` directly still moves everything in one
statement, as in the original (the original test passes unchanged). Details of the fixes after the first version:
the partition and the move target are read in one statement (before, a finishing move could give 0 documents); step 1
is idempotent when a job stops halfway; fill, backfill and copy jobs wait for a move (`JobPostponed`, 30 s) instead
of failing (a copy waits for a move of its source and of its destination). The number of moved documents in the
job's progress and result is counted in the target partition when a job (re)starts, so it is right after an
interruption. Between setting `moving_to` and the first batch the job waits `MOVE_GRACE_SECONDS` (60 s, a constant in
`projects/partitions.py`): a request reads its projects' partitions once and then runs its statements, so a request
that read them just before the move started would otherwise miss documents moved meanwhile. This covers requests
whose statements run within 60 s of reading the partitions (with the default pool and statement timeouts, a pool
wait plus two statements); it is not a guarantee for longer requests (see Known limits). Files: `projects/partitions.py`, `postgres/layout.py`, `projects/jobs.py`, `__main__.py`, migration 0003.

## 7. Robustness under load, and other fixes

**Separate pools and timeouts** (`AMCAT4_STATEMENT_TIMEOUT_MS`, default 30000, 0 = off; `AMCAT4_POOL_TIMEOUT_S`,
default 5). The API pool has the tuned options and a statement timeout (HTTP 503 with advice to be more specific; a statement
cancelled for another reason, e.g. `pg_cancel_backend`, gets a 503 that does not mention a timeout); a
second pool for background jobs, maintenance, CLI commands and long writes keeps the server's settings and has no
timeout (`server_defaults`, `with_server_defaults`). An API request waits at most the pool timeout for a connection,
then gets HTTP 503 with `Retry-After: 1` (the original: 30 s). `create_fields` was added to the long writes. Off:
set the timeout to 0, the pool timeout to 30. Files: `connections.py`, `api/__init__.py`.

**Estimated project size** for projects over 20,000 documents: the average of a 20,000 document sample times the
number of documents. Summing every document read the whole project (12 s for 10.7 M documents) on every
`GET /index/{ix}`. Files: `projects/index.py`. Off: `AMCAT4_FAST_PATH=false` (the original sums every document).

**Upload fixes.** ANALYZE of the staging table (the unstable upload of the night benchmark spent 626 of 632 s in one
statement); a retry once after a concurrent duplicate upload (`UniqueViolation`), then a clear error instead of HTTP
500. Aggregation sort with empty strings no longer crashes (e.g. on `section`). Files: `postgres/documents.py`,
`projects/documents.py`, `postgres/aggregate.py`.

## 8. Tests

New or changed: `tests/test_search_v2.py`, `test_phase_c.py`, `test_review_fixes.py`, `test_fast_path.py`,
`test_migrations.py`, `test_maintenance.py`, `conftest.py`, `test_querystring.py`, `test_storage.py`,
`test_review_fixes2.py`. They cover isolation in a shared partition, parity with the original SQL for 15 query shapes, the switches, migrations from a
0001 schema, moves, jobs and maintenance. Each test says in its docstring which behaviour or bug it protects.

## Known limits

Design limits of this version, not bugs; each is a choice, with its cost:

- **One job at a time.** The job worker of a server process runs one job after the other (several server processes
  run several). A long job (a move, a slot fill on a large project) delays the jobs behind it.
- **Two extra statements per write.** Every write takes the move guard (`writing()`: a shared advisory lock and a
  read of `projects.moving_to`). Small statements, but the cost is not measured.
- **No `Retry-After` on a statement timeout.** The 503 after a statement timeout has none (retrying the same query
  would time out again); the 503 for a busy pool has `Retry-After: 1`.
- **The size sample is not random.** The estimated project size (projects over 20,000 documents) averages the first
  20,000 rows the search index returns, not a random sample.
- **BM25 statistics per partition.** How rare a word is, is computed per partition, so other projects in a shared
  partition (and, during a move, the two partitions) affect the relevance order, not which documents match.
- **The grace period of a move is not a guarantee.** A request that read its partitions just before a move started,
  and runs statements more than `MOVE_GRACE_SECONDS` (60 s) later, can miss documents moved meanwhile (see 6).
- **Cursors are not signed.** A hand-made offset cursor pages deeper than `AMCAT4_MAX_OFFSET`; the limit is a
  performance guard, not a security boundary (cursor values are validated, see 2).
- **The parallel count can be wasted.** On a first page that is not full, the count that ran next to the page is not
  used (the page itself gives the total). Avoiding it would mean waiting for the page first, which is what the
  parallel count saves; there is no cheap estimate to decide beforehand.
- **Project creation waits for migrations.** Assigning a partition uses the same advisory lock key as the migrations
  (as in the original), so creating a project waits while migrations run; kept on purpose (see `postgres/layout.py`).
