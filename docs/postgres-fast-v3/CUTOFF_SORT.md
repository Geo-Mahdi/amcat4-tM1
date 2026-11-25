# The cutoff sort (version 3)

Branch `postgres-fast-v3-cutoff`, 25 Nov 2026. PostgreSQL 18, pg_search 0.27.0.
Code: `backend/amcat4/postgres/cutoff_sort.py`, called from `page_rows()` in `backend/amcat4/postgres/search.py`.
Tests: `backend/tests/test_cutoff_sort.py`, `backend/tests/test_parity.py`.

## The problem

A sort on a field that is stored in JSON (`exact_fields`, key `f<pk>`) and has no sort slot cannot be a Top-K scan
in pg_search. Version 2 therefore reads and sorts every hit to find the ids of one page (the first statement of the
"late fetch", see `docs/postgres-fast-v2/RATIONALE.md`, section 4.10). That takes 0.1 to 10 s on large results.

## What the cutoff sort does

It replaces only that first statement. The second statement (the full rows of the ids of the page, same
`ORDER BY`) is unchanged.

0. **Small results are sorted as a whole.** The first statement reads at most 5,001 hits (`SMALL_HITS` + 1) with
   their sort values, in no order, and sorts them. If the search has 5,000 hits or fewer, these are the ids of the
   page and no aggregation runs: the same work as the late fetch, so a small result costs what it cost in version 2.
   If there are more hits, the statement has stopped after 5,001 rows (about 10 ms) and the steps below follow.

1. **Find the cutoff.** A `pdb.agg` aggregation over the same search gives the values of the field in sort order,
   with the number of documents per value (`terms`, ordered by `_key`). Walking them until `offset + per_page`
   documents are passed gives the value of the last document of the page: the cutoff.
   Number fields use `stats` (smallest and largest value) and a `histogram` instead, because `terms` on a field with
   whole and decimal numbers is not ordered in pg_search 0.27.0. If the histogram bucket of the cutoff is large,
   a second histogram inside that bucket follows.
2. **Read only the rows up to the cutoff.** The normal statement runs with one more clause inside the JSON query:
   value <= cutoff (ascending) or value >= cutoff (descending). Only these rows are read and sorted.

The order is the order of version 2: key 1, key 2, ..., then the internal id; missing values last, for ascending
and descending.

- **Missing values.** Documents without the field are a group of their own, after the documents with a value.
  If the page reaches into that group, its documents are ordered by the remaining sort keys (found the same way),
  and with no keys left by id (a Top-K scan).
- **Ties at the cutoff.** The clause includes the cutoff value, so all documents with that value are candidates
  and are ordered by the remaining keys and the id. If more than 1,000 documents have exactly the cutoff value, they
  are not sorted on the first key at all: the documents before the cutoff value are sorted as usual, and the
  documents with the cutoff value are a group ordered by the remaining keys.
- **Tags with many documents per tag.** If more than 1,000 documents have the cutoff tag, the documents with a value
  are three groups: (1) a tag before the cutoff tag, sorted as usual; (2) the cutoff tag and no tag before it,
  ordered by the remaining keys (by id if there are none, a Top-K scan); (3) only tags after the cutoff tag, where
  the same is done again for the rest of the page. The aggregation counts a document once per tag, so the sizes of
  the groups are counted with a count statement or follow from the rows that come back.
- **Terms instead of a text range.** For keyword, url and tag fields "up to the cutoff" is sent as the set of the
  values that the aggregation listed (up to 1,000 values, then a range), and "equal to" as a term. pg_search looks
  terms up in its index; a text range reads the values of every document of the partition (tag field, 1 M
  documents: 3 ms against 80 ms).
- **No `exists` clause** next to a clause on a value (it cost 10 to 15 ms per statement on 1 M documents).
- **Several sort keys.** The cutoff is on the first key. The other keys order the fetched rows. Inside the two
  groups above (missing values, many ties) the next key is the first key, so it can get a cutoff of its own.
- **Paging conditions are in the JSON query**, never SQL comparisons on JSON keys (pg_search turns
  `(json->>'k')::numeric > c` into a text range, which is wrong and slow). Number bounds are always sent as floats.
- **One snapshot.** With the switch on, all statements of a page with a late fetch (ids, aggregation, rows) run in
  one `REPEATABLE READ` transaction, also for small results and when the late fetch itself is used. A document that
  is deleted or changed between the statements is still on the page as it was. The count on the second connection
  is not part of it (as before). Searches without a late fetch (sort slot columns, relevance, id order) are one
  statement and get no transaction and no extra statement (tested).

## The switch

`AMCAT4_CUTOFF_SORT` (settings `cutoff_sort`), default `true`. `false` gives the statements and the order of
`postgres-fast-v2`: the late fetch, no transaction, the old tag order.

## Tag sorts change (decision of the project owner)

With the switch on, a tag field sorts like Elasticsearch: ascending on the **smallest** tag of the document,
descending on the **largest** tag, in byte order. Documents without tags (no value, or an empty list) come last.
Version 2 sorted on the JSON text of the whole tag list (so on the first tag as stored, and an empty list after
all others).

The new order is used in every statement while the switch is on (cutoff sort, late fetch, the single statement of
`AMCAT4_FAST_PATH=false`, several projects), so the order of a tag sort does not depend on the path. With
`AMCAT4_BYTE_ORDER_SORT=false` the smallest/largest tag is taken in the database's collation, and the cutoff sort
is not used for text fields.

## When it is used

All of these must hold, otherwise the late fetch of version 2 runs (same result):

- the first sort key is a keyword, url, tag, integer, number or date field without a (filled) sort slot;
- the field has one JSON key in the query (one project, or several projects of which only one has the field);
- the search runs on one partition table (the fast path) and all its conditions are in the JSON query.

So the late fetch is used for:

| Case | Why |
|---|---|
| first key `_score`, `_id`, `_seq`, random (`?`), a sort slot field, a text field | not a JSON key in the index, or already a Top-K scan (these never were a cutoff case) |
| boolean, geo_point and other types | not built |
| the same field in several projects of one query | another JSON key per project |
| similarity search, keyset paging (`_seq`, scrolling) | no late fetch there |
| a filter on `_id`, an `exists` filter on a text field, a second key `_score` (keeps a SQL condition), `AMCAT4_FAST_PATH=false`, a project that is being moved | SQL conditions next to the JSON query, or not one partition table |
| `AMCAT4_FILTER_CLAUSES=false` | the extra clauses would change the scores |
| `AMCAT4_BYTE_ORDER_SORT=false`, for keyword, url and tag | the index only knows byte order |
| `offset + per_page + 2` > 65,000 (terms aggregation) | the most buckets pg_search returns |
| keyword, url or tag values among the hits that look like a timestamp (`2020-01-01T00:00:00Z`) | pg_search indexes such text as a date; the aggregation lists it in another place than SQL sorts it |
| bucket keys of the wrong type or not in order, fewer rows than the aggregation counted, numbers above 2^53, a database error in the cutoff statements | not as expected: do not trust it |
| a tag cutoff that is still too early after 3 repeats | the aggregation counts a document once per tag |

Fewer hits than the page asks for is not a fallback: then no range clause is sent (pg_search gives an error for a
range without bounds) and all documents with a value are sorted.

A database error in the statements that find the ids (not a timeout or a connection problem) ends the transaction;
the page is then fetched with the late fetch in a new one.

The check for text that is indexed as a date is a `stats` aggregation in the same statement as the `terms`
aggregation. Measured on 1 M documents it adds nothing that can be seen (terms alone 13.6 ms, with stats 12.8 to
14.5 ms; stats alone 1.7 ms, most of it planning), so it stays for every text sort.

## What was tested

- `tests/test_cutoff_sort.py`: small results (one statement, no aggregation, the limit itself), tag groups, no
  transaction for searches without a late fetch, a failing statement; every type in both directions, missing values, ties larger than a page, deep
  pages, several keys, filters and query strings, few and no hits, the tag order, every fallback above that can be
  reached in a test, the switch (statements of version 2), and the snapshot (documents deleted and uploaded between
  the statements).
- `tests/test_parity.py`: random filters, sorts (one to three keys, with tags and a date field without slot) and
  pages against a reference computed in Python. Each case runs with the switch on (always the cutoff sort), on
  with small limits (so the small test set reaches the branches for large results), on with small results sorted as
  a whole (up to 100 hits) and off. `AMCAT4_PARITY_CASES=2000`: 0 differences.
- One run by hand on 150,000 synthetic documents (with 1,000 updated and 200 deleted documents): 126 cases
  (14 sorts, pages 1, 51 and 500, no query and two query strings), cutoff sort against late fetch: same ids in all.

## What was measured, and what was not

Benchmark `backend/benchmark/short_test` (1,000,000 documents in the big project, PostgreSQL 18, pg_search 0.27.0,
no parallel workers, 25 Nov 2026, a machine shared with other jobs: differences of 10 to 20 % between runs are
noise). Median, ms, whole request with the count.

| Query | version 3 without cutoff | first cutoff version | this version |
|---|---|---|---|
| author (keyword) asc, whole project | 845 | 49 | 26 |
| n_num (number) desc, whole project | 1,007 | 82 | 47 |
| n_int (integer) asc, whole project | 1,050 | 18 | 22 |
| tags asc, whole project | 1,475 | 895 | 76 |
| author asc, subset of 84,000 | 130 | 18 | 22 |
| n_num desc, subset | 167 | 29 | 36 |
| n_int asc, subset | 151 | 14 | 22 |
| tags asc, subset | 178 | 207 | 31 |
| author asc, whole project, page 500 | 970 | 96 | 75 |
| load test, 32 clients, "filtered sorted lists": requests/s | 289 | 168 to 186 | 323 to 333 (three runs) |
| the same: 95th percentile, ms | 201 | 470 to 478 | 176 to 194 |

- The 4 to 8 ms that subsets lost against the first cutoff version is the first statement (5,001 rows), which is
  wasted when the result is large. With `SMALL_HITS` 2,000 or 3,000 the subsets took 17 to 19 ms, but the load test
  gave 306 and 300 requests/s: its lists of one month (about 3,600 hits) then use the cutoff sort.
- Same-results step of the benchmark: 2,000 cases, 0 mismatches.
- Not measured: more than 1 M documents, parallel workers, several projects in one partition at scale, the cost of
  the transaction alone under load (one run with the small-result statement outside the transaction: 333 requests/s,
  inside with savepoints: 314; the savepoints were then removed and it was 333 inside).

## Known limits

- Deep pages on a field with (nearly) unique values need a large aggregation and can be slower than the late fetch.
- Tag sorts over a large result cost one aggregation over all tags of the hits (55 ms on 1 M documents with 2 M
  tags). Tag fields with few documents per tag still compute the smallest/largest tag per candidate row.
- Results between 5,000 and roughly 10,000 hits gain little; the first statement is extra work for every larger
  result.
- An aggregation over a search with a date range filter was slow together with an `exists` clause (33 ms on 3,500
  hits); without the clause it is 5 ms. Why was not looked into.
- Number fields with one value for most documents: all of them are sorted (the tie group is not built for numbers).
- Paging is by offset, as before. The value cursor of the research prototype and NULLS FIRST were not ported.
- The cutoff depends on pg_search behaviour found by testing 0.27.0 (terms order by key, exact counts, `exists`
  false for empty lists, date keys in microseconds since 2000). Run the parity test after every pg_search upgrade.
