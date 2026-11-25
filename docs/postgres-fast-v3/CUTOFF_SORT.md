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
- **Several sort keys.** The cutoff is on the first key. The other keys order the fetched rows. Inside the two
  groups above (missing values, many ties) the next key is the first key, so it can get a cutoff of its own.
- **Paging conditions are in the JSON query**, never SQL comparisons on JSON keys (pg_search turns
  `(json->>'k')::numeric > c` into a text range, which is wrong and slow). Number bounds are always sent as floats.
- **One snapshot.** With the switch on, all statements of the page (aggregation, ids, rows) run in one
  `REPEATABLE READ` transaction, also when the late fetch is used. A document that is deleted or changed between
  the statements is still on the page as it was. The count on the second connection is not part of it (as before).

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

There is no lower limit on the number of hits. For a search with few hits the cutoff sort costs one or two
statements more than the late fetch and wins nothing.

## What was tested

- `tests/test_cutoff_sort.py`: every type in both directions, missing values, ties larger than a page, deep
  pages, several keys, filters and query strings, few and no hits, the tag order, every fallback above that can be
  reached in a test, the switch (statements of version 2), and the snapshot (documents deleted and uploaded between
  the statements).
- `tests/test_parity.py`: random filters, sorts (one to three keys, with tags and a date field without slot) and
  pages against a reference computed in Python. Each case runs with the switch on, on with small limits (so the
  small test set reaches the branches for large results) and off. `AMCAT4_PARITY_CASES=2000`: 0 differences.
- One run by hand on 150,000 synthetic documents (with 1,000 updated and 200 deleted documents): 126 cases
  (14 sorts, pages 1, 51 and 500, no query and two query strings), cutoff sort against late fetch: same ids in all.

## What was measured, and what was not

- Research prototype (`json-speed-research/round2/R2_cutoff.md`, 1 M documents, 25 Nov 2026): page 1 in 7 to 43 ms
  against 110 to 1,100 ms for sorting all hits. Not re-measured on this code.
- This code, the same 150,000 documents, whole request through `query_documents` (with the count), a shared machine,
  median of 3, one run: page 1 without query 17 to 44 ms against 62 to 240 ms; page 500 on a unique field (url) was
  slower (174 against 137 ms), as in the research. A query with few hits: about the same or a few ms slower.
  These numbers are an indication only.
- Not measured: 1 M documents and more with this code, concurrent load, the cost of the transaction (two round
  trips), parallel workers, several projects in one partition at scale.

## Known limits

- Deep pages on a field with (nearly) unique values need a large aggregation and can be slower than the late fetch.
- Tag sorts stay the slowest (smallest/largest tag is computed per row).
- Number fields with one value for most documents: all of them are sorted (the tie group is not built for numbers).
- Paging is by offset, as before. The value cursor of the research prototype and NULLS FIRST were not ported.
- The cutoff depends on pg_search behaviour found by testing 0.27.0 (terms order by key, exact counts, `exists`
  false for empty lists, date keys in microseconds since 2000). Run the parity test after every pg_search upgrade.
