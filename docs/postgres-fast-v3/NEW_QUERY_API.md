# Version 3: the new query API of ParadeDB

25 Nov 2026. PostgreSQL 18, pg_search 0.27.0. No migration, no change to the documents table or the search index.

ParadeDB deprecated its legacy json query objects (`documents.id @@@ '{"boolean": {...}}'::jsonb`, `paradedb.score`,
`paradedb.snippet_positions`) and will remove them (no date yet). AmCAT can now send its searches in the new SQL
query API instead (the `pdb.*` functions and the operators `|||`, `===`, `###`, `@@@` on columns and json keys).

## What changed

- AmCAT keeps its own query parser (`amcat4/postgres/querystring.py`), its filters, the cutoff sort and the
  aggregations. They still build the search condition as a json query tree. Only the statement that is sent changes.
- New module `amcat4/postgres/pdb_sql.py`. `render(json_query)` turns the tree into a SQL condition with
  parameters. `search_condition(json_query)` returns that condition, or the legacy one, depending on the switch.
- Every statement that sends a search condition gets it from `search_condition`: search, count, late fetch, the
  statements of the cutoff sort, aggregations (`pdb.agg` and GROUP BY), field values and field statistics, the size
  of a project, copy jobs, update and delete by query.
- With the switch on, scores come from `pdb.score` and match positions from `pdb.snippet_positions`.
- User text never goes to `pdb.parse`, and is never cast to `pdb.query` (that type reads a text as a query).
- The json of aggregations (`pdb.agg('{"terms": ...}')`) is not a legacy query object and did not change.

## The switch

`AMCAT4_NEW_QUERY_API` (setting `new_query_api`), default `false`.

- `false`: the legacy json objects, as before. Nothing changes.
- `true`: the new API in all statements listed above.

The switch can be changed at any time; no data depends on it.

## From the json tree to SQL

`K` is the column or json key of the field: `documents.project_pk`, `documents.text_fields->>'f12'`,
`documents.exact_fields->'f5'->>'lat'`. Values are bind parameters with an explicit type.

| Legacy json | New SQL | Notes |
|---|---|---|
| `{"boolean": {"must": [a, b]}}` | `(a AND b)` | |
| `{"boolean": {"should": [a, b]}}` | `(a OR b)` | |
| `{"boolean": {"must_not": [a]}}` | `NOT (a)` | `a` gets constant score 0, see "What postgres does" |
| `must` and `should` together | `(must AND (should OR documents.id @@@ pdb.all()::pdb.const(0)))` | should only adds to the score |
| `{"all": null}` | `documents.id @@@ pdb.all()::pdb.const(0)` | `pdb.all()` alone scores 1, the legacy `all` scores 0 |
| `{"const_score": {"score": 0, "query": q}}` | `::pdb.const(0)` on every predicate of `q` | the project and the filters |
| `{"boost": {"factor": 2, "query": q}}` | `::pdb.boost(2)` on the predicate | |
| `{"match": {field, value}}` | `K \|\|\| value` | any word of the value; `&&&` with `conjunction_mode` |
| `{"term": {field, value}}`, text | `K === value` | |
| `{"term": {field, value}}`, number or boolean | `K @@@ pdb.term(value::bigint)` (or `float8`, `boolean`) | whole numbers as bigint, see `term_value` in fields.py |
| `{"term_set": {field, terms}}`, or `{"term_set": {terms: [{field, value}, ...]}}` with one field | `K @@@ pdb.term_set(values)` | the cutoff sort ("one of these values", text fields) |
| `{"fuzzy_term": {field, value, distance}}` | `K === value::pdb.fuzzy(distance, f, t)` | `t`: see below |
| `{"phrase": {field, phrases, slop}}` | `K ### words::text[]::pdb.slop(slop)` | |
| `{"phrase_prefix": {field, phrases, max_expansions}}` | `K @@@ pdb.phrase_prefix(words, max_expansions)` | prefix (`econ*`) and phrase prefix |
| `{"exists": {field}}` | `K @@@ pdb.exists()` | |
| `{"range": ...}`, dates (`is_datetime`) | `K @@@ pdb.range(tstzrange(lower, upper, '[)'))` | open bound: NULL |
| `{"range": ...}`, whole numbers | `K @@@ pdb.range(int8range(lower, upper, '[]'))` | |
| `{"range": ...}`, decimal numbers or text | `K @@@ '{"range": {...}}'::pdb.query` | see below |
| lower bound above upper bound | `K @@@ pdb.empty()` | postgres refuses such a range; the legacy form finds nothing |

Things that are not the same as a plain reading of the ParadeDB documentation (all tested on 0.27.0):

- **Fuzzy.** The legacy `fuzzy_term` counts a swap of two letters as one edit (`ecnoomy~1` finds `economy`).
  `::pdb.fuzzy(1)` counts it as two and finds nothing. The third argument `t` gives the legacy behaviour.
- **Ranges with decimal bounds** (number fields; the cutoff sort always uses them for number fields).
  `pdb.range(numrange(0.5, 17.5))` sends its bounds as text and finds nothing on a json number.
  `pdb.range(int8range(1, 17, '[]'))` becomes `[1, 18)` and also finds 17.5. A SQL comparison on the json key is not
  an option either (`::numeric` gives a text range; `::float8` gave the right documents in a test but is always
  scored, and falls back to reading rows if pg_search does not take it).
- **Ranges of text** (keyword, url and tag fields, `kw:al*`, the cutoff sort). The new API has no function for them.
- For both, the range is written as a constant of the type `pdb.query`: `K @@@ '{"range": {"lower_bound":
  {"included": 0.5}, "upper_bound": null}}'::pdb.query`. This is the json that `pdb.range` itself produces, with real
  numbers. pg_search runs the same range as in the legacy format, and `::pdb.const(0)` works on it. It is new-API
  syntax (a predicate on the json key, no `documents.id @@@ jsonb`), but the text form of `pdb.query` is not in the
  ParadeDB documentation. The json is made by AmCAT from the bounds; user text is only ever a json string inside it.
  The test `test_pg_search_numrange_on_json_numbers` fails when a new pg_search version makes `pdb.range(numrange)`
  work: then `leaf_range` in pdb_sql.py can use it for numbers.

## What stays on the legacy format, and why

- **Nothing that the existing test suite sends.** In the whole existing suite with the switch on, no search
  condition fell back (checked once by logging every condition: 5,966 conditions, 0 fallbacks).
- A json tree that the renderer has no tested form for is sent in the legacy format, with a warning in the log
  ("Search condition has no form in the new query API"). This happens for:
  - whole numbers outside the range of a bigint in a values filter (the legacy format finds nothing for them);
  - `nan` and `inf` as a number (not valid json: the legacy format gives a database error, and so does the fallback);
  - a range without any bound (`num:[* TO *]`: an error in the legacy format too);
  - node types that AmCAT does not produce (regex, parse, match with a distance, ...).
- `paradedb.is_merging` (maintenance) and `paradedb.index_info` (segment statistics) are unchanged. The `pdb`
  schema of 0.27.0 has no function like `is_merging`. `pdb.index_segments(index)` returns the same numbers as
  `paradedb.index_info` (checked once by hand); it was not asked for and is not used yet.
- The setting `paradedb.max_term_agg_buckets` and the json of `pdb.agg` are not part of the deprecation.

## What postgres does to the condition, and what the renderer does about it

In the legacy format pg_search gets the json tree as it is. In the new format the condition is a SQL expression, and
postgres simplifies it before pg_search sees it. The matching documents are always the same. Scores would not be:

1. `a OR a` becomes `a`, and `(a AND b) OR (a AND c)` becomes `a AND (b OR c)`. The legacy tree scores `a` once for
   every place it is in. This is common: two queries that share a word, or `(economy AND growth) OR (economy AND
   jobs)`. Measured on a test set: sum of scores 6245.8 (legacy) against 5035.4 (merged), with another top 10.
   The renderer therefore writes the second and later copy of the same scored predicate as
   `K ||| CASE WHEN extract(epoch FROM now()) = -1 THEN NULL ELSE value END`. The condition is never true; its
   number makes the copy another expression to postgres, so nothing is merged. pg_search computes the value when
   the statement starts (the way it does for the parameters of a prepared statement) and runs the same query.
   Predicates with constant score 0 (project, filters) are not marked: merging them changes no score.
2. `NOT (a AND NOT b)` becomes `(NOT a OR b)`, where `b` would get a score. Everything under a NOT is therefore
   written with `::pdb.const(0)` (in the legacy tree, what is excluded is never scored).
3. An OR inside an OR, and an AND inside an AND, are flattened. Nothing can be done about that; see the next section.

## Scoring: what is the same and what is not

- Hits and counts: always the same.
- A score that is the sum of at most two parts (one or two words in one field, a phrase, a boost, a word and a
  keyword, any of these with filters) is the very same number in both formats. So is the relevance order.
- A score that is the sum of three or more parts can differ in its last digit. A score is a sum of 32-bit floats, one
  per part of the query that matches. Because of the flattening, and because the filters are separate clauses, the
  parts can be added up in another order.
  Example: the two queries `w3` and `w2`, each searched in two text fields, 1,200 documents: 11 scores differ, such
  as 8.647248 and 8.647249. With three words (`w3 w2 w1`), 140 of 1,105 scores differ in their last digit.
- The effect on the relevance order: two documents whose scores are the same up to that last digit can change
  places. Seen in the tests at the top of a page (two documents with nearly the same score swapped). No other
  difference in order was found in 2,000 random searches plus all listed query types.
- No fix was found. Extra clauses that keep the nesting (`... AND pdb.all()`) are taken out again by postgres.

## Plans (EXPLAIN)

Checked on the partition table (the fast read path) with 200,500 documents, for a word, for two queries with
filters, and for filters only. Both formats get the same plan for every statement:

| Statement | Scan in both formats |
|---|---|
| relevance top 10 (`ORDER BY score DESC, id LIMIT 10`) | Top-K scan (`TopKScanExecState`) |
| sort on a slot column, top 10 | Top-K scan |
| sort on a json field (late fetch, cutoff statements) | normal scan plus sort |
| `count(*)` | Aggregate Scan |
| `pdb.agg` (terms; stats and histogram of the cutoff sort) | Aggregate Scan |
| GROUP BY on a keyword | Aggregate Scan |

The test `test_plans` checks this on the test data. EXPLAIN also shows the query that pg_search runs ("Tantivy
Query"): it is the same query in both formats, apart from the grouping described above.

One difference, on the parent table only (fast path off, searches over several partitions, writes): for a count with
filters, postgres chose an Index Only Scan on the search index in the legacy format and pg_search's own scan in the
new format. The new format was not slower (see below).

No statement lost a fast scan, so no statement was kept on the legacy format for speed.

## Speed

A rough comparison, not a benchmark: 200,500 synthetic documents in one partition, through the normal functions
(page plus total), median of 22 runs, no parallel workers, same machine, one after the other.

| Request | Legacy (ms) | New (ms) |
|---|---|---|
| relevance top 10, common word in two text fields | 10.3 | 8.6 |
| relevance top 10, three words and three filters | 22.3 | 19.9 |
| relevance top 10, phrase, prefix and boost | 33.1 | 32.8 |
| relevance top 10, two queries that share a word (marked copy) | 18.9 | 19.3 |
| count, common word | 3.3 | 3.4 |
| count, filters only | 18.3 | 18.8 |
| sort on a slot column, word and filters | 15.9 | 11.8 |
| sort on a json field (cutoff sort), word | 18.0 | 18.8 |
| terms aggregation, word and filters | 19.0 | 14.8 |
| terms aggregation, no query | 7.3 | 7.4 |
| parent table: count, two queries and filters | 22.3 | 19.3 |
| parent table: relevance top 10, words and filters | 18.7 | 11.2 |

The new format is as fast as the legacy one, and somewhat faster with filters.

## Other findings about pg_search 0.27.0

- **The Top-K bug is in the new format too.** A statement on a partition table whose WHERE is only the search
  condition, ordered by score with a LIMIT, returns a wrong top list. With `AMCAT4_SCORE_PARTITION_CONDITION=false`,
  `test_parity_relevance` fails in both formats (14 of 60 results wrong in the legacy format, 12 in the new one). The
  extra condition `partition_id = N` stays, in both formats.
- **A phrase of three or more words with slop** (`"w1 w2 w3"~3`) finds more documents when pg_search does not
  compute scores (count, sort on a field) than when it does (relevance order): 236 counted, 152 with scores, on the
  test data. This is the same in both formats, and it was there before this work. It was not looked into further.
  Phrases of two words do not have it.
- A text that is not valid json, cast to `pdb.query`, is read as a query string. AmCAT never does that.

## Tests

`backend/tests/test_new_query_api.py`. Every test runs the same search in both formats on the same documents (two
projects, so every field has two json keys) and compares all hits, all scores, the count and the first pages:

- 118 query strings: words, AND/OR/NOT, `+` and `-`, fields, the default search over all text fields, phrases with
  slop, prefix, phrase prefix, fuzzy, boosts, keywords, tags, urls, whole, decimal and negative numbers, every kind
  of range, dates with and without a time, booleans, geo sub-fields, repeated words, errors;
- 83 filters: values, ranges with open and closed bounds, exists true and false for every field type, date parts,
  document ids, combinations; alone and with a query string;
- fields that a user may not search (`queryable`), several labelled queries;
- every sort order (relevance, slot column, json field with cutoff sort and late fetch, tags, id), highlighting and
  snippets, aggregations inside the index and with GROUP BY, field values and statistics, update and delete by query;
- the other switches off (`filter_clauses`, `fast_path`, `text_exists_key`), and prepared statements with a
  generic plan;
- random query strings with random filters (150 cases by default, `AMCAT4_NEW_API_CASES`; run several times with 2,000).
  Scores depend on the state of the index: deleted rows count until pg_search merges segments, and it merges in
  the background. So the same legacy search can give other scores a moment later. With 2,000 cases this happened
  in about one run of three. The test now runs the legacy search again when scores differ, and compares once
  more if the legacy scores themselves have changed;
- the SQL text the renderer writes, the plans, and the pg_search facts listed above.

Once by hand, on the table of 200,500 documents (15 index segments): 60 random searches, 3.39 million hits compared,
same hits, counts and pages, all scores the same up to the last digit.

The existing suite passes with the switch off and with `AMCAT4_NEW_QUERY_API=true`. `test_parity.py` now takes its
score function from the switch. Results of the final runs are in the commit message of this document.

## Not tested

- A pg_search version other than 0.27.0. The fuzzy argument, the `pdb.query` constant for ranges, the marked copies
  and the constant score of `pdb.all()` should be checked again after an upgrade; the tests named above do that.
- Real data and real size: the largest table was 200,500 synthetic documents in one partition. Not tested: several
  partitions with many documents, concurrent load, parallel workers, a merge running during a search.
- Speed through the HTTP API (a separate benchmark exists).
- Vector search (`similar`) with a query in the new format beyond what the existing suite does.
- Whether the differences in the last digit of a score ever move a document more than one place.
- A project that is being moved between partitions, beyond what the existing suite does with the switch on.
