# Version 3, phase 3: quick speed wins without schema change

25 Nov 2026. PostgreSQL 18, pg_search 0.27.0. Five changes that make more requests run inside the search index,
without changing the documents table or rebuilding the search index. Each has a settings switch (default on; off
gives the behaviour of postgres-fast-v2 / phase 1). The tests are in `backend/tests/test_quick_wins.py`; every test
compares the new path with the old path on the same data.

| # | Change | Switch (environment variable) |
|---|---|---|
| 1 | More aggregations inside the index | `AMCAT4_INDEX_AGG_TYPES` |
| 2 | Field values and field statistics from the index | `AMCAT4_INDEX_FIELD_STATS` |
| 3 | "Text field exists" filter inside the index | `AMCAT4_TEXT_EXISTS_KEY` |
| 4 | A sort slot for every new field, while slots are free | `AMCAT4_AUTO_SORT_SLOT` |
| 5 | lz4 for new values of exact_fields and stored_fields, and for uploads | `AMCAT4_LZ4_JSON_COLUMNS` |

One migration was added (0004, for change 3). It adds one column to `fields` and a trigger on `documents`. It does
not rewrite documents and does not touch the search index.

## 1. More aggregations inside the index

File: `amcat4/postgres/index_aggregate.py`.

Now done with `pdb.agg` inside the index (before: GROUP BY, which reads every matching row):

- group by a number, integer or boolean field (without an interval), alone or nested with other axes;
- the minimum and maximum of a date field;
- metrics without any axis (the count and one `stats` aggregation per metric, in one statement).

Facts that the code depends on (checked by hand and in the tests):

- Dates come back from `stats` as microseconds since 2000-01-01. They are converted to seconds since 1970 with exact
  arithmetic, so the result is the same as `extract(epoch ...)` in SQL.
- Boolean buckets have the key 0 or 1 and `key_as_string` "false" or "true".
- A number field with whole and decimal values gives one bucket per value. Whole values come back as integers (17),
  others as decimals (2.5), in no particular order. The rows are sorted afterwards, so the order does not matter. The
  test uploads whole numbers, decimals and both in separate index segments, with 1e22, 1e-7, 0.0 and -0.0, and gets
  the same rows as GROUP BY.

The result of the index is not used, and GROUP BY runs instead, when:

- an integer key is 2^53 or larger, or a date is more than 285 years from 2000 (the index computes with 64-bit
  floats, which cannot hold these exactly);
- the same key comes back in two buckets (never seen).

Left on the old path, and why:

- number intervals (`floor(x / interval)`): a histogram in the index has no group for documents without a value;
- hour and minute axes: not solved (too many buckets for a date histogram);
- url axes: not solved (the in-index GROUP BY runs out of memory on a field that is unique per document);
- average and sum of dates: the index adds up floats, SQL exact numbers, so the last digits could differ.

Not measured on this code. By hand on 200,000 documents before this work: group by an integer field 199 ms
(GROUP BY) against 23 ms (`pdb.agg`); a keyword axis with max(date) 409 ms (GROUP BY), where `stats` on the date
key alone took 3.8 ms.

## 2. Field values and field statistics from the index

Files: `amcat4/systemdata/fields.py` (`field_values`, `field_stats`), `amcat4/postgres/index_aggregate.py`
(`index_field_values`, `index_field_stats`).

Both read every document of the project before. Now `field_values` is a terms aggregation and `field_stats` a stats
aggregation on the partition table. Values with the same count are still sorted by postgres, so the order is the
same on any database collation.

The original statement is still used when:

- the values of a number or date field are asked (their text is not the stored text: `17` against `17.0`);
- the field has more than 65,000 different values (the index would cut the list);
- a bucket key has an unexpected type. A keyword value that looks like a full timestamp
  (`2024-05-01T00:00:00Z`) is a date inside the index, and two spellings of one moment become one bucket;
- an integer is 2^53 or larger, or a date cannot be held in a float;
- the project is being moved to another partition, or `AMCAT4_FAST_PATH` is off.

Two known differences in the output:

- The average is computed with floats inside the index. For numbers this is float precision; for dates it can differ
  from the SQL average by microseconds. Count, minimum and maximum are the same.
- A tag that is twice in one document counts once (the number of documents). The original statement counted it twice.

Not measured on this code. By hand on 200,000 documents before: values 441 ms, statistics 235 ms with the original
statements; 2 to 12 ms for the aggregation.

## 3. "Text field exists" filter inside the index

Files: migration `0004_text_keys.py`, `amcat4/postgres/filters.py`, `amcat4/postgres/fields.py`,
`amcat4/projects/jobs.py`.

This was the only filter outside the index: the SQL condition `text_fields ? 'f2'`, which reads the long text of
every hit. Now every document has the keys of its text fields that have a value as a short list in exact_fields,
for example `"_text": ["f1", "f2"]`. exact_fields is columnar in the index, so the filter is a term clause.

- One list, not one boolean key per text field: one column in the index, and a few bytes per document.
- The list is written by a trigger on documents (`BEFORE INSERT OR UPDATE OF text_fields`), not by the upload code.
  Reason: the list depends on all text fields of the row, also after an upload that is merged with the stored row,
  and five other statements write text (update by query, deleting a field, changing a field type, copy, move). With
  the trigger all of them keep the list right, and the upload code did not change. Statements that do not write
  text_fields do not fire it.
- Documents stored before the migration get the list from the background job `backfill_text_keys` (scheduled every
  minute, like `backfill_quarter`). `fields.text_exists_ready` is false for existing text fields until the job of
  their project is done. Until then the filter uses the SQL condition, with the same results. The job compares
  every list with the text fields, so it also corrects a wrong list (for example after a downgrade and upgrade).
- A text field that is created after the migration is ready at once.
- No index rebuild: exact_fields is indexed as a whole.

Cost of the trigger: in a crude test (insert 60,000 rows of 1.5 kB text, a few runs) the insert took 3.6 to 4.2 s
with the trigger and 3.2 to 3.8 s without. The difference is about as large as the noise; at most about 10%.

The speed of the filter is not measured on this code. By hand on 200,000 documents before: a page 180 ms and a count
260 ms with the SQL condition; a term filter takes 1 to 2 ms.

## 4. A sort slot for every new field, while slots are free

File: `amcat4/systemdata/fields.py` (`create_fields`).

A new field gets the first free sort slot of its kind in the project: keyword and url fields share four slots, date
fields have two, number and integer fields share one. Before, only the first date field and a keyword field called
`source` got a slot without asking.

- The field is new, so no document has a value. Uploads fill the slot column from the first document, no fill job
  runs, and the slot is ready at once.
- When the slots of a kind are taken, later fields get none: first come, first served. `fast_sort: false` on a
  field frees its slot, and `fast_sort: true` gives a free slot to another field, as before.
- Existing fields are not changed.
- A slot whose last clear or fill job is not done (waiting, running, failed or cancelled) is not given out: the
  column can still hold values of the field that left it.

Note: the first keyword field of a project now gets the `source` column, also when it has another name.

Not measured on this code (sort by publisher, Nov 2026: 2.9 to 4.6 ms on a slot against 48.7 ms on the json value).

## 5. lz4 for new values of exact_fields and stored_fields, and for uploads

Files: `amcat4/postgres/layout.py` (`ensure_json_compression`), `amcat4/systemdata/manage.py`,
`amcat4/postgres/documents.py` (the staging table).

- At startup, after the migrations, the compression of exact_fields and stored_fields is set to lz4 on documents and
  on every partition. New partitions copy the setting. Existing rows are not rewritten. This is not a migration,
  because it follows a setting: with the switch off, the next start sets the server's default again.
- The server here supports lz4 (`pg_config` shows `--with-lz4`; `default_toast_compression` is pglz). On a server
  without lz4, or when a table stays locked for 5 seconds, a warning is logged and nothing changes.
- Found on the way: uploads did not use the compression setting of documents at all. A large value is compressed
  when it is written to the staging table, and `INSERT ... SELECT` copies the compressed value as it is. So
  uploaded texts were pglz, although text_fields is set to lz4 since migration 0002. Now the staging table gets the
  settings of documents (with the switch on), so uploaded texts are lz4 too.
- Postgres only compresses values of about 2 kB and more. Most exact_fields values are smaller, so for that column
  the effect is small.

Crude check (20,000 documents with about 4 kB of text and a 2 kB object, two runs each, noisy): upload 8.3 to 9.6 s
with lz4 and 9.8 to 20.5 s without; reading all values 0.39 to 0.50 s against 0.70 to 1.19 s. The compressed values
were about 20% larger with lz4 (53 MB against 43 MB for the texts).

## Found, not changed

A keyword or tag value that looks like a full timestamp (`2024-05-01T00:00:00Z`) is indexed by pg_search as a date.
The existing in-index aggregations on such a field (both `pdb.agg` and GROUP BY on the partition table) then return
a number as the group, and merge two spellings of the same moment. The original SQL on the parent table is right.
`field_values` (change 2) checks for this; the aggregation paths of postgres-fast-v2 do not.
