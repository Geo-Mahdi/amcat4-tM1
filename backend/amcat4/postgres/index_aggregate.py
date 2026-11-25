"""
Aggregations computed inside the BM25 index with pg_search's pdb.agg (Elasticsearch-style aggregation json).

The plain SQL GROUP BY (amcat4.postgres.aggregate) is pushed into the index by pg_search only for some shapes, and
only on a partition table. It falls back to fetching every matching row (and unpacking its json) for tag axes
(lateral join), metrics (casts), quarters (date_trunc) and more: 0.6-0.8 s per million documents. pdb.agg reads
the columnar (fast) fields of the index directly for all of these: 30-50 ms per million documents.

This module handles the common shapes and returns None for everything else, so the caller can use GROUP BY:
- axes: keyword and tag fields, and date fields with an interval that is stored as a derived key (year, quarter,
  month, ...). Fields whose documents do not all have the quarter key yet (stored before migration 0002, until the
  backfill job is done) compute quarters from the month buckets. Nested axes become nested terms aggregations.
- metrics: avg, min, max, sum of number and integer fields (not with a quarter axis computed from months), from a
  stats aggregation, so that a bucket without values gives null (as GROUP BY) for every function.
- with AMCAT4_INDEX_AGG_TYPES (version 3, on by default): axes on number, integer and boolean fields (without an
  interval), min and max of date fields, and metrics without any axis. See the notes at _typed_key and _date_seconds.
- a single project per field (fields of several projects have different keys: GROUP BY handles that).
The query must use the partition table (CompiledSearch.fast), because pdb.agg does not work on the parent.

Correctness rules: every terms aggregation asks for all buckets (MAX_BUCKETS); if pg_search reports buckets that
were left out (sum_other_doc_count > 0) or fails, the result is not used (None) and GROUP BY runs instead (but not
after a statement timeout or a connection problem: these errors are raised).
Documents without a value are reported by pg_search as a bucket with key null, which is the same as the NULL group
of GROUP BY, except for tags, where GROUP BY (a lateral join over the tag list) has no NULL group.
"""

import json
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any

import psycopg
from psycopg import AsyncConnection, sql

from amcat4.postgres.connection import must_not_fall_back
from amcat4.postgres.fields import DATE_DERIVED, FieldInfo, FieldSet, use_quarter_key

# Ask every terms aggregation for all its buckets: the most pg_search returns (paradedb.max_term_agg_buckets).
# A smaller size would make pg_search leave out the smallest buckets, which GROUP BY never does.
MAX_BUCKETS = 65_000
METRICS = {"avg", "min", "max", "sum"}
# pg_search computes aggregations with 64-bit floats, which hold whole numbers exactly only below 2^53. Larger integer
# keys or date values could be rounded, so results with such values are not used (GROUP BY runs instead)
EXACT_FLOAT = 2**53
# pdb.agg returns dates as microseconds since 2000-01-01 (the postgres epoch); GROUP BY as seconds since 1970-01-01
POSTGRES_EPOCH = 946_684_800
TYPED_AXES = ("number", "integer", "boolean")


def _typed() -> bool:
    """The switch AMCAT4_INDEX_AGG_TYPES (see amcat4.config)"""
    from amcat4.config import get_settings

    return get_settings().index_agg_types


class _NotSupported(Exception):
    """This aggregation (or its result) cannot be done with pdb.agg: the caller uses GROUP BY instead"""


def _axis_field(fieldset: FieldSet, axis) -> tuple[FieldInfo, str]:
    """The field and the index path to aggregate on for this axis"""
    fs = fieldset.resolve(axis.field)
    if len(fs) != 1:
        raise _NotSupported("field of several projects")
    f = fs[0]
    if f.subkey or f.in_labels:  # (in_labels: a tag field in the label table is aggregated with a join, see labels.py)
        raise _NotSupported("sub-field or label table")
    if f.type in ("keyword", "tag") and axis.interval is None:
        return f, f.path
    if f.type in TYPED_AXES and axis.interval is None and _typed():
        # (a number interval is floor(x / interval) in GROUP BY; that stays there)
        return f, f.path
    if f.type == "date" and axis.interval in DATE_DERIVED and axis.interval != "quarter":
        return f, f.derived_path(axis.interval)
    if f.type == "date" and axis.interval == "quarter":
        # (the quarter key, or, for documents stored before it existed, the month buckets merged into quarters)
        return f, f.derived_path("quarter" if use_quarter_key([f]) else "month")
    raise _NotSupported(f"{f.type} axis with interval {axis.interval}")


def _metric_field(fieldset: FieldSet, metric) -> FieldInfo:
    """
    The field of a metric: avg/min/max/sum of a number or integer field of one project, or (AMCAT4_INDEX_AGG_TYPES)
    min/max of a date field. Not avg and sum of dates: the index adds up 64-bit floats, GROUP BY exact numerics, so
    the average date could differ in its last digits.
    """
    fs = fieldset.resolve(metric.field)
    if len(fs) != 1 or fs[0].subkey or metric.function not in METRICS:
        raise _NotSupported("metric")
    if fs[0].type in ("number", "integer"):
        return fs[0]
    if fs[0].type == "date" and metric.function in ("min", "max") and _typed():
        return fs[0]
    raise _NotSupported("metric")


def _key(value: Any) -> Any:
    """Bucket keys as GROUP BY on the json text (->>) would return them"""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return str(value)


def _typed_key(bucket: dict, ftype: str) -> Any:
    """
    The key of a bucket of a number, integer or boolean axis, as text that postprocess_value converts like the value
    of GROUP BY. Checked by hand and in tests (pg_search 0.27.0, 25 Nov 2026):
    - boolean: the key is 0 or 1, with key_as_string "false" or "true";
    - number: whole values come back as json integers (17, also 1e22 in full), others as decimals; a field that
      mixes both gives one bucket per value (in no particular order, also over several index segments);
    - integer: keys are computed as 64-bit floats, so from 2^53 on they could be rounded: not used.
    """
    key = bucket.get("key")
    if key is None:
        return None
    if ftype == "boolean":
        text = bucket.get("key_as_string")
        if text not in ("true", "false"):
            raise _NotSupported(f"boolean key {key!r}")
        return text
    if isinstance(key, bool) or not isinstance(key, (int, float)):
        raise _NotSupported(f"{ftype} key {key!r}")
    if ftype == "integer" and (abs(key) >= EXACT_FLOAT or not float(key).is_integer()):
        raise _NotSupported(f"integer key {key!r}")
    return _key(key)


def _date_seconds(value: Any) -> Decimal:
    """
    A date from a stats aggregation (microseconds since 2000-01-01, as a float) as seconds since 1970-01-01, exact to
    the microsecond like extract(epoch ...) in GROUP BY. Dates more than 2^53 microseconds (285 years) from 2000 cannot
    be exact in a float: not used.
    """
    if not isinstance(value, (int, float)) or abs(value) >= EXACT_FLOAT or not float(value).is_integer():
        raise _NotSupported(f"date value {value!r}")
    return Decimal(int(value)) / 1_000_000 + POSTGRES_EPOCH


def _quarter(month: str | None) -> datetime | None:
    """The first day of the quarter of a month key (2024-05 -> 2024-04-01), as date_trunc('quarter') in GROUP BY"""
    if month is None:
        return None
    year, m = month.split("-")
    return datetime(int(year), (int(m) - 1) // 3 * 3 + 1, 1)


def build_request(fieldset: FieldSet, axes: list, metrics: list) -> tuple[dict, list[FieldInfo]]:
    """
    The pdb.agg json for these axes and metrics, and the fields of the axes (raises _NotSupported).
    Without axes: {m0: stats, m1: stats, ...}, one pdb.agg each (see index_aggregate).
    """
    fields = []
    paths = []
    for axis in axes:
        f, path = _axis_field(fieldset, axis)
        fields.append(f)
        paths.append(path)
    if metrics and any(a.interval == "quarter" and not path.endswith("_quarter") for a, path in zip(axes, paths)):
        raise _NotSupported("metrics with a quarter axis computed from months")
    # stats, not the function itself: its count says whether the bucket has values. sum alone gives 0.0 for a bucket
    # without any value, where GROUP BY (and Elasticsearch's min/max/avg) give null
    metric_fields = [_metric_field(fieldset, m) for m in metrics]
    leaf = {f"m{j}": {"stats": {"field": f.path}} for j, f in enumerate(metric_fields)}
    if not axes:
        if not metrics or not _typed():
            raise _NotSupported("no axes")  # a single count: GROUP BY handles it
        return leaf, fields
    # pdb.agg takes one (unnamed) aggregation at the top; nested aggregations are named a1, a2, ... and m0, m1, ...
    sub: dict = leaf
    agg: dict = {}
    for i in reversed(range(len(axes))):
        agg = {"terms": {"field": paths[i], "size": MAX_BUCKETS}}
        if sub:
            agg["aggs"] = sub
        sub = {f"a{i}": agg}
    return agg, fields


def _rows(
    result: dict, axes: list, fields: list[FieldInfo], metrics: list, metric_fields: list[FieldInfo], level: int, prefix: tuple
) -> list[dict]:
    """
    Flatten the (nested) terms buckets into rows {a0.., n, m0..}, one per combination of axis values.
    Raises _NotSupported if pg_search left out buckets, so a cut result is never returned.
    """
    if level == len(axes):
        return []
    agg = result if level == 0 else result[f"a{level}"]
    if agg.get("sum_other_doc_count", 0):
        raise _NotSupported("buckets left out")
    rows = []
    for bucket in agg.get("buckets", []):
        ftype = fields[level].type
        key = _typed_key(bucket, ftype) if ftype in TYPED_AXES else _key(bucket.get("key"))
        if key is None and ftype == "tag":
            continue  # documents without tags: GROUP BY (a lateral join over the tag list) has no such group
        if axes[level].interval == "quarter":
            key = _quarter(key)
        if level + 1 < len(axes):
            rows.extend(_rows(bucket, axes, fields, metrics, metric_fields, level + 1, (*prefix, key)))
        else:
            row = {f"a{i}": v for i, v in enumerate((*prefix, key))}
            row["n"] = bucket["doc_count"]
            for j, metric in enumerate(metrics):
                row[f"m{j}"] = _metric_value(bucket.get(f"m{j}", {}), metric.function, metric_fields[j].type)
            rows.append(row)
    return rows


def _metric_value(stats: dict | None, function: str, ftype: str = "number") -> Any:
    """The value of a metric from a stats result: None (like GROUP BY) if no document of the bucket has a value"""
    if not stats or not stats.get("count"):
        return None
    if ftype == "date":
        return _date_seconds(stats.get(function))
    return stats.get(function)


def _merge_quarters(rows: list[dict], n_axes: int) -> list[dict]:
    """
    Add up the counts of rows with the same axis values: the three month buckets of a quarter (when quarters are
    computed from months) become one row. With the quarter key the rows are unique already and nothing changes.
    Metrics are not merged: build_request refuses metrics with quarters computed from months.
    """
    merged: dict[tuple, dict] = {}
    for row in rows:
        key = tuple(row[f"a{i}"] for i in range(n_axes))
        if key in merged:
            merged[key]["n"] += row["n"]
        else:
            merged[key] = dict(row)
    return list(merged.values())


async def index_aggregate(
    conn: AsyncConnection, table: sql.Composable, where: sql.Composable, params: list, fieldset: FieldSet, axes, metrics
) -> list[dict] | None:
    """
    Rows {a0.., n, m0..} like the GROUP BY statement returns (raw axis values, before postprocessing),
    or None if this aggregation cannot (or should not) be done with pdb.agg.
    """
    try:
        request, fields = build_request(fieldset, axes, metrics)
        metric_fields = [_metric_field(fieldset, m) for m in metrics]
    except _NotSupported:
        return None
    if axes:
        select: sql.Composable = sql.SQL("pdb.agg({}::jsonb) AS result").format(sql.Literal(json.dumps(request)))
    else:
        # metrics without an axis: the count and one stats aggregation per metric, in one scan of the index
        select = sql.SQL(", ").join(
            [sql.SQL("count(*) AS n")]
            + [
                sql.SQL("pdb.agg({}::jsonb) AS {}").format(sql.Literal(json.dumps(r)), sql.Identifier(m))
                for m, r in request.items()
            ]
        )
    stmt = sql.SQL("SELECT {} FROM {} WHERE {}").format(select, table, where)
    try:
        cur = await conn.execute(stmt, params)
        row: dict = await cur.fetchone()  # type: ignore[assignment]
    except psycopg.Error as e:
        if must_not_fall_back(e):
            raise
        logging.warning(f"pdb.agg failed, using GROUP BY instead: {e}")
        return None
    try:
        if not axes:
            return [_single_row(row, metrics, metric_fields)]
        rows = _rows(_json(row["result"]), axes, fields, metrics, metric_fields, 0, ())
        if any(a.interval == "quarter" for a in axes):
            rows = _merge_quarters(rows, len(axes))
        elif len({tuple(row[f"a{i}"] for i in range(len(axes))) for row in rows}) != len(rows):
            # (never seen; GROUP BY cannot return the same group twice, so such a result is not used)
            raise _NotSupported("the same key in two buckets")
    except _NotSupported as e:
        logging.info(f"pdb.agg result not usable ({e}), using GROUP BY instead")
        return None
    return rows


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _single_row(row: dict | None, metrics: list, metric_fields: list[FieldInfo]) -> dict:
    """The one row {n, m0, ...} of an aggregation without axes"""
    if row is None or row.get("n") is None:
        raise _NotSupported("no result")
    out = {"n": row["n"]}
    for j, metric in enumerate(metrics):
        out[f"m{j}"] = _metric_value(_json(row[f"m{j}"]), metric.function, metric_fields[j].type)
    return out


# field_values: which json type the bucket keys of each field type must have. A keyword value that looks like a full
# timestamp (2024-05-01T00:00:00Z) is indexed by pg_search as a date and comes back as a number; such a result is not
# used. Number and date fields are not listed (their values are not formatted like the stored json): old statement.
_STRING_VALUES = "jsonb_typeof(b->'key') <> 'string'"
_VALUE_CHECKS = {
    **{t: _STRING_VALUES for t in ("keyword", "tag", "url", "image", "video", "audio")},
    "boolean": "NOT b ? 'key_as_string'",
    # whole numbers of at most 15 characters: below 2^53, so not rounded by the 64-bit floats of the index
    "integer": "jsonb_typeof(b->'key') <> 'number' OR b->>'key' !~ '^-?[0-9]{1,14}$'",
}


async def _try(conn: AsyncConnection, stmt: sql.Composable, params: list) -> dict | None:
    """The one row of a statement, or None if it fails with an error that allows using the old statement"""
    try:
        cur = await conn.execute(stmt, params)
        return await cur.fetchone()  # type: ignore[return-value]
    except psycopg.Error as e:
        if must_not_fall_back(e):
            raise
        logging.warning(f"pdb.agg failed, using the statement without the index: {e}")
        return None


async def index_field_values(
    conn: AsyncConnection, table: sql.Composable, where: sql.Composable, params: list, f: FieldInfo, size: int
) -> list[str] | None:
    """
    The most frequent values of a field (most frequent first, then by value) from a terms aggregation in the index,
    or None if that cannot be used: the field type is not listed in _VALUE_CHECKS, there are more than MAX_BUCKETS
    different values, a key has an unexpected type, or the statement fails. Sorted by postgres, so values with the
    same count are in the same order as in the statement without the index.
    The count is the number of documents with the value; the statement without the index counts a tag that is twice
    in one document twice.
    """
    check = _VALUE_CHECKS.get(f.type)
    if check is None or f.subkey:
        return None
    request = {"terms": {"field": f.path, "size": MAX_BUCKETS}}
    stmt = sql.SQL(
        """WITH a AS MATERIALIZED (SELECT pdb.agg({request}::jsonb) AS r FROM {table} WHERE {where}),
                b AS (SELECT b, coalesce(b->>'key_as_string', b->>'key') AS value, (b->>'doc_count')::bigint AS n
                      FROM a, jsonb_array_elements(a.r->'buckets') AS b WHERE jsonb_typeof(b->'key') <> 'null')
           SELECT (SELECT r IS NOT NULL AND coalesce((r->>'sum_other_doc_count')::bigint, 0) = 0 FROM a) AS complete,
                  (SELECT count(*) FROM b WHERE {check}) AS unexpected,
                  (SELECT coalesce(array_agg(value ORDER BY n DESC, value), '{{}}')
                   FROM (SELECT value, n FROM b ORDER BY n DESC, value LIMIT %s) top) AS values"""
    ).format(request=sql.Literal(json.dumps(request)), table=table, where=where, check=sql.SQL(check))  # type: ignore[arg-type]
    row = await _try(conn, stmt, [*params, size])
    if row is None or not row["complete"] or row["unexpected"]:
        return None
    return row["values"]


async def index_field_stats(
    conn: AsyncConnection, table: sql.Composable, where: sql.Composable, params: list, f: FieldInfo
) -> dict[str, Any] | None:
    """
    {count, min, max, avg} of a number, integer or date field from a stats aggregation in the index (dates as seconds
    since 1970, like the statement without the index), or None if that cannot be used.
    """
    stmt = sql.SQL("SELECT pdb.agg({}::jsonb) AS r FROM {} WHERE {}").format(
        sql.Literal(json.dumps({"stats": {"field": f.path}})), table, where
    )
    row = await _try(conn, stmt, params)
    stats = _json(row["r"]) if row is not None else None
    if not isinstance(stats, dict) or not isinstance(stats.get("count"), (int, float)):
        return None
    if not stats["count"]:
        return {"count": 0, "min": None, "max": None, "avg": None}
    try:
        if f.type == "date":
            # min and max are exact; the average is a 64-bit float (it can differ from the SQL average by microseconds)
            avg = Decimal(stats["avg"]) / 1_000_000 + POSTGRES_EPOCH
            return {
                "count": int(stats["count"]),
                "min": _date_seconds(stats["min"]),
                "max": _date_seconds(stats["max"]),
                "avg": avg,
            }
        return {"count": int(stats["count"]), **{k: float(stats[k]) for k in ("min", "max", "avg")}}
    except (_NotSupported, TypeError, ValueError, ArithmeticError) as e:
        logging.info(f"pdb.agg result not usable ({e}), using the statement without the index")
        return None
