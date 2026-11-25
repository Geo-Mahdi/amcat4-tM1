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
from typing import Any

import psycopg
from psycopg import AsyncConnection, sql

from amcat4.postgres.connection import must_not_fall_back
from amcat4.postgres.fields import DATE_DERIVED, FieldInfo, FieldSet, use_quarter_key

# Ask every terms aggregation for all its buckets: the most pg_search returns (paradedb.max_term_agg_buckets).
# A smaller size would make pg_search leave out the smallest buckets, which GROUP BY never does.
MAX_BUCKETS = 65_000
METRICS = {"avg", "min", "max", "sum"}


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
    if f.type == "date" and axis.interval in DATE_DERIVED and axis.interval != "quarter":
        return f, f.derived_path(axis.interval)
    if f.type == "date" and axis.interval == "quarter":
        # (the quarter key, or, for documents stored before it existed, the month buckets merged into quarters)
        return f, f.derived_path("quarter" if use_quarter_key([f]) else "month")
    raise _NotSupported(f"{f.type} axis with interval {axis.interval}")


def _metric_path(fieldset: FieldSet, metric) -> str:
    """The index path of a metric field: only avg/min/max/sum of a number or integer field of one project"""
    fs = fieldset.resolve(metric.field)
    if len(fs) != 1 or fs[0].type not in ("number", "integer") or fs[0].subkey or metric.function not in METRICS:
        raise _NotSupported("metric")
    return fs[0].path


def _key(value: Any) -> Any:
    """Bucket keys as GROUP BY on the json text (->>) would return them"""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return str(value)


def _quarter(month: str | None) -> datetime | None:
    """The first day of the quarter of a month key (2024-05 -> 2024-04-01), as date_trunc('quarter') in GROUP BY"""
    if month is None:
        return None
    year, m = month.split("-")
    return datetime(int(year), (int(m) - 1) // 3 * 3 + 1, 1)


def build_request(fieldset: FieldSet, axes: list, metrics: list) -> tuple[dict, list[FieldInfo]]:
    """The pdb.agg json for these axes and metrics (raises _NotSupported)"""
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
    leaf = {f"m{j}": {"stats": {"field": _metric_path(fieldset, m)}} for j, m in enumerate(metrics)}
    if not axes:
        raise _NotSupported("no axes")  # a single count (with metrics): GROUP BY handles it
    # pdb.agg takes one (unnamed) aggregation at the top; nested aggregations are named a1, a2, ... and m0, m1, ...
    sub: dict = leaf
    agg: dict = {}
    for i in reversed(range(len(axes))):
        agg = {"terms": {"field": paths[i], "size": MAX_BUCKETS}}
        if sub:
            agg["aggs"] = sub
        sub = {f"a{i}": agg}
    return agg, fields


def _rows(result: dict, axes: list, fields: list[FieldInfo], metrics: list, level: int, prefix: tuple) -> list[dict]:
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
        key = _key(bucket.get("key"))
        if key is None and fields[level].type == "tag":
            continue  # documents without tags: GROUP BY (a lateral join over the tag list) has no such group
        if axes[level].interval == "quarter":
            key = _quarter(key)
        if level + 1 < len(axes):
            rows.extend(_rows(bucket, axes, fields, metrics, level + 1, (*prefix, key)))
        else:
            row = {f"a{i}": v for i, v in enumerate((*prefix, key))}
            row["n"] = bucket["doc_count"]
            for j, metric in enumerate(metrics):
                row[f"m{j}"] = _metric_value(bucket.get(f"m{j}", {}), metric.function)
            rows.append(row)
    return rows


def _metric_value(stats: dict, function: str) -> Any:
    """The value of a metric from a stats result: None (like GROUP BY) if no document of the bucket has a value"""
    if not stats.get("count"):
        return None
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
    except _NotSupported:
        return None
    stmt = sql.SQL("SELECT pdb.agg({}::jsonb) AS result FROM {} WHERE {}").format(
        sql.Literal(json.dumps(request)), table, where
    )
    try:
        cur = await conn.execute(stmt, params)
        result = (await cur.fetchone())["result"]  # type: ignore[index, call-overload]
    except psycopg.Error as e:
        if must_not_fall_back(e):
            raise
        logging.warning(f"pdb.agg failed, using GROUP BY instead: {e}")
        return None
    if isinstance(result, str):
        result = json.loads(result)
    try:
        rows = _rows(result, axes, fields, metrics, 0, ())
    except _NotSupported as e:
        logging.info(f"pdb.agg result not usable ({e}), using GROUP BY instead")
        return None
    if any(a.interval == "quarter" for a in axes):
        rows = _merge_quarters(rows, len(axes))
    return rows
