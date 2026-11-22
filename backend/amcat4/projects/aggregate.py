"""
Aggregate queries
"""

import logging
from datetime import UTC, datetime
from typing import Any, Iterable, List, Literal, Sequence

import psycopg
from psycopg import sql

from amcat4.models import FilterSpec, SortSpec
from amcat4.postgres.aggregate import Axis as StorageAxis
from amcat4.postgres.aggregate import axis_expr, postprocess_value, sort_key
from amcat4.postgres.connection import connection, must_not_fall_back
from amcat4.postgres.documents import field_select
from amcat4.postgres.fields import FieldSet, QueryError
from amcat4.postgres.filters import field_sql
from amcat4.postgres.index_aggregate import index_aggregate
from amcat4.postgres.search import SearchQuery, compile_search, count_at_most
from amcat4.systemdata.fields import get_fieldset

# Aggregations over at most this many hits use the original GROUP BY on the parent table, without the in-index paths:
# those cost at least 10 to 25 ms whatever the number of hits, while GROUP BY over a few hundred rows is faster.
# (Counting up to this limit first is cheap: it stops at SMALL_RESULT + 1 hits.)
SMALL_RESULT = 1_000

# Maximum number of rows (buckets) that can be requested
MAX_LIMIT = 10_000


class Axis:
    """
    Class that specifies an aggregation axis
    """

    def __init__(self, field: str, interval: str | None = None, name: str | None = None, field_type: str | None = None):
        self.field = field
        self.interval = interval
        self.ftype = field_type
        if name:
            self.name = name
        elif interval:
            self.name = f"{field}_{interval}"
        else:
            self.name = field

    def __repr__(self):
        return f"<Axis field={self.field} ftype={self.ftype}>"

    def asdict(self):
        return {"name": self.name, "field": self.field, "type": self.ftype, "interval": self.interval}


class TopHitsAggregation:
    """
    Specification of a top hits aggregation: the first n documents in each bucket
    """

    def __init__(
        self,
        fields: list[str],
        sort: list[dict[str, SortSpec]] | None = None,
        *,
        n: int = 1,
        name: str | None = None,
        function: Literal["top_hits"] = "top_hits",
    ):
        self.fields = fields
        self.name = name or "tophits"
        self.sort = sort
        self.n = n
        self.type = "_tophits"

    def asdict(self):
        return {"fields": self.fields, "function": "top_hits", "name": self.name}

    def set_ftype(self, fieldset: FieldSet):
        for f in self.fields:
            fieldset.resolve(f)


class Aggregation:
    """
    Specification of a single aggregation, that is, field and aggregation function
    """

    FUNCTIONS = {"avg", "min", "max", "sum"}

    def __init__(self, field: str, function: str, name: str | None = None, ftype: str | None = None):
        if function not in self.FUNCTIONS:
            raise ValueError(f"Unknown aggregation function {function}, use one of {self.FUNCTIONS}")
        self.field = field
        self.function = function
        self.name = name or f"{function}_{field}"
        self.ftype = ftype

    def asdict(self):
        return {"field": self.field, "type": self.ftype, "function": self.function, "name": self.name}

    def set_ftype(self, fieldset: FieldSet):
        self.ftype = fieldset.resolve(self.field)[0].type

    def sql(self, fieldset: FieldSet) -> sql.Composable:
        fs = fieldset.resolve(self.field)
        if fs[0].type not in ("number", "integer", "date"):
            raise QueryError(f"Cannot compute {self.function} of {fs[0].type} field {self.field}")
        exprs = [field_sql(f) for f in fs]
        x = exprs[0] if len(exprs) == 1 else sql.SQL("coalesce({})").format(sql.SQL(", ").join(exprs))
        if fs[0].type == "date":
            # aggregate dates as epoch seconds (e.g. avg is not defined for timestamps)
            x = sql.SQL("extract(epoch FROM {})").format(x)
        function = {"avg": sql.SQL("avg"), "min": sql.SQL("min"), "max": sql.SQL("max"), "sum": sql.SQL("sum")}
        return sql.SQL("{}({})").format(function[self.function], x)

    def get_value(self, value: Any) -> Any:
        if value is None:
            return None
        if self.ftype == "date":
            return datetime.fromtimestamp(float(value), tz=UTC).isoformat()
        return float(value)


class AggregateResult:
    def __init__(
        self,
        axes: Sequence[Axis],
        aggregations: List[Aggregation | TopHitsAggregation],
        data: List[tuple],
        count_column: str = "n",
        truncated: bool = False,
    ):
        self.axes = axes
        self.data = data
        self.aggregations = aggregations
        self.count_column = count_column
        self.truncated = truncated

    def as_dicts(self) -> Iterable[dict]:
        """Return the results as a sequence of {axis1, ..., n} dicts"""
        keys = tuple(ax.name for ax in self.axes) + (self.count_column,)
        if self.aggregations:
            keys += tuple(a.name for a in self.aggregations)
        for row in self.data:
            yield dict(zip(keys, row))


async def _aggregate(
    fieldset: FieldSet,
    query: SearchQuery,
    axes: list[Axis],
    aggregations: list[Aggregation | TopHitsAggregation],
) -> list[tuple]:
    """
    Run an aggregation (without _query axis). Returns rows of (axis values..., count, aggregation values...)

    Unlike the original (one GROUP BY on the parent table), the first of these that applies is used:
    1. no fast path (projects in several partitions, a project being moved, a filter on document ids,
       AMCAT4_FAST_PATH=false), or at most SMALL_RESULT hits: the original GROUP BY on the parent table;
    2. pdb.agg inside the BM25 index of the one partition (amcat4.postgres.index_aggregate), for the shapes it handles;
    3. the same GROUP BY on the partition table, which pg_search can partly do inside the index;
    4. if 3 fails with a query or feature error: the original GROUP BY on the parent table.
    All give the same rows. Measured: a whole-project aggregation on the partition table took 37 ms against 1,640 ms on
    the parent (2.7 M documents, pg_search 0.26); pdb.agg takes 30-50 ms per million documents for tag, quarter and
    metric axes, where GROUP BY fetches every row (0.6-0.8 s per million).
    A statement timeout or connection problem is raised at once, without a slower second statement.
    """
    c = compile_search(fieldset, query, fast=True)
    selects: list[sql.Composable] = []
    laterals: list[sql.Composable] = []
    for i, axis in enumerate(axes):
        expr, lateral = axis_expr(StorageAxis(axis.field, axis.interval), fieldset.resolve(axis.field), f"tag_{i}")
        selects.append(sql.SQL("{} AS {}").format(expr, sql.Identifier(f"a{i}")))
        if lateral is not None:
            laterals.append(lateral)
    selects.append(sql.SQL("count(*) AS n"))
    metrics = [a for a in aggregations if isinstance(a, Aggregation)]
    for j, metric in enumerate(metrics):
        selects.append(sql.SQL("{} AS {}").format(metric.sql(fieldset), sql.Identifier(f"m{j}")))

    group = sql.SQL("")
    if axes:
        group = sql.SQL("GROUP BY {}").format(sql.SQL(", ").join(sql.Literal(i + 1) for i in range(len(axes))))

    def statement(c) -> sql.Composable:
        return sql.SQL("SELECT {} FROM {} {} WHERE {} {}").format(
            sql.SQL(", ").join(selects), c.table, sql.SQL(" ").join(laterals), c.where, group
        )

    async with connection() as conn:
        if c.fast and await count_at_most(conn, c, SMALL_RESULT) <= SMALL_RESULT:
            # In-index aggregation has a floor of 10-25 ms; for a few hundred hits the original statement is faster
            c = compile_search(fieldset, query)
        # In-index aggregation with pdb.agg where possible (returns None for shapes it does not handle)
        rows = await index_aggregate(conn, c.table, c.where, c.params, fieldset, axes, metrics) if c.fast else None
        if rows is None:
            try:
                cur = await conn.execute(statement(c), c.params)
            except psycopg.Error as e:
                if not c.fast or must_not_fall_back(e):
                    raise  # (a timed out request must not run a second, slower statement)
                # The in-index GROUP BY has limits (e.g. at most paradedb.max_term_agg_buckets = 65,000 groups, an
                # error rather than a silent cut). The original statement on the parent table has none of these.
                logging.warning(f"In-index aggregation failed, using the original SQL: {e}")
                c = compile_search(fieldset, query)
                cur = await conn.execute(statement(c), c.params)
            rows = await cur.fetchall()

    storage_axes = [StorageAxis(ax.field, ax.interval) for ax in axes]
    results = []
    for row in rows:
        key = {sa.name: postprocess_value(row[f"a{i}"], sa, fieldset.type(sa.field)) for i, sa in enumerate(storage_axes)}  # type: ignore[index, call-overload]
        values = {"n": row["n"]}  # type: ignore[index, call-overload]
        for j, metric in enumerate(metrics):
            values[metric.name] = metric.get_value(row[f"m{j}"])  # type: ignore[index, call-overload]
        results.append((key, values))
    results.sort(key=lambda r: sort_key(r[0], storage_axes))

    tophits = [a for a in aggregations if isinstance(a, TopHitsAggregation)]
    for th in tophits:
        hits = await _top_hits(fieldset, query, axes, th)
        for key, values in results:
            values[th.name] = hits.get(tuple(_hashable(v) for v in key.values()), [])

    out = []
    for key, values in results:
        row = tuple(key.values()) + (values["n"],)
        row += tuple(values[a.name] for a in aggregations)
        out.append(row)
    return out


def _hashable(v):
    return tuple(v) if isinstance(v, list) else v


async def _top_hits(fieldset: FieldSet, query: SearchQuery, axes: list[Axis], th: TopHitsAggregation) -> dict[tuple, list]:
    """Get the top n documents per bucket using a window function"""
    # On the partition table when the fast path applies (c.table, always named "documents"), as the other read paths
    c = compile_search(fieldset, query, fast=True)
    selects, laterals, partition = [], [], []
    for i, axis in enumerate(axes):
        expr, lateral = axis_expr(StorageAxis(axis.field, axis.interval), fieldset.resolve(axis.field), f"tag_{i}")
        selects.append(sql.SQL("{} AS {}").format(expr, sql.Identifier(f"a{i}")))
        partition.append(expr)
        if lateral is not None:
            laterals.append(lateral)
    for name in th.fields:
        for f in fieldset.resolve(name):
            selects.append(sql.SQL("{} AS {}").format(field_select(f), sql.Identifier(f.key)))
    order: list[sql.Composable] = []
    for s in th.sort or []:
        for name, spec in s.items():
            direction = "DESC" if SortSpec.model_validate(spec).order == "desc" else "ASC"
            exprs = [field_sql(f) for f in fieldset.resolve(name)]
            x = exprs[0] if len(exprs) == 1 else sql.SQL("coalesce({})").format(sql.SQL(", ").join(exprs))
            order.append(sql.SQL("{} {} NULLS LAST").format(x, sql.SQL(direction)))
    order.append(sql.SQL("documents.id"))
    window = sql.SQL("row_number() OVER (PARTITION BY {} ORDER BY {}) AS rn").format(
        sql.SQL(", ").join(partition) if partition else sql.SQL("true"), sql.SQL(", ").join(order)
    )
    stmt = sql.SQL("SELECT * FROM (SELECT {}, documents.project_pk, {} FROM {} {} WHERE {}) t WHERE rn <= %s").format(
        sql.SQL(", ").join(selects), window, c.table, sql.SQL(" ").join(laterals), c.where
    )
    async with connection() as conn:
        cur = await conn.execute(stmt, [*c.params, th.n])
        rows = await cur.fetchall()
    storage_axes = [StorageAxis(ax.field, ax.interval) for ax in axes]
    hits: dict[tuple, list] = {}
    for row in sorted(rows, key=lambda r: r["rn"]):  # type: ignore[index, call-overload]
        key = tuple(
            _hashable(postprocess_value(row[f"a{i}"], sa, fieldset.type(sa.field)))  # type: ignore[index, call-overload]
            for i, sa in enumerate(storage_axes)
        )
        project_fields = fieldset.project_fields[row["project_pk"]]  # type: ignore[index, call-overload]
        doc = {}
        for name in th.fields:
            f = project_fields.get(name)
            if f is not None and row[f.key] is not None:  # type: ignore[index, call-overload]
                doc[name] = row[f.key]  # type: ignore[index, call-overload]
        hits.setdefault(key, []).append(doc)
    return hits


async def query_aggregate(
    index: str | list[str],
    axes: list[Axis] | None = None,
    aggregations: list[Aggregation | TopHitsAggregation] | None = None,
    *,
    queries: dict[str, str] | None = None,
    filters: dict[str, FilterSpec] | None = None,
    order: Literal["axes", "count"] = "axes",
    limit: int = 1000,
    queryable: set[str] | None = None,
) -> AggregateResult:
    """
    Conduct an aggregate query.

    :param index: The name of the index (or a list of indices)
    :param axes: Aggregation axes. Use field "_query" to split the results by query label
    :param aggregations: Aggregation fields
    :param queries: Optional query strings {label: query}
    :param filters: if not None, a dict of filters
    :param order: sort the rows by the axis values (axes), or by the number of documents (count, descending)
    :param limit: return at most this many rows (max MAX_LIMIT). result.truncated says whether there were more rows.
    :param queryable: the fields that may be used in queries and filters (None = all fields)
    """
    if not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f"limit should be between 1 and {MAX_LIMIT}")
    axes = axes or []
    aggregations = aggregations or []
    if sum(x.field == "_query" for x in axes) > 1:
        raise ValueError("Only one aggregation axis may be by query")

    indices = index if isinstance(index, list) else [index]
    fieldset = await get_fieldset(indices, queryable=queryable)
    for axis in axes:
        axis.ftype = "_query" if axis.field == "_query" else fieldset.resolve(axis.field)[0].type
    for aggregation in aggregations:
        aggregation.set_ftype(fieldset)

    if any(ax.field == "_query" for ax in axes):
        if not queries:
            raise ValueError("Queries must be specified when aggregating by query")
        i = [ax.field for ax in axes].index("_query")
        other_axes = axes[:i] + axes[i + 1 :]
        rows: list[tuple] = []
        for label, q in queries.items():
            for row in await _aggregate(fieldset, SearchQuery(queries={label: q}, filters=filters), other_axes, aggregations):
                rows.append(row[:i] + (label,) + row[i:])
    else:
        rows = await _aggregate(fieldset, SearchQuery(queries=queries, filters=filters), axes, aggregations)

    if order == "count":
        n_index = len(axes)
        rows = sorted(rows, key=lambda row: -row[n_index])
    return AggregateResult(axes, aggregations, rows[:limit], count_column="n", truncated=len(rows) > limit)
