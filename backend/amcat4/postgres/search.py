"""
Searching documents in the postgres backend.

A search is a single SQL query on the documents table. Everything that can be expressed as a pg_search query
(project selection, query string, most filters) is combined into one json query, so it is evaluated inside the
BM25 index. Remaining conditions are added as plain SQL.
"""

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Literal

from psycopg import AsyncConnection, sql
from psycopg.types.json import Jsonb

from amcat4.config import get_settings
from amcat4.models import FilterSpec, SnippetParams
from amcat4.postgres.connection import spare_connection
from amcat4.postgres.fields import FieldInfo, FieldSet, QueryError
from amcat4.postgres.filters import compile_filters, field_sql
from amcat4.postgres.layout import partition_table
from amcat4.postgres.querystring import highlight_patterns, match_positions, parse_query, query_string_to_json
from amcat4.postgres.snippets import byte_to_char_positions, make_snippet
from amcat4.postgres.snippets import highlight as highlight_text


@dataclass
class SearchQuery:
    """The 'where' part of a search: which documents match"""

    queries: dict[str, str] | None = None  # {label: query string}, combined with OR
    filters: dict[str, FilterSpec] | None = None
    ids: list[str] | None = None


@dataclass
class CompiledSearch:
    json_query: dict
    where: sql.Composable
    params: list[Any] = field(default_factory=list)
    # The FROM item, always available under the name "documents": the partitioned table, or (fast read path) the
    # table of the one partition that holds all projects of the query
    table: sql.Composable = field(default_factory=lambda: sql.Identifier("documents"))
    fast: bool = False


def project_clause(project_pks: list[int]) -> dict:
    terms = [{"term": {"field": "project_pk", "value": pk}} for pk in project_pks]
    return terms[0] if len(terms) == 1 else {"boolean": {"should": terms}}


def unscored(clauses: list[dict]) -> dict:
    """
    Clauses that select documents without adding to the relevance score (all must match).

    pg_search has no 'filter' list in its json boolean query: an unknown key such as "filter" is silently ignored,
    so {"boolean": {"must": [...], "filter": [project]}} would NOT select the project (tested on 0.26.0 and 0.27.0).
    A const_score clause with score 0 inside 'must' does select, and adds nothing to the score. That is what makes the
    filter form faster: the project term is not scored for every hit (top 10 of a very common word in a 156k test
    partition: 5.8 ms -> 2.9 ms), and the relevance order becomes the plain BM25 order of the query (the scored
    project term added a score that depended on the index segment). Counts and hits do not change.
    """
    query = clauses[0] if len(clauses) == 1 else {"boolean": {"must": clauses}}
    return {"const_score": {"score": 0.0, "query": query}}


def _compile_query_string(label: str, q: str, fieldset: FieldSet) -> dict:
    try:
        return query_string_to_json(q, fieldset)
    except QueryError as e:
        where = f"query {q!r}" if label == q else f"query {label!r} ({q!r})"
        raise QueryError(f"Error in {where}: {e}") from e


def _id_filter_values(spec: FilterSpec) -> list[str]:
    """The _id 'field' is the doc_id column, which can only be filtered on exact values"""
    d = spec.model_dump(exclude_none=True)
    values = d.pop("values", None)
    if "value" in d:
        values = (values or []) + [d.pop("value")]
    if d or values is None:
        raise QueryError("Filter on _id only supports values")
    return [str(v) for v in values]


def _equals_any(column: str, values: list[int]) -> tuple[sql.Composable, Any]:
    """
    `column = value` for a single value, `column = ANY(values)` otherwise. pg_search turns `= ANY(...)` into a term
    set that it evaluates over every row of the partition (about 14 ms at 13 million rows, growing with the
    partition); a plain value is a single term.
    """
    if len(values) == 1:
        return sql.SQL("{} = %s").format(sql.SQL(column)), values[0]
    return sql.SQL("{} = ANY(%s)").format(sql.SQL(column)), list(values)


def compile_search(fieldset: FieldSet, query: SearchQuery, fast: bool = False) -> CompiledSearch:
    """
    Compile a search into a json query for the BM25 index plus SQL conditions.

    fast: (read-only statements only: count, search, aggregate) if all projects are in one partition and there is no
    filter on document ids, query that partition's table directly (CompiledSearch.table). pg_search can then count
    and aggregate inside its index (Aggregate Scan), which it cannot do through the partitioned parent table.
    On the partition table the projects are selected only by the project clause of the json query (inside the first
    `must` clause, built here and never from user input, so a query string cannot escape it). A second SQL condition on
    project_pk would be evaluated by pg_search as a second term inside the index and made large counts up to 2x
    slower. Write statements (update/delete by query) must not use this: they keep both SQL conditions.
    """
    # selection: the project clause first (built here, never from user input), then the user's filters
    selection: list[dict] = [project_clause(fieldset.project_pks)]
    scored: list[dict] = []
    if query.queries:
        qs = [_compile_query_string(label, q, fieldset) for label, q in query.queries.items()]
        scored.append(qs[0] if len(qs) == 1 else {"boolean": {"should": qs}})
    # The project condition is also in the json query, but postgres needs the partitions in SQL to only scan the
    # partitions of these projects
    partitions = sorted({fieldset.partition(pk) for pk in fieldset.project_pks})
    filters = dict(query.filters or {})
    use_partition_table = fast and get_settings().fast_path and len(partitions) == 1 and "_id" not in filters and not query.ids
    project_cond, project_param = _equals_any("documents.project_pk", list(fieldset.project_pks))
    if use_partition_table:
        # every row of documents_p<N> has partition_id N, so no partition condition is needed
        sql_clauses: list[sql.Composable] = []
        params: list[Any] = []
        table: sql.Composable = sql.SQL("{} AS documents").format(sql.Identifier(partition_table(partitions[0])))
    else:
        partition_cond, partition_param = _equals_any("documents.partition_id", partitions)
        sql_clauses = [partition_cond, project_cond]
        params = [partition_param, project_param]
        table = sql.Identifier("documents")
    if "_id" in filters:
        sql_clauses.append(sql.SQL("documents.doc_id = ANY(%s)"))
        params.append(_id_filter_values(filters.pop("_id")))
    if filters:
        compiled = compile_filters(filters, fieldset)
        selection.extend(compiled.json_clauses)
        sql_clauses.extend(compiled.sql_clauses)
    if query.ids:
        sql_clauses.append(sql.SQL("documents.doc_id = ANY(%s)"))
        params.append(list(query.ids))
    if get_settings().filter_clauses:
        # the project and the filters select without scoring; only the query string is scored
        must = [unscored(selection), *scored]
    else:
        # original form: everything is a scored must clause (same hits and counts, other scores)
        must = [selection[0], *scored, *selection[1:]]
    json_query = must[0] if len(must) == 1 else {"boolean": {"must": must}}
    where = sql.SQL(" AND ").join([sql.SQL("documents.id @@@ %s::jsonb"), *sql_clauses])
    return CompiledSearch(json_query, where, [Jsonb(json_query), *params], table=table, fast=use_partition_table)


@dataclass
class SearchResult:
    total: int
    results: list[dict]
    last_id: int | None = None  # internal id of the last result, for keyset pagination


def _coalesce(fs: list[FieldInfo]) -> sql.Composable:
    exprs = [field_sql(f) for f in fs]
    return exprs[0] if len(exprs) == 1 else sql.SQL("coalesce({})").format(sql.SQL(", ").join(exprs))


def order_by(fieldset: FieldSet, sort: list[tuple[str, Literal["asc", "desc"]]] | None, scored: bool) -> sql.Composable:
    items: list[sql.Composable] = []
    for name, order in sort or []:
        direction = sql.SQL("DESC NULLS LAST" if order == "desc" else "ASC NULLS LAST")
        if name == "_score":
            items.append(sql.SQL("paradedb.score(documents.id) {}").format(direction))
        elif name == "?":
            items.append(sql.SQL("random()"))
        elif name == "_id":
            # the internal id: the order in which documents were stored. Same order as id-ordered (keyset) paging, and
            # pg_search can use it for a fast Top-K scan (the doc_id column is not in the search index)
            items.append(sql.SQL("documents.id {}").format(direction))
        else:
            if name not in fieldset.by_name:
                raise QueryError(f"Cannot sort on unknown field: {name}")
            fs = fieldset.by_name[name]
            sort_columns = {f.sort_column for f in fs}
            if len(sort_columns) == 1 and None not in sort_columns:
                # the field is in a sort slot: a real column, which pg_search can use for a fast Top-K scan
                items.append(sql.SQL("documents.{} {}").format(sql.Identifier(fs[0].sort_column), direction))  # type: ignore[arg-type]
            else:
                items.append(sql.SQL("{} {}").format(_coalesce(fs), direction))
    if not items and scored:
        items.append(sql.SQL("paradedb.score(documents.id) DESC"))
    items.append(sql.SQL("documents.id"))
    return sql.SQL(", ").join(items)


def _needs_late_fetch(fieldset: FieldSet, sort: list[tuple[str, Literal["asc", "desc"]]] | None) -> bool:
    """Whether the sort uses a json field (so all hits are read to sort them), and gives the same order twice"""
    names = [name for name, _ in sort or []]
    if "?" in names:  # random order: the second statement would give another order
        return False
    for name in names:
        if name in ("_score", "_id") or name not in fieldset.by_name:
            continue
        sort_columns = {f.sort_column for f in fieldset.by_name[name]}
        if len(sort_columns) != 1 or None in sort_columns:
            return True
    return False


# Counting in two steps (fast path, only with AMCAT4_CAPPED_COUNT=true): pg_search's in-index count (Aggregate Scan)
# had a floor of about 7-12 ms with parallel workers, while counting the hits one by one is 2-3 ms for a few hundred
# hits but grows with them. So first count at most COUNT_STEP + 1 hits one by one, and only if there are more, count
# inside the index. Without parallel workers (AMCAT4_API_PARALLEL_WORKERS=0) one full count takes 2-4 ms, so the
# default is a single count, which saves a statement (about 2.4 ms of planning) for every search with more hits.
# The result is always exact.
COUNT_STEP = 1_000


async def count_at_most(conn: AsyncConnection, c: CompiledSearch, limit: int) -> int:
    """The number of hits, counted one by one up to limit + 1 (so a result above limit means 'more than limit')"""
    cur = await conn.execute(
        sql.SQL("SELECT count(*) AS n FROM (SELECT 1 FROM {} WHERE {} LIMIT {}) capped").format(
            c.table, c.where, sql.Literal(limit + 1)
        ),
        c.params,
    )
    return (await cur.fetchone())["n"]  # type: ignore[index, call-overload]


async def count(conn: AsyncConnection, fieldset: FieldSet, query: SearchQuery) -> int:
    c = compile_search(fieldset, query, fast=True)
    if c.fast and get_settings().capped_count:
        n = await count_at_most(conn, c, COUNT_STEP)
        if n <= COUNT_STEP:
            return n
    cur = await conn.execute(sql.SQL("SELECT count(*) AS n FROM {} WHERE {}").format(c.table, c.where), c.params)
    row = await cur.fetchone()
    return row["n"]  # type: ignore[index, call-overload]


async def search(
    conn: AsyncConnection,
    fieldset: FieldSet,
    query: SearchQuery,
    fields: list[str],
    *,
    sort: list[tuple[str, Literal["asc", "desc"]]] | None = None,
    page: int = 0,
    per_page: int = 10,
    snippets: dict[str, SnippetParams] | None = None,
    highlight: bool = False,
    with_total: bool = True,
    keyset: bool = False,
    after_id: int | None = None,
    descending: bool = False,
    offset: int | None = None,
    similar: tuple[str, list[float]] | None = None,
) -> SearchResult:
    """
    Search documents, returning the given fields (and snippets for the fields in snippets).
    Access control on which fields may be returned (or only as snippets) is the responsibility of the caller.

    If keyset is True, results are ordered by internal id and only results after after_id are returned
    (efficient pagination through large result sets; sort and page are ignored). descending: keyset in reverse order.
    offset overrides page * per_page.
    similar: (vector field, vector): order by cosine distance to the vector (documents without vector are skipped)
    """
    from amcat4.postgres.documents import field_select

    c = compile_search(fieldset, query, fast=not similar)
    scored = bool(query.queries)
    snippets = snippets or {}

    columns: list[sql.Composable] = [sql.SQL("documents.doc_id"), sql.SQL("documents.project_pk")]
    # fields that do not exist (in any of the projects) are simply not returned
    fields = [name for name in fields if name in fieldset.by_name]
    snippets = {name: s for name, s in snippets.items() if name in fieldset.by_name}
    for name in dict.fromkeys([*fields, *snippets.keys()]):
        for f in fieldset.by_name[name]:
            columns.append(sql.SQL("{} AS {}").format(field_select(f), sql.Identifier(f.key)))
            if scored and f.type == "text" and (name in snippets or highlight):
                columns.append(
                    sql.SQL("paradedb.snippet_positions({}->{}) AS {}").format(
                        sql.Identifier(f.column), sql.Literal(f.key), sql.Identifier("_pos_" + f.key)
                    )
                )
    if scored:
        columns.append(sql.SQL("paradedb.score(documents.id) AS _score"))
    columns.append(sql.SQL("documents.id AS _internal_id"))

    where, params = c.where, list(c.params)
    join: sql.Composable = sql.SQL("")
    if similar:
        vfield, vector = similar
        vfs = fieldset.by_name.get(vfield)
        if not vfs or vfs[0].type != "vector":
            raise QueryError(f"{vfield} is not a vector field")
        join = sql.SQL("JOIN document_vectors sim ON sim.document_id = documents.id AND sim.field_pk = ANY({})").format(
            sql.Literal([f.pk for f in vfs])
        )
        columns.append(
            sql.SQL("1 - (sim.embedding <=> {}::public.vector) AS _similarity").format(sql.Literal(json.dumps(vector)))
        )
    if keyset:
        if after_id is not None:
            where = sql.SQL("{} AND documents.id {} %s").format(where, sql.SQL("<" if descending else ">"))
            params.append(after_id)
        ordering: sql.Composable = sql.SQL("documents.id DESC" if descending else "documents.id")
        offset = 0
    elif similar:
        ordering = sql.SQL("_similarity DESC, documents.id")
        offset = page * per_page if offset is None else offset
    else:
        ordering = order_by(fieldset, sort, scored)
        offset = page * per_page if offset is None else offset
    late_fetch = get_settings().fast_path and not keyset and not similar and _needs_late_fetch(fieldset, sort)

    async def page_rows() -> list[dict]:
        where_, params_, query_offset = where, list(params), offset
        if late_fetch:
            # Late fetch: the sort is on a json field (not a sort slot), so pg_search cannot do a Top-K scan and every
            # hit is read and sorted. Do that with only the internal id (no text, no snippets), then fetch the full
            # rows of just this page with the same statement as below, restricted to these ids (same order, columns).
            cur = await conn.execute(
                sql.SQL("SELECT documents.id FROM {} WHERE {} ORDER BY {} LIMIT %s OFFSET %s").format(
                    c.table, where_, ordering
                ),
                [*params_, per_page, offset],
            )
            page_ids = [row["id"] for row in await cur.fetchall()]  # type: ignore[index, call-overload]
            where_ = sql.SQL("{} AND documents.id = ANY(%s)").format(where_)
            params_.append(page_ids)
            query_offset = 0
        stmt = sql.SQL("SELECT {} FROM {} {} WHERE {} ORDER BY {} LIMIT %s OFFSET %s").format(
            sql.SQL(", ").join(columns), c.table, join, where_, ordering
        )
        cur = await conn.execute(stmt, [*params_, per_page, query_offset])
        return await cur.fetchall()  # type: ignore[return-value]

    counted: int | None = None
    if with_total and get_settings().parallel_count:
        # Count on a second connection while the page is fetched: the request then takes as long as the slower of the
        # two, not the sum. Without a spare connection (busy pool), the count runs after the page as before.
        async with spare_connection() as conn2:
            if conn2 is None:
                rows = await page_rows()
            else:
                # return_exceptions: wait for both, so no statement is still running when conn2 goes back to the pool
                got_rows, got_count = await asyncio.gather(page_rows(), count(conn2, fieldset, query), return_exceptions=True)
                if isinstance(got_rows, BaseException):
                    raise got_rows
                if isinstance(got_count, BaseException):
                    raise got_count
                rows, counted = got_rows, got_count
    else:
        rows = await page_rows()

    # Patterns to find query matches for highlighting / snippets (combined with the positions reported by pg_search,
    # which does not report positions for all query types)
    nodes = [parse_query(q) for q in (query.queries or {}).values()]
    default_names = {f.name for f in fieldset.default_fields()}
    patterns = {
        name: [p for node in nodes for p in highlight_patterns(node, name, name in default_names)]
        for name in dict.fromkeys([*fields, *snippets.keys()])
    }

    results = []
    for row in rows:
        project_fields = fieldset.project_fields[row["project_pk"]]  # type: ignore[index, call-overload]
        doc: dict[str, Any] = {"_id": row["doc_id"]}  # type: ignore[index, call-overload]
        for name in dict.fromkeys([*fields, *snippets.keys()]):
            f = project_fields.get(name)
            if f is None:
                continue
            value = row[f.key]  # type: ignore[index, call-overload]
            positions = None
            if isinstance(value, str) and nodes and (name in snippets or highlight):
                positions = byte_to_char_positions(value, row.get("_pos_" + f.key)) or []  # type: ignore[union-attr]
                positions = _merge_positions(positions + match_positions(value, patterns[name]))
            if name in snippets:
                value = make_snippet(value, positions, snippets[name], *(("<em>", "</em>") if highlight else ("", "")))
            elif highlight and positions:
                value = highlight_text(value, positions)
            if value is not None:
                doc[name] = value
        if similar:
            doc["_similarity"] = row["_similarity"]  # type: ignore[index, call-overload]
        results.append(doc)

    if not with_total:
        total = len(results)
    elif offset == 0 and after_id is None and not similar and len(rows) < per_page:
        # the first page is not full, so these are all the hits: no need to count
        total = len(rows)
    elif counted is not None:
        total = counted
    else:
        total = await count(conn, fieldset, query)
    last_id = rows[-1]["_internal_id"] if rows else None  # type: ignore[index, call-overload]
    return SearchResult(total=total, results=results, last_id=last_id)


def _merge_positions(positions: list[list[int]]) -> list[list[int]]:
    """Sort and merge overlapping (start, end) positions"""
    merged: list[list[int]] = []
    for start, end in sorted((p[0], p[1]) for p in positions):
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return merged
