"""
The search condition in ParadeDB's new SQL query API (pg_search "v2": the pdb.* functions and operators).

The rest of the postgres backend builds a search condition as a json query tree, the format of pg_search's legacy
json query objects (`documents.id @@@ '{"boolean": ...}'::jsonb`). ParadeDB deprecated that format. This module
renders the same tree as a SQL boolean expression made of new-API predicates on the columns and json keys:

    {"boolean": {"must": [a, b]}}        ->  (a AND b)
    {"boolean": {"should": [a, b]}}      ->  (a OR b)
    {"boolean": {"must_not": [a]}}       ->  NOT (a)
    {"match": {field, value}}            ->  documents.text_fields->>'f12' ||| value
    {"const_score": {score: 0, query}}   ->  ...::pdb.const(0) on every leaf of the query

So the query parser, the filters, the cutoff sort and the aggregations keep producing the json tree, and only the
statement that is sent changes (switch: AMCAT4_NEW_QUERY_API, see config.py). pg_search turns both forms into the same
internal query (visible as "Tantivy Query" in EXPLAIN), so hits, counts and scores are the same; the places where the
forms differ are listed with each node type below and in docs/postgres-fast-v3/NEW_QUERY_API.md.

Rules that come from pg_search 0.27.0 (tested 25 Nov 2026):
- Values are bind parameters with an explicit type (pdb.term has a variant for every type). A text value is never
  cast to pdb.query: that type reads a text that looks like json as a query.
- Ranges with whole number or date bounds use pdb.range. pdb.range(numrange) sends its bounds as text and finds
  nothing on a json number, and there is no range function for text. Ranges with decimal or text bounds are therefore
  written as a pdb.query constant (the json of the one range, without its field): see _range.
- Fuzzy: the legacy fuzzy_term counted a swap of two letters as one edit; ::pdb.fuzzy(n) counts it as two unless
  told otherwise, so the third argument (t) is always given.
- pdb.all() scores 1 for every document, the legacy {"all": null} scored 0: always pdb.all()::pdb.const(0).
- Postgres rewrites the boolean expression before pg_search sees it (it flattens nested ANDs and ORs, applies
  De Morgan's laws to NOT, removes copies and takes common parts out of an OR). The matching documents stay the same,
  the scores would not: see _Renderer.distinct and the must_not clauses in _Renderer.boolean. What is left is the order
  in which the parts of a score are added up (32-bit floats: a score can differ in its last digit).
- A json tree that cannot be rendered raises NotRenderable; the caller then sends the legacy form.
"""

import json
import logging
import math
import re
from typing import Any

from psycopg import sql
from psycopg.types.json import Jsonb

from amcat4.config import get_settings
from amcat4.postgres.fields import parse_date

# The json columns of documents that are in the search index; a field path is <column>.<key>[.<subkey>]
JSON_COLUMNS = ("text_fields", "exact_fields")
_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")
_INT8 = 2**63


class NotRenderable(Exception):
    """This json query has no (tested) form in the new query API"""


def key_sql(path: str, table: str = "documents") -> sql.Composable:
    """The SQL expression for a field path of the search index: a json key (text_fields.f12) or a column (project_pk)"""
    column, *keys = path.split(".")
    if not _IDENTIFIER.fullmatch(column):
        raise NotRenderable(f"field {path!r}")
    expr = sql.SQL("{}.{}").format(sql.Identifier(table), sql.Identifier(column))
    if not keys:
        return expr
    if column not in JSON_COLUMNS:
        raise NotRenderable(f"field {path!r}")
    for key in keys[:-1]:
        expr = sql.SQL("{}->{}").format(expr, sql.Literal(key))
    return sql.SQL("{}->>{}").format(expr, sql.Literal(keys[-1]))


def _number(value: Any) -> sql.SQL:
    """A boost factor or constant score as it is written inside ::pdb.boost(..) (a type modifier cannot be a parameter)"""
    text = format(float(value), ".9g")
    if not re.fullmatch(r"\d+(\.\d+)?", text):
        raise NotRenderable(f"score {value!r}")
    return sql.SQL(text)


class _Renderer:
    def __init__(self, table: str):
        self.table = table
        self.params: list[Any] = []
        self.scored_leaves: dict[tuple, int] = {}  # how often each scored predicate was written (see distinct)

    def param(self, value: Any, pgtype: str) -> sql.Composable:
        self.params.append(value)
        return sql.SQL("%s::" + pgtype)  # type: ignore[arg-type]

    def typed(self, value: Any) -> sql.Composable:
        """A parameter for a term value, with the SQL type that pg_search sends on as the same json type"""
        if isinstance(value, bool):
            return self.param(value, "boolean")
        if isinstance(value, int):
            if not -_INT8 <= value < _INT8:
                raise NotRenderable(f"number {value!r}")
            return self.param(value, "bigint")
        if isinstance(value, float):
            if not math.isfinite(value):
                raise NotRenderable(f"number {value!r}")  # (an error in the legacy form: not valid json)
            return self.param(value, "float8")
        if isinstance(value, str):
            return self.param(value, "text")
        raise NotRenderable(f"value {value!r}")

    def adjusted(self, rhs: sql.Composable, const: float | None, boost: float | None) -> sql.Composable:
        """The right-hand side of a predicate with its constant score or boost"""
        if const is not None:
            # (a constant score replaces the score of the clause, so a boost inside it has no effect)
            return sql.SQL("{}::pdb.const({})").format(rhs, _number(const))
        if boost is not None:
            return sql.SQL("{}::pdb.boost({})").format(rhs, _number(boost))
        return rhs

    def all(self, const: float | None) -> sql.Composable:
        # the legacy {"all": null} scores 0 for every document, pdb.all() scores 1
        return sql.SQL("{}.id @@@ pdb.all()::pdb.const({})").format(sql.Identifier(self.table), _number(const or 0))

    def render(self, node: Any, const: float | None = None, boost: float | None = None) -> sql.Composable:
        if not isinstance(node, dict) or len(node) != 1:
            raise NotRenderable(f"query {node!r}")
        ((kind, q),) = node.items()
        if kind == "all":
            return self.all(const)
        if not isinstance(q, dict):
            raise NotRenderable(f"query {node!r}")
        if kind == "boolean":
            return self.boolean(q, const, boost)
        if kind == "const_score":
            score = float(q["score"])
            if score != 0 and not _is_leaf(q["query"]):
                # (a boolean query with one constant score for every hit: cannot be given to its leaves)
                raise NotRenderable("const_score with a score on a boolean query")
            return self.render(q["query"], score if const is None else const, None)
        if kind == "boost":
            factor = float(q["factor"]) * (1.0 if boost is None else boost)
            return self.render(q["query"], const, factor)
        leaf = getattr(self, f"leaf_{kind}", None)
        if leaf is None:
            raise NotRenderable(f"query type {kind!r}")
        key = key_sql(q["field"], self.table)
        first_param = len(self.params)
        operator, rhs = leaf(q)
        rhs = self.adjusted(rhs, const, boost)
        if const != 0:
            rhs = self.distinct(q["field"], operator, rhs, self.params[first_param:])
        return sql.SQL("{} {} {}").format(key, sql.SQL(operator), rhs)

    def distinct(self, field: str, operator: str, rhs: sql.Composable, params: list[Any]) -> sql.Composable:
        """
        The right-hand side of a scored predicate, made different from earlier copies of the same predicate.

        Postgres simplifies a WHERE clause before pg_search sees it: `a OR a` becomes `a`, and `(a AND b) OR (a AND c)`
        becomes `a AND (b OR c)`. The same documents match, but `a` is then scored once, where the legacy json query
        scored it once for every place it is in (so the relevance order would change, e.g. for two queries that share
        a word). A copy inside `CASE WHEN <never true> THEN NULL ELSE ... END` with its own number is another
        expression to postgres, so nothing is merged; pg_search evaluates it when the statement starts (as it does
        for the parameters of a prepared statement), and the query it runs is the same.
        The condition uses now() because postgres does not compute that while planning; it is never true.
        Predicates with a constant score of 0 are not marked: merging them does not change a score.
        """
        signature = (field, operator, rhs.as_string(None), json.dumps(params, default=str))
        copies = self.scored_leaves.get(signature, 0)
        self.scored_leaves[signature] = copies + 1
        if not copies:
            return rhs
        return sql.SQL("CASE WHEN extract(epoch FROM now()) = {} THEN NULL ELSE {} END").format(sql.SQL(str(-copies)), rhs)  # type: ignore[arg-type]

    def boolean(self, q: dict, const: float | None, boost: float | None) -> sql.Composable:
        if set(q) - {"must", "should", "must_not"}:
            raise NotRenderable(f"boolean with {sorted(q)}")
        must, should, must_not = q.get("must") or [], q.get("should") or [], q.get("must_not") or []
        if not must and not should:
            raise NotRenderable("boolean without must or should")
        # {"all": null} in must selects every document and scores 0: nothing to add to an AND
        parts = [self.render(c, const, boost) for c in must if not _is_all(c)]
        if should and not must:
            parts.append(_join(" OR ", [self.render(c, const, boost) for c in should]))
        elif should and const is None:
            # next to a must clause, should clauses only add to the score: (should OR every document, scoring 0).
            # (Inside a constant score they do nothing at all, and are left out)
            parts.append(_join(" OR ", [*(self.render(c, const, boost) for c in should), self.all(None)]))
        # What is excluded is never scored. Said explicitly (constant score 0), because postgres can rewrite a NOT:
        # NOT (a AND NOT b) becomes (NOT a OR b), where b would get a score
        parts.extend(sql.SQL("NOT ({})").format(self.render(c, 0.0, None)) for c in must_not)
        if not parts:
            return self.all(const)
        return _join(" AND ", parts)

    # ---- leaves: (operator, right-hand side)

    def leaf_match(self, q: dict) -> tuple[str, sql.Composable]:
        """Any (or with conjunction_mode all) of the words of the value, split by the tokenizer of the field"""
        if set(q) - {"field", "value", "conjunction_mode"} or not isinstance(q["value"], str):
            raise NotRenderable(f"match with {sorted(q)}")
        return ("&&&" if q.get("conjunction_mode") else "|||"), self.param(q["value"], "text")

    def leaf_term(self, q: dict) -> tuple[str, sql.Composable]:
        value = q["value"]
        if isinstance(value, str):
            return "===", self.param(value, "text")
        return "@@@", sql.SQL("pdb.term({})").format(self.typed(value))

    def leaf_term_set(self, q: dict) -> tuple[str, sql.Composable]:
        terms = [t["value"] if isinstance(t, dict) else t for t in q["terms"]]
        types = {type(t) for t in terms}
        if len(types) != 1 or not types <= {str, int, float, bool}:
            raise NotRenderable("term_set with values of several types")
        self.typed(terms[0])  # (checks the type; its parameter is replaced by the list)
        self.params.pop()
        pgtype = {str: "text", int: "bigint", float: "float8", bool: "boolean"}[types.pop()]
        return "@@@", sql.SQL("pdb.term_set({})").format(self.param(terms, pgtype + "[]"))

    def leaf_fuzzy_term(self, q: dict) -> tuple[str, sql.Composable]:
        if set(q) - {"field", "value", "distance"}:
            raise NotRenderable(f"fuzzy_term with {sorted(q)}")
        # (distance, prefix, transposition_cost_one): t, because the legacy form counts a swap of two letters as one edit
        distance = sql.Literal(int(q.get("distance", 2)))
        return "===", sql.SQL("{}::pdb.fuzzy({}, f, t)").format(self.param(q["value"], "text"), distance)

    def leaf_phrase(self, q: dict) -> tuple[str, sql.Composable]:
        words = self.param(list(q["phrases"]), "text[]")
        slop = int(q.get("slop") or 0)
        return "###", sql.SQL("{}::pdb.slop({})").format(words, sql.Literal(slop)) if slop else words

    def leaf_phrase_prefix(self, q: dict) -> tuple[str, sql.Composable]:
        words = self.param(list(q["phrases"]), "text[]")
        if q.get("max_expansions") is None:
            return "@@@", sql.SQL("pdb.phrase_prefix({})").format(words)
        return "@@@", sql.SQL("pdb.phrase_prefix({}, {})").format(words, self.param(int(q["max_expansions"]), "integer"))

    def leaf_exists(self, q: dict) -> tuple[str, sql.Composable]:
        return "@@@", sql.SQL("pdb.exists()")

    def leaf_range(self, q: dict) -> tuple[str, sql.Composable]:
        if set(q) - {"field", "lower_bound", "upper_bound", "is_datetime"}:
            raise NotRenderable(f"range with {sorted(q)}")
        lower, upper = _bound(q.get("lower_bound")), _bound(q.get("upper_bound"))
        values = [b[0] for b in (lower, upper) if b is not None]
        if not values:
            raise NotRenderable("range without bounds")  # (an error in the legacy form too)
        if any(isinstance(v, bool) for v in values):
            raise NotRenderable("range on booleans")
        if q.get("is_datetime"):
            return "@@@", self.pg_range("tstzrange", "timestamptz", lower, upper, parse_date)
        if all(isinstance(v, int) and -_INT8 <= v < _INT8 for v in values):
            return "@@@", self.pg_range("int8range", "bigint", lower, upper, int)
        if all(isinstance(v, (int, float)) for v in values) or all(isinstance(v, str) for v in values):
            # Decimal bounds (number fields) and text bounds (keyword, url and tag fields): the new API has no function
            # that works on a json key. pdb.range(numrange(...)) sends the bounds as text ("17.5"), which finds
            # nothing on a json number, and int8range rounds the range to whole numbers; there is no range of text.
            # A pdb.query constant with the json of the range (what pdb.range itself returns, with real numbers) does
            # work: the same range as in the legacy form, but on the json key like every other predicate.
            # The bounds are data inside a json text made here, sent as a parameter.
            bounds = {"lower_bound": q.get("lower_bound"), "upper_bound": q.get("upper_bound")}
            # (allow_nan=False: with a text that is not valid json, pdb.query would read the text as a query string)
            return "@@@", self.param(json.dumps({"range": bounds}, allow_nan=False), "pdb.query")
        raise NotRenderable("range with bounds of several types")

    def pg_range(self, function: str, pgtype: str, lower, upper, comparable) -> sql.Composable:
        """pdb.range over a postgres range of these (value, inclusive) bounds (None: open)"""
        if lower is not None and upper is not None and comparable(lower[0]) > comparable(upper[0]):
            # postgres refuses a range whose lower bound is above its upper bound; the legacy form finds nothing
            return sql.SQL("pdb.empty()")
        flags = ("[" if lower is not None and lower[1] else "(") + ("]" if upper is not None and upper[1] else ")")
        return sql.SQL("pdb.range({}({}, {}, {}))").format(
            sql.SQL(function),  # type: ignore[arg-type]
            self.param(None if lower is None else lower[0], pgtype),
            self.param(None if upper is None else upper[0], pgtype),
            sql.Literal(flags),
        )


def _bound(bound: dict | None) -> tuple[Any, bool] | None:
    """A json range bound ({"included": v} or {"excluded": v}) as (value, inclusive)"""
    if bound is None:
        return None
    if not isinstance(bound, dict) or len(bound) != 1 or next(iter(bound)) not in ("included", "excluded"):
        raise NotRenderable(f"range bound {bound!r}")
    ((kind, value),) = bound.items()
    return value, kind == "included"


def _is_all(node: Any) -> bool:
    return isinstance(node, dict) and set(node) == {"all"}


def _is_leaf(node: Any) -> bool:
    return isinstance(node, dict) and not set(node) & {"boolean", "const_score", "boost"}


def _join(operator: str, parts: list[sql.Composable]) -> sql.Composable:
    if len(parts) == 1:
        return parts[0]
    return sql.SQL("({})").format(sql.SQL(operator).join(parts))  # type: ignore[arg-type]


def render(json_query: dict, table: str = "documents") -> tuple[sql.Composable, list[Any]]:
    """
    The SQL condition (and its parameters) that selects and scores the same documents as
    `<table>.id @@@ json_query::jsonb`. table: the name (alias) of the documents table in the statement.
    Raises NotRenderable for a query that has no form in the new API: send the legacy form then.
    """
    r = _Renderer(table)
    try:
        condition = r.render(json_query)
    except (KeyError, TypeError, ValueError, OverflowError) as e:
        raise NotRenderable(f"{type(e).__name__}: {e}") from e
    return condition, r.params


def search_condition(json_query: dict, table: str = "documents") -> tuple[sql.Composable, list[Any]]:
    """
    The SQL condition (and its parameters) for a json query, in the format of the settings: the new query API with
    AMCAT4_NEW_QUERY_API=true, otherwise (and for a query that the new API has no form for) the legacy json object.
    Every statement that sends a search condition gets it from here.
    """
    if get_settings().new_query_api:
        try:
            return render(json_query, table)
        except NotRenderable as e:
            logging.warning(f"Search condition has no form in the new query API ({e}), sending the legacy json query")
    return sql.SQL("{}.id @@@ %s::jsonb").format(sql.Identifier(table)), [Jsonb(json_query)]


def score_sql(table: str = "documents") -> sql.Composable:
    """The relevance score of a row: pdb.score in the new query API, paradedb.score in the legacy one (same value)"""
    function = "pdb.score" if get_settings().new_query_api else "paradedb.score"
    return sql.SQL("{}({}.id)").format(sql.SQL(function), sql.Identifier(table))  # type: ignore[arg-type]


def snippet_positions_sql(column: str, key: str) -> sql.Composable:
    """The positions of the matches of the search in the text of a json key (see amcat4.postgres.snippets)"""
    function = "pdb.snippet_positions" if get_settings().new_query_api else "paradedb.snippet_positions"
    return sql.SQL("{}({}->{})").format(sql.SQL(function), sql.Identifier(column), sql.Literal(key))  # type: ignore[arg-type]
