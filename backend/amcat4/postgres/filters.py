"""
Filters for the postgres backend.

A filter compiles to pg_search json clauses where possible (so filtering happens inside the BM25 index),
and to plain SQL conditions otherwise (only 'exists' on text fields whose documents do not all have the list of
text keys yet, see amcat4.postgres.fields.TEXT_KEYS). Date part filters (month number,
day of week) use the derived keys that are stored for every date field.
"""

from dataclasses import dataclass, field
from typing import Any

from psycopg import sql

from amcat4.models import FilterSpec
from amcat4.postgres import labels
from amcat4.postgres.fields import (
    DATE_DERIVED,
    TEXT_KEYS,
    FieldInfo,
    FieldSet,
    QueryError,
    normalize_value,
    term_value,
    use_text_keys,
)
from amcat4.postgres.querystring import range_query


def field_sql(f: FieldInfo, table: str = "documents") -> sql.Composable:
    """SQL expression for the (typed) value of a field"""
    if f.subkey:
        path = sql.Literal([f"f{f.pk}", f.subkey])
        raw = sql.SQL("{}.{}#>>{}::text[]").format(sql.Identifier(table), sql.Identifier(f.column), path)
    else:
        raw = sql.SQL("{}.{}->>{}").format(sql.Identifier(table), sql.Identifier(f.column), sql.Literal(f.key))
    if f.in_labels:
        # (the text of the json array of the values, as for a tag field in json)
        return sql.SQL("{}::text").format(labels.values_select(f))
    match f.type:
        case "number" | "integer":
            return sql.SQL("({})::numeric").format(raw)
        case "boolean":
            return sql.SQL("({})::boolean").format(raw)
        case "date":
            # timestamptz cast of a fixed-format UTC string; AT TIME ZONE gives a UTC timestamp for date parts
            return sql.SQL("(({})::timestamptz AT TIME ZONE 'UTC')").format(raw)
        case _:
            return raw


# Date intervals that are computed in SQL for aggregation (other intervals use the derived keys, see fields.py)
DATE_TRUNC = {"quarter", "hour", "minute"}


@dataclass
class CompiledFilters:
    json_clauses: list[dict] = field(default_factory=list)
    sql_clauses: list[sql.Composable] = field(default_factory=list)


def _value(f: FieldInfo, value: Any) -> Any:
    v = normalize_value(value, f.type)
    return v[0] if isinstance(v, list) else v


def compile_filters(filters: dict[str, FilterSpec], fieldset: FieldSet) -> CompiledFilters:
    """
    Compile filters. All filters must match (AND). For multi-project queries, a field can have a different
    storage key per project, so each filter is an OR over the project-specific fields.
    """
    out = CompiledFilters()
    for name, spec in filters.items():
        json_alternatives, sql_alternatives = [], []
        fs = fieldset.resolve(name)
        mixed = len({f.in_labels for f in fs}) > 1
        for f in fs:
            json_clauses, sql_clauses = _compile_filter(f, spec)
            if mixed:
                # Tags of some projects are in the label table (SQL conditions) and of others in json: the rule below
                # ("OR the json parts and the sql parts separately") then needs the project in both parts. The json
                # part lets every document of a label project through, and the SQL part decides about those.
                pk = next(pk for pk, fields in fieldset.project_fields.items() if fields.get(f.name) is f)
                in_project = sql.SQL("documents.project_pk = {}").format(sql.Literal(pk))
                if f.in_labels:
                    json_clauses = [{"term": {"field": "project_pk", "value": pk}}]
                    sql_clauses = [in_project, *sql_clauses]
                else:
                    sql_clauses = [in_project]
            json_alternatives.append(_all_of_json(json_clauses))
            sql_alternatives.append(sql.SQL(" AND ").join(sql_clauses) if sql_clauses else None)
        if any(sql_alternatives):
            # Storage keys are unique per project, so a document can only match the alternative of its own
            # project. This means we can OR the json parts and the sql parts separately.
            out.sql_clauses.append(
                sql.SQL("({})").format(sql.SQL(" OR ").join(a if a is not None else sql.SQL("TRUE") for a in sql_alternatives))
            )
        json_alternatives = [j for j in json_alternatives if j is not None]
        if json_alternatives:
            out.json_clauses.append(
                json_alternatives[0] if len(json_alternatives) == 1 else {"boolean": {"should": json_alternatives}}
            )
    return out


def _all_of_json(clauses: list[dict]) -> dict | None:
    if not clauses:
        return None
    if len(clauses) == 1:
        return clauses[0]
    return {"boolean": {"must": clauses}}


def _compile_filter(f: FieldInfo, spec: FilterSpec) -> tuple[list[dict], list[sql.Composable]]:
    d = spec.model_dump(exclude_none=True)
    clauses: list[dict] = []
    sql_clauses: list[sql.Composable] = []

    values = d.pop("values", None)
    if "value" in d:
        values = (values or []) + [d.pop("value")]
    if f.in_labels:
        # a tag field whose values are rows of doc_labels: plain SQL conditions (see amcat4.postgres.labels)
        bounds = {k: d.pop(k) for k in ("gt", "gte", "lt", "lte") if k in d}
        sql_clauses = labels.filter_clauses(f, values, bounds, d.pop("exists", None))
        if d:
            raise QueryError(f"Unknown filter type(s): {d}")
        return [], sql_clauses
    if values is not None:
        if f.type == "text":
            clauses.append({"boolean": {"should": [{"match": {"field": f.path, "value": str(v)}} for v in values]}})
        elif f.type == "date":
            clauses.append({"boolean": {"should": [range_query(f, v, v) for v in values]}})
        else:
            terms = [{"term": {"field": f.path, "value": term_value(v, f.type)}} for v in values]
            clauses.append({"boolean": {"should": terms}})

    bounds = {k: d.pop(k) for k in ("gt", "gte", "lt", "lte") if k in d}
    if bounds:
        lower = bounds.get("gte", bounds.get("gt"))
        upper = bounds.get("lte", bounds.get("lt"))
        clauses.append(range_query(f, lower, upper, "gt" not in bounds, "lt" not in bounds))

    if "exists" in d:
        exists = d.pop("exists")
        if f.column == "exact_fields":
            q = {"exists": {"field": f.path}}
            clauses.append(q if exists else {"boolean": {"must": [{"all": None}], "must_not": [q]}})
        elif use_text_keys(f):
            # pg_search 'exists' requires columnar fields, which text fields are not. The keys of the text fields of
            # a document are also a (columnar) list in exact_fields (TEXT_KEYS), so this is a term inside the index
            q = {"term": {"field": f"exact_fields.{TEXT_KEYS}", "value": f.key}}
            clauses.append(q if exists else {"boolean": {"must": [{"all": None}], "must_not": [q]}})
        else:
            # ... and until every document has that list (or with AMCAT4_TEXT_EXISTS_KEY=false), we check the jsonb
            # key in SQL, which reads the text of every hit
            cond = sql.SQL("{}.{} ? {}").format(sql.Identifier("documents"), sql.Identifier(f.column), sql.Literal(f.key))
            sql_clauses.append(cond if exists else sql.SQL("NOT ({})").format(cond))

    for part in list(d.keys()):
        if part in DATE_DERIVED:
            if f.type != "date":
                raise QueryError(f"Filter {part} requires a date field, {f.name} is {f.type}")
            clauses.append({"term": {"field": f.derived_path(part), "value": d.pop(part)}})

    if d:
        raise QueryError(f"Unknown filter type(s): {d}")
    return clauses, sql_clauses
