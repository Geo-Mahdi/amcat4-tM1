"""
Tags (and annotations) in a table of their own: doc_labels (migration 0005), behind the switch AMCAT4_LABEL_TABLES.

Why: a tag inside the json of a document means that adding or removing one tag rewrites the whole document row and
indexes all its text again. As a row of doc_labels, a tag change writes one small row and does not touch the document.

Which fields: a tag field whose fields.label_table is true (FieldInfo.in_labels). New tag fields get it when the switch
is on and every document of the project has its doc_pk (projects.labels_ready); the backfill_labels job moves the tags
of older projects (amcat4.projects.label_jobs). A field never changes back by itself, and the switch only decides
about new fields and the backfill: a field that is in the label table stays readable and writable when the switch is
turned off.

A label row belongs to a document (doc_pk, which all row versions of the document share), not to a row version. It
has its own created_seq and retired_seq, with the same rules as document rows (amcat4.postgres.versions): a label
that is in a release (created_seq <= newest release) is never deleted, only retired.

Reading:
- a filter is a plain SQL condition on documents: a lookup in doc_labels for every row that the rest of the search
  selects (see exists, also for the forms that were tried and why they are not used);
- an aggregation axis is a JOIN with doc_labels (one row per label);
- the values of the documents of a result page are read with one extra statement (page_values);
- the order of the values of a document is the order in which they were stored (label id), as in the json array.

Differences with tags in json (see docs/postgres-fast-v3/RELEASES_AND_LABELS.md): a value occurs at most once per
document; an empty list is the same as no value; a tag field in the label table cannot be used in a query string.
"""

from typing import Any, Iterable

from psycopg import AsyncConnection, sql

from amcat4.postgres.fields import FieldInfo, QueryError
from amcat4.postgres.versions import CURRENT, WriteSeq, visible

_STAGING = """
CREATE TEMP TABLE IF NOT EXISTS staging_labels (
    rn integer, doc_pk bigint, field_pk integer, vals text[] COLLATE "C", same boolean DEFAULT false
) ON COMMIT DELETE ROWS
"""


def unique_values(values: Iterable[Any]) -> list[str]:
    """The values as strings, each once, in their first order"""
    return list(dict.fromkeys(str(v) for v in values))


# ---------------------------------------------------------------- reading


def _of_document(f: FieldInfo, alias: str = "l", table: str = "documents") -> sql.Composable:
    """Condition on doc_labels (as alias): the labels of this field of the document row, current or of the release"""
    return sql.SQL("{l}.doc_pk = {t}.doc_pk AND {l}.field_pk = {pk} AND {visible}").format(
        l=sql.Identifier(alias), t=sql.Identifier(table), pk=sql.Literal(f.pk), visible=visible(f.release, alias)
    )


def exists(f: FieldInfo, condition: sql.Composable | None = None, negate: bool = False) -> sql.Composable:
    """
    SQL condition on documents: it has a label of this field (that meets the condition on l.value).

    The form is `CASE WHEN EXISTS (SELECT 1 FROM doc_labels l WHERE l.doc_pk = documents.doc_pk AND ...) THEN true
    ELSE false END`: postgres looks every row that the rest of the search selects up in doc_labels (B-tree on doc_pk,
    field_pk, value). The cost grows with the number of hits of the rest of the search, not with the size of the
    label. pg_search does no Top-K and no in-index count for such a statement: the hits are read, checked and (for a
    page) sorted by postgres, as for a sort on a json field.

    Why CASE: without it postgres turns EXISTS (or IN) into a join, and that is not safe with pg_search 0.27.0
    (measured 25 Nov 2026, 40,000 documents, a label on 20,000 of them):
    - on a partition table pg_search can run ONE such join inside its indexes (ParadeDB Join Scan). That is fast
      (count 11 ms, page 8 ms, against 22-28 ms and 8-10 ms for this form) and was correct, but two of them in one
      statement fail ("Failed to build DataFusion aggregate plan");
    - when postgres does the join itself above a Top-K scan (always on the parent table), the scan stops after LIMIT
      rows BEFORE the join: the page misses documents, without an error (10 asked, 783 match, 7 or 0 returned).
    A condition inside CASE cannot become a join, so neither can happen.
    Also tried: the list of documents with the label as an array (`doc_pk = ANY(ARRAY(SELECT ...))`), which pg_search
    evaluates inside its scan and keeps Top-K. Fine for a small label (5 ms for 400 documents), but 130-850 ms for
    20,000-66,000 entries, and the negation ("has no label") took 7-12 s for 66,000 entries.
    """
    extra = sql.SQL(" AND {}").format(condition) if condition is not None else sql.SQL("")
    return sql.SQL(
        "(CASE WHEN EXISTS (SELECT 1 FROM doc_labels l WHERE l.doc_pk = documents.doc_pk AND l.field_pk = {} AND {}{})"
        " THEN {} ELSE {} END)"
    ).format(sql.Literal(f.pk), visible(f.release, "l"), extra, sql.Literal(not negate), sql.Literal(negate))


def filter_clauses(f: FieldInfo, values: list | None, bounds: dict[str, Any], exists_: bool | None) -> list[sql.Composable]:
    """The parts of a filter on a tag field in the label table, as SQL conditions on documents (all must hold)"""
    out: list[sql.Composable] = []
    if values is not None:
        texts = unique_values(values)
        if not texts:
            out.append(sql.SQL("FALSE"))
        elif len(texts) == 1:
            out.append(exists(f, sql.SQL("l.value = {}").format(sql.Literal(texts[0]))))
        else:
            array = sql.SQL("ARRAY[{}]").format(sql.SQL(", ").join(sql.Literal(t) for t in texts))
            out.append(exists(f, sql.SQL("l.value = ANY({})").format(array)))
    if bounds:
        # (value has COLLATE "C": the byte order, in which the search index compares strings too)
        operators = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
        parts = [sql.SQL("l.value {} {}").format(sql.SQL(operators[k]), sql.Literal(str(v))) for k, v in bounds.items()]
        out.append(exists(f, sql.SQL(" AND ").join(parts)))
    if exists_ is not None:
        out.append(exists(f, negate=not exists_))
    return out


def values_select(f: FieldInfo) -> sql.Composable:
    """SQL expression (a subselect on a row of documents): the values of the field as a json array, NULL if none"""
    return sql.SQL("(SELECT jsonb_agg(l.value ORDER BY l.id) FROM doc_labels l WHERE {})").format(_of_document(f))


def sort_value(f: FieldInfo, desc: bool, byte_order: bool = True) -> sql.Composable:
    """
    SQL expression (a subselect on a row of documents): the value a tag field in the label table is sorted on while
    the cutoff sort is switched on, as cutoff_sort.tag_sort_sql for a tag field in json: the smallest label of the
    document for an ascending sort, the largest for a descending sort, NULL (sorted last) without labels.
    (doc_labels.value has COLLATE "C", the byte order; with AMCAT4_BYTE_ORDER_SORT=false the database's collation.)
    """
    return sql.SQL("(SELECT {}(l.value{}) FROM doc_labels l WHERE {})").format(
        sql.SQL("max" if desc else "min"), sql.SQL("" if byte_order else ' COLLATE "default"'), _of_document(f)
    )


def values_array(f: FieldInfo, table: str = "documents") -> sql.Composable:
    """
    SQL expression on a row of documents: the values of a tag field as text[], each value once, in stored order; NULL
    if the document has none. For a field in the label table and for a field in json (used to copy between the two).
    """
    if f.in_labels:
        return sql.SQL("(SELECT array_agg(l.value ORDER BY l.id) FROM doc_labels l WHERE {})").format(
            _of_document(f, table=table)
        )
    value = sql.SQL("{}.exact_fields->{}").format(sql.Identifier(table), sql.Literal(f.key))
    # (a single string instead of a list: a keyword field that became a tag field)
    return sql.SQL(
        """(SELECT array_agg(x.v ORDER BY x.o) FROM (
                SELECT t.v, min(t.o) AS o FROM jsonb_array_elements_text(
                    CASE jsonb_typeof({value}) WHEN 'array' THEN {value} WHEN 'string' THEN jsonb_build_array({value})
                    ELSE '[]'::jsonb END) WITH ORDINALITY AS t(v, o) GROUP BY t.v) x)"""
    ).format(value=value)


def axis_join(fs: list[FieldInfo], alias: str) -> sql.Composable:
    """JOIN for an aggregation axis over tag fields that are all in the label table: one row per label"""
    if len({f.release for f in fs}) != 1:
        raise QueryError("Cannot aggregate fields of different releases")
    pks = [f.pk for f in fs]
    field = (
        sql.SQL("{}.field_pk = {}").format(sql.Identifier(alias), sql.Literal(pks[0]))
        if len(pks) == 1
        else sql.SQL("{}.field_pk = ANY({})").format(sql.Identifier(alias), sql.Literal(pks))
    )
    return sql.SQL("JOIN doc_labels AS {l} ON {l}.doc_pk = documents.doc_pk AND {field} AND {visible}").format(
        l=sql.Identifier(alias), field=field, visible=visible(fs[0].release, alias)
    )


async def page_values(conn: AsyncConnection, fields: list[FieldInfo], doc_pks: list[int]) -> dict[tuple[int, int], list[str]]:
    """The label values of these fields for these documents: {(doc_pk, field_pk): [values in stored order]}"""
    doc_pks = [pk for pk in doc_pks if pk is not None]
    if not fields or not doc_pks:
        return {}
    cur = await conn.execute(
        sql.SQL(
            """SELECT l.doc_pk, l.field_pk, array_agg(l.value ORDER BY l.id) AS vals FROM doc_labels l
               WHERE l.doc_pk = ANY(%s) AND l.field_pk = ANY(%s) AND {} GROUP BY 1, 2"""
        ).format(visible(fields[0].release, "l")),
        [doc_pks, [f.pk for f in fields]],
    )
    return {(row["doc_pk"], row["field_pk"]): row["vals"] for row in await cur.fetchall()}  # type: ignore[index, call-overload]


# ---------------------------------------------------------------- writing
# All of these are called inside the write lock of the project and inside a transaction, with the number of the write.


def _retire_and_delete(w: WriteSeq, where: sql.Composable, staged: bool = False) -> list[sql.Composable]:
    """
    The statements that remove the current labels (doc_labels l) that match: a label that is in a release gets
    retired_seq = S, the others are really deleted (the second statement only finds what the first left current).
    staged: the condition also uses the rows of staging_labels (sl).
    """
    statements = []
    if w.versioned:
        statements.append(
            sql.SQL(
                "UPDATE doc_labels l SET retired_seq = {s} {using} WHERE {where} AND {live} AND l.created_seq <= {r}"
            ).format(
                s=sql.Literal(w.seq),
                using=sql.SQL("FROM staging_labels sl" if staged else ""),
                where=where,
                live=visible(None, "l"),
                r=sql.Literal(w.released),
            )
        )
    statements.append(
        sql.SQL("DELETE FROM doc_labels l {using} WHERE {where} AND {live}").format(
            using=sql.SQL("USING staging_labels sl" if staged else ""),
            where=where,
            live=visible(None, "l"),
        )
    )
    return statements


async def stage(conn: AsyncConnection) -> None:
    """Create (or empty) the temporary table staging_labels: one row per (document, field) whose labels are set"""
    await conn.execute(_STAGING)  # type: ignore[arg-type]
    await conn.execute("DELETE FROM staging_labels")


async def apply_staged(conn: AsyncConnection, project_pk: int, w: WriteSeq) -> None:
    """
    Make the current labels of every (doc_pk, field_pk) in staging_labels equal to its vals, in that order.
    A document whose labels are already these values in this order is not written at all. Otherwise its current
    labels of the field are removed (retired if a release has them) and the new list is stored: so the order of the
    values is always the order that was given, as in a json array.
    """
    await conn.execute(
        sql.SQL(
            """UPDATE staging_labels sl SET same = true
               WHERE sl.vals = coalesce((SELECT array_agg(l.value ORDER BY l.id) FROM doc_labels l
                                         WHERE l.doc_pk = sl.doc_pk AND l.field_pk = sl.field_pk AND {}), '{{}}')"""
        ).format(visible(None, "l"))
    )
    where = sql.SQL("NOT sl.same AND l.doc_pk = sl.doc_pk AND l.field_pk = sl.field_pk")
    for statement in _retire_and_delete(w, where, staged=True):
        await conn.execute(statement)
    await conn.execute(
        """INSERT INTO doc_labels (project_pk, doc_pk, field_pk, value, created_seq)
           SELECT %s, sl.doc_pk, sl.field_pk, v.value, %s
           FROM staging_labels sl CROSS JOIN LATERAL unnest(sl.vals) WITH ORDINALITY AS v(value, ord)
           WHERE NOT sl.same ORDER BY sl.rn, sl.doc_pk, sl.field_pk, v.ord""",
        [project_pk, w.seq],
    )
    await conn.execute("DELETE FROM staging_labels")


async def remove_of_documents(conn: AsyncConnection, doc_pks: list[int], w: WriteSeq) -> None:
    """Remove the current labels of deleted documents (kept, retired, if a release has them)"""
    if not doc_pks:
        return
    for statement in _retire_and_delete(w, sql.SQL("l.doc_pk = ANY(%s)")):
        await conn.execute(statement, [doc_pks])


async def add_by_query(
    conn: AsyncConnection, project_pk: int, f: FieldInfo, value: str, where: sql.Composable, params: list, w: WriteSeq
) -> int:
    """Give the documents that match `where` (a condition on documents) this label. Returns the number that got it"""
    cur = await conn.execute(
        sql.SQL(
            # (the matching documents first, on their own: pg_search refuses constants in the select list of a search)
            """WITH matching AS MATERIALIZED (SELECT documents.id, documents.doc_pk FROM documents WHERE {where})
               INSERT INTO doc_labels (project_pk, doc_pk, field_pk, value, created_seq)
               SELECT {project}, m.doc_pk, {field}, {value}, {s} FROM matching m ORDER BY m.id
               ON CONFLICT (doc_pk, field_pk, value) WHERE retired_seq = {current} DO NOTHING"""
        ).format(
            project=sql.Literal(project_pk),
            field=sql.Literal(f.pk),
            value=sql.Literal(value),
            s=sql.Literal(w.seq),
            where=where,
            current=sql.Literal(CURRENT),
        ),
        params,
    )
    return cur.rowcount


async def remove_by_query(
    conn: AsyncConnection, f: FieldInfo, value: str, where: sql.Composable, params: list, w: WriteSeq
) -> int:
    """Take this label from the documents that match `where`. Returns the number of documents that had it"""
    # (the matching documents first, on their own: joined directly, postgres ran the search once per label row)
    matching = sql.SQL("WITH matching AS MATERIALIZED (SELECT documents.doc_pk FROM documents WHERE {}) ").format(where)
    condition = sql.SQL("l.field_pk = {} AND l.value = {} AND l.doc_pk IN (SELECT m.doc_pk FROM matching m)").format(
        sql.Literal(f.pk), sql.Literal(value)
    )
    n = 0
    for statement in _retire_and_delete(w, condition):
        n += (await conn.execute(matching + statement, params)).rowcount
    return n


async def set_by_query(
    conn: AsyncConnection,
    project_pk: int,
    f: FieldInfo,
    values: list[str],
    where: sql.Composable,
    params: list,
    w: WriteSeq,
) -> int:
    """Set the labels of this field to these values (none: remove them) for the documents that match `where`"""
    await stage(conn)
    cur = await conn.execute(
        sql.SQL(
            "WITH matching AS MATERIALIZED (SELECT documents.doc_pk FROM documents WHERE {where}) "
            "INSERT INTO staging_labels (rn, doc_pk, field_pk, vals) "
            "SELECT 0, m.doc_pk, {field}, {values}::text[] FROM matching m"
        ).format(field=sql.Literal(f.pk), values=sql.Literal(values), where=where),
        params,
    )
    await apply_staged(conn, project_pk, w)
    return cur.rowcount


async def delete_field(conn: AsyncConnection, f: FieldInfo) -> None:
    """Remove every label of a field (only for a project without releases: the caller checks)"""
    await conn.execute("DELETE FROM doc_labels WHERE field_pk = %s", [f.pk])
