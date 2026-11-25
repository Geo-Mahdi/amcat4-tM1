"""
Document storage.

Documents are uploaded via COPY into a temporary staging table, followed by a single INSERT/UPDATE statement.
This is fast and gives us proper create / replace / update / upsert semantics.
"""

import json
import re
import uuid
from typing import TYPE_CHECKING, Any, Iterable, Literal, Sequence

from psycopg import AsyncConnection, sql
from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from amcat4.errors import ConflictError
from amcat4.postgres import labels
from amcat4.postgres.fields import (
    DATE_DERIVED,
    SLOT_TYPES,
    SORT_SLOTS,
    FieldInfo,
    FieldSet,
    derived_date_values,
    normalize_value,
)
from amcat4.postgres.layout import guard_writes, partition_of, project_filter
from amcat4.postgres.versions import (
    has_releases,
    live,
    retire_or_delete,
    version_assignments,
    versioned_update,
    write_seq,
)

if TYPE_CHECKING:
    from amcat4.postgres.search import SearchQuery

# index: create or replace; create: only create new documents; update: update existing documents (merge fields);
# upsert: update or create
# create: only create new documents (error if a document already exists)
# update: update existing documents, keeping fields that are not given (error if a document does not exist)
# upsert: update existing documents (keeping fields that are not given), or create new documents
# replace: replace existing documents completely (fields that are not given are removed), or create new documents
OpType = Literal["create", "update", "upsert", "replace"]

# The sort slot columns: the standard metadata columns date and source (see amcat4.postgres.layout), and the generic
# slots of migration 0002. Uploads fill them directly from the values of the fields that are mapped to them.
STANDARD_COLUMNS: list[str] = list(SORT_SLOTS)


class UploadError(ValueError):
    """An upload could not be completed (nothing was saved)"""


def stored_values(f: FieldInfo, value: Any) -> dict[str, Any]:
    """The json key(s) and value(s) to store for a field value (date fields also store derived values)"""
    normalized = normalize_value(value, f.type)
    out = {f.key: normalized}
    if f.type == "date":
        out.update(derived_date_values(f, normalized))
    return out


def stored_keys(f: FieldInfo) -> list[str]:
    keys = [f.key]
    if f.type == "date":
        keys += [f.derived_key(part) for part in DATE_DERIVED]
    return keys


class SplitDocument:
    """A document split into the values for the different storage columns"""

    def __init__(self, document: dict[str, Any], fields: dict[str, FieldInfo]):
        self.columns: dict[str, dict[str, Any]] = {"text_fields": {}, "exact_fields": {}, "stored_fields": {}}
        self.standard: dict[str, Any] = {c: None for c in STANDARD_COLUMNS}
        self.vectors: dict[int, list[float]] = {}
        # field pk -> values, for tag fields whose values are rows of the label table (not in the json columns)
        self.labels: dict[int, list[str]] = {}
        for name, value in document.items():
            if name == "_id":
                continue
            f = fields.get(name)
            if f is None:
                raise ValueError(f"Field '{name}' is not yet specified")
            if value is None:
                continue
            try:
                if f.column == "vector":
                    self.vectors[f.pk] = normalize_value(value, f.type)
                    continue
                if f.in_labels:
                    self.labels[f.pk] = labels.unique_values(normalize_value(value, f.type))
                    continue
                values = stored_values(f, value)
            except (ValueError, TypeError) as e:
                raise ValueError(f"Invalid value for field '{name}' ({f.type}): {e}") from e
            self.columns[f.column].update(values)
            if f.sort_column:
                self.standard[f.sort_column] = values[f.key]


def dedup_expression(unique_fields: list[FieldInfo], table: str = "documents") -> sql.Composable:
    """
    SQL expression for the deduplication hash: the md5 of the (normalized) values of the unique fields.
    Computed in SQL, so it is identical for new and existing documents.
    """
    values = sql.SQL(", ").join(
        sql.SQL("{}.{}->{}").format(sql.Identifier(table), sql.Identifier(f.column), sql.Literal(f.key))
        for f in sorted(unique_fields, key=lambda f: f.pk)
    )
    return sql.SQL("md5(jsonb_build_array({})::text)").format(values)


# Every write function of this module is wrapped in guard_writes (amcat4.postgres.layout), which the original did not
# need: while a project is being moved to a partition of its own, writes to it are refused (ConflictError), and a move
# that starts waits for the writes that are running. Uploads look up existing documents in one partition only, so a
# write during a move could create duplicates.
@guard_writes(lambda conn, project_pk, *a, **k: project_pk)
async def update_dedup_hashes(
    conn: AsyncConnection, project_pk: int, fields: dict[str, FieldInfo], ids: list[int] | None = None
):
    """
    (Re)compute the deduplication hashes of documents (all documents of the project, or the given internal ids).
    Raises UploadError if this would create duplicate documents.
    """
    unique = [f for f in fields.values() if f.unique]
    expr = dedup_expression(unique) if unique else sql.SQL("NULL")
    # only current rows: an old row version (kept for a release) has no hash, so it cannot block a new document.
    # (Not versioned: the hash is derived from the content)
    where = await project_filter(conn, project_pk, live=True)
    params: list[Any] = []
    if ids is not None:
        where = sql.SQL("{} AND documents.id = ANY(%s)").format(where)
        params.append(ids)
    try:
        async with conn.transaction():
            await conn.execute(sql.SQL("UPDATE documents SET dedup_hash = {} WHERE {}").format(expr, where), params)
    except UniqueViolation:
        names = ", ".join(f.name for f in unique)
        raise UploadError(f"This would create multiple documents with the same values for the unique fields ({names})")


def join_document(row: dict[str, Any], fields: dict[str, FieldInfo], names: Iterable[str] | None = None) -> dict:
    """Convert a stored row back into a {name: value} dict (only for the given field names)"""
    out: dict[str, Any] = {"_id": row["doc_id"]}
    for name in names if names is not None else fields.keys():
        f = fields.get(name)
        if f is None:
            continue
        if f.column == "vector":
            value = row.get(f.key)
        else:
            value = (row.get(f.column) or {}).get(f.key)
        if value is not None:
            out[name] = value
    return out


# The staging table and the column lists below name every sort slot column, so uploads fill all slots directly
# (the original named only date and source)
_STAGING = """
CREATE TEMP TABLE IF NOT EXISTS staging_documents (
    rn integer, doc_id text, new_doc_id text, dedup_hash text, target_id bigint, target_doc_id text,
    text_fields jsonb, exact_fields jsonb, stored_fields jsonb, copied_from jsonb,
    {slots}
) ON COMMIT DELETE ROWS
""".format(slots=", ".join(f"{c} {SLOT_TYPES[c]}" for c in SORT_SLOTS))  # type: ignore[index]

# created_seq: the number of the write (amcat4.postgres.versions); it has no default, so every INSERT must give it
_INSERT_COLUMNS = (
    "partition_id, project_pk, created_seq, doc_id, text_fields, exact_fields, stored_fields, copied_from, "
    f"{', '.join(STANDARD_COLUMNS)}"
)

_MERGE = (
    """text_fields = documents.text_fields || s.text_fields,
            exact_fields = documents.exact_fields || s.exact_fields,
            stored_fields = coalesce(documents.stored_fields, '{}') || coalesce(s.stored_fields, '{}'),
            """
    + ", ".join(f"{c} = coalesce(s.{c}, documents.{c})" for c in STANDARD_COLUMNS)
    + """,
            updated_at = now()"""
)

_REPLACE = (
    """text_fields = s.text_fields, exact_fields = s.exact_fields, stored_fields = s.stored_fields,
              """
    + ", ".join(f"{c} = s.{c}" for c in STANDARD_COLUMNS)
    + """,
              updated_at = now()"""
)


# Current rows of the table alias d (the literal, so postgres uses the unique index over current rows)
_LIVE = "d.retired_seq = 9223372036854775807"


async def fields_are_current(conn: AsyncConnection, project_pk: int, fields: Iterable[FieldInfo]) -> None:
    """
    Call inside the write lock, before writing tags into the json: the backfill_labels job may have moved a tag field
    to the label table after this request read the field definitions (the job holds the write lock exclusively, so
    from here on it cannot happen any more). Then the write is refused; the next try reads the new definitions.
    """
    in_json = [f.pk for f in fields if f.type == "tag" and not f.in_labels]
    if not in_json:
        return
    cur = await conn.execute("SELECT 1 FROM fields WHERE pk = ANY(%s) AND label_table LIMIT 1", [in_json])
    if await cur.fetchone() is not None:
        raise ConflictError(f"The tag fields of project {project_pk} were just moved to the label table; please try again")


def _examples(rows: Sequence[Any], key: str = "doc_id") -> str:
    ids = [str(r[key]) for r in rows[:5]]
    return ", ".join(ids) + (f" (and {len(rows) - 5} more)" if len(rows) > 5 else "")


@guard_writes(lambda conn, project_pk, *a, **k: project_pk)
async def upload_documents(
    conn: AsyncConnection,
    project_pk: int,
    documents: list[dict[str, Any]],
    fields: dict[str, FieldInfo],
    op_type: OpType = "replace",
    copied_from: dict | None = None,
) -> dict[str, int]:
    """
    Upload documents to a project, in a single transaction: either all documents are saved, or none (UploadError).

    Documents are identified by their _id, or (if the project has unique fields) by the values of the unique fields.
    New documents without _id get a random id.
    Returns {"created": n, "updated": n}
    """
    split = [SplitDocument(doc, fields) for doc in documents]
    ids = [str(doc["_id"]) if doc.get("_id") is not None else None for doc in documents]
    unique = [f for f in fields.values() if f.unique]

    async with conn.transaction():
        project = [await partition_of(conn, project_pk), project_pk]
        w = await write_seq(conn, project_pk)
        await fields_are_current(conn, project_pk, fields.values())
        await conn.execute(_STAGING)  # type: ignore[arg-type]
        async with conn.cursor().copy(
            "COPY staging_documents (rn, doc_id, new_doc_id, text_fields, exact_fields, stored_fields, copied_from, "
            f"{', '.join(STANDARD_COLUMNS)}) FROM STDIN"  # type: ignore[arg-type]
        ) as copy:
            for rn, (doc_id, d) in enumerate(zip(ids, split)):
                extra = d.columns["stored_fields"]
                await copy.write_row(
                    [
                        rn,
                        doc_id,
                        uuid.uuid4().hex,
                        Jsonb(d.columns["text_fields"]),
                        Jsonb(d.columns["exact_fields"]),
                        Jsonb(extra) if extra else None,
                        Jsonb(copied_from) if copied_from else None,
                        *(d.standard[c] for c in STANDARD_COLUMNS),
                    ]
                )
        if unique:
            await conn.execute(
                sql.SQL("UPDATE staging_documents SET dedup_hash = {}").format(dedup_expression(unique, "staging_documents"))
            )
        # The staging table is a fresh temporary table, so postgres has no statistics for it and guesses its size.
        # With a wrong guess the duplicate lookups below can pick a plan that walks the whole project (the "unstable
        # upload" of the night run: 626 of 632 s in one statement). Analysing a few thousand rows takes milliseconds.
        await conn.execute("ANALYZE staging_documents")

        # Duplicates within the upload
        for key in ["doc_id", "dedup_hash"]:
            cur = await conn.execute(
                f"SELECT {key} FROM staging_documents WHERE {key} IS NOT NULL GROUP BY {key} HAVING count(*) > 1"  # type: ignore[arg-type]
            )
            if await cur.fetchall():
                what = "_id" if key == "doc_id" else "values for the unique fields"
                raise UploadError(f"The upload contains multiple documents with the same {what}")

        # Find existing documents, by id or by the values of the unique fields. Only current rows (_LIVE): an old
        # version or a deleted document that a release keeps is not "existing"
        await conn.execute(
            f"""UPDATE staging_documents s SET target_id = d.id, target_doc_id = d.doc_id FROM documents d
               WHERE d.partition_id = %s AND d.project_pk = %s AND d.doc_id = s.doc_id AND {_LIVE}""",  # type: ignore[arg-type]
            project,
        )
        if unique:
            cur = await conn.execute(
                f"""SELECT s.doc_id FROM staging_documents s JOIN documents d
                   ON d.partition_id = %s AND d.project_pk = %s AND d.dedup_hash = s.dedup_hash
                      AND d.id <> coalesce(s.target_id, -1) AND {_LIVE}
                   WHERE s.doc_id IS NOT NULL""",  # type: ignore[arg-type]
                project,
            )
            if conflicts := await cur.fetchall():
                raise UploadError(
                    f"Documents {_examples(conflicts)} have the same unique field values as other existing documents"
                )
            await conn.execute(
                f"""UPDATE staging_documents s SET target_id = d.id, target_doc_id = d.doc_id FROM documents d
                   WHERE d.partition_id = %s AND d.project_pk = %s AND d.dedup_hash = s.dedup_hash
                     AND s.target_id IS NULL AND {_LIVE}""",  # type: ignore[arg-type]
                project,
            )

        if op_type == "create":
            cur = await conn.execute("SELECT target_doc_id AS doc_id FROM staging_documents WHERE target_id IS NOT NULL")
            if existing := await cur.fetchall():
                raise UploadError(f"Documents already exist: {_examples(existing)}")
        if op_type == "update":
            cur = await conn.execute("SELECT rn, doc_id FROM staging_documents WHERE target_id IS NULL ORDER BY rn")
            if missing := await cur.fetchall():
                raise UploadError(f"Documents do not exist: {_examples(missing, 'doc_id' if missing[0]['doc_id'] else 'rn')}")  # type: ignore[index, call-overload]

        columns = f"text_fields, exact_fields, stored_fields, copied_from, {', '.join(STANDARD_COLUMNS)}"
        cur = await conn.execute(
            f"""INSERT INTO documents ({_INSERT_COLUMNS}, dedup_hash)
                SELECT %s, %s, %s, coalesce(doc_id, new_doc_id), {columns}, dedup_hash
                FROM staging_documents WHERE target_id IS NULL ORDER BY rn
                RETURNING id, doc_id""",  # type: ignore[arg-type]
            [*project, w.seq],
        )
        created = await cur.fetchall()
        assignments = _MERGE if op_type in ("update", "upsert") else _REPLACE
        # (through versioned_update: the old content of a document that is in a release is kept as an extra row)
        update = sql.SQL(
            """UPDATE documents SET {assignments}{version} FROM staging_documents s
               WHERE documents.partition_id = %s AND documents.project_pk = %s AND documents.id = s.target_id
                 AND {live}"""
        ).format(assignments=sql.SQL(assignments), version=version_assignments(w), live=live())  # type: ignore[arg-type]
        cur = await conn.execute(versioned_update(update, w), project)
        updated = [row["id"] for row in await cur.fetchall()]  # type: ignore[index, call-overload]
        if unique and updated and op_type in ("update", "upsert"):
            # merging can change the values of unique fields
            await update_dedup_hashes(conn, project_pk, fields, updated)

        # Vectors
        if any(f.column == "vector" for f in fields.values()):
            cur = await conn.execute(
                f"""SELECT s.rn, d.id FROM staging_documents s JOIN documents d
                   ON d.partition_id = %s AND d.project_pk = %s AND {_LIVE}
                      AND d.doc_id = coalesce(s.target_doc_id, s.doc_id, s.new_doc_id)""",  # type: ignore[arg-type]
                project,
            )
            internal = {row["rn"]: row["id"] for row in await cur.fetchall()}  # type: ignore[index, call-overload]
            if op_type == "replace" and updated:
                await conn.execute("DELETE FROM document_vectors WHERE document_id = ANY(%s)", [updated])
            vector_rows = [(internal[rn], field_pk, v) for rn, d in enumerate(split) for field_pk, v in d.vectors.items()]
            if vector_rows:
                await store_vectors(conn, project_pk, vector_rows)

        # Tags in the label table: one staged row per (document, field) whose values this upload sets. A replace
        # sets every field (a field that is not given loses its values), the other operations only the given fields
        label_fields = [f for f in fields.values() if f.in_labels]
        if label_fields:
            await labels.stage(conn)
            async with conn.cursor().copy("COPY staging_labels (rn, field_pk, vals) FROM STDIN") as copy:
                for rn, d in enumerate(split):
                    for f in label_fields:
                        if f.pk in d.labels or op_type == "replace":
                            await copy.write_row([rn, f.pk, d.labels.get(f.pk, [])])
            await conn.execute(
                f"""UPDATE staging_labels sl SET doc_pk = coalesce(d.doc_pk, d.id)
                    FROM staging_documents s JOIN documents d
                      ON d.partition_id = %s AND d.project_pk = %s AND {_LIVE}
                         AND d.doc_id = coalesce(s.target_doc_id, s.doc_id, s.new_doc_id)
                    WHERE s.rn = sl.rn""",  # type: ignore[arg-type]
                project,
            )
            await labels.apply_staged(conn, project_pk, w)

    return {"created": len(created), "updated": len(updated)}


async def store_vectors(conn: AsyncConnection, project_pk: int, rows: list[tuple[int, int, list[float]]]) -> None:
    """Store vectors (document id, field pk, vector) of documents in the project, creating a vector index for new fields"""
    dims: dict[int, int] = {}
    for _, field_pk, vector in rows:
        if dims.setdefault(field_pk, len(vector)) != len(vector):
            raise ValueError("All vectors of a field must have the same number of dimensions")
    for field_pk, n in dims.items():
        await ensure_vector_index(conn, field_pk, n)
    partition_id = await partition_of(conn, project_pk)
    async with conn.cursor() as cur:
        await cur.executemany(
            """INSERT INTO document_vectors (document_id, partition_id, field_pk, embedding)
               VALUES (%s, %s, %s, %s::text::public.vector)
               ON CONFLICT (field_pk, document_id) DO UPDATE SET embedding = EXCLUDED.embedding""",
            [(doc, partition_id, field, json.dumps(vector)) for doc, field, vector in rows],
        )


async def ensure_vector_index(conn: AsyncConnection, field_pk: int, dims: int) -> None:
    """
    Create an HNSW index (cosine distance) for the vectors of this field. pgvector indexes need a fixed number of
    dimensions, so this is a partial expression index per field. Once it exists, inserting a vector with a
    different number of dimensions for this field fails.
    """
    await conn.execute(
        sql.SQL(
            "CREATE INDEX IF NOT EXISTS {} ON document_vectors USING hnsw ((embedding::public.vector({})) "
            "public.vector_cosine_ops) WHERE field_pk = {}"
        ).format(sql.Identifier(f"document_vectors_f{field_pk}"), sql.Literal(dims), sql.Literal(field_pk))
    )


@guard_writes(lambda conn, project_pk, *a, **k: project_pk)
async def delete_field_values(conn: AsyncConnection, project_pk: int, f: FieldInfo) -> None:
    """Remove the values of a field from all documents of the project"""
    await _refuse_with_releases(conn, project_pk, "delete a field")
    if f.column == "vector":
        await conn.execute("DELETE FROM document_vectors WHERE field_pk = %s", [f.pk])
        await conn.execute(sql.SQL("DROP INDEX IF EXISTS {}").format(sql.Identifier(f"document_vectors_f{f.pk}")))
        return
    if f.in_labels:
        await labels.delete_field(conn, f)
        return
    column = sql.Identifier(f.column)
    sort = sql.SQL(", {} = NULL").format(sql.Identifier(f.sort_column)) if f.sort_column else sql.SQL("")
    await conn.execute(
        sql.SQL(
            "UPDATE documents SET {col} = {col} - %s::text[]{sort}, updated_at = now() WHERE {project} AND {col} ? %s"
        ).format(col=column, sort=sort, project=await project_filter(conn, project_pk)),
        [stored_keys(f), f.key],
    )


async def project_size_at_most(conn: AsyncConnection, project_pk: int, n: int) -> bool:
    """
    Whether the project has at most n documents (reads at most n + 1 rows, so it is cheap also for a large project,
    where count(*) of the whole project is not). Used to decide between filling a sort slot in the request or a job.
    """
    cur = await conn.execute(
        sql.SQL("SELECT count(*) AS n FROM (SELECT 1 FROM documents WHERE {} LIMIT %s) x").format(
            await project_filter(conn, project_pk, live=True)
        ),
        [n + 1],
    )
    return (await cur.fetchone())["n"] <= n  # type: ignore[index]


async def _refuse_with_releases(conn: AsyncConnection, project_pk: int, what: str) -> None:
    """
    Deleting a field or changing its type rewrites its values in every document, and the field definitions are not
    versioned: a release could no longer show the field as it was. So these are refused for a project with releases.
    """
    if await has_releases(conn, project_pk):
        raise ConflictError(f"Cannot {what}: this project has releases, which must stay as they are")


def _sort_slot_lock(project_pk: int, slot: str) -> list[int]:
    """
    The keys of the advisory lock of a sort slot of a project: a fill and a clear of the same slot (in the request or
    in the fill_sort_slot job) must not run at the same time, or the slot could end up with values of the wrong field.
    """
    # (two-key advisory locks do not overlap with the single-key locks of layout.py and the migrations. The second key
    # is the position of the slot in SORT_SLOTS, 0 to 6, so it does not meet the write lock (project_pk, 100) of
    # layout.py either)
    return [project_pk, SORT_SLOTS.index(slot)]  # type: ignore[arg-type]


async def try_lock_sort_slot(conn: AsyncConnection, project_pk: int, slot: str) -> bool:
    """Lock a sort slot of a project for filling it, until the end of the transaction (False if it is locked)"""
    cur = await conn.execute("SELECT pg_try_advisory_xact_lock(%s, %s) AS locked", _sort_slot_lock(project_pk, slot))
    return (await cur.fetchone())["locked"]  # type: ignore[index]


async def lock_sort_slot(conn: AsyncConnection, project_pk: int, slot: str, unlock: bool = False) -> None:
    """Lock (or unlock) a sort slot of a project for the session, waiting for others (for the fill job)"""
    function = "pg_advisory_unlock" if unlock else "pg_advisory_lock"
    await conn.execute(f"SELECT {function}(%s, %s)", _sort_slot_lock(project_pk, slot))  # type: ignore[arg-type]


async def fill_sort_slot(
    conn: AsyncConnection,
    project_pk: int,
    slot: str,
    f: FieldInfo | None,
    after_id: int | None = None,
    batch_size: int | None = None,
) -> int | None:
    """
    Set the sort slot column to the value of field f (or NULL if f is None) for the documents of the project. Rows that
    already have the right value are not written (an update rewrites the row and indexes it again), so filling a slot
    for a field that few documents have is cheap, and filling it again (e.g. after an interruption) only reads the rows.
    With batch_size: only the next batch_size documents after after_id (in id order); returns the last id of the
    batch, or None if there were no more documents. Without: all documents (returns None).
    """
    return await fill_sort_slots(conn, project_pk, {slot: f}, after_id=after_id, batch_size=batch_size)


@guard_writes(lambda conn, project_pk, *a, **k: project_pk)
async def fill_sort_slots(
    conn: AsyncConnection,
    project_pk: int,
    slots: dict[str, FieldInfo | None],
    after_id: int | None = None,
    batch_size: int | None = None,
) -> int | None:
    """
    fill_sort_slot for several slots at once (slot -> field, or None to clear it): one UPDATE sets all the slot
    columns, so each row is rewritten (and indexed again by pg_search) once, however many slots are filled. A row is
    only written if at least one of its slot columns changes. Unlike the original, which filled one slot per statement
    (it had only the date and source slots, filled in the request).
    """

    def value(slot: str, f: FieldInfo | None) -> sql.Composable:
        if f is None:
            return sql.SQL("NULL::{}").format(sql.SQL(SLOT_TYPES[slot]))  # type: ignore[index]
        return sql.SQL("({}->>{})::{}").format(sql.Identifier(f.column), sql.Literal(f.key), sql.SQL(SLOT_TYPES[slot]))  # type: ignore[index]

    values = {slot: value(slot, f) for slot, f in slots.items()}
    where = await project_filter(conn, project_pk)
    params: list[Any] = []
    last_id = None
    if batch_size is not None:
        batch = sql.SQL("SELECT id FROM documents WHERE {} AND id > %s ORDER BY id LIMIT %s").format(where)
        cur = await conn.execute(sql.SQL("SELECT max(id) AS last FROM ({}) b").format(batch), [after_id or 0, batch_size])
        last_id = (await cur.fetchone())["last"]  # type: ignore[index]
        if last_id is None:
            return None
        where = sql.SQL("{} AND documents.id > %s AND documents.id <= %s").format(where)
        params = [after_id or 0, last_id]
    assignments = sql.SQL(", ").join(sql.SQL("{} = {}").format(sql.Identifier(slot), v) for slot, v in values.items())
    changed = sql.SQL(" OR ").join(
        sql.SQL("documents.{} IS DISTINCT FROM {}").format(sql.Identifier(slot), v) for slot, v in values.items()
    )
    await conn.execute(
        sql.SQL("UPDATE documents SET {assignments} WHERE {where} AND ({changed})").format(
            assignments=assignments, where=where, changed=changed
        ),
        params,
    )
    return last_id


def vector_select(f: FieldInfo) -> sql.Composable:
    """SQL expression to select the vector of a document for this field (as a json array)"""
    return sql.SQL(
        "(SELECT embedding::text::jsonb FROM document_vectors v WHERE v.document_id = documents.id AND v.field_pk = {})"
    ).format(sql.Literal(f.pk))


def field_select(f: FieldInfo) -> sql.Composable:
    """SQL expression to select the value of a field"""
    if f.column == "vector":
        return vector_select(f)
    if f.in_labels:
        return labels.values_select(f)
    return sql.SQL("documents.{}->{}").format(sql.Identifier(f.column), sql.Literal(f.key))


async def get_document(
    conn: AsyncConnection, project_pk: int, doc_id: str, fields: dict[str, FieldInfo], names: list[str] | None = None
) -> dict | None:
    names = list(fields.keys()) if names is None else [n for n in names if n in fields]
    columns = [sql.SQL("documents.doc_id")] + [
        sql.SQL("{} AS {}").format(field_select(fields[n]), sql.Identifier(fields[n].key)) for n in names
    ]
    cur = await conn.execute(
        sql.SQL("SELECT {} FROM documents WHERE {} AND doc_id = %s").format(
            sql.SQL(", ").join(columns), await project_filter(conn, project_pk, live=True)
        ),
        [doc_id],
    )
    row = await cur.fetchone()
    if row is None:
        return None
    out: dict[str, Any] = {"_id": row["doc_id"]}  # type: ignore[index, call-overload]
    for n in names:
        value = row[fields[n].key]  # type: ignore[index, call-overload]
        if value is not None:
            out[n] = value
    return out


@guard_writes(lambda conn, project_pk, *a, **k: project_pk)
async def delete_document(conn: AsyncConnection, project_pk: int, doc_id: str) -> bool:
    w = await write_seq(conn, project_pk)
    where = sql.SQL("{} AND documents.doc_id = %s").format(await project_filter(conn, project_pk))
    return bool(await retire_or_delete(conn, where, [doc_id], w))


@guard_writes(lambda conn, from_project_pk, to_project_pk, *a, **k: to_project_pk)
async def copy_batch(
    conn: AsyncConnection,
    from_project_pk: int,
    to_project_pk: int,
    field_map: dict[FieldInfo, FieldInfo],
    dest_fields: dict[str, FieldInfo],
    ids: list[int],
) -> int:
    """
    Physically copy documents (given by internal id) to another project, keeping only the fields in field_map
    (source field -> destination field). Provenance is recorded in the copied_from column. Documents that were copied
    before (same id in the destination) are updated. Used by the copy job (amcat4.projects.jobs).
    """
    for s, d in field_map.items():
        if s.column != d.column:
            raise ValueError(f"Cannot copy {s.name} ({s.type}) to {d.name} ({d.type})")

    def remap(column: str) -> str:
        # (tag fields in the label table are not in the json: see the labels below)
        pairs = [
            (s_key, d_key)
            for s, d in field_map.items()
            if s.column == column and not s.in_labels and not d.in_labels
            for s_key, d_key in zip(stored_keys(s), stored_keys(d))
        ]
        args = [f"'{d_key}', {column}->'{s_key}'" for s_key, d_key in pairs]
        # from the label table of the source to a json tag field of the destination
        args += [
            f"'{d.key}', to_jsonb({labels.values_array(s).as_string(conn)})"
            for s, d in field_map.items()
            if s.column == column and s.in_labels and not d.in_labels
        ]
        if not args:
            return "'{}'::jsonb"
        return f"jsonb_strip_nulls(jsonb_build_object({', '.join(args)}))"

    def sort_value(slot_column: str) -> str:
        # The value for a slot column of the destination: from the source field's slot column if that is filled and
        # of the same SQL type (e.g. source -> sort_kw1), else from its json value. Not from a slot that is still being
        # filled: some documents would get NULL
        source = [s for s, d in field_map.items() if d.sort_column == slot_column]
        if not source:
            return "NULL"
        s = source[0]
        cast = SLOT_TYPES[slot_column]  # type: ignore[index]
        if s.sort_read_column and SLOT_TYPES[s.sort_read_column] == cast:  # type: ignore[index]
            return s.sort_read_column
        return f"({s.column}->>'{s.key}')::{cast}"

    merge = re.sub(r"\bs\.", "EXCLUDED.", _MERGE)
    from_project = [await partition_of(conn, from_project_pk), from_project_pk]
    to_project = [await partition_of(conn, to_project_pk), to_project_pk]
    async with conn.transaction():
        w = await write_seq(conn, to_project_pk)
        # ON CONFLICT names the unique index over current rows (documents_doc_id_live). A document of the destination
        # that is in a release keeps its old content as an extra row (versioned_update)
        stmt = sql.SQL(
            f"""INSERT INTO documents ({_INSERT_COLUMNS})
               SELECT %s, %s, %s, doc_id, {remap("text_fields")}, {remap("exact_fields")}, {remap("stored_fields")},
                      jsonb_build_object('project_pk', project_pk, 'doc_id', doc_id),
                      {", ".join(sort_value(c) for c in STANDARD_COLUMNS)}
               FROM documents WHERE partition_id = %s AND project_pk = %s AND id = ANY(%s)
               ON CONFLICT (partition_id, project_pk, doc_id) WHERE retired_seq = 9223372036854775807
               DO UPDATE SET {merge}{version_assignments(w).as_string(conn)}"""  # type: ignore[arg-type]
        )
        cur = await conn.execute(versioned_update(stmt, w), [*to_project, w.seq, *from_project, ids])
        copied = [row["id"] for row in await cur.fetchall()]  # type: ignore[index, call-overload]
        if any(f.unique for f in dest_fields.values()):
            await update_dedup_hashes(conn, to_project_pk, dest_fields, copied)
        for s, d in field_map.items():
            if not d.in_labels:
                continue
            # to a tag field in the label table (from json or from labels): as an upload that sets the field for the
            # documents that have values in the source (a document without values keeps what it has, as _MERGE does)
            await labels.stage(conn)
            await conn.execute(
                sql.SQL(
                    """INSERT INTO staging_labels (rn, doc_pk, field_pk, vals)
                       SELECT 0, coalesce(new.doc_pk, new.id), {field}, src.vals
                       FROM documents old
                       JOIN documents new ON new.partition_id = %s AND new.project_pk = %s AND new.doc_id = old.doc_id
                                             AND {live}
                       CROSS JOIN LATERAL (SELECT {values} AS vals) src
                       WHERE old.partition_id = %s AND old.project_pk = %s AND old.id = ANY(%s) AND src.vals IS NOT NULL"""
                ).format(field=sql.Literal(d.pk), live=live("new"), values=labels.values_array(s, "old")),
                [*to_project, *from_project, ids],
            )
            await labels.apply_staged(conn, to_project_pk, w)
        for s, d in field_map.items():
            if s.column != "vector":
                continue
            await conn.execute(
                """INSERT INTO document_vectors (document_id, partition_id, field_pk, embedding)
                   SELECT new.id, new.partition_id, %s, v.embedding FROM document_vectors v
                   JOIN documents old ON old.partition_id = %s AND old.project_pk = %s AND old.id = v.document_id
                   JOIN documents new ON new.partition_id = %s AND new.project_pk = %s AND new.doc_id = old.doc_id
                   WHERE v.field_pk = %s AND old.id = ANY(%s)
                   ON CONFLICT (field_pk, document_id) DO UPDATE SET embedding = EXCLUDED.embedding""",
                [d.pk, *from_project, *to_project, s.pk, ids],
            )
    return len(copied)


def _project_of_field(fieldset: FieldSet, f: FieldInfo) -> int:
    for pk, fields in fieldset.project_fields.items():
        if fields.get(f.name) is f:
            return pk
    raise ValueError(f"Field {f.name} not found")


def _project_condition(fieldset: FieldSet, f: FieldInfo) -> sql.Composable:
    """SQL condition for the documents of the project of this field"""
    pk = _project_of_field(fieldset, f)
    return sql.SQL("documents.partition_id = {} AND documents.project_pk = {}").format(
        sql.Literal(fieldset.partition(pk)), sql.Literal(pk)
    )


@guard_writes(lambda conn, fieldset, *a, **k: fieldset.project_pks)
async def update_tag_by_query(
    conn: AsyncConnection,
    fieldset: FieldSet,
    query: "SearchQuery",
    field: str,
    tag: str,
    action: Literal["add", "remove"],
) -> int:
    """Add or remove a tag on all documents matching the query. Returns the number of changed documents."""
    from amcat4.postgres.search import compile_search

    c = compile_search(fieldset, query)
    updated = 0
    for f in fieldset.by_name[field]:
        if f.type != "tag":
            raise ValueError(f"Field {field} is not a tag field")
        if f.in_labels:
            # the tag is a row of the label table: no document row is written (and so nothing is indexed again)
            pk = _project_of_field(fieldset, f)
            where = sql.SQL("{} AND {}").format(c.where, _project_condition(fieldset, f))
            async with conn.transaction():
                w = await write_seq(conn, pk)
                if action == "add":
                    updated += await labels.add_by_query(conn, pk, f, str(tag), where, c.params, w)
                else:
                    updated += await labels.remove_by_query(conn, f, str(tag), where, c.params, w)
            continue
        key = sql.Literal(f.key)
        # existing values can be a single string (e.g. after changing a keyword field to a tag field)
        current = sql.SQL(
            "(CASE jsonb_typeof(exact_fields->{key}) WHEN 'array' THEN exact_fields->{key} "
            "WHEN 'string' THEN jsonb_build_array(exact_fields->{key}) ELSE '[]'::jsonb END)"
        ).format(key=key)
        if action == "add":
            stmt = sql.SQL(
                """UPDATE documents SET exact_fields = jsonb_set(exact_fields, ARRAY[{key}], {current} || to_jsonb(%s::text)),
                   updated_at = now(){version}
                   WHERE {where} AND {project} AND NOT ({current} ? %s)"""
            )
            params = [tag, *c.params, tag]
        else:
            stmt = sql.SQL(
                """UPDATE documents SET exact_fields = CASE WHEN {current} - %s = '[]'::jsonb THEN exact_fields - {key}
                   ELSE jsonb_set(exact_fields, ARRAY[{key}], {current} - %s) END, updated_at = now(){version}
                   WHERE {where} AND {project} AND {current} ? %s"""
            )
            params = [tag, tag, *c.params, tag]
        project = _project_condition(fieldset, f)
        await fields_are_current(conn, _project_of_field(fieldset, f), [f])
        w = await write_seq(conn, _project_of_field(fieldset, f))
        update = stmt.format(key=key, current=current, where=c.where, project=project, version=version_assignments(w))
        cur = await conn.execute(versioned_update(update, w), params)
        updated += cur.rowcount
    return updated


@guard_writes(lambda conn, fieldset, *a, **k: fieldset.project_pks)
async def update_by_query(conn: AsyncConnection, fieldset: FieldSet, query: "SearchQuery", field: str, value: Any) -> int:
    """Set (or with value=None: remove) a field on all documents matching the query. Returns the number updated."""
    from amcat4.postgres.search import compile_search

    c = compile_search(fieldset, query)
    updated = 0
    for f in fieldset.by_name[field]:
        if f.column == "vector":
            raise ValueError("Cannot update vector fields by query")
        if f.in_labels:
            pk = _project_of_field(fieldset, f)
            where = sql.SQL("{} AND {}").format(c.where, _project_condition(fieldset, f))
            values = [] if value is None else labels.unique_values(normalize_value(value, f.type))
            async with conn.transaction():
                w = await write_seq(conn, pk)
                updated += await labels.set_by_query(conn, pk, f, values, where, c.params, w)
            continue
        column = sql.Identifier(f.column)
        sort = sql.SQL(", {} = %s").format(sql.Identifier(f.sort_column)) if f.sort_column else sql.SQL("")
        project = _project_condition(fieldset, f)
        if value is None:
            stmt = sql.SQL(
                "UPDATE documents SET {col} = {col} - %s::text[]{sort}, updated_at = now(){version} "
                "WHERE {where} AND {project}"
            )
            params: list[Any] = [stored_keys(f)]
            if f.sort_column:
                params.append(None)
        else:
            stmt = sql.SQL(
                "UPDATE documents SET {col} = coalesce({col}, '{{}}') || %s{sort}, updated_at = now(){version} "
                "WHERE {where} AND {project}"
            )
            values = stored_values(f, value)
            params = [Jsonb(values)]
            if f.sort_column:
                params.append(values[f.key])
        pk = _project_of_field(fieldset, f)
        async with conn.transaction():
            w = await write_seq(conn, pk)
            await fields_are_current(conn, pk, [f])
            update = stmt.format(col=column, sort=sort, where=c.where, project=project, version=version_assignments(w))
            cur = await conn.execute(versioned_update(update, w), [*params, *c.params])
            updated += cur.rowcount
            if f.unique:
                ids = [row["id"] for row in await cur.fetchall()]  # type: ignore[index, call-overload]
                await update_dedup_hashes(conn, pk, fieldset.project_fields[pk], ids)
    return updated


@guard_writes(lambda conn, fieldset, *a, **k: fieldset.project_pks)
async def delete_by_query(conn: AsyncConnection, fieldset: FieldSet, query: "SearchQuery") -> int:
    from amcat4.postgres.search import compile_search

    c = compile_search(fieldset, query)
    deleted = 0
    for pk in fieldset.project_pks:
        # per project: each has its own releases (and so its own rule for what is kept)
        w = await write_seq(conn, pk)
        where = sql.SQL("{} AND documents.project_pk = {}").format(c.where, sql.Literal(pk))
        deleted += len(await retire_or_delete(conn, where, c.params, w))
    return deleted


def convert_value(value: Any, old_type: str, new_type: str) -> Any:
    """Convert a stored value to another field type (raises ValueError if that is not possible)"""
    if old_type == "tag" and new_type != "tag":
        values = value if isinstance(value, list) else [value]
        if len(values) != 1:
            raise ValueError(f"cannot convert multiple tags {values!r} to a single {new_type} value")
        value = values[0]
    if new_type in ("text", "keyword", "url", "image", "video", "audio") and isinstance(value, (dict, list)):
        value = json.dumps(value)
    if new_type in ("integer",) and isinstance(value, float) and not value.is_integer():
        raise ValueError(f"{value} is not a whole number")
    return normalize_value(value, new_type)


@guard_writes(lambda conn, project_pk, *a, **k: project_pk)
async def convert_field(conn: AsyncConnection, project_pk: int, old: FieldInfo, new: FieldInfo, batch_size: int = 5000):
    """
    Convert the stored values of a field to a new type (old and new have the same pk, but a different type).
    This moves values between storage columns if needed. Raises ValueError (and changes nothing, if called within a
    transaction) if a value cannot be converted.
    """
    await _refuse_with_releases(conn, project_pk, "change the type of a field")
    if old.in_labels:
        raise ValueError(f"Cannot change the type of tag field {old.name}: its values are in the label table")
    project = await project_filter(conn, project_pk)
    if (old.column == "vector") != (new.column == "vector"):
        cur = await conn.execute(
            sql.SQL(
                "SELECT EXISTS (SELECT 1 FROM documents WHERE {} "
                "AND (text_fields ? %s OR exact_fields ? %s OR stored_fields ? %s)) AS e"
            ).format(project),
            [old.key, old.key, old.key],
        )
        row = await cur.fetchone()
        if old.column == "vector" or row["e"]:  # type: ignore[index, call-overload]
            raise ValueError(f"Cannot convert between vector and {new.type if old.column == 'vector' else old.type} fields")
        return
    if old.column == "vector":
        return
    await conn.execute("CREATE TEMP TABLE IF NOT EXISTS converted (id bigint, vals jsonb) ON COMMIT DROP")
    last_id = 0
    old_col, new_col = sql.Identifier(old.column), sql.Identifier(new.column)
    while True:
        cur = await conn.execute(
            sql.SQL(
                "SELECT id, doc_id, {col}->{key} AS value FROM documents "
                "WHERE {project} AND id > %s AND {col} ? {key} ORDER BY id LIMIT %s"
            ).format(col=old_col, key=sql.Literal(old.key), project=project),
            [last_id, batch_size],
        )
        rows = await cur.fetchall()
        if not rows:
            break
        await conn.execute("TRUNCATE converted")
        async with conn.cursor().copy("COPY converted (id, vals) FROM STDIN") as copy:
            for row in rows:
                try:
                    value = convert_value(row["value"], old.type, new.type)  # type: ignore[index, call-overload]
                except (ValueError, TypeError) as e:
                    raise ValueError(f"Cannot convert document {row['doc_id']} to {new.type}: {e}")  # type: ignore[index, call-overload]
                await copy.write_row([row["id"], Jsonb(stored_values(new, value))])  # type: ignore[index, call-overload]
        removed = sql.Literal(stored_keys(old))
        if old.column == new.column:
            assignments = sql.SQL("{col} = ({col} - {removed}::text[]) || c.vals").format(col=old_col, removed=removed)
        else:
            assignments = sql.SQL("{old} = {old} - {removed}::text[], {new} = coalesce({new}, '{{}}') || c.vals").format(
                old=old_col, new=new_col, removed=removed
            )
        await conn.execute(
            sql.SQL("UPDATE documents SET {} FROM converted c WHERE {} AND documents.id = c.id").format(assignments, project)
        )
        last_id = rows[-1]["id"]  # type: ignore[index, call-overload]
