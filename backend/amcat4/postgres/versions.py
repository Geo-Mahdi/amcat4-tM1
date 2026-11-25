"""
Row versions for frozen releases ("shared rows", migration 0005).

Nothing that a release needs is overwritten. Every row of documents (and of doc_labels) has created_seq and retired_seq,
numbers from the sequence change_seq:
- a current row has retired_seq = CURRENT (the largest bigint);
- a release is one number R, and consists of the rows with created_seq <= R AND retired_seq > R (visible).

Writes:
- Every write function takes one number S (write_seq), after it has the shared write lock of the project
  (amcat4.postgres.layout.writing) and before it changes anything; it holds the lock until it has committed.
  Creating a release takes the same lock exclusively and then takes R from the same sequence: so every write with a
  number below R has committed, and every later write gets a number above R. (The order of the numbers alone is not the
  order of the commits; the lock is what makes R a clean cut.)
- A row that belongs to a release (created_seq <= the newest release of its project) is never changed or deleted:
    - an update keeps the old content as an extra row (same doc_pk and created_seq, retired_seq = S) and gives the
      current row created_seq = S, in ONE statement (versioned_update), so no other write can come in between;
    - a delete sets retired_seq = S on the row (retire_or_delete).
  The current row of a document keeps its id for as long as the document exists, so vectors, cursors and the unique
  index on doc_id are not affected; the older versions are the extra rows, with doc_pk = the document.
- A row that is in no release (a project without releases, or a document that was created or already changed after
  the newest release) is updated and deleted in place, exactly as before migration 0005. So projects without releases
  pay nothing but one sequence number per write, and the table does not grow.
  AMCAT4_VERSIONED_WRITES=false switches the versions off for every project (see config.py).

Not versioned, on purpose: columns that are derived from the content (sort slots, the derived date keys, dedup_hash).
Jobs fill these in place for all row versions, so a release sorts and aggregates like the current data.

doc_pk is NULL for rows from before migration 0005, which means "the same as id" (DOC_PK). Every row written since
has it filled.
"""

from dataclasses import dataclass

from psycopg import AsyncConnection, sql

from amcat4.config import get_settings

# retired_seq of a current row. Sent to pg_search as an integer: as a float the term matches nothing
CURRENT = 9223372036854775807

# The document a row is a version of, in plain SQL (not for pg_search joins: those need the column itself, which is
# filled for every row of a project whose tags are in the label table, see projects.labels_ready)
DOC_PK = sql.SQL("coalesce(documents.doc_pk, documents.id)")

# The columns that are copied to keep an old version (everything but id, the version columns and dedup_hash: an old
# version must not take part in the unique index on the hash)
VERSION_COLUMNS = [
    "partition_id",
    "project_pk",
    "doc_id",
    "text_fields",
    "exact_fields",
    "stored_fields",
    "copied_from",
    "date",
    "source",
    "sort_kw1",
    "sort_kw2",
    "sort_kw3",
    "sort_num1",
    "sort_date2",
    "created_at",
    "updated_at",
]


def live(table: str = "documents") -> sql.Composable:
    """SQL condition: the row is current (not an old version, not deleted)"""
    return sql.SQL("{}.retired_seq = {}").format(sql.Identifier(table), sql.Literal(CURRENT))


def visible(release: int | None, table: str = "documents") -> sql.Composable:
    """SQL condition: the row is current (release None), or part of the release with this number R"""
    if release is None:
        return live(table)
    return sql.SQL("{t}.created_seq <= {r} AND {t}.retired_seq > {r}").format(t=sql.Identifier(table), r=sql.Literal(release))


def visible_clauses(release: int | None) -> list[dict]:
    """
    The same as json clauses for the BM25 index (of documents or doc_labels), for the unscored block of a query:
    a term for current rows, two ranges for a release. Integers, never floats.
    """
    if release is None:
        return [{"term": {"field": "retired_seq", "value": CURRENT}}]
    return [
        {"range": {"field": "created_seq", "lower_bound": None, "upper_bound": {"included": int(release)}}},
        {"range": {"field": "retired_seq", "lower_bound": {"excluded": int(release)}, "upper_bound": None}},
    ]


@dataclass(frozen=True)
class WriteSeq:
    """The numbers of one write to a project"""

    seq: int  # S: the number of this write
    released: int  # the number of the newest release of the project; -1 if rows are changed in place (no release)

    @property
    def versioned(self) -> bool:
        return self.released >= 0


async def write_seq(conn: AsyncConnection, project_pk: int) -> WriteSeq:
    """
    Take the number for a write to this project. Call it inside the write lock of the project (guard_writes / writing),
    before the first change: then no release can be created until the write has committed.
    """
    cur = await conn.execute(
        "SELECT nextval('change_seq') AS seq, (SELECT max(seq) FROM releases WHERE project_pk = %s) AS released",
        [project_pk],
    )
    row: dict = await cur.fetchone()  # type: ignore[assignment]
    released = row["released"] if get_settings().versioned_writes else None
    return WriteSeq(seq=row["seq"], released=-1 if released is None else released)


async def has_releases(conn: AsyncConnection, project_pk: int) -> bool:
    cur = await conn.execute("SELECT 1 FROM releases WHERE project_pk = %s LIMIT 1", [project_pk])
    return await cur.fetchone() is not None


def version_assignments(w: WriteSeq) -> sql.Composable:
    """
    Extra assignments (starting with a comma) for an UPDATE of current documents that goes through versioned_update:
    a row that is in a release gets the number of this write, because its old content lives on in a copy.
    """
    if not w.versioned:
        return sql.SQL("")
    return sql.SQL(
        ", created_seq = CASE WHEN documents.created_seq <= {r} THEN {s} ELSE documents.created_seq END"
        ", doc_pk = coalesce(documents.doc_pk, documents.id)"
    ).format(r=sql.Literal(w.released), s=sql.Literal(w.seq))


def versioned_update(update: sql.Composable, w: WriteSeq) -> sql.Composable:
    """
    Wrap an UPDATE (or INSERT ... ON CONFLICT DO UPDATE) of current rows of documents, without RETURNING, whose SET
    list ends with version_assignments(w). The result is one statement that returns the id of every row it wrote.
    With a release: the old content of every changed row that is in a release is kept as a new row with
    retired_seq = S (RETURNING old.*, PostgreSQL 18), in the same statement and so in the same snapshot.
    Without: the UPDATE as it is.
    """
    if not w.versioned:
        return sql.SQL("{} RETURNING documents.id").format(update)
    columns = sql.SQL(", ").join(sql.Identifier(c) for c in VERSION_COLUMNS)
    return sql.SQL(
        """WITH changed AS ({update} RETURNING old.*, new.id AS _id),
                kept AS (INSERT INTO documents ({columns}, doc_pk, created_seq, retired_seq)
                         SELECT {columns}, coalesce(doc_pk, id), created_seq, {s} FROM changed
                         WHERE created_seq <= {r})
           SELECT _id AS id FROM changed"""
    ).format(update=update, columns=columns, s=sql.Literal(w.seq), r=sql.Literal(w.released))


async def retire_or_delete(conn: AsyncConnection, where: sql.Composable, params: list, w: WriteSeq) -> list[int]:
    """
    Delete the current documents that match `where` (a condition on the table "documents"). Rows that are in a release
    are kept and get retired_seq = S; the others are really deleted. Returns the doc_pk of every deleted document.
    The two statements see different rows (the second only rows that are in no release), so a document is never both.
    """
    doc_pks: list[int] = []
    async with conn.transaction():
        if w.versioned:
            cur = await conn.execute(
                sql.SQL(
                    """UPDATE documents SET retired_seq = {s}, dedup_hash = NULL, doc_pk = coalesce(doc_pk, id)
                       WHERE {where} AND {live} AND documents.created_seq <= {r}
                       RETURNING documents.doc_pk"""
                ).format(s=sql.Literal(w.seq), where=where, live=live(), r=sql.Literal(w.released)),
                params,
            )
            doc_pks += [row["doc_pk"] for row in await cur.fetchall()]  # type: ignore[index, call-overload]
        cur = await conn.execute(
            sql.SQL("DELETE FROM documents WHERE {where} AND {live} RETURNING {doc_pk} AS doc_pk").format(
                where=where, live=live(), doc_pk=DOC_PK
            ),
            params,
        )
        doc_pks += [row["doc_pk"] for row in await cur.fetchall()]  # type: ignore[index, call-overload]
    return doc_pks
