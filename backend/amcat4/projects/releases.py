"""
Frozen releases of a project.

A release is the state of a project at one moment: its documents, their values and their tags. It is not a copy:
it is one sequence number R, and the release is every row (of documents and of doc_labels) with
created_seq <= R AND retired_seq > R. Writes after the release keep those rows (amcat4.postgres.versions), so the
release keeps giving the same results for searches, counts and aggregations (query_documents and query_aggregate take
a release number). Everything stays inside PostgreSQL.

Creating a release (create_release) takes the write lock of the project exclusively for a moment: it waits for the
writes that are running, takes R from change_seq, and stores the release. The manifest (document count, counts per
field, and fingerprints) is computed right after, without the lock: the rows of the release can no longer change.
verify_release computes it again and compares, to show that the release is still what it was.

Deleting a release (only without a DOI) frees the row versions that only this release needed; a purge job removes
them (purge).

The relevance order (BM25 score) of a release query is not frozen: scores depend on the other rows in the index.
Which documents match, the counts, and every order on a field or on _seq are.
"""

import hashlib
from typing import Any, AsyncIterator

from psycopg import AsyncConnection, sql
from psycopg.types.json import Jsonb

from amcat4.config import get_settings
from amcat4.connections import with_server_defaults
from amcat4.errors import ConflictError, NotFoundError
from amcat4.postgres.connection import connection, fetch_all, fetch_one
from amcat4.postgres.fields import FieldInfo, list_field_infos
from amcat4.postgres.layout import _WRITE_LOCK, project_filter
from amcat4.postgres.projects import project_pk
from amcat4.postgres.versions import CURRENT, DOC_PK, visible
from amcat4.projects.release_metadata import make_release_metadata, read_field_rows, rerun_golden_query, sha256_hex

_COLUMNS = (
    "number, seq, created_at, created_by, doi, manifest, metadata_sha256, metadata_public, withdrawn_at, "
    "withdrawn_reason, doi_status"
)
# The frozen metadata of migration 0006 (see amcat4.projects.release_metadata): only read when asked for
_FROZEN = "metadata_jsonld, versions, field_definitions, golden_queries"
DOI_STATUSES = ("none", "draft", "registered", "findable")


async def create_release(index: str, created_by: str | None = None, doi: str | None = None) -> dict:
    """
    Freeze the current state of the project as a new release. Returns the release (as get_release), with its manifest.
    """
    if not get_settings().versioned_writes:
        raise ValueError(
            "Releases need versioned writes (AMCAT4_VERSIONED_WRITES is false): without them a later update or delete "
            "would change the release"
        )
    pk = await project_pk(index)
    async with connection() as conn:
        async with conn.transaction():
            # Exclusive: waits until every write to the project that is running has committed (they hold the lock
            # shared, see layout.writing), and keeps new writes waiting until this short transaction is done. So every
            # write with a number below R is complete and visible, and every later write gets a number above R
            await conn.execute("SELECT pg_advisory_xact_lock(%s, %s)", [pk, _WRITE_LOCK])
            cur = await conn.execute(
                """INSERT INTO releases (project_pk, number, seq, created_by, doi)
                   VALUES (%s, (SELECT coalesce(max(number), 0) + 1 FROM releases WHERE project_pk = %s),
                           nextval('change_seq'), %s, %s)
                   RETURNING number, seq, created_at""",
                [pk, pk, created_by, doi],
            )
            row: dict = await cur.fetchone()  # type: ignore[assignment]
            # the field definitions as they are at the cut, read in the same transaction (frozen in the metadata)
            field_rows = await read_field_rows(conn, pk)
    await _store_frozen(index, pk, row, created_by, field_rows)
    return await get_release(index, row["number"])


async def _store_frozen(index: str, pk: int, row: dict, created_by: str | None, field_rows: list[dict]) -> None:
    """
    The manifest and the frozen metadata (migration 0006) of a new release, computed from its rows (which can no longer
    change) and stored once. The golden queries are run on the release, so this happens after its row has committed.
    """
    seq = row["seq"]
    async with connection() as conn:
        manifest = await compute_manifest(conn, pk, seq)
        frozen = await make_release_metadata(
            conn,
            index=index,
            pk=pk,
            number=row["number"],
            seq=seq,
            created_at=row["created_at"],
            created_by=created_by,
            field_rows=field_rows,
            manifest=manifest,
        )
        manifest["metadata_sha256"] = frozen["metadata_sha256"]
        manifest["golden_queries_sha256"] = sha256_hex(frozen["golden_queries"])
        await conn.execute(
            """UPDATE releases SET manifest = %s, metadata_jsonld = %s, metadata_sha256 = %s, versions = %s,
                      field_definitions = %s, golden_queries = %s
               WHERE project_pk = %s AND seq = %s""",
            [
                Jsonb(manifest),
                Jsonb(frozen["metadata_jsonld"]),
                frozen["metadata_sha256"],
                Jsonb(frozen["versions"]),
                Jsonb(frozen["field_definitions"]),
                Jsonb(frozen["golden_queries"]),
                pk,
                seq,
            ],
        )


async def list_releases(index: str) -> list[dict]:
    pk = await project_pk(index)
    return await fetch_all(f"SELECT {_COLUMNS} FROM releases WHERE project_pk = %s ORDER BY number", [pk])  # type: ignore[arg-type]


async def get_release(index: str, number: int) -> dict:
    pk = await project_pk(index)
    row = await fetch_one(f"SELECT {_COLUMNS} FROM releases WHERE project_pk = %s AND number = %s", [pk, number])  # type: ignore[arg-type]
    if row is None:
        raise NotFoundError(f"Release {number} of project {index} does not exist")
    return row


async def get_release_frozen(index: str, number: int) -> dict:
    """A release with its frozen metadata (metadata_jsonld, versions, field_definitions, golden_queries)"""
    pk = await project_pk(index)
    row = await fetch_one(
        f"SELECT {_COLUMNS}, {_FROZEN} FROM releases WHERE project_pk = %s AND number = %s",  # type: ignore[arg-type]
        [pk, number],
    )
    if row is None:
        raise NotFoundError(f"Release {number} of project {index} does not exist")
    return row


async def release_seq(index: str, number: int) -> int:
    """The sequence number R of a release: what the search functions take (get_fieldset(release=R))"""
    return (await get_release(index, number))["seq"]


async def set_release_doi(index: str, number: int, doi: str | None) -> dict:
    return await update_release(index, number, doi=doi)


_UNSET: Any = object()


async def update_release(
    index: str,
    number: int,
    *,
    doi: str | None = _UNSET,
    doi_status: str | None = None,
    metadata_public: bool | None = None,
    withdrawn: bool | None = None,
    withdrawn_reason: str | None = None,
) -> dict:
    """
    Change the live properties of a release (never its rows or its frozen metadata): the DOI and its status, whether
    the metadata is public, and whether it is withdrawn. Withdrawing only marks the release: its rows stay and it can
    still be queried. withdrawn=False takes the mark (and the reason) away.
    """
    await get_release(index, number)
    pk = await project_pk(index)
    sets: list[sql.Composable] = []
    params: list[Any] = []
    if doi is not _UNSET:
        sets.append(sql.SQL("doi = %s"))
        params.append(doi)
    if doi_status is not None:
        if doi_status not in DOI_STATUSES:
            raise ValueError(f"doi_status must be one of {', '.join(DOI_STATUSES)}")
        sets.append(sql.SQL("doi_status = %s"))
        params.append(doi_status)
    if metadata_public is not None:
        sets.append(sql.SQL("metadata_public = %s"))
        params.append(metadata_public)
    if withdrawn is True:
        sets.append(sql.SQL("withdrawn_at = coalesce(withdrawn_at, now()), withdrawn_reason = %s"))
        params.append(withdrawn_reason)
    elif withdrawn is False:
        sets.append(sql.SQL("withdrawn_at = NULL, withdrawn_reason = NULL"))
    elif withdrawn_reason is not None:
        sets.append(sql.SQL("withdrawn_reason = CASE WHEN withdrawn_at IS NULL THEN NULL ELSE %s END"))
        params.append(withdrawn_reason)
    if sets:
        async with connection() as conn:
            await conn.execute(
                sql.SQL("UPDATE releases SET {} WHERE project_pk = %s AND number = %s").format(sql.SQL(", ").join(sets)),
                [*params, pk, number],
            )
    return await get_release(index, number)


async def delete_release(index: str, number: int) -> str | None:
    """
    Delete a release (refused if it has a DOI: a published release must stay). The row versions that only this
    release needed are removed by a purge job, which is created here; returns its id.
    """
    from amcat4.projects.jobs import create_job

    release = await get_release(index, number)
    if release["doi"]:
        raise ConflictError(f"Release {number} of project {index} has a DOI ({release['doi']}) and cannot be deleted")
    pk = await project_pk(index)
    async with connection() as conn:
        await conn.execute("DELETE FROM releases WHERE project_pk = %s AND number = %s", [pk, number])
    job = await create_job("purge_releases", index, None, {"project_pk": pk})
    return job["id"]


###################### MANIFEST ######################


async def _stream(conn: AsyncConnection, query: sql.Composable, params: list | None = None) -> AsyncIterator[dict]:
    """The rows of a (large) query, read in batches with a server-side cursor"""
    async with conn.transaction():
        async with conn.cursor(name="amcat4_manifest") as cur:
            cur.itersize = 20_000
            await cur.execute(query, params)
            async for row in cur:
                yield row  # type: ignore[misc]


def _tag_values(f: FieldInfo, release: int) -> sql.Composable:
    """SELECT (field_pk, doc_pk, value) of a tag field in a release, wherever the field stores its values"""
    if f.in_labels:
        return sql.SQL("SELECT l.field_pk, l.doc_pk, l.value FROM doc_labels l WHERE l.field_pk = {} AND {}").format(
            sql.Literal(f.pk), visible(release, "l")
        )
    value = sql.SQL("documents.exact_fields->{}").format(sql.Literal(f.key))
    return sql.SQL(
        """SELECT {pk}::int AS field_pk, {doc_pk} AS doc_pk, v.value
           FROM documents, jsonb_array_elements_text(CASE jsonb_typeof({value}) WHEN 'array' THEN {value}
                WHEN 'string' THEN jsonb_build_array({value}) ELSE '[]'::jsonb END) AS v(value)
           WHERE {{project}} AND {versions}"""
    ).format(pk=sql.Literal(f.pk), doc_pk=DOC_PK, value=value, versions=visible(release))


async def compute_manifest(conn: AsyncConnection, pk: int, release: int) -> dict[str, Any]:
    """
    What a release contains, computed from its rows:
    - documents: the number of documents;
    - fingerprint: md5 over the ordered list of (doc_pk, created_seq): which version of which document is in it;
    - fields: per field (by name), its pk and type and the number of documents with a value;
    - tags: the number of (document, tag field, value) and an md5 over the ordered list of them. Computed from the
      values themselves, so it stays the same when the tags of a project move to the label table.
    Reads every row of the release once (plain SQL, in batches).
    """
    project = await project_filter(conn, pk)
    versions = visible(release)
    fields = await list_field_infos(conn, pk)

    digest, n = hashlib.md5(), 0
    query = sql.SQL("SELECT {} AS doc_pk, created_seq FROM documents WHERE {} AND {} ORDER BY 1, 2").format(
        DOC_PK, project, versions
    )
    async for row in _stream(conn, query):
        digest.update(f"{row['doc_pk']}:{row['created_seq']}\n".encode())
        n += 1
    manifest: dict[str, Any] = {"seq": release, "documents": n, "fingerprint": digest.hexdigest(), "fields": {}}

    counted = [f for f in fields.values() if f.column != "vector" and not f.in_labels]
    counts: dict = {}
    if counted:
        filters = sql.SQL(", ").join(
            sql.SQL("count(*) FILTER (WHERE documents.{} ? {}) AS {}").format(
                sql.Identifier(f.column), sql.Literal(f.key), sql.Identifier(f.key)
            )
            for f in counted
        )
        cur = await conn.execute(sql.SQL("SELECT {} FROM documents WHERE {} AND {}").format(filters, project, versions))
        counts = await cur.fetchone()  # type: ignore[assignment]
    cur = await conn.execute(
        sql.SQL(
            "SELECT l.field_pk, count(DISTINCT l.doc_pk) AS n FROM doc_labels l WHERE l.project_pk = %s AND {} GROUP BY 1"
        ).format(visible(release, "l")),
        [pk],
    )
    label_counts = {row["field_pk"]: row["n"] for row in await cur.fetchall()}  # type: ignore[index, call-overload]
    for name, f in fields.items():
        if f.column == "vector":
            continue  # (vectors are not part of a release)
        documents = label_counts.get(f.pk, 0) if f.in_labels else counts[f.key]
        manifest["fields"][name] = {"pk": f.pk, "type": f.type, "documents": documents}

    tag_fields = [f for f in fields.values() if f.type == "tag"]
    digest, n = hashlib.md5(), 0
    if tag_fields:
        union = sql.SQL(" UNION ").join(_tag_values(f, release) for f in tag_fields)  # (UNION: a value counts once)
        query = sql.SQL('SELECT * FROM ({}) t ORDER BY field_pk, doc_pk, value COLLATE "C"').format(
            sql.SQL(union.as_string(conn)).format(project=project)
        )
        async for row in _stream(conn, query):
            digest.update(f"{row['field_pk']}:{row['doc_pk']}:{row['value']}\n".encode())
            n += 1
    manifest["tags"] = {"values": n, "fingerprint": digest.hexdigest()}
    return manifest


@with_server_defaults
async def verify_release(index: str, number: int) -> dict:
    """
    Compute the manifest of a release again from its rows and compare it with the stored one.
    Returns {"ok": bool, "differences": [keys that differ], "stored": manifest, "computed": manifest}.
    Field names are compared by pk, so renaming a field is not a difference; fields made after the release (with no
    values in it) are ignored.
    For a release with frozen metadata (migration 0006) also: the SHA-256 of the stored JSON-LD must still be the one
    stored (and in the manifest), and the golden queries are run again ("golden_queries": one entry per query, with
    the stored and the new result). Differences are named "metadata_sha256", "golden_queries_sha256" and
    "golden:<name>".
    """
    release = await get_release_frozen(index, number)
    pk = await project_pk(index)
    golden: list[dict] = []
    metadata: dict | None = None
    async with connection() as conn:
        computed = await compute_manifest(conn, pk, release["seq"])
        if release["metadata_jsonld"] is not None:
            for spec in release["golden_queries"] or []:
                golden.append(await rerun_golden_query(conn, index, pk, release["seq"], spec))
    stored = release["manifest"]
    differences = [key for key in ("documents", "fingerprint", "tags") if stored.get(key) != computed.get(key)]

    def by_pk(manifest: dict) -> dict:
        return {f["pk"]: f["documents"] for f in manifest.get("fields", {}).values() if f["documents"]}

    if by_pk(stored) != by_pk(computed):
        differences.append("fields")
    if release["metadata_jsonld"] is not None:
        sha = sha256_hex(release["metadata_jsonld"])
        golden_sha = sha256_hex(release["golden_queries"])
        metadata = {
            "stored_sha256": release["metadata_sha256"],
            "computed_sha256": sha,
            "ok": sha == release["metadata_sha256"] == stored.get("metadata_sha256"),
        }
        if not metadata["ok"]:
            differences.append("metadata_sha256")
        if golden_sha != stored.get("golden_queries_sha256"):
            differences.append("golden_queries_sha256")  # (the stored queries or results were changed)
        differences += [f"golden:{g['name']}" for g in golden if not g["ok"]]
    return {
        "ok": not differences,
        "differences": differences,
        "stored": stored,
        "computed": computed,
        "metadata": metadata,
        "golden_queries": golden,
    }


###################### PURGE ######################


@with_server_defaults
async def purge(pk: int | None = None) -> dict[str, int]:
    """
    Remove the old row versions (of documents and labels) that no release needs: rows that are not current and whose
    interval [created_seq, retired_seq) contains the number of no release of their project. Of one project, or of all.
    Safe next to writes and new releases: a new release gets a number above every retired_seq that exists.
    """
    unneeded = sql.SQL(
        """{t}.retired_seq <> {current} AND NOT EXISTS (
               SELECT 1 FROM releases r WHERE r.project_pk = {t}.project_pk AND r.seq >= {t}.created_seq
               AND r.seq < {t}.retired_seq)"""
    )
    result = {}
    async with connection() as conn:
        pks = [pk] if pk is not None else [row["pk"] for row in await fetch_all("SELECT pk FROM projects ORDER BY pk")]
        documents = labels = 0
        for project in pks:
            cur = await conn.execute(
                sql.SQL("DELETE FROM documents WHERE {} AND {}").format(
                    await project_filter(conn, project),
                    unneeded.format(t=sql.Identifier("documents"), current=sql.Literal(CURRENT)),
                )
            )
            documents += cur.rowcount
            cur = await conn.execute(
                sql.SQL("DELETE FROM doc_labels WHERE doc_labels.project_pk = %s AND {}").format(
                    unneeded.format(t=sql.Identifier("doc_labels"), current=sql.Literal(CURRENT))
                ),
                [project],
            )
            labels += cur.rowcount
        result = {"documents": documents, "labels": labels}
    return result


async def purge_job(job) -> dict:
    """Job: purge (params: project_pk, or none for all projects), then ask for a vacuum of the changed partitions"""
    from amcat4.postgres.projects import project_partitions
    from amcat4.projects.maintenance import vacuum_soon

    pk = job.params.get("project_pk")
    result = await purge(pk)
    if pk is not None:
        await vacuum_soon((await project_partitions([pk])).values(), result["documents"])
    return result
