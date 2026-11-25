"""
The backfill_labels job: move the tags of a project from the json of its documents to the label table (doc_labels,
see amcat4.postgres.labels). Only scheduled when AMCAT4_LABEL_TABLES is on, for projects that have a tag field in json.

Three steps, each resumable:
A. Give every row of the project its doc_pk (rows from before migration 0005 have NULL, which means "= id"), in
   batches, then set projects.labels_ready. Joins with the label table inside the search index need the real column.
   This rewrites every such row once. A project made after migration 0005 skips this step.
B. Per tag field (not unique fields: their hash is computed from the json), in one transaction that holds the write
   lock of the project exclusively: copy the tags of every row version to doc_labels, each label with the
   created_seq and retired_seq of its row version (so every release reads the same tags as before), and set
   fields.label_table. Readers switch from json to labels when it commits; writers wait for it.
C. After STRIP_GRACE_SECONDS (requests that read the field definitions just before B still read the json), remove
   the tags from the json of the documents, in batches.

A write that read the field definitions before step B and got the write lock after it would still write its tags
into the json: amcat4.postgres.documents checks this inside the lock (fields_are_current) and refuses the write.
"""

import asyncio
from typing import TYPE_CHECKING, Any

from psycopg import AsyncConnection, sql

from amcat4.config import get_settings
from amcat4.errors import ConflictError
from amcat4.postgres import labels
from amcat4.postgres.connection import connection, fetch_all, fetch_one
from amcat4.postgres.fields import FIELD_COLUMNS, FieldInfo, field_info_from_row
from amcat4.postgres.layout import _WRITE_LOCK, project_filter, writing
from amcat4.postgres.projects import project_partitions
from amcat4.projects import maintenance

if TYPE_CHECKING:
    from amcat4.projects.jobs import Job

BATCH_SIZE = 5_000
# Seconds between step B (readers switch to the label table) and step C (the json values are removed)
STRIP_GRACE_SECONDS = 60.0
MAX_ATTEMPTS = 3
RETRY_AFTER = "1 hour"


async def _batch_end(conn: AsyncConnection, where: sql.Composable, after: int) -> int | None:
    """The id of the last of the next BATCH_SIZE rows of the project after id `after` (None: no more rows)"""
    cur = await conn.execute(
        sql.SQL("SELECT max(id) AS last FROM (SELECT id FROM documents WHERE {} AND id > %s ORDER BY id LIMIT %s) b").format(
            where
        ),
        [after, BATCH_SIZE],
    )
    return (await cur.fetchone())["last"]  # type: ignore[index]


async def _in_batches(job: "Job", project: int, key: str, update: sql.Composable) -> None:
    """Run `update` (with a condition on documents, ending in the id range) for all rows of the project, in id batches"""
    from amcat4.projects.jobs import JobPostponed, postpone_if_moving

    after = job.progress.get(key, 0)
    while after is not None:
        await postpone_if_moving(project)
        async with connection() as conn:
            try:
                async with writing(conn, project):
                    where = await project_filter(conn, project)
                    last = await _batch_end(conn, where, after)
                    if last is not None:
                        await conn.execute(
                            sql.SQL("{} AND {} AND documents.id > %s AND documents.id <= %s").format(update, where),
                            [after, last],
                        )
            except ConflictError as e:  # a move started
                raise JobPostponed(str(e)) from e
        after = last
        await job.save_progress(**{key: after})


async def _copy_field(project: int, f: FieldInfo) -> int:
    """Step B for one field. Returns the number of labels"""
    from amcat4.projects.jobs import JobPostponed

    async with connection() as conn, conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock(%s, %s)", [project, _WRITE_LOCK])
        cur = await conn.execute("SELECT moving_to FROM projects WHERE pk = %s", [project])
        if (await cur.fetchone())["moving_to"] is not None:  # type: ignore[index]
            raise JobPostponed(f"project {project} is being moved to another partition")
        cur = await conn.execute(
            sql.SQL(
                """INSERT INTO doc_labels (project_pk, doc_pk, field_pk, value, created_seq, retired_seq)
                   SELECT documents.project_pk, documents.doc_pk, {field}, v.value, documents.created_seq,
                          documents.retired_seq
                   FROM documents CROSS JOIN LATERAL unnest({values}) WITH ORDINALITY AS v(value, ord)
                   WHERE {project} AND documents.exact_fields ? {key}
                   ORDER BY documents.doc_pk, documents.created_seq, v.ord"""
            ).format(
                field=sql.Literal(f.pk),
                values=labels.values_array(f),
                project=await project_filter(conn, project),
                key=sql.Literal(f.key),
            )
        )
        await conn.execute("UPDATE fields SET label_table = true WHERE pk = %s", [f.pk])
        return cur.rowcount


async def backfill_labels_job(job: "Job") -> dict[str, Any]:
    """params: project_pk. See the module text."""
    from amcat4.projects.jobs import postpone_if_moving

    project = job.params["project_pk"]
    await postpone_if_moving(project)
    row = await fetch_one("SELECT labels_ready FROM projects WHERE pk = %s", [project])
    if row is None:
        return {"fields": []}
    if not row["labels_ready"]:
        await _in_batches(job, project, "doc_pk_after", sql.SQL("UPDATE documents SET doc_pk = id WHERE doc_pk IS NULL"))
        # (rows written meanwhile got their doc_pk from the trigger or from the write itself)
        async with connection() as conn:
            cur = await conn.execute(
                sql.SQL(
                    """UPDATE projects SET labels_ready = true WHERE pk = %s
                       AND NOT EXISTS (SELECT 1 FROM documents WHERE {} AND doc_pk IS NULL)"""
                ).format(await project_filter(conn, project)),
                [project],
            )
            if cur.rowcount != 1:
                raise RuntimeError(f"Project {project} still has documents without doc_pk")

    rows = await fetch_all(
        f"""SELECT {FIELD_COLUMNS} FROM fields
            WHERE project_pk = %s AND type = 'tag' AND NOT label_table AND NOT unique_field ORDER BY pk""",  # type: ignore[arg-type]
        [project],
    )
    moved: dict[str, str] = job.progress.get("moved", {})
    for row in rows:
        f = field_info_from_row(row)
        await _copy_field(project, f)
        moved[str(f.pk)] = f.name
        await job.save_progress(moved=moved)

    if moved and not job.progress.get("stripped"):
        await asyncio.sleep(STRIP_GRACE_SECONDS)
        keys = [f"f{pk}" for pk in moved]
        update = sql.SQL(
            "UPDATE documents SET exact_fields = exact_fields - {keys}::text[] WHERE exact_fields ?| {keys}::text[]"
        ).format(keys=sql.Literal(keys))
        await _in_batches(job, project, "strip_after", update)
        await job.save_progress(stripped=True)
        await maintenance.vacuum_soon((await project_partitions([project])).values())
    return {"fields": list(moved.values())}


async def schedule_label_backfill(state: dict) -> dict:
    """
    Periodic task, only with AMCAT4_LABEL_TABLES: a backfill_labels job for every project that still has a tag field
    in json (not unique fields, not a project that is being moved). A failed job is tried again after RETRY_AFTER, at
    most MAX_ATTEMPTS times in all.
    """
    if not get_settings().label_tables:
        return state
    from amcat4.projects.jobs import create_job_in_transaction

    rows = await fetch_all(
        f"""SELECT DISTINCT f.project_pk FROM fields f JOIN projects p ON p.pk = f.project_pk
            WHERE f.type = 'tag' AND NOT f.label_table AND NOT f.unique_field AND p.moving_to IS NULL
              AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.project_pk = f.project_pk
                              AND j.status IN ('pending', 'running') AND j.type IN ('backfill_labels', 'move_project'))
              AND (SELECT count(*) FROM jobs j WHERE j.type = 'backfill_labels' AND j.project_pk = f.project_pk
                   AND j.status = 'failed') < {MAX_ATTEMPTS}
              AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.type = 'backfill_labels' AND j.project_pk = f.project_pk
                              AND j.status = 'failed' AND j.updated_at > now() - interval '{RETRY_AFTER}')"""  # type: ignore[arg-type]
    )
    for row in rows:
        async with connection() as conn:  # (autocommit: the job exists at once)
            await create_job_in_transaction(conn, "backfill_labels", row["project_pk"], {"project_pk": row["project_pk"]})
    return state
