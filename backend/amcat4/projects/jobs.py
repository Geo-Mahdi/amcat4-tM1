"""
Background jobs.

Jobs are stored in the jobs table. A worker loop (started with the API server, see job_worker) claims pending jobs
with FOR UPDATE SKIP LOCKED, so multiple server processes can run jobs safely. Job handlers work in batches and
store their progress after every batch, so a job that was interrupted (e.g. by a server restart) continues where it
left off: running jobs that have not been updated for a while are claimed again.

While a job runs, a heartbeat keeps updated_at current, so long steps (e.g. rebuilding an index) are not mistaken
for interrupted jobs.

Periodic tasks are quick checks that run every interval (e.g. "does a partition need a reindex?", or later "are
outsourced tasks done?"). The worker loop runs them too; the periodic_tasks table makes sure each task runs at most
once per interval across all server processes, and keeps its state between runs. Periodic tasks should be quick:
for heavy work, they create a job.

To add a new job type, write an async handler (job: Job) -> result dict and register it in HANDLERS.
To add a periodic task, write an async function (state: dict) -> new state dict and register it in PERIODIC.
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Awaitable, Callable

from psycopg import AsyncConnection, sql
from psycopg.types.json import Jsonb

from amcat4.connections import server_defaults
from amcat4.errors import NotFoundError
from amcat4.models import CreateDocumentField, FieldType, FilterSpec
from amcat4.postgres import documents as storage
from amcat4.postgres.connection import connection, execute, fetch_all, fetch_one
from amcat4.postgres.fields import FIELD_COLUMNS, FieldSet
from amcat4.postgres.projects import project_partitions, project_pk
from amcat4.postgres.search import SearchQuery, compile_search
from amcat4.projects import maintenance, partitions

BATCH_SIZE = 2000
# Documents per transaction when filling a sort slot. Every transaction adds a segment to the search index (on
# pg_search 0.26, which does not merge them, see amcat4.projects.maintenance), so not too small
FILL_BATCH_SIZE = 10_000
STALE_AFTER = "2 minutes"  # running jobs that were not updated for this long are considered interrupted
HEARTBEAT_SECONDS = 30

_COLUMNS = (
    "id, type, status, (SELECT id FROM projects WHERE pk = jobs.project_pk) AS project, created_by, params, "
    "progress, result, error, created_at, updated_at"
)


class JobCancelled(Exception):
    pass


@dataclass
class Job:
    id: str
    type: str
    params: dict
    progress: dict

    async def save_progress(self, **progress) -> None:
        """Store progress (and check whether the job was cancelled)"""
        self.progress.update(progress)
        row = await fetch_one(
            "UPDATE jobs SET progress = %s, updated_at = now() WHERE id = %s RETURNING status", [Jsonb(self.progress), self.id]
        )
        if row is None or row["status"] == "cancelled":
            raise JobCancelled()


async def create_job(type: str, project: str | None, created_by: str | None, params: dict) -> dict:
    if type not in HANDLERS:
        raise ValueError(f"Unknown job type {type}")
    job_id = uuid.uuid4().hex
    pk = await project_pk(project) if project else None
    await execute(
        "INSERT INTO jobs (id, type, status, project_pk, created_by, params) VALUES (%s, %s, 'pending', %s, %s, %s)",
        [job_id, type, pk, created_by, Jsonb(params)],
    )
    return await get_job(job_id)


async def create_job_in_transaction(conn: AsyncConnection, type: str, project_pk: int | None, params: dict) -> str:
    """Create a job with this connection, so it only exists if the transaction it is in commits. Returns its id."""
    if type not in HANDLERS:
        raise ValueError(f"Unknown job type {type}")
    job_id = uuid.uuid4().hex
    await conn.execute(
        "INSERT INTO jobs (id, type, status, project_pk, params) VALUES (%s, %s, 'pending', %s, %s)",
        [job_id, type, project_pk, Jsonb(params)],
    )
    return job_id


async def get_job(job_id: str) -> dict:
    row = await fetch_one(f"SELECT {_COLUMNS} FROM jobs WHERE id = %s", [job_id])  # type: ignore[arg-type]
    if row is None:
        raise NotFoundError(f"Job {job_id} does not exist")
    return row


async def list_jobs(project: str | None = None, created_by: str | None = None) -> list[dict]:
    conditions, params = ["TRUE"], []
    if project:
        conditions.append("project_pk = (SELECT pk FROM projects WHERE id = %s)")
        params.append(project)
    if created_by:
        conditions.append("created_by = %s")
        params.append(created_by)
    return await fetch_all(
        f"SELECT {_COLUMNS} FROM jobs WHERE {' AND '.join(conditions)} ORDER BY created_at DESC LIMIT 100",
        params,  # type: ignore[arg-type]
    )


async def cancel_job(job_id: str) -> None:
    await execute(
        "UPDATE jobs SET status = 'cancelled', updated_at = now() WHERE id = %s AND status IN ('pending', 'running')", [job_id]
    )


async def _claim_job() -> Job | None:
    async with connection() as conn:
        async with conn.transaction():
            cur = await conn.execute(
                f"""SELECT id, type, params, progress FROM jobs
                    WHERE status = 'pending' OR (status = 'running' AND updated_at < now() - interval '{STALE_AFTER}')
                    ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED"""  # type: ignore[arg-type]
            )
            row = await cur.fetchone()
            if row is None:
                return None
            await conn.execute("UPDATE jobs SET status = 'running', updated_at = now() WHERE id = %s", [row["id"]])  # type: ignore[index, call-overload]
    return Job(id=row["id"], type=row["type"], params=row["params"], progress=row["progress"])  # type: ignore[index, call-overload]


async def _heartbeat(job_id: str) -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_SECONDS)
        await execute("UPDATE jobs SET updated_at = now() WHERE id = %s AND status = 'running'", [job_id])


async def run_job(job: Job) -> None:
    heartbeat = asyncio.create_task(_heartbeat(job.id))
    try:
        result = await HANDLERS[job.type](job)
    except JobCancelled:
        logging.info(f"Job {job.id} was cancelled")
        return
    except Exception as e:
        logging.exception(f"Job {job.id} failed")
        await execute("UPDATE jobs SET status = 'failed', error = %s, updated_at = now() WHERE id = %s", [str(e), job.id])
        return
    finally:
        heartbeat.cancel()
    await execute(
        "UPDATE jobs SET status = 'done', result = %s, updated_at = now() WHERE id = %s AND status = 'running'",
        [Jsonb(result), job.id],
    )


async def run_pending_jobs() -> int:
    """Run all pending jobs (until none are left). Returns the number of jobs that were run."""
    n = 0
    while (job := await _claim_job()) is not None:
        await run_job(job)
        n += 1
    return n


@dataclass
class PeriodicTask:
    interval: timedelta
    run: Callable[[dict], Awaitable[dict]]


async def run_periodic_tasks() -> list[str]:
    """Run the periodic tasks that are due (and not already run by another process). Returns their names."""
    ran = []
    for name, task in PERIODIC.items():
        # claim the task: insert it, or update last_run if the interval has passed (atomic, so only one process wins)
        row = await fetch_one(
            """INSERT INTO periodic_tasks AS t (name, last_run) VALUES (%s, now())
               ON CONFLICT (name) DO UPDATE SET last_run = now() WHERE t.last_run <= now() - %s
               RETURNING state""",
            [name, task.interval],
        )
        if row is None:
            continue
        try:
            state = await task.run(row["state"])
        except Exception:
            logging.exception(f"Periodic task {name} failed")
            continue
        await execute("UPDATE periodic_tasks SET state = %s WHERE name = %s", [Jsonb(state), name])
        ran.append(name)
    return ran


async def job_worker(poll_interval: float = 2.0) -> None:
    """Run periodic tasks and jobs forever (started as a background task by the API server)"""
    # Jobs and maintenance use the server's own settings (parallel workers, no API timeouts), not the API's tuning.
    # This task runs in its own context, so this holds for the worker (and the tasks it starts) only.
    with server_defaults():
        await _job_loop(poll_interval)


async def _job_loop(poll_interval: float) -> None:
    while True:
        try:
            await run_periodic_tasks()
            await run_pending_jobs()
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.exception("Error in job worker")
        await asyncio.sleep(poll_interval)


###################### JOB TYPES ######################


async def _copy_job(job: Job) -> dict[str, Any]:
    """
    Copy documents (optionally a subset selected by queries/filters, and a subset of fields) to another project.
    params: source, destination, queries, filters, field_options
      field_options: per source field: {rename: str, exclude: bool, type: FieldType}
    """
    from amcat4.systemdata.fields import create_fields, field_infos, list_fields

    p = job.params
    from_pk, to_pk = await project_pk(p["source"]), await project_pk(p["destination"])
    if await fetch_one("SELECT 1 FROM projects WHERE pk = %s AND moving_to IS NOT NULL", [from_pk]):
        # (the batches below read the source's documents in its partition: they would miss the ones already moved)
        raise ValueError(f"Project {p['source']} is being moved to another partition; copy it when that is done")

    if "field_map" not in job.progress:
        # Set up the destination fields (only once)
        field_options = p.get("field_options") or {}
        dest_fields = await list_fields(p["destination"])
        new_fields: dict[str, CreateDocumentField] = {}
        field_map: dict[str, str] = {}
        for name, definition in (await list_fields(p["source"])).items():
            opts = field_options.get(name, {})
            if opts.get("exclude"):
                continue
            dest_name = opts.get("rename") or name
            field_map[name] = dest_name
            if dest_name in dest_fields:
                continue
            type_override: FieldType | None = opts.get("type")
            if type_override and type_override != definition.type:
                new_fields[dest_name] = CreateDocumentField(type=type_override)
            else:
                new_fields[dest_name] = CreateDocumentField(
                    type=definition.type,
                    unique=definition.unique,
                    metareader=definition.metareader,
                    reader=definition.reader,
                    client_settings=definition.client_settings,
                )
        if new_fields:
            await create_fields(p["destination"], new_fields)
        source_infos = await field_infos(p["source"])
        # date fields copied from fields whose documents do not all have the quarter key yet: the copies don't either
        # (the backfill task fills them later)
        not_ready = [d for s, d in field_map.items() if s in source_infos and not source_infos[s].quarter_ready]
        if not_ready:
            await execute(
                "UPDATE fields SET quarter_ready = false WHERE project_pk = %s AND name = ANY(%s) AND type = 'date'",
                [to_pk, not_ready],
            )
        query = SearchQuery(
            queries=p.get("queries"), filters={k: FilterSpec(**v) for k, v in (p.get("filters") or {}).items()} or None
        )
        c = compile_search(FieldSet({from_pk: source_infos}, partitions=await project_partitions([from_pk])), query)
        async with connection() as conn:
            cur = await conn.execute(sql.SQL("SELECT count(*) AS n FROM documents WHERE {}").format(c.where), c.params)
            total = (await cur.fetchone())["n"]  # type: ignore[index, call-overload]
        await job.save_progress(field_map=field_map, total=total, copied=0, after_id=0)

    source_infos = await field_infos(p["source"])
    dest_infos = await field_infos(p["destination"])
    field_map_infos = {source_infos[s]: dest_infos[d] for s, d in job.progress["field_map"].items()}
    query = SearchQuery(
        queries=p.get("queries"), filters={k: FilterSpec(**v) for k, v in (p.get("filters") or {}).items()} or None
    )
    c = compile_search(FieldSet({from_pk: source_infos}, partitions=await project_partitions([from_pk])), query)
    while True:
        async with connection() as conn:
            cur = await conn.execute(
                sql.SQL("SELECT id FROM documents WHERE {} AND documents.id > %s ORDER BY id LIMIT %s").format(c.where),
                [*c.params, job.progress["after_id"], BATCH_SIZE],
            )
            ids = [row["id"] for row in await cur.fetchall()]  # type: ignore[index, call-overload]
            if not ids:
                break
            n = await storage.copy_batch(conn, from_pk, to_pk, field_map_infos, dest_infos, ids)
        await job.save_progress(after_id=ids[-1], copied=job.progress["copied"] + n)
    return {"copied": job.progress["copied"]}


async def _fill_sort_slot_job(job: Job) -> dict[str, Any]:
    """
    Copy the values of a field into its sort slot column for all documents of the project, in batches (or clear the
    column: field_pk None). params: project_pk, slot, field_pk. Resumable: progress is the last document id.
    Stops early if the slot was given to another field meanwhile (that field's own fill covers every document).
    When done, the field's sort_slot_ready is set, and sorting uses the slot.
    """
    from amcat4.postgres.fields import field_info_from_row

    p = job.params
    project, slot, field_pk = p["project_pk"], p["slot"], p.get("field_pk")
    async with connection() as conn:
        # waits for a fill or clear of the same slot that is still running (another job, or a field update)
        await storage.lock_sort_slot(conn, project, slot)
        try:
            while True:
                cur = await conn.execute(
                    f"SELECT {FIELD_COLUMNS} FROM fields WHERE project_pk = %s AND sort_slot = %s",  # type: ignore[arg-type]
                    [project, slot],
                )
                owner = await cur.fetchone()
                if (owner["pk"] if owner else None) != field_pk:  # type: ignore[index, call-overload]
                    return {"stopped": f"slot {slot} is now used by another field", **job.progress}
                f = field_info_from_row(owner) if owner else None  # type: ignore[arg-type]
                last = await storage.fill_sort_slot(
                    conn, project, slot, f, after_id=job.progress.get("after_id"), batch_size=FILL_BATCH_SIZE
                )
                if last is None:
                    break
                await job.save_progress(after_id=last, batches=job.progress.get("batches", 0) + 1)
            if field_pk is not None:
                await conn.execute(
                    "UPDATE fields SET sort_slot_ready = true WHERE pk = %s AND sort_slot = %s", [field_pk, slot]
                )
        finally:
            await storage.lock_sort_slot(conn, project, slot, unlock=True)
    return {"slot": slot, "field_pk": field_pk, "batches": job.progress.get("batches", 0)}


async def _backfill_quarter_job(job: Job) -> dict[str, Any]:
    """
    Give the documents of a project that were stored before migration 0002 the derived quarter key of their date
    fields, in batches (resumable), then mark the fields as ready (quarter aggregations then use the key).
    params: project_pk. Only touches documents that have a date value but no quarter key.
    """
    from amcat4.postgres.fields import field_info_from_row, quarter_sql
    from amcat4.postgres.layout import project_filter

    project = job.params["project_pk"]
    rows = await fetch_all(
        f"SELECT {FIELD_COLUMNS} FROM fields WHERE project_pk = %s AND type = 'date' AND NOT quarter_ready",  # type: ignore[arg-type]
        [project],
    )
    done = job.progress.get("fields", {})
    for row in rows:
        f = field_info_from_row(row)
        after = done.get(str(f.pk), 0)
        while after is not None:
            async with connection() as conn:
                where = await project_filter(conn, project)
                cur = await conn.execute(
                    sql.SQL("SELECT max(id) AS last FROM ({}) b").format(
                        sql.SQL("SELECT id FROM documents WHERE {} AND id > %s ORDER BY id LIMIT %s").format(where)
                    ),
                    [after, FILL_BATCH_SIZE],
                )
                last = (await cur.fetchone())["last"]  # type: ignore[index]
                if last is not None:
                    value = sql.SQL("(documents.exact_fields->>{})").format(sql.Literal(f.key))
                    await conn.execute(
                        sql.SQL(
                            """UPDATE documents SET exact_fields = exact_fields || jsonb_build_object({qkey}, {quarter})
                               WHERE {where} AND documents.id > %s AND documents.id <= %s
                                 AND exact_fields ? {key} AND NOT exact_fields ? {qkey}"""
                        ).format(
                            qkey=sql.Literal(f.derived_key("quarter")),
                            quarter=quarter_sql(value),
                            key=sql.Literal(f.key),
                            where=where,
                        ),
                        [after, last],
                    )
            after = last
            done[str(f.pk)] = last
            await job.save_progress(fields=done)
        await execute("UPDATE fields SET quarter_ready = true WHERE pk = %s", [f.pk])
    return {"fields": [row["name"] for row in rows]}


async def schedule_quarter_backfill(state: dict) -> dict:
    """
    Periodic task: a backfill_quarter job for every project with date fields that do not have their quarter keys
    (after migration 0002, or a copy from such a project). Not again for a project whose job failed (that needs a
    look; delete the failed job to try again).
    """
    rows = await fetch_all(
        """SELECT DISTINCT f.project_pk FROM fields f WHERE f.type = 'date' AND NOT f.quarter_ready
           AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.type = 'backfill_quarter' AND j.project_pk = f.project_pk
                           AND j.status IN ('pending', 'running', 'failed'))"""
    )
    for row in rows:
        async with connection() as conn:
            await create_job_in_transaction(conn, "backfill_quarter", row["project_pk"], {"project_pk": row["project_pk"]})
    return state


HANDLERS: dict[str, Callable[[Job], Awaitable[dict[str, Any]]]] = {
    "copy": _copy_job,
    "fill_sort_slot": _fill_sort_slot_job,
    "backfill_quarter": _backfill_quarter_job,
    "reindex": maintenance.reindex_job,
    "vacuum": maintenance.vacuum_job,
    "move_project": partitions.move_project_job,
}

PERIODIC: dict[str, PeriodicTask] = {
    "partition_maintenance": PeriodicTask(interval=maintenance.CHECK_INTERVAL, run=maintenance.check_partitions),
    "quarter_backfill": PeriodicTask(interval=timedelta(minutes=1), run=schedule_quarter_backfill),
}
