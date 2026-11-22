"""
Background jobs.

Jobs are stored in the jobs table. A worker loop (started with the API server, see job_worker) claims pending jobs
with FOR UPDATE SKIP LOCKED, so multiple server processes can run jobs safely. Job handlers work in batches and
store their progress after every batch, so a job that was interrupted (e.g. by a server restart) continues where it
left off: running jobs that have not been updated for a while are claimed again.

While a job runs, a heartbeat keeps updated_at current, so long steps (e.g. rebuilding an index) are not mistaken
for interrupted jobs.

A handler that cannot continue now (e.g. its project is being moved to another partition) raises JobPostponed: the
job goes back to pending with its progress, and is claimed again after POSTPONE_FOR. Other exceptions fail the job.

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
from amcat4.errors import ConflictError, NotFoundError
from amcat4.models import CreateDocumentField, FieldType, FilterSpec
from amcat4.postgres import documents as storage
from amcat4.postgres.connection import connection, execute, fetch_all, fetch_one
from amcat4.postgres.fields import FIELD_COLUMNS, FieldInfo, FieldSet, quarter_sql
from amcat4.postgres.layout import project_filter, writing
from amcat4.postgres.projects import project_partitions, project_pk
from amcat4.postgres.search import SearchQuery, compile_search
from amcat4.projects import maintenance, partitions

BATCH_SIZE = 2000
# Documents per transaction when filling a sort slot or adding quarter keys. Every transaction adds a segment to the
# search index (pg_search 0.26 does not merge them, 0.27 merges small ones in the background, see
# amcat4.projects.maintenance), so not too small; and not too large, so each transaction stays short
FILL_BATCH_SIZE = 10_000
STALE_AFTER = "2 minutes"  # running jobs that were not updated for this long are considered interrupted
HEARTBEAT_SECONDS = 30
POSTPONE_FOR = "30 seconds"  # a postponed job (JobPostponed) is not claimed again for this long
# A failed backfill_quarter job is retried by the periodic task after RETRY_AFTER, at most MAX_BACKFILL_ATTEMPTS times
MAX_BACKFILL_ATTEMPTS = 3
RETRY_AFTER = "5 minutes"

_COLUMNS = (
    "id, type, status, (SELECT id FROM projects WHERE pk = jobs.project_pk) AS project, created_by, params, "
    "progress, result, error, created_at, updated_at"
)


class JobCancelled(Exception):
    pass


class JobPostponed(Exception):
    """
    Raised by a job handler that cannot continue now (e.g. its project is being moved to another partition). The job
    goes back to pending, keeps its progress, and is claimed again after POSTPONE_FOR: it does not fail.
    """


async def postpone_if_moving(project_pk: int) -> None:
    """Raise JobPostponed if the project is being moved (its documents cannot be written until the move is done)"""
    if await fetch_one("SELECT 1 FROM projects WHERE pk = %s AND moving_to IS NOT NULL", [project_pk]):
        raise JobPostponed(f"project {project_pk} is being moved to another partition")


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
    """
    Create a job with this connection, so it only exists if the transaction it is in commits (used where a check and
    the insert must be atomic, e.g. maintenance.vacuum_soon). Takes the project's pk, not its id. Returns the job id.
    """
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


async def _claim_job(job_id: str | None = None) -> Job | None:
    """Claim the oldest job that can run now (or only this job, if job_id is given), or return None"""
    async with connection() as conn:
        async with conn.transaction():
            # (postponed jobs wait POSTPONE_FOR: otherwise run_pending_jobs would claim them again at once)
            cur = await conn.execute(
                f"""SELECT id, type, params, progress FROM jobs
                    WHERE ((status = 'pending'
                            AND NOT (progress ? 'postponed' AND updated_at > now() - interval '{POSTPONE_FOR}'))
                           OR (status = 'running' AND updated_at < now() - interval '{STALE_AFTER}'))
                      AND (%(id)s::text IS NULL OR id = %(id)s)
                    ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED""",  # type: ignore[arg-type]
                {"id": job_id},
            )
            row = await cur.fetchone()
            if row is None:
                return None
            await conn.execute(
                "UPDATE jobs SET status = 'running', progress = progress - 'postponed', updated_at = now() WHERE id = %s",
                [row["id"]],  # type: ignore[index, call-overload]
            )
    progress = {k: v for k, v in row["progress"].items() if k != "postponed"}  # type: ignore[index, call-overload]
    return Job(id=row["id"], type=row["type"], params=row["params"], progress=progress)  # type: ignore[index, call-overload]


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
    except JobPostponed as e:
        # back to pending; the progress is kept, the 'postponed' note makes _claim_job wait POSTPONE_FOR. Only if the job
        # is still running (not cancelled meanwhile)
        logging.info(f"Job {job.id} is postponed: {e}")
        await execute(
            """UPDATE jobs SET status = 'pending', progress = progress || jsonb_build_object('postponed', %s::text),
                   updated_at = now()
               WHERE id = %s AND status = 'running'""",
            [str(e), job.id],
        )
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


async def run_job_now(job_id: str, poll_interval: float = 2.0) -> dict:
    """
    Run this one job here, until it is done, failed or cancelled, and return it (as get_job). If another process (the
    server's job worker) claimed it first, or it was postponed, wait for it, and claim it again when it can run. Other
    pending jobs are left to the job worker. Used by `amcat4 move-project --now`.
    """
    while True:
        job = await _claim_job(job_id)
        if job is not None:
            await run_job(job)
        row = await get_job(job_id)
        if row["status"] in ("done", "failed", "cancelled"):
            return row
        await asyncio.sleep(poll_interval)


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
    # Jobs and maintenance use the server's own settings (parallel workers, no API timeouts), not the API's tuning
    # (no parallel workers, a statement timeout; see amcat4.connections). Unlike the original, which had one pool.
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
    # While the source or the destination is being moved to another partition, the job is postponed (JobPostponed),
    # at the start or between batches, and continues after the move: the batches read the source's documents in its
    # partition (they would miss the ones already moved), and the destination refuses writes (ConflictError). Before,
    # a moving source failed the job, and a destination that started moving failed it for good with ConflictError
    await postpone_if_moving(from_pk)
    await postpone_if_moving(to_pk)

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
        # date fields copied from fields whose documents do not all have the quarter key yet: the copies don't either,
        # so the destination field is marked not ready too (quarter aggregations use the slower path with the same
        # results, and the backfill task fills the keys later)
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
        await postpone_if_moving(from_pk)  # (c names the source's partition from before a move)
        async with connection() as conn:
            cur = await conn.execute(
                sql.SQL("SELECT id FROM documents WHERE {} AND documents.id > %s ORDER BY id LIMIT %s").format(c.where),
                [*c.params, job.progress["after_id"], BATCH_SIZE],
            )
            ids = [row["id"] for row in await cur.fetchall()]  # type: ignore[index, call-overload]
            if not ids:
                break
            try:
                n = await storage.copy_batch(conn, from_pk, to_pk, field_map_infos, dest_infos, ids)
            except ConflictError as e:  # the destination is being moved (writes to it are refused until it is done)
                raise JobPostponed(str(e)) from e
        await job.save_progress(after_id=ids[-1], copied=job.progress["copied"] + n)
    return {"copied": job.progress["copied"]}


async def _fill_sort_slot_job(job: Job) -> dict[str, Any]:
    """
    Copy the values of fields into their sort slot columns for all documents of the project, in batches (or clear a
    column: field_pk None). params: project_pk, slots {slot: field_pk} (a job of an earlier version: slot, field_pk).
    All slots of the job are filled in one pass over the rows (fill_sort_slots: one UPDATE per batch sets every slot
    column), so filling N slots rewrites each row once, not N times: a request that maps several fields, or several
    requests before the job starts, give one job (queue_fill_sort_slot). Before, every slot had its own job and its own
    pass; measured on 2.68 M real documents with the previous version of this job (22 Nov 2026): two slots took more
    than 10 minutes in total, and the index was rebuilt afterwards (searches 2-4x slower meanwhile).
    Resumable: progress is the last document id.
    A slot that was given to another field meanwhile is left out from then on (that field's own fill covers every
    document); the job stops if that leaves no slot. When done, each filled field's sort_slot_ready is set, and
    sorting uses the slot.
    Used for projects with more than sort_slot_job_rows documents (AMCAT4_SORT_SLOT_JOB_ROWS): filling rewrites every
    document with a value, too long for one request. Smaller projects are filled in the request, as in the original
    (which had only the date and source slots, filled in the request).
    While the project is being moved to another partition, the job is postponed (JobPostponed), at the start or
    between batches.
    """
    from amcat4.postgres.fields import SORT_SLOTS, field_info_from_row

    p = job.params
    project = p["project_pk"]
    slots: dict[str, int | None] = p["slots"] if "slots" in p else {p["slot"]: p.get("field_pk")}
    order = sorted(slots, key=SORT_SLOTS.index)  # type: ignore[arg-type]  # (locks always in the same order)
    await postpone_if_moving(project)
    stopped: list[str] = []
    async with connection() as conn:
        # waits for a fill or clear of the same slots that is still running (another job, or a field update)
        for slot in order:
            await storage.lock_sort_slot(conn, project, slot)
        try:
            while True:
                cur = await conn.execute(
                    f"SELECT {FIELD_COLUMNS} FROM fields WHERE project_pk = %s AND sort_slot = ANY(%s)",  # type: ignore[arg-type]
                    [project, order],
                )
                owners: dict[str, dict] = {row["sort_slot"]: row for row in await cur.fetchall()}  # type: ignore[index, call-overload, misc]
                active: dict[str, FieldInfo | None] = {}
                for slot in order:
                    owner = owners.get(slot)
                    if (owner["pk"] if owner else None) != slots[slot]:
                        if slot not in stopped:
                            stopped.append(slot)
                        continue
                    active[slot] = field_info_from_row(owner) if owner else None
                if not active:
                    return {"stopped": f"slot(s) {', '.join(stopped)} now used by another field", **job.progress}
                try:
                    last = await storage.fill_sort_slots(
                        conn, project, active, after_id=job.progress.get("after_id"), batch_size=FILL_BATCH_SIZE
                    )
                except ConflictError as e:  # a move started (writes to the project are refused until it is done)
                    raise JobPostponed(str(e)) from e
                if last is None:
                    break
                await job.save_progress(after_id=last, batches=job.progress.get("batches", 0) + 1)
            for slot, f in active.items():
                if f is not None:
                    await conn.execute(
                        "UPDATE fields SET sort_slot_ready = true WHERE pk = %s AND sort_slot = %s", [f.pk, slot]
                    )
        finally:
            for slot in reversed(order):
                await storage.lock_sort_slot(conn, project, slot, unlock=True)
    # a mass update: without a vacuum, the next counts and sorts must check every rewritten row (measured on a 300k
    # test database after filling a slot for 270k documents: a sort on the slot 187 ms before, 3.4 ms after VACUUM)
    await maintenance.vacuum_soon((await project_partitions([project])).values())
    result: dict[str, Any] = {"slots": slots, "batches": job.progress.get("batches", 0)}
    if stopped:
        result["stopped"] = stopped
    return result


async def queue_fill_sort_slot(conn: AsyncConnection, project_pk: int, slot: str, field_pk: int | None) -> str:
    """
    Have a fill_sort_slot job fill (or clear: field_pk None) this slot of the project, in the transaction of the field
    update. The slot is added to a pending fill job of the project that has not started yet (no progress) and does not
    have this slot yet, so slots requested together are filled in one pass; otherwise a new job is created. Returns the
    job id. (FOR UPDATE: a job that a worker is claiming right now is skipped once it is running, then a new job is
    created; and a worker cannot claim the job until this transaction commits, SKIP LOCKED.)
    """
    cur = await conn.execute(
        """UPDATE jobs SET params = jsonb_set(params, '{slots}', params->'slots' || jsonb_build_object(%s::text, %s::int))
           WHERE id = (SELECT id FROM jobs WHERE type = 'fill_sort_slot' AND status = 'pending' AND project_pk = %s
                       AND params ? 'slots' AND NOT params->'slots' ? %s AND progress = '{}'::jsonb
                       ORDER BY created_at LIMIT 1 FOR UPDATE)
           RETURNING id""",
        [slot, field_pk, project_pk, slot],
    )
    row = await cur.fetchone()
    if row is not None:
        return row["id"]  # type: ignore[index, call-overload]
    return await create_job_in_transaction(
        conn, "fill_sort_slot", project_pk, {"project_pk": project_pk, "slots": {slot: field_pk}}
    )


async def _backfill_quarter_job(job: Job) -> dict[str, Any]:
    """
    Give the documents of a project that were stored before migration 0002 the derived quarter key of their date
    fields, in batches (resumable), then mark the fields as ready (quarter aggregations then use the key).
    params: project_pk. Only touches documents that have a date value but no quarter key.
    While the project is being moved to another partition, the job is postponed (JobPostponed), at the start or
    between batches. Its updates are writes to the project (writing(), so a move waits for a running batch).
    Why a job plus a readiness flag (fields.quarter_ready) rather than a fallback at query time: the fast path then
    needs no extra statement per aggregation, and results are the same before, during and after the backfill.
    """
    from amcat4.postgres.fields import field_info_from_row

    project = job.params["project_pk"]
    await postpone_if_moving(project)
    rows = await fetch_all(
        f"SELECT {FIELD_COLUMNS} FROM fields WHERE project_pk = %s AND type = 'date' AND NOT quarter_ready",  # type: ignore[arg-type]
        [project],
    )
    done = job.progress.get("fields", {})
    for row in rows:
        f = field_info_from_row(row)
        after = done.get(str(f.pk), 0)
        while after is not None:
            await postpone_if_moving(project)
            async with connection() as conn:
                try:
                    async with writing(conn, project):
                        last = await _backfill_quarter_batch(conn, project, f, after)
                except ConflictError as e:  # a move started (writes to the project are refused until it is done)
                    raise JobPostponed(str(e)) from e
            after = last
            done[str(f.pk)] = last
            await job.save_progress(fields=done)
        await execute("UPDATE fields SET quarter_ready = true WHERE pk = %s", [f.pk])
    await maintenance.vacuum_soon((await project_partitions([project])).values())  # (a mass update, see above)
    return {"fields": [row["name"] for row in rows]}


async def _backfill_quarter_batch(conn: AsyncConnection, project: int, f: FieldInfo, after: int) -> int | None:
    """
    Give the next FILL_BATCH_SIZE documents after id `after` the quarter key of f. Returns the last id (None: done).
    The batch is a range of ids (read over the B-tree index of migration 0002), so a resumed job starts after the last
    batch. quarter_sql gives the same value as the Python function used at upload (checked in a test).
    """
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
                qkey=sql.Literal(f.derived_key("quarter")), quarter=quarter_sql(value), key=sql.Literal(f.key), where=where
            ),
            [after, last],
        )
    return last


async def schedule_quarter_backfill(state: dict) -> dict:
    """
    Periodic task: a backfill_quarter job for every project with date fields that do not have their quarter keys
    (after migration 0002, or a copy from such a project). Not for a project that is being moved (or has a move job
    waiting): the job would only be postponed. A failed job is tried again after RETRY_AFTER, at most
    MAX_BACKFILL_ATTEMPTS times in all (then it needs a look; delete the failed jobs to try again).
    """
    rows = await fetch_all(
        f"""SELECT DISTINCT f.project_pk FROM fields f JOIN projects p ON p.pk = f.project_pk
            WHERE f.type = 'date' AND NOT f.quarter_ready AND p.moving_to IS NULL
              AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.project_pk = f.project_pk
                              AND j.status IN ('pending', 'running') AND j.type IN ('backfill_quarter', 'move_project'))
              AND (SELECT count(*) FROM jobs j WHERE j.type = 'backfill_quarter' AND j.project_pk = f.project_pk
                   AND j.status = 'failed') < {MAX_BACKFILL_ATTEMPTS}
              AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.type = 'backfill_quarter' AND j.project_pk = f.project_pk
                              AND j.status = 'failed' AND j.updated_at > now() - interval '{RETRY_AFTER}')"""  # type: ignore[arg-type]
    )
    for row in rows:
        async with connection() as conn:  # (autocommit: the job exists at once)
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
