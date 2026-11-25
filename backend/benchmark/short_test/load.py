"""
Step 1, LOAD: one project of --docs documents in a partition of its own, and one of 200,000 documents in the shared
partition.

Everything goes through the normal code of the branch under test: the projects, the fields (create_fields), the
partition of the big project (the move_project job, on the still empty project) and the documents
(create_or_update_documents, batches of 5000). So the rows always have the layout of the branch under test, whatever
that layout is. The documents of a project are uploaded in a fixed order, so the internal ids have the same order on
every run (ties in a sort are broken by internal id).

After the upload the search indexes are rebuilt (REINDEX) and the tables vacuumed, so every branch starts the
measurements with a fresh index and not with the segments the uploads happened to leave.
"""

import asyncio
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Any

from benchmark.short_test import data
from benchmark.short_test.common import PROJECT_BIG, PROJECT_SMALL, SMALL_DOCS, Deadline, own_connection


async def project_counts() -> dict[str, int] | None:
    """The number of documents of the two test projects (None if the schema or a project does not exist)"""
    async with own_connection(120) as conn:
        cur = await conn.execute("SELECT to_regclass('projects') IS NOT NULL AS ok")
        if not (await cur.fetchone())["ok"]:  # type: ignore[index]
            return None
        cur = await conn.execute(
            """SELECT p.id, p.partition_id, p.own_partition,
                      (SELECT count(*) FROM documents d WHERE d.partition_id = p.partition_id AND d.project_pk = p.pk) AS n,
                      (SELECT count(*) FROM fields f WHERE f.project_pk = p.pk) AS fields
               FROM projects p WHERE p.id = ANY(%s)""",
            [[PROJECT_BIG, PROJECT_SMALL]],
        )
        rows = {row["id"]: row for row in await cur.fetchall()}  # type: ignore[index, call-overload]
    if set(rows) != {PROJECT_BIG, PROJECT_SMALL}:
        return None
    if not rows[PROJECT_BIG]["own_partition"] or rows[PROJECT_SMALL]["own_partition"]:
        return None
    if any(row["fields"] < len(data.FIELDS) for row in rows.values()):
        return None
    return {name: row["n"] for name, row in rows.items()}


async def _drop_everything() -> None:
    """Drop the whole test schema. Refused if it holds projects that are not ours"""
    from amcat4.systemdata.manage import delete_systemdata

    async with own_connection(60) as conn:
        cur = await conn.execute("SELECT to_regclass('projects') IS NOT NULL AS ok")
        if (await cur.fetchone())["ok"]:  # type: ignore[index]
            cur = await conn.execute("SELECT id FROM projects WHERE id <> ALL(%s)", [[PROJECT_BIG, PROJECT_SMALL]])
            others = [row["id"] for row in await cur.fetchall()]  # type: ignore[index, call-overload]
            if others:
                raise RuntimeError(f"The schema holds other projects ({others[:5]}): not dropping it. Use an own schema")
    await delete_systemdata()


async def _create_projects() -> None:
    from amcat4.models import ProjectSettings
    from amcat4.projects import partitions
    from amcat4.projects.index import create_project_index
    from amcat4.projects.jobs import get_job, run_pending_jobs
    from amcat4.systemdata.fields import create_fields

    for name in (PROJECT_SMALL, PROJECT_BIG):
        await create_project_index(ProjectSettings(id=name, name=f"Short test {name}"))
        await create_fields(name, data.FIELDS)  # type: ignore[arg-type]
    # The big project gets a partition of its own, with the normal job, while it is still empty (so nothing is moved)
    partitions.MOVE_GRACE_SECONDS = 0.0
    job = await partitions.start_move(PROJECT_BIG)
    await run_pending_jobs()
    job = await get_job(job["id"])
    if job["status"] != "done":
        raise RuntimeError(f"Could not give {PROJECT_BIG} its own partition: {job}")


async def _upload(project: str, total: int, pool: ProcessPoolExecutor, deadline: Deadline, progress: dict) -> None:
    """Upload the documents of a project, batch after batch in a fixed order; the batches are made in other processes"""
    from amcat4.projects.documents import create_or_update_documents

    loop = asyncio.get_running_loop()
    n_batches = -(-total // data.BATCH)
    ahead: list[asyncio.Future] = []  # the next batches, being made
    next_batch = 0
    for b in range(n_batches):
        while next_batch < n_batches and len(ahead) < 4:
            ahead.append(loop.run_in_executor(pool, data.make_batch, project, next_batch, total))
            next_batch += 1
        docs = await ahead.pop(0)
        deadline.check("upload")
        result = await create_or_update_documents(project, docs, op_type="create")
        if result["created"] != len(docs):
            raise RuntimeError(f"Batch {b} of {project}: {result} for {len(docs)} documents")
        progress[project] = progress.get(project, 0) + len(docs)


async def _layout() -> dict[str, Any]:
    """Sizes and index segments of the partitions, and which fields are in a sort slot"""
    from amcat4.postgres.layout import index_segments, list_partitions

    out: dict[str, Any] = {"partitions": [], "sort_slots": {}}
    async with own_connection(120) as conn:
        for part in await list_partitions(conn):
            seg = await index_segments(conn, part["index"])
            out["partitions"].append(
                {
                    "table": part["table"],
                    "total_mb": round(part["total_bytes"] / 2**20),
                    "index_mb": round(part["index_bytes"] / 2**20),
                    "segments": seg["segments"],
                    "deleted_in_index": seg["deleted"],
                }
            )
        cur = await conn.execute(
            """SELECT p.id, f.name, f.sort_slot FROM fields f JOIN projects p ON p.pk = f.project_pk
               WHERE p.id = ANY(%s) AND f.sort_slot IS NOT NULL ORDER BY 1, 2""",
            [[PROJECT_BIG, PROJECT_SMALL]],
        )
        for row in await cur.fetchall():
            out["sort_slots"].setdefault(row["id"], {})[row["name"]] = row["sort_slot"]  # type: ignore[index, call-overload]
    return out


async def rebuild_indexes(deadline: Deadline) -> dict[str, float]:
    """REINDEX the search index of every partition and VACUUM (ANALYZE) the documents: a fresh, comparable start"""
    from amcat4.postgres.layout import list_partitions, reindex

    times: dict[str, float] = {}
    async with own_connection(max(1.0, deadline.left())) as conn:
        t0 = time.monotonic()
        for part in await list_partitions(conn):
            await reindex(conn, part["index"])
        times["reindex_s"] = round(time.monotonic() - t0, 1)
        t0 = time.monotonic()
        await conn.execute("VACUUM (ANALYZE) documents")
        times["vacuum_s"] = round(time.monotonic() - t0, 1)
    return times


async def step_load(args, deadline: Deadline, out: dict[str, Any]) -> None:
    from amcat4.systemdata.manage import create_or_update_systemdata

    wanted = {PROJECT_BIG: args.docs, PROJECT_SMALL: min(SMALL_DOCS, args.docs)}
    out["documents"] = wanted
    counts = await project_counts()
    if counts == wanted and not args.force_load:
        out["skipped"] = "the data was already there with the right counts (use --force-load to load again)"
        out.update(await _layout())
        print(f"  data is already loaded: {counts}")
        return
    if counts is not None or args.force_load:
        print("  dropping the test schema and loading again")
        await _drop_everything()

    t0 = time.monotonic()
    await create_or_update_systemdata()
    await _create_projects()
    out["create_s"] = round(time.monotonic() - t0, 1)

    t0 = time.monotonic()
    progress: dict[str, int] = {}
    out["uploaded"] = progress
    with ProcessPoolExecutor(max_workers=2) as pool:
        try:
            await asyncio.gather(*(_upload(name, n, pool, deadline, progress) for name, n in wanted.items()))
        finally:
            pool.shutdown(wait=False, cancel_futures=True)
    seconds = time.monotonic() - t0
    out["upload_s"] = round(seconds, 1)
    out["upload_docs_per_s"] = round(sum(wanted.values()) / seconds)
    print(f"  uploaded {sum(wanted.values())} documents in {seconds:.0f} s")

    out.update(await rebuild_indexes(deadline))
    out.update(await _layout())
    counts = await project_counts()
    if counts != wanted:
        raise RuntimeError(f"After the load the counts are {counts}, expected {wanted}")
