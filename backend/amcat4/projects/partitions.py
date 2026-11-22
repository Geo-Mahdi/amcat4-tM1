"""
Moving a (big) project to a documents partition of its own.

Projects share partitions (see amcat4.postgres.layout). A big project in a shared partition is slower than it needs
to be (whole-project counts and aggregations go through the other projects' index entries: measured for 2.68 M
documents next to 10.7 M others, whole-project aggregation 14.5 ms shared against 6.2 ms alone, count 3.4 ms against
1.3 ms; searches gain little), its relevance scores depend on the
other projects, and its index cannot be rebuilt on its own. Moving it gives it its own partition and search index.

The original layout foresaw this move, as an update of projects.partition_id. That moves all documents in one
statement (still possible, see migration 0003): one huge transaction that locks every document of the project until
the end. The move_project job moves them in batches instead (migration 0003 lets a project's
documents be in two partitions):
1. create a new partition, register it in project_partitions, set projects.moving_to and own_partition, and store
   the target in the job's progress (one transaction, so a resumed job never creates a second partition). This waits
   for the writes to the project that are running; from then on writes are refused (ConflictError: uploads look up
   existing documents in one partition); reads look in both partitions and see every document exactly once (a
   document moves in one statement)
2. move the documents in batches (UPDATE of partition_id: postgres moves each row, and its vectors, to the new
   partition); resumable, as the documents that are left are exactly those still to move
3. set projects.partition_id to the new partition and clear moving_to (a short transaction)
Reads are never blocked. During the move, the relevance scores mix the statistics of two indexes; the in-index fast
paths (which need a single partition) are not used. Afterwards the old partition has deleted documents in its index,
which the automatic maintenance cleans up (a rebuild), and both partitions get a vacuum job.

Start it with `amcat4 move-project --project <id>` (an admin command; the job worker of the server runs it, or with
--now the command itself). A move is refused while the project has unfinished jobs (start_move).
"""

import logging
from typing import TYPE_CHECKING, Any

from psycopg.types.json import Jsonb

from amcat4.postgres.connection import connection, fetch_one
from amcat4.postgres.layout import _PARTITION_LOCK, create_partition, lock_writes, next_partition_id, partition_table

if TYPE_CHECKING:
    from amcat4.projects.jobs import Job

# Documents per transaction in step 2: each batch is a short transaction (rows locked only until its commit)
MOVE_BATCH_SIZE = 10_000


async def start_move(project: str, created_by: str | None = None) -> dict:
    """
    Create a move_project job for this project (ValueError if it cannot be moved now: it does not exist, is already
    being moved, already has a partition of its own, or has jobs that are not done yet).
    """
    from amcat4.projects.jobs import create_job

    row = await fetch_one("SELECT pk, partition_id, moving_to, own_partition FROM projects WHERE id = %s", [project])
    if row is None:
        raise ValueError(f"Project {project} does not exist")
    if row["moving_to"] is not None or await fetch_one(
        "SELECT 1 FROM jobs WHERE type = 'move_project' AND status IN ('pending', 'running') AND project_pk = %s", [row["pk"]]
    ):
        raise ValueError(f"Project {project} is already being moved")
    if row["own_partition"]:
        raise ValueError(f"Project {project} already has a partition of its own")
    # jobs that write to the project would fail during the move (and a copy from it would miss documents)
    if await fetch_one(
        """SELECT 1 FROM jobs WHERE status IN ('pending', 'running')
           AND (project_pk = %s OR (type = 'copy' AND (params->>'source' = %s OR params->>'destination' = %s)))""",
        [row["pk"], project, project],
    ):
        raise ValueError(f"Project {project} has jobs that are not done yet; move it when they are")
    return await create_job("move_project", project, created_by, {"project_pk": row["pk"]})


async def move_project_job(job: "Job") -> dict[str, Any]:
    """Job: move a project to a new partition of its own, in batches (params: project_pk). See the module docstring."""
    from amcat4.projects.maintenance import vacuum_soon

    project = job.params["project_pk"]
    if "target" not in job.progress:
        async with connection() as conn:
            row = await (
                await conn.execute("SELECT partition_id, moving_to FROM projects WHERE pk = %s", [project])
            ).fetchone()
            source, target = row["partition_id"], row["moving_to"]  # type: ignore[index]
            if target is not None:
                # Step 1 was done, but the job stopped before its progress was saved (a job of an older version, which
                # saved it afterwards): continue with that partition, do not create a second one
                logging.info(f"Resuming the move of project {project} to partition {target}")
                await job.save_progress(source=source, target=target, moved=0)
            else:
                await lock_writes(conn, project)  # wait for running writes; new ones wait until moving_to is set
                try:
                    async with conn.transaction():
                        # the lock of assign_partition: the two never create a partition with the same id, and a new
                        # project is not assigned to this one (it sees moving_to once this commits)
                        await conn.execute("SELECT pg_advisory_xact_lock(%s)", [_PARTITION_LOCK])
                        target = await next_partition_id(conn)
                        await create_partition(conn, target, lock_timeout="30s")
                        await conn.execute(
                            "INSERT INTO project_partitions (project_pk, partition_id) VALUES (%s, %s)", [project, target]
                        )
                        await conn.execute(
                            "UPDATE projects SET moving_to = %s, own_partition = true WHERE pk = %s", [target, project]
                        )
                        # the progress in the same transaction: a job that stops here resumes with this partition
                        progress = {**job.progress, "source": source, "target": target, "moved": 0}
                        await conn.execute(
                            "UPDATE jobs SET progress = %s, updated_at = now() WHERE id = %s", [Jsonb(progress), job.id]
                        )
                finally:
                    await lock_writes(conn, project, unlock=True)
                job.progress.update(progress)
                logging.info(f"Moving project {project} from partition {source} to new partition {target}")
    source, target = job.progress["source"], job.progress["target"]

    while True:
        async with connection() as conn:
            # the next batch in id order (over the B-tree index of migration 0002); moving a row changes its partition,
            # so the documents left in the source are exactly those still to move (no progress needed to resume)
            cur = await conn.execute(
                """WITH batch AS (SELECT id FROM documents WHERE partition_id = %s AND project_pk = %s ORDER BY id LIMIT %s)
                   UPDATE documents d SET partition_id = %s FROM batch
                   WHERE d.partition_id = %s AND d.project_pk = %s AND d.id = batch.id""",
                [source, project, MOVE_BATCH_SIZE, target, source, project],
            )
            n = cur.rowcount
        if n == 0:
            break
        await job.save_progress(moved=job.progress["moved"] + n)

    async with connection() as conn, conn.transaction():
        cur = await conn.execute(
            "SELECT count(*) AS n FROM documents WHERE partition_id = %s AND project_pk = %s", [source, project]
        )
        # (writes are refused during the move, so this is only a safety check before the switch)
        if (await cur.fetchone())["n"]:  # type: ignore[index]
            raise RuntimeError("Documents were added to the old partition during the move")
        # (the trigger on projects moves nothing now, and removes the old partition from project_partitions)
        await conn.execute("UPDATE projects SET partition_id = %s, moving_to = NULL WHERE pk = %s", [target, project])
    # both partitions changed a lot: the source has dead rows, the target new ones (see maintenance, VACUUM)
    await vacuum_soon([source, target], job.progress["moved"])
    return {"source": partition_table(source), "target": partition_table(target), "moved": job.progress["moved"]}
