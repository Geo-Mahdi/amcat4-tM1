"""
Automatic maintenance of the BM25 indexes of the documents partitions.

Every upload adds segments to the index of a partition, and updates leave deleted documents in the index. Both make
searches slower: rebuilding the index (REINDEX, online) fixes that and is cheap (seconds per million documents), but
rebuilding while a partition is still changing is wasted work.

How much this is needed depends on the pg_search version. pg_search 0.26 does not merge segments (in our tests: one
new segment per upload, forever). From 0.27 on, pg_search merges small segments in the background, which also
drops most deleted documents, so the number of segments stays low (about 5 to 10 in tests) and the segment trigger
below rarely fires; the trigger for deleted documents (large segments are not merged again soon) stays relevant.
The rebuild stays as the safety net for both.

So a periodic task (check_partitions, run by the job worker) looks at every partition:
- whether it changed since the previous check (postgres counts inserts, updates and deletes per table), or whether
  pg_search is merging its index right now: a merge that is still running is activity too, and it may solve the
  problem by itself, so we wait for it like for changes
- whether its index needs a rebuild: many more segments than after the previous rebuild, or many deleted documents
If a partition needs a rebuild and has been quiet for QUIET_PERIOD (or has needed one for MAX_WAIT, for partitions
that are never quiet), it creates a reindex job. Only one reindex job exists at a time.
"""

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Iterable

from psycopg import sql

from amcat4.config import get_settings
from amcat4.postgres.connection import connection, fetch_all, fetch_one
from amcat4.postgres.layout import (
    drop_reindex_leftovers,
    index_merging,
    index_segments,
    list_partitions,
    partition_changes,
    partition_table,
    reindex,
)

if TYPE_CHECKING:
    from amcat4.projects.jobs import Job

CHECK_INTERVAL = timedelta(minutes=5)
QUIET_PERIOD = timedelta(minutes=15)
MAX_WAIT = timedelta(hours=24)
# A rebuild gives up to ~one segment per parallel worker; small indexes have fewer. The threshold is relative to the
# number of segments after the previous rebuild (see needs_reindex), so a server with many cores does not rebuild
# again right away. With background merging (pg_search 0.27+) the count stays below this in normal use.
MIN_SEGMENTS = 16
MAX_DELETED = 0.2  # fraction of the documents in the index


def needs_reindex(segments: dict, segments_after_rebuild: int | None) -> str | None:
    """The reason why an index needs a rebuild (or None)"""
    if segments["deleted"] > MAX_DELETED * max(segments["documents"], 1):
        return f"{segments['deleted']} deleted documents"
    if segments["segments"] > max(MIN_SEGMENTS, 2 * (segments_after_rebuild or 0)):
        return f"{segments['segments']} segments"
    return None


async def _segments_after_rebuild() -> dict[int, int]:
    """The number of segments after the last (automatic) rebuild of each partition"""
    rows = await fetch_all(
        """SELECT DISTINCT ON (params->>'partition_id') (params->>'partition_id')::int AS partition_id,
                  (result->>'segments_after')::int AS segments
           FROM jobs WHERE type = 'reindex' AND status = 'done'
           ORDER BY params->>'partition_id', updated_at DESC"""
    )
    return {row["partition_id"]: row["segments"] for row in rows}


async def check_partitions(state: dict, now: datetime | None = None) -> dict:
    """
    Periodic task: create a reindex job for a partition that needs it (see module docstring).
    state contains, per partition, the number of changes seen in the previous check, when it last changed, and
    since when it needs a rebuild.
    """
    from amcat4.projects.jobs import create_job

    now = now or datetime.now(UTC)
    if await fetch_one("SELECT 1 FROM jobs WHERE type = 'reindex' AND status IN ('pending', 'running')"):
        return state
    async with connection() as conn:
        await drop_reindex_leftovers(conn)
        changes = await partition_changes(conn)
        partitions = await list_partitions(conn)
        segments = {p["partition_id"]: await index_segments(conn, p["index"]) for p in partitions}
        merging = {p["partition_id"]: await index_merging(conn, p["index"]) for p in partitions}
    after_rebuild = await _segments_after_rebuild()

    seen = state.get("partitions", {})
    new_state: dict[str, dict] = {}
    due = []
    for p in partitions:
        pid = p["partition_id"]
        previous = seen.get(str(pid), {})
        n = changes.get(p["table"], 0)
        quiet = previous.get("changes") == n and not merging[pid]
        changed_at = previous.get("changed_at") if quiet else now.isoformat()
        reason = needs_reindex(segments[pid], after_rebuild.get(pid))
        needed_since = (previous.get("needed_since") or now.isoformat()) if reason else None
        new_state[str(pid)] = {"changes": n, "changed_at": changed_at, "needed_since": needed_since}
        if reason and (
            now - datetime.fromisoformat(changed_at) >= QUIET_PERIOD or now - datetime.fromisoformat(needed_since) >= MAX_WAIT  # type: ignore[arg-type]
        ):
            due.append((segments[pid]["segments"], pid, reason))
    if due:
        _, pid, reason = max(due)
        await create_job("reindex", None, None, {"partition_id": pid, "reason": reason})
    return {"partitions": new_state}


async def reindex_job(job: "Job") -> dict:
    """Job: rebuild the BM25 index of a partition (params: partition_id)"""
    partition_id = job.params["partition_id"]
    async with connection() as conn:
        partition = next((p for p in await list_partitions(conn) if p["partition_id"] == partition_id), None)
        if partition is None:
            raise ValueError(f"Partition {partition_id} does not exist")
        before = await index_segments(conn, partition["index"])
        started = datetime.now(UTC)
        await reindex(conn, partition["index"])
        seconds = (datetime.now(UTC) - started).total_seconds()
        after = await index_segments(conn, partition["index"])
    return {
        "partition_id": partition_id,
        "seconds": round(seconds, 1),
        "segments_before": before["segments"],
        "segments_after": after["segments"],
        "deleted_before": before["deleted"],
    }


###################### VACUUM AND PREWARM ######################
#
# The fast count (and other in-index shortcuts of pg_search) skip the visibility check for pages that the visibility
# map marks as all-visible, and that map is only updated by VACUUM. Autovacuum starts late on big tables (by default
# after 20% of the rows changed), so after a large upload or update by query, counts can be slow (measured: on a
# freshly vacuumed table, all segments were skipped). So after large changes we vacuum the partition ourselves, in a
# job (VACUUM cannot run in a transaction, and takes a while on big partitions; requests do not wait for it).


# Single-key advisory lock per partition (this plus the partition id) for scheduling its vacuum job. (Far from the
# other single-key locks, 4_000_001 and 4_000_002; two-key locks are a separate key space)
_VACUUM_LOCK = 4_100_000_000


async def vacuum_soon(partition_ids: Iterable[int], changed_rows: int = 0) -> None:
    """
    After a write: create a vacuum job for each partition that changed a lot since its last vacuum (at least
    vacuum_after_rows rows: this change, or inserted plus dead rows according to postgres' statistics, which also
    catches many small uploads). Never raises: a write must not fail because of this.
    """
    threshold = get_settings().vacuum_after_rows
    if threshold <= 0:
        return
    from amcat4.projects.jobs import create_job_in_transaction

    try:
        for pid in set(partition_ids):
            if changed_rows < threshold:
                row = await fetch_one(
                    """SELECT n_ins_since_vacuum + n_dead_tup AS changed FROM pg_stat_user_tables
                       WHERE relid = to_regclass(%s)""",
                    [partition_table(pid)],
                )
                if row is None or row["changed"] < threshold:
                    continue
            async with connection() as conn, conn.transaction():
                # check and insert under a lock per partition, so two writes that finish at the same time do not both
                # create a job (the lock is released at commit, when the new job is visible to the next one)
                await conn.execute("SELECT pg_advisory_xact_lock(%s)", [_VACUUM_LOCK + pid])
                cur = await conn.execute(
                    """SELECT 1 FROM jobs
                       WHERE type = 'vacuum' AND status IN ('pending', 'running') AND params->>'partition_id' = %s""",
                    [str(pid)],
                )
                if await cur.fetchone() is None:
                    await create_job_in_transaction(conn, "vacuum", None, {"partition_id": pid})
    except Exception:
        logging.exception("Could not schedule a vacuum")


async def vacuum_job(job: "Job") -> dict:
    """Job: VACUUM (ANALYZE) a documents partition (params: partition_id)"""
    table = partition_table(job.params["partition_id"])
    started = datetime.now(UTC)
    async with connection() as conn:
        await conn.execute(sql.SQL("VACUUM (ANALYZE) {}").format(sql.Identifier(table)))
    return {"table": table, "seconds": round((datetime.now(UTC) - started).total_seconds(), 1)}


async def prewarm_indexes() -> list[str]:
    """
    Load the search indexes of all partitions into postgres' cache (pg_prewarm), so the first searches after a restart
    are not slow. Needs the pg_prewarm extension (created if possible). Never raises. Returns the indexes loaded.
    """
    loaded = []
    try:
        async with connection() as conn:
            cur = await conn.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_prewarm'")
            if await cur.fetchone() is None:
                await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_prewarm")
            for p in await list_partitions(conn):
                await conn.execute("SELECT pg_prewarm(%s::regclass)", [p["index"]])
                loaded.append(p["index"])
    except Exception as e:
        logging.warning(f"Could not prewarm the search indexes ({e}); continuing without")
    else:
        logging.info(f"Prewarmed search indexes: {', '.join(loaded)}")
    return loaded
