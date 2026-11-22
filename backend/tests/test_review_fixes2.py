"""
Tests for the fixes of the second review of the postgres-fast-v2 branch (one section per finding, numbered as in the
review list).
"""

import pytest

from amcat4.config import get_settings
from amcat4.postgres.connection import connection, execute, fetch_all, fetch_one
from amcat4.projects import jobs
from amcat4.projects.query import query_documents
from tests.test_fast_path import two_projects  # noqa: F401
from tests.test_review_fixes import _finish_job, _make_claimable, _stop_move_after_first_batch


@pytest.fixture()
async def moving_projects(two_projects):  # noqa: F811
    """Two projects in one partition (40 and 25 documents); cleans up the jobs a move leaves behind"""
    yield two_projects
    await fetch_all("DELETE FROM jobs WHERE type IN ('move_project', 'vacuum', 'copy', 'fill_sort_slot') RETURNING id")


###################### 1 and 2. Project size and partition overview during a move ######################


@pytest.mark.anyio
async def test_size_during_move(moving_projects, monkeypatch):
    """
    The estimated project size counted and sampled only projects.partition_id: during a move the documents already in
    the target partition were left out (the size shrank). Now both partitions count
    """
    from amcat4.projects import index as index_module

    a, _ = moving_projects
    monkeypatch.setattr(index_module, "SIZE_SAMPLE", 20)  # (the estimate, not the exact sum: 40 documents > 20)
    await _stop_move_after_first_batch(monkeypatch, a)  # 15 documents moved, 25 left
    estimate = await index_module.index_size_in_bytes(a)
    monkeypatch.setattr(get_settings(), "fast_path", False)  # the exact sum (over both partitions)
    exact = await index_module.index_size_in_bytes(a)
    assert abs(estimate - exact) < 0.15 * exact, (estimate, exact)  # (before: about 25/40 of the exact size)


@pytest.mark.anyio
async def test_partition_overview_during_move(moving_projects, monkeypatch):
    """The partition overview counted a moving project's documents only in its old partition; now in both"""
    from amcat4.postgres.layout import partition_overview

    a, _ = moving_projects
    await _stop_move_after_first_batch(monkeypatch, a)
    row = await fetch_one("SELECT partition_id, moving_to FROM projects WHERE id = %s", [a])
    assert row is not None and row["moving_to"] is not None
    async with connection() as conn:
        overview = {p["partition_id"]: p for p in await partition_overview(conn)}
    counts = {
        pid: next((m["documents"] for m in overview[pid]["projects"] if m["project"] == a), None)
        for pid in (row["partition_id"], row["moving_to"])
    }
    assert counts == {row["partition_id"]: 25, row["moving_to"]: 15}
    assert overview[row["moving_to"]]["documents"] == 15


###################### 3. A copy job waits for a move of its source or destination ######################


@pytest.mark.anyio
async def test_copy_postponed_while_destination_moves(moving_projects, monkeypatch):
    """
    A copy whose destination started moving failed for good (ConflictError from the refused write), and a copy from a
    moving source failed too. Now the job is postponed and continues after the move
    """
    a, b = moving_projects
    monkeypatch.setattr(jobs, "BATCH_SIZE", 7)
    job = await jobs.create_job("copy", b, None, {"source": a, "destination": b})
    original = jobs.Job.save_progress

    async def move_starts(self, **progress):  # (as if a move of the destination set moving_to now)
        await original(self, **progress)
        if progress.get("copied"):
            await execute("UPDATE projects SET moving_to = partition_id WHERE id = %s", [b])

    monkeypatch.setattr(jobs.Job, "save_progress", move_starts)
    try:
        await jobs.run_pending_jobs()
        copy = await jobs.get_job(job["id"])
        assert copy["status"] == "pending" and "being moved" in copy["progress"]["postponed"], copy
        assert copy["progress"]["copied"] == 7  # one batch, then the next write was refused
        # still moving: postponed again, at the start
        await _make_claimable(job["id"])
        await jobs.run_pending_jobs()
        assert (await jobs.get_job(job["id"]))["status"] == "pending"
    finally:
        await execute("UPDATE projects SET moving_to = NULL WHERE id = %s", [b])
    monkeypatch.setattr(jobs.Job, "save_progress", original)
    await _make_claimable(job["id"])
    await jobs.run_pending_jobs()
    copy = await jobs.get_job(job["id"])
    assert copy["status"] == "done" and copy["result"]["copied"] == 40
    assert (await query_documents(b, per_page=1)).total_count == 25 + 40


@pytest.mark.anyio
async def test_copy_postponed_while_source_moves(moving_projects, monkeypatch):
    """A copy from a project that is being moved is postponed (it failed before), and runs after the move"""
    a, b = moving_projects
    move = await _stop_move_after_first_batch(monkeypatch, a)
    job = await jobs.create_job("copy", b, None, {"source": a, "destination": b})
    await jobs.run_pending_jobs()
    assert (await jobs.get_job(job["id"]))["status"] == "pending"
    assert (await _finish_job(move["id"]))["status"] == "done"
    await _make_claimable(job["id"])
    await jobs.run_pending_jobs()
    copy = await jobs.get_job(job["id"])
    assert copy["status"] == "done" and copy["result"]["copied"] == 40
