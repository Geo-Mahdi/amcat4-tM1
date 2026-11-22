"""
Tests for the fixes of the second review of the postgres-fast-v2 branch (one section per finding, numbered as in the
review list).
"""

# ruff: noqa: F811 (the fixtures are imported from other test files)

import pytest

from amcat4.config import get_settings
from amcat4.postgres.connection import connection, execute, fetch_all, fetch_one
from amcat4.projects import jobs, partitions
from amcat4.projects.query import query_documents
from tests.test_fast_path import two_projects  # noqa: F401
from tests.test_phase_c import slot_project  # noqa: F401
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


###################### 4. move-project --now runs (or waits for) this move job only ######################


async def _cli_move(monkeypatch, project: str) -> None:
    """Run `amcat4 move-project --project <project> --now` in the test process (with its connections)"""
    import argparse
    from contextlib import asynccontextmanager

    from amcat4 import __main__ as cli

    @asynccontextmanager
    async def test_connections():  # (the tests have their connections already)
        yield

    monkeypatch.setattr(cli, "amcat_connections", test_connections)
    await cli.move_project(argparse.Namespace(project=project, now=True))


@pytest.mark.anyio
async def test_move_now_runs_only_its_job(moving_projects, monkeypatch, caplog):
    """--now ran every pending job (run_pending_jobs); now it runs the move job only, and reports its result"""
    import logging

    from tests.test_review_fixes import _jobs_of

    a, b = moving_projects
    other = await jobs.create_job("copy", b, None, {"source": b, "destination": b})  # (someone else's pending job)
    with caplog.at_level(logging.INFO):
        await _cli_move(monkeypatch, a)
    [move] = await _jobs_of(a, "move_project")
    assert move["status"] == "done" and move["result"]["moved"] == 40
    assert (await jobs.get_job(other["id"]))["status"] == "pending"
    assert "Done: " in caplog.text


@pytest.mark.anyio
async def test_move_now_waits_for_the_worker(moving_projects, monkeypatch):
    """If the server's worker claimed the job first, run_job_now waits until it is done (it returned at once before)"""
    import asyncio

    a, _ = moving_projects
    job = await partitions.start_move(a)
    await execute("UPDATE jobs SET status = 'running', updated_at = now() WHERE id = %s", [job["id"]])  # (the worker)
    waiting = asyncio.create_task(jobs.run_job_now(job["id"], poll_interval=0.05))
    await asyncio.sleep(0.2)
    assert not waiting.done()  # (not claimed: another process is running it)
    await execute("UPDATE jobs SET status = 'done', result = '{}' WHERE id = %s", [job["id"]])
    assert (await asyncio.wait_for(waiting, 5))["status"] == "done"
    await execute("UPDATE projects SET moving_to = NULL WHERE id = %s", [a])


@pytest.mark.anyio
async def test_move_now_reports_failure(moving_projects, monkeypatch):
    """A failed move: --now exits with code 1 (it said "Done" before)"""

    async def fail(job):
        raise RuntimeError("the disk is full")

    monkeypatch.setitem(jobs.HANDLERS, "move_project", fail)
    a, _ = moving_projects
    with pytest.raises(SystemExit) as e:
        await _cli_move(monkeypatch, a)
    assert e.value.code == 1


###################### 5. The number of moved documents after an interruption ######################


@pytest.mark.anyio
async def test_move_count_after_interruption(moving_projects, monkeypatch):
    """
    A move job that stopped between a batch and saving its progress left that batch out of "moved" (and a job that
    resumed without a saved target started from 0). Now the moved documents are counted in the target partition
    """
    a, _ = moving_projects
    monkeypatch.setattr(partitions, "MOVE_BATCH_SIZE", 15)
    original = jobs.Job.save_progress

    async def stop_before_saving(self, **progress):  # (the server stopped right after the first batch)
        if progress.get("moved"):
            raise jobs.JobCancelled()
        await original(self, **progress)

    monkeypatch.setattr(jobs.Job, "save_progress", stop_before_saving)
    job = await partitions.start_move(a)
    await jobs.run_pending_jobs()
    monkeypatch.setattr(jobs.Job, "save_progress", original)
    assert (await jobs.get_job(job["id"]))["progress"]["moved"] == 0  # (15 were moved, but not saved)
    done = await _finish_job(job["id"])
    assert done["status"] == "done" and done["result"]["moved"] == 40  # (before: 25)


###################### 6. A request that read the partitions just before a move started ######################


@pytest.mark.anyio
async def test_move_grace_period(moving_projects, monkeypatch):
    """
    A request reads the partitions of its projects once (get_fieldset). If a move started after that and moved
    documents before the request's statement ran, the request missed them. The move now waits MOVE_GRACE_SECONDS
    between setting moving_to and the first batch
    """
    import asyncio

    from amcat4.postgres.search import SearchQuery, count
    from amcat4.systemdata.fields import get_fieldset

    a, _ = moving_projects
    monkeypatch.setattr(partitions, "MOVE_GRACE_SECONDS", 0.5)
    monkeypatch.setattr(partitions, "MOVE_BATCH_SIZE", 15)
    fieldset = await get_fieldset([a])  # (read before the move starts)
    job = await partitions.start_move(a)
    running = asyncio.create_task(jobs.run_job_now(job["id"], poll_interval=0.05))
    while not await fetch_one("SELECT 1 FROM projects WHERE id = %s AND moving_to IS NOT NULL", [a]):
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.1)  # (without the grace period, the batches run now)
    async with connection() as conn:
        assert await count(conn, fieldset, SearchQuery()) == 40
    done = await asyncio.wait_for(running, 10)
    assert done["status"] == "done" and done["result"]["moved"] == 40


###################### 7. Sort slots requested together are filled in one pass ######################


@pytest.mark.anyio
async def test_slots_filled_in_one_pass(slot_project, monkeypatch):
    """
    Every slot had its own fill job, and each job rewrote every row of the project (measured: two slots on 2.68 M
    documents took more than 10 minutes). Slots requested together (in one request, or in several before the job
    starts) are now filled by one job, in one pass over the rows
    """
    from amcat4.models import UpdateDocumentField
    from amcat4.postgres import documents as storage
    from amcat4.systemdata.fields import field_infos, update_fields
    from tests.test_phase_c import _order, _slot_values
    from tests.test_review_fixes import _jobs_of

    p = slot_project
    monkeypatch.setattr(get_settings(), "sort_slot_job_rows", 0)  # (fill in a job)
    monkeypatch.setattr(jobs, "FILL_BATCH_SIZE", 7)
    await update_fields(p, {"k1": UpdateDocumentField(fast_sort=True), "k2": UpdateDocumentField(fast_sort=True)})
    await update_fields(p, {"date2": UpdateDocumentField(fast_sort=True)})  # (a second request, before the job runs)
    [job] = await _jobs_of(p, "fill_sort_slot")
    infos = await field_infos(p)
    slots = {infos[name].sort_slot: infos[name].pk for name in ("k1", "k2", "date2")}
    assert len(slots) == 3 and (await jobs.get_job(job["id"]))["params"]["slots"] == slots

    calls = []
    original = storage.fill_sort_slots

    async def spy(conn, project_pk, slots, **kwargs):
        calls.append(sorted(slots))
        return await original(conn, project_pk, slots, **kwargs)

    monkeypatch.setattr(storage, "fill_sort_slots", spy)
    expected = {name: await _order(p, name, "desc") for name in ("k1", "k2", "date2")}  # (on the json values)
    assert await jobs.run_pending_jobs() >= 1
    [job] = await _jobs_of(p, "fill_sort_slot")
    assert job["status"] == "done" and job["result"]["batches"] == 6  # 40 documents in batches of 7, once
    assert calls == [sorted(slots)] * 7  # (six batches and the empty seventh), every statement with all slots
    infos = await field_infos(p)
    assert all(infos[name].sort_ready and infos[name].sort_read_column for name in ("k1", "k2", "date2"))
    values = set((await _slot_values(p, infos["k1"].sort_slot)).values())  # type: ignore[arg-type]
    assert values - {None} == {f"v{i}" for i in range(17)}
    for name, order in expected.items():
        assert await _order(p, name, "desc") == order


@pytest.mark.anyio
async def test_slot_not_added_to_started_job(slot_project, monkeypatch):
    """A fill job that already started (it has progress) gets no more slots: it would miss the rows it did already"""
    from amcat4.models import UpdateDocumentField
    from amcat4.systemdata.fields import update_fields
    from tests.test_review_fixes import _jobs_of

    p = slot_project
    monkeypatch.setattr(get_settings(), "sort_slot_job_rows", 0)
    await update_fields(p, {"k1": UpdateDocumentField(fast_sort=True)})
    [job] = await _jobs_of(p, "fill_sort_slot")
    await execute("""UPDATE jobs SET progress = '{"after_id": 1, "postponed": "moving"}' WHERE id = %s""", [job["id"]])
    await update_fields(p, {"k2": UpdateDocumentField(fast_sort=True)})
    assert len(await _jobs_of(p, "fill_sort_slot")) == 2
