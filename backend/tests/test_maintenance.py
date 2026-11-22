from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import pytest
from psycopg import sql

from amcat4.models import ProjectSettings
from amcat4.postgres.connection import connection, execute, fetch_all
from amcat4.postgres.layout import index_segments, list_partitions, partition_of
from amcat4.postgres.projects import project_pk
from amcat4.projects import jobs, maintenance
from amcat4.projects.index import create_project_index, delete_project_index
from tests.conftest import upload


async def _partition_index(index: str) -> tuple[int, str]:
    async with connection() as conn:
        partition_id = await partition_of(conn, await project_pk(index))
        partition = next(p for p in await list_partitions(conn) if p["partition_id"] == partition_id)
    return partition_id, partition["index"]


@asynccontextmanager
async def no_background_merging(index: str):
    """
    Switch off pg_search's background merging for the index of the project's partition, so every upload leaves its
    own segment, as on pg_search 0.26 (from 0.27 on, segments are merged within seconds). The option exists in both
    versions, so the tests check the same behaviour on both.
    """

    async def set_option(reset: bool):
        _, name = await _partition_index(index)
        option = "RESET (background_layer_sizes)" if reset else "SET (background_layer_sizes = '0')"
        await execute(sql.SQL("ALTER INDEX {} " + option).format(sql.Identifier(name)))  # type: ignore[arg-type]

    await set_option(False)
    try:
        yield
    finally:
        await set_option(True)


@pytest.fixture
async def clean_jobs():
    await execute("DELETE FROM jobs WHERE type = 'reindex'")
    await execute("DELETE FROM periodic_tasks")
    yield
    await execute("DELETE FROM jobs WHERE type = 'reindex'")
    await execute("DELETE FROM periodic_tasks")


@pytest.mark.anyio
async def test_needs_reindex():
    assert maintenance.needs_reindex({"segments": 10, "documents": 100, "deleted": 0}, None) is None
    assert maintenance.needs_reindex({"segments": 20, "documents": 100, "deleted": 0}, None) == "20 segments"
    assert maintenance.needs_reindex({"segments": 50, "documents": 100, "deleted": 0}, 30) is None
    assert maintenance.needs_reindex({"segments": 61, "documents": 100, "deleted": 0}, 30) == "61 segments"
    assert maintenance.needs_reindex({"segments": 1, "documents": 100, "deleted": 30}, None) == "30 deleted documents"


@pytest.mark.anyio
async def test_periodic_tasks(clean_jobs, monkeypatch):
    calls = []

    async def task(state: dict) -> dict:
        calls.append(state)
        return {"n": state.get("n", 0) + 1}

    monkeypatch.setattr(jobs, "PERIODIC", {"test": jobs.PeriodicTask(interval=timedelta(hours=1), run=task)})
    assert await jobs.run_periodic_tasks() == ["test"]
    assert await jobs.run_periodic_tasks() == []  # not due yet
    await execute("UPDATE periodic_tasks SET last_run = now() - interval '2 hours'")
    assert await jobs.run_periodic_tasks() == ["test"]
    assert calls == [{}, {"n": 1}]  # the state is kept between runs


@pytest.mark.anyio
async def test_reindex_when_quiet(clean_jobs):
    index = "amcat4_unittest_maintenance"
    await delete_project_index(index, ignore_missing=True)
    try:
        await create_project_index(ProjectSettings(id=index))
        async with no_background_merging(index):
            for i in range(maintenance.MIN_SEGMENTS + 1):  # every upload adds a segment
                await upload(index, [{"text": f"document {i}"}], fields={"text": "text"})
            partition_id, name = await _partition_index(index)
            async with connection() as conn:
                assert (await index_segments(conn, name))["segments"] > maintenance.MIN_SEGMENTS
            await _check_reindex_when_quiet(partition_id)
    finally:
        await delete_project_index(index, ignore_missing=True)


async def _check_reindex_when_quiet(partition_id: int, reason: str = "segments"):
    """The partition needs a rebuild (for this reason): a reindex job is created once it is quiet, and it works"""

    def reindex_jobs():
        return fetch_all("SELECT status, params, result FROM jobs WHERE type = 'reindex'")

    # the partition just changed: wait until it is quiet
    t0 = datetime.now(UTC)
    state = await maintenance.check_partitions({}, now=t0)
    assert await reindex_jobs() == []
    state = await maintenance.check_partitions(state, now=t0 + timedelta(minutes=5))
    assert await reindex_jobs() == []
    state = await maintenance.check_partitions(state, now=t0 + maintenance.QUIET_PERIOD + timedelta(minutes=1))
    [job] = await reindex_jobs()
    assert job["status"] == "pending" and job["params"]["partition_id"] == partition_id
    if reason != "segments":  # (other tests can leave deleted documents in the shared partition, which also count)
        assert job["params"]["reason"].endswith(reason)

    assert await jobs.run_pending_jobs() == 1
    [job] = await reindex_jobs()
    assert job["status"] == "done"
    if reason == "segments":
        assert job["result"]["segments_after"] < job["result"]["segments_before"]
    else:
        assert job["result"]["deleted_before"] > 0
        async with connection() as conn:
            [partition] = [p for p in await list_partitions(conn) if p["partition_id"] == partition_id]
            assert (await index_segments(conn, partition["index"]))["deleted"] == 0

    # after the rebuild, the partition doesn't need another one
    await maintenance.check_partitions(state, now=t0 + timedelta(hours=1))
    assert len(await reindex_jobs()) == 1


@pytest.mark.anyio
async def test_reindex_if_never_quiet(clean_jobs):
    """A partition that keeps changing is rebuilt anyway once it needed a rebuild for MAX_WAIT"""
    index = "amcat4_unittest_maintenance"
    await delete_project_index(index, ignore_missing=True)
    try:
        await create_project_index(ProjectSettings(id=index))
        async with no_background_merging(index):
            for i in range(maintenance.MIN_SEGMENTS + 1):
                await upload(index, [{"text": f"document {i}"}], fields={"text": "text"})
            t0 = datetime.now(UTC)
            state = await maintenance.check_partitions({}, now=t0)
            await upload(index, [{"text": "another document"}], fields={"text": "text"})
            await maintenance.check_partitions(state, now=t0 + maintenance.MAX_WAIT + timedelta(minutes=1))
            assert len(await fetch_all("SELECT 1 FROM jobs WHERE type = 'reindex'")) == 1
    finally:
        await delete_project_index(index, ignore_missing=True)


@pytest.mark.anyio
async def test_reindex_deleted_documents(clean_jobs):
    """Many deleted documents (e.g. after a mass update or delete) also lead to a rebuild, on every pg_search version"""
    index = "amcat4_unittest_maintenance"
    await delete_project_index(index, ignore_missing=True)
    try:
        await create_project_index(ProjectSettings(id=index))
        async with no_background_merging(index):
            # one segment, so the segment trigger cannot fire
            await upload(index, [{"_id": str(i), "text": f"document {i}"} for i in range(200)], fields={"text": "text"})
            partition_id, name = await _partition_index(index)
            async with connection() as conn:
                before = await index_segments(conn, name)
                await conn.execute(
                    "DELETE FROM documents WHERE project_pk = %s AND doc_id::int < 150", [await project_pk(index)]
                )
                # pg_search marks documents in its segments as deleted when the table is vacuumed
                await conn.execute(sql.SQL("VACUUM {}").format(sql.Identifier(f"documents_p{partition_id}")))
                after = await index_segments(conn, name)
            assert after["deleted"] >= 150 and after["segments"] <= before["segments"] + 1
            assert after["deleted"] > maintenance.MAX_DELETED * after["documents"]
            await _check_reindex_when_quiet(partition_id, reason="deleted documents")
    finally:
        await delete_project_index(index, ignore_missing=True)


@pytest.mark.anyio
async def test_merging_is_activity(clean_jobs, monkeypatch):
    """While pg_search merges segments in the background, a partition does not count as quiet"""
    index = "amcat4_unittest_maintenance"
    await delete_project_index(index, ignore_missing=True)
    try:
        await create_project_index(ProjectSettings(id=index))
        async with no_background_merging(index):
            for i in range(maintenance.MIN_SEGMENTS + 1):
                await upload(index, [{"text": f"document {i}"}], fields={"text": "text"})

        async def merging(conn, name):
            return True

        monkeypatch.setattr(maintenance, "index_merging", merging)
        t0 = datetime.now(UTC)
        state = await maintenance.check_partitions({}, now=t0)
        state = await maintenance.check_partitions(state, now=t0 + maintenance.QUIET_PERIOD + timedelta(minutes=1))
        assert await fetch_all("SELECT 1 FROM jobs WHERE type = 'reindex'") == []
    finally:
        await delete_project_index(index, ignore_missing=True)
