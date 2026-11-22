"""
Tests for the fixes of the second review of the postgres-fast-v2 branch (one section per finding, numbered as in the
review list).
"""

import pytest

from amcat4.config import get_settings
from amcat4.postgres.connection import connection, fetch_all, fetch_one
from tests.test_fast_path import two_projects  # noqa: F401
from tests.test_review_fixes import _stop_move_after_first_batch


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
