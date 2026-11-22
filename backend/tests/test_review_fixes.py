"""
Tests for the fixes after the code review of the postgres-fast-v2 branch (one section per finding).
"""

import pytest

from amcat4.config import get_settings
from amcat4.models import FieldSpec, ProjectSettings, SortSpec, UpdateDocumentField
from amcat4.postgres.search import slot_sort_column
from amcat4.projects.index import create_project_index, delete_project_index
from amcat4.projects.query import query_documents
from amcat4.systemdata.fields import get_fieldset, list_fields, update_fields
from tests.conftest import upload
from tests.test_fast_path import two_projects  # noqa: F401


@pytest.fixture()
async def two():
    names = ["amcat4_unittest_review_a", "amcat4_unittest_review_b"]
    for name in names:
        await delete_project_index(name, ignore_missing=True)
        await create_project_index(ProjectSettings(id=name))
    yield names
    for name in names:
        await delete_project_index(name, ignore_missing=True)


async def _ids(index, **kwargs) -> list[str]:
    r = await query_documents(index, per_page=100, **kwargs)
    return [d["_id"] for d in r.data]


###################### 1. Sort slots in a multi-project query ######################


@pytest.mark.anyio
@pytest.mark.parametrize("fast_path", [True, False])
async def test_slot_sort_other_project_same_slot(two, monkeypatch, fast_path):
    """
    A slot column holds a different field in another project: a sort on one project's field must not order the other
    project's documents by that (maybe hidden) field. They have no value, so they come last (NULLS LAST), by id.
    """
    monkeypatch.setattr(get_settings(), "fast_path", fast_path)
    a, b = two
    await upload(a, [{"_id": f"a{i}", "n": i * 10} for i in range(3)], fields={"n": "integer"})
    await upload(b, [{"_id": f"b{i}", "secret": 100 - i * 10} for i in range(3)], fields={"secret": "integer"})
    await update_fields(a, {"n": UpdateDocumentField(fast_sort=True)})
    await update_fields(b, {"secret": UpdateDocumentField(fast_sort=True)})
    assert (await list_fields(a))["n"].sort_slot == (await list_fields(b))["secret"].sort_slot == "sort_num1"
    fieldset = await get_fieldset([a, b])
    assert slot_sort_column(fieldset, "n") is None
    assert slot_sort_column(await get_fieldset([a]), "n") == "sort_num1"
    asc = await _ids([a, b], sort=[{"n": SortSpec(order="asc")}], fields=[FieldSpec(name="n")])
    assert asc == ["a0", "a1", "a2", "b0", "b1", "b2"]
    desc = await _ids([a, b], sort=[{"n": SortSpec(order="desc")}], fields=[FieldSpec(name="n")])
    assert desc == ["a2", "a1", "a0", "b0", "b1", "b2"]


@pytest.mark.anyio
async def test_slot_sort_auto_mapped_date(two):
    """The automatic date slot: another project's first date field (with another name) is in the same column"""
    a, b = two
    await upload(a, [{"_id": f"a{i}", "date": f"2020-01-0{i + 1}"} for i in range(3)], fields={"date": "date"})
    await upload(b, [{"_id": f"b{i}", "published": f"2019-01-0{3 - i}"} for i in range(3)], fields={"published": "date"})
    assert (await list_fields(a))["date"].sort_slot == (await list_fields(b))["published"].sort_slot == "date"
    assert slot_sort_column(await get_fieldset([a, b]), "date") is None
    assert await _ids([a, b], sort=[{"date": SortSpec(order="asc")}]) == ["a0", "a1", "a2", "b0", "b1", "b2"]
    assert await _ids([a, b], sort=[{"published": SortSpec(order="desc")}]) == ["b0", "b1", "b2", "a0", "a1", "a2"]


@pytest.mark.anyio
async def test_slot_sort_used_when_all_projects_have_it(two):
    """If every project of the query has the field in the same filled slot, the slot column is used"""
    a, b = two
    await upload(a, [{"_id": f"a{i}", "date": f"2020-01-0{i + 1}"} for i in range(3)], fields={"date": "date"})
    await upload(b, [{"_id": f"b{i}", "date": f"2020-01-0{i + 4}"} for i in range(3)], fields={"date": "date"})
    assert slot_sort_column(await get_fieldset([a, b]), "date") == "date"
    assert await _ids([a, b], sort=[{"date": SortSpec(order="desc")}]) == ["b2", "b1", "b0", "a2", "a1", "a0"]


###################### 2, 3, 4. Sorting on _id and _seq, and the page depth limit ######################


async def _all_pages(index, per_page=2, **kwargs) -> list[str]:
    """All documents, following the 'next' cursors"""
    r = await query_documents(index, per_page=per_page, **kwargs)
    ids = [d["_id"] for d in r.data]
    while r.next:
        r = await query_documents(index, per_page=per_page, after=r.next, **kwargs)
        ids += [d["_id"] for d in r.data]
    return ids


ID_ORDER = ["a10", "a9", "b", "c1"]  # the doc_id (text) order of the documents below, which are stored as b, c1, a9, a10


@pytest.mark.anyio
@pytest.mark.parametrize("fast_path", [True, False])
async def test_sort_id_is_doc_id(two, monkeypatch, fast_path):
    """sort on _id is the order of the document ids (as before), with page numbers and offset cursors"""
    monkeypatch.setattr(get_settings(), "fast_path", fast_path)
    a, _ = two
    await upload(a, [{"_id": i, "text": "x"} for i in ["b", "c1", "a9", "a10"]], fields={"text": "text"})
    for order, expected in (("asc", ID_ORDER), ("desc", ID_ORDER[::-1])):
        sort = [{"_id": SortSpec(order=order)}]  # type: ignore[arg-type]
        assert await _ids(a, sort=sort) == expected
        p0 = await query_documents(a, sort=sort, page=0, per_page=2)
        p1 = await query_documents(a, sort=sort, page=1, per_page=2)
        assert [d["_id"] for d in p0.data] + [d["_id"] for d in p1.data] == expected
        assert await _all_pages(a, sort=sort) == expected
        # with a query, too
        assert await _all_pages(a, sort=sort, queries={"q": "x"}) == expected


@pytest.mark.anyio
async def test_sort_seq(two):
    """_seq is the storage order with a keyset cursor at page 0 (or with a keyset cursor), and offset paging for page > 0"""
    from amcat4.projects.query import _decode_cursor

    a, _ = two
    stored = ["b", "c1", "a9", "a10", "d", "e"]
    await upload(a, [{"_id": i, "text": "x"} for i in stored], fields={"text": "text"})
    for order, expected in (("asc", stored), ("desc", stored[::-1])):
        sort = [{"_seq": SortSpec(order=order)}]  # type: ignore[arg-type]
        r = await query_documents(a, sort=sort, per_page=2, queries={"q": "x"})
        assert r.next is not None and "id" in _decode_cursor(r.next)
        assert await _all_pages(a, sort=sort, queries={"q": "x"}) == expected
        assert await _all_pages(a, sort=sort) == expected
        pages = [await query_documents(a, sort=sort, page=p, per_page=2, queries={"q": "x"}) for p in range(3)]
        assert [d["_id"] for p in pages for d in p.data] == expected
        assert all(p.total_count == 6 for p in pages)
        # the cursor of a page number continues after that page (as a keyset cursor)
        assert pages[1].next is not None and "id" in _decode_cursor(pages[1].next)
        rest = await query_documents(a, sort=sort, per_page=10, after=pages[1].next, queries={"q": "x"})
        assert [d["_id"] for d in rest.data] == expected[4:]


@pytest.mark.anyio
async def test_cursor_beyond_max_offset(two, monkeypatch):
    """The 'next' cursors of a query without sort (offset cursors) are not limited by max_offset; page numbers are"""
    monkeypatch.setattr(get_settings(), "max_offset", 4)
    a, _ = two
    await upload(a, [{"_id": f"d{i}", "text": "word"} for i in range(11)], fields={"text": "text"})
    ids = await _all_pages(a, queries={"q": "word"})
    assert sorted(ids) == sorted(f"d{i}" for i in range(11)) and len(ids) == 11
    ids = await _all_pages(a, queries={"q": "word"}, sort=[{"_id": SortSpec(order="asc")}])
    assert ids == sorted(f"d{i}" for i in range(11))
    with pytest.raises(ValueError, match='"sort": "_seq"'):
        await query_documents(a, queries={"q": "word"}, page=2, per_page=2)
    with pytest.raises(ValueError, match='"sort": "_seq"'):
        await query_documents(a, sort=[{"_seq": SortSpec(order="asc")}], page=2, per_page=2)


###################### 5. pdb.agg metrics of a bucket without values ######################


@pytest.mark.anyio
async def test_index_metrics_empty_bucket(two, monkeypatch):
    """In-index metrics (pdb.agg) of buckets where no document has the field: null, the same as GROUP BY"""
    from amcat4.postgres import search as search_module
    from amcat4.projects import aggregate as aggregate_module
    from amcat4.projects.aggregate import Aggregation, Axis, TopHitsAggregation, query_aggregate

    monkeypatch.setattr(aggregate_module, "SMALL_RESULT", 0)
    monkeypatch.setattr(search_module, "COUNT_STEP", 0)
    used: list = []
    original = aggregate_module.index_aggregate

    async def spy(*args, **kwargs):
        rows = await original(*args, **kwargs)
        used.append(rows is not None)
        return rows

    monkeypatch.setattr(aggregate_module, "index_aggregate", spy)
    a, _ = two
    docs = [{"_id": f"d{i}", "pub": "AB"[i // 4], "cat": "xy"[i % 2], **({"n": i - 2} if i < 4 else {})} for i in range(8)]
    await upload(a, docs, fields={"pub": "keyword", "cat": "keyword", "n": "integer"})
    aggs: list[Aggregation | TopHitsAggregation] = [Aggregation("n", f) for f in ("sum", "avg", "min", "max")]
    for axes in ([Axis("pub")], [Axis("pub"), Axis("cat")]):
        used.clear()
        fast = sorted((await query_aggregate(a, axes, aggs)).data)
        assert used == [True]  # pdb.agg was used, not the fallback
        monkeypatch.setattr(get_settings(), "fast_path", False)
        slow = sorted((await query_aggregate(a, axes, aggs)).data)
        monkeypatch.setattr(get_settings(), "fast_path", True)
        assert fast == slow
        assert all(row[-4:] == (None, None, None, None) for row in fast if row[0] == "B")
    assert ("A", 4, -2.0 + -1 + 0 + 1, -0.5, -2.0, 1.0) in sorted((await query_aggregate(a, [Axis("pub")], aggs)).data)


###################### 6. No slower fallback after a statement timeout or a connection problem ######################


class _FailingConnection:
    """A connection that raises `error` for statements for which marker(sql) is true (and counts them)"""

    def __init__(self, conn, marker, error: Exception):
        self.conn, self.marker, self.error, self.failed = conn, marker, error, 0

    async def execute(self, stmt, params=None):
        text = stmt if isinstance(stmt, str) else stmt.as_string(self.conn)
        if self.marker(text):
            self.failed += 1
            raise self.error
        return await self.conn.execute(stmt, params)


def _failing(monkeypatch, module, marker, error: Exception) -> list[_FailingConnection]:
    from contextlib import asynccontextmanager

    from amcat4.postgres.connection import connection

    made: list[_FailingConnection] = []

    @asynccontextmanager
    async def failing_connection():
        async with connection() as conn:
            made.append(_FailingConnection(conn, marker, error))
            yield made[-1]

    monkeypatch.setattr(module, "connection", failing_connection)
    return made


@pytest.mark.anyio
async def test_statement_timeout_is_not_a_fallback_error():
    """A real statement timeout is QueryCanceled, which must_not_fall_back recognizes"""
    import psycopg

    from amcat4.postgres.connection import connection, must_not_fall_back

    async with connection() as conn:
        await conn.execute("SET statement_timeout = 10")
        try:
            with pytest.raises(psycopg.errors.QueryCanceled) as e:
                await conn.execute("SELECT pg_sleep(1)")
        finally:
            await conn.execute("RESET statement_timeout")
    assert must_not_fall_back(e.value)
    assert must_not_fall_back(psycopg.OperationalError("connection lost"))
    assert not must_not_fall_back(psycopg.errors.InternalError_("too many buckets"))
    assert not must_not_fall_back(psycopg.errors.FeatureNotSupported("not in the index"))


IN_INDEX = {
    # pdb.agg (a keyword axis)
    "pdb.agg": ("pub", lambda text: "pdb.agg" in text),
    # GROUP BY on the partition table (an integer axis, which pdb.agg does not do)
    "in-index GROUP BY": ("n", lambda text: "GROUP BY" in text and '"documents_p' in text),
}


@pytest.mark.anyio
@pytest.mark.parametrize("path", IN_INDEX.keys())
async def test_aggregate_fallbacks(two, monkeypatch, path):
    """
    The in-index aggregations (pdb.agg; GROUP BY on the partition table) fall back to slower statements after a query
    error, but raise a statement timeout or connection error at once (no second statement)
    """
    import psycopg

    from amcat4.postgres import search as search_module
    from amcat4.projects import aggregate as aggregate_module
    from amcat4.projects.aggregate import Axis, query_aggregate

    monkeypatch.setattr(aggregate_module, "SMALL_RESULT", 0)
    monkeypatch.setattr(search_module, "COUNT_STEP", 0)
    axis, marker = IN_INDEX[path]
    a, _ = two
    await upload(
        a, [{"_id": f"d{i}", "pub": "AB"[i % 2], "n": i % 3} for i in range(9)], fields={"pub": "keyword", "n": "integer"}
    )
    expected = sorted((await query_aggregate(a, [Axis(axis)])).data)
    # a query error: the fallback gives the same result
    made = _failing(monkeypatch, aggregate_module, marker, psycopg.errors.InternalError_("in-index limit"))
    assert sorted((await query_aggregate(a, [Axis(axis)])).data) == expected
    assert made[-1].failed == 1
    # a statement timeout, or a lost connection: raised, without a second (slower) statement
    for error in (psycopg.errors.QueryCanceled("statement timeout"), psycopg.OperationalError("connection lost")):
        made = _failing(monkeypatch, aggregate_module, marker, error)
        with pytest.raises(type(error)):
            await query_aggregate(a, [Axis(axis)])
        assert made[-1].failed == 1


###################### 7. Long field writes with the server's settings ######################


@pytest.mark.anyio
async def test_create_unique_field_uses_server_defaults(two, monkeypatch):
    """A new unique field hashes every document: on the work pool, without the API's statement timeout"""
    from amcat4 import connections
    from amcat4.systemdata import fields as fields_module

    a, _ = two
    seen = []
    original = fields_module.update_dedup_hashes

    async def spy(conn, *args, **kwargs):
        seen.append(connections.db() is connections.CONNECTIONS.db_work)
        return await original(conn, *args, **kwargs)

    monkeypatch.setattr(fields_module, "update_dedup_hashes", spy)
    from amcat4.models import CreateDocumentField

    await fields_module.create_fields(a, {"u": CreateDocumentField(type="keyword", unique=True)})
    assert seen == [True]


###################### 9 and 10. Reading the partitions of a project during a move; resuming step 1 ######################


async def _stop_move_after_first_batch(monkeypatch, project: str) -> dict:
    """Start a move of the project, and stop the job after its first batch (as if the server stopped)"""
    from amcat4.projects import jobs, partitions

    monkeypatch.setattr(partitions, "MOVE_BATCH_SIZE", 15)
    original = jobs.Job.save_progress

    async def stop_after_first_batch(self, **progress):
        await original(self, **progress)
        if progress.get("moved"):
            raise jobs.JobCancelled()

    monkeypatch.setattr(jobs.Job, "save_progress", stop_after_first_batch)
    job = await partitions.start_move(project)
    await jobs.run_pending_jobs()
    monkeypatch.setattr(jobs.Job, "save_progress", original)
    return job


async def _finish_job(job_id: str) -> dict:
    from amcat4.postgres.connection import execute
    from amcat4.projects import jobs

    await execute("UPDATE jobs SET status = 'pending' WHERE id = %s", [job_id])
    await jobs.run_pending_jobs()
    return await jobs.get_job(job_id)


@pytest.fixture()
async def moving_projects(two_projects):  # noqa: F811
    from amcat4.postgres.connection import fetch_all

    yield two_projects
    await fetch_all("DELETE FROM jobs WHERE type IN ('move_project', 'vacuum') RETURNING id")


@pytest.mark.anyio
async def test_fieldset_partitions_one_snapshot(moving_projects, monkeypatch):
    """A move that finishes while the fieldset is read: the request still sees every document of the project"""
    from amcat4.postgres import projects as projects_module
    from amcat4.postgres.connection import connection
    from amcat4.postgres.search import SearchQuery, count

    a, _ = moving_projects
    job = await _stop_move_after_first_batch(monkeypatch, a)
    original = projects_module.fetch_all
    finished: list = []

    async def finish_move_after_reading_partitions(query, params=None):
        rows = await original(query, params)
        if "partition_id" in query and not finished:
            finished.append(True)
            monkeypatch.setattr(projects_module, "fetch_all", original)  # (the job reads partitions too)
            assert (await _finish_job(job["id"]))["status"] == "done"
        return rows

    monkeypatch.setattr(projects_module, "fetch_all", finish_move_after_reading_partitions)
    fieldset = await get_fieldset([a])
    assert finished
    async with connection() as conn:
        assert await count(conn, fieldset, SearchQuery()) == 40


async def _partition_ids() -> list[int]:
    from amcat4.postgres.connection import connection
    from amcat4.postgres.layout import list_partitions

    async with connection() as conn:
        return [p["partition_id"] for p in await list_partitions(conn)]


@pytest.mark.anyio
async def test_move_step1_resume(moving_projects, monkeypatch):
    """A move that stops right after step 1 resumes with the partition it created: no second (orphan) partition"""
    from amcat4.postgres.connection import execute, fetch_one
    from amcat4.postgres.projects import project_pk
    from amcat4.projects import jobs, partitions

    a, _ = moving_projects
    before = await _partition_ids()
    original = jobs.Job.save_progress

    # stop at the first save_progress, after step 1 and the first batch: the target was stored with step 1 itself
    async def stop(self, **progress):
        raise jobs.JobCancelled()

    monkeypatch.setattr(partitions, "MOVE_BATCH_SIZE", 15)
    monkeypatch.setattr(jobs.Job, "save_progress", stop)
    job = await partitions.start_move(a)
    await jobs.run_pending_jobs()
    monkeypatch.setattr(jobs.Job, "save_progress", original)
    pk = await project_pk(a)
    row = await fetch_one("SELECT moving_to FROM projects WHERE pk = %s", [pk])
    progress = (await jobs.get_job(job["id"]))["progress"]
    assert row is not None and progress["target"] == row["moving_to"] and progress["moved"] == 0
    assert len(await _partition_ids()) == len(before) + 1

    # a job of the older version saved the target after step 1: no target in the progress, but moving_to is set
    await execute("UPDATE jobs SET progress = '{}' WHERE id = %s", [job["id"]])
    done = await _finish_job(job["id"])
    assert done["status"] == "done" and done["result"]["moved"] == 40 - 15  # (moved since the progress was lost)
    assert len(await _partition_ids()) == len(before) + 1
    row = await fetch_one("SELECT partition_id, moving_to FROM projects WHERE pk = %s", [pk])
    assert row == {"partition_id": progress["target"], "moving_to": None}
    assert (await query_documents(a, per_page=100)).total_count == 40


###################### 8. Fill and backfill jobs during a move: postponed, not failed ######################


async def _jobs_of(index: str, type: str) -> list[dict]:
    from amcat4.postgres.connection import fetch_all
    from amcat4.postgres.projects import project_pk

    return await fetch_all(
        "SELECT id, status, progress, result, error FROM jobs WHERE type = %s AND project_pk = %s ORDER BY created_at",
        [type, await project_pk(index)],
    )


async def _make_claimable(job_id: str) -> None:
    """A postponed job waits POSTPONE_FOR before it is claimed again: let that time pass"""
    from amcat4.postgres.connection import execute

    await execute("UPDATE jobs SET updated_at = now() - interval '1 hour' WHERE id = %s", [job_id])


@pytest.mark.anyio
async def test_fill_and_backfill_jobs_wait_for_a_move(moving_projects, monkeypatch):
    from amcat4.postgres.connection import connection, fetch_all
    from amcat4.postgres.projects import project_pk
    from amcat4.projects import jobs
    from amcat4.systemdata.fields import field_infos
    from tests.test_phase_c import _remove_quarter_keys

    a, _ = moving_projects
    pk = await project_pk(a)
    await _remove_quarter_keys(a)
    monkeypatch.setattr(get_settings(), "sort_slot_job_rows", 0)  # fill sort slots in a job
    move = await _stop_move_after_first_batch(monkeypatch, a)  # halfway the move

    # the scheduler does not start a backfill for a project that is being moved
    await jobs.schedule_quarter_backfill({})
    assert await _jobs_of(a, "backfill_quarter") == []
    # jobs that were created anyway (e.g. before the move) are postponed, not failed, and not claimed again at once
    await update_fields(a, {"pub": UpdateDocumentField(fast_sort=True)})
    async with connection() as conn:
        await jobs.create_job_in_transaction(conn, "backfill_quarter", pk, {"project_pk": pk})
    assert await jobs.run_pending_jobs() == 2
    assert await jobs.run_pending_jobs() == 0
    [fill], [backfill] = await _jobs_of(a, "fill_sort_slot"), await _jobs_of(a, "backfill_quarter")
    for job in (fill, backfill):
        assert job["status"] == "pending" and "being moved" in job["progress"]["postponed"]

    # after the move, they run
    assert (await _finish_job(move["id"]))["status"] == "done"
    for job in (fill, backfill):
        await _make_claimable(job["id"])
    assert await jobs.run_pending_jobs() >= 2
    [fill], [backfill] = await _jobs_of(a, "fill_sort_slot"), await _jobs_of(a, "backfill_quarter")
    assert fill["status"] == backfill["status"] == "done"
    assert "postponed" not in fill["progress"] and "postponed" not in backfill["progress"]
    infos = await field_infos(a)
    assert infos["pub"].sort_ready and infos["date"].quarter_ready
    await fetch_all("DELETE FROM jobs WHERE type IN ('fill_sort_slot', 'backfill_quarter') RETURNING id")


@pytest.mark.anyio
async def test_fill_job_postponed_between_batches(moving_projects, monkeypatch):
    """A move that starts while the fill job runs: the next batch is refused, and the job is postponed"""
    from amcat4.postgres.connection import execute, fetch_all
    from amcat4.projects import jobs

    a, _ = moving_projects
    monkeypatch.setattr(get_settings(), "sort_slot_job_rows", 0)
    monkeypatch.setattr(jobs, "FILL_BATCH_SIZE", 7)
    await update_fields(a, {"pub": UpdateDocumentField(fast_sort=True)})
    original = jobs.Job.save_progress

    async def move_starts(self, **progress):  # (as if the move job set moving_to now)
        await original(self, **progress)
        await execute("UPDATE projects SET moving_to = partition_id WHERE id = %s", [a])

    monkeypatch.setattr(jobs.Job, "save_progress", move_starts)
    try:
        await jobs.run_pending_jobs()
    finally:
        await execute("UPDATE projects SET moving_to = NULL WHERE id = %s", [a])
    monkeypatch.setattr(jobs.Job, "save_progress", original)
    [fill] = await _jobs_of(a, "fill_sort_slot")
    assert fill["status"] == "pending" and fill["progress"]["after_id"] and fill["progress"]["batches"] == 1
    await _make_claimable(fill["id"])
    await jobs.run_pending_jobs()
    [fill] = await _jobs_of(a, "fill_sort_slot")
    assert fill["status"] == "done" and fill["result"]["batches"] == 6  # resumed after the first batch
    assert (await list_fields(a))["pub"].sort_slot == "source"
    await fetch_all("DELETE FROM jobs WHERE type IN ('fill_sort_slot', 'vacuum') RETURNING id")


@pytest.mark.anyio
async def test_backfill_retried_after_failure(two_projects, monkeypatch):  # noqa: F811
    """The scheduler retries a failed backfill after RETRY_AFTER, at most MAX_BACKFILL_ATTEMPTS times"""
    import uuid

    from amcat4.postgres.connection import execute, fetch_all
    from amcat4.postgres.projects import project_pk
    from amcat4.projects import jobs
    from tests.test_phase_c import _remove_quarter_keys

    a, _ = two_projects
    pk = await project_pk(a)
    await _remove_quarter_keys(a)

    async def failed(age: str) -> None:
        await execute(
            f"""INSERT INTO jobs (id, type, status, project_pk, params, error, updated_at)
                VALUES (%s, 'backfill_quarter', 'failed', %s, %s, 'error', now() - interval '{age}')""",  # type: ignore[arg-type]
            [uuid.uuid4().hex, pk, '{"project_pk": %d}' % pk],
        )

    async def pending() -> int:
        return len([j for j in await _jobs_of(a, "backfill_quarter") if j["status"] == "pending"])

    try:
        await failed("1 minute")
        await jobs.schedule_quarter_backfill({})
        assert await pending() == 0  # a recent failure: not yet
        await execute("UPDATE jobs SET updated_at = now() - interval '1 hour' WHERE type = 'backfill_quarter'")
        await jobs.schedule_quarter_backfill({})
        assert await pending() == 1  # retried
        await execute("DELETE FROM jobs WHERE type = 'backfill_quarter' AND status = 'pending'")
        for _ in range(jobs.MAX_BACKFILL_ATTEMPTS - 1):
            await failed("1 hour")
        await jobs.schedule_quarter_backfill({})
        assert await pending() == 0  # too many failures: needs a look
    finally:
        await fetch_all("DELETE FROM jobs WHERE type = 'backfill_quarter' RETURNING id")


@pytest.mark.anyio
async def test_fast_sort_restarts_unfinished_fill(two_projects, monkeypatch):  # noqa: F811
    """fast_sort on a field whose slot is reserved but not filled (its job failed): the fill starts again"""
    from amcat4.postgres.connection import execute, fetch_all
    from amcat4.projects import jobs
    from amcat4.systemdata.fields import field_infos

    a, _ = two_projects
    monkeypatch.setattr(get_settings(), "sort_slot_job_rows", 0)
    try:
        await update_fields(a, {"pub": UpdateDocumentField(fast_sort=True)})
        await execute("UPDATE jobs SET status = 'failed' WHERE type = 'fill_sort_slot'")
        assert not (await field_infos(a))["pub"].sort_ready
        await update_fields(a, {"pub": UpdateDocumentField(fast_sort=True)})
        await update_fields(a, {"pub": UpdateDocumentField(fast_sort=True)})  # (not a second job while one is pending)
        assert [j["status"] for j in await _jobs_of(a, "fill_sort_slot")] == ["failed", "pending"]
        await jobs.run_pending_jobs()
        assert (await field_infos(a))["pub"].sort_ready
        # a small project (filled in the request): at once
        await execute("UPDATE fields SET sort_slot_ready = false WHERE name = 'pub' AND sort_slot IS NOT NULL")
        monkeypatch.setattr(get_settings(), "sort_slot_job_rows", 10_000)
        await update_fields(a, {"pub": UpdateDocumentField(fast_sort=True)})
        assert (await field_infos(a))["pub"].sort_ready
    finally:
        await fetch_all("DELETE FROM jobs WHERE type IN ('fill_sort_slot', 'vacuum') RETURNING id")


###################### 11. One vacuum job; the migrations' lock timeout and index attachment ######################


@pytest.mark.anyio
async def test_vacuum_soon_concurrent(two_projects, monkeypatch):  # noqa: F811
    """Writes that finish at the same time schedule one vacuum job per partition, not one each"""
    import asyncio

    from amcat4.postgres.connection import connection, fetch_all
    from amcat4.postgres.layout import partition_of
    from amcat4.postgres.projects import project_pk
    from amcat4.projects import maintenance

    a, _ = two_projects
    monkeypatch.setattr(get_settings(), "vacuum_after_rows", 10)
    async with connection() as conn:
        pid = await partition_of(conn, await project_pk(a))
    await fetch_all("DELETE FROM jobs WHERE type = 'vacuum' RETURNING id")
    try:
        await asyncio.gather(*[maintenance.vacuum_soon([pid], 100) for _ in range(8)])
        rows = await fetch_all("SELECT status FROM jobs WHERE type = 'vacuum' AND params->>'partition_id' = %s", [str(pid)])
        assert [row["status"] for row in rows] == ["pending"]
    finally:
        await fetch_all("DELETE FROM jobs WHERE type = 'vacuum' RETURNING id")


LOCK_SCHEMA = "amcat_unittest_locking"


@pytest.fixture()
def scratch_schema():
    """A sqlalchemy connection (autocommit) with its search path on a scratch schema, as the migrations use"""
    from sqlalchemy import create_engine, text

    # (as amcat4.migrations.env.sqlalchemy_url, which cannot be imported outside alembic: it runs the migrations)
    engine = create_engine(get_settings().postgres_url.replace("postgresql://", "postgresql+psycopg://", 1))
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {LOCK_SCHEMA} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {LOCK_SCHEMA}"))
        conn.execute(text(f"SET search_path TO {LOCK_SCHEMA}, public"))
        conn.execute(text("CREATE TABLE documents (id int, partition_id int) PARTITION BY LIST (partition_id)"))
        conn.execute(text("CREATE TABLE documents_p1 PARTITION OF documents FOR VALUES IN (1)"))
        try:
            yield conn
        finally:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {LOCK_SCHEMA} CASCADE"))
    engine.dispose()


def _columns_of(conn, table: str) -> set[str]:
    from sqlalchemy import text

    rows = conn.execute(
        text("SELECT column_name FROM information_schema.columns WHERE table_schema = :s AND table_name = :t"),
        {"s": LOCK_SCHEMA, "t": table},
    )
    return set(rows.scalars())


@pytest.mark.anyio
async def test_migration_alter_lock_timeout(scratch_schema):
    """ALTER TABLE in a migration gives up after a lock timeout (with a clear error), and retries meanwhile"""
    import asyncio

    import psycopg

    from amcat4.migrations.locking import TableBusyError, alter_with_lock_timeout

    conn = scratch_schema

    def alter(column: str, attempts: int) -> None:
        from sqlalchemy import text

        with conn.engine.connect() as migration, migration.begin():  # (the migrations run in a transaction)
            migration.execute(text(f"SET LOCAL search_path TO {LOCK_SCHEMA}"))
            alter_with_lock_timeout(migration, f"ALTER TABLE documents ADD COLUMN {column} int", "50ms", attempts)

    async with await psycopg.AsyncConnection.connect(get_settings().postgres_url, autocommit=True) as other:
        # a long query on a partition holds a lock until it is done
        async with other.transaction():
            await other.execute(f"LOCK TABLE {LOCK_SCHEMA}.documents_p1 IN ACCESS SHARE MODE")
            with pytest.raises(TableBusyError, match="run the migration again"):
                await asyncio.to_thread(alter, "a", 2)
        assert "a" not in _columns_of(conn, "documents")

        # the lock is released during the retries: the column is added
        async def release_soon():
            await asyncio.sleep(0.3)
            await other.execute("COMMIT")

        await other.execute("BEGIN")
        await other.execute(f"LOCK TABLE {LOCK_SCHEMA}.documents_p1 IN ACCESS SHARE MODE")
        await asyncio.gather(asyncio.to_thread(alter, "b", 3), release_soon())
    assert "b" in _columns_of(conn, "documents") and "b" in _columns_of(conn, "documents_p1")


@pytest.mark.anyio
async def test_build_index_attached_elsewhere(scratch_schema, monkeypatch):
    """
    _build_index (migration 0002): a partition index with the expected name that is attached to another parent index
    is not taken as the partition's index of the new parent
    """
    import importlib.util
    import pathlib
    from types import SimpleNamespace

    from sqlalchemy import text

    import amcat4.migrations

    conn = scratch_schema
    path = pathlib.Path(amcat4.migrations.__file__).parent / "versions" / "0002_sort_slots_and_indexes.py"
    spec = importlib.util.spec_from_file_location("migration_0002", path)
    assert spec and spec.loader
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    monkeypatch.setattr(migration, "op", SimpleNamespace(get_bind=lambda: conn))

    # an interrupted earlier run: documents_p1_by_id belongs to the parent documents_other
    conn.execute(text("CREATE INDEX documents_other ON ONLY documents (id)"))
    conn.execute(text("CREATE INDEX documents_p1_by_id ON documents_p1 (id)"))
    conn.execute(text("ALTER INDEX documents_other ATTACH PARTITION documents_p1_by_id"))
    migration._build_index("documents_by_id", "(id)", ["documents_p1"])

    def parent_valid(name: str) -> bool:
        return conn.execute(
            text("SELECT indisvalid FROM pg_index WHERE indexrelid = CAST(:n AS regclass)"), {"n": name}
        ).scalar_one()

    assert parent_valid("documents_by_id") and parent_valid("documents_other")
    children = conn.execute(
        text(
            """SELECT c.relname, i.inhparent::regclass::text AS parent FROM pg_inherits i
               JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent IN (CAST('documents_by_id' AS regclass),
               CAST('documents_other' AS regclass)) ORDER BY 1"""
        )
    ).all()
    assert [tuple(r) for r in children] == [
        ("documents_p1_by_id", "documents_other"),
        ("documents_p1_by_id_1", "documents_by_id"),
    ]
    # idempotent: a second run changes nothing
    migration._build_index("documents_by_id", "(id)", ["documents_p1"])
    count = text("SELECT count(*) FROM pg_inherits WHERE inhparent = CAST('documents_by_id' AS regclass)")
    assert conn.execute(count).scalar_one() == 1
