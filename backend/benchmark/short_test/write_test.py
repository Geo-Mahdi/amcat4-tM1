"""
Step 5, WRITE TEST: add a tag to 10% of the big project (100,000 documents) through the normal code (update_tag_query,
what the API calls for "tag the documents of this search"), and measure what it does to "count the tags over a search"
(a terms aggregation on the tag field, with a query string and for the whole project):
before the update, right after it, and after VACUUM.

Then the tag is removed again (also timed) and the search indexes are rebuilt, so the data is as it was after the
load. If the branch has a setting label_tables, all of this runs once for each value of it.
"""

import statistics
import time
from typing import Any

from benchmark.short_test import data
from benchmark.short_test.common import PROJECT_BIG, Deadline, own_connection, setting
from benchmark.short_test.load import rebuild_indexes

TAG = "st_benchmark"
RUNS = 10


async def _count_tags(query: str | None) -> tuple[float, int, int]:
    """Median time (ms) of counting tags over a search, the number of tags found, and the count of the test tag"""
    from amcat4.projects.aggregate import Axis, query_aggregate

    times, rows = [], []
    for i in range(2 + RUNS):
        t0 = time.perf_counter()
        result = await query_aggregate(PROJECT_BIG, [Axis("tags")], queries={"q": query} if query else None)
        if i >= 2:
            times.append((time.perf_counter() - t0) * 1000)
        rows = result.data
    ours = [row[1] for row in rows if row[0] == TAG]
    return round(statistics.median(times), 2), len(rows), ours[0] if ours else 0


async def _measure(label: str, phases: list[dict[str, Any]]) -> dict[str, Any]:
    search_ms, _, _ = await _count_tags(data.MEDIUM)
    all_ms, n_tags, tagged = await _count_tags(None)
    phase = {"phase": label, "count_tags_search_ms": search_ms, "count_tags_all_ms": all_ms, "tags": n_tags, "tagged": tagged}
    phases.append(phase)
    print(f"  {label:28s} count tags over a search {search_ms:8.1f} ms, whole project {all_ms:8.1f} ms", flush=True)
    return phase


async def _vacuum(deadline: Deadline) -> float:
    async with own_connection(max(1.0, deadline.left())) as conn:
        cur = await conn.execute("SELECT partition_id FROM projects WHERE id = %s", [PROJECT_BIG])
        partition = (await cur.fetchone())["partition_id"]  # type: ignore[index]
        t0 = time.monotonic()
        await conn.execute(f"VACUUM (ANALYZE) documents_p{int(partition)}")  # type: ignore[arg-type]
        return round(time.monotonic() - t0, 1)


async def _one_run(deadline: Deadline, out: dict[str, Any]) -> None:
    from amcat4.models import FilterSpec
    from amcat4.projects.query import update_tag_query

    selection = {"n_int": FilterSpec(lt=100)}  # exactly 10% of the documents (see data.make_doc)
    phases: list[dict[str, Any]] = []
    out["phases"] = phases
    _, _, left_over = await _count_tags(None)
    if left_over:  # an earlier write step was stopped before it removed its tag
        await update_tag_query(PROJECT_BIG, "remove", "tags", TAG)
        await _vacuum(deadline)
        out["note"] = f"removed the test tag that an earlier run left on {left_over} documents (the index is not fresh)"
    await _measure("before", phases)

    t0 = time.monotonic()
    result = await update_tag_query(PROJECT_BIG, "add", "tags", TAG, filters=selection)
    out["add_tag_s"] = round(time.monotonic() - t0, 1)
    out["documents_tagged"] = result["updated"]
    print(f"  added the tag to {result['updated']} documents in {out['add_tag_s']} s", flush=True)
    after = await _measure("after the update", phases)
    out["tag_count_right"] = after["tagged"] == result["updated"]

    out["vacuum_s"] = await _vacuum(deadline)
    await _measure("after VACUUM", phases)

    deadline.check()
    t0 = time.monotonic()
    result = await update_tag_query(PROJECT_BIG, "remove", "tags", TAG, filters=selection)
    out["remove_tag_s"] = round(time.monotonic() - t0, 1)
    out["documents_untagged"] = result["updated"]
    out["restore"] = await rebuild_indexes(deadline)
    end = await _measure("after removing the tag and rebuilding the index", phases)
    out["restored"] = end["tagged"] == 0


async def step_write(args, deadline: Deadline, out: dict[str, Any]) -> None:
    from amcat4.config import get_settings

    settings = get_settings()
    if not hasattr(settings, "label_tables"):
        out["runs"] = {"default": {}}
        await _one_run(deadline, out["runs"]["default"])
        return
    original = setting("label_tables")
    out["runs"] = {}
    try:
        for value in (False, True):
            deadline.check()
            print(f"  label_tables = {value}")
            setattr(settings, "label_tables", value)
            run: dict[str, Any] = {}
            out["runs"][f"label_tables={str(value).lower()}"] = run
            try:
                await _one_run(deadline, run)
            except Exception as e:
                if deadline.left() <= 0:
                    raise
                run["error"] = f"{type(e).__name__}: {e}"
                out["status"] = "failed"
                out.setdefault("problems", []).append(f"label_tables={value}: {run['error']}")
    finally:
        setattr(settings, "label_tables", original)
