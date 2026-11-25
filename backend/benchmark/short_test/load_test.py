"""
Step 4, LOAD TEST: 32 clients at the same time, 60 seconds for each of three mixes of requests:
relevance searches, filtered and sorted lists, and aggregations.

The clients call the python query functions (not HTTP). They are spread over 4 processes of 8 clients, each process
with its own connection pools, so python itself is not the limit. Every client sends its next request as soon as the
previous one is answered. Reported per mix: requests per second, median and 95th percentile of the response time,
and the number of errors (for example a statement over the statement limit, or no free connection).
The requests are random but the same on every run (fixed seeds).
"""

import asyncio
import multiprocessing
import random
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Any

from benchmark.short_test import data
from benchmark.short_test.common import PROJECT_BIG, PROJECT_SMALL, Deadline, apply_settings, percentile

PROCESSES = 4
CLIENTS_PER_PROCESS = 8
SECONDS = 60
MIXES = ["relevance searches", "filtered sorted lists", "aggregations"]
STATEMENT_LIMIT_MS = 10_000


def _rank(rnd: random.Random, lowest: int, highest: int) -> int:
    return int(lowest * (highest / lowest) ** rnd.random())


def _year(rnd: random.Random) -> dict:
    year = rnd.randrange(2000, 2025)
    return {"date": {"gte": f"{year}-01-01", "lt": f"{year + 1}-01-01"}}


def _month(rnd: random.Random) -> dict:
    year, month = rnd.randrange(2000, 2024), rnd.randrange(1, 12)
    return {"date": {"gte": f"{year}-{month:02d}-01", "lt": f"{year}-{month + 1:02d}-01"}}


def make_request(mix: str, rnd: random.Random) -> dict[str, Any]:
    """One random request of a mix, as keyword arguments for _send"""
    section = {"section": {"values": [rnd.choice(data.SECTIONS)]}}
    tag = {"tags": {"values": [rnd.choice(data.TAGS)]}}
    source = {"source": {"values": [rnd.choice(data.SOURCES)]}}
    if mix == "relevance searches":
        words = " ".join(data.word(_rank(rnd, 1, 20_000)) for _ in range(rnd.choice([1, 1, 2])))
        project = PROJECT_BIG if rnd.random() < 0.8 else PROJECT_SMALL
        return {"project": project, "query": words, "page": rnd.choice([0, 0, 0, 1, 2])}
    if mix == "filtered sorted lists":
        kind = rnd.randrange(7)
        if kind == 0:
            return {"filters": section, "sort": [("date", "desc")]}
        if kind == 1:
            return {"filters": tag, "sort": [("date", "desc")], "page": rnd.randrange(5)}
        if kind == 2:
            return {"filters": _year(rnd), "sort": [("source", "asc")]}
        if kind == 3:
            return {"filters": source, "sort": [("date", "asc")], "page": rnd.randrange(20)}
        if kind == 4:  # a sort on a field without slot, over a few thousand documents
            return {"filters": _month(rnd), "sort": [("author", "asc")]}
        if kind == 5:
            author = {"author": {"values": [data.author(rnd.randrange(data.N_AUTHORS))]}}
            return {"filters": author, "sort": [("n_num", "desc")]}
        return {"query": data.word(_rank(rnd, 100, 20_000)), "sort": [("date", "desc")]}
    kind = rnd.randrange(6)
    if kind == 0:
        return {"axes": [("source", None)], "filters": _year(rnd)}
    if kind == 1:
        return {"axes": [("date", "month")], "query": data.word(_rank(rnd, 50, 5000))}
    if kind == 2:
        return {"axes": [("tags", None)], "filters": section}
    if kind == 3:
        return {"axes": [("section", None)]}
    if kind == 4:
        return {"axes": [("section", None)], "metrics": [("n_num", "avg")], "filters": tag}
    return {"axes": [("date", "year"), ("source", None)], "query": data.word(_rank(rnd, 20, 5000))}


async def _send(request: dict[str, Any]) -> None:
    from amcat4.models import FieldSpec, FilterSpec, SortSpec
    from amcat4.projects.aggregate import Aggregation, Axis, query_aggregate
    from amcat4.projects.query import query_documents

    project = request.get("project", PROJECT_BIG)
    queries = {"q": request["query"]} if request.get("query") else None
    filters = {k: FilterSpec(**v) for k, v in request["filters"].items()} if request.get("filters") else None
    if "axes" in request:
        await query_aggregate(
            project,
            [Axis(field, interval) for field, interval in request["axes"]],
            [Aggregation(field, function) for field, function in request.get("metrics", [])],
            queries=queries,
            filters=filters,
        )
    else:
        await query_documents(
            project,
            fields=[FieldSpec(name="title"), FieldSpec(name="date"), FieldSpec(name="source")],
            queries=queries,
            filters=filters,
            sort=[{k: SortSpec(order=order)} for k, order in request["sort"]] if request.get("sort") else None,
            page=request.get("page", 0),
        )


async def _clients(mix: str, seed: int, start_at: float, seconds: float) -> dict[str, Any]:
    from amcat4.connections import amcat_connections

    times: list[float] = []
    errors: dict[str, int] = {}

    async def client(number: int) -> None:
        rnd = random.Random(f"{seed}-{number}")
        while time.time() < start_at + seconds:
            request = make_request(mix, rnd)
            t0 = time.perf_counter()
            try:
                await _send(request)
                times.append((time.perf_counter() - t0) * 1000)
            except Exception as e:
                errors[type(e).__name__] = errors.get(type(e).__name__, 0) + 1

    apply_settings(statement_timeout_ms=STATEMENT_LIMIT_MS)
    async with amcat_connections():
        await _send(make_request(mix, random.Random(seed)))  # open a connection and import everything before the start
        await asyncio.sleep(max(0.0, start_at - time.time()))
        await asyncio.gather(*(client(i) for i in range(CLIENTS_PER_PROCESS)))
    return {"times": times, "errors": errors, "late_s": max(0.0, time.time() - start_at - seconds)}


def worker(mix: str, seed: int, start_at: float, seconds: float) -> dict[str, Any]:
    """Runs in a worker process: 8 clients for `seconds` seconds, starting at the time `start_at`"""
    return asyncio.run(_clients(mix, seed, start_at, seconds))


async def step_loadtest(args, deadline: Deadline, out: dict[str, Any]) -> None:
    loop = asyncio.get_running_loop()
    out["clients"] = PROCESSES * CLIENTS_PER_PROCESS
    out["seconds_per_mix"] = SECONDS
    out["mixes"] = {}
    pool = ProcessPoolExecutor(max_workers=PROCESSES, mp_context=multiprocessing.get_context("spawn"))
    try:
        for m, mix in enumerate(MIXES):
            deadline.check()
            start_at = time.time() + 8  # time for the worker processes to start and connect
            futures = [loop.run_in_executor(pool, worker, mix, 1000 * m + p, start_at, SECONDS) for p in range(PROCESSES)]
            parts = await asyncio.wait_for(asyncio.gather(*futures), timeout=min(SECONDS + 30, max(1.0, deadline.left())))
            times = [t for part in parts for t in part["times"]]
            errors: dict[str, int] = {}
            for part in parts:
                for name, n in part["errors"].items():
                    errors[name] = errors.get(name, 0) + n
            result: dict[str, Any] = {"requests": len(times), "errors": sum(errors.values()), "error_kinds": errors}
            if times:
                result.update(
                    {
                        "requests_per_s": round(len(times) / SECONDS, 1),
                        "median_ms": round(percentile(times, 50), 1),
                        "p95_ms": round(percentile(times, 95), 1),
                        "max_ms": round(max(times), 1),
                    }
                )
            out["mixes"][mix] = result
            print(
                f"  {mix:24s} {result.get('requests_per_s', 0):8.1f} requests/s  p95 {result.get('p95_ms', 0):8.1f} ms"
                f"  errors {result['errors']}",
                flush=True,
            )
    finally:
        for process in list(getattr(pool, "_processes", {}).values()):  # also stops workers that are still busy
            process.kill()
        pool.shutdown(wait=False, cancel_futures=True)
    if any(mix["errors"] for mix in out["mixes"].values()):
        out["note"] = "some requests gave an error (see error_kinds)"
