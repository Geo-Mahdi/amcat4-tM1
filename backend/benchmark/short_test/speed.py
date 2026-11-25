"""
Step 3, SPEED: about 50 fixed queries, through the python query functions of the code under test (not HTTP).

Each query: 3 warm-up runs, then 20 timed runs; reported are the median and the 95th percentile in milliseconds, the
number of hits, and a short fingerprint of the result (so compare.py can tell whether two branches gave the same
result). A query gets at most QUERY_BUDGET_S seconds: a slow query gets fewer runs (the number of runs is reported),
and a query whose statement passes the statement limit is reported as "too slow".
"""

import hashlib
import json
import time
from typing import Any, Awaitable, Callable

from benchmark.short_test import data
from benchmark.short_test.common import PROJECT_BIG, PROJECT_SMALL, Deadline, TooSlow, summary_ms

WARMUP = 3
RUNS = 20
QUERY_BUDGET_S = 45
DEEP_PAGE = 500  # with 10 per page: offset 5,000


def fingerprint(value: Any) -> str:
    return hashlib.sha1(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()[:12]


def queries() -> list[tuple[str, Callable[[], Awaitable[tuple[int | None, Any]]]]]:
    """The fixed queries: (name, function that runs it and returns (number of hits, result to fingerprint))"""
    from amcat4.models import FieldSpec, FilterSpec, SortSpec
    from amcat4.projects.aggregate import Aggregation, Axis, query_aggregate
    from amcat4.projects.query import query_documents
    from amcat4.systemdata.fields import field_stats, field_values

    fields = [FieldSpec(name="title"), FieldSpec(name="date"), FieldSpec(name="source")]
    out: list[tuple[str, Callable[[], Awaitable[tuple[int | None, Any]]]]] = []

    def q(text: str | None) -> dict[str, str] | None:
        return {"q": text} if text else None

    def search(name: str, project: str = PROJECT_BIG, query=None, filters=None, sort=None, page=0, per_page=10) -> None:
        async def run() -> tuple[int | None, Any]:
            result = await query_documents(
                project,
                fields=fields,
                queries=q(query),
                filters={k: FilterSpec(**v) for k, v in filters.items()} if filters else None,
                sort=[{k: SortSpec(order=order)} for k, order in sort] if sort else None,
                page=page,
                per_page=per_page,
            )
            return result.total_count, [d["_id"] for d in result.data]

        out.append((name, run))

    def aggregate(name: str, axes, aggregations=None, project: str = PROJECT_BIG, query=None, filters=None) -> None:
        async def run() -> tuple[int | None, Any]:
            result = await query_aggregate(
                project,
                [Axis(field, interval) for field, interval in axes],
                [Aggregation(field, function) for field, function in aggregations or []],
                queries=q(query),
                filters={k: FilterSpec(**v) for k, v in filters.items()} if filters else None,
                limit=10_000,
            )
            rows = [list(row) for row in result.data]
            return sum(row[len(axes)] for row in rows), rows

        out.append((name, run))

    # --- relevance
    search("relevance: rare word", query=data.RARE)
    search("relevance: common word", query=data.COMMON)
    search("relevance: 3 words", query=f"{data.word(40)} {data.word(120)} {data.word(400)}")
    search("relevance: phrase", query=f'"{data.word(1)} {data.word(2)}"')
    search("relevance: common word, deep page", query=data.COMMON, page=DEEP_PAGE)
    search("relevance: common word (small project, shared partition)", project=PROJECT_SMALL, query=data.COMMON)

    # --- filters, without and with a query string
    the_filters = {
        "keyword": {"section": {"values": ["Sport"]}},
        "number whole value": {"n_num": {"values": [17]}},
        "number range": {"n_num": {"gte": 10, "lt": 20}},
        "date range": {"date": {"gte": "2010-01-01", "lt": "2011-01-01"}},
        "tag": {"tags": {"values": ["tag05"]}},
        "exists": {"author": {"exists": True}},
    }
    for label, f in the_filters.items():
        search(f"filter: {label}", filters=f)
    for label, f in the_filters.items():
        search(f"filter: {label} + query", filters=f, query=data.MEDIUM)

    # --- sorts: the whole project and a filtered subset (one section, 1 in 12 documents)
    the_sorts = {
        "date (slot) desc": [("date", "desc")],
        "source (slot) asc": [("source", "asc")],
        "author (JSON keyword) asc": [("author", "asc")],
        "author (JSON keyword) desc": [("author", "desc")],
        "n_num (JSON number) desc": [("n_num", "desc")],
        "n_int (JSON integer) asc": [("n_int", "asc")],
        "tags asc": [("tags", "asc")],
    }
    subset = {"section": {"values": ["Politics"]}}
    for label, sort in the_sorts.items():
        search(f"sort: {label}, whole project", sort=sort)
    for label, sort in the_sorts.items():
        search(f"sort: {label}, subset", sort=sort, filters=subset)
    search("sort: date (slot) desc, whole project, deep page", sort=the_sorts["date (slot) desc"], page=DEEP_PAGE)
    search(
        "sort: author (JSON keyword) asc, whole project, deep page",
        sort=the_sorts["author (JSON keyword) asc"],
        page=DEEP_PAGE,
    )
    search(
        "sort: author (JSON keyword) asc, subset, deep page",
        sort=the_sorts["author (JSON keyword) asc"],
        filters=subset,
        page=DEEP_PAGE,
    )
    search("sort: date (slot) desc + query", sort=the_sorts["date (slot) desc"], query=data.MEDIUM)
    search("sort: author (JSON keyword) asc + query", sort=the_sorts["author (JSON keyword) asc"], query=data.MEDIUM)
    search("sort: multi-key source asc, date desc (slots), whole project", sort=[("source", "asc"), ("date", "desc")])
    search(
        "sort: multi-key section asc, n_int desc (JSON), subset",
        sort=[("section", "asc"), ("n_int", "desc")],
        filters=the_filters["tag"],
    )
    search(
        "sort: author (JSON keyword) asc (small project)", project=PROJECT_SMALL, sort=the_sorts["author (JSON keyword) asc"]
    )

    # --- aggregations
    aggregate("aggregate: terms on keyword (source)", [("source", None)])
    aggregate("aggregate: terms on keyword with many values (author)", [("author", None)])
    aggregate("aggregate: terms on tags", [("tags", None)])
    aggregate("aggregate: terms on tags + query", [("tags", None)], query=data.MEDIUM)
    aggregate("aggregate: date histogram by month", [("date", "month")])
    aggregate("aggregate: date histogram by month + query", [("date", "month")], query=data.MEDIUM)
    aggregate("aggregate: number avg/min/max (n_num)", [], [("n_num", "avg"), ("n_num", "min"), ("n_num", "max")])
    aggregate("aggregate: avg n_num per section", [("section", None)], [("n_num", "avg")])
    aggregate("aggregate: 2 axes (year x source)", [("date", "year"), ("source", None)])
    aggregate("aggregate: 2 axes (year x source) + common word", [("date", "year"), ("source", None)], query=data.COMMON)
    aggregate("aggregate: terms on keyword (small project)", [("source", None)], project=PROJECT_SMALL)

    # --- count only, and the field values / statistics calls
    search("count only: whole project", per_page=0)
    search("count only: common word", query=data.COMMON, per_page=0)
    search("count only: filter + query", query=data.MEDIUM, filters=the_filters["date range"], per_page=0)

    async def values() -> tuple[int | None, Any]:
        result = await field_values(PROJECT_BIG, "section", 100)
        return len(result), result

    async def stats() -> tuple[int | None, Any]:
        result = await field_stats(PROJECT_BIG, "n_int")
        return result.get("count"), result

    out.append(("field values (section)", values))
    out.append(("field statistics (n_int)", stats))
    return out


async def measure(run: Callable[[], Awaitable[tuple[int | None, Any]]], deadline: Deadline) -> dict[str, Any]:
    """Warm up and time one query. Stops early when the query's own budget or the step's time limit is used up"""
    from psycopg.errors import QueryCanceled

    t_start = time.monotonic()
    times: list[float] = []
    hits, result = None, None
    try:
        for i in range(WARMUP + RUNS):
            deadline.check()
            if i >= WARMUP + 3 and time.monotonic() - t_start > QUERY_BUDGET_S:
                break
            t0 = time.perf_counter()
            hits, result = await run()
            if i >= WARMUP:
                times.append((time.perf_counter() - t0) * 1000)
    except TooSlow:
        raise  # the time limit of the whole step
    except QueryCanceled:
        return {"status": "too slow", "error": "a statement passed the statement limit"}
    except Exception as e:
        if "timeout" in str(e).lower():
            return {"status": "too slow", "error": f"{type(e).__name__}: {e}"}
        return {"status": "failed", "error": f"{type(e).__name__}: {e}"}
    out = {"status": "ok", "runs": len(times), "hits": hits, "fingerprint": fingerprint(result), **summary_ms(times)}
    if len(times) < RUNS:
        out["note"] = f"only {len(times)} runs (over {QUERY_BUDGET_S} s for this query)"
    return out


async def step_speed(args, deadline: Deadline, out: dict[str, Any]) -> None:
    out["words"] = {"rare": data.RARE, "medium": data.MEDIUM, "common": data.COMMON}
    out["queries"] = {}
    for name, run in queries():
        if args.only and args.only not in name:
            continue
        r = await measure(run, deadline)
        out["queries"][name] = r
        if r["status"] == "ok":
            print(f"  {name:72s} {r['median_ms']:9.1f} ms  p95 {r['p95_ms']:9.1f}  hits {r['hits']}", flush=True)
        else:
            print(f"  {name:72s} {r['status']}: {r.get('error')}", flush=True)
    bad = [name for name, r in out["queries"].items() if r["status"] != "ok"]
    if bad:
        out["note"] = f"{len(bad)} queries were too slow or failed: {bad}"
