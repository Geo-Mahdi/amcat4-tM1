"""
Short test plan for the AmCAT 4 PostgreSQL backend: one command that loads test data, checks that searches give the
right results, and measures speed. Run it on two or more branches and put the results side by side with compare.py.

Run from the backend directory:

    uv run python -m benchmark.short_test.run --out <dir> \
        --database-url "postgresql://postgres@/bench_v3?host=/var/run/postgresql&port=5435" --schema short_test

    --steps load,same,speed,loadtest,write,report   which steps to run (default: all, in this order)
    --docs 1000000        documents in the big project (the small project has 200,000, or --docs if that is less)
    --force-load          drop the test schema and load the data again
    --cases 2000          number of same-results cases
    --only <text>         speed step: only the queries with this text in their name

Each step has a hard time limit (LIMITS below). A step that passes it is stopped and reported as "too slow"; it is
never extended. The limits together are 60 minutes. Steps can be run one at a time: results.json in --out keeps the
results of earlier runs of the other steps.

The write step changes documents and puts them back (it removes its tag and rebuilds the search indexes). If it was
stopped or failed half-way, load again with --force-load before measuring speed.

The database url and schema default to AMCAT4_POSTGRES_URL and AMCAT4_POSTGRES_SCHEMA. The schema is created if it
does not exist. Use a schema of its own for the test: --force-load drops it.
"""

import argparse
import asyncio
import os
import sys
import time
import traceback

STEPS = ["load", "same", "speed", "loadtest", "write", "report"]
# Hard time limits per step, in seconds (60 minutes together)
LIMITS = {"load": 15 * 60, "same": 10 * 60, "speed": 15 * 60, "loadtest": 5 * 60, "write": 10 * 60, "report": 5 * 60}
# The limit of one statement of the code under test (the API statement timeout), per step, in seconds
STATEMENT_LIMITS = {"load": 15 * 60, "same": 30, "speed": 20, "loadtest": 10, "write": 60, "report": 30}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", required=True, help="directory for results.json and RESULTS.md")
    parser.add_argument("--database-url", default=os.environ.get("AMCAT4_POSTGRES_URL"))
    parser.add_argument("--schema", default=os.environ.get("AMCAT4_POSTGRES_SCHEMA", "short_test"))
    parser.add_argument("--steps", default=",".join(STEPS))
    parser.add_argument("--docs", type=int, default=1_000_000)
    parser.add_argument("--force-load", action="store_true")
    parser.add_argument("--cases", type=int, default=2000)
    parser.add_argument("--only", default=None)
    args = parser.parse_args()
    if not args.database_url:
        parser.error("give --database-url or set AMCAT4_POSTGRES_URL")
    args.steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    for step in args.steps:
        if step not in STEPS:
            parser.error(f"unknown step {step!r}, choose from {STEPS}")
    return args


async def run_step(name: str, args: argparse.Namespace, results: dict) -> None:
    """Run one step inside its time limit and store its result (also what it had when it was stopped)"""
    from amcat4.connections import amcat_connections
    from benchmark.short_test import common

    func = _step_function(name)
    limit = LIMITS[name]
    out: dict = {}
    entry = {"status": "failed", "limit_s": limit, "started": common.now_iso(), "data": out}
    print(f"\n=== {name} (limit {limit // 60} min)", flush=True)
    common.apply_settings(statement_timeout_ms=STATEMENT_LIMITS[name] * 1000)
    deadline = common.Deadline(limit)
    try:
        async with amcat_connections():
            await asyncio.wait_for(func(args, deadline, out), timeout=limit)
        entry["status"] = out.pop("status", "ok")
        if out.get("problems"):
            entry["error"] = "; ".join(out["problems"])
        elif out.get("stopped_early") or out.get("note"):
            entry["remark"] = out.get("stopped_early") or out.get("note")
    except (asyncio.TimeoutError, TimeoutError, common.TooSlow) as e:
        entry["status"] = "too slow"
        entry["error"] = str(e) or f"stopped at the time limit of {limit} s"
    except Exception as e:
        entry["error"] = f"{type(e).__name__}: {e}"
        entry["traceback"] = traceback.format_exc()
        traceback.print_exc()
    if entry["status"] != "ok":
        common.cancel_running_statements()
    entry["seconds"] = round(deadline.elapsed(), 1)
    entry["settings"] = common.settings_snapshot()
    results["steps"][name] = entry
    remark = entry.get("error") or entry.get("remark")
    print(f"=== {name}: {entry['status']} in {entry['seconds']} s" + (f" ({remark})" if remark else ""), flush=True)


def _step_function(name: str):
    if name == "load":
        from benchmark.short_test.load import step_load as func
    elif name == "same":
        from benchmark.short_test.same_results import step_same as func
    elif name == "speed":
        from benchmark.short_test.speed import step_speed as func
    elif name == "loadtest":
        from benchmark.short_test.load_test import step_loadtest as func
    elif name == "write":
        from benchmark.short_test.write_test import step_write as func
    else:
        from benchmark.short_test.report import step_report as func
    return func


async def main(args: argparse.Namespace) -> int:
    from benchmark.short_test import common

    results = common.load_results(args.out)
    results["git"] = common.git_info()
    results["database"] = await common.database_info()
    results["schema"] = args.schema
    results["docs"] = args.docs
    results.setdefault("first_run", common.now_iso())
    t0 = time.monotonic()
    for name in args.steps:
        if name == "report":
            results["last_run"] = common.now_iso()
            common.save_results(args.out, results)  # the report step reads the file
        await run_step(name, args, results)
        results["last_run"] = common.now_iso()
        common.save_results(args.out, results)
    print(f"\nDone in {(time.monotonic() - t0) / 60:.1f} min. Results: {os.path.join(args.out, 'results.json')}")
    for name in args.steps:
        print(f"  {name:9s} {results['steps'][name]['status']:9s} {results['steps'][name]['seconds']:7.1f} s")
    return 0 if all(results["steps"][s]["status"] == "ok" for s in args.steps) else 1


if __name__ == "__main__":
    arguments = parse_args()
    # amcat4 reads its settings from the environment, once: set them before anything of amcat4 is imported.
    # Worker processes (load test) get the same environment.
    from benchmark.short_test.common import with_application_name

    os.environ["AMCAT4_POSTGRES_URL"] = with_application_name(arguments.database_url)
    os.environ["AMCAT4_POSTGRES_SCHEMA"] = arguments.schema
    os.environ.setdefault("AMCAT4_AUTH", "no_auth")
    sys.exit(asyncio.run(main(arguments)))
