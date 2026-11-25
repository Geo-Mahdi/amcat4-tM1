"""
Shared helpers of the short test: settings, own database connections, timing, and the results file.

Import this module only after run.py has set AMCAT4_POSTGRES_URL and AMCAT4_POSTGRES_SCHEMA: amcat4 reads its settings
from the environment once.
"""

import json
import os
import statistics
import subprocess
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncGenerator

import psycopg
from psycopg.rows import dict_row

PROJECT_BIG = "st_big"  # in a partition of its own
PROJECT_SMALL = "st_small"  # in the shared partition
SMALL_DOCS = 200_000
APP_NAME = "amcat_short_test"  # application_name of every connection of the test: used to stop statements that hang


class TooSlow(Exception):
    """A step (or a part of it) passed its time limit"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def database_url() -> str:
    return os.environ["AMCAT4_POSTGRES_URL"]


def schema() -> str:
    return os.environ["AMCAT4_POSTGRES_SCHEMA"]


def with_application_name(url: str) -> str:
    """Add application_name to a postgresql:// url (so pg_stat_activity shows which connections are ours)"""
    if "application_name=" in url or "://" not in url:
        return url
    return f"{url}{'&' if '?' in url else '?'}application_name={APP_NAME}"


def apply_settings(statement_timeout_ms: int, overrides: dict[str, Any] | None = None) -> None:
    """
    Settings of the code under test that the short test changes. Only settings that exist on this branch are set
    (hasattr), so the script runs on branches with other settings.
    - statement_timeout_ms: the limit for one statement of an API request in this step
    - vacuum_after_rows = 0: writes do not create background vacuum jobs (nothing runs them here; the test runs
      VACUUM itself where it matters)
    """
    from amcat4.config import get_settings

    settings = get_settings()
    wanted = {"statement_timeout_ms": statement_timeout_ms, "vacuum_after_rows": 0, **(overrides or {})}
    for name, value in wanted.items():
        if hasattr(settings, name):
            setattr(settings, name, value)


def settings_snapshot() -> dict[str, Any]:
    """The settings of the code under test, without secrets"""
    from amcat4.config import get_settings

    hidden = ("secret", "key", "password", "postgres_url")
    return {
        k: (v if isinstance(v, (int, float, bool, str, type(None))) else str(v))
        for k, v in get_settings().model_dump().items()
        if not any(h in k for h in hidden)
    }


def setting(name: str, default: Any = None) -> Any:
    """A setting of the code under test, or default if this branch does not have it"""
    from amcat4.config import get_settings

    return getattr(get_settings(), name, default)


@asynccontextmanager
async def own_connection(timeout_s: float | None = None) -> AsyncGenerator[psycopg.AsyncConnection, None]:
    """
    A connection of the test itself (not from the pools of the code under test), in autocommit mode, with the test
    schema first in the search path and, if given, a statement timeout.
    """
    options = f"-c search_path={schema()},public"
    if timeout_s is not None:
        options += f" -c statement_timeout={max(1, int(timeout_s * 1000))}"
    conn = await psycopg.AsyncConnection.connect(
        database_url(), autocommit=True, row_factory=dict_row, options=options, application_name=APP_NAME
    )
    try:
        yield conn
    finally:
        await conn.close()


def cancel_running_statements() -> int:
    """Stop every statement that connections of the test are still running (after a step passed its time limit)"""
    try:
        with psycopg.connect(database_url(), autocommit=True, application_name=APP_NAME + "_cancel") as conn:
            cur = conn.execute(
                """SELECT count(pg_cancel_backend(pid)) FROM pg_stat_activity
                   WHERE application_name = %s AND datname = current_database() AND state <> 'idle'""",
                [APP_NAME],
            )
            return cur.fetchone()[0]  # type: ignore[index]
    except Exception as e:  # never hide the result of the step behind a failed clean-up
        print(f"  (could not cancel running statements: {e})")
        return 0


class Deadline:
    """The wall-clock limit of a step"""

    def __init__(self, seconds: float):
        self.seconds = seconds
        self.start = time.monotonic()

    def elapsed(self) -> float:
        return time.monotonic() - self.start

    def left(self) -> float:
        return self.seconds - self.elapsed()

    def check(self, what: str = "") -> None:
        if self.left() <= 0:
            raise TooSlow(f"time limit of {self.seconds:.0f} s passed{' in ' + what if what else ''}")


def percentile(values: list[float], p: float) -> float:
    """The p-th percentile (0-100) by the nearest-rank method"""
    ordered = sorted(values)
    rank = max(1, -(-len(ordered) * p // 100))  # ceil
    return ordered[int(rank) - 1]


def summary_ms(times_ms: list[float]) -> dict[str, float]:
    return {
        "median_ms": round(statistics.median(times_ms), 2),
        "p95_ms": round(percentile(times_ms, 95), 2),
        "min_ms": round(min(times_ms), 2),
        "max_ms": round(max(times_ms), 2),
    }


def git_info() -> dict[str, str]:
    def git(*args: str) -> str:
        try:
            here = os.path.dirname(os.path.abspath(__file__))
            return subprocess.run(["git", *args], cwd=here, capture_output=True, text=True, timeout=10).stdout.strip()
        except Exception:
            return ""

    return {
        "branch": git("rev-parse", "--abbrev-ref", "HEAD"),
        "commit": git("rev-parse", "HEAD"),
        "dirty": "yes" if git("status", "--porcelain", "--untracked-files=no") else "no",
    }


async def database_info() -> dict[str, Any]:
    async with own_connection(30) as conn:
        cur = await conn.execute(
            """SELECT current_setting('server_version') AS postgres,
                      (SELECT extversion FROM pg_extension WHERE extname = 'pg_search') AS pg_search,
                      current_database() AS database,
                      current_setting('shared_buffers') AS shared_buffers,
                      current_setting('max_parallel_workers_per_gather') AS max_parallel_workers_per_gather"""
        )
        return dict(await cur.fetchone())  # type: ignore[arg-type]


def load_results(out_dir: str) -> dict[str, Any]:
    path = os.path.join(out_dir, "results.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"steps": {}}


def save_results(out_dir: str, results: dict[str, Any]) -> None:
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "results.json")
    with open(path + ".tmp", "w") as f:
        json.dump(results, f, indent=1, default=str)
    os.replace(path + ".tmp", path)
