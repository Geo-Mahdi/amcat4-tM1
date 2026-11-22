"""
Helpers for migrations that change the (big, busy) documents table.
"""

import logging
import time

from psycopg.errors import LockNotAvailable
from sqlalchemy import Connection, text
from sqlalchemy.exc import DBAPIError

log = logging.getLogger("alembic.runtime.migration")

# How long one attempt waits for the lock, and how many attempts are made
LOCK_TIMEOUT = "5s"
LOCK_ATTEMPTS = 6


class TableBusyError(RuntimeError):
    """The documents table stayed in use; the migration stopped without changing it (see alter_with_lock_timeout)"""


def alter_with_lock_timeout(
    bind: Connection, statement: str, timeout: str = LOCK_TIMEOUT, attempts: int = LOCK_ATTEMPTS
) -> None:
    """
    Run an ALTER TABLE statement on the documents table with a lock timeout, in a savepoint, retrying a few times.

    ALTER TABLE (ADD COLUMN, DROP CONSTRAINT, ...) needs an exclusive lock on the table and all its partitions. While
    it waits for a long query or job that uses the table, every new query on the table waits behind it: the server
    would hang until that query is done. With a lock timeout the statement gives up instead (and the queries go on),
    and is tried again a little later. If the table stays busy, the migration stops with TableBusyError, without
    changing anything; it can simply be run again (restart the server, or `alembic upgrade head`).
    The original migration (0001) created the table and needed no lock timeout. Up to LOCK_ATTEMPTS * LOCK_TIMEOUT plus
    the pauses (about 45 s) in all.
    """
    for attempt in range(1, attempts + 1):
        try:
            with bind.begin_nested():
                bind.execute(text(f"SET LOCAL lock_timeout = '{timeout}'"))
                bind.execute(text(statement))
                # SET LOCAL holds until the end of the migration's transaction, not of the savepoint: reset it, so the
                # other steps of the migration wait as long as they need
                bind.execute(text("SET LOCAL lock_timeout = DEFAULT"))
            return
        except DBAPIError as e:
            if not isinstance(e.orig, LockNotAvailable):
                raise
            log.warning(f"The documents table is in use (attempt {attempt} of {attempts}, waited {timeout}): {e.orig}")
            if attempt < attempts:
                time.sleep(attempt)  # a short pause, a bit longer each time (1, 2, ... seconds)
    raise TableBusyError(
        f"Migration stopped: could not get a lock on the documents table within {timeout}, {attempts} times, because "
        "other queries or jobs keep using it. Nothing was changed. Please run the migration again (restart the server, "
        "or run `alembic upgrade head`), at a quieter moment or after stopping long-running jobs and queries "
        "(see pg_stat_activity)."
    )
