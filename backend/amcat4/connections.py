import functools
import logging
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from contextvars import ContextVar
from typing import Any, AsyncGenerator, Awaitable, Callable, Iterator, ParamSpec, TypeVar

import httpx
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from types_aiobotocore_s3.client import S3Client

from amcat4.config import get_settings


class AmcatConnections:
    db: AsyncConnectionPool[Any] | None
    db_work: AsyncConnectionPool[Any] | None
    s3_client: S3Client | None
    s3_context_stack: AsyncExitStack | None
    http_client: httpx.AsyncClient | None

    def __init__(
        self,
        db: AsyncConnectionPool[Any] | None = None,
        db_work: AsyncConnectionPool[Any] | None = None,
        s3_client: S3Client | None = None,
        s3_proxy_client: S3Client | None = None,
        s3_context_stack: AsyncExitStack | None = None,
        http_client: httpx.AsyncClient | None = None,
    ):
        self.db = db
        self.db_work = db_work
        self.s3_client = s3_client
        self.s3_proxy_client = s3_client
        self.s3_context_stack = s3_context_stack
        self.http_client = http_client


CONNECTIONS = AmcatConnections(s3_client=None, s3_proxy_client=None, db=None, http_client=None)  # type: ignore


@asynccontextmanager
async def amcat_connections() -> AsyncGenerator[None, None]:
    """
    The main context manager to start and stop connections used by amcat.
    Always use this once (and only once):
        - For running the server: in the FastAPI startup and shutdown events
        - For tests: in the setup fixture in the tests
        - For CLI commands: within the CLI command
    """
    try:
        await _start_s3()
        await _start_db()
        await _start_http()
        yield
    finally:
        await _close_s3()
        await _close_db()
        await _close_http()


# Whether the current task does background or maintenance work (see server_defaults)
_SERVER_DEFAULTS: ContextVar[bool] = ContextVar("amcat4_server_defaults", default=False)


@contextmanager
def server_defaults() -> Iterator[None]:
    """
    Run the database work inside this block with the server's own settings, on a separate connection pool.

    The API's request connections are tuned for many short read queries (see _api_options). Background jobs,
    index maintenance, CLI commands and long writes (uploads, update/delete by query) should not get those settings:
    they keep the parallel workers and have no API statement timeout. The setting is a context variable, so it
    holds for the current task and for tasks started from it (e.g. the job worker and its heartbeat).
    """
    token = _SERVER_DEFAULTS.set(True)
    try:
        yield
    finally:
        _SERVER_DEFAULTS.reset(token)


P = ParamSpec("P")
T = TypeVar("T")


def with_server_defaults(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
    """Decorator: run this (long) database work with the server's settings, see server_defaults"""

    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        with server_defaults():
            return await func(*args, **kwargs)

    return wrapper


def db() -> AsyncConnectionPool[Any]:
    """
    Access the postgres connection pool. Use amcat4.postgres.connection.connection() to get a connection.
    Inside server_defaults() this is the pool for background and maintenance work, otherwise the API pool.
    """
    pool = CONNECTIONS.db_work if _SERVER_DEFAULTS.get() else CONNECTIONS.db
    if pool is None:
        raise ConnectionError("Database connection not initialized")
    return pool


def db_schema() -> str:
    """The postgres schema that contains the amcat tables (a separate schema is used for unit tests)"""
    settings = get_settings()
    return f"{settings.postgres_schema}_test" if settings.test_mode else settings.postgres_schema


def s3() -> S3Client:
    """
    Access the s3 client.
    """
    if CONNECTIONS.s3_client is None:
        raise ConnectionError("S3 client not started")
    return CONNECTIONS.s3_client


def s3_public() -> S3Client:
    """
    Only use this for creating presigned requests for the public
    s3 server. If the s3 server is not publicly accessible, set s3_use_proxy
    to use the s3_proxy_client, which signs urls
    """
    settings = get_settings()
    use_proxy = settings.s3_use_proxy and not settings.test_mode
    s3 = CONNECTIONS.s3_proxy_client if use_proxy else CONNECTIONS.s3_client
    if s3 is None:
        raise ConnectionError("S3 client not started")
    return s3


def s3_enabled() -> bool:
    settings = get_settings()
    return all([settings.s3_host, settings.s3_access_key, settings.s3_secret_key])


def http() -> httpx.AsyncClient:
    if CONNECTIONS.http_client is None:
        raise ConnectionError("HTTP client not started")
    return CONNECTIONS.http_client


def _api_options() -> str:
    """
    Settings for the API's request connections (startup options, so they hold for every statement on them).
    """
    settings = get_settings()
    options = []
    workers = settings.api_parallel_workers
    if workers is not None:
        # pg_search searches of the API take a few ms; starting parallel workers for them costs more than it saves
        # (a count of a normal word: 10 ms with 4 workers, 3.7 ms without; and it also costs throughput under load).
        # Only very common words get slower, which the filter form of the project clause (search.py) makes up for.
        options.append(f"-c max_parallel_workers_per_gather={workers}")
    if settings.statement_timeout_ms:
        # a runaway search should not hold a connection (and a CPU) for minutes; the request gets a 503
        options.append(f"-c statement_timeout={int(settings.statement_timeout_ms)}")
    return " ".join(options)


async def _open_pool(name: str, options: str, min_size: int, max_size: int, timeout: float = 30.0) -> AsyncConnectionPool[Any]:
    settings = get_settings()
    options = f"-c search_path={db_schema()},public {options}"
    if settings.custom_plans:
        # psycopg prepares a statement after 5 uses on a connection, and postgres may then switch to a generic plan
        # that does not know the project or query. For pg_search that plan is 5-10x slower (row estimate 1, no
        # parallel workers). Planning each statement costs 1-3 ms, which is much less.
        options += " -c plan_cache_mode=force_custom_plan"
    pool = AsyncConnectionPool(
        settings.postgres_url,
        min_size=min_size,
        max_size=max_size,
        open=False,
        name=name,
        timeout=timeout,
        kwargs={"row_factory": dict_row, "autocommit": True, "options": options.strip()},
    )
    try:
        await pool.open(wait=True, timeout=10)
    except Exception as e:
        await pool.close()
        raise ConnectionError(f"Cannot connect to postgres server: {e}") from e
    return pool


async def _start_db():
    logging.debug(f"Connecting with postgres, schema {db_schema()}")
    # Two pools: one for API requests (tuned for short queries), one with the server's defaults for background jobs,
    # maintenance, CLI commands and long writes (see server_defaults). The second one opens connections only when needed.
    # An API request waits at most pool_timeout_s for a connection (then HTTP 503, see api/__init__.py)
    CONNECTIONS.db = await _open_pool(
        "amcat4-api", _api_options(), min_size=1, max_size=20, timeout=get_settings().pool_timeout_s
    )
    try:
        CONNECTIONS.db_work = await _open_pool("amcat4-work", "", min_size=0, max_size=10)
    except Exception:
        await _close_db()
        raise


async def _close_db() -> None:
    if CONNECTIONS.db is not None:
        await CONNECTIONS.db.close()
        CONNECTIONS.db = None
    if CONNECTIONS.db_work is not None:
        await CONNECTIONS.db_work.close()
        CONNECTIONS.db_work = None


async def _start_s3() -> None:
    if s3_enabled() is False:
        return None

    settings = get_settings()

    if settings.s3_host is None:
        raise ValueError("s3_host not specified")
    if settings.s3_access_key is None or settings.s3_secret_key is None:
        raise ValueError("s3_access_key or s3_secret_key not specified")

    session = get_session()
    client = session.create_client(
        service_name="s3",
        endpoint_url=settings.s3_host,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        config=AioConfig(signature_version="s3v4"),
    )

    # If the s3 server is hosted directly with docker compose, fastapi needs to
    # use a client with the internal s3_host, but presigned requests need to be created
    # for the /s3 proxy.
    proxy_url = settings.host + "/s3" if settings.s3_use_proxy else settings.s3_host
    proxy_client = session.create_client(
        service_name="s3",
        endpoint_url=proxy_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        config=AioConfig(signature_version="s3v4"),
    )

    CONNECTIONS.s3_context_stack = AsyncExitStack()
    CONNECTIONS.s3_client = await CONNECTIONS.s3_context_stack.enter_async_context(client)
    CONNECTIONS.s3_proxy_client = await CONNECTIONS.s3_context_stack.enter_async_context(proxy_client)


async def _close_s3():
    if CONNECTIONS.s3_context_stack is not None:
        await CONNECTIONS.s3_context_stack.aclose()
        CONNECTIONS.s3_client = None
        CONNECTIONS.s3_proxy_client = None
        CONNECTIONS.s3_context_stack = None


async def _start_http():
    # You can set global timeouts or headers here
    CONNECTIONS.http_client = httpx.AsyncClient(timeout=10.0)


async def _close_http():
    if CONNECTIONS.http_client is not None:
        await CONNECTIONS.http_client.aclose()
        CONNECTIONS.http_client = None
