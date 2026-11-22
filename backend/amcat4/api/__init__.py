"""AmCAT4 API."""

import asyncio
import contextlib
import logging
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from psycopg.errors import DataError, QueryCanceled
from psycopg_pool import PoolTimeout
from starlette.middleware.sessions import SessionMiddleware

from amcat4.api.api_keys import app_api_keys
from amcat4.api.auth import app_auth
from amcat4.api.index import app_index
from amcat4.api.index_documents import app_index_documents
from amcat4.api.index_fields import app_index_fields
from amcat4.api.index_multimedia import app_multimedia
from amcat4.api.index_query import app_index_query
from amcat4.api.index_users import app_index_users
from amcat4.api.jobs import app_jobs
from amcat4.api.requests import app_requests
from amcat4.api.server import app_info
from amcat4.api.users import app_users
from amcat4.auth.CSRFMiddleware import CSRFMiddleware
from amcat4.auth.oauth import MAX_AGE_SESSION
from amcat4.config import get_settings
from amcat4.connections import amcat_connections, server_defaults
from amcat4.errors import ConflictError, NotFoundError
from amcat4.projects.jobs import job_worker
from amcat4.projects.maintenance import prewarm_indexes
from amcat4.systemdata.manage import create_or_update_systemdata


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("Initializing system data...")
    async with amcat_connections():
        await create_or_update_systemdata()
        worker = asyncio.create_task(job_worker())
        prewarm = asyncio.create_task(_prewarm()) if get_settings().prewarm else None
        try:
            yield
        finally:
            for task in (worker, prewarm):
                if task is not None:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task


async def _prewarm() -> None:
    """
    Load the search index of every partition into postgres' cache (AMCAT4_PREWARM, default false, so off as in the
    original): the first searches after a restart otherwise read the index from disk. Runs in the background, so the
    server starts at once. With the server's settings (no API statement timeout): loading a large index takes a while
    """
    with server_defaults():
        await prewarm_indexes()


app = FastAPI(
    title="AmCAT4",
    description=__doc__ if __doc__ else "",
    openapi_url="/api/openapi.json",
    docs_url="/api/docs",
    redoc_url=None,
    openapi_tags=[
        dict(name="auth", description="Endpoints for authentication"),
        dict(name="users", description="Endpoints for server user management"),
        dict(name="requests", description="Endpoints for authorization requests"),
        dict(
            name="index",
            description="Endpoints to create, list, and delete indices",
        ),
        dict(name="documents", description="Endpoints to upload, retrieve, modify, and delete documents"),
        dict(name="project users", description="Endpoints for project user management"),
        dict(name="project fields", description="Endpoints to list, create, and modify project index fields"),
        dict(name="query", description="Endpoints to list or query documents or run aggregate queries"),
        dict(name="middlecat", description="MiddleCat authentication"),
        dict(name="api keys", description="Endpoints for API key management"),
        dict(name="jobs", description="Endpoints to follow or cancel background jobs"),
    ],
    lifespan=lifespan,
)

api_router = APIRouter(prefix="/api")
api_router.include_router(app_auth)
api_router.include_router(app_info)
api_router.include_router(app_users)
api_router.include_router(app_index)
api_router.include_router(app_index_documents)
api_router.include_router(app_index_fields)
api_router.include_router(app_index_users)
api_router.include_router(app_index_query)
api_router.include_router(app_requests)
api_router.include_router(app_multimedia)
api_router.include_router(app_api_keys)
api_router.include_router(app_jobs)
app.include_router(api_router)


@app.get("/")
async def root_message():
    return {"message": "This is where the client goes"}


app.add_middleware(GZipMiddleware, minimum_size=1000)

app.add_middleware(
    SessionMiddleware,
    secret_key=get_settings().cookie_secret,
    session_cookie="amcat_session",
    same_site="lax",
    https_only=True,
    max_age=MAX_AGE_SESSION,
)

app.add_middleware(CSRFMiddleware)


@app.exception_handler(NotFoundError)
async def not_found_handler(request: Request, exc: NotFoundError) -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": str(exc)})


@app.exception_handler(ConflictError)
async def conflict_handler(request: Request, exc: ConflictError) -> JSONResponse:
    return JSONResponse(status_code=409, content={"detail": str(exc)})


@app.exception_handler(DataError)
async def data_error_handler(request: Request, exc: DataError) -> JSONResponse:
    """Invalid values in the database layer, e.g. a non-numeric value in a numeric filter"""
    return JSONResponse(status_code=422, content={"detail": str(exc).split("\n")[0]})


@app.exception_handler(PoolTimeout)
async def pool_timeout_handler(request: Request, exc: PoolTimeout) -> JSONResponse:
    """
    No free database connection within pool_timeout_s (AMCAT4_POOL_TIMEOUT_S, default 5 s; the original waited the
    pool's default of 30 s and then failed with HTTP 500): answer quickly so the client can back off and retry
    """
    return JSONResponse(
        status_code=503,
        headers={"Retry-After": "1"},
        content={"detail": "The server is busy (no free database connection), please try again shortly"},
    )


@app.exception_handler(QueryCanceled)
async def query_canceled_handler(request: Request, exc: QueryCanceled) -> JSONResponse:
    """
    A statement of an API request ran longer than statement_timeout_ms (AMCAT4_STATEMENT_TIMEOUT_MS, default 30 s; 0 =
    no limit, as in the original) and was stopped, so that a runaway search does not hold a connection for minutes.
    Background jobs and long writes run without this limit (connections.with_server_defaults)
    """
    seconds = get_settings().statement_timeout_ms / 1000
    return JSONResponse(
        status_code=503,
        content={"detail": f"The query took longer than {seconds:g} seconds and was stopped; try a more specific query"},
    )


@app.exception_handler(ValueError)
async def value_error_exception_handler(request: Request, exc: ValueError):
    return JSONResponse(
        status_code=400,
        content={"message": str(exc)},
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=422, content={"message": "There was an issue with the data you sent.", "detail": exc.errors()}
    )
