"""
AmCAT4 Configuration

We read configuration from 2 sources, in order of precedence (higher is more priority)
- Environment variables
- A .env file, either in the current working directory or in a location specified
  by the AMCAT4_CONFIG_FILE environment variable
"""

import functools
import secrets
from enum import Enum
from typing import Annotated

import questionary
from class_doc import extract_docs_from_cls_obj
from dotenv import load_dotenv
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_PREFIX = "amcat4_"


class AuthOptions(str, Enum):
    #: everyone (that can reach the server) can do anything they want
    no_auth = "no_auth"

    #: everyone can use the server, dependent on index-level guest_role authorization settings
    allow_guests = "allow_guests"

    #: everyone can use the server, if they have a valid middlecat login,
    #: and dependent on index-level guest_role authorization settings
    allow_authenticated_guests = "allow_authenticated_guests"

    @classmethod
    def validate(cls, value: str):
        if value not in cls.__members__:
            options = ", ".join(AuthOptions.__members__.keys())
            return f"{value} is not a valid authorization option. Choose one of {{{options}}}"


# Set the __doc__ attribute of each AuthOptions enum member using extract_docs_from_cls_obj
for field, doc in extract_docs_from_cls_obj(AuthOptions).items():
    AuthOptions[field].__doc__ = "\n".join(doc)


class Settings(BaseSettings):
    host: Annotated[
        str,
        Field(
            description="Host this instance is served at (needed for checking tokens)",
        ),
    ] = "http://localhost:5000"

    postgres_url: Annotated[
        str,
        Field(
            description="PostgreSQL connection URL. The server needs the pg_search extension (e.g. the paradedb image)",
        ),
    ] = "postgresql://amcat:amcat@localhost:5432/amcat"

    postgres_schema: Annotated[
        str,
        Field(
            description="Postgres schema in which AmCAT stores its tables",
        ),
    ] = "amcat"

    partition_max_gb: Annotated[
        float,
        Field(
            description="New projects go to a new documents partition once the BM25 index of the current one is this big (GB)",
        ),
    ] = 5

    fast_path: Annotated[
        bool,
        Field(
            description=(
                "Use the faster read paths (counts and aggregations inside the BM25 index of a single partition). "
                "Set to false to use the original SQL, e.g. to compare results (AMCAT4_FAST_PATH=false)"
            ),
        ),
    ] = True

    filter_clauses: Annotated[
        bool,
        Field(
            description=(
                "Select the project and apply filters in the search index without scoring them (a const_score clause), "
                "so only the query string is scored. Faster for common words; changes the relevance order once (to the "
                "plain BM25 order of the query). Counts and hits are the same. false = the original form, in which "
                "every clause is scored (AMCAT4_FILTER_CLAUSES=false)"
            ),
        ),
    ] = True

    capped_count: Annotated[
        bool,
        Field(
            description=(
                "Count hits in two steps (first up to 1000 one by one, then in the index if there are more), as in "
                "version 1. Default false: one count, which is cheaper without parallel workers (AMCAT4_CAPPED_COUNT)"
            ),
        ),
    ] = False

    parallel_count: Annotated[
        bool,
        Field(
            description=(
                "Count the hits of a search on a second connection while the page is fetched, if the pool has one to "
                "spare (otherwise one after the other). AMCAT4_PARALLEL_COUNT=false: always one after the other"
            ),
        ),
    ] = True

    max_offset: Annotated[
        int,
        Field(
            description=(
                "Maximum depth (offset + per_page) of offset paging in score or field order; deeper pages get HTTP 400, "
                'with the advice to use "sort": "_id" (paging by id, fast at any depth). 0 = no limit'
            ),
        ),
    ] = 10000

    btree_listing: Annotated[
        bool,
        Field(
            description=(
                "List documents without a query string (and without filters that need the search index) in id order "
                "with plain SQL over the B-tree index (partition_id, project_pk, id) of migration 0002, which reads only "
                "the rows of the page. false = through the search index, as before (AMCAT4_BTREE_LISTING=false)"
            ),
        ),
    ] = True

    byte_order_sort: Annotated[
        bool,
        Field(
            description=(
                'Sort text values (keyword, url) that are not in a sort slot in byte order (COLLATE "C"), like the slot '
                "columns and Elasticsearch, so the order does not change when a field gets a slot. false = the database's "
                "collation, as before (AMCAT4_BYTE_ORDER_SORT=false)"
            ),
        ),
    ] = True

    quarter_key: Annotated[
        bool,
        Field(
            description=(
                "Aggregate date fields by quarter on the derived quarter key (migration 0002), inside the search index, "
                "also with metrics. Fields whose older documents do not have the key yet keep the slower way until the "
                "backfill job is done. false = always the slower way, as before (AMCAT4_QUARTER_KEY=false)"
            ),
        ),
    ] = True

    sort_slot_job_rows: Annotated[
        int,
        Field(
            description=(
                "fast_sort on a field of a project with more documents than this fills (or clears) the sort slot in a "
                "background job, in batches; until it is done the field is sorted the slower way. Smaller projects: "
                "right away, in the request. -1 = always in the request (as before); 0 = always a job"
            ),
        ),
    ] = 10000

    vacuum_after_rows: Annotated[
        int,
        Field(
            description=(
                "After an upload, update or delete (by query) that changed at least this many documents of a partition "
                "(also counted over several smaller uploads), VACUUM (ANALYZE) the partition in a background job, so "
                "counts can use the visibility map. 0 = off (leave it to autovacuum)"
            ),
        ),
    ] = 10000

    prewarm: Annotated[
        bool,
        Field(
            description=(
                "At startup, load the search indexes into postgres' cache with pg_prewarm (in the background; skipped "
                "with a warning if the extension is not available)"
            ),
        ),
    ] = False

    custom_plans: Annotated[
        bool,
        Field(
            description=(
                "Run API statements with plan_cache_mode=force_custom_plan, so prepared statements never switch to a "
                "generic plan that ignores the project and query (which made counts 5-10x slower after 10 requests)"
            ),
        ),
    ] = True

    api_parallel_workers: Annotated[
        int | None,
        Field(
            description=(
                "max_parallel_workers_per_gather for the connections of API requests. 0 (default) means no parallel "
                "workers, which is faster for the short searches of the API. Empty or 'default' keeps the server's "
                "setting. Background jobs, maintenance and CLI commands always keep the server's setting"
            ),
        ),
    ] = 0

    statement_timeout_ms: Annotated[
        int,
        Field(
            description=(
                "Stop a statement of an API request that runs longer than this (milliseconds); the request gets HTTP 503. "
                "0 = no limit. Background jobs, maintenance, CLI commands and long writes (uploads, update or delete by "
                "query, field changes) have no limit"
            ),
        ),
    ] = 30000

    pool_timeout_s: Annotated[
        float,
        Field(
            description=(
                "How long an API request waits for a free database connection (seconds). After that it gets HTTP 503 "
                "with Retry-After, so an overloaded server answers quickly instead of letting requests queue"
            ),
        ),
    ] = 5

    auth: Annotated[AuthOptions, Field(description="Do we require authorization?")] = AuthOptions.no_auth

    cookie_secret: Annotated[
        str,
        Field(
            description="Secret key for encrypting session cookies.",
        ),
    ] = secrets.token_urlsafe(32)

    middlecat_url: Annotated[
        str,
        Field(
            description="Middlecat server to trust as ID provider. ",
        ),
    ] = "https://middlecat.net"

    admin_email: Annotated[
        str | None,
        Field(
            description="Email address for a hardcoded admin email (useful for setup and recovery)",
        ),
    ] = None

    s3_host: Annotated[str | None, Field(description="S3-compatible object storage host")] = None
    s3_access_key: Annotated[str | None, Field(description="S3 access key")] = None
    s3_secret_key: Annotated[str | None, Field(description="S3 secret key")] = None
    s3_use_proxy: Annotated[
        str | None,
        Field(description="If true, use a proxy for s3_host at /s3. (proxy needs to be created by Caddy)"),
    ] = None

    oidc_url: Annotated[
        str | None,
        Field(
            description="OIDC provider URL (if using OIDC for authentication). This overrides middlecat_url if set",
        ),
    ] = None
    oidc_client_id: Annotated[
        str | None,
        Field(
            description="OIDC client ID (if using OIDC for authentication)",
        ),
    ] = None
    oidc_client_secret: Annotated[
        str | None,
        Field(
            description="OIDC client secret (if using OIDC for authentication)",
        ),
    ] = None

    test_mode: Annotated[
        bool,
        Field(
            description="Use a separate test database (for unit tests)",
        ),
    ] = False

    model_config = SettingsConfigDict(env_prefix=ENV_PREFIX)

    @field_validator("api_parallel_workers", mode="before")
    @classmethod
    def _server_default(cls, value):
        # an empty value or 'default' means: do not set it, keep the server's setting
        if isinstance(value, str) and value.strip().lower() in ("", "default", "none"):
            return None
        return value


def generate_interactive_settings(current: Settings, skip: list[str] = [], only: list[str] | None = None):
    print(f"🛠️  Configuring {Settings.__name__}\n")
    user_input = {}

    fields = Settings.model_fields

    for name, field in fields.items():
        if name in ["model_config"]:
            continue

        if name in skip:
            continue

        if only and name not in only:
            continue

        description = field.description or name

        default = getattr(current, name)
        default = default.value if isinstance(default, Enum) else default

        if field.annotation is bool or field.annotation is (bool | None):
            user_input[name] = questionary.confirm(f"{description}?", default=bool(default) if default != "" else True).ask()

        elif field.annotation and hasattr(field.annotation, "__members__"):
            choices = list(field.annotation.__members__.keys())
            user_input[name] = questionary.select(f"{description}:", choices=choices, default=default).ask()

        else:
            val = questionary.text(f"{description}:\n >>", default=str(default)).ask()
            if field.annotation is int:
                user_input[name] = int(val) if val else 0
            else:
                user_input[name] = val

        # Add an empty line for readability
        print()

    try:
        return Settings(**user_input)
    except Exception as e:
        raise ValueError(f"Invalid configuration: {e}")


def save_to_env(settings: BaseSettings, filename=".env"):
    with open(filename, "w") as f:
        for key, value in settings.model_dump().items():
            val_str = value.value if isinstance(value, Enum) else value
            val_str = str(val_str) if val_str is not None else ""

            env_var = f"{ENV_PREFIX}{key}"
            f.write(f"{env_var.upper()}={val_str}\n")


def config_tui_editor(env_file: str, dev: bool):
    print("🛠️ Amcat4 Dev Setup Wizard\n")
    current = get_settings(env_file)

    if dev:
        only = ["auth", "admin_email", "postgres_url", "s3_host", "s3_access_key", "s3_secret_key"]
        setattr(current, "s3_host", "http://localhost:8333")
        setattr(current, "s3_access_key", "DEV_S3_ACCESS_KEY")
        setattr(current, "s3_secret_key", "DEV_S3_SECRET_KEY")
        settings = generate_interactive_settings(current, only=only)
    else:
        skip = ["test_mode", "cookie_secret"]
        settings = generate_interactive_settings(current, skip=skip)

    save_to_env(settings, env_file)

    print("\n✅ Config saved to .env")


@functools.lru_cache()
def get_settings(env_file: str = ".env") -> Settings:
    load_dotenv(env_file, override=False)
    return Settings()


def validate_settings():
    if get_settings().auth != "no_auth":
        if get_settings().host.startswith("http://") and not get_settings().host.startswith("http://localhost"):
            return (
                "You have set the host at an http address and enabled authentication."
                "Authentication through middlecat will not work in your browser"
                " without additional steps. See https://github.com/ccs-amsterdam/amcat4py/issues/9"
            )


if __name__ == "__main__":
    # Echo the settings
    for k, v in get_settings().model_dump().items():
        print(f"{ENV_PREFIX.upper()}{k.upper()}={v}")
