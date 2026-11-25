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
                "Use the faster read paths: counts and aggregations inside the BM25 index of a single partition, "
                "without a duplicate project term. "
                "Why: aggregating on the partition table instead of the parent table took 37 ms instead of 1,640 ms, "
                "with the same result (the original statement, a 2.68 M document project in a partition of 13.4 M "
                "documents, pg_search 0.26.0, 16 Nov 2026; not re-measured on this code). "
                "Set to false to use the original SQL, e.g. to compare results "
                "or if a result looks wrong (AMCAT4_FAST_PATH=false)"
            ),
        ),
    ] = True

    score_partition_condition: Annotated[
        bool,
        Field(
            description=(
                "On the fast read path, keep the SQL condition on partition_id when results are ordered by relevance. "
                "Why: pg_search 0.27.0 returns a wrong top list (not the documents with the highest scores) when the "
                "search condition is the only condition of the statement and the order is by score; with one more "
                "plain condition the top list is correct (25 Nov 2026, 6 of 6 test queries wrong without it, 6 of 6 "
                "correct with it). Other orders (date, sort columns, id) were correct and do not get the condition. "
                "false = the statement of postgres-fast-v2, e.g. to test whether a new pg_search version still "
                "needs this (AMCAT4_SCORE_PARTITION_CONDITION=false)"
            ),
        ),
    ] = True

    new_query_api: Annotated[
        bool,
        Field(
            description=(
                "Send search conditions in ParadeDB's new SQL query API (pdb.* functions and the operators |||, ===, "
                "###, @@@ on the json keys) instead of the legacy json query objects, and use pdb.score and "
                "pdb.snippet_positions. "
                "Why: ParadeDB deprecated the legacy json objects and will remove them (no date yet). AmCAT's own "
                "query parser, filters and aggregations do not change: only the statement that is sent does "
                "(amcat4/postgres/pdb_sql.py). Hits, counts, scores and the relevance order are the same in both "
                "formats (tests/test_new_query_api.py). "
                "false (the default for now) = the legacy json objects, as before "
                "(AMCAT4_NEW_QUERY_API=true switches to the new API; see docs/postgres-fast-v3/NEW_QUERY_API.md)"
            ),
        ),
    ] = False

    filter_clauses: Annotated[
        bool,
        Field(
            description=(
                "Select the project and apply filters in the search index without scoring them (a const_score "
                "clause), so only the query string is scored. "
                "Why: top 10 for a very common word: 5.8 ms -> 2.9 ms (synthetic 156k documents, pg_search 0.27). "
                "Counts and hits are the same; only the relevance order changes, once, to the plain BM25 order of "
                "the query. "
                "false = the original form, in which every clause is scored (AMCAT4_FILTER_CLAUSES=false)"
            ),
        ),
    ] = True

    capped_count: Annotated[
        bool,
        Field(
            description=(
                "Count hits in two steps (first up to 1000 one by one, then in the index if there are more), as in "
                "the original. "
                "Default false: one count. Why: the two steps paid off only while a count had a floor of 7-12 ms "
                "with parallel workers; "
                "without workers a full count takes 2-4 ms and every extra statement costs planning time. true = "
                "the original behaviour (AMCAT4_CAPPED_COUNT)"
            ),
        ),
    ] = False

    parallel_count: Annotated[
        bool,
        Field(
            description=(
                "Count the hits of a search on a second connection while the page is fetched, if the pool has one to "
                "spare (otherwise one after the other). "
                "Why: the request takes as long as the slower of the two (a common word: 8.0 ms -> 4.9 ms; not "
                "measured under concurrent load). "
                "false = always one after the other, as in the original (AMCAT4_PARALLEL_COUNT=false)"
            ),
        ),
    ] = True

    max_offset: Annotated[
        int,
        Field(
            description=(
                "Maximum depth (offset + per_page) of a request for a page number (page); deeper pages get HTTP 400, "
                'with the advice to use "sort": "_seq" (paging by id, fast at any depth). Why: offset paging '
                "computes and throws away all earlier hits, "
                "so a download got slower with every page (a download by offset: 25,500 documents/s, by id: 107,600, "
                "on a synthetic 156k document partition with an earlier version of this branch; "
                "like max_result_window in Elasticsearch). "
                "Cursors (after) that the server issued are never limited. 0 = no limit, as in the original"
            ),
        ),
    ] = 10000

    btree_listing: Annotated[
        bool,
        Field(
            description=(
                "List documents without a query string (and without filters that need the search index) in id order "
                "with plain SQL over the B-tree index (partition_id, project_pk, id) of migration 0002, which reads "
                "only the rows of the page. "
                "Why: a plain listing in id order does not need the search index. Counts stay in the search "
                "index. "
                "false = through the search index, as in the original (AMCAT4_BTREE_LISTING=false)"
            ),
        ),
    ] = True

    byte_order_sort: Annotated[
        bool,
        Field(
            description=(
                'Sort text values (keyword, url, text, tag) that are not in a sort slot in byte order (COLLATE "C"), like the '
                "slot columns and Elasticsearch. "
                "Why: otherwise the order of mixed-case values changes when a field gets a slot, or while a slot is "
                "being filled. "
                "false = the database's collation, as in the original (AMCAT4_BYTE_ORDER_SORT=false)"
            ),
        ),
    ] = True

    cutoff_sort: Annotated[
        bool,
        Field(
            description=(
                "Sort on a field without a sort slot (keyword, url, tag, integer, number, date) with the cutoff sort: "
                "an aggregation in the search index finds the value of the last document of the page, and only the "
                "documents up to that value are read and sorted, instead of every hit. Both statements of such a page "
                "then run in one snapshot. "
                "Why: sorting all hits took 110 to 1,100 ms for 100,000 to 1,000,000 hits, the cutoff sort 7 to 43 ms "
                "(research prototype on 1 M documents, pg_search 0.27.0, 25 Nov 2026; not re-measured on this code). "
                "The results are the same, except for tag fields: with this switch on they sort like Elasticsearch "
                "(ascending on the smallest tag of a document, descending on the largest), not on the text of the "
                "whole tag list. "
                "false = sort all hits, and the old tag order, as in postgres-fast-v2 (AMCAT4_CUTOFF_SORT=false)"
            ),
        ),
    ] = True

    quarter_key: Annotated[
        bool,
        Field(
            description=(
                "Aggregate date fields by quarter on the derived quarter key (migration 0002), inside the search "
                "index, also with metrics. "
                "Why: it avoids date_trunc per row, and avg/min/max/sum per quarter were not possible in the index "
                "before. "
                "Fields whose older documents do not have the key yet keep the slower way until the backfill job is "
                "done (results are the same). "
                "false = always the slower way, as in the original (AMCAT4_QUARTER_KEY=false)"
            ),
        ),
    ] = True

    index_agg_types: Annotated[
        bool,
        Field(
            description=(
                "Aggregate inside the search index (pdb.agg) also for: group by a number, integer or boolean field, "
                "the minimum and maximum of a date field, and metrics without any axis. "
                "Why: these read every matching row before (group by an integer field: 199 ms, a keyword axis with "
                "max(date): 409 ms, against 2-23 ms for the same aggregation in the index, tried by hand on 200k "
                "documents, pg_search 0.27.0, 25 Nov 2026; not re-measured on this code). "
                "The rows are the same as GROUP BY (tested, also for number fields with whole and decimal values). "
                "Still read from the rows: number intervals, hour and minute, url axes, avg and sum of dates. "
                "false = these use GROUP BY, as in postgres-fast-v2 (AMCAT4_INDEX_AGG_TYPES=false)"
            ),
        ),
    ] = True

    index_field_stats: Annotated[
        bool,
        Field(
            description=(
                "Get the values of a field (the lists of filter values) and the statistics of a field (count, "
                "minimum, maximum, average) from an aggregation inside the search index. "
                "Why: the original statements read every document of the project (values: 441 ms, statistics: "
                "235 ms on 200k documents, against 2-12 ms for the aggregation, tried by hand, pg_search 0.27.0, "
                "25 Nov 2026; not re-measured on this code). "
                "The output is the same, except: the average is computed with floats inside the index (for dates it "
                "can differ by microseconds), and a tag that is twice in one document counts once. "
                "Values of number and date fields, fields with more than 65,000 different values, and keyword "
                "values that look like a full timestamp keep the original statement. "
                "false = always the original statements (AMCAT4_INDEX_FIELD_STATS=false)"
            ),
        ),
    ] = True

    text_exists_key: Annotated[
        bool,
        Field(
            description=(
                "Filter on 'this text field has a value' (exists) inside the search index, on the list of text keys "
                "that every document has in exact_fields since migration 0004 (written by a database trigger). "
                "Why: it was the only filter outside the index, a SQL condition that reads the long text of every "
                "hit (a page 180 ms and a count 260 ms on 200k documents, measured by hand on postgres-fast-v2, "
                "25 Nov 2026; the new filter is a term like any keyword filter, not measured on this code). "
                "Text fields whose older documents do not have the list yet keep the SQL condition until the "
                "background job of their project is done (results are the same). "
                "false = always the SQL condition, as in the original (AMCAT4_TEXT_EXISTS_KEY=false). The list is "
                "written in both cases"
            ),
        ),
    ] = True

    auto_sort_slot: Annotated[
        bool,
        Field(
            description=(
                "Give every new field a free sort slot of its kind when it is created (keyword and url: 4 slots, "
                "date: 2, number and integer: 1 per project), first come, first served. "
                "Why: sorting on a slot is a Top-K scan inside the search index (sort by publisher: 2.9 to 4.6 ms "
                "on a slot against 48.7 ms on the json value, Nov 2026), and a new field has no values yet, so the "
                "slot costs no fill job; before, only the first date field and a keyword field called source got a "
                "slot without asking (fast_sort). "
                "When the slots of a kind are taken, later fields get none; fast_sort false on a field frees its "
                "slot. Existing fields are not changed. "
                "false = only the date and source slots are given automatically, as in postgres-fast-v2 "
                "(AMCAT4_AUTO_SORT_SLOT=false)"
            ),
        ),
    ] = True

    lz4_json_columns: Annotated[
        bool,
        Field(
            description=(
                "Compress new values of exact_fields and stored_fields with lz4 instead of postgres' default "
                "(pglz), as text_fields since migration 0002, and give the staging table of uploads the same "
                "settings. Set at startup on the documents table and its partitions; existing rows are not "
                "rewritten and keep their compression. "
                "Why: lz4 is faster to compress and to read. An uploaded value kept the compression of the "
                "staging table (pglz), so until now uploaded texts were not lz4 either (seen with "
                "pg_column_compression, 25 Nov 2026). Postgres only compresses values of about 2 kB and more, so "
                "for exact_fields and stored_fields this matters for documents with much metadata or large "
                "objects. The effect on speed is not measured. Skipped with a warning if the server has no lz4. "
                "false = the server's default for new values of the two columns, and uploads as before "
                "(AMCAT4_LZ4_JSON_COLUMNS=false)"
            ),
        ),
    ] = True

    sort_slot_job_rows: Annotated[
        int,
        Field(
            description=(
                "fast_sort on a field of a project with more documents than this fills (or clears) the sort slot in "
                "a background job, in batches. "
                "Why: filling a slot rewrites every document that has a value, too long for one request on a large "
                "project. "
                "Until the job is done the field is sorted the slower way. Smaller projects: right away, in the "
                "request. "
                "-1 = always in the request (as in the original); 0 = always a job"
            ),
        ),
    ] = 10000

    vacuum_after_rows: Annotated[
        int,
        Field(
            description=(
                "After an upload, update or delete (by query) that changed at least this many documents of a "
                "partition (also counted over several smaller uploads), "
                "VACUUM (ANALYZE) the partition in a background job. Why: in-index counts skip the visibility check "
                "only for pages that the visibility map "
                "marks all-visible, and only VACUUM updates that map; autovacuum starts late on big tables (20% of "
                "the rows by default). "
                "0 = off (leave it to autovacuum, as in the original)"
            ),
        ),
    ] = 10000

    prewarm: Annotated[
        bool,
        Field(
            description=(
                "At startup, load the search indexes into postgres' cache with pg_prewarm (in the background; "
                "skipped with a warning if the extension is not available). "
                "Why: the first searches after a restart read the index from disk. Default false (off, as in the "
                "original); the effect is not measured"
            ),
        ),
    ] = False

    custom_plans: Annotated[
        bool,
        Field(
            description=(
                "Run API statements with plan_cache_mode=force_custom_plan, so prepared statements never switch to a "
                "generic plan that ignores the project "
                "and query. Why: psycopg prepares a statement after 5 uses on a connection; the generic plan made "
                "counts 5-10x slower (469 ms against 84 ms "
                "for a very common word, from the 11th request on, in the benchmark of the original code on 2.68 M "
                "documents, pg_search 0.26.0, 15-16 Nov 2026); planning each statement costs 1-3 ms. false = "
                "the postgres default (AMCAT4_CUSTOM_PLANS=false)"
            ),
        ),
    ] = True

    api_parallel_workers: Annotated[
        int | None,
        Field(
            description=(
                "max_parallel_workers_per_gather for the connections of API requests. 0 (default) means no parallel "
                "workers. "
                "Why: the searches of the API take a few ms and starting workers costs more than it saves (a count "
                "of a normal word: 10 ms with 4 workers, 3.7 ms without), "
                "also under load; very common words get slower, which the unscored project clause (filter_clauses) "
                "makes up for. "
                "Empty or 'default' keeps the server's setting (as in the original). Background jobs, maintenance and "
                "CLI commands always keep the server's setting"
            ),
        ),
    ] = 0

    statement_timeout_ms: Annotated[
        int,
        Field(
            description=(
                "Stop a statement of an API request that runs longer than this (milliseconds); the request gets HTTP "
                "503. "
                "Why: a runaway search should not hold a connection and a CPU for minutes. 0 = no limit, as in "
                "the original. "
                "Background jobs, maintenance, CLI commands and long writes (uploads, update or delete by query, "
                "field changes) have no limit"
            ),
        ),
    ] = 30000

    pool_timeout_s: Annotated[
        float,
        Field(
            description=(
                "How long an API request waits for a free database connection (seconds). After that it gets HTTP 503 "
                "with Retry-After, "
                "so an overloaded server answers quickly and clients can back off, instead of letting requests "
                "queue. "
                "The original used the pool's default of 30 seconds; set 30 to get that back"
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
        # An empty value or 'default' means: do not set the option, keep the server's setting (the original behaviour).
        # Needed because an empty environment variable would otherwise fail to parse as an int.
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
