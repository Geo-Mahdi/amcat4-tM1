"""
The frozen metadata of a release (migration 0006, see docs/postgres-fast-v3/release-metadata.md).

When a release is made, its description is computed once and stored in the releases row, never changed afterwards:
- metadata_jsonld: a schema.org / DCAT Dataset in JSON-LD (name, version, dates, fields with counts, software
  versions, access conditions). It never contains document text or values of text fields;
- metadata_sha256: SHA-256 of canonical_json(metadata_jsonld), also copied into the manifest;
- versions: the PostgreSQL, pg_search and amcat4 versions and the migration revision;
- field_definitions: the fields as they were (name, type, identifier, metareader access);
- golden_queries: a few fixed, cheap queries on the release with their results (count and first doc_pks, or the
  top values, or a date range), which verify_release runs again.

What can change later (DOI, DOI status, withdrawn, public flag) are separate columns, added at display time
(live_metadata). The DOI is never part of the frozen JSON-LD.
"""

import hashlib
import json
import re
from datetime import date, datetime
from decimal import Decimal
from importlib.metadata import version as package_version
from typing import Any

from psycopg import AsyncConnection, sql

from amcat4.config import get_settings
from amcat4.postgres.layout import project_filter
from amcat4.postgres.versions import DOC_PK, visible

# Field types whose values are counted in the golden queries (top values). Never text fields.
_TOP_VALUE_TYPES = ("keyword", "tag")
_MAX_TOP_VALUE_FIELDS = 10
_MAX_DATE_FIELDS = 3
TOP_N = 10
# The aggregation asks for more rows than TOP_N and the top is cut here with a fixed tie order (count, then value),
# so the result does not depend on the order in which the database returns equal counts
_TOP_LIMIT = 1000

CONTEXT = {
    "@vocab": "https://schema.org/",
    "schema": "https://schema.org/",
    "dcat": "http://www.w3.org/ns/dcat#",
    "dct": "http://purl.org/dc/terms/",
    "prov": "http://www.w3.org/ns/prov#",
}

_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+$")

_ACCESS = {
    "NONE": "The documents are not public: access needs permission from the administrators of the project.",
    "OBSERVER": "The documents are not public: access needs permission from the administrators of the project.",
    "METAREADER": (
        "Anyone may search the documents and see their metadata; reading the full texts needs permission from the "
        "administrators of the project."
    ),
    "READER": "Anyone with access to the server may read the documents.",
    "WRITER": "Anyone with access to the server may read the documents.",
}


def canonical_json(obj: Any) -> bytes:
    """
    The canonical JSON of obj (as RFC 8785 for the data used here: keys sorted, no whitespace, UTF-8, non-ASCII
    characters as they are). Use only with strings, integers, booleans, null, lists and objects: floats are not
    canonical across systems.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def sha256_hex(obj: Any) -> str:
    return hashlib.sha256(canonical_json(obj)).hexdigest()


def _jsonable(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else str(value)
    if isinstance(value, float):
        return int(value) if value.is_integer() else repr(value)
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


###################### PARTS ######################


async def server_versions(conn: AsyncConnection) -> dict[str, str | None]:
    """The versions of the software that made the release"""
    cur = await conn.execute(
        """SELECT current_setting('server_version') AS postgres,
                  (SELECT extversion FROM pg_extension WHERE extname = 'pg_search') AS pg_search"""
    )
    row: dict = await cur.fetchone()  # type: ignore[assignment]
    revision = None
    try:
        async with conn.transaction():
            cur = await conn.execute("SELECT version_num FROM alembic_version")
            r = await cur.fetchone()
            revision = r["version_num"] if r else None  # type: ignore[index, call-overload]
    except Exception:
        pass  # (no alembic table, e.g. a schema made without migrations)
    try:
        amcat4 = package_version("amcat4")
    except Exception:
        amcat4 = None
    return {"postgres": row["postgres"], "pg_search": row["pg_search"], "amcat4": amcat4, "migration": revision}


async def read_field_rows(conn: AsyncConnection, pk: int) -> list[dict]:
    """The rows of the fields of a project, as create_release reads them at the cut"""
    cur = await conn.execute(
        "SELECT pk, name, type, unique_field, metareader, client_settings FROM fields WHERE project_pk = %s ORDER BY pk",
        [pk],
    )
    return await cur.fetchall()  # type: ignore[return-value]


def field_definitions(rows: list[dict]) -> list[dict]:
    """The frozen definitions of the fields: no client settings (only a description, if it has one)"""
    out = []
    for row in rows:
        metareader = row.get("metareader") or {}
        access = metareader.get("access", "none")
        queryable = metareader.get("queryable")
        d: dict[str, Any] = {
            "pk": row["pk"],
            "name": row["name"],
            "type": row["type"],
            "identifier": bool(row["unique_field"]),
            "metareader": {"access": access, "queryable": (access != "none") if queryable is None else bool(queryable)},
        }
        description = (row.get("client_settings") or {}).get("description")
        if isinstance(description, str) and description:
            d["description"] = description
        out.append(d)
    return out


###################### GOLDEN QUERIES ######################


def golden_query_specs(definitions: list[dict], number: int) -> list[dict]:
    """The fixed queries of a release: {name, endpoint, query}, query being the request body of that endpoint"""
    specs: list[dict] = [
        {
            "name": "total",
            "endpoint": "query",
            "query": {"release": number, "sort": [{"_seq": {"order": "asc"}}], "per_page": TOP_N},
        }
    ]
    for f in [f for f in definitions if f["type"] in _TOP_VALUE_TYPES][:_MAX_TOP_VALUE_FIELDS]:
        specs.append(
            {
                "name": f"top_values:{f['name']}",
                "endpoint": "aggregate",
                "field_pk": f["pk"],
                "query": {"release": number, "axes": [{"field": f["name"]}], "order": "count", "limit": _TOP_LIMIT},
            }
        )
    for f in [f for f in definitions if f["type"] == "date"][:_MAX_DATE_FIELDS]:
        specs.append(
            {
                "name": f"date_range:{f['name']}",
                "endpoint": "aggregate",
                "field_pk": f["pk"],
                "query": {
                    "release": number,
                    "aggregations": [
                        {"field": f["name"], "function": "min", "name": "min"},
                        {"field": f["name"], "function": "max", "name": "max"},
                    ],
                    "filters": {f["name"]: {"exists": True}},
                },
            }
        )
    return specs


async def _doc_pks(conn: AsyncConnection, pk: int, seq: int, ids: list[str]) -> list[int]:
    """The doc_pk of the documents with these _ids in a release, in the same order"""
    if not ids:
        return []
    cur = await conn.execute(
        sql.SQL("SELECT doc_id, {} AS doc_pk FROM documents WHERE {} AND {} AND doc_id = ANY(%s)").format(
            DOC_PK, await project_filter(conn, pk), visible(seq)
        ),
        [ids],
    )
    by_id = {row["doc_id"]: row["doc_pk"] for row in await cur.fetchall()}  # type: ignore[index, call-overload]
    return [by_id.get(i) for i in ids]  # type: ignore[misc]


async def _current_query(conn: AsyncConnection, spec: dict) -> dict:
    """
    The query of a golden query with the current name of its field: a field can be renamed after the release (its
    values are stored by pk), and the query must still be the same one. (field_pk identifies the field.)
    """
    q = json.loads(json.dumps(spec["query"]))
    if "field_pk" not in spec:
        return q
    cur = await conn.execute("SELECT name FROM fields WHERE pk = %s", [spec["field_pk"]])
    row = await cur.fetchone()
    if row is None:
        raise ValueError(f"The field of golden query {spec['name']} does not exist any more")
    name = row["name"]  # type: ignore[index, call-overload]
    for a in q.get("axes") or []:
        a["field"] = name
    for a in q.get("aggregations") or []:
        a["field"] = name
    if q.get("filters"):
        q["filters"] = {name: v for v in q["filters"].values()}
    return q


async def run_golden_query(conn: AsyncConnection, index: str, pk: int, seq: int, spec: dict) -> dict:
    """Run one golden query (as golden_query_specs makes them) and return its result"""
    from amcat4.models import FilterSpec
    from amcat4.projects.aggregate import Aggregation, Axis, query_aggregate
    from amcat4.projects.query import query_documents

    q = await _current_query(conn, spec)
    if spec["endpoint"] == "query":
        result = await query_documents(index, sort=q.get("sort"), per_page=q.get("per_page", 10), release=q["release"])
        ids = [d["_id"] for d in result.data]
        return {"count": result.total_count, "first_ids": await _doc_pks(conn, pk, seq, ids)}
    filters = {k: FilterSpec(**v) for k, v in (q.get("filters") or {}).items()} or None
    result = await query_aggregate(
        index,
        [Axis(**a) for a in q.get("axes") or []],
        [Aggregation(**a) for a in q.get("aggregations") or []],
        filters=filters,
        order=q.get("order", "axes"),
        limit=q.get("limit", 1000),
        release=q["release"],
    )
    rows = list(result.as_dicts())
    if q.get("axes"):
        name = result.axes[0].name
        values = sorted(
            ({"value": _jsonable(r[name]), "n": r[result.count_column]} for r in rows),
            key=lambda v: (-v["n"], json.dumps(v["value"], sort_keys=True)),
        )
        return {"count": len(rows), "values": values[:TOP_N]}
    row = rows[0] if rows else {}
    return {
        "count": row.get(result.count_column, 0),
        "min": _jsonable(row.get("min")),
        "max": _jsonable(row.get("max")),
    }


async def run_golden_queries(conn: AsyncConnection, index: str, pk: int, seq: int, specs: list[dict]) -> list[dict]:
    return [{**spec, "result": await run_golden_query(conn, index, pk, seq, spec)} for spec in specs]


async def rerun_golden_query(conn: AsyncConnection, index: str, pk: int, seq: int, spec: dict) -> dict:
    """A golden query run again, for verify: {name, ok, stored, computed}; an error is a difference, not a failure"""
    try:
        result = await run_golden_query(conn, index, pk, seq, spec)
    except Exception as e:
        result = {"error": str(e)}
    return {"name": spec["name"], "ok": result == spec["result"], "stored": spec["result"], "computed": result}


###################### JSON-LD ######################


def release_url(index: str, number: int) -> str:
    base = (get_settings().release_id_base or get_settings().host).rstrip("/")
    return f"{base}/index/{index}/releases/{number}"


def project_url(index: str) -> str:
    base = (get_settings().release_id_base or get_settings().host).rstrip("/")
    return f"{base}/index/{index}"


def build_jsonld(
    *,
    index: str,
    number: int,
    seq: int,
    created_at: datetime,
    created_by: str | None,
    project_name: str | None,
    project_description: str | None,
    guest_role: str | None,
    manifest: dict,
    definitions: list[dict],
    versions: dict,
) -> dict:
    """
    The frozen description of a release. Only names, types, counts, dates and fingerprints: no document text and no
    field values.
    """
    url = release_url(index, number)
    counts = {f["pk"]: f["documents"] for f in manifest.get("fields", {}).values()}
    variables = []
    for f in definitions:
        if f["type"] == "vector":
            continue  # (vectors are not part of a release)
        v: dict[str, Any] = {
            "@type": "PropertyValue",
            "name": f["name"],
            "propertyID": f"field:{f['pk']}",
            "additionalType": f["type"],
            "value": counts.get(f["pk"], 0),
            "unitText": "documents with a value",
        }
        if f.get("description"):
            v["description"] = f["description"]
        variables.append(v)
    name = project_name or index
    doc: dict[str, Any] = {
        "@context": CONTEXT,
        "@type": ["Dataset", "dcat:Dataset"],
        "@id": url,
        "url": url,
        "name": f"{name}, release {number}",
        "alternateName": name,
        "version": str(number),
        "dateCreated": created_at.isoformat(),
        "datePublished": created_at.isoformat(),
        "isPartOf": {"@id": project_url(index), "@type": "Dataset", "identifier": index, "name": name},
        "identifier": [
            {"@type": "PropertyValue", "propertyID": "amcat4:release", "value": f"{index}/releases/{number}"},
            {"@type": "PropertyValue", "propertyID": "amcat4:seq", "value": str(seq)},
            {"@type": "PropertyValue", "propertyID": "amcat4:fingerprint", "value": manifest.get("fingerprint")},
        ],
        "size": {"@type": "QuantitativeValue", "value": manifest.get("documents", 0), "unitText": "documents"},
        "variableMeasured": variables,
        "conditionsOfAccess": _ACCESS.get(guest_role or "NONE", _ACCESS["NONE"]),
        "prov:wasGeneratedBy": {
            "@type": "prov:Activity",
            "name": "AmCAT release",
            "prov:endedAtTime": created_at.isoformat(),
            "instrument": {
                "@type": "SoftwareApplication",
                "name": "AmCAT",
                "softwareVersion": versions.get("amcat4"),
                "additionalProperty": [
                    {"@type": "PropertyValue", "name": k, "value": v} for k, v in sorted(versions.items()) if v is not None
                ],
            },
        },
    }
    if project_description:
        doc["description"] = project_description
    if created_by and not _EMAIL.match(created_by.strip()):
        doc["creator"] = {"name": created_by}
    return doc


async def make_release_metadata(
    conn: AsyncConnection,
    *,
    index: str,
    pk: int,
    number: int,
    seq: int,
    created_at: datetime,
    created_by: str | None,
    field_rows: list[dict],
    manifest: dict,
) -> dict[str, Any]:
    """Everything that is frozen at creation: {metadata_jsonld, metadata_sha256, versions, field_definitions,
    golden_queries}"""
    from amcat4.systemdata.roles import get_project_guest_role
    from amcat4.systemdata.settings import get_project_settings

    settings = await get_project_settings(index)
    guest_role = await get_project_guest_role(index)
    versions = await server_versions(conn)
    definitions = field_definitions(field_rows)
    golden = await run_golden_queries(conn, index, pk, seq, golden_query_specs(definitions, number))
    jsonld = build_jsonld(
        index=index,
        number=number,
        seq=seq,
        created_at=created_at,
        created_by=created_by,
        project_name=settings.name,
        project_description=settings.description,
        guest_role=guest_role,
        manifest=manifest,
        definitions=definitions,
        versions=versions,
    )
    jsonld = json.loads(canonical_json(jsonld))  # (exactly what will be stored and hashed)
    return {
        "metadata_jsonld": jsonld,
        "metadata_sha256": sha256_hex(jsonld),
        "versions": versions,
        "field_definitions": definitions,
        "golden_queries": golden,
    }


###################### LIVE ######################


def live_metadata(release: dict) -> dict:
    """The frozen JSON-LD of a release with what can change added: the DOI (identifier, sameAs), its status, withdrawn"""
    doc = json.loads(json.dumps(release["metadata_jsonld"]))  # (a copy)
    doi = release.get("doi")
    if doi:
        doi_url = doi if doi.startswith("http") else f"https://doi.org/{doi}"
        doc["identifier"] = [
            {"@type": "PropertyValue", "propertyID": "doi", "value": doi, "url": doi_url},
            *doc.get("identifier", []),
        ]
        doc["sameAs"] = doi_url
    status: list[dict[str, Any]] = [{"@type": "PropertyValue", "name": "doiStatus", "value": release.get("doi_status")}]
    withdrawn = release.get("withdrawn_at")
    if withdrawn:
        doc["creativeWorkStatus"] = "Withdrawn"
        status.append(
            {
                "@type": "PropertyValue",
                "name": "withdrawnAt",
                "value": withdrawn.isoformat() if isinstance(withdrawn, datetime) else withdrawn,
            }
        )
        if release.get("withdrawn_reason"):
            status.append({"@type": "PropertyValue", "name": "withdrawnReason", "value": release["withdrawn_reason"]})
    else:
        doc["creativeWorkStatus"] = "Published"
    doc["additionalProperty"] = status
    return doc
