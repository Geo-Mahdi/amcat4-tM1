"""API endpoints for the frozen releases of a project (see amcat4.projects.releases)."""

import json
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, Response, Security, status
from pydantic import BaseModel, Field

from amcat4.api.auth_helpers import api_key_scheme, authenticated_user, session_cookie_scheme
from amcat4.errors import NotFoundError
from amcat4.models import IndexId, Roles, User
from amcat4.projects.release_metadata import live_metadata
from amcat4.projects.releases import (
    create_release,
    delete_release,
    get_release,
    get_release_frozen,
    list_releases,
    update_release,
    verify_release,
)
from amcat4.systemdata.roles import HTTPException_if_not_project_index_role

app_index_releases = APIRouter(prefix="", tags=["releases"])


class CreateReleaseBody(BaseModel):
    """Form to create a release."""

    doi: str | None = Field(None, description="The DOI of the release, if it already has one (it can be set later)")


class UpdateReleaseBody(BaseModel):
    """
    Form to change the live properties of a release (its documents and its frozen metadata never change). Only the
    properties that are given are changed.
    """

    doi: str | None = Field(None, description="The DOI of the release (null to remove it)")
    doi_status: Literal["none", "draft", "registered", "findable"] | None = Field(
        None, description="The status of the DOI at the registration agency"
    )
    metadata_public: bool | None = Field(
        None, description="Whether anyone (also without login) may see the metadata of the release (never the texts)"
    )
    withdrawn: bool | None = Field(
        None,
        description="Mark the release as withdrawn (true) or not (false). Its documents stay and can still be queried",
    )
    withdrawn_reason: str | None = Field(None, description="Why the release was withdrawn")


class Release(BaseModel):
    """A frozen release of a project."""

    number: int = Field(description="The number of the release within the project (1, 2, ...): use it as 'release' in queries")
    seq: int = Field(description="The sequence number that defines the release")
    created_at: datetime
    created_by: str | None
    doi: str | None
    manifest: dict[str, Any] = Field(description="What the release contains: document count, counts per field, fingerprints")
    metadata_sha256: str | None = Field(
        None, description="SHA-256 of the canonical JSON of the frozen metadata (null for releases made before it)"
    )
    metadata_public: bool = Field(False, description="Whether the metadata of the release is public")
    withdrawn_at: datetime | None = None
    withdrawn_reason: str | None = None
    doi_status: str = Field("none", description="none, draft, registered or findable")


class ReleaseCheck(BaseModel):
    """The result of verifying a release."""

    ok: bool = Field(description="True if the release still contains exactly what its manifest says")
    differences: list[str] = Field(description="The parts of the manifest that differ")
    stored: dict[str, Any]
    computed: dict[str, Any]
    metadata: dict[str, Any] | None = Field(
        None, description="The check of the frozen metadata: stored and computed SHA-256 (null: no frozen metadata)"
    )
    golden_queries: list[dict[str, Any]] = Field(
        [], description="The golden queries run again: name, ok, stored and computed result"
    )


class ReleaseField(BaseModel):
    """A field as it was when the release was made, with the number of documents of the release that have a value."""

    pk: int
    name: str
    type: str
    identifier: bool
    metareader: dict[str, Any]
    description: str | None = None
    documents: int | None = Field(None, description="The number of documents in the release with a value")


@app_index_releases.post("/index/{ix}/releases", status_code=status.HTTP_201_CREATED)
async def create_release_post(
    ix: Annotated[IndexId, Path(description="The index id")],
    body: Annotated[CreateReleaseBody | None, Body()] = None,
    user: User = Depends(authenticated_user),
) -> Release:
    """
    Freeze the current state of the project as a new release. Requires ADMIN role on the project.
    Later changes to the project do not change the release.
    """
    await HTTPException_if_not_project_index_role(user, ix, Roles.ADMIN)
    return Release(**await create_release(ix, created_by=user.email, doi=body.doi if body else None))


@app_index_releases.get("/index/{ix}/releases")
async def list_releases_get(
    ix: Annotated[IndexId, Path(description="The index id")], user: User = Depends(authenticated_user)
) -> list[Release]:
    """List the releases of a project. Requires METAREADER role on the project."""
    await HTTPException_if_not_project_index_role(user, ix, Roles.METAREADER)
    return [Release(**r) for r in await list_releases(ix)]


@app_index_releases.get("/index/{ix}/releases/{number}")
async def get_release_get(
    ix: Annotated[IndexId, Path(description="The index id")],
    number: Annotated[int, Path(description="The number of the release")],
    user: User = Depends(authenticated_user),
) -> Release:
    """Get a release of a project. Requires METAREADER role on the project."""
    await HTTPException_if_not_project_index_role(user, ix, Roles.METAREADER)
    return Release(**await get_release(ix, number))


@app_index_releases.put("/index/{ix}/releases/{number}")
async def update_release_put(
    ix: Annotated[IndexId, Path(description="The index id")],
    number: Annotated[int, Path(description="The number of the release")],
    body: Annotated[UpdateReleaseBody, Body(...)],
    user: User = Depends(authenticated_user),
) -> Release:
    """
    Change the live properties of a release: the DOI (and its status), whether its metadata is public, and whether it
    is withdrawn. Only the properties in the body are changed. Requires ADMIN role on the project.
    """
    await HTTPException_if_not_project_index_role(user, ix, Roles.ADMIN)
    given = body.model_fields_set
    changes: dict[str, Any] = {k: getattr(body, k) for k in given if k != "doi"}
    if "doi" in given:
        changes["doi"] = body.doi
    return Release(**await update_release(ix, number, **changes))


async def optional_user(
    api_key: str | None = Security(api_key_scheme),
    access_token: str | None = Security(session_cookie_scheme),
) -> User | None:
    """The user, or None for a request without login on a server that requires one (for public metadata)"""
    try:
        return await authenticated_user(api_key, access_token)
    except HTTPException:
        if api_key is None and access_token is None:
            return None
        raise


async def _release_for_metadata(ix: str, number: int, user: User | None) -> dict:
    """
    The release with its frozen metadata, if the user may see it: anyone if its metadata is public, otherwise
    METAREADER role on the project. (A release that does not exist, or is not public, looks the same to a user
    without access.)
    """
    try:
        release: dict | None = await get_release_frozen(ix, number)
    except NotFoundError:
        release = None
    if release is None or not release["metadata_public"]:
        if user is None:
            raise HTTPException(401, "This release is not public: log in to see its metadata")
        await HTTPException_if_not_project_index_role(user, ix, Roles.METAREADER)
    if release is None:
        release = await get_release_frozen(ix, number)  # (raises not found)
    return release


@app_index_releases.get(
    "/index/{ix}/releases/{number}/metadata",
    responses={200: {"content": {"application/ld+json": {}, "text/turtle": {}}}},
)
async def release_metadata_get(
    ix: Annotated[IndexId, Path(description="The index id")],
    number: Annotated[int, Path(description="The number of the release")],
    format: Annotated[Literal["jsonld", "turtle"], Query(description="jsonld (default) or turtle")] = "jsonld",
    user: User | None = Depends(optional_user),
) -> Response:
    """
    The metadata of a release as JSON-LD (schema.org / DCAT): the description frozen when the release was made, with
    the live properties added (DOI, DOI status, withdrawn). Never contains document text.
    Anyone may see it if the metadata of the release is public; otherwise it requires METAREADER role on the project.
    """
    release = await _release_for_metadata(ix, number, user)
    if release["metadata_jsonld"] is None:
        raise HTTPException(404, f"Release {number} of project {ix} was made before release metadata existed")
    doc = live_metadata(release)
    if format == "turtle":
        from rdflib import Graph

        graph = Graph().parse(data=json.dumps(doc), format="json-ld")
        return Response(graph.serialize(format="turtle"), media_type="text/turtle")
    return Response(json.dumps(doc, ensure_ascii=False), media_type="application/ld+json")


@app_index_releases.get("/index/{ix}/releases/{number}/fields")
async def release_fields_get(
    ix: Annotated[IndexId, Path(description="The index id")],
    number: Annotated[int, Path(description="The number of the release")],
    user: User | None = Depends(optional_user),
) -> list[ReleaseField]:
    """
    The fields of a release as they were when it was made (the codebook), with the number of documents that have a
    value. The same access as the metadata.
    """
    release = await _release_for_metadata(ix, number, user)
    if release["field_definitions"] is None:
        raise HTTPException(404, f"Release {number} of project {ix} was made before release metadata existed")
    counts = {f["pk"]: f["documents"] for f in release["manifest"].get("fields", {}).values()}
    return [ReleaseField(**f, documents=counts.get(f["pk"])) for f in release["field_definitions"]]


@app_index_releases.get("/index/{ix}/releases/{number}/verify")
async def verify_release_get(
    ix: Annotated[IndexId, Path(description="The index id")],
    number: Annotated[int, Path(description="The number of the release")],
    user: User = Depends(authenticated_user),
) -> ReleaseCheck:
    """
    Compute the manifest of a release again from its documents and compare it with the stored manifest.
    Reads every document of the release. Requires WRITER role on the project.
    """
    await HTTPException_if_not_project_index_role(user, ix, Roles.WRITER)
    return ReleaseCheck(**await verify_release(ix, number))


@app_index_releases.delete("/index/{ix}/releases/{number}")
async def delete_release_delete(
    ix: Annotated[IndexId, Path(description="The index id")],
    number: Annotated[int, Path(description="The number of the release")],
    user: User = Depends(authenticated_user),
) -> dict[str, str | None]:
    """
    Delete a release (not possible once it has a DOI). Requires ADMIN role on the project. The old versions of
    documents that only this release needed are removed by a background job, whose id is returned.
    """
    await HTTPException_if_not_project_index_role(user, ix, Roles.ADMIN)
    return {"job": await delete_release(ix, number)}
