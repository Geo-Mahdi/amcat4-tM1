"""API endpoints for the frozen releases of a project (see amcat4.projects.releases)."""

from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, Path, status
from pydantic import BaseModel, Field

from amcat4.api.auth_helpers import authenticated_user
from amcat4.models import IndexId, Roles, User
from amcat4.projects.releases import (
    create_release,
    delete_release,
    get_release,
    list_releases,
    set_release_doi,
    verify_release,
)
from amcat4.systemdata.roles import HTTPException_if_not_project_index_role

app_index_releases = APIRouter(prefix="", tags=["releases"])


class CreateReleaseBody(BaseModel):
    """Form to create a release."""

    doi: str | None = Field(None, description="The DOI of the release, if it already has one (it can be set later)")


class UpdateReleaseBody(BaseModel):
    """Form to change a release: only its DOI can change."""

    doi: str | None = Field(description="The DOI of the release (null to remove it)")


class Release(BaseModel):
    """A frozen release of a project."""

    number: int = Field(description="The number of the release within the project (1, 2, ...): use it as 'release' in queries")
    seq: int = Field(description="The sequence number that defines the release")
    created_at: datetime
    created_by: str | None
    doi: str | None
    manifest: dict[str, Any] = Field(description="What the release contains: document count, counts per field, fingerprints")


class ReleaseCheck(BaseModel):
    """The result of verifying a release."""

    ok: bool = Field(description="True if the release still contains exactly what its manifest says")
    differences: list[str] = Field(description="The parts of the manifest that differ")
    stored: dict[str, Any]
    computed: dict[str, Any]


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
    """Set (or remove) the DOI of a release. Requires ADMIN role on the project."""
    await HTTPException_if_not_project_index_role(user, ix, Roles.ADMIN)
    return Release(**await set_release_doi(ix, number, body.doi))


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
