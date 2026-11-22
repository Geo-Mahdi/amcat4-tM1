from amcat4.errors import NotFoundError
from amcat4.postgres.connection import fetch_all, fetch_one


async def project_pk(project_id: str) -> int:
    """Get the internal primary key of a project"""
    row = await fetch_one("SELECT pk FROM projects WHERE id = %s", [project_id])
    if row is None:
        raise NotFoundError(f"Project {project_id} does not exist")
    return row["pk"]


async def project_pks(project_ids: list[str]) -> dict[str, int]:
    rows = await fetch_all("SELECT id, pk FROM projects WHERE id = ANY(%s)", [project_ids])
    pks = {row["id"]: row["pk"] for row in rows}
    for project_id in project_ids:
        if project_id not in pks:
            raise NotFoundError(f"Project {project_id} does not exist")
    return pks


async def project_partitions(pks: list[int]) -> dict[int, int]:
    """The documents partition of each project (by internal primary key)"""
    rows = await fetch_all("SELECT pk, partition_id FROM projects WHERE pk = ANY(%s)", [pks])
    return {row["pk"]: row["partition_id"] for row in rows}


async def project_moves(pks: list[int]) -> dict[int, int]:
    """The partition each of these projects is being moved to (only projects that are being moved)"""
    rows = await fetch_all("SELECT pk, moving_to FROM projects WHERE pk = ANY(%s) AND moving_to IS NOT NULL", [pks])
    return {row["pk"]: row["moving_to"] for row in rows}
