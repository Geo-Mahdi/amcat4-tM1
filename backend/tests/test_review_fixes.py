"""
Tests for the fixes after the code review of the postgres-fast-v2 branch (one section per finding).
"""

import pytest

from amcat4.config import get_settings
from amcat4.models import FieldSpec, ProjectSettings, SortSpec, UpdateDocumentField
from amcat4.postgres.search import slot_sort_column
from amcat4.projects.index import create_project_index, delete_project_index
from amcat4.projects.query import query_documents
from amcat4.systemdata.fields import get_fieldset, list_fields, update_fields
from tests.conftest import upload


@pytest.fixture()
async def two():
    names = ["amcat4_unittest_review_a", "amcat4_unittest_review_b"]
    for name in names:
        await delete_project_index(name, ignore_missing=True)
        await create_project_index(ProjectSettings(id=name))
    yield names
    for name in names:
        await delete_project_index(name, ignore_missing=True)


async def _ids(index, **kwargs) -> list[str]:
    r = await query_documents(index, per_page=100, **kwargs)
    return [d["_id"] for d in r.data]


###################### 1. Sort slots in a multi-project query ######################


@pytest.mark.anyio
@pytest.mark.parametrize("fast_path", [True, False])
async def test_slot_sort_other_project_same_slot(two, monkeypatch, fast_path):
    """
    A slot column holds a different field in another project: a sort on one project's field must not order the other
    project's documents by that (maybe hidden) field. They have no value, so they come last (NULLS LAST), by id.
    """
    monkeypatch.setattr(get_settings(), "fast_path", fast_path)
    a, b = two
    await upload(a, [{"_id": f"a{i}", "n": i * 10} for i in range(3)], fields={"n": "integer"})
    await upload(b, [{"_id": f"b{i}", "secret": 100 - i * 10} for i in range(3)], fields={"secret": "integer"})
    await update_fields(a, {"n": UpdateDocumentField(fast_sort=True)})
    await update_fields(b, {"secret": UpdateDocumentField(fast_sort=True)})
    assert (await list_fields(a))["n"].sort_slot == (await list_fields(b))["secret"].sort_slot == "sort_num1"
    fieldset = await get_fieldset([a, b])
    assert slot_sort_column(fieldset, "n") is None
    assert slot_sort_column(await get_fieldset([a]), "n") == "sort_num1"
    asc = await _ids([a, b], sort=[{"n": SortSpec(order="asc")}], fields=[FieldSpec(name="n")])
    assert asc == ["a0", "a1", "a2", "b0", "b1", "b2"]
    desc = await _ids([a, b], sort=[{"n": SortSpec(order="desc")}], fields=[FieldSpec(name="n")])
    assert desc == ["a2", "a1", "a0", "b0", "b1", "b2"]


@pytest.mark.anyio
async def test_slot_sort_auto_mapped_date(two):
    """The automatic date slot: another project's first date field (with another name) is in the same column"""
    a, b = two
    await upload(a, [{"_id": f"a{i}", "date": f"2020-01-0{i + 1}"} for i in range(3)], fields={"date": "date"})
    await upload(b, [{"_id": f"b{i}", "published": f"2019-01-0{3 - i}"} for i in range(3)], fields={"published": "date"})
    assert (await list_fields(a))["date"].sort_slot == (await list_fields(b))["published"].sort_slot == "date"
    assert slot_sort_column(await get_fieldset([a, b]), "date") is None
    assert await _ids([a, b], sort=[{"date": SortSpec(order="asc")}]) == ["a0", "a1", "a2", "b0", "b1", "b2"]
    assert await _ids([a, b], sort=[{"published": SortSpec(order="desc")}]) == ["b0", "b1", "b2", "a0", "a1", "a2"]


@pytest.mark.anyio
async def test_slot_sort_used_when_all_projects_have_it(two):
    """If every project of the query has the field in the same filled slot, the slot column is used"""
    a, b = two
    await upload(a, [{"_id": f"a{i}", "date": f"2020-01-0{i + 1}"} for i in range(3)], fields={"date": "date"})
    await upload(b, [{"_id": f"b{i}", "date": f"2020-01-0{i + 4}"} for i in range(3)], fields={"date": "date"})
    assert slot_sort_column(await get_fieldset([a, b]), "date") == "date"
    assert await _ids([a, b], sort=[{"date": SortSpec(order="desc")}]) == ["b2", "b1", "b0", "a2", "a1", "a0"]
