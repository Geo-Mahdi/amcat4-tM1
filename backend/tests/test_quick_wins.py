"""
Tests for version 3, phase 3: quick speed wins without schema change (docs/postgres-fast-v3/QUICK_WINS.md).
Every test compares the new path with the old path on the same data.
"""

import pytest

from amcat4.config import get_settings
from amcat4.models import FilterSpec, ProjectSettings
from amcat4.projects import aggregate as aggregate_module
from amcat4.projects.aggregate import Aggregation, Axis, query_aggregate
from amcat4.projects.index import create_project_index, delete_project_index
from tests.conftest import upload
from tests.test_fast_path import index_paths  # noqa: F401

PROJECT = "amcat4_unittest_quick"
FIELDS = {
    "text": "text",
    "title": "text",
    "pub": "keyword",
    "date": "date",
    "tags": "tag",
    "n": "integer",
    "x": "number",
    "flag": "boolean",
    "url": "url",
}
# whole and decimal values in one number field, very large and very small ones, and both zeros
NUMBERS = [1.0, 2.5, 3.0, -0.5, 1e10, 0.1, 17, -3.25, 1e22, 1e-7, 0.0, -0.0, 2.0**53 + 2]


def _docs(prefix: str, n: int, numbers: list) -> list[dict]:
    docs = []
    for i in range(n):
        doc: dict = {"_id": f"{prefix}{i}", "text": f"common word{i % 7}", "pub": "ABC"[i % 3], "url": f"http://x/{i % 4}"}
        if i % 4:
            doc["title"] = f"title {i}" if i % 8 else ""
        if i % 5:
            doc["date"] = f"20{10 + i % 5}-0{1 + i % 9}-1{i % 9}T0{i % 7}:1{i % 5}:00.{i:06d}"
        if i % 6:
            doc["x"] = numbers[i % len(numbers)]
        if i % 7:
            doc["n"] = i % 11 - 4
        if i % 3:
            doc["flag"] = bool(i % 2)
        if i % 5 > 1:
            doc["tags"] = ["x", "y", "z"][: i % 3 + 1]
        docs.append(doc)
    return docs


@pytest.fixture()
async def project():
    """
    One project, uploaded in four parts (four segments of the search index): only whole numbers, only decimals, both,
    and documents with almost no fields
    """
    await delete_project_index(PROJECT, ignore_missing=True)
    await create_project_index(ProjectSettings(id=PROJECT))
    await upload(PROJECT, _docs("a", 40, [1.0, 2.0, 17.0]), fields=FIELDS)  # type: ignore[arg-type]
    await upload(PROJECT, _docs("b", 40, [2.5, 0.1, -3.25]))
    await upload(PROJECT, _docs("c", 80, NUMBERS))
    await upload(PROJECT, [{"_id": f"d{i}", "pub": "D"} for i in range(5)])
    yield PROJECT
    await delete_project_index(PROJECT, ignore_missing=True)


@pytest.fixture()
def index_aggregations(monkeypatch):
    """Records what amcat4.postgres.index_aggregate returned: rows (in the index) or None (GROUP BY)"""
    calls: list = []
    original = aggregate_module.index_aggregate

    async def spy(*args, **kwargs):
        rows = await original(*args, **kwargs)
        calls.append(rows)
        return rows

    monkeypatch.setattr(aggregate_module, "index_aggregate", spy)
    return calls


async def _aggregate(index, axes, aggregations=None, **kwargs):
    r = await query_aggregate(index, axes, aggregations, limit=10000, **kwargs)
    return r.data


async def _old_and_new(monkeypatch, switch: str, run):
    """The result of run() with the original SQL (no fast path), with the switch off, and with the switch on"""
    settings = get_settings()
    monkeypatch.setattr(settings, "fast_path", False)
    original = await run()
    monkeypatch.setattr(settings, "fast_path", True)
    monkeypatch.setattr(settings, switch, False)
    off = await run()
    monkeypatch.setattr(settings, switch, True)
    return original, off, await run()


############ 1. Aggregations inside the index ############

TYPED_AGGREGATIONS = [
    ([Axis("n")], [], {}),
    ([Axis("x")], [], {}),
    ([Axis("flag")], [], {}),
    ([Axis("flag"), Axis("x")], [], {}),
    ([Axis("pub"), Axis("n")], [], {}),
    ([Axis("x"), Axis("date", "year")], [], {}),
    ([Axis("tags"), Axis("flag")], [], {}),
    ([Axis("n")], [], {"queries": {"q": "word1 OR word2"}}),
    ([Axis("x")], [], {"filters": {"flag": FilterSpec(value=True)}}),
    ([Axis("x")], [Aggregation("n", "sum"), Aggregation("n", "min")], {}),
    ([Axis("flag")], [Aggregation("x", "max"), Aggregation("date", "min")], {}),
    ([Axis("pub")], [Aggregation("date", "min"), Aggregation("date", "max")], {}),
    ([Axis("n")], [Aggregation("date", "max")], {"queries": {"q": "common"}}),
    ([Axis("date", "quarter")], [Aggregation("date", "min")], {}),
    ([], [Aggregation("n", "min"), Aggregation("n", "max"), Aggregation("x", "min")], {}),
    ([], [Aggregation("date", "min"), Aggregation("date", "max"), Aggregation("n", "max")], {}),
    ([], [Aggregation("date", "max")], {"filters": {"pub": FilterSpec(value="D")}}),  # no document has a date
    ([], [Aggregation("x", "min")], {"queries": {"q": "word3"}}),
]


def _same_rows(result: list[tuple], expected: list[tuple], n_exact: int):
    """The same rows; the metrics (after the axes and the count) with a float tolerance (sums and averages)"""
    assert [row[:n_exact] for row in result] == [row[:n_exact] for row in expected]
    for row, exp in zip(result, expected):
        for value, e in zip(row[n_exact:], exp[n_exact:]):
            assert value == (pytest.approx(e) if isinstance(e, float) else e)


@pytest.mark.anyio
@pytest.mark.parametrize("axes,aggregations,kwargs", TYPED_AGGREGATIONS)
async def test_typed_aggregation_same_rows(project, index_paths, index_aggregations, monkeypatch, axes, aggregations, kwargs):  # noqa: F811
    """Group by number, integer and boolean, min/max of dates and metrics without axes: the same rows as GROUP BY"""

    async def run():
        return await _aggregate(project, axes, aggregations, **kwargs)

    original, off, new = await _old_and_new(monkeypatch, "index_agg_types", run)
    assert len(original) > 0
    # min, max (of numbers and dates) are exact; no sum or avg of decimals here, so everything is exact
    assert new == original
    assert off == original
    # with the switch on the rows came from the index; with it off these shapes used GROUP BY
    assert [rows is not None for rows in index_aggregations] == [False, True]


@pytest.mark.anyio
@pytest.mark.parametrize("function", ["avg", "sum", "min", "max"])
async def test_typed_metric_same_values(project, index_paths, index_aggregations, monkeypatch, function):  # noqa: F811
    """avg and sum of a number field with whole and decimal values per boolean, and without axes (float tolerance)"""
    for axes in ([Axis("flag")], [Axis("n")], []):

        async def run():
            return await _aggregate(project, axes, [Aggregation("x", function), Aggregation("n", function)])

        original, off, new = await _old_and_new(monkeypatch, "index_agg_types", run)
        _same_rows(new, original, len(axes) + 1)
        _same_rows(off, original, len(axes) + 1)
    assert [rows is not None for rows in index_aggregations] == [False, True] * 3


@pytest.mark.anyio
@pytest.mark.parametrize(
    "axes,aggregations",
    [
        ([Axis("n", "5")], []),  # number interval
        ([Axis("date", "hour")], []),
        ([Axis("url")], []),
        ([Axis("pub")], [Aggregation("date", "avg")]),
        ([], [Aggregation("date", "avg")]),
        ([], []),
    ],
)
async def test_shapes_left_on_group_by(project, index_paths, index_aggregations, monkeypatch, axes, aggregations):  # noqa: F811
    """These stay on GROUP BY (see QUICK_WINS.md), with the same rows as the original SQL"""
    new = await _aggregate(project, axes, aggregations)
    monkeypatch.setattr(get_settings(), "fast_path", False)
    assert new == await _aggregate(project, axes, aggregations)
    assert index_aggregations == [None]


@pytest.mark.anyio
async def test_values_a_float_cannot_hold(project, index_paths, index_aggregations, monkeypatch):  # noqa: F811
    """
    The index computes with 64-bit floats. Integers from 2^53 and dates more than 285 years from 2000 could be
    rounded there, so GROUP BY is used for them, and the rows stay the same.
    """
    await upload(
        project,
        [
            {"_id": "big1", "pub": "E", "n": 2**53 + 1, "date": "1500-03-04T05:06:07.123457"},
            {"_id": "big2", "pub": "E", "n": 2**53 + 2, "date": "1500-03-04T05:06:07.123456"},
            {"_id": "big3", "pub": "E", "n": -(2**53) - 1},
        ],
    )
    for axes, aggregations in [
        ([Axis("n")], []),
        ([Axis("pub")], [Aggregation("date", "min")]),
        ([], [Aggregation("date", "min")]),
    ]:

        async def run():
            return await _aggregate(project, axes, aggregations)

        original, off, new = await _old_and_new(monkeypatch, "index_agg_types", run)
        assert new == original == off
    assert index_aggregations == [None] * 6
