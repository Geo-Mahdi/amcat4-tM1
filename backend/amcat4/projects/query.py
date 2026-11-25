"""
All things query
"""

import base64
import json
from math import ceil
from typing import Any, Literal, Union

from amcat4.config import get_settings
from amcat4.connections import with_server_defaults
from amcat4.models import FieldSpec, FilterSpec, SnippetParams, SortSpec
from amcat4.postgres import documents as storage
from amcat4.postgres.connection import connection
from amcat4.postgres.search import SEQ, SearchQuery, search
from amcat4.projects.maintenance import vacuum_soon
from amcat4.systemdata.fields import create_or_verify_tag_field, get_fieldset


class QueryResult:
    def __init__(
        self,
        data: list[dict],
        n: int | None = None,
        per_page: int | None = None,
        page: int | None = None,
        next: str | None = None,
    ):
        self.data = data
        self.total_count = n
        self.page = page
        self.page_count = ceil(n / per_page) if n is not None and per_page else None
        self.per_page = per_page
        self.next = next

    def as_dict(self) -> dict:
        meta = {
            "total_count": self.total_count,
            "per_page": self.per_page,
            "page_count": self.page_count,
            "page": self.page,
            "next": self.next,
        }
        return dict(meta=meta, results=self.data)


async def release_seq_for_query(index: str | list[str], release: int | None, similar: bool = False) -> int | None:
    """The sequence number of release `release` of the project, for a query on it (None: the current documents)"""
    if release is None:
        return None
    from amcat4.projects.releases import release_seq

    indices = [index] if isinstance(index, str) else index
    if len(indices) != 1:
        raise ValueError("A release belongs to one project: query it without other projects")
    if similar:
        raise ValueError("Vectors are not part of a release: a similarity search on a release is not possible")
    return await release_seq(indices[0], release)


def _search_query(queries, filters, ids=None) -> SearchQuery:
    return SearchQuery(queries=queries or None, filters=filters or None, ids=list(ids) if ids else None)


def _sort_spec(sort: list[dict[str, SortSpec]] | None) -> list[tuple[str, Literal["asc", "desc"]]] | None:
    if not sort:
        return None
    out: list[tuple[str, Literal["asc", "desc"]]] = []
    for s in sort:
        for k, v in s.items():
            order = v.order if isinstance(v, SortSpec) else SortSpec.model_validate(v).order
            out.append((k, order))
    return out


def _seq_order(sort: list[tuple[str, Literal["asc", "desc"]]] | None) -> Literal["asc", "desc"] | None:
    """The direction if the sort is only on _seq (the internal id), which is paged with a keyset cursor, else None"""
    if sort and len(sort) == 1 and sort[0][0] == SEQ:
        return sort[0][1]
    return None


def _encode_cursor(d: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(d).encode()).decode()


# The keys a cursor can have: "id" (keyset: after this internal id) or "offset", plus "total" and "desc" (keyset only).
# Cursors are plain base64 json, not signed: a client can make one by hand. So every value is checked (types, ranges,
# known keys) before it reaches SQL, and a hand-made cursor gives HTTP 400, not a database error. Not signed on
# purpose: the only thing a forged cursor can do is page deeper than max_offset (an offset cursor is not limited, see
# query_documents), and that limit is a performance guard, not a security boundary: what the user may see is
# decided by the fields and roles, as for any other page. (A forged total only changes the reported total.)
_MAX_BIGINT = 2**63 - 1


def _decode_cursor(cursor: str) -> dict:
    try:
        d = json.loads(base64.urlsafe_b64decode(cursor.encode()))
    except Exception:
        raise ValueError(f"Invalid cursor: {cursor}")
    valid = isinstance(d, dict) and set(d) <= {"id", "offset", "total", "desc"} and ("id" in d) != ("offset" in d)
    for key, value in d.items() if valid else []:
        if key == "desc":
            valid = valid and isinstance(value, bool) and "id" in d
        else:  # (bool is an int in python)
            valid = valid and isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= _MAX_BIGINT
    if not valid:
        raise ValueError(f"Invalid cursor: {cursor}")
    return d


async def query_documents(
    index: Union[str, list[str]],
    fields: list[FieldSpec] | None = None,
    queries: dict[str, str] | None = None,
    filters: dict[str, FilterSpec] | None = None,
    sort: list[dict[str, SortSpec]] | None = None,
    *,
    page: int = 0,
    per_page: int = 10,
    after: str | None = None,
    highlight: bool = False,
    similar: tuple[str, list[float]] | None = None,
    queryable: set[str] | None = None,
    release: int | None = None,
) -> QueryResult:
    """
    Conduct a query, returning the found documents.

    :param index: The id of the project, or a list of project ids
    :param fields: List of fields using the FieldSpec syntax. If not specified, only return _id.
                   !We require the fields to be specified for security reasons.
                   !Any logic for determining whether a user can see the field should be done in the API layer.
    :param queries: if not None, a dict with labels and queries {label1: query1, ...}
    :param filters: if not None, a dict where the key is the field and the value is a FilterSpec
    :param sort: Sort order of results, a list of {field: SortSpec} dicts. Use "?" as field for random order.
                 "_id" sorts on the document id. Use "_seq" (alone) for the order in which the documents were stored:
                 the cursor is then "after this document", which stays fast at any depth (use it to download all
                 results of a query). With page > 0, _seq pages by offset like other orders.
    :param page: The number of the page to request (starting from zero)
    :param per_page: The number of results per page
    :param after: A cursor (the 'next' value of a previous result) to get the next batch of results. This is the
                  efficient way to retrieve large result sets: without sort and without query, or with sort on _seq,
                  results are ordered by internal id and the cursor is "after this id", which makes retrieving the
                  next batch fast. In other orders the cursor is an offset. The cursor also carries the total.
    :param highlight: if True, add <em> tags to query matches in fields
    :param similar: (vector field, vector): order results by (cosine) similarity to this vector
    :param queryable: the fields that may be used in queries and filters (None = all fields)
    :param release: the number of a release of the (one) project: query the documents of that release instead of the
                    current ones (see amcat4.projects.releases). The relevance order of a release is not frozen.
    """
    if fields is not None and not isinstance(fields, list):
        raise ValueError("fields should be a list")
    release_seq = await release_seq_for_query(index, release, similar=similar is not None)
    fieldset = await get_fieldset([index] if isinstance(index, str) else index, queryable=queryable, release=release_seq)
    fields = fields or []
    snippets: dict[str, SnippetParams] = {f.name: f.snippet for f in fields if f.snippet is not None}
    names = [f.name for f in fields if f.snippet is None]
    sort_spec = _sort_spec(sort)

    cursor = _decode_cursor(after) if after else {}
    # Without explicit sort (and without query/similarity scores), or with sort on _seq only, cursors use keyset
    # pagination on the internal id, otherwise the cursor is an offset. A _seq request for a page number (page > 0)
    # is an offset request in the same order; its next cursor is a keyset cursor again
    seq_order = None if similar else _seq_order(sort_spec)
    keyset = "id" in cursor or (seq_order is not None and "offset" not in cursor and page == 0)
    descending = cursor.get("desc", seq_order == "desc")
    offset = cursor.get("offset", page * per_page)
    max_offset = get_settings().max_offset
    if after is None and not keyset and max_offset and offset + per_page > max_offset:
        # A page deep in score (or field) order means sorting all hits up to that page and throwing most away: a page
        # at offset 100,000 costs 20x a page by id (83 ms against 3.9 ms, 22 Nov 2026). Elasticsearch has the same
        # limit (max_result_window). The limit is for page numbers only: a cursor (after) is assumed to be issued by
        # this server, and a download that follows the 'next' cursors of a query without sort (an offset cursor) must
        # be able to finish. A hand-made offset cursor therefore passes too: the limit guards against slow requests by
        # mistake, it is not a security boundary (see _decode_cursor). New in this version; AMCAT4_MAX_OFFSET=0
        # switches it off (no limit, as in the original)
        raise ValueError(
            f"Cannot page beyond {max_offset} results in this order (offset {offset} + per_page {per_page}). "
            'To retrieve all results, use "sort": "_seq" with the "after" cursor, which is fast at any depth.'
        )

    async with connection() as conn:
        result = await search(
            conn,
            fieldset,
            _search_query(queries, filters),
            names,
            sort=sort_spec,
            page=0,
            offset=offset,
            per_page=per_page,
            snippets=snippets,
            highlight=highlight,
            keyset=keyset,
            after_id=cursor.get("id"),
            descending=descending,
            similar=similar,
            # The total of a scroll is counted once, on the first page, and carried in the cursor: counting again on
            # every page was two thirds of the work of a download
            with_total="total" not in cursor,
        )
    total = cursor.get("total", result.total)
    next_cursor = None
    if len(result.results) == per_page:
        if keyset or seq_order is not None or (after is None and not sort_spec and not similar and not queries):
            next_cursor = _encode_cursor({"id": result.last_id, "total": total, **({"desc": True} if descending else {})})
        else:
            next_cursor = _encode_cursor({"offset": offset + per_page, "total": total})
    return QueryResult(result.results, n=total, per_page=per_page, page=None if after else page, next=next_cursor)


# The write functions below can touch many documents: they run with the server's settings (no API statement timeout,
# the work pool), and afterwards ask for a vacuum of the changed partitions if the change was large (vacuum_soon:
# pg_search's fast counts depend on the visibility map, which only VACUUM updates)
@with_server_defaults
async def update_tag_query(
    index: str | list[str],
    action: Literal["add", "remove"],
    field: str,
    tag: str,
    queries: dict[str, str] | None = None,
    filters: dict[str, FilterSpec] | None = None,
    ids: list[str] | None = None,
):
    """Add or remove tags using a query"""
    await create_or_verify_tag_field(index, field)
    fieldset = await get_fieldset(index)
    query = _search_query(queries, filters, ids)
    async with connection() as conn:
        from amcat4.postgres.search import count

        total = await count(conn, fieldset, query)
        updated = await storage.update_tag_by_query(conn, fieldset, query, field, tag, action)
    await vacuum_soon(fieldset.partitions.values(), updated)
    return dict(updated=updated, total=total)


@with_server_defaults
async def update_query(
    index: str | list[str],
    field: str,
    value: Any,
    queries: dict[str, str] | None = None,
    filters: dict[str, FilterSpec] | None = None,
    ids: list[str] | None = None,
):
    fieldset = await get_fieldset(index)
    if field not in fieldset.by_name:
        raise ValueError(f"Field {field} does not exist")
    async with connection() as conn:
        updated = await storage.update_by_query(conn, fieldset, _search_query(queries, filters, ids), field, value)
    await vacuum_soon(fieldset.partitions.values(), updated)
    return dict(updated=updated, total=updated)


@with_server_defaults
async def delete_query(
    index: str | list[str],
    queries: dict[str, str] | None = None,
    filters: dict[str, FilterSpec] | None = None,
    ids: list[str] | None = None,
):
    fieldset = await get_fieldset(index)
    async with connection() as conn:
        deleted = await storage.delete_by_query(conn, fieldset, _search_query(queries, filters, ids))
    await vacuum_soon(fieldset.partitions.values(), deleted)
    return dict(updated=deleted, total=deleted)
