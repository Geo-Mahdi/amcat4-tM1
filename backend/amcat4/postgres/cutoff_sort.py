"""
The cutoff sort: the ids of one page of a search that is sorted on a json field, without reading every hit.

A sort on a field that is stored in json (exact_fields, no sort slot) cannot be a Top-K scan in pg_search, so the
late fetch (search.py) reads and sorts every hit to find the ids of the page. This module finds the same ids in
another way:

1. An aggregation inside the search index (pdb.agg, over the same search) gives the values of the field in sort
   order with the number of documents per value. Walking these until offset + per_page documents are passed gives
   the cutoff: the value of the last document of the page. (Number fields: stats + histogram, see _number_cutoff.)
2. The normal statement (same ORDER BY) then runs with one more clause inside the json query: value <= cutoff
   (ascending) or >= cutoff (descending). Only these few rows are read and sorted.

The order is exactly the order of the late fetch: key 1, key 2, ..., documents.id, with missing values last.
- Documents without a value are a group of their own (after the documents with a value). Inside it the order is
  that of the remaining keys, found the same way.
- Ties at the cutoff: the clause includes the cutoff value, so all documents with that value are sorted. If there
  are many (more than SPLIT_ROWS), they are a group of their own too: the documents before the cutoff value are
  sorted, and the documents with exactly the cutoff value are ordered by the remaining keys (with no keys left
  that is ORDER BY id, a Top-K scan).
- Tags: Elasticsearch order (ascending on the smallest tag of a document, descending on the largest, see
  tag_sort_sql). A clause on a tag list matches if any tag matches, which is the same as "smallest tag <= cutoff"
  (or "largest tag >= cutoff"). The aggregation counts a document once per tag, so the cutoff can be too early;
  then the page comes back short and the walk is repeated with a larger target. Many documents with the cutoff
  tag: groups, see _tag_groups.
- Small results (up to SMALL_HITS hits) are not cut off at all: small_page_ids sorts all hits in one statement.

Rules that come from pg_search 0.27.0 (found in the research of 25 Nov 2026, json-speed-research/round2/R2_cutoff.md):
- conditions on json keys are always part of the json query, never SQL comparisons (pg_search turns
  `(json->>'k')::numeric > c` into a text range: wrong results);
- bounds on number fields are always floats (an integer upper bound is wrong on a path with whole and decimal values);
- a terms aggregation on a number field with whole and decimal values is not ordered: number fields use a histogram;
- a text value that looks like a timestamp (2020-01-01T00:00:00Z) is indexed as a date, and sorts in another place
  in the aggregation than in SQL: if the hits have such values in a keyword, url or tag field, the cutoff is not used;
- pdb.agg needs its json as a constant in the statement (not a bind parameter).

Correctness first: page_ids raises _Fallback (and returns None) whenever something is not as expected, and the
caller then uses the late fetch. Switch: AMCAT4_CUTOFF_SORT (config.py).
"""

import dataclasses
import json
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable

from psycopg import AsyncConnection, sql

from amcat4.postgres.fields import FieldInfo, FieldSet

# The most buckets pg_search returns for a terms aggregation (paradedb.max_term_agg_buckets). Every bucket has at
# least one document, so offset + per_page + 2 buckets are always enough; deeper pages use the late fetch
MAX_BUCKETS = 65_000
# Number fields: buckets per histogram, and from how many documents in the bucket of the cutoff a second, finer
# histogram inside that bucket is worth its statement
HIST_BUCKETS = 1_000
ZOOM_ROWS = 2_000
# More documents than this with exactly the cutoff value: do not sort them on the first key (see module text)
SPLIT_ROWS = 1_000
# Text fields: up to this many values, "up to the cutoff" is a set of terms instead of a range (see _among)
MAX_TERMS = 1_000
# Tags: how often to repeat the walk with a larger target before giving up
MAX_RETRY = 3

# Up to this many hits, all hits are sorted (one statement, as the late fetch does) and no cutoff is looked for: the
# aggregation would cost more than it saves. See small_page_ids for the measurements behind the number
SMALL_HITS = 5_000

TEXT_TYPES = ("keyword", "url", "tag")
CUTOFF_TYPES = (*TEXT_TYPES, "integer", "number", "date")
# pdb.agg gives date values as microseconds since this moment
_DATE_EPOCH = datetime(2000, 1, 1)
# Above this a float cannot hold every whole number
_EXACT_FLOAT = 2**53

_ID = sql.SQL("documents.id")


OrderItem = tuple[sql.Composable, sql.Composable | None]


def order_item(expr: sql.Composable, direction: sql.Composable | None) -> sql.Composable:
    return expr if direction is None else sql.SQL("{} {}").format(expr, direction)


class _Fallback(Exception):
    """The cutoff cannot be used (or its result is not as expected): the caller uses the late fetch"""


@dataclass(frozen=True)
class SortKey:
    """A sort key that the cutoff can be used for: a field stored under one json key of exact_fields"""

    field: FieldInfo
    desc: bool


def tag_sort_sql(f: FieldInfo, desc: bool, byte_order: bool = True) -> sql.Composable:
    """
    The value a tag field is sorted on, in the order of Elasticsearch: the smallest tag of the document for an
    ascending sort, the largest for a descending sort. NULL (sorted last) without tags: no key, null or an empty list.
    A value that is not a list (should not happen) counts as a single tag.
    """
    value = sql.SQL("documents.{}->{}").format(sql.Identifier(f.column), sql.Literal(f.key))
    return sql.SQL(
        "(SELECT {fn}(tag{collate}) FROM jsonb_array_elements_text(CASE jsonb_typeof({v}) WHEN 'array' THEN {v} "
        "WHEN 'null' THEN '[]'::jsonb ELSE jsonb_build_array({v}) END) AS t(tag))"
    ).format(fn=sql.SQL("max" if desc else "min"), collate=sql.SQL(' COLLATE "C"' if byte_order else ""), v=value)


def sort_key(
    fieldset: FieldSet, name: str, order: str, has_slot: Callable[[str], bool], byte_order: bool = True
) -> SortKey | None:
    """The SortKey for this item of a sort, or None if the cutoff cannot be used for it"""
    fs = fieldset.by_name.get(name)
    if not fs or len(fs) != 1:
        # no field (_score, _id, _seq, random), or a field with another json key in each project of the query
        return None
    f = fs[0]
    if f.type not in CUTOFF_TYPES or f.column != "exact_fields" or f.subkey:
        return None
    if has_slot(name):
        return None  # a real column: pg_search sorts it with a Top-K scan
    if f.type in TEXT_TYPES and not byte_order:
        return None  # the aggregation is in byte order, the sort (AMCAT4_BYTE_ORDER_SORT=false) is not
    return SortKey(f, order == "desc")


# ---------------------------------------------------------------- json clauses


def _bound(k: SortKey, value: Any) -> Any:
    """A bucket key or number as the value of a range bound"""
    t = k.field.type
    if t == "date":
        # (same format as fields.normalize_date)
        try:
            dt = _DATE_EPOCH + timedelta(microseconds=value)
        except (OverflowError, TypeError) as e:
            raise _Fallback(f"date out of range: {value!r}") from e
        return f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}T{dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}.{dt.microsecond:06d}Z"
    if t == "number":
        return float(value)
    return value


def _range(k: SortKey, lower: Any = None, upper: Any = None, include_lower: bool = True, include_upper: bool = True) -> dict:
    if lower is None and upper is None:
        raise _Fallback("range without bounds")  # pg_search gives an error for it

    def bound(value: Any, inclusive: bool) -> dict | None:
        return None if value is None else {"included" if inclusive else "excluded": _bound(k, value)}

    q: dict[str, Any] = {
        "field": k.field.path,
        "lower_bound": bound(lower, include_lower),
        "upper_bound": bound(upper, include_upper),
    }
    if k.field.type == "date":
        q["is_datetime"] = True
    return {"range": q}


def _up_to(k: SortKey, value: Any, inclusive: bool = True) -> dict:
    """Documents whose value is at or before this value in sort order (not at: inclusive=False)"""
    if k.desc:
        return _range(k, lower=value, include_lower=inclusive)
    return _range(k, upper=value, include_upper=inclusive)


def _among(k: SortKey, values: list[Any]) -> dict:
    """
    Documents with a value up to the last of these values: the values of the search in sort order, from the first
    one, without gaps (the bucket keys of the walk). For text fields this is a set of terms as long as it is not
    too long: pg_search looks terms up in its index, while a text range reads the values of every document of the
    partition (on a tag field of 1 M documents: 3 ms against 80 ms, 25 Nov 2026). Both select the same documents
    """
    if k.field.type in TEXT_TYPES and len(values) <= MAX_TERMS:
        return _equal(k, values[0]) if len(values) == 1 else {"term_set": {"terms": [_term(k, v) for v in values]}}
    return _up_to(k, values[-1])


def _term(k: SortKey, value: Any) -> dict:
    return {"field": k.field.path, "value": value}


def _equal(k: SortKey, value: Any) -> dict:
    """Documents with exactly this value (tags: with this tag)"""
    if k.field.type in TEXT_TYPES:
        return {"term": _term(k, value)}
    return _range(k, value, value)


def _exists(k: SortKey) -> dict:
    return {"exists": {"field": k.field.path}}


def _not(clause: dict) -> dict:
    return {"boolean": {"must": [{"all": None}], "must_not": [clause]}}


def _missing(k: SortKey) -> dict:
    return _not(_exists(k))


# ---------------------------------------------------------------- the sort


@dataclass
class _Cutoff:
    clause: dict  # selects (at least) the documents with a value that can be on the page
    cut: bool  # False: the clause selects all documents with a value
    # terms walk only: the cutoff value, the number of documents before it and with it (exact, except for tags)
    value: Any = None
    before: int = 0
    ties: int = 0
    values_before: list[Any] = dataclasses.field(default_factory=list)  # the values before the cutoff value, in sort order


def _check_keys(k: SortKey, keys: list[Any]) -> None:
    """The bucket keys must have the type of the field and be in sort order, otherwise the walk means nothing"""
    text = k.field.type in TEXT_TYPES
    for key in keys:
        if text and not isinstance(key, str):
            raise _Fallback(f"bucket key {key!r} of a text field is not text")
        if not text and (isinstance(key, bool) or not isinstance(key, int)):
            # (whole numbers only: integer fields, and dates as microseconds)
            raise _Fallback(f"bucket key {key!r} is not a whole number")
    ordered = [key.encode("utf-8") for key in keys] if text else keys
    for a, b in zip(ordered, ordered[1:]):
        if not (a > b if k.desc else a < b):
            raise _Fallback("bucket keys are not in sort order")


class _Sorter:
    def __init__(
        self,
        conn: AsyncConnection,
        table: sql.Composable,
        json_query: Callable[[list[dict]], dict],
        keys: list[SortKey | None],
        order_items: list[OrderItem],
    ):
        self.conn, self.table, self.json_query, self.keys, self.order_items = conn, table, json_query, keys, order_items

    # ---- statements

    async def fetch(self, extra: list[dict], level: int, offset: int, limit: int) -> list[int]:
        """Ids of the documents of the search plus these clauses, ordered by the sort keys from this level on"""
        ordering = sql.SQL(", ").join([*(order_item(*item) for item in self.order_items[level:]), _ID])
        cur = await self.conn.execute(
            sql.SQL("SELECT documents.id FROM {} WHERE documents.id @@@ %s::jsonb ORDER BY {} LIMIT %s OFFSET %s").format(
                self.table, ordering
            ),
            [json.dumps(self.json_query(extra)), limit, offset],
        )
        return [row["id"] for row in await cur.fetchall()]  # type: ignore[index, call-overload]

    async def count(self, extra: list[dict]) -> int:
        cur = await self.conn.execute(
            sql.SQL("SELECT count(*) AS n FROM {} WHERE documents.id @@@ %s::jsonb").format(self.table),
            [json.dumps(self.json_query(extra))],
        )
        return (await cur.fetchone())["n"]  # type: ignore[index, call-overload]

    async def aggregate(self, extra: list[dict], *requests: dict) -> list[dict]:
        """The results of these aggregations (one statement) over the search plus these clauses"""
        aggs = [
            sql.SQL("pdb.agg({}::jsonb) AS {}").format(sql.Literal(json.dumps(r)), sql.Identifier(f"r{i}"))
            for i, r in enumerate(requests)
        ]
        cur = await self.conn.execute(
            sql.SQL("SELECT {} FROM {} WHERE documents.id @@@ %s::jsonb").format(sql.SQL(", ").join(aggs), self.table),
            [json.dumps(self.json_query(extra))],
        )
        row: dict = await cur.fetchone()  # type: ignore[assignment]
        results = [row[f"r{i}"] for i in range(len(requests))]
        results = [json.loads(r) if isinstance(r, str) else r for r in results]
        if not all(isinstance(r, dict) for r in results):
            raise _Fallback("no aggregation result")
        return results

    # ---- the cutoff of one key

    async def _terms_cutoff(self, extra: list[dict], k: SortKey, need: int) -> _Cutoff:
        """Walk the values in sort order until `need` documents are passed"""
        size = need + 2
        if size > MAX_BUCKETS:
            raise _Fallback("page too deep for a terms aggregation")
        terms = {"terms": {"field": k.field.path, "size": size, "order": {"_key": "desc" if k.desc else "asc"}}}
        if k.field.type in TEXT_TYPES:
            # stats only sees values that are indexed as a number or a date: text that looks like a timestamp. These
            # sort in another place in the aggregation than in SQL (and after the text values, so the walk alone
            # would not see them)
            result, stats = await self.aggregate(extra, terms, {"stats": {"field": k.field.path}})
            if stats.get("count") != 0:
                raise _Fallback("text field with values that are indexed as dates or numbers")
        else:
            (result,) = await self.aggregate(extra, terms)
        buckets = [b for b in result.get("buckets", []) if b.get("key") is not None]
        _check_keys(k, [b["key"] for b in buckets])
        passed = 0
        for i, b in enumerate(buckets):
            n = b["doc_count"]
            if passed + n >= need:
                values = [x["key"] for x in buckets[: i + 1]]
                return _Cutoff(_among(k, values), True, value=b["key"], before=passed, ties=n, values_before=values[:-1])
            passed += n
        if len(buckets) >= size:
            raise _Fallback("more buckets than expected")
        return _Cutoff(_exists(k), False)  # all documents with a value are on or before this page

    async def _number_cutoff(self, extra: list[dict], k: SortKey, need: int) -> _Cutoff:
        """
        Number fields: the smallest and largest value, then a histogram between them; the cutoff is the far edge of
        the bucket in which `need` documents are passed. If that bucket is large, a second histogram inside it.
        The clause may select more documents than needed (the edges get a small margin), never fewer.
        """
        (stats,) = await self.aggregate(extra, {"stats": {"field": k.field.path}})
        n, lo, hi = stats.get("count"), stats.get("min"), stats.get("max")
        if not n or n <= need:
            return _Cutoff(_exists(k), False)
        if not all(isinstance(x, (int, float)) and math.isfinite(x) and abs(x) < _EXACT_FLOAT for x in (lo, hi)):
            raise _Fallback("number range not usable")
        if lo == hi:
            return _Cutoff(_exists(k), False)
        a, b, passed_before = lo, hi, 0
        for level in range(2):
            interval = (b - a) / HIST_BUCKETS
            if not interval > 0 or not math.isfinite(interval):
                break
            eps = max(interval * 1e-3, abs(a) * 1e-12, abs(b) * 1e-12)
            inside = extra if level == 0 else [*extra, _range(k, a - eps, b + eps)]
            (hist,) = await self.aggregate(
                inside, {"histogram": {"field": k.field.path, "interval": interval, "offset": a % interval}}
            )
            buckets = [x for x in hist.get("buckets", []) if x.get("doc_count", 0) > 0]
            if not all(isinstance(x.get("key"), (int, float)) for x in buckets):
                raise _Fallback("histogram key is not a number")
            buckets.sort(key=lambda x: x["key"], reverse=k.desc)
            passed, hit = passed_before, None
            for x in buckets:
                if passed + x["doc_count"] >= need:
                    hit = x
                    break
                passed += x["doc_count"]
            if hit is None:
                raise _Fallback("histogram has fewer documents than stats")
            edge_lo, edge_hi = hit["key"] - eps, hit["key"] + interval + eps
            if hit["doc_count"] <= ZOOM_ROWS or level == 1:
                return _Cutoff(_range(k, lower=edge_lo) if k.desc else _range(k, upper=edge_hi), True)
            # the documents before this bucket are on or before the page for sure: refine inside the bucket
            passed_before, a, b = passed, max(edge_lo, lo), min(edge_hi, hi)
        return _Cutoff(_range(k, lower=a) if k.desc else _range(k, upper=b), True)

    # ---- groups of documents

    async def with_value(self, extra: list[dict], level: int, offset: int, limit: int) -> list[int]:
        """The documents that have a value for the key of this level, in order, from offset"""
        k = self.keys[level]
        assert k is not None
        # (No "exists" clause next to the clauses below: a clause on a value only matches documents that have one, and
        # the aggregations skip documents without. On 1 M documents the clause cost 10 to 15 ms per statement)
        need = offset + limit
        for _ in range(MAX_RETRY + 1):
            if k.field.type == "number":
                c = await self._number_cutoff(extra, k, need)
            else:
                c = await self._terms_cutoff(extra, k, need)
            if c.cut and c.value is not None and k.field.type == "tag" and c.ties > SPLIT_ROWS:
                return await self._tag_groups(extra, level, offset, limit, c)
            if c.cut and c.value is not None and c.ties > SPLIT_ROWS:
                # Many documents with exactly the cutoff value: the documents before that value (c.before, exact) are
                # sorted as usual, the documents with the value are a group ordered by the remaining keys
                ids: list[int] = []
                if offset < c.before:
                    ids = await self.fetch([*extra, _among(k, c.values_before)], level, offset, limit)
                    if len(ids) != min(limit, c.before - offset):
                        raise _Fallback("fewer documents before the cutoff value than counted")
                if len(ids) < limit:
                    same = [*extra, _equal(k, c.value)]
                    ids += await self.page(same, level + 1, max(0, offset - c.before), limit - len(ids))
                if len(ids) != limit:
                    raise _Fallback("fewer documents with the cutoff value than counted")
                return ids
            ids = await self.fetch([*extra, c.clause], level, offset, limit)
            if len(ids) == limit or not c.cut:
                return ids
            if k.field.type != "tag":
                raise _Fallback("fewer documents up to the cutoff than counted")
            # tags: documents with several tags were counted more than once, so the cutoff was too early
            need = need * 2 + (limit - len(ids))
        raise _Fallback("tag cutoff too early after retries")

    async def _tag_groups(self, extra: list[dict], level: int, offset: int, limit: int, c: _Cutoff) -> list[int]:
        """
        Tags, with many documents that have the cutoff tag (so sorting all documents up to the cutoff would read many
        rows, each with its list of tags). The documents with a value are three groups, in this order:
        1. a tag before the cutoff tag: their smallest (descending: largest) tag is before the cutoff. Sorted as usual;
        2. the cutoff tag and no tag before it: the cutoff tag is their smallest (largest) tag, so they are ordered
           by the remaining keys (with no keys left: by id, a Top-K scan);
        3. only tags after the cutoff tag: the same question again, for what is left of the page.
        The aggregation counts a document once per tag, so its counts only say where to look: the sizes of the groups
        are counted (group 1: nothing to count if the cutoff tag is the first value), or follow from the rows.
        """
        k = self.keys[level]
        assert k is not None
        first_value = not c.values_before  # no value before the cutoff tag among these documents
        before_cutoff = {} if first_value else _among(k, c.values_before)
        before = 0 if first_value else await self.count([*extra, before_cutoff])
        ids: list[int] = []
        if offset < before:
            ids = await self.fetch([*extra, before_cutoff], level, offset, limit)
            if len(ids) != min(limit, before - offset):
                raise _Fallback("other number of documents before the cutoff tag than counted")
            if len(ids) == limit:
                return ids
        group = [*extra, _equal(k, c.value), *([] if first_value else [_not(before_cutoff)])]
        group_offset = max(0, offset - before)
        in_group = await self.page(group, level + 1, group_offset, limit - len(ids))
        ids += in_group
        if len(ids) < limit:
            # the group ran out: the rest of the page has only tags after the cutoff tag
            group_size = group_offset + len(in_group) if in_group else await self.count(group)
            rest = [*extra, _not(c.clause)]
            ids += await self.with_value(rest, level, max(0, offset - before - group_size), limit - len(ids))
        return ids

    async def page(self, extra: list[dict], level: int, offset: int, limit: int) -> list[int]:
        """`limit` ids from `offset` of the search plus these clauses, ordered by the sort keys from this level on"""
        if limit <= 0:
            return []
        k = self.keys[level] if level < len(self.keys) else None
        if k is None:
            # no keys left (ORDER BY id), or a key the cutoff is not for (a slot column, score, doc_id, ...)
            return await self.fetch(extra, level, offset, limit)
        ids = await self.with_value(extra, level, offset, limit)
        if len(ids) < limit:
            # The documents with a value ran out: the rest of the page is documents without a value, in the order of
            # the remaining keys. If any were returned, offset + len(ids) is the number of documents with a value
            with_value = offset + len(ids) if ids else await self.count([*extra, _exists(k)])
            ids += await self.page([*extra, _missing(k)], level + 1, max(0, offset - with_value), limit - len(ids))
        return ids


async def small_page_ids(
    conn: AsyncConnection,
    table: sql.Composable,
    json_query: dict,
    order_items: list[OrderItem],
    offset: int,
    limit: int,
) -> list[int] | None:
    """
    The internal ids of one page (in order) if the search has at most SMALL_HITS hits, otherwise None.

    One statement: it reads at most SMALL_HITS + 1 hits with their sort values, in no order, counts them and sorts
    them. For a small result this is the work of the first statement of the late fetch (which reads and sorts all
    hits), so a small result costs what it cost before the cutoff sort existed. For a large result the statement
    stops after SMALL_HITS + 1 hits and the cutoff sort follows.

    The number (benchmark of 1 M documents, 25 Nov 2026, pg_search 0.27.0): the statement costs about 2 ms per 1,000
    hits it reads, the cutoff sort 8 to 15 ms whatever the number of hits, so they meet at 3,000 to 6,000 hits. For a
    large result the statement is wasted: 4 ms at 2,000, 10 ms at 5,000. Under load (32 clients, lists of a few
    thousand hits sorted on a json field among other lists) 5,000 gave 323 to 333 requests/s in three runs, 2,000 and 3,000
    gave 306 and 300, and always using the cutoff sort 168 to 186.
    """
    columns = [sql.SQL("{} AS {}").format(expr, sql.Identifier(f"o{i}")) for i, (expr, _) in enumerate(order_items)]
    ordering = [order_item(sql.Identifier(f"o{i}"), direction) for i, (_, direction) in enumerate(order_items)]
    cur = await conn.execute(
        sql.SQL(
            "WITH hits AS MATERIALIZED (SELECT {} FROM {} WHERE documents.id @@@ %s::jsonb LIMIT %s) "
            "SELECT (SELECT count(*) FROM hits) AS n, "
            "ARRAY(SELECT id FROM hits ORDER BY {} LIMIT %s OFFSET %s) AS ids"
        ).format(sql.SQL(", ").join([_ID, *columns]), table, sql.SQL(", ").join([*ordering, sql.Identifier("id")])),
        [json.dumps(json_query), SMALL_HITS + 1, limit, offset],
    )
    row: dict = await cur.fetchone()  # type: ignore[assignment]
    return list(row["ids"]) if row["n"] <= SMALL_HITS else None


async def page_ids(
    conn: AsyncConnection,
    table: sql.Composable,
    json_query: Callable[[list[dict]], dict],
    keys: list[SortKey | None],
    order_items: list[OrderItem],
    offset: int,
    limit: int,
) -> list[int] | None:
    """
    The internal ids of one page (in order), or None if the cutoff cannot be used: the caller then sorts all hits.

    table: the partition table of the search, under the name "documents".
    json_query(clauses): the json query of the search with these extra clauses added to its unscored selection.
    keys, order_items: per item of the sort, its SortKey (None: the cutoff is not for this item) and its ORDER BY
    item. The first key must be a SortKey. The order is these items and then documents.id.
    All statements must see the same data: the caller runs this in one snapshot (REPEATABLE READ).
    Database errors are raised (the caller decides whether the late fetch is still worth trying).
    """
    if not keys or keys[0] is None or len(keys) != len(order_items) or limit <= 0 or offset < 0:
        return None
    try:
        ids = await _Sorter(conn, table, json_query, keys, order_items).page([], 0, offset, limit)
        if len(set(ids)) != len(ids):
            raise _Fallback("a document is in two groups")
        return ids
    except _Fallback as e:
        logging.info(f"Cutoff sort not used ({e}), sorting all hits instead")
        return None
