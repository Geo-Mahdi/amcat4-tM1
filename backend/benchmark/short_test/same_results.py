"""
Step 2, SAME-RESULTS: many random searches through query_documents, each compared with a reference that does not use
the Top-K scan of pg_search.

The reference:
- At the start, the field values of every document are copied with plain SQL into a flat table per project (the
  "reference table": one typed column per field, no search index on it). A sample of it is compared with the
  documents the generator made, so the reference is tied to what was uploaded.
- Filters and sorts on fields, without a query string: one plain SQL statement on the reference table (WHERE, ORDER
  BY, LIMIT in postgres). The search index is not used at all.
- With a query string and a sort on fields: the search index only says which documents match the query string (all
  of them, materialized, no ORDER BY or LIMIT inside, so no Top-K scan). Filters and order are plain SQL on the
  reference table.
- Relevance order: every hit of the query string with its score is materialized first, then filtered and sorted by
  postgres (as tests/test_parity.py does).
EXPLAIN of the reference statements is checked: no Top-K scan, and for the first kind no pg_search scan at all.

Ties are broken by the internal id (the order of upload), in the code under test and in the reference.

Sorting on a tag field has two definitions. "json_text" (postgres-fast-v2/v3): the JSON text of the list.
"min_max" (the cutoff branch, as Elasticsearch): ascending by the smallest tag, descending by the largest tag.
The definition is chosen from the setting cutoff_sort, if the branch has it.
"""

import random
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from psycopg import sql

from benchmark.short_test import data
from benchmark.short_test.common import PROJECT_BIG, PROJECT_SMALL, Deadline, TooSlow, own_connection, setting

CASE_SEED = 2026
KEYWORDS = ["source", "author", "section"]
NUMBERS = ["n_int", "n_num"]
SORT_FIELDS = ["source", "date", "author", "section", "n_num", "n_int", "tags"]
SLOT_FIELDS = {"source", "date"}  # the fields in a sort slot (sorted inside the search index, with Top-K)
DEFAULT_MAX_OFFSET = 10_000
# Stop starting new cases this long before the time limit, so the step itself ends in time
MARGIN_S = 40


def ref_table(project: str) -> sql.Identifier:
    return sql.Identifier(f"short_test_ref_{project}")


def tag_sort_definition() -> str:
    return "min_max" if setting("cutoff_sort", False) else "json_text"


# ---------------------------------------------------------------- the reference table


async def build_reference_table(conn, project: str) -> int:
    """Copy the field values of all documents of a project into a flat table, with plain SQL (no search index)"""
    from amcat4.postgres.projects import project_pk
    from amcat4.systemdata.fields import field_infos

    pk = await project_pk(project)
    infos = await field_infos(project)
    cur = await conn.execute("SELECT partition_id FROM projects WHERE pk = %s", [pk])
    partition = (await cur.fetchone())["partition_id"]

    def raw(name: str) -> sql.Composable:
        return sql.SQL("(d.exact_fields->>{})").format(sql.Literal(infos[name].key))

    tags = sql.SQL("(d.exact_fields->{})").format(sql.Literal(infos["tags"].key))
    columns = [sql.SQL("d.id"), sql.SQL("d.doc_id")]
    for name in [*KEYWORDS, "url"]:
        columns.append(sql.SQL('{} COLLATE "C" AS {}').format(raw(name), sql.Identifier(name)))
    for name in NUMBERS:
        columns.append(sql.SQL("{}::numeric AS {}").format(raw(name), sql.Identifier(name)))
    columns.append(sql.SQL("{}::timestamptz AS date").format(raw("date")))
    columns.append(sql.SQL("{} AS tags").format(tags))
    columns.append(sql.SQL('{}::text COLLATE "C" AS tags_text').format(tags))
    for function in ("min", "max"):
        columns.append(
            sql.SQL('(SELECT {f}(t COLLATE "C") FROM jsonb_array_elements_text({tags}) t) AS {name}').format(
                f=sql.SQL(function), tags=tags, name=sql.Identifier(f"tag_{function}")
            )
        )
    table = ref_table(project)
    await conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table))
    await conn.execute(
        sql.SQL(
            "CREATE UNLOGGED TABLE {} AS SELECT {} FROM documents d WHERE d.partition_id = {} AND d.project_pk = {}"
        ).format(table, sql.SQL(", ").join(columns), sql.Literal(partition), sql.Literal(pk))
    )
    await conn.execute(sql.SQL("ALTER TABLE {} ADD PRIMARY KEY (id)").format(table))
    await conn.execute(sql.SQL("ANALYZE {}").format(table))
    cur = await conn.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(table))
    return (await cur.fetchone())["n"]


async def check_against_generator(conn, project: str, total: int, rnd: random.Random) -> dict[str, Any]:
    """Compare the reference table with the documents as the generator made them, for two batches"""
    n_batches = -(-total // data.BATCH)
    checked, differences = 0, []
    for batch in sorted({0, rnd.randrange(n_batches)}):
        docs = {d["_id"]: d for d in data.make_batch(project, batch, total)}
        cur = await conn.execute(sql.SQL("SELECT * FROM {} WHERE doc_id = ANY(%s)").format(ref_table(project)), [list(docs)])
        rows = {row["doc_id"]: row for row in await cur.fetchall()}
        for doc_id, doc in docs.items():
            checked += 1
            row = rows.get(doc_id)
            if row is None:
                differences.append((doc_id, "missing"))
                continue
            date = datetime.fromisoformat(doc["date"]).replace(tzinfo=timezone.utc)
            n_num = None if row["n_num"] is None else float(row["n_num"])
            expected = [doc["source"], doc.get("author"), doc["section"], doc["url"], doc["n_int"], doc.get("n_num"), date]
            stored = [row["source"], row["author"], row["section"], row["url"], int(row["n_int"]), n_num, row["date"]]
            if expected != stored or doc.get("tags") != row["tags"]:
                differences.append((doc_id, str(expected), str(stored), doc.get("tags"), row["tags"]))
    return {"documents_checked": checked, "differences": len(differences), "first": differences[:5]}


# ---------------------------------------------------------------- the reference statements


def _utc(value: str) -> str:
    """A date as the code under test reads it: without a time zone it is UTC"""
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc).isoformat()


def filter_condition(name: str, spec: dict) -> sql.Composable:
    """A filter as plain SQL on the reference table r"""
    col = sql.SQL("r.{}").format(sql.Identifier(name))
    parts: list[sql.Composable] = []
    for op, value in spec.items():
        if op == "exists":
            parts.append(sql.SQL("{} IS NOT NULL" if value else "{} IS NULL").format(col))
        elif op == "values":
            if name == "tags":
                parts.append(sql.SQL("{} ?| {}::text[]").format(col, sql.Literal([str(v) for v in value])))
            elif name in NUMBERS:
                parts.append(sql.SQL("{} = ANY({}::numeric[])").format(col, sql.Literal([Decimal(str(v)) for v in value])))
            elif name == "date":
                parts.append(sql.SQL("{} = ANY({}::timestamptz[])").format(col, sql.Literal([_utc(v) for v in value])))
            else:
                parts.append(sql.SQL("{} = ANY({}::text[])").format(col, sql.Literal([str(v) for v in value])))
        else:
            operator = sql.SQL({"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[op])
            if name == "date":
                bound: sql.Composable = sql.SQL("{}::timestamptz").format(sql.Literal(_utc(value)))
            else:
                bound = sql.SQL("{}::numeric").format(sql.Literal(Decimal(str(value))))
            parts.append(sql.SQL("{} {} {}").format(col, operator, bound))
    return sql.SQL(" AND ").join(parts)


def where_clause(filters: dict[str, dict]) -> sql.Composable:
    if not filters:
        return sql.SQL("TRUE")
    return sql.SQL(" AND ").join(filter_condition(name, spec) for name, spec in filters.items())


def order_clause(sort: list[tuple[str, str]], tag_definition: str) -> sql.Composable:
    """ORDER BY on the reference table r: values first, missing values last, ties by internal id"""
    items = []
    for name, order in sort:
        if name == "tags":
            name = "tags_text" if tag_definition == "json_text" else ("tag_min" if order == "asc" else "tag_max")
        direction = sql.SQL("DESC NULLS LAST" if order == "desc" else "ASC NULLS LAST")
        items.append(sql.SQL("r.{} {}").format(sql.Identifier(name), direction))
    items.append(sql.SQL("r.id"))
    return sql.SQL(", ").join(items)


class Reference:
    """Runs the reference statements of one project, and checks their plans"""

    def __init__(self, conn, project: str, tag_definition: str):
        self.conn = conn
        self.project = project
        self.table = ref_table(project)
        self.tag_definition = tag_definition
        self.plans: dict[str, dict[str, Any]] = {}
        self.fieldset: Any = None

    async def _hits(
        self, queries: dict[str, str], with_score: bool, filters: dict[str, dict] | None = None
    ) -> tuple[sql.Composable, list]:
        """The CTE with all documents that match the query string (and their score): the only use of the search index"""
        from amcat4.models import FilterSpec
        from amcat4.postgres.search import SearchQuery, compile_search
        from amcat4.systemdata.fields import get_fieldset

        if self.fieldset is None:
            self.fieldset = await get_fieldset([self.project])
        # Normally without the filters (they are applied in plain SQL, on the reference table): filters do not change
        # the score. Only with AMCAT4_FILTER_CLAUSES=false (filters are scored too) the filters must be in the search
        # for the score to be the same; they are then still applied in plain SQL as well.
        in_index = None
        if with_score and filters and not setting("filter_clauses", True):
            in_index = {name: FilterSpec(**spec) for name, spec in filters.items()}
        c = compile_search(self.fieldset, SearchQuery(queries=queries, filters=in_index))  # parent table, no fast path
        score = sql.SQL(", paradedb.score(documents.id) AS s") if with_score else sql.SQL("")
        cte = sql.SQL("WITH hits AS MATERIALIZED (SELECT documents.id{} FROM {} WHERE {}) ").format(score, c.table, c.where)
        return cte, list(c.params)

    async def run(
        self, queries: dict[str, str] | None, filters: dict[str, dict], sort: list[tuple[str, str]], limit: int
    ) -> tuple[int, list[str]]:
        """(number of hits, the first `limit` document ids in the expected order). sort [] with queries = relevance"""
        where = where_clause(filters)
        params: list = []
        if queries is None:
            shape, cte, source = "plain", sql.SQL(""), sql.SQL("{} r").format(self.table)
            order = order_clause(sort, self.tag_definition)
        else:
            by_score = not sort
            shape = "relevance" if by_score else "query_then_plain"
            cte, params = await self._hits(queries, with_score=by_score, filters=filters)
            source = sql.SQL("hits h JOIN {} r ON r.id = h.id").format(self.table)
            order = sql.SQL("h.s DESC, h.id") if by_score else order_clause(sort, self.tag_definition)
        stmt = sql.SQL(
            "{cte}SELECT (SELECT count(*) FROM {source} WHERE {where}) AS total, "
            "ARRAY(SELECT r.doc_id FROM {source} WHERE {where} ORDER BY {order} LIMIT {limit}) AS ids"
        ).format(cte=cte, source=source, where=where, order=order, limit=sql.Literal(limit))
        await self._check_plan(shape, stmt, params)
        cur = await self.conn.execute(stmt, params)
        row = await cur.fetchone()
        return row["total"], row["ids"]

    async def _check_plan(self, shape: str, stmt: sql.Composable, params: list) -> None:
        """EXPLAIN the reference statement (the first 3 of each kind, then every 20th): it must not use Top-K"""
        info = self.plans.setdefault(shape, {"statements": 0, "plans_checked": 0, "top_k_found": 0, "index_used": 0})
        info["statements"] += 1
        if info["statements"] > 3 and info["statements"] % 20:
            return
        cur = await self.conn.execute(sql.SQL("EXPLAIN (COSTS OFF) ") + stmt, params)
        plan = "\n".join(row["QUERY PLAN"] for row in await cur.fetchall())
        info["plans_checked"] += 1
        if "topk" in plan.lower().replace(" ", "").replace("-", ""):
            info["top_k_found"] += 1
            info["bad_plan"] = plan
        if "ParadeDB" in plan or "Custom Scan" in plan:
            info["index_used"] += 1
        info.setdefault("example_plan", plan)


# ---------------------------------------------------------------- random cases


def _random_day(rnd: random.Random) -> str:
    return (data.DATE_START + timedelta(days=rnd.randrange(data.DATE_DAYS))).strftime("%Y-%m-%d")


def random_filters(rnd: random.Random) -> dict[str, dict]:
    filters: dict[str, dict] = {}
    for _ in range(rnd.choice([0, 1, 1, 2])):
        name = rnd.choice(["source", "author", "section", "n_num", "n_int", "date", "tags"])
        kind = rnd.choice(["values", "values", "range", "range", "exists"])
        if kind == "exists":
            filters[name] = {"exists": rnd.random() < 0.7}
        elif kind == "values" or name in ("source", "author", "section", "tags"):
            if name == "source":
                values: list[Any] = rnd.sample(data.SOURCES, rnd.randrange(1, 4))
            elif name == "author":
                values = [data.author(int(data.N_AUTHORS * rnd.random() ** 1.5)) for _ in range(rnd.randrange(1, 4))]
            elif name == "section":
                values = rnd.sample(data.SECTIONS, rnd.randrange(1, 3))
            elif name == "tags":
                values = rnd.sample(data.TAGS, rnd.randrange(1, 3))
            elif name == "n_num":  # whole and decimal values, as numbers and as text
                values = rnd.sample(
                    [17, "17.5", 18, "3.25", 40, -2, "-2.5", 0, 99, "0.1", "17.0", "55.75"], rnd.randrange(1, 3)
                )
            elif name == "n_int":
                values = [rnd.randrange(1000) for _ in range(rnd.randrange(1, 4))]
            else:
                values = [_random_day(rnd) for _ in range(rnd.randrange(1, 3))]
            filters[name] = {"values": values}
        else:
            if name == "date":
                lo, hi = sorted([_random_day(rnd), _random_day(rnd)])
            elif name == "n_int":
                lo, hi = sorted([rnd.randrange(1000), rnd.randrange(1000)])
            else:
                # (a filter value is a whole number or text, so decimal bounds are given as text)
                lo, hi = rnd.choice([-5, 0, "3.5", 10, 17, "17.5"]), rnd.choice([18, "25.25", 40, 60, 99])
            spec: dict[str, Any] = {}
            if rnd.random() < 0.8:
                spec[rnd.choice(["gt", "gte"])] = lo
            if rnd.random() < 0.8 or not spec:
                spec[rnd.choice(["lt", "lte"])] = hi
            filters[name] = spec
    return filters


def random_words(rnd: random.Random, lowest_rank: int) -> str:
    """A query string: 1 to 3 words of very different frequencies (any of them may match), or a phrase"""
    if rnd.random() < 0.1:
        return f'"{data.word(rnd.randrange(1, 6))} {data.word(rnd.randrange(1, 6))}"'
    ranks = [int(lowest_rank * (20_000 / lowest_rank) ** rnd.random()) for _ in range(rnd.choice([1, 1, 2, 3]))]
    return " ".join(data.word(r) for r in ranks)


def make_groups(rnd: random.Random) -> list[dict[str, Any]]:
    """
    An endless-enough list of case groups, the same on every run. A group is one selection and order; its cases are
    pages of it (so the reference is computed once per group).
    """
    groups = []
    for _ in range(4000):
        project = PROJECT_BIG if rnd.random() < 0.4 else PROJECT_SMALL
        lowest_rank = 20 if project == PROJECT_BIG else 1  # (very common words in the big project take too long)
        kind = rnd.choice(["plain", "plain", "plain", "query_then_plain", "relevance", "relevance"])
        group: dict[str, Any] = {"project": project, "kind": kind, "queries": None, "sort": []}
        if kind == "plain":
            group["filters"] = random_filters(rnd)
        else:
            group["queries"] = {"q": random_words(rnd, lowest_rank)}
            group["filters"] = random_filters(rnd) if rnd.random() < 0.5 else {}
        if kind != "relevance":
            names = rnd.sample(SORT_FIELDS, rnd.choice([1, 1, 1, 2, 2, 3]))
            group["sort"] = [(name, rnd.choice(["asc", "desc"])) for name in names]
        group["seed"] = rnd.randrange(10**9)
        groups.append(group)
    return groups


def group_cases(group: dict[str, Any], total: int, max_offset: int) -> list[dict[str, Any]]:
    """The pages to check of a group, given its number of hits: the first page, a random page, and a deep page"""
    rnd = random.Random(group["seed"])
    cases = []

    def case(page: int, per_page: int, explicit_score: bool = False) -> None:
        cases.append({"page": page, "per_page": per_page, "explicit_score": explicit_score})

    def last_page(per_page: int) -> int:
        reachable = min(total, max_offset)
        return max(0, min((reachable - 1) // per_page, max_offset // per_page - 1))

    if group["kind"] == "relevance":
        per_page = rnd.choice([5, 10, 20])
        case(0, per_page)
        case(0, per_page, explicit_score=True)  # "sort": "_score" must give the same as no sort
        case(min(rnd.choice([1, 3, 10, last_page(per_page)]), last_page(per_page)), per_page, rnd.random() < 0.5)
        return cases
    json_sort = any(name not in SLOT_FIELDS for name, _ in group["sort"])
    per_page = rnd.choice([1, 5, 10, 50])
    case(0, per_page)
    if json_sort and total > 100_000:
        return cases  # a sort on a field without slot reads every hit: one page of these is enough
    per_page = rnd.choice([1, 5, 10, 50])
    case(rnd.randrange(0, last_page(per_page) + 1), per_page)
    per_page = rnd.choice([5, 10, 50])
    case(last_page(per_page), per_page)
    return cases


def labels(group: dict[str, Any], case: dict[str, Any]) -> list[str]:
    """Names of the kinds of case this is, for the counts in the report"""
    out = [
        group["project"],
        {"plain": "filter + sort", "query_then_plain": "query + sort", "relevance": "relevance"}[group["kind"]],
    ]
    names = [name for name, _ in group["sort"]]
    if len(names) > 1:
        out.append("multi-key sort")
    if any(name in SLOT_FIELDS for name in names):
        out.append("sort on slot field")
    if any(name not in SLOT_FIELDS and name != "tags" for name in names):
        out.append("sort on JSON field")
    if "tags" in names:
        out.append("sort on tags")
    if group["filters"]:
        out.append("with filter")
    if case["page"] * case["per_page"] >= 1000:
        out.append("deep page (offset >= 1000)")
    return out


# ---------------------------------------------------------------- the step


async def _query(group: dict[str, Any], case: dict[str, Any]):
    from amcat4.models import FieldSpec, FilterSpec, SortSpec
    from amcat4.projects.query import query_documents

    sort = [{name: SortSpec(order=order)} for name, order in group["sort"]]  # type: ignore[arg-type]
    if case["explicit_score"]:
        sort = [{"_score": SortSpec(order="desc")}]
    return await query_documents(
        group["project"],
        fields=[FieldSpec(name="section")],
        queries=group["queries"],
        filters={name: FilterSpec(**spec) for name, spec in group["filters"].items()} or None,
        sort=sort or None,  # type: ignore[arg-type]
        page=case["page"],
        per_page=case["per_page"],
    )


async def check_text_matching(conn, ref: Reference, out: dict[str, Any]) -> None:
    """
    A few one-word queries: the documents the search index finds must be the documents whose title or text holds
    the word, found by postgres with a regular expression (no search index)
    """
    from amcat4.postgres.projects import project_pk
    from amcat4.systemdata.fields import field_infos

    infos = await field_infos(ref.project)
    pk = await project_pk(ref.project)
    checks = []
    for rank in (40, 300, 3000, 15_000):
        w = data.word(rank)
        cte, params = await ref._hits({"q": w}, with_score=False)
        cur = await conn.execute(cte + sql.SQL("SELECT coalesce(array_agg(id ORDER BY id), '{}') AS ids FROM hits"), params)
        found = (await cur.fetchone())["ids"]
        pattern = rf"\m{w}\M"
        cur = await conn.execute(
            sql.SQL(
                "SELECT coalesce(array_agg(d.id ORDER BY d.id), '{{}}') AS ids FROM documents d "
                "WHERE d.project_pk = %s AND (d.text_fields->>{} ~* %s OR d.text_fields->>{} ~* %s)"
            ).format(sql.Literal(infos["title"].key), sql.Literal(infos["text"].key)),
            [pk, pattern, pattern],
        )
        expected = (await cur.fetchone())["ids"]
        checks.append({"word": w, "rank": rank, "documents": len(expected), "same": found == expected})
    out["text_match_check"] = checks


async def step_same(args, deadline: Deadline, out: dict[str, Any]) -> None:
    max_offset = setting("max_offset", DEFAULT_MAX_OFFSET) or DEFAULT_MAX_OFFSET
    tag_definition = tag_sort_definition()
    out.update(
        {"cases_target": args.cases, "cases_run": 0, "mismatches": 0, "errors": 0, "tag_sort_definition": tag_definition}
    )
    out["mismatch_list"] = []
    by_label: dict[str, dict[str, int]] = {}
    out["by_kind"] = by_label
    rnd = random.Random(CASE_SEED)

    async with own_connection(120) as conn:
        await conn.execute("SET work_mem = '256MB'")
        refs: dict[str, Reference] = {}
        t0 = time.monotonic()
        out["storage_check"] = {}
        for project in (PROJECT_SMALL, PROJECT_BIG):
            n = await build_reference_table(conn, project)
            refs[project] = Reference(conn, project, tag_definition)
            out["storage_check"][project] = await check_against_generator(conn, project, n, rnd)
        out["reference_tables_s"] = round(time.monotonic() - t0, 1)
        await check_text_matching(conn, refs[PROJECT_SMALL], out)
        print(f"  reference tables built in {out['reference_tables_s']} s", flush=True)

        for group in make_groups(rnd):
            if out["cases_run"] >= args.cases:
                break
            if deadline.left() < MARGIN_S:
                out["stopped_early"] = f"stopped at the time limit after {out['cases_run']} of {args.cases} cases"
                print("  " + out["stopped_early"])
                break
            ref = refs[group["project"]]
            try:
                total, ids = await ref.run(group["queries"], group["filters"], group["sort"], max_offset)
            except Exception as e:  # e.g. the reference itself timed out: report, do not count as a mismatch
                out["errors"] += 1
                out.setdefault("error_list", []).append({"group": _plain(group), "reference_error": _short(e)})
                continue
            for case in group_cases(group, total, max_offset):
                names = labels(group, case)
                problem: dict[str, Any] | None = None
                try:
                    result = await _query(group, case)
                    got = [d["_id"] for d in result.data]
                    start = case["page"] * case["per_page"]
                    want = ids[start : start + case["per_page"]]
                    if got != want or result.total_count != total:
                        problem = {"got": got[:10], "want": want[:10], "total_got": result.total_count, "total_want": total}
                except Exception as e:
                    out["errors"] += 1
                    out.setdefault("error_list", []).append({"group": _plain(group), **case, "error": _short(e)})
                    continue
                out["cases_run"] += 1
                for name in names:
                    counts = by_label.setdefault(name, {"cases": 0, "mismatches": 0})
                    counts["cases"] += 1
                    counts["mismatches"] += problem is not None
                if problem is not None:
                    out["mismatches"] += 1
                    if len(out["mismatch_list"]) < 200:
                        out["mismatch_list"].append({"group": _plain(group), **case, **problem})
                    print(f"  MISMATCH: {_plain(group)} {case} {problem}", flush=True)
        out["reference_plans"] = {**refs[PROJECT_SMALL].plans}
        for shape, info in refs[PROJECT_BIG].plans.items():
            mine = out["reference_plans"].setdefault(shape, info)
            if mine is not info:
                for key in ("statements", "plans_checked", "top_k_found", "index_used"):
                    mine[key] += info[key]
                if "bad_plan" in info:
                    mine["bad_plan"] = info["bad_plan"]
        for project in refs:
            await conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(ref_table(project)))

    problems = []
    if out["mismatches"]:
        problems.append(f"{out['mismatches']} of {out['cases_run']} cases differ from the reference")
    if out["errors"]:
        problems.append(f"{out['errors']} cases gave an error")
    plans = out["reference_plans"]
    if any(info["top_k_found"] for info in plans.values()) or plans.get("plain", {}).get("index_used"):
        problems.append("a reference statement used the search index in a way it should not (see reference_plans)")
    if any(check["differences"] for check in out["storage_check"].values()):
        problems.append("the stored documents differ from the generated documents (see storage_check)")
    if not all(check["same"] for check in out["text_match_check"]):
        problems.append("the search index and the regular expression find different documents (see text_match_check)")
    print(f"  {out['cases_run']} cases, {out['mismatches']} mismatches, {out['errors']} errors")
    if not out["cases_run"] and not problems:
        raise TooSlow("no case could be run inside the time limit")
    if problems:
        out["status"] = "failed"
        out["problems"] = problems
        for p in problems:
            print("  PROBLEM: " + p)


def _plain(group: dict[str, Any]) -> dict[str, Any]:
    return {k: group[k] for k in ("project", "kind", "queries", "filters", "sort")}


def _short(e: Exception) -> str:
    return f"{type(e).__name__}: {str(e).splitlines()[0][:300] if str(e) else ''}"
