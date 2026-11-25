"""
Step 6, REPORT: write RESULTS.md (short tables) from results.json in the output directory.
results.json itself is written by run.py after every step.
"""

import os
from typing import Any

from benchmark.short_test.common import Deadline, load_results


def table(header: list[str], rows: list[list[Any]]) -> list[str]:
    def cell(v: Any) -> str:
        return "" if v is None else str(v).replace("|", "\\|").replace("\n", " ")

    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(cell(v) for v in row) + " |" for row in rows]
    return lines + [""]


def step_data(results: dict, name: str) -> tuple[dict, dict]:
    entry = results.get("steps", {}).get(name) or {}
    return entry, entry.get("data") or {}


def render(results: dict) -> str:
    git, db = results.get("git", {}), results.get("database", {})
    lines = ["# Short test results", ""]
    lines += [
        f"- Branch `{git.get('branch')}`, commit `{(git.get('commit') or '')[:10]}`"
        + (" (with uncommitted changes)" if git.get("dirty") == "yes" else ""),
        f"- PostgreSQL {db.get('postgres')}, pg_search {db.get('pg_search')}, database `{db.get('database')}`, "
        f"schema `{results.get('schema')}`",
        f"- Big project: {results.get('docs'):,} documents. "
        f"Run from {results.get('first_run')} to {results.get('last_run')} (UTC)",
        "",
        "## Steps",
        "",
    ]
    rows = []
    for name, entry in results.get("steps", {}).items():
        rows.append(
            [
                name,
                entry.get("status"),
                entry.get("seconds"),
                entry.get("limit_s"),
                entry.get("error") or entry.get("remark", ""),
            ]
        )
    lines += table(["step", "status", "seconds", "limit (s)", "remark"], rows)

    entry, d = step_data(results, "load")
    if entry:
        lines += ["## Load", ""]
        if d.get("skipped"):
            lines += [f"Not loaded in this run: {d['skipped']}.", ""]
        else:
            lines += [
                f"- Upload through the normal upload code: {d.get('upload_s')} s "
                f"({d.get('upload_docs_per_s')} documents per second)",
                f"- Rebuild of the search indexes: {d.get('reindex_s')} s; VACUUM (ANALYZE): {d.get('vacuum_s')} s",
                "",
            ]
        rows = [[p["table"], p["total_mb"], p["index_mb"], p["segments"]] for p in d.get("partitions", [])]
        lines += table(["partition", "total size (MB)", "search index (MB)", "index segments"], rows)
        lines += [f"Fields in a sort slot: {d.get('sort_slots')}", ""]

    entry, d = step_data(results, "same")
    if entry:
        lines += ["## Same results", ""]
        lines += [
            f"- Cases: {d.get('cases_run')} of {d.get('cases_target')} run. "
            f"**Mismatches: {d.get('mismatches')}**. Errors: {d.get('errors')}.",
            f"- Tag sort definition used for the reference: `{d.get('tag_sort_definition')}`",
        ]
        if d.get("stopped_early"):
            lines.append(f"- {d['stopped_early']}")
        for project, check in (d.get("storage_check") or {}).items():
            lines.append(
                f"- Stored values against the generated documents, {project}: {check['documents_checked']} documents "
                f"checked, {check['differences']} different"
            )
        checks = d.get("text_match_check") or []
        if checks:
            same = sum(c["same"] for c in checks)
            lines.append(
                f"- Word search against a regular expression in postgres: {same} of {len(checks)} words give the same"
                " documents"
            )
        for shape, info in (d.get("reference_plans") or {}).items():
            lines.append(
                f"- Reference kind `{shape}`: {info['statements']} statements, {info['plans_checked']} plans checked, "
                f"Top-K scan in {info['top_k_found']}, search index used in {info['index_used']}"
            )
        lines.append("")
        rows = [[name, c["cases"], c["mismatches"]] for name, c in sorted((d.get("by_kind") or {}).items())]
        lines += table(["kind of case (a case can have several)", "cases", "mismatches"], rows)
        if d.get("mismatch_list"):
            lines += ["First mismatches:", ""]
            for m in d["mismatch_list"][:20]:
                lines.append(f"- `{m}`")
            lines.append("")
        if d.get("error_list"):
            lines += ["First errors:", ""]
            for m in d["error_list"][:10]:
                lines.append(f"- `{m}`")
            lines.append("")

    entry, d = step_data(results, "speed")
    if entry:
        lines += ["## Speed", "", "Milliseconds per call of the python query function; 20 runs after 3 warm-up runs.", ""]
        rows = []
        for name, r in (d.get("queries") or {}).items():
            if r.get("status") == "ok":
                rows.append([name, r["median_ms"], r["p95_ms"], r["hits"], r.get("note", "")])
            else:
                rows.append([name, r.get("status"), "", "", r.get("error", "")])
        lines += table(["query", "median (ms)", "p95 (ms)", "hits", "remark"], rows)

    entry, d = step_data(results, "loadtest")
    if entry:
        lines += [
            "## Load test",
            "",
            f"{d.get('clients')} clients at the same time, {d.get('seconds_per_mix')} seconds per mix.",
            "",
        ]
        rows = [
            [name, m.get("requests_per_s"), m.get("median_ms"), m.get("p95_ms"), m.get("errors"), m.get("error_kinds") or ""]
            for name, m in (d.get("mixes") or {}).items()
        ]
        lines += table(["mix", "requests per second", "median (ms)", "p95 (ms)", "errors", "kinds of error"], rows)

    entry, d = step_data(results, "write")
    if entry:
        lines += ["## Write test", ""]
        for run_name, run in (d.get("runs") or {}).items():
            lines += [
                f"Run `{run_name}`: tag added to {run.get('documents_tagged')} documents in **{run.get('add_tag_s')} s**; "
                f"VACUUM {run.get('vacuum_s')} s; tag removed in {run.get('remove_tag_s')} s."
                + (f" Error: {run['error']}" if run.get("error") else ""),
                "",
            ]
            rows = [
                [p["phase"], p["count_tags_search_ms"], p["count_tags_all_ms"], p["tagged"]] for p in run.get("phases", [])
            ]
            lines += table(
                ["moment", "count tags over a search (ms)", "count tags, whole project (ms)", "documents with the test tag"],
                rows,
            )
    return "\n".join(lines)


async def step_report(args, deadline: Deadline, out: dict[str, Any]) -> None:
    results = load_results(args.out)
    path = os.path.join(args.out, "RESULTS.md")
    with open(path, "w") as f:
        f.write(render(results))
    out["file"] = path
    print(f"  wrote {path}")
