"""
Put the results of two or more branches side by side.

    uv run python -m benchmark.short_test.compare <dir-or-results.json> <dir-or-results.json> [...] [--out COMPARE.md]

Writes one Markdown table per step. Ratios are "this branch / first branch": for times below 1 is faster, for
requests per second above 1 is better. In the speed table, "same results" says whether all branches returned the same
number of hits and the same documents (or aggregation rows) for the query.
"""

import argparse
import json
import os
from typing import Any

from benchmark.short_test.report import step_data, table


def ratio(value: Any, base: Any) -> str:
    if not isinstance(value, (int, float)) or not isinstance(base, (int, float)) or not base:
        return ""
    return f"{value / base:.2f}x"


def load(path: str) -> dict:
    if os.path.isdir(path):
        path = os.path.join(path, "results.json")
    with open(path) as f:
        return json.load(f)


def names_of(all_results: list[dict], paths: list[str]) -> list[str]:
    names = [
        r.get("git", {}).get("branch") or os.path.basename(os.path.dirname(os.path.abspath(p)))
        for r, p in zip(all_results, paths)
    ]
    if len(set(names)) < len(names):  # the same branch twice: tell them apart by directory
        names = [
            f"{n} ({os.path.basename(os.path.dirname(os.path.abspath(p)) if p.endswith('.json') else os.path.abspath(p))})"
            for n, p in zip(names, paths)
        ]
    return names


def with_ratios(values: list[Any]) -> list[Any]:
    """value of the first branch, then value and ratio for each other branch"""
    out = [values[0]]
    for v in values[1:]:
        out += [v, ratio(v, values[0])]
    return out


def ratio_header(names: list[str], unit: str) -> list[str]:
    out = [f"{names[0]} {unit}"]
    for n in names[1:]:
        out += [f"{n} {unit}", "ratio"]
    return out


def compare(all_results: list[dict], names: list[str]) -> str:
    lines = ["# Short test: branches side by side", ""]
    rows = []
    for r, n in zip(all_results, names):
        git, db = r.get("git", {}), r.get("database", {})
        rows.append([n, (git.get("commit") or "")[:10], db.get("pg_search"), r.get("docs"), r.get("last_run")])
    lines += table(["branch", "commit", "pg_search", "documents (big project)", "last run (UTC)"], rows)
    lines += ["Ratios are this branch / first branch. For times, below 1 is faster.", ""]

    lines += ["## Steps", ""]
    steps = [
        s
        for s in ["load", "same", "speed", "loadtest", "write", "report"]
        if any(s in r.get("steps", {}) for r in all_results)
    ]
    rows = []
    for s in steps:
        entries = [r.get("steps", {}).get(s, {}) for r in all_results]
        rows.append([s] + [f"{e.get('status', '-')} ({e.get('seconds', '-')} s)" for e in entries])
    lines += table(["step", *names], rows)

    datas = [step_data(r, "load")[1] for r in all_results]
    if any(datas):
        lines += ["## Load", ""]
        rows = [
            [label, *with_ratios([d.get(key) for d in datas])]
            for label, key in [
                ("upload (s)", "upload_s"),
                ("rebuild search indexes (s)", "reindex_s"),
                ("vacuum (s)", "vacuum_s"),
            ]
        ]
        rows.append(
            [
                "size of all partitions (MB)",
                *with_ratios([sum(p["total_mb"] for p in d.get("partitions", [])) or None for d in datas]),
            ]
        )
        lines += table(["", *ratio_header(names, "")], rows)

    datas = [step_data(r, "same")[1] for r in all_results]
    if any(datas):
        lines += ["## Same results (each branch against its own reference)", ""]
        rows = [
            [
                n,
                d.get("cases_run"),
                d.get("mismatches"),
                d.get("errors"),
                d.get("tag_sort_definition"),
                "yes"
                if d and not d.get("mismatches") and not d.get("errors") and not d.get("problems")
                else "NO"
                if d
                else "-",
            ]
            for n, d in zip(names, datas)
        ]
        lines += table(["branch", "cases run", "mismatches", "errors", "tag sort definition", "all the same"], rows)
        kinds = sorted({k for d in datas for k in (d.get("by_kind") or {})})
        rows = []
        for k in kinds:
            cells = [(d.get("by_kind") or {}).get(k) for d in datas]
            rows.append([k] + [f"{c['mismatches']} of {c['cases']}" if c else "-" for c in cells])
        lines += table(["kind of case: mismatches", *names], rows)

    datas = [step_data(r, "speed")[1].get("queries") or {} for r in all_results]
    if any(datas):
        tag_definitions = {step_data(r, "same")[1].get("tag_sort_definition") for r in all_results} - {None}
        lines += ["## Speed (median, ms)", ""]
        queries = list(dict.fromkeys(q for d in datas for q in d))
        rows = []
        for q in queries:
            rs = [d.get(q) or {} for d in datas]
            ok = [r for r in rs if r.get("status") == "ok"]
            if len(ok) < 2:
                same = "-"
            elif len({(r["hits"], r["fingerprint"]) for r in ok}) == 1:
                same = "yes"
            else:
                same = "NO"
                if "tags" in q and "sort" in q and len(tag_definitions) > 1:
                    same = "no (the branches define the tag order differently)"
                elif len({r["hits"] for r in ok}) > 1:
                    same = "NO (hits: " + ", ".join(str(r["hits"]) for r in ok) + ")"
            medians = [r.get("median_ms") if r.get("status") == "ok" else r.get("status", "-") for r in rs]
            rows.append([q, same, *with_ratios(medians)])
        lines += table(["query", "same results", *ratio_header(names, "")], rows)
        lines += ["## Speed (95th percentile, ms)", ""]
        rows = [[q, *with_ratios([(d.get(q) or {}).get("p95_ms") for d in datas])] for q in queries]
        lines += table(["query", *ratio_header(names, "")], rows)

    datas = [step_data(r, "loadtest")[1].get("mixes") or {} for r in all_results]
    if any(datas):
        lines += ["## Load test (32 clients)", "", "For requests per second, a ratio above 1 is better.", ""]
        mixes = list(dict.fromkeys(m for d in datas for m in d))
        rows = []
        for m in mixes:
            for label, key in [("requests per second", "requests_per_s"), ("p95 (ms)", "p95_ms"), ("errors", "errors")]:
                rows.append([m, label, *with_ratios([(d.get(m) or {}).get(key) for d in datas])])
        lines += table(["mix", "", *ratio_header(names, "")], rows)

    datas = [step_data(r, "write")[1].get("runs") or {} for r in all_results]
    if any(datas):
        lines += ["## Write test (tag 10% of the big project)", ""]
        runs = list(dict.fromkeys(run for d in datas for run in d))
        if len(runs) > 1 and "default" in runs:
            runs.remove("default")
        rows = []
        for run in runs:
            # a branch without the label_tables setting has one run, "default": show it next to every run of the others
            rs = [d.get(run) or d.get("default") or {} for d in datas]
            for label, key in [
                ("add the tag (s)", "add_tag_s"),
                ("vacuum (s)", "vacuum_s"),
                ("remove the tag (s)", "remove_tag_s"),
            ]:
                rows.append([run, label, *with_ratios([r.get(key) for r in rs])])
            phases = list(dict.fromkeys(p["phase"] for r in rs for p in r.get("phases", [])))
            for phase in phases:
                for label, key in [
                    ("count tags over a search (ms)", "count_tags_search_ms"),
                    ("count tags, whole project (ms)", "count_tags_all_ms"),
                ]:
                    values = [next((p[key] for p in r.get("phases", []) if p["phase"] == phase), None) for r in rs]
                    rows.append([run, f"{phase}: {label}", *with_ratios(values)])
        lines += table(["run", "", *ratio_header(names, "")], rows)
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("results", nargs="+", help="two or more results.json files (or their directories)")
    parser.add_argument("--out", default=None, help="write the Markdown here (default: print it)")
    args = parser.parse_args()
    all_results = [load(p) for p in args.results]
    text = compare(all_results, names_of(all_results, args.results))
    if args.out:
        with open(args.out, "w") as f:
            f.write(text + "\n")
        print(f"wrote {args.out}")
    else:
        print(text)


if __name__ == "__main__":
    main()
