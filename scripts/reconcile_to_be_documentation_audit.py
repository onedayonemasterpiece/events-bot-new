#!/usr/bin/env python3
"""Apply the reviewed TO-BE branch/path disposition ledger to a raw audit.

Unlike the advisory scanner, this command is a release/documentation gate. It
fails when a new requirement-bearing branch or path has no explicit semantic
verdict. It never merges refs and never selects by commit date alone.
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import gzip
import json
import pathlib
import sys
from typing import Any

REQUIREMENT_PATH_HINTS = (
    "requirement",
    "strategy",
    "architecture",
    "contract",
    "to-be",
    "release-plan",
)


def is_requirement_doc(doc: dict[str, Any]) -> bool:
    path = str(doc.get("path") or "").lower()
    return bool(
        doc.get("requirement_markers")
        or doc.get("status_hint")
        or any(hint in path for hint in REQUIREMENT_PATH_HINTS)
    )


def stable_json(path: pathlib.Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def render_markdown(result: dict[str, Any]) -> str:
    summary = result["summary"]
    lines = [
        "# Resolved TO-BE branch-to-main audit",
        "",
        f"> Generated: `{result['generated_at']}`  ",
        f"> Raw audit main SHA: `{result.get('raw_audit_main_sha')}`  ",
        f"> Ledger: `{result.get('ledger_id')}`",
        "",
        "## Gate result",
        "",
        f"- requirement/manual branches: **{summary['branches']}**",
        f"- requirement-like paths observed: **{summary['paths_observed']}**",
        f"- unresolved branches: **{summary['unresolved_branches']}**",
        f"- unresolved paths: **{summary['unresolved_paths']}**",
        f"- branch head drift warnings: **{summary['head_drift_warnings']}**",
        "",
        "## Dispositions",
        "",
        "| Verdict | Branches |",
        "|---|---:|",
    ]
    for verdict, count in sorted(summary["branch_dispositions"].items()):
        lines.append(f"| `{verdict}` | {count} |")
    if result.get("head_drift"):
        lines.extend(["", "## Head drift warnings", ""])
        for item in result["head_drift"]:
            lines.append(
                f"- `{item['branch']}`: reviewed `{item['reviewed_head_sha']}`, current `{item['current_head_sha']}`; policy `{item['policy']}`."
            )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "A passing result means every requirement-bearing remote branch/path in this snapshot has an explicit canonical, ported, superseded, evidence-only, backlog-only or not-accepted verdict. It does not claim that every historical file was copied into main or that all described runtime work is implemented.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit-json", type=pathlib.Path, required=True)
    parser.add_argument("--ledger-json", type=pathlib.Path, required=True)
    parser.add_argument("--output-json", type=pathlib.Path, required=True)
    parser.add_argument("--output-md", type=pathlib.Path, required=True)
    args = parser.parse_args()

    audit = json.loads(args.audit_json.read_text(encoding="utf-8"))
    ledger_bytes = args.ledger_json.read_bytes()
    if args.ledger_json.suffix == ".gz" or ledger_bytes[:2] == b"\x1f\x8b":
        ledger_bytes = gzip.decompress(ledger_bytes)
    ledger = json.loads(ledger_bytes.decode("utf-8"))
    branch_ledger = {
        name: code
        for code, names in (ledger.get("branch_dispositions_by_code") or {}).items()
        for name in names
    }
    path_ledger = {
        path: code
        for code, paths in (ledger.get("path_decisions_by_code") or {}).items()
        for path in paths
    }
    branch_verdict_codes = ledger.get("branch_verdict_codes") or {}
    path_status_codes = ledger.get("path_status_codes") or {}
    reviewed_branch_heads = ledger.get("reviewed_branch_heads") or {}
    path_notes = ledger.get("path_notes") or {}
    canonical_paths = set(ledger.get("canonical_paths_added_or_updated") or [])

    candidates = [
        branch
        for branch in audit.get("branches", [])
        if branch.get("requirement_candidate")
        or branch.get("disposition") != "unclassified_review_required"
    ]

    unresolved_branches: list[str] = []
    unresolved_paths: list[dict[str, str]] = []
    head_drift: list[dict[str, str]] = []
    resolved_branches: list[dict[str, Any]] = []
    observed_paths: set[str] = set()

    for branch in candidates:
        name = str(branch.get("branch") or "")
        reviewed_code = branch_ledger.get(name)
        if reviewed_code is None:
            unresolved_branches.append(name)
            continue
        disposition = branch_verdict_codes.get(reviewed_code)
        if not disposition:
            unresolved_branches.append(name)
            continue

        reviewed_head = reviewed_branch_heads.get(name)
        current_head = branch.get("head_sha")
        if reviewed_head and current_head and reviewed_head != current_head:
            policy = "current_consolidation_branch" if disposition == "current_consolidation_pr" else "exact_review_required"
            head_drift.append(
                {
                    "branch": name,
                    "reviewed_head_sha": reviewed_head,
                    "current_head_sha": current_head,
                    "policy": policy,
                }
            )
            if policy == "exact_review_required":
                unresolved_branches.append(name)
                continue

        docs: list[dict[str, Any]] = []
        for doc in branch.get("changed_docs", []):
            if not is_requirement_doc(doc):
                continue
            path = str(doc.get("path") or "")
            observed_paths.add(path)
            decision_code = path_ledger.get(path)
            if decision_code is None and path not in canonical_paths:
                unresolved_paths.append({"branch": name, "path": path})
                continue
            status = path_status_codes.get(decision_code) if decision_code is not None else "canonical_added_or_updated"
            if decision_code is not None and not status:
                unresolved_paths.append({"branch": name, "path": path})
                continue
            docs.append(
                {
                    "path": path,
                    "main_state": doc.get("main_state"),
                    "branch_blob": doc.get("branch_blob"),
                    "main_blob": doc.get("main_blob"),
                    "decision": {
                        "status": status,
                        "rationale": path_notes.get(path) or "Reviewed in the branch/path disposition ledger.",
                    },
                }
            )

        resolved_branches.append(
            {
                "branch": name,
                "head_sha": current_head,
                "committed_at": branch.get("committed_at"),
                "ahead_commits": branch.get("ahead_commits"),
                "disposition": disposition,
                "rationale": "Reviewed in the branch/path disposition ledger; see the human conflict summary for semantic precedence.",
                "requirement_docs": docs,
            }
        )

    unresolved_branches = sorted(set(unresolved_branches))
    unresolved_paths = [
        {"branch": branch, "path": path}
        for branch, path in sorted({(item["branch"], item["path"]) for item in unresolved_paths})
    ]
    disposition_counts = collections.Counter(
        branch.get("disposition") for branch in resolved_branches if branch.get("disposition")
    )
    generated_at = dt.datetime.now(dt.timezone.utc).isoformat()
    result = {
        "schema_version": 1,
        "generated_at": generated_at,
        "raw_audit_generated_at": audit.get("generated_at"),
        "raw_audit_main_sha": audit.get("main_sha"),
        "ledger_id": ledger.get("ledger_id"),
        "summary": {
            "branches": len(candidates),
            "paths_observed": len(observed_paths),
            "unresolved_branches": len(unresolved_branches),
            "unresolved_paths": len(unresolved_paths),
            "head_drift_warnings": len(head_drift),
            "branch_dispositions": dict(sorted(disposition_counts.items())),
        },
        "unresolved_branches": unresolved_branches,
        "unresolved_paths": unresolved_paths,
        "head_drift": head_drift,
        "branches": resolved_branches,
    }

    stable_json(args.output_json, result)
    args.output_md.parent.mkdir(parents=True, exist_ok=True)
    args.output_md.write_text(render_markdown(result), encoding="utf-8")

    if unresolved_branches or unresolved_paths:
        print(
            json.dumps(
                {
                    "status": "FAIL",
                    "unresolved_branches": len(unresolved_branches),
                    "unresolved_paths": len(unresolved_paths),
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2

    print(json.dumps({"status": "PASS", **result["summary"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
