#!/usr/bin/env python3
"""Advisory audit of requirement-bearing documentation left outside main.

The script intentionally does not choose the newest branch as canonical and
never merges or edits refs. It produces an inventory for manual semantic review.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import pathlib
import re
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from typing import Iterable

DOC_SUFFIXES = {".md", ".yml", ".yaml", ".json"}
REQUIREMENT_MARKERS = (
    "to-be",
    "to be",
    "requirements",
    "требован",
    "strategy",
    "стратег",
    "architecture",
    "архитект",
    "contract",
    "контракт",
    "product decision",
    "owner-corrected",
    "accepted",
    "release plan",
    "target",
    "целев",
)
RESEARCH_MARKERS = (
    "research",
    "исследован",
    "workbench",
    "consultation",
    "lab",
    "prototype",
    "эксперимент",
)
DATE_RE = re.compile(r"\b20\d{2}[-./]\d{2}[-./]\d{2}\b")
HEADING_RE = re.compile(r"^#\s+(.+?)\s*$", re.MULTILINE)
STATUS_RE = re.compile(
    r"(?im)^(?:>|\s*)?\s*(?:\*\*)?(?:status|статус|решение|вердикт)(?:\*\*)?\s*:\s*(.+)$"
)

# Manual semantic dispositions from the 2026-08-05 review. The audit still
# inventories these branches so drift remains visible.
MANUAL_DISPOSITIONS: dict[str, tuple[str, str]] = {
    "origin/agent/static-site-general-follow-up-audit-20260804": (
        "ported",
        "Latest owner-corrected focus/profile handoff; anonymous server feedback and silent anonymous Auth are forbidden.",
    ),
    "origin/docs/focus-group-release-control-20260802": (
        "superseded",
        "Anonymous-first focus model superseded by the later explicit-auth decision.",
    ),
    "origin/docs/focus-group-release-control-v5-20260803": (
        "superseded",
        "Anonymous-first focus model superseded by the later explicit-auth decision.",
    ),
    "origin/agent/launch-readiness-dashboard-20260804": (
        "conflict_blocked",
        "Dashboard contains stale anonymous-first assumptions and must be regenerated against current main.",
    ),
    "origin/docs/p13n-transport-profile-20260804": (
        "ported",
        "Staged zero-backend/profile architecture and corrected Favorites/hidden/profile boundaries.",
    ),
    "origin/agent/yandex-dependency-resilience-docs-20260803": (
        "historical_donor",
        "Capability/SOR/ack evidence is useful, but the older storage ownership model is not canonical wholesale.",
    ),
    "origin/agent/personalization-implementation-contract-20260802": (
        "ported_partial",
        "Only missing extension documents were ported; main core target documents were not overwritten.",
    ),
    "origin/feature/personalization-legal-release-gate-20260802": (
        "ported",
        "Activation/legal release gate ported as a staged requirement, not legal approval.",
    ),
    "origin/agent/hero-talk-chain-research-20260803": (
        "ported",
        "Accepted chain-first Hero Talk package ported without runtime claims.",
    ),
    "origin/agent/standard-user-onboarding-strategy": (
        "already_in_main",
        "Owner-corrected onboarding v0.4 is already canonical in main.",
    ),
    "origin/agent/keyboard-navigation-v8-product-evidence-20260804": (
        "ported_docs_only",
        "Owner-corrected product/test documents ported; code/workflow remains branch evidence.",
    ),
    "origin/agent/volunteer-recruitment-contract-20260804": (
        "ported_docs_only",
        "Volunteer product/test/handoff contract ported; runtime remains unimplemented.",
    ),
    "origin/feature/event-reminders-calendar-test-design-20260802": (
        "ported_docs_only",
        "Favorites/reminders/Push target and test design ported without workflow/runtime claims.",
    ),
    "origin/feature/editorial-collections-research-lab": (
        "research_evidence",
        "Owner acceptance and visual comparison are still required.",
    ),
    "origin/agent/editorial-style-research": (
        "research_evidence",
        "Living workbench; final editorial standard is not selected.",
    ),
    "origin/agent/static-collections-gastronomy-data-prep": (
        "implementation_only",
        "Owner decision store is not approved and publication remains blocked.",
    ),
    "origin/agent/prelaunch-landing-20260803": (
        "implementation_only",
        "Prelaunch candidate/runtime branch, not a general TO-BE source of truth.",
    ),
    "origin/feature/static-launch-tile-mosaic-20260803": (
        "implementation_only",
        "Noindex visual candidate/lab.",
    ),
    "origin/feature/tile-mosaic-material-generator-20260803": (
        "implementation_only",
        "Visual generator evidence, not product requirements.",
    ),
    "origin/agent/static-collections-quality/smart-update-facts-v3-handoff": (
        "superseded",
        "Later selective implementation entered main through the current-main integration track.",
    ),
    "origin/agent/static-release/checklist-cdn-social": (
        "historical_donor",
        "Large diverged umbrella branch; only source-faithful slices may be reused.",
    ),
    "origin/integration/static-site-medallions-release-20260712": (
        "historical_donor",
        "Implementation readiness/visual evidence; current product docs in main govern semantics.",
    ),
}


@dataclass
class ChangedDoc:
    status: str
    path: str
    main_state: str
    heading: str | None = None
    status_hint: str | None = None
    date_hints: list[str] = field(default_factory=list)
    requirement_markers: list[str] = field(default_factory=list)
    research_markers: list[str] = field(default_factory=list)
    branch_blob: str | None = None
    main_blob: str | None = None


@dataclass
class BranchAudit:
    branch: str
    head_sha: str
    committed_at: str
    merge_base: str | None
    ahead_commits: int | None
    disposition: str
    disposition_note: str
    requirement_candidate: bool
    changed_docs: list[ChangedDoc] = field(default_factory=list)
    error: str | None = None


def run_git(repo: pathlib.Path, *args: str, check: bool = True) -> str:
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if check and proc.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {proc.stderr.strip()}")
    return proc.stdout


def blob_sha(repo: pathlib.Path, ref: str, path: str) -> str | None:
    proc = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", f"{ref}:{path}"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    value = proc.stdout.strip()
    return value if proc.returncode == 0 and value else None


def show_text(repo: pathlib.Path, ref: str, path: str, max_bytes: int = 128_000) -> str:
    proc = subprocess.run(
        ["git", "-C", str(repo), "show", f"{ref}:{path}"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    if proc.returncode != 0:
        return ""
    return proc.stdout[:max_bytes].decode("utf-8", errors="replace")


def analyze_text(text: str) -> tuple[str | None, str | None, list[str], list[str], list[str]]:
    sample = text[:32_000]
    lower = sample.lower()
    heading_match = HEADING_RE.search(sample)
    status_match = STATUS_RE.search(sample[:8_000])
    dates = sorted(set(DATE_RE.findall(sample[:8_000])))[:8]
    req = [marker for marker in REQUIREMENT_MARKERS if marker in lower]
    research = [marker for marker in RESEARCH_MARKERS if marker in lower]
    return (
        heading_match.group(1).strip() if heading_match else None,
        status_match.group(1).strip()[:240] if status_match else None,
        dates,
        req,
        research,
    )


def parse_name_status(lines: Iterable[str]) -> Iterable[tuple[str, str]]:
    for raw in lines:
        raw = raw.strip()
        if not raw:
            continue
        parts = raw.split("\t")
        code = parts[0]
        if code.startswith("R") and len(parts) >= 3:
            yield code, parts[2]
        elif len(parts) >= 2:
            yield code, parts[1]


def branch_refs(repo: pathlib.Path) -> list[tuple[str, str, str]]:
    fmt = "%(refname:short)|%(objectname)|%(committerdate:iso8601-strict)"
    rows = run_git(repo, "for-each-ref", f"--format={fmt}", "refs/remotes/origin").splitlines()
    result: list[tuple[str, str, str]] = []
    for row in rows:
        try:
            name, sha, date = row.split("|", 2)
        except ValueError:
            continue
        if name in {"origin", "origin/HEAD", "origin/main"}:
            continue
        result.append((name, sha, date))
    return sorted(result)


def audit_branch(repo: pathlib.Path, main_ref: str, branch: str, sha: str, date: str) -> BranchAudit:
    manual_status, manual_note = MANUAL_DISPOSITIONS.get(
        branch, ("unclassified_review_required", "No manual semantic disposition recorded.")
    )
    try:
        merge_base = run_git(repo, "merge-base", main_ref, branch).strip()
        ahead_raw = run_git(repo, "rev-list", "--count", f"{merge_base}..{branch}").strip()
        ahead = int(ahead_raw)
        diff = run_git(
            repo,
            "diff",
            "--name-status",
            "--find-renames",
            f"{merge_base}..{branch}",
            "--",
            "docs",
        )
        docs: list[ChangedDoc] = []
        candidate = False
        for status, path in parse_name_status(diff.splitlines()):
            if pathlib.PurePosixPath(path).suffix.lower() not in DOC_SUFFIXES:
                continue
            branch_sha = None if status.startswith("D") else blob_sha(repo, branch, path)
            main_sha = blob_sha(repo, main_ref, path)
            if status.startswith("D"):
                main_state = "branch_deleted"
                text = ""
            elif main_sha is None:
                main_state = "absent_from_main"
                text = show_text(repo, branch, path)
            elif branch_sha == main_sha:
                main_state = "identical_to_main"
                text = show_text(repo, branch, path)
            else:
                main_state = "differs_from_main"
                text = show_text(repo, branch, path)
            heading, status_hint, dates, req, research = analyze_text(text)
            path_lower = path.lower()
            path_marker = any(marker.replace(" ", "-") in path_lower for marker in REQUIREMENT_MARKERS)
            doc_candidate = bool(req or path_marker or status_hint)
            candidate = candidate or doc_candidate
            docs.append(
                ChangedDoc(
                    status=status,
                    path=path,
                    main_state=main_state,
                    heading=heading,
                    status_hint=status_hint,
                    date_hints=dates,
                    requirement_markers=req,
                    research_markers=research,
                    branch_blob=branch_sha,
                    main_blob=main_sha,
                )
            )
        return BranchAudit(
            branch=branch,
            head_sha=sha,
            committed_at=date,
            merge_base=merge_base,
            ahead_commits=ahead,
            disposition=manual_status,
            disposition_note=manual_note,
            requirement_candidate=candidate,
            changed_docs=docs,
        )
    except Exception as exc:  # fail this branch closed but keep the inventory
        return BranchAudit(
            branch=branch,
            head_sha=sha,
            committed_at=date,
            merge_base=None,
            ahead_commits=None,
            disposition=manual_status,
            disposition_note=manual_note,
            requirement_candidate=True,
            error=str(exc),
        )


def render_markdown(main_ref: str, generated_at: str, audits: list[BranchAudit]) -> str:
    candidates = [a for a in audits if a.requirement_candidate or a.disposition != "unclassified_review_required"]
    unclassified = [a for a in candidates if a.disposition == "unclassified_review_required"]
    lines = [
        "# Advisory audit TO-BE-документации в remote branches",
        "",
        f"> Generated: `{generated_at}`  ",
        f"> Main ref: `{main_ref}`  ",
        "> Скрипт не выбирает победителя и не выполняет merge.",
        "",
        "## Summary",
        "",
        f"- remote branches scanned: **{len(audits)}**",
        f"- requirement-like/manual branches: **{len(candidates)}**",
        f"- unclassified review required: **{len(unclassified)}**",
        f"- branch audit errors: **{sum(bool(a.error) for a in audits)}**",
        "",
        "## Branch ledger",
        "",
        "| Branch | Disposition | Ahead | Requirement-like docs | Note |",
        "|---|---|---:|---:|---|",
    ]
    for audit in candidates:
        req_docs = sum(bool(d.requirement_markers or d.status_hint) for d in audit.changed_docs)
        note = audit.disposition_note.replace("|", "\\|")
        lines.append(
            f"| `{audit.branch}` | `{audit.disposition}` | {audit.ahead_commits if audit.ahead_commits is not None else 'ERR'} | {req_docs} | {note} |"
        )
    lines.extend(["", "## Unclassified requirement-like branches", ""])
    if not unclassified:
        lines.append("None in this snapshot.")
    for audit in unclassified:
        lines.extend([f"### `{audit.branch}`", ""])
        if audit.error:
            lines.append(f"Audit error: `{audit.error}`")
            lines.append("")
        lines.append("| State | Path | Heading/status | Markers |")
        lines.append("|---|---|---|---|")
        for doc in audit.changed_docs:
            if not (doc.requirement_markers or doc.status_hint):
                continue
            title = (doc.heading or doc.status_hint or "—").replace("|", "\\|")
            markers = ", ".join(doc.requirement_markers[:6]).replace("|", "\\|")
            lines.append(f"| `{doc.main_state}` | `{doc.path}` | {title} | {markers or '—'} |")
        lines.append("")
    lines.extend(
        [
            "## Interpretation rules",
            "",
            "- Newer commit date is a search hint, not precedence.",
            "- Research/lab/prototype markers do not become accepted requirements automatically.",
            "- A path absent from main requires semantic review, not automatic copy.",
            "- A path differing from main requires owner-decision and current-runtime comparison.",
            "- `unclassified_review_required` prevents a claim of complete consolidation.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--main-ref", default="origin/main")
    parser.add_argument("--output-md", default="artifacts/docs/to-be-documentation-audit.md")
    parser.add_argument("--output-json", default="artifacts/docs/to-be-documentation-audit.json")
    args = parser.parse_args()

    repo = pathlib.Path(args.repo_root).resolve()
    if not (repo / ".git").exists():
        print(f"not a git checkout: {repo}", file=sys.stderr)
        return 2

    generated_at = dt.datetime.now(dt.timezone.utc).isoformat()
    audits = [audit_branch(repo, args.main_ref, *row) for row in branch_refs(repo)]

    payload = {
        "schema_version": 1,
        "generated_at": generated_at,
        "main_ref": args.main_ref,
        "main_sha": run_git(repo, "rev-parse", args.main_ref).strip(),
        "manual_ledger_sha256": hashlib.sha256(
            json.dumps(MANUAL_DISPOSITIONS, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "branches": [asdict(item) for item in audits],
    }

    md_path = repo / args.output_md
    json_path = repo / args.output_json
    md_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(render_markdown(args.main_ref, generated_at, audits), encoding="utf-8")
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "branches_scanned": len(audits),
                "requirement_candidates": sum(a.requirement_candidate for a in audits),
                "unclassified": sum(
                    a.requirement_candidate and a.disposition == "unclassified_review_required" for a in audits
                ),
                "errors": sum(bool(a.error) for a in audits),
                "output_md": str(md_path.relative_to(repo)),
                "output_json": str(json_path.relative_to(repo)),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
