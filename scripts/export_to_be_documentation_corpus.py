#!/usr/bin/env python3
"""Export the full text corpus needed for semantic review of TO-BE branch docs.

The existing branch audit is intentionally metadata-only. This companion exporter
materializes every non-identical requirement-like document blob and its main
counterpart so a reviewer can resolve precedence and contradictions without
blindly treating commit dates as truth.
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import difflib
import hashlib
import json
import pathlib
import subprocess
from typing import Any

MAX_BLOB_BYTES = 512_000


def run_git(repo: pathlib.Path, *args: str, check: bool = True) -> bytes:
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if check and proc.returncode != 0:
        stderr = proc.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"git {' '.join(args)} failed: {stderr}")
    return proc.stdout


def read_blob(repo: pathlib.Path, sha: str | None) -> tuple[str, bool, int]:
    if not sha:
        return "", False, 0
    raw = run_git(repo, "cat-file", "blob", sha)
    original_size = len(raw)
    truncated = original_size > MAX_BLOB_BYTES
    raw = raw[:MAX_BLOB_BYTES]
    return raw.decode("utf-8", errors="replace"), truncated, original_size


def stable_json(path: pathlib.Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def safe_text(path: pathlib.Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", errors="replace")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=pathlib.Path, default=pathlib.Path("."))
    parser.add_argument("--audit-json", type=pathlib.Path, required=True)
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    args = parser.parse_args()

    repo = args.repo.resolve()
    audit = json.loads(args.audit_json.read_text(encoding="utf-8"))
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)

    entries: list[dict[str, Any]] = []
    blob_index: dict[str, dict[str, Any]] = {}
    diff_index: dict[str, dict[str, Any]] = {}
    path_index: dict[str, list[int]] = collections.defaultdict(list)
    branch_index: dict[str, list[int]] = collections.defaultdict(list)

    candidate_branches = [
        branch
        for branch in audit.get("branches", [])
        if branch.get("requirement_candidate")
        and branch.get("disposition") == "unclassified_review_required"
    ]

    for branch in candidate_branches:
        for doc in branch.get("changed_docs", []):
            state = doc.get("main_state")
            if state == "identical_to_main":
                continue
            branch_sha = doc.get("branch_blob")
            main_sha = doc.get("main_blob")
            branch_text, branch_truncated, branch_size = read_blob(repo, branch_sha)
            main_text, main_truncated, main_size = read_blob(repo, main_sha)

            for role, sha, text, truncated, size in (
                ("branch", branch_sha, branch_text, branch_truncated, branch_size),
                ("main", main_sha, main_text, main_truncated, main_size),
            ):
                if not sha or sha in blob_index:
                    continue
                rel = pathlib.PurePosixPath("blobs") / f"{sha}.txt"
                safe_text(out / rel, text)
                blob_index[sha] = {
                    "sha": sha,
                    "role_first_seen": role,
                    "file": str(rel),
                    "original_bytes": size,
                    "truncated": truncated,
                    "sha256_exported_text": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                }

            pair_key = hashlib.sha256(f"{main_sha or '-'}->{branch_sha or '-'}".encode()).hexdigest()[:24]
            if pair_key not in diff_index:
                diff_lines = difflib.unified_diff(
                    main_text.splitlines(keepends=True),
                    branch_text.splitlines(keepends=True),
                    fromfile=f"main:{main_sha or 'absent'}",
                    tofile=f"branch:{branch_sha or 'deleted'}",
                    n=12,
                )
                diff_text = "".join(diff_lines)
                rel = pathlib.PurePosixPath("diffs") / f"{pair_key}.patch"
                safe_text(out / rel, diff_text)
                diff_index[pair_key] = {
                    "key": pair_key,
                    "main_blob": main_sha,
                    "branch_blob": branch_sha,
                    "file": str(rel),
                    "sha256": hashlib.sha256(diff_text.encode("utf-8")).hexdigest(),
                }

            entry = {
                "branch": branch.get("branch"),
                "head_sha": branch.get("head_sha"),
                "committed_at": branch.get("committed_at"),
                "merge_base": branch.get("merge_base"),
                "ahead_commits": branch.get("ahead_commits"),
                "path": doc.get("path"),
                "status": doc.get("status"),
                "main_state": state,
                "heading": doc.get("heading"),
                "status_hint": doc.get("status_hint"),
                "date_hints": doc.get("date_hints") or [],
                "requirement_markers": doc.get("requirement_markers") or [],
                "research_markers": doc.get("research_markers") or [],
                "branch_blob": branch_sha,
                "main_blob": main_sha,
                "branch_blob_file": blob_index.get(branch_sha, {}).get("file") if branch_sha else None,
                "main_blob_file": blob_index.get(main_sha, {}).get("file") if main_sha else None,
                "diff_key": pair_key,
                "diff_file": diff_index[pair_key]["file"],
            }
            index = len(entries)
            entries.append(entry)
            path_index[str(doc.get("path"))].append(index)
            branch_index[str(branch.get("branch"))].append(index)

    blob_occurrences: dict[str, list[int]] = collections.defaultdict(list)
    for idx, entry in enumerate(entries):
        if entry.get("branch_blob"):
            blob_occurrences[entry["branch_blob"]].append(idx)

    clusters = []
    for sha, indexes in sorted(blob_occurrences.items(), key=lambda item: (-len(item[1]), item[0])):
        clusters.append(
            {
                "branch_blob": sha,
                "occurrences": len(indexes),
                "paths": sorted({entries[i]["path"] for i in indexes}),
                "branches": sorted({entries[i]["branch"] for i in indexes}),
                "newest_committed_at": max((entries[i]["committed_at"] or "") for i in indexes),
                "entry_indexes": indexes,
            }
        )

    generated_at = dt.datetime.now(dt.timezone.utc).isoformat()
    manifest = {
        "schema_version": 1,
        "generated_at": generated_at,
        "source_audit_generated_at": audit.get("generated_at"),
        "source_main_sha": audit.get("main_sha"),
        "candidate_branches": len(candidate_branches),
        "entries": len(entries),
        "unique_paths": len(path_index),
        "unique_branch_blobs": len(blob_occurrences),
        "unique_exported_blobs": len(blob_index),
        "unique_diffs": len(diff_index),
        "max_blob_bytes": MAX_BLOB_BYTES,
    }

    stable_json(out / "manifest.json", manifest)
    stable_json(out / "entries.json", entries)
    stable_json(out / "blob-index.json", blob_index)
    stable_json(out / "diff-index.json", diff_index)
    stable_json(out / "path-index.json", path_index)
    stable_json(out / "branch-index.json", branch_index)
    stable_json(out / "blob-clusters.json", clusters)

    overview = [
        "# TO-BE semantic review corpus",
        "",
        f"> Generated: `{generated_at}`  ",
        f"> Source audit main SHA: `{audit.get('main_sha')}`",
        "",
        "This artifact is evidence for semantic review. It does not select a canonical requirement automatically.",
        "",
        "## Counts",
        "",
        f"- unclassified requirement-like branches: **{len(candidate_branches)}**",
        f"- non-identical document occurrences: **{len(entries)}**",
        f"- unique paths: **{len(path_index)}**",
        f"- unique branch blobs: **{len(blob_occurrences)}**",
        f"- unique exported branch/main blobs: **{len(blob_index)}**",
        f"- unique main→branch diffs: **{len(diff_index)}**",
        "",
        "Review `entries.json`, then open the referenced `blobs/*.txt` and `diffs/*.patch` files.",
        "Commit dates are hints only; owner-corrected decisions and accepted canonical documents take precedence.",
        "",
    ]
    safe_text(out / "README.md", "\n".join(overview))
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
