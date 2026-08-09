#!/usr/bin/env python3
"""Resolve the latest durable Unusual artifacts from the existing builder.

The command is read-only.  It never enqueues or launches Kaggle and never emits
the secret-candidate URL.  Its output is the bounded resolver envelope consumed
by ``scripts/unusual_events_health.py evaluate-bundle``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SHA40_RE = re.compile(r"^[0-9a-f]{40}$")
RUN_MODES = frozenset({"warm", "cold"})
MAX_JSON_BYTES = 8 * 1024 * 1024


class ResolverError(RuntimeError):
    """The durable current builder state cannot yet produce exact evidence."""


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    return _sha256_bytes(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    )


def _load_json_file(path: Path) -> tuple[dict[str, Any], str]:
    if not path.is_file():
        raise ResolverError(f"artifact_missing:{path.name}")
    raw = path.read_bytes()
    if not raw or len(raw) > MAX_JSON_BYTES:
        raise ResolverError(f"artifact_unbounded:{path.name}")
    try:
        value = json.loads(raw)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ResolverError(f"artifact_invalid_json:{path.name}") from exc
    if not isinstance(value, dict):
        raise ResolverError(f"artifact_not_object:{path.name}")
    return value, _sha256_bytes(raw)


def _load_success_receipt(database: Path) -> dict[str, Any]:
    if not database.is_file():
        raise ResolverError("database_missing")
    try:
        with sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=5) as connection:
            row = connection.execute(
                """
                SELECT last_success_receipt_json
                FROM static_site_build_state
                WHERE release_channel='secret_preview'
                """
            ).fetchone()
    except sqlite3.Error as exc:
        raise ResolverError(f"builder_state_unavailable:{exc.__class__.__name__}") from exc
    if not row or not row[0]:
        raise ResolverError("builder_success_receipt_missing")
    try:
        receipt = json.loads(str(row[0]))
    except json.JSONDecodeError as exc:
        raise ResolverError("builder_success_receipt_invalid_json") from exc
    if not isinstance(receipt, dict):
        raise ResolverError("builder_success_receipt_not_object")
    return receipt


def _required_sha(value: Any, label: str) -> str:
    clean = str(value or "").strip().lower()
    if not SHA256_RE.fullmatch(clean):
        raise ResolverError(f"builder_receipt_invalid_sha256:{label}")
    return clean


def resolve_bundle(
    *,
    database: Path,
    artifact_root: Path,
    expect_mode: str | None = None,
    expect_effective_date: str | None = None,
    expect_repo_sha: str | None = None,
    after_run_id: str | None = None,
) -> dict[str, Any]:
    receipt = _load_success_receipt(database)
    if receipt.get("schema_version") != "static_site_success_receipt_v2":
        raise ResolverError("builder_success_receipt_schema_mismatch")
    run_id = str(receipt.get("run_id") or "").strip()
    build_id = str(receipt.get("build_id") or "").strip()
    if not run_id or len(run_id) > 240 or not build_id or len(build_id) > 240:
        raise ResolverError("builder_success_receipt_identity_missing")
    repo_sha = str(receipt.get("repo_sha") or "").strip().lower()
    if not SHA40_RE.fullmatch(repo_sha):
        raise ResolverError("builder_success_receipt_repo_sha_invalid")
    if expect_repo_sha and repo_sha != expect_repo_sha:
        raise ResolverError("builder_repo_sha_pending")
    if after_run_id and run_id == after_run_id:
        raise ResolverError("builder_run_not_advanced")
    mode = str(receipt.get("semantic_cache_mode") or "").strip().lower()
    if mode not in RUN_MODES:
        raise ResolverError("builder_semantic_cache_mode_missing")
    if expect_mode and mode != expect_mode:
        raise ResolverError("builder_semantic_cache_mode_pending")
    effective_date = str(receipt.get("effective_date") or "").strip()
    if expect_effective_date and effective_date != expect_effective_date:
        raise ResolverError("builder_effective_date_pending")
    snapshot_sha = _required_sha(receipt.get("snapshot_sha256"), "snapshot")
    input_fingerprint = _required_sha(
        receipt.get("input_fingerprint"), "input_fingerprint"
    )
    semantic = receipt.get("semantic")
    if not isinstance(semantic, dict):
        raise ResolverError("builder_semantic_receipt_missing")
    if semantic.get("input_fingerprint") != input_fingerprint:
        raise ResolverError("builder_semantic_fingerprint_mismatch")

    paths = {
        "bge_receipt": artifact_root / "static_event_bge_vectors.receipt.json",
        "unusual_manifest": artifact_root / "unusual-events-manifest.json",
        "unusual_cache": artifact_root / "unusual_events_cache.json",
    }
    artifacts: dict[str, dict[str, Any]] = {}
    expected_hashes = {
        "bge_receipt": _required_sha(
            semantic.get("vector_receipt_sha256"), "vector_receipt"
        ),
        "unusual_manifest": _required_sha(
            semantic.get("unusual_events_manifest_sha256")
            or semantic.get("manifest_sha256"),
            "unusual_manifest",
        ),
        "unusual_cache": _required_sha(
            semantic.get("unusual_cache_sha256"), "unusual_cache"
        ),
    }
    for key, path in paths.items():
        payload, digest = _load_json_file(path)
        if digest != expected_hashes[key]:
            raise ResolverError(f"artifact_hash_mismatch:{key}")
        artifacts[key] = {"sha256": digest, "payload": payload}

    if receipt.get("snapshot_sha256") != snapshot_sha:
        raise ResolverError("builder_snapshot_identity_mismatch")
    artifacts["builder_receipt"] = {
        "sha256": _canonical_sha256(receipt),
        "payload": receipt,
    }
    return {
        "schema_version": "unusual-events-health-resolver-v1",
        "request_id": run_id,
        "run_mode": mode,
        "artifacts": artifacts,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=os.getenv("DB_PATH") or "/data/db.sqlite")
    parser.add_argument(
        "--artifact-root",
        default=os.getenv("STATIC_SITE_ARTIFACT_ROOT") or "/data/static_site_builder",
    )
    parser.add_argument("--expect-mode", choices=sorted(RUN_MODES))
    parser.add_argument("--expect-effective-date", default="")
    parser.add_argument("--expect-repo-sha", default="")
    parser.add_argument("--after-run-id", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        bundle = resolve_bundle(
            database=Path(args.db),
            artifact_root=Path(args.artifact_root),
            expect_mode=args.expect_mode,
            expect_effective_date=args.expect_effective_date or None,
            expect_repo_sha=args.expect_repo_sha or None,
            after_run_id=args.after_run_id or None,
        )
    except ResolverError as exc:
        print(f"unusual health evidence pending: {exc}", file=sys.stderr)
        return 75
    print(
        json.dumps(bundle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
