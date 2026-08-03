#!/usr/bin/env python3
"""Build source-bound gastronomy review, manifest and batch overlay artifacts.

This command is provider-free and read-only with respect to SQLite. It consumes
high-recall ``gastronomy`` candidates already produced by the common collection
BGE pass. Exact membership remains empty until the owner decision store covers
every candidate family with current source-bound evidence.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SITE_SCRIPTS = ROOT / "site" / "scripts"
if str(SITE_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SITE_SCRIPTS))

import static_collection_batch as batch_module
import static_collection_gastronomy as gastronomy
import static_collection_product_snapshot as product_snapshot_module

DEFAULT_DECISION_STORE = (
    ROOT / "docs" / "review-data" / "static_collections_gastronomy_decisions_v1.json"
)


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: top-level JSON must be an object")
    return value


def atomic_write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def run(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db).expanduser().resolve()
    batch_path = Path(args.collection_batch).expanduser().resolve()
    store_path = Path(args.decision_store).expanduser().resolve()
    if not db_path.is_file():
        raise ValueError(f"SQLite snapshot is missing: {db_path}")
    if not batch_path.is_file():
        raise ValueError(f"collection batch is missing: {batch_path}")
    if not store_path.is_file():
        raise ValueError(f"gastronomy decision store is missing: {store_path}")

    batch = load_object(batch_path)
    store = load_object(store_path)
    uri = f"file:{db_path.as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as con:
        quick_check = "\n".join(str(row[0]) for row in con.execute("PRAGMA quick_check"))
        if quick_check != "ok":
            raise ValueError(f"SQLite quick_check failed: {quick_check}")
        events, _decisions, sources = product_snapshot_module.load_snapshot_inputs(
            con,
            current_date=args.current_date,
        )

    candidate_ids = gastronomy.candidate_event_ids(batch)
    queue = gastronomy.build_review_queue(
        events,
        source_records_by_event=sources,
        candidate_ids=candidate_ids,
        current_date=args.current_date,
        source_scope=args.source_scope,
        batch_sha256=str(batch.get("batch_sha256") or "") or None,
        evidence_trust_scope=args.evidence_trust_scope,
    )
    manifest = gastronomy.build_manifest(queue, store)
    manifest_validation = gastronomy.validate_manifest(manifest)
    if not manifest_validation["valid"]:
        raise ValueError(
            "gastronomy manifest validation failed: "
            + "; ".join(manifest_validation["errors"])
        )
    overlaid_batch = gastronomy.overlay_collection_batch(batch, manifest)
    batch_validation = batch_module.validate_collection_batch(
        overlaid_batch,
        catalog_item_ids=[int(event["id"]) for event in events],
        require_compute=True,
    )
    if not batch_validation["valid"]:
        raise ValueError(
            "overlaid collection batch validation failed: "
            + "; ".join(batch_validation["errors"])
        )
    product_snapshot = gastronomy.build_product_quality_snapshot(manifest)

    atomic_write(Path(args.review_queue_output), queue)
    atomic_write(Path(args.manifest_output), manifest)
    atomic_write(Path(args.product_snapshot_output), product_snapshot)
    if args.batch_output:
        atomic_write(Path(args.batch_output), overlaid_batch)

    return {
        "status": manifest["extraction_status"],
        "policy_version": gastronomy.POLICY_VERSION,
        "provider_calls": 0,
        "db_quick_check": quick_check,
        "catalog_event_count": len(events),
        "candidate_event_count": manifest["candidate_event_count"],
        "candidate_family_count": manifest["candidate_family_count"],
        "reviewed_family_count": manifest["reviewed_family_count"],
        "accepted_future_family_count": manifest["accepted_future_family_count"],
        "catalog_state": manifest["catalog_state"],
        "publication_status": manifest["publication_status"],
        "queue_sha256": queue["queue_sha256"],
        "manifest_sha256": manifest["manifest_sha256"],
        "collection_batch_sha256": overlaid_batch["batch_sha256"],
        "product_input_fingerprint": product_snapshot["input_fingerprint"],
        "outputs": {
            "review_queue": str(Path(args.review_queue_output)),
            "manifest": str(Path(args.manifest_output)),
            "product_snapshot": str(Path(args.product_snapshot_output)),
            "collection_batch": str(Path(args.batch_output)) if args.batch_output else None,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--collection-batch", required=True)
    parser.add_argument("--decision-store", default=str(DEFAULT_DECISION_STORE))
    parser.add_argument("--current-date", required=True)
    parser.add_argument("--source-scope", default="static-site-builder-export")
    parser.add_argument(
        "--evidence-trust-scope",
        choices=("all", "trusted"),
        default="all",
    )
    parser.add_argument("--review-queue-output", required=True)
    parser.add_argument("--manifest-output", required=True)
    parser.add_argument("--product-snapshot-output", required=True)
    parser.add_argument("--batch-output")
    return parser.parse_args()


def main() -> int:
    result = run(parse_args())
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
