#!/usr/bin/env python3
"""Dry-run by default. Only the two documented disposable input families."""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from kaggle_dataset_retention import KaggleRetentionStore, cleanup_inputs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner", default=os.getenv("KAGGLE_USERNAME", ""))
    parser.add_argument("--database", help="Canonical project DB: protect active/recoverable handoffs")
    parser.add_argument("--min-age-hours", type=int, default=24)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--approved-refs", help="Optional JSON allowlist from a reviewed audit")
    parser.add_argument("--report", required=True, help="Managed artifact JSON; .jsonl contains durable mutation receipts")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()
    report = Path(args.report)
    report.parent.mkdir(parents=True, exist_ok=True)
    approved = set(json.loads(Path(args.approved_refs).read_text())) if args.approved_refs else None
    with report.with_suffix(".jsonl").open("a", encoding="utf-8") as journal:
        def record(item):
            journal.write(json.dumps(item, ensure_ascii=False) + "\n")
            journal.flush()
            os.fsync(journal.fileno())
        result = cleanup_inputs(
            KaggleRetentionStore(api), owner=args.owner, database_path=args.database,
            apply=args.apply, min_age_hours=args.min_age_hours, limit=args.limit,
            approved_refs=approved, record=record,
        )
    report.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps({"candidates": len(result["candidates"]), "kept": len(result["kept"]),
                      "verified_absent": len(result.get("verified_absent", [])),
                      "remaining": len(result["remaining"]), "failed": len(result["failed"])}))
    return 1 if result["failed"] or (args.apply and result["remaining"]) else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
