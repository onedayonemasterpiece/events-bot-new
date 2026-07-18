#!/usr/bin/env python3
"""Print redacted, read-only static-site build diagnostics."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from static_site_diagnostics import (  # noqa: E402
    collect_static_site_diagnostics,
    format_static_site_diagnostics,
    redact,
)


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only, redacted static-site build and channel diagnostics",
    )
    parser.add_argument("--db", required=True, help="SQLite database to open read-only")
    parser.add_argument("--hours", type=int, default=24, help="lookback window (default: 24)")
    parser.add_argument("--manifest", action="append", default=[], help="optional release manifest JSON; repeatable")
    parser.add_argument(
        "--bucket-inventory",
        action="append",
        default=[],
        help="optional JSON object listing/current-pointer evidence; repeatable",
    )
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--limit", type=int, default=100, help="maximum build/issue details")
    parser.add_argument("--now", help=argparse.SUPPRESS)
    return parser.parse_args()


def main() -> int:
    args = _args()
    try:
        now = datetime.fromisoformat(args.now.replace("Z", "+00:00")) if args.now else None
        report = collect_static_site_diagnostics(
            args.db,
            hours=args.hours,
            now=now,
            manifest_paths=args.manifest,
            bucket_inventory_paths=args.bucket_inventory,
            detail_limit=args.limit,
        )
    except (OSError, sqlite3.Error, ValueError):
        # Paths, SQLite errors and malformed values can contain credentials.
        print(json.dumps({"ok": False, "error": "diagnostics_input_or_database_error"}))
        return 2
    if args.format == "json":
        print(json.dumps(redact(report), ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(format_static_site_diagnostics(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
