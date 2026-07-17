#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from transport_refresh.store import TransportManifestStore


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate/fan-in a provider result on the server")
    parser.add_argument("--provider", choices=["kppk", "bus"], required=True)
    parser.add_argument("--manifest")
    parser.add_argument("--failure-reason", choices=["timeout", "empty", "kernel_failed"])
    parser.add_argument("--state-root", required=True)
    parser.add_argument("--db", help="Existing Fly SQLite DB; enqueues static_site_build:prod on change")
    args = parser.parse_args()
    if bool(args.manifest) == bool(args.failure_reason):
        parser.error("provide exactly one of --manifest or --failure-reason")
    candidate = json.loads(Path(args.manifest).read_text(encoding="utf-8")) if args.manifest else None

    enqueue = None
    if args.db:
        from db import Database
        from main import enqueue_job
        from models import JobTask

        database = Database(args.db)
        def enqueue(key: str, payload: dict) -> None:
            asyncio.run(enqueue_job(database, 0, JobTask.static_site_build, payload=payload, coalesce_key=key, requeue_done=True))

    report = TransportManifestStore(args.state_root).publish(
        args.provider, candidate, failure_reason=args.failure_reason, enqueue=enqueue,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    if report.get("rebuild_pending"):
        return 3
    return 0 if report["provider_accepted"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
