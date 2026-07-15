#!/usr/bin/env python3
"""Launch the CPU event-age BGE worker through the standard Kaggle ledger."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in os.sys.path:
    os.sys.path.insert(0, str(PROJECT_ROOT))

from db import Database  # noqa: E402
from source_parsing.kaggle_runner import run_kaggle_kernel  # noqa: E402


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not raw.strip() or raw.lstrip().startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        if key.strip() and key.strip() not in os.environ:
            os.environ[key.strip()] = value.strip().strip('"').strip("'")


async def amain() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--dataset-source", action="append", default=[])
    parser.add_argument("--timeout-minutes", type=int, default=45)
    parser.add_argument("--poll-interval", type=int, default=30)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--db", type=Path)
    args = parser.parse_args()
    load_env(args.env_file)
    if not args.dataset_source:
        parser.error("at least one --dataset-source with input/prepared artifacts is required")
    run_id = args.run_id or "event-age-bge-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    db_path = args.db or Path(os.getenv("DB_PATH") or PROJECT_ROOT / "db.sqlite")
    db = Database(str(db_path))
    await db.init()

    async def status_callback(phase: str, kernel_ref: str, status: dict | None) -> None:
        print(
            json.dumps(
                {"phase": phase, "kernel_ref": kernel_ref, "status": status or {}},
                ensure_ascii=False,
            ),
            flush=True,
        )

    try:
        status, files, duration = await run_kaggle_kernel(
            "EventAgeBgeAssessment",
            timeout_minutes=max(1, args.timeout_minutes),
            poll_interval=max(5, args.poll_interval),
            status_callback=status_callback,
            run_config={"run_id": run_id, "cpu_only": True},
            dataset_sources=list(dict.fromkeys(args.dataset_source)),
            db=db,
            registry_job_type="event_age_bge",
            ledger_kind="event_age_bge_assessment",
            resource_leases=["kaggle_kernel:event_age_bge"],
            output_namespace=run_id,
            registry_meta={"run_id": run_id},
        )
    finally:
        await db.close()
    print(json.dumps({"run_id": run_id, "status": status, "files": files, "duration": duration}))
    return 0 if status == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(amain()))
