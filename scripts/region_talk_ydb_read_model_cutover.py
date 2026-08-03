#!/usr/bin/env python3
"""Build a reviewable Region Talk typed-read-model cutover plan offline.

This command intentionally performs no YDB connection or write.  It converts a
trusted exported/full state JSON into DDL, generation work rows and the current
counter pointer.  A later explicitly approved operator run can use the same
builders through CandidateReport's generation-first/pointer-last writer.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.region_talk_ydb_read_model import (  # noqa: E402
    build_read_model,
    build_work_items,
    read_model_table_ddl,
    table_paths,
    validate_read_model,
    work_table_ddl,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-json", required=True, help="trusted full-state JSON export")
    parser.add_argument("--database", required=True, help="expected target database path used only to render DDL")
    parser.add_argument("--namespace", default="region_talk_compact")
    parser.add_argument("--max-per-queue", type=int, default=200)
    parser.add_argument("--cutover-state", choices=("shadow", "ready"), default="shadow")
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def build_plan(
    state: dict[str, Any],
    *,
    database: str,
    namespace: str,
    max_per_queue: int,
    cutover_state: str,
) -> dict[str, Any]:
    work = build_work_items(state, max_per_queue=max_per_queue)
    model = build_read_model(state, work, cutover_state=cutover_state)
    if cutover_state == "ready":
        validate_read_model(model)
    work_table, read_model_table = table_paths(database, namespace)
    return {
        "schema_version": model["schema_version"],
        "work_queue_schema_version": model["work_queue_schema_version"],
        "cutover_order": [
            "create_schema",
            "write_complete_generation",
            "validate_counts_and_point_keys",
            "publish_current_pointer_last",
        ],
        "live_execution_performed": False,
        "ddl": {
            "work_queue": work_table_ddl(work_table),
            "read_model": read_model_table_ddl(read_model_table),
        },
        "work_rows": [item.as_row(updated_at=str(state.get("updated_at") or "")) for item in work],
        "read_model": model,
    }


def main() -> int:
    args = parse_args()
    raw = json.loads(Path(args.state_json).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("state JSON must be an object")
    plan = build_plan(
        raw,
        database=args.database,
        namespace=args.namespace,
        max_per_queue=max(1, args.max_per_queue),
        cutover_state=args.cutover_state,
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({
        "ok": True,
        "live_execution_performed": False,
        "generation": plan["read_model"]["generation"],
        "work_rows": len(plan["work_rows"]),
        "output": str(output),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
