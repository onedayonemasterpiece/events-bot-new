#!/usr/bin/env python3
"""Prepare and (explicitly) render the once-per-day service-share card."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import Database
from service_share_card import (
    SERVICE_SHARE_TZ, enrich_snapshot_metrics, export_asset_bundle,
    load_active_promo_candidates, load_catalog_snapshot,
)
from scripts.research.select_service_share_events import select


def _write(path: Path, value: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def _run(command: list[str]) -> None:
    print("[service-share-daily] $ " + " ".join(command), flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def _latest_result(artifact_root: Path, profile: str) -> Path:
    paths = [path for path in artifact_root.glob("*/service_share_render_result.json")
             if json.loads(path.read_text()).get("profile") == profile]
    paths.sort(key=lambda p: p.stat().st_mtime)
    if not paths:
        raise RuntimeError(f"missing {profile} Kaggle result under {artifact_root}")
    return paths[-1]


async def prepare(args: argparse.Namespace) -> tuple[Path, Path, Path, dict, dict]:
    measured_at = datetime.fromisoformat(args.measured_at.replace("Z", "+00:00")) if args.measured_at else datetime.now(timezone.utc)
    local_date = measured_at.astimezone(SERVICE_SHARE_TZ).date().isoformat()
    work = Path(args.output_root) / "work" / local_date
    snapshot_path, selection_path, faces_dir = work / "catalog_snapshot.json", work / "selection.json", work / "kaggle_faces"
    db = Database(args.db)
    try:
        snapshot = await load_catalog_snapshot(db, measured_at=measured_at)
        await enrich_snapshot_metrics(db, snapshot)
        promos = await load_active_promo_candidates(db, snapshot=snapshot, measured_at=measured_at)
    finally:
        await db.close()
    selection = select(snapshot["events"], local_date=local_date, promo_candidates=promos,
                       popular_count=3, promoted_count=2, random_count=3, strict_promo=args.strict_promo)
    _write(snapshot_path, snapshot); _write(work / "promo_candidates.internal.json", promos); _write(selection_path, selection)
    if faces_dir.exists() and args.force:
        import shutil
        shutil.rmtree(faces_dir)
    if not (faces_dir / "face_manifest.json").exists():
        _run([sys.executable, str(ROOT / "scripts/research/prepare_service_share_faces.py"),
              "--selection", str(selection_path), "--output-dir", str(faces_dir),
              "--bold-font", str(ROOT / "assets/fonts/Cygre-Bold.ttf"),
              "--semibold-font", str(ROOT / "assets/fonts/Cygre-SemiBold.ttf")])
    return work, snapshot_path, faces_dir, snapshot, selection


async def main_async(args: argparse.Namespace) -> int:
    current = Path(args.output_root) / "current" / "manifest.json"
    if current.exists() and not args.force:
        prior = json.loads(current.read_text())
        today = datetime.now(SERVICE_SHARE_TZ).date().isoformat() if not args.measured_at else datetime.fromisoformat(args.measured_at.replace("Z", "+00:00")).astimezone(SERVICE_SHARE_TZ).date().isoformat()
        if prior.get("local_date") == today:
            print(json.dumps({"ok": True, "status": "already_accepted_for_local_date", "manifest": str(current)})); return 0
    work, snapshot_path, faces_dir, snapshot, selection = await prepare(args)
    if args.prepare_only:
        print(json.dumps({"ok": True, "status": "prepared", "work": str(work), "event_ids": [row["event_id"] for row in selection["events"]]})); return 0
    kaggle_root = work / "kaggle"
    common = [sys.executable, str(ROOT / "scripts/run_service_share_still_kaggle.py"),
              "--bundle-dir", str(work), "--artifact-root", str(kaggle_root),
              "--catalog-snapshot", str(snapshot_path), "--composition-date", snapshot["local_date"]]
    if args.status_db and args.status_callback_url:
        common += ["--status-db", args.status_db, "--status-callback-url", args.status_callback_url]
    _run(common + ["--profile", "debug-gpu"])
    debug_result = _latest_result(kaggle_root, "debug-gpu")
    _run(common + ["--profile", "final-cpu", "--require-debug-result", str(debug_result)])
    final_result = _latest_result(kaggle_root, "final-cpu")
    result = json.loads(final_result.read_text())
    master = final_result.parent / result["output_filename"]
    faces = json.loads((faces_dir / "face_manifest.json").read_text())
    visual_payload = {
        "contract": "service_share_visual_payload_v1", "copy_version": "cube_product_v11_daily_v1",
        "metrics": {key: snapshot[key] for key in ("eligible_event_count", "city_count", "recent_added_count", "catalog_hash")},
        "faces": [{"event_id": row["event_id"], "group": row["selection_group"], "sha256": row["face_sha256"]} for row in faces["faces"]],
        "composition": result["composition"],
    }
    manifest = export_asset_bundle(
        master_png=master, output_dir=Path(args.output_root), visual_payload=visual_payload,
        selection=selection, snapshot=snapshot, composition=result["composition"],
        bundle_sha256=result["bundle_sha256"], result_sha256=result["output_sha256"],
    )
    receipt = {"ok": True, "status": "local_artifact_ready_not_published", "manifest": str(manifest),
               "manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(), "gpu_result": str(debug_result), "cpu_result": str(final_result)}
    _write(work / "daily_receipt.json", receipt); print(json.dumps(receipt, ensure_ascii=False)); return 0


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    db_default = os.getenv("DB_PATH", "/data/db.sqlite")
    callback_default = os.getenv("SERVICE_SHARE_STATUS_CALLBACK_URL") or os.getenv("KAGGLE_STATUS_CALLBACK_URL")
    if not callback_default and os.getenv("WEBHOOK_URL"):
        callback_default = os.getenv("WEBHOOK_URL", "").rstrip("/") + "/internal/kaggle/run-event"
    p.add_argument("--db", default=db_default)
    p.add_argument("--output-root", default=os.getenv("SERVICE_SHARE_CARD_OUTPUT_ROOT", str(ROOT / "artifacts/codex/service-share-card")))
    p.add_argument("--measured-at"); p.add_argument("--prepare-only", action="store_true")
    p.add_argument("--strict-promo", action="store_true"); p.add_argument("--force", action="store_true")
    p.add_argument("--status-db", default=(os.getenv("SERVICE_SHARE_STATUS_DB") or db_default) if callback_default else None)
    p.add_argument("--status-callback-url", default=callback_default)
    return p


def main() -> int:
    return asyncio.run(main_async(parser().parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
