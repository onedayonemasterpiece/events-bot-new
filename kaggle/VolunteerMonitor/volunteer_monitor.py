#!/usr/bin/env python3
"""Generated read-only Kaggle live-canary entrypoint.

GitHub Actions replaces the embedded runtime ZIP marker before `kernels push`.
The kernel never connects to production SQLite and writes only bounded output
artifacts under `/kaggle/working`.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

RUNTIME_ZIP_B64 = "__RUNTIME_ZIP_B64__"
MAX_ITEMS = int("__MAX_ITEMS__")
OUTPUT = Path("/kaggle/working")


def _ensure_runtime() -> None:
    if RUNTIME_ZIP_B64.startswith("__"):
        raise RuntimeError("embedded volunteer-monitor runtime was not injected")
    runtime_zip = OUTPUT / "volunteer-monitor-runtime.zip"
    runtime_zip.write_bytes(base64.b64decode(RUNTIME_ZIP_B64.encode("ascii")))
    sys.path.insert(0, str(runtime_zip))

    missing: list[str] = []
    for module_name, package_name in (
        ("bs4", "beautifulsoup4"),
        ("requests", "requests"),
        ("playwright", "playwright"),
    ):
        try:
            __import__(module_name)
        except ImportError:
            missing.append(package_name)
    if missing:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "--disable-pip-version-check", *missing],
            check=True,
        )
    subprocess.run(
        [
            sys.executable,
            "-m",
            "playwright",
            "install",
            "--with-deps",
            "--only-shell",
            "chromium",
        ],
        check=True,
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


async def _run() -> int:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(timezone.utc)
    run_uid = os.getenv("VOLUNTEER_MONITOR_RUN_UID") or started_at.strftime(
        "volunteer-monitor-%Y%m%dT%H%M%SZ"
    )
    result_path = OUTPUT / "volunteer-monitor-result.json"
    receipt_path = OUTPUT / "volunteer-monitor-receipt.json"
    evidence_dir = OUTPUT / "volunteer-monitor-evidence"
    try:
        _ensure_runtime()
        from volunteer_monitor.service import run_live_monitor
        from volunteer_monitor.source_config import DobroSourceConfig

        config = DobroSourceConfig(
            search_url="https://dobro.ru/search?d_c=1&d_s=1&t=e",
            region_name="Калининградская обл",
            max_more_clicks=40,
            max_items=max(1, min(MAX_ITEMS, 250)),
            playwright_timeout_ms=30_000,
            detail_timeout_seconds=30.0,
            headless=True,
            permission_reference="github-environment-volunteer-monitor-canary",
            evidence_dir=evidence_dir,
        )
        result = await run_live_monitor(config=config)
        payload = result.to_dict()
        result_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        receipt_status = "partial" if payload["run_status"] == "PARTIAL" else "success"
        receipt = {
            "schema_version": "volunteer-monitor-kaggle-receipt-v1",
            "run_uid": run_uid,
            "status": receipt_status,
            "started_at": started_at.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "result_file": result_path.name,
            "result_sha256": _sha256(result_path),
            "run_status": payload["run_status"],
            "source_pages_seen": payload["source_pages_seen"],
            "opportunity_count": payload["opportunity_count"],
            "status_counts": payload["status_counts"],
            "warnings": payload["warnings"][:30],
        }
        receipt_path.write_text(
            json.dumps(receipt, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(receipt, ensure_ascii=False))
        return 0
    except Exception as exc:
        receipt = {
            "schema_version": "volunteer-monitor-kaggle-receipt-v1",
            "run_uid": run_uid,
            "status": "error",
            "started_at": started_at.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "error_type": type(exc).__name__,
            "error": str(exc)[:2_000],
        }
        receipt_path.write_text(
            json.dumps(receipt, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(receipt, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_run()))
