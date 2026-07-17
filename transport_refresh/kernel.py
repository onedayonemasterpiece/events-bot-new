from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path

from .provider_job import run_provider_job

WORKING = Path("/kaggle/working") if Path("/kaggle/working").exists() else Path.cwd() / "output"
INPUT = Path("/kaggle/input") if Path("/kaggle/input").exists() else Path.cwd() / "input"
STATUS = None
RESOURCES: list[str] = []
PROGRESS: dict[str, object] = {"phase": "bootstrap", "progress_percent": 0}


def _find(name: str) -> Path | None:
    if INPUT.exists():
        return next((path for path in INPUT.rglob(name) if path.is_file()), None)
    return None


def _event(event: str, phase: str, percent: int, message: str) -> None:
    PROGRESS.update(phase=phase, progress_percent=percent, progress_label=message)
    if STATUS is not None and getattr(STATUS, "enabled", False):
        STATUS.event(event, phase=phase, status="running" if percent < 100 else "done", progress=dict(PROGRESS), message=message)


def _init_status() -> None:
    global STATUS
    try:
        from kaggle_status_client import load_status_client
    except ImportError:
        load_status_client = None
        for root in (Path.cwd(), Path("/kaggle/working"), INPUT):
            if not root.exists():
                continue
            for candidate in sorted(root.rglob("kaggle_status_client.py")):
                spec = importlib.util.spec_from_file_location("transport_kaggle_status_client", candidate)
                if not spec or not spec.loader:
                    continue
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                load_status_client = module.load_status_client
                break
            if load_status_client is not None:
                break
        if load_status_client is None:
            return
    STATUS = load_status_client(output_dir=WORKING, log=lambda value: print(value, flush=True))
    if not getattr(STATUS, "enabled", False):
        return
    _event("kernel_started", "preflight", 2, "transport refresh started")
    for resource in STATUS.config.get("resource_leases") or []:
        if not STATUS.acquire_resource(str(resource), ttl_seconds=2 * 60 * 60):
            raise RuntimeError(f"Required Kaggle resource is busy: {resource}")
        RESOURCES.append(str(resource))
    STATUS.start_alive(interval_seconds=60, progress_provider=lambda: dict(PROGRESS))


def _finish(ok: bool, message: str) -> None:
    try:
        if STATUS is not None and getattr(STATUS, "enabled", False):
            STATUS.event("report_written", phase="report" if ok else "failed", status="done" if ok else "failed", progress=dict(PROGRESS), message=message)
    finally:
        if STATUS is not None:
            for resource in RESOURCES:
                try:
                    STATUS.release_resource(resource)
                except Exception as exc:
                    print(f"[transport-refresh] resource release failed: {exc}", flush=True)
            RESOURCES.clear()
            STATUS.stop_alive()


def kernel_main(provider: str) -> int:
    WORKING.mkdir(parents=True, exist_ok=True)
    _init_status()
    try:
        config_path = _find("transport_refresh_config.json") or Path("transport_refresh_config.json")
        config = json.loads(config_path.read_text(encoding="utf-8"))
        if config.get("provider") != provider:
            raise ValueError(f"provider config mismatch: {config.get('provider')} != {provider}")
        payload_name = config.pop("source_payload_filename", None)
        if payload_name:
            payload = _find(str(payload_name))
            if payload is None:
                raise FileNotFoundError(payload_name)
            config["source_payload_path"] = str(payload)
        _event("preflight_ok", "preflight", 10, f"{provider} input ready")
        _event("alive", "fetch", 35, f"fetching {provider} schedule")
        result = run_provider_job(config, output_dir=WORKING)
        _event("alive", "validate", 80, f"validated {result['service_count']} exact-date services")
        PROGRESS.update(phase="report", progress_percent=100, progress_label="transport manifest ready")
        _finish(True, f"{provider} transport manifest ready")
        return 0
    except Exception as exc:
        PROGRESS.update(phase="failed", progress_label="transport refresh failed")
        _finish(False, f"{exc.__class__.__name__}: {exc}")
        raise
