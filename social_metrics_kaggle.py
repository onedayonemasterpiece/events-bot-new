from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import shutil
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from db import Database
from kaggle_status import create_kaggle_run_config, create_kaggle_status_dataset, resolve_callback_url
from social_metrics_batch import build_social_metrics_manifest, import_social_metrics_result

ROOT = Path(__file__).resolve().parent
KERNEL_SOURCE = ROOT / "kaggle" / "SocialMetricsCollector"
RESULT_FILENAME = "social_metrics_results.json"


def _enabled(name: str, default: bool = False) -> bool:
    value = (os.getenv(name) or ("1" if default else "0")).strip().lower()
    return value in {"1", "true", "yes", "on"}


def _first_env(*names: str) -> str:
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return ""


def _dataset_metadata(path: Path, ref: str, title: str) -> None:
    (path / "dataset-metadata.json").write_text(json.dumps({
        "title": title[:50], "id": ref, "licenses": [{"name": "CC0-1.0"}],
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _secret_payload(has_tg: bool, has_vk: bool) -> dict[str, str]:
    payload: dict[str, str] = {}
    if has_tg:
        bundle = _first_env("TELEGRAM_AUTH_BUNDLE_CHECK_POPULAR")
        api_id = _first_env("SOCIAL_METRICS_TG_API_ID", "TG_API_ID", "TELEGRAM_API_ID")
        api_hash = _first_env("SOCIAL_METRICS_TG_API_HASH", "TG_API_HASH", "TELEGRAM_API_HASH")
        if not bundle or not api_id or not api_hash:
            raise RuntimeError("dedicated popularity Telegram credentials are incomplete")
        payload.update({
            "TELEGRAM_AUTH_BUNDLE_CHECK_POPULAR": bundle,
            "TG_API_ID": api_id,
            "TG_API_HASH": api_hash,
        })
    if has_vk:
        token = _first_env("VK_USER_TOKEN", "VK_ACCESS_TOKEN4", "VK_SERVICE_TOKEN", "VK_TOKEN")
        if not token:
            raise RuntimeError("VK token is missing")
        payload["VK_TOKEN"] = token
    return payload


async def _active_social_run(db: Database) -> str | None:
    cutoff = datetime.fromtimestamp(time.time() - 3 * 60 * 60, tz=timezone.utc).isoformat()
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT run_id FROM kaggle_run_ledger
            WHERE kind='social_metrics_collector'
              AND status NOT IN ('done','complete','failed','error','cancelled','canceled')
              AND updated_at >= ?
            ORDER BY updated_at DESC LIMIT 1
            """,
            (cutoff,),
        )
        row = await cur.fetchone()
    return str(row[0]) if row else None


def _launch_sync(
    *,
    manifest: dict[str, Any],
    status_config: dict[str, Any],
    username: str,
    secret_payload: dict[str, str],
    timeout_seconds: int,
    refs: list[str],
) -> dict[str, Any]:
    from cryptography.fernet import Fernet
    from scripts.run_static_site_builder_kaggle import (
        create_input_dataset,
        wait_dataset_ready,
        wait_kernel_dataset_sources,
    )
    from video_announce.kaggle_client import KaggleClient

    client = KaggleClient()
    suffix = f"{datetime.now(timezone.utc):%Y%m%d%H%M%S}-{uuid.uuid4().hex[:8]}"
    with tempfile.TemporaryDirectory(prefix="social-metrics-kaggle-") as tmp:
        root = Path(tmp)
        manifest_dir = root / "manifest"
        cipher_dir = root / "cipher"
        key_dir = root / "key"
        kernel_dir = root / "kernel"
        for path in (manifest_dir, cipher_dir, key_dir):
            path.mkdir(parents=True)
        shutil.copytree(KERNEL_SOURCE, kernel_dir)

        manifest_ref = f"{username}/social-metrics-input-{suffix}"
        cipher_ref = f"{username}/social-metrics-cipher-{suffix}"
        key_ref = f"{username}/social-metrics-key-{suffix}"
        (manifest_dir / "social_metrics_manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8"
        )
        _dataset_metadata(manifest_dir, manifest_ref, f"Social metrics input {suffix}")

        key = Fernet.generate_key()
        encrypted = Fernet(key).encrypt(json.dumps(secret_payload, ensure_ascii=False).encode("utf-8"))
        (cipher_dir / "secrets.enc").write_bytes(encrypted)
        (key_dir / "fernet.key").write_bytes(key)
        _dataset_metadata(cipher_dir, cipher_ref, f"Social metrics cipher {suffix}")
        _dataset_metadata(key_dir, key_ref, f"Social metrics key {suffix}")

        for path, ref, files in (
            (manifest_dir, manifest_ref, ["social_metrics_manifest.json"]),
            (cipher_dir, cipher_ref, ["secrets.enc"]),
            (key_dir, key_ref, ["fernet.key"]),
        ):
            refs.append(ref)
            create_input_dataset(client, path, ref)
            wait_dataset_ready(client, ref, expected_files=files)

        status_ref = create_kaggle_status_dataset(
            client,
            username=username,
            slug_prefix="status-social-metrics",
            run_id=str(manifest["run_id"]),
            config=status_config,
        )
        if not status_ref:
            raise RuntimeError("Kaggle status dataset is required")
        refs.append(status_ref)
        wait_dataset_ready(client, status_ref, expected_files=["kaggle_run.json", "kaggle_status_client.py"])

        meta_path = kernel_dir / "kernel-metadata.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        kernel_ref = f"{username}/kenigevents-social-metrics-collector"
        meta["id"] = kernel_ref
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        sources = [manifest_ref, cipher_ref, key_ref, status_ref]
        client.push_kernel(kernel_path=kernel_dir, dataset_sources=sources)
        wait_kernel_dataset_sources(client, kernel_ref, sources)

        deadline = time.monotonic() + timeout_seconds
        last_status: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            time.sleep(15)
            last_status = client.get_kernel_status(kernel_ref)
            raw = str(last_status.get("status") or "").upper()
            if raw == "COMPLETE":
                output_dir = root / "output"
                output_dir.mkdir()
                client.download_kernel_output(kernel_ref, path=output_dir, force=True)
                result_path = output_dir / RESULT_FILENAME
                if not result_path.exists():
                    matches = list(output_dir.rglob(RESULT_FILENAME))
                    if not matches:
                        raise RuntimeError("Kaggle result artifact is missing")
                    result_path = matches[0]
                result = json.loads(result_path.read_text(encoding="utf-8"))
                return result
            if raw in {"ERROR", "FAILED", "CANCELLED"}:
                raise RuntimeError(f"Kaggle social metrics failed: {last_status}")
        raise TimeoutError(f"Kaggle social metrics timeout: {last_status}")


def _cleanup_sync(refs: list[str]) -> None:
    from video_announce.kaggle_client import KaggleClient

    client = KaggleClient()
    for ref in refs:
        try:
            client.delete_dataset(ref, no_confirm=True)
        except Exception:
            logging.warning("social metrics Kaggle dataset cleanup failed: %s", ref, exc_info=True)


async def run_social_metrics_kaggle_batch(db: Database) -> dict[str, Any]:
    """Thin Fly orchestration: DB plan/import around one remote Kaggle batch."""
    if not _enabled("ENABLE_SOCIAL_METRICS_KAGGLE", default=False):
        return {"enabled": False}
    active = await _active_social_run(db)
    if active:
        return {"enabled": True, "skipped": "active_run", "run_id": active}

    slot = int(time.time()) // max(300, int(_first_env("SOCIAL_METRICS_BATCH_INTERVAL_MINUTES") or "30") * 60)
    run_id = f"social-metrics:{slot}"
    manifest = await build_social_metrics_manifest(db, run_id=run_id)
    targets = list(manifest.get("targets") or [])
    resolve_candidates = list(manifest.get("vk_resolve_candidates") or [])
    if not targets and not resolve_candidates:
        return {"enabled": True, "run_id": run_id, "targets": 0, "empty": True}
    has_tg = any(row.get("platform") == "telegram" for row in targets)
    has_vk = bool(resolve_candidates) or any(row.get("platform") == "vk" for row in targets)
    secrets = _secret_payload(has_tg, has_vk)
    username = _first_env("KAGGLE_USERNAME")
    callback = resolve_callback_url()
    if not username or not callback:
        raise RuntimeError("KAGGLE_USERNAME and Kaggle status callback URL are required")
    kernel_ref = f"{username}/kenigevents-social-metrics-collector"
    status_config = await create_kaggle_run_config(
        db,
        run_id=run_id,
        session_id=None,
        kind="social_metrics_collector",
        notebook="SocialMetricsCollector",
        kernel_ref=kernel_ref,
        dataset_ref=None,
        callback_url=callback,
        resource_leases=[
            "job:social_metrics_batch",
            *(["telegram_session:telegram_auth_bundle_check_popular"] if has_tg else []),
        ],
        replace_existing=False,
    )
    if not status_config:
        return {"enabled": True, "skipped": "slot_claimed", "run_id": run_id}

    refs: list[str] = []
    failure_phase = "server_launch_failed"
    try:
        result = await asyncio.to_thread(
            _launch_sync,
            manifest=manifest,
            status_config=status_config,
            username=username,
            secret_payload=secrets,
            timeout_seconds=max(600, int(_first_env("SOCIAL_METRICS_KAGGLE_TIMEOUT_SECONDS") or "1800")),
            refs=refs,
        )
        failure_phase = "import_failed"
        imported = await import_social_metrics_result(db, manifest=manifest, result=result)
        async with db.raw_conn() as conn:
            await conn.execute(
                "UPDATE kaggle_run_ledger SET phase='imported', progress_json=?, updated_at=? WHERE run_id=?",
                (json.dumps({"targets": len(targets), "imported": imported}, ensure_ascii=False), datetime.now(timezone.utc).isoformat(), run_id),
            )
            await conn.commit()
        return {
            "enabled": True,
            "run_id": run_id,
            "targets": len(targets),
            "resolve_candidates": len(resolve_candidates),
            "imported": imported,
        }
    except Exception as exc:
        now = datetime.now(timezone.utc).isoformat()
        async with db.raw_conn() as conn:
            await conn.execute(
                """
                UPDATE kaggle_run_ledger
                SET status='error', phase=?, error=?, terminal_at=?, updated_at=?
                WHERE run_id=?
                """,
                (failure_phase, f"{type(exc).__name__}: {exc}"[:1000], now, now, run_id),
            )
            await conn.commit()
        raise
    finally:
        if refs:
            await asyncio.to_thread(_cleanup_sync, refs)
