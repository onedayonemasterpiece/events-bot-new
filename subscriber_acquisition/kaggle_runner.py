from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import AcqConfig

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUNTIME_DIR = PROJECT_ROOT / "kaggle" / "SubscriberAcquisitionDiscovery"
RUNTIME_SCRIPT = RUNTIME_DIR / "subscriber_acquisition_discovery.py"
OUTPUT_FILENAME = "acq_discovery_result.json"


def live_telegram_scan_enabled() -> bool:
    return (os.getenv("ACQ_ENABLE_LIVE_TG_SCAN") or "").strip().lower() in {"1", "true", "yes", "on"}


def discovery_remote_auth_scope() -> str:
    if (os.getenv("TELEGRAM_AUTH_BUNDLE_S22") or "").strip():
        return "TELEGRAM_AUTH_BUNDLE_S22"
    if (os.getenv("TG_SESSION") or os.getenv("TELEGRAM_SESSION") or "").strip():
        return "TG_SESSION"
    return "TELEGRAM_AUTH_BUNDLE_S22"


async def ensure_remote_telegram_session_available_for_discovery() -> None:
    if not live_telegram_scan_enabled():
        return
    from remote_telegram_session import raise_if_remote_telegram_session_busy

    await raise_if_remote_telegram_session_busy(
        current_job_type="subscriber_acquisition_discovery",
        current_auth_scope=discovery_remote_auth_scope(),
    )


@dataclass(frozen=True)
class DiscoveryRuntimeResult:
    payload: dict[str, Any]
    output_path: Path
    runner: str


def _json_env_value(items: list[str]) -> str:
    return json.dumps([str(item) for item in items if str(item).strip()], ensure_ascii=False)


def _approved_seed_urls_from_payload(payload: dict[str, Any]) -> tuple[list[str], list[str]]:
    tg: list[str] = []
    vk: list[str] = []
    for item in payload.get("surfaces") or []:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        if not url:
            continue
        platform = str(item.get("platform") or "").strip().lower()
        status = str(item.get("status") or "").strip().lower()
        source = str(item.get("source") or "").strip().lower()
        if status not in {"seed", "candidate", "approved"} and source != "seed":
            continue
        if platform == "vk":
            vk.append(url)
        else:
            tg.append(url)
    return tg, vk


async def collect_runtime_seed_payload(db) -> dict[str, Any]:
    """Collect reviewed/acquisition and existing VK-monitoring surfaces.

    This is read-only DB access. It intentionally does not approve or reject
    anything; the runtime remains shadow-mode and only returns JSON for import.
    """
    try:
        from sqlalchemy import select, text
        from models import AcqSurface
    except Exception:
        return {"surfaces": []}
    surfaces: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    async with db.get_session() as session:
        rows = (await session.execute(
            select(AcqSurface).where(AcqSurface.status.in_(["seed", "candidate", "approved"])).limit(100)
        )).scalars().all()
        for row in rows:
            item = row.model_dump()
            key = (str(item.get("platform") or ""), str(item.get("external_id") or item.get("url") or ""))
            seen.add(key)
            surfaces.append(item)
        table_exists = (await session.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='vk_source'"))).scalar_one_or_none()
        if table_exists:
            info = (await session.execute(text("PRAGMA table_info(vk_source)"))).all()
            columns = {str(row[1]) for row in info}
            select_cols = ["group_id"]
            for col in ["screen_name", "name", "owner_type"]:
                if col in columns:
                    select_cols.append(col)
            result = await session.execute(text(f"SELECT {', '.join(select_cols)} FROM vk_source ORDER BY group_id LIMIT 250"))
            for row in result.mappings().all():
                owner_type = str(row.get("owner_type") or "group").strip().lower()
                if owner_type and owner_type != "group":
                    continue
                screen_name = str(row.get("screen_name") or "").strip()
                group_id = row.get("group_id")
                handle = screen_name or (f"club{int(group_id)}" if group_id is not None else "")
                if not handle:
                    continue
                external_id = f"vk:{handle}"
                key = ("vk", external_id)
                if key in seen:
                    continue
                seen.add(key)
                surfaces.append({
                    "platform": "vk",
                    "surface_type": "community",
                    "url": f"https://vk.com/{handle}",
                    "title": row.get("name"),
                    "handle": handle,
                    "external_id": external_id,
                    "status": "candidate",
                    "source": "vk_source",
                    "topic_hint": "existing VK monitoring source",
                    "reach": {"confidence": "low", "basis": "vk_source_seed"},
                    "risk": {"safety_risk": "low", "spam_risk": "unknown"},
                })
    return {"surfaces": surfaces}


def run_local_discovery_runtime(*, config: AcqConfig, seed_payload: dict[str, Any] | None = None) -> DiscoveryRuntimeResult:
    """Run the Kaggle script locally as the same safe shadow runtime.

    This is the deterministic no-Kaggle fallback used by `/acq_run` in dev/test
    and by production only when explicitly configured. The script itself owns all
    TG/VK no-send constraints and writes the same JSON artifact as Kaggle.
    """
    if not RUNTIME_SCRIPT.exists():
        raise FileNotFoundError(f"acquisition runtime script not found: {RUNTIME_SCRIPT}")
    seed_payload = seed_payload or {}
    tg_seeds, vk_seeds = _approved_seed_urls_from_payload(seed_payload)
    with tempfile.TemporaryDirectory(prefix="acq-discovery-") as tmp:
        output_dir = Path(tmp)
        env = os.environ.copy()
        env["ACQ_OUTPUT_DIR"] = str(output_dir)
        env.setdefault("ACQ_DEFAULT_LINK_TARGET_URL", config.default_link_target_url)
        env.setdefault("ACQ_MAX_SURFACES_PER_RUN", str(config.max_surfaces_per_run))
        env.setdefault("ACQ_MAX_MESSAGES_PER_SURFACE", str(config.max_messages_per_surface))
        env.setdefault("ACQ_MAX_THREADS_PER_SURFACE", str(config.max_threads_per_surface))
        env.setdefault("ACQ_MAX_OPPORTUNITIES_PER_RUN", str(config.max_opportunities_per_run))
        if tg_seeds and "ACQ_TG_SEEDS_JSON" not in env:
            env["ACQ_TG_SEEDS_JSON"] = _json_env_value(tg_seeds)
        if vk_seeds and "ACQ_VK_SEEDS_JSON" not in env:
            env["ACQ_VK_SEEDS_JSON"] = _json_env_value(vk_seeds)
        completed = subprocess.run(
            [sys.executable, str(RUNTIME_SCRIPT)],
            cwd=str(PROJECT_ROOT),
            env=env,
            text=True,
            capture_output=True,
            timeout=int(os.getenv("ACQ_LOCAL_RUNTIME_TIMEOUT_SECONDS") or "180"),
            check=False,
        )
        if completed.returncode != 0:
            stderr = (completed.stderr or "").strip()[-2000:]
            raise RuntimeError(f"acquisition local runtime failed rc={completed.returncode}: {stderr}")
        output_path = output_dir / OUTPUT_FILENAME
        if not output_path.exists():
            raise FileNotFoundError(f"acquisition runtime did not write {OUTPUT_FILENAME}")
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        # Preserve a durable artifact copy for diagnostics without committing it.
        artifact_dir = PROJECT_ROOT / "artifacts" / "codex" / "subscriber-acquisition-discovery"
        artifact_dir.mkdir(parents=True, exist_ok=True)
        durable_path = artifact_dir / OUTPUT_FILENAME
        durable_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return DiscoveryRuntimeResult(payload=payload, output_path=durable_path, runner="local_shadow_runtime")
