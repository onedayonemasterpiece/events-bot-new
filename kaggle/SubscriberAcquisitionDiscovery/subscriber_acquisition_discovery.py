from __future__ import annotations

import importlib.util
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("subscriber_acquisition_discovery")

SCRIPT_DIR = Path(globals().get("__file__", Path.cwd() / "subscriber_acquisition_discovery.py")).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

DEFAULT_TG_SEEDS = [
    "https://t.me/tg_kgd",
    "https://t.me/chatkalin",
    "https://t.me/kenig01chat",
    "https://t.me/zhest_kaliningrada",
    "https://t.me/pereezd_v_kaliningrad_legko",
]


def _load_status_loader():
    try:
        from kaggle_status_client import load_status_client as loader
        return loader
    except Exception as exc:
        logger.warning("kaggle_status direct import failed: %s", exc)
    for root in [SCRIPT_DIR, Path.cwd(), Path("/kaggle/working"), Path("/kaggle/input")]:
        if not root.exists():
            continue
        candidates = [root / "kaggle_status_client.py"]
        try:
            candidates.extend(sorted(root.rglob("kaggle_status_client.py")))
        except Exception:
            pass
        for candidate in candidates:
            if not candidate.exists():
                continue
            try:
                spec = importlib.util.spec_from_file_location("events_bot_kaggle_status_client", candidate)
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    logger.info("[kaggle_status] loaded helper from %s", candidate)
                    return module.load_status_client
            except Exception as exc:
                logger.warning("kaggle_status helper load failed from %s: %s", candidate, exc)
    return None


load_status_client = _load_status_loader()
try:
    STATUS_CLIENT = load_status_client(log=lambda message: logger.info(message)) if load_status_client else None
except Exception as exc:
    logger.warning("kaggle_status client init failed; continuing without callbacks: %s", exc)
    STATUS_CLIENT = None
STATUS_PROGRESS: dict[str, object] = {"phase": "bootstrap"}


def _status_event(event: str, *, phase: str | None = None, status: str | None = None, progress: dict[str, Any] | None = None, message: str | None = None) -> None:
    if STATUS_CLIENT is None:
        return
    try:
        STATUS_CLIENT.event(event, phase=phase, status=status, progress=progress, message=message)
    except Exception:
        logger.warning("acq.status_event_failed event=%s", event, exc_info=True)


def _json_env(name: str, default: Any) -> Any:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        logger.warning("invalid JSON env %s", name)
        return default


def _seed_surface(url: str, *, platform: str = "tg") -> dict[str, Any]:
    handle = url.rstrip("/").split("/")[-1].lstrip("@")
    external_id = f"{platform}:{handle}" if handle else url
    surface_type = "community" if platform == "vk" else "unknown_public"
    return {
        "platform": platform,
        "surface_type": surface_type,
        "url": url,
        "handle": handle or None,
        "external_id": external_id,
        "status": "candidate",
        "source": "seed",
        "topic_hint": "Kaliningrad public/community seed",
        "reach": {"confidence": "low", "basis": "seed_only"},
        "risk": {"level": "unknown", "reason": "not scanned yet"},
    }


def build_shadow_payload() -> dict[str, Any]:
    tg_seeds = _json_env("ACQ_TG_SEEDS_JSON", DEFAULT_TG_SEEDS)
    vk_seeds = _json_env("ACQ_VK_SEEDS_JSON", [])
    vk_allowlist = _json_env("ACQ_VK_ALLOWLIST_JSON", [])
    surfaces: list[dict[str, Any]] = []
    for url in list(tg_seeds or []):
        surfaces.append(_seed_surface(str(url), platform="tg"))
    # VK-ready but disabled unless explicit allowlist is provided.
    allowed_vk = {str(x).strip().lower() for x in list(vk_allowlist or []) if str(x).strip()}
    for url in list(vk_seeds or []):
        normalized = str(url).strip()
        if not normalized:
            continue
        if normalized.lower() not in allowed_vk:
            surfaces.append({**_seed_surface(normalized, platform="vk"), "status": "candidate"})
            continue
        surfaces.append({**_seed_surface(normalized, platform="vk"), "status": "approved"})
    return {
        "run_id": os.getenv("KAGGLE_RUN_ID") or f"acq-shadow-{int(time.time())}",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "surfaces": surfaces,
        "opportunities": [],
        "stats": {
            "mode": "shadow_preflight",
            "surfaces_total": len(surfaces),
            "telegram_seeds": len(list(tg_seeds or [])),
            "vk_seeds": len(list(vk_seeds or [])),
            "vk_allowlist": len(allowed_vk),
            "external_sends": 0,
            "comments_posted": 0,
            "stickers_sent": 0,
        },
        "diagnostics": [
            "Runtime scaffold is safe/read-only and produces importable seed surfaces; live Telethon/VK scans must remain shadow-mode and budgeted."
        ],
    }


def main() -> None:
    _status_event("kernel_started", phase="bootstrap", status="running", progress={"progress_percent": 1, "progress_label": "bootstrap"})
    payload = build_shadow_payload()
    _status_event(
        "preflight_ok",
        phase="preflight",
        status="running",
        progress={
            "progress_percent": 20,
            "progress_label": f"surfaces {len(payload['surfaces'])}",
            "surfaces_total": len(payload["surfaces"]),
            "opportunities_found": 0,
            "external_sends": 0,
        },
    )
    output_dir = Path(os.getenv("ACQ_OUTPUT_DIR") or "/kaggle/working")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "acq_discovery_result.json"
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _status_event(
        "report_written",
        phase="output",
        status="done",
        progress={
            "progress_percent": 100,
            "progress_label": "shadow payload written",
            "surfaces_total": len(payload["surfaces"]),
            "opportunities_found": 0,
            "output": str(output_path),
        },
        message=f"Wrote {output_path}",
    )
    logger.info("wrote %s", output_path)


if __name__ == "__main__":
    main()
