from __future__ import annotations

import asyncio
import base64
import importlib.util
import json
import re
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


_LINK_RE = re.compile(r"(?i)\b(?:https?://)?(?:t\.me|telegram\.me|vk\.com)/[A-Za-z0-9_./?=&-]+")
_TG_HOST_RE = re.compile(r"(?i)(?:https?://)?(?:t\.me|telegram\.me)/(?P<handle>[A-Za-z0-9_]{4,})")
_VK_HOST_RE = re.compile(r"(?i)(?:https?://)?vk\.com/(?P<handle>[A-Za-z0-9_.-]{3,})")
_OPPORTUNITY_RE = re.compile(
    r"(?i)\b(куда|где|что\s+посетить|афиша|концерт|выставк|спектакл|детям|с детьми|выходные|мероприят)")
_STICKER_HINT_RE = re.compile(r"(?i)\b(стикер|sticker|😂|👍|🔥|🤣|❤️|❤|👏)")


def _truthy_env(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _find_input_file(filename: str) -> Path | None:
    roots = [SCRIPT_DIR, Path.cwd(), Path("/kaggle/input")]
    for root in roots:
        if not root.exists():
            continue
        direct = root / filename
        if direct.is_file():
            return direct
        try:
            for path in root.rglob(filename):
                if path.is_file():
                    return path
        except Exception:
            pass
    return None


def _load_kaggle_env() -> dict[str, Any]:
    """Load existing encrypted Kaggle config/secrets if mounted.

    This mirrors the TelegramMonitor pattern but stays best-effort so local smoke
    runs and fixture-only shadow runs do not fail just because Kaggle datasets are absent.
    """
    loaded: dict[str, Any] = {"config_loaded": False, "secrets_loaded": False}
    config_path = _find_input_file("config.json")
    if config_path:
        try:
            config = json.loads(config_path.read_text(encoding="utf-8"))
            loaded["config_loaded"] = True
            loaded["config_keys"] = sorted(config.keys())
            for key, value in config.items():
                if isinstance(value, (str, int, float, bool)) and os.getenv(str(key)) is None:
                    os.environ[str(key)] = str(value)
        except Exception as exc:
            loaded["config_error"] = str(exc)
    enc_path = _find_input_file("secrets.enc")
    key_path = _find_input_file("fernet.key")
    if enc_path and key_path:
        try:
            from cryptography.fernet import Fernet

            fernet = Fernet(key_path.read_bytes().strip())
            secrets = json.loads(fernet.decrypt(enc_path.read_bytes()).decode("utf-8"))
            loaded["secrets_loaded"] = True
            loaded["secret_keys"] = sorted(secrets.keys())
            for key, value in secrets.items():
                if key and value not in (None, ""):
                    os.environ[str(key)] = str(value)
        except Exception as exc:
            loaded["secrets_error"] = str(exc)
    return loaded


def _decode_tg_auth() -> tuple[str, dict[str, Any]]:
    bundle_b64 = (os.getenv("TELEGRAM_AUTH_BUNDLE_S22") or "").strip()
    device_config = {
        "device_model": "Samsung S22 Ultra",
        "system_version": "13.0",
        "app_version": "9.6.6",
    }
    if bundle_b64:
        raw = base64.urlsafe_b64decode(bundle_b64.encode("ascii")).decode("utf-8")
        bundle = json.loads(raw)
        session = str(bundle.get("session") or "")
        for key in ["device_model", "system_version", "app_version", "lang_code", "system_lang_code"]:
            if bundle.get(key):
                device_config[key] = bundle[key]
        return session, device_config
    return (os.getenv("TG_SESSION") or "").strip(), device_config


def _handle_from_url(url: str, platform: str = "tg") -> str:
    raw = str(url or "").strip().rstrip("/")
    if raw.startswith("@"):
        return raw.lstrip("@")
    return raw.split("/")[-1].split("?")[0].lstrip("@")


def extract_candidate_surfaces(text: str) -> list[dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for match in _LINK_RE.findall(text or ""):
        url = match if match.startswith(("http://", "https://")) else f"https://{match}"
        tg = _TG_HOST_RE.search(url)
        vk = _VK_HOST_RE.search(url)
        if tg:
            handle = tg.group("handle")
            if handle.lower() in {"c", "s", "joinchat"}:
                continue
            out[f"tg:{handle}"] = _seed_surface(f"https://t.me/{handle}", platform="tg") | {"source": "discovered"}
        elif vk:
            handle = vk.group("handle")
            out[f"vk:{handle}"] = _seed_surface(f"https://vk.com/{handle}", platform="vk") | {"source": "discovered"}
    return list(out.values())


def build_opportunity_from_message(surface: dict[str, Any], message: Any, *, default_target_url: str) -> dict[str, Any] | None:
    text = str(getattr(message, "message", None) or getattr(message, "text", None) or "").strip()
    if not text or not _OPPORTUNITY_RE.search(text):
        return None
    msg_id = int(getattr(message, "id", 0) or 0)
    url = surface.get("url") or ""
    handle = surface.get("handle") or _handle_from_url(url)
    context_url = f"https://t.me/{handle}/{msg_id}" if surface.get("platform") == "tg" and handle and msg_id else url
    reactions = getattr(message, "reactions", None)
    sticker_possible = bool(_STICKER_HINT_RE.search(text) or any(mark in text for mark in ["😂", "👍", "🔥", "🤣", "❤️", "❤", "👏"]) or getattr(message, "sticker", None) or reactions)
    date = getattr(message, "date", None)
    return {
        "platform": surface.get("platform") or "tg",
        "surface_external_id": surface.get("external_id"),
        "context_url": context_url,
        "context_external_id": str(msg_id) if msg_id else None,
        "context_created_at": date.astimezone(timezone.utc).isoformat() if hasattr(date, "astimezone") else None,
        "context_text_snippet": " ".join(text.split())[:500],
        "matched_intent": "event_recommendation_question",
        "topic_cluster": "local_events",
        "event_ids": [],
        "candidate_events": [],
        "link_target": {
            "kind": "pka_channel",
            "url": default_target_url,
            "label": "Полюбить Калининград Анонсы",
            "reason": "shadow discovery found a contextual event question",
        },
        "fallback_link_target": {"kind": "pka_channel", "url": default_target_url, "label": "Полюбить Калининград Анонсы"},
        "reach": {"low": 5, "confidence": "low", "formula": "shadow_group_low"},
        "scores": {"relevance": 0.55, "spam_risk": "low", "safety_risk": "low", "source": "deterministic_shadow_prefilter"},
        "sticker_observation": {
            "fit": "possible" if sticker_possible else "weak",
            "stickers_seen": 1 if sticker_possible else 0,
            "reason": "shadow prefilter only; needs LLM review before any reply",
        },
    }



VK_READ_METHODS = {"wall.get", "wall.getComments"}


def _vk_api(method: str, *, token: str, params: dict[str, Any]) -> dict[str, Any]:
    if method not in VK_READ_METHODS:
        raise RuntimeError(f"forbidden VK method in acquisition shadow scanner: {method}")
    import requests

    payload = dict(params)
    payload.setdefault("access_token", token)
    payload.setdefault("v", os.getenv("ACQ_VK_API_VERSION") or "5.199")
    response = requests.get(f"https://api.vk.com/method/{method}", params=payload, timeout=20)
    response.raise_for_status()
    data = response.json()
    if data.get("error"):
        raise RuntimeError(f"VK {method} error: {data['error']}")
    return data.get("response") or {}


def build_vk_opportunity(surface: dict[str, Any], *, owner_id: int, post_id: int, comment: dict[str, Any], default_target_url: str) -> dict[str, Any] | None:
    text = str(comment.get("text") or "").strip()
    if not text or not _OPPORTUNITY_RE.search(text):
        return None
    comment_id = int(comment.get("id") or 0)
    context_url = f"https://vk.com/wall{owner_id}_{post_id}"
    if comment_id:
        context_url += f"?reply={comment_id}"
    attachments = comment.get("attachments") or []
    sticker_possible = any((a.get("type") == "sticker") for a in attachments if isinstance(a, dict))
    return {
        "platform": "vk",
        "surface_external_id": surface.get("external_id"),
        "context_url": context_url,
        "context_external_id": f"{post_id}:{comment_id}" if comment_id else str(post_id),
        "context_created_at": datetime.fromtimestamp(int(comment.get("date") or 0), tz=timezone.utc).isoformat() if comment.get("date") else None,
        "context_text_snippet": " ".join(text.split())[:500],
        "matched_intent": "event_recommendation_question",
        "topic_cluster": "local_events",
        "event_ids": [],
        "candidate_events": [],
        "link_target": {
            "kind": "pka_channel",
            "url": default_target_url,
            "label": "Полюбить Калининград Анонсы",
            "reason": "VK shadow discovery found a contextual event question",
        },
        "fallback_link_target": {"kind": "pka_channel", "url": default_target_url, "label": "Полюбить Калининград Анонсы"},
        "reach": {"low": 3, "confidence": "low", "formula": "vk_comment_thread_low"},
        "scores": {"relevance": 0.5, "spam_risk": "low", "safety_risk": "low", "source": "deterministic_shadow_prefilter"},
        "sticker_observation": {
            "fit": "possible" if sticker_possible else "weak",
            "stickers_seen": 1 if sticker_possible else 0,
            "reason": "VK shadow prefilter only; needs LLM review before any reply",
        },
    }


def scan_vk_shadow_surfaces(seed_urls: list[str], allowlist: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Read-only VK discovery for explicitly allowlisted communities.

    Uses only `wall.get` and `wall.getComments`; no VK wall/comment/message
    write methods, joins, or personal-wall expansion are available here.
    """
    diagnostics: list[str] = []
    token = (os.getenv("VK_ACCESS_TOKEN") or os.getenv("VK_ACCESS_TOKEN4") or "").strip()
    allowed = {str(x).strip().lower() for x in allowlist if str(x).strip()}
    if not seed_urls or not allowed:
        return [], [], diagnostics
    if not token:
        return [], [], ["VK allowlist is non-empty but VK token is not configured; emitted VK seeds only"]
    max_posts = int(os.getenv("ACQ_MAX_VK_POSTS_PER_SURFACE") or os.getenv("ACQ_MAX_MESSAGES_PER_SURFACE") or "10")
    max_comments = int(os.getenv("ACQ_MAX_VK_COMMENTS_PER_POST") or os.getenv("ACQ_MAX_THREADS_PER_SURFACE") or "15")
    default_target_url = (os.getenv("ACQ_DEFAULT_LINK_TARGET_URL") or "https://t.me/kenigevents").strip()
    surfaces: dict[str, dict[str, Any]] = {}
    opportunities: list[dict[str, Any]] = []
    for raw_url in seed_urls:
        normalized = str(raw_url or "").strip()
        if not normalized or normalized.lower() not in allowed:
            continue
        domain = _handle_from_url(normalized, platform="vk")
        surface = _seed_surface(f"https://vk.com/{domain}", platform="vk")
        surface.update({
            "surface_type": "community",
            "status": "approved",
            "source": "allowlist",
            "risk": {"spam_risk": "unknown", "safety_risk": "low", "reason": "read-only VK wall/comment scan"},
            "reach": {"confidence": "low", "basis": "vk_wall"},
        })
        surfaces[surface["external_id"]] = surface
        try:
            wall = _vk_api("wall.get", token=token, params={"domain": domain, "count": max_posts})
        except Exception as exc:
            diagnostics.append(f"vk {domain}: wall.get failed: {exc}")
            continue
        for post in wall.get("items") or []:
            owner_id = int(post.get("owner_id") or 0)
            post_id = int(post.get("id") or 0)
            post_text = str(post.get("text") or "")
            for discovered in extract_candidate_surfaces(post_text):
                discovered["topic_hint"] = f"discovered in vk:{domain}"
                surfaces.setdefault(discovered["external_id"], discovered)
            if not owner_id or not post_id:
                continue
            try:
                comments = _vk_api("wall.getComments", token=token, params={"owner_id": owner_id, "post_id": post_id, "count": max_comments, "need_likes": 1})
            except Exception as exc:
                diagnostics.append(f"vk {domain}: wall.getComments {post_id} failed: {exc}")
                continue
            for comment in comments.get("items") or []:
                if not isinstance(comment, dict):
                    continue
                for discovered in extract_candidate_surfaces(str(comment.get("text") or "")):
                    discovered["topic_hint"] = f"discovered in vk:{domain} comments"
                    surfaces.setdefault(discovered["external_id"], discovered)
                opp = build_vk_opportunity(surface, owner_id=owner_id, post_id=post_id, comment=comment, default_target_url=default_target_url)
                if opp:
                    opp["evidence"] = {"relation": "vk_comment", "scanner": "vk_shadow"}
                    opportunities.append(opp)
                if len(opportunities) >= int(os.getenv("ACQ_MAX_OPPORTUNITIES_PER_RUN") or "30"):
                    break
    return list(surfaces.values()), opportunities, diagnostics

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



async def scan_telegram_shadow_surfaces(seed_urls: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Read-only Telegram discovery.

    The function never sends messages, never joins private chats, and only calls
    public read APIs. If credentials/dependencies are absent it returns seed-only
    diagnostics so `/acq_run` can still import a safe shadow payload.
    """
    diagnostics: list[str] = []
    env_info = _load_kaggle_env()
    if env_info.get("config_error"):
        diagnostics.append(f"config load warning: {env_info['config_error']}")
    if env_info.get("secrets_error"):
        diagnostics.append(f"secrets load warning: {env_info['secrets_error']}")
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        from telethon.tl.functions.channels import GetFullChannelRequest
    except Exception as exc:
        diagnostics.append(f"telethon unavailable: {exc}")
        return [], [], diagnostics

    api_id = (os.getenv("TG_API_ID") or os.getenv("TELEGRAM_API_ID") or "").strip()
    api_hash = (os.getenv("TG_API_HASH") or os.getenv("TELEGRAM_API_HASH") or "").strip()
    try:
        session_string, device_config = _decode_tg_auth()
    except Exception as exc:
        diagnostics.append(f"telegram auth bundle decode failed: {exc}")
        return [], [], diagnostics
    if not session_string or not api_id or not api_hash:
        diagnostics.append("telegram credentials are not configured; emitted seed-only shadow payload")
        return [], [], diagnostics

    max_surfaces = int(os.getenv("ACQ_MAX_SURFACES_PER_RUN") or "5")
    max_messages = int(os.getenv("ACQ_MAX_MESSAGES_PER_SURFACE") or "25")
    max_threads = int(os.getenv("ACQ_MAX_THREADS_PER_SURFACE") or "5")
    default_target_url = (os.getenv("ACQ_DEFAULT_LINK_TARGET_URL") or "https://t.me/kenigevents").strip()
    surfaces: dict[str, dict[str, Any]] = {}
    opportunities: list[dict[str, Any]] = []

    async with TelegramClient(StringSession(session_string), int(api_id), api_hash, flood_sleep_threshold=60, **device_config) as client:
        for index, raw_url in enumerate(seed_urls[:max_surfaces], start=1):
            handle = _handle_from_url(raw_url)
            if not handle:
                diagnostics.append(f"skip invalid tg seed: {raw_url}")
                continue
            progress = {
                "progress_percent": min(90, 20 + int(index / max(1, len(seed_urls[:max_surfaces])) * 60)),
                "progress_label": f"telegram {index}/{min(len(seed_urls), max_surfaces)}",
                "surfaces_done": index - 1,
                "surfaces_total": min(len(seed_urls), max_surfaces),
                "opportunities_found": len(opportunities),
            }
            _status_event("alive", phase="telegram_scan", status="running", progress=progress)
            try:
                entity = await client.get_entity(handle)
            except Exception as exc:
                diagnostics.append(f"{handle}: get_entity failed: {exc}")
                continue
            surface_type = "channel" if bool(getattr(entity, "broadcast", False)) else "group" if bool(getattr(entity, "megagroup", False)) else "chat"
            surface = _seed_surface(f"https://t.me/{handle}", platform="tg")
            surface.update({
                "surface_type": surface_type,
                "title": getattr(entity, "title", None),
                "status": "candidate",
                "reach": {"members": getattr(entity, "participants_count", None), "confidence": "low", "basis": "telegram_entity"},
                "risk": {"spam_risk": "unknown", "safety_risk": "low", "reason": "read-only public scan"},
            })
            surfaces[surface["external_id"]] = surface

            scan_entities: list[tuple[Any, dict[str, Any], str | None]] = [(entity, surface, None)]
            if surface_type == "channel":
                try:
                    full = await client(GetFullChannelRequest(entity))
                    linked_chat_id = getattr(getattr(full, "full_chat", None), "linked_chat_id", None)
                    if linked_chat_id:
                        linked = await client.get_entity(linked_chat_id)
                        linked_handle = getattr(linked, "username", None) or str(linked_chat_id)
                        linked_surface = _seed_surface(f"https://t.me/{linked_handle}", platform="tg")
                        linked_surface.update({
                            "surface_type": "linked_discussion",
                            "title": getattr(linked, "title", None),
                            "source": "linked_discussion",
                            "topic_hint": f"linked discussion for {handle}",
                            "risk": {"spam_risk": "unknown", "safety_risk": "low", "reason": "read-only linked discussion scan"},
                        })
                        surfaces[linked_surface["external_id"]] = linked_surface
                        scan_entities.append((linked, linked_surface, "linked_discussion"))
                except Exception as exc:
                    diagnostics.append(f"{handle}: linked discussion lookup failed: {exc}")

            for scan_entity, scan_surface, relation in scan_entities:
                seen = 0
                try:
                    async for message in client.iter_messages(scan_entity, limit=max_messages):
                        seen += 1
                        text = str(getattr(message, "message", None) or "")
                        for discovered in extract_candidate_surfaces(text):
                            discovered["topic_hint"] = f"discovered in {scan_surface.get('external_id')}"
                            surfaces.setdefault(discovered["external_id"], discovered)
                        opp = build_opportunity_from_message(scan_surface, message, default_target_url=default_target_url)
                        if opp:
                            opp["evidence"] = {"relation": relation or "surface", "scanner": "telegram_shadow"}
                            opportunities.append(opp)
                        if len(opportunities) >= int(os.getenv("ACQ_MAX_OPPORTUNITIES_PER_RUN") or "30"):
                            break
                except Exception as exc:
                    diagnostics.append(f"{handle}: iter_messages failed: {exc}")
                if relation == "linked_discussion" and seen > max_threads:
                    diagnostics.append(f"{handle}: linked discussion scan capped at {max_threads} threads")
    return list(surfaces.values()), opportunities, diagnostics


def build_shadow_payload(*, scanned_surfaces: list[dict[str, Any]] | None = None, scanned_opportunities: list[dict[str, Any]] | None = None, diagnostics: list[str] | None = None) -> dict[str, Any]:
    tg_seeds = _json_env("ACQ_TG_SEEDS_JSON", DEFAULT_TG_SEEDS)
    vk_seeds = _json_env("ACQ_VK_SEEDS_JSON", [])
    vk_allowlist = _json_env("ACQ_VK_ALLOWLIST_JSON", [])
    surfaces_by_external: dict[str, dict[str, Any]] = {}
    for url in list(tg_seeds or []):
        seed = _seed_surface(str(url), platform="tg")
        surfaces_by_external[seed["external_id"]] = seed
    for scanned in scanned_surfaces or []:
        if scanned.get("external_id"):
            surfaces_by_external[str(scanned["external_id"])] = scanned
    # VK-ready but disabled unless explicit allowlist is provided.
    allowed_vk = {str(x).strip().lower() for x in list(vk_allowlist or []) if str(x).strip()}
    for url in list(vk_seeds or []):
        normalized = str(url).strip()
        if not normalized:
            continue
        if normalized.lower() not in allowed_vk:
            seed = {**_seed_surface(normalized, platform="vk"), "status": "candidate"}
            surfaces_by_external[seed["external_id"]] = seed
            continue
        seed = {**_seed_surface(normalized, platform="vk"), "status": "approved"}
        surfaces_by_external[seed["external_id"]] = seed
    return {
        "run_id": os.getenv("KAGGLE_RUN_ID") or f"acq-shadow-{int(time.time())}",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "surfaces": list(surfaces_by_external.values()),
        "opportunities": list(scanned_opportunities or []),
        "stats": {
            "mode": "telegram_shadow_scan" if scanned_surfaces or scanned_opportunities else "shadow_preflight",
            "surfaces_total": len(surfaces_by_external),
            "telegram_seeds": len(list(tg_seeds or [])),
            "vk_seeds": len(list(vk_seeds or [])),
            "vk_allowlist": len(allowed_vk),
            "telegram_live_scan_enabled": _truthy_env("ACQ_ENABLE_LIVE_TG_SCAN", False),
            "vk_allowlist_scan_enabled": bool(allowed_vk),
            "external_sends": 0,
            "comments_posted": 0,
            "stickers_sent": 0,
        },
        "diagnostics": list(diagnostics or []) + [
            "Runtime is safe/read-only: no Telegram/VK send/comment/post/join methods are called."
        ],
    }


def main() -> None:
    _status_event("kernel_started", phase="bootstrap", status="running", progress={"progress_percent": 1, "progress_label": "bootstrap"})
    scanned_surfaces: list[dict[str, Any]] = []
    scanned_opportunities: list[dict[str, Any]] = []
    diagnostics: list[str] = []
    if _truthy_env("ACQ_ENABLE_LIVE_TG_SCAN", False):
        try:
            tg_seeds = [str(x) for x in list(_json_env("ACQ_TG_SEEDS_JSON", DEFAULT_TG_SEEDS) or [])]
            tg_surfaces, tg_opportunities, tg_diagnostics = asyncio.run(scan_telegram_shadow_surfaces(tg_seeds))
            scanned_surfaces.extend(tg_surfaces)
            scanned_opportunities.extend(tg_opportunities)
            diagnostics.extend(tg_diagnostics)
        except Exception as exc:
            diagnostics.append(f"telegram shadow scan failed: {exc}")
            logger.exception("telegram shadow scan failed")
    vk_seeds = [str(x) for x in list(_json_env("ACQ_VK_SEEDS_JSON", []) or [])]
    vk_allowlist = [str(x) for x in list(_json_env("ACQ_VK_ALLOWLIST_JSON", []) or [])]
    if vk_allowlist:
        try:
            vk_surfaces, vk_opportunities, vk_diagnostics = scan_vk_shadow_surfaces(vk_seeds, vk_allowlist)
            scanned_surfaces.extend(vk_surfaces)
            scanned_opportunities.extend(vk_opportunities)
            diagnostics.extend(vk_diagnostics)
        except Exception as exc:
            diagnostics.append(f"vk shadow scan failed: {exc}")
            logger.exception("vk shadow scan failed")
    payload = build_shadow_payload(scanned_surfaces=scanned_surfaces, scanned_opportunities=scanned_opportunities, diagnostics=diagnostics)
    _status_event(
        "preflight_ok",
        phase="preflight",
        status="running",
        progress={
            "progress_percent": 20,
            "progress_label": f"surfaces {len(payload['surfaces'])}",
            "surfaces_total": len(payload["surfaces"]),
            "opportunities_found": len(payload.get("opportunities") or []),
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
            "opportunities_found": len(payload.get("opportunities") or []),
            "output": str(output_path),
        },
        message=f"Wrote {output_path}",
    )
    logger.info("wrote %s", output_path)


if __name__ == "__main__":
    main()
