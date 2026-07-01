from __future__ import annotations

import asyncio
import base64
import importlib.util
import json
import re
import logging
import os
import random
import subprocess
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


def ensure_libs() -> None:
    """Install the tiny read-only scanner dependencies on Kaggle if absent.

    Mirrors the TelegramMonitor notebook pattern: Kaggle base images do not
    always include Telethon, while local/E2E venvs normally do.
    """
    modules = [("telethon", "telethon")]
    missing: list[str] = []
    for module_name, package_name in modules:
        try:
            __import__(module_name)
        except Exception:
            missing.append(package_name)
    if missing:
        print(f"Installing Python packages: {', '.join(missing)}", flush=True)
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", *missing])


ensure_libs()

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
_VK_WALL_RE = re.compile(r"(?i)^wall-(?P<group_id>\d+)_\d+$")
_EVENT_INTENT_RE = re.compile(
    r"(?i)\b("
    r"что\s+(?:посетить|посмотреть|выбрать)|"
    r"(?:посоветуй|посоветуйте|порекомендуй|порекомендуйте)\b.*(?:сходить|пойти|афиш|концерт|выставк|спектакл|мероприят|событи)|"
    r"афиша(?:\s+на\s+(?:выходные|сегодня|завтра))?|"
    r"куда\s+(?:сходить|пойти)(?:\b|.*(?:выходн|с\s+детьми|детям|концерт|выставк|спектакл))|"
    r"где\s+(?:найти|посмотреть|послушать)?\s*(?:афишу?|концерт|выставк|спектакл|мероприят)|"
    r"(?:на\s+выходные|с\s+детьми|детям)\b.*(?:куда|что\s+посмотреть|что\s+посетить)"
    r")"
)
_SITE_SEARCH_INTENT_RE = re.compile(
    r"(?i)\b("
    r"(?:есть|где|какой)\s+(?:сайт|поиск|каталог|календарь|подборк).*?(?:афиш|событи|мероприят|концерт|выставк)|"
    r"(?:поиск|каталог|календарь|подборк|популярное|топ)\s+(?:событи|мероприят|афиш|концерт|выставк)|"
    r"(?:все|найти|посмотреть)\s+(?:выставки|концерты|спектакли|события|мероприятия)\b"
    r")"
)
_PARTNER_INTENT_RE = re.compile(
    r"(?i)\b("
    r"(?:как|куда|где)\s+(?:добавить|прислать|отправить|разместить|опубликовать)\s+(?:афишу|анонс|мероприятие|событие)|"
    r"(?:добавить|разместить|опубликовать)\s+(?:афишу|анонс|мероприятие|событие)|"
    r"(?:инфо[-\s]?партн[её]р|информационн(?:ое|ым)\s+партн[её]р)"
    r")"
)
_REQUEST_HINT_RE = re.compile(r"(?i)(\?|\b(?:подскажите|подскажи|где|как|куда|ищу|нуж(?:ен|на|но)|интересует|можно\s+ли|поиск|найти|посмотреть)\b)")
_TRIP_ROUTE_INTENT_RE = re.compile(
    r"(?i)\b("
    r"(?:куда|как)\s+(?:съездить|поехать|прокатиться)(?:\b|.*(?:на\s+день|за\s+день|из\s+калининграда|на\s+электричк|маршрут|выходн))|"
    r"(?:что|где)\s+(?:посмотреть|посетить).*?(?:в|во|на)\s+(?:зеленоградск|светлогорск|балтийск|янтарн|черняховск|советск|гусев|георгенбург)|"
    r"(?:однодневн(?:ая|ый|ое)|на\s+один\s+день|за\s+день)\b.*(?:маршрут|поездк|съездить|поехать)|"
    r"(?:маршрут|поездк[аи]|трип|trip)\b.*(?:зеленоградск|светлогорск|балтийск|янтарн|черняховск|советск|гусев|георгенбург|электричк|пригородн|из\s+калининграда)|"
    r"(?:электричк|пригородн(?:ый|ая)\s+поезд)\b.*(?:куда|маршрут|съездить|поехать|выходн)"
    r")"
)
_BADGE_FILTER_INTENT_RE = re.compile(
    r"(?i)\b("
    r"пушкинск(?:ая|ой)\s+карт|по\s+пушкинской|"
    r"(?:для\s+детей|детям|семейн(?:ое|ые|ый)|с\s+детьми)|"
    r"благотворительн|"
    r"(?:будет|есть)\s+(?:запись|трансляция)|онлайн[-\s]?(?:трансляц|показ)|"
    r"(?:бесплатн|свободный\s+вход)"
    r")"
)
_STICKER_HINT_RE = re.compile(r"(?i)\b(стикер|sticker|😂|👍|🔥|🤣|❤️|❤|👏)")
_KGD_REGION_HINT_RE = re.compile(
    r"(?i)(калининград|к[её]ниг|kenig|kgd|\b39\b|светлогорск|svetlogorsk|зеленоградск|zelenogradsk|"
    r"балтийск|baltiysk|гурьевск|guryevsk|черняховск|chernyakhovsk|советск|sovetsk|янтарн|yantarn|"
    r"гусев|gusev|гвардейск|gvardeysk|багратионовск|мамоново|неман|пионерск|полесск|правдинск|славск)"
)
_OUT_OF_REGION_HINT_RE = re.compile(
    r"(?i)(navahrudak|novogrud|новогруд|минск|minsk|гродно|grodno|брест|brest|витебск|vitebsk|"
    r"гомель|gomel|могил[её]в|mogilev|москва|moscow|петербург|spb|казань|kazan|челябинск|chelyabinsk)"
)


def _int_env(name: str, default: int, *, min_value: int = 0) -> int:
    try:
        return max(min_value, int(os.getenv(name) or default))
    except Exception:
        return max(min_value, int(default))


def _float_env(name: str, default: float, *, min_value: float = 0.0, max_value: float | None = None) -> float:
    try:
        value = float(os.getenv(name) or default)
    except Exception:
        value = float(default)
    value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def _human_delay_bounds() -> tuple[float, float]:
    min_s = _float_env("ACQ_HUMAN_DELAY_MIN_SECONDS", 0.35, min_value=0.0, max_value=5.0)
    max_s = _float_env("ACQ_HUMAN_DELAY_MAX_SECONDS", 1.4, min_value=min_s, max_value=8.0)
    return min_s, max_s


async def _human_pause_async(*, multiplier: float = 1.0) -> None:
    min_s, max_s = _human_delay_bounds()
    if max_s <= 0:
        return
    await asyncio.sleep(random.uniform(min_s, max_s) * max(0.0, multiplier))


def _human_pause_sync(*, multiplier: float = 1.0) -> None:
    min_s, max_s = _human_delay_bounds()
    if max_s <= 0:
        return
    time.sleep(random.uniform(min_s, max_s) * max(0.0, multiplier))


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
                key_text = str(key)
                if not isinstance(value, (str, int, float, bool)):
                    continue
                # The encrypted per-run config is the canonical source for ACQ_*
                # budgets/feature flags.  Kaggle images or notebook metadata may
                # carry stale defaults, so do not let them silently disable the
                # selected scan mode.
                if key_text.startswith("ACQ_") or os.getenv(key_text) is None:
                    os.environ[key_text] = str(value)
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


def _surface_region_text(surface: dict[str, Any]) -> str:
    return " ".join(str(surface.get(key) or "") for key in ["url", "handle", "title", "topic_hint", "external_id"])


def _is_out_of_region_surface(surface: dict[str, Any]) -> bool:
    text = _surface_region_text(surface)
    if not text:
        return False
    # A positive Kaliningrad signal wins over broad foreign/city words in titles,
    # but pure out-of-region handles such as visitNavahrudak are rejected early.
    return bool(_OUT_OF_REGION_HINT_RE.search(text) and not _KGD_REGION_HINT_RE.search(text))


def _mark_out_of_region_surface(surface: dict[str, Any], *, reason: str = "deterministic_region_filter") -> dict[str, Any]:
    updated = dict(surface)
    updated["status"] = "rejected_out_of_region"
    updated["topic_cluster"] = "out_of_region"
    updated["risk"] = {
        "spam_risk": "high",
        "safety_risk": "low",
        "level": "rejected",
        "reason": reason,
    }
    return updated


def _static_site_base_url() -> str:
    return (os.getenv("ACQ_STATIC_SITE_BASE_URL") or "https://kenigevents.ru").strip().rstrip("/")


def _classify_acq_intent(text: str) -> dict[str, Any] | None:
    compact = " ".join(str(text or "").split())
    if not compact:
        return None
    base = _static_site_base_url()
    if _TRIP_ROUTE_INTENT_RE.search(compact):
        return {
            "matched_intent": "trip_route_recommendation_context",
            "topic_cluster": "trip_route_recommendation",
            "target_url": None,
            "target_kind": "other",
            "target_label": "Конкретный маршрут из базы маршрутов",
            "fallback_url": None,
            "reason": "trip-recomendation requirements: recommend a concrete route where the discussion context makes it useful",
            "relevance": 0.64,
        }
    if _PARTNER_INTENT_RE.search(compact):
        return {
            "matched_intent": "organizer_submission_or_partnership",
            "topic_cluster": "organizer_partnership",
            "target_url": os.getenv("ACQ_PARTNER_PAGE_URL") or f"{base}/partnerstvo/",
            "target_kind": "topic_landing",
            "target_label": "Добавить событие / инфопартнёрство",
            "reason": "question about adding or publishing an event announcement",
            "relevance": 0.62,
        }
    if _BADGE_FILTER_INTENT_RE.search(compact) and _REQUEST_HINT_RE.search(compact):
        return {
            "matched_intent": "event_badge_or_filter_request",
            "topic_cluster": "event_badges_filters",
            "target_url": os.getenv("ACQ_SEARCH_PAGE_URL") or f"{base}/poisk/",
            "target_kind": "topic_landing",
            "target_label": "Поиск событий с быстрыми признаками",
            "reason": "question matches recent site badge/filter features: Pushkin card, kids/family, charity, recording, free",
            "relevance": 0.58,
        }
    if _SITE_SEARCH_INTENT_RE.search(compact):
        url = os.getenv("ACQ_SEARCH_PAGE_URL") or f"{base}/poisk/"
        label = "Поиск событий KenigEvents"
        if re.search(r"(?i)\bвыставк", compact):
            url = os.getenv("ACQ_EXHIBITIONS_PAGE_URL") or f"{base}/vystavki/"
            label = "Выставки KenigEvents"
        elif re.search(r"(?i)\b(?:популярное|топ)", compact):
            url = os.getenv("ACQ_POPULAR_PAGE_URL") or f"{base}/populyarnoe/"
            label = "Популярное KenigEvents"
        return {
            "matched_intent": "event_site_search_or_listing",
            "topic_cluster": "event_site_search",
            "target_url": url,
            "target_kind": "topic_landing",
            "target_label": label,
            "reason": "question matches recent static-site search/listing pages",
            "relevance": 0.6,
        }
    if _EVENT_INTENT_RE.search(compact):
        return {
            "matched_intent": "event_recommendation_question",
            "topic_cluster": "local_events",
            "target_url": os.getenv("ACQ_DEFAULT_LINK_TARGET_URL") or "https://t.me/kenigevents",
            "target_kind": "pka_channel",
            "target_label": "Полюбить Калининград Анонсы",
            "reason": "contextual local event recommendation question",
            "relevance": 0.55,
        }
    return None


def _is_surface_scan_candidate(surface: dict[str, Any]) -> bool:
    return not str(surface.get("status") or "").startswith("rejected")


def _vk_surface_from_handle(handle: str) -> dict[str, Any] | None:
    clean = str(handle or "").strip().strip("/")
    if not clean:
        return None
    wall = _VK_WALL_RE.match(clean)
    if wall:
        clean = f"club{wall.group('group_id')}"
    if clean.lower().startswith(("wall", "photo", "video", "topic", "im")):
        return None
    return _seed_surface(f"https://vk.com/{clean}", platform="vk") | {"source": "discovered"}


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
            surface = _seed_surface(f"https://t.me/{handle}", platform="tg") | {"source": "discovered"}
            if _is_out_of_region_surface(surface):
                surface = _mark_out_of_region_surface(surface, reason="out-of-region Telegram handle discovered in scanned text")
            out[f"tg:{handle}"] = surface
        elif vk:
            surface = _vk_surface_from_handle(vk.group("handle"))
            if surface:
                if _is_out_of_region_surface(surface):
                    surface = _mark_out_of_region_surface(surface, reason="out-of-region VK handle discovered in scanned text")
                out[str(surface["external_id"])] = surface
    return list(out.values())


def build_opportunity_from_message(surface: dict[str, Any], message: Any, *, default_target_url: str) -> dict[str, Any] | None:
    text = str(getattr(message, "message", None) or getattr(message, "text", None) or "").strip()
    intent = _classify_acq_intent(text)
    if not intent:
        return None
    msg_id = int(getattr(message, "id", 0) or 0)
    url = surface.get("url") or ""
    handle = surface.get("handle") or _handle_from_url(url)
    if surface.get("platform") == "tg" and handle and msg_id:
        context_url = f"https://t.me/c/{handle}/{msg_id}" if str(handle).isdigit() else f"https://t.me/{handle}/{msg_id}"
    else:
        context_url = url
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
        "matched_intent": intent["matched_intent"],
        "topic_cluster": intent["topic_cluster"],
        "event_ids": [],
        "candidate_events": [],
        "link_target": {
            "kind": intent.get("target_kind") or "topic_landing",
            "url": intent["target_url"] if "target_url" in intent else default_target_url,
            "label": intent.get("target_label") or "Полюбить Калининград Анонсы",
            "reason": intent.get("reason") or "shadow discovery found an acquisition opportunity",
        },
        "fallback_link_target": {"kind": "pka_channel", "url": intent.get("fallback_url", default_target_url), "label": "Полюбить Калининград Анонсы"},
        "reach": {"low": 5, "confidence": "low", "formula": "shadow_group_low"},
        "scores": {"relevance": intent.get("relevance") or 0.55, "spam_risk": "low", "safety_risk": "low", "source": "deterministic_shadow_prefilter"},
        "sticker_observation": {
            "fit": "possible" if sticker_possible else "weak",
            "stickers_seen": 1 if sticker_possible else 0,
            "reason": "shadow prefilter only; needs LLM review before any reply",
        },
    }


def is_comment_opportunity_message(message: Any, *, surface_type: str | None, relation: str | None) -> bool:
    """Return whether a Telegram message is a human comment/reply candidate.

    Acquisition replies should target conversations, not source/channel ad
    posts.  We still read channel posts to discover links and linked discussion
    chats, but candidate opportunities come from group/chat comments only.
    """
    if getattr(message, "post", False):
        return False
    if getattr(message, "fwd_from", None):
        return False
    normalized_surface_type = str(surface_type or "").casefold()
    normalized_relation = str(relation or "").casefold()
    if normalized_surface_type == "channel":
        return False
    if normalized_relation == "linked_discussion" or normalized_surface_type == "linked_discussion":
        return bool(getattr(message, "reply_to", None))
    return normalized_surface_type in {"group", "chat", "megagroup", "linked_discussion"}



VK_READ_METHODS = {"wall.get", "wall.getComments"}


def _vk_token_lanes() -> list[tuple[str, str]]:
    """Return VK read-token lanes without exposing token values.

    `VK_ACCESS_TOKEN4` is the publishing/monitoring lane used elsewhere in this
    project and has historically been less likely to be IP-restricted than the
    generic `VK_ACCESS_TOKEN`, so discovery tries it first and falls back safely.
    """
    lanes: list[tuple[str, str]] = []
    seen: set[str] = set()
    for name in ["VK_ACCESS_TOKEN4", "VK_ACCESS_TOKEN"]:
        token = (os.getenv(name) or "").strip()
        if token and token not in seen:
            lanes.append((name, token))
            seen.add(token)
    return lanes


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


def _vk_api_with_fallback(method: str, *, token_lanes: list[tuple[str, str]], params: dict[str, Any]) -> tuple[dict[str, Any], str]:
    errors: list[str] = []
    for lane_name, token in token_lanes:
        try:
            return _vk_api(method, token=token, params=params), lane_name
        except Exception as exc:
            errors.append(f"{lane_name}: {exc}")
    raise RuntimeError("; ".join(errors) if errors else "no VK token lanes configured")


def build_vk_opportunity(surface: dict[str, Any], *, owner_id: int, post_id: int, comment: dict[str, Any], default_target_url: str) -> dict[str, Any] | None:
    text = str(comment.get("text") or "").strip()
    intent = _classify_acq_intent(text)
    if not intent:
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
        "matched_intent": intent["matched_intent"],
        "topic_cluster": intent["topic_cluster"],
        "event_ids": [],
        "candidate_events": [],
        "link_target": {
            "kind": intent.get("target_kind") or "topic_landing",
            "url": intent["target_url"] if "target_url" in intent else default_target_url,
            "label": intent.get("target_label") or "Полюбить Калининград Анонсы",
            "reason": intent.get("reason") or "VK shadow discovery found an acquisition opportunity",
        },
        "fallback_link_target": {"kind": "pka_channel", "url": intent.get("fallback_url", default_target_url), "label": "Полюбить Калининград Анонсы"},
        "reach": {"low": 3, "confidence": "low", "formula": "vk_comment_thread_low"},
        "scores": {"relevance": intent.get("relevance") or 0.5, "spam_risk": "low", "safety_risk": "low", "source": "deterministic_shadow_prefilter"},
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
    token_lanes = _vk_token_lanes()
    allowed = {str(x).strip().lower() for x in allowlist if str(x).strip()}
    if not seed_urls or not allowed:
        return [], [], diagnostics
    if not token_lanes:
        return [], [], ["VK allowlist is non-empty but VK token is not configured; emitted VK seeds only"]
    max_surfaces = _int_env("ACQ_MAX_VK_SURFACES_PER_RUN", _int_env("ACQ_MAX_SURFACES_PER_RUN", 5, min_value=1), min_value=1)
    max_posts = _int_env("ACQ_MAX_VK_POSTS_PER_SURFACE", _int_env("ACQ_MAX_MESSAGES_PER_SURFACE", 10, min_value=1), min_value=1)
    max_comments = _int_env("ACQ_MAX_VK_COMMENTS_PER_POST", _int_env("ACQ_MAX_THREADS_PER_SURFACE", 15, min_value=1), min_value=1)
    default_target_url = (os.getenv("ACQ_DEFAULT_LINK_TARGET_URL") or "https://t.me/kenigevents").strip()
    surfaces: dict[str, dict[str, Any]] = {}
    opportunities: list[dict[str, Any]] = []
    for raw_url in seed_urls[:max_surfaces]:
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
            wall, wall_lane = _vk_api_with_fallback("wall.get", token_lanes=token_lanes, params={"domain": domain, "count": max_posts})
            diagnostics.append(f"vk {domain}: wall.get ok via {wall_lane}")
        except Exception as exc:
            diagnostics.append(f"vk {domain}: wall.get failed: {exc}")
            continue
        _human_pause_sync(multiplier=0.6)
        for post in wall.get("items") or []:
            owner_id = int(post.get("owner_id") or 0)
            post_id = int(post.get("id") or 0)
            post_text = str(post.get("text") or "")
            for discovered in extract_candidate_surfaces(post_text):
                discovered["topic_hint"] = f"discovered in vk:{domain}"
                if _is_out_of_region_surface(discovered):
                    discovered = _mark_out_of_region_surface(discovered, reason="out-of-region surface discovered in VK post")
                surfaces.setdefault(discovered["external_id"], discovered)
            if not owner_id or not post_id:
                continue
            try:
                comments, comments_lane = _vk_api_with_fallback("wall.getComments", token_lanes=token_lanes, params={"owner_id": owner_id, "post_id": post_id, "count": max_comments, "need_likes": 1})
                if comments_lane != wall_lane:
                    diagnostics.append(f"vk {domain}: wall.getComments {post_id} ok via {comments_lane}")
            except Exception as exc:
                diagnostics.append(f"vk {domain}: wall.getComments {post_id} failed: {exc}")
                continue
            _human_pause_sync(multiplier=0.35)
            for comment in comments.get("items") or []:
                if not isinstance(comment, dict):
                    continue
                for discovered in extract_candidate_surfaces(str(comment.get("text") or "")):
                    discovered["topic_hint"] = f"discovered in vk:{domain} comments"
                    if _is_out_of_region_surface(discovered):
                        discovered = _mark_out_of_region_surface(discovered, reason="out-of-region surface discovered in VK comments")
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



def _tg_queue_key(url: str) -> str:
    handle = _handle_from_url(url, platform="tg")
    return handle.casefold() if handle else str(url or "").strip().rstrip("/").casefold()


def _enqueue_tg_url(
    queue: list[str],
    queued: set[str],
    url: str,
    *,
    limit: int,
) -> bool:
    if len(queued) >= limit:
        return False
    key = _tg_queue_key(url)
    if not key or key in queued:
        return False
    queued.add(key)
    queue.append(url)
    return True


async def scan_telegram_shadow_surfaces(seed_urls: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Read-only Telegram discovery with bounded frontier walk.

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

    max_surfaces = _int_env("ACQ_MAX_SURFACES_PER_RUN", 5, min_value=1)
    max_frontier = _int_env("ACQ_MAX_TG_FRONTIER_PER_RUN", max(20, max_surfaces * 4), min_value=max_surfaces)
    max_messages = _int_env("ACQ_MAX_MESSAGES_PER_SURFACE", 25, min_value=1)
    max_threads = _int_env("ACQ_MAX_THREADS_PER_SURFACE", 5, min_value=1)
    max_opportunities = _int_env("ACQ_MAX_OPPORTUNITIES_PER_RUN", 30, min_value=1)
    default_target_url = (os.getenv("ACQ_DEFAULT_LINK_TARGET_URL") or "https://t.me/kenigevents").strip()
    surfaces: dict[str, dict[str, Any]] = {}
    opportunities: list[dict[str, Any]] = []
    queue: list[str] = []
    queued: set[str] = set()
    for raw in seed_urls:
        _enqueue_tg_url(queue, queued, str(raw), limit=max_frontier)
    discovered_queued = 0
    processed = 0

    async with TelegramClient(StringSession(session_string), int(api_id), api_hash, flood_sleep_threshold=60, **device_config) as client:
        while queue and processed < max_surfaces:
            raw_url = queue.pop(0)
            handle = _handle_from_url(raw_url)
            if not handle:
                diagnostics.append(f"skip invalid tg seed: {raw_url}")
                continue
            processed += 1
            progress = {
                "progress_percent": min(90, 20 + int(processed / max(1, max_surfaces) * 60)),
                "progress_label": f"telegram {processed}/{max_surfaces} · frontier +{discovered_queued}",
                "surfaces_done": processed - 1,
                "surfaces_total": max_surfaces,
                "tg_frontier_queued": discovered_queued,
                "opportunities_found": len(opportunities),
            }
            _status_event("alive", phase="telegram_scan", status="running", progress=progress)
            await _human_pause_async(multiplier=0.8 if processed > 1 else 0.2)
            try:
                entity = await client.get_entity(int(handle) if str(handle).isdigit() else handle)
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
            if _is_out_of_region_surface(surface):
                surface = _mark_out_of_region_surface(surface, reason="resolved Telegram title/handle is outside Kaliningrad Oblast")
                surfaces[surface["external_id"]] = surface
                diagnostics.append(f"{handle}: rejected out-of-region surface")
                continue
            surfaces[surface["external_id"]] = surface

            scan_entities: list[tuple[Any, dict[str, Any], str | None]] = [(entity, surface, None)]
            if surface_type == "channel":
                try:
                    full = await client(GetFullChannelRequest(entity))
                    linked_chat_id = getattr(getattr(full, "full_chat", None), "linked_chat_id", None)
                    if linked_chat_id:
                        linked = await client.get_entity(linked_chat_id)
                        linked_handle = getattr(linked, "username", None) or str(linked_chat_id)
                        linked_url = f"https://t.me/{linked_handle}" if getattr(linked, "username", None) else f"https://t.me/c/{linked_handle}"
                        linked_surface = _seed_surface(linked_url, platform="tg")
                        linked_surface.update({
                            "surface_type": "linked_discussion",
                            "title": getattr(linked, "title", None),
                            "source": "linked_discussion",
                            "topic_hint": f"linked discussion for {handle}",
                            "risk": {"spam_risk": "unknown", "safety_risk": "low", "reason": "read-only linked discussion scan"},
                        })
                        if _is_out_of_region_surface(linked_surface):
                            linked_surface = _mark_out_of_region_surface(linked_surface, reason="linked discussion title/handle is outside Kaliningrad Oblast")
                            surfaces[linked_surface["external_id"]] = linked_surface
                            diagnostics.append(f"{linked_handle}: rejected out-of-region linked discussion")
                        else:
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
                            if _is_out_of_region_surface(discovered):
                                discovered = _mark_out_of_region_surface(discovered, reason="out-of-region surface discovered in Telegram message")
                            surfaces.setdefault(discovered["external_id"], discovered)
                            if (
                                discovered.get("platform") == "tg"
                                and _is_surface_scan_candidate(discovered)
                                and _enqueue_tg_url(queue, queued, str(discovered.get("url") or ""), limit=max_frontier)
                            ):
                                discovered_queued += 1
                        if is_comment_opportunity_message(
                            message,
                            surface_type=str(scan_surface.get("surface_type") or ""),
                            relation=relation,
                        ):
                            opp = build_opportunity_from_message(scan_surface, message, default_target_url=default_target_url)
                            if opp:
                                opp["evidence"] = {"relation": relation or "surface", "scanner": "telegram_shadow", "comment_only": True}
                                opportunities.append(opp)
                        if len(opportunities) >= max_opportunities:
                            break
                        if seen % 8 == 0:
                            await _human_pause_async(multiplier=0.25)
                        if relation == "linked_discussion" and seen >= max_threads:
                            break
                except Exception as exc:
                    diagnostics.append(f"{handle}: iter_messages failed: {exc}")
                if relation == "linked_discussion" and seen >= max_threads:
                    diagnostics.append(f"{handle}: linked discussion scan capped at {max_threads} threads")
            if len(opportunities) >= max_opportunities:
                diagnostics.append(f"telegram opportunity scan capped at {max_opportunities} candidates")
                break
    diagnostics.append(f"telegram frontier walk processed {processed} surfaces; queued {discovered_queued} newly discovered tg links")
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
    scanned_list = list(scanned_surfaces or [])
    scanned_tg = [s for s in scanned_list if s.get("platform") == "tg"]
    scanned_vk = [s for s in scanned_list if s.get("platform") == "vk"]
    return {
        "run_id": os.getenv("KAGGLE_RUN_ID") or f"acq-shadow-{int(time.time())}",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "surfaces": list(surfaces_by_external.values()),
        "opportunities": list(scanned_opportunities or []),
        "stats": {
            "mode": "telegram_vk_shadow_scan" if scanned_surfaces or scanned_opportunities else "shadow_preflight",
            "surfaces_total": len(surfaces_by_external),
            "telegram_seeds": len(list(tg_seeds or [])),
            "vk_seeds": len(list(vk_seeds or [])),
            "vk_allowlist": len(allowed_vk),
            "telegram_scanned_or_discovered_surfaces": len(scanned_tg),
            "vk_scanned_or_discovered_surfaces": len(scanned_vk),
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
    _load_kaggle_env()
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
