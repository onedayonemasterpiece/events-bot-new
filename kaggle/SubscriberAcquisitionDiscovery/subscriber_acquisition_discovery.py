from __future__ import annotations

import asyncio
import base64
import importlib
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
    modules = [("telethon", "telethon"), ("google.genai", "google-genai")]
    missing: list[str] = []
    for module_name, package_name in modules:
        try:
            importlib.import_module(module_name)
        except ImportError:
            missing.append(package_name)
    if missing:
        print(f"Installing Python packages: {', '.join(missing)}", flush=True)
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", *missing])


ensure_libs()

TELEGA_IN_KALININGRAD_TG_SEEDS = [
    # Public handles discovered from Telega.in regional/Kaliningrad cards.
    "https://t.me/Kaliningrad_jenskiy",
    "https://t.me/kpkld",
    "https://t.me/gokaliningrad_ru",
    "https://t.me/kenig01",
    "https://t.me/Davai_KLD",
    "https://t.me/kaliklove",
    "https://t.me/jobs39",
    "https://t.me/anons39",
    "https://t.me/nedvizhimostkalinigrad",
    "https://t.me/remont3939",
    "https://t.me/autoclub_kld",
    "https://t.me/kaliningrad_now_ru",
]

DEFAULT_TG_SEEDS = [
    "https://t.me/tg_kgd",
    "https://t.me/chatkalin",
    "https://t.me/kenig01chat",
    "https://t.me/zhest_kaliningrada",
    "https://t.me/pereezd_v_kaliningrad_legko",
    *TELEGA_IN_KALININGRAD_TG_SEEDS,
]

_TELEGA_IN_TG_HANDLES = {url.rstrip("/").rsplit("/", 1)[-1].lower() for url in TELEGA_IN_KALININGRAD_TG_SEEDS}


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
    r"(?:куда|как)\s+(?:съездить|поехать|прокатиться)(?:\b|.*(?:на\s+день|за\s+день|из\s+калининграда|на\s+электричк|маршрут|выходн|с\s+детьми))|"
    r"(?:куда|что)\s+(?:поехать|посмотреть|посетить)\b.*(?:за\s+день|на\s+день|на\s+выходн|с\s+детьми|в\s+области|из\s+калининграда)|"
    r"(?:что|где)\s+(?:посмотреть|посетить).*?(?:в|во|на)\s+(?:зеленоградск|светлогорск|балтийск|янтарн|черняховск|советск|гусев|георгенбург|област[ьи])|"
    r"(?:однодневн(?:ая|ый|ое)|на\s+один\s+день|за\s+день)\b.*(?:маршрут|поездк|съездить|поехать|посмотреть)|"
    r"(?:маршрут|поездк[аи]|трип|trip)\b.*(?:зеленоградск|светлогорск|балтийск|янтарн|черняховск|советск|гусев|георгенбург|электричк|пригородн|из\s+калининграда|по\s+области)|"
    r"(?:электричк|пригородн(?:ый|ая)\s+поезд)\b.*(?:куда|маршрут|съездить|поехать|выходн)|"
    r"(?:замк[иов]|побережь[ея]|куршск(?:ая|ую)\s+кос[ау])\b.*(?:маршрут|куда|что\s+посмотреть|съездить|поехать)"
    r")"
)
_VENUE_POLICY_TERM_RE = re.compile(
    r"(?i)\b(льгот\w*|скидк\w*|дет[еи][-\s]?инвалид\w*|инвалид\w*|маломобильн\w*|доступн\w*|пандус\w*|коляск\w*|билет\w*|услови[яй]\w*)\b"
)
_VENUE_POLICY_LOCAL_RE = re.compile(
    r"(?i)\b(у\s+вас|ваши|вашего|вашем|для\s+посещени[яй]|на\s+ваш(?:е|ем|у)|в\s+ваш(?:ем|у)|можно\s+ли\s+к\s+вам)\b"
)
_GENERAL_DISCOVERY_RE = re.compile(
    r"(?i)\b(где\s+найти|есть\s+поиск|подборк|каталог|афиш|мероприят|событи|куда\s+сходить|куда\s+пойти|куда\s+съездить|что\s+посетить|что\s+посмотреть)\b"
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

DEFAULT_ACQ_LLM_MODEL = "models/gemma-4-31b-it"
VK_SOCIAL_DISCOVERY_HANDLES = {
    "club42481124",  # Подслушано в Калининграде
    "club31556867",  # Типичный Калининград
    "club80149142",  # ЧС - Калининград и область
    "club186019893",  # ДТП и ЧП | KADAUTO
    "kuda_go_kld",
    "club_topplace",
    "kuda_dety39",
    "kidsreview_kaliningrad",
    "visit.kaliningrad",
    "peshiytur",
    "tourguilde39",
    "blog_batsev",
    "otextour",
}


DEFAULT_TG_SEARCH_QUERIES = [
    "куда сходить",
    "куда пойти",
    "с детьми",
    "на выходных",
    "афиша",
    "выставки",
    "концерт",
    "мероприятия",
    "куда съездить",
    "куда поехать",
    "что посмотреть за день",
    "на один день",
    "маршрут",
    "маршрут на день",
    "из Калининграда",
    "на электричке",
    "что посетить",
    "замки",
    "побережье",
    "Пушкинская карта",
    "бесплатно",
    "добавить событие",
]
LLM_GATE_STATS: dict[str, int] = {
    "prefilter_candidates": 0,
    "calls": 0,
    "reserved": 0,
    "accepted": 0,
    "rejected": 0,
    "rejected_low_confidence": 0,
    "errors": 0,
    "skipped_no_key": 0,
    "skipped_seen_context": 0,
    "skipped_same_run_context": 0,
    "blocked_rate_limit": 0,
    "estimated_input_tokens": 0,
}
VK_SCAN_STATS: dict[str, int] = {
    "surfaces_attempted": 0,
    "wall_posts_seen": 0,
    "posts_with_comments": 0,
    "posts_without_comments_skipped": 0,
    "comments_seen": 0,
    "board_topics_seen": 0,
    "board_comments_seen": 0,
    "wall_search_posts_seen": 0,
    "comment_prefilter_candidates": 0,
    "rate_limit_backoffs": 0,
}
TG_SCAN_STATS: dict[str, int] = {
    "surfaces_attempted": 0,
    "channel_resolve_attempts": 0,
    "channels_with_linked_discussion": 0,
    "channels_rejected_no_comments": 0,
    "groups_or_chats_scanned": 0,
    "linked_discussions_scanned": 0,
    "channel_posts_seen_for_links": 0,
    "replyable_messages_seen": 0,
    "replyable_surfaces_scanned": 0,
    "frontier_links_queued": 0,
}

LLM_GATE_SCHEMA = {
    "type": "object",
    "properties": {
        "is_candidate": {"type": "boolean"},
        "matched_intent": {"type": "string"},
        "topic_cluster": {"type": "string"},
        "best_reply_strategy": {"type": "string"},
        "target_kind": {"type": "string"},
        "target_label": {"type": "string"},
        "target_url": {"type": "string"},
        "reason": {"type": "string"},
        "relevance": {"type": "number"},
        "spam_risk": {"type": "string"},
        "safety_risk": {"type": "string"},
        "checklist": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "question": {"type": "string"},
                    "answer": {"type": "boolean"},
                    "note": {"type": "string"},
                },
                "required": ["id", "question", "answer", "note"],
            },
        },
    },
    "required": ["is_candidate", "matched_intent", "topic_cluster", "best_reply_strategy", "target_kind", "target_label", "target_url", "reason", "relevance", "spam_risk", "safety_risk", "checklist"],
}


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


def _deadline_after_seconds() -> float | None:
    seconds = _int_env("ACQ_RUNTIME_DEADLINE_SECONDS", 0, min_value=0)
    if seconds <= 0:
        return None
    return time.monotonic() + float(seconds)


def _deadline_reached(deadline: float | None) -> bool:
    return deadline is not None and time.monotonic() >= deadline


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


def _llm_gate_enabled() -> bool:
    return _truthy_env("ACQ_ENABLE_LLM_GATE", True)


def _google_key_env_names() -> list[str]:
    # Keep acquisition discovery on the same isolated Google key lane as the
    # Gemma 4 Telegram-monitoring Kaggle path by default. Generic fallback keys
    # are intentionally opt-in so semantic candidate gating is not silently run
    # on an unrelated provider/key pool.
    configured = (os.getenv("ACQ_GOOGLE_KEY_ENV") or "GOOGLE_API_KEY3").strip() or "GOOGLE_API_KEY3"
    names = [configured]
    if configured == "GOOGLE_API_KEY3":
        names.append("GOOGLE_API_KEY_3")
    if _truthy_env("ACQ_ALLOW_GOOGLE_KEY_FALLBACKS", False):
        for fallback in ["GOOGLE_API_KEY", "GOOGLE_API_KEY4", "GOOGLE_API_KEY_4"]:
            if fallback not in names:
                names.append(fallback)
    return names


def _google_api_key() -> tuple[str | None, str | None]:
    for name in _google_key_env_names():
        value = (os.getenv(name) or "").strip()
        if value:
            return value, name
    return None, None


def _acq_llm_model() -> str:
    return (os.getenv("ACQ_LLM_MODEL") or DEFAULT_ACQ_LLM_MODEL).strip() or DEFAULT_ACQ_LLM_MODEL


def _estimate_llm_input_tokens(prompt: str) -> int:
    # Conservative enough for budget visibility without depending on provider tokenizers.
    return max(1, int(len(prompt.encode("utf-8")) / 4) + 1)


def _llm_gate_max_calls_per_run() -> int:
    return _int_env("ACQ_MAX_LLM_CALLS_PER_RUN", 80, min_value=1)


def _llm_limit_snapshot() -> dict[str, Any]:
    return {
        "scope": "kaggle_process",
        "max_calls_per_run": _llm_gate_max_calls_per_run(),
        "calls_used_this_run": int(LLM_GATE_STATS.get("calls", 0)),
        "calls_reserved_this_run": int(LLM_GATE_STATS.get("reserved", 0)),
        "blocked_rate_limit": int(LLM_GATE_STATS.get("blocked_rate_limit", 0)),
        "estimated_input_tokens_this_run": int(LLM_GATE_STATS.get("estimated_input_tokens", 0)),
        "key_env": _google_api_key()[1],
        "model": _acq_llm_model(),
    }


def _reserve_llm_gate_call(prompt: str, diagnostics: list[str]) -> bool:
    """Fail-fast local budget gate for Gemma calls made by this Kaggle process.

    The project-wide Google AI gateway is still preferred where available, but
    the Kaggle discovery script must never call Gemma from an unbounded loop.
    This guard is intentionally before the provider request and is reported in
    the payload so the operator can see today's/run spending instead of guessing.
    """
    used = int(LLM_GATE_STATS.get("calls", 0))
    max_calls = _llm_gate_max_calls_per_run()
    if used >= max_calls:
        LLM_GATE_STATS["blocked_rate_limit"] += 1
        diagnostics.append(f"acq llm gate budget exhausted: calls {used}/{max_calls}")
        return False
    estimated = _estimate_llm_input_tokens(prompt)
    LLM_GATE_STATS["reserved"] += 1
    LLM_GATE_STATS["estimated_input_tokens"] += estimated
    return True


def _llm_gate_prompt(opp: dict[str, Any], surface: dict[str, Any]) -> str:
    checklist_questions = [
        "Есть явный вопрос, просьба, поиск рекомендации или нерешённая потребность?",
        "Это будущая/актуальная потребность, а не постфактум-отзыв, спасибо или отчёт о прошедшем событии?",
        "Понятно, что именно полезно ответить: конкретное событие, конкретный маршрут, поиск/подборка, фильтр или страница добавления события?",
        "Это не локальная логистика/правила конкретного места или уже обсуждаемого события/поста (афиша/программа/расписание 1 дня, время, адрес, билеты, льготы, скидки, доступность), где общий acquisition-ответ не поможет?",
        "Ответ может быть коротким, нативным и не выглядеть рекламой?",
        "Риск спама низкий при единичном аккуратном reply?",
    ]
    payload = {
        "platform": opp.get("platform"),
        "surface": {
            "title": surface.get("title"),
            "handle": surface.get("handle"),
            "url": surface.get("url"),
            "surface_type": surface.get("surface_type"),
        },
        "comment_text": opp.get("context_text_snippet"),
        "prefilter_intent": opp.get("matched_intent"),
        "prefilter_topic_cluster": opp.get("topic_cluster"),
        "prefilter_target": opp.get("link_target"),
        "checklist_questions": checklist_questions,
    }
    return (
        "Ты оцениваешь кандидата для Subscriber Acquisition в Калининградской области. "
        "Нужно решить, стоит ли показывать оператору карточку потенциального аккуратного reply. "
        "Не создавай кандидат, если комментарий — просто благодарность, отзыв, похвала организаторам, отчёт о прошедшем событии, эмоция без вопроса, логистический вопрос внутри уже известного события без полезного acquisition-ответа, или если непонятно что полезно сообщить. "
        "Особенно консервативно отклоняй вопросы вида 'где афиша/программа/расписание 1 дня', 'до скольки', 'где вход/адрес' — это обычно локальная логистика текущего события/поста, а не повод вести в общий канал, если у нас нет точной страницы/ссылки именно с этим расписанием. "
        "Отклоняй вопросы к конкретному месту/организатору вида 'у вас есть льготы/скидки/билеты/условия/доступность/пандус/можно с коляской/для инвалидов': это venue policy, а не acquisition-кандидат, если человек не спрашивает именно где найти городскую подборку/поиск доступных или бесплатных событий. "
        "Если все ключевые вопросы checklist дают нет, верни is_candidate=false. "
        "Для маршрутов: кандидат только если пользователь спрашивает куда съездить/поехать, что посмотреть за день/на выходных, маршрут по области или конкретный Калининградский пригород; ответ должен вести к конкретному маршруту/подборке маршрутов, не к общему паблику. "
        "Для обычных событий: кандидат только если человеку реально нужна афиша/рекомендация/подборка. "
        "Для организаторов: кандидат только если человек спрашивает как добавить/разместить/прислать событие или про инфопартнёрство. "
        "Верни строго JSON по схеме, без markdown. target_url может быть пустой строкой, если нужен будущий конкретный маршрут без готовой ссылки.\n\n"
        + json.dumps(payload, ensure_ascii=False, indent=2)
    )


def _extract_llm_text(response: Any) -> str:
    text = getattr(response, "text", None)
    if isinstance(text, str) and text.strip():
        return text.strip()
    parts: list[str] = []
    try:
        for candidate in getattr(response, "candidates", []) or []:
            content = getattr(candidate, "content", None)
            for part in getattr(content, "parts", []) or []:
                if bool(getattr(part, "thought", False)):
                    continue
                value = getattr(part, "text", None)
                if isinstance(value, str) and value.strip():
                    parts.append(value.strip())
    except Exception:
        pass
    return "\n".join(parts).strip()


def _parse_llm_json(raw: str) -> dict[str, Any]:
    text = (raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.I).strip()
        text = re.sub(r"\s*```$", "", text).strip()
    return json.loads(text)


def _call_acq_llm_gate_sync(opp: dict[str, Any], surface: dict[str, Any]) -> dict[str, Any]:
    api_key, key_env = _google_api_key()
    if not api_key:
        raise RuntimeError("no Google API key configured for acquisition LLM gate")
    from google import genai
    from google.genai import types as gtypes

    client = genai.Client(api_key=api_key)
    model = _acq_llm_model()
    logger.info("acq.llm_gate_call model=%s key_env=%s", model, key_env)
    prompt = _llm_gate_prompt(opp, surface)
    response = client.models.generate_content(
        model=model,
        contents=prompt,
        config=gtypes.GenerateContentConfig(
            temperature=0,
            max_output_tokens=_int_env("ACQ_LLM_GATE_MAX_OUTPUT_TOKENS", 900, min_value=256),
            response_mime_type="application/json",
            response_schema=LLM_GATE_SCHEMA,
        ),
    )
    return _parse_llm_json(_extract_llm_text(response))


def _review_is_high_confidence(review: dict[str, Any]) -> tuple[bool, str]:
    min_relevance = float(os.getenv("ACQ_LLM_GATE_MIN_RELEVANCE") or "0.85")
    try:
        relevance = float(review.get("relevance") or 0.0)
    except Exception:
        relevance = 0.0
    if relevance < min_relevance:
        return False, f"relevance {relevance:.2f} < {min_relevance:.2f}"
    if str(review.get("spam_risk") or "low").strip().lower() not in {"none", "low"}:
        return False, f"spam_risk={review.get('spam_risk')}"
    if str(review.get("safety_risk") or "low").strip().lower() not in {"none", "low"}:
        return False, f"safety_risk={review.get('safety_risk')}"
    checklist = review.get("checklist") or []
    if isinstance(checklist, list):
        false_required = []
        for item in checklist[:5]:
            if isinstance(item, dict) and item.get("answer") is False:
                false_required.append(str(item.get("id") or item.get("question") or "check"))
        if false_required:
            return False, "checklist_false=" + ",".join(false_required[:3])
    return True, "high_confidence"


def _apply_llm_gate_result(opp: dict[str, Any], review: dict[str, Any]) -> dict[str, Any] | None:
    if not bool(review.get("is_candidate")):
        return None
    high_confidence, confidence_reason = _review_is_high_confidence(review)
    if not high_confidence:
        review["confidence_reject_reason"] = confidence_reason
        return None
    updated = dict(opp)
    if review.get("matched_intent"):
        updated["matched_intent"] = str(review.get("matched_intent"))
    if review.get("topic_cluster"):
        updated["topic_cluster"] = str(review.get("topic_cluster"))
    link = dict(updated.get("link_target") or {})
    link["kind"] = str(review.get("target_kind") or link.get("kind") or "other")
    link["label"] = str(review.get("target_label") or link.get("label") or link["kind"])
    if "target_url" in review:
        link["url"] = str(review.get("target_url") or "") or None
    link["reason"] = str(review.get("reason") or link.get("reason") or "Gemma acquisition gate accepted")
    updated["link_target"] = link
    scores = dict(updated.get("scores") or {})
    scores["source"] = "gemma4_acquisition_gate"
    scores["relevance"] = float(review.get("relevance") or scores.get("relevance") or 0.0)
    scores["spam_risk"] = str(review.get("spam_risk") or scores.get("spam_risk") or "low")
    scores["safety_risk"] = str(review.get("safety_risk") or scores.get("safety_risk") or "low")
    updated["scores"] = scores
    evidence = dict(updated.get("evidence") or {})
    evidence["llm_gate"] = {
        "model": _acq_llm_model(),
        "is_candidate": True,
        "confidence": "very_high",
        "checklist": review.get("checklist") or [],
        "reason": review.get("reason"),
        "best_reply_strategy": review.get("best_reply_strategy"),
    }
    updated["evidence"] = evidence
    return updated


def _llm_review_opportunity_sync(opp: dict[str, Any], surface: dict[str, Any], diagnostics: list[str]) -> dict[str, Any] | None:
    LLM_GATE_STATS["prefilter_candidates"] += 1
    if not _llm_gate_enabled():
        return opp
    if not _google_api_key()[0]:
        LLM_GATE_STATS["skipped_no_key"] += 1
        diagnostics.append("acq llm gate skipped candidate: Google API key is not configured")
        return None
    prompt = _llm_gate_prompt(opp, surface)
    if not _reserve_llm_gate_call(prompt, diagnostics):
        return None
    try:
        LLM_GATE_STATS["calls"] += 1
        review = _call_acq_llm_gate_sync(opp, surface)
        accepted = _apply_llm_gate_result(opp, review)
        if accepted is None:
            reason = str(review.get("confidence_reject_reason") or "semantic_not_candidate")
            if "confidence_reject_reason" in review:
                LLM_GATE_STATS["rejected_low_confidence"] += 1
            else:
                LLM_GATE_STATS["rejected"] += 1
            diagnostics.append(f"acq Gemma understanding rejected ({reason}): {(opp.get('context_text_snippet') or '')[:120]}")
        else:
            LLM_GATE_STATS["accepted"] += 1
        return accepted
    except Exception as exc:
        LLM_GATE_STATS["errors"] += 1
        diagnostics.append(f"acq llm gate error: {type(exc).__name__}: {str(exc)[:300]}")
        logger.warning("acq llm gate failed", exc_info=True)
        return None


async def _llm_review_opportunity_async(opp: dict[str, Any], surface: dict[str, Any], diagnostics: list[str]) -> dict[str, Any] | None:
    return await asyncio.to_thread(_llm_review_opportunity_sync, opp, surface, diagnostics)


def _is_venue_policy_local_question(compact: str) -> bool:
    if not compact:
        return False
    if not _VENUE_POLICY_TERM_RE.search(compact):
        return False
    # Narrow deterministic guardrail: questions addressed to a concrete venue/community
    # ("у вас", "ваши билеты", etc.) should not spend review attention unless
    # the text explicitly asks for a city-wide event/search/route surface.
    if _VENUE_POLICY_LOCAL_RE.search(compact) and not _GENERAL_DISCOVERY_RE.search(compact):
        return True
    return False


def _classify_acq_intent(text: str) -> dict[str, Any] | None:
    compact = " ".join(str(text or "").split())
    if not compact:
        return None
    if _is_venue_policy_local_question(compact):
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
    status = str(surface.get("status") or "").strip().lower()
    return not status.startswith("rejected") and status not in {"resolved_has_linked_discussion"}


def _is_tg_replyable_surface_type(surface_type: str | None) -> bool:
    return str(surface_type or "").strip().lower() in {"group", "chat", "megagroup", "linked_discussion"}


def _mark_tg_channel_resolved_with_linked_discussion(
    channel_surface: dict[str, Any],
    *,
    linked_surface: dict[str, Any],
) -> dict[str, Any]:
    updated = dict(channel_surface)
    updated["status"] = "resolved_has_linked_discussion"
    updated["topic_cluster"] = "telegram_channel_with_comments"
    updated["reach"] = {
        **(updated.get("reach") or {}),
        "basis": "telegram_channel_resolved",
        "linked_discussion_external_id": linked_surface.get("external_id"),
        "linked_discussion_url": linked_surface.get("url"),
    }
    updated["risk"] = {
        **(updated.get("risk") or {}),
        "spam_risk": "low",
        "safety_risk": "low",
        "reason": "Channel is metadata only; replies must happen in linked discussion.",
        "reply_policy": "use_linked_discussion",
        "linked_discussion_external_id": linked_surface.get("external_id"),
        "linked_discussion_url": linked_surface.get("url"),
    }
    return updated


def _mark_tg_channel_rejected_no_comments(surface: dict[str, Any]) -> dict[str, Any]:
    updated = dict(surface)
    updated["status"] = "rejected_no_comments"
    updated["topic_cluster"] = "telegram_channel_without_comments"
    updated["risk"] = {
        "spam_risk": "high",
        "safety_risk": "low",
        "level": "rejected",
        "reason": "Telegram channel has no accessible linked discussion/comments; reply acquisition requires comments/chat",
        "reply_policy": "no_reply_surface",
    }
    return updated


def _vk_surface_from_handle(handle: str) -> dict[str, Any] | None:
    clean = str(handle or "").strip().strip("/").rstrip(".,)")
    if not clean:
        return None
    wall = _VK_WALL_RE.match(clean)
    if wall:
        clean = f"club{wall.group('group_id')}"
    lowered = clean.lower()
    if lowered.startswith(("wall", "photo", "video", "topic", "im", "album", "app", "market", "away.php", "id")):
        return None
    return _seed_surface(f"https://vk.com/{clean}", platform="vk") | {"source": "discovered"}


def _is_vk_scan_domain_candidate(domain: str) -> bool:
    return _vk_surface_from_handle(domain) is not None


def _is_tg_discovery_bot_or_service_handle(handle: str) -> bool:
    lowered = str(handle or "").strip().strip("/").lower()
    return (
        not lowered
        or lowered in {"c", "s", "joinchat", "share", "addstickers", "addemoji", "iv", "boost"}
        or lowered.endswith("bot")
    )


def extract_candidate_surfaces(text: str) -> list[dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for match in _LINK_RE.findall(text or ""):
        url = match if match.startswith(("http://", "https://")) else f"https://{match}"
        tg = _TG_HOST_RE.search(url)
        vk = _VK_HOST_RE.search(url)
        if tg:
            handle = tg.group("handle")
            if _is_tg_discovery_bot_or_service_handle(handle):
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


def _should_skip_opportunity_before_llm(
    opp: dict[str, Any],
    *,
    seen_contexts: set[str],
    opportunity_keys: set[str],
    diagnostics: list[str],
) -> bool:
    context_url = str(opp.get("context_url") or "")
    key = f"{opp.get('platform')}|{context_url}|{(opp.get('context_text_snippet') or '')[:120]}"
    if context_url and context_url in seen_contexts:
        LLM_GATE_STATS["skipped_seen_context"] += 1
        diagnostics.append(f"skip already analyzed context before Gemma: {context_url}")
        return True
    if key in opportunity_keys:
        LLM_GATE_STATS["skipped_same_run_context"] += 1
        diagnostics.append(f"skip same-run duplicate before Gemma: {context_url or key[:80]}")
        return True
    opportunity_keys.add(key)
    return False



VK_READ_METHODS = {"wall.get", "wall.getComments", "wall.search", "groups.getById", "board.getTopics", "board.getComments"}


class VKApiError(RuntimeError):
    def __init__(self, method: str, error: dict[str, Any]):
        self.method = method
        self.error = error
        self.error_code = int(error.get("error_code") or 0)
        super().__init__(f"VK {method} error: {error}")


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
        raise VKApiError(method, data["error"])
    return data.get("response") or {}


def _vk_api_with_fallback(method: str, *, token_lanes: list[tuple[str, str]], params: dict[str, Any]) -> tuple[dict[str, Any], str]:
    errors: list[str] = []
    for lane_name, token in token_lanes:
        try:
            return _vk_api(method, token=token, params=params), lane_name
        except Exception as exc:
            if isinstance(exc, VKApiError) and exc.error_code == 6:
                VK_SCAN_STATS["rate_limit_backoffs"] += 1
                _human_pause_sync(multiplier=2.5)
                try:
                    return _vk_api(method, token=token, params=params), lane_name
                except Exception as retry_exc:
                    exc = retry_exc
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


def _is_vk_social_discovery_surface(surface: dict[str, Any]) -> bool:
    handle = str(surface.get("handle") or "").strip().strip("/").rstrip(".,)").casefold()
    external = str(surface.get("external_id") or "").strip().casefold()
    if external.startswith("vk:"):
        handle = external.split(":", 1)[1].rstrip(".,)") or handle
    return handle in VK_SOCIAL_DISCOVERY_HANDLES


def _vk_group_id_from_domain(domain: str, token_lanes: list[tuple[str, str]], diagnostics: list[str]) -> int | None:
    clean = str(domain or "").strip().strip("/").rstrip(".,)")
    if not clean:
        return None
    m = re.match(r"(?i)^club(\d+)$", clean)
    if m:
        return int(m.group(1))
    try:
        response, _lane = _vk_api_with_fallback("groups.getById", token_lanes=token_lanes, params={"group_ids": clean})
        groups = response.get("groups") if isinstance(response, dict) else None
        if not groups and isinstance(response, list):
            groups = response
        if groups:
            gid = int((groups[0] or {}).get("id") or 0)
            return gid if gid > 0 else None
    except Exception as exc:
        diagnostics.append(f"vk {clean}: groups.getById failed: {exc}")
    return None


def _vk_topic_context_url(group_id: int, topic_id: int, comment_id: int | None = None) -> str:
    url = f"https://vk.com/topic-{group_id}_{topic_id}"
    if comment_id:
        url += f"?post={comment_id}"
    return url


def build_vk_wall_post_opportunity(surface: dict[str, Any], *, post: dict[str, Any], default_target_url: str) -> dict[str, Any] | None:
    if not _is_vk_social_discovery_surface(surface):
        return None
    text = str(post.get("text") or "").strip()
    intent = _classify_acq_intent(text)
    if not intent:
        return None
    owner_id = int(post.get("owner_id") or 0)
    post_id = int(post.get("id") or 0)
    if not owner_id or not post_id:
        return None
    # UGC/community wall posts are replyable by writing a comment to the post.
    # Official event-source wall posts are deliberately excluded by
    # _is_vk_social_discovery_surface to avoid ad-post false positives.
    context_url = f"https://vk.com/wall{owner_id}_{post_id}"
    views = (post.get("views") or {}).get("count") if isinstance(post.get("views"), dict) else None
    comments_count = (post.get("comments") or {}).get("count") if isinstance(post.get("comments"), dict) else None
    reach_low = 3
    try:
        if views:
            reach_low = max(3, min(25, int(views) // 200))
        elif comments_count:
            reach_low = max(3, min(15, int(comments_count) + 2))
    except Exception:
        reach_low = 3
    return {
        "platform": "vk",
        "surface_external_id": surface.get("external_id"),
        "context_url": context_url,
        "context_external_id": str(post_id),
        "context_created_at": datetime.fromtimestamp(int(post.get("date") or 0), tz=timezone.utc).isoformat() if post.get("date") else None,
        "context_text_snippet": " ".join(text.split())[:500],
        "matched_intent": intent["matched_intent"],
        "topic_cluster": intent["topic_cluster"],
        "event_ids": [],
        "candidate_events": [],
        "link_target": {
            "kind": intent.get("target_kind") or "topic_landing",
            "url": intent["target_url"] if "target_url" in intent else default_target_url,
            "label": intent.get("target_label") or "Полюбить Калининград Анонсы",
            "reason": intent.get("reason") or "VK social community post can be answered by a native comment",
        },
        "fallback_link_target": {"kind": "pka_channel", "url": intent.get("fallback_url", default_target_url), "label": "Полюбить Калининград Анонсы"},
        "reach": {"low": reach_low, "confidence": "low", "formula": "vk_social_wall_post_conservative_low"},
        "scores": {"relevance": intent.get("relevance") or 0.5, "spam_risk": "low", "safety_risk": "low", "source": "deterministic_shadow_prefilter"},
        "sticker_observation": {"fit": "weak", "stickers_seen": 0, "reason": "VK social wall-post prefilter only; needs LLM review before any reply"},
        "evidence": {"relation": "vk_social_wall_post", "scanner": "vk_shadow", "comments_count": comments_count, "views": views},
    }


def build_vk_board_opportunity(
    surface: dict[str, Any],
    *,
    group_id: int,
    topic: dict[str, Any],
    comment: dict[str, Any],
    default_target_url: str,
) -> dict[str, Any] | None:
    text = str(comment.get("text") or "").strip()
    intent = _classify_acq_intent(text)
    if not intent:
        return None
    topic_id = int(topic.get("id") or 0)
    comment_id = int(comment.get("id") or 0)
    if not group_id or not topic_id or not comment_id:
        return None
    title = str(topic.get("title") or "").strip()
    return {
        "platform": "vk",
        "surface_external_id": surface.get("external_id"),
        "context_url": _vk_topic_context_url(group_id, topic_id, comment_id),
        "context_external_id": f"{topic_id}:{comment_id}",
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
            "reason": intent.get("reason") or "VK discussion-board comment can be answered natively in the topic",
        },
        "fallback_link_target": {"kind": "pka_channel", "url": intent.get("fallback_url", default_target_url), "label": "Полюбить Калининград Анонсы"},
        "reach": {"low": 3, "confidence": "low", "formula": "vk_board_topic_comment_low"},
        "scores": {"relevance": intent.get("relevance") or 0.5, "spam_risk": "low", "safety_risk": "low", "source": "deterministic_shadow_prefilter"},
        "sticker_observation": {"fit": "weak", "stickers_seen": 0, "reason": "VK discussion-board prefilter only; needs LLM review before any reply"},
        "evidence": {"relation": "vk_board_comment", "scanner": "vk_shadow", "topic_title": title, "topic_comments": topic.get("comments")},
    }


def _scan_vk_board_discussions(
    *,
    surface: dict[str, Any],
    domain: str,
    token_lanes: list[tuple[str, str]],
    default_target_url: str,
    seen_contexts: set[str],
    opportunity_keys: set[str],
    diagnostics: list[str],
    opportunities: list[dict[str, Any]],
    deadline: float | None,
) -> bool:
    max_topics = _int_env("ACQ_MAX_VK_BOARD_TOPICS_PER_SURFACE", 3, min_value=0)
    max_comments = _int_env("ACQ_MAX_VK_BOARD_COMMENTS_PER_TOPIC", 20, min_value=1)
    max_opportunities = _int_env("ACQ_MAX_OPPORTUNITIES_PER_RUN", 20, min_value=1)
    if max_topics <= 0 or _deadline_reached(deadline):
        return False
    group_id = _vk_group_id_from_domain(domain, token_lanes, diagnostics)
    if not group_id:
        return False
    try:
        topics, topics_lane = _vk_api_with_fallback(
            "board.getTopics",
            token_lanes=token_lanes,
            params={"group_id": group_id, "count": max_topics, "extended": 0, "order": 1},
        )
    except Exception as exc:
        diagnostics.append(f"vk {domain}: board.getTopics failed: {exc}")
        return False
    items = list((topics or {}).get("items") or [])
    if items:
        diagnostics.append(f"vk {domain}: board.getTopics ok via {topics_lane}, topics={len(items)}")
    for topic in items[:max_topics]:
        if _deadline_reached(deadline):
            diagnostics.append("vk board scan stopped by ACQ_RUNTIME_DEADLINE_SECONDS")
            break
        if not isinstance(topic, dict):
            continue
        VK_SCAN_STATS["board_topics_seen"] += 1
        topic_id = int(topic.get("id") or 0)
        if not topic_id:
            continue
        try:
            comments, comments_lane = _vk_api_with_fallback(
                "board.getComments",
                token_lanes=token_lanes,
                params={"group_id": group_id, "topic_id": topic_id, "count": max_comments, "sort": "desc"},
            )
            if comments_lane != topics_lane:
                diagnostics.append(f"vk {domain}: board.getComments {topic_id} ok via {comments_lane}")
        except Exception as exc:
            diagnostics.append(f"vk {domain}: board.getComments {topic_id} failed: {exc}")
            continue
        _human_pause_sync(multiplier=0.25)
        for comment in comments.get("items") or []:
            if _deadline_reached(deadline):
                diagnostics.append("vk board comment scan stopped by ACQ_RUNTIME_DEADLINE_SECONDS")
                break
            if not isinstance(comment, dict):
                continue
            VK_SCAN_STATS["board_comments_seen"] += 1
            opp = build_vk_board_opportunity(surface, group_id=group_id, topic=topic, comment=comment, default_target_url=default_target_url)
            if not opp:
                continue
            VK_SCAN_STATS["comment_prefilter_candidates"] += 1
            if _should_skip_opportunity_before_llm(opp, seen_contexts=seen_contexts, opportunity_keys=opportunity_keys, diagnostics=diagnostics):
                continue
            reviewed = _llm_review_opportunity_sync(opp, surface, diagnostics)
            if reviewed:
                opportunities.append(reviewed)
                if len(opportunities) >= max_opportunities:
                    return True
    return False


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
    seen_contexts = _seen_context_urls()
    opportunity_keys: set[str] = set()
    deadline = _deadline_after_seconds()
    for raw_url in seed_urls[:max_surfaces]:
        if _deadline_reached(deadline):
            diagnostics.append("vk scan stopped by ACQ_RUNTIME_DEADLINE_SECONDS")
            break
        normalized = str(raw_url or "").strip()
        if not normalized or normalized.lower() not in allowed:
            continue
        VK_SCAN_STATS["surfaces_attempted"] += 1
        domain = _handle_from_url(normalized, platform="vk")
        if not _is_vk_scan_domain_candidate(domain):
            diagnostics.append(f"vk {domain}: skipped non-community surface")
            continue
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
            wall, wall_lane = _vk_api_with_fallback("wall.get", token_lanes=token_lanes, params={"domain": domain, "count": max_posts, "filter": "all"})
            diagnostics.append(f"vk {domain}: wall.get ok via {wall_lane}")
        except Exception as exc:
            diagnostics.append(f"vk {domain}: wall.get failed: {exc}")
            if _scan_vk_board_discussions(
                surface=surface,
                domain=domain,
                token_lanes=token_lanes,
                default_target_url=default_target_url,
                seen_contexts=seen_contexts,
                opportunity_keys=opportunity_keys,
                diagnostics=diagnostics,
                opportunities=opportunities,
                deadline=deadline,
            ):
                return list(surfaces.values()), opportunities, diagnostics
            continue
        _human_pause_sync(multiplier=0.6)
        for post in wall.get("items") or []:
            if _deadline_reached(deadline):
                diagnostics.append("vk post scan stopped by ACQ_RUNTIME_DEADLINE_SECONDS")
                break
            owner_id = int(post.get("owner_id") or 0)
            post_id = int(post.get("id") or 0)
            post_text = str(post.get("text") or "")
            VK_SCAN_STATS["wall_posts_seen"] += 1
            for discovered in extract_candidate_surfaces(post_text):
                discovered["topic_hint"] = f"discovered in vk:{domain}"
                if _is_out_of_region_surface(discovered):
                    discovered = _mark_out_of_region_surface(discovered, reason="out-of-region surface discovered in VK post")
                surfaces.setdefault(discovered["external_id"], discovered)
            post_opp = build_vk_wall_post_opportunity(surface, post=post, default_target_url=default_target_url)
            if post_opp:
                VK_SCAN_STATS["comment_prefilter_candidates"] += 1
                if not _should_skip_opportunity_before_llm(
                    post_opp,
                    seen_contexts=seen_contexts,
                    opportunity_keys=opportunity_keys,
                    diagnostics=diagnostics,
                ):
                    reviewed_post_opp = _llm_review_opportunity_sync(post_opp, surface, diagnostics)
                    if reviewed_post_opp:
                        opportunities.append(reviewed_post_opp)
                        if len(opportunities) >= _int_env("ACQ_MAX_OPPORTUNITIES_PER_RUN", 20, min_value=1):
                            return list(surfaces.values()), opportunities, diagnostics
            if not owner_id or not post_id:
                continue
            comment_count = int(((post.get("comments") or {}).get("count") or 0) if isinstance(post.get("comments"), dict) else 0)
            if comment_count <= 0:
                VK_SCAN_STATS["posts_without_comments_skipped"] += 1
                continue
            VK_SCAN_STATS["posts_with_comments"] += 1
            try:
                comments, comments_lane = _vk_api_with_fallback("wall.getComments", token_lanes=token_lanes, params={"owner_id": owner_id, "post_id": post_id, "count": max_comments, "need_likes": 1, "sort": "desc"})
                if comments_lane != wall_lane:
                    diagnostics.append(f"vk {domain}: wall.getComments {post_id} ok via {comments_lane}")
            except Exception as exc:
                diagnostics.append(f"vk {domain}: wall.getComments {post_id} failed: {exc}")
                continue
            _human_pause_sync(multiplier=0.35)
            for comment in comments.get("items") or []:
                if _deadline_reached(deadline):
                    diagnostics.append("vk comment scan stopped by ACQ_RUNTIME_DEADLINE_SECONDS")
                    break
                if not isinstance(comment, dict):
                    continue
                VK_SCAN_STATS["comments_seen"] += 1
                for discovered in extract_candidate_surfaces(str(comment.get("text") or "")):
                    discovered["topic_hint"] = f"discovered in vk:{domain} comments"
                    if _is_out_of_region_surface(discovered):
                        discovered = _mark_out_of_region_surface(discovered, reason="out-of-region surface discovered in VK comments")
                    surfaces.setdefault(discovered["external_id"], discovered)
                opp = build_vk_opportunity(surface, owner_id=owner_id, post_id=post_id, comment=comment, default_target_url=default_target_url)
                if opp:
                    VK_SCAN_STATS["comment_prefilter_candidates"] += 1
                    opp["evidence"] = {**(opp.get("evidence") or {}), "relation": "vk_comment", "scanner": "vk_shadow"}
                    if _should_skip_opportunity_before_llm(
                        opp,
                        seen_contexts=seen_contexts,
                        opportunity_keys=opportunity_keys,
                        diagnostics=diagnostics,
                    ):
                        opp = None
                    else:
                        opp = _llm_review_opportunity_sync(opp, surface, diagnostics)
                if opp:
                    opportunities.append(opp)
                if len(opportunities) >= int(os.getenv("ACQ_MAX_OPPORTUNITIES_PER_RUN") or "30"):
                    break
        if _scan_vk_board_discussions(
            surface=surface,
            domain=domain,
            token_lanes=token_lanes,
            default_target_url=default_target_url,
            seen_contexts=seen_contexts,
            opportunity_keys=opportunity_keys,
            diagnostics=diagnostics,
            opportunities=opportunities,
            deadline=deadline,
        ):
            return list(surfaces.values()), opportunities, diagnostics
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
    source = "telega_in" if platform == "tg" and handle.lower() in _TELEGA_IN_TG_HANDLES else "seed"
    status = "needs_comment_resolve" if platform == "tg" else "candidate"
    return {
        "platform": platform,
        "surface_type": surface_type,
        "url": url,
        "handle": handle or None,
        "external_id": external_id,
        "status": status,
        "source": source,
        "topic_hint": "Telega.in Kaliningrad regional catalog seed" if source == "telega_in" else "Kaliningrad public/community seed",
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
        from telethon.tl.types import InputPeerChannel, PeerChannel
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
    max_channel_resolves = _int_env("ACQ_MAX_TG_CHANNEL_RESOLVES_PER_RUN", max(30, max_surfaces * 3), min_value=1)
    max_frontier = _int_env("ACQ_MAX_TG_FRONTIER_PER_RUN", max(20, max_surfaces * 4), min_value=max_surfaces)
    max_messages = _int_env("ACQ_MAX_MESSAGES_PER_SURFACE", 25, min_value=1)
    max_threads = _int_env("ACQ_MAX_THREADS_PER_SURFACE", 5, min_value=1)
    max_channel_link_posts = _int_env("ACQ_MAX_TG_CHANNEL_POSTS_FOR_LINKS", 5, min_value=0)
    max_opportunities = _int_env("ACQ_MAX_OPPORTUNITIES_PER_RUN", 30, min_value=1)
    search_queries = [str(q).strip() for q in list(_json_env("ACQ_TG_SEARCH_QUERIES_JSON", DEFAULT_TG_SEARCH_QUERIES) or []) if str(q).strip()]
    search_limit = _int_env("ACQ_TG_SEARCH_MESSAGES_PER_QUERY", 0, min_value=0)
    default_target_url = (os.getenv("ACQ_DEFAULT_LINK_TARGET_URL") or "https://t.me/kenigevents").strip()
    surfaces: dict[str, dict[str, Any]] = {}
    opportunities: list[dict[str, Any]] = []
    seen_contexts = _seen_context_urls()
    known_terminal_tg = _known_terminal_tg_handles()
    known_terminal_tg_skipped: set[str] = set()
    tg_seed_meta = _tg_seed_metadata()
    opportunity_keys: set[str] = set()
    queue: list[str] = []
    queued: set[str] = set()
    for raw in seed_urls:
        _enqueue_tg_url(queue, queued, str(raw), limit=max_frontier)
    discovered_queued = 0
    processed = 0
    replyable_processed = 0
    channel_resolves = 0
    deadline = _deadline_after_seconds()

    async with TelegramClient(StringSession(session_string), int(api_id), api_hash, flood_sleep_threshold=60, **device_config) as client:
        while queue and (replyable_processed < max_surfaces or channel_resolves < max_channel_resolves):
            if _deadline_reached(deadline):
                diagnostics.append("telegram scan stopped by ACQ_RUNTIME_DEADLINE_SECONDS")
                break
            raw_url = queue.pop(0)
            handle = _handle_from_url(raw_url)
            if not handle:
                diagnostics.append(f"skip invalid tg seed: {raw_url}")
                continue
            processed += 1
            progress = {
                "progress_percent": min(90, 20 + int(min(replyable_processed, max_surfaces) / max(1, max_surfaces) * 50)),
                "progress_label": f"telegram replyable {replyable_processed}/{max_surfaces} · channel resolve {channel_resolves}/{max_channel_resolves} · frontier +{discovered_queued}",
                "surfaces_done": replyable_processed,
                "surfaces_total": max_surfaces,
                "tg_channel_resolves_done": channel_resolves,
                "tg_channel_resolves_total": max_channel_resolves,
                "tg_frontier_queued": discovered_queued,
                "opportunities_found": len(opportunities),
                "llm_calls": int(LLM_GATE_STATS.get("calls", 0)),
                "llm_call_limit": _llm_gate_max_calls_per_run(),
            }
            _status_event("alive", phase="telegram_scan", status="running", progress=progress)
            await _human_pause_async(multiplier=0.8 if processed > 1 else 0.2)
            seed_meta = _metadata_for_tg_seed(raw_url, handle, tg_seed_meta)
            try:
                entity_ref = _telegram_entity_ref_from_seed(handle, seed_meta)
                entity = await client.get_entity(entity_ref)
            except Exception as exc:
                diagnostics.append(f"{handle}: get_entity failed: {exc}")
                continue
            TG_SCAN_STATS["surfaces_attempted"] += 1
            seed_surface_type = str(seed_meta.get("surface_type") or "").strip().lower()
            seed_source = str(seed_meta.get("source") or "").strip()
            surface_type = "channel" if bool(getattr(entity, "broadcast", False)) else "group" if bool(getattr(entity, "megagroup", False)) else "chat"
            if seed_surface_type == "linked_discussion":
                surface_type = "linked_discussion"
            surface = _seed_surface(f"https://t.me/{handle}", platform="tg")
            surface.update({
                "surface_type": surface_type,
                "title": getattr(entity, "title", None),
                "status": "needs_comment_resolve" if surface_type == "channel" else "candidate",
                "reach": {"members": getattr(entity, "participants_count", None), "confidence": "low", "basis": "telegram_entity"},
                "risk": {"spam_risk": "unknown", "safety_risk": "low", "reason": "read-only public scan"},
            })
            if seed_source:
                surface["source"] = seed_source
            if seed_meta.get("external_id"):
                surface["external_id"] = str(seed_meta.get("external_id"))
            if seed_meta.get("url"):
                surface["url"] = str(seed_meta.get("url"))
            if _is_out_of_region_surface(surface):
                surface = _mark_out_of_region_surface(surface, reason="resolved Telegram title/handle is outside Kaliningrad Oblast")
                surfaces[surface["external_id"]] = surface
                diagnostics.append(f"{handle}: rejected out-of-region surface")
                continue
            surfaces[surface["external_id"]] = surface

            scan_entities: list[tuple[Any, dict[str, Any], str | None]] = []
            channel_link_scan_entities: list[tuple[Any, dict[str, Any], str | None]] = []
            if surface_type == "channel":
                if channel_resolves >= max_channel_resolves:
                    diagnostics.append(f"{handle}: channel commentability resolve queued for future run; resolve budget exhausted")
                    continue
                channel_resolves += 1
                TG_SCAN_STATS["channel_resolve_attempts"] += 1
                linked_comment_scan_added = False
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
                            "status": "candidate",
                            "source": "linked_discussion",
                            "topic_hint": f"linked discussion for {handle}",
                            "reach": {"members": getattr(linked, "participants_count", None), "confidence": "low", "basis": "telegram_linked_discussion"},
                            "risk": {"spam_risk": "unknown", "safety_risk": "low", "reason": "read-only linked discussion scan"},
                        })
                        linked_access_hash = getattr(linked, "access_hash", None)
                        linked_id = getattr(linked, "id", None)
                        if linked_id is not None and linked_access_hash is not None:
                            linked_surface["telegram_access"] = {"id": str(linked_id), "access_hash": str(linked_access_hash)}
                        if _is_out_of_region_surface(linked_surface):
                            linked_surface = _mark_out_of_region_surface(linked_surface, reason="linked discussion title/handle is outside Kaliningrad Oblast")
                            surfaces[linked_surface["external_id"]] = linked_surface
                            diagnostics.append(f"{linked_handle}: rejected out-of-region linked discussion")
                        else:
                            surface = _mark_tg_channel_resolved_with_linked_discussion(surface, linked_surface=linked_surface)
                            surfaces[surface["external_id"]] = surface
                            surfaces[linked_surface["external_id"]] = linked_surface
                            if replyable_processed < max_surfaces:
                                scan_entities.append((linked, linked_surface, "linked_discussion"))
                            else:
                                diagnostics.append(f"{linked_handle}: linked discussion queued for future scan; replyable budget exhausted")
                            if max_channel_link_posts > 0:
                                channel_link_scan_entities.append((entity, surface, "channel_link_discovery"))
                            linked_comment_scan_added = True
                            TG_SCAN_STATS["channels_with_linked_discussion"] += 1
                except Exception as exc:
                    diagnostics.append(f"{handle}: linked discussion lookup failed: {exc}")
                if not linked_comment_scan_added:
                    surface = _mark_tg_channel_rejected_no_comments(surface)
                    surfaces[surface["external_id"]] = surface
                    TG_SCAN_STATS["channels_rejected_no_comments"] += 1
                    diagnostics.append(f"{handle}: rejected channel without accessible comments")
            elif replyable_processed < max_surfaces:
                relation_hint = "linked_discussion" if surface_type == "linked_discussion" else None
                scan_entities.append((entity, surface, relation_hint))
            else:
                diagnostics.append(f"{handle}: queued for future scan; replyable budget exhausted")

            async def process_tg_message(message: Any, scan_surface: dict[str, Any], relation: str | None, *, retrieval: str, search_query: str | None = None) -> bool:
                nonlocal discovered_queued
                text = str(getattr(message, "message", None) or "")
                for discovered in extract_candidate_surfaces(text):
                    discovered["topic_hint"] = f"discovered in {scan_surface.get('external_id')}"
                    if discovered.get("platform") == "tg":
                        discovered_handle = str(discovered.get("handle") or _handle_from_url(str(discovered.get("url") or ""))).strip().casefold()
                        if discovered_handle and discovered_handle in known_terminal_tg:
                            if discovered_handle not in known_terminal_tg_skipped:
                                diagnostics.append(f"{discovered_handle}: skipped discovered terminal tg surface from previous runs")
                                known_terminal_tg_skipped.add(discovered_handle)
                            continue
                    if _is_out_of_region_surface(discovered):
                        discovered = _mark_out_of_region_surface(discovered, reason="out-of-region surface discovered in Telegram message")
                    surfaces.setdefault(discovered["external_id"], discovered)
                    if (
                        discovered.get("platform") == "tg"
                        and _is_surface_scan_candidate(discovered)
                        and _enqueue_tg_url(queue, queued, str(discovered.get("url") or ""), limit=max_frontier)
                    ):
                        discovered_queued += 1
                        TG_SCAN_STATS["frontier_links_queued"] += 1
                if not is_comment_opportunity_message(
                    message,
                    surface_type=str(scan_surface.get("surface_type") or ""),
                    relation=relation,
                ):
                    return False
                opp = build_opportunity_from_message(scan_surface, message, default_target_url=default_target_url)
                if not opp:
                    return False
                opp["evidence"] = {
                    **(opp.get("evidence") or {}),
                    "relation": relation or "surface",
                    "scanner": "telegram_shadow",
                    "retrieval": retrieval,
                    "search_query": search_query,
                    "comment_only": True,
                }
                if _should_skip_opportunity_before_llm(
                    opp,
                    seen_contexts=seen_contexts,
                    opportunity_keys=opportunity_keys,
                    diagnostics=diagnostics,
                ):
                    return False
                reviewed = await _llm_review_opportunity_async(opp, scan_surface, diagnostics)
                if reviewed:
                    opportunities.append(reviewed)
                    return True
                return False

            for scan_entity, scan_surface, relation in [*channel_link_scan_entities, *scan_entities]:
                relation_is_channel_link_discovery = relation == "channel_link_discovery"
                if not relation_is_channel_link_discovery and _is_tg_replyable_surface_type(str(scan_surface.get("surface_type") or "")):
                    replyable_processed += 1
                    TG_SCAN_STATS["replyable_surfaces_scanned"] += 1
                    if relation == "linked_discussion":
                        TG_SCAN_STATS["linked_discussions_scanned"] += 1
                    else:
                        TG_SCAN_STATS["groups_or_chats_scanned"] += 1
                seen = 0
                try:
                    limit = max_channel_link_posts if relation_is_channel_link_discovery else max_messages
                    async for message in client.iter_messages(scan_entity, limit=limit):
                        if _deadline_reached(deadline):
                            diagnostics.append("telegram message scan stopped by ACQ_RUNTIME_DEADLINE_SECONDS")
                            break
                        seen += 1
                        if relation_is_channel_link_discovery:
                            TG_SCAN_STATS["channel_posts_seen_for_links"] += 1
                        else:
                            TG_SCAN_STATS["replyable_messages_seen"] += 1
                        await process_tg_message(message, scan_surface, relation, retrieval="latest")
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
                if search_limit > 0 and search_queries and len(opportunities) < max_opportunities:
                    if not relation_is_channel_link_discovery and str(scan_surface.get("surface_type") or "").casefold() != "channel":
                        for query in search_queries:
                            if _deadline_reached(deadline):
                                diagnostics.append("telegram search scan stopped by ACQ_RUNTIME_DEADLINE_SECONDS")
                                break
                            if len(opportunities) >= max_opportunities:
                                break
                            found = 0
                            try:
                                async for message in client.iter_messages(scan_entity, search=query, limit=search_limit):
                                    if _deadline_reached(deadline):
                                        diagnostics.append("telegram search message scan stopped by ACQ_RUNTIME_DEADLINE_SECONDS")
                                        break
                                    found += 1
                                    await process_tg_message(message, scan_surface, relation, retrieval="telegram_search", search_query=query)
                                    if len(opportunities) >= max_opportunities:
                                        break
                                    if found % 5 == 0:
                                        await _human_pause_async(multiplier=0.2)
                            except Exception as exc:
                                diagnostics.append(f"{handle}: iter_messages search={query!r} failed: {exc}")
                            if found:
                                diagnostics.append(f"{handle}: telegram_search {query!r} inspected {found} messages")
            if len(opportunities) >= max_opportunities:
                diagnostics.append(f"telegram opportunity scan capped at {max_opportunities} candidates")
                break
    diagnostics.append(
        "telegram resolver-first walk processed "
        f"{processed} handles; resolved {channel_resolves} channels; "
        f"scanned {replyable_processed} replyable surfaces; queued {discovered_queued} newly discovered tg links"
    )
    return list(surfaces.values()), opportunities, diagnostics


def _seen_context_urls() -> set[str]:
    raw = _json_env("ACQ_SEEN_CONTEXT_URLS_JSON", [])
    return {str(x).strip() for x in list(raw or []) if str(x).strip()}


def _known_terminal_tg_handles() -> set[str]:
    raw = _json_env("ACQ_KNOWN_TERMINAL_TG_HANDLES_JSON", [])
    return {str(x).strip().strip("/").casefold() for x in list(raw or []) if str(x).strip()}


def _tg_seed_metadata() -> dict[str, dict[str, Any]]:
    raw = _json_env("ACQ_TG_SEED_SURFACES_JSON", [])
    out: dict[str, dict[str, Any]] = {}
    for item in list(raw or []):
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        handle = str(item.get("handle") or _handle_from_url(url)).strip().strip("/")
        external_id = str(item.get("external_id") or "").strip()
        for key in {handle.casefold(), url.rstrip("/").casefold(), external_id.casefold()}:
            if key:
                out[key] = item
    return out


def _metadata_for_tg_seed(raw_url: str, handle: str, metadata: dict[str, dict[str, Any]]) -> dict[str, Any]:
    for key in [str(handle or "").casefold(), str(raw_url or "").rstrip("/").casefold(), f"tg:{handle}".casefold() if handle else ""]:
        if key and key in metadata:
            return metadata[key]
    return {}


def _telegram_entity_ref_from_seed(handle: str, seed_meta: dict[str, Any]) -> Any:
    access = seed_meta.get("telegram_access") if isinstance(seed_meta.get("telegram_access"), dict) else {}
    access_id = str(access.get("id") or handle or "").strip()
    access_hash = str(access.get("access_hash") or "").strip()
    if access_id.isdigit() and access_hash.lstrip("-").isdigit():
        return InputPeerChannel(int(access_id), int(access_hash))
    if str(handle or "").isdigit():
        return PeerChannel(int(handle))
    return handle


def build_shadow_payload(*, scanned_surfaces: list[dict[str, Any]] | None = None, scanned_opportunities: list[dict[str, Any]] | None = None, diagnostics: list[str] | None = None) -> dict[str, Any]:
    tg_seeds = _json_env("ACQ_TG_SEEDS_JSON", DEFAULT_TG_SEEDS)
    vk_seeds = _json_env("ACQ_VK_SEEDS_JSON", [])
    vk_allowlist = _json_env("ACQ_VK_ALLOWLIST_JSON", [])
    surfaces_by_external: dict[str, dict[str, Any]] = {}
    for url in list(tg_seeds or []):
        seed = _seed_surface(str(url), platform="tg")
        surfaces_by_external[seed["external_id"]] = seed
    # VK-ready but disabled unless explicit allowlist is provided. Seed rows are
    # stored for map/queue visibility, but scanned rows must overwrite them below
    # so the server can mark only actually touched surfaces as scanned.
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
    for scanned in scanned_surfaces or []:
        if scanned.get("external_id"):
            surfaces_by_external[str(scanned["external_id"])] = scanned
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
            "llm_gate_enabled": _llm_gate_enabled(),
            "llm_gate_model": _acq_llm_model(),
            "llm_gate": dict(LLM_GATE_STATS),
            "llm_gate_limits": _llm_limit_snapshot(),
            "tg_scan": dict(TG_SCAN_STATS),
            "vk_scan": dict(VK_SCAN_STATS),
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
