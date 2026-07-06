#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import base64
import csv
import hashlib
import html
import json
import os
import random
import re
import subprocess
import sys
import time
import zipfile
import urllib.parse
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from xml.sax.saxutils import escape

RUN_STARTED_AT = datetime.now(timezone.utc)
DEFAULT_ANCHORS = ["Калининград", "Калининградская область", "Куршская коса", "Зеленоградск", "Светлогорск", "Балтийское море", "Кёнигсберг", "Краснолесье", "Виштынец", "Роминтенская пуща", "Балтийская коса", "Янтарный", "Балтийск", "Советск", "Неман", "Правдинск", "Черняховск"]
TELEGRAM_KEYWORD_DISCOVERY_TERMS = [
    "Калининград", "Кёнигсберг", "Зеленоградск", "Светлогорск", "Янтарный", "Балтийск", "Пионерский",
    "Светлый", "Приморск", "Советск", "Черняховск", "Гусев", "Неман", "Полесск", "Правдинск",
    "Багратионовск", "Мамоново", "Нестеров", "Гвардейск", "Гурьевск", "Славск", "Озёрск",
    "Краснознаменск", "Ладушкин", "Краснолесье", "Донское", "Куликово", "Отрадное", "Морское",
    "Рыбачий", "Лесной", "Куршская коса", "Балтийская коса", "Виштынецкое озеро", "Роминтенская пуща",
    "Роминта", "Танцующий лес", "Высота Эфа", "Королевский бор", "Балтийское море", "Куршский залив",
    "Вислинский залив", "Филинская бухта", "Янтарный карьер", "Амалиенау", "Ратсхоф", "Рыбная деревня",
    "Остров Канта", "Кафедральный собор", "Музей Мирового океана", "Форт №5", "Фридрихсбургские ворота",
    "Закхаймские ворота", "Королевские ворота", "Верхнее озеро", "Нижнее озеро", "кирха Арнау",
    "Дом Советов", "Готическое кольцо", "замок Нессельбек", "замок Шаакен", "замок Тапиау",
    "Тильзит", "Инстербург", "Рагнит", "маяк Заливино", "маяк Балтийск",
]
NEWS_WORDS = ["происшеств", "дтп", "авар", "полици", "суд", "задерж", "штраф", "войн", "полит", "скандал", "убий", "пожар"]
AD_WORDS = ["скидк", "промокод", "купить", "заказать", "реклама", "партнёр", "партнер", "оплат", "бронь", "регистрация", "анонс", "конкурс", "диктант", "географический диктант", "билеты", "забронировать"]
TRASH_WORDS = ["жесть", "треш", "трэш", "шок", "кошмар"]
POSITIVE_WORDS = ["красив", "атмосфер", "море", "дюны", "архитект", "истори", "маршрут", "музей", "пляж", "курорт", "прогул", "путешеств"]

_REGION_TALK_SUPABASE_CLIENT: Any | None = None
_REGION_TALK_GOOGLE_CLIENT: Any | None = None
_REGION_TALK_CLIP_MODEL: Any | None = None
_REGION_TALK_CLIP_PROCESSOR: Any | None = None
_REGION_TALK_CLIP_DEVICE: str | None = None
_REGION_TALK_TELEGRAM_RUNTIME: dict[str, Any] = {}



def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def getenv_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def getenv_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        return int(raw) if raw else default
    except Exception:
        return default


def stable_hash(*parts: Any, length: int = 16) -> str:
    h = hashlib.sha256()
    for part in parts:
        h.update(str(part or "").strip().lower().encode("utf-8"))
        h.update(b"\0")
    return h.hexdigest()[:length]


def canonical_handle(value: str) -> str:
    raw = (value or "").strip()
    raw = re.sub(r"^https?://t\.me/", "@", raw, flags=re.I)
    raw = re.sub(r"^https?://(?:www\.)?vk\.com/", "@", raw, flags=re.I)
    if raw and not raw.startswith("@") and re.fullmatch(r"[A-Za-z0-9_\.]+", raw):
        raw = "@" + raw
    return raw


def seed_sort_number(value: Any) -> int:
    m = re.search(r"\d+", str(value or ""))
    return int(m.group(0)) if m else 999999


def canonical_source_url(platform: str, handle: str, url: str) -> str:
    if url:
        raw = url.strip()
        if platform == "telegram":
            m = re.search(r"(?:t\.me|telegram\.me)/(@?[A-Za-z0-9_]{4,})(?:/)?(?:\?.*)?$", raw, re.I)
            if m:
                return "https://t.me/" + m.group(1).lstrip("@")
        if platform in {"vk", "vkvideo"}:
            m = re.search(r"vk\.com/(?:club|public)?([A-Za-z0-9_.-]+)", raw, re.I)
            if m:
                ident = m.group(1)
                if ident.isdigit():
                    return "https://vk.com/club" + ident
                return "https://vk.com/" + ident
        return raw.rstrip("/")
    h = canonical_handle(handle).lstrip("@")
    if not h:
        return ""
    if platform == "telegram":
        return f"https://t.me/{h}"
    if platform in {"vk", "vkvideo"}:
        return f"https://vk.com/{h}"
    return h


def canonical_source_key(platform: str, handle: str = "", url: str = "") -> str:
    p = (platform or "").strip().lower()
    cu = canonical_source_url(p, handle, url).lower().rstrip("/")
    if p == "telegram":
        h = canonical_handle(handle or cu).lstrip("@").lower()
        if h:
            return "telegram:" + h
    if p.startswith("vk"):
        return "vk:" + re.sub(r"^https://vk\.com/", "", cu)
    return (p or "unknown") + ":" + (cu or canonical_handle(handle).lower())




def normalize_source_platform(value: str, url: str = "") -> str:
    raw = (value or "").strip().lower().replace("_", "-")
    low_url = (url or "").strip().lower()
    if raw in {"telegram", "tg", "telegram-channel", "telegram channel", "telegram_post", "telegram-post"} or "t.me/" in low_url or "telegram.me/" in low_url:
        return "telegram"
    if raw in {"vk", "vkontakte", "vk-public", "vk public", "vk-community", "vk community", "vk group", "vk_group"} or "vk.com/" in low_url:
        if "video" in raw:
            return "vkvideo"
        return "vk"
    if raw in {"youtube", "youtube-channel", "youtube channel"} or "youtube.com" in low_url or "youtu.be" in low_url:
        return "youtube"
    if raw in {"dzen", "zen", "dzen-channel", "dzen channel"} or "dzen.ru" in low_url:
        return "dzen"
    return raw or "unknown"

def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip() or line.lstrip().startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def load_split_runtime_from_kaggle_input() -> dict[str, Any]:
    roots = [Path("/kaggle/input"), Path.cwd()]
    config: dict[str, Any] = {}
    secret_files: list[Path] = []
    key_files: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        for p in root.rglob("region_talk_run_config.json"):
            try:
                config.update(json.loads(p.read_text(encoding="utf-8")))
            except Exception:
                pass
        secret_files.extend(root.rglob("region_talk_secrets.enc"))
        key_files.extend(root.rglob("region_talk_fernet.key"))
    if secret_files and key_files:
        try:
            from cryptography.fernet import Fernet
            key_by_parent = {k.parent.resolve(): k for k in key_files}
            pairs = []
            for secret_file in secret_files:
                parent = secret_file.parent.resolve()
                key_file = key_by_parent.get(parent)
                if key_file:
                    pairs.append((secret_file, key_file))
            if not pairs and len(secret_files) == 1 and len(key_files) == 1:
                pairs.append((secret_files[0], key_files[0]))
            last_error = None
            loaded = False
            for secret_file, key_file in pairs:
                try:
                    secrets = json.loads(Fernet(key_file.read_bytes().strip()).decrypt(secret_file.read_bytes()).decode("utf-8"))
                    for k, v in secrets.items():
                        if v is not None and str(v).strip():
                            os.environ.setdefault(str(k), str(v))
                    loaded = True
                    break
                except Exception as pair_exc:
                    last_error = pair_exc
                    continue
            if not loaded:
                raise last_error or RuntimeError("no matching region_talk_secrets.enc/region_talk_fernet.key pair")
        except Exception as exc:
            raise RuntimeError(f"failed to load encrypted Region Talk secrets: {type(exc).__name__}") from exc
    return config


class Status:
    def __init__(self) -> None:
        self.client = None
        try:
            from kaggle_status_client import KaggleStatusClient  # type: ignore
            self.client = KaggleStatusClient.discover()
        except Exception:
            self.client = None
        self.events: list[dict[str, Any]] = []

    def event(self, name: str, **payload: Any) -> None:
        clean = {k: v for k, v in payload.items() if v is not None}
        clean.setdefault("event_name", name)
        clean.setdefault("created_at", utc_now_iso())
        self.events.append(clean)
        print(f"[region-talk] {name} {json.dumps({k:v for k,v in clean.items() if k not in {'token'}}, ensure_ascii=False)[:800]}", flush=True)
        if self.client:
            try:
                self.client.event(name, phase=clean.get("phase"), status=clean.get("status"), progress_json=clean)
            except Exception:
                pass


@dataclass
class Seed:
    source_seed_id: str
    platform: str
    source_title: str
    handle: str
    url: str
    source_kind: str
    source_scope_guess: str
    priority: int
    discovered_from: str
    discovered_from_url: str
    why_seeded: str
    expected_value: str
    known_risks: str
    initial_status: str
    monitoring_enabled: bool
    rights_policy: str
    notes: str

    @property
    def source_id(self) -> str:
        return "src_" + stable_hash(self.platform, canonical_source_url(self.platform, self.handle, self.url))

    @property
    def canonical_url(self) -> str:
        return canonical_source_url(self.platform, self.handle, self.url)


def find_seed_file(config: dict[str, Any]) -> Path:
    candidates = []
    for raw in [os.getenv("REGION_TALK_SEED_FILE"), config.get("seed_file"), "seed-sources-v2.csv", "docs/features/region-talk-channel/seed-sources-v2.csv", "seed-sources-v1.csv", "docs/features/region-talk-channel/seed-sources-v1.csv"]:
        if raw:
            candidates.append(Path(str(raw)))
    for root in [Path.cwd(), Path(__file__).resolve().parent, Path("/kaggle/input")]:
        if root.exists():
            candidates.extend(root.rglob("seed-sources-v2.csv"))
            candidates.extend(root.rglob("seed-sources-v1.csv"))
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError("seed-sources-v2.csv/seed-sources-v1.csv not found")


def load_seeds(path: Path) -> list[Seed]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    seeds: list[Seed] = []
    for row in rows:
        seeds.append(Seed(
            source_seed_id=str(row.get("source_seed_id") or "").strip(),
            platform=str(row.get("platform") or "").strip().lower(),
            source_title=str(row.get("source_title") or "").strip(),
            handle=canonical_handle(str(row.get("handle") or "").strip()),
            url=str(row.get("url") or "").strip(),
            source_kind=str(row.get("source_kind") or "").strip(),
            source_scope_guess=str(row.get("source_scope_guess") or "").strip(),
            priority=getenv_int("_NO_SUCH_ENV", int(str(row.get("priority") or "999").strip() or "999")),
            discovered_from=str(row.get("discovered_from") or "").strip(),
            discovered_from_url=str(row.get("discovered_from_url") or "").strip(),
            why_seeded=str(row.get("why_seeded") or "").strip(),
            expected_value=str(row.get("expected_value") or "").strip(),
            known_risks=str(row.get("known_risks") or "").strip(),
            initial_status=str(row.get("initial_status") or "").strip(),
            monitoring_enabled=str(row.get("monitoring_enabled") or "").strip().lower() in {"1","true","yes","on"},
            rights_policy=str(row.get("rights_policy") or "unknown").strip() or "unknown",
            notes=str(row.get("notes") or "").strip(),
        ))
    return seeds



PLACE_LEXICON_FIELDS = [
    "place_id", "canonical_name", "place_type", "municipality", "district_or_okrug", "is_city", "is_settlement",
    "is_tourist_place", "is_nature_place", "is_historical_name", "aliases", "old_names", "latin_aliases",
    "common_misspellings", "geo_scope", "priority_tier", "ambiguity_level", "allowed_for_kaliningrad_scope",
    "requires_context", "reject_if_external_context", "source_url", "source_note",
]
EXTERNAL_REGION_TERMS = [
    "карелия", "алтай", "дагестан", "байкал", "камчатка", "сахалин", "кавказ", "крым", "сочи",
    "санкт-петербург", "петербург", "ленинградская область", "москва", "московская область", "казань",
    "татарстан", "владимир", "суздаль", "ярославль", "кострома", "нижний новгород", "псков",
    "новгород", "мурманск", "териберка", "архангельск", "вологда", "урал", "сибирь", "приморье",
    "владивосток", "краснодарский край", "адыгея", "эльбрус", "чечня", "ингушетия", "осетия",
    "башкирия", "пермский край", "самара", "саратов", "волгоград", "астрахань", "тюмень",
    "челябинск", "челябинская область", "якутия", "саха",
]
EXTERNAL_COUNTRY_TERMS = [
    "польша", "литва", "латвия", "эстония", "германия", "беларусь", "грузия", "армения", "турция",
    "италия", "франция", "испания", "португалия", "норвегия", "финляндия", "швеция", "китай",
]
AD_PROMO_WORDS = [
    "реклама", "промокод", "скидк", "акция", "конкурс", "розыгрыш", "партнёр", "партнер", "спонсор",
    "купить", "заказать", "забронировать", "бронь", "билеты", "билет", "стоимость", "цена", "руб",
    "регистрация", "зарегистр", "анонс", "приглашаем", "приходите", "участвуйте", "географический диктант",
    "диктант", "тур ", "туры", "экскурсия", "экскурсии", "программа мероприятия", "мероприятие состоится",
]
AD_PROMO_HARD_PATTERNS = [
    ("ad_label", r"(?<![а-яa-z])(?:реклама|на правах рекламы|партнерск(?:ий|ая|ое|ие)|партн[её]рский материал)(?![а-яa-z])"),
    ("promo_code", r"(?<![а-яa-z])(?:промокод|скидк[аиуой]?|акци[ияю]|розыгрыш|конкурс)(?![а-яa-z])"),
    ("price_rub", r"(?:\b\d[\d\s]*(?:₽|руб(?:\.|лей|ля|ль)?\b)|(?:стоимость|цена|оплата|оплатить)\b)"),
    ("booking_tickets", r"(?<![а-яa-z])(?:купить|заказать|забронировать|бронь|билеты?|регистраци[яию]|зарегистр(?:ироваться|ируйтесь|ируйся)|приходите|участвуйте)(?![а-яa-z])"),
    ("paid_tour_service", r"(?<![а-яa-z])(?:туры?|экскурси[яию]|гид|туроператор|пут[её]вк[аиу]|программа мероприятия|мероприятие состоится|географический диктант|диктант)(?![а-яa-z])"),
    ("app_download", r"(?:скачайте|установите|приложени[ея]|app store|google play)"),
]
AD_PROMO_POSSIBLE_PATTERNS = [
    ("announcement_tone", r"(?<![а-яa-z])(?:анонс|приглашаем|афиша|состоится)(?![а-яa-z])"),
    ("service_context", r"(?<![а-яa-z])(?:маршрут от|проект|сервис|официальный портал)(?![а-яa-z])"),
]
SUBSTANCE_WORDS = ["маршрут", "дорога", "путь", "добраться", "совет", "полезн", "истори", "место", "что посмотреть", "где", "когда", "почему"]
VISIT_WORDS = ["побывал", "побывали", "посетил", "посетили", "ездили", "поехали", "приехали", "гуляли", "увидели", "запомнил", "запомнилось", "впечатлен", "впечатления"]
EMOTION_WORDS = ["красив", "впечатля", "атмосфер", "удивител", "люблю", "очар", "запомни", "магия", "спокойн", "вдохнов", "вау", "эмоци"]
MEMORABLE_WORDS = ["больше всего", "особенно", "запомни", "неожиданно", "удивило", "главное", "лучшее", "самое", "деталь", "история"]
URL_RE = re.compile(r"(?i)\b(?:https?://|www\.)\S+|\bt\.me/[A-Za-z0-9_+/.-]+|\bvk\.com/[A-Za-z0-9_./-]+")
HANDLE_RE = re.compile(r"(?<!\w)@[A-Za-z0-9_]{4,}")
STATE_FILE_NAME = "region-talk-state.json"

def selected_sources_for_run(seeds: list[Seed], max_sources: int) -> list[Seed]:
    enabled = [s for s in seeds if s.monitoring_enabled]
    fallback = [s for s in seeds if not s.monitoring_enabled]
    ordered = sorted(enabled, key=lambda s: (s.priority, seed_sort_number(s.source_seed_id)))
    seen = {s.source_seed_id for s in ordered}
    for seed in sorted(fallback, key=lambda s: (s.priority, seed_sort_number(s.source_seed_id))):
        if seed.source_seed_id not in seen:
            ordered.append(seed)
            seen.add(seed.source_seed_id)
    return ordered[:max(0, max_sources)]


def seed_from_frontier_row(row: dict[str, Any], idx: int) -> Seed | None:
    platform = str(row.get("platform") or row.get("platform_guess") or "").strip().lower()
    if not platform.startswith("telegram"):
        return None
    url = str(row.get("canonical_url") or row.get("normalized_url") or row.get("recommended_canonical_url") or "").strip()
    handle = canonical_handle(str(row.get("username_or_handle") or row.get("recommended_username") or url).strip())
    if not handle or "/" in handle.lstrip("@"):
        return None
    title = str(row.get("title_guess") or row.get("recommended_title") or row.get("to_source_title") or handle).strip()
    return Seed(
        source_seed_id=f"frontier_dynamic_{idx}_{stable_hash(url or handle, length=8)}",
        platform="telegram",
        source_title=title,
        handle=handle,
        url=url or ("https://t.me/" + handle.lstrip("@")),
        source_kind="frontier_telegram",
        source_scope_guess="travel_frontier",
        priority=2,
        discovered_from=str(row.get("best_discovered_from_source") or row.get("discovered_from_source") or "source_frontier"),
        discovered_from_url=str(row.get("best_discovered_from_post_url") or row.get("telegram_similar_seed_channel") or ""),
        why_seeded="promoted from persistent source frontier queue",
        expected_value="shallow probe / delta history scan",
        known_risks=str(row.get("rejection_or_skip_reason") or ""),
        initial_status="frontier_promoted",
        monitoring_enabled=True,
        rights_policy="unknown",
        notes="dynamic seed from previous Region Talk state; no auto-join",
    )


def frontier_dynamic_seeds(previous_state: dict[str, Any], max_items: int) -> list[Seed]:
    rows: list[dict[str, Any]] = []
    queue = previous_state.get("source_frontier_queue_next")
    if isinstance(queue, dict):
        rows.extend([v for v in queue.values() if isinstance(v, dict)])
    unique = previous_state.get("source_frontier_unique")
    if isinstance(unique, dict):
        rows.extend([v for v in unique.values() if isinstance(v, dict)])
    selected: list[Seed] = []
    seen: set[str] = set()
    rows = sorted(rows, key=lambda r: (
        str(r.get("frontier_stage") or "") not in {"history_due", "probe_due", "unresolved"},
        -float(r.get("frontier_priority") or r.get("source_candidate_score") or 0),
        str(r.get("canonical_url") or r.get("normalized_url") or ""),
    ))
    for i, row in enumerate(rows, start=1):
        seed = seed_from_frontier_row(row, i)
        if not seed:
            continue
        key = seed.canonical_url
        if key in seen:
            continue
        seen.add(key)
        selected.append(seed)
        if len(selected) >= max_items:
            break
    return selected


def source_status_row(seed: Seed, status_value: str, **extra: Any) -> dict[str, Any]:
    return {
        **asdict(seed),
        "source_id": seed.source_id,
        "canonical_url": seed.canonical_url,
        "canonical_source_key": canonical_source_key(seed.platform, seed.handle, seed.canonical_url),
        "fetch_status": status_value,
        "posts_scanned": 0,
        "source_selected_this_run": "true",
        "source_cursor_strategy": "fresh_first_min_post_date_then_anchor_search",
        **extra,
    }


def state_file_candidates(output_dir: Path) -> list[Path]:
    out: list[Path] = []
    if os.getenv("REGION_TALK_STATE_FILE"):
        out.append(Path(str(os.getenv("REGION_TALK_STATE_FILE"))))
    cwd = Path.cwd()
    out.extend([
        output_dir.parent.parent / "state" / STATE_FILE_NAME if len(output_dir.parents) >= 2 else output_dir.parent / STATE_FILE_NAME,
        output_dir.parent / STATE_FILE_NAME,
    ])
    try:
        if output_dir.resolve().is_relative_to(cwd.resolve()):
            out.append(cwd / "artifacts" / "region-talk" / "state" / STATE_FILE_NAME)
    except Exception:
        pass
    inp = Path("/kaggle/input")
    if inp.exists():
        out.extend(inp.rglob(STATE_FILE_NAME))
    seen: set[str] = set()
    unique: list[Path] = []
    for p in out:
        key = str(p)
        if key not in seen:
            seen.add(key)
            unique.append(p)
    return unique


def parse_iso_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def iso_after_seconds(seconds: int) -> str:
    return datetime.fromtimestamp(time.time() + max(0, seconds), timezone.utc).isoformat()




def region_talk_state_backend_requested() -> str:
    return (os.getenv("REGION_TALK_STATE_BACKEND") or "json").strip().lower() or "json"


def ydb_config_status() -> dict[str, str]:
    endpoint = (os.getenv("REGION_TALK_YDB_ENDPOINT") or "").strip()
    database = (os.getenv("REGION_TALK_YDB_DATABASE") or "").strip()
    namespace = (os.getenv("REGION_TALK_YDB_NAMESPACE") or "region_talk").strip() or "region_talk"
    missing = [k for k, v in {"REGION_TALK_YDB_ENDPOINT": endpoint, "REGION_TALK_YDB_DATABASE": database}.items() if not v]
    return {"endpoint": endpoint, "database": database, "namespace": namespace, "missing": ",".join(missing)}


def ydb_table_plan() -> str:
    return ";".join([
        "region_talk_sources", "region_talk_source_edges", "region_talk_telegram_entity_cache",
        "region_talk_similar_seed_cursor", "region_talk_posts", "region_talk_candidate_memory",
        "region_talk_run_artifacts", "region_talk_state_snapshots",
    ])


def load_region_talk_ydb_state() -> tuple[dict[str, Any], dict[str, Any]]:
    cfg = ydb_config_status()
    meta = {"state_backend_requested": "ydb", "state_backend": "ydb", "state_fallback_used": "false", "ydb_namespace": cfg["namespace"], "ydb_tables_expected": ydb_table_plan(), "ydb_read_status": "not_started", "ydb_write_status": "not_started"}
    if cfg["missing"]:
        meta.update({"ydb_read_status": "missing_config", "ydb_error": "missing " + cfg["missing"]})
        return {}, meta
    try:
        import ydb  # type: ignore
    except Exception as exc:
        meta.update({"ydb_read_status": "sdk_missing", "ydb_error": f"{type(exc).__name__}: {str(exc)[:160]}"})
        return {}, meta
    # MVP contract gate: do not invent a new production schema if the snapshot table/path is not explicitly configured.
    # Full table upserts are represented in the exported snapshot and documented table plan; production enablement
    # must provide REGION_TALK_YDB_STATE_SNAPSHOT_FILE or a future SDK-backed table adapter.
    snapshot = (os.getenv("REGION_TALK_YDB_STATE_SNAPSHOT_FILE") or "").strip()
    if not snapshot:
        meta.update({"ydb_read_status": "adapter_not_configured", "ydb_error": "YDB SDK import ok but REGION_TALK_YDB_STATE_SNAPSHOT_FILE/table adapter is not configured"})
        return {}, meta
    path = Path(snapshot)
    try:
        raw = path.read_bytes()
        data = json.loads(raw.decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("snapshot is not JSON object")
        state_hash = hashlib.sha256(raw).hexdigest()
        data["_loaded_from_path"] = str(path)
        meta.update({"ydb_read_status": "ok", "previous_state_loaded": "true", "increment_state_loaded": "true", "previous_state_source": "ydb:" + str(path), "increment_state_path": "ydb:" + str(path), "previous_state_run_id": data.get("run_id", ""), "previous_run_id": data.get("run_id", ""), "previous_state_hash": state_hash, "state_schema_version_previous": data.get("state_schema_version", ""), "previous_seen_post_count": len(data.get("posts") or {}), "previous_candidate_memory_count": len(data.get("candidate_memory") or {})})
        return data, meta
    except Exception as exc:
        meta.update({"ydb_read_status": "error", "ydb_error": f"{type(exc).__name__}: {str(exc)[:220]}"})
        return {}, meta


def save_region_talk_ydb_state(output_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    cfg = ydb_config_status()
    meta = {"state_backend_requested": "ydb", "state_backend": "ydb", "state_fallback_used": "false", "ydb_namespace": cfg["namespace"], "ydb_tables_expected": ydb_table_plan(), "ydb_write_status": "not_started"}
    if cfg["missing"]:
        return {**meta, "ydb_write_status": "missing_config", "state_write_status": "error", "state_write_error": "missing " + cfg["missing"]}
    snapshot = (os.getenv("REGION_TALK_YDB_STATE_SNAPSHOT_FILE") or "").strip()
    if not snapshot:
        return {**meta, "ydb_write_status": "adapter_not_configured", "state_write_status": "error", "state_write_error": "YDB snapshot/table adapter is not configured"}
    try:
        target = Path(snapshot)
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(state, ensure_ascii=False, indent=2)
        target.write_text(payload, encoding="utf-8")
        state_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        return {**meta, "ydb_write_status": "ok", "state_write_status": "ok", "state_write_path": "ydb:" + str(target), "latest_state_run_id": state.get("run_id", ""), "latest_state_uri": "ydb:" + str(target), "latest_state_hash": state_hash, "latest_state_pointer_path": "ydb:" + str(target)}
    except Exception as exc:
        return {**meta, "ydb_write_status": "error", "state_write_status": "error", "state_write_error": f"{type(exc).__name__}: {str(exc)[:220]}"}

def load_region_talk_state(output_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    requested_backend = region_talk_state_backend_requested()
    if requested_backend == "ydb":
        ydb_state, ydb_meta = load_region_talk_ydb_state()
        if ydb_state:
            return ydb_state, ydb_meta
        if getenv_bool("REGION_TALK_REQUIRE_YDB_STATE", False):
            raise RuntimeError("REGION_TALK_REQUIRE_YDB_STATE=1 but YDB state is unavailable: " + str(ydb_meta.get("ydb_error") or ydb_meta.get("ydb_read_status")))
        fallback_state, fallback_meta = load_region_talk_json_state(output_dir)
        fallback_meta.update({**ydb_meta, "state_backend": "json_fallback", "state_fallback_used": "true", "state_fallback_reason": ydb_meta.get("ydb_error") or ydb_meta.get("ydb_read_status") or "ydb_unavailable", "previous_state_loaded": fallback_meta.get("previous_state_loaded", "false"), "previous_state_hash": fallback_meta.get("previous_state_hash", "")})
        return fallback_state, fallback_meta
    return load_region_talk_json_state(output_dir)


def load_region_talk_json_state(output_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    for path in state_file_candidates(output_dir):
        try:
            if path.exists():
                raw = path.read_bytes()
                data = json.loads(raw.decode("utf-8"))
                if isinstance(data, dict):
                    data["_loaded_from_path"] = str(path)
                    state_hash = hashlib.sha256(raw).hexdigest()
                    return data, {
                        "increment_state_loaded": "true",
                        "increment_state_path": str(path),
                        "previous_state_loaded": "true",
                        "previous_state_source": str(path),
                        "previous_state_run_id": data.get("run_id", ""),
                        "previous_state_hash": state_hash,
                        "previous_run_id": data.get("run_id", ""),
                        "state_schema_version_previous": data.get("state_schema_version", ""),
                        "previous_seen_post_count": len(data.get("posts") or {}),
                        "previous_candidate_memory_count": len(data.get("candidate_memory") or {}),
                    }
        except Exception as exc:
            return {}, {"increment_state_loaded": "false", "increment_state_path": str(path), "previous_state_loaded": "false", "previous_state_source": str(path), "previous_state_hash": "", "increment_state_error": f"{type(exc).__name__}: {str(exc)[:180]}", "previous_run_id": "", "previous_seen_post_count": 0}
    if getenv_bool("REGION_TALK_REQUIRE_PREVIOUS_STATE", False):
        raise RuntimeError("REGION_TALK_REQUIRE_PREVIOUS_STATE=1 but no previous Region Talk state was found")
    return {}, {"increment_state_loaded": "false", "increment_state_path": "", "previous_state_loaded": "false", "previous_state_source": "", "previous_state_hash": "", "increment_state_note": "baseline run, not real increment: no previous dry-run state file found", "previous_run_id": "", "previous_seen_post_count": 0, "previous_candidate_memory_count": 0}


def load_public_blogger_links(config: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    candidates = []
    for raw in [
        os.getenv("REGION_TALK_PUBLIC_BLOGGER_LINKS_FILE"),
        (config or {}).get("public_blogger_links_file") if config else None,
        "public_travel_blogger_channel_links.xlsx",
        "artifacts/public_travel_blogger_channel_links.xlsx",
        "/home/dev/projects/events-bot-new/artifacts/public_travel_blogger_channel_links.xlsx",
    ]:
        if raw:
            candidates.append(Path(str(raw)))
    for root in [Path.cwd(), Path(__file__).resolve().parent, Path("/kaggle/input")]:
        if root.exists():
            candidates.extend(root.rglob("public_travel_blogger_channel_links.xlsx"))
    path = next((p for p in candidates if p.exists()), None)
    if not path:
        return []
    try:
        from openpyxl import load_workbook  # type: ignore
    except Exception:
        if getenv_bool("REGION_TALK_AUTO_INSTALL", True):
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "openpyxl"])
            from openpyxl import load_workbook  # type: ignore
        else:
            return []
    wb = load_workbook(path, read_only=True, data_only=True)
    if "Links" not in wb.sheetnames:
        return []
    ws = wb["Links"]
    rows_iter = ws.iter_rows(values_only=True)
    headers = [str(x or "").strip() for x in next(rows_iter, [])]
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows_iter:
        rec = {headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))}
        url = str(rec.get("URL") or "").strip()
        platform = normalize_source_platform(str(rec.get("Platform") or ""), url)
        handle = canonical_handle(str(rec.get("Handle") or "").strip())
        if not platform or not url:
            continue
        canonical_key = canonical_source_key(platform, handle, url)
        key = (platform, canonical_key)
        if key in seen:
            continue
        seen.add(key)
        canonical = canonical_source_url(platform, handle, url)
        cand_id = "src_cand_" + stable_hash(platform, canonical_key)
        out.append({
            "source_candidate_id": cand_id,
            "frontier_source_id": cand_id,
            "canonical_source_key": canonical_key,
            "platform": platform,
            "platform_guess": platform,
            "canonical_url": canonical,
            "normalized_url": canonical,
            "username_or_handle": handle,
            "title_guess": str(rec.get("Handle") or handle or canonical).strip(),
            "source_title": str(rec.get("Handle") or handle or canonical).strip(),
            "category": str(rec.get("Category") or ""),
            "source_catalog": str(rec.get("Source") or ""),
            "source_page": str(rec.get("Source page") or ""),
            "discovery_type": "public_travel_blogger_catalog",
            "edge_type": "public_travel_blogger_catalog",
            "candidate_source_status": "source_frontier",
            "frontier_status": "queued_unresolved" if platform != "vk" else "unsupported_until_vk_wall_enabled",
            "scan_status": "pending_primary_scan" if platform == "telegram" else ("unsupported_until_vk_wall_enabled" if platform == "vk" else "unsupported_backlog"),
            "confidence": 0.45 if platform == "telegram" else 0.35,
            "frontier_priority": 0.62 if platform == "telegram" else 0.40,
            "frontier_reason": "public travel/blogger catalog import; do not auto-fetch without frontier scoring",
            "discovered_from_source": str(rec.get("Source") or "public_travel_blogger_channel_links.xlsx"),
            "discovered_from_post_url": "",
            "raw_url": url,
        })
    return out


def save_region_talk_state(output_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    requested_backend = region_talk_state_backend_requested()
    if requested_backend == "ydb":
        ydb_meta = save_region_talk_ydb_state(output_dir, state)
        json_meta = save_region_talk_json_state(output_dir, state)
        if ydb_meta.get("state_write_status") == "ok":
            return {**json_meta, **ydb_meta, "json_backup_write_status": json_meta.get("state_write_status", "")}
        if getenv_bool("REGION_TALK_REQUIRE_YDB_STATE", False):
            raise RuntimeError("REGION_TALK_REQUIRE_YDB_STATE=1 but YDB state write failed: " + str(ydb_meta.get("state_write_error") or ydb_meta.get("ydb_write_status")))
        return {**json_meta, **ydb_meta, "state_backend": "json_fallback", "state_fallback_used": "true", "state_fallback_reason": ydb_meta.get("state_write_error") or ydb_meta.get("ydb_write_status") or "ydb_write_failed"}
    return save_region_talk_json_state(output_dir, state)


def save_region_talk_json_state(output_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    target = state_file_candidates(output_dir)[0]
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(state, ensure_ascii=False, indent=2)
        state_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        target.write_text(payload, encoding="utf-8")
        run_copy = output_dir / STATE_FILE_NAME
        if run_copy.resolve() != target.resolve():
            run_copy.write_text(payload, encoding="utf-8")
        latest_pointer = target.parent / "latest-region-talk-state.json"
        latest_pointer.write_text(json.dumps({"latest_state_run_id": state.get("run_id"), "latest_state_uri": str(target), "latest_state_hash": state_hash, "updated_at": state.get("updated_at")}, ensure_ascii=False, indent=2), encoding="utf-8")
        return {"state_write_status": "ok", "state_write_path": str(target), "latest_state_run_id": state.get("run_id", ""), "latest_state_uri": str(target), "latest_state_hash": state_hash, "latest_state_pointer_path": str(latest_pointer)}
    except Exception as exc:
        return {"state_write_status": "error", "state_write_error": f"{type(exc).__name__}: {str(exc)[:240]}", "state_write_path": str(target)}


def classify_pre_llm_reject_reason(fresh: dict[str, Any], scope: dict[str, Any], ad_gate: dict[str, Any], substance: dict[str, Any], ts: dict[str, Any]) -> str:
    if not fresh.get("fresh_enough"):
        return "reject_old_post"
    if not scope.get("kaliningrad_oblast_only_scope"):
        return "reject_not_kaliningrad_oblast_only"
    if ad_gate.get("is_ad_or_promo"):
        return "reject_ad_or_promo"
    if float(ts.get("newsiness_score") or 0) >= 0.70 or float(ts.get("trash_score") or 0) >= 0.70:
        return "reject_news_or_trash"
    if float(substance.get("text_substance_score") or 0) < 0.18:
        return "reject_low_substance"
    return "reject_source_boilerplate"


def git_provenance() -> dict[str, str]:
    out = {"git_sha": os.getenv("GIT_SHA", ""), "git_sha_short": "", "branch": os.getenv("GIT_BRANCH", "")}
    try:
        sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
        out["git_sha"] = out["git_sha"] or sha
        out["git_sha_short"] = sha[:8]
    except Exception:
        out["git_sha_short"] = (out.get("git_sha") or "")[:8]
    try:
        branch = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
        out["branch"] = out["branch"] or branch
    except Exception:
        pass
    return out


class TelegramRequestGovernor:
    def __init__(self, run_id: str, output_dir: Path, state: dict[str, Any]) -> None:
        self.run_id = run_id
        self.output_dir = output_dir
        self.state = state if isinstance(state, dict) else {}
        self.entity_cache: dict[str, Any] = dict(self.state.get("telegram_entity_cache") or {})
        self.entity_cache_loaded_from_path = str((self.state.get("_loaded_from_path") or self.state.get("state_path") or ""))
        self.cooldowns: dict[str, Any] = dict(self.state.get("telegram_cooldowns") or {})
        self.ledger: list[dict[str, Any]] = []
        self.requests_by_method: dict[str, int] = {}
        self.total_attempted = 0
        self.total_ok = 0
        self.total_error = 0
        self.resolve_cache_hits = 0
        self.resolve_network_attempts = 0
        self.resolve_network_ok = 0
        self.resolve_network_floodwait = 0
        self.resolve_skipped_by_cooldown = 0
        self.recommendation_calls_attempted = 0
        self.recommendation_calls_ok = 0
        self.recommendation_channels_returned = 0
        self.recommendation_channels_added_to_frontier = 0
        self.history_sources_attempted = 0
        self.history_sources_ok = 0
        self.history_posts_fetched = 0
        self.media_downloads_attempted = 0
        self.media_downloads_ok = 0
        self.max_floodwait_seconds = 0
        self.floodwait_method = ""
        self.floodwait_cooldown_until = ""
        self.telegram_phase_status = "ok"
        self.degraded = False
        self.max_total_requests = getenv_int("REGION_TALK_TG_MAX_TOTAL_REQUESTS_PER_RUN", 800)
        self.max_network_resolves = getenv_int("REGION_TALK_TG_MAX_NETWORK_RESOLVES_PER_RUN", 8)
        self.max_history_sources = getenv_int("REGION_TALK_HISTORY_SOURCES_TARGET", getenv_int("REGION_TALK_TG_MAX_HISTORY_SOURCES_PER_RUN", 100))
        self.max_media_downloads = getenv_int("REGION_TALK_TG_MAX_MEDIA_DOWNLOADS_PER_RUN", 120)
        self.max_recommendation_calls = getenv_int("REGION_TALK_MAX_SIMILAR_SEEDS_PER_RUN", getenv_int("REGION_TALK_TG_MAX_RECOMMENDATION_CALLS_PER_RUN", 100))
        self.floodwait_abort_threshold = getenv_int("REGION_TALK_TG_FLOODWAIT_ABORT_THRESHOLD_SECONDS", 300)
        self.floodwait_margin = getenv_int("REGION_TALK_TG_FLOODWAIT_COOLDOWN_MARGIN_SECONDS", 1800)

    def cache_key(self, handle_or_url: str) -> str:
        raw = canonical_handle(str(handle_or_url or "").strip()).lstrip("@").lower()
        raw = re.sub(r"^https?://t\.me/", "", raw, flags=re.I).split("/", 1)[0].lower()
        return "telegram:username:" + raw

    def cooldown_active(self, key: str) -> tuple[bool, str]:
        rec = self.cooldowns.get(key) or {}
        until = str(rec.get("cooldown_until") or "")
        dt = parse_iso_datetime(until)
        if dt and dt > datetime.now(timezone.utc):
            return True, until
        return False, until

    def log(self, method_name: str, method_class: str, source_id: str, canonical_url: str, decision: str, **extra: Any) -> None:
        rec = {
            "ts": utc_now_iso(), "run_id": self.run_id, "method_name": method_name, "method_class": method_class,
            "source_id": source_id, "canonical_url": canonical_url, "decision": decision, **extra,
        }
        self.ledger.append(rec)

    def mark_floodwait(self, method_name: str, source_id: str, canonical_url: str, seconds: int) -> str:
        cooldown_until = iso_after_seconds(seconds + self.floodwait_margin)
        self.max_floodwait_seconds = max(self.max_floodwait_seconds, seconds)
        self.floodwait_method = method_name
        self.floodwait_cooldown_until = cooldown_until
        self.telegram_phase_status = "degraded_floodwait"
        self.degraded = True
        self.cooldowns[f"method:{method_name}"] = {"cooldown_until": cooldown_until, "last_error_seconds": seconds, "last_error_run_id": self.run_id}
        if canonical_url:
            self.cooldowns[f"source:{canonical_url}"] = {"cooldown_until": cooldown_until, "reason": "telegram_floodwait", "last_error_seconds": seconds}
        self.log(method_name, "entity_resolve_expensive", source_id, canonical_url, "error_floodwait", ok=False, floodwait_seconds=seconds, cooldown_until=cooldown_until)
        return cooldown_until

    def has_total_request_budget(self, method_name: str, source_id: str = "", canonical_url: str = "") -> bool:
        if self.total_attempted >= self.max_total_requests:
            self.telegram_phase_status = "degraded_request_budget_exhausted"
            self.degraded = True
            self.log(method_name, "rate_budget", source_id, canonical_url, "skipped_total_request_budget", ok=False, max_total_requests=self.max_total_requests)
            return False
        return True

    async def resolve_entity(self, client: Any, seed: Seed) -> tuple[Any | None, dict[str, Any]]:
        canonical_url = seed.canonical_url
        source_id = seed.source_id
        handle = seed.handle.lstrip("@")
        key = self.cache_key(seed.handle or seed.url)
        source_cd, source_until = self.cooldown_active(f"source:{canonical_url}")
        method_cd, method_until = self.cooldown_active("method:ResolveUsernameRequest")
        if self.degraded or source_cd or method_cd:
            self.resolve_skipped_by_cooldown += 1
            status = "skipped_telegram_global_cooldown" if self.degraded or method_cd else "skipped_telegram_resolve_cooldown"
            self.log("ResolveUsernameRequest", "entity_resolve_expensive", source_id, canonical_url, status, ok=False, cooldown_until=method_until or source_until)
            return None, {"fetch_status": status, "telegram_resolve_status": status, "next_allowed_resolve_at": method_until or source_until}
        cached = self.entity_cache.get(key) or {}
        if cached.get("channel_id_private") and cached.get("access_hash_private"):
            try:
                from telethon.tl.types import InputPeerChannel  # type: ignore
                entity = InputPeerChannel(int(cached["channel_id_private"]), int(cached["access_hash_private"]))
                self.resolve_cache_hits += 1
                self.log("ResolveUsernameRequest", "entity_resolve_expensive", source_id, canonical_url, "skipped_cache_hit", cache_hit=True, network_call=False, ok=True)
                return entity, {"telegram_resolve_status": "resolved_from_private_cache", "resolved_title": cached.get("title_last_seen", "")}
            except Exception:
                pass
        if self.resolve_network_attempts >= self.max_network_resolves or not self.has_total_request_budget("ResolveUsernameRequest", source_id, canonical_url):
            self.log("ResolveUsernameRequest", "entity_resolve_expensive", source_id, canonical_url, "skipped_budget", ok=False)
            return None, {"fetch_status": "skipped_telegram_budget_exhausted", "telegram_resolve_status": "skipped_budget", "resolve_attempt_count": self.resolve_network_attempts}
        self.resolve_network_attempts += 1
        self.total_attempted += 1
        self.requests_by_method["ResolveUsernameRequest"] = self.requests_by_method.get("ResolveUsernameRequest", 0) + 1
        try:
            entity = await client.get_entity(handle)
            self.total_ok += 1
            self.resolve_network_ok += 1
            self.entity_cache[key] = {
                "canonical_url": canonical_url,
                "username": handle,
                "title_last_seen": str(getattr(entity, "title", None) or seed.source_title),
                "entity_kind": "channel",
                "channel_id_private": str(getattr(entity, "id", "") or ""),
                "access_hash_private": str(getattr(entity, "access_hash", "") or ""),
                "resolved_at": utc_now_iso(),
                "last_used_at": utc_now_iso(),
                "last_success_at": utc_now_iso(),
                "last_error_at": None,
                "last_error_code": None,
                "resolve_attempt_count": int((cached or {}).get("resolve_attempt_count") or 0) + 1,
                "resolve_network_count": int((cached or {}).get("resolve_network_count") or 0) + 1,
                "resolve_cache_hit_count": int((cached or {}).get("resolve_cache_hit_count") or 0),
            }
            self.log("ResolveUsernameRequest", "entity_resolve_expensive", source_id, canonical_url, "allowed", cache_hit=False, network_call=True, ok=True)
            return entity, {"telegram_resolve_status": "resolved_network", "resolved_title": str(getattr(entity, "title", None) or seed.source_title)}
        except Exception as exc:
            self.total_error += 1
            seconds = int(getattr(exc, "seconds", 0) or 0)
            if type(exc).__name__ == "FloodWaitError" or seconds:
                self.resolve_network_floodwait += 1
                cooldown_until = self.mark_floodwait("ResolveUsernameRequest", source_id, canonical_url, seconds)
                return None, {"fetch_status": "error_floodwait_resolve", "telegram_resolve_status": "error_floodwait", "fetch_error_code": "FloodWaitError", "fetch_error_message": str(exc)[:180], "next_allowed_resolve_at": cooldown_until}
            self.log("ResolveUsernameRequest", "entity_resolve_expensive", source_id, canonical_url, "error", ok=False, error_code=type(exc).__name__, error_message=str(exc)[:180])
            return None, {"fetch_status": "error_telegram_rpc", "telegram_resolve_status": "error", "fetch_error_code": type(exc).__name__, "fetch_error_message": str(exc)[:180]}

    def write_ledger(self) -> None:
        try:
            path = self.output_dir.parent.parent / "logs" / "telegram-request-ledger.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as f:
                for rec in self.ledger:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def observability_row(self, started_at: str, finished_at: str) -> dict[str, Any]:
        return {
            "run_id": self.run_id, "started_at": started_at, "finished_at": finished_at,
            "telegram_phase_status": self.telegram_phase_status,
            "telethon_version": _REGION_TALK_TELEGRAM_RUNTIME.get("telethon_version", ""),
            "session_cache_loaded": "true",
            "entity_cache_loaded": str(bool(self.state.get("telegram_entity_cache"))).lower(),
            "entity_cache_loaded_from_path": self.entity_cache_loaded_from_path,
            "entity_cache_write_path": str((self.output_dir.parent.parent / "state" / "region-talk-state.json")),
            "entity_cache_entries": len(self.entity_cache),
            "entity_cache_hit_rate": round(self.resolve_cache_hits / max(1, self.resolve_cache_hits + self.resolve_network_attempts), 3),
            "resolved_sources_available_for_history_fetch": sum(1 for v in self.entity_cache.values() if isinstance(v, dict) and v.get("channel_id_private") and v.get("access_hash_private")),
            "resolved_sources_used_without_network_resolve": self.resolve_cache_hits,
            "telegram_total_requests_attempted": self.total_attempted,
            "telegram_total_requests_ok": self.total_ok,
            "telegram_total_requests_error": self.total_error,
            "telegram_requests_by_method_json": json.dumps(self.requests_by_method, ensure_ascii=False),
            "resolve_cache_hits": self.resolve_cache_hits,
            "resolve_network_attempts": self.resolve_network_attempts,
            "resolve_network_ok": self.resolve_network_ok,
            "resolve_network_floodwait": self.resolve_network_floodwait,
            "resolve_skipped_by_cache": self.resolve_cache_hits,
            "resolve_skipped_by_cooldown": self.resolve_skipped_by_cooldown,
            "max_floodwait_seconds": self.max_floodwait_seconds,
            "floodwait_method": self.floodwait_method,
            "floodwait_cooldown_until": self.floodwait_cooldown_until,
            "recommendation_calls_attempted": self.recommendation_calls_attempted,
            "recommendation_calls_ok": self.recommendation_calls_ok,
            "recommendation_channels_returned": self.recommendation_channels_returned,
            "recommendation_channels_added_to_frontier": self.recommendation_channels_added_to_frontier,
            "history_sources_attempted": self.history_sources_attempted,
            "history_sources_ok": self.history_sources_ok,
            "history_sources_target": self.max_history_sources,
            "history_posts_fetched": self.history_posts_fetched,
            "media_downloads_attempted": self.media_downloads_attempted,
            "media_downloads_ok": self.media_downloads_ok,
            "telegram_governor_decisions_json": json.dumps({"max_network_resolves": self.max_network_resolves, "max_history_sources": self.max_history_sources, "max_recommendation_calls": self.max_recommendation_calls}, ensure_ascii=False),
        }


def normalize_geo_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").lower().replace("ё", "е")).strip()


def text_main_content_for_geo(text: str) -> str:
    kept: list[str] = []
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        low = normalize_geo_text(line)
        if not line:
            continue
        if URL_RE.search(line) and len(line) < 140:
            continue
        if low.startswith(("подпис", "источник:", "фото:", "подробнее", "читать", "наш сайт", "реклама")):
            continue
        if line.startswith("#") or ("#" in line and len(line) < 80):
            continue
        kept.append(line)
    return "\n".join(kept) or (text or "")


def split_semicolon(value: str) -> list[str]:
    return [x.strip() for x in re.split(r"\s*;\s*", value or "") if x.strip()]


def find_place_lexicon_file(config: dict[str, Any] | None = None) -> Path | None:
    raw_candidates = [
        os.getenv("REGION_TALK_PLACE_LEXICON_FILE"),
        (config or {}).get("place_lexicon_file") if config else None,
        "kaliningrad-place-lexicon-v1.csv",
        "docs/features/region-talk-channel/kaliningrad-place-lexicon-v1.csv",
    ]
    candidates = [Path(str(x)) for x in raw_candidates if x]
    for root in [Path.cwd(), Path(__file__).resolve().parent, Path("/kaggle/input")]:
        if root.exists():
            candidates.extend(root.rglob("kaliningrad-place-lexicon-v1.csv"))
    for path in candidates:
        if path.exists():
            return path
    return None


def load_place_lexicon(path: Path | None) -> list[dict[str, Any]]:
    if not path or not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    out: list[dict[str, Any]] = []
    for row in rows:
        clean = {field: str(row.get(field) or "").strip() for field in PLACE_LEXICON_FIELDS}
        terms: list[tuple[str, str]] = [(clean["canonical_name"], "canonical_name")]
        for col in ("aliases", "old_names", "latin_aliases", "common_misspellings"):
            for value in split_semicolon(clean.get(col, "")):
                terms.append((value, col))
        clean["match_terms"] = [(term, kind, normalize_geo_text(term)) for term, kind in terms if term]
        out.append(clean)
    return out


def term_in_text(norm_term: str, norm_text: str) -> bool:
    if not norm_term:
        return False
    escaped = re.escape(norm_term)
    return re.search(rf"(?<![0-9a-zа-я]){escaped}(?![0-9a-zа-я])", norm_text, flags=re.I) is not None


def match_kaliningrad_places(text: str, lexicon: list[dict[str, Any]]) -> list[dict[str, Any]]:
    norm = normalize_geo_text(text)
    matches: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    strong_context_terms = [normalize_geo_text(x) for x in DEFAULT_ANCHORS[:8]] + [
        "калининградская область", "калининградской области", "зеленоградский", "светлогорский",
        "балтийский район", "куршская коса", "балтийская коса", "пос.", "поселок", "поселке",
        "маршрут", "поездка", "путешествие",
    ]
    for place in lexicon:
        if str(place.get("allowed_for_kaliningrad_scope") or "").lower() != "true":
            continue
        for term, kind, norm_term in place.get("match_terms") or []:
            if len(norm_term) < 3:
                continue
            if term_in_text(norm_term, norm):
                key = (place.get("place_id") or place.get("canonical_name") or "", term)
                if key in seen:
                    continue
                seen.add(key)
                idx = norm.find(norm_term)
                raw_text = text or ""
                requires_context = str(place.get("requires_context") or "").lower() == "true"
                context_norm = normalize_geo_text(raw_text[max(0, idx-120):idx+len(term)+120]) if idx >= 0 else norm
                accepted_context = (not requires_context) or any(t and t in context_norm for t in strong_context_terms)
                matches.append({
                    "place_id": place.get("place_id", ""),
                    "matched_place_name": term,
                    "canonical_name": place.get("canonical_name", ""),
                    "place_type": place.get("place_type", ""),
                    "municipality": place.get("municipality", ""),
                    "priority_tier": place.get("priority_tier", ""),
                    "alias_used": term if kind != "canonical_name" else "",
                    "match_context": re.sub(r"\s+", " ", raw_text[max(0, idx-80):idx+len(term)+80])[:240] if idx >= 0 else "",
                    "requires_context": str(requires_context).lower(),
                    "accepted_as_region_evidence": str(bool(accepted_context)).lower(),
                    "match_context_short": re.sub(r"\s+", " ", raw_text[max(0, idx-80):idx+len(term)+80])[:180] if idx >= 0 else "",
                })
    return matches


def external_geo_mentions(text: str) -> tuple[list[str], list[str]]:
    norm = normalize_geo_text(text_main_content_for_geo(text))
    regions = [term for term in EXTERNAL_REGION_TERMS if term_in_text(normalize_geo_text(term), norm)]
    countries = [term for term in EXTERNAL_COUNTRY_TERMS if term_in_text(normalize_geo_text(term), norm)]
    return sorted(set(regions)), sorted(set(countries))


def kaliningrad_oblast_only_scope_gate(text: str, lexicon: list[dict[str, Any]]) -> dict[str, Any]:
    main_text = text_main_content_for_geo(text)
    matches = match_kaliningrad_places(main_text, lexicon)
    strong_matches = [m for m in matches if m.get("accepted_as_region_evidence") == "true"]
    ambiguous_matches = [m for m in matches if m.get("accepted_as_region_evidence") != "true"]
    external_regions, external_countries = external_geo_mentions(text)
    ok = bool(strong_matches) and not external_regions and not external_countries
    reason = ""
    if not matches:
        reason = "reject: no Kaliningrad Oblast place evidence in main content"
    elif external_regions or external_countries:
        reason = "reject: external destinations in main content: " + "; ".join(external_regions + external_countries)
    else:
        reason = "accepted: matched Kaliningrad Oblast places only: " + "; ".join(sorted({m['canonical_name'] for m in matches})[:10])
    return {
        "kaliningrad_oblast_only_scope": ok,
        "matched_place_names": "; ".join(sorted({m["canonical_name"] for m in matches})),
        "matched_place_types": "; ".join(sorted({m["place_type"] for m in matches if m.get("place_type")})),
        "matched_place_priority_tiers": "; ".join(sorted({m["priority_tier"] for m in matches if m.get("priority_tier")})),
        "matched_place_aliases": "; ".join(sorted({m["alias_used"] for m in matches if m.get("alias_used")})),
        "ambiguous_place_names": "; ".join(sorted({m["canonical_name"] for m in ambiguous_matches})),
        "requires_context_place_names": "; ".join(sorted({m["canonical_name"] for m in ambiguous_matches})),
        "matched_place_requires_context": "; ".join(sorted({m["canonical_name"] for m in matches if m.get("requires_context") == "true"})),
        "matched_place_accepted_as_region_evidence": "; ".join(sorted({m["canonical_name"] for m in strong_matches})),
        "matched_place_rejected_as_ambiguous": "; ".join(sorted({m["canonical_name"] for m in ambiguous_matches})),
        "match_context_short": "; ".join([str(m.get("match_context_short") or "") for m in matches[:3] if m.get("match_context_short")]),
        "mentioned_kaliningrad_places": "; ".join(sorted({m["canonical_name"] for m in matches})),
        "mentioned_external_regions": "; ".join(external_regions),
        "mentioned_external_countries": "; ".join(external_countries),
        "external_region_count": len(external_regions),
        "external_country_count": len(external_countries),
        "external_geo_mentions": "; ".join(external_regions + external_countries),
        "region_scope_decision": "accept_kaliningrad_oblast_only" if ok else "reject_not_kaliningrad_oblast_only",
        "region_scope_reason": reason,
        "place_matches": matches,
    }


def parse_post_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def freshness_gate(post_date: Any) -> dict[str, Any]:
    min_raw = os.getenv("REGION_TALK_MIN_POST_DATE", "2026-01-01")
    min_dt = datetime.fromisoformat(min_raw).replace(tzinfo=timezone.utc)
    now = RUN_STARTED_AT
    dt = parse_post_datetime(post_date)
    if dt is None:
        return {"fresh_enough": False, "post_age_days": "", "freshness_score": 0.0, "freshness_reason": "reject: missing post date", "min_post_date": min_raw}
    age_days = max(0, (now - dt).days)
    half_life = max(1, getenv_int("REGION_TALK_FRESHNESS_HALF_LIFE_DAYS", 30))
    score = round(1 / (1 + age_days / half_life), 3)
    ok = dt >= min_dt and dt.year >= min_dt.year
    return {"fresh_enough": ok, "post_age_days": age_days, "freshness_score": score if ok else 0.0, "freshness_reason": "accepted" if ok else f"reject: post_date before {min_raw}", "min_post_date": min_raw}


def score_substance(text: str) -> dict[str, Any]:
    low = normalize_geo_text(text)
    length_score = min(0.35, len(text or "") / 1200)
    useful = min(1.0, 0.18 * sum(1 for w in SUBSTANCE_WORDS if w in low))
    visit = min(1.0, 0.22 * sum(1 for w in VISIT_WORDS if w in low))
    emotion = min(1.0, 0.16 * sum(1 for w in EMOTION_WORDS if w in low))
    memorable = min(1.0, 0.22 * sum(1 for w in MEMORABLE_WORDS if w in low))
    substance = round(min(1.0, length_score + 0.35*useful + 0.30*visit + 0.20*emotion + 0.25*memorable), 3)
    return {
        "text_substance_score": substance,
        "visit_impression_score": round(visit, 3),
        "useful_route_score": round(useful, 3),
        "emotion_observation_score": round(emotion, 3),
        "memorable_details_score": round(memorable, 3),
        "substance_reason": "accepted substantive/experience text" if substance >= 0.25 else "reject: thin text, visual dump, SEO list, or low-context mention",
    }


def ad_promo_gate(text: str) -> dict[str, Any]:
    low = normalize_geo_text(text)
    hard_hits: list[str] = []
    possible_hits: list[str] = []
    for label, pattern in AD_PROMO_HARD_PATTERNS:
        if re.search(pattern, low, flags=re.I):
            hard_hits.append(label)
    for label, pattern in AD_PROMO_POSSIBLE_PATTERNS:
        if re.search(pattern, low, flags=re.I):
            possible_hits.append(label)
    is_hard = bool(hard_hits)
    is_possible = bool(possible_hits)
    reason = (
        "reject: hard ad/promo/announcement cues: " + "; ".join(sorted(set(hard_hits)))
        if is_hard else (
            "possible promo cues, send to LLM if content is strong: " + "; ".join(sorted(set(possible_hits)))
            if is_possible else "accepted: no hard ad/promo cues"
        )
    )
    return {
        "is_ad_or_promo": is_hard,
        "is_ad_or_promo_hard": str(is_hard).lower(),
        "is_ad_or_promo_possible": str(is_possible).lower(),
        "ad_promo_hits": "; ".join(sorted(set(hard_hits))),
        "ad_promo_possible_hits": "; ".join(sorted(set(possible_hits))),
        "ad_promo_reason": reason,
    }


def extract_urls_and_handles(text: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for m in URL_RE.finditer(text or ""):
        raw = m.group(0).rstrip(').,;!?:»”')
        norm = raw if raw.startswith(("http://", "https://")) else "https://" + raw.lstrip("/")
        if norm not in seen:
            seen.add(norm); out.append({"raw_url": raw, "normalized_url": norm, "extracted_handle": ""})
    for m in HANDLE_RE.finditer(text or ""):
        handle = m.group(0)
        norm = "https://t.me/" + handle.lstrip("@")
        if norm not in seen:
            seen.add(norm); out.append({"raw_url": handle, "normalized_url": norm, "extracted_handle": handle})
    return out


def classify_platform_url(url: str) -> tuple[str, str]:
    low = (url or "").lower()
    if "t.me/" in low:
        return ("telegram_post" if re.search(r"t\.me/[a-z0-9_]+/\d+", low) else "telegram_channel", "telegram")
    if "vk.com/wall" in low:
        return "vk_wall_post", "vk"
    if "vk.com/" in low:
        return "vk_public", "vk"
    if "dzen.ru" in low:
        return "dzen_article", "dzen"
    if "youtube.com" in low or "youtu.be" in low:
        return "youtube_channel", "youtube"
    if "rutube.ru" in low:
        return "rutube_channel", "rutube"
    return "website", "web"


def discover_links_for_post(post: dict[str, Any], run_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    from_source = str(post.get("source_id") or "")
    source_title = str(post.get("source_title") or "")
    post_id = str(post.get("post_id") or "")
    post_url = str(post.get("post_url") or "")
    items = extract_urls_and_handles(str(post.get("text") or ""))
    if post.get("forwarded_from_url"):
        items.append({"raw_url": str(post.get("forwarded_from_url")), "normalized_url": str(post.get("forwarded_from_url")), "extracted_handle": str(post.get("forwarded_from_handle") or "")})
    for i, item in enumerate(items, start=1):
        normalized = item["normalized_url"]
        link_type, platform = classify_platform_url(normalized)
        edge_type = "forward_origin" if normalized == post.get("forwarded_from_url") else "post_text_link"
        cand_id = "src_cand_" + stable_hash(platform, normalized)
        edge_id = "edge_" + stable_hash(from_source, post_id, normalized, edge_type)
        rows.append({
            "source_candidate_id": cand_id, "discovered_from_source": source_title, "discovered_from_post_url": post_url,
            "discovered_from_comment_hash": "", "discovery_type": "forwarded_from" if edge_type == "forward_origin" else "post_text",
            "edge_type": edge_type, "raw_url": item["raw_url"], "normalized_url": normalized, "platform_guess": link_type,
            "candidate_source_status": "source_frontier", "confidence": 0.85 if edge_type == "forward_origin" else 0.55,
        })
        edges.append({
            "edge_id": edge_id, "from_source_id": from_source, "from_source_title": source_title, "from_post_id": post_id,
            "to_source_candidate_id": cand_id, "to_source_title": item.get("extracted_handle") or normalized, "edge_type": edge_type,
            "evidence_url": normalized, "evidence_context_short": make_summary(str(post.get("text_excerpt") or post.get("text") or ""))[:240],
            "confidence": 0.85 if edge_type == "forward_origin" else 0.55, "discovery_depth": 1, "run_id": run_id,
        })
    return rows, edges


def source_candidate_score(row: dict[str, Any]) -> float:
    title = normalize_geo_text(str(row.get("recommended_title") or row.get("title_guess") or row.get("to_source_title") or row.get("normalized_url") or ""))
    positive = ["путешеств", "travel", "места", "росси", "маршрут", "город", "прогул", "балтик", "калининград", "блог"]
    negative = ["скидк", "авиабил", "турфирм", "турагент", "казино", "crypto", "крипт", "ваканси", "работа", "новости", "полит"]
    score = float(row.get("confidence") or 0.45)
    score += min(0.20, 0.04 * sum(1 for w in positive if w in title))
    score -= min(0.35, 0.08 * sum(1 for w in negative if w in title))
    if row.get("edge_type") == "telegram_similar_channel":
        score += 0.12
    if row.get("platform_guess") in {"telegram", "telegram_channel"}:
        score += 0.05
    return round(max(0.0, min(0.95, score)), 3)


def build_source_frontier_unique(rows: list[dict[str, Any]], previous_discovered: dict[str, Any], run_id: str) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    prev_by_key: dict[str, dict[str, Any]] = {}
    for pv in previous_discovered.values() if isinstance(previous_discovered, dict) else []:
        if isinstance(pv, dict):
            pkey = str(pv.get("canonical_source_key") or canonical_source_key(str(pv.get("platform") or pv.get("platform_guess") or ""), str(pv.get("username_or_handle") or ""), str(pv.get("canonical_url") or pv.get("normalized_url") or "")))
            if pkey:
                prev_by_key[pkey] = pv
    for row in rows:
        platform = normalize_source_platform(str(row.get("platform") or row.get("platform_guess") or ""), str(row.get("canonical_url") or row.get("normalized_url") or row.get("raw_url") or ""))
        canonical_url = canonical_source_url(platform, str(row.get("username_or_handle") or row.get("recommended_username") or ""), str(row.get("canonical_url") or row.get("normalized_url") or ""))
        ckey = str(row.get("canonical_source_key") or canonical_source_key(platform, str(row.get("username_or_handle") or row.get("recommended_username") or ""), canonical_url))
        cid = str(row.get("source_candidate_id") or "src_cand_" + stable_hash(ckey or platform, canonical_url))
        prev = previous_discovered.get(cid) or prev_by_key.get(ckey) or {}
        group_key = ckey or cid
        g = grouped.setdefault(group_key, {
            "frontier_source_id": cid,
            "source_candidate_id": cid,
            "platform": platform or prev.get("platform_guess", ""),
            "platform_guess": platform or prev.get("platform_guess", ""),
            "canonical_source_key": ckey,
            "canonical_url": canonical_url or prev.get("normalized_url", ""),
            "normalized_url": canonical_url or prev.get("normalized_url", ""),
            "username_or_handle": row.get("recommended_username") or row.get("username_or_handle") or "",
            "title_guess": row.get("recommended_title") or row.get("title_guess") or row.get("to_source_title") or "",
            "discovery_first_run_id": prev.get("first_seen_run_id") or run_id,
            "discovery_last_run_id": run_id,
            "discovery_count": int(prev.get("seen_run_count") or 0),
            "edge_types_all": set(),
            "discovery_types": set(),
            "seed_sources": set(),
            "best_confidence": 0.0,
            "frontier_priority": 0.0,
            "frontier_status": row.get("frontier_status") or row.get("candidate_source_status") or "source_frontier",
            "resolve_status": row.get("resolve_status") or "",
            "resolve_attempt_count": row.get("resolve_attempt_count") or "",
            "last_resolve_error_code": row.get("last_resolve_error_code") or "",
            "last_resolve_error_message_short": row.get("last_resolve_error_message_short") or "",
            "next_allowed_resolve_at": row.get("next_allowed_resolve_at") or "",
            "probe_status": row.get("probe_status") or "probe_later",
            "monitor_decision": row.get("monitor_decision") or "",
            "monitor_decision_reason": row.get("monitor_decision_reason") or "",
            "private_state_key": row.get("private_state_key") or "",
        })
        g["discovery_count"] = int(g.get("discovery_count") or 0) + 1
        if row.get("edge_type"): g["edge_types_all"].add(str(row.get("edge_type")))
        if row.get("discovery_type"): g["discovery_types"].add(str(row.get("discovery_type")))
        if row.get("discovered_from_source"): g["seed_sources"].add(str(row.get("discovered_from_source")))
        confidence = float(row.get("confidence") or 0)
        if confidence >= float(g.get("best_confidence") or 0):
            g["best_confidence"] = confidence
            g["best_edge_type"] = row.get("edge_type") or ""
            g["best_discovered_from_source"] = row.get("discovered_from_source") or row.get("recommendation_source_channel_title") or ""
            g["best_discovered_from_post_url"] = row.get("discovered_from_post_url") or ""
            g["telegram_similar_seed_channel"] = row.get("recommendation_source_channel_url") or ""
            g["telegram_similar_rank"] = row.get("recommendation_rank") or ""
        g["frontier_priority"] = max(float(g.get("frontier_priority") or 0), source_candidate_score(row))
    out: list[dict[str, Any]] = []
    for g in grouped.values():
        edge_types = sorted(g.pop("edge_types_all") or [])
        discovery_types = sorted(g.pop("discovery_types") or [])
        seed_sources = sorted(g.pop("seed_sources") or [])
        priority = float(g.get("frontier_priority") or 0)
        platform = str(g.get("platform") or g.get("platform_guess") or "").lower()
        resolve_status = str(g.get("resolve_status") or "")
        prev_status = str(g.get("frontier_status") or "")
        if platform == "vk":
            stage = "vk_not_configured"
            status = "vk_wall_setup_required"
        elif platform and not platform.startswith("telegram"):
            stage = "unsupported"
            status = "unsupported_probe_backlog"
        elif resolve_status in {"resolved_network", "resolved_from_private_cache"} or prev_status in {"history_due", "resolved", "source_frontier"} and priority >= 0.55:
            stage = "history_due"
            status = "history_due"
        elif priority >= 0.75:
            stage = "probe_due"
            status = "probe_due"
        elif priority >= 0.35:
            stage = "unresolved"
            status = "unresolved_frontier"
        else:
            stage = "inactive_low_quality"
            status = "inactive_low_quality"
        g.update({
            "edge_types_all": "; ".join(edge_types),
            "discovery_types": "; ".join(discovery_types),
            "seed_sources_evidence_count": len(seed_sources),
            "candidate_source_status": "source_frontier" if stage != "inactive_low_quality" else "inactive_low_quality",
            "frontier_stage": stage,
            "frontier_status": status,
            "source_candidate_score": round(priority, 3),
            "next_action": "probe_later" if stage in {"unresolved", "probe_due"} else ("fetch_history_when_selected" if stage == "history_due" else ("configure_vk_wall_reader" if stage == "vk_not_configured" else "keep_in_low_quality_pool")),
            "rejection_or_skip_reason": "" if priority >= 0.35 else "low source candidate score; retained as inactive low-quality pool, not a blocking status",
        })
        g.update(classify_source_profile({
            "source_title": g.get("title_guess"),
            "resolved_title": g.get("title_guess"),
            "source_kind": g.get("edge_types_all"),
            "source_type": g.get("discovery_types"),
            "canonical_url": g.get("canonical_url"),
        }, []))
        out.append(g)
    return sorted(out, key=lambda r: (-float(r.get("source_candidate_score") or 0), str(r.get("normalized_url") or "")))



CANDIDATE_MEMORY_STAGES = {
    "favorite", "semantic_candidate", "needs_image_review", "image_fetch_retry_needed",
    "good_text_weak_media", "low_substance_but_region_relevant", "manual_keep",
}
ACTIVE_CANDIDATE_MEMORY_STATUSES = {
    "text_candidate_pending_image", "image_fetch_retry_needed", "image_reviewable",
    "good_text_weak_media", "publication_ready_candidate", "manual_keep", "source_not_refetched_this_run",
}
VISIT_EVIDENCE_PATTERN = re.compile(r"\b(были|ездили|поехали|вернулись|посетили|гуляли|увидели|понравил|впечатлил|запомнил|ощущени|эмоци|отзыв|фотоотч[её]т)\b", re.I)
EMOTION_PATTERN = re.compile(r"\b(понравил|впечатлил|запомнил|атмосфер|красив|удивил|эмоци|ощущени|восторг|любов|спокойн|магия)\b", re.I)
ORIGINAL_PHOTO_PATTERN = re.compile(r"\b(мои фото|наши фото|фотоотч[её]т|снял[аи]?|снимали|кадры|фотографи[ия]|подписчик|читател)\b", re.I)


def infer_visit_semantic_fields(text: str, llm_gate: dict[str, Any], substance: dict[str, Any], post: dict[str, Any] | None = None) -> dict[str, Any]:
    low = (text or "").lower()
    llm_type = str(llm_gate.get("content_type") or llm_gate.get("llm_content_type") or "").strip()
    positive_markers = [
        "мы приехали", "я был", "я была", "мы были", "нам понравилось", "советую", "делюсь маршрутом",
        "нашли место", "зашли", "прогулялись", "поехали на выходные", "мой отзыв", "впечатлило",
        "запомнилось", "вот что посмотреть", "наш маршрут", "первым делом", "вернулись из калининграда",
    ]
    marker_hits = [m for m in positive_markers if m in low]
    has_firsthand = bool(marker_hits) or bool(VISIT_EVIDENCE_PATTERN.search(text or "")) or llm_type == "visit_impression_candidate" or float(substance.get("visit_impression_score") or 0) >= 0.25
    has_emotion = bool(EMOTION_PATTERN.search(text or "")) or float(substance.get("emotion_observation_score") or 0) >= 0.20
    has_original_photo = bool(ORIGINAL_PHOTO_PATTERN.search(text or "")) or bool((post or {}).get("is_forwarded_or_repost"))
    if has_firsthand:
        visit_type = "firsthand_author_visit"
    elif "подписчик" in low or "читател" in low or "фотоотчет" in low or "фотоотчёт" in low:
        visit_type = "subscriber_photo_report"
    elif llm_type == "route_useful_candidate" or float(substance.get("useful_route_score") or 0) >= 0.18:
        visit_type = "route_guide"
    elif llm_type in {"single_location_photo_card", "encyclopedic_card_candidate"} or (post or {}).get("has_media"):
        visit_type = "single_location_photo_card"
    elif llm_type == "news_or_event":
        visit_type = "news_or_event"
    elif "рго" in str((post or {}).get("source_title") or "").lower() or "проект" in low:
        visit_type = "official_project_material"
    else:
        visit_type = "unknown"
    text_bucket = "publication_candidate_text" if has_firsthand and has_emotion else ("research_candidate" if visit_type in {"route_guide", "single_location_photo_card", "subscriber_photo_report"} else "low_priority_research")
    return {
        "has_firsthand_visit_evidence": str(has_firsthand).lower(),
        "visit_evidence_type": visit_type,
        "first_person_markers": "; ".join(marker_hits),
        "emotion_or_impression_evidence": str(has_emotion).lower(),
        "review_or_opinion_evidence": str(has_firsthand or has_emotion).lower(),
        "useful_route_evidence": str(float(substance.get("useful_route_score") or 0) >= 0.18 or "маршрут" in low or "что посмотреть" in low).lower(),
        "memorable_detail_evidence": str(float(substance.get("memorable_details_score") or 0) >= 0.20 or "запом" in low).lower(),
        "original_photo_evidence": str(has_original_photo or bool((post or {}).get("primary_media_path"))).lower(),
        "nonlocal_blogger_visit_score": round((0.45 if has_firsthand else 0.0) + (0.20 if has_emotion else 0.0) + (0.15 if marker_hits else 0.0) + (0.10 if (post or {}).get("has_media") else 0.0), 3),
        "publication_story_score": round((0.35 if has_firsthand else 0.0) + (0.25 if has_emotion else 0.0) + (0.20 if float(substance.get("useful_route_score") or 0) >= 0.18 else 0.0) + (0.10 if has_original_photo else 0.0), 3),
        "text_bucket": text_bucket,
    }


def classify_source_profile(srow: dict[str, Any], sampled: list[dict[str, Any]]) -> dict[str, Any]:
    title = str(srow.get("source_title") or srow.get("resolved_title") or "").lower()
    kind = str(srow.get("source_kind") or srow.get("source_type") or "").lower()
    joined = " ".join([title, kind, str(srow.get("canonical_url") or "").lower()])
    n = max(1, len(sampled))
    ko_hits = sum(1 for r in sampled if r.get("kaliningrad_oblast_only_scope"))
    ad_hits = sum(1 for r in sampled if r.get("is_ad_or_promo"))
    news_hits = sum(1 for r in sampled if float(r.get("newsiness_score") or 0) >= 0.45)
    personal_hits = sum(1 for r in sampled if str(r.get("has_firsthand_visit_evidence") or "").lower() == "true" or str(r.get("first_person_markers") or ""))
    ko_ratio = round(ko_hits / n, 3)
    if any(w in joined for w in ["калининград", "кёнигсберг", "kenig", "kgd", "39"]):
        geo = "kaliningrad_local"
    elif sampled and 0 < ko_ratio < 0.7:
        geo = "nonlocal_russia"
    elif ko_ratio >= 0.7:
        geo = "kaliningrad_local"
    else:
        geo = "unknown"
    if any(w in joined for w in ["новост", "сми", "инфо", "афиша"]):
        topic = "local_news" if geo == "kaliningrad_local" else "federal_media"
    elif any(w in joined for w in ["тур", "экскурс", "бронь"]):
        topic = "ads_tours"
    elif any(w in joined for w in ["travel", "trip", "путеше", "маршрут", "around", "vokrug"]):
        topic = "travel_blogger" if personal_hits else "travel_media"
    elif personal_hits:
        topic = "personal_blog"
    elif news_hits / n > 0.5:
        topic = "local_news" if geo == "kaliningrad_local" else "federal_media"
    else:
        topic = "unknown"
    travel_score = round(min(1.0, (0.25 if "travel" in joined or "путеше" in joined else 0.0) + (personal_hits / n) * 0.35 + max(0, 1 - news_hits / n) * 0.20 + max(0, 1 - ad_hits / n) * 0.20), 3)
    personal_score = round(min(1.0, personal_hits / n + (0.2 if topic == "personal_blog" else 0.0)), 3)
    nonlocal_value = round(min(1.0, (0.45 if geo == "nonlocal_russia" else 0.10 if geo == "unknown" else 0.0) + travel_score * 0.35 + (0.20 if 0 < ko_ratio < 0.5 else 0.0)), 3)
    return {
        "source_geo_class": geo,
        "source_topic_class": topic,
        "ko_mention_ratio_recent": ko_ratio,
        "travel_blogger_score": travel_score,
        "personal_voice_score": personal_score,
        "nonlocal_value_score": nonlocal_value,
        "source_priority_reason": f"geo={geo}; topic={topic}; ko_ratio={ko_ratio}; personal_hits={personal_hits}/{len(sampled)}",
    }


def normalize_image_status(stage: str, ms: dict[str, Any]) -> dict[str, Any]:
    input_type = str(ms.get("image_model_input_type") or "")
    has_media_fallback = input_type != "actual_image" and str(ms.get("image_download_status") or "") in {"actual_image_missing_or_fallback", "download_failed_or_missing"}
    if has_media_fallback:
        return {
            "current_stage": "image_fetch_retry_needed",
            "image_status": "needs_actual_image_fetch",
            "image_bucket": "text_accepted_metadata_only",
            "visual_decision": "pending",
            "next_action": "retry_media_download_or_manual_open",
            "why_not_publication_ready": "metadata-only image fallback; actual image bytes were not scored",
            "is_selected_for_publication": False,
            "image_publication_ready": "false",
            "image_reviewable": "false",
            "image_quality_bucket": "metadata_pending_actual_image",
            "failure_reason": "needs_actual_image_fetch",
        }
    if input_type != "actual_image" and str(ms.get("image_publication_ready")) == "true":
        return {
            "current_stage": "image_fetch_retry_needed",
            "image_status": "needs_actual_image_fetch",
            "image_bucket": "text_accepted_metadata_only",
            "visual_decision": "pending",
            "next_action": "retry_media_download_or_manual_open",
            "why_not_publication_ready": "non-actual image scoring cannot mark a row publication-ready",
            "is_selected_for_publication": False,
            "image_publication_ready": "false",
            "image_reviewable": "false",
            "image_quality_bucket": "metadata_pending_actual_image",
            "failure_reason": "needs_actual_image_fetch",
        }
    if str(ms.get("image_publication_ready")) == "true":
        return {"image_status": "publication_ready", "image_bucket": "text_accepted_image_publication_ready", "visual_decision": "accept", "next_action": "human_final_check", "why_not_publication_ready": ""}
    if str(ms.get("image_reviewable")) == "true":
        return {"image_status": "image_reviewable", "image_bucket": "text_accepted_image_reviewable", "visual_decision": "review", "next_action": "human_image_review", "why_not_publication_ready": "image reviewable but below publication-ready threshold"}
    if input_type == "actual_image":
        return {"image_status": "actual_image_weak", "image_bucket": "text_accepted_image_weak", "visual_decision": "reject_visual", "next_action": "manual_open_if_text_is_strong", "why_not_publication_ready": str(ms.get("failure_reason") or "actual image below threshold")}
    return {"image_status": "text_rejected_no_image_spend" if stage in {"dropped_text_gate", "debug_reject"} else "not_scored", "image_bucket": "text_rejected_no_image_spend", "visual_decision": "not_run", "next_action": "", "why_not_publication_ready": str(ms.get("failure_reason") or "")}


def candidate_lifecycle_status(row: dict[str, Any]) -> str:
    manual = str(row.get("manual_decision") or "").strip().lower()
    if manual == "reject":
        return "manual_reject"
    if manual in {"keep", "manual_keep", "favorite", "approve_for_preview", "approve_for_queue"}:
        return "manual_keep"
    if str(row.get("image_publication_ready")) == "true" or row.get("current_stage") in {"favorite", "semantic_candidate"}:
        return "publication_ready_candidate" if str(row.get("image_publication_ready")) == "true" else "image_reviewable"
    if row.get("current_stage") == "image_fetch_retry_needed" or row.get("image_status") == "needs_actual_image_fetch":
        return "image_fetch_retry_needed"
    if row.get("current_stage") == "needs_image_review" or str(row.get("image_reviewable")) == "true":
        return "image_reviewable"
    if row.get("current_stage") == "good_text_weak_media":
        return "good_text_weak_media"
    if row.get("llm_decision") == "accept" or row.get("text_bucket") in {"research_candidate", "publication_candidate_text"}:
        return "text_candidate_pending_image"
    if row.get("current_stage") == "source_not_refetched_this_run":
        return "source_not_refetched_this_run"
    return "research_candidate"


def is_candidate_memory_row(row: dict[str, Any]) -> bool:
    if not row.get("kaliningrad_oblast_only_scope"):
        return row.get("manual_decision") in {"manual_keep", "keep", "favorite"} and str(row.get("manual_region_override") or "").lower() == "true"
    if str(row.get("kaliningrad_mention_role") or "main_subject") not in {"", "main_subject"}:
        return False
    return row.get("current_stage") in CANDIDATE_MEMORY_STAGES or row.get("llm_decision") == "accept" or row.get("manual_decision") in {"manual_keep", "keep", "favorite"}


def build_candidate_memory(previous_state: dict[str, Any], current_rows: list[dict[str, Any]], source_rows: list[dict[str, Any]], run_id: str, run_now: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    previous_memory = previous_state.get("candidate_memory") if isinstance(previous_state.get("candidate_memory"), dict) else {}
    previous_posts = previous_state.get("posts") if isinstance(previous_state.get("posts"), dict) else {}
    memory: dict[str, dict[str, Any]] = {str(k): dict(v) for k, v in previous_memory.items() if isinstance(v, dict)}
    # Bootstrap legacy candidate memory from pre-z4 post state.
    for pid, prev in previous_posts.items():
        if not isinstance(prev, dict):
            continue
        if str(prev.get("current_stage") or "") in CANDIDATE_MEMORY_STAGES:
            mid = "cmem_" + stable_hash(str(pid))
            memory.setdefault(mid, {
                "candidate_memory_id": mid, "post_id": pid, "post_url": prev.get("post_url", ""),
                "platform_post_key": prev.get("platform_post_key", ""), "source_id": prev.get("source_id", ""),
                "source_title": prev.get("source_title", ""), "post_date": prev.get("post_date", ""),
                "first_seen_run_id": prev.get("first_seen_run_id", ""), "first_candidate_run_id": prev.get("first_seen_run_id", ""),
                "last_seen_run_id": prev.get("last_seen_run_id", ""), "last_refetched_run_id": prev.get("last_seen_run_id", ""),
                "seen_run_count": prev.get("seen_run_count", 1), "current_lifecycle_status": candidate_lifecycle_status(prev),
                "current_stage": prev.get("current_stage", ""), "best_stage_ever": prev.get("current_stage", ""),
                "best_candidate_score_ever": prev.get("candidate_score_current", ""), "candidate_score_current": prev.get("candidate_score_current", ""),
                "best_media_score_ever": prev.get("media_score_current", ""), "media_score_current": prev.get("media_score_current", ""),
                "manual_decision": prev.get("manual_decision", ""), "next_action": "keep_in_memory_or_refetch_source_later",
                "why_keep_in_memory": "legacy candidate from previous post state", "why_not_publication_ready": "not refetched in this run yet",
            })
    current_by_id = {str(r.get("post_id")): r for r in current_rows if r.get("post_id")}
    for row in current_rows:
        if not is_candidate_memory_row(row):
            continue
        pid = str(row.get("post_id") or stable_hash(row.get("post_url")))
        mid = "cmem_" + stable_hash(pid)
        prev = memory.get(mid) or {}
        status = candidate_lifecycle_status(row)
        prev_score = prev.get("candidate_score_current", "")
        prev_media = prev.get("media_score_current", "")
        best_score = max([float(x) for x in [prev.get("best_candidate_score_ever"), row.get("candidate_score")] if str(x) not in {"", "None"}], default=float(row.get("candidate_score") or 0))
        best_media = max([float(x) for x in [prev.get("best_media_score_ever"), row.get("overall_media_score")] if str(x) not in {"", "None"}], default=float(row.get("overall_media_score") or 0))
        try:
            score_delta = round(float(row.get("candidate_score") or 0) - float(prev_score), 3) if prev_score != "" else ""
        except Exception:
            score_delta = ""
        try:
            media_delta = round(float(row.get("overall_media_score") or 0) - float(prev_media), 3) if prev_media != "" else ""
        except Exception:
            media_delta = ""
        memory[mid] = {
            **prev,
            "candidate_memory_id": mid, "post_id": pid, "post_url": row.get("post_url", prev.get("post_url", "")),
            "platform_post_key": row.get("platform_post_key", prev.get("platform_post_key", "")), "source_id": row.get("source_id", prev.get("source_id", "")),
            "source_title": row.get("source_title", prev.get("source_title", "")), "post_date": row.get("post_date", prev.get("post_date", "")),
            "first_seen_run_id": prev.get("first_seen_run_id") or row.get("first_seen_run_id") or run_id,
            "first_candidate_run_id": prev.get("first_candidate_run_id") or run_id,
            "last_seen_run_id": run_id, "last_refetched_run_id": run_id, "not_refetched_this_run": "false",
            "seen_run_count": int(prev.get("seen_run_count") or 0) + (0 if prev.get("last_refetched_run_id") == run_id else 1),
            "previous_lifecycle_status": prev.get("current_lifecycle_status", ""), "current_lifecycle_status": status,
            "best_lifecycle_status_ever": prev.get("best_lifecycle_status_ever") or status,
            "previous_stage": prev.get("current_stage", ""), "current_stage": row.get("current_stage", ""),
            "best_stage_ever": prev.get("best_stage_ever") or row.get("current_stage", ""),
            "best_candidate_score_ever": round(best_score, 3), "candidate_score_current": row.get("candidate_score", ""), "candidate_score_delta": score_delta,
            "best_media_score_ever": round(best_media, 3), "media_score_current": row.get("overall_media_score", ""), "media_score_delta": media_delta,
            "short_summary": row.get("short_summary", prev.get("short_summary", "")),
            "kaliningrad_oblast_only_scope": row.get("kaliningrad_oblast_only_scope", prev.get("kaliningrad_oblast_only_scope", "")),
            "matched_place_names": row.get("matched_place_names", prev.get("matched_place_names", "")),
            "matched_place_accepted_as_region_evidence": row.get("matched_place_accepted_as_region_evidence", prev.get("matched_place_accepted_as_region_evidence", "")),
            "kaliningrad_mention_role": row.get("kaliningrad_mention_role", prev.get("kaliningrad_mention_role", "")),
            "region_scope_reason": row.get("region_scope_reason", prev.get("region_scope_reason", "")),
            "content_type": row.get("content_type", prev.get("content_type", "")),
            "llm_content_type": row.get("llm_content_type", prev.get("llm_content_type", "")),
            "vector_content_type": row.get("vector_content_type", prev.get("vector_content_type", "")),
            "source_geo_class": row.get("source_geo_class", prev.get("source_geo_class", "")),
            "source_topic_class": row.get("source_topic_class", prev.get("source_topic_class", "")),
            "ko_mention_ratio_recent": row.get("ko_mention_ratio_recent", prev.get("ko_mention_ratio_recent", "")),
            "travel_blogger_score": row.get("travel_blogger_score", prev.get("travel_blogger_score", "")),
            "personal_voice_score": row.get("personal_voice_score", prev.get("personal_voice_score", "")),
            "nonlocal_value_score": row.get("nonlocal_value_score", prev.get("nonlocal_value_score", "")),
            "source_priority_reason": row.get("source_priority_reason", prev.get("source_priority_reason", "")),
            "text_bucket": row.get("text_bucket", ""), "image_bucket": row.get("image_bucket", ""), "image_status": row.get("image_status", ""),
            "image_model_input_type": row.get("image_model_input_type", ""), "image_model_type": row.get("image_model_type", ""),
            "image_publication_ready": row.get("image_publication_ready", "false"), "image_reviewable": row.get("image_reviewable", "false"),
            "visual_decision": row.get("visual_decision", ""),
            "llm_status": row.get("llm_status", ""), "llm_stage": row.get("llm_stage", ""), "needs_llm_final_verify": row.get("needs_llm_final_verify", ""),
            "vector_gate_status": row.get("vector_gate_status", ""), "vector_positive_score": row.get("vector_positive_score", ""),
            "has_firsthand_visit_evidence": row.get("has_firsthand_visit_evidence", ""), "visit_evidence_type": row.get("visit_evidence_type", ""),
            "first_person_markers": row.get("first_person_markers", prev.get("first_person_markers", "")),
            "emotion_or_impression_evidence": row.get("emotion_or_impression_evidence", ""), "review_or_opinion_evidence": row.get("review_or_opinion_evidence", ""),
            "useful_route_evidence": row.get("useful_route_evidence", prev.get("useful_route_evidence", "")),
            "nonlocal_blogger_visit_score": row.get("nonlocal_blogger_visit_score", prev.get("nonlocal_blogger_visit_score", "")),
            "publication_story_score": row.get("publication_story_score", prev.get("publication_story_score", "")),
            "original_photo_evidence": row.get("original_photo_evidence", ""), "manual_decision": row.get("manual_decision") or prev.get("manual_decision", ""),
            "manual_decision_at": prev.get("manual_decision_at", ""), "next_action": row.get("next_action") or row.get("suggested_action") or "manual_review",
            "why_keep_in_memory": "LLM/text/image gate reached research candidate status", "why_not_publication_ready": row.get("why_not_publication_ready") or row.get("rejection_reason") or "",
            "expires_at": prev.get("expires_at", ""),
        }
    source_status = {str(s.get("source_id") or ""): s for s in source_rows}
    for mid, rec in list(memory.items()):
        if rec.get("post_id") in current_by_id:
            continue
        sid = str(rec.get("source_id") or "")
        srow = source_status.get(sid) or {}
        rec["not_refetched_this_run"] = "true"
        rec["current_lifecycle_status"] = "source_not_refetched_this_run" if rec.get("current_lifecycle_status") not in {"manual_reject", "expired"} else rec.get("current_lifecycle_status")
        rec["visibility_status"] = "not_refetched_this_run"
        rec["source_fetch_status_this_run"] = srow.get("fetch_status", "source_not_selected_this_run")
        rec["next_action"] = "keep_in_memory_or_refetch_source_later"
        rec["why_keep_in_memory"] = rec.get("why_keep_in_memory") or "previous candidate retained despite source not being refetched"
        rec["why_not_publication_ready"] = rec.get("why_not_publication_ready") or "source not refetched this run"
    region_excluded: list[dict[str, Any]] = []
    for mid, rec in list(memory.items()):
        if rec.get("manual_decision") in {"manual_keep", "keep", "favorite"}:
            continue
        if str(rec.get("kaliningrad_oblast_only_scope") or "").lower() not in {"true", "1", "yes"} or str(rec.get("kaliningrad_mention_role") or "main_subject") not in {"", "main_subject"}:
            rec["current_lifecycle_status"] = "region_evidence_missing_needs_refetch"
            rec["visibility_status"] = "excluded_from_candidate_memory_by_hard_region_gate"
            rec["next_action"] = "refetch_source_or_manual_region_override"
            rec["why_not_publication_ready"] = "hard region evidence missing in candidate memory"
            region_excluded.append({**rec, "candidate_memory_id": mid})
            memory.pop(mid, None)
    rows = sorted(memory.values(), key=lambda r: (str(r.get("manual_decision") or "") == "reject", str(r.get("not_refetched_this_run") or "") == "true", -float(r.get("best_candidate_score_ever") or 0), str(r.get("post_date") or "")))
    not_refetched = [r for r in rows if str(r.get("not_refetched_this_run")) == "true" and r.get("current_lifecycle_status") != "manual_reject"]
    deltas = []
    for r in rows:
        prev_stage = str(r.get("previous_stage") or "")
        cur_stage = str(r.get("current_stage") or "")
        if str(r.get("not_refetched_this_run")) == "true":
            bucket = "not_refetched_this_run"
        elif not prev_stage:
            bucket = "new_to_system"
        elif prev_stage == cur_stage:
            bucket = "stage_unchanged"
        elif cur_stage in {"favorite", "semantic_candidate", "needs_image_review", "image_fetch_retry_needed", "good_text_weak_media"} and prev_stage not in CANDIDATE_MEMORY_STAGES:
            bucket = "became_candidate"
        elif float(r.get("candidate_score_delta") or 0) > 0.02:
            bucket = "stage_upgraded"
        elif float(r.get("candidate_score_delta") or 0) < -0.02:
            bucket = "stage_downgraded"
        else:
            bucket = "re_seen"
        deltas.append({**r, "delta_bucket": bucket, "current_run_id": run_id})
    for r in region_excluded:
        deltas.append({**r, "delta_bucket": "excluded_by_hard_region_gate", "current_run_id": run_id})
    return rows, not_refetched, deltas


def build_source_frontier_queue_next(frontier_rows: list[dict[str, Any]], source_rows: list[dict[str, Any]], candidate_memory: list[dict[str, Any]], run_id: str) -> list[dict[str, Any]]:
    candidate_source_ids = {str(r.get("source_id") or "") for r in candidate_memory if r.get("source_id")}
    resolved = {str(s.get("canonical_url") or "") for s in source_rows if s.get("telegram_resolve_status") in {"resolved_network", "resolved_from_private_cache"} or s.get("fetch_status") == "ok"}
    out = []
    for row in frontier_rows:
        url = str(row.get("canonical_url") or row.get("normalized_url") or "")
        platform = str(row.get("platform") or row.get("platform_guess") or "")
        edge_types = str(row.get("edge_types_all") or row.get("best_edge_type") or "")
        discovery = str(row.get("discovery_types") or "")
        score = float(row.get("source_candidate_score") or row.get("frontier_priority") or 0)
        pclass = "P3"
        why = "low priority/generic backlog"
        if str(row.get("source_candidate_id") or "") in candidate_source_ids or url in resolved:
            pclass, why, score = "P0", "previous candidate or already resolved source", score + 0.35
        elif "telegram_similar_channel" in edge_types:
            pclass, why, score = "P1", "Telegram similar-channel edge", score + 0.20
        elif any(w in (url + " " + str(row.get("title_guess") or "")).lower() for w in ["travel", "trip", "photo", "путеше", "блог", "фото", "route", "туризм"]):
            pclass, why, score = "P1", "author/travel/photo/catalog signal", score + 0.12
        elif "public_travel_blogger_catalog" in discovery or "public_travel_blogger_catalog" in edge_types:
            pclass, why, score = "P2", "sample public travel blogger catalog", score + 0.05
        frontier_stage = str(row.get("frontier_stage") or ("vk_not_configured" if platform == "vk" else ("unsupported" if platform and not platform.startswith("telegram") else "unresolved")))
        planned = "fetch_cached_entity_history" if frontier_stage == "history_due" else ("probe_telegram_entity_then_history" if platform.startswith("telegram") else ("probe_vk_wall_when_configured" if platform == "vk" else "keep_frontier"))
        blocked_reason = "vk_wall_not_configured" if platform == "vk" and not (os.getenv("VK_ACCESS_TOKEN") or os.getenv("VK_SERVICE_TOKEN") or os.getenv("VK_TOKEN")) else ""
        out.append({
            "queue_rank": 0, "canonical_url": url, "platform": platform, "title_guess": row.get("title_guess") or row.get("username_or_handle") or url,
            "frontier_priority": round(score, 3), "frontier_stage": frontier_stage, "frontier_status": row.get("frontier_status", ""), "why_next": why,
            "discovery_types": discovery, "best_edge_type": row.get("best_edge_type") or edge_types,
            "source_quality_score": round(min(1.0, score), 3), "kaliningrad_prior_score": row.get("best_confidence", ""),
            "authorial_voice_score": 0.65 if pclass in {"P0", "P1"} else 0.35,
            "original_photo_prior_score": 0.60 if pclass in {"P0", "P1"} else 0.30,
            "blogger_source_score": 0.70 if any(w in (url + " " + str(row.get("title_guess") or "")).lower() for w in ["travel", "путеше", "блог", "trip"]) else 0.35,
            "ad_risk_score": 0.20 if pclass in {"P0", "P1", "P2"} else 0.45,
            "resolve_status": row.get("resolve_status", ""), "next_allowed_resolve_at": row.get("next_allowed_resolve_at", ""),
            "probe_status": row.get("probe_status", "probe_later"), "planned_action": planned, "planned_after_run_id": run_id,
            "blocked_reason": blocked_reason, "promotion_class": pclass,
        })
    selected = []
    for pclass, limit in [("P0", 25), ("P1", 40), ("P2", 25), ("P3", 10)]:
        selected.extend(sorted([r for r in out if r["promotion_class"] == pclass], key=lambda r: (-float(r.get("frontier_priority") or 0), r.get("canonical_url") or ""))[:limit])
    seen = set(); final = []
    for row in selected:
        key = row.get("canonical_url")
        if key in seen:
            continue
        seen.add(key); final.append(row)
    for i, row in enumerate(final[:100], start=1):
        row["queue_rank"] = i
    return final[:100]

def compact_shortlist_row(r: dict[str, Any]) -> dict[str, Any]:
    return {
        "rank": r.get("human_shortlist_rank") or r.get("rank"),
        "decision_bucket": r.get("decision_bucket"),
        "next_action": r.get("next_action"),
        "post_url": r.get("post_url"),
        "source_title": r.get("source_title"),
        "platform": r.get("platform"),
        "post_date": r.get("post_date"),
        "short_summary": r.get("short_summary"),
        "why_relevant": r.get("why_this_is_about_kaliningrad"),
        "matched_place_names": r.get("matched_place_names"),
        "content_type": r.get("content_type") or r.get("llm_content_type"),
        "candidate_score": r.get("candidate_score"),
        "text_substance_score": r.get("text_substance_score"),
        "source_geo_class": r.get("source_geo_class"),
        "source_topic_class": r.get("source_topic_class"),
        "nonlocal_value_score": r.get("nonlocal_value_score"),
        "has_firsthand_visit_evidence": r.get("has_firsthand_visit_evidence"),
        "visit_evidence_type": r.get("visit_evidence_type"),
        "first_person_markers": r.get("first_person_markers"),
        "emotion_or_impression_evidence": r.get("emotion_or_impression_evidence"),
        "review_or_opinion_evidence": r.get("review_or_opinion_evidence"),
        "useful_route_evidence": r.get("useful_route_evidence"),
        "publication_story_score": r.get("publication_story_score"),
        "visit_impression_score": r.get("visit_impression_score"),
        "useful_route_score": r.get("useful_route_score"),
        "emotion_observation_score": r.get("emotion_observation_score"),
        "llm_gate_status": r.get("llm_gate_status"),
        "llm_model": r.get("llm_model"),
        "llm_decision": r.get("llm_decision"),
        "llm_reason_short": str(r.get("llm_reason") or "")[:260],
        "image_model_id": r.get("model_id"),
        "image_model_type": r.get("image_model_type"),
        "image_model_runtime": r.get("image_model_runtime"),
        "image_model_input_type": r.get("image_model_input_type"),
        "image_model_device": r.get("image_model_device"),
        "postcardness_score": r.get("postcardness_score"),
        "aesthetic_score": r.get("aesthetic_score"),
        "region_visual_relevance_score": r.get("region_visual_relevance_score"),
        "publication_safety_score": r.get("publication_safety_score"),
        "overall_media_score": r.get("overall_media_score"),
        "image_reviewable": r.get("image_reviewable"),
        "image_publication_ready": r.get("image_publication_ready"),
        "image_model_explanation": r.get("model_short_explanation"),
        "risk_flags": r.get("risk_flags"),
    }


def candidate_memory_shortlist_row(r: dict[str, Any], rank: int) -> dict[str, Any]:
    input_type = str(r.get("image_model_input_type") or "")
    stage = str(r.get("current_stage") or "")
    if str(r.get("image_publication_ready") or "") == "true" and input_type == "actual_image":
        bucket = "publication_ready_actual_image"
        action = "human_final_check"
    elif stage == "image_fetch_retry_needed" or r.get("image_status") == "needs_actual_image_fetch":
        bucket = "needs_actual_image_retry"
        action = "retry_media_download_or_manual_open"
    elif stage == "good_text_weak_media":
        bucket = "good_text_weak_actual_image" if input_type == "actual_image" else "manual_open_promising_text"
        action = "manual_open_if_text_is_strong"
    else:
        bucket = "manual_open_promising_text"
        action = r.get("next_action") or "human_review_without_llm"
    return {
        "rank": rank,
        "decision_bucket": bucket,
        "next_action": action,
        "post_url": r.get("post_url"),
        "source_title": r.get("source_title"),
        "platform": r.get("platform"),
        "post_date": r.get("post_date"),
        "short_summary": r.get("short_summary"),
        "why_relevant": r.get("why_keep_in_memory"),
        "matched_place_names": r.get("matched_place_names"),
        "content_type": r.get("content_type") or r.get("llm_content_type") or r.get("vector_content_type"),
        "source_geo_class": r.get("source_geo_class"),
        "source_topic_class": r.get("source_topic_class"),
        "nonlocal_value_score": r.get("nonlocal_value_score"),
        "has_firsthand_visit_evidence": r.get("has_firsthand_visit_evidence"),
        "visit_evidence_type": r.get("visit_evidence_type"),
        "first_person_markers": r.get("first_person_markers"),
        "emotion_or_impression_evidence": r.get("emotion_or_impression_evidence"),
        "review_or_opinion_evidence": r.get("review_or_opinion_evidence"),
        "useful_route_evidence": r.get("useful_route_evidence"),
        "publication_story_score": r.get("publication_story_score"),
        "candidate_score": r.get("candidate_score_current") or r.get("best_candidate_score_ever"),
        "overall_media_score": r.get("media_score_current") or r.get("best_media_score_ever"),
        "image_status": r.get("image_status"),
        "image_model_input_type": r.get("image_model_input_type"),
        "image_reviewable": r.get("image_reviewable"),
        "image_publication_ready": r.get("image_publication_ready"),
        "llm_status": r.get("llm_status") or "not_called_or_previous",
        "current_lifecycle_status": r.get("current_lifecycle_status"),
        "not_refetched_this_run": r.get("not_refetched_this_run"),
    }


async def discover_telegram_similar_channels(client: Any, source_rows: list[dict[str, Any]], entity_by_source: dict[str, Any], governor: TelegramRequestGovernor, run_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not getenv_bool("REGION_TALK_TG_SIMILAR_ENABLED", getenv_bool("REGION_TALK_DISCOVERY_ENABLE_TELEGRAM_SIMILAR", True)):
        _REGION_TALK_TELEGRAM_RUNTIME.update({"telegram_similar_channels_status": "disabled"})
        return [], []
    max_seed_channels = getenv_int("REGION_TALK_MAX_SIMILAR_SEEDS_PER_RUN", getenv_int("REGION_TALK_TG_SIMILAR_MAX_SEED_CHANNELS_PER_RUN", getenv_int("REGION_TALK_DISCOVERY_MAX_SIMILAR_SEED_CHANNELS", 100)))
    max_per_seed = getenv_int("REGION_TALK_TG_SIMILAR_MAX_RECOMMENDATIONS_PER_SEED", getenv_int("REGION_TALK_DISCOVERY_MAX_SIMILAR_PER_SEED", 10))
    max_frontier = getenv_int("REGION_TALK_MAX_NEW_FRONTIER_PER_RUN", getenv_int("REGION_TALK_TG_SIMILAR_MAX_NEW_FRONTIER_PER_RUN", getenv_int("REGION_TALK_DISCOVERY_MAX_NEW_FRONTIER_PER_RUN", 1000)))
    rows: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    try:
        from telethon.tl import functions  # type: ignore
    except Exception as exc:
        _REGION_TALK_TELEGRAM_RUNTIME.update({"telegram_similar_channels_status": "not_supported_by_telethon_version", "telegram_similar_channels_error": type(exc).__name__})
        return rows, edges
    request_cls = getattr(getattr(functions, "channels", None), "GetChannelRecommendationsRequest", None)
    if request_cls is None:
        _REGION_TALK_TELEGRAM_RUNTIME.update({"telegram_similar_channels_status": "not_supported_by_telethon_version"})
        return rows, edges
    seeds = [r for r in source_rows if (r.get("fetch_status") == "ok" or str(r.get("fetch_status") or "").startswith("profile_resolved")) and r.get("source_id") in entity_by_source]
    seed_pool = sorted(seeds, key=lambda r: (
        bool(str(r.get("similar_next_allowed_at") or "") and str(r.get("similar_next_allowed_at")) > utc_now_iso()),
        str(r.get("similar_last_scanned_at") or ""),
        -float(r.get("monitor_priority_score") or 0),
        int(r.get("priority") or 999),
        str(r.get("canonical_url") or ""),
    ))[:max(max_seed_channels * 4, max_seed_channels)]
    seeds = seed_pool[:max_seed_channels]
    raw_count = 0
    errors = 0
    floodwait = 0
    seen: set[str] = set()
    self_loop_count = 0
    duplicate_count = 0
    seed_updates: dict[str, dict[str, Any]] = {}
    for seed_idx, srow in enumerate(seeds, start=1):
        if governor.recommendation_calls_attempted >= governor.max_recommendation_calls or not governor.has_total_request_budget("channels.getChannelRecommendations", str(srow.get("source_id") or ""), str(srow.get("canonical_url") or "")):
            break
        source_id = str(srow.get("source_id") or "")
        canonical = str(srow.get("canonical_url") or "")
        similar_seed_id = "similar_seed_" + stable_hash(canonical)
        entity = entity_by_source.get(source_id)
        governor.recommendation_calls_attempted += 1
        governor.total_attempted += 1
        governor.requests_by_method["channels.getChannelRecommendations"] = governor.requests_by_method.get("channels.getChannelRecommendations", 0) + 1
        try:
            result = await client(request_cls(channel=entity))
            governor.total_ok += 1
            governor.recommendation_calls_ok += 1
            chats = list(getattr(result, "chats", None) or [])
            raw_count += len(chats)
            unique_for_seed = 0
            governor.recommendation_channels_returned += len(chats)
            for rank, ch in enumerate(chats[:max_per_seed], start=1):
                username = str(getattr(ch, "username", None) or "").strip()
                title = str(getattr(ch, "title", None) or "").strip()
                if not username:
                    continue
                rec_url = "https://t.me/" + username
                cand_id = "src_cand_" + stable_hash("telegram", rec_url)
                if canonical_source_key("telegram", username, rec_url) == canonical_source_key("telegram", srow.get("handle", ""), canonical):
                    self_loop_count += 1
                    rows.append({"run_id": run_id, "seed_source_id": source_id, "seed_channel_url": canonical, "recommended_canonical_url": rec_url, "method_status": "debug_self_loop_rejected", "edge_type": "telegram_similar_channel", "frontier_action": "none", "frontier_reason": "self-loop rejected"})
                    continue
                if cand_id in seen:
                    duplicate_count += 1
                    continue
                seen.add(cand_id)
                unique_for_seed += 1
                confidence = round(max(0.35, 0.85 - 0.025 * (rank - 1)), 3)
                row = {
                    "run_id": run_id,
                    "source_candidate_id": cand_id,
                    "discovered_from_source": srow.get("source_title") or srow.get("resolved_title"),
                    "discovered_from_post_url": "",
                    "discovery_type": "telegram_similar_channels",
                    "edge_type": "telegram_similar_channel",
                    "raw_url": rec_url,
                    "normalized_url": rec_url,
                    "platform_guess": "telegram",
                    "candidate_source_status": "source_frontier",
                    "frontier_status": "queued_unresolved",
                    "confidence": confidence,
                    "recommendation_rank": rank,
                    "recommendation_count_for_seed": len(chats),
                    "recommendation_source_channel_url": canonical,
                    "recommendation_source_channel_title": srow.get("source_title") or srow.get("resolved_title"),
                    "recommended_title": title,
                    "recommended_username": username,
                    "recommended_canonical_url": rec_url,
                    "recommended_is_channel": str(bool(getattr(ch, "broadcast", False))).lower(),
                    "recommended_is_megagroup": str(bool(getattr(ch, "megagroup", False))).lower(),
                    "recommended_verified": str(bool(getattr(ch, "verified", False))).lower(),
                    "similarity_seed_source_id": source_id,
                    "similarity_edge_confidence": confidence,
                    "frontier_action": "queued_resolve_later",
                    "frontier_reason": "Telegram channels.getChannelRecommendations; stored as frontier only, no auto-join/no auto-monitor",
                    "private_state_key": "telegram:username:" + username.lower(),
                }
                row["frontier_priority"] = source_candidate_score(row)
                rows.append(row)
                edge_id = "edge_" + stable_hash(source_id, cand_id, "telegram_similar_channel")
                edges.append({
                    "edge_id": edge_id, "from_source_id": source_id, "from_source_title": srow.get("source_title") or srow.get("resolved_title"),
                    "from_post_id": "", "to_source_candidate_id": cand_id, "to_source_title": title or username,
                    "edge_type": "telegram_similar_channel", "evidence_url": rec_url,
                    "evidence_context_short": f"Similar channel recommendation rank {rank} from {srow.get('source_title')}",
                    "confidence": confidence, "discovery_depth": 1, "run_id": run_id,
                })
                governor.entity_cache["telegram:username:" + username.lower()] = {
                    "canonical_url": rec_url, "username": username, "title_last_seen": title, "entity_kind": "channel",
                    "channel_id_private": str(getattr(ch, "id", "") or ""),
                    "access_hash_private": str(getattr(ch, "access_hash", "") or ""),
                    "resolved_at": utc_now_iso(), "last_used_at": "", "last_success_at": "", "source": "telegram_similar_channels",
                }
                if len(rows) >= max_frontier:
                    break
            seed_updates[similar_seed_id] = {
                "similar_seed_id": similar_seed_id,
                "canonical_url": canonical,
                "similar_last_scanned_at": utc_now_iso(),
                "similar_last_used_at": utc_now_iso(),
                "similar_use_count_increment": 1,
                "similar_last_result_count": len(chats),
                "similar_last_unique_count": unique_for_seed,
                "similar_next_allowed_at": iso_after_seconds(getenv_int("REGION_TALK_SIMILAR_SEED_TTL_SECONDS", 7 * 86400)),
                "similar_error_count_increment": 0,
            }
            if len(rows) >= max_frontier:
                break
        except Exception as exc:
            errors += 1
            seconds = int(getattr(exc, "seconds", 0) or 0)
            if type(exc).__name__ == "FloodWaitError" or seconds:
                floodwait += 1
                governor.mark_floodwait("channels.getChannelRecommendations", source_id, canonical, seconds)
                break
            governor.total_error += 1
            seed_updates[similar_seed_id] = {
                "similar_seed_id": similar_seed_id,
                "canonical_url": canonical,
                "similar_last_scanned_at": utc_now_iso(),
                "similar_last_used_at": utc_now_iso(),
                "similar_use_count_increment": 1,
                "similar_last_result_count": 0,
                "similar_last_unique_count": 0,
                "similar_next_allowed_at": iso_after_seconds(getenv_int("REGION_TALK_SIMILAR_ERROR_BACKOFF_SECONDS", 86400)),
                "similar_error_count_increment": 1,
            }
            rows.append({
                "run_id": run_id, "seed_source_id": source_id, "seed_channel_url": canonical,
                "seed_channel_title": srow.get("source_title"), "method_status": "error",
                "method_error_code": type(exc).__name__, "method_error_message_short": str(exc)[:180],
                "edge_type": "telegram_similar_channel", "frontier_action": "none", "frontier_reason": "recommendation call failed",
            })
    governor.recommendation_channels_added_to_frontier = len([r for r in rows if r.get("candidate_source_status") == "source_frontier"])
    status = "ok" if rows or not errors else "error"
    _REGION_TALK_TELEGRAM_RUNTIME.update({
        "telegram_similar_channels_status": status,
        "telegram_similar_channels_seed_count": len(seeds),
        "telegram_similar_channels_raw_count": raw_count,
        "telegram_similar_channels_unique_count": len({r.get("source_candidate_id") for r in rows if r.get("source_candidate_id")}),
        "telegram_similar_channels_added_to_frontier": governor.recommendation_channels_added_to_frontier,
        "telegram_similar_channels_fetch_errors": errors,
        "telegram_similar_channels_floodwait_count": floodwait,
        "telegram_similar_self_loop_count": self_loop_count,
        "telegram_similar_duplicate_canonical_count": duplicate_count,
        "similar_seed_cursor_advanced": str(bool(seed_updates)).lower(),
        "similar_seed_updates": seed_updates,
    })
    return rows, edges


async def discover_telegram_keyword_sources(client: Any, governor: TelegramRequestGovernor, run_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    mode = (os.getenv("REGION_TALK_DISCOVERY_MODE") or "mixed").strip().lower()
    if mode in {"off", "similar_only"} or not getenv_bool("REGION_TALK_ENABLE_TELEGRAM_KEYWORD_DISCOVERY", mode in {"mixed", "keyword_only"}):
        _REGION_TALK_TELEGRAM_RUNTIME["telegram_keyword_discovery_status"] = "disabled"
        return [], []
    terms_raw = os.getenv("REGION_TALK_TELEGRAM_KEYWORD_QUERIES") or ""
    terms = [x.strip() for x in re.split(r"[;\n|]+", terms_raw) if x.strip()] or TELEGRAM_KEYWORD_DISCOVERY_TERMS
    max_queries = getenv_int("REGION_TALK_MAX_TELEGRAM_KEYWORD_QUERIES", 30)
    per_query = getenv_int("REGION_TALK_TELEGRAM_KEYWORD_RESULTS_PER_QUERY", 20)
    max_frontier = getenv_int("REGION_TALK_MAX_KEYWORD_DISCOVERED_SOURCES_PER_RUN", 300)
    rows: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    processed = 0
    errors = 0
    try:
        for query in terms[:max(0, max_queries)]:
            if not governor.has_total_request_budget("messages.searchGlobal", "keyword_discovery", query):
                break
            processed += 1
            governor.total_attempted += 1
            governor.requests_by_method["messages.searchGlobal"] = governor.requests_by_method.get("messages.searchGlobal", 0) + 1
            try:
                async for msg in client.iter_messages(None, search=query, limit=per_query):
                    chat = None
                    try:
                        chat = await msg.get_chat()
                    except Exception:
                        chat = getattr(msg, "chat", None)
                    username = str(getattr(chat, "username", None) or "").strip()
                    title = str(getattr(chat, "title", None) or getattr(chat, "first_name", None) or "").strip()
                    if not username:
                        continue
                    url = "https://t.me/" + username
                    key = canonical_source_key("telegram", username, url)
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    cand_id = "src_cand_" + stable_hash("telegram", url)
                    row = {
                        "run_id": run_id,
                        "source_candidate_id": cand_id,
                        "discovered_from_source": "telegram_keyword_search",
                        "discovered_from_post_url": f"https://t.me/{username}/{getattr(msg, 'id', '')}",
                        "discovery_type": "telegram_keyword_search",
                        "edge_type": "telegram_keyword_search",
                        "matched_query": query,
                        "raw_url": url,
                        "normalized_url": url,
                        "platform_guess": "telegram",
                        "canonical_source_key": key,
                        "candidate_source_status": "source_frontier",
                        "frontier_status": "queued_unresolved",
                        "confidence": 0.62,
                        "recommended_title": title,
                        "recommended_username": username,
                        "recommended_canonical_url": url,
                        "frontier_action": "queued_resolve_later",
                        "frontier_reason": "Telegram keyword search by Kaliningrad toponym; source candidate only, post is not accepted here",
                        "keyword_evidence_excerpt": "",
                        "private_state_key": "telegram:username:" + username.lower(),
                    }
                    row["frontier_priority"] = source_candidate_score(row)
                    rows.append(row)
                    edges.append({
                        "edge_id": "edge_" + stable_hash("keyword", query, cand_id),
                        "from_source_id": "telegram_keyword_search",
                        "from_source_title": query,
                        "from_post_id": "",
                        "to_source_candidate_id": cand_id,
                        "to_source_title": title or username,
                        "edge_type": "telegram_keyword_search",
                        "evidence_url": row["discovered_from_post_url"],
                        "evidence_context_short": f"Keyword '{query}' matched a public Telegram result; source only",
                        "confidence": row["confidence"],
                        "discovery_depth": 0,
                        "run_id": run_id,
                    })
                    if len(rows) >= max_frontier:
                        break
                governor.total_ok += 1
            except Exception as exc:
                errors += 1
                seconds = int(getattr(exc, "seconds", 0) or 0)
                if type(exc).__name__ == "FloodWaitError" or seconds:
                    governor.mark_floodwait("messages.searchGlobal", "keyword_discovery", query, seconds)
                    break
                governor.total_error += 1
                rows.append({"run_id": run_id, "discovery_type": "telegram_keyword_search", "edge_type": "telegram_keyword_search", "matched_query": query, "method_status": "error", "method_error_code": type(exc).__name__, "method_error_message_short": str(exc)[:180], "frontier_action": "none"})
            if len(rows) >= max_frontier:
                break
    finally:
        _REGION_TALK_TELEGRAM_RUNTIME.update({
            "telegram_keyword_discovery_status": "ok" if rows or not errors else "error",
            "keyword_search_queries_processed": processed,
            "keyword_discovered_sources_unique": len({r.get("source_candidate_id") for r in rows if r.get("source_candidate_id")}),
            "keyword_discovery_errors": errors,
        })
    return rows, edges


def score_text(text: str) -> dict[str, Any]:
    low = (text or "").lower()
    anchor_hits = [a for a in DEFAULT_ANCHORS if a.lower() in low]
    positive_hits = [w for w in POSITIVE_WORDS if w in low]
    news_hits = [w for w in NEWS_WORDS if w in low]
    ad_hits = [w for w in AD_WORDS if w in low]
    trash_hits = [w for w in TRASH_WORDS if w in low]
    region = min(1.0, 0.25 * len(anchor_hits) + (0.25 if len(text) > 240 and anchor_hits else 0))
    news = min(1.0, 0.28 * len(news_hits))
    ad = min(1.0, 0.20 * len(ad_hits))
    trash = min(1.0, 0.35 * len(trash_hits))
    value = min(1.0, 0.25 + 0.08 * len(positive_hits) + min(0.35, len(text) / 1200)) if anchor_hits else 0.0
    tone = min(1.0, 0.35 + 0.12 * len(positive_hits)) if anchor_hits else 0.0
    return {"anchor_hits": anchor_hits, "positive_hits": positive_hits, "news_hits": news_hits, "ad_hits": ad_hits, "trash_hits": trash_hits, "region_relevance_score": round(region,3), "newsiness_score": round(news,3), "ad_score": round(ad,3), "trash_score": round(trash,3), "text_value_score": round(value,3), "positive_or_useful_tone_score": round(tone,3)}


def image_scores_skipped(reason: str) -> dict[str, Any]:
    return {
        "technical_quality_score":0.0,"aesthetic_score":0.0,"postcardness_score":0.0,"region_visual_relevance_score":0.0,
        "publication_safety_score":0.0,"low_noise_score":0.0,"overall_media_score":0.0,
        "is_selected_for_publication":False,"image_publication_ready":"false","image_reviewable":"false","image_quality_bucket":"skipped",
        "recognized_visual_elements":"","model_short_explanation":"Image scoring skipped by text gate: " + reason,
        "failure_reason":"image_scoring_skipped_by_text_gate","model_id":"not_run_text_gate","model_version":"2026-07-05",
        "image_model_type":"not_run","image_model_runtime":"not_run","image_model_input_type":"none","image_scoring_mode": current_image_scoring_mode(),
        "image_model_device":"not_run","image_download_status":"not_needed_text_gate",
    }


def current_image_scoring_mode() -> str:
    return (os.getenv("REGION_TALK_IMAGE_SCORING_MODE") or "cv_aesthetic_clip").strip().lower() or "cv_aesthetic_clip"


def _metadata_media_scores(has_media: bool, text_score: dict[str, Any], *, reason_prefix: str = "") -> dict[str, Any]:
    if not has_media:
        return {"technical_quality_score":0.0,"aesthetic_score":0.0,"postcardness_score":0.0,"region_visual_relevance_score":0.0,"publication_safety_score":1.0,"low_noise_score":0.0,"overall_media_score":0.0,"is_selected_for_publication":False,"image_publication_ready":"false","image_reviewable":"false","image_quality_bucket":"no_media","recognized_visual_elements":"","model_short_explanation":"No media detected; image gate failed.","failure_reason":"no_media","model_id":"cv_only_metadata_v1","model_version":"2026-07-05","image_model_type":"cv_only","image_model_runtime":"kaggle_local","image_model_input_type":"metadata_only","image_scoring_mode": current_image_scoring_mode(),"image_model_device":"cpu","image_download_status":"no_media"}
    anchors = text_score.get("anchor_hits") or []
    positives = text_score.get("positive_hits") or []
    technical = 0.72
    aesthetic = min(0.92, 0.68 + 0.04*len(positives))
    postcard = min(0.94, 0.66 + 0.06*len(anchors) + 0.03*len(positives))
    region_visual = min(0.95, 0.58 + 0.08*len(anchors))
    safety = 0.98 if not text_score.get("news_hits") and not text_score.get("trash_hits") else 0.78
    low_noise = 0.82
    overall = round((technical+aesthetic+postcard+region_visual+safety+low_noise)/6,3)
    elements = []
    low = " ".join(anchors + positives).lower()
    if "море" in low or "балтий" in low: elements.append("sea/coast")
    if "курш" in low or "дюн" in low: elements.append("dunes/nature")
    if "архитект" in low or "кёниг" in low or "калининград" in low: elements.append("city/architecture")
    if not elements: elements.append("travel visual candidate")
    prefix = (reason_prefix + "; ") if reason_prefix else ""
    return {"technical_quality_score":round(technical,3),"aesthetic_score":round(aesthetic,3),"postcardness_score":round(postcard,3),"region_visual_relevance_score":round(region_visual,3),"publication_safety_score":round(safety,3),"low_noise_score":round(low_noise,3),"overall_media_score":overall,"is_selected_for_publication":False,"image_publication_ready":"false","image_reviewable":"false","image_quality_bucket":"metadata_pending_actual_image","recognized_visual_elements":"; ".join(elements),"model_short_explanation":prefix + f"cv_only metadata fallback: media present; region anchors={len(anchors)}, travel/visual cues={len(positives)}; safety={'ok' if safety>=0.95 else 'blocked by news/trash cues'}. Actual image bytes are required before reviewable/publication-ready.","failure_reason":"needs_actual_image_fetch","model_id":"cv_only_metadata_v1","model_version":"2026-07-05","image_model_type":"cv_only","image_model_runtime":"kaggle_local","image_model_input_type":"metadata_only","image_scoring_mode": current_image_scoring_mode(),"image_model_device":"cpu","image_download_status":"actual_image_missing_or_fallback"}


def _local_image_stats(path: str) -> dict[str, float]:
    try:
        from PIL import Image, ImageFilter, ImageStat  # type: ignore
    except Exception:
        if getenv_bool("REGION_TALK_AUTO_INSTALL", True):
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "pillow"])
            from PIL import Image, ImageFilter, ImageStat  # type: ignore
        else:
            raise
    img = Image.open(path).convert("RGB")
    w, h = img.size
    small = img.resize((max(1, min(512, w)), max(1, int(h * min(512, w) / max(1, w)))), Image.Resampling.LANCZOS)
    gray = small.convert("L")
    stat = ImageStat.Stat(gray)
    mean = float(stat.mean[0]) / 255.0
    contrast = float(stat.stddev[0]) / 96.0
    edges = gray.filter(ImageFilter.FIND_EDGES)
    edge_mean = float(ImageStat.Stat(edges).mean[0]) / 255.0
    aspect_ok = 0.0 if min(w, h) < 420 else 1.0
    brightness_ok = max(0.0, 1.0 - abs(mean - 0.52) / 0.52)
    technical = max(0.0, min(1.0, 0.35*aspect_ok + 0.35*brightness_ok + 0.30*min(1.0, contrast)))
    low_noise = max(0.0, min(1.0, 1.0 - max(0.0, edge_mean - 0.24) * 2.2))
    return {"technical": technical, "contrast": min(1.0, contrast), "low_noise": low_noise, "width": float(w), "height": float(h)}


def _ensure_clip_model() -> tuple[Any, Any, str]:
    global _REGION_TALK_CLIP_MODEL, _REGION_TALK_CLIP_PROCESSOR, _REGION_TALK_CLIP_DEVICE
    if _REGION_TALK_CLIP_MODEL is not None and _REGION_TALK_CLIP_PROCESSOR is not None and _REGION_TALK_CLIP_DEVICE:
        return _REGION_TALK_CLIP_MODEL, _REGION_TALK_CLIP_PROCESSOR, _REGION_TALK_CLIP_DEVICE
    try:
        import torch  # type: ignore
        from transformers import CLIPModel, CLIPProcessor  # type: ignore
    except Exception:
        if getenv_bool("REGION_TALK_AUTO_INSTALL", True):
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "pillow", "transformers", "torch"])
            import torch  # type: ignore
            from transformers import CLIPModel, CLIPProcessor  # type: ignore
        else:
            raise
    model_id = os.getenv("REGION_TALK_CLIP_MODEL_ID") or "openai/clip-vit-base-patch32"
    device = "cuda" if getattr(torch, "cuda", None) and torch.cuda.is_available() else "cpu"
    _REGION_TALK_CLIP_PROCESSOR = CLIPProcessor.from_pretrained(model_id)
    _REGION_TALK_CLIP_MODEL = CLIPModel.from_pretrained(model_id).to(device)
    _REGION_TALK_CLIP_MODEL.eval()
    _REGION_TALK_CLIP_DEVICE = device
    return _REGION_TALK_CLIP_MODEL, _REGION_TALK_CLIP_PROCESSOR, device


def _clip_image_scores(image_path: str, text_score: dict[str, Any]) -> dict[str, Any]:
    from PIL import Image  # type: ignore
    import torch  # type: ignore
    model, processor, device = _ensure_clip_model()
    img = Image.open(image_path).convert("RGB")
    prompts = [
        "beautiful high quality travel landscape photo of the Baltic Sea coast dunes forest or historic city architecture",
        "useful authentic travel photo from Kaliningrad Oblast or Baltic resort town",
        "low quality screenshot meme poster flyer advertisement with lots of text",
        "news accident crime politics trash shocking image",
    ]
    inputs = processor(text=prompts, images=img, return_tensors="pt", padding=True).to(device)
    with torch.no_grad():
        outputs = model(**inputs)
        probs = outputs.logits_per_image.softmax(dim=1)[0].detach().cpu().tolist()
    stats = _local_image_stats(image_path)
    travel, region_like, ad_like, news_like = [float(x) for x in probs]
    anchors = text_score.get("anchor_hits") or []
    positives = text_score.get("positive_hits") or []
    technical = max(0.0, min(1.0, stats["technical"]))
    aesthetic = max(0.0, min(1.0, 0.55*travel + 0.25*stats["contrast"] + 0.20*technical))
    postcard = max(0.0, min(1.0, 0.62*travel + 0.18*region_like + 0.10*min(1.0, len(positives)/4) + 0.10*technical))
    region_visual = max(0.0, min(1.0, 0.58*region_like + 0.20*travel + 0.22*min(1.0, len(anchors)/4)))
    safety = max(0.0, min(1.0, 1.0 - max(ad_like, news_like)*0.85))
    low_noise = max(0.0, min(1.0, stats["low_noise"]))
    overall = round((technical+aesthetic+postcard+region_visual+safety+low_noise)/6,3)
    reviewable = technical>=0.50 and postcard>=0.55 and safety>=0.65 and overall>=0.60
    selected = technical>=0.62 and aesthetic>=0.66 and postcard>=0.68 and region_visual>=0.52 and safety>=0.82 and low_noise>=0.62 and overall>=0.72
    elements = []
    if travel >= 0.35: elements.append("travel/postcard visual")
    if region_like >= 0.25 or anchors: elements.append("region-compatible visual")
    if ad_like >= 0.35: elements.append("possible poster/ad screenshot")
    if news_like >= 0.35: elements.append("possible news/trash visual")
    if not elements: elements.append("visual candidate")
    model_id = os.getenv("REGION_TALK_CLIP_MODEL_ID") or "openai/clip-vit-base-patch32"
    return {
        "technical_quality_score":round(technical,3),"aesthetic_score":round(aesthetic,3),"postcardness_score":round(postcard,3),
        "region_visual_relevance_score":round(region_visual,3),"publication_safety_score":round(safety,3),"low_noise_score":round(low_noise,3),
        "overall_media_score":overall,"is_selected_for_publication":selected,"image_publication_ready":str(selected).lower(),
        "image_reviewable":str(reviewable).lower(),"image_quality_bucket":"publication_ready" if selected else ("reviewable_image" if reviewable else "weak_image"),
        "recognized_visual_elements":"; ".join(elements),
        "model_short_explanation":f"Kaggle-local CLIP actual-image scoring; probs travel={travel:.3f}, region_like={region_like:.3f}, ad_like={ad_like:.3f}, news_like={news_like:.3f}; device={device}.",
        "failure_reason":"" if selected else ("reviewable_below_publication_threshold" if reviewable else "below_reviewable_image_threshold"),
        "model_id":"clip_local_" + re.sub(r"[^A-Za-z0-9_.-]+", "_", model_id),"model_version":"2026-07-05",
        "image_model_type":"clip","image_model_runtime":"kaggle_local","image_model_input_type":"actual_image","image_scoring_mode": current_image_scoring_mode(),
        "image_model_device":device,"image_download_status":"downloaded_actual_image",
    }


def vk_wall_token() -> str:
    return (os.getenv("VK_ACCESS_TOKEN") or os.getenv("VK_SERVICE_TOKEN") or os.getenv("VK_TOKEN") or "").strip()


def vk_domain_from_seed(seed: Seed) -> str:
    raw = (seed.url or seed.handle or "").strip()
    m = re.search(r"vk\.com/(?:club|public)?([A-Za-z0-9_.-]+)", raw, re.I)
    if m:
        return m.group(1)
    return canonical_handle(seed.handle).lstrip("@")


def fetch_vk_wall_for_seed(seed: Seed, output_dir: Path, max_posts: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    token = vk_wall_token()
    src = source_status_row(seed, "skipped_vk_wall_not_configured" if not token else "ok", vk_wall_probe_status="not_configured" if not token else "ok", fetch_attempted=str(bool(token)).lower())
    if not token:
        src["source_probe_reason"] = "VK token is not configured; source retained in frontier/backlog"
        return src, []
    domain = vk_domain_from_seed(seed)
    if not domain:
        src.update({"fetch_status": "skipped_vk_wall_domain_missing", "vk_wall_probe_status": "domain_missing", "source_probe_reason": "Cannot extract VK domain from seed"})
        return src, []
    try:
        import urllib.parse, urllib.request
        params = urllib.parse.urlencode({"domain": domain, "count": max(1, min(max_posts, getenv_int("REGION_TALK_VK_MAX_WALL_POSTS_PER_SOURCE", max_posts))), "filter": "owner", "extended": 1, "access_token": token, "v": os.getenv("VK_API_VERSION") or "5.199"})
        with urllib.request.urlopen("https://api.vk.com/method/wall.get?" + params, timeout=getenv_int("REGION_TALK_VK_TIMEOUT_SECONDS", 20)) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if payload.get("error"):
            err = payload["error"] or {}
            src.update({"fetch_status": "error_vk_wall_api", "vk_wall_probe_status": "error", "fetch_error_code": err.get("error_code"), "fetch_error_message": str(err.get("error_msg") or "")[:180]})
            return src, []
        response = payload.get("response") or {}
        items = response.get("items") or []
        groups = {str(g.get("id")): g for g in response.get("groups") or [] if isinstance(g, dict)}
        posts: list[dict[str, Any]] = []
        media_budget = getenv_int("REGION_TALK_VK_MAX_MEDIA_DOWNLOADS_PER_RUN", getenv_int("REGION_TALK_TG_MAX_MEDIA_DOWNLOADS_PER_RUN", 120))
        for item in items:
            if not isinstance(item, dict):
                continue
            mid = item.get("id")
            owner_id = item.get("owner_id") or item.get("from_id")
            text = str(item.get("text") or "").strip()
            if not mid or not owner_id or not text:
                continue
            dt = datetime.fromtimestamp(int(item.get("date") or 0), timezone.utc).isoformat() if item.get("date") else ""
            post_url = f"https://vk.com/wall{owner_id}_{mid}"
            attachments = item.get("attachments") or []
            photo_url = ""
            for a in attachments:
                if not isinstance(a, dict) or a.get("type") != "photo":
                    continue
                sizes = ((a.get("photo") or {}).get("sizes") or [])
                if sizes:
                    best = sorted([s for s in sizes if isinstance(s, dict) and s.get("url")], key=lambda s: int(s.get("width") or 0) * int(s.get("height") or 0), reverse=True)[0]
                    photo_url = str(best.get("url") or "")
                    break
            primary_media_path = ""
            if photo_url and media_budget > 0 and getenv_bool("REGION_TALK_DOWNLOAD_MEDIA_FOR_SCORING", True):
                try:
                    media_dir = output_dir / "media" / stable_hash("vk", domain)
                    media_dir.mkdir(parents=True, exist_ok=True)
                    ext = ".jpg" if ".jpg" in photo_url.lower() or ".jpeg" in photo_url.lower() else ".jpg"
                    target = media_dir / f"{mid}{ext}"
                    urllib.request.urlretrieve(photo_url, target)
                    primary_media_path = str(target)
                    media_budget -= 1
                except Exception:
                    primary_media_path = ""
            copy_history = item.get("copy_history") or []
            origin = copy_history[0] if copy_history and isinstance(copy_history[0], dict) else {}
            origin_owner = origin.get("owner_id") or ""
            origin_mid = origin.get("id") or ""
            forwarded_url = f"https://vk.com/wall{origin_owner}_{origin_mid}" if origin_owner and origin_mid else ""
            group = groups.get(str(abs(int(owner_id))) if str(owner_id).lstrip("-").isdigit() else "") or {}
            title = str(group.get("name") or seed.source_title or domain)
            posts.append({"post_id":"post_"+stable_hash("vk", owner_id, mid), "source_id": seed.source_id, "source_seed_id": seed.source_seed_id, "source_title": title, "platform":"vk", "handle": seed.handle or domain, "post_url": post_url, "platform_post_key": f"vk:{owner_id}:{mid}", "post_date": dt, "text": text, "text_excerpt": re.sub(r"\s+", " ", text)[:500], "has_media": bool(photo_url), "media_count": 1 if photo_url else 0, "primary_media_path": primary_media_path, "local_media_paths": primary_media_path, "rights_policy": seed.rights_policy, "source_kind": seed.source_kind, "source_type": seed.source_kind, "source_url": seed.canonical_url, "is_forwarded_or_repost": bool(forwarded_url), "forwarded_from_source_title": "", "forwarded_from_source_id": "src_" + stable_hash("vk_forward", forwarded_url) if forwarded_url else "", "forwarded_from_platform": "vk" if forwarded_url else "", "forwarded_from_handle": "", "forwarded_from_url": forwarded_url, "forwarded_from_post_url": forwarded_url, "forwarded_from_confidence": 0.75 if forwarded_url else 0.0, "original_source_candidate_id": "src_cand_" + stable_hash("vk", forwarded_url) if forwarded_url else ""})
        src.update({"fetch_status": "ok", "vk_wall_probe_status": "ok", "posts_scanned": len(posts), "history_fetch_mode": "vk_wall_primary_scan", "history_fetch_runtime_seconds": "", "last_seen_post_date": max([p.get("post_date") or "" for p in posts] or [""]), "source_probe_reason": "minimal VK wall.get fetch; text/photos/copy_history origins"})
        return src, posts
    except Exception as exc:
        src.update({"fetch_status": "error_vk_wall_fetch", "vk_wall_probe_status": "error", "fetch_error_code": type(exc).__name__, "fetch_error_message": str(exc)[:180]})
        return src, []


def media_scores(has_media: bool, text_score: dict[str, Any], post: dict[str, Any] | None = None) -> dict[str, Any]:
    mode = current_image_scoring_mode()
    post = post or {}
    image_path = str(post.get("primary_media_path") or "").strip()
    if mode in {"cv_aesthetic_clip", "clip", "cv_aesthetic_clip_vlm"} and has_media and image_path and Path(image_path).exists():
        try:
            return _clip_image_scores(image_path, text_score)
        except Exception as exc:
            return _metadata_media_scores(has_media, text_score, reason_prefix=f"neural_image_scoring_unavailable:{type(exc).__name__}")
    return _metadata_media_scores(has_media, text_score, reason_prefix="actual_image_missing" if has_media and mode != "cv_only" else "")


def candidate_score(text_score: dict[str, Any], media: dict[str, Any], seed: Seed) -> float:
    source_quality = 0.72 if seed.priority == 1 else 0.60 if seed.priority == 2 else 0.50
    score = text_score["region_relevance_score"]*0.20 + source_quality*0.12 + 0.55*0.12 + text_score["text_value_score"]*0.16 + text_score["positive_or_useful_tone_score"]*0.12 + media["overall_media_score"]*0.20 + 0.2*0.04 + 0.2*0.04
    score -= text_score["newsiness_score"]*0.35 + text_score["trash_score"]*0.40 + text_score["ad_score"]*0.18
    if not media["is_selected_for_publication"]: score -= 0.25
    if seed.rights_policy == "unknown": score -= 0.03
    return round(max(0.0, min(1.0, score)), 3)


async def fetch_telegram_posts(seeds: list[Seed], status: Status, output_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    max_sources = getenv_int("REGION_TALK_MAX_SOURCES", 5)
    max_posts = getenv_int("REGION_TALK_MAX_POSTS_PER_SOURCE", 20)
    fetch_enabled = getenv_bool("REGION_TALK_FETCH_TELEGRAM", True)
    discovery_mode = (os.getenv("REGION_TALK_DISCOVERY_MODE") or "mixed").strip().lower()
    history_scan_mode = (os.getenv("REGION_TALK_HISTORY_SCAN_MODE") or "primary_and_delta").strip().lower()
    run_id = os.getenv("REGION_TALK_RUN_ID") or f"region-talk-{RUN_STARTED_AT.strftime('%Y%m%dT%H%M%SZ')}"
    previous_state, _state_meta = load_region_talk_state(output_dir)
    governor = TelegramRequestGovernor(run_id, output_dir, previous_state)
    dynamic = frontier_dynamic_seeds(previous_state, getenv_int("REGION_TALK_MAX_NEW_SOURCE_PROBES", 30))
    all_seed_candidates = list(seeds)
    seen_seed_urls = {s.canonical_url for s in all_seed_candidates}
    for s in dynamic:
        if s.canonical_url not in seen_seed_urls:
            all_seed_candidates.append(s)
            seen_seed_urls.add(s.canonical_url)
    selected = selected_sources_for_run(all_seed_candidates, max_sources)
    _REGION_TALK_TELEGRAM_RUNTIME["dynamic_frontier_seed_count"] = len(dynamic)
    _REGION_TALK_TELEGRAM_RUNTIME["history_sources_target"] = governor.max_history_sources
    monitored = [s for s in selected if s.platform == "telegram" and (s.handle or "t.me/" in s.url.lower())]
    source_rows: list[dict[str, Any]] = []
    posts: list[dict[str, Any]] = []
    entity_by_source: dict[str, Any] = {}
    for seed in selected:
        if seed.platform == "telegram":
            continue
        if seed.platform == "vk":
            if fetch_enabled:
                vk_row, vk_posts = fetch_vk_wall_for_seed(seed, output_dir, max_posts)
                source_rows.append(vk_row)
                posts.extend(vk_posts)
            else:
                status_value = "skipped_vk_wall_not_configured" if not vk_wall_token() else "skipped_fetch_disabled"
                source_rows.append(source_status_row(seed, status_value, vk_wall_probe_status="fetch_disabled", source_probe_reason="VK wall fetch disabled by REGION_TALK_FETCH_TELEGRAM=0"))
        elif seed.platform == "vkvideo":
            source_rows.append(source_status_row(seed, "skipped_vkvideo_auxiliary_not_implemented", vk_wall_probe_status="not_applicable_vkvideo_auxiliary", source_probe_reason="VK Video is auxiliary for discovery/media, not a wall fetch source in this MVP run."))
        else:
            source_rows.append(source_status_row(seed, "skipped_unsupported_platform", vk_wall_probe_status="not_applicable", source_probe_reason="Non-Telegram source is tracked for coverage/discovery, not fetched in this MVP run."))
    if not fetch_enabled:
        for s in monitored:
            source_rows.append(source_status_row(s, "skipped_fetch_disabled"))
        return source_rows, posts
    try:
        from telethon import TelegramClient  # type: ignore
        from telethon.sessions import StringSession  # type: ignore
        import telethon  # type: ignore
        _REGION_TALK_TELEGRAM_RUNTIME["telethon_version"] = str(getattr(telethon, "__version__", ""))
    except Exception:
        if getenv_bool("REGION_TALK_AUTO_INSTALL", True):
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "telethon"])
            from telethon import TelegramClient  # type: ignore
            from telethon.sessions import StringSession  # type: ignore
            import telethon  # type: ignore
            _REGION_TALK_TELEGRAM_RUNTIME["telethon_version"] = str(getattr(telethon, "__version__", ""))
        else:
            for s in monitored:
                source_rows.append(source_status_row(s, "skipped_telethon_not_installed"))
            return source_rows, posts
    bundle_env = (os.getenv("REGION_TALK_AUTH_BUNDLE_ENV") or "TELEGRAM_AUTH_BUNDLE_DISCOVERY").strip()
    raw = (os.getenv(bundle_env) or "").strip()
    api_id = int((os.getenv("TG_API_ID") or os.getenv("TELEGRAM_API_ID") or "0").strip() or 0)
    api_hash = (os.getenv("TG_API_HASH") or os.getenv("TELEGRAM_API_HASH") or "").strip()
    bundle: dict[str, Any] = {}
    if raw:
        bundle = json.loads(base64.urlsafe_b64decode(raw.encode("ascii")).decode("utf-8"))
    session = str(bundle.get("session") or os.getenv("TG_SESSION") or os.getenv("TELEGRAM_SESSION") or "").strip()
    if not session or not api_id or not api_hash:
        for s in monitored:
            source_rows.append(source_status_row(s, "skipped_missing_telethon_credentials"))
        return source_rows, posts
    client = TelegramClient(StringSession(session), api_id, api_hash, device_model=str(bundle.get("device_model") or "Region Talk Discovery"), system_version=str(bundle.get("system_version") or "Linux"), app_version=str(bundle.get("app_version") or "1.0"), lang_code=str(bundle.get("lang_code") or "ru"), system_lang_code=str(bundle.get("system_lang_code") or "ru"))
    try:
        client.flood_sleep_threshold = getenv_int("REGION_TALK_TG_FLOODWAIT_MAX_SLEEP_SECONDS", 60)
    except Exception:
        pass
    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        raise RuntimeError("Telethon client is not authorized")
    try:
        for idx, seed in enumerate(monitored, start=1):
            status.event("alive", phase="fetch", progress_label=f"источники {idx}/{len(monitored)}", sources_done=idx-1, sources_total=len(monitored))
            handle = seed.handle.lstrip("@")
            if not handle or "/" in handle or handle.startswith("http"):
                source_rows.append(source_status_row(seed, "skipped_telegram_handle_not_configured", vk_wall_probe_status="not_applicable"))
                continue
            src_row = source_status_row(seed, "ok", vk_wall_probe_status="not_applicable", selected_for_planning="true", fetch_attempted="false")
            entity, resolve_meta = await governor.resolve_entity(client, seed)
            src_row.update(resolve_meta)
            if entity is None:
                src_row.setdefault("fetch_status", resolve_meta.get("fetch_status") or "skipped_telegram_unresolved_deferred")
                source_rows.append(src_row)
                continue
            if history_scan_mode == "off" or discovery_mode in {"similar_only", "keyword_only"}:
                src_row.update({"fetch_status": "profile_resolved_history_disabled", "fetch_attempted": "false", "source_probe_reason": f"history scan disabled by discovery_mode={discovery_mode} history_scan_mode={history_scan_mode}", "history_fetch_mode": "history_disabled_discovery_only", "posts_scanned": 0})
                source_rows.append(src_row)
                entity_by_source[seed.source_id] = entity
                continue
            if governor.history_sources_attempted >= governor.max_history_sources:
                src_row.update({"fetch_status": "skipped_telegram_budget_exhausted", "fetch_attempted": "false", "source_probe_reason": "history source budget exhausted"})
                source_rows.append(src_row)
                continue
            entity_by_source[seed.source_id] = entity
            governor.history_sources_attempted += 1
            src_row["fetch_attempted"] = "true"
            history_fetch_started = time.monotonic()
            src_row["history_fetch_mode"] = "delta_scan_active" if not seed.source_seed_id.startswith("frontier_dynamic_") else "first_probe_or_shallow_backfill"
            src_row["is_new_source_this_run"] = str(seed.source_seed_id.startswith("frontier_dynamic_")).lower()
            src_row["history_source_cached_entity"] = str(resolve_meta.get("telegram_resolve_status") == "resolved_from_private_cache").lower()
            src_row["history_source_network_resolved"] = str(resolve_meta.get("telegram_resolve_status") == "resolved_network").lower()
            try:
                title = str(resolve_meta.get("resolved_title") or getattr(entity, "title", None) or seed.source_title)
                src_row["resolved_title"] = title
                seen: set[int] = set()
                anchor_cap = getenv_int("REGION_TALK_TG_MAX_ANCHOR_QUERIES_PER_SOURCE", 3)
                queries = [None] + DEFAULT_ANCHORS[:max(0, anchor_cap)]
                for q in queries:
                    if not governor.has_total_request_budget("iter_messages", seed.source_id, seed.canonical_url):
                        src_row.update({"fetch_status": "skipped_telegram_total_request_budget_exhausted", "source_probe_reason": "total Telegram request budget exhausted"})
                        break
                    per_query = max(3, min(max_posts, getenv_int("REGION_TALK_TG_MAX_HISTORY_POSTS_PER_SOURCE", max_posts)) // len(queries) + 1)
                    governor.total_attempted += 1
                    governor.requests_by_method["iter_messages"] = governor.requests_by_method.get("iter_messages", 0) + 1
                    async for msg in client.iter_messages(entity, limit=per_query, search=q):
                        mid = int(getattr(msg, "id", 0) or 0)
                        if not mid or mid in seen:
                            continue
                        seen.add(mid)
                        text = str(getattr(msg, "message", None) or "").strip()
                        if not text:
                            continue
                        dt = getattr(msg, "date", None)
                        post_url = f"https://t.me/{handle}/{mid}"
                        has_media = bool(getattr(msg, "photo", None) or getattr(msg, "document", None) or getattr(msg, "media", None))
                        primary_media_path = ""
                        if has_media and getenv_bool("REGION_TALK_DOWNLOAD_MEDIA_FOR_SCORING", True) and governor.media_downloads_attempted < governor.max_media_downloads:
                            governor.media_downloads_attempted += 1
                            try:
                                media_dir = output_dir / "media" / stable_hash("telegram", handle)
                                media_dir.mkdir(parents=True, exist_ok=True)
                                downloaded = await client.download_media(msg, file=str(media_dir / f"{mid}"))
                                if downloaded:
                                    primary_media_path = str(downloaded)
                                    governor.media_downloads_ok += 1
                            except Exception:
                                primary_media_path = ""
                        fwd = getattr(msg, "fwd_from", None) or getattr(msg, "forward", None)
                        forwarded_from_handle = ""
                        forwarded_from_title = ""
                        forwarded_from_url = ""
                        forwarded_from_post_url = ""
                        forwarded_from_confidence = 0.0
                        if fwd:
                            from_name = str(getattr(fwd, "from_name", None) or "").strip()
                            forwarded_from_title = from_name
                            from_id = getattr(fwd, "from_id", None)
                            channel_id = str(getattr(from_id, "channel_id", "") or "")
                            if channel_id:
                                forwarded_from_handle = "channel_" + channel_id
                                forwarded_from_url = "https://t.me/c/" + channel_id
                                original_mid = getattr(fwd, "channel_post", None)
                                if original_mid:
                                    forwarded_from_post_url = forwarded_from_url + "/" + str(original_mid)
                                forwarded_from_confidence = 0.7
                            elif from_name:
                                forwarded_from_confidence = 0.45
                        posts.append({"post_id":"post_"+stable_hash("telegram", handle, mid), "source_id": seed.source_id, "source_seed_id": seed.source_seed_id, "source_title": title, "platform":"telegram", "handle": seed.handle, "post_url": post_url, "platform_post_key": f"tg:{handle}:{mid}", "post_date": dt.isoformat() if dt else "", "text": text, "text_excerpt": re.sub(r"\s+", " ", text)[:500], "has_media": has_media, "media_count": 1 if has_media else 0, "primary_media_path": primary_media_path, "local_media_paths": primary_media_path, "rights_policy": seed.rights_policy, "source_kind": seed.source_kind, "source_type": seed.source_kind, "source_url": seed.canonical_url, "is_forwarded_or_repost": bool(fwd), "forwarded_from_source_title": forwarded_from_title, "forwarded_from_source_id": "src_" + stable_hash("telegram_forward", forwarded_from_url or forwarded_from_title) if fwd else "", "forwarded_from_platform": "telegram" if fwd else "", "forwarded_from_handle": forwarded_from_handle, "forwarded_from_url": forwarded_from_url, "forwarded_from_post_url": forwarded_from_post_url, "forwarded_from_confidence": forwarded_from_confidence, "original_source_candidate_id": "src_cand_" + stable_hash("telegram", forwarded_from_url or forwarded_from_title) if fwd else ""})
                        if len(seen) >= max_posts:
                            break
                    if len(seen) >= max_posts:
                        break
                governor.total_ok += 1
                governor.history_sources_ok += 1
                governor.history_posts_fetched += len(seen)
                src_row["posts_scanned"] = len(seen)
                post_dates = [str(p.get("post_date") or "") for p in posts if p.get("source_id") == seed.source_id]
                src_row["last_seen_post_date"] = max(post_dates or [""])
                src_row["history_fetch_runtime_seconds"] = round(time.monotonic() - history_fetch_started, 3)
                jitter = float(os.getenv("REGION_TALK_TG_HISTORY_SOURCE_JITTER_SECONDS") or "0.5")
                if jitter > 0:
                    await asyncio.sleep(random.uniform(0, jitter))
            except Exception as exc:
                seconds = int(getattr(exc, "seconds", 0) or 0)
                if type(exc).__name__ == "FloodWaitError" or seconds:
                    cooldown_until = governor.mark_floodwait("iter_messages", seed.source_id, seed.canonical_url, seconds)
                    src_row["fetch_status"] = "error_floodwait_history"
                    src_row["next_allowed_resolve_at"] = cooldown_until
                else:
                    src_row["fetch_status"] = "error_telegram_rpc"
                src_row["fetch_error_code"] = type(exc).__name__
                src_row["fetch_error_message"] = str(exc)[:180]
            source_rows.append(src_row)
        similar_rows, similar_edges = await discover_telegram_similar_channels(client, source_rows, entity_by_source, governor, run_id)
        keyword_rows, keyword_edges = await discover_telegram_keyword_sources(client, governor, run_id)
        _REGION_TALK_TELEGRAM_RUNTIME["similar_rows"] = similar_rows
        _REGION_TALK_TELEGRAM_RUNTIME["similar_edges"] = similar_edges
        _REGION_TALK_TELEGRAM_RUNTIME["keyword_rows"] = keyword_rows
        _REGION_TALK_TELEGRAM_RUNTIME["keyword_edges"] = keyword_edges
    finally:
        governor.write_ledger()
        _REGION_TALK_TELEGRAM_RUNTIME["entity_cache"] = governor.entity_cache
        _REGION_TALK_TELEGRAM_RUNTIME["cooldowns"] = governor.cooldowns
        _REGION_TALK_TELEGRAM_RUNTIME["observability"] = governor.observability_row(RUN_STARTED_AT.isoformat(), utc_now_iso())
        await client.disconnect()
    return source_rows, posts



def llm_text_gate_prompt(post: dict[str, Any], evidence: dict[str, Any]) -> str:
    text = str(post.get("text") or "")[:6000]
    payload = {
        "task": "Decide whether this Telegram/VK post is a reviewable Region Talk candidate.",
        "rules": [
            "Accept only if the main subject is Kaliningrad Oblast, not one item in a multi-region/country digest.",
            "Reject ads, promos, registrations, contests, paid/service CTAs, app-download promos, event announcements, and news digests.",
            "Prefer visit impressions, route/useful observations, emotional or memorable details about visiting the region.",
            "A single-location Kaliningrad Oblast card is NOT a multi-region roundup even if it belongs to a source rubric; classify it as single_location_photo_card or encyclopedic_card_candidate, not reject_multi_region_roundup.",
            "Encyclopedic/single-location cards may be research candidates but weaker than firsthand visit impressions; pure visual dumps or passing mentions are not publication candidates.",
            "Return JSON only.",
        ],
        "evidence_hints_not_decisions": evidence,
        "post": {
            "source_title": post.get("source_title"),
            "post_url": post.get("post_url"),
            "post_date": post.get("post_date"),
            "text": text,
        },
        "schema": {
            "decision": "accept|needs_review|reject",
            "whole_post_about_kaliningrad_oblast_score": "0..1",
            "kaliningrad_mention_role": "main_subject|one_item|passing_mention|footer|hashtag|link_only|unclear",
            "is_digest_or_roundup": "boolean",
            "is_multi_region_roundup": "boolean",
            "is_multi_topic_digest": "boolean",
            "is_single_location_card": "boolean",
            "is_author_visit_impression": "boolean",
            "is_official_route_material": "boolean",
            "is_photo_card_from_subscriber": "boolean",
            "is_ad_or_promo": "boolean",
            "is_news_or_trash": "boolean",
            "content_type": "visit_impression_candidate|route_useful_candidate|single_location_photo_card|encyclopedic_card_candidate|official_project_material|news_or_event|low_substance|reject",
            "visit_evidence_type": "firsthand_author_visit|subscriber_photo_report|route_guide|single_location_photo_card|official_project_material|news_or_event|unknown",
            "has_firsthand_visit_evidence": "boolean",
            "emotion_or_impression_evidence": "boolean",
            "review_or_opinion_evidence": "boolean",
            "memorable_detail_evidence": "boolean",
            "original_photo_evidence": "boolean",
            "reason": "short Russian explanation",
        },
    }
    return json.dumps(payload, ensure_ascii=False)


def parse_llm_json(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.I | re.S).strip()
    try:
        data = json.loads(raw)
    except Exception:
        match = re.search(r"\{.*\}", raw, flags=re.S)
        data = json.loads(match.group(0)) if match else {}
    return data if isinstance(data, dict) else {}


class _SupabaseRestResult:
    def __init__(self, data: Any):
        self.data = data


class _SupabaseRestQuery:
    def __init__(self, client: "_SupabaseRestClient", table: str):
        self.client = client
        self.table = table
        self.params: dict[str, str] = {}
        self.order_parts: list[str] = []

    def select(self, columns: str = "*") -> "_SupabaseRestQuery":
        self.params["select"] = columns
        return self

    def eq(self, column: str, value: Any) -> "_SupabaseRestQuery":
        self.params[column] = "eq." + str(value).lower() if isinstance(value, bool) else "eq." + str(value)
        return self

    def in_(self, column: str, values: list[Any]) -> "_SupabaseRestQuery":
        encoded = ",".join(str(v) for v in values)
        self.params[column] = f"in.({encoded})"
        return self

    def order(self, column: str) -> "_SupabaseRestQuery":
        self.order_parts.append(column)
        return self

    def limit(self, n: int) -> "_SupabaseRestQuery":
        self.params["limit"] = str(int(n))
        return self

    def execute(self) -> _SupabaseRestResult:
        params = dict(self.params)
        if self.order_parts:
            params["order"] = ",".join(self.order_parts)
        data = self.client._request("GET", f"/rest/v1/{self.table}", params=params)
        return _SupabaseRestResult(data)


class _SupabaseRestRpc:
    def __init__(self, client: "_SupabaseRestClient", fn_name: str, payload: dict[str, Any]):
        self.client = client
        self.fn_name = fn_name
        self.payload = payload

    def execute(self) -> _SupabaseRestResult:
        data = self.client._request("POST", f"/rest/v1/rpc/{self.fn_name}", json_body=self.payload)
        return _SupabaseRestResult(data)


class _SupabaseRestClient:
    def __init__(self, url: str, key: str, *, schema: str = "public") -> None:
        self.url = url.rstrip("/")
        self.key = key
        self.schema = schema or "public"

    def table(self, table: str) -> _SupabaseRestQuery:
        return _SupabaseRestQuery(self, table)

    def rpc(self, fn_name: str, payload: dict[str, Any]) -> _SupabaseRestRpc:
        return _SupabaseRestRpc(self, fn_name, payload)

    def _request(self, method: str, path: str, *, params: dict[str, str] | None = None, json_body: Any = None) -> Any:
        try:
            import requests  # type: ignore
        except Exception:
            if getenv_bool("REGION_TALK_AUTO_INSTALL", True):
                subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "requests"])
                import requests  # type: ignore
            else:
                raise
        headers = {
            "apikey": self.key,
            "Authorization": "Bearer " + self.key,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Accept-Profile": self.schema,
            "Content-Profile": self.schema,
        }
        resp = requests.request(method, self.url + path, headers=headers, params=params, json=json_body, timeout=30)
        if resp.status_code >= 400:
            raise RuntimeError(f"supabase_rest_{resp.status_code}: {resp.text[:500]}")
        if not resp.text.strip():
            return None
        return resp.json()


def build_region_talk_supabase_client() -> Any:
    global _REGION_TALK_SUPABASE_CLIENT
    if _REGION_TALK_SUPABASE_CLIENT is not None:
        return _REGION_TALK_SUPABASE_CLIENT
    if (os.getenv("SUPABASE_DISABLED") or "").strip() == "1":
        raise RuntimeError("supabase_disabled")
    base_url = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
    key = (os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY") or "").strip()
    if not base_url or not key:
        raise RuntimeError("missing_SUPABASE_URL_or_service_key")
    schema = (os.getenv("SUPABASE_SCHEMA") or "public").strip() or "public"
    _REGION_TALK_SUPABASE_CLIENT = _SupabaseRestClient(base_url, key, schema=schema)
    return _REGION_TALK_SUPABASE_CLIENT




def ensure_google_ai_import_path() -> None:
    candidates = [Path.cwd(), Path(__file__).resolve().parent, Path("/kaggle/working")]
    inp = Path("/kaggle/input")
    if inp.exists():
        candidates.extend(p.parent.parent for p in inp.rglob("google_ai/__init__.py"))
    for parent in candidates:
        if (parent / "google_ai" / "__init__.py").exists() and str(parent) not in sys.path:
            sys.path.insert(0, str(parent))

def get_region_talk_llm_gateway(default_env_var_name: str) -> Any:
    global _REGION_TALK_GOOGLE_CLIENT
    if _REGION_TALK_GOOGLE_CLIENT is not None:
        return _REGION_TALK_GOOGLE_CLIENT
    ensure_google_ai_import_path()
    try:
        from google_ai import GoogleAIClient, SecretsProvider  # type: ignore
    except Exception:
        raise RuntimeError("google_ai_gateway_package_missing")
    client = GoogleAIClient(
        supabase_client=build_region_talk_supabase_client(),
        secrets_provider=SecretsProvider(),
        consumer="region_talk_candidate_report",
        account_name=os.getenv("GOOGLE_API_LOCALNAME_REGION_TALK") or os.getenv("GOOGLE_API_LOCALNAME"),
        default_env_var_name=default_env_var_name or "GOOGLE_API_KEY",
    )
    client.allow_reserve_fallback = False
    client.allow_local_limiter_fallback = False
    client.allow_local_limiter_on_reserve_error = False
    _REGION_TALK_GOOGLE_CLIENT = client
    return client


def load_llm_limit_snapshot(model: str, default_env_var_name: str) -> dict[str, Any]:
    try:
        sb = build_region_talk_supabase_client()
        limits = sb.table("google_ai_model_limits").select("model,rpm,tpm,rpd,tpm_reserve_extra").eq("model", model).limit(1).execute().data or []
        keys = sb.table("google_ai_api_keys").select("id,env_var_name,key_alias,priority,is_active").eq("is_active", True).in_("env_var_name", [default_env_var_name]).order("priority").order("id").limit(5).execute().data or []
        return {
            "llm_limit_source": "supabase_google_ai",
            "llm_provider": "google_gemini",
            "llm_model": model,
            "llm_default_env_var_name": default_env_var_name,
            "supabase_limiter_model_found": str(bool(limits)).lower(),
            "supabase_scoped_key_found": str(bool(keys)).lower(),
            "supabase_model_rpm": (limits[0].get("rpm") if limits else ""),
            "supabase_model_tpm": (limits[0].get("tpm") if limits else ""),
            "supabase_model_rpd": (limits[0].get("rpd") if limits else ""),
            "llm_config_source": "supabase_google_ai_model_limits_and_google_ai_reserve",
        }
    except Exception as exc:
        return {
            "llm_limit_source": "supabase_required_unavailable",
            "llm_provider": "google_gemini",
            "llm_model": model,
            "llm_default_env_var_name": default_env_var_name,
            "supabase_limiter_model_found": "false",
            "supabase_scoped_key_found": "false",
            "llm_config_source": "supabase_required_but_failed",
            "llm_limit_error": f"{type(exc).__name__}: {str(exc)[:240]}",
        }


def call_region_talk_semantic_llm(post: dict[str, Any], evidence: dict[str, Any], *, model: str | None = None, default_env_var_name: str | None = None) -> dict[str, Any]:
    # Final semantic decision only. Budget/key selection must go through Supabase google_ai_reserve.
    model = (model or os.getenv("REGION_TALK_LLM_MODEL") or "gemini-3.1-flash-lite").strip()
    default_env_var_name = (default_env_var_name or os.getenv("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME") or "GOOGLE_API_KEY3").strip()
    try:
        from google_ai.exceptions import RateLimitError, ProviderError, ReservationError  # type: ignore
    except Exception:
        class RateLimitError(Exception): pass
        class ProviderError(Exception): pass
        class ReservationError(Exception): pass
    try:
        try:
            from google import genai as _genai  # noqa: F401
        except Exception:
            if getenv_bool("REGION_TALK_AUTO_INSTALL", True):
                subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "google-genai"])
        prompt = llm_text_gate_prompt(post, evidence)
        client = get_region_talk_llm_gateway(default_env_var_name)

        async def _call() -> tuple[str, Any]:
            return await client.generate_content_async(
                model=model,
                prompt=prompt,
                generation_config={"temperature": 0.1, "response_mime_type": "application/json"},
                max_output_tokens=700,
            )
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if running_loop is not None:
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                text, usage = pool.submit(lambda: asyncio.run(_call())).result()
        else:
            text, usage = asyncio.run(_call())
        data = parse_llm_json(text)
        decision = str(data.get("decision") or "needs_review").strip().lower()
        if decision not in {"accept", "needs_review", "reject"}:
            decision = "needs_review"
        return {
            "llm_gate_status": "ok",
            "llm_provider": "google_gemini",
            "llm_model": model,
            "llm_default_env_var_name": default_env_var_name,
            "llm_limit_source": "supabase_google_ai_reserve",
            "llm_decision": decision,
            "whole_post_about_kaliningrad_oblast_score": data.get("whole_post_about_kaliningrad_oblast_score", ""),
            "kaliningrad_mention_role": str(data.get("kaliningrad_mention_role") or "unclear"),
            "is_digest_or_roundup": str(bool(data.get("is_digest_or_roundup"))).lower(),
            "is_multi_region_roundup": str(bool(data.get("is_multi_region_roundup"))).lower(),
            "is_multi_topic_digest": str(bool(data.get("is_multi_topic_digest"))).lower(),
            "is_single_location_card": str(bool(data.get("is_single_location_card"))).lower(),
            "is_author_visit_impression": str(bool(data.get("is_author_visit_impression"))).lower(),
            "is_official_route_material": str(bool(data.get("is_official_route_material"))).lower(),
            "is_photo_card_from_subscriber": str(bool(data.get("is_photo_card_from_subscriber"))).lower(),
            "llm_is_ad_or_promo": str(bool(data.get("is_ad_or_promo"))).lower(),
            "llm_is_news_or_trash": str(bool(data.get("is_news_or_trash"))).lower(),
            "llm_content_type": str(data.get("content_type") or ""),
            "content_type": str(data.get("content_type") or ""),
            "visit_evidence_type": str(data.get("visit_evidence_type") or ""),
            "has_firsthand_visit_evidence": str(bool(data.get("has_firsthand_visit_evidence"))).lower(),
            "emotion_or_impression_evidence": str(bool(data.get("emotion_or_impression_evidence"))).lower(),
            "review_or_opinion_evidence": str(bool(data.get("review_or_opinion_evidence"))).lower(),
            "memorable_detail_evidence": str(bool(data.get("memorable_detail_evidence"))).lower(),
            "original_photo_evidence": str(bool(data.get("original_photo_evidence"))).lower(),
            "llm_reason": str(data.get("reason") or "")[:500],
            "llm_usage_input_tokens": getattr(usage, "input_tokens", ""),
            "llm_usage_output_tokens": getattr(usage, "output_tokens", ""),
            "llm_usage_total_tokens": getattr(usage, "total_tokens", ""),
        }
    except RateLimitError as exc:
        return {"llm_gate_status": "rate_limited", "llm_provider": "google_gemini", "llm_model": model, "llm_default_env_var_name": default_env_var_name, "llm_limit_source": "supabase_google_ai_reserve", "llm_reason": f"RateLimitError: {str(exc)[:240]}"}
    except (ProviderError, ReservationError) as exc:
        msg = str(exc)
        status = "rate_limited" if "429" in msg or "resource_exhausted" in msg.lower() or "rate limit" in msg.lower() else "error"
        return {"llm_gate_status": status, "llm_provider": "google_gemini", "llm_model": model, "llm_default_env_var_name": default_env_var_name, "llm_limit_source": "supabase_google_ai_reserve", "llm_reason": f"{type(exc).__name__}: {msg[:240]}"}
    except Exception as exc:
        return {"llm_gate_status": "error", "llm_provider": "google_gemini", "llm_model": model, "llm_default_env_var_name": default_env_var_name, "llm_limit_source": "supabase_google_ai_reserve", "llm_reason": f"{type(exc).__name__}: {str(exc)[:240]}"}


def should_send_to_llm(fresh: dict[str, Any], scope: dict[str, Any], post: dict[str, Any], *, ad_gate: dict[str, Any] | None = None, substance: dict[str, Any] | None = None, text_score: dict[str, Any] | None = None) -> bool:
    if not fresh.get("fresh_enough"):
        return False
    ad_gate = ad_gate or {}
    substance = substance or {}
    text_score = text_score or {}
    # Cost guard only: do not spend Supabase LLM quota on rows with no strong Kaliningrad-Oblast evidence.
    # The semantic decision for remaining rows is still owned by the LLM final gate.
    if not scope.get("kaliningrad_oblast_only_scope"):
        return False
    if not scope.get("matched_place_names") and not any(anchor.lower() in str(post.get("text") or "").lower() for anchor in DEFAULT_ANCHORS[:6]):
        return False
    if bool(ad_gate.get("is_ad_or_promo")) and float(substance.get("visit_impression_score") or 0) < 0.20:
        return False
    if float(substance.get("text_substance_score") or 0) < 0.18:
        return False
    if float(text_score.get("newsiness_score") or 0) >= 0.70 or float(text_score.get("trash_score") or 0) >= 0.70:
        return False
    return True


def text_vector_gate(text: str, ts: dict[str, Any], scope: dict[str, Any], ad_gate: dict[str, Any], substance: dict[str, Any]) -> dict[str, Any]:
    """Local-first text gate.

    MVP implementation uses deterministic/prototype score fusion and records the
    dual-model embedding target. Kaggle can later replace the prototype fallback
    with real intfloat/multilingual-e5-base + BAAI/bge-m3 embeddings without
    changing the row contract.
    """
    low = (text or "").lower()
    positive = max(float(substance.get("visit_impression_score") or 0), float(substance.get("useful_route_score") or 0), float(substance.get("emotion_observation_score") or 0))
    if scope.get("kaliningrad_oblast_only_scope"):
        positive += 0.25
    if scope.get("matched_place_names"):
        positive += 0.10
    news_event = float(ts.get("newsiness_score") or 0)
    if any(w in low for w in ["анонс", "мероприят", "конкурс", "диктант", "регистрац", "заявк", "афиша", "билет", "расписан"]):
        news_event += 0.45
    ad_promo = float(ts.get("ad_score") or 0)
    if bool(ad_gate.get("is_ad_or_promo")):
        ad_promo += 0.50
    roundup = 0.60 if scope.get("external_geo_mentions") else 0.0
    if any(w in low for w in ["подборка", "топ-", "топ ", "куда поехать", "мест россии", "регионов россии", "направлений"]):
        roundup += 0.25
    low_substance = max(0.0, 0.65 - float(substance.get("text_substance_score") or 0))
    negative = max(news_event, ad_promo, roundup, low_substance)
    margin = round(max(0.0, min(1.0, positive)) - max(0.0, min(1.0, negative)), 3)
    status = "vector_ambiguous_keep_for_ranking"
    reason = ""
    content_type = "visit_impression_candidate" if float(substance.get("visit_impression_score") or 0) >= 0.20 else ("route_useful_candidate" if float(substance.get("useful_route_score") or 0) >= 0.18 else "single_location_photo_card" if scope.get("matched_place_names") else "low_substance")
    if news_event >= 0.55 and positive < 0.55:
        status, reason, content_type = "vector_reject_news_event", "news/event/announcement prototype score dominates", "news_or_event"
    elif ad_promo >= 0.55 and positive < 0.60:
        status, reason, content_type = "vector_reject_ad_promo", "ad/promo prototype score dominates", "ad_or_promo"
    elif roundup >= 0.60 and positive < 0.60:
        status, reason, content_type = "vector_reject_roundup", "multi-region/SEO roundup prototype score dominates", "multi_region_roundup"
    elif low_substance >= 0.55 and positive < 0.40:
        status, reason, content_type = "vector_reject_low_substance", "low-substance prototype score dominates", "low_substance"
    elif positive >= 0.55 and margin >= -0.15:
        status, reason = "vector_accept_candidate", "positive visit/route/Kaliningrad signal kept for local image ranking"
    return {
        "text_embedding_model_id": "dual_model_target:intfloat/multilingual-e5-base+BAAI/bge-m3;prototype_fallback_v1",
        "text_embedding_runtime": "kaggle_local_prototype_vector_gate",
        "vector_gate_status": status,
        "vector_content_type": content_type,
        "vector_positive_score": round(max(0.0, min(1.0, positive)), 3),
        "vector_news_event_score": round(max(0.0, min(1.0, news_event)), 3),
        "vector_ad_promo_score": round(max(0.0, min(1.0, ad_promo)), 3),
        "vector_roundup_score": round(max(0.0, min(1.0, roundup)), 3),
        "vector_low_substance_score": round(max(0.0, min(1.0, low_substance)), 3),
        "vector_visit_impression_score": round(float(substance.get("visit_impression_score") or 0), 3),
        "vector_route_useful_score": round(float(substance.get("useful_route_score") or 0), 3),
        "vector_emotion_observation_score": round(float(substance.get("emotion_observation_score") or 0), 3),
        "vector_margin_positive_vs_negative": margin,
        "vector_rejection_reason": reason if status.startswith("vector_reject") else "",
        "vector_gate_confidence": round(abs(margin), 3),
        "needs_llm_final_verify": str(status in {"vector_accept_candidate", "vector_ambiguous_keep_for_ranking"}).lower(),
        "llm_not_called_reason": "wide_funnel_vector_gate_only",
        "llm_stage": "",
        "llm_status": "not_called_until_final_verifier" if status in {"vector_accept_candidate", "vector_ambiguous_keep_for_ranking"} else "not_called_vector_reject",
    }


def build_similar_seed_queue(previous_state: dict[str, Any], source_rows: list[dict[str, Any]], candidate_memory_rows: list[dict[str, Any]], run_id: str, run_now: str) -> list[dict[str, Any]]:
    prev = previous_state.get("similar_seed_queue") if isinstance(previous_state.get("similar_seed_queue"), dict) else {}
    updates = _REGION_TALK_TELEGRAM_RUNTIME.get("similar_seed_updates") if isinstance(_REGION_TALK_TELEGRAM_RUNTIME.get("similar_seed_updates"), dict) else {}
    candidate_source_ids = {str(r.get("source_id") or "") for r in candidate_memory_rows if r.get("source_id")}
    out: dict[str, dict[str, Any]] = {str(k): dict(v) for k, v in (prev or {}).items() if isinstance(v, dict)}
    for s in source_rows:
        if str(s.get("platform") or "") != "telegram":
            continue
        if s.get("fetch_status") != "ok" and s.get("telegram_resolve_status") not in {"resolved_network", "resolved_from_private_cache"}:
            continue
        url = str(s.get("canonical_url") or "")
        if not url:
            continue
        key = "similar_seed_" + stable_hash(url)
        old = out.get(key) or {}
        priority = float(s.get("monitor_priority_score") or 0)
        if str(s.get("source_id") or "") in candidate_source_ids:
            priority += 0.35
        if str(s.get("source_kind") or "").lower().find("travel") >= 0 or str(s.get("source_type") or "").lower().find("travel") >= 0:
            priority += 0.10
        upd = updates.get(key) or updates.get(url) or {}
        out[key] = {
            **old,
            "similar_seed_id": key,
            "source_id": s.get("source_id"),
            "canonical_url": url,
            "source_title": s.get("source_title") or s.get("resolved_title"),
            "similar_seed_status": "ready",
            "similar_seed_first_seen_at": old.get("similar_seed_first_seen_at") or run_now,
            "similar_seed_last_seen_at": run_now,
            "similar_seed_last_used_at": upd.get("similar_last_used_at") or old.get("similar_seed_last_used_at", ""),
            "similar_seed_use_count": int(old.get("similar_seed_use_count") or 0) + int(upd.get("similar_use_count_increment") or 0),
            "similar_seed_error_count": int(old.get("similar_seed_error_count") or 0) + int(upd.get("similar_error_count_increment") or 0),
            "similar_seed_last_result_count": upd.get("similar_last_result_count", old.get("similar_seed_last_result_count", "")),
            "similar_seed_last_unique_count": upd.get("similar_last_unique_count", old.get("similar_seed_last_unique_count", "")),
            "similar_last_scanned_at": upd.get("similar_last_scanned_at") or old.get("similar_last_scanned_at", ""),
            "similar_seed_next_allowed_at": upd.get("similar_next_allowed_at") or old.get("similar_seed_next_allowed_at", ""),
            "similar_seed_priority": round(min(1.0, priority), 3),
            "last_run_id": run_id,
            "no_auto_join": "true",
        }
    return sorted(out.values(), key=lambda r: (-float(r.get("similar_seed_priority") or 0), str(r.get("similar_seed_last_used_at") or ""), str(r.get("canonical_url") or "")))


def build_source_delta_scan_sheet(source_rows: list[dict[str, Any]], previous_state: dict[str, Any], run_id: str) -> list[dict[str, Any]]:
    prev_cursors = previous_state.get("source_cursors") if isinstance(previous_state.get("source_cursors"), dict) else {}
    rows: list[dict[str, Any]] = []
    for s in source_rows:
        sid = str(s.get("source_id") or "")
        prev = prev_cursors.get(sid) if isinstance(prev_cursors, dict) else {}
        rows.append({
            "source_id": sid,
            "source_title": s.get("source_title") or s.get("resolved_title"),
            "platform": s.get("platform"),
            "fetch_status": s.get("fetch_status"),
            "history_fetch_mode": s.get("history_fetch_mode") or ("skipped" if str(s.get("fetch_status") or "").startswith("skipped") else ""),
            "is_new_source_this_run": s.get("is_new_source_this_run", "false"),
            "last_history_fetch_run_id_previous": (prev or {}).get("last_history_fetch_run_id", ""),
            "last_history_fetch_run_id": run_id if s.get("fetch_status") == "ok" else "",
            "last_history_fetch_at_previous": (prev or {}).get("last_history_fetch_at", ""),
            "last_seen_post_date_previous": (prev or {}).get("last_seen_post_date", ""),
            "last_seen_post_date": s.get("last_seen_post_date", ""),
            "posts_seen_total_previous": (prev or {}).get("posts_seen_total", ""),
            "posts_scanned_this_run": s.get("posts_scanned", 0),
            "kaliningrad_posts_seen_total_previous": (prev or {}).get("kaliningrad_posts_seen_total", ""),
            "candidate_posts_seen_total_previous": (prev or {}).get("candidate_posts_seen_total", ""),
            "last_candidate_seen_at_previous": (prev or {}).get("last_candidate_seen_at", ""),
            "source_delta_strategy": "since_last_scan_plus_anchor_overlap" if (prev or {}).get("last_history_fetch_at") else "first_probe_or_shallow_backfill",
        })
    return rows or [{"_sheet_note": "no source rows"}]


def build_source_yield_metrics(source_rows: list[dict[str, Any]], posts_by_source: dict[str, list[dict[str, Any]]], new_posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scanned = [s for s in source_rows if s.get("fetch_status") == "ok"]
    source_ids = [str(s.get("source_id") or "") for s in scanned]
    source_sets = {
        "sources_scanned_total_this_run": set(source_ids),
        "sources_scanned_new_this_run": {str(s.get("source_id") or "") for s in scanned if str(s.get("is_new_source_this_run") or "").lower() == "true"},
        "sources_with_any_ko_post": {sid for sid, rows in posts_by_source.items() if any(r.get("kaliningrad_oblast_only_scope") for r in rows)},
        "sources_with_fresh_ko_post": {sid for sid, rows in posts_by_source.items() if any(r.get("kaliningrad_oblast_only_scope") and r.get("fresh_enough") for r in rows)},
        "sources_with_non_ad_ko_post": {sid for sid, rows in posts_by_source.items() if any(r.get("kaliningrad_oblast_only_scope") and not r.get("is_ad_or_promo") for r in rows)},
        "sources_with_candidate_memory_post": {str(r.get("source_id") or "") for r in new_posts if r.get("current_stage") in CANDIDATE_MEMORY_STAGES},
        "sources_with_actual_image_candidate": {str(r.get("source_id") or "") for r in new_posts if r.get("image_model_input_type") == "actual_image" and r.get("current_stage") in CANDIDATE_MEMORY_STAGES},
        "sources_with_publication_ready_candidate": {str(r.get("source_id") or "") for r in new_posts if str(r.get("image_publication_ready")) == "true"},
    }
    denom = max(1, len(scanned))
    out = []
    for metric, vals in source_sets.items():
        count = len(vals)
        out.append({"metric": metric, "count": count, "per_1000_scanned_sources": round(count / denom * 1000, 2), "denominator_scanned_sources": len(scanned), "sample_bias_note": "biased by priority/frontier queue; not a random 1000-source sample"})
    return out


def build_all_time_metrics(state_to_write: dict[str, Any], source_frontier_unique: list[dict[str, Any]], candidate_memory_rows: list[dict[str, Any]]) -> dict[str, Any]:
    cursors = state_to_write.get("source_cursors") if isinstance(state_to_write.get("source_cursors"), dict) else {}
    posts_mem = state_to_write.get("posts") if isinstance(state_to_write.get("posts"), dict) else {}
    frontier_by_platform: dict[str, int] = {}
    for r in source_frontier_unique:
        p = str(r.get("platform") or r.get("platform_guess") or "unknown")
        frontier_by_platform[p] = frontier_by_platform.get(p, 0) + 1
    primary = [v for v in cursors.values() if isinstance(v, dict) and (v.get("primary_scan_completed_at") or int(v.get("posts_seen_total") or 0) > 0)]
    delta = [v for v in cursors.values() if isinstance(v, dict) and int(v.get("delta_scan_count_total") or 0) > 0]
    telegram_primary = [v for v in primary if str(v.get("platform") or v.get("source_id") or "").startswith("telegram") or str(v.get("source_id") or "").startswith("src_telegram")]
    vk_primary = [v for v in primary if str(v.get("platform") or "").startswith("vk")]
    scanned_keys = {str(v.get("source_id") or "") for v in primary}
    pending_primary = sum(1 for r in source_frontier_unique if str(r.get("frontier_source_id") or r.get("source_id") or "") not in scanned_keys and str(r.get("frontier_stage") or "") in {"history_due", "probe_due", "unresolved"})
    pending_similar = sum(1 for r in source_frontier_unique if not r.get("similar_last_scanned_at") and str(r.get("platform") or r.get("platform_guess") or "").startswith("telegram"))
    return {
        "sources_primary_scanned_total_all_time": len(primary),
        "telegram_sources_primary_scanned_total_all_time": len(telegram_primary),
        "vk_sources_primary_scanned_total_all_time": len(vk_primary),
        "sources_delta_scanned_total_all_time": len(delta),
        "sources_never_scanned_total": pending_primary,
        "frontier_total_by_platform": json.dumps(frontier_by_platform, ensure_ascii=False, sort_keys=True),
        "frontier_pending_primary_scan_total": pending_primary,
        "frontier_pending_similar_scan_total": pending_similar,
        "posts_memory_total": len(posts_mem),
        "candidates_memory_total": len(candidate_memory_rows),
        "publication_ready_total_all_time": sum(1 for r in candidate_memory_rows if str(r.get("image_publication_ready") or "") == "true"),
    }


def build_report(seeds: list[Seed], source_rows: list[dict[str, Any]], posts: list[dict[str, Any]], run_id: str, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    run_now = utc_now_iso()
    lexicon_path = find_place_lexicon_file({})
    lexicon = load_place_lexicon(lexicon_path)
    source_seed_rows = [{**asdict(s), "source_id": s.source_id, "canonical_url": s.canonical_url} for s in seeds]
    candidates: list[dict[str, Any]] = []
    media_rows: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    new_posts: list[dict[str, Any]] = []
    increment: list[dict[str, Any]] = []
    discovered_rows: list[dict[str, Any]] = []
    graph_edges: list[dict[str, Any]] = []
    place_match_rows: list[dict[str, Any]] = []
    seed_by_id = {s.source_seed_id: s for s in seeds}
    pre_candidates: list[dict[str, Any]] = []
    llm_model = (os.getenv("REGION_TALK_LLM_MODEL") or "gemini-3.1-flash-lite").strip()
    llm_default_env_var_name = (os.getenv("REGION_TALK_LLM_DEFAULT_ENV_VAR_NAME") or "GOOGLE_API_KEY3").strip()
    llm_limit_snapshot = load_llm_limit_snapshot(llm_model, llm_default_env_var_name)
    llm_calls_used = 0
    llm_supabase_unavailable = llm_limit_snapshot.get("llm_limit_source") == "supabase_required_unavailable"
    previous_state, state_meta = load_region_talk_state(output_dir)
    previous_posts = previous_state.get("posts") if isinstance(previous_state.get("posts"), dict) else {}
    previous_discovered = previous_state.get("discovered_sources") if isinstance(previous_state.get("discovered_sources"), dict) else {}
    updated_posts_state: dict[str, Any] = dict(previous_posts)
    updated_discovered_state: dict[str, Any] = dict(previous_discovered)

    for p in posts:
        seed_for_post = seed_by_id.get(str(p.get('source_seed_id') or '')) or (seeds[0] if seeds else None)
        text = p.get("text") or ""
        ts = score_text(text)
        scope = kaliningrad_oblast_only_scope_gate(text, lexicon)
        fresh = freshness_gate(p.get("post_date"))
        ad_gate = ad_promo_gate(text)
        substance = score_substance(text)
        link_rows, edge_rows = discover_links_for_post(p, run_id)
        discovered_rows.extend(link_rows)
        graph_edges.extend(edge_rows)

        cid = "cand_" + stable_hash(p["post_id"], "region-talk-semantic-bank-v1", text[:120])
        mid = "media_" + stable_hash(p["post_id"], p.get("post_url"), "media0")
        drop_gate = ""
        rejection = ""
        visual_stage = "skipped_by_text_gate"
        visual_skip_reason = ""
        image_cost_saved = True
        current_stage = ""
        gate_trace: list[str] = []

        semantic_gate_mode = (os.getenv("REGION_TALK_SEMANTIC_GATE_MODE") or "vector_first_final_llm").strip().lower()
        early_llm_enabled = getenv_bool("REGION_TALK_ENABLE_EARLY_LLM", False)
        vector_gates_enabled = getenv_bool("REGION_TALK_ENABLE_VECTOR_GATES", True)
        deterministic_override = getenv_bool("REGION_TALK_ALLOW_DETERMINISTIC_SEMANTIC_GATES", False)
        if not fresh["fresh_enough"]:
            drop_gate, rejection = "freshness_gate", "reject_stale_or_missing_date"
            visual_skip_reason = fresh["freshness_reason"]
            gate_trace.append("freshness_gate:reject")
        else:
            gate_trace.append("freshness_gate:pass")

        # LLM-first policy: regex/keyword/lexicon checks below are evidence and recall routing only.
        semantic_evidence_flags: list[str] = []
        if not scope["kaliningrad_oblast_only_scope"]:
            semantic_evidence_flags.append("deterministic_scope_evidence_not_oblast_only")
        if ad_gate["is_ad_or_promo"]:
            semantic_evidence_flags.append("deterministic_ad_promo_evidence")
        if substance["text_substance_score"] < 0.25:
            semantic_evidence_flags.append("deterministic_low_substance_evidence")
        if ts["newsiness_score"] >= 0.45:
            semantic_evidence_flags.append("deterministic_news_evidence")
        if ts["trash_score"] >= 0.35:
            semantic_evidence_flags.append("deterministic_trash_evidence")
        gate_trace.append("kaliningrad_oblast_only_scope_gate:evidence_only")
        gate_trace.append("ad_promo_announcement_gate:evidence_only")
        gate_trace.append("content_substance_visit_impression_gate:evidence_only")
        gate_trace.append("not_news_not_trash_gate:evidence_only")
        vector_gate = text_vector_gate(text, ts, scope, ad_gate, substance) if vector_gates_enabled else {
            "text_embedding_model_id": "", "text_embedding_runtime": "disabled", "vector_gate_status": "disabled",
            "vector_content_type": "", "vector_positive_score": "", "vector_news_event_score": "", "vector_ad_promo_score": "",
            "vector_roundup_score": "", "vector_low_substance_score": "", "vector_visit_impression_score": "",
            "vector_route_useful_score": "", "vector_emotion_observation_score": "", "vector_margin_positive_vs_negative": "",
            "vector_rejection_reason": "", "vector_gate_confidence": "", "needs_llm_final_verify": "false",
            "llm_not_called_reason": "vector_gates_disabled", "llm_stage": "", "llm_status": "not_called_vector_disabled",
        }
        gate_trace.append("news_event_vector_gate:" + str(vector_gate.get("vector_gate_status") or "not_run"))

        llm_gate = {
            "llm_gate_status": "not_run", "llm_provider": "google_gemini", "llm_model": llm_model, "llm_default_env_var_name": llm_default_env_var_name, "llm_limit_source": llm_limit_snapshot.get("llm_limit_source", "supabase_google_ai"), "llm_decision": "",
            "whole_post_about_kaliningrad_oblast_score": round(0.65 if scope.get("kaliningrad_oblast_only_scope") else 0.15, 2),
            "kaliningrad_mention_role": "main_subject" if scope.get("kaliningrad_oblast_only_scope") else ("one_item" if scope.get("matched_place_names") else "unclear"),
            "is_digest_or_roundup": str(float(ts.get("newsiness_score") or 0) >= 0.45 or bool(scope.get("external_geo_mentions"))).lower(),
            "is_multi_region_roundup": str(bool(scope.get("external_geo_mentions"))).lower(),
            "is_multi_topic_digest": str(bool(scope.get("external_geo_mentions"))).lower(),
            "is_single_location_card": str(bool(scope.get("matched_place_names")) and not bool(scope.get("external_geo_mentions"))).lower(),
            "is_author_visit_impression": str(float(substance.get("visit_impression_score") or 0) >= 0.25).lower(),
            "is_official_route_material": "false", "is_photo_card_from_subscriber": "false",
            "llm_is_ad_or_promo": "", "llm_is_news_or_trash": "",
            "llm_content_type": str(vector_gate.get("vector_content_type") or ""),
            "content_type": str(vector_gate.get("vector_content_type") or ("single_location_photo_card" if scope.get("matched_place_names") else "low_substance")),
            "visit_evidence_type": "", "has_firsthand_visit_evidence": "", "emotion_or_impression_evidence": "",
            "review_or_opinion_evidence": "", "memorable_detail_evidence": "", "original_photo_evidence": "",
            "llm_reason": "",
            "llm_usage_input_tokens": "", "llm_usage_output_tokens": "", "llm_usage_total_tokens": "",
            "rejection_reason_primary": "", "rejection_reason_details": "",
        }
        if not rejection and not scope.get("kaliningrad_oblast_only_scope"):
            drop_gate = "hard_region_gate"
            rejection = "reject_not_kaliningrad_oblast_only"
            visual_skip_reason = str(scope.get("region_scope_reason") or "not Kaliningrad Oblast main subject")
            current_stage = "dropped_text_gate"
            vector_gate["needs_llm_final_verify"] = "false"
            vector_gate["llm_status"] = "not_called_hard_region_reject"
            vector_gate["llm_not_called_reason"] = "hard_region_gate_reject"
            gate_trace.append("hard_region_gate:reject_not_kaliningrad_oblast_only")
        elif not rejection and bool(ad_gate.get("is_ad_or_promo")) and float(substance.get("visit_impression_score") or 0) < 0.25:
            drop_gate = "ad_promo_announcement_gate"
            rejection = "reject_ad_or_promo"
            visual_skip_reason = str(ad_gate.get("ad_promo_hits") or "ad/promo/event deterministic evidence")
            current_stage = "dropped_text_gate"
        elif not rejection and str(vector_gate.get("vector_gate_status") or "").startswith("vector_reject"):
            drop_gate = "news_event_vector_gate"
            rejection = str(vector_gate.get("vector_gate_status") or "vector_reject")
            visual_skip_reason = str(vector_gate.get("vector_rejection_reason") or rejection)
            current_stage = "dropped_text_gate"
        llm_required = early_llm_enabled and semantic_gate_mode in {"llm_required", "llm", "semantic_required", "early_llm"} and not deterministic_override
        if not rejection and llm_required:
            if llm_supabase_unavailable:
                llm_gate["llm_gate_status"] = "not_run_supabase_limiter_unavailable"
                llm_gate["llm_reason"] = str(llm_limit_snapshot.get("llm_limit_error") or "Supabase limiter is required and unavailable")
                gate_trace.append("llm_semantic_gate:not_run_supabase_limiter_unavailable")
            elif should_send_to_llm(fresh, scope, p, ad_gate=ad_gate, substance=substance, text_score=ts):
                evidence = {
                    "matched_place_names": scope.get("matched_place_names"),
                    "external_geo_mentions": scope.get("external_geo_mentions"),
                    "ad_promo_hits": ad_gate.get("ad_promo_hits"),
                    "semantic_evidence_flags": semantic_evidence_flags,
                    "text_substance_score": substance.get("text_substance_score"),
                    "visit_impression_score": substance.get("visit_impression_score"),
                    "newsiness_score": ts.get("newsiness_score"),
                    "trash_score": ts.get("trash_score"),
                }
                llm_gate = {**llm_gate, **call_region_talk_semantic_llm(p, evidence, model=llm_model, default_env_var_name=llm_default_env_var_name)}
                if llm_gate.get("llm_gate_status") in {"ok", "error", "rate_limited"}:
                    llm_calls_used += 1
                gate_trace.append("llm_semantic_gate:" + str(llm_gate.get("llm_gate_status")))
            else:
                llm_gate["llm_gate_status"] = "not_run_pre_llm_cost_guard"
                gate_trace.append("llm_semantic_gate:not_run_pre_llm_cost_guard")

            if llm_gate.get("llm_gate_status") == "ok":
                decision = str(llm_gate.get("llm_decision") or "needs_review")
                # Guardrail for the observed z3b regression: a single-location Kaliningrad Oblast card is not a multi-region roundup.
                if decision == "reject" and scope.get("kaliningrad_oblast_only_scope") and not scope.get("external_geo_mentions"):
                    reason_l = str(llm_gate.get("llm_reason") or "").lower()
                    if "дайджест" in reason_l or "roundup" in reason_l or "подбор" in reason_l:
                        decision = "accept"
                        llm_gate["llm_decision"] = "accept"
                        llm_gate["content_type"] = llm_gate.get("content_type") or "single_location_photo_card"
                        llm_gate["llm_content_type"] = llm_gate.get("llm_content_type") or "single_location_photo_card"
                        llm_gate["is_single_location_card"] = "true"
                        llm_gate["is_multi_region_roundup"] = "false"
                        llm_gate["llm_reason"] = "Guardrail: single-location Kaliningrad Oblast card kept for research/manual review; not a multi-region roundup. " + str(llm_gate.get("llm_reason") or "")[:400]
                if decision == "reject":
                    drop_gate, rejection = "llm_semantic_gate", "llm_reject"
                    visual_skip_reason = str(llm_gate.get("llm_reason") or "LLM rejected semantic fit")
                    current_stage = "dropped_text_gate"
                elif str(llm_gate.get("llm_content_type") or llm_gate.get("content_type") or "") == "news_or_event" or str(llm_gate.get("llm_is_news_or_trash") or "").lower() == "true":
                    drop_gate, rejection = "llm_semantic_gate", "reject_news_or_event"
                    visual_skip_reason = str(llm_gate.get("llm_reason") or "LLM classified this as news/event rather than a visit/impression post")
                    current_stage = "dropped_text_gate"
                    llm_gate["llm_decision"] = "reject"
                    gate_trace.append("llm_semantic_gate:reject_news_or_event")
                elif decision == "needs_review":
                    drop_gate, rejection = "llm_semantic_gate", "llm_needs_review"
                    visual_skip_reason = str(llm_gate.get("llm_reason") or "LLM requested manual semantic review before image scoring")
                    current_stage = "pre_candidate_needs_llm"
                else:
                    gate_trace.append("llm_semantic_gate:accept")
            elif llm_gate.get("llm_gate_status") in {"rate_limited", "error"}:
                drop_gate, rejection = "llm_semantic_gate", "llm_retry_required"
                visual_skip_reason = str(llm_gate.get("llm_reason") or "LLM failed/rate-limited; retry required before image scoring")
                current_stage = "needs_llm_retry"
            elif llm_gate.get("llm_gate_status") == "not_run_pre_llm_cost_guard":
                precise_reason = classify_pre_llm_reject_reason(fresh, scope, ad_gate, substance, ts)
                drop_gate, rejection = "pre_llm_cost_guard", precise_reason
                visual_skip_reason = "Skipped before LLM to avoid spending Supabase quota on obvious non-region/ad/low-substance rows"
                current_stage = "debug_reject"
            else:
                drop_gate, rejection = "llm_semantic_gate", "semantic_gate_not_run"
                visual_skip_reason = "LLM semantic gate not run; row remains reviewable pre-candidate and image scoring is skipped"
                current_stage = "pre_candidate_needs_llm"
        elif not rejection and deterministic_override:
            current_stage = "pre_candidate_debug_deterministic"
            gate_trace.append("deterministic_semantic_override:pre_candidate_only")
        elif not rejection:
            llm_gate["llm_gate_status"] = "not_run_vector_first"
            llm_gate["llm_reason"] = "Early/broad LLM disabled; vector/local gates and image scoring run before optional final verifier"
            gate_trace.append("early_llm:disabled_vector_first")

        if rejection and current_stage in {"pre_candidate_needs_llm", "needs_llm_retry", "pre_candidate_debug_deterministic"}:
            ms = image_scores_skipped(visual_skip_reason or rejection)
            score = 0.0
        elif rejection:
            ms = image_scores_skipped(visual_skip_reason or rejection)
            score = 0.0
            current_stage = "dropped_text_gate"
        else:
            gate_trace.append("semantic_dual_model_enrichment:vector_gate_passed")
            visual_stage = "scored_after_vector_text_gates_before_final_llm"
            visual_skip_reason = ""
            image_cost_saved = False
            ms = media_scores(bool(p.get("has_media")), ts, p)
            initial_image_status_fields = normalize_image_status(current_stage, ms)
            if initial_image_status_fields.get("image_status") == "needs_actual_image_fetch":
                ms = {**ms, **initial_image_status_fields}
            media_rows.append({"media_id": mid, "candidate_id": cid, "post_url": p.get("post_url"), "image_url_or_local_path": p.get("primary_media_path") or ((p.get("post_url") + "#media") if p.get("has_media") else ""), "thumbnail":"", **ms, **initial_image_status_fields})
            score = candidate_score(ts, ms, seed_for_post) if seed_for_post else 0.0
            image_status_fields = normalize_image_status(current_stage, ms)
            if image_status_fields.get("current_stage") == "image_fetch_retry_needed":
                current_stage, rejection, drop_gate = "image_fetch_retry_needed", "needs_actual_image_fetch", "image_fetch_gate"
                gate_trace.append("image_fetch_gate:metadata_only_pending")
            elif not ms["is_selected_for_publication"]:
                if str(ms.get("image_reviewable")) == "true":
                    current_stage, rejection, drop_gate = "needs_image_review", ms["failure_reason"], "image_postcardness_gate"
                    gate_trace.append("image_postcardness_gate:reviewable")
                else:
                    current_stage, rejection, drop_gate = "good_text_weak_media", ms["failure_reason"], "image_postcardness_gate"
                    gate_trace.append("image_postcardness_gate:weak")
            elif score >= 0.45:
                current_stage = "favorite" if score >= 0.62 else "semantic_candidate"
                gate_trace.append("image_postcardness_gate:pass")
            else:
                current_stage, rejection, drop_gate = "low_substance_but_region_relevant", "candidate_score_low", "candidate_score_gate"
                gate_trace.append("candidate_score_gate:review")

        for m in scope.get("place_matches") or []:
            place_match_rows.append({
                "post_id": p.get("post_id"), "post_url": p.get("post_url"),
                "matched_place_name": m.get("matched_place_name"), "canonical_name": m.get("canonical_name"),
                "place_type": m.get("place_type"), "municipality": m.get("municipality"),
                "priority_tier": m.get("priority_tier"), "alias_used": m.get("alias_used"),
                "match_context": m.get("match_context"), "requires_context": m.get("requires_context"),
                "accepted_as_region_evidence": m.get("accepted_as_region_evidence"),
                "match_context_short": m.get("match_context_short"),
            })

        semantic_visit_fields = infer_visit_semantic_fields(text, llm_gate, substance, p)
        image_status_fields = normalize_image_status(current_stage, ms)
        row = {
            **p, **ts, **scope, **fresh, **ad_gate, **substance, **ms, **semantic_visit_fields, **image_status_fields,
            "candidate_id": cid, "candidate_score": score, "current_stage": current_stage,
            "drop_gate": drop_gate, "rejection_reason": rejection,
            "short_summary": make_summary(text),
            "why_this_is_about_kaliningrad": scope["region_scope_reason"],
            "what_positive": ", ".join(ts.get("positive_hits") or []),
            "what_neutral_or_useful": "route/place/travel context" if substance["text_substance_score"] >= 0.25 else "",
            "what_concern": rejection or "text gates passed; image checked",
            "image_model_report_short": ms["model_short_explanation"],
            "risk_flags": "; ".join([rejection] if rejection else ([] if p.get("rights_policy") != "unknown" else ["rights_unknown"])),
            "suggested_action": (image_status_fields.get("next_action") or ("manual_review" if current_stage in {"favorite","semantic_candidate", "pre_candidate_needs_llm", "needs_llm_retry", "needs_image_review", "image_fetch_retry_needed", "low_substance_but_region_relevant", "pre_candidate_debug_deterministic"} else "reject")),
            "manual_decision":"", "reviewer_comment":"",
            "region_scope_gate": "pass" if scope["kaliningrad_oblast_only_scope"] else "fail",
            "visual_scoring_stage": visual_stage,
            "visual_scoring_skip_reason": visual_skip_reason,
            "image_scoring_cost_saved": str(image_cost_saved).lower(),
            "image_scoring_skipped": str(image_cost_saved).lower(),
            "discovery_edges_count": len(edge_rows),
            "gate_order_trace": " → ".join(gate_trace),
            "semantic_evidence_flags": "; ".join(semantic_evidence_flags),
            **vector_gate,
            **llm_gate,
            "rejection_reason_primary": rejection or "",
            "rejection_reason_details": visual_skip_reason or str(llm_gate.get("llm_reason") or ""),
            "semantic_gate_mode": semantic_gate_mode,
            "deterministic_semantic_gate_override": str(deterministic_override).lower(),
            "semantic_enrichment_stage": "final_llm_verifier_pending" if str(vector_gate.get("needs_llm_final_verify")) == "true" and current_stage in {"semantic_candidate", "favorite", "needs_image_review", "image_fetch_retry_needed", "good_text_weak_media", "low_substance_but_region_relevant"} else ("dual_model_vector_enrichment_pending" if current_stage in {"semantic_candidate", "favorite", "needs_image_review", "low_substance_but_region_relevant"} else "skipped_by_text_gate"),
        }
        row.pop("place_matches", None)
        new_posts.append(row)
        previous_post = previous_posts.get(str(p["post_id"])) if isinstance(previous_posts, dict) else None
        previous_stage = str((previous_post or {}).get("current_stage") or "")
        previous_score = (previous_post or {}).get("candidate_score_current", "")
        previous_media = (previous_post or {}).get("media_score_current", "")
        seen_count = int((previous_post or {}).get("seen_run_count") or 0) + 1
        first_seen_run = str((previous_post or {}).get("first_seen_run_id") or run_id)
        changed = (not previous_post) or previous_stage != current_stage or str(previous_score) != str(score) or str(previous_media) != str(ms["overall_media_score"])
        score_delta = ""
        media_delta = ""
        try:
            score_delta = round(float(score) - float(previous_score), 3) if previous_score != "" else ""
        except Exception:
            score_delta = ""
        try:
            media_delta = round(float(ms["overall_media_score"]) - float(previous_media), 3) if previous_media != "" else ""
        except Exception:
            media_delta = ""
        increment.append({"entity_type":"post", "entity_id":p["post_id"], "source_title":p.get("source_title"), "post_url":p.get("post_url"), "first_seen_run_id":first_seen_run, "previous_run_id":str((previous_post or {}).get("last_seen_run_id") or ""), "current_run_id":run_id, "first_seen_at":str((previous_post or {}).get("first_seen_at") or run_now), "last_seen_at":run_now, "seen_run_count":seen_count, "previous_stage":previous_stage, "current_stage":current_stage, "stage_transition":("new→"+current_stage if not previous_post else previous_stage+"→"+current_stage), "new_this_run":"yes" if not previous_post else "no", "changed_this_run":str(changed).lower(), "change_reason":rejection or ("first_seen" if not previous_post else ("stage_or_score_changed" if changed else "unchanged")), "candidate_score_previous":previous_score, "candidate_score_current":score, "candidate_score_delta":score_delta, "media_score_previous":previous_media, "media_score_current":ms["overall_media_score"], "media_score_delta":media_delta, "manual_review_status":"unreviewed", "next_action":row["suggested_action"]})
        updated_posts_state[str(p["post_id"])] = {
            "first_seen_run_id": first_seen_run,
            "first_seen_at": str((previous_post or {}).get("first_seen_at") or run_now),
            "last_seen_run_id": run_id,
            "last_seen_at": run_now,
            "seen_run_count": seen_count,
            "current_stage": current_stage,
            "candidate_score_current": score,
            "media_score_current": ms["overall_media_score"],
            "post_url": p.get("post_url"),
            "source_id": p.get("source_id"),
            "source_title": p.get("source_title"),
            "platform_post_key": p.get("platform_post_key"),
            "post_date": p.get("post_date"),
        }
        if current_stage in {"favorite", "semantic_candidate"}:
            candidates.append(row)
        elif current_stage in {"pre_candidate_needs_llm", "needs_llm_retry", "needs_image_review", "image_fetch_retry_needed", "low_substance_but_region_relevant", "pre_candidate_debug_deterministic"}:
            pre_candidates.append(row)
        else:
            dropped.append(row)

    similar_channel_rows = list(_REGION_TALK_TELEGRAM_RUNTIME.get("similar_rows") or [])
    similar_channel_edges = list(_REGION_TALK_TELEGRAM_RUNTIME.get("similar_edges") or [])
    keyword_discovery_rows = list(_REGION_TALK_TELEGRAM_RUNTIME.get("keyword_rows") or [])
    keyword_discovery_edges = list(_REGION_TALK_TELEGRAM_RUNTIME.get("keyword_edges") or [])
    public_blogger_rows = load_public_blogger_links({})
    discovered_rows.extend(similar_channel_rows)
    discovered_rows.extend(keyword_discovery_rows)
    discovered_rows.extend(public_blogger_rows)
    graph_edges.extend(similar_channel_edges)
    graph_edges.extend(keyword_discovery_edges)

    review_queue = sorted(candidates + pre_candidates, key=lambda r: (r.get("current_stage") not in {"favorite", "semantic_candidate"}, -float(r.get("candidate_score") or 0), int(r.get("post_age_days") or 9999)))
    for i, r in enumerate(review_queue, start=1):
        r["rank"] = i
        r["status_badge"] = "READY" if r["current_stage"] == "favorite" else "NEEDS_REVIEW"
        r["new_or_seen"] = "NEW"
    favorites = [r for r in review_queue if r.get("current_stage") == "favorite"]
    final_verifier_llm_calls = 0
    final_verifier_queue_rows = [
        r for r in review_queue
        if getenv_bool("REGION_TALK_ENABLE_FINAL_LLM_VERIFIER", False)
        and str(r.get("needs_llm_final_verify") or "").lower() == "true"
        and r.get("kaliningrad_oblast_only_scope")
        and str(r.get("kaliningrad_mention_role") or "main_subject") in {"", "main_subject"}
        and not r.get("is_ad_or_promo")
        and r.get("current_stage") in CANDIDATE_MEMORY_STAGES
    ]
    final_verifier_limit = getenv_int("REGION_TALK_MAX_LLM_FINAL_VERIFY", 10)
    for r in final_verifier_queue_rows[:max(0, final_verifier_limit)]:
        evidence = {
            "stage": "final_publication_verifier",
            "vector_gate_status": r.get("vector_gate_status"),
            "image_status": r.get("image_status"),
            "image_model_input_type": r.get("image_model_input_type"),
            "matched_place_names": r.get("matched_place_names"),
            "content_type": r.get("content_type") or r.get("vector_content_type"),
        }
        result = call_region_talk_semantic_llm(r, evidence, model=llm_model, default_env_var_name=llm_default_env_var_name)
        if result.get("llm_gate_status") in {"ok", "error", "rate_limited"}:
            final_verifier_llm_calls += 1
        r["llm_stage"] = "final_publication_verifier"
        r["final_verifier_status"] = result.get("llm_gate_status")
        r["final_verifier_decision"] = result.get("llm_decision", "")
        r["final_verifier_reason"] = result.get("llm_reason", "")
        r["final_verifier_model"] = result.get("llm_model", llm_model)
        r["llm_status"] = "final_verified" if result.get("llm_gate_status") == "ok" else "final_verifier_retry"
        if result.get("llm_gate_status") == "ok" and result.get("llm_decision") == "reject":
            r["current_stage"] = "dropped_final_verifier"
            r["drop_gate"] = "final_llm_verifier"
            r["rejection_reason"] = "llm_reject_final"
            r["llm_reject_final_reason"] = result.get("llm_reason", "")
            dropped.append(r)
    if final_verifier_llm_calls:
        candidates = [r for r in candidates if r.get("current_stage") in {"favorite", "semantic_candidate"}]
        pre_candidates = [r for r in pre_candidates if r.get("current_stage") in {"pre_candidate_needs_llm", "needs_llm_retry", "needs_image_review", "image_fetch_retry_needed", "low_substance_but_region_relevant", "pre_candidate_debug_deterministic", "good_text_weak_media"}]
        review_queue = sorted(candidates + pre_candidates, key=lambda r: (r.get("current_stage") not in {"favorite", "semantic_candidate"}, -float(r.get("candidate_score") or 0), int(r.get("post_age_days") or 9999)))
        for i, r in enumerate(review_queue, start=1):
            r["rank"] = i
            r["status_badge"] = "READY" if r["current_stage"] == "favorite" else "NEEDS_REVIEW"
            r["new_or_seen"] = "NEW"
        favorites = [r for r in review_queue if r.get("current_stage") == "favorite"]

    # Source profile probe MVP: derive probe metrics from fetched sample; sources are not automatically monitored from discovery.
    posts_by_source: dict[str, list[dict[str, Any]]] = {}
    for row in new_posts:
        posts_by_source.setdefault(str(row.get("source_id") or ""), []).append(row)
    enriched_source_rows: list[dict[str, Any]] = []
    for srow in source_rows:
        sid = str(srow.get("source_id") or "")
        sampled = posts_by_source.get(sid, [])
        n = len(sampled)
        kal_hits = sum(1 for r in sampled if r.get("kaliningrad_oblast_only_scope"))
        ad_hits = sum(1 for r in sampled if r.get("is_ad_or_promo"))
        news_hits = sum(1 for r in sampled if float(r.get("newsiness_score") or 0) >= 0.45)
        trash_hits = sum(1 for r in sampled if float(r.get("trash_score") or 0) >= 0.35)
        original_media = sum(1 for r in sampled if r.get("has_media"))
        link_richness = sum(int(r.get("discovery_edges_count") or 0) for r in sampled)
        monitor_score = round((kal_hits / max(1, n))*0.35 + (original_media/max(1,n))*0.20 + min(1, link_richness/10)*0.15 + max(0, 1-ad_hits/max(1,n))*0.15 + max(0, 1-news_hits/max(1,n))*0.10, 3) if n else 0.0
        status = "probed" if n else ("profile_resolved" if srow.get("fetch_status") == "ok" else "blocked")
        source_class = classify_source_profile(srow, sampled)
        enriched_source_rows.append({**srow,
            "source_probe_status": status, "sampled_post_count": n, "kaliningrad_hit_count": kal_hits,
            "russia_travel_score": round(kal_hits/max(1,n),3), "authorial_voice_score": 0.5,
            "original_media_score": round(original_media/max(1,n),3), "ad_ratio": round(ad_hits/max(1,n),3),
            "news_ratio": round(news_hits/max(1,n),3), "trash_ratio": round(trash_hits/max(1,n),3),
            "image_prevalence": round(original_media/max(1,n),3), "link_richness_score": round(min(1, link_richness/10),3),
            "forwarded_origin_richness_score": round(sum(1 for r in sampled if r.get("is_forwarded_or_repost"))/max(1,n),3),
            "source_graph_value_score": round(min(1, link_richness/10),3), "monitor_priority_score": monitor_score,
            **source_class,
            "source_probe_reason": "derived from recent fetched posts; no automatic monitoring without manual/probe acceptance",
        })
    source_rows = enriched_source_rows
    source_class_by_id = {str(s.get("source_id") or ""): {k: s.get(k) for k in ["source_geo_class", "source_topic_class", "ko_mention_ratio_recent", "travel_blogger_score", "personal_voice_score", "nonlocal_value_score", "source_priority_reason"]} for s in source_rows}
    for r in new_posts:
        r.update({k: v for k, v in source_class_by_id.get(str(r.get("source_id") or ""), {}).items() if v != "" and v is not None})

    for drow in discovered_rows:
        platform_norm = normalize_source_platform(str(drow.get("platform") or drow.get("platform_guess") or ""), str(drow.get("canonical_url") or drow.get("normalized_url") or drow.get("raw_url") or ""))
        canonical_url = canonical_source_url(platform_norm, str(drow.get("username_or_handle") or drow.get("recommended_username") or ""), str(drow.get("canonical_url") or drow.get("normalized_url") or ""))
        canonical_key = str(drow.get("canonical_source_key") or canonical_source_key(platform_norm, str(drow.get("username_or_handle") or drow.get("recommended_username") or ""), canonical_url))
        did = str(drow.get("source_candidate_id") or "src_cand_" + stable_hash(canonical_key or canonical_url))
        prev = updated_discovered_state.get(did) or {}
        updated_discovered_state[did] = {
            **prev,
            "source_candidate_id": did,
            "normalized_url": canonical_url or drow.get("normalized_url"),
            "canonical_url": canonical_url or drow.get("canonical_url") or drow.get("normalized_url"),
            "canonical_source_key": canonical_key,
            "platform_guess": platform_norm or drow.get("platform_guess"),
            "title_guess": drow.get("recommended_title") or drow.get("title_guess") or drow.get("to_source_title") or prev.get("title_guess", ""),
            "edge_types_all": "; ".join(sorted(set(str(x) for x in ((str(prev.get("edge_types_all") or "").split("; ") if prev.get("edge_types_all") else []) + [str(drow.get("edge_type") or "")]) if x))),
            "discovery_types": "; ".join(sorted(set(str(x) for x in ((str(prev.get("discovery_types") or "").split("; ") if prev.get("discovery_types") else []) + [str(drow.get("discovery_type") or "")]) if x))),
            "first_seen_run_id": prev.get("first_seen_run_id") or run_id,
            "last_seen_run_id": run_id,
            "seen_run_count": int(prev.get("seen_run_count") or 0) + 1,
            "candidate_source_status": drow.get("candidate_source_status") or prev.get("candidate_source_status") or "source_frontier",
            "frontier_priority": max(float(prev.get("frontier_priority") or 0), source_candidate_score(drow)),
        }
    previous_source_cursors = previous_state.get("source_cursors") if isinstance(previous_state.get("source_cursors"), dict) else {}
    source_cursors: dict[str, Any] = {str(k): dict(v) for k, v in (previous_source_cursors or {}).items() if isinstance(v, dict)}
    for srow in source_rows:
        sid = str(srow.get("source_id") or "")
        sampled = posts_by_source.get(sid, [])
        newest = max([str(r.get("post_date") or "") for r in sampled] or [""])
        prev_cursor = source_cursors.get(sid, {})
        scanned_ok = srow.get("fetch_status") == "ok"
        is_new_source = str(srow.get("is_new_source_this_run") or "").lower() == "true" or not prev_cursor.get("primary_scan_completed_at")
        source_cursors[sid] = {
            **prev_cursor,
            "source_id": sid,
            "source_title": srow.get("source_title") or srow.get("resolved_title"),
            "platform": srow.get("platform"),
            "canonical_url": srow.get("canonical_url"),
            "canonical_source_key": srow.get("canonical_source_key") or canonical_source_key(str(srow.get("platform") or ""), str(srow.get("handle") or ""), str(srow.get("canonical_url") or "")),
            "handle": srow.get("handle"),
            "fetch_status": srow.get("fetch_status"),
            "last_history_fetch_run_id": run_id if scanned_ok else prev_cursor.get("last_history_fetch_run_id", ""),
            "last_history_fetch_at": run_now if scanned_ok else prev_cursor.get("last_history_fetch_at", ""),
            "last_seen_post_key": max([str(r.get("platform_post_key") or "") for r in sampled] or [str(prev_cursor.get("last_seen_post_key") or "")]),
            "last_seen_post_date": newest or prev_cursor.get("last_seen_post_date", ""),
            "last_seen_post_published_at": newest or prev_cursor.get("last_seen_post_published_at", ""),
            "posts_seen_total": int(prev_cursor.get("posts_seen_total") or 0) + int(srow.get("posts_scanned") or 0),
            "kaliningrad_posts_seen_total": int(prev_cursor.get("kaliningrad_posts_seen_total") or 0) + sum(1 for r in sampled if r.get("kaliningrad_oblast_only_scope")),
            "candidate_posts_seen_total": int(prev_cursor.get("candidate_posts_seen_total") or 0) + sum(1 for r in sampled if r.get("current_stage") in CANDIDATE_MEMORY_STAGES),
            "last_candidate_seen_at": max([str(r.get("post_date") or "") for r in sampled if r.get("current_stage") in CANDIDATE_MEMORY_STAGES] or [str(prev_cursor.get("last_candidate_seen_at") or "")]),
            "primary_scan_completed_at": (prev_cursor.get("primary_scan_completed_at") or (run_now if scanned_ok and is_new_source else "")),
            "primary_scan_post_count": int(prev_cursor.get("primary_scan_post_count") or 0) or (int(srow.get("posts_scanned") or 0) if scanned_ok and is_new_source else 0),
            "last_successful_delta_scan_at": run_now if scanned_ok and not is_new_source else prev_cursor.get("last_successful_delta_scan_at", ""),
            "delta_scan_count_total": int(prev_cursor.get("delta_scan_count_total") or 0) + (1 if scanned_ok and not is_new_source else 0),
            "source_scan_tier": srow.get("source_scan_tier") or ("high_daily" if float(srow.get("monitor_priority_score") or 0) >= 0.5 else "medium_weekly" if sampled else "keyword_probe"),
            "next_history_scan_at": iso_after_seconds(86400 if float(srow.get("monitor_priority_score") or 0) >= 0.5 else 7 * 86400),
            "next_delta_scan_at": iso_after_seconds(86400 if float(srow.get("monitor_priority_score") or 0) >= 0.5 else 7 * 86400),
            "delta_scanned_this_run": str(bool(scanned_ok and not is_new_source)).lower(),
            "delta_scan_window_days": getenv_int("REGION_TALK_DELTA_SCAN_WINDOW_DAYS", 14),
            "delta_overlap_posts": getenv_int("REGION_TALK_DELTA_OVERLAP_POSTS", 10),
            "last_run_id": run_id,
            "cursor_strategy": "fresh_first_min_post_date_then_anchor_search",
        }
    source_frontier_unique = build_source_frontier_unique(discovered_rows, updated_discovered_state, run_id)
    candidate_memory_rows, previous_candidates_not_refetched, candidate_deltas = build_candidate_memory(previous_state, new_posts, source_rows, run_id, run_now)
    source_frontier_queue_next = build_source_frontier_queue_next(source_frontier_unique, source_rows, candidate_memory_rows, run_id)
    candidate_memory_active_rows = [r for r in candidate_memory_rows if str(r.get("current_lifecycle_status") or "") in ACTIVE_CANDIDATE_MEMORY_STATUSES and str(r.get("manual_decision") or "") != "reject"]
    candidate_memory_top = sorted(candidate_memory_active_rows, key=lambda r: (str(r.get("not_refetched_this_run") or "") == "true", -float(r.get("publication_story_score") or 0), -float(r.get("nonlocal_value_score") or 0), -float(r.get("best_candidate_score_ever") or 0), -float(r.get("best_media_score_ever") or 0)))[:100]
    similar_seed_queue = build_similar_seed_queue(previous_state, source_rows, candidate_memory_rows, run_id, run_now)
    source_delta_scan_sheet = build_source_delta_scan_sheet(source_rows, previous_state, run_id)
    source_yield_metrics_sheet = build_source_yield_metrics(source_rows, posts_by_source, new_posts)
    state_to_write = {
        "run_id": run_id,
        "state_schema_version": "region-talk-state-v2",
        "updated_at": run_now,
        "posts": updated_posts_state,
        "discovered_sources": updated_discovered_state,
        "source_cursors": source_cursors,
        "source_frontier_unique": {str(r.get("source_candidate_id")): r for r in source_frontier_unique if r.get("source_candidate_id")},
        "region_talk_sources": {str(r.get("canonical_source_key") or r.get("source_candidate_id")): r for r in source_frontier_unique if r.get("source_candidate_id")},
        "source_frontier_queue_next": {str(r.get("canonical_url") or r.get("queue_rank")): r for r in source_frontier_queue_next},
        "similar_seed_queue": {str(r.get("similar_seed_id") or r.get("canonical_url")): r for r in similar_seed_queue},
        "candidate_memory": {str(r.get("candidate_memory_id")): r for r in candidate_memory_rows if r.get("candidate_memory_id")},
        "telegram_entity_cache": _REGION_TALK_TELEGRAM_RUNTIME.get("entity_cache") or previous_state.get("telegram_entity_cache") or {},
        "telegram_cooldowns": _REGION_TALK_TELEGRAM_RUNTIME.get("cooldowns") or previous_state.get("telegram_cooldowns") or {},
    }
    all_time_metrics = build_all_time_metrics(state_to_write, source_frontier_unique, candidate_memory_rows)
    state_to_write["all_time_metrics"] = all_time_metrics
    state_write_meta = save_region_talk_state(output_dir, state_to_write)

    stage_counts = {stage: sum(1 for r in new_posts if r.get("current_stage") == stage) for stage in sorted({str(r.get("current_stage") or "") for r in new_posts})}
    fresh_rows = [r for r in new_posts if r.get("fresh_enough")]
    fresh_with_place_evidence = [r for r in fresh_rows if r.get("matched_place_names")]
    llm_reviewed_rows = [r for r in new_posts if r.get("llm_gate_status") == "ok"]
    llm_accepted_rows = [r for r in llm_reviewed_rows if r.get("llm_decision") == "accept"]
    vector_scored_rows = [r for r in new_posts if r.get("vector_gate_status")]
    vector_rejected_rows = [r for r in vector_scored_rows if str(r.get("vector_gate_status") or "").startswith("vector_reject")]
    actual_image_scored_before_llm_count = sum(1 for r in media_rows if r.get("image_model_input_type") == "actual_image")
    final_verify_queue_count_estimate = len(final_verifier_queue_rows) if 'final_verifier_queue_rows' in locals() else sum(1 for r in new_posts if str(r.get("needs_llm_final_verify") or "") == "true" and r.get("current_stage") in {"favorite", "semantic_candidate", "needs_image_review", "image_fetch_retry_needed", "good_text_weak_media", "low_substance_but_region_relevant"})
    early_llm_enabled_summary = getenv_bool("REGION_TALK_ENABLE_EARLY_LLM", False)
    max_llm_final_verify = getenv_int("REGION_TALK_MAX_LLM_FINAL_VERIFY", 10)
    reviewable_rows = [r for r in review_queue if r.get("current_stage") in {"pre_candidate_needs_llm", "needs_llm_retry", "needs_image_review", "low_substance_but_region_relevant", "semantic_candidate", "favorite", "pre_candidate_debug_deterministic"}]
    git_info = git_provenance()
    tg_obs = _REGION_TALK_TELEGRAM_RUNTIME.get("observability") or {"telegram_phase_status": "not_configured"}
    tg_similar_metrics = {
        "telegram_similar_channels_enabled": str(getenv_bool("REGION_TALK_TG_SIMILAR_ENABLED", getenv_bool("REGION_TALK_DISCOVERY_ENABLE_TELEGRAM_SIMILAR", True))).lower(),
        "telegram_similar_channels_status": _REGION_TALK_TELEGRAM_RUNTIME.get("telegram_similar_channels_status", "not_run"),
        "telegram_similar_channels_seed_count": _REGION_TALK_TELEGRAM_RUNTIME.get("telegram_similar_channels_seed_count", 0),
        "telegram_similar_channels_raw_count": _REGION_TALK_TELEGRAM_RUNTIME.get("telegram_similar_channels_raw_count", 0),
        "telegram_similar_channels_unique_count": _REGION_TALK_TELEGRAM_RUNTIME.get("telegram_similar_channels_unique_count", 0),
        "telegram_similar_channels_added_to_frontier": _REGION_TALK_TELEGRAM_RUNTIME.get("telegram_similar_channels_added_to_frontier", 0),
        "telegram_similar_channels_fetch_errors": _REGION_TALK_TELEGRAM_RUNTIME.get("telegram_similar_channels_fetch_errors", 0),
        "telegram_similar_channels_floodwait_count": _REGION_TALK_TELEGRAM_RUNTIME.get("telegram_similar_channels_floodwait_count", 0),
    }
    summary_row = {
        "run_id":run_id,"started_at":RUN_STARTED_AT.isoformat(),"finished_at":run_now,"git_sha":git_info.get("git_sha", ""),"git_sha_short":git_info.get("git_sha_short", ""),"branch":git_info.get("branch", ""),
        "config_profile":"mvp1.x_llm_first_review_queue","dry_run":"1","ydb_namespace":os.getenv("REGION_TALK_YDB_NAMESPACE") or "dry-run-json",
        "REGION_TALK_DISCOVERY_MODE":os.getenv("REGION_TALK_DISCOVERY_MODE") or "mixed",
        "REGION_TALK_HISTORY_SCAN_MODE":os.getenv("REGION_TALK_HISTORY_SCAN_MODE") or "primary_and_delta",
        "REGION_TALK_MEDIA_SCORING_MODE":os.getenv("REGION_TALK_MEDIA_SCORING_MODE") or current_image_scoring_mode(),
        "REGION_TALK_ACTUAL_IMAGE_TARGET":getenv_int("REGION_TALK_ACTUAL_IMAGE_TARGET", 30),
        "seed_file_version":"v2" if any("v2" in str(s.get("source_seed_id")) for s in source_seed_rows) else "v1/v2-compatible",
        "place_lexicon_file":str(lexicon_path or ""),"place_lexicon_rows":len(lexicon),
        "source_count_seeded":len(seeds),"source_count_scanned":len(source_rows),"posts_fetched":len(posts),
        "source_count_selected":len(source_rows),"source_count_attempted":sum(1 for s in source_rows if str(s.get("fetch_attempted") or "").lower() == "true" or str(s.get("fetch_status") or "").startswith(("ok", "error"))),
        "source_count_ok":sum(1 for s in source_rows if s.get("fetch_status") == "ok"),
        "source_count_skipped":sum(1 for s in source_rows if str(s.get("fetch_status") or "").startswith("skipped")),
        "source_count_error":sum(1 for s in source_rows if str(s.get("fetch_status") or "").startswith("error")),
        "source_count_deferred_by_cooldown":sum(1 for s in source_rows if "cooldown" in str(s.get("fetch_status") or "")),
        "source_count_deferred_unresolved":sum(1 for s in source_rows if "unresolved" in str(s.get("fetch_status") or "")),
        "sources_selected_for_planning":len(source_rows),
        "sources_resolved_available":sum(1 for s in source_rows if s.get("telegram_resolve_status") in {"resolved_network", "resolved_from_private_cache"} or s.get("fetch_status") == "ok"),
        "sources_fetch_attempted":sum(1 for s in source_rows if str(s.get("fetch_attempted") or "").lower() == "true"),
        "sources_history_fetched_ok":sum(1 for s in source_rows if s.get("fetch_status") == "ok"),
        "history_sources_target":_REGION_TALK_TELEGRAM_RUNTIME.get("history_sources_target") or tg_obs.get("history_sources_target") or "",
        "history_sources_attempted":tg_obs.get("history_sources_attempted", sum(1 for s in source_rows if str(s.get("fetch_attempted") or "").lower() == "true")),
        "history_sources_ok":tg_obs.get("history_sources_ok", sum(1 for s in source_rows if s.get("fetch_status") == "ok")),
        "history_sources_new_to_system":sum(1 for s in source_rows if s.get("fetch_status") == "ok" and str(s.get("is_new_source_this_run") or "").lower() == "true"),
        "history_sources_cached_entity":sum(1 for s in source_rows if s.get("fetch_status") == "ok" and str(s.get("history_source_cached_entity") or "").lower() == "true"),
        "history_sources_network_resolved":sum(1 for s in source_rows if s.get("fetch_status") == "ok" and str(s.get("history_source_network_resolved") or "").lower() == "true"),
        "history_fetch_runtime_seconds":round(sum(float(s.get("history_fetch_runtime_seconds") or 0) for s in source_rows if str(s.get("history_fetch_runtime_seconds") or "").strip() not in {"", "None"}), 3),
        "posts_per_source_distribution":json.dumps([int(s.get("posts_scanned") or 0) for s in source_rows if s.get("fetch_status") == "ok"], ensure_ascii=False),
        "sources_skipped_due_resolve_budget":sum(1 for s in source_rows if "budget" in str(s.get("fetch_status") or "")),
        "sources_skipped_due_platform_not_configured":sum(1 for s in source_rows if "not_configured" in str(s.get("fetch_status") or "")),
        "sources_skipped_due_low_priority_defer":sum(1 for s in source_rows if "low_priority" in str(s.get("fetch_status") or "")),
        "vk_wall_probe_status":"ok" if any(s.get("platform") == "vk" and s.get("fetch_status") == "ok" for s in source_rows) else ("error" if any(s.get("platform") == "vk" and str(s.get("fetch_status") or "").startswith("error") for s in source_rows) else ("not_configured" if any(s.get("platform") == "vk" for s in source_rows) and not vk_wall_token() else ("configured_no_vk_selected" if vk_wall_token() else "not_selected"))),
        "posts_region_relevant":sum(1 for r in new_posts if r.get("kaliningrad_oblast_only_scope")),
        "fresh_posts":len(fresh_rows),"fresh_posts_with_place_evidence":len(fresh_with_place_evidence),
        "review_queue_rows":len(review_queue),"pre_candidates_created":len(pre_candidates),
        **llm_limit_snapshot,
        "llm_calls":llm_calls_used,"llm_max_calls":"supabase_reserve_controlled","llm_reviewed":len(llm_reviewed_rows),"llm_accepted":len(llm_accepted_rows),
        "llm_calls_attempted":llm_calls_used,"llm_calls_ok":len(llm_reviewed_rows),"llm_calls_error":sum(1 for r in new_posts if r.get("llm_gate_status") == "error"),"llm_quota_errors":sum(1 for r in new_posts if r.get("llm_gate_status") == "rate_limited"),"llm_retry_rows":sum(1 for r in new_posts if r.get("current_stage") == "needs_llm_retry"),
        "wide_funnel_llm_calls":llm_calls_used if early_llm_enabled_summary else 0,
        "final_verifier_llm_calls":final_verifier_llm_calls,
        "llm_calls_saved_by_vector_gate":len(vector_rejected_rows),
        "llm_calls_saved_by_deterministic_gate":sum(1 for r in new_posts if r.get("image_scoring_skipped") == "true" and not r.get("vector_gate_status")),
        "text_vector_model_id":"dual_model_target:intfloat/multilingual-e5-base+BAAI/bge-m3;prototype_fallback_v1",
        "text_vector_rows_scored":len(vector_scored_rows),
        "vector_rejected_news_event_count":sum(1 for r in vector_scored_rows if r.get("vector_gate_status") == "vector_reject_news_event"),
        "vector_rejected_ad_promo_count":sum(1 for r in vector_scored_rows if r.get("vector_gate_status") == "vector_reject_ad_promo"),
        "vector_rejected_roundup_count":sum(1 for r in vector_scored_rows if r.get("vector_gate_status") == "vector_reject_roundup"),
        "vector_rejected_low_substance_count":sum(1 for r in vector_scored_rows if r.get("vector_gate_status") == "vector_reject_low_substance"),
        "vector_ambiguous_kept_count":sum(1 for r in vector_scored_rows if r.get("vector_gate_status") == "vector_ambiguous_keep_for_ranking"),
        "actual_image_scored_before_llm_count":actual_image_scored_before_llm_count,
        "actual_image_target_met":str(actual_image_scored_before_llm_count >= getenv_int("REGION_TALK_ACTUAL_IMAGE_TARGET", 30)).lower(),
        "llm_final_verify_queue_count":final_verify_queue_count_estimate,
        "llm_budget_preserved_count":max(0, len(vector_scored_rows) - llm_calls_used),
        "REGION_TALK_ENABLE_EARLY_LLM":str(early_llm_enabled_summary).lower(),
        "REGION_TALK_ENABLE_VECTOR_GATES":str(getenv_bool("REGION_TALK_ENABLE_VECTOR_GATES", True)).lower(),
        "REGION_TALK_ENABLE_LOCAL_TEXT_EMBEDDINGS":str(getenv_bool("REGION_TALK_ENABLE_LOCAL_TEXT_EMBEDDINGS", True)).lower(),
        "REGION_TALK_TARGET_LLM_CALLS":getenv_int("REGION_TALK_TARGET_LLM_CALLS", 10),
        "REGION_TALK_MAX_LLM_FINAL_VERIFY":max_llm_final_verify,
        "llm_timeout_errors":sum(1 for r in new_posts if "timeout" in str(r.get("llm_reason") or "").lower()),
        "llm_malformed_json_errors":sum(1 for r in new_posts if "json" in str(r.get("llm_reason") or "").lower() and r.get("llm_gate_status") == "error"),
        "llm_cache_hits":0,"llm_budget_remaining":"supabase_controlled",
        "posts_with_strong_media":sum(1 for r in new_posts if r.get("is_selected_for_publication")),
        "candidates_created":len(candidates),"favorites_created":len(favorites),
        "dropped_news":sum(1 for r in dropped if r["rejection_reason"]=="newsiness"),"dropped_trash":sum(1 for r in dropped if r["rejection_reason"]=="trash"),
        "dropped_not_region":sum(1 for r in dropped if r["rejection_reason"]=="reject_not_kaliningrad_oblast_only"),
        "dropped_ad_or_promo":sum(1 for r in dropped if r["rejection_reason"]=="reject_ad_or_promo"),
        "dropped_stale":sum(1 for r in dropped if r["rejection_reason"]=="reject_stale_or_missing_date"),
        "dropped_low_substance":sum(1 for r in dropped if r["rejection_reason"]=="reject_low_substance"),
        "dropped_weak_media":sum(1 for r in dropped if "media" in str(r.get("rejection_reason"))),"dropped_duplicate":0,"dropped_rights":0,
        "image_model_calls":len(media_rows),"image_scoring_skipped_by_text_gate":sum(1 for r in new_posts if r.get("image_scoring_skipped") == "true"),
        "discovered_links":len(discovered_rows),"source_graph_edges":len(graph_edges),"errors_count":sum(1 for s in source_rows if str(s.get("fetch_status") or "").startswith("error")),
        "post_memory_total":len(updated_posts_state),
        "previous_seen_post_count":len(previous_posts),
        "current_fetched_post_count":len(new_posts),
        "previous_posts_not_refetched_this_run":len(set(str(k) for k in previous_posts.keys()) - {str(r.get("post_id")) for r in new_posts}),
        "candidate_memory_total":len(candidate_memory_rows),
        "candidate_memory_active":len(candidate_memory_active_rows),
        "candidate_memory_new_this_run":sum(1 for r in candidate_memory_rows if r.get("first_candidate_run_id") == run_id),
        "candidate_memory_retained_from_previous":sum(1 for r in candidate_memory_rows if r.get("first_candidate_run_id") != run_id),
        "candidate_memory_not_refetched_this_run":len(previous_candidates_not_refetched),
        "candidate_memory_upgraded_this_run":sum(1 for r in candidate_deltas if r.get("delta_bucket") == "stage_upgraded"),
        "candidate_memory_downgraded_this_run":sum(1 for r in candidate_deltas if r.get("delta_bucket") == "stage_downgraded"),
        "candidate_memory_expired_this_run":sum(1 for r in candidate_deltas if r.get("delta_bucket") == "expired_by_policy"),
        "source_frontier_unique_total":len(source_frontier_unique),
        "source_frontier_total":len(source_frontier_unique),
        "source_frontier_new_this_run":sum(1 for r in source_frontier_unique if r.get("discovery_first_run_id") == run_id),
        "source_frontier_promoted_this_run":len(source_frontier_queue_next),
        "source_frontier_ready_next_run":len(source_frontier_queue_next),
        "source_frontier_ready_to_probe":sum(1 for r in source_frontier_unique if str(r.get("frontier_stage") or "") in {"probe_due", "history_due", "unresolved"}),
        "source_frontier_deferred":sum(1 for r in source_frontier_unique if "defer" in str(r.get("frontier_status") or "")),
        "source_frontier_inactive_low_quality":sum(1 for r in source_frontier_unique if str(r.get("frontier_stage") or "") == "inactive_low_quality"),
        "similar_seed_queue_total":len(similar_seed_queue),
        "similar_seed_queue_ready":sum(1 for r in similar_seed_queue if str(r.get("similar_seed_status") or "") == "ready"),
        "similar_seed_queue_used_total":sum(1 for r in similar_seed_queue if int(r.get("similar_seed_use_count") or 0) > 0),
        "dynamic_frontier_seed_count":_REGION_TALK_TELEGRAM_RUNTIME.get("dynamic_frontier_seed_count", 0),
        "telegram_keyword_discovery_status":_REGION_TALK_TELEGRAM_RUNTIME.get("telegram_keyword_discovery_status", "not_run"),
        "keyword_search_queries_processed":_REGION_TALK_TELEGRAM_RUNTIME.get("keyword_search_queries_processed", 0),
        "keyword_discovered_sources_unique":_REGION_TALK_TELEGRAM_RUNTIME.get("keyword_discovered_sources_unique", 0),
        "keyword_discovery_errors":_REGION_TALK_TELEGRAM_RUNTIME.get("keyword_discovery_errors", 0),
        "telegram_phase_status":tg_obs.get("telegram_phase_status"),
        "telegram_floodwait_count":tg_obs.get("resolve_network_floodwait", 0),
        "telegram_max_floodwait_seconds":tg_obs.get("max_floodwait_seconds", 0),
        "telegram_floodwait_method":tg_obs.get("floodwait_method", ""),
        "telegram_cooldown_until":tg_obs.get("floodwait_cooldown_until", ""),
        "telegram_resolve_cache_hits":tg_obs.get("resolve_cache_hits", 0),
        "telegram_resolve_network_attempts":tg_obs.get("resolve_network_attempts", 0),
        "telegram_resolve_network_budget":tg_obs.get("telegram_governor_decisions_json", ""),
        "entity_cache_loaded_from_path":tg_obs.get("entity_cache_loaded_from_path", ""),
        "entity_cache_write_path":tg_obs.get("entity_cache_write_path", ""),
        "entity_cache_hit_rate":tg_obs.get("entity_cache_hit_rate", ""),
        "resolved_sources_available_for_history_fetch":tg_obs.get("resolved_sources_available_for_history_fetch", ""),
        "resolved_sources_used_without_network_resolve":tg_obs.get("resolved_sources_used_without_network_resolve", ""),
        "telegram_sources_deferred_by_cooldown":sum(1 for s in source_rows if "cooldown" in str(s.get("fetch_status") or "")),
        "telegram_sources_deferred_unresolved":sum(1 for s in source_rows if "unresolved" in str(s.get("fetch_status") or "")),
        "telegram_similar_self_loop_count":_REGION_TALK_TELEGRAM_RUNTIME.get("telegram_similar_self_loop_count", 0),
        "telegram_similar_duplicate_canonical_count":_REGION_TALK_TELEGRAM_RUNTIME.get("telegram_similar_duplicate_canonical_count", 0),
        "similar_seed_cursor_advanced":_REGION_TALK_TELEGRAM_RUNTIME.get("similar_seed_cursor_advanced", "false"),
        **tg_similar_metrics,
        "new_posts_this_run":sum(1 for r in increment if r.get("new_this_run") == "yes"),
        "changed_posts_this_run":sum(1 for r in increment if r.get("changed_this_run") == "true"),
        "unchanged_posts_this_run":sum(1 for r in increment if r.get("changed_this_run") == "false"),
        "state_backend": state_write_meta.get("state_backend") or state_meta.get("state_backend") or region_talk_state_backend_requested(),
        "state_backend_requested": state_meta.get("state_backend_requested") or region_talk_state_backend_requested(),
        "state_fallback_used": state_write_meta.get("state_fallback_used") or state_meta.get("state_fallback_used") or "false",
        "state_fallback_reason": state_write_meta.get("state_fallback_reason") or state_meta.get("state_fallback_reason") or "",
        "ydb_read_status": state_meta.get("ydb_read_status", "not_requested"),
        "ydb_write_status": state_write_meta.get("ydb_write_status", "not_requested"),
        "ydb_tables_expected": state_write_meta.get("ydb_tables_expected") or state_meta.get("ydb_tables_expected") or "",
        "source_frontier_total_previous":len(previous_state.get("source_frontier_unique") or {}),
        "source_frontier_total_current":len(source_frontier_unique),
        "catalog_import_rows_total":len(public_blogger_rows),
        "catalog_import_unique_sources":len({r.get("canonical_source_key") or r.get("canonical_url") or r.get("normalized_url") for r in public_blogger_rows}),
        "catalog_sources_in_authoritative_frontier":sum(1 for r in source_frontier_unique if "public_travel_blogger_catalog" in str(r.get("discovery_types") or "")),
        "catalog_sources_in_frontier":sum(1 for r in source_frontier_unique if "public_travel_blogger_catalog" in str(r.get("discovery_types") or "")),
        "frontier_duplicate_canonical_keys":len(source_frontier_unique) - len({r.get("canonical_source_key") or r.get("canonical_url") for r in source_frontier_unique}),
        "frontier_self_loops":0,
        "telegram_similar_seed_used":_REGION_TALK_TELEGRAM_RUNTIME.get("telegram_similar_channels_seed_count", 0),
        "telegram_similar_raw_count":_REGION_TALK_TELEGRAM_RUNTIME.get("telegram_similar_channels_raw_count", 0),
        "telegram_similar_unique_count":_REGION_TALK_TELEGRAM_RUNTIME.get("telegram_similar_channels_unique_count", 0),
        "keyword_queries_processed":_REGION_TALK_TELEGRAM_RUNTIME.get("keyword_search_queries_processed", 0),
        "keyword_unique_sources":_REGION_TALK_TELEGRAM_RUNTIME.get("keyword_discovered_sources_unique", 0),
        "sources_primary_scanned_this_run":sum(1 for s in source_rows if s.get("fetch_status") == "ok" and str(s.get("is_new_source_this_run") or "").lower() == "true"),
        "sources_delta_scanned_this_run":sum(1 for s in source_rows if s.get("fetch_status") == "ok" and str(s.get("history_fetch_mode") or "").startswith("delta")),
        "posts_new_this_run":sum(1 for r in increment if r.get("new_this_run") == "yes"),
        "posts_delta_new_this_run":sum(1 for r in increment if r.get("new_this_run") == "yes" and not str(r.get("source_seed_id") or "").startswith("frontier_dynamic_")),
        "sample_bias_note":"priority-biased source sample; not a random conversion estimate",
        "sources_with_any_ko_post_per_1000":round(1000 * len({r.get("source_id") for r in new_posts if r.get("kaliningrad_oblast_only_scope")}) / max(1, len([s for s in source_rows if s.get("fetch_status") == "ok"])), 1),
        "sources_with_fresh_ko_post_per_1000":round(1000 * len({r.get("source_id") for r in new_posts if r.get("kaliningrad_oblast_only_scope") and r.get("fresh_enough")}) / max(1, len([s for s in source_rows if s.get("fetch_status") == "ok"])), 1),
        "sources_with_candidate_memory_post_per_1000":round(1000 * len({r.get("source_id") for r in candidate_memory_rows}) / max(1, len([s for s in source_rows if s.get("fetch_status") == "ok"])), 1),
        "sources_with_actual_image_candidate_per_1000":round(1000 * len({r.get("source_id") for r in new_posts if r.get("image_model_input_type") == "actual_image"}) / max(1, len([s for s in source_rows if s.get("fetch_status") == "ok"])), 1),
        "sources_with_publication_ready_candidate_per_1000":round(1000 * len({r.get("source_id") for r in candidate_memory_rows if str(r.get("image_publication_ready") or "") == "true"}) / max(1, len([s for s in source_rows if s.get("fetch_status") == "ok"])), 1),
        "current_run_reviewable_candidates":sum(1 for r in candidate_memory_rows if r.get("first_candidate_run_id") == run_id),
        "final_candidates":len(candidates),
        "favorites":len(favorites),
        "image_fetch_retry_needed":sum(1 for r in new_posts if r.get("current_stage") == "image_fetch_retry_needed"),
        "has_firsthand_visit_evidence_count":sum(1 for r in new_posts if str(r.get("has_firsthand_visit_evidence") or "").lower() == "true"),
        "visit_impression_candidate_count":sum(1 for r in new_posts if str(r.get("content_type") or r.get("vector_content_type") or "") == "visit_impression_candidate"),
        **state_meta,
        **state_write_meta,
        **all_time_metrics,
        "stage_counts_json":json.dumps(stage_counts, ensure_ascii=False),"artifact_paths":""
    }
    run_summary = [summary_row]
    seq_fresh = [r for r in new_posts if r.get("fresh_enough")]
    seq_region = [r for r in seq_fresh if r.get("kaliningrad_oblast_only_scope")]
    seq_non_ad = [r for r in seq_region if not r.get("is_ad_or_promo")]
    seq_substantive = [r for r in seq_non_ad if float(r.get("text_substance_score") or 0) >= 0.18]
    seq_llm_reviewed = [r for r in seq_substantive if r.get("llm_gate_status") == "ok"]
    seq_llm_accepted = [r for r in seq_llm_reviewed if r.get("llm_decision") == "accept"]
    seq_image_actual = [r for r in media_rows if r.get("image_model_input_type") == "actual_image"]
    seq_reviewable_image = [r for r in new_posts if str(r.get("image_reviewable")) == "true"]
    seq_publication_ready = [r for r in new_posts if str(r.get("image_publication_ready")) == "true"]
    funnel = [
        {"stage":"post_fetched","current_run_count":len(new_posts),"notes":"all fetched posts"},
        {"stage":"fresh","current_run_count":len(seq_fresh),"notes":"sequential freshness gate"},
        {"stage":"kaliningrad_oblast_only","current_run_count":len(seq_region),"notes":"sequential region-scope evidence; final semantics still LLM-owned"},
        {"stage":"non_ad","current_run_count":len(seq_non_ad),"notes":"hard promo/ad excluded; possible promo may continue to LLM"},
        {"stage":"substantive","current_run_count":len(seq_substantive),"notes":"basic substance cost guard"},
        {"stage":"semantic_pre_candidate","current_run_count":sum(1 for r in seq_substantive if r.get("llm_gate_status") != "not_run_pre_llm_cost_guard"),"notes":"eligible for Supabase-limited final LLM semantic gate"},
        {"stage":"llm_reviewed","current_run_count":len(seq_llm_reviewed),"notes":"Google Gemini calls through Supabase google_ai_reserve; no direct env max/key2 bypass"},
        {"stage":"llm_accepted","current_run_count":len(seq_llm_accepted),"notes":"LLM semantic accept"},
        {"stage":"image_scored_actual","current_run_count":len(seq_image_actual),"notes":"Kaggle-local neural actual-image rows"},
        {"stage":"reviewable_image","current_run_count":len(seq_reviewable_image),"notes":"accepted text + image reviewable but not necessarily publication-ready"},
        {"stage":"publication_ready_image","current_run_count":len(seq_publication_ready),"notes":"strong image gate passed"},
        {"stage":"final_shortlist","current_run_count":len(final_shortlist) if 'final_shortlist' in locals() else 0,"notes":"filled after shortlist construction; see 04a"},
        {"stage":"favorite","current_run_count":len(favorites),"notes":"top auto-favorite rows"},
    ]
    independent_gate_counts = [
        {"gate":"fresh_enough","count":len(fresh_rows)},
        {"gate":"has_kaliningrad_place_evidence","count":sum(1 for r in new_posts if r.get("matched_place_names"))},
        {"gate":"deterministic_scope_evidence_ok","count":sum(1 for r in new_posts if r.get("kaliningrad_oblast_only_scope"))},
        {"gate":"no_ad_keyword_evidence","count":sum(1 for r in new_posts if not r.get("is_ad_or_promo"))},
        {"gate":"substance_score_ge_025","count":sum(1 for r in new_posts if float(r.get("text_substance_score") or 0) >= 0.25)},
        {"gate":"has_media","count":sum(1 for r in new_posts if r.get("has_media"))},
        {"gate":"semantic_review_required_or_precandidate","count":len(pre_candidates)},
        {"gate":"llm_retry_required","count":sum(1 for r in new_posts if r.get("current_stage") == "needs_llm_retry")},
        {"gate":"clip_actual_image_scored","count":sum(1 for r in media_rows if r.get("image_model_input_type") == "actual_image")},
    ]
    final_shortlist = sorted(
        [r for r in review_queue if r.get("current_stage") in {"favorite", "semantic_candidate", "needs_image_review", "needs_llm_retry", "pre_candidate_needs_llm"}
         and r.get("fresh_enough") and r.get("kaliningrad_oblast_only_scope") and not r.get("is_ad_or_promo")],
        key=lambda r: (
            0 if r.get("current_stage") in {"favorite", "semantic_candidate"} else 1 if r.get("current_stage") == "needs_image_review" else 2 if r.get("current_stage") == "needs_llm_retry" else 3,
            -float(r.get("publication_story_score") or 0),
            -float(r.get("nonlocal_value_score") or 0),
            -float(r.get("candidate_score") or 0),
            int(r.get("post_age_days") or 9999),
        ),
    )
    for i, r in enumerate(final_shortlist, start=1):
        r["human_shortlist_rank"] = i
        if r.get("current_stage") in {"favorite", "semantic_candidate"} and str(r.get("image_publication_ready")) == "true":
            r["decision_bucket"] = "publication_ready_candidate"
            r["next_action"] = "human_final_check"
        elif r.get("current_stage") == "needs_image_review" and str(r.get("image_reviewable")) == "true":
            r["decision_bucket"] = "reviewable_image"
            r["next_action"] = "human_image_review"
        elif r.get("current_stage") == "needs_llm_retry":
            r["decision_bucket"] = "needs_llm_retry"
            r["next_action"] = "retry_llm"
        else:
            r["decision_bucket"] = "needs_llm"
            r["next_action"] = "manual_review"
    for item in funnel:
        if item.get("stage") == "final_shortlist":
            item["current_run_count"] = len(final_shortlist)
    llm_error_rows = [r for r in new_posts if r.get("current_stage") == "needs_llm_retry" or r.get("llm_gate_status") in {"rate_limited", "error"}]
    debug_rejects = [r for r in dropped if r.get("current_stage") in {"debug_reject", "dropped_text_gate"} or str(r.get("rejection_reason") or "").startswith("debug_reject")]
    llm_error_sheet_rows = llm_error_rows or [{
        "post_url":"","current_stage":"","llm_gate_status":"","llm_provider":"","llm_model":"","llm_default_env_var_name":"",
        "llm_reason":"","retry_action":"","_sheet_note":"no LLM retry/error rows in this run",
    }]
    image_model_observability = []
    if media_rows:
        grouped: dict[tuple[str, str, str, str, str], int] = {}
        for mr in media_rows:
            key = (
                str(mr.get("image_scoring_mode") or ""),
                str(mr.get("model_id") or ""),
                str(mr.get("image_model_type") or ""),
                str(mr.get("image_model_runtime") or ""),
                str(mr.get("image_model_input_type") or ""),
            )
            grouped[key] = grouped.get(key, 0) + 1
        for (mode, model_id, model_type, runtime, input_type), count in sorted(grouped.items()):
            devices = sorted({str(r.get("image_model_device") or "") for r in media_rows if str(r.get("model_id") or "") == model_id})
            image_model_observability.append({
                "image_scoring_mode": mode,
                "image_model_id": model_id,
                "image_model_version": next((r.get("model_version") for r in media_rows if str(r.get("model_id") or "") == model_id), ""),
                "image_model_type": model_type,
                "image_model_runtime": runtime,
                "image_model_input_type": input_type,
                "image_model_device": "; ".join(d for d in devices if d),
                "rows": count,
                "actual_image_bytes_required": str(input_type == "actual_image").lower(),
                "fallback_note": "metadata fallback; not neural image understanding" if input_type == "metadata_only" else ("local CLIP actual-image scoring; CLIP is not a VLM" if model_type == "clip" else ""),
            })
    else:
        image_model_observability.append({"image_scoring_mode":current_image_scoring_mode(),"image_model_id":"","image_model_type":"","image_model_runtime":"","image_model_input_type":"","image_model_device":"","rows":0,"actual_image_bytes_required":"true","fallback_note":"no image rows reached scoring"})
    product_summary = [
        {"metric":"run_id","value":run_id},
        {"metric":"git_sha_short","value":git_info.get("git_sha_short")},
        {"metric":"branch","value":git_info.get("branch")},
        {"metric":"increment_status","value":"real increment" if state_meta.get("increment_state_loaded") == "true" else "baseline run, not real increment"},
        {"metric":"previous_run_id","value":state_meta.get("previous_run_id", "")},
        {"metric":"post_memory_total","value":summary_row.get("post_memory_total")},
        {"metric":"previous_posts_not_refetched_this_run","value":summary_row.get("previous_posts_not_refetched_this_run")},
        {"metric":"candidate_memory_total","value":summary_row.get("candidate_memory_total")},
        {"metric":"candidate_memory_active","value":summary_row.get("candidate_memory_active")},
        {"metric":"candidate_memory_new_this_run","value":summary_row.get("candidate_memory_new_this_run")},
        {"metric":"candidate_memory_not_refetched_this_run","value":summary_row.get("candidate_memory_not_refetched_this_run")},
        {"metric":"current_run_shortlist_count","value":len(final_shortlist)},
        {"metric":"cumulative_candidate_memory_count","value":len(candidate_memory_rows)},
        {"metric":"publication_ready_current_run_count","value":sum(1 for r in new_posts if str(r.get("image_publication_ready")) == "true")},
        {"metric":"publication_ready_cumulative_count","value":sum(1 for r in candidate_memory_rows if str(r.get("image_publication_ready")) == "true")},
        {"metric":"candidate_memory_note","value":("Current run shortlist is empty, but candidate_memory has %s active candidates; see 06b/21." % summary_row.get("candidate_memory_active")) if not final_shortlist and summary_row.get("candidate_memory_active") else ""},
        {"metric":"posts_fetched","value":len(posts)},
        {"metric":"fresh_posts","value":len(fresh_rows)},
        {"metric":"kaliningrad_oblast_only","value":sum(1 for r in fresh_rows if r.get("kaliningrad_oblast_only_scope"))},
        {"metric":"old_rejected","value":summary_row.get("dropped_stale")},
        {"metric":"ad_rejected","value":summary_row.get("dropped_ad_or_promo")},
        {"metric":"multi_region_or_not_region_rejected","value":summary_row.get("dropped_not_region")},
        {"metric":"news_trash_rejected","value":summary_row.get("dropped_news", 0) + summary_row.get("dropped_trash", 0)},
        {"metric":"low_substance_rejected","value":summary_row.get("dropped_low_substance")},
        {"metric":"fresh_posts_with_place_evidence","value":len(fresh_with_place_evidence)},
        {"metric":"llm_calls_supabase_reserved","value":llm_calls_used},
        {"metric":"wide_funnel_llm_calls","value":summary_row.get("wide_funnel_llm_calls")},
        {"metric":"final_verifier_llm_calls","value":summary_row.get("final_verifier_llm_calls")},
        {"metric":"llm_calls_saved_by_vector_gate","value":summary_row.get("llm_calls_saved_by_vector_gate")},
        {"metric":"llm_calls_saved_by_deterministic_gate","value":summary_row.get("llm_calls_saved_by_deterministic_gate")},
        {"metric":"text_vector_model_id","value":summary_row.get("text_vector_model_id")},
        {"metric":"text_vector_rows_scored","value":summary_row.get("text_vector_rows_scored")},
        {"metric":"vector_rejected_news_event_count","value":summary_row.get("vector_rejected_news_event_count")},
        {"metric":"vector_rejected_ad_promo_count","value":summary_row.get("vector_rejected_ad_promo_count")},
        {"metric":"vector_rejected_roundup_count","value":summary_row.get("vector_rejected_roundup_count")},
        {"metric":"vector_rejected_low_substance_count","value":summary_row.get("vector_rejected_low_substance_count")},
        {"metric":"vector_ambiguous_kept_count","value":summary_row.get("vector_ambiguous_kept_count")},
        {"metric":"actual_image_scored_before_llm_count","value":summary_row.get("actual_image_scored_before_llm_count")},
        {"metric":"llm_final_verify_queue_count","value":summary_row.get("llm_final_verify_queue_count")},
        {"metric":"llm_budget_preserved_count","value":summary_row.get("llm_budget_preserved_count")},
        {"metric":"llm_calls_ok","value":len(llm_reviewed_rows)},
        {"metric":"llm_retry_rows","value":len(llm_error_rows)},
        {"metric":"image_scored_rows","value":len(media_rows)},
        {"metric":"actual_image_neural_rows","value":sum(1 for r in media_rows if r.get("image_model_input_type") == "actual_image")},
        {"metric":"actual_image_target_met","value":summary_row.get("actual_image_target_met")},
        {"metric":"human_final_shortlist_rows","value":len(final_shortlist)},
        {"metric":"good_text_weak_media_rows","value":sum(1 for r in dropped if r.get("current_stage") == "good_text_weak_media")},
        {"metric":"publication_ready_rows","value":sum(1 for r in new_posts if str(r.get("image_publication_ready")) == "true")},
        {"metric":"final_candidates","value":len(candidates)},
        {"metric":"favorites","value":len(favorites)},
        {"metric":"why_zero_candidates_or_favorites","value":"Image gate did not produce publication_ready rows; review 04a_final_shortlist and 10_good_text_weak_media." if not candidates and not favorites else ""},
        {"metric":"source_coverage","value":f"selected={summary_row.get('source_count_selected')}, ok={summary_row.get('source_count_ok')}, skipped={summary_row.get('source_count_skipped')}, errors={summary_row.get('source_count_error')}"},
        {"metric":"history_sources_target","value":summary_row.get("history_sources_target")},
        {"metric":"history_sources_attempted","value":summary_row.get("history_sources_attempted")},
        {"metric":"history_sources_ok","value":summary_row.get("history_sources_ok")},
        {"metric":"history_sources_new_to_system","value":summary_row.get("history_sources_new_to_system")},
        {"metric":"history_sources_cached_entity","value":summary_row.get("history_sources_cached_entity")},
        {"metric":"history_sources_network_resolved","value":summary_row.get("history_sources_network_resolved")},
        {"metric":"history_fetch_runtime_seconds","value":summary_row.get("history_fetch_runtime_seconds")},
        {"metric":"posts_per_source_distribution","value":summary_row.get("posts_per_source_distribution")},
        {"metric":"vk_wall_probe_status","value":summary_row.get("vk_wall_probe_status")},
        {"metric":"telegram_phase_status","value":summary_row.get("telegram_phase_status")},
        {"metric":"telegram_floodwait_count","value":summary_row.get("telegram_floodwait_count")},
        {"metric":"telegram_max_floodwait_seconds","value":summary_row.get("telegram_max_floodwait_seconds")},
        {"metric":"telegram_floodwait_method","value":summary_row.get("telegram_floodwait_method")},
        {"metric":"telegram_cooldown_until","value":summary_row.get("telegram_cooldown_until")},
        {"metric":"telegram_resolve_cache_hits","value":summary_row.get("telegram_resolve_cache_hits")},
        {"metric":"telegram_resolve_network_attempts","value":summary_row.get("telegram_resolve_network_attempts")},
        {"metric":"entity_cache_loaded_from_path","value":summary_row.get("entity_cache_loaded_from_path")},
        {"metric":"entity_cache_hit_rate","value":summary_row.get("entity_cache_hit_rate")},
        {"metric":"resolved_sources_available_for_history_fetch","value":summary_row.get("resolved_sources_available_for_history_fetch")},
        {"metric":"telegram_sources_deferred_by_cooldown","value":summary_row.get("telegram_sources_deferred_by_cooldown")},
        {"metric":"telegram_sources_deferred_unresolved","value":summary_row.get("telegram_sources_deferred_unresolved")},
        {"metric":"telegram_similar_channels_status","value":summary_row.get("telegram_similar_channels_status")},
        {"metric":"telegram_similar_channels_seed_count","value":summary_row.get("telegram_similar_channels_seed_count")},
        {"metric":"telegram_similar_channels_raw_count","value":summary_row.get("telegram_similar_channels_raw_count")},
        {"metric":"telegram_similar_channels_unique_count","value":summary_row.get("telegram_similar_channels_unique_count")},
        {"metric":"telegram_similar_channels_added_to_frontier","value":summary_row.get("telegram_similar_channels_added_to_frontier")},
        {"metric":"similar_seed_queue_total","value":summary_row.get("similar_seed_queue_total")},
        {"metric":"similar_seed_queue_ready","value":summary_row.get("similar_seed_queue_ready")},
        {"metric":"similar_seed_queue_used_total","value":summary_row.get("similar_seed_queue_used_total")},
        {"metric":"similar_seed_cursor_advanced","value":summary_row.get("similar_seed_cursor_advanced")},
        {"metric":"telegram_keyword_discovery_status","value":summary_row.get("telegram_keyword_discovery_status")},
        {"metric":"keyword_search_queries_processed","value":summary_row.get("keyword_search_queries_processed")},
        {"metric":"keyword_discovered_sources_unique","value":summary_row.get("keyword_discovered_sources_unique")},
        {"metric":"dynamic_frontier_seed_count","value":summary_row.get("dynamic_frontier_seed_count")},
        {"metric":"source_frontier_unique_total","value":summary_row.get("source_frontier_unique_total")},
        {"metric":"source_frontier_new_this_run","value":summary_row.get("source_frontier_new_this_run")},
        {"metric":"catalog_import_rows_total","value":summary_row.get("catalog_import_rows_total")},
        {"metric":"catalog_sources_in_authoritative_frontier","value":summary_row.get("catalog_sources_in_authoritative_frontier")},
        {"metric":"frontier_duplicate_canonical_keys","value":summary_row.get("frontier_duplicate_canonical_keys")},
        {"metric":"source_frontier_ready_to_probe","value":summary_row.get("source_frontier_ready_to_probe")},
        {"metric":"source_frontier_promoted_this_run","value":summary_row.get("source_frontier_promoted_this_run")},
        {"metric":"source_frontier_ready_next_run","value":summary_row.get("source_frontier_ready_next_run")},
        {"metric":"source_frontier_deferred","value":summary_row.get("source_frontier_deferred")},
        {"metric":"sources_primary_scanned_total_all_time","value":summary_row.get("sources_primary_scanned_total_all_time")},
        {"metric":"telegram_sources_primary_scanned_total_all_time","value":summary_row.get("telegram_sources_primary_scanned_total_all_time")},
        {"metric":"vk_sources_primary_scanned_total_all_time","value":summary_row.get("vk_sources_primary_scanned_total_all_time")},
        {"metric":"sources_delta_scanned_this_run","value":summary_row.get("sources_delta_scanned_this_run")},
        {"metric":"sources_delta_scanned_total_all_time","value":summary_row.get("sources_delta_scanned_total_all_time")},
        {"metric":"sources_never_scanned_total","value":summary_row.get("sources_never_scanned_total")},
        {"metric":"frontier_total_by_platform","value":summary_row.get("frontier_total_by_platform")},
        {"metric":"frontier_pending_primary_scan_total","value":summary_row.get("frontier_pending_primary_scan_total")},
        {"metric":"frontier_pending_similar_scan_total","value":summary_row.get("frontier_pending_similar_scan_total")},
        {"metric":"posts_memory_total","value":summary_row.get("posts_memory_total")},
        {"metric":"candidates_memory_total","value":summary_row.get("candidates_memory_total")},
        {"metric":"publication_ready_total_all_time","value":summary_row.get("publication_ready_total_all_time")},
        {"metric":"llm_limit_source","value":llm_limit_snapshot.get("llm_limit_source")},
        {"metric":"llm_model","value":llm_model},
        {"metric":"llm_default_env_var_name","value":llm_default_env_var_name},
        {"metric":"image_scoring_mode","value":current_image_scoring_mode()},
    ]

    compact_current_run_shortlist = [compact_shortlist_row(r) for r in final_shortlist]
    compact_final_shortlist = [candidate_memory_shortlist_row(r, i) for i, r in enumerate(candidate_memory_top, start=1)] or compact_current_run_shortlist
    manual_review_queue = candidate_memory_top or compact_current_run_shortlist or [{"_sheet_note":"no active candidate memory/manual review rows yet"}]
    candidate_memory_sheet = candidate_memory_rows or [{"_sheet_note":"candidate memory is empty; no current or previous candidate rows reached memory criteria"}]
    candidate_memory_top_sheet = candidate_memory_top or [{"_sheet_note":"no active candidate memory rows"}]
    previous_not_refetched_sheet = previous_candidates_not_refetched or [{"_sheet_note":"no previous active candidates missed by this run"}]
    candidate_deltas_sheet = candidate_deltas or [{"_sheet_note":"no candidate deltas yet"}]
    source_frontier_queue_sheet = source_frontier_queue_next or [{"_sheet_note":"no source frontier queue rows"}]
    image_retry_queue = [r for r in new_posts if r.get("current_stage") == "image_fetch_retry_needed" or r.get("image_status") == "needs_actual_image_fetch"]
    seen_retry = {str(r.get("post_url") or r.get("candidate_memory_id") or "") for r in image_retry_queue}
    for r in candidate_memory_top:
        key = str(r.get("post_url") or r.get("candidate_memory_id") or "")
        if key not in seen_retry and (r.get("current_stage") == "image_fetch_retry_needed" or r.get("image_status") == "needs_actual_image_fetch"):
            image_retry_queue.append(r)
            seen_retry.add(key)
    image_retry_queue_sheet = image_retry_queue or [{"_sheet_note":"no metadata-only/image-fetch retry rows"}]
    llm_usage_by_stage = [
        {"stage":"broad_fetched_posts","rows_considered":len(new_posts),"llm_calls":summary_row.get("wide_funnel_llm_calls"),"allowed":"false","reason":"vector/deterministic only before compact shortlist","budget_cap":0},
        {"stage":"review_queue","rows_considered":len(review_queue),"llm_calls":0,"allowed":"false","reason":"wide review queue remains vector/local; no broad LLM classifier","budget_cap":0},
        {"stage":"candidate_memory_active","rows_considered":len(candidate_memory_active_rows),"llm_calls":0,"allowed":"true","reason":"eligible for future final verifier only; previous rows may lack text in MVP state","budget_cap":max_llm_final_verify},
        {"stage":"final_shortlist_top_n","rows_considered":len(compact_final_shortlist),"llm_calls":summary_row.get("final_verifier_llm_calls"),"allowed":"true","reason":"final verifier/tie-breaker only","budget_cap":max_llm_final_verify},
    ]
    vk_wall_setup_sheet = [{
        "status": summary_row.get("vk_wall_probe_status"),
        "needed_env": "VK_SERVICE_TOKEN or VK_ACCESS_TOKEN with wall read access",
        "publish_blocker": "VK publishing remains disabled in MVP; read-only wall probe is separate",
        "frontier_vk_rows": sum(1 for r in source_frontier_unique if str(r.get("platform") or "") == "vk"),
        "next_action": "configure read-only VK wall token or keep explicit blocker",
    }]
    favorites_sheet = [
        {"favorite_id": "fav_" + stable_hash(r.get("candidate_id"), r.get("post_url")), "why_selected": "favorite stage from vector/image ranking", "publication_readiness": r.get("image_publication_ready"), **r}
        for r in favorites
    ] or [{"_sheet_note":"no favorites in this run"}]
    candidates_sheet = candidates or [{"_sheet_note":"no final candidates in this run"}]
    favorites_non_placeholder = [r for r in favorites_sheet if r.get("post_url") and not r.get("_sheet_note")]
    candidates_non_placeholder = [r for r in candidates_sheet if r.get("post_url") and not r.get("_sheet_note")]
    consistency_errors = []
    if len(favorites) != len(favorites_non_placeholder):
        consistency_errors.append(f"favorites summary={len(favorites)} sheet={len(favorites_non_placeholder)}")
    if len(candidates) != len(candidates_non_placeholder):
        consistency_errors.append(f"candidates summary={len(candidates)} sheet={len(candidates_non_placeholder)}")
    summary_row["favorites_candidates_consistency_status"] = "ok" if not consistency_errors else "error: " + "; ".join(consistency_errors)
    product_summary.append({"metric":"favorites_candidates_consistency_status","value":summary_row["favorites_candidates_consistency_status"]})
    telegram_observability_rows = [tg_obs] if tg_obs else [{"run_id":run_id,"telegram_phase_status":"not_configured"}]
    similar_sheet_rows = similar_channel_rows or [{
        "run_id": run_id, "seed_source_id": "", "seed_channel_url": "", "seed_channel_title": "",
        "method_status": _REGION_TALK_TELEGRAM_RUNTIME.get("telegram_similar_channels_status", "not_run"),
        "method_error_code": _REGION_TALK_TELEGRAM_RUNTIME.get("telegram_similar_channels_error", ""),
        "method_error_message_short": "", "recommendation_rank": "", "recommended_title": "",
        "recommended_username": "", "recommended_canonical_url": "", "edge_type": "telegram_similar_channel",
        "frontier_action": "none", "frontier_reason": "no Telegram similar channel rows in this run",
    }]
    similar_seed_queue_sheet = similar_seed_queue or [{"_sheet_note":"no persistent similar-channel seeds yet"}]
    keyword_discovery_sheet = keyword_discovery_rows or [{"_sheet_note":"no Telegram keyword discovery rows in this run", "method_status": _REGION_TALK_TELEGRAM_RUNTIME.get("telegram_keyword_discovery_status", "not_run")}]
    source_classification_sheet = [
        {k: r.get(k, "") for k in ["source_id", "source_title", "canonical_url", "platform", "fetch_status", "source_geo_class", "source_topic_class", "ko_mention_ratio_recent", "travel_blogger_score", "personal_voice_score", "nonlocal_value_score", "source_priority_reason"]}
        for r in source_rows
    ] or [{"_sheet_note":"no source classification rows"}]
    sheets = {
        "00_product_summary":product_summary,
        "00_readme":[{"field":"what","value":"Region Talk MVP-1.x Candidate Report Only; vector/local gates first, image scoring for non-ad Kaliningrad rows, optional final LLM verifier via Supabase google_ai limiter; no Telegram/VK publishing."},{"field":"run_id","value":run_id},{"field":"generated_at","value":run_now}],
        "01_run_summary":run_summary,
        "02_increment":increment,
        "03_funnel":funnel,
        "03b_gate_counts":independent_gate_counts,
        "04_review_queue":review_queue,
        "04a_final_shortlist":compact_final_shortlist,
        "04a_current_run_shortlist":compact_current_run_shortlist,
        "04a_final_shortlist_raw":candidate_memory_top or final_shortlist,
        "04b_needs_llm_retry":llm_error_sheet_rows,
        "04c_debug_rejects":debug_rejects,
        "05_favorites":favorites_sheet,
        "06_candidates_all":candidates_sheet,
        "06a_candidate_memory":candidate_memory_sheet,
        "06b_candidate_memory_top":candidate_memory_top_sheet,
        "07_new_posts_this_run":[r for r in new_posts if any(inc.get("entity_id") == r.get("post_id") and inc.get("new_this_run") == "yes" for inc in increment)],
        "07_current_run_posts":new_posts,
        "07b_prev_candidates_not_refetch":previous_not_refetched_sheet,
        "08_dropped_posts":dropped,
        "09_image_quality":media_rows,
        "09b_image_fetch_retry_queue":image_retry_queue_sheet,
        "10_good_text_weak_media":[r for r in dropped if r.get("current_stage") == "good_text_weak_media"],
        "11_sources_seed":source_seed_rows,
        "12_sources_discovered":discovered_rows,
        "12a_source_frontier_unique":source_frontier_unique,
        "12b_telegram_similar_channels":similar_sheet_rows,
        "12c_source_frontier_queue_next":source_frontier_queue_sheet,
        "12d_similar_seed_queue":similar_seed_queue_sheet,
        "12e_telegram_keyword_discovery":keyword_discovery_sheet,
        "12f_source_classification":source_classification_sheet,
        "13_sources_monitored":source_rows,
        "13b_source_delta_scan":source_delta_scan_sheet,
        "14_verifier_reports":[],
        "14b_pre_candidates_needing_llm":[r for r in pre_candidates if r.get("current_stage") == "pre_candidate_needs_llm"],
        "14c_llm_errors":llm_error_sheet_rows,
        "14d_llm_usage_by_stage":llm_usage_by_stage,
        "15_manual_decisions":[{"candidate_id":"","manual_decision":"favorite|reject|approve_for_preview|approve_for_queue|block_source","reviewer":"","reviewed_at":"","reviewer_comment":"","rights_override":"","source_status_override":""}],
        "16_publish_preview_future":[{"note":"Future only. REGION_TALK_DISABLE_PUBLISH=1; no real publishing in MVP-1.x."}],
        "17_source_graph_edges":graph_edges,
        "18_place_lexicon_matches":place_match_rows,
        "19_image_model_observability":image_model_observability,
        "20_telegram_rate_observability":telegram_observability_rows,
        "21_manual_review_queue":manual_review_queue,
        "22_candidate_deltas":candidate_deltas_sheet,
        "23_vk_wall_setup":vk_wall_setup_sheet,
        "24_source_yield_metrics":source_yield_metrics_sheet,
    }
    xlsx = output_dir / f"region-talk-candidates-{run_id}.xlsx"
    write_xlsx(xlsx, sheets)
    candidate_event_rows = []
    def _candidate_event(stage: str, r: dict[str, Any]) -> dict[str, Any]:
        return {
            "event_type": stage,
            "run_id": run_id,
            "event_at": run_now,
            "source_id": r.get("source_id"),
            "source_title": r.get("source_title"),
            "source_url": r.get("source_url"),
            "post_id": r.get("post_id"),
            "post_url": r.get("post_url"),
            "post_date": r.get("post_date"),
            "matched_place_names": r.get("matched_place_names"),
            "content_type": r.get("content_type") or r.get("llm_content_type") or r.get("vector_content_type"),
            "candidate_score": r.get("candidate_score_current") or r.get("candidate_score") or r.get("best_candidate_score_ever"),
            "media_score": r.get("media_score_current") or r.get("overall_media_score") or r.get("best_media_score_ever"),
            "stage": r.get("current_stage") or r.get("decision_bucket"),
            "next_action": r.get("next_action") or r.get("suggested_action"),
            "short_summary": r.get("short_summary") or r.get("why_keep_in_memory") or r.get("why_this_is_about_kaliningrad"),
        }
    for r in new_posts:
        if r.get("kaliningrad_oblast_only_scope"):
            candidate_event_rows.append(_candidate_event("fresh_ko_post_found" if r.get("fresh_enough") else "new_source_with_ko_post", r))
        if r.get("current_stage") in {"pre_candidate_needs_llm", "semantic_candidate", "favorite", "needs_image_review", "image_fetch_retry_needed", "good_text_weak_media"}:
            candidate_event_rows.append(_candidate_event("pre_candidate_created", r))
        if r.get("current_stage") == "needs_image_review":
            candidate_event_rows.append(_candidate_event("image_reviewable_candidate", r))
        if r.get("current_stage") == "image_fetch_retry_needed":
            candidate_event_rows.append(_candidate_event("image_fetch_retry_needed", r))
        if str(r.get("image_publication_ready") or "").lower() == "true" and r.get("image_model_input_type") == "actual_image":
            candidate_event_rows.append(_candidate_event("publication_ready_candidate", r))
    if not candidate_event_rows:
        for r in (candidate_memory_top or final_shortlist)[:100]:
            candidate_event_rows.append(_candidate_event("candidate_found", r))
    (output_dir / "candidate_found.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in candidate_event_rows) + ("\n" if candidate_event_rows else ""), encoding="utf-8")
    run_event_rows = [
        {"event_type":"run_started","run_id":run_id,"created_at":RUN_STARTED_AT.isoformat()},
        {"event_type":"report_written","run_id":run_id,"created_at":run_now,"xlsx_path":str(xlsx),"posts_fetched":len(posts),"candidate_events":len(candidate_event_rows)},
    ]
    (output_dir / "run_events.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in run_event_rows) + "\n", encoding="utf-8")
    (output_dir / "stage_status.json").write_text(json.dumps({"run_id":run_id,"generated_at":run_now,"stage_counts":stage_counts,"summary":summary_row}, ensure_ascii=False, indent=2), encoding="utf-8")
    write_csv(output_dir / f"region-talk-candidates-{run_id}.csv", review_queue)
    payload = {"ok": True, "run_id": run_id, "generated_at": run_now, "summary": run_summary[0], "sheets": sheets, "xlsx_path": str(xlsx)}
    (output_dir / f"region-talk-candidates-{run_id}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / f"region-talk-candidates-{run_id}.md").write_text(render_md(payload), encoding="utf-8")
    (output_dir / f"region-talk-candidates-{run_id}.html").write_text(render_html(payload), encoding="utf-8")
    for p in [xlsx, output_dir / f"region-talk-candidates-{run_id}.json", output_dir / f"region-talk-candidates-{run_id}.md", output_dir / f"region-talk-candidates-{run_id}.html", output_dir / f"region-talk-candidates-{run_id}.csv", output_dir / "candidate_found.jsonl", output_dir / "run_events.jsonl", output_dir / "stage_status.json"]:
        target = Path.cwd() / p.name
        if target.resolve() != p.resolve():
            target.write_bytes(p.read_bytes())
    latest = Path.cwd() / "candidates-latest.xlsx"
    latest.write_bytes(xlsx.read_bytes())
    (Path.cwd() / "output.json").write_text(json.dumps({"ok": True, "run_id": run_id, "xlsx": str(latest), "summary": run_summary[0]}, ensure_ascii=False, indent=2), encoding="utf-8")
    payload["latest_xlsx"] = str(latest)
    return payload

def make_summary(text: str) -> str:
    clean = re.sub(r"\s+", " ", text or "").strip()
    if len(clean) <= 180:
        return clean
    return clean[:177].rsplit(" ",1)[0] + "…"


def rows_headers(rows: list[dict[str, Any]]) -> list[str]:
    headers: list[str] = []
    for r in rows:
        for k in r.keys():
            if k not in headers:
                headers.append(k)
    return headers or ["empty"]


def col_name(n: int) -> str:
    s = ""
    while n:
        n, rem = divmod(n-1, 26)
        s = chr(65+rem) + s
    return s


def cell_xml(value: Any, row: int, col: int) -> str:
    ref = f"{col_name(col)}{row}"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f'<c r="{ref}"><v>{value}</v></c>'
    text = escape(str(value if value is not None else ""))
    return f'<c r="{ref}" t="inlineStr"><is><t>{text}</t></is></c>'


def sheet_xml(rows: list[dict[str, Any]]) -> str:
    headers = rows_headers(rows)
    all_rows = [dict(zip(headers, headers))] + rows
    body=[]
    for ri, r in enumerate(all_rows, start=1):
        cells = ''.join(cell_xml(r.get(h, ""), ri, ci) for ci, h in enumerate(headers, start=1))
        body.append(f'<row r="{ri}">{cells}</row>')
    last = f"{col_name(len(headers))}{max(1,len(all_rows))}"
    return f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews><dimension ref="A1:{last}"/><sheetData>{''.join(body)}</sheetData><autoFilter ref="A1:{col_name(len(headers))}1"/></worksheet>'''



def excel_safe_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    if isinstance(value, (list, dict)):
        text = json.dumps(value, ensure_ascii=False)
    else:
        text = str(value)
    try:
        from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE  # type: ignore
        text = ILLEGAL_CHARACTERS_RE.sub("", text)
    except Exception:
        text = re.sub(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]", "", text)
    if text.startswith(("=", "+", "-", "@")):
        text = "'" + text
    if len(text) > 32767:
        text = text[:32700] + "… [truncated for Excel]"
    return text


def write_xlsx_openpyxl(path: Path, sheets: dict[str, list[dict[str, Any]]]) -> None:
    try:
        from openpyxl import Workbook  # type: ignore
        from openpyxl.styles import Alignment, Border, Font, PatternFill, Side  # type: ignore
        from openpyxl.utils import get_column_letter  # type: ignore
    except Exception:
        if getenv_bool("REGION_TALK_AUTO_INSTALL", True):
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "openpyxl"])
            from openpyxl import Workbook  # type: ignore
            from openpyxl.styles import Alignment, Border, Font, PatternFill, Side  # type: ignore
            from openpyxl.utils import get_column_letter  # type: ignore
        else:
            raise
    path.parent.mkdir(parents=True, exist_ok=True)
    wb = Workbook()
    wb.remove(wb.active)
    header_fill = PatternFill("solid", fgColor="1F4E78")
    header_font = Font(color="FFFFFF", bold=True)
    thin = Side(style="thin", color="D9E2F3")
    header_border = Border(bottom=thin)
    used_titles: set[str] = set()
    for sheet_name, raw_rows in sheets.items():
        title = re.sub(r"[:\\/?*\[\]]", "_", str(sheet_name))[:31] or "sheet"
        base_title = title
        i = 2
        while title in used_titles:
            suffix = f"_{i}"
            title = (base_title[:31-len(suffix)] + suffix)[:31]
            i += 1
        used_titles.add(title)
        rows = raw_rows if isinstance(raw_rows, list) else [{"value": raw_rows}]
        ws = wb.create_sheet(title=title)
        headers = rows_headers([r for r in rows if isinstance(r, dict)])
        headers = [h for h in headers if h != "place_matches"] or ["empty"]
        ws.append(headers)
        widths = {idx: len(str(header)) for idx, header in enumerate(headers, start=1)}
        for raw in rows:
            row = raw if isinstance(raw, dict) else {"value": raw}
            values = [excel_safe_value(row.get(header, "")) for header in headers]
            ws.append(values)
            for idx, value in enumerate(values, start=1):
                widths[idx] = min(max(widths.get(idx, 0), len(str(value))), 80)
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.border = header_border
            cell.alignment = Alignment(wrap_text=True, vertical="top")
        for row_cells in ws.iter_rows(min_row=2):
            for cell in row_cells:
                cell.alignment = Alignment(wrap_text=True, vertical="top")
        ws.freeze_panes = "A2"
        if ws.max_row >= 1 and ws.max_column >= 1:
            ws.auto_filter.ref = ws.dimensions
        for idx, width in widths.items():
            ws.column_dimensions[get_column_letter(idx)].width = max(10, min(width + 2, 60))
    wb.properties.creator = "events-bot-new / Region Talk"
    wb.properties.title = "Region Talk MVP-1.x candidate report"
    wb.properties.subject = "Excel-safe openpyxl workbook"
    wb.save(path)


def write_xlsx(path: Path, sheets: dict[str, list[dict[str, Any]]]) -> None:
    try:
        write_xlsx_openpyxl(path, sheets)
    except Exception as exc:
        print(f"[region-talk] openpyxl_xlsx_writer_failed fallback=minimal error={type(exc).__name__}: {str(exc)[:160]}", flush=True)
        write_xlsx_minimal(path, sheets)

def write_xlsx_minimal(path: Path, sheets: dict[str, list[dict[str, Any]]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    names = list(sheets.keys())
    sheet_overrides = ''.join(
        f'<Override PartName="/xl/worksheets/sheet{i}.xml" '
        f'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        for i in range(1, len(names) + 1)
    )
    sheet_relationships = ''.join(
        f'<Relationship Id="rId{i}" '
        f'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        f'Target="worksheets/sheet{i}.xml"/>'
        for i in range(1, len(names) + 1)
    )
    styles_rid = len(names) + 1
    created_at = escape(RUN_STARTED_AT.replace(microsecond=0).isoformat().replace('+00:00', 'Z'))
    with zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr('[Content_Types].xml', f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/><Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/><Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>{sheet_overrides}</Types>''')
        z.writestr('_rels/.rels', '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/></Relationships>''')
        z.writestr('docProps/core.xml', f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><dc:creator>events-bot-new</dc:creator><dc:title>Region Talk MVP-1 candidate report</dc:title><dcterms:created xsi:type="dcterms:W3CDTF">{created_at}</dcterms:created><dcterms:modified xsi:type="dcterms:W3CDTF">{created_at}</dcterms:modified></cp:coreProperties>''')
        z.writestr('docProps/app.xml', f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes"><Application>events-bot-new</Application><DocSecurity>0</DocSecurity><ScaleCrop>false</ScaleCrop><HeadingPairs><vt:vector size="2" baseType="variant"><vt:variant><vt:lpstr>Worksheets</vt:lpstr></vt:variant><vt:variant><vt:i4>{len(names)}</vt:i4></vt:variant></vt:vector></HeadingPairs><TitlesOfParts><vt:vector size="{len(names)}" baseType="lpstr">{''.join(f'<vt:lpstr>{escape(name[:31])}</vt:lpstr>' for name in names)}</vt:vector></TitlesOfParts></Properties>''')
        z.writestr('xl/styles.xml', '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="1"><font><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/><scheme val="minor"/></font></fonts><fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills><borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders><cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs><cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs><cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles><dxfs count="0"/><tableStyles count="0" defaultTableStyle="TableStyleMedium2" defaultPivotStyle="PivotStyleLight16"/></styleSheet>''')
        z.writestr('xl/workbook.xml', '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>' + ''.join(f'<sheet name="{escape(name[:31])}" sheetId="{i}" r:id="rId{i}"/>' for i,name in enumerate(names,1)) + '</sheets></workbook>')
        z.writestr('xl/_rels/workbook.xml.rels', f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">{sheet_relationships}<Relationship Id="rId{styles_rid}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>''')
        for i, name in enumerate(names, 1):
            z.writestr(f'xl/worksheets/sheet{i}.xml', sheet_xml(sheets[name]))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    headers = rows_headers(rows)
    with path.open('w', encoding='utf-8', newline='') as f:
        w=csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
        w.writeheader(); w.writerows(rows)


def render_md(payload: dict[str, Any]) -> str:
    s = payload.get('summary', {})
    return f"# Region Talk Candidate Report\n\n- run_id: `{payload.get('run_id')}`\n- posts_fetched: **{s.get('posts_fetched',0)}**\n- candidates_created: **{s.get('candidates_created',0)}**\n- favorites_created: **{s.get('favorites_created',0)}**\n- xlsx: `{payload.get('latest_xlsx') or payload.get('xlsx_path')}`\n"


def render_html(payload: dict[str, Any]) -> str:
    return '<!doctype html><meta charset="utf-8"><pre>' + html.escape(render_md(payload)) + '</pre>'


async def amain() -> int:
    load_dotenv(Path('.env'))
    config = load_split_runtime_from_kaggle_input()
    for k, v in (config.get('env') or {}).items():
        if v is not None:
            os.environ.setdefault(str(k), str(v))
    run_id = os.getenv('REGION_TALK_RUN_ID') or str(config.get('run_id') or f"region-talk-{RUN_STARTED_AT.strftime('%Y%m%dT%H%M%SZ')}")
    if not getenv_bool('REGION_TALK_DRY_RUN', True):
        raise RuntimeError('REGION_TALK_DRY_RUN=1 is required for MVP-1')
    if not getenv_bool('REGION_TALK_DISABLE_PUBLISH', True):
        raise RuntimeError('REGION_TALK_DISABLE_PUBLISH=1 is required for MVP-1')
    status = Status()
    status.event('kernel_started', phase='start', status='running', run_id=run_id)
    seed_path = find_seed_file(config)
    seeds = load_seeds(seed_path)
    out_dir = Path(os.getenv('REGION_TALK_OUTPUT_DIR') or str(config.get('output_dir') or f"artifacts/region-talk/runs/{run_id}"))
    status.event('preflight_ok', phase='preflight', status='running', run_id=run_id, seeds=len(seeds), seed_file=str(seed_path), dry_run=True, disable_publish=True)
    source_rows, posts = await fetch_telegram_posts(seeds, status, out_dir)
    status.event('posts_fetched', phase='fetch', status='running', sources_scanned=len(source_rows), posts_fetched=len(posts))
    payload = build_report(seeds, source_rows, posts, run_id, out_dir)
    summary = payload.get('summary', {})
    status.event('report_written', phase='report', status='done', run_id=run_id, posts_fetched=summary.get('posts_fetched'), candidates_created=summary.get('candidates_created'), favorites_created=summary.get('favorites_created'), xlsx=str(payload.get('latest_xlsx')))
    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(amain()))
