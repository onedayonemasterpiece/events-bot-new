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
NEWS_WORDS = ["происшеств", "дтп", "авар", "полици", "суд", "задерж", "штраф", "войн", "полит", "скандал", "убий", "пожар"]
AD_WORDS = ["скидк", "промокод", "купить", "заказать", "реклама", "партнёр", "партнер", "оплат", "бронь", "регистрация", "анонс", "конкурс", "диктант", "географический диктант", "билеты", "забронировать"]
TRASH_WORDS = ["жесть", "треш", "трэш", "шок", "кошмар"]
POSITIVE_WORDS = ["красив", "атмосфер", "море", "дюны", "архитект", "истори", "маршрут", "музей", "пляж", "курорт", "прогул", "путешеств"]

_REGION_TALK_SUPABASE_CLIENT: Any | None = None
_REGION_TALK_GOOGLE_CLIENT: Any | None = None
_REGION_TALK_CLIP_MODEL: Any | None = None
_REGION_TALK_CLIP_PROCESSOR: Any | None = None
_REGION_TALK_CLIP_DEVICE: str | None = None



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
        return url.strip()
    h = canonical_handle(handle).lstrip("@")
    if not h:
        return ""
    if platform == "telegram":
        return f"https://t.me/{h}"
    if platform in {"vk", "vkvideo"}:
        return f"https://vk.com/{h}"
    return h


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
            secrets = json.loads(Fernet(key_files[0].read_bytes().strip()).decrypt(secret_files[0].read_bytes()).decode("utf-8"))
            for k, v in secrets.items():
                if v is not None and str(v).strip():
                    os.environ.setdefault(str(k), str(v))
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
SUBSTANCE_WORDS = ["маршрут", "дорога", "путь", "добраться", "совет", "полезн", "истори", "место", "что посмотреть", "где", "когда", "почему"]
VISIT_WORDS = ["побывал", "побывали", "посетил", "посетили", "ездили", "поехали", "приехали", "гуляли", "увидели", "запомнил", "запомнилось", "впечатлен", "впечатления"]
EMOTION_WORDS = ["красив", "впечатля", "атмосфер", "удивител", "люблю", "очар", "запомни", "магия", "спокойн", "вдохнов", "вау", "эмоци"]
MEMORABLE_WORDS = ["больше всего", "особенно", "запомни", "неожиданно", "удивило", "главное", "лучшее", "самое", "деталь", "история"]
URL_RE = re.compile(r"(?i)\b(?:https?://|www\.)\S+|\bt\.me/[A-Za-z0-9_+/.-]+|\bvk\.com/[A-Za-z0-9_./-]+")
HANDLE_RE = re.compile(r"(?<!\w)@[A-Za-z0-9_]{4,}")


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
                    "accepted_as_region_evidence": "false" if requires_context else "true",
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
    hits = [w.strip() for w in AD_PROMO_WORDS if w.strip() and w in low]
    is_ad = bool(hits)
    return {"is_ad_or_promo": is_ad, "ad_promo_hits": "; ".join(sorted(set(hits))), "ad_promo_reason": "reject: ad/promo/announcement cues: " + "; ".join(sorted(set(hits))) if is_ad else "accepted: no hard ad/promo cues"}


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
    }


def current_image_scoring_mode() -> str:
    return (os.getenv("REGION_TALK_IMAGE_SCORING_MODE") or "cv_aesthetic_clip").strip().lower() or "cv_aesthetic_clip"


def _metadata_media_scores(has_media: bool, text_score: dict[str, Any], *, reason_prefix: str = "") -> dict[str, Any]:
    if not has_media:
        return {"technical_quality_score":0.0,"aesthetic_score":0.0,"postcardness_score":0.0,"region_visual_relevance_score":0.0,"publication_safety_score":1.0,"low_noise_score":0.0,"overall_media_score":0.0,"is_selected_for_publication":False,"image_publication_ready":"false","image_reviewable":"false","image_quality_bucket":"no_media","recognized_visual_elements":"","model_short_explanation":"No media detected; image gate failed.","failure_reason":"no_media","model_id":"cv_only_metadata_v1","model_version":"2026-07-05","image_model_type":"cv_only","image_model_runtime":"kaggle_local","image_model_input_type":"metadata_only","image_scoring_mode": current_image_scoring_mode()}
    anchors = text_score.get("anchor_hits") or []
    positives = text_score.get("positive_hits") or []
    technical = 0.72
    aesthetic = min(0.92, 0.68 + 0.04*len(positives))
    postcard = min(0.94, 0.66 + 0.06*len(anchors) + 0.03*len(positives))
    region_visual = min(0.95, 0.58 + 0.08*len(anchors))
    safety = 0.98 if not text_score.get("news_hits") and not text_score.get("trash_hits") else 0.78
    low_noise = 0.82
    overall = round((technical+aesthetic+postcard+region_visual+safety+low_noise)/6,3)
    reviewable = technical>=0.55 and postcard>=0.60 and safety>=0.80 and overall>=0.62
    selected = technical>=0.65 and aesthetic>=0.70 and postcard>=0.72 and safety>=0.95 and low_noise>=0.80 and overall>=0.75
    elements = []
    low = " ".join(anchors + positives).lower()
    if "море" in low or "балтий" in low: elements.append("sea/coast")
    if "курш" in low or "дюн" in low: elements.append("dunes/nature")
    if "архитект" in low or "кёниг" in low or "калининград" in low: elements.append("city/architecture")
    if not elements: elements.append("travel visual candidate")
    prefix = (reason_prefix + "; ") if reason_prefix else ""
    return {"technical_quality_score":round(technical,3),"aesthetic_score":round(aesthetic,3),"postcardness_score":round(postcard,3),"region_visual_relevance_score":round(region_visual,3),"publication_safety_score":round(safety,3),"low_noise_score":round(low_noise,3),"overall_media_score":overall,"is_selected_for_publication":selected,"image_publication_ready":str(selected).lower(),"image_reviewable":str(reviewable).lower(),"image_quality_bucket":"publication_ready" if selected else ("reviewable_image" if reviewable else "weak_image"),"recognized_visual_elements":"; ".join(elements),"model_short_explanation":prefix + f"cv_only metadata fallback: media present; region anchors={len(anchors)}, travel/visual cues={len(positives)}; safety={'ok' if safety>=0.95 else 'blocked by news/trash cues'}.","failure_reason":"" if selected else ("reviewable_below_publication_threshold" if reviewable else "below_reviewable_image_threshold"),"model_id":"cv_only_metadata_v1","model_version":"2026-07-05","image_model_type":"cv_only","image_model_runtime":"kaggle_local","image_model_input_type":"metadata_only","image_scoring_mode": current_image_scoring_mode()}


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
    }


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
    monitored = [s for s in seeds if s.platform == "telegram" and s.monitoring_enabled]
    monitored = sorted(monitored, key=lambda s: (s.priority, seed_sort_number(s.source_seed_id)))[:max_sources]
    source_rows: list[dict[str, Any]] = []
    posts: list[dict[str, Any]] = []
    if not fetch_enabled:
        for s in monitored:
            source_rows.append({**asdict(s), "source_id": s.source_id, "canonical_url": s.canonical_url, "fetch_status":"skipped_fetch_disabled"})
        return source_rows, posts
    try:
        from telethon import TelegramClient  # type: ignore
        from telethon.sessions import StringSession  # type: ignore
    except Exception:
        if getenv_bool("REGION_TALK_AUTO_INSTALL", True):
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "telethon"])
            from telethon import TelegramClient  # type: ignore
            from telethon.sessions import StringSession  # type: ignore
        else:
            for s in monitored:
                source_rows.append({**asdict(s), "source_id": s.source_id, "canonical_url": s.canonical_url, "fetch_status":"skipped_telethon_not_installed"})
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
            source_rows.append({**asdict(s), "source_id": s.source_id, "canonical_url": s.canonical_url, "fetch_status":"skipped_missing_telethon_credentials"})
        return source_rows, posts
    client = TelegramClient(StringSession(session), api_id, api_hash, device_model=str(bundle.get("device_model") or "Region Talk Discovery"), system_version=str(bundle.get("system_version") or "Linux"), app_version=str(bundle.get("app_version") or "1.0"), lang_code=str(bundle.get("lang_code") or "ru"), system_lang_code=str(bundle.get("system_lang_code") or "ru"))
    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        raise RuntimeError("Telethon client is not authorized")
    try:
        for idx, seed in enumerate(monitored, start=1):
            status.event("alive", phase="fetch", progress_label=f"источники {idx}/{len(monitored)}", sources_done=idx-1, sources_total=len(monitored))
            handle = seed.handle.lstrip("@")
            src_row = {**asdict(seed), "source_id": seed.source_id, "canonical_url": seed.canonical_url, "fetch_status":"ok", "posts_scanned":0}
            try:
                entity = await client.get_entity(handle)
                title = str(getattr(entity, "title", None) or seed.source_title)
                src_row["resolved_title"] = title
                seen: set[int] = set()
                queries = [None] + DEFAULT_ANCHORS[:5]
                for q in queries:
                    per_query = max(3, max_posts // len(queries) + 1)
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
                        if has_media and getenv_bool("REGION_TALK_DOWNLOAD_MEDIA_FOR_SCORING", True):
                            try:
                                media_dir = output_dir / "media" / stable_hash("telegram", handle)
                                media_dir.mkdir(parents=True, exist_ok=True)
                                downloaded = await client.download_media(msg, file=str(media_dir / f"{mid}"))
                                if downloaded:
                                    primary_media_path = str(downloaded)
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
                src_row["posts_scanned"] = len(seen)
            except Exception as exc:
                src_row["fetch_status"] = "error"
                src_row["fetch_error_code"] = type(exc).__name__
                src_row["fetch_error_message"] = str(exc)[:180]
            source_rows.append(src_row)
    finally:
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
            "Encyclopedic cards may be reviewable but weaker; pure visual dumps or passing mentions are not candidates.",
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
            "is_multi_topic_digest": "boolean",
            "is_ad_or_promo": "boolean",
            "is_news_or_trash": "boolean",
            "content_type": "visit_impression_candidate|route_useful_candidate|encyclopedic_card_candidate|low_substance|reject",
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
            "is_multi_topic_digest": str(bool(data.get("is_multi_topic_digest"))).lower(),
            "llm_is_ad_or_promo": str(bool(data.get("is_ad_or_promo"))).lower(),
            "llm_is_news_or_trash": str(bool(data.get("is_news_or_trash"))).lower(),
            "llm_content_type": str(data.get("content_type") or ""),
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

        semantic_gate_mode = (os.getenv("REGION_TALK_SEMANTIC_GATE_MODE") or "llm_required").strip().lower()
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

        llm_gate = {
            "llm_gate_status": "not_run", "llm_provider": "google_gemini", "llm_model": llm_model, "llm_default_env_var_name": llm_default_env_var_name, "llm_limit_source": llm_limit_snapshot.get("llm_limit_source", "supabase_google_ai"), "llm_decision": "",
            "whole_post_about_kaliningrad_oblast_score": "", "kaliningrad_mention_role": "",
            "is_digest_or_roundup": "", "is_multi_topic_digest": "", "llm_is_ad_or_promo": "",
            "llm_is_news_or_trash": "", "llm_content_type": "", "llm_reason": "",
            "llm_usage_input_tokens": "", "llm_usage_output_tokens": "", "llm_usage_total_tokens": "",
        }
        llm_required = semantic_gate_mode in {"llm_required", "llm", "semantic_required"} and not deterministic_override
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
                if decision == "reject":
                    drop_gate, rejection = "llm_semantic_gate", "llm_reject"
                    visual_skip_reason = str(llm_gate.get("llm_reason") or "LLM rejected semantic fit")
                    current_stage = "dropped_text_gate"
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
                drop_gate, rejection = "pre_llm_cost_guard", "debug_reject_pre_llm_guard"
                visual_skip_reason = "Skipped before LLM to avoid spending Supabase quota on obvious non-region/ad/low-substance rows"
                current_stage = "debug_reject"
            else:
                drop_gate, rejection = "llm_semantic_gate", "semantic_gate_not_run"
                visual_skip_reason = "LLM semantic gate not run; row remains reviewable pre-candidate and image scoring is skipped"
                current_stage = "pre_candidate_needs_llm"
        elif not rejection and deterministic_override:
            current_stage = "pre_candidate_debug_deterministic"
            gate_trace.append("deterministic_semantic_override:pre_candidate_only")

        if rejection and current_stage in {"pre_candidate_needs_llm", "needs_llm_retry", "pre_candidate_debug_deterministic"}:
            ms = image_scores_skipped(visual_skip_reason or rejection)
            score = 0.0
        elif rejection:
            ms = image_scores_skipped(visual_skip_reason or rejection)
            score = 0.0
            current_stage = "dropped_text_gate"
        else:
            gate_trace.append("semantic_dual_model_enrichment:llm_semantic_gate_passed")
            visual_stage = "scored_after_llm_text_gates"
            visual_skip_reason = ""
            image_cost_saved = False
            ms = media_scores(bool(p.get("has_media")), ts, p)
            media_rows.append({"media_id": mid, "candidate_id": cid, "post_url": p.get("post_url"), "image_url_or_local_path": p.get("primary_media_path") or ((p.get("post_url") + "#media") if p.get("has_media") else ""), "thumbnail":"", **ms})
            score = candidate_score(ts, ms, seed_for_post) if seed_for_post else 0.0
            if not ms["is_selected_for_publication"]:
                current_stage, rejection, drop_gate = "needs_image_review", ms["failure_reason"], "image_postcardness_gate"
                gate_trace.append("image_postcardness_gate:reviewable" if str(ms.get("image_reviewable")) == "true" else "image_postcardness_gate:weak")
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
            })

        row = {
            **p, **ts, **scope, **fresh, **ad_gate, **substance, **ms,
            "candidate_id": cid, "candidate_score": score, "current_stage": current_stage,
            "drop_gate": drop_gate, "rejection_reason": rejection,
            "short_summary": make_summary(text),
            "why_this_is_about_kaliningrad": scope["region_scope_reason"],
            "what_positive": ", ".join(ts.get("positive_hits") or []),
            "what_neutral_or_useful": "route/place/travel context" if substance["text_substance_score"] >= 0.25 else "",
            "what_concern": rejection or "text gates passed; image checked",
            "image_model_report_short": ms["model_short_explanation"],
            "risk_flags": "; ".join([rejection] if rejection else ([] if p.get("rights_policy") != "unknown" else ["rights_unknown"])),
            "suggested_action": "manual_review" if current_stage in {"favorite","semantic_candidate", "pre_candidate_needs_llm", "needs_llm_retry", "needs_image_review", "low_substance_but_region_relevant", "pre_candidate_debug_deterministic"} else "reject",
            "manual_decision":"", "reviewer_comment":"",
            "region_scope_gate": "pass" if scope["kaliningrad_oblast_only_scope"] else "fail",
            "visual_scoring_stage": visual_stage,
            "visual_scoring_skip_reason": visual_skip_reason,
            "image_scoring_cost_saved": str(image_cost_saved).lower(),
            "image_scoring_skipped": str(image_cost_saved).lower(),
            "discovery_edges_count": len(edge_rows),
            "gate_order_trace": " → ".join(gate_trace),
            "semantic_evidence_flags": "; ".join(semantic_evidence_flags),
            **llm_gate,
            "semantic_gate_mode": semantic_gate_mode,
            "deterministic_semantic_gate_override": str(deterministic_override).lower(),
            "semantic_enrichment_stage": "llm_semantic_gate_pending" if current_stage == "pre_candidate_needs_llm" else ("dual_model_vector_enrichment_pending" if current_stage in {"semantic_candidate", "favorite", "needs_image_review", "low_substance_but_region_relevant"} else "skipped_by_text_gate"),
        }
        row.pop("place_matches", None)
        new_posts.append(row)
        increment.append({"entity_type":"post", "entity_id":p["post_id"], "source_title":p.get("source_title"), "post_url":p.get("post_url"), "first_seen_run_id":run_id, "previous_run_id":"", "current_run_id":run_id, "first_seen_at":run_now, "last_seen_at":run_now, "seen_run_count":1, "previous_stage":"", "current_stage":current_stage, "stage_transition":"new→"+current_stage, "new_this_run":"yes", "changed_this_run":"yes", "change_reason":rejection or "first_seen", "candidate_score_previous":"", "candidate_score_current":score, "candidate_score_delta":"", "media_score_previous":"", "media_score_current":ms["overall_media_score"], "media_score_delta":"", "manual_review_status":"unreviewed", "next_action":row["suggested_action"]})
        if current_stage in {"favorite", "semantic_candidate"}:
            candidates.append(row)
        elif current_stage in {"pre_candidate_needs_llm", "needs_llm_retry", "needs_image_review", "low_substance_but_region_relevant", "pre_candidate_debug_deterministic"}:
            pre_candidates.append(row)
        else:
            dropped.append(row)

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
        enriched_source_rows.append({**srow,
            "source_probe_status": status, "sampled_post_count": n, "kaliningrad_hit_count": kal_hits,
            "russia_travel_score": round(kal_hits/max(1,n),3), "authorial_voice_score": 0.5,
            "original_media_score": round(original_media/max(1,n),3), "ad_ratio": round(ad_hits/max(1,n),3),
            "news_ratio": round(news_hits/max(1,n),3), "trash_ratio": round(trash_hits/max(1,n),3),
            "image_prevalence": round(original_media/max(1,n),3), "link_richness_score": round(min(1, link_richness/10),3),
            "forwarded_origin_richness_score": round(sum(1 for r in sampled if r.get("is_forwarded_or_repost"))/max(1,n),3),
            "source_graph_value_score": round(min(1, link_richness/10),3), "monitor_priority_score": monitor_score,
            "source_probe_reason": "derived from recent fetched posts; no automatic monitoring without manual/probe acceptance",
        })
    source_rows = enriched_source_rows

    stage_counts = {stage: sum(1 for r in new_posts if r.get("current_stage") == stage) for stage in sorted({str(r.get("current_stage") or "") for r in new_posts})}
    fresh_rows = [r for r in new_posts if r.get("fresh_enough")]
    fresh_with_place_evidence = [r for r in fresh_rows if r.get("matched_place_names")]
    llm_reviewed_rows = [r for r in new_posts if r.get("llm_gate_status") == "ok"]
    llm_accepted_rows = [r for r in llm_reviewed_rows if r.get("llm_decision") == "accept"]
    reviewable_rows = [r for r in review_queue if r.get("current_stage") in {"pre_candidate_needs_llm", "needs_llm_retry", "needs_image_review", "low_substance_but_region_relevant", "semantic_candidate", "favorite", "pre_candidate_debug_deterministic"}]
    summary_row = {
        "run_id":run_id,"started_at":RUN_STARTED_AT.isoformat(),"finished_at":run_now,"git_sha":os.getenv("GIT_SHA", ""),"branch":"",
        "config_profile":"mvp1.x_llm_first_review_queue","dry_run":"1","ydb_namespace":os.getenv("REGION_TALK_YDB_NAMESPACE") or "dry-run-json",
        "seed_file_version":"v2" if any("v2" in str(s.get("source_seed_id")) for s in source_seed_rows) else "v1/v2-compatible",
        "place_lexicon_file":str(lexicon_path or ""),"place_lexicon_rows":len(lexicon),
        "source_count_seeded":len(seeds),"source_count_scanned":len(source_rows),"posts_fetched":len(posts),
        "posts_region_relevant":sum(1 for r in new_posts if r.get("kaliningrad_oblast_only_scope")),
        "fresh_posts":len(fresh_rows),"fresh_posts_with_place_evidence":len(fresh_with_place_evidence),
        "review_queue_rows":len(review_queue),"pre_candidates_created":len(pre_candidates),
        **llm_limit_snapshot,
        "llm_calls":llm_calls_used,"llm_max_calls":"supabase_reserve_controlled","llm_reviewed":len(llm_reviewed_rows),"llm_accepted":len(llm_accepted_rows),
        "llm_calls_attempted":llm_calls_used,"llm_calls_ok":len(llm_reviewed_rows),"llm_calls_error":sum(1 for r in new_posts if r.get("llm_gate_status") == "error"),"llm_quota_errors":sum(1 for r in new_posts if r.get("llm_gate_status") == "rate_limited"),"llm_retry_rows":sum(1 for r in new_posts if r.get("current_stage") == "needs_llm_retry"),
        "posts_with_strong_media":sum(1 for r in new_posts if r.get("is_selected_for_publication")),
        "candidates_created":len(candidates),"favorites_created":len(favorites),
        "dropped_news":sum(1 for r in dropped if r["rejection_reason"]=="newsiness"),"dropped_trash":sum(1 for r in dropped if r["rejection_reason"]=="trash"),
        "dropped_not_region":sum(1 for r in dropped if r["rejection_reason"]=="reject_not_kaliningrad_oblast_only"),
        "dropped_ad_or_promo":sum(1 for r in dropped if r["rejection_reason"]=="reject_ad_or_promo"),
        "dropped_stale":sum(1 for r in dropped if r["rejection_reason"]=="reject_stale_or_missing_date"),
        "dropped_low_substance":sum(1 for r in dropped if r["rejection_reason"]=="reject_low_substance"),
        "dropped_weak_media":sum(1 for r in dropped if "media" in str(r.get("rejection_reason"))),"dropped_duplicate":0,"dropped_rights":0,
        "image_model_calls":len(media_rows),"image_scoring_skipped_by_text_gate":sum(1 for r in new_posts if r.get("image_scoring_skipped") == "true"),
        "discovered_links":len(discovered_rows),"source_graph_edges":len(graph_edges),"errors_count":sum(1 for s in source_rows if s.get("fetch_status") == "error"),
        "stage_counts_json":json.dumps(stage_counts, ensure_ascii=False),"artifact_paths":""
    }
    run_summary = [summary_row]
    funnel = [
        {"stage":"post_fetched","current_run_count":len(new_posts),"notes":"all fetched posts"},
        {"stage":"fresh","current_run_count":len(fresh_rows),"notes":"freshness gate only; deterministic date gate"},
        {"stage":"fresh_with_place_evidence","current_run_count":len(fresh_with_place_evidence),"notes":"lexicon/anchor evidence only, not final semantic decision"},
        {"stage":"sent_to_llm_semantic_gate","current_run_count":llm_calls_used,"notes":"Google Gemini calls through Supabase google_ai_reserve; no direct env max/key2 bypass"},
        {"stage":"llm_accepted_for_image","current_run_count":len(llm_accepted_rows),"notes":"LLM semantic accept"},
        {"stage":"image_scored","current_run_count":len(media_rows),"notes":"only after LLM semantic accept"},
        {"stage":"review_queue","current_run_count":len(review_queue),"notes":"candidates + pre-candidates visible to human"},
        {"stage":"candidate","current_run_count":len(candidates),"notes":"image-scored candidate/favorite rows"},
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
            -float(r.get("candidate_score") or 0),
            int(r.get("post_age_days") or 9999),
        ),
    )
    for i, r in enumerate(final_shortlist, start=1):
        r["human_shortlist_rank"] = i
        r["decision_bucket"] = "publication_ready" if r.get("current_stage") in {"favorite", "semantic_candidate"} else ("reviewable_image" if r.get("current_stage") == "needs_image_review" else ("needs_llm_retry" if r.get("current_stage") == "needs_llm_retry" else "needs_llm"))
        r["next_action"] = "look_at_images" if r.get("current_stage") == "needs_image_review" else ("retry_llm" if r.get("current_stage") == "needs_llm_retry" else "manual_review")
    llm_error_rows = [r for r in new_posts if r.get("current_stage") == "needs_llm_retry" or r.get("llm_gate_status") in {"rate_limited", "error"}]
    debug_rejects = [r for r in dropped if r.get("current_stage") in {"debug_reject", "dropped_text_gate"} or str(r.get("rejection_reason") or "").startswith("debug_reject")]
    product_summary = [
        {"metric":"run_id","value":run_id},
        {"metric":"posts_fetched","value":len(posts)},
        {"metric":"fresh_posts","value":len(fresh_rows)},
        {"metric":"fresh_posts_with_place_evidence","value":len(fresh_with_place_evidence)},
        {"metric":"llm_calls_supabase_reserved","value":llm_calls_used},
        {"metric":"llm_calls_ok","value":len(llm_reviewed_rows)},
        {"metric":"llm_retry_rows","value":len(llm_error_rows)},
        {"metric":"image_scored_rows","value":len(media_rows)},
        {"metric":"actual_image_neural_rows","value":sum(1 for r in media_rows if r.get("image_model_input_type") == "actual_image")},
        {"metric":"human_final_shortlist_rows","value":len(final_shortlist)},
        {"metric":"final_candidates","value":len(candidates)},
        {"metric":"favorites","value":len(favorites)},
        {"metric":"llm_limit_source","value":llm_limit_snapshot.get("llm_limit_source")},
        {"metric":"llm_model","value":llm_model},
        {"metric":"llm_default_env_var_name","value":llm_default_env_var_name},
        {"metric":"image_scoring_mode","value":current_image_scoring_mode()},
    ]

    sheets = {
        "00_product_summary":product_summary,
        "00_readme":[{"field":"what","value":"Region Talk MVP-1.x Candidate Report Only; LLM final semantic gate via Supabase google_ai limiter; image scoring only after LLM accept; no Telegram/VK publishing."},{"field":"run_id","value":run_id},{"field":"generated_at","value":run_now}],
        "01_run_summary":run_summary,
        "02_increment":increment,
        "03_funnel":funnel,
        "03b_gate_counts":independent_gate_counts,
        "04_review_queue":review_queue,
        "04a_final_shortlist":final_shortlist,
        "04b_needs_llm_retry":llm_error_rows,
        "04c_debug_rejects":debug_rejects,
        "05_favorites":favorites,
        "06_candidates_all":candidates,
        "07_new_posts_this_run":new_posts,
        "08_dropped_posts":dropped,
        "09_image_quality":media_rows,
        "10_good_text_weak_media":[r for r in dropped if r.get("current_stage") == "good_text_weak_media"],
        "11_sources_seed":source_seed_rows,
        "12_sources_discovered":discovered_rows,
        "13_sources_monitored":source_rows,
        "14_verifier_reports":[],
        "14b_pre_candidates_needing_llm":[r for r in pre_candidates if r.get("current_stage") == "pre_candidate_needs_llm"],
        "14c_llm_errors":llm_error_rows,
        "15_manual_decisions":[{"candidate_id":"","manual_decision":"favorite|reject|approve_for_preview|approve_for_queue|block_source","reviewer":"","reviewed_at":"","reviewer_comment":"","rights_override":"","source_status_override":""}],
        "16_publish_preview_future":[{"note":"Future only. REGION_TALK_DISABLE_PUBLISH=1; no real publishing in MVP-1.x."}],
        "17_source_graph_edges":graph_edges,
        "18_place_lexicon_matches":place_match_rows,
    }
    xlsx = output_dir / f"region-talk-candidates-{run_id}.xlsx"
    write_xlsx(xlsx, sheets)
    write_csv(output_dir / f"region-talk-candidates-{run_id}.csv", review_queue)
    payload = {"ok": True, "run_id": run_id, "generated_at": run_now, "summary": run_summary[0], "sheets": sheets, "xlsx_path": str(xlsx)}
    (output_dir / f"region-talk-candidates-{run_id}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / f"region-talk-candidates-{run_id}.md").write_text(render_md(payload), encoding="utf-8")
    (output_dir / f"region-talk-candidates-{run_id}.html").write_text(render_html(payload), encoding="utf-8")
    for p in [xlsx, output_dir / f"region-talk-candidates-{run_id}.json", output_dir / f"region-talk-candidates-{run_id}.md", output_dir / f"region-talk-candidates-{run_id}.html", output_dir / f"region-talk-candidates-{run_id}.csv"]:
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
