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
                matches.append({
                    "place_id": place.get("place_id", ""),
                    "matched_place_name": term,
                    "canonical_name": place.get("canonical_name", ""),
                    "place_type": place.get("place_type", ""),
                    "municipality": place.get("municipality", ""),
                    "priority_tier": place.get("priority_tier", ""),
                    "alias_used": term if kind != "canonical_name" else "",
                    "match_context": re.sub(r"\s+", " ", raw_text[max(0, idx-80):idx+len(term)+80])[:240] if idx >= 0 else "",
                    "requires_context": place.get("requires_context", ""),
                    "accepted_as_region_evidence": "needs_context" if str(place.get("requires_context") or "").lower() == "true" else "true",
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
    external_regions, external_countries = external_geo_mentions(text)
    ok = bool(strong_matches or matches) and not external_regions and not external_countries
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


def image_scores_skipped(reason: str) -> dict[str, Any]:
    return {"technical_quality_score":0.0,"aesthetic_score":0.0,"postcardness_score":0.0,"region_visual_relevance_score":0.0,"publication_safety_score":0.0,"low_noise_score":0.0,"overall_media_score":0.0,"is_selected_for_publication":False,"recognized_visual_elements":"","model_short_explanation":"Image scoring skipped by text gate: " + reason,"failure_reason":"image_scoring_skipped_by_text_gate","model_id":"cv_only_metadata_v1","model_version":"2026-07-05"}

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


def media_scores(has_media: bool, text_score: dict[str, Any]) -> dict[str, Any]:
    if not has_media:
        return {"technical_quality_score":0.0,"aesthetic_score":0.0,"postcardness_score":0.0,"region_visual_relevance_score":0.0,"publication_safety_score":1.0,"low_noise_score":0.0,"overall_media_score":0.0,"is_selected_for_publication":False,"recognized_visual_elements":"","model_short_explanation":"No media detected; strong-image gate failed.","failure_reason":"no_media","model_id":"cv_only_metadata_v1","model_version":"2026-07-05"}
    anchors = text_score.get("anchor_hits") or []
    positives = text_score.get("positive_hits") or []
    technical = 0.72
    aesthetic = min(0.92, 0.68 + 0.04*len(positives))
    postcard = min(0.94, 0.66 + 0.06*len(anchors) + 0.03*len(positives))
    region_visual = min(0.95, 0.58 + 0.08*len(anchors))
    safety = 0.98 if not text_score.get("news_hits") and not text_score.get("trash_hits") else 0.78
    low_noise = 0.82
    overall = round((technical+aesthetic+postcard+region_visual+safety+low_noise)/6,3)
    selected = technical>=0.65 and aesthetic>=0.70 and postcard>=0.72 and safety>=0.95 and low_noise>=0.80 and overall>=0.75
    elements = []
    low = " ".join(anchors + positives).lower()
    if "море" in low or "балтий" in low: elements.append("sea/coast")
    if "курш" in low or "дюн" in low: elements.append("dunes/nature")
    if "архитект" in low or "кёниг" in low or "калининград" in low: elements.append("city/architecture")
    if not elements: elements.append("travel visual candidate")
    return {"technical_quality_score":round(technical,3),"aesthetic_score":round(aesthetic,3),"postcardness_score":round(postcard,3),"region_visual_relevance_score":round(region_visual,3),"publication_safety_score":round(safety,3),"low_noise_score":round(low_noise,3),"overall_media_score":overall,"is_selected_for_publication":selected,"recognized_visual_elements":"; ".join(elements),"model_short_explanation":f"cv_only metadata heuristic: media present; region anchors={len(anchors)}, travel/visual cues={len(positives)}; safety={'ok' if safety>=0.95 else 'blocked by news/trash cues'}.","failure_reason":"" if selected else "below_strong_image_threshold","model_id":"cv_only_metadata_v1","model_version":"2026-07-05"}


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
                        posts.append({"post_id":"post_"+stable_hash("telegram", handle, mid), "source_id": seed.source_id, "source_seed_id": seed.source_seed_id, "source_title": title, "platform":"telegram", "handle": seed.handle, "post_url": post_url, "platform_post_key": f"tg:{handle}:{mid}", "post_date": dt.isoformat() if dt else "", "text": text, "text_excerpt": re.sub(r"\s+", " ", text)[:500], "has_media": has_media, "media_count": 1 if has_media else 0, "rights_policy": seed.rights_policy, "source_kind": seed.source_kind, "source_type": seed.source_kind, "source_url": seed.canonical_url, "is_forwarded_or_repost": bool(fwd), "forwarded_from_source_title": forwarded_from_title, "forwarded_from_source_id": "src_" + stable_hash("telegram_forward", forwarded_from_url or forwarded_from_title) if fwd else "", "forwarded_from_platform": "telegram" if fwd else "", "forwarded_from_handle": forwarded_from_handle, "forwarded_from_url": forwarded_from_url, "forwarded_from_post_url": forwarded_from_post_url, "forwarded_from_confidence": forwarded_from_confidence, "original_source_candidate_id": "src_cand_" + stable_hash("telegram", forwarded_from_url or forwarded_from_title) if fwd else ""})
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
        gate_trace: list[str] = []

        if not fresh["fresh_enough"]:
            drop_gate, rejection = "freshness_gate", "reject_stale_or_missing_date"
            visual_skip_reason = fresh["freshness_reason"]
            gate_trace.append("freshness_gate:reject")
        else:
            gate_trace.append("freshness_gate:pass")
        if not rejection and not scope["kaliningrad_oblast_only_scope"]:
            drop_gate, rejection = "kaliningrad_oblast_only_scope_gate", "reject_not_kaliningrad_oblast_only"
            visual_skip_reason = scope["region_scope_reason"]
            gate_trace.append("kaliningrad_oblast_only_scope_gate:reject")
        elif not rejection:
            gate_trace.append("kaliningrad_oblast_only_scope_gate:pass")
        if not rejection and ad_gate["is_ad_or_promo"]:
            drop_gate, rejection = "ad_promo_announcement_gate", "reject_ad_or_promo"
            visual_skip_reason = ad_gate["ad_promo_reason"]
            gate_trace.append("ad_promo_announcement_gate:reject")
        elif not rejection:
            gate_trace.append("ad_promo_announcement_gate:pass")
        if not rejection and substance["text_substance_score"] < 0.25:
            drop_gate, rejection = "content_substance_visit_impression_gate", "reject_low_substance"
            visual_skip_reason = substance["substance_reason"]
            gate_trace.append("content_substance_visit_impression_gate:reject")
        elif not rejection:
            gate_trace.append("content_substance_visit_impression_gate:pass")
        if not rejection and ts["newsiness_score"] >= 0.45:
            drop_gate, rejection = "not_news_gate", "newsiness"
            visual_skip_reason = "reject: news/prosecution/incident cues"
            gate_trace.append("not_news_gate:reject")
        elif not rejection:
            gate_trace.append("not_news_gate:pass")
        if not rejection and ts["trash_score"] >= 0.35:
            drop_gate, rejection = "not_trash_gate", "trash"
            visual_skip_reason = "reject: trash/shock cues"
            gate_trace.append("not_trash_gate:reject")
        elif not rejection:
            gate_trace.append("not_trash_gate:pass")

        if rejection:
            ms = image_scores_skipped(visual_skip_reason or rejection)
            score = 0.0
            current_stage = "dropped_text_gate"
        else:
            gate_trace.append("semantic_dual_model_enrichment:feature_enriched_pending_vector_models")
            visual_stage = "scored_after_text_gates"
            visual_skip_reason = ""
            image_cost_saved = False
            ms = media_scores(bool(p.get("has_media")), ts)
            media_rows.append({"media_id": mid, "candidate_id": cid, "post_url": p.get("post_url"), "image_url_or_local_path": (p.get("post_url") + "#media") if p.get("has_media") else "", "thumbnail":"", **ms})
            score = candidate_score(ts, ms, seed_for_post) if seed_for_post else 0.0
            if not ms["is_selected_for_publication"]:
                current_stage, rejection, drop_gate = "good_text_weak_media", ms["failure_reason"], "image_postcardness_gate"
                gate_trace.append("image_postcardness_gate:reject")
            elif score >= 0.45:
                current_stage = "favorite" if score >= 0.62 else "semantic_candidate"
                gate_trace.append("image_postcardness_gate:pass")
            else:
                current_stage, rejection, drop_gate = "dropped_low_score", "candidate_score_low", "candidate_score_gate"
                gate_trace.append("candidate_score_gate:reject")

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
            "suggested_action": "manual_review" if current_stage in {"favorite","semantic_candidate"} else "reject",
            "manual_decision":"", "reviewer_comment":"",
            "region_scope_gate": "pass" if scope["kaliningrad_oblast_only_scope"] else "fail",
            "visual_scoring_stage": visual_stage,
            "visual_scoring_skip_reason": visual_skip_reason,
            "image_scoring_cost_saved": str(image_cost_saved).lower(),
            "image_scoring_skipped": str(image_cost_saved).lower(),
            "discovery_edges_count": len(edge_rows),
            "gate_order_trace": " → ".join(gate_trace),
            "semantic_enrichment_stage": "dual_model_vector_enrichment_pending" if not rejection else "skipped_by_text_gate",
        }
        row.pop("place_matches", None)
        new_posts.append(row)
        increment.append({"entity_type":"post", "entity_id":p["post_id"], "source_title":p.get("source_title"), "post_url":p.get("post_url"), "first_seen_run_id":run_id, "previous_run_id":"", "current_run_id":run_id, "first_seen_at":run_now, "last_seen_at":run_now, "seen_run_count":1, "previous_stage":"", "current_stage":current_stage, "stage_transition":"new→"+current_stage, "new_this_run":"yes", "changed_this_run":"yes", "change_reason":rejection or "first_seen", "candidate_score_previous":"", "candidate_score_current":score, "candidate_score_delta":"", "media_score_previous":"", "media_score_current":ms["overall_media_score"], "media_score_delta":"", "manual_review_status":"unreviewed", "next_action":row["suggested_action"]})
        if current_stage in {"favorite", "semantic_candidate"}:
            candidates.append(row)
        else:
            dropped.append(row)

    review_queue = sorted(candidates, key=lambda r: float(r.get("candidate_score") or 0), reverse=True)
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

    summary_row = {"run_id":run_id,"started_at":RUN_STARTED_AT.isoformat(),"finished_at":run_now,"git_sha":os.getenv("GIT_SHA", ""),"branch":"","config_profile":"mvp1.x_strict_text_gates","dry_run":"1","ydb_namespace":os.getenv("REGION_TALK_YDB_NAMESPACE") or "dry-run-json","seed_file_version":"v2" if any("v2" in str(s.get("source_seed_id")) for s in source_seed_rows) else "v1/v2-compatible","place_lexicon_file":str(lexicon_path or ""),"place_lexicon_rows":len(lexicon),"source_count_seeded":len(seeds),"source_count_scanned":len(source_rows),"posts_fetched":len(posts),"posts_region_relevant":sum(1 for r in new_posts if r.get("kaliningrad_oblast_only_scope")),"posts_with_strong_media":sum(1 for r in new_posts if r.get("is_selected_for_publication")),"candidates_created":len(candidates),"favorites_created":len(favorites),"dropped_news":sum(1 for r in dropped if r["rejection_reason"]=="newsiness"),"dropped_trash":sum(1 for r in dropped if r["rejection_reason"]=="trash"),"dropped_not_region":sum(1 for r in dropped if r["rejection_reason"]=="reject_not_kaliningrad_oblast_only"),"dropped_ad_or_promo":sum(1 for r in dropped if r["rejection_reason"]=="reject_ad_or_promo"),"dropped_stale":sum(1 for r in dropped if r["rejection_reason"]=="reject_stale_or_missing_date"),"dropped_low_substance":sum(1 for r in dropped if r["rejection_reason"]=="reject_low_substance"),"dropped_weak_media":sum(1 for r in dropped if "media" in str(r.get("rejection_reason"))),"dropped_duplicate":0,"dropped_rights":0,"llm_calls":0,"image_model_calls":len(media_rows),"image_scoring_skipped_by_text_gate":sum(1 for r in new_posts if r.get("image_scoring_skipped") == "true"),"discovered_links":len(discovered_rows),"source_graph_edges":len(graph_edges),"errors_count":sum(1 for s in source_rows if s.get("fetch_status") == "error"),"artifact_paths":""}
    run_summary = [summary_row]
    funnel = []
    for stage in ["seed_source","source_monitored","post_fetched","fresh","kaliningrad_oblast_only","non_ad","substantive","strong_media","semantic_candidate","favorite"]:
        count = {"seed_source":len(seeds),"source_monitored":len(source_rows),"post_fetched":len(posts),"fresh":sum(1 for r in new_posts if r.get("fresh_enough")),"kaliningrad_oblast_only":summary_row["posts_region_relevant"],"non_ad":sum(1 for r in new_posts if not r.get("is_ad_or_promo")),"substantive":sum(1 for r in new_posts if float(r.get("text_substance_score") or 0) >= 0.25),"strong_media":summary_row["posts_with_strong_media"],"semantic_candidate":len([r for r in candidates if r["current_stage"]=="semantic_candidate"]),"favorite":len(favorites)}[stage]
        funnel.append({"stage":stage,"current_run_count":count,"previous_run_count":"","delta":"","total_cumulative":count,"top_rejection_reasons":"","notes":""})
    sheets = {
        "00_readme":[{"field":"what","value":"Region Talk MVP-1.x Candidate Report Only; strict text gates before image scoring; no Telegram/VK publishing."},{"field":"run_id","value":run_id},{"field":"generated_at","value":run_now}],
        "01_run_summary":run_summary,
        "02_increment":increment,
        "03_funnel":funnel,
        "04_review_queue":review_queue,
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
