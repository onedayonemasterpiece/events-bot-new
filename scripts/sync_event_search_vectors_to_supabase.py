#!/usr/bin/env python3
"""Build KenigEvents search documents, embed them and sync to personalization Supabase.

This is the pgvector ingestion lane for the static-site/search sidecar. It reads
already-exported static-site event fixtures (or a generated preview JSON), stores
compact card snapshots and 768-dim Gemini embeddings in the separate
PERSONALIZATION Supabase project. It never stores raw OCR/source posts or core bot
state in Supabase.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import html
import json
import os
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from google_ai import GoogleAIClient, SecretsProvider
from google_ai.exceptions import RateLimitError

DEFAULT_PREVIEW_JSON = ROOT / "site" / "src" / "data" / "preview-events.json"
WEEKDAY_RU = {
    1: "понедельник",
    2: "вторник",
    3: "среда",
    4: "четверг",
    5: "пятница",
    6: "суббота",
    7: "воскресенье",
}
MONTH_RU = {
    1: "январь",
    2: "февраль",
    3: "март",
    4: "апрель",
    5: "май",
    6: "июнь",
    7: "июль",
    8: "август",
    9: "сентябрь",
    10: "октябрь",
    11: "ноябрь",
    12: "декабрь",
}
SEASON_RU = {
    12: "зима",
    1: "зима",
    2: "зима",
    3: "весна",
    4: "весна",
    5: "весна",
    6: "лето",
    7: "лето",
    8: "лето",
    9: "осень",
    10: "осень",
    11: "осень",
}
TIME_OF_DAY_RU = {
    "morning": "утро",
    "day": "день",
    "evening": "вечер",
    "night": "ночь",
}
MONTHS_GENITIVE_RU = (
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
)

CATALOG_REVISION_SCHEMA_VERSION = "event_search_catalog_revision_v1"
CORPUS_REVISION_SCHEMA_VERSION = "event_search_corpus_revision_v2"
COVERAGE_RECEIPT_SCHEMA_VERSION = "event_search_projection_coverage_v1"
DOCUMENT_KIND_CONTRACT = {
    "search_v3": ("text_hash", "search_doc_version"),
    "related_v1": ("related_text_hash", "related_doc_version"),
}


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def clean_text(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[\uFE0F\u200D]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def exported_search_catalog_revision(events: Iterable[dict[str, Any]]) -> str:
    """Hash the exact exported eligible event payload, independent of ordering.

    The static exporter and vector projector intentionally share this small
    JSON contract.  Hashing every complete exported event (rather than only an
    ID or timestamp) makes a display-only correction advance the catalog even
    when a legacy SQLite snapshot has no usable source revision column.
    """

    entries: list[dict[str, Any]] = []
    seen: set[int] = set()
    for raw in events:
        event = dict(raw)
        event_id = int(event.get("id") or 0)
        if event_id <= 0:
            raise ValueError("exported Search catalog contains a non-positive event id")
        if event_id in seen:
            raise ValueError(f"exported Search catalog contains duplicate event id {event_id}")
        seen.add(event_id)
        entries.append({
            "event_id": event_id,
            "event_sha256": sha256_text(_canonical_json(event)),
        })
    manifest = {
        "schema_version": CATALOG_REVISION_SCHEMA_VERSION,
        "events": sorted(entries, key=lambda item: item["event_id"]),
    }
    return sha256_text(_canonical_json(manifest))


def _occurrence_time(event: dict[str, Any]) -> str | None:
    match = re.search(
        r"(\d{1,2}):(\d{2})",
        clean_text(event.get("start_time") or event.get("display_time")),
    )
    if not match:
        return None
    hour, minute = int(match.group(1)), int(match.group(2))
    if hour > 23 or minute > 59:
        return None
    return f"{hour:02d}:{minute:02d}"


def _human_join(values: list[str]) -> str:
    if len(values) < 2:
        return values[0] if values else ""
    return f"{', '.join(values[:-1])} и {values[-1]}"


def _format_occurrence_date(value: str, current_year: int) -> str:
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return value
    suffix = f" {parsed.year}" if parsed.year != current_year else ""
    return f"{parsed.day} {MONTHS_GENITIVE_RU[parsed.month - 1]}{suffix}"


def _format_occurrence_dates(values: list[str], current_year: int, *, aria: bool = False) -> str:
    dates = list(dict.fromkeys(values))
    try:
        parsed = [date.fromisoformat(value) for value in dates]
    except ValueError:
        return _human_join(dates) if aria else ", ".join(dates)
    if parsed and all(item.month == parsed[0].month and item.year == parsed[0].year for item in parsed):
        days = [str(item.day) for item in parsed]
        joined = _human_join(days) if aria else ", ".join(days)
        suffix = f" {parsed[0].year}" if parsed[0].year != current_year else ""
        return f"{joined} {MONTHS_GENITIVE_RU[parsed[0].month - 1]}{suffix}"
    formatted = [_format_occurrence_date(value, current_year) for value in dates]
    return _human_join(formatted) if aria else ", ".join(formatted)


def build_occurrence_projections(
    events: list[dict[str, Any]], *, current_year: int | None = None
) -> dict[int, dict[str, Any]]:
    """Project reciprocal explicit occurrence families into search snapshots.

    This is publication projection only: it never infers identity from title,
    type or venue. Connected components contain reciprocal `other_date_ids`
    edges exclusively, matching the static per-family collapse contract.
    """
    year = int(current_year or date.today().year)
    public = {
        int(event["id"]): event
        for event in events
        if int(event.get("id") or 0) > 0
        and clean_text(event.get("lifecycle_status")).lower() in {"", "active"}
        and (
            not clean_text(event.get("end_date"))
            or clean_text(event.get("end_date"))
            == clean_text(event.get("start_date"))
        )
    }
    parent = {event_id: event_id for event_id in public}

    def find(event_id: int) -> int:
        while parent[event_id] != event_id:
            parent[event_id] = parent[parent[event_id]]
            event_id = parent[event_id]
        return event_id

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    links = {
        event_id: {int(value) for value in (event.get("other_date_ids") or []) if str(value).isdigit()}
        for event_id, event in public.items()
    }
    for event_id, linked_ids in links.items():
        for linked_id in linked_ids:
            if linked_id in public and event_id in links.get(linked_id, set()):
                union(event_id, linked_id)

    components: dict[int, list[dict[str, Any]]] = {}
    for event_id, event in public.items():
        components.setdefault(find(event_id), []).append(event)

    output: dict[int, dict[str, Any]] = {}
    for members in components.values():
        members.sort(
            key=lambda item: (
                clean_text(item.get("start_date")),
                _occurrence_time(item) or "99:99",
                int(item["id"]),
            )
        )
        member_ids = [int(item["id"]) for item in members]
        dates = list(dict.fromkeys(clean_text(item.get("start_date")) for item in members))
        times = [_occurrence_time(item) for item in members]
        known_times = [value for value in times if value]
        rows: dict[str, list[str | None]] = {}
        for member, time_value in zip(members, times, strict=True):
            rows.setdefault(clean_text(member.get("start_date")), []).append(time_value)
        if len(rows) == 1 and len(known_times) == len(members):
            date_label = _format_occurrence_date(dates[0], year)
            compact = f"{date_label} {', '.join(known_times)}"
            aria = f"{date_label} в {_human_join(known_times)}"
        elif known_times and len(known_times) == len(members) and len(set(known_times)) == 1:
            compact = f"{_format_occurrence_dates(dates, year)} {known_times[0]}"
            aria = f"{_format_occurrence_dates(dates, year, aria=True)} в {known_times[0]}"
        else:
            chunks: list[str] = []
            for date_value, row_times in rows.items():
                date_label = _format_occurrence_date(date_value, year)
                if all(row_times):
                    chunks.append(f"{date_label} {', '.join(value for value in row_times if value)}")
                elif any(row_times):
                    chunks.append(f"{date_label}, время уточняется")
                else:
                    chunks.append(date_label)
            compact = "; ".join(chunks)
            aria = compact
        for event_id in member_ids:
            output[event_id] = {
                "occurrence_member_ids": member_ids,
                "occurrence_compact_label": compact,
                "occurrence_aria_label": aria,
            }
    return output


def vector_corpus_hash(
    docs: Iterable["SearchDoc"],
    *,
    document_kind: str,
    embedding_model: str,
    embedding_dim: int,
    catalog_revision: str = "",
) -> str:
    """Return a stable revision for one complete embedding corpus.

    The revision is intentionally based on the ordered event/text-hash manifest
    plus the embedding contract, not timestamps or provider-call ordering.  It
    therefore changes whenever membership, semantic input, model, dimension or
    document kind changes and remains identical for no-op reconciliations.
    """

    if document_kind not in DOCUMENT_KIND_CONTRACT:
        raise ValueError(f"unsupported vector corpus document kind: {document_kind}")
    docs = list(docs)
    hash_key, version_key = DOCUMENT_KIND_CONTRACT[document_kind]
    document_versions = {
        str(doc.document.get(version_key) or "unspecified") for doc in docs
    }
    if len(document_versions) > 1:
        raise ValueError(
            f"{document_kind} corpus mixes document versions: "
            f"{', '.join(sorted(document_versions))}"
        )
    manifest = {
        "schema_version": CORPUS_REVISION_SCHEMA_VERSION,
        "catalog_revision": str(catalog_revision),
        "embedding_model": str(embedding_model),
        "embedding_dim": int(embedding_dim),
        "embedding_doc_kind": document_kind,
        "document_version": next(iter(document_versions), "unspecified"),
        "documents": sorted(
            (
                {
                    "event_id": int(doc.event_id),
                    "text_hash": str(doc.document[hash_key]),
                }
                for doc in docs
            ),
            key=lambda item: (item["event_id"], item["text_hash"]),
        ),
    }
    canonical = json.dumps(
        manifest,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return sha256_text(canonical)


def build_revision_contract(
    docs: Iterable["SearchDoc"],
    *,
    catalog_revision: str,
    embedding_model: str,
    embedding_dim: int,
) -> dict[str, Any]:
    """Build the revision payload shared by reports and persisted metadata."""

    docs = list(docs)
    if not re.fullmatch(r"[0-9a-f]{64}", str(catalog_revision or "")):
        raise ValueError("catalog_revision must be a lowercase SHA-256")
    corpora: dict[str, dict[str, Any]] = {}
    for document_kind, (_hash_key, version_key) in DOCUMENT_KIND_CONTRACT.items():
        versions = {
            str(doc.document.get(version_key) or "unspecified") for doc in docs
        }
        if len(versions) > 1:
            raise ValueError(
                f"{document_kind} corpus mixes document versions: "
                f"{', '.join(sorted(versions))}"
            )
        corpora[document_kind] = {
            "revision": vector_corpus_hash(
                docs,
                document_kind=document_kind,
                embedding_model=embedding_model,
                embedding_dim=embedding_dim,
                catalog_revision=catalog_revision,
            ),
            "document_version": next(iter(versions), "unspecified"),
            "embedding_model": str(embedding_model),
            "embedding_dim": int(embedding_dim),
            "embedding_doc_kind": document_kind,
            "document_count": len(docs),
        }
    search_revision = corpora["search_v3"]["revision"]
    return {
        "schema_version": CORPUS_REVISION_SCHEMA_VERSION,
        "catalog_revision": catalog_revision,
        # Search response/cache identity is the searchable document corpus.
        "corpus_revision": search_revision,
        "search_document_revision": search_revision,
        "corpora": corpora,
    }


def annotate_search_documents(
    docs: Iterable["SearchDoc"], revision_contract: dict[str, Any]
) -> None:
    """Persist compact global revision identities on every Search document."""

    corpora = revision_contract["corpora"]
    for doc in docs:
        metadata = dict(doc.document.get("metadata") or {})
        metadata.update({
            "catalog_revision": revision_contract["catalog_revision"],
            "corpus_revision": revision_contract["corpus_revision"],
            "search_document_revision": revision_contract["search_document_revision"],
            "search_doc_version": doc.document.get("search_doc_version"),
            "search_corpus_revision": corpora["search_v3"]["revision"],
            "related_corpus_revision": corpora["related_v1"]["revision"],
        })
        doc.document["metadata"] = metadata


def embedding_revision_metadata(
    doc: "SearchDoc",
    *,
    document_kind: str,
    catalog_revision: str,
    revision_contract: dict[str, Any],
) -> dict[str, Any]:
    _hash_key, version_key = DOCUMENT_KIND_CONTRACT[document_kind]
    return {
        "doc_kind": document_kind,
        "doc_version": doc.document.get(version_key),
        "search_doc_version": doc.document["search_doc_version"],
        "related_doc_version": doc.document.get("related_doc_version"),
        "catalog_revision": catalog_revision,
        # Keep the response/cache Search corpus identity identical across both
        # stored document-kind rows.
        "corpus_revision": revision_contract["corpus_revision"],
        "search_document_revision": revision_contract["search_document_revision"],
        "embedding_corpus_revision": revision_contract["corpora"][document_kind]["revision"],
    }


def event_weekday(event: dict[str, Any]) -> tuple[int | None, str | None]:
    raw = clean_text(event.get("start_date"))
    try:
        iso = date.fromisoformat(raw).isoweekday()
    except Exception:
        return None, None
    return iso, WEEKDAY_RU.get(iso)


def event_time_of_day(event: dict[str, Any]) -> str | None:
    raw = clean_text(event.get("start_time")) or clean_text(event.get("starts_at"))[11:16]
    match = re.search(r"(\d{1,2}):", raw)
    if not match:
        return None
    hour = int(match.group(1))
    if 6 <= hour < 12:
        return "morning"
    if 12 <= hour < 17:
        return "day"
    if 17 <= hour < 22:
        return "evening"
    return "night"


def event_month_and_season(event: dict[str, Any]) -> tuple[str | None, str | None]:
    raw = clean_text(event.get("start_date"))
    try:
        month = date.fromisoformat(raw).month
    except Exception:
        return None, None
    return MONTH_RU.get(month), SEASON_RU.get(month)


def event_availability(event: dict[str, Any]) -> str:
    lifecycle = clean_text(event.get("lifecycle_status")).lower() or "active"
    ticket = event.get("ticket") or {}
    status = " ".join([
        clean_text(ticket.get("status")),
        clean_text(ticket.get("label")),
        clean_text(event.get("status_label")),
    ]).lower()
    if lifecycle and lifecycle != "active":
        if "cancel" in lifecycle or "отмен" in lifecycle:
            return "cancelled"
        if "postpon" in lifecycle or "перен" in lifecycle:
            return "postponed"
        return lifecycle[:40]
    if re.search(r"sold|unavailable|not[_\s-]?available|нет\s+бил|законч|распрод", status, re.I):
        return "sold_out"
    if ticket.get("kind") in {"ticket", "registration", "phone", "free"} or ticket.get("href") or ticket.get("is_free"):
        return "available"
    return "unknown"


def event_admission_type(event: dict[str, Any]) -> str:
    ticket = event.get("ticket") or {}
    if ticket.get("is_free"):
        return "free"
    kind = clean_text(ticket.get("kind"))
    if kind in {"ticket", "registration", "phone", "status"}:
        return "registration_required" if kind == "registration" else kind
    return "unknown"


def ru_event_category(event: dict[str, Any]) -> str:
    haystack = " ".join(
        str(x or "")
        for x in [
            event.get("title"),
            event.get("event_type"),
            event.get("summary"),
            *(event.get("topics") or []),
        ]
    ).lower()
    patterns = [
        (r"архитект|урбан|городск\w*\s+сред|будущ\w*\s+город|общественн\w*\s+пространств|концепци|моделир", "urbanism"),
        (r"опера|опероман|вокал|концерт|симфони|музык|CONCERT", "music"),
        (r"выстав|EXHIB", "exhibition"),
        (r"спектак|театр|THEATRE", "theatre"),
        (r"лекц|встреч", "lecture"),
        (r"мастер|воркш|MASTER", "workshop"),
        (r"ярмарк|фестив|FEST", "festival"),
        (r"экскурс", "excursion"),
        (r"кино|фильм", "cinema"),
    ]
    for pattern, category in patterns:
        if re.search(pattern, haystack, re.I):
            return category
    event_type = clean_text(event.get("event_type"))
    return re.sub(r"\W+", "_", event_type.lower()).strip("_") or "event"


def unique(values: Iterable[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = clean_text(value).lower()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def event_tags(event: dict[str, Any], category: str) -> list[str]:
    values: list[Any] = [category, event.get("event_type")]
    values.extend(event.get("topics") or [])
    if event.get("city"):
        values.append(event.get("city"))
    ticket = event.get("ticket") or {}
    if ticket.get("is_free"):
        values.append("free")
    elif event_admission_type(event) in {"ticket", "registration_required", "phone"}:
        values.append("ticketed")
    if event.get("festival"):
        values.append("festival")
    return unique(values)[:24]


def join_parts(parts: Iterable[Any], sep: str = " · ") -> str:
    return sep.join(clean_text(part) for part in parts if clean_text(part))


def absolute_url(path_or_url: str, *, site_origin: str, base_path: str) -> str:
    value = clean_text(path_or_url)
    if value.startswith("http://") or value.startswith("https://"):
        return value
    if not value.startswith("/"):
        value = f"/{value}"
    base = f"/{base_path.strip('/')}" if base_path.strip("/") else ""
    return f"{site_origin.rstrip('/')}{base}{value}"


def event_href(event: dict[str, Any], *, base_path: str) -> str:
    slug = clean_text(event.get("slug"))
    base = f"/{base_path.strip('/')}" if base_path.strip("/") else ""
    return f"{base}/sobytiya/{slug}/"


def calendar_href(event: dict[str, Any], *, ics_base_url: str, base_path: str) -> str:
    if ics_base_url:
        return f"{ics_base_url.rstrip('/')}/{int(event['id'])}.ics"
    base = f"/{base_path.strip('/')}" if base_path.strip("/") else ""
    return f"{base}/sobytiya/{clean_text(event.get('slug'))}/event.ics"


def is_calendar_eligible(event: dict[str, Any]) -> bool:
    return not event.get("end_date") or event.get("end_date") == event.get("start_date")


def build_search_digest(event: dict[str, Any], category: str, tags: list[str]) -> str:
    ticket = event.get("ticket") or {}
    weekday_iso, weekday_ru = event_weekday(event)
    time_of_day = event_time_of_day(event)
    month_ru, season_ru = event_month_and_season(event)
    availability = event_availability(event)
    admission = event_admission_type(event)
    daypart_ru = TIME_OF_DAY_RU.get(time_of_day or "")
    joined_text = clean_text(" ".join(clean_text(part) for part in [
        event.get("title"),
        event.get("event_type"),
        event.get("summary"),
        event.get("description_html"),
        ticket.get("label"),
        ticket.get("note"),
    ] if part is not None))
    audience_flags: list[str] = []
    tag_set = set(tags)
    if {"family", "kids", "kids_school"} & tag_set or re.search(r"детск|детям|реб[её]н|семейн|для\s+дет", joined_text, re.I):
        audience_flags.extend(["для детей", "для семьи"])
    if "tourist_friendly" in tag_set or re.search(r"турист|экскурс", joined_text, re.I):
        audience_flags.append("для туристов")
    if re.search(r"благотвор|пожертв|донат", joined_text, re.I):
        audience_flags.append("благотворительное")
    if event.get("pushkin_card"):
        audience_flags.append("Пушкинская карта")
    calendar_words = [
        weekday_ru,
        "выходной" if weekday_iso in {6, 7} else "будний день" if weekday_iso else None,
        daypart_ru,
        month_ru,
        season_ru,
    ]
    admission_words = [
        "бесплатно" if admission == "free" else None,
        "регистрация" if admission == "registration_required" else None,
        "по билетам" if admission == "ticket" else None,
        "запись по телефону" if admission == "phone" else None,
        "билеты закончились" if availability == "sold_out" else None,
    ]
    facts = [
        f"Категория: {category}",
        f"Тип: {clean_text(event.get('event_type')) or 'событие'}",
        f"Название: {clean_text(event.get('title'))}",
        f"Кратко: {clean_text(event.get('summary'))}",
        f"Описание: {clean_text(event.get('description_html'))[:2500]}",
        f"Место: {join_parts([event.get('venue_name'), event.get('address'), event.get('city')])}",
        f"Дата: {join_parts([event.get('display_date'), event.get('display_time')])}",
        f"Календарь/поиск: {join_parts(calendar_words, sep=', ')}",
        f"День недели: {weekday_ru}" if weekday_ru else "",
        f"День недели ISO: {weekday_iso}" if weekday_iso else "",
        f"Время суток: {time_of_day}" if time_of_day else "",
        f"Время суток RU: {daypart_ru}" if daypart_ru else "",
        f"Месяц: {month_ru}" if month_ru else "",
        f"Сезон: {season_ru}" if season_ru else "",
        f"Темы: {', '.join(tags)}",
        f"Аудитория/свойства: {', '.join(unique(audience_flags))}" if audience_flags else "",
        f"Доступ/поиск: {join_parts(admission_words, sep=', ')}",
        f"Условия: {join_parts([admission, availability, ticket.get('label'), ticket.get('price_label'), ticket.get('status')])}",
    ]
    return "\n".join(part for part in facts if part and not part.endswith(": "))[:7000]


def build_related_digest(event: dict[str, Any], category: str, tags: list[str]) -> str:
    """Cleaner event-to-event document for related pages.

    Search documents intentionally include weekday/month/price/free words so
    explicit user queries like "бесплатно в воскресенье вечером" work. Related
    pages need a different representation: theme, format, audience and venue are
    useful; calendar/admission words can pollute similarity.
    """
    joined_text = clean_text(" ".join(clean_text(part) for part in [
        event.get("title"),
        event.get("event_type"),
        event.get("summary"),
        event.get("description_html"),
        *(event.get("topics") or []),
    ] if part is not None))
    audience_flags: list[str] = []
    tag_set = set(tags)
    if {"family", "kids", "kids_school"} & tag_set or re.search(r"детск|детям|реб[её]н|семейн|для\s+дет", joined_text, re.I):
        audience_flags.extend(["для детей", "для семьи"])
    if re.search(r"турист|экскурс", joined_text, re.I):
        audience_flags.append("для туристов")
    if re.search(r"благотвор|пожертв|донат", joined_text, re.I):
        audience_flags.append("благотворительное")
    if event.get("pushkin_card"):
        audience_flags.append("Пушкинская карта")
    semantic_tags = [
        tag for tag in tags
        if tag not in {"free", "ticketed", clean_text(event.get("city")).lower()}
    ]
    facts = [
        f"Категория: {category}",
        f"Тип/формат: {clean_text(event.get('event_type')) or 'событие'}",
        f"Название: {clean_text(event.get('title'))}",
        f"Кратко: {clean_text(event.get('summary'))}",
        f"Описание: {clean_text(event.get('description_html'))[:2200]}",
        f"Место/контекст: {join_parts([event.get('venue_name'), event.get('city')])}",
        f"Темы: {', '.join(semantic_tags)}" if semantic_tags else "",
        f"Аудитория/свойства: {', '.join(unique(audience_flags))}" if audience_flags else "",
    ]
    return "\n".join(part for part in facts if part and not part.endswith(": "))[:5200]


def build_card_snapshot(event: dict[str, Any], *, site_origin: str, base_path: str, ics_base_url: str) -> dict[str, Any]:
    category = ru_event_category(event)
    tags = event_tags(event, category)
    ticket = event.get("ticket") or {}
    href = event_href(event, base_path=base_path)
    abs_url = absolute_url(f"/sobytiya/{event.get('slug')}/", site_origin=site_origin, base_path=base_path)
    display_date_time = clean_text(event.get("occurrence_compact_label")) or join_parts(
        [event.get("display_date"), event.get("display_time")]
    )
    occurrence_member_ids = [int(value) for value in (event.get("occurrence_member_ids") or [event["id"]])]
    place = join_parts([event.get("city"), event.get("venue_name")])
    status_label = clean_text(ticket.get("price_label")) or clean_text(ticket.get("label")) or clean_text(event.get("status_label"))
    image_url = clean_text(event.get("image_url"))
    image_assets = [asset for asset in (event.get("image_assets") or []) if isinstance(asset, dict)]
    primary_image = next(
        (asset for asset in image_assets if clean_text(asset.get("src")) == image_url),
        image_assets[0] if image_assets else {},
    )

    def positive_image_dimension(value: Any) -> int | None:
        try:
            numeric = int(value)
        except (TypeError, ValueError):
            return None
        return numeric if numeric > 0 else None

    focal_point = primary_image.get("focal_point") if isinstance(primary_image.get("focal_point"), dict) else {}
    focal_value = focal_point.get("y", primary_image.get("focal_y"))
    try:
        focal_y = float(focal_value)
    except (TypeError, ValueError):
        focal_y = None
    if focal_y is not None and not 0 <= focal_y <= 1:
        focal_y = None
    display = {
        "id": int(event["id"]),
        "event_id": int(event["id"]),
        "title": clean_text(event.get("title")),
        "href": href,
        "absolute_url": abs_url,
        "event_type": clean_text(event.get("event_type")) or None,
        "image_url": image_url or None,
        "image_alt": clean_text(event.get("image_alt")) or f"Афиша события «{clean_text(event.get('title'))}»",
        "image_text_mode": primary_image.get("image_text_mode") or event.get("image_text_mode") or "unknown",
        "image_media_role": clean_text(primary_image.get("media_role")) or clean_text(event.get("image_media_role")) or "unknown_document",
        "image_width": positive_image_dimension(primary_image.get("width")),
        "image_height": positive_image_dimension(primary_image.get("height")),
        "focal_y": focal_y,
        "display_date": clean_text(event.get("display_date")),
        "display_time": clean_text(event.get("display_time")) or None,
        "display_date_time": display_date_time,
        "occurrence_aria_label": clean_text(event.get("occurrence_aria_label")) or display_date_time,
        "occurrence_member_ids": occurrence_member_ids,
        "city": clean_text(event.get("city")) or None,
        "venue_name": clean_text(event.get("venue_name")) or None,
        "place": place,
        "status_label": status_label,
        "price_label": clean_text(ticket.get("price_label")) or None,
        "likes_count": int(event.get("likes_count") or 0),
        "shares_count": int(event.get("shares_count") or 0),
        "calendar_href": calendar_href(event, ics_base_url=ics_base_url, base_path=base_path),
        "calendar_eligible": is_calendar_eligible(event),
        "age_restriction": event.get("age_restriction"),
        "age_restriction_status": event.get("age_restriction_status") or "unknown",
        "age_recommendation": event.get("age_recommendation"),
        "age_recommendation_label": event.get("age_recommendation_label"),
    }
    return {
        "event_id": int(event["id"]),
        "id": int(event["id"]),
        "title": clean_text(event.get("title")),
        "category": category,
        "tags": tags,
        "audience_exclusion_tags": [],
        "city": clean_text(event.get("city")) or None,
        "location_name": clean_text(event.get("venue_name")) or None,
        "date": clean_text(event.get("start_date")),
        "age_restriction": event.get("age_restriction"),
        "age_restriction_status": event.get("age_restriction_status") or "unknown",
        "status": clean_text(event.get("status_label")) or "active",
        "lifecycle_status": clean_text(event.get("lifecycle_status")) or "active",
        "is_free": bool(ticket.get("is_free")),
        "base_similarity": 0,
        "static_score": 0,
        "reason_codes": ["pgvector_search_candidate"],
        "exploration_candidate": False,
        "display": display,
        "occurrence_member_ids": occurrence_member_ids,
    }


@dataclass(frozen=True)
class SearchDoc:
    event_id: int
    document: dict[str, Any]
    search_embedding_input: str
    related_embedding_input: str


def build_search_doc(event: dict[str, Any], *, site_origin: str, base_path: str, ics_base_url: str) -> SearchDoc:
    category = ru_event_category(event)
    tags = event_tags(event, category)
    digest = build_search_digest(event, category, tags)
    related_digest = build_related_digest(event, category, tags)
    # Gemini Embedding 2 has no taskType field; the task is part of the prompt.
    search_embedding_input = f"title: {clean_text(event.get('title'))} | text: {digest}"
    related_embedding_input = f"related-event: title: {clean_text(event.get('title'))} | text: {related_digest}"
    search_text_hash = sha256_text(search_embedding_input)
    related_text_hash = sha256_text(related_embedding_input)
    ticket = event.get("ticket") or {}
    weekday_iso, weekday_ru = event_weekday(event)
    time_of_day = event_time_of_day(event)
    availability = event_availability(event)
    admission = event_admission_type(event)
    start_date = clean_text(event.get("start_date")) or None
    doc = {
        "event_id": int(event["id"]),
        "search_doc_version": "event-search-doc-v3-search-facets",
        "related_doc_version": "event-related-doc-v1",
        "card_snapshot_version": "event-card-v3-media-layout",
        "text_hash": search_text_hash,
        "related_text_hash": related_text_hash,
        "slug": clean_text(event.get("slug")) or None,
        "canonical_path": event_href(event, base_path="").rstrip("/") + "/",
        "title": clean_text(event.get("title")),
        "search_digest": digest,
        "related_digest": related_digest,
        "event_type": clean_text(event.get("event_type")) or None,
        "category": category,
        "tags": tags,
        "city": clean_text(event.get("city")) or None,
        "venue_name": clean_text(event.get("venue_name")) or None,
        "start_date": start_date,
        "end_date": clean_text(event.get("end_date")) or None,
        "date_local": start_date,
        "starts_at": clean_text(event.get("starts_at")) or None,
        "ends_at": clean_text(event.get("end_at")) or None,
        "timezone": clean_text(event.get("timezone")) or "Europe/Kaliningrad",
        "weekday_iso": weekday_iso,
        "weekday_ru": weekday_ru,
        "is_weekend": weekday_iso in {6, 7} if weekday_iso else None,
        "time_of_day": time_of_day,
        "lifecycle_status": clean_text(event.get("lifecycle_status")) or "active",
        "ticket_kind": clean_text(ticket.get("kind")) or None,
        "admission_type": admission,
        "availability_status": availability,
        "price_label": clean_text(ticket.get("price_label")) or None,
        "is_free": bool(ticket.get("is_free")),
        "age_restriction": event.get("age_restriction"),
        "age_restriction_status": event.get("age_restriction_status") or "unknown",
        "active": clean_text(event.get("lifecycle_status")) in {"", "active"} or event.get("lifecycle_status") is None,
        "is_public": True,
        "is_searchable": True,
        "card_snapshot": build_card_snapshot(event, site_origin=site_origin, base_path=base_path, ics_base_url=ics_base_url),
        "source_event_updated_at": clean_text(event.get("updated_at")) or None,
        "indexed_at": datetime.now(timezone.utc).isoformat(),
        "metadata": {
            "source": "static_site_preview_events_json",
            "source_prod_id": event.get("source_prod_id"),
            "image_text_mode": event.get("image_text_mode"),
            "search_text_hash": search_text_hash,
            "related_text_hash": related_text_hash,
        },
    }
    return SearchDoc(
        event_id=int(event["id"]),
        document=doc,
        search_embedding_input=search_embedding_input,
        related_embedding_input=related_embedding_input,
    )


def build_projection_coverage(
    docs: Iterable[SearchDoc],
    *,
    document_rows: Iterable[dict[str, Any]],
    embedding_rows: Iterable[dict[str, Any]],
    document_kinds: Iterable[str],
    embedding_model: str,
    embedding_dim: int,
) -> dict[str, Any]:
    """Reconcile the stored projection against one exact eligible fixture.

    Counts are derived from a fresh post-write inventory, not from attempted
    upserts.  This lets release automation distinguish complete storage from a
    successful HTTP request that left stale, orphaned or incompatible rows.
    """

    docs = list(docs)
    kinds = list(dict.fromkeys(str(kind) for kind in document_kinds))
    expected_docs = {int(doc.event_id): doc for doc in docs}
    eligible_ids = set(expected_docs)
    stored_docs = [dict(row) for row in document_rows]
    stored_embeddings = [dict(row) for row in embedding_rows]

    orphan_document_ids = sorted({
        int(row.get("event_id") or 0)
        for row in stored_docs
        if int(row.get("event_id") or 0) not in eligible_ids
    })
    stored_docs_by_id = {
        int(row.get("event_id") or 0): row
        for row in stored_docs
        if int(row.get("event_id") or 0) in eligible_ids
    }
    missing_document_ids = sorted(eligible_ids - set(stored_docs_by_id))
    stale_document_ids: list[int] = []
    for event_id, doc in expected_docs.items():
        row = stored_docs_by_id.get(event_id)
        if row is None:
            continue
        if any(
            str(row.get(field) or "") != str(doc.document.get(field) or "")
            for field in (
                "text_hash",
                "related_text_hash",
                "search_doc_version",
                "related_doc_version",
            )
        ):
            stale_document_ids.append(event_id)

    expected_embedding_keys = {
        (event_id, kind)
        for event_id in eligible_ids
        for kind in kinds
    }
    present_contract_keys: set[tuple[int, str]] = set()
    current_embedding_keys: set[tuple[int, str]] = set()
    stale_embedding_keys: set[tuple[int, str]] = set()
    wrong_contract_keys: set[tuple[int, str, str, int]] = set()
    wrong_kind_keys: set[tuple[int, str]] = set()
    orphan_embedding_ids: set[int] = set()

    for row in stored_embeddings:
        event_id = int(row.get("event_id") or 0)
        kind = str(row.get("embedding_doc_kind") or "")
        model = str(row.get("embedding_model") or "")
        try:
            dim = int(row.get("embedding_dim") or 0)
        except (TypeError, ValueError):
            dim = 0
        if event_id not in eligible_ids:
            orphan_embedding_ids.add(event_id)
            continue
        if kind not in DOCUMENT_KIND_CONTRACT:
            wrong_kind_keys.add((event_id, kind))
            continue
        if kind not in kinds:
            # A search-only audit does not invalidate a separately maintained
            # related_v1 corpus (and vice versa).
            continue
        if model != str(embedding_model) or dim != int(embedding_dim):
            wrong_contract_keys.add((event_id, kind, model, dim))
            continue
        present_contract_keys.add((event_id, kind))
        doc = expected_docs[event_id]
        hash_key, version_key = DOCUMENT_KIND_CONTRACT[kind]
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        if (
            str(row.get("text_hash") or "")
            != str(doc.document.get(hash_key) or "")
            or str(metadata.get("doc_version") or "")
            != str(doc.document.get(version_key) or "")
        ):
            stale_embedding_keys.add((event_id, kind))
            continue
        current_embedding_keys.add((event_id, kind))

    missing_embedding_keys = sorted(expected_embedding_keys - present_contract_keys)
    current_document_count = len(eligible_ids - set(missing_document_ids) - set(stale_document_ids))
    expected_embedding_count = len(expected_embedding_keys)
    current_embedding_count = len(current_embedding_keys)
    blockers = (
        len(missing_document_ids)
        + len(stale_document_ids)
        + len(orphan_document_ids)
        + len(missing_embedding_keys)
        + len(stale_embedding_keys)
        + len(orphan_embedding_ids)
        + len(wrong_contract_keys)
        + len(wrong_kind_keys)
    )
    return {
        "schema_version": COVERAGE_RECEIPT_SCHEMA_VERSION,
        "status": "complete" if blockers == 0 else "incomplete",
        "eligible_event_count": len(eligible_ids),
        "current_document_count": current_document_count,
        "expected_embedding_count": expected_embedding_count,
        "current_embedding_count": current_embedding_count,
        "document_coverage_percent": round(
            100.0 * current_document_count / max(1, len(eligible_ids)), 6
        ) if eligible_ids else 100.0,
        "embedding_coverage_percent": round(
            100.0 * current_embedding_count / max(1, expected_embedding_count), 6
        ) if expected_embedding_count else 100.0,
        "missing_document_count": len(missing_document_ids),
        "stale_document_count": len(stale_document_ids),
        "orphan_document_count": len(orphan_document_ids),
        "missing_embedding_count": len(missing_embedding_keys),
        "stale_embedding_count": len(stale_embedding_keys),
        "orphan_embedding_count": len(orphan_embedding_ids),
        "wrong_model_or_dimension_count": len(wrong_contract_keys),
        "wrong_document_kind_count": len(wrong_kind_keys),
        # IDs are bounded operational evidence and contain no user/query data.
        "missing_document_ids": missing_document_ids,
        "stale_document_ids": sorted(stale_document_ids),
        "orphan_document_ids": orphan_document_ids,
        "missing_embedding_keys": [
            {"event_id": event_id, "document_kind": kind}
            for event_id, kind in missing_embedding_keys
        ],
        "stale_embedding_keys": [
            {"event_id": event_id, "document_kind": kind}
            for event_id, kind in sorted(stale_embedding_keys)
        ],
    }


def env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required env: {name}")
    return value


def supabase_request(method: str, path: str, *, body: Any | None = None, timeout: float = 30.0) -> Any:
    base_url = env_required("PERSONALIZATION_SUPABASE_URL").rstrip("/")
    key = os.getenv("PERSONALIZATION_SUPABASE_SECRET_KEY") or os.getenv("PERSONALIZATION_SUPABASE_SERVICE_ROLE_KEY")
    if not key:
        raise SystemExit("Missing PERSONALIZATION_SUPABASE_SECRET_KEY for backend vector sync")
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if method in {"POST", "PATCH"}:
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
    data = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8") if body is not None else None
    req = urllib.request.Request(f"{base_url}/rest/v1/{path}", data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:1500]
        raise RuntimeError(f"Supabase REST {method} {path} failed HTTP {exc.code}: {detail}") from exc


def fetch_existing_embeddings(event_ids: list[int], *, model: str, dim: int, doc_kind: str) -> dict[int, str]:
    if not event_ids:
        return {}
    out: dict[int, str] = {}
    chunk_size = 80
    for start in range(0, len(event_ids), chunk_size):
        chunk = event_ids[start : start + chunk_size]
        in_list = ",".join(str(int(x)) for x in chunk)
        model_q = urllib.parse.quote(model, safe="")
        kind_q = urllib.parse.quote(doc_kind, safe="")
        path = f"event_embeddings?select=event_id,text_hash&embedding_model=eq.{model_q}&embedding_dim=eq.{int(dim)}&embedding_doc_kind=eq.{kind_q}&event_id=in.({in_list})"
        rows = supabase_request("GET", path) or []
        for row in rows:
            out[int(row["event_id"])] = str(row.get("text_hash") or "")
    return out


def upsert_documents(docs: list[SearchDoc], *, chunk_size: int = 200) -> int:
    sent = 0
    for start in range(0, len(docs), chunk_size):
        chunk = [doc.document for doc in docs[start : start + chunk_size]]
        supabase_request("POST", "event_search_documents?on_conflict=event_id", body=chunk, timeout=45.0)
        sent += len(chunk)
    return sent


def upsert_embeddings(rows: list[dict[str, Any]], *, chunk_size: int = 50) -> int:
    sent = 0
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start : start + chunk_size]
        supabase_request("POST", "event_embeddings?on_conflict=event_id,embedding_model,embedding_dim,embedding_doc_kind", body=chunk, timeout=60.0)
        sent += len(chunk)
    return sent


def patch_embedding_revision_metadata(
    event_ids: Iterable[int],
    *,
    model: str,
    dim: int,
    doc_kind: str,
    metadata: dict[str, Any],
    chunk_size: int = 80,
) -> int:
    """Advance revision metadata on unchanged vectors without provider calls."""

    ids = sorted({int(event_id) for event_id in event_ids})
    model_q = urllib.parse.quote(model, safe="")
    kind_q = urllib.parse.quote(doc_kind, safe="")
    sent = 0
    for start in range(0, len(ids), max(1, int(chunk_size))):
        chunk = ids[start : start + max(1, int(chunk_size))]
        in_list = ",".join(str(event_id) for event_id in chunk)
        supabase_request(
            "PATCH",
            "event_embeddings?"
            f"event_id=in.({in_list})&embedding_model=eq.{model_q}"
            f"&embedding_dim=eq.{int(dim)}&embedding_doc_kind=eq.{kind_q}",
            body={"metadata": metadata},
            timeout=45.0,
        )
        sent += len(chunk)
    return sent


def fetch_indexed_event_ids() -> set[int]:
    out: set[int] = set()
    page_size = 1000
    offset = 0
    while True:
        rows = supabase_request(
            "GET",
            f"event_search_documents?select=event_id&order=event_id.asc&limit={page_size}&offset={offset}",
        ) or []
        out.update(int(row["event_id"]) for row in rows if row.get("event_id") is not None)
        if len(rows) < page_size:
            return out
        offset += page_size


def fetch_projection_inventory(*, page_size: int = 1000) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Read a complete narrow inventory for the post-sync coverage receipt."""

    def fetch_all(table: str, projection: str, *, order: str) -> list[dict[str, Any]]:
        rows_out: list[dict[str, Any]] = []
        offset = 0
        while True:
            rows = supabase_request(
                "GET",
                f"{table}?select={projection}&order={order}&limit={page_size}&offset={offset}",
            ) or []
            rows_out.extend(dict(row) for row in rows)
            if len(rows) < page_size:
                return rows_out
            offset += page_size

    documents = fetch_all(
        "event_search_documents",
        "event_id,text_hash,related_text_hash,search_doc_version,related_doc_version,metadata",
        order="event_id.asc",
    )
    embeddings = fetch_all(
        "event_embeddings",
        "event_id,embedding_model,embedding_dim,embedding_doc_kind,text_hash,metadata",
        order="event_id.asc,embedding_model.asc,embedding_dim.asc,embedding_doc_kind.asc",
    )
    return documents, embeddings


def noncanonical_embedding_rows(
    docs: Iterable[SearchDoc],
    embedding_rows: Iterable[dict[str, Any]],
    *,
    document_kinds: Iterable[str],
    embedding_model: str,
    embedding_dim: int,
) -> list[dict[str, Any]]:
    """Select rows that cannot belong to the requested authoritative corpus."""

    expected = {int(doc.event_id): doc for doc in docs}
    requested = set(document_kinds)
    rejected: list[dict[str, Any]] = []
    for raw in embedding_rows:
        row = dict(raw)
        event_id = int(row.get("event_id") or 0)
        kind = str(row.get("embedding_doc_kind") or "")
        if event_id not in expected or kind not in DOCUMENT_KIND_CONTRACT:
            rejected.append(row)
            continue
        if kind not in requested:
            continue
        try:
            dim = int(row.get("embedding_dim") or 0)
        except (TypeError, ValueError):
            dim = 0
        doc = expected[event_id]
        hash_key, version_key = DOCUMENT_KIND_CONTRACT[kind]
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        if (
            str(row.get("embedding_model") or "") != str(embedding_model)
            or dim != int(embedding_dim)
            or str(row.get("text_hash") or "") != str(doc.document.get(hash_key) or "")
            or str(metadata.get("doc_version") or "")
            != str(doc.document.get(version_key) or "")
        ):
            rejected.append(row)
    return rejected


def delete_embedding_rows(rows: Iterable[dict[str, Any]]) -> int:
    """Delete exact incompatible embedding identities without broad filters."""

    identities = sorted({
        (
            int(row.get("event_id") or 0),
            str(row.get("embedding_model") or ""),
            int(row.get("embedding_dim") or 0),
            str(row.get("embedding_doc_kind") or ""),
        )
        for row in rows
    })
    for event_id, model, dim, kind in identities:
        model_q = urllib.parse.quote(model, safe="")
        kind_q = urllib.parse.quote(kind, safe="")
        supabase_request(
            "DELETE",
            "event_embeddings?"
            f"event_id=eq.{event_id}&embedding_model=eq.{model_q}"
            f"&embedding_dim=eq.{dim}&embedding_doc_kind=eq.{kind_q}",
            timeout=45.0,
        )
    return len(identities)


def delete_stale_events(event_ids: Iterable[int], *, chunk_size: int = 80) -> int:
    """Remove sidecar projections absent from the authoritative full fixture.

    Embeddings are deleted explicitly before documents so this remains safe if a
    deployment has not applied the FK cascade from the canonical migration.
    """

    stale = sorted({int(event_id) for event_id in event_ids})
    for start in range(0, len(stale), max(1, int(chunk_size))):
        chunk = stale[start : start + max(1, int(chunk_size))]
        in_list = ",".join(str(event_id) for event_id in chunk)
        supabase_request("DELETE", f"event_embeddings?event_id=in.({in_list})", timeout=45.0)
        supabase_request("DELETE", f"event_search_documents?event_id=in.({in_list})", timeout=45.0)
    return len(stale)


def write_report(report: dict[str, Any], path: str) -> None:
    if not path:
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def build_embedding_client(*, key_env: str) -> GoogleAIClient:
    """Build a fail-closed embedding client backed by the shared quota ledger."""

    from google_ai.limiter_supabase import build_google_ai_limiter_supabase_client

    def legacy_limiter_client():
        try:
            from supabase import create_client
        except ImportError as exc:  # pragma: no cover - deployment dependency guard
            raise SystemExit("supabase package is required for shared Google AI limiting") from exc

        url = env_required("SUPABASE_URL")
        service_key = os.getenv("SUPABASE_SERVICE_KEY", "").strip()
        if not service_key:
            raise SystemExit("Missing SUPABASE_SERVICE_KEY for shared Google AI limiting")
        return create_client(url, service_key)

    return GoogleAIClient(
        supabase_client=build_google_ai_limiter_supabase_client(
            fallback_factory=legacy_limiter_client,
            require_configured=True,
        ),
        secrets_provider=SecretsProvider(),
        consumer="event_vector_sync",
        default_env_var_name=key_env,
    )


def gemini_embed(
    text: str,
    *,
    model: str,
    dim: int,
    client: GoogleAIClient,
    rate_limit_retries: int = 0,
    rate_limit_max_wait_seconds: float = 65.0,
    sleep_fn=time.sleep,
    jitter_fn=lambda: random.uniform(0.05, 0.25),
) -> list[float]:
    """Embed through the shared gateway and smooth minute-bucket admission.

    The shared client intentionally follows a NO_WAIT contract. A batch
    projector may wait for a bounded RPM/TPM minute bucket and retry the same
    idempotent embedding input. Day-level exhaustion and unknown admission
    failures still fail closed immediately.
    """

    retries = max(0, int(rate_limit_retries))
    max_wait = max(0.0, float(rate_limit_max_wait_seconds))
    attempt = 0
    while True:
        try:
            values, _usage = asyncio.run(
                client.embed_content_async(
                    model=model,
                    text=text,
                    output_dimensionality=dim,
                )
            )
            return list(values)
        except RateLimitError as exc:
            if exc.blocked_reason not in {"rpm", "tpm"} or attempt >= retries:
                raise
            requested_wait = max(0.25, float(exc.retry_after_ms or 1000) / 1000.0)
            if requested_wait > max_wait:
                raise
            attempt += 1
            wait_seconds = requested_wait + max(0.0, float(jitter_fn()))
            print(
                json.dumps(
                    {
                        "stage": "embedding_rate_limit_wait",
                        "blocked_reason": exc.blocked_reason,
                        "retry": attempt,
                        "wait_seconds": round(wait_seconds, 3),
                    },
                    ensure_ascii=False,
                ),
                file=sys.stderr,
                flush=True,
            )
            sleep_fn(wait_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync static-site event search docs + Gemini embeddings to personalization Supabase pgvector.")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--preview-events-json", default=str(DEFAULT_PREVIEW_JSON))
    parser.add_argument("--site-origin", default=os.getenv("PUBLIC_SITE_ORIGIN") or "https://kenigevents.ru")
    parser.add_argument("--base-path", default=os.getenv("PUBLIC_PREVIEW_BUILD_ID") or "")
    parser.add_argument("--ics-base-url", default=os.getenv("PUBLIC_ICS_BASE_URL") or (f"{(os.getenv('PUBLIC_ASSET_BASE_URL') or 'https://static.kenigevents.ru').rstrip('/')}/ics"))
    parser.add_argument("--embedding-model", default="gemini-embedding-2")
    parser.add_argument("--embedding-dim", type=int, default=768)
    parser.add_argument("--google-key-env", default="GOOGLE_API_KEY4")
    parser.add_argument(
        "--document-kinds",
        default="search_v3,related_v1",
        help="Comma-separated embedding document kinds to sync: search_v3,related_v1.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Limit events for a canary backfill; 0 = all in fixture.")
    parser.add_argument("--event-ids", default="", help="Comma-separated event IDs to sync.")
    parser.add_argument("--max-provider-calls", type=int, default=1000)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--rate-limit-retries", type=int, default=3)
    parser.add_argument("--rate-limit-max-wait-seconds", type=float, default=65.0)
    parser.add_argument("--upsert-chunk-size", type=int, default=20)
    parser.add_argument("--force", action="store_true", help="Regenerate embeddings even when text_hash matches.")
    parser.add_argument(
        "--prune-missing",
        action="store_true",
        help="Delete sidecar rows absent from this authoritative full-catalog fixture.",
    )
    parser.add_argument(
        "--require-complete",
        action="store_true",
        help="Exit non-zero when the provider-call cap leaves any requested embedding missing.",
    )
    parser.add_argument("--report-json", default="", help="Write the final structured run report to this path.")
    parser.add_argument("--apply", action="store_true", help="Write documents/embeddings to Supabase. Without this, only prints a plan.")
    parser.add_argument("--dry-run", action="store_true", help="Explicit no-op alias for the default planning mode.")
    args = parser.parse_args()

    load_env(ROOT / args.env_file)
    fixture = json.loads(Path(args.preview_events_json).read_text(encoding="utf-8"))
    events = [dict(event) for event in (fixture.get("events") or [])]
    computed_catalog_revision = exported_search_catalog_revision(events)
    fixture_catalog_revision = str(
        ((fixture.get("build") or {}).get("catalog_revision") or "")
    ).strip()
    if fixture_catalog_revision:
        if not re.fullmatch(r"[0-9a-f]{64}", fixture_catalog_revision):
            raise SystemExit("preview fixture catalog_revision is not a lowercase SHA-256")
        if fixture_catalog_revision != computed_catalog_revision:
            raise SystemExit(
                "preview fixture catalog_revision does not match its exact event payload"
            )
    catalog_revision = fixture_catalog_revision or computed_catalog_revision
    occurrence_projections = build_occurrence_projections(events)
    for event in events:
        event.update(occurrence_projections.get(int(event.get("id") or 0), {}))
    if args.event_ids.strip():
        wanted = {int(part) for part in args.event_ids.split(",") if part.strip()}
        events = [event for event in events if int(event.get("id") or 0) in wanted]
    if args.limit and args.limit > 0:
        events = events[: args.limit]
    docs = [build_search_doc(event, site_origin=args.site_origin, base_path=args.base_path, ics_base_url=args.ics_base_url) for event in events]
    document_kinds = [
        item.strip()
        for item in str(args.document_kinds or "").split(",")
        if item.strip()
    ] or ["search_v3", "related_v1"]
    invalid_kinds = sorted(set(document_kinds) - {"search_v3", "related_v1"})
    if invalid_kinds:
        raise SystemExit(f"Unsupported document kinds: {', '.join(invalid_kinds)}")
    revision_contract = build_revision_contract(
        docs,
        catalog_revision=catalog_revision,
        embedding_model=args.embedding_model,
        embedding_dim=args.embedding_dim,
    )
    annotate_search_documents(docs, revision_contract)
    corpus_hashes = {
        kind: revision_contract["corpora"][kind]["revision"]
        for kind in DOCUMENT_KIND_CONTRACT
    }

    report: dict[str, Any] = {
        "preview_events_json": str(args.preview_events_json),
        "events": len(events),
        "embedding_model": args.embedding_model,
        "embedding_dim": args.embedding_dim,
        "document_kinds": document_kinds,
        "apply": bool(args.apply),
        "site_origin": args.site_origin,
        "base_path": args.base_path,
        "catalog_revision": catalog_revision,
        "catalog_revision_source": "fixture" if fixture_catalog_revision else "computed_legacy_fixture",
        "corpus_revision": revision_contract["corpus_revision"],
        "search_document_revision": revision_contract["search_document_revision"],
        "revision_contract": revision_contract,
        "search_v3_hash": corpus_hashes["search_v3"],
        "related_v1_hash": corpus_hashes["related_v1"],
    }
    if not args.apply:
        report["sample"] = [
            {
                "event_id": doc.event_id,
                "search_text_hash": doc.document["text_hash"],
                "related_text_hash": doc.document["related_text_hash"],
                "title": doc.document["title"],
                "search_digest_chars": len(doc.document["search_digest"]),
                "related_digest_chars": len(doc.document["related_digest"] or ""),
            }
            for doc in docs[:5]
        ]
        write_report(report, args.report_json)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    upserted_docs = upsert_documents(docs)
    existing_by_kind = {
        kind: fetch_existing_embeddings(
            [doc.event_id for doc in docs],
            model=args.embedding_model,
            dim=args.embedding_dim,
            doc_kind=kind,
        )
        for kind in document_kinds
    }
    rows: list[dict[str, Any]] = []
    provider_calls = 0
    embedding_client: GoogleAIClient | None = None
    skipped_by_kind = {kind: 0 for kind in document_kinds}
    skipped_ids_by_kind: dict[str, list[int]] = {kind: [] for kind in document_kinds}
    upserted_embeddings = 0
    started_at = time.monotonic()
    for doc in docs:
        for doc_kind in document_kinds:
            if doc_kind == "search_v3":
                embedding_input = doc.search_embedding_input
                text_hash = str(doc.document["text_hash"])
            else:
                embedding_input = doc.related_embedding_input
                text_hash = str(doc.document["related_text_hash"])
            if not args.force and existing_by_kind.get(doc_kind, {}).get(doc.event_id) == text_hash:
                skipped_by_kind[doc_kind] += 1
                skipped_ids_by_kind[doc_kind].append(doc.event_id)
                continue
            if provider_calls >= max(0, int(args.max_provider_calls)):
                # The call cap limits provider work, not verification. Keep
                # scanning the remaining documents so a zero-call audit can
                # prove that every stored embedding is current.
                continue
            if embedding_client is None:
                embedding_client = build_embedding_client(key_env=args.google_key_env)
            vector = gemini_embed(
                embedding_input,
                model=args.embedding_model,
                dim=args.embedding_dim,
                client=embedding_client,
                rate_limit_retries=args.rate_limit_retries,
                rate_limit_max_wait_seconds=args.rate_limit_max_wait_seconds,
            )
            provider_calls += 1
            rows.append({
                "event_id": doc.event_id,
                "embedding_model": args.embedding_model,
                "embedding_dim": int(args.embedding_dim),
                "embedding_doc_kind": doc_kind,
                "embedding": vector,
                "text_hash": text_hash,
                "embedded_at": datetime.now(timezone.utc).isoformat(),
                "metadata": embedding_revision_metadata(
                    doc,
                    document_kind=doc_kind,
                    catalog_revision=catalog_revision,
                    revision_contract=revision_contract,
                ),
            })
            if len(rows) >= max(1, int(args.upsert_chunk_size)):
                upserted_embeddings += upsert_embeddings(rows)
                rows = []
                print(
                    json.dumps({
                        "stage": "embedding_partial_upsert",
                        "provider_calls": provider_calls,
                        "skipped_by_kind": skipped_by_kind,
                        "elapsed_seconds": round(time.monotonic() - started_at, 1),
                    }, ensure_ascii=False),
                    file=sys.stderr,
                    flush=True,
                )
            if args.sleep_seconds > 0:
                time.sleep(float(args.sleep_seconds))
    if rows:
        upserted_embeddings += upsert_embeddings(rows)
    embedding_metadata_patched = 0
    if args.prune_missing:
        docs_by_id = {doc.event_id: doc for doc in docs}
        for doc_kind, skipped_ids in skipped_ids_by_kind.items():
            if not skipped_ids:
                continue
            sample = docs_by_id[skipped_ids[0]]
            embedding_metadata_patched += patch_embedding_revision_metadata(
                skipped_ids,
                model=args.embedding_model,
                dim=args.embedding_dim,
                doc_kind=doc_kind,
                metadata=embedding_revision_metadata(
                    sample,
                    document_kind=doc_kind,
                    catalog_revision=catalog_revision,
                    revision_contract=revision_contract,
                ),
            )
    skipped_total = sum(skipped_by_kind.values())
    expected_embeddings = len(docs) * len(document_kinds)
    not_embedded_due_call_cap = max(0, expected_embeddings - skipped_total - provider_calls)
    stale_event_ids: list[int] = []
    if args.prune_missing:
        indexed_ids = fetch_indexed_event_ids()
        fixture_ids = {doc.event_id for doc in docs}
        stale_event_ids = sorted(indexed_ids - fixture_ids)
        delete_stale_events(stale_event_ids)
    coverage: dict[str, Any] | None = None
    noncanonical_embeddings_deleted = 0
    if args.prune_missing:
        document_rows, embedding_rows = fetch_projection_inventory()
        rejected_embeddings = noncanonical_embedding_rows(
            docs,
            embedding_rows,
            document_kinds=document_kinds,
            embedding_model=args.embedding_model,
            embedding_dim=args.embedding_dim,
        )
        if rejected_embeddings:
            noncanonical_embeddings_deleted = delete_embedding_rows(
                rejected_embeddings
            )
            document_rows, embedding_rows = fetch_projection_inventory()
        coverage = build_projection_coverage(
            docs,
            document_rows=document_rows,
            embedding_rows=embedding_rows,
            document_kinds=document_kinds,
            embedding_model=args.embedding_model,
            embedding_dim=args.embedding_dim,
        )
    projection_complete = bool(
        not_embedded_due_call_cap == 0
        and (coverage is None or coverage["status"] == "complete")
    )
    report.update({
        "documents_upserted": upserted_docs,
        "embeddings_upserted": upserted_embeddings,
        "embeddings_skipped_unchanged": skipped_total,
        "embeddings_skipped_by_kind": skipped_by_kind,
        "embedding_revision_metadata_patched": embedding_metadata_patched,
        "provider_calls": provider_calls,
        "not_embedded_due_call_cap": not_embedded_due_call_cap,
        "stale_events_deleted": len(stale_event_ids),
        "stale_event_ids": stale_event_ids,
        "noncanonical_embeddings_deleted": noncanonical_embeddings_deleted,
        "coverage": coverage,
        "complete": projection_complete,
        "elapsed_seconds": round(time.monotonic() - started_at, 3),
    })
    if coverage is not None:
        report.update({
            key: coverage[key]
            for key in (
                "eligible_event_count",
                "current_document_count",
                "expected_embedding_count",
                "current_embedding_count",
                "document_coverage_percent",
                "embedding_coverage_percent",
                "missing_document_count",
                "stale_document_count",
                "orphan_document_count",
                "missing_embedding_count",
                "stale_embedding_count",
                "orphan_embedding_count",
                "wrong_model_or_dimension_count",
                "wrong_document_kind_count",
            )
        })
    write_report(report, args.report_json)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if args.require_complete and not projection_complete:
        print(
            "vector sync incomplete: "
            f"{not_embedded_due_call_cap} embeddings left by provider-call cap; "
            f"coverage={coverage['status'] if coverage else 'not_requested'}",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
