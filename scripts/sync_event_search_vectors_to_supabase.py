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
import hashlib
import html
import json
import os
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
    else:
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
    availability = event_availability(event)
    admission = event_admission_type(event)
    facts = [
        f"Категория: {category}",
        f"Тип: {clean_text(event.get('event_type')) or 'событие'}",
        f"Название: {clean_text(event.get('title'))}",
        f"Кратко: {clean_text(event.get('summary'))}",
        f"Описание: {clean_text(event.get('description_html'))[:2500]}",
        f"Место: {join_parts([event.get('venue_name'), event.get('address'), event.get('city')])}",
        f"Дата: {join_parts([event.get('display_date'), event.get('display_time')])}",
        f"День недели: {weekday_ru}" if weekday_ru else "",
        f"День недели ISO: {weekday_iso}" if weekday_iso else "",
        f"Время суток: {time_of_day}" if time_of_day else "",
        f"Темы: {', '.join(tags)}",
        f"Условия: {join_parts([admission, availability, ticket.get('label'), ticket.get('price_label'), ticket.get('status')])}",
    ]
    return "\n".join(part for part in facts if part and not part.endswith(": "))[:7000]


def build_card_snapshot(event: dict[str, Any], *, site_origin: str, base_path: str, ics_base_url: str) -> dict[str, Any]:
    category = ru_event_category(event)
    tags = event_tags(event, category)
    ticket = event.get("ticket") or {}
    href = event_href(event, base_path=base_path)
    abs_url = absolute_url(f"/sobytiya/{event.get('slug')}/", site_origin=site_origin, base_path=base_path)
    display_date_time = join_parts([event.get("display_date"), event.get("display_time")])
    place = join_parts([event.get("city"), event.get("venue_name")])
    status_label = clean_text(ticket.get("price_label")) or clean_text(ticket.get("label")) or clean_text(event.get("status_label"))
    display = {
        "id": int(event["id"]),
        "event_id": int(event["id"]),
        "title": clean_text(event.get("title")),
        "href": href,
        "absolute_url": abs_url,
        "event_type": clean_text(event.get("event_type")) or None,
        "image_url": clean_text(event.get("image_url")) or None,
        "image_alt": clean_text(event.get("image_alt")) or f"Афиша события «{clean_text(event.get('title'))}»",
        "image_text_mode": event.get("image_text_mode") or "unknown",
        "display_date": clean_text(event.get("display_date")),
        "display_time": clean_text(event.get("display_time")) or None,
        "display_date_time": display_date_time,
        "city": clean_text(event.get("city")) or None,
        "venue_name": clean_text(event.get("venue_name")) or None,
        "place": place,
        "status_label": status_label,
        "price_label": clean_text(ticket.get("price_label")) or None,
        "likes_count": int(event.get("likes_count") or 0),
        "shares_count": int(event.get("shares_count") or 0),
        "calendar_href": calendar_href(event, ics_base_url=ics_base_url, base_path=base_path),
        "calendar_eligible": is_calendar_eligible(event),
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
        "status": clean_text(event.get("status_label")) or "active",
        "lifecycle_status": clean_text(event.get("lifecycle_status")) or "active",
        "is_free": bool(ticket.get("is_free")),
        "base_similarity": 0,
        "static_score": 0,
        "reason_codes": ["pgvector_search_candidate"],
        "exploration_candidate": False,
        "display": display,
    }


@dataclass(frozen=True)
class SearchDoc:
    event_id: int
    document: dict[str, Any]
    embedding_input: str


def build_search_doc(event: dict[str, Any], *, site_origin: str, base_path: str, ics_base_url: str) -> SearchDoc:
    category = ru_event_category(event)
    tags = event_tags(event, category)
    digest = build_search_digest(event, category, tags)
    # Gemini Embedding 2 has no taskType field; the task is part of the prompt.
    embedding_input = f"title: {clean_text(event.get('title'))} | text: {digest}"
    text_hash = sha256_text(embedding_input)
    ticket = event.get("ticket") or {}
    weekday_iso, weekday_ru = event_weekday(event)
    time_of_day = event_time_of_day(event)
    availability = event_availability(event)
    admission = event_admission_type(event)
    start_date = clean_text(event.get("start_date")) or None
    doc = {
        "event_id": int(event["id"]),
        "search_doc_version": "event-search-doc-v2-weekday",
        "card_snapshot_version": "event-card-v1",
        "text_hash": text_hash,
        "slug": clean_text(event.get("slug")) or None,
        "canonical_path": event_href(event, base_path="").rstrip("/") + "/",
        "title": clean_text(event.get("title")),
        "search_digest": digest,
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
        },
    }
    return SearchDoc(event_id=int(event["id"]), document=doc, embedding_input=embedding_input)


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


def fetch_existing_embeddings(event_ids: list[int], *, model: str, dim: int) -> dict[int, str]:
    if not event_ids:
        return {}
    out: dict[int, str] = {}
    chunk_size = 80
    for start in range(0, len(event_ids), chunk_size):
        chunk = event_ids[start : start + chunk_size]
        in_list = ",".join(str(int(x)) for x in chunk)
        model_q = urllib.parse.quote(model, safe="")
        path = f"event_embeddings?select=event_id,text_hash&embedding_model=eq.{model_q}&embedding_dim=eq.{int(dim)}&event_id=in.({in_list})"
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
        supabase_request("POST", "event_embeddings?on_conflict=event_id,embedding_model,embedding_dim", body=chunk, timeout=60.0)
        sent += len(chunk)
    return sent


def gemini_embed(text: str, *, model: str, dim: int, key_env: str, timeout: float = 45.0, retries: int = 3) -> list[float]:
    key = env_required(key_env)
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:embedContent"
    body = {
        "model": f"models/{model}",
        "content": {"parts": [{"text": text}]},
        "outputDimensionality": int(dim),
    }
    req = urllib.request.Request(
        endpoint,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json", "x-goog-api-key": key},
    )
    last_error: Exception | None = None
    for attempt in range(max(1, retries)):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
            break
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:1500]
            last_error = RuntimeError(f"Gemini embedding failed HTTP {exc.code}: {detail}")
            if exc.code not in {429, 500, 502, 503, 504} or attempt >= max(1, retries) - 1:
                raise last_error from exc
            time.sleep(min(8.0, 1.5 * (attempt + 1)))
        except Exception as exc:
            last_error = exc
            if attempt >= max(1, retries) - 1:
                raise
            time.sleep(min(8.0, 1.5 * (attempt + 1)))
    else:
        raise RuntimeError(f"Gemini embedding failed: {last_error}")
    values = payload.get("embedding", {}).get("values")
    if not isinstance(values, list) or len(values) != int(dim):
        raise RuntimeError(f"Gemini embedding returned unexpected dimension: {len(values) if isinstance(values, list) else 'missing'}")
    return [float(x) for x in values]


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
    parser.add_argument("--limit", type=int, default=0, help="Limit events for a canary backfill; 0 = all in fixture.")
    parser.add_argument("--event-ids", default="", help="Comma-separated event IDs to sync.")
    parser.add_argument("--max-provider-calls", type=int, default=1000)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--force", action="store_true", help="Regenerate embeddings even when text_hash matches.")
    parser.add_argument("--apply", action="store_true", help="Write documents/embeddings to Supabase. Without this, only prints a plan.")
    parser.add_argument("--dry-run", action="store_true", help="Explicit no-op alias for the default planning mode.")
    args = parser.parse_args()

    load_env(ROOT / args.env_file)
    fixture = json.loads(Path(args.preview_events_json).read_text(encoding="utf-8"))
    events = list(fixture.get("events") or [])
    if args.event_ids.strip():
        wanted = {int(part) for part in args.event_ids.split(",") if part.strip()}
        events = [event for event in events if int(event.get("id") or 0) in wanted]
    if args.limit and args.limit > 0:
        events = events[: args.limit]
    docs = [build_search_doc(event, site_origin=args.site_origin, base_path=args.base_path, ics_base_url=args.ics_base_url) for event in events]

    report: dict[str, Any] = {
        "preview_events_json": str(args.preview_events_json),
        "events": len(events),
        "embedding_model": args.embedding_model,
        "embedding_dim": args.embedding_dim,
        "apply": bool(args.apply),
        "site_origin": args.site_origin,
        "base_path": args.base_path,
    }
    if not args.apply:
        report["sample"] = [
            {"event_id": doc.event_id, "text_hash": doc.document["text_hash"], "title": doc.document["title"], "digest_chars": len(doc.document["search_digest"])}
            for doc in docs[:5]
        ]
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    upserted_docs = upsert_documents(docs)
    existing = fetch_existing_embeddings([doc.event_id for doc in docs], model=args.embedding_model, dim=args.embedding_dim)
    rows: list[dict[str, Any]] = []
    provider_calls = 0
    skipped = 0
    for doc in docs:
        if not args.force and existing.get(doc.event_id) == doc.document["text_hash"]:
            skipped += 1
            continue
        if provider_calls >= max(0, int(args.max_provider_calls)):
            break
        vector = gemini_embed(doc.embedding_input, model=args.embedding_model, dim=args.embedding_dim, key_env=args.google_key_env)
        provider_calls += 1
        rows.append({
            "event_id": doc.event_id,
            "embedding_model": args.embedding_model,
            "embedding_dim": int(args.embedding_dim),
            "embedding": vector,
            "text_hash": doc.document["text_hash"],
            "embedded_at": datetime.now(timezone.utc).isoformat(),
            "metadata": {"search_doc_version": doc.document["search_doc_version"]},
        })
        if args.sleep_seconds > 0:
            time.sleep(float(args.sleep_seconds))
    upserted_embeddings = upsert_embeddings(rows) if rows else 0
    report.update({
        "documents_upserted": upserted_docs,
        "embeddings_upserted": upserted_embeddings,
        "embeddings_skipped_unchanged": skipped,
        "provider_calls": provider_calls,
        "not_embedded_due_call_cap": max(0, len(docs) - skipped - provider_calls),
    })
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
