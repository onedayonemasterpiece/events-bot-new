#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.util
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import smart_event_update as su
from smart_update_lollipop_lab import editorial_layout_family as layout_family
from smart_update_lollipop_lab import facts_prioritize_family as prioritize_family
from smart_update_lollipop_lab import legacy_writer_family
from smart_update_lollipop_lab import writer_final_4o_family as writer_final_family
from smart_update_lollipop_lab import writer_pack_compose_family as writer_pack_family
from smart_update_lollipop_lab import full_cascade as cascade_family


ARTIFACTS_ROOT = PROJECT_ROOT / "artifacts" / "codex"
DEFAULT_BASELINE_MODEL = "gemma-3-27b-it"
DEFAULT_CANDIDATE_MODEL = "gemma-4-31b-it"
DEFAULT_4O_MODEL = "gpt-4o"
DEFAULT_PROD_DB_EVENT_IDS = "4517,4518,4208"
MONTHS_RU = [
    "января",
    "февраля",
    "марта",
    "апреля",
    "мая",
    "июня",
    "июля",
    "августа",
    "сентября",
    "октября",
    "ноября",
    "декабря",
]


class StageBenchmarkFixture:
    __slots__ = (
        "fixture_id",
        "title",
        "event_type",
        "date",
        "time",
        "location_name",
        "location_address",
        "city",
        "sources",
        "end_date",
        "end_date_is_inferred",
        "ticket_price_min",
        "ticket_price_max",
        "ticket_link",
        "ticket_status",
        "is_free",
        "pushkin_card",
        "lifecycle_status",
        "telegraph_url",
        "source_post_url",
        "baseline_description_md",
        "baseline_short_description",
        "baseline_search_digest",
        "baseline_raw_facts",
        "baseline_facts_text_clean",
        "baseline_source_artifact",
    )

    def __init__(self, **kwargs: Any) -> None:
        for name in self.__slots__:
            setattr(self, name, kwargs.get(name))
        self.sources = list(kwargs.get("sources") or [])
        self.end_date_is_inferred = bool(kwargs.get("end_date_is_inferred") or False)
        self.baseline_raw_facts = list(kwargs.get("baseline_raw_facts") or [])
        self.baseline_facts_text_clean = list(kwargs.get("baseline_facts_text_clean") or [])


def _load_benchmark_module() -> Any:
    path = Path(__file__).with_name("benchmark_lollipop_g4.py")
    spec = importlib.util.spec_from_file_location("benchmark_lollipop_g4", path)
    if not spec or not spec.loader:
        raise RuntimeError(f"Cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


bench = _load_benchmark_module()


def _load_env_file() -> None:
    candidates = [
        PROJECT_ROOT / ".env",
        PROJECT_ROOT.parent / "events-bot-new" / ".env",
    ]
    for env_path in candidates:
        if not env_path.exists():
            continue
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
        return


def _set_smart_update_model(model: str) -> None:
    su.SMART_UPDATE_MODEL = model


def _text_hash(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()[:16]


def _clip(value: Any, limit: int = 900) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text if len(text) <= limit else text[:limit].rstrip() + "..."


def _object_to_dict(value: Any) -> dict[str, Any]:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return dict(value)
    out: dict[str, Any] = {}
    for name in getattr(value, "__slots__", []):
        raw = getattr(value, name, None)
        if name == "sources":
            out[name] = [_object_to_dict(item) for item in list(raw or [])]
        else:
            out[name] = raw
    return out


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    try:
        return bool(int(value))
    except Exception:
        return str(value).strip().casefold() in {"true", "yes", "on"}


def _parse_iso_date(raw: str | None) -> date | None:
    value = (raw or "").strip()
    if not value:
        return None
    if ".." in value:
        value = value.split("..", 1)[0].strip()
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _format_day_month(raw: str | None) -> str:
    parsed = _parse_iso_date(raw)
    if not parsed:
        return (raw or "").strip()
    return f"{parsed.day} {MONTHS_RU[parsed.month - 1]}"


def _format_money(value: Any) -> str:
    try:
        amount = int(value)
    except Exception:
        return ""
    return f"{amount} ₽"


def _summary_infoblock_lines(fixture: Any) -> list[str]:
    lines: list[str] = []
    lifecycle = str(getattr(fixture, "lifecycle_status", "") or "").strip().casefold()
    if lifecycle and lifecycle != "active":
        if lifecycle == "cancelled":
            lines.append("❌ Отменено")
        elif lifecycle == "postponed":
            lines.append("⏸ Перенесено")
        else:
            lines.append(f"⛔ Статус: {lifecycle}")

    event_type = str(getattr(fixture, "event_type", "") or "").strip().casefold()
    raw_date = str(getattr(fixture, "date", "") or "").strip()
    raw_end_date = str(getattr(fixture, "end_date", "") or "").strip()
    raw_time = str(getattr(fixture, "time", "") or "").strip()
    if raw_date:
        if event_type == "выставка" and raw_end_date and raw_end_date != raw_date:
            date_line = f"🗓 {_format_day_month(raw_date)} — {_format_day_month(raw_end_date)}"
        else:
            date_line = f"🗓 {_format_day_month(raw_date)}"
            if raw_time and raw_time != "00:00":
                date_line += f" в {raw_time}"
        lines.append(date_line)

    location_parts: list[str] = []
    for raw in [
        getattr(fixture, "location_name", None),
        getattr(fixture, "location_address", None),
        getattr(fixture, "city", None),
    ]:
        value = str(raw or "").strip()
        if not value:
            continue
        value_norm = value.casefold()
        if any(value_norm in existing.casefold() for existing in location_parts):
            continue
        location_parts.append(value)
    if location_parts:
        lines.append("📍 " + ", ".join(location_parts))

    if _as_bool(getattr(fixture, "pushkin_card", None)):
        lines.append("✅ Пушкинская карта")

    ticket_link = str(getattr(fixture, "ticket_link", "") or "").strip()
    ticket_status = str(getattr(fixture, "ticket_status", "") or "").strip()
    if ticket_status == "sold_out":
        lines.append("❌ Билеты все проданы")
    elif _as_bool(getattr(fixture, "is_free", None)):
        lines.append("🆓 Бесплатно, по регистрации" if ticket_link else "🆓 Бесплатно")
    elif ticket_link:
        lines.append("🎟 Билеты")
    else:
        price_min = getattr(fixture, "ticket_price_min", None)
        price_max = getattr(fixture, "ticket_price_max", None)
        if price_min is not None or price_max is not None:
            if price_min is not None and price_max is not None and int(price_min) != int(price_max):
                lines.append(f"🎟 Билеты {_format_money(price_min)} — {_format_money(price_max)}")
            else:
                lines.append(f"🎟 Билеты {_format_money(price_min if price_min is not None else price_max)}")
    return lines


def _telegraph_preview_text(fixture: Any, variant: dict[str, Any]) -> str:
    parts: list[str] = []
    infoblock = _summary_infoblock_lines(fixture)
    if infoblock:
        parts.extend(infoblock)
        parts.append("")
    search_digest = str(variant.get("search_digest") or "").strip()
    if search_digest:
        parts.append(search_digest)
        parts.append("")
    description = str(variant.get("description_md") or "").strip()
    if description:
        parts.append(description)
    return "\n".join(parts).strip()


def _candidate_from_fixture(fixture: Any) -> su.EventCandidate:
    source_text = "\n\n".join(
        f"[{source.source_id}] {source.url}\n{source.text}" for source in fixture.sources
    ).strip()
    source = fixture.sources[0]
    return su.EventCandidate(
        source_type=source.source_type,
        source_url=source.url,
        source_text=source_text,
        raw_excerpt=source_text[:1400],
        title=fixture.title,
        date=fixture.date,
        time=fixture.time,
        location_name=fixture.location_name,
        location_address=fixture.location_address,
        city=fixture.city,
        ticket_link=getattr(fixture, "ticket_link", None),
        ticket_price_min=getattr(fixture, "ticket_price_min", None),
        ticket_price_max=getattr(fixture, "ticket_price_max", None),
        ticket_status=getattr(fixture, "ticket_status", None),
        is_free=getattr(fixture, "is_free", None),
        pushkin_card=getattr(fixture, "pushkin_card", None),
        event_type=fixture.event_type,
    )


def _source_evidence(fixture: Any) -> dict[str, Any]:
    sources = []
    full_text = []
    for source in fixture.sources:
        full_text.append(source.text)
        sources.append(
            {
                "source_id": source.source_id,
                "source_type": source.source_type,
                "url": source.url,
                "chars": len(source.text or ""),
                "sha256_16": _text_hash(source.text or ""),
            }
        )
    joined = "\n\n".join(full_text)
    return {"sources": sources, "total_chars": len(joined), "sha256_16": _text_hash(joined)}


def _normalize_bundle_facts(bundle: dict[str, Any] | None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    if not isinstance(bundle, dict):
        return out
    for raw in list(bundle.get("facts") or []):
        cleaned = su._normalize_fact_item(str(raw or ""), limit=180)
        if not cleaned:
            continue
        key = cleaned.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(cleaned)
        if len(out) >= 24:
            break
    return out


def _bundle_derived_fields(bundle: dict[str, Any] | None) -> tuple[str | None, str | None]:
    if not isinstance(bundle, dict):
        return None, None
    search_digest = su._clean_search_digest(bundle.get("search_digest"))
    short_description = su._clean_short_description(bundle.get("short_description"))
    if short_description and not su._is_short_description_acceptable(
        short_description,
        min_words=12,
        max_words=16,
    ):
        short_description = None
    return search_digest, short_description


def _field_snapshot(candidate: su.EventCandidate, bundle: dict[str, Any] | None) -> dict[str, Any]:
    bundle = bundle if isinstance(bundle, dict) else {}
    return {
        "title_input": candidate.title,
        "title_bundle": (bundle.get("title") if isinstance(bundle.get("title"), str) else None),
        "event_type": candidate.event_type,
        "date": candidate.date,
        "time": candidate.time,
        "end_date": candidate.end_date,
        "location_name": candidate.location_name,
        "location_address": candidate.location_address,
        "city": candidate.city,
        "ticket_status": candidate.ticket_status,
        "search_digest_bundle": su._clean_search_digest(bundle.get("search_digest")),
        "short_description_bundle": su._clean_short_description(bundle.get("short_description")),
    }


def _variant_metrics(text: str | None) -> dict[str, Any]:
    text = str(text or "")
    return {
        "chars": len(text.strip()),
        "paragraphs": len([p for p in re.split(r"\n\s*\n", text.strip()) if p.strip()]),
        "semantic_headings": len(
            [
                h
                for h in re.findall(r"(?m)^###\s+(.+?)\s*$", text)
                if h.strip().casefold() not in {"когда и где"}
            ]
        ),
        "logistics_headings": len(
            [h for h in re.findall(r"(?m)^###\s+(.+?)\s*$", text) if h.strip().casefold() == "когда и где"]
        ),
        "bullets": len(re.findall(r"(?m)^\s*[-*]\s+\S", text)),
        "epigraph": bool(re.search(r"(?m)^>\s+\S", text)),
    }


def _flatten_per_source_facts(per_source_facts: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    if not isinstance(per_source_facts, dict):
        return out
    for source_facts in per_source_facts.values():
        for raw in list(source_facts or []):
            text = re.sub(r"\s+", " ", str(raw or "")).strip()
            if not text:
                continue
            key = text.casefold()
            if key in seen:
                continue
            seen.add(key)
            out.append(text)
    return out


def _fixture_id(fixture: Any) -> str:
    if isinstance(fixture, dict):
        return str(fixture.get("fixture_id") or "").strip()
    return str(getattr(fixture, "fixture_id", "") or "").strip()


def _find_result_for_fixture(artifact: dict[str, Any], fixture: Any) -> dict[str, Any]:
    target = _fixture_id(fixture)
    if isinstance(artifact.get("baseline"), dict) and _fixture_id(artifact.get("fixture") or {}) == target:
        return artifact
    for result in list(artifact.get("results") or []):
        if _fixture_id(result.get("fixture") or {}) == target:
            return result
    available = [
        _fixture_id(result.get("fixture") or {})
        for result in list(artifact.get("results") or [])
        if _fixture_id(result.get("fixture") or {})
    ]
    raise RuntimeError(
        f"Baseline artifact does not contain fixture {target!r}; available fixtures: {available}"
    )


def _baseline_from_artifact_result(result: dict[str, Any], fixture: Any) -> dict[str, Any]:
    baseline = result.get("baseline") if isinstance(result, dict) else None
    if not isinstance(baseline, dict):
        raise RuntimeError("Baseline artifact result does not contain a baseline object")

    model = str(baseline.get("model") or baseline.get("gemma_model") or DEFAULT_BASELINE_MODEL)
    candidate = _candidate_from_fixture(fixture)
    raw_facts = _flatten_per_source_facts(baseline.get("per_source_facts"))
    if not raw_facts:
        raw_facts = [str(item).strip() for item in list(baseline.get("raw_facts") or []) if str(item).strip()]
    facts_text_clean = [
        str(item).strip()
        for item in list(baseline.get("facts_text_clean") or [])
        if str(item).strip()
    ]
    description = str(baseline.get("description_md") or "")
    timings = baseline.get("timings") if isinstance(baseline.get("timings"), dict) else {}
    wall = timings.get("wall_clock_sec")
    return {
        "model": model,
        "path": "frozen_current_smart_update_baseline",
        "baseline_artifact_mode": str(baseline.get("baseline_mode") or "reused"),
        "candidate_has_g3": "gemma-3" in model,
        "fields": _field_snapshot(candidate, baseline.get("create_bundle") if isinstance(baseline.get("create_bundle"), dict) else None),
        "create_bundle": baseline.get("create_bundle") or {},
        "per_source_facts": baseline.get("per_source_facts") or {},
        "raw_facts": raw_facts,
        "facts_text_clean": facts_text_clean,
        "description_md": description,
        "short_description": baseline.get("short_description") or "",
        "search_digest": baseline.get("search_digest") or "",
        "metrics": _variant_metrics(description),
        "quality_profile": baseline.get("quality_profile") or writer_final_family._describe_text_quality(description),
        "timings": {
            "wall_clock_sec": wall,
            "stage_sec": timings.get("stage_sec") or {},
            "source_artifact_timings": timings,
            "gemma_calls_observed": timings.get("gemma_calls")
            if timings.get("gemma_calls") is not None
            else timings.get("gemma_calls_observed"),
            "four_o_calls_observed": timings.get("four_o_calls")
            if timings.get("four_o_calls") is not None
            else timings.get("four_o_calls_observed"),
        },
    }


def _load_frozen_baseline(path_value: str, fixture: Any) -> dict[str, Any]:
    path = Path(path_value)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    artifact = json.loads(path.read_text(encoding="utf-8"))
    result = _find_result_for_fixture(artifact, fixture)
    baseline = _baseline_from_artifact_result(result, fixture)
    baseline["source_artifact"] = str(path)
    return baseline


def _clean_db_facts(raw_facts: list[str], fixture: Any) -> list[str]:
    return su._facts_text_clean_from_facts(
        raw_facts,
        anchors=[
            getattr(fixture, "date", None) or "",
            getattr(fixture, "time", None) or "",
            getattr(fixture, "city", None) or "",
            getattr(fixture, "location_name", None) or "",
            getattr(fixture, "location_address", None) or "",
        ],
    )


def _baseline_from_fixture_snapshot(fixture: Any) -> dict[str, Any]:
    description = str(getattr(fixture, "baseline_description_md", None) or "").strip()
    raw_facts = [
        re.sub(r"\s+", " ", str(item or "")).strip()
        for item in list(getattr(fixture, "baseline_raw_facts", None) or [])
        if re.sub(r"\s+", " ", str(item or "")).strip()
    ]
    facts_text_clean = _clean_db_facts(raw_facts, fixture) if raw_facts else []
    candidate = _candidate_from_fixture(fixture)
    return {
        "model": "prod_db_gemma3_snapshot",
        "path": "prod_db_snapshot_current_smart_update_text",
        "baseline_artifact_mode": "prod_db_snapshot",
        "candidate_has_g3": True,
        "fields": _field_snapshot(
            candidate,
            {
                "title": getattr(fixture, "title", None),
                "search_digest": getattr(fixture, "baseline_search_digest", None),
                "short_description": getattr(fixture, "baseline_short_description", None),
            },
        ),
        "create_bundle": {},
        "per_source_facts": {"prod_db_event_source_fact": raw_facts},
        "raw_facts": raw_facts,
        "facts_text_clean": facts_text_clean,
        "baseline_fact_source": "prod_db_event_source_fact",
        "description_md": description,
        "short_description": str(getattr(fixture, "baseline_short_description", None) or "").strip(),
        "search_digest": str(getattr(fixture, "baseline_search_digest", None) or "").strip(),
        "metrics": _variant_metrics(description),
        "quality_profile": writer_final_family._describe_text_quality(description),
        "summary_infoblock": _summary_infoblock_lines(fixture),
        "telegraph_preview_text": "",
        "timings": {
            "wall_clock_sec": None,
            "stage_sec": {},
            "source_artifact_timings": {},
            "gemma_calls_observed": None,
            "four_o_calls_observed": None,
        },
        "source_artifact": str(getattr(fixture, "baseline_source_artifact", None) or ""),
    }


def _source_url_from_event_row(row: sqlite3.Row) -> str:
    for key in ("source_post_url", "source_vk_post_url", "telegraph_url"):
        value = str(row[key] or "").strip() if key in row.keys() else ""
        if value:
            return value
    path = str(row["telegraph_path"] or "").strip() if "telegraph_path" in row.keys() else ""
    return f"https://telegra.ph/{path.lstrip('/')}" if path else ""


def _fixtures_from_prod_db(path_value: str, event_ids: str) -> list[StageBenchmarkFixture]:
    path = Path(path_value)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    ids = [int(item.strip()) for item in (event_ids or DEFAULT_PROD_DB_EVENT_IDS).split(",") if item.strip()]
    if not ids:
        raise RuntimeError("No event ids provided for prod DB benchmark")
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    fixtures: list[StageBenchmarkFixture] = []
    for event_id in ids:
        event = con.execute("SELECT * FROM event WHERE id = ?", (event_id,)).fetchone()
        if event is None:
            raise RuntimeError(f"Event {event_id} not found in {path}")
        source_rows = con.execute(
            """
            SELECT id, source_type, source_url, source_text
            FROM event_source
            WHERE event_id = ?
            ORDER BY id
            """,
            (event_id,),
        ).fetchall()
        sources: list[Any] = []
        for idx, row in enumerate(source_rows, start=1):
            text = re.sub(r"\s+", " ", str(row["source_text"] or "")).strip()
            if not text:
                continue
            sources.append(
                bench.SourcePacket(
                    source_id=f"{row['source_type'] or 'source'}-{row['id'] or idx}",
                    source_type=str(row["source_type"] or "source"),
                    url=str(row["source_url"] or "").strip(),
                    text=text,
                )
            )
        if not sources:
            fallback_text = re.sub(r"\s+", " ", str(event["source_text"] or "")).strip()
            if fallback_text:
                sources.append(
                    bench.SourcePacket(
                        source_id="event-source-text",
                        source_type="source",
                        url=_source_url_from_event_row(event),
                        text=fallback_text,
                    )
                )
        if not sources:
            raise RuntimeError(f"Event {event_id} has no usable source text in {path}")
        fact_rows = con.execute(
            """
            SELECT status, fact
            FROM event_source_fact
            WHERE event_id = ?
            ORDER BY id
            """,
            (event_id,),
        ).fetchall()
        raw_facts: list[str] = []
        seen_facts: set[str] = set()
        for row in fact_rows:
            status = str(row["status"] or "").strip().casefold()
            if status in {"conflict", "drop", "skipped"}:
                continue
            fact = re.sub(r"\s+", " ", str(row["fact"] or "")).strip()
            if not fact:
                continue
            key = fact.casefold()
            if key in seen_facts:
                continue
            seen_facts.add(key)
            raw_facts.append(fact)
        fixture = StageBenchmarkFixture(
            fixture_id=f"PRODDB-{event_id}",
            title=str(event["title"] or "").strip(),
            event_type=str(event["event_type"] or "").strip(),
            date=str(event["date"] or "").strip() or None,
            time=str(event["time"] or "").strip() or None,
            end_date=str(event["end_date"] or "").strip() or None,
            end_date_is_inferred=bool(event["end_date_is_inferred"] or False),
            location_name=str(event["location_name"] or "").strip() or None,
            location_address=str(event["location_address"] or "").strip() or None,
            city=str(event["city"] or "").strip() or None,
            ticket_price_min=event["ticket_price_min"],
            ticket_price_max=event["ticket_price_max"],
            ticket_link=str(event["ticket_link"] or "").strip() or None,
            ticket_status=str(event["ticket_status"] or "").strip() or None,
            is_free=_as_bool(event["is_free"]),
            pushkin_card=_as_bool(event["pushkin_card"]),
            lifecycle_status=str(event["lifecycle_status"] or "").strip() or None,
            telegraph_url=str(event["telegraph_url"] or "").strip() or None,
            source_post_url=str(event["source_post_url"] or "").strip() or None,
            sources=sources,
            baseline_description_md=str(event["description"] or "").strip(),
            baseline_short_description=str(event["short_description"] or "").strip(),
            baseline_search_digest=str(event["search_digest"] or "").strip(),
            baseline_raw_facts=raw_facts,
            baseline_facts_text_clean=[],
            baseline_source_artifact=str(path),
        )
        fixture.baseline_facts_text_clean = _clean_db_facts(raw_facts, fixture)
        fixtures.append(fixture)
    return fixtures


def _fact_item_list(facts: list[str], *, category: str = "public") -> list[dict[str, Any]]:
    return [
        {"index": idx, "text": fact, "kind": category, "source_span": ""}
        for idx, fact in enumerate(facts)
        if str(fact or "").strip()
    ]


_LOGISTICS_FACT_RE = re.compile(
    r"(?iu)("
    r"дат[ауы]?|время|начало|окончани[ея]|"
    r"локаци[яи]|адрес|место|зал|"
    r"бесплатн\w*|регистраци\w*|зарегистрир\w*|"
    r"билет\w*|стоимост\w*|цен[ауы]?|₽|рубл\w*|"
    r"пушкинск\w*\s+карт\w*|"
    r"возрастн\w*\s+ограничени\w*|\b\d{1,2}\+|"
    r"ежедневно|работает|час[ао]в|"
    r"https?://"
    r")"
)


def _looks_like_logistics_fact(text: str) -> bool:
    return bool(_LOGISTICS_FACT_RE.search(str(text or "")))


def _unique_fact_texts(values: list[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = re.sub(r"\s+", " ", str(raw or "")).strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _candidate_logistics_facts(fixture: Any, candidate: dict[str, Any]) -> list[str]:
    facts: list[str] = []
    for raw in list(candidate.get("raw_facts") or []):
        text = re.sub(r"\s+", " ", str(raw or "")).strip()
        if text and _looks_like_logistics_fact(text):
            facts.append(text)
    for value in [
        getattr(fixture, "date", None),
        getattr(fixture, "time", None),
        getattr(fixture, "location_name", None),
        getattr(fixture, "location_address", None),
        getattr(fixture, "city", None),
    ]:
        if value:
            facts.append(str(value))
    ticket_link = str(getattr(fixture, "ticket_link", "") or "").strip()
    if _as_bool(getattr(fixture, "is_free", None)):
        facts.append("Бесплатно")
        if ticket_link:
            facts.append(f"Регистрация: {ticket_link}")
    elif ticket_link:
        facts.append(f"Билеты: {ticket_link}")
    price_min = getattr(fixture, "ticket_price_min", None)
    price_max = getattr(fixture, "ticket_price_max", None)
    if price_min is not None or price_max is not None:
        if price_min is not None and price_max is not None and int(price_min) != int(price_max):
            facts.append(f"Цена: {_format_money(price_min)} — {_format_money(price_max)}")
        else:
            facts.append(f"Цена: {_format_money(price_min if price_min is not None else price_max)}")
    if _as_bool(getattr(fixture, "pushkin_card", None)):
        facts.append("Пушкинская карта")
    if getattr(fixture, "ticket_status", None):
        facts.append(f"ticket_status: {getattr(fixture, 'ticket_status')}")
    return _unique_fact_texts(facts)


def _fact_coverage_error_summary(
    *,
    errors: list[str],
    baseline_count: int,
    g4_count: int,
    public_fact_count: int,
    logistics_fact_count: int,
    candidate_logistics_texts: list[str],
    baseline_surfaces: dict[str, Any],
) -> dict[str, Any]:
    return {
        "baseline_fact_count": baseline_count,
        "grounded_baseline_fact_count": baseline_count,
        "covered_grounded_baseline_fact_count": None,
        "g4_fact_count": g4_count,
        "grounded_g4_fact_count": None,
        "lost_baseline_facts": [],
        "added_g4_facts": [],
        "suspicious_g4_facts": [],
        "llm_overall_verdict": "review_error",
        "deterministic_verdict_floor": "review_error",
        "verdict": "review_error",
        "review_errors": errors,
        "coverage_summary": {
            "overall_verdict": "review_error",
            "verdict_reason": "; ".join(errors)[:300],
        },
        "baseline_raw_extracted_fact_count": len(baseline_surfaces["raw_extracted_facts"]),
        "baseline_writer_fact_count": len(baseline_surfaces["writer_facts_text_clean"]),
        "baseline_filtered_out_fact_count": len(baseline_surfaces["filtered_out_before_writer"]),
        "baseline_metadata_fact_count": len(baseline_surfaces["metadata_anchors"]),
        "g4_public_fact_count": public_fact_count,
        "g4_logistics_fact_count": logistics_fact_count,
        "g4_logistics_fact_texts": candidate_logistics_texts,
    }


def _bucket_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "assignments": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "fact_index": {"type": "INTEGER"},
                        "bucket": {
                            "type": "STRING",
                            "format": "enum",
                            "enum": [
                                "event_core",
                                "program_list",
                                "people_and_roles",
                                "forward_looking",
                                "support_context",
                                "uncertain",
                            ],
                        },
                        "literal_items": {"type": "ARRAY", "items": {"type": "STRING"}},
                    },
                    "required": ["fact_index", "bucket", "literal_items"],
                },
            }
        },
        "required": ["assignments"],
    }


def _bucket_system_prompt() -> str:
    return (
        "You do one small step for Smart Update G4 variant 2: smart_update.facts_to_lollipop_buckets.v1.\n"
        "Return only JSON. Do not write prose. Do not rewrite fact text.\n"
        "Assign every input fact_index exactly once to one lollipop-light bucket.\n"
        "Use literal_items only when the original fact contains an explicit list/program/repertoire/object list."
    )


def _writer_response_schema_gemma() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "title": {"type": "STRING"},
            "description_md": {"type": "STRING"},
        },
        "required": ["title", "description_md"],
    }


def _compact_gemma_writer_system_prompt() -> str:
    return (
        "You are smart_update.g4_lollipop_light.final_writer.v2. Return only JSON.\n"
        "Write polished Russian event copy from the provided writer_pack only.\n"
        "Cover every must_cover_fact_id exactly once in natural prose. Do not add unsupported facts.\n"
        "Keep logistics out of narrative. Use exact ### headings from each section. "
        "If a section has literal_items, render every item on its own line as `- item` "
        "with a hyphen and a space. Do not use `* item` bullets.\n"
        "Tone: cultural city digest, lively but restrained. No direct address, CTA, promo promises, or report formulas.\n"
        "Never output report words like `характеризуется`, `осуществляется`, `представляет собой`, "
        "even if a fact uses them. Rewrite them naturally without changing meaning.\n"
        "Target length: 700-1100 characters for 8+ facts; 450-800 for smaller packs."
    )


def _compact_gemma_writer_payload(pack: dict[str, Any]) -> dict[str, Any]:
    sections: list[dict[str, Any]] = []
    for section in list(pack.get("sections") or []):
        if not isinstance(section, dict):
            continue
        sections.append(
            {
                "role": section.get("role"),
                "style": section.get("style"),
                "heading": section.get("heading"),
                "fact_ids": section.get("fact_ids") or [],
                "facts": [
                    {
                        "fact_id": fact.get("fact_id"),
                        "text": fact.get("text"),
                    }
                    for fact in list(section.get("facts") or [])
                    if isinstance(fact, dict)
                ],
                "coverage_plan": section.get("coverage_plan") or [],
                "literal_items": section.get("literal_items") or [],
            }
        )
    return {
        "title": ((pack.get("title_context") or {}).get("original_title") or ""),
        "event_type": pack.get("event_type"),
        "must_cover_fact_ids": (pack.get("constraints") or {}).get("must_cover_fact_ids") or [],
        "required_headings": (pack.get("constraints") or {}).get("headings") or [],
        "sections": sections,
        "output_contract": {
            "title": "Keep original title unless writer_pack explicitly asks otherwise.",
            "description_md": "Markdown prose only; no infoblock, no date/time/address/city/tickets.",
        },
    }


def _normalize_bucket_payload(facts: list[str], raw: dict[str, Any], fixture: Any) -> dict[str, Any]:
    bucket_prefix = {
        "event_core": "EC",
        "program_list": "PL",
        "people_and_roles": "PR",
        "forward_looking": "FL",
        "support_context": "SC",
        "uncertain": "UN",
    }
    allowed = set(bucket_prefix)
    assignments: dict[int, tuple[str, list[str]]] = {}
    for item in list((raw or {}).get("assignments") or []):
        if not isinstance(item, dict):
            continue
        try:
            idx = int(item.get("fact_index"))
        except Exception:
            continue
        if idx < 0 or idx >= len(facts) or idx in assignments:
            continue
        bucket = str(item.get("bucket") or "").strip()
        if bucket not in allowed:
            bucket = "support_context"
        literal_items = [
            re.sub(r"\s+", " ", str(raw_item or "")).strip()
            for raw_item in list(item.get("literal_items") or [])
            if re.sub(r"\s+", " ", str(raw_item or "")).strip()
        ][:12]
        assignments[idx] = (bucket, literal_items)
    for idx in range(len(facts)):
        assignments.setdefault(idx, ("support_context", []))

    pack: dict[str, Any] = {bucket: [] for bucket in allowed}
    counters = {bucket: 0 for bucket in allowed}
    for idx, fact in enumerate(facts):
        bucket, literal_items = assignments[idx]
        counters[bucket] += 1
        pack[bucket].append(
            {
                "fact_id": f"{bucket_prefix[bucket]}{counters[bucket]:02d}",
                "bucket": bucket,
                "text": fact,
                "literal_items": literal_items,
                "record_ids": [f"SU{idx:02d}"],
                "source_refs": ["smart_update.facts_text_clean"],
            }
        )
    logistics: list[dict[str, Any]] = []
    for value, label in [
        (fixture.date, "date"),
        (fixture.time, "time"),
        (fixture.location_name, "location"),
        (fixture.location_address, "address"),
        (fixture.city, "city"),
    ]:
        if value:
            logistics.append(
                {
                    "fact_id": f"LG{len(logistics) + 1:02d}",
                    "bucket": "logistics_infoblock",
                    "text": str(value),
                    "literal_items": [],
                    "record_ids": [label],
                    "source_refs": ["fixture.metadata"],
                }
            )
    pack["logistics_infoblock"] = logistics
    return pack


async def _time_stage(timings: dict[str, float], stage: str, coro: Any) -> Any:
    started = time.perf_counter()
    try:
        return await coro
    finally:
        timings[stage] = round(time.perf_counter() - started, 6)


async def _ask_gemma_json_gateway(
    *,
    model: str,
    system_prompt: str,
    user_payload: dict[str, Any],
    max_tokens: int,
    response_schema: dict[str, Any] | None = None,
    timeout_sec: float | None = None,
    allow_json_repair: bool = False,
) -> dict[str, Any]:
    client = su._get_gemma_client()
    if client is None:
        raise RuntimeError("GoogleAIClient is unavailable")
    old_timeout = getattr(client, "provider_timeout_seconds", None)
    if timeout_sec:
        client.provider_timeout_seconds = float(timeout_sec)
    generation_config: dict[str, Any] = {
        "temperature": 0,
        "max_output_tokens": max_tokens,
        "response_mime_type": "application/json",
        "system_instruction": system_prompt.strip(),
    }
    if response_schema is not None:
        generation_config["response_schema"] = response_schema
    try:
        raw, _usage = await client.generate_content_async(
            model=model,
            prompt=json.dumps(user_payload, ensure_ascii=False, indent=2),
            generation_config=generation_config,
            max_output_tokens=max_tokens,
        )
    finally:
        if timeout_sec and old_timeout is not None:
            client.provider_timeout_seconds = old_timeout
    data = su._extract_json(raw)
    if data is None:
        raise RuntimeError(f"Invalid JSON from {model}: {raw[:1200]}")
    return data


async def _run_current_smart_update_baseline(fixture: Any, *, model: str) -> dict[str, Any]:
    _set_smart_update_model(model)
    candidate = _candidate_from_fixture(fixture)
    timings: dict[str, float] = {}
    started = time.perf_counter()
    bundle: dict[str, Any] | None = await _time_stage(
        timings,
        "create_bundle",
        su._llm_create_description_facts_and_digest(
            candidate,
            clean_title=fixture.title,
            clean_source_text=candidate.source_text or "",
            clean_raw_excerpt=candidate.raw_excerpt,
            normalized_event_type=fixture.event_type,
        ),
    )
    raw_facts = _normalize_bundle_facts(bundle)
    if not raw_facts:
        raw_facts = await _time_stage(
            timings,
            "facts_extract_fallback",
            su._llm_extract_candidate_facts(candidate, text_for_facts=candidate.source_text),
        )
    facts_text_clean = su._facts_text_clean_from_facts(
        raw_facts,
        anchors=[
            fixture.date or "",
            fixture.time or "",
            fixture.city or "",
            fixture.location_name or "",
            fixture.location_address or "",
        ],
    )
    timings["facts_text_clean"] = 0.0
    description = ""
    if facts_text_clean:
        description = (
            await _time_stage(
                timings,
                "fact_first_description",
                su._llm_fact_first_description_md(
                    title=fixture.title,
                    event_type=fixture.event_type,
                    facts_text_clean=facts_text_clean,
                    anchors=[
                        fixture.date or "",
                        fixture.time or "",
                        fixture.city or "",
                        fixture.location_name or "",
                        fixture.location_address or "",
                    ],
                    label="smart_update_stage_benchmark_baseline",
                ),
            )
            or ""
        )
    if not description and isinstance(bundle, dict):
        description = str(bundle.get("description") or "").strip()
    search_digest = su._clean_search_digest((bundle or {}).get("search_digest"))
    if not search_digest:
        search_digest = await _time_stage(
            timings,
            "search_digest",
            su._llm_build_search_digest(title=fixture.title, description=description, event_type=fixture.event_type),
        )
    short_description = su._clean_short_description((bundle or {}).get("short_description"))
    if not short_description:
        short_description = await _time_stage(
            timings,
            "short_description",
            su._llm_build_short_description(title=fixture.title, description=description, event_type=fixture.event_type),
        )
    wall = round(time.perf_counter() - started, 6)
    return {
        "model": model,
        "path": "current_smart_update_create_path",
        "candidate_has_g3": "gemma-3" in model,
        "fields": _field_snapshot(candidate, bundle),
        "create_bundle": bundle or {},
        "raw_facts": raw_facts,
        "facts_text_clean": facts_text_clean,
        "description_md": description,
        "short_description": short_description,
        "search_digest": search_digest,
        "metrics": _variant_metrics(description),
        "quality_profile": writer_final_family._describe_text_quality(description),
        "timings": {
            "wall_clock_sec": wall,
            "stage_sec": timings,
            "gemma_calls_observed": len([k for k in timings if k not in {"facts_text_clean"}]),
            "four_o_calls_observed": 0,
        },
    }


async def _run_candidate_g4_lollipop2(
    fixture: Any,
    *,
    model: str,
    four_o_model: str,
) -> dict[str, Any]:
    _set_smart_update_model(model)
    candidate = _candidate_from_fixture(fixture)
    timings: dict[str, float] = {}
    stage_errors: list[str] = []
    started = time.perf_counter()
    bundle: dict[str, Any] | None = await _time_stage(
        timings,
        "create_bundle_g4",
        su._llm_create_description_facts_and_digest(
            candidate,
            clean_title=fixture.title,
            clean_source_text=candidate.source_text or "",
            clean_raw_excerpt=candidate.raw_excerpt,
            normalized_event_type=fixture.event_type,
        ),
    )
    raw_facts = _normalize_bundle_facts(bundle)
    if not raw_facts:
        raw_facts = await _time_stage(
            timings,
            "facts_extract_g4_fallback",
            su._llm_extract_candidate_facts(candidate, text_for_facts=candidate.source_text),
        )
    facts_text_clean = su._facts_text_clean_from_facts(
        raw_facts,
        anchors=[
            fixture.date or "",
            fixture.time or "",
            fixture.city or "",
            fixture.location_name or "",
            fixture.location_address or "",
        ],
    )
    bucket_raw = await _time_stage(
        timings,
        "lollipop.bucket_facts",
        _ask_gemma_json_gateway(
            model=model,
            system_prompt=_bucket_system_prompt(),
            user_payload={
                "title": fixture.title,
                "event_type": fixture.event_type,
                "facts_text_clean": [{"index": idx, "text": text} for idx, text in enumerate(facts_text_clean)],
            },
            max_tokens=900,
            response_schema=_bucket_schema(),
            timeout_sec=min(bench._gemma_direct_timeout_sec(), 70.0),
            allow_json_repair=False,
        ),
    )
    fact_pack = _normalize_bucket_payload(facts_text_clean, bucket_raw or {}, fixture)
    flat_weight_facts = [
        {"fact_id": item["fact_id"], "bucket": item["bucket"], "text": item["text"], "literal_items": item.get("literal_items") or []}
        for item in prioritize_family._flat_facts(fact_pack)
    ]
    weight_raw = await _time_stage(
        timings,
        "lollipop.prioritize.weight",
        _ask_gemma_json_gateway(
            model=model,
            system_prompt=cascade_family._prioritize_weight_system_prompt(gemma4=True),
            user_payload={"event_title": fixture.title, "event_type": fixture.event_type, "facts": flat_weight_facts},
            max_tokens=900,
            response_schema=cascade_family._prioritize_weight_response_schema(),
            timeout_sec=min(bench._gemma_direct_timeout_sec(), 70.0),
            allow_json_repair=False,
        ),
    )
    weighted_pack = cascade_family._apply_weight_payload(fact_pack, weight_raw or {})
    weighted_pack = prioritize_family._apply_narrative_policies(weighted_pack, event_type=fixture.event_type)
    flat_lead_facts = [
        {"fact_id": item["fact_id"], "bucket": item["bucket"], "text": item["text"], "weight": item.get("weight")}
        for item in prioritize_family._flat_facts(weighted_pack)
        if item.get("narrative_policy") != "suppress"
    ]
    lead_raw = await _time_stage(
        timings,
        "lollipop.prioritize.lead",
        _ask_gemma_json_gateway(
            model=model,
            system_prompt=cascade_family._prioritize_lead_system_prompt(gemma4=True),
            user_payload={"event_id": 0, "event_title": fixture.title, "event_type": fixture.event_type, "facts": flat_lead_facts},
            max_tokens=500,
            response_schema=cascade_family._prioritize_lead_response_schema(),
            timeout_sec=min(bench._gemma_direct_timeout_sec(), 70.0),
            allow_json_repair=False,
        ),
    )
    lead_payload = prioritize_family._clean_lead(lead_raw or {}, weighted_pack, title=fixture.title, event_type=fixture.event_type)
    lead_payload["event_title"] = fixture.title
    prioritized_pack = layout_family._prioritized_fact_pack(weighted_pack)
    precompute = layout_family._precompute_layout_state(
        event_type=fixture.event_type,
        pack=prioritized_pack,
        lead_payload=lead_payload,
    )
    layout_raw = await _time_stage(
        timings,
        "lollipop.editorial.layout",
        _ask_gemma_json_gateway(
            model=model,
            system_prompt=cascade_family._editorial_layout_system_prompt(gemma4=True),
            user_payload={
                "event_title": fixture.title,
                "event_type": fixture.event_type,
                "lead_payload": lead_payload,
                "precompute": precompute,
                "fact_pack": prioritized_pack,
            },
            max_tokens=1200,
            response_schema=cascade_family._editorial_layout_response_schema(),
            timeout_sec=min(bench._gemma_direct_timeout_sec(), 70.0),
            allow_json_repair=False,
        ),
    )
    layout_payload = layout_family._clean_layout_plan(
        layout_raw or {},
        title=fixture.title,
        pack=prioritized_pack,
        lead_payload=lead_payload,
        precompute=precompute,
    )
    layout_audit = layout_family._audit_layout(
        plan_payload=layout_payload,
        pack=prioritized_pack,
        precompute=precompute,
        lead_payload=lead_payload,
        title=fixture.title,
    )
    writer_pack = writer_pack_family._compose_writer_pack(
        event_id=0,
        title=fixture.title,
        layout_result={"event_type": fixture.event_type, "layout_result": {"precompute": precompute, "payload": layout_payload}},
        prioritize_result={"weight_result": {"payload": weighted_pack}},
    )
    writer_output: dict[str, Any] = {}
    writer_validation = writer_final_family.ValidationResult(errors=["writer.final_g4.not_run"], warnings=[])
    writer_model = model
    try:
        writer_output = await _time_stage(
            timings,
            "writer.final_g4_primary",
            _ask_gemma_json_gateway(
                model=model,
                system_prompt=_compact_gemma_writer_system_prompt(),
                user_payload=_compact_gemma_writer_payload(writer_pack["payload"]),
                max_tokens=1200,
                response_schema=_writer_response_schema_gemma(),
                timeout_sec=min(bench._gemma_direct_timeout_sec(), 70.0),
                allow_json_repair=False,
            ),
        )
        writer_validation = writer_final_family._validate_writer_output(writer_pack["payload"], writer_output)
    except Exception as exc:
        stage_errors.append(f"writer.final_g4.error:{type(exc).__name__}:{str(exc)[:240]}")
        writer_output = {"title": fixture.title, "description_md": ""}
        writer_validation = writer_final_family.ValidationResult(
            errors=[f"writer.final_g4.error:{type(exc).__name__}"],
            warnings=[],
        )
    # Previous 4o-primary lane kept for quick rollback/comparison when quota is available:
    #
    # writer_model = four_o_model
    # try:
    #     writer_output = await _time_stage(
    #         timings,
    #         "writer.final_4o",
    #         bench._ask_4o_json(
    #             prompt=writer_final_family._build_prompt(writer_pack["payload"]),
    #             schema={
    #                 "type": "object",
    #                 "properties": {"title": {"type": "string"}, "description_md": {"type": "string"}},
    #                 "required": ["title", "description_md"],
    #                 "additionalProperties": False,
    #             },
    #             model=four_o_model,
    #         ),
    #     )
    #     writer_validation = writer_final_family._validate_writer_output(writer_pack["payload"], writer_output)
    # except Exception as exc:
    #     stage_errors.append(f"writer.final_4o.error:{type(exc).__name__}:{str(exc)[:240]}")
    #     writer_model = f"{four_o_model}->gemma-4-compact"
    #     writer_output = await _time_stage(
    #         timings,
    #         "writer.final_g4_compact_after_4o_error",
    #         _ask_gemma_json_gateway(
    #             model=model,
    #             system_prompt=_compact_gemma_writer_system_prompt(),
    #             user_payload=_compact_gemma_writer_payload(writer_pack["payload"]),
    #             max_tokens=1200,
    #             response_schema=_writer_response_schema_gemma(),
    #             timeout_sec=min(bench._gemma_direct_timeout_sec(), 70.0),
    #             allow_json_repair=False,
    #         ),
    #     )
    #     writer_validation = writer_final_family._validate_writer_output(writer_pack["payload"], writer_output)
    applied = writer_final_family._apply_writer_output(writer_pack["payload"], writer_output)
    description = str(applied.get("description_md") or "")
    search_digest, short_description = _bundle_derived_fields(bundle)
    if not search_digest:
        search_digest = await _time_stage(
            timings,
            "search_digest_g4",
            su._llm_build_search_digest(title=applied.get("title") or fixture.title, description=description, event_type=fixture.event_type),
        )
    if not search_digest:
        search_digest = su._fallback_digest_from_description(description)
    if not short_description:
        short_description = await _time_stage(
            timings,
            "short_description_g4",
            su._llm_build_short_description(title=applied.get("title") or fixture.title, description=description, event_type=fixture.event_type),
        )
    if not short_description:
        short_description = su._fallback_short_description_from_text(description)
    wall = round(time.perf_counter() - started, 6)
    return {
        "model": model,
        "path": "smart_update_g4_variant2_lollipop_light_create_path",
        "candidate_has_g3": False,
        "writer_model": writer_model,
        "stage_errors": stage_errors,
        "fields": _field_snapshot(candidate, bundle),
        "create_bundle": bundle or {},
        "raw_facts": raw_facts,
        "facts_text_clean": facts_text_clean,
        "fact_pack": fact_pack,
        "weight_raw": weight_raw or {},
        "weighted_pack": weighted_pack,
        "lead_payload": lead_payload,
        "layout_raw": layout_raw or {},
        "layout_payload": layout_payload,
        "layout_audit": layout_audit,
        "writer_pack": writer_pack,
        "writer_output": writer_output,
        "writer_validation": {"errors": writer_validation.errors, "warnings": writer_validation.warnings},
        "applied_output": applied,
        "description_md": description,
        "short_description": short_description,
        "search_digest": search_digest,
        "metrics": _variant_metrics(description),
        "quality_profile": writer_final_family._describe_text_quality(description),
        "summary_infoblock": _summary_infoblock_lines(fixture),
        "telegraph_preview_text": "",
        "timings": {
            "wall_clock_sec": wall,
            "stage_sec": timings,
            "gemma_calls_observed": len([k for k in timings if k.startswith(("create", "facts", "lollipop", "writer.final_g4", "search", "short"))]),
            "four_o_calls_observed": 0,
        },
    }


async def _run_fact_coverage(fixture: Any, baseline: dict[str, Any], candidate: dict[str, Any], *, model: str) -> dict[str, Any]:
    source_excerpt = bench._source_excerpt(fixture.sources, limit=9000)
    baseline_for_review = {
        "per_source_facts": {"smart_update": baseline.get("raw_facts") or []},
        "facts_text_clean": baseline.get("facts_text_clean") or [],
        "description_md": baseline.get("description_md") or "",
    }
    public_facts = _fact_item_list(list(candidate.get("facts_text_clean") or []), category="public")
    candidate_logistics_texts = _candidate_logistics_facts(fixture, candidate)
    logistics_facts = _fact_item_list(candidate_logistics_texts, category="logistics")
    baseline_surfaces = bench._baseline_fact_surfaces_for_review(baseline_for_review, fixture)
    review_payload_user = legacy_writer_family.build_fact_coverage_payload(
        title=fixture.title,
        event_type=fixture.event_type,
        date=fixture.date,
        time=fixture.time,
        location_name=fixture.location_name,
        location_address=fixture.location_address,
        city=fixture.city,
        source_excerpt=source_excerpt,
        baseline_facts=baseline_surfaces["raw_extracted_facts"],
        baseline_writer_facts=baseline_surfaces["writer_facts_text_clean"],
        baseline_metadata_facts=baseline_surfaces["metadata_anchors"],
        g4_public_facts=public_facts,
        g4_logistics_facts=logistics_facts,
    )
    baseline_count = len(review_payload_user["baseline_facts"])
    g4_count = len(review_payload_user["g4_facts"])
    errors: list[str] = []
    raw: dict[str, Any] = {}
    if baseline_count or g4_count:
        try:
            raw = await _ask_gemma_json_gateway(
                model=model,
                system_prompt=legacy_writer_family.build_fact_coverage_system_prompt(),
                user_payload=review_payload_user,
                max_tokens=2400,
                response_schema=legacy_writer_family.fact_coverage_response_schema(),
                timeout_sec=max(bench._gemma_direct_timeout_sec(), 180.0),
            )
        except Exception as exc:
            errors.append(f"reviewer.error:{type(exc).__name__}:{str(exc)[:240]}")
    else:
        errors.append("no_facts_on_either_side")
    if errors:
        normalized = {
            "baseline_facts_review": [],
            "g4_facts_review": [],
            "coverage_summary": {
                "overall_verdict": "review_error",
                "verdict_reason": "; ".join(errors)[:300],
            },
        }
        summary = _fact_coverage_error_summary(
            errors=errors,
            baseline_count=baseline_count,
            g4_count=g4_count,
            public_fact_count=len(public_facts),
            logistics_fact_count=len(logistics_facts),
            candidate_logistics_texts=candidate_logistics_texts,
            baseline_surfaces=baseline_surfaces,
        )
    else:
        normalized = legacy_writer_family.normalize_fact_coverage_payload(
            raw or {},
            baseline_count=baseline_count,
            g4_count=g4_count,
            baseline_facts=review_payload_user["baseline_facts"],
            g4_facts=review_payload_user["g4_facts"],
        )
        summary = legacy_writer_family.summarize_fact_coverage(normalized)
        summary.update(
            {
                "baseline_raw_extracted_fact_count": len(baseline_surfaces["raw_extracted_facts"]),
                "baseline_writer_fact_count": len(baseline_surfaces["writer_facts_text_clean"]),
                "baseline_filtered_out_fact_count": len(baseline_surfaces["filtered_out_before_writer"]),
                "baseline_metadata_fact_count": len(baseline_surfaces["metadata_anchors"]),
                "g4_public_fact_count": len(public_facts),
                "g4_logistics_fact_count": len(logistics_facts),
                "g4_logistics_fact_texts": candidate_logistics_texts,
            }
        )
    return {
        "input": review_payload_user,
        "baseline_fact_surfaces": baseline_surfaces,
        "review": normalized,
        "summary": summary,
        "errors": errors,
        "gemma_calls": 1 if raw else 0,
        "model": model,
        "verdict": summary.get("verdict") or "unknown",
    }


def _compare_stage_summary(baseline: dict[str, Any], candidate: dict[str, Any], fact_coverage: dict[str, Any]) -> list[dict[str, Any]]:
    b_metrics = baseline.get("metrics") or {}
    c_metrics = candidate.get("metrics") or {}
    coverage_summary = (fact_coverage.get("summary") if isinstance(fact_coverage, dict) else {}) or {}
    return [
        {
            "stage": "source_evidence",
            "baseline": "same fixture evidence",
            "candidate": "same fixture evidence",
            "verdict": "accepted",
        },
        {
            "stage": "create_bundle.fields",
            "baseline": baseline.get("fields"),
            "candidate": candidate.get("fields"),
            "verdict": "manual_review",
        },
        {
            "stage": "facts.raw",
            "baseline": len(baseline.get("raw_facts") or []),
            "candidate": len(candidate.get("raw_facts") or []),
            "verdict": "accepted" if len(candidate.get("raw_facts") or []) >= len(baseline.get("raw_facts") or []) else "review_loss",
        },
        {
            "stage": "facts_text_clean",
            "baseline": len(baseline.get("facts_text_clean") or []),
            "candidate": len(candidate.get("facts_text_clean") or []),
            "verdict": coverage_summary.get("verdict") or "unknown",
        },
        {
            "stage": "lollipop_light.writer_pack",
            "baseline": "not applicable",
            "candidate": {
                "must_cover": len((((candidate.get("writer_pack") or {}).get("payload") or {}).get("constraints") or {}).get("must_cover_fact_ids") or []),
                "headings": ((((candidate.get("writer_pack") or {}).get("payload") or {}).get("constraints") or {}).get("headings") or []),
                "layout_flags": (candidate.get("layout_audit") or {}).get("flags") or [],
            },
            "verdict": "review" if ((candidate.get("layout_audit") or {}).get("flags") or []) else "accepted",
        },
        {
            "stage": "final_description",
            "baseline": b_metrics,
            "candidate": c_metrics,
            "verdict": "accepted"
            if c_metrics.get("chars") and not (candidate.get("writer_validation") or {}).get("errors")
            else "rejected",
        },
        {
            "stage": "derived.short_search",
            "baseline": {"short": bool(baseline.get("short_description")), "search": bool(baseline.get("search_digest"))},
            "candidate": {"short": bool(candidate.get("short_description")), "search": bool(candidate.get("search_digest"))},
            "verdict": "accepted" if candidate.get("short_description") and candidate.get("search_digest") else "partial",
        },
        {
            "stage": "latency",
            "baseline": (baseline.get("timings") or {}).get("wall_clock_sec"),
            "candidate": (candidate.get("timings") or {}).get("wall_clock_sec"),
            "verdict": "warning"
            if (baseline.get("timings") or {}).get("wall_clock_sec")
            and (candidate.get("timings") or {}).get("wall_clock_sec")
            and float((candidate.get("timings") or {}).get("wall_clock_sec")) > 3 * float((baseline.get("timings") or {}).get("wall_clock_sec"))
            else "accepted",
        },
    ]


def _fenced(text: str | None, info: str = "md") -> list[str]:
    body = str(text or "").strip() or "_empty_"
    return [f"```{info}", body, "```"]


def _coverage_cell(summary: dict[str, Any]) -> str:
    total = summary.get("grounded_baseline_fact_count")
    if summary.get("verdict") == "review_error":
        return f"review_error/{total}"
    return f"{summary.get('covered_grounded_baseline_fact_count')}/{total}"


def _render_single_report(data: dict[str, Any], json_path: Path, *, heading_level: int = 1) -> list[str]:
    fixture = data["fixture"]
    baseline = data["baseline"]
    candidate = data["candidate"]
    fact_coverage = data["fact_coverage"]
    heading = "#" * heading_level
    sub = "#" * (heading_level + 1)
    subsub = "#" * (heading_level + 2)
    lines = [
        f"{heading} Smart Update G4 Stage Benchmark: {fixture['fixture_id']}",
        "",
        f"- generated_at: `{data['generated_at']}`",
        f"- artifact_json: `{json_path}`",
        f"- fixture: `{fixture['fixture_id']}`",
        f"- title: `{fixture.get('title') or ''}`",
        f"- baseline: `{baseline['model']}`",
        f"- baseline_path: `{baseline.get('path') or '-'}`",
        f"- baseline_source_artifact: `{baseline.get('source_artifact') or '-'}`",
        f"- candidate: `{candidate['model']}`",
        f"- candidate_path: `{candidate['path']}`",
        f"- writer_model: `{candidate.get('writer_model') or '-'}`",
        f"- candidate_has_g3: `{candidate.get('candidate_has_g3')}`",
        "",
        f"{sub} Stage Summary",
        "",
        "| Stage | Baseline | Candidate | Verdict |",
        "| --- | --- | --- | --- |",
    ]
    for row in data["stage_summary"]:
        lines.append(
            f"| `{row['stage']}` | `{_clip(row.get('baseline'), 220)}` | `{_clip(row.get('candidate'), 220)}` | `{row.get('verdict')}` |"
        )
    coverage_summary = (fact_coverage.get("summary") or {}) if isinstance(fact_coverage, dict) else {}
    lost = list(coverage_summary.get("lost_baseline_facts") or [])
    critical_losses = sum(1 for item in lost if str(item.get("loss_severity") or "").strip() == "critical")
    major_losses = sum(1 for item in lost if str(item.get("loss_severity") or "").strip() == "major")
    minor_losses = sum(1 for item in lost if str(item.get("loss_severity") or "").strip() == "minor")
    lines.extend(
        [
            "",
            f"{sub} Fact Coverage",
            "",
            f"- verdict: `{coverage_summary.get('verdict')}`",
            f"- grounded covered: `{_coverage_cell(coverage_summary)}`",
            f"- critical/major/minor losses: `{critical_losses}` / `{major_losses}` / `{minor_losses}`",
            f"- useful added: `{len(coverage_summary.get('added_g4_facts') or [])}`",
            f"- suspicious: `{len(coverage_summary.get('suspicious_g4_facts') or [])}`",
            "",
            f"{subsub} Baseline facts_text_clean",
            "",
        ]
    )
    if baseline.get("baseline_fact_source"):
        lines.append(f"_source: `{baseline.get('baseline_fact_source')}`_")
        lines.append("")
    for fact in baseline.get("facts_text_clean") or []:
        lines.append(f"- {fact}")
    lines.extend(["", f"{subsub} Candidate facts_text_clean", ""])
    for fact in candidate.get("facts_text_clean") or []:
        lines.append(f"- {fact}")
    candidate_logistics = list(coverage_summary.get("g4_logistics_fact_texts") or [])
    if candidate_logistics:
        lines.extend(["", f"{subsub} Candidate logistics / infoblock facts", ""])
        for fact in candidate_logistics:
            lines.append(f"- {fact}")
    lines.extend(["", f"{sub} Lollipop-Light Writer Pack", ""])
    constraints = (((candidate.get("writer_pack") or {}).get("payload") or {}).get("constraints") or {})
    lines.append(f"- must_cover_fact_ids: `{constraints.get('must_cover_fact_ids') or []}`")
    lines.append(f"- headings: `{constraints.get('headings') or []}`")
    lines.append(f"- layout_flags: `{(candidate.get('layout_audit') or {}).get('flags') or []}`")
    lines.extend(["", f"{subsub} Layout Blocks", ""])
    for block in (candidate.get("layout_payload") or {}).get("blocks") or []:
        lines.append(f"- role=`{block.get('role')}` heading=`{block.get('heading')}` refs=`{block.get('fact_refs')}` style=`{block.get('style')}`")
    lines.extend(["", f"{sub} Text Comparison", "", f"{subsub} Baseline Description", ""])
    lines.extend(_fenced(baseline.get("description_md"), "md"))
    lines.extend(["", f"{subsub} Candidate Description", ""])
    lines.extend(_fenced(candidate.get("description_md"), "md"))
    lines.extend(["", f"{sub} Telegraph Preview", "", f"{subsub} Baseline Telegraph Preview", ""])
    lines.extend(_fenced(baseline.get("telegraph_preview_text") or _telegraph_preview_text(type("FixtureView", (), fixture), baseline), "md"))
    lines.extend(["", f"{subsub} Candidate Telegraph Preview", ""])
    lines.extend(_fenced(candidate.get("telegraph_preview_text") or _telegraph_preview_text(type("FixtureView", (), fixture), candidate), "md"))
    lines.extend(["", f"{sub} Derived Fields", ""])
    lines.append(f"- baseline short_description: `{baseline.get('short_description') or ''}`")
    lines.append(f"- candidate short_description: `{candidate.get('short_description') or ''}`")
    lines.append(f"- baseline search_digest: `{baseline.get('search_digest') or ''}`")
    lines.append(f"- candidate search_digest: `{candidate.get('search_digest') or ''}`")
    lines.extend(["", f"{sub} Timings", ""])
    lines.append(f"- baseline wall: `{(baseline.get('timings') or {}).get('wall_clock_sec')}`")
    lines.append(f"- candidate wall: `{(candidate.get('timings') or {}).get('wall_clock_sec')}`")
    lines.append(f"- baseline stages: `{(baseline.get('timings') or {}).get('stage_sec')}`")
    lines.append(f"- candidate stages: `{(candidate.get('timings') or {}).get('stage_sec')}`")
    if candidate.get("stage_errors"):
        lines.extend(["", f"{sub} Stage Errors", ""])
        for err in candidate.get("stage_errors") or []:
            lines.append(f"- `{err}`")
    return lines


def _render_report(data: dict[str, Any], json_path: Path) -> str:
    if "results" not in data:
        return "\n".join(_render_single_report(data, json_path)).rstrip() + "\n"
    results = list(data.get("results") or [])
    lines = [
        "# Smart Update G4 Stage Benchmark",
        "",
        f"- generated_at: `{data['generated_at']}`",
        f"- artifact_json: `{json_path}`",
        f"- fixtures: `{len(results)}`",
        f"- writer_lane: `{data.get('writer_lane') or 'gemma-4-primary'}`",
        f"- prod_db: `{data.get('prod_db') or '-'}`",
        f"- checkpoint: `{data.get('checkpoint') or {}}`",
        "",
        "## Fixture Summary",
        "",
        "| Fixture | Title | Writer | Facts | Description chars | Verdict | Latency |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for result in results:
        fixture = result.get("fixture") or {}
        candidate = result.get("candidate") or {}
        baseline = result.get("baseline") or {}
        coverage_summary = ((result.get("fact_coverage") or {}).get("summary") or {})
        lines.append(
            "| "
            + f"`{fixture.get('fixture_id')}` | "
            + f"{_clip(fixture.get('title'), 70)} | "
            + f"`{candidate.get('writer_model') or '-'}` | "
            + f"`{_coverage_cell(coverage_summary)}` | "
            + f"`{(baseline.get('metrics') or {}).get('chars')} -> {(candidate.get('metrics') or {}).get('chars')}` | "
            + f"`{coverage_summary.get('verdict')}` | "
            + f"`{(candidate.get('timings') or {}).get('wall_clock_sec')}` |"
        )
    for result in results:
        lines.extend(["", ""])
        lines.extend(_render_single_report(result, json_path, heading_level=2))
    return "\n".join(lines).rstrip() + "\n"


async def _run_one(args: argparse.Namespace, fixture: Any) -> dict[str, Any]:
    if args.reuse_baseline_artifact:
        baseline = _load_frozen_baseline(args.reuse_baseline_artifact, fixture)
    elif getattr(fixture, "baseline_description_md", None):
        baseline = _baseline_from_fixture_snapshot(fixture)
    else:
        baseline = await _run_current_smart_update_baseline(fixture, model=args.baseline_model)
    candidate = await _run_candidate_g4_lollipop2(
        fixture,
        model=args.candidate_model,
        four_o_model=args.four_o_model,
    )
    fact_coverage = await _run_fact_coverage(fixture, baseline, candidate, model=args.candidate_model)
    data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fixture": _object_to_dict(fixture),
        "source_evidence": _source_evidence(fixture),
        "baseline": baseline,
        "candidate": candidate,
        "fact_coverage": fact_coverage,
    }
    data["baseline"]["telegraph_preview_text"] = _telegraph_preview_text(fixture, baseline)
    data["candidate"]["telegraph_preview_text"] = _telegraph_preview_text(fixture, candidate)
    data["stage_summary"] = _compare_stage_summary(baseline, candidate, fact_coverage)
    return data


async def _run(args: argparse.Namespace) -> tuple[Path, Path]:
    _load_env_file()
    ARTIFACTS_ROOT.mkdir(parents=True, exist_ok=True)
    if args.prod_db:
        fixtures = _fixtures_from_prod_db(args.prod_db, args.event_ids or args.fixtures)
    elif args.reuse_fixture_artifact:
        fixtures = bench._fixtures_from_artifact(args.reuse_fixture_artifact, args.fixtures)
    else:
        fixtures = bench._fixtures_from_cli(args.fixtures)
    if not fixtures:
        raise RuntimeError("No fixtures selected")
    generated_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    json_path = ARTIFACTS_ROOT / f"smart_update_g4_stage_benchmark_{generated_at}.json"
    md_path = ARTIFACTS_ROOT / f"smart_update_g4_stage_benchmark_{generated_at}.md"
    results: list[dict[str, Any]] = []
    for fixture in fixtures:
        results.append(await _run_one(args, fixture))
        checkpoint_data: dict[str, Any]
        if len(fixtures) == 1:
            checkpoint_data = results[0]
        else:
            checkpoint_data = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "writer_lane": "gemma-4-primary",
                "prod_db": args.prod_db or "",
                "checkpoint": {
                    "completed": len(results),
                    "total": len(fixtures),
                    "complete": len(results) == len(fixtures),
                },
                "results": results,
            }
        json_path.write_text(json.dumps(checkpoint_data, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(_render_report(checkpoint_data, json_path), encoding="utf-8")
    if len(results) == 1:
        data = results[0]
    else:
        data = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "writer_lane": "gemma-4-primary",
            "prod_db": args.prod_db or "",
            "checkpoint": {"completed": len(results), "total": len(fixtures), "complete": True},
            "results": results,
        }
    json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_report(data, json_path), encoding="utf-8")
    return json_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run staged Smart Update G3 vs G4+lollipop-light benchmark.")
    parser.add_argument("--fixtures", default="red_cosmos")
    parser.add_argument("--reuse-fixture-artifact", default="")
    parser.add_argument(
        "--reuse-baseline-artifact",
        default="",
        help=(
            "Reuse frozen Gemma 3 baseline data from a lollipop_g4 or staged benchmark artifact. "
            "Use this after Gemma 3 is unavailable; candidate generation still receives no baseline payload."
        ),
    )
    parser.add_argument("--baseline-model", default=DEFAULT_BASELINE_MODEL)
    parser.add_argument("--candidate-model", default=DEFAULT_CANDIDATE_MODEL)
    parser.add_argument("--four-o-model", default=DEFAULT_4O_MODEL)
    parser.add_argument(
        "--prod-db",
        default="",
        help="Load benchmark fixtures and frozen Gemma 3-era baseline text/facts from a local production SQLite snapshot.",
    )
    parser.add_argument(
        "--event-ids",
        default="",
        help=f"Comma-separated event ids for --prod-db. Default: {DEFAULT_PROD_DB_EVENT_IDS}.",
    )
    args = parser.parse_args()
    json_path, md_path = asyncio.run(_run(args))
    print(json_path)
    print(md_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
