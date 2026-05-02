#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.util
import json
import os
import re
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone
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


def _fact_item_list(facts: list[str], *, category: str = "public") -> list[dict[str, Any]]:
    return [
        {"index": idx, "text": fact, "kind": category, "source_span": ""}
        for idx, fact in enumerate(facts)
        if str(fact or "").strip()
    ]


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
        bench._ask_gemma_json_direct(
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
        bench._ask_gemma_json_direct(
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
        bench._ask_gemma_json_direct(
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
        bench._ask_gemma_json_direct(
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
    writer_validation = writer_final_family.ValidationResult(errors=["writer.final_4o.not_run"], warnings=[])
    writer_model = four_o_model
    try:
        writer_output = await _time_stage(
            timings,
            "writer.final_4o",
            bench._ask_4o_json(
                prompt=writer_final_family._build_prompt(writer_pack["payload"]),
                schema={
                    "type": "object",
                    "properties": {"title": {"type": "string"}, "description_md": {"type": "string"}},
                    "required": ["title", "description_md"],
                    "additionalProperties": False,
                },
                model=four_o_model,
            ),
        )
        writer_validation = writer_final_family._validate_writer_output(writer_pack["payload"], writer_output)
    except Exception as exc:
        stage_errors.append(f"writer.final_4o.error:{type(exc).__name__}:{str(exc)[:240]}")
        try:
            writer_model = f"{four_o_model}->gemma-4"
            writer_output = await _time_stage(
                timings,
                "writer.final_g4_after_4o_error",
                bench._ask_gemma_json_direct(
                    model=model,
                    system_prompt=writer_final_family._build_prompt(writer_pack["payload"]),
                    user_payload={"task": "Return final title and description_md JSON for this writer_pack."},
                    max_tokens=1400,
                    response_schema=_writer_response_schema_gemma(),
                    timeout_sec=min(bench._gemma_direct_timeout_sec(), 90.0),
                    allow_json_repair=False,
                ),
            )
            writer_validation = writer_final_family._validate_writer_output(writer_pack["payload"], writer_output)
            writer_validation.warnings.append(f"writer.final_4o.error:{type(exc).__name__}")
        except Exception as gemma_exc:
            stage_errors.append(f"writer.final_g4.error:{type(gemma_exc).__name__}:{str(gemma_exc)[:240]}")
            writer_output = {"title": fixture.title, "description_md": ""}
            writer_validation = writer_final_family.ValidationResult(
                errors=[
                    f"writer.final_4o.error:{type(exc).__name__}",
                    f"writer.final_g4.error:{type(gemma_exc).__name__}",
                ],
                warnings=[],
            )
    applied = writer_final_family._apply_writer_output(writer_pack["payload"], writer_output)
    description = str(applied.get("description_md") or "")
    search_digest = await _time_stage(
        timings,
        "search_digest_g4",
        su._llm_build_search_digest(title=applied.get("title") or fixture.title, description=description, event_type=fixture.event_type),
    )
    short_description = await _time_stage(
        timings,
        "short_description_g4",
        su._llm_build_short_description(title=applied.get("title") or fixture.title, description=description, event_type=fixture.event_type),
    )
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
        "timings": {
            "wall_clock_sec": wall,
            "stage_sec": timings,
            "gemma_calls_observed": len([k for k in timings if k.startswith(("create", "facts", "lollipop", "search", "short"))]),
            "four_o_calls_observed": 1 if "writer.final_4o" in timings else 0,
        },
    }


async def _run_fact_coverage(fixture: Any, baseline: dict[str, Any], candidate: dict[str, Any], *, model: str) -> dict[str, Any]:
    source_excerpt = bench._source_excerpt(fixture.sources, limit=9000)
    return await bench._run_fact_coverage_reviewer(
        fixture=fixture,
        baseline={
            "per_source_facts": {"smart_update": baseline.get("raw_facts") or []},
            "facts_text_clean": baseline.get("facts_text_clean") or [],
            "description_md": baseline.get("description_md") or "",
        },
        public_facts=_fact_item_list(list(candidate.get("facts_text_clean") or []), category="public"),
        logistics_facts=_fact_item_list(
            [
                str(value)
                for value in [
                    fixture.date,
                    fixture.time,
                    fixture.location_name,
                    fixture.location_address,
                    fixture.city,
                ]
                if value
            ],
            category="logistics",
        ),
        source_excerpt=source_excerpt,
        gemma_model=model,
    )


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


def _render_report(data: dict[str, Any], json_path: Path) -> str:
    fixture = data["fixture"]
    baseline = data["baseline"]
    candidate = data["candidate"]
    fact_coverage = data["fact_coverage"]
    lines = [
        "# Smart Update G4 Stage Benchmark",
        "",
        f"- generated_at: `{data['generated_at']}`",
        f"- artifact_json: `{json_path}`",
        f"- fixture: `{fixture['fixture_id']}`",
        f"- baseline: `{baseline['model']}`",
        f"- candidate: `{candidate['model']}`",
        f"- candidate_path: `{candidate['path']}`",
        f"- writer_model: `{candidate.get('writer_model') or '-'}`",
        f"- candidate_has_g3: `{candidate.get('candidate_has_g3')}`",
        "",
        "## Stage Summary",
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
            "## Fact Coverage",
            "",
            f"- verdict: `{coverage_summary.get('verdict')}`",
            f"- grounded covered: `{coverage_summary.get('covered_grounded_baseline_fact_count')}` / `{coverage_summary.get('grounded_baseline_fact_count')}`",
            f"- critical/major/minor losses: `{critical_losses}` / `{major_losses}` / `{minor_losses}`",
            f"- useful added: `{len(coverage_summary.get('added_g4_facts') or [])}`",
            f"- suspicious: `{len(coverage_summary.get('suspicious_g4_facts') or [])}`",
            "",
            "### Baseline facts_text_clean",
            "",
        ]
    )
    for fact in baseline.get("facts_text_clean") or []:
        lines.append(f"- {fact}")
    lines.extend(["", "### Candidate facts_text_clean", ""])
    for fact in candidate.get("facts_text_clean") or []:
        lines.append(f"- {fact}")
    lines.extend(["", "## Lollipop-Light Writer Pack", ""])
    constraints = (((candidate.get("writer_pack") or {}).get("payload") or {}).get("constraints") or {})
    lines.append(f"- must_cover_fact_ids: `{constraints.get('must_cover_fact_ids') or []}`")
    lines.append(f"- headings: `{constraints.get('headings') or []}`")
    lines.append(f"- layout_flags: `{(candidate.get('layout_audit') or {}).get('flags') or []}`")
    lines.extend(["", "### Layout Blocks", ""])
    for block in (candidate.get("layout_payload") or {}).get("blocks") or []:
        lines.append(f"- role=`{block.get('role')}` heading=`{block.get('heading')}` refs=`{block.get('fact_refs')}` style=`{block.get('style')}`")
    lines.extend(["", "## Text Comparison", "", "### Baseline Description", "", baseline.get("description_md") or "_empty_", "", "### Candidate Description", "", candidate.get("description_md") or "_empty_"])
    lines.extend(["", "## Derived Fields", ""])
    lines.append(f"- baseline short_description: `{baseline.get('short_description') or ''}`")
    lines.append(f"- candidate short_description: `{candidate.get('short_description') or ''}`")
    lines.append(f"- baseline search_digest: `{baseline.get('search_digest') or ''}`")
    lines.append(f"- candidate search_digest: `{candidate.get('search_digest') or ''}`")
    lines.extend(["", "## Timings", ""])
    lines.append(f"- baseline wall: `{(baseline.get('timings') or {}).get('wall_clock_sec')}`")
    lines.append(f"- candidate wall: `{(candidate.get('timings') or {}).get('wall_clock_sec')}`")
    lines.append(f"- baseline stages: `{(baseline.get('timings') or {}).get('stage_sec')}`")
    lines.append(f"- candidate stages: `{(candidate.get('timings') or {}).get('stage_sec')}`")
    if candidate.get("stage_errors"):
        lines.extend(["", "## Stage Errors", ""])
        for err in candidate.get("stage_errors") or []:
            lines.append(f"- `{err}`")
    return "\n".join(lines).rstrip() + "\n"


async def _run(args: argparse.Namespace) -> tuple[Path, Path]:
    _load_env_file()
    ARTIFACTS_ROOT.mkdir(parents=True, exist_ok=True)
    if args.reuse_fixture_artifact:
        fixtures = bench._fixtures_from_artifact(args.reuse_fixture_artifact, args.fixtures)
    else:
        fixtures = bench._fixtures_from_cli(args.fixtures)
    if len(fixtures) != 1:
        raise RuntimeError("Stage debug runner currently expects exactly one fixture")
    fixture = fixtures[0]
    baseline = await _run_current_smart_update_baseline(fixture, model=args.baseline_model)
    candidate = await _run_candidate_g4_lollipop2(
        fixture,
        model=args.candidate_model,
        four_o_model=args.four_o_model,
    )
    fact_coverage = await _run_fact_coverage(fixture, baseline, candidate, model=args.candidate_model)
    generated_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fixture": asdict(fixture),
        "source_evidence": _source_evidence(fixture),
        "baseline": baseline,
        "candidate": candidate,
        "fact_coverage": fact_coverage,
    }
    data["stage_summary"] = _compare_stage_summary(baseline, candidate, fact_coverage)
    json_path = ARTIFACTS_ROOT / f"smart_update_g4_stage_benchmark_{generated_at}.json"
    md_path = ARTIFACTS_ROOT / f"smart_update_g4_stage_benchmark_{generated_at}.md"
    json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_report(data, json_path), encoding="utf-8")
    return json_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run staged Smart Update G3 vs G4+lollipop-light benchmark.")
    parser.add_argument("--fixtures", default="red_cosmos")
    parser.add_argument("--reuse-fixture-artifact", default="")
    parser.add_argument("--baseline-model", default=DEFAULT_BASELINE_MODEL)
    parser.add_argument("--candidate-model", default=DEFAULT_CANDIDATE_MODEL)
    parser.add_argument("--four-o-model", default=DEFAULT_4O_MODEL)
    args = parser.parse_args()
    json_path, md_path = asyncio.run(_run(args))
    print(json_path)
    print(md_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
