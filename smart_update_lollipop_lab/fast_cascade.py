from __future__ import annotations

import asyncio
import os
import re
import sys
import time
from typing import Any

from . import editorial_layout_family as layout_family
from . import facts_prioritize_family as prioritize_family
from . import fast_extract_family
from . import writer_final_4o_family as writer_final_family
from . import writer_pack_compose_family as writer_pack_family
from .full_cascade import (
    FourOJsonCaller,
    GemmaJsonCaller,
    SleepCaller,
    _bucket_slug,
    _gemma_json_call_with_timeout,
    _new_timing_profile,
    _record_stage_timing,
    _source_record_prefix,
)


FAST_CONTRACT_VERSION = "lollipop_g4_fast.v2"


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _fast_literal_gate_errors(*, extract_records: list[dict[str, Any]], merged_records: list[dict[str, Any]]) -> list[str]:
    source_literals = {
        str(item)
        for record in extract_records
        for item in list(record.get("literal_items") or [])
        if str(item)
    }
    source_texts = [
        f"{record.get('text') or ''} {record.get('evidence') or ''}"
        for record in extract_records
    ]
    errors: list[str] = []
    for record in merged_records:
        for item in list(record.get("literal_items") or []):
            literal = str(item)
            if literal and literal not in source_literals and not any(literal in text for text in source_texts):
                errors.append(f"literal.title_mutation:{record.get('record_id')}:{literal}")
    return errors


def _salience_rank(value: str) -> int:
    return {"must_keep": 0, "support": 1, "uncertain": 2, "suppress": 3}.get(str(value or "").strip(), 2)


def _hook_rank(value: str) -> int:
    return {
        "rarity": 0,
        "atmosphere": 1,
        "quote": 2,
        "local_context": 3,
        "format_action": 4,
        "program_literal": 5,
        "staging": 6,
        "people_roles": 7,
        "other": 8,
        "none": 9,
    }.get(str(value or "").strip(), 8)


def _bucket_rank(value: str) -> int:
    return {
        "event_core": 0,
        "forward_looking": 1,
        "support_context": 2,
        "program_list": 3,
        "people_and_roles": 4,
        "uncertain": 5,
    }.get(str(value or "").strip(), 9)


def _compose_fast_fact_pack(records: list[dict[str, Any]], fixture: Any) -> dict[str, Any]:
    pack: dict[str, Any] = {}
    for bucket in ("event_core", "program_list", "people_and_roles", "forward_looking", "support_context", "uncertain"):
        counter = 1
        bucket_items = []
        for record in records:
            if record.get("bucket") != bucket:
                continue
            hook_type = str(record.get("hook_type") or "other")
            salience = str(record.get("salience") or "support")
            weight = {"must_keep": "high", "support": "medium", "uncertain": "low", "suppress": "low"}.get(salience, "medium")
            bucket_items.append(
                {
                    "fact_id": f"{_bucket_slug(bucket)}{counter:02d}",
                    "bucket": bucket,
                    "text": _clean_text(record.get("text")),
                    "literal_items": list(record.get("literal_items") or []),
                    "record_ids": list(record.get("record_ids") or [record.get("record_id")]),
                    "source_refs": list(record.get("source_refs") or []),
                    "weight": weight,
                    "weight_reasoning": "",
                    "narrative_policy": "suppress" if salience in {"suppress", "uncertain"} or bucket == "uncertain" else "include",
                    "fast": {
                        "salience": salience,
                        "hook_type": hook_type,
                        "dedup_key": str(record.get("dedup_key") or ""),
                        "role_class": str(record.get("role_class") or "none"),
                    },
                }
            )
            counter += 1
        pack[bucket] = bucket_items

    if not pack.get("event_core"):
        fallback = next(
            (
                item
                for bucket in ("forward_looking", "support_context", "program_list", "people_and_roles")
                for item in list(pack.get(bucket) or [])
                if item.get("narrative_policy") != "suppress"
            ),
            None,
        )
        if fallback is not None:
            source_bucket = fallback["bucket"]
            pack[source_bucket] = [item for item in pack[source_bucket] if item["fact_id"] != fallback["fact_id"]]
            fallback = {**fallback, "bucket": "event_core", "fact_id": "EC01"}
            fallback.setdefault("fast", {})["fallback_promoted_to_event_core"] = True
            pack["event_core"] = [fallback]

    logistics: list[dict[str, Any]] = []
    for fact_id, value in (
        ("LG01", fixture.date),
        ("LG02", fixture.time),
        ("LG03", fixture.location_name),
        ("LG04", fixture.location_address),
    ):
        if value:
            logistics.append(
                {
                    "fact_id": fact_id,
                    "bucket": "logistics_infoblock",
                    "text": _clean_text(value),
                    "literal_items": [],
                    "record_ids": [],
                    "source_refs": [],
                    "weight": "high",
                    "weight_reasoning": "",
                    "narrative_policy": "include",
                }
            )
    pack["logistics_infoblock"] = logistics
    pack = prioritize_family._apply_narrative_policies(pack, event_type=fixture.event_type)
    return pack


def _choose_fast_lead(weighted_pack: dict[str, Any], *, title: str, event_type: str) -> dict[str, Any]:
    included = [
        item
        for item in prioritize_family._flat_facts(weighted_pack)
        if item.get("narrative_policy") != "suppress" and item.get("bucket") != "logistics_infoblock"
    ]
    if not included:
        return {"lead_fact_id": "", "lead_support_id": "", "event_title": title, "cleaning_stats": {"fallback_reasons": ["empty_fast_pack"]}}
    lead = min(
        included,
        key=lambda item: (
            0 if item.get("bucket") == "event_core" else 1,
            _salience_rank(str(item.get("fast", {}).get("salience") or "")),
            _hook_rank(str(item.get("fast", {}).get("hook_type") or "")),
        ),
    )
    support_candidates = [
        item
        for item in included
        if item["fact_id"] != lead["fact_id"] and not (item.get("bucket") == "program_list" and item.get("literal_items"))
    ]
    support = None
    if support_candidates:
        support = min(
            support_candidates,
            key=lambda item: (
                _hook_rank(str(item.get("fast", {}).get("hook_type") or "")),
                _salience_rank(str(item.get("fast", {}).get("salience") or "")),
                _bucket_rank(str(item.get("bucket") or "")),
            ),
        )
    cleaned = prioritize_family._clean_lead(
        {
            "lead_fact_id": lead["fact_id"],
            "lead_support_id": support["fact_id"] if support else "",
        },
        weighted_pack,
        title=title,
        event_type=event_type,
    )
    cleaned["event_title"] = title
    return cleaned


def _build_fast_layout_payload(weighted_pack: dict[str, Any], lead_payload: dict[str, Any], precompute: dict[str, Any]) -> dict[str, Any]:
    included_ids = [
        fact_id
        for fact_id in list(precompute.get("all_fact_ids") or [])
        if not str(fact_id).startswith("LG")
    ]
    lead_ids = {lead_payload.get("lead_fact_id"), lead_payload.get("lead_support_id")} - {None, ""}
    catalog = {
        item["fact_id"]: item
        for item in prioritize_family._flat_facts(weighted_pack)
        if isinstance(item, dict) and item.get("fact_id")
    }
    program_ids = [
        fact_id
        for fact_id in included_ids
        if fact_id not in lead_ids and catalog.get(fact_id, {}).get("bucket") == "program_list"
    ]
    people_ids = [
        fact_id
        for fact_id in included_ids
        if fact_id not in lead_ids and catalog.get(fact_id, {}).get("bucket") == "people_and_roles"
    ]
    production_people_ids = [
        fact_id
        for fact_id in people_ids
        if str(catalog.get(fact_id, {}).get("fast", {}).get("role_class") or "") == "production_team"
    ]
    cast_people_ids = [
        fact_id
        for fact_id in people_ids
        if fact_id not in set(production_people_ids)
    ]
    body_ids = [
        fact_id
        for fact_id in included_ids
        if fact_id not in lead_ids and fact_id not in set(program_ids + people_ids)
    ]
    blocks = [{"role": "lead", "fact_refs": [item for item in [lead_payload.get("lead_fact_id"), lead_payload.get("lead_support_id")] if item], "style": "narrative", "heading": None}]
    if body_ids:
        body_heading = None
        if precompute.get("allow_semantic_headings") and people_ids and not program_ids:
            body_heading = "О событии"
        blocks.append({"role": "body", "fact_refs": body_ids, "style": "narrative", "heading": body_heading})
    if program_ids:
        blocks.append({"role": "program", "fact_refs": program_ids, "style": "list", "heading": "Программа" if precompute.get("allow_semantic_headings") else None})
    if production_people_ids and cast_people_ids:
        blocks.append({"role": "body", "fact_refs": production_people_ids, "style": "structured", "heading": "Постановочная группа" if precompute.get("allow_semantic_headings") else None})
        blocks.append({"role": "body", "fact_refs": cast_people_ids, "style": "structured", "heading": "Действующие лица и исполнители" if precompute.get("allow_semantic_headings") else None})
    elif people_ids:
        blocks.append({"role": "body", "fact_refs": people_ids, "style": "structured", "heading": "Участники" if precompute.get("allow_semantic_headings") else None})
    logistics_ids = list(precompute.get("logistics_ids") or [])
    if logistics_ids:
        blocks.append({"role": "infoblock", "fact_refs": logistics_ids, "style": "structured", "heading": None})
    return {"title_strategy": "keep", "title_hint_ref": None, "blocks": blocks}


def _build_fast_writer_profile(weighted_pack: dict[str, Any], writer_pack_payload: dict[str, Any]) -> dict[str, Any]:
    catalog = {
        item["fact_id"]: item
        for item in prioritize_family._flat_facts(weighted_pack)
        if isinstance(item, dict)
        and item.get("fact_id")
        and item.get("narrative_policy") != "suppress"
        and item.get("bucket") != "logistics_infoblock"
    }
    must_cover_ids = [
        str(fact_id)
        for fact_id in list(writer_pack_payload.get("constraints", {}).get("must_cover_fact_ids") or [])
        if str(fact_id) in catalog
    ]
    must_cover_items = [catalog[fact_id] for fact_id in must_cover_ids]
    hook_types = sorted(
        {
            str(item.get("fast", {}).get("hook_type") or "other")
            for item in must_cover_items
            if str(item.get("fast", {}).get("hook_type") or "").strip()
        }
    )
    buckets = sorted({str(item.get("bucket") or "") for item in must_cover_items if str(item.get("bucket") or "").strip()})
    source_refs = sorted(
        {
            str(source_ref)
            for item in must_cover_items
            for source_ref in list(item.get("source_refs") or [])
            if str(source_ref).strip()
        }
    )
    has_literal_program = any(section.get("literal_items") for section in list(writer_pack_payload.get("sections") or []))
    has_named_roles = "people_and_roles" in buckets
    has_rarity = "rarity" in hook_types
    has_atmosphere = "atmosphere" in hook_types
    has_quote = "quote" in hook_types
    has_local_context = "local_context" in hook_types
    narrative_fact_count = sum(1 for item in must_cover_items if item.get("bucket") not in {"people_and_roles", "logistics_infoblock"})
    lead_strategy = "format_action"
    if has_rarity:
        lead_strategy = "rarity"
    elif has_quote:
        lead_strategy = "quote"
    elif has_atmosphere:
        lead_strategy = "atmosphere"
    elif has_local_context:
        lead_strategy = "local_context"
    elif has_literal_program:
        lead_strategy = "program_material"
    return {
        "contract": "coverage_first_fast_writer_profile.v1",
        "must_cover_fact_count": len(must_cover_ids),
        "extractable_source_count": len(source_refs),
        "hook_types": hook_types,
        "buckets": buckets,
        "has_rarity": has_rarity,
        "has_atmosphere": has_atmosphere,
        "has_quote_candidate": has_quote,
        "has_local_context": has_local_context,
        "has_literal_program": has_literal_program,
        "has_named_roles": has_named_roles,
        "narrative_fact_count": narrative_fact_count,
        "rich_case": len(must_cover_ids) >= 8 or (has_named_roles and narrative_fact_count >= 3),
        "lead_strategy": lead_strategy,
        "section_count": len(list(writer_pack_payload.get("sections") or [])),
    }


def _fast_merge_passthrough_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for record in records:
        raw_key = _clean_text(record.get("dedup_key"))
        key = raw_key or f"record:{record.get('record_id')}"
        current = by_key.get(key)
        if current is None:
            by_key[key] = {**record}
            order.append(key)
            continue
        if (
            _salience_rank(str(record.get("salience") or "")),
            _bucket_rank(str(record.get("bucket") or "")),
            -len(_clean_text(record.get("text"))),
        ) < (
            _salience_rank(str(current.get("salience") or "")),
            _bucket_rank(str(current.get("bucket") or "")),
            -len(_clean_text(current.get("text"))),
        ):
            replacement = {**record}
            replacement["source_refs"] = list(dict.fromkeys(list(current.get("source_refs") or []) + list(record.get("source_refs") or [])))
            replacement["literal_items"] = list(dict.fromkeys(list(current.get("literal_items") or []) + list(record.get("literal_items") or [])))
            replacement["record_ids"] = list(dict.fromkeys(list(current.get("record_ids") or [current.get("record_id")]) + list(record.get("record_ids") or [record.get("record_id")])))
            by_key[key] = replacement
        else:
            current["source_refs"] = list(dict.fromkeys(list(current.get("source_refs") or []) + list(record.get("source_refs") or [])))
            current["literal_items"] = list(dict.fromkeys(list(current.get("literal_items") or []) + list(record.get("literal_items") or [])))
            current["record_ids"] = list(dict.fromkeys(list(current.get("record_ids") or [current.get("record_id")]) + list(record.get("record_ids") or [record.get("record_id")])))

    merged: list[dict[str, Any]] = []
    for index, key in enumerate(order, start=1):
        record = by_key[key]
        merged.append(
            {
                **record,
                "record_id": f"FMG_{index:02d}",
                "stage_id": fast_extract_family.FAST_MERGE_STAGE_ID,
                "record_ids": list(dict.fromkeys(list(record.get("record_ids") or [record.get("record_id")]))),
            }
        )
    return merged


async def run_fast_cascade_variant(
    *,
    fixture: Any,
    gemma_model: str,
    gemma4: bool,
    gemma_json_call: GemmaJsonCaller,
    four_o_json_call: FourOJsonCaller,
    sleep: SleepCaller,
    four_o_model: str,
    gemma_call_gap_s: float,
) -> dict[str, Any]:
    stage_errors: list[str] = []
    started_at = time.perf_counter()
    timing_profile = _new_timing_profile()

    def _log(stage: str) -> None:
        print(f"[fast-cascade] {gemma_model} {fixture.fixture_id} {stage}", file=sys.stderr, flush=True)

    async def _timed_sleep(label: str, seconds: float) -> None:
        sleep_started = time.perf_counter()
        await sleep(seconds)
        _record_stage_timing(timing_profile, label, time.perf_counter() - sleep_started, kind="sleep")

    async def _timed_gemma(stage_label: str, **kwargs: Any) -> dict[str, Any]:
        call_started = time.perf_counter()
        result = await _gemma_json_call_with_timeout(
            gemma_json_call=gemma_json_call,
            stage_errors=stage_errors,
            stage_label=stage_label,
            **kwargs,
        )
        _record_stage_timing(timing_profile, stage_label, time.perf_counter() - call_started, kind="gemma")
        return result

    async def _timed_four_o(stage_label: str, *, prompt: str, schema: dict[str, Any], model: str) -> dict[str, Any]:
        call_started = time.perf_counter()
        result = await four_o_json_call(prompt=prompt, schema=schema, model=model)
        _record_stage_timing(timing_profile, stage_label, time.perf_counter() - call_started, kind="four_o")
        return result

    async def _extract_one_source(source: Any) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        stage_label = f"{fast_extract_family.FAST_EXTRACT_STAGE_ID}[{source.source_id}]"
        _log(stage_label)
        user_payload = fast_extract_family.build_fast_extract_payload(fixture=fixture, source=source)
        payload = await _timed_gemma(
            stage_label,
            model=gemma_model,
            system_prompt=fast_extract_family.build_fast_extract_compact_system_prompt(),
            user_payload=user_payload,
            max_tokens=1600,
            response_schema=fast_extract_family.fast_extract_response_schema() if gemma4 else None,
        ) or {"facts": []}
        records = fast_extract_family.normalize_fast_extract_items(
            payload=payload,
            source_id=source.source_id,
            record_prefix=_source_record_prefix(fast_extract_family.FAST_EXTRACT_STAGE_ID, source.source_id),
            source_excerpt=str(user_payload.get("source", {}).get("excerpt") or ""),
        )
        run = {"stage_id": fast_extract_family.FAST_EXTRACT_STAGE_ID, "source_id": source.source_id, "source_type": source.source_type, "payload": payload, "records": records}
        return run, records

    extract_runs: list[dict[str, Any]] = []
    extract_records: list[dict[str, Any]] = []
    serial_extract = os.getenv("LOLLIPOP_G4_FAST_SERIAL_EXTRACT", "").strip() == "1"
    if not serial_extract and len(fixture.sources) > 1:
        source_results = await asyncio.gather(*[_extract_one_source(source) for source in fixture.sources])
        for run, records in source_results:
            extract_runs.append(run)
            extract_records.extend(records)
    else:
        for index, source in enumerate(fixture.sources):
            run, records = await _extract_one_source(source)
            extract_runs.append(run)
            extract_records.extend(records)
            if index < len(fixture.sources) - 1:
                await _timed_sleep("sleep.gemma_gap", gemma_call_gap_s)

    merge_input_records = [
        record
        for record in extract_records
        if record.get("bucket") not in {"drop", "infoblock_only"}
        and record.get("salience") not in {"suppress", "uncertain"}
    ]
    relevant_source_ids = sorted(
        {
            str(source_ref)
            for record in merge_input_records
            for source_ref in list(record.get("source_refs") or [])
            if str(source_ref).strip()
        }
    )
    merge_raw: dict[str, Any]
    merge_owner = fast_extract_family.FAST_MERGE_STAGE_ID
    llm_merge_enabled = os.getenv("LOLLIPOP_G4_FAST_LLM_MERGE", "").strip() == "1"
    if len(relevant_source_ids) <= 1 or not llm_merge_enabled:
        merge_reason = "single_relevant_source_passthrough" if len(relevant_source_ids) <= 1 else "default_no_llm_merge_passthrough"
        merge_raw = {"facts": [], "skipped": True, "reason": merge_reason}
        merged_records = _fast_merge_passthrough_records(merge_input_records)
        merge_owner = merge_reason
    else:
        await _timed_sleep("sleep.gemma_gap", gemma_call_gap_s)
        _log(fast_extract_family.FAST_MERGE_STAGE_ID)
        merge_raw = await _timed_gemma(
            fast_extract_family.FAST_MERGE_STAGE_ID,
            model=gemma_model,
            system_prompt=fast_extract_family.build_fast_merge_system_prompt(),
            user_payload=fast_extract_family.build_fast_merge_payload(fixture=fixture, records=extract_records),
            max_tokens=3200,
            response_schema=fast_extract_family.fast_merge_response_schema() if gemma4 else None,
        ) or {"facts": []}
        merged_records = fast_extract_family.normalize_fast_merge_items(payload=merge_raw, source_records=extract_records)
    fast_merge_stats = {
        "input_records": len(extract_records),
        "output_records": len(merged_records),
        "merge_owner": merge_owner,
        "contract": "coverage_first_no_fixed_fact_cap",
        "relevant_source_ids": relevant_source_ids,
    }
    fact_pack = _compose_fast_fact_pack(merged_records, fixture)
    weighted_pack = fact_pack
    lead_payload = _choose_fast_lead(weighted_pack, title=fixture.title, event_type=fixture.event_type)
    prioritized_pack = layout_family._prioritized_fact_pack(weighted_pack)
    precompute = layout_family._precompute_layout_state(event_type=fixture.event_type, pack=prioritized_pack, lead_payload=lead_payload)

    planner_raw: dict[str, Any] | None = None
    planner_enabled = os.getenv("LOLLIPOP_G4_FAST_PLANNER", "").strip() == "1"
    if planner_enabled:
        _log(fast_extract_family.FAST_PLANNER_STAGE_ID)
        planner_raw = await _timed_gemma(
            fast_extract_family.FAST_PLANNER_STAGE_ID,
            model=gemma_model,
            system_prompt=fast_extract_family.build_fast_planner_system_prompt(),
            user_payload={
                "event_title": fixture.title,
                "event_type": fixture.event_type,
                "lead_payload": lead_payload,
                "precompute": precompute,
                "fact_pack": prioritized_pack,
            },
            max_tokens=1400,
            response_schema=fast_extract_family.fast_planner_response_schema() if gemma4 else None,
        ) or {}
        if planner_raw.get("lead_fact_id"):
            lead_payload = prioritize_family._clean_lead(planner_raw, weighted_pack, title=fixture.title, event_type=fixture.event_type)
            lead_payload["event_title"] = fixture.title
            precompute = layout_family._precompute_layout_state(event_type=fixture.event_type, pack=prioritized_pack, lead_payload=lead_payload)
        await _timed_sleep("sleep.gemma_gap", gemma_call_gap_s)

    raw_layout = planner_raw if planner_raw and planner_raw.get("blocks") else _build_fast_layout_payload(weighted_pack, lead_payload, precompute)
    layout_payload = layout_family._clean_layout_plan(
        raw_layout,
        title=fixture.title,
        pack=prioritized_pack,
        lead_payload=lead_payload,
        precompute=precompute,
    )

    layout_result = {
        "event_type": fixture.event_type,
        "layout_result": {"precompute": precompute, "payload": layout_payload},
    }
    prioritize_result = {"weight_result": {"payload": weighted_pack}}
    writer_pack = writer_pack_family._compose_writer_pack(
        event_id=0,
        title=fixture.title,
        layout_result=layout_result,
        prioritize_result=prioritize_result,
    )
    pack_payload = writer_pack["payload"]
    writer_profile = _build_fast_writer_profile(weighted_pack, pack_payload)
    pack_payload["meta"] = {
        "variant": "lollipop_g4_fast",
        "contract_version": FAST_CONTRACT_VERSION,
        "upstream_model": gemma_model,
        "merge_model": gemma_model,
        "planner_enabled": planner_enabled,
        "source_local_gemma_calls": len(fixture.sources),
        "source_extract_parallel": not serial_extract and len(fixture.sources) > 1,
        "writer_profile": writer_profile,
        "extract_record_count": len(extract_records),
        "merged_record_count": len(merged_records),
    }
    writer_schema = {
        "type": "object",
        "properties": {"title": {"type": "string"}, "description_md": {"type": "string"}},
        "required": ["title", "description_md"],
        "additionalProperties": False,
    }
    _log("writer.final_4o")
    writer_output = await _timed_four_o(
        "writer.final_4o.initial",
        prompt=writer_final_family._build_prompt(pack_payload),
        schema=writer_schema,
        model=four_o_model,
    )
    validation = writer_final_family._validate_writer_output(pack_payload, writer_output)
    fast_gate_errors = _fast_literal_gate_errors(extract_records=extract_records, merged_records=merged_records)
    applied_output = writer_final_family._apply_writer_output(pack_payload, writer_output)
    timing_profile["wall_clock_sec"] = round(time.perf_counter() - started_at, 6)

    return {
        "gemma_model": gemma_model,
        "variant_mode": "lollipop_g4_fast",
        "fast_contract_version": FAST_CONTRACT_VERSION,
        "scope_select": {
            "selected_source_ids": [source.source_id for source in fixture.sources],
            "background_source_ids": [],
            "mixed_phase": False,
            "reason": "fast variant keeps source-local extraction for every provided fixture source",
        },
        "extract_runs": extract_runs,
        "extract_records": extract_records,
        "dedup_input_records": extract_records,
        "dedup": {"decisions": [{"record_id": item["record_id"], "keep": "keep", "canonical_record_id": item["record_id"], "relation": "enrichment"} for item in extract_records]},
        "merged_records": merged_records,
        "merge_raw": merge_raw,
        "fast_merge_stats": fast_merge_stats,
        "fact_pack": fact_pack,
        "weight_result": {"payload": weighted_pack},
        "lead_payload": lead_payload,
        "layout_precompute": precompute,
        "layout_payload_raw": raw_layout,
        "layout_payload": layout_payload,
        "planner_raw": planner_raw,
        "writer_pack": pack_payload,
        "writer_output": writer_output,
        "applied_output": applied_output,
        "validation": {"errors": list(validation.errors) + fast_gate_errors, "warnings": validation.warnings},
        "metrics": {
            "chars": len(applied_output["description_md"]),
            "headings": len(re.findall(r"(?m)^###\s+\S", applied_output["description_md"])),
            "bullets": len(re.findall(r"(?m)^\-\s+\S", applied_output["description_md"])),
            "extract_record_count": len(extract_records),
            "merged_record_count": len(merged_records),
            "must_cover_fact_count": writer_profile["must_cover_fact_count"],
        },
        "timings": timing_profile,
        "stage_errors": stage_errors,
    }
