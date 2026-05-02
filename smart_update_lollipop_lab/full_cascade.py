from __future__ import annotations

import asyncio
import re
import sys
import time
from typing import Any, Awaitable, Callable

from . import editorial_layout_family as layout_family
from . import facts_extract_family as extract_family
from . import facts_prioritize_family as prioritize_family
from . import writer_final_4o_family as writer_final_family
from . import writer_pack_compose_family as writer_pack_family


GemmaJsonCaller = Callable[..., Awaitable[dict[str, Any]]]
FourOJsonCaller = Callable[..., Awaitable[dict[str, Any]]]
SleepCaller = Callable[[float], Awaitable[None]]
GEMMA_STAGE_TIMEOUT_S = 180.0


def _bucket_slug(bucket: str) -> str:
    return {
        "event_core": "EC",
        "program_list": "PL",
        "people_and_roles": "PR",
        "forward_looking": "FL",
        "logistics_infoblock": "LG",
        "support_context": "SC",
        "uncertain": "UN",
    }[bucket]


def _source_excerpt(sources: list[Any], *, limit: int = 3200) -> str:
    blocks: list[str] = []
    for source in sources:
        text = re.sub(r"\s+", " ", str(source.text or "")).strip()
        if text:
            blocks.append(f"[{source.source_id}] {source.url}\n{text[:limit]}")
    return "\n\n".join(blocks).strip()


def _source_record_prefix(stage_id: str, source_id: str) -> str:
    base_map = {
        "baseline_fact_extractor.v1": "BF",
        "facts.extract_subject.v1": "SUB",
        "facts.extract_card.v1": "CAR",
        "facts.extract_agenda.v1": "AGE",
        "facts.extract_support.v1": "SUP",
        "facts.extract_profiles.v1": "PRO",
        "facts.extract_performer.v1": "PER",
        "facts.extract_participation.v1": "PAR",
        "facts.extract_stage.tightened.v1": "STG",
        "facts.extract_theme.challenger.v1": "THE",
    }
    base = base_map.get(stage_id, re.sub(r"[^A-Za-z0-9]+", "", stage_id).upper()[:3] or "REC")
    source_slug = re.sub(r"[^A-Za-z0-9]+", "", str(source_id or "")).upper()[:3] or "SRC"
    return f"{base}{source_slug}_"


def _scope_extract_system_prompt(*, gemma4: bool) -> str:
    rules = [
        "You do one small step: source.scope.extract.v1.",
        "Identify which source excerpts describe the same target attendable event.",
        "Flag mixed-phase or cross-promo contamination instead of silently blending it.",
        "Return raw JSON only.",
    ]
    if gemma4:
        rules.extend(
            [
                "Use source-local evidence and keep scope judgments compact.",
                "Do not collapse distinct phases or sibling events into one scope by convenience.",
            ]
        )
    rules.append(
        'Output schema: {"sources":[{"source_id":"string","in_scope":true,"temporal_status":"future|past|mixed|uncertain","notes":"string","evidence":["string"]}]}'
    )
    return "\n".join(rules)


def _scope_select_system_prompt(*, gemma4: bool) -> str:
    rules = [
        "You do one small step: source.scope.select.v1.",
        "Select the source ids that should feed the target event card.",
        "Prefer fail-closed behavior over blending weak or background-only material into the target scope.",
        "Return raw JSON only.",
    ]
    if gemma4:
        rules.extend(
            [
                "Keep future-target vs background-context explicit.",
                "A source marked temporal_status=mixed is not automatically background.",
                "If a mixed source carries unique cast, production, program, or staging detail for the same event, keep it selected.",
                "Move a mixed source to background only when its primary content is genuinely about another event phase or date.",
            ]
        )
    rules.append(
        'Output schema: {"selected_source_ids":["source_id"],"background_source_ids":["source_id"],"mixed_phase":false,"reason":"string"}'
    )
    return "\n".join(rules)


def _dedup_system_prompt(*, gemma4: bool) -> str:
    rules = [
        "You do one small step: facts.dedup.v1.",
        "Classify every record as keep or drop.",
        "Allowed relations: covered, reframe, enrichment, conflict.",
        "Prefer keep/enrichment when a record adds unique named people, repertoire items, program-form detail, scarcity, staging, or attendance-relevant detail.",
        "Do not mark official-source atmosphere, emotional characterisation, or rarity signals as covered by a drier overview, personnel credit, or plain date fact.",
        "If one record describes effect / atmosphere / experience / why the event feels special and another only describes role / attribution / person credit, relation cannot be covered.",
        "Do not mark an explicit repertoire/title list as covered by a broader program summary that loses the titles.",
        "If one record gives a named repertoire/title list and another gives program-form detail like duets, arias, tercets, marches, or songs, keep both when they add distinct grounded program information.",
        "If you must choose a canonical record for near-duplicate program facts, prefer the record that keeps the clearest explicit title list and the richest literal_items.",
        "Drop only true duplicates or empty reframes.",
        "Return raw JSON only.",
    ]
    if gemma4:
        rules.extend(
            [
                "Do not use aggressive collapse that would erase grounded secondary facts.",
                "If one fact says why the event feels rare/special and another only says who participates or what role someone has, that is enrichment, not covered.",
                "Use stage_ids and source_refs as grounding hints: facts.extract_agenda.v1 often carries canonical program evidence and should not be dropped in favour of a blurrier summary.",
            ]
        )
    rules.append(
        'Output schema: {"decisions":[{"record_id":"string","keep":"keep|drop","canonical_record_id":"string","relation":"covered|reframe|enrichment|conflict"}]}'
    )
    return "\n".join(rules)


def _dedup_response_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "decisions": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "record_id": {"type": "STRING"},
                        "keep": {"type": "STRING", "format": "enum", "enum": ["keep", "drop"]},
                        "canonical_record_id": {"type": "STRING"},
                        "relation": {"type": "STRING", "format": "enum", "enum": ["covered", "reframe", "enrichment", "conflict"]},
                    },
                    "required": ["record_id", "keep", "canonical_record_id", "relation"],
                },
            },
        },
        "required": ["decisions"],
    }


def _merge_system_prompt(*, gemma4: bool) -> str:
    rules = [
        "You do one small step: facts.merge.emit.v1.",
        "Map kept records into canonical buckets: event_core, program_list, people_and_roles, forward_looking, support_context, uncertain.",
        "Return raw JSON only.",
        'Start the response immediately with `{` and end with `}`.',
        "No prose. No analysis. No markdown fences. No commentary.",
        "Do not invent bridging prose.",
        "Preserve literal_items when the source gives explicit program items.",
        "When the records contain a named repertoire/title list and a separate forms/performance-units line, preserve both instead of collapsing them into one generic program fact.",
        "A named repertoire/title list should stay as a dedicated program_list fact with verbatim literal_items.",
        "Do not replace an explicit title list with a generic summary like 'хиты лучших оперетт' if the titles themselves are present upstream.",
        "If another kept record says what kind of items make up the program (for example duets, arias, tercets, marches, songs), keep that as a separate narrative-safe fact rather than absorbing it into the title list.",
    ]
    if gemma4:
        rules.extend(
            [
                "Keep provenance-rich secondary facts instead of over-compressing the pack.",
                "Keep official-source atmosphere/event-characterisation and rarity/scarcity as separate support facts when they add meaning beyond a dry summary.",
                "Use stage_ids and source_refs as grounding signals: facts.extract_agenda.v1 usually points to program evidence that should survive as program_list.",
                "Gemma 4 rule: emit one JSON object only and do not explain bucket choices.",
            ]
        )
    rules.append("Prefer compact source_refs over long repeated record_id lists. If record_ids are redundant, you may omit them.")
    rules.append(
        'Output schema: {"event_core":[{"text":"string","literal_items":["string"],"source_refs":["string"]}],"program_list":[],"people_and_roles":[],"forward_looking":[],"support_context":[],"uncertain":[]}'
    )
    return "\n".join(rules)


def _merge_response_schema() -> dict[str, Any]:
    item_schema = {
        "type": "OBJECT",
        "properties": {
            "text": {"type": "STRING"},
            "literal_items": {"type": "ARRAY", "items": {"type": "STRING"}},
            "source_refs": {"type": "ARRAY", "items": {"type": "STRING"}},
            "record_ids": {"type": "ARRAY", "items": {"type": "STRING"}},
        },
        "required": ["text", "literal_items", "source_refs"],
    }
    return {
        "type": "OBJECT",
        "properties": {
            "event_core": {"type": "ARRAY", "items": item_schema},
            "program_list": {"type": "ARRAY", "items": item_schema},
            "people_and_roles": {"type": "ARRAY", "items": item_schema},
            "forward_looking": {"type": "ARRAY", "items": item_schema},
            "support_context": {"type": "ARRAY", "items": item_schema},
            "uncertain": {"type": "ARRAY", "items": item_schema},
        },
        "required": [
            "event_core",
            "program_list",
            "people_and_roles",
            "forward_looking",
            "support_context",
            "uncertain",
        ],
    }


def _prioritize_weight_system_prompt(*, gemma4: bool) -> str:
    rules = [
        "You do one small step: facts.prioritize.weight.v1.",
        "Classify every input fact_id exactly once.",
        "Return raw JSON only.",
        'Start the response immediately with `{` and end with `}`.',
        "No prose. No analysis. No markdown fences. No commentary.",
        "Do not echo or restate the input fact texts.",
        'Use this exact schema: {"facts":[{"fact_id":"EC01","weight":"high|medium|low"}]}.',
        "Weight guide:",
        "- high: event-defining format/action, core repertoire, named repertoire title lists, scarcity/frequency, must-know attendance-relevant context.",
        "- medium: named people with explicit roles, supporting repertoire detail, program-forms detail like duets/arias/marches, strong grounded context, official atmosphere/event-characterisation that shapes attendance expectations.",
        "- low: secondary credits, tertiary flavor, weak support detail.",
        "Do not drop fact_ids. Do not invent fact_ids.",
        "If unsure, still return the best JSON classification.",
    ]
    if gemma4:
        rules.extend(
            [
                "Gemma 4 rule: never explain your reasoning outside the JSON object.",
                "Treat scarcity/frequency signals like 'раз в сезоне', 'два вечера подряд', and 'последний показ' as attendance-relevant, not flavour-only.",
            ]
        )
    return "\n".join(rules)


def _prioritize_lead_system_prompt(*, gemma4: bool) -> str:
    rules = [
        "You do one small step: facts.prioritize.lead.v1.",
        "Choose exactly one lead_fact_id and optionally one lead_support_id.",
        "Return raw JSON only.",
        'Start the response immediately with `{` and end with `}`.',
        "No prose. No analysis. No markdown fences. No commentary.",
        'Use this exact schema: {"lead_fact_id":"EC01","lead_support_id":"PR01|null"}.',
        "lead_fact_id must come from the input facts list.",
        "lead_support_id must be null or one fact_id from the input facts list.",
        "Lead must explain what attendable event this is, not only who participates.",
        "Lead support selection priority: grounded rarity/scarcity first, grounded atmosphere/emotional characterisation second, distinguishing format/program/action fact third, secondary role credit only if stronger hooks are absent.",
        "When a grounded rarity or atmosphere fact makes the attendance value clearer, prefer it as lead_support over a second dry summary fact.",
        "Reserve list-heavy program/repertoire title lists with literal_items for a downstream program block when a grounded non-list support fact exists.",
        "For concert and list-heavy cultural cases, prefer a hook that sounds like an announcement of the evening, not a catalog preamble.",
        "Do not default to lead choices that would force a dry opening like `событие посвящено ...`, if rarity, atmosphere, or attendance-shaping support can ground a stronger lead.",
        "Reject biography-only, cast-only, project-definition-only, and role-credit openings when a more event-facing anchor exists.",
        "For opaque titles, prefer a fact that clarifies the format or main event action.",
    ]
    if gemma4:
        rules.append("Gemma 4 rule: do not narrate the choice, only return the JSON object.")
    return "\n".join(rules)


def _editorial_layout_system_prompt(*, gemma4: bool) -> str:
    rules = [
        "You do one small step: editorial.layout.plan.v1.",
        "Plan block structure only.",
        "Return raw JSON only.",
        'Start the response immediately with `{` and end with `}`.',
        "No prose. No analysis. No markdown fences. No commentary.",
        'Use this schema: {"title_strategy":"keep|enhance","title_hint_ref":"fact_id|null","blocks":[{"role":"lead|body|program|infoblock","fact_refs":["fact_id"],"style":"narrative|list|structured","heading":"string|null"}]}.',
        "Use all_fact_ids exactly once across blocks.",
        "Lead stays first. Infoblock stays last.",
        "Program facts should prefer a separate program block.",
        "If the pack contains both a named repertoire/title list and a separate program-format/program-forms fact, keep them in separate adjacent blocks instead of flattening them into one list block.",
        "Prefer the title list inside the program block; keep the program-format/program-forms fact in a nearby narrative block so both survive.",
        "If lead_support points to a list-heavy program fact with literal_items, detach it from the lead and reserve it for the program block.",
        "If a narrative block mixes vivid atmosphere/rarity facts with generic ensemble-category or service-note facts, do not make the generic fact the opening beat of that block.",
        "Prefer to keep atmosphere/rarity in an earlier narrative block and move ensemble-category detail closer to people/performers when possible.",
        "Do not strand a rarity hook in the final weak tail behind admin/service material if it can support the lead or first narrative block.",
        "If body_block_floor = 2, keep at least two post-lead narrative sections unless the program block already covers one cluster.",
        "If a people-heavy block is dense, structured style is allowed.",
        "When a people block is only ensemble categories rather than a true named cast list, keep it narrative instead of list-like.",
        "When support facts carry rarity or official atmosphere, place them in the lead or first narrative body block instead of stranding them as a weak tail note.",
        "If a narrative block mixes a vivid hook with a generic ensemble-category line, the vivid hook should open and the ensemble detail should follow.",
        "Avoid generic filler headings.",
    ]
    if gemma4:
        rules.append("Gemma 4 rule: do not describe the plan in prose, only emit the JSON object.")
    return "\n".join(rules)


def _prioritize_weight_response_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "facts": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "fact_id": {"type": "STRING"},
                        "weight": {"type": "STRING", "format": "enum", "enum": ["high", "medium", "low"]},
                    },
                    "required": ["fact_id", "weight"],
                },
            },
        },
        "required": ["facts"],
    }


def _prioritize_lead_response_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "lead_fact_id": {"type": "STRING"},
            "lead_support_id": {"type": "STRING", "nullable": True},
        },
        "required": ["lead_fact_id"],
    }


def _editorial_layout_response_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "title_strategy": {"type": "STRING", "format": "enum", "enum": ["keep", "enhance"]},
            "title_hint_ref": {"type": "STRING", "nullable": True},
            "blocks": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "role": {"type": "STRING", "format": "enum", "enum": ["lead", "body", "program", "infoblock"]},
                        "fact_refs": {"type": "ARRAY", "items": {"type": "STRING"}},
                        "style": {"type": "STRING", "format": "enum", "enum": ["narrative", "list", "structured"]},
                        "heading": {"type": "STRING", "nullable": True},
                    },
                    "required": ["role", "fact_refs", "style"],
                },
            },
        },
        "required": ["title_strategy", "blocks"],
    }


def _extract_stage_response_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "facts": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "text": {"type": "STRING"},
                        "evidence": {"type": "STRING"},
                        "source_refs": {"type": "ARRAY", "items": {"type": "STRING"}},
                        "literal_items": {"type": "ARRAY", "items": {"type": "STRING"}},
                        "strength": {"type": "STRING", "format": "enum", "enum": ["high", "medium", "low"]},
                    },
                    "required": ["text", "evidence", "source_refs", "literal_items", "strength"],
                },
            },
        },
        "required": ["facts"],
    }


def _extract_stage_system_prompt(stage: dict[str, Any], *, gemma4: bool) -> str:
    stage_id = str(stage["stage_id"])
    rules = [
        f"You do one small step: {stage_id}.",
        stage["focus"],
        "Return only JSON.",
        'Start the response immediately with `{` and end with `}`.',
        "Do not write public prose.",
        "Keep facts compact and literal.",
        "Prefer source-shaped natural Russian over abstract report formulas when both preserve the same fact.",
        "SOURCE-LOCAL UNIQUENESS OBLIGATION: this call sees one source block that may carry facts no other source carries.",
        "If the source block carries a unique grounded fact, keep that fact instead of repeating only the safest cross-source summary.",
        "When this source contributes a meaningful atmosphere, rarity, or staging hook, emit that source-local fact as a first-class record rather than collapsing it into a generic overview.",
        "Do not add logistics here.",
        "All fact texts must stay in Russian. Never translate roles into English.",
        "LITERAL TITLE FIDELITY: when extracting names of works, preserve exact source spelling with no declension, abbreviation, or normalization.",
        "FACT TEXT VOICE: preserve event-facing action wording from the source like `звучат`, `собраны`, `идет`, `показывают`; do not auto-rewrite into `посвящен`, `характеризуется`, `наполнена`, or `представлены`.",
        'If you are unsure, return {"facts": []} instead of an explanation.',
    ]
    if gemma4:
        rules.extend(
            [
                "Use raw JSON only, no markdown fences.",
                "If a meaningful source fact does not fit this stage, omit it instead of paraphrasing it into another type.",
            ]
        )
    if stage_id in {"facts.extract_subject.v1", "facts.extract_card.v1"}:
        rules.append("If the source excerpt is short promotional text, still extract the strongest stage-local fact you can from the available evidence.")
        rules.append("Prefer event-facing action wording like `звучат`, `собраны`, `идет`, `показывают`, if grounded, instead of flattening everything into `посвящен`.")
    if stage_id == "facts.extract_support.v1":
        rules.extend(
            [
                "Keep attendance-relevant notices like stage smoke or format constraints when explicitly stated.",
                "Keep grounded rarity/frequency/anticipation facts like `редкий гость в афише`, `долгожданный`, `раз в сезоне`, `два вечера подряд`.",
                "Do not discard official-source rarity lines just because the wording is promotional.",
                "Rarity/scarcity/anticipation signals are first-class attendance facts, not promo filler.",
            ]
        )
    if stage_id in {"baseline_fact_extractor.v1", "facts.extract_theme.challenger.v1"}:
        rules.extend(
            [
                "Official-source atmosphere or emotional framing can survive when it characterises the event experience rather than generic praise.",
                "Examples that may count when grounded: `романтические истории`, `любовь, надежда, одиночество, радость, удивление`, `легкая, волшебная музыка`.",
                "Preserve mood/context facts close to source wording when possible: `романтические истории из оперетт Кальмана`, `легкая волшебная музыка`, `атмосфера интриги и игры` are better than dry rewrites with `характеризуется`, `представлены`, or `наполнена`.",
                "Official organiser atmosphere lines are first-class event-characterisation, not promo filler, when they describe what this evening feels like.",
                "Do not import audience reactions or third-party reviews.",
            ]
        )
    if stage_id in {"facts.extract_performer.v1", "facts.extract_participation.v1"}:
        rules.append("Preserve full named lists when roles are explicit; do not truncate to one or two names.")
    if stage_id == "facts.extract_participation.v1":
        rules.append("Collective formations like 'солисты, оркестр, хор и балет театра' count as participation even without individual names.")
    if stage_id == "facts.extract_agenda.v1":
        rules.append("Preserve literal repertoire/program items exactly as written when available.")
    rules.append("literal_items must contain only verbatim program or repertoire items from an explicit source list.")
    rules.append("Do not put event title, dates, person names, venue names, scarcity markers, or ensemble categories into literal_items.")
    rules.append(
        'Output schema: {"facts":[{"text":"string","evidence":"string","source_refs":["source_id"],"literal_items":["string"],"strength":"high|medium|low"}]}'
    )
    return "\n".join(rules)


def _normalize_scope_extract(payload: dict[str, Any], source_ids: list[str]) -> dict[str, Any]:
    rows = []
    seen: set[str] = set()
    for item in list(payload.get("sources") or []):
        if not isinstance(item, dict):
            continue
        source_id = str(item.get("source_id") or "").strip()
        if source_id not in source_ids or source_id in seen:
            continue
        seen.add(source_id)
        rows.append(
            {
                "source_id": source_id,
                "in_scope": bool(item.get("in_scope", True)),
                "temporal_status": str(item.get("temporal_status") or "future").strip() or "future",
                "notes": str(item.get("notes") or "").strip(),
                "evidence": [str(x).strip() for x in list(item.get("evidence") or []) if str(x).strip()],
            }
        )
    for source_id in source_ids:
        if source_id not in seen:
            rows.append({"source_id": source_id, "in_scope": True, "temporal_status": "future", "notes": "", "evidence": []})
    return {"sources": rows}


def _normalize_scope_select(payload: dict[str, Any], source_ids: list[str]) -> dict[str, Any]:
    selected = [str(item).strip() for item in list(payload.get("selected_source_ids") or []) if str(item).strip() in source_ids]
    if not selected:
        selected = list(source_ids)
    background = [str(item).strip() for item in list(payload.get("background_source_ids") or []) if str(item).strip() in source_ids and str(item).strip() not in selected]
    return {
        "selected_source_ids": selected,
        "background_source_ids": background,
        "mixed_phase": bool(payload.get("mixed_phase", False)),
        "reason": str(payload.get("reason") or "").strip(),
    }


def _normalize_dedup(payload: dict[str, Any], records: list[dict[str, Any]]) -> dict[str, Any]:
    record_ids = {item["record_id"] for item in records}
    decision_map: dict[str, dict[str, Any]] = {}
    for item in list(payload.get("decisions") or []):
        if not isinstance(item, dict):
            continue
        record_id = str(item.get("record_id") or "").strip()
        if record_id not in record_ids:
            continue
        decision_map[record_id] = {
            "record_id": record_id,
            "keep": str(item.get("keep") or "keep").strip() or "keep",
            "canonical_record_id": str(item.get("canonical_record_id") or record_id).strip() or record_id,
            "relation": str(item.get("relation") or "enrichment").strip() or "enrichment",
        }
    decisions = [decision_map.get(item["record_id"], {"record_id": item["record_id"], "keep": "keep", "canonical_record_id": item["record_id"], "relation": "enrichment"}) for item in records]
    return {"decisions": decisions}


def _kept_records(records: list[dict[str, Any]], dedup_result: dict[str, Any]) -> list[dict[str, Any]]:
    drop_ids = {
        item["record_id"]
        for item in list(dedup_result.get("decisions") or [])
        if str(item.get("keep") or "").strip() == "drop"
    }
    return [item for item in records if item["record_id"] not in drop_ids]


def _compact_records_for_dedup(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    compacted: list[dict[str, Any]] = []
    key_to_index: dict[tuple[str, str, tuple[str, ...]], int] = {}
    merged_count = 0
    for record in records:
        key = (
            str(record.get("bucket_hint") or ""),
            str(record.get("text") or "").strip(),
            tuple(sorted(str(item).strip() for item in list(record.get("literal_items") or []) if str(item).strip())),
        )
        existing_index = key_to_index.get(key)
        if existing_index is None:
            compacted.append(
                {
                    **record,
                    "source_refs": list(dict.fromkeys(record.get("source_refs") or [])),
                    "stage_ids": [record["stage_id"]],
                }
            )
            key_to_index[key] = len(compacted) - 1
            continue
        existing = compacted[existing_index]
        merged_count += 1
        existing["source_refs"] = list(dict.fromkeys([*(existing.get("source_refs") or []), *(record.get("source_refs") or [])]))
        existing["stage_ids"] = list(dict.fromkeys([*(existing.get("stage_ids") or []), record["stage_id"]]))
    return compacted, {"merged_exact_duplicates": merged_count}


def _normalize_merge_pack(payload: dict[str, Any], fixture: Any) -> dict[str, Any]:
    pack: dict[str, Any] = {}
    for bucket in ("event_core", "program_list", "people_and_roles", "forward_looking", "support_context", "uncertain"):
        items: list[dict[str, Any]] = []
        counter = 1
        for raw in list(payload.get(bucket) or []):
            if not isinstance(raw, dict):
                continue
            text = re.sub(r"\s+", " ", str(raw.get("text") or "")).strip()
            if not text:
                continue
            items.append(
                {
                    "fact_id": f"{_bucket_slug(bucket)}{counter:02d}",
                    "bucket": bucket,
                    "text": text,
                    "literal_items": [
                        re.sub(r"\s+", " ", str(item or "")).strip()
                        for item in list(raw.get("literal_items") or [])
                        if re.sub(r"\s+", " ", str(item or "")).strip()
                    ],
                    "record_ids": [str(item).strip() for item in list(raw.get("record_ids") or []) if str(item).strip()],
                    "source_refs": [str(item).strip() for item in list(raw.get("source_refs") or []) if str(item).strip()],
                }
            )
            counter += 1
        pack[bucket] = items

    logistics: list[dict[str, Any]] = []
    if fixture.date:
        logistics.append({"fact_id": "LG01", "bucket": "logistics_infoblock", "text": fixture.date, "literal_items": [], "source_refs": [], "record_ids": []})
    if fixture.time:
        logistics.append({"fact_id": "LG02", "bucket": "logistics_infoblock", "text": fixture.time, "literal_items": [], "source_refs": [], "record_ids": []})
    if fixture.location_name:
        logistics.append({"fact_id": "LG03", "bucket": "logistics_infoblock", "text": fixture.location_name, "literal_items": [], "source_refs": [], "record_ids": []})
    if fixture.location_address:
        logistics.append({"fact_id": "LG04", "bucket": "logistics_infoblock", "text": fixture.location_address, "literal_items": [], "source_refs": [], "record_ids": []})
    pack["logistics_infoblock"] = logistics
    return pack


def _apply_weight_payload(fact_pack: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    weight_map = {
        str(item.get("fact_id") or "").strip(): str(item.get("weight") or "medium").strip() or "medium"
        for item in list(payload.get("facts") or [])
        if isinstance(item, dict)
    }
    weighted: dict[str, Any] = {}
    for bucket, items in fact_pack.items():
        if not isinstance(items, list):
            continue
        weighted[bucket] = []
        for item in items:
            fact_id = item["fact_id"]
            default_weight = "high" if fact_id.startswith("LG") else "medium"
            weighted[bucket].append({**item, "weight": weight_map.get(fact_id, default_weight), "weight_reasoning": ""})
    return weighted


def _merge_prompt_payload_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "record_id": item["record_id"],
            "stage_id": item["stage_id"],
            "stage_ids": item.get("stage_ids") or ([item["stage_id"]] if item.get("stage_id") else []),
            "bucket_hint": item["bucket_hint"],
            "text": item["text"],
            "literal_items": item.get("literal_items") or [],
            "source_refs": item.get("source_refs") or [],
        }
        for item in records
    ]


def _dedup_prompt_payload_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "record_id": item["record_id"],
            "bucket_hint": item["bucket_hint"],
            "text": item["text"],
            "literal_items": item.get("literal_items") or [],
            "source_refs": item.get("source_refs") or [],
            "stage_ids": item.get("stage_ids") or ([item["stage_id"]] if item.get("stage_id") else []),
        }
        for item in records
    ]


def _dedup_record_chunks(records: list[dict[str, Any]], *, chunk_size: int = 16) -> list[list[dict[str, Any]]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    return [records[idx : idx + chunk_size] for idx in range(0, len(records), chunk_size)]


def _stage_family_label(stage_label: str) -> str:
    label = re.sub(r"\[[^\]]+\]$", "", str(stage_label or "").strip())
    if label.startswith("facts.dedup.chunk") or label == "facts.dedup.final":
        return "facts.dedup"
    if label.startswith("writer.final_4o."):
        return "writer.final_4o"
    return label


def _new_timing_profile() -> dict[str, Any]:
    return {
        "wall_clock_sec": 0.0,
        "model_active_sec": 0.0,
        "sleep_sec": 0.0,
        "gemma_calls": 0,
        "four_o_calls": 0,
        "stage_sec": {},
        "stage_family_sec": {},
    }


def _record_stage_timing(profile: dict[str, Any], stage_label: str, duration_sec: float, *, kind: str) -> None:
    duration = round(max(0.0, float(duration_sec)), 6)
    stage_sec = dict(profile.get("stage_sec") or {})
    stage_sec[stage_label] = round(float(stage_sec.get(stage_label) or 0.0) + duration, 6)
    profile["stage_sec"] = stage_sec
    family = _stage_family_label(stage_label)
    family_sec = dict(profile.get("stage_family_sec") or {})
    family_sec[family] = round(float(family_sec.get(family) or 0.0) + duration, 6)
    profile["stage_family_sec"] = family_sec
    if kind == "sleep":
        profile["sleep_sec"] = round(float(profile.get("sleep_sec") or 0.0) + duration, 6)
    else:
        profile["model_active_sec"] = round(float(profile.get("model_active_sec") or 0.0) + duration, 6)
        if kind == "gemma":
            profile["gemma_calls"] = int(profile.get("gemma_calls") or 0) + 1
        elif kind == "four_o":
            profile["four_o_calls"] = int(profile.get("four_o_calls") or 0) + 1


async def _gemma_json_call_with_timeout(
    *,
    gemma_json_call: GemmaJsonCaller,
    stage_errors: list[str],
    stage_label: str,
    timeout_s: float = GEMMA_STAGE_TIMEOUT_S,
    **kwargs: Any,
) -> dict[str, Any]:
    try:
        return await asyncio.wait_for(gemma_json_call(**kwargs), timeout=timeout_s)
    except Exception as exc:
        stage_errors.append(f"{stage_label}:{exc}")
        return {}


async def _run_dedup_pass(
    *,
    fixture: Any,
    records: list[dict[str, Any]],
    gemma_model: str,
    gemma4: bool,
    gemma_json_call: GemmaJsonCaller,
    stage_errors: list[str],
    stage_label: str,
    log: Callable[[str], None] | None = None,
    timing_profile: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if log is not None:
        log(stage_label)
    if not records:
        empty = {"decisions": []}
        return empty, empty
    call_started = time.perf_counter()
    try:
        raw = await _gemma_json_call_with_timeout(
            gemma_json_call=gemma_json_call,
            stage_errors=stage_errors,
            stage_label=stage_label,
            model=gemma_model,
            system_prompt=_dedup_system_prompt(gemma4=gemma4),
            user_payload={
                "event_title": fixture.title,
                "event_type": fixture.event_type,
                "records": _dedup_prompt_payload_records(records),
            },
            max_tokens=max(900, min(1800, 600 + len(records) * 32)),
            response_schema=_dedup_response_schema() if gemma4 else None,
        ) or {"decisions": []}
    except Exception:
        raw = {"decisions": []}
    if timing_profile is not None:
        _record_stage_timing(timing_profile, stage_label, time.perf_counter() - call_started, kind="gemma")
    return raw, _normalize_dedup(raw, records)


async def _run_chunked_dedup(
    *,
    fixture: Any,
    records: list[dict[str, Any]],
    gemma_model: str,
    gemma4: bool,
    gemma_json_call: GemmaJsonCaller,
    stage_errors: list[str],
    log: Callable[[str], None] | None = None,
    chunk_size: int = 16,
    timing_profile: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if len(records) <= chunk_size:
        return await _run_dedup_pass(
            fixture=fixture,
            records=records,
            gemma_model=gemma_model,
            gemma4=gemma4,
            gemma_json_call=gemma_json_call,
            stage_errors=stage_errors,
            stage_label="facts.dedup",
            log=log,
            timing_profile=timing_profile,
        )

    chunk_raws: list[dict[str, Any]] = []
    chunk_decisions: list[dict[str, Any]] = []
    for idx, chunk in enumerate(_dedup_record_chunks(records, chunk_size=chunk_size), start=1):
        raw, normalized = await _run_dedup_pass(
            fixture=fixture,
            records=chunk,
            gemma_model=gemma_model,
            gemma4=gemma4,
            gemma_json_call=gemma_json_call,
            stage_errors=stage_errors,
            stage_label=f"facts.dedup.chunk{idx:02d}",
            log=log,
            timing_profile=timing_profile,
        )
        chunk_raws.append(raw)
        chunk_decisions.extend(list(normalized.get("decisions") or []))

    chunk_result = {"decisions": chunk_decisions}
    kept_after_chunks = _kept_records(records, chunk_result)
    final_raw, final_normalized = await _run_dedup_pass(
        fixture=fixture,
        records=kept_after_chunks,
        gemma_model=gemma_model,
        gemma4=gemma4,
        gemma_json_call=gemma_json_call,
        stage_errors=stage_errors,
        stage_label="facts.dedup.final",
        log=log,
        timing_profile=timing_profile,
    )
    final_map = {
        str(item.get("record_id") or "").strip(): item
        for item in list(final_normalized.get("decisions") or [])
        if str(item.get("record_id") or "").strip()
    }
    chunk_map = {
        str(item.get("record_id") or "").strip(): item
        for item in list(chunk_decisions)
        if str(item.get("record_id") or "").strip()
    }
    combined_decisions: list[dict[str, Any]] = []
    for record in records:
        record_id = record["record_id"]
        chunk_decision = chunk_map.get(record_id)
        if chunk_decision and str(chunk_decision.get("keep") or "").strip() == "drop":
            combined_decisions.append(chunk_decision)
            continue
        combined_decisions.append(
            final_map.get(
                record_id,
                {
                    "record_id": record_id,
                    "keep": "keep",
                    "canonical_record_id": record_id,
                    "relation": "enrichment",
                },
            )
        )
    raw = {"mode": "chunked", "chunk_size": chunk_size, "chunk_passes": chunk_raws, "final_pass": final_raw}
    return raw, {"decisions": combined_decisions}


async def run_full_cascade_variant(
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
        print(f"[full-cascade] {gemma_model} {fixture.fixture_id} {stage}", file=sys.stderr, flush=True)
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
    source_ids = [source.source_id for source in fixture.sources]
    source_payload = [
        {
            "source_id": source.source_id,
            "source_type": source.source_type,
            "source_url": source.url,
            "excerpt": re.sub(r"\s+", " ", str(source.text or "")).strip()[:3200],
        }
        for source in fixture.sources
    ]
    _log("source.scope.extract")
    scope_extract_raw = await _timed_gemma(
        "source.scope.extract",
        model=gemma_model,
        system_prompt=_scope_extract_system_prompt(gemma4=gemma4),
        user_payload={"event_title": fixture.title, "event_type": fixture.event_type, "sources": source_payload},
        max_tokens=1200,
    ) or {"sources": []}
    await _timed_sleep("sleep.gemma_gap", gemma_call_gap_s)
    scope_extract = _normalize_scope_extract(scope_extract_raw, source_ids)
    _log("source.scope.select")
    scope_select_raw = await _timed_gemma(
        "source.scope.select",
        model=gemma_model,
        system_prompt=_scope_select_system_prompt(gemma4=gemma4),
        user_payload={"event_title": fixture.title, "event_type": fixture.event_type, "scope_map": scope_extract},
        max_tokens=700,
    ) or {"selected_source_ids": source_ids, "background_source_ids": [], "mixed_phase": False, "reason": ""}
    scope_select = _normalize_scope_select(scope_select_raw, source_ids)
    selected_sources = [source for source in fixture.sources if source.source_id in scope_select["selected_source_ids"]]
    await _timed_sleep("sleep.gemma_gap", gemma_call_gap_s)

    baseline_stage = extract_family.STAGE_SPECS[0]
    baseline_payload_rows: list[dict[str, Any]] = []
    baseline_records: list[dict[str, Any]] = []
    raw_facts_by_source: dict[str, list[str]] = {}
    for source in selected_sources:
        _log(f"facts.extract.baseline[{source.source_id}]")
        source_payload = await _timed_gemma(
            f"{baseline_stage['stage_id']}[{source.source_id}]",
            model=gemma_model,
            system_prompt=_extract_stage_system_prompt(baseline_stage, gemma4=gemma4),
            user_payload={
                "event_title": fixture.title,
                "event_type": fixture.event_type,
                "source_id": source.source_id,
                "source_type": source.source_type,
                "source_local": True,
                "source_excerpt": _source_excerpt([source], limit=2600),
                "raw_facts": [],
            },
            max_tokens=2200,
            response_schema=_extract_stage_response_schema() if gemma4 else None,
        ) or {"facts": []}
        source_records = extract_family.normalize_stage_items(
            stage=baseline_stage,
            payload=source_payload,
            record_prefix=_source_record_prefix(baseline_stage["stage_id"], source.source_id),
        )
        baseline_payload_rows.append(
            {
                "source_id": source.source_id,
                "source_type": source.source_type,
                "payload": source_payload,
            }
        )
        baseline_records.extend(source_records)
        raw_facts_by_source[source.source_id] = [item["text"] for item in source_records]
        await _timed_sleep("sleep.gemma_gap", gemma_call_gap_s)
    baseline_payload = {"per_source": baseline_payload_rows}
    raw_facts = [item["text"] for item in baseline_records]

    extract_runs: list[dict[str, Any]] = []
    extract_records: list[dict[str, Any]] = list(baseline_records)
    for stage in extract_family.STAGE_SPECS[1:]:
        for source in selected_sources:
            _log(f"{stage['stage_id']}[{source.source_id}]")
            payload = await _timed_gemma(
                f"{stage['stage_id']}[{source.source_id}]",
                model=gemma_model,
                system_prompt=_extract_stage_system_prompt(stage, gemma4=gemma4),
                user_payload={
                    "event_title": fixture.title,
                    "event_type": fixture.event_type,
                    "source_id": source.source_id,
                    "source_type": source.source_type,
                    "source_local": True,
                    "source_excerpt": _source_excerpt([source], limit=2600),
                    "raw_facts": raw_facts_by_source.get(source.source_id) or [],
                },
                max_tokens=2200,
                response_schema=_extract_stage_response_schema() if gemma4 else None,
            ) or {"facts": []}
            records = extract_family.normalize_stage_items(
                stage=stage,
                payload=payload,
                record_prefix=_source_record_prefix(stage["stage_id"], source.source_id),
            )
            extract_runs.append(
                {
                    "stage_id": stage["stage_id"],
                    "source_id": source.source_id,
                    "source_type": source.source_type,
                    "payload": payload,
                    "records": records,
                }
            )
            extract_records.extend(records)
            await _timed_sleep("sleep.gemma_gap", gemma_call_gap_s)

    dedup_input_records, dedup_compaction = _compact_records_for_dedup(extract_records)
    dedup_raw, dedup = await _run_chunked_dedup(
        fixture=fixture,
        records=dedup_input_records,
        gemma_model=gemma_model,
        gemma4=gemma4,
        gemma_json_call=gemma_json_call,
        stage_errors=stage_errors,
        log=_log,
        timing_profile=timing_profile,
    )
    kept = _kept_records(dedup_input_records, dedup)
    await _timed_sleep("sleep.gemma_gap", gemma_call_gap_s)

    _log("facts.merge")
    merge_raw = await _timed_gemma(
        "facts.merge",
        model=gemma_model,
        system_prompt=_merge_system_prompt(gemma4=gemma4),
        user_payload={"event_title": fixture.title, "event_type": fixture.event_type, "records": _merge_prompt_payload_records(kept)},
        max_tokens=3600,
        response_schema=_merge_response_schema() if gemma4 else None,
    ) or {}
    fact_pack = _normalize_merge_pack(merge_raw, fixture)
    await _timed_sleep("sleep.gemma_gap", gemma_call_gap_s)

    augmented_pack = prioritize_family._augment_fact_pack_from_raw_facts(
        fact_pack,
        event_type=fixture.event_type,
        raw_facts=raw_facts,
    )
    flat_weight_facts = [
        {
            "fact_id": item["fact_id"],
            "bucket": item["bucket"],
            "text": item["text"],
            "literal_items": item.get("literal_items") or [],
        }
        for item in prioritize_family._flat_facts(augmented_pack)
    ]
    _log("facts.prioritize.weight")
    weight_raw = await _timed_gemma(
        "facts.prioritize.weight",
        model=gemma_model,
        system_prompt=_prioritize_weight_system_prompt(gemma4=gemma4),
        user_payload={"event_title": fixture.title, "event_type": fixture.event_type, "facts": flat_weight_facts},
        max_tokens=1400,
        response_schema=_prioritize_weight_response_schema(),
    ) or {}
    weighted_pack = _apply_weight_payload(augmented_pack, weight_raw)
    weighted_pack = prioritize_family._apply_narrative_policies(weighted_pack, event_type=fixture.event_type)
    await _timed_sleep("sleep.gemma_gap", gemma_call_gap_s)

    flat_lead_facts = [
        {
            "fact_id": item["fact_id"],
            "bucket": item["bucket"],
            "weight": item.get("weight"),
            "text": item["text"],
        }
        for item in prioritize_family._flat_facts(weighted_pack)
        if item.get("narrative_policy") != "suppress"
    ]
    _log("facts.prioritize.lead")
    lead_raw = await _timed_gemma(
        "facts.prioritize.lead",
        model=gemma_model,
        system_prompt=_prioritize_lead_system_prompt(gemma4=gemma4),
        user_payload={
            "event_id": fixture.fixture_id,
            "title": fixture.title,
            "event_type": fixture.event_type,
            "title_is_bare": prioritize_family._title_is_bare(fixture.title, fixture.event_type),
            "title_needs_format_anchor": prioritize_family._title_needs_format_anchor(fixture.title, fixture.event_type),
            "facts": flat_lead_facts,
        },
        max_tokens=600,
        response_schema=_prioritize_lead_response_schema(),
    ) or {}
    lead_payload = prioritize_family._clean_lead(lead_raw, weighted_pack, title=fixture.title, event_type=fixture.event_type)
    lead_payload["event_title"] = fixture.title
    await _timed_sleep("sleep.gemma_gap", gemma_call_gap_s)

    prioritized_pack = layout_family._prioritized_fact_pack(weighted_pack)
    precompute = layout_family._precompute_layout_state(event_type=fixture.event_type, pack=prioritized_pack, lead_payload=lead_payload)
    _log("editorial.layout")
    layout_raw = await _timed_gemma(
        "editorial.layout",
        model=gemma_model,
        system_prompt=_editorial_layout_system_prompt(gemma4=gemma4),
        user_payload={
            "event_title": fixture.title,
            "event_type": fixture.event_type,
            "lead_payload": lead_payload,
            "precompute": precompute,
            "fact_pack": prioritized_pack,
        },
        max_tokens=1400,
        response_schema=_editorial_layout_response_schema(),
    ) or {}
    layout_payload = layout_family._clean_layout_plan(
        layout_raw,
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

    writer_schema = {
        "type": "object",
        "properties": {"title": {"type": "string"}, "description_md": {"type": "string"}},
        "required": ["title", "description_md"],
        "additionalProperties": False,
    }
    pack_payload = writer_pack["payload"]
    _log("writer.final_4o")
    writer_output: dict[str, Any] = {}
    validation = writer_final_family.ValidationResult(errors=["writer.final_4o.not_run"], warnings=[])
    try:
        writer_output = await _timed_four_o(
            "writer.final_4o.initial",
            prompt=writer_final_family._build_prompt(pack_payload),
            schema=writer_schema,
            model=four_o_model,
        )
        validation = writer_final_family._validate_writer_output(pack_payload, writer_output)
        if validation.errors:
            writer_output = await _timed_four_o(
                "writer.final_4o.retry",
                prompt=writer_final_family._build_retry_prompt(pack_payload, validation),
                schema=writer_schema,
                model=four_o_model,
            )
            validation = writer_final_family._validate_writer_output(pack_payload, writer_output)
    except Exception as exc:
        stage_errors.append(f"writer.final_4o.error:{type(exc).__name__}:{str(exc)[:240]}")
        writer_output = {"title": fixture.title, "description_md": ""}
        validation = writer_final_family.ValidationResult(
            errors=[f"writer.final_4o.error:{type(exc).__name__}"],
            warnings=[],
        )
    applied_output = writer_final_family._apply_writer_output(pack_payload, writer_output)
    timing_profile["wall_clock_sec"] = round(time.perf_counter() - started_at, 6)

    return {
        "gemma_model": gemma_model,
        "scope_extract_raw": scope_extract_raw,
        "scope_extract": scope_extract,
        "scope_select_raw": scope_select_raw,
        "scope_select": scope_select,
        "baseline_extract_payload": baseline_payload,
        "baseline_extract_records": baseline_records,
        "extract_runs": extract_runs,
        "extract_records": extract_records,
        "dedup_input_records": dedup_input_records,
        "dedup_compaction": dedup_compaction,
        "dedup_raw": dedup_raw,
        "dedup": dedup,
        "merge_raw": merge_raw,
        "fact_pack": fact_pack,
        "weight_raw": weight_raw,
        "weight_result": {"payload": weighted_pack},
        "lead_raw": lead_raw,
        "lead_payload": lead_payload,
        "layout_precompute": precompute,
        "layout_payload_raw": layout_raw,
        "layout_payload": layout_payload,
        "writer_pack": pack_payload,
        "writer_output": writer_output,
        "applied_output": applied_output,
        "validation": {"errors": validation.errors, "warnings": validation.warnings},
        "metrics": {
            "chars": len(applied_output["description_md"]),
            "headings": len(re.findall(r"(?m)^###\s+\S", applied_output["description_md"])),
            "bullets": len(re.findall(r"(?m)^\-\s+\S", applied_output["description_md"])),
        },
        "timings": timing_profile,
        "stage_errors": stage_errors,
    }
