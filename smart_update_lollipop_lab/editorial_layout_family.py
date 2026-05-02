from __future__ import annotations

import copy
import re
import textwrap
from typing import Any

from .facts_prioritize_family import _event_type_label, _title_is_bare, _title_needs_format_anchor


def _prioritized_fact_pack(weighted_pack: dict[str, Any]) -> dict[str, Any]:
    filtered: dict[str, Any] = {}
    for bucket, values in weighted_pack.items():
        if not isinstance(values, list):
            continue
        filtered[bucket] = [item for item in values if item.get("narrative_policy") != "suppress"]
    return filtered


def _precompute_layout_state(*, event_type: str, pack: dict[str, Any], lead_payload: dict[str, Any]) -> dict[str, Any]:
    non_logistics_total = sum(len(list(pack.get(bucket) or [])) for bucket in ("event_core", "program_list", "people_and_roles", "forward_looking", "support_context"))
    body_cluster_count = sum(1 for bucket in ("event_core", "people_and_roles", "forward_looking", "support_context") if list(pack.get(bucket) or []))
    title = str(lead_payload.get("event_title") or "")
    title_is_bare = _title_is_bare(title, event_type)
    title_needs_format_anchor = _title_needs_format_anchor(title, event_type)
    allow_semantic_headings = title_needs_format_anchor or non_logistics_total >= 4
    return {
        "density": "rich" if non_logistics_total >= 6 else "standard" if non_logistics_total >= 4 else "minimal",
        "has_long_program": any(item.get("literal_items") for item in list(pack.get("program_list") or [])),
        "non_logistics_total": non_logistics_total,
        "body_cluster_count": body_cluster_count,
        "body_block_floor": 2 if body_cluster_count >= 2 and non_logistics_total >= 5 else 1,
        "multi_body_split_recommended": body_cluster_count >= 2 and non_logistics_total >= 5,
        "title_is_bare": title_is_bare,
        "title_needs_format_anchor": title_needs_format_anchor,
        "allow_semantic_headings": allow_semantic_headings,
        "heading_guardrail_recommended": non_logistics_total >= 5,
        "all_fact_ids": [
            item["fact_id"]
            for bucket in ("event_core", "program_list", "people_and_roles", "forward_looking", "support_context", "logistics_infoblock")
            for item in list(pack.get(bucket) or [])
        ],
        "logistics_ids": [item["fact_id"] for item in list(pack.get("logistics_infoblock") or [])],
    }


def _fact_catalog(pack: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        item["fact_id"]: item
        for bucket in ("event_core", "program_list", "people_and_roles", "forward_looking", "support_context", "logistics_infoblock")
        for item in list(pack.get(bucket) or [])
        if isinstance(item, dict) and item.get("fact_id")
    }


def _is_literal_program_fact(item: dict[str, Any] | None) -> bool:
    if not isinstance(item, dict):
        return False
    return str(item.get("bucket") or "").strip() == "program_list" and bool(item.get("literal_items"))


def _program_insert_index(blocks: list[dict[str, Any]]) -> int:
    for idx, block in enumerate(blocks[1:], start=1):
        if str(block.get("role") or "").strip() == "body":
            return idx + 1
    return 1 if blocks else 0


def _reorder_body_refs_for_opening(refs: list[str], catalog: dict[str, dict[str, Any]]) -> list[str]:
    bucket_rank = {
        "support_context": 0,
        "forward_looking": 1,
        "event_core": 2,
        "people_and_roles": 3,
        "program_list": 4,
    }
    return sorted(
        refs,
        key=lambda fact_id: (
            bucket_rank.get(str(catalog.get(fact_id, {}).get("bucket") or "").strip(), 5),
            refs.index(fact_id),
        ),
    )


def _clean_heading(value: Any) -> str | None:
    heading = re.sub(r"\s+", " ", str(value or "")).strip()
    if not heading:
        return None
    if len(heading) < 3:
        return None
    if not re.search(r"[A-Za-zА-Яа-яЁё0-9]", heading):
        return None
    if heading.casefold() in {"о событии", "подробности", "основная идея", "что будет"}:
        return None
    return heading


def _build_prompt(
    *,
    title: str,
    event_type: str,
    lead_payload: dict[str, Any],
    precompute: dict[str, Any],
    pack: dict[str, Any],
    gemma4: bool = False,
) -> str:
    return textwrap.dedent(
        f"""
        {'SYSTEM' if gemma4 else 'PROMPT'}
        You do one small step: editorial.layout.plan.v1.
        Return only JSON.
        Use all_fact_ids as an exact once-only checklist.
        Do not write prose.
        If title_needs_format_anchor is true, keep the lead focused on event format/action clarity.
        If the weighted pack contains a rarity or atmosphere fact with meaningful narrative value, place it in the lead support or the first body block instead of burying it in a tail sentence.
        If a narrative block mixes vivid atmosphere/rarity facts with generic ensemble-category or service-note facts, do not make the generic fact the opening beat of that block.
        Prefer to keep atmosphere/rarity in an earlier narrative block and move ensemble-category detail closer to people/performers when possible.
        Split literal repertoire list away from atmosphere narrative when both appear in the same cluster.
        Do not strand a rarity hook in the final weak tail behind admin/service material if it can support the lead or first narrative block.
        If body_block_floor = 2, plan at least two post-lead narrative sections unless a program block already carries one cluster.
        Avoid generic filler headings.

        TITLE: {title}
        EVENT_TYPE: {event_type}
        LEAD_PAYLOAD: {lead_payload}
        PRECOMPUTE: {precompute}
        PACK: {pack}
        """
    ).strip()


def _build_layout_prompt(*, input_payload: dict[str, Any]) -> str:
    return textwrap.dedent(
        f"""
        You do one small step: editorial.layout.plan.v1.
        Return only JSON.
        heading_guardrail_recommended must be respected when true.
        body_block_floor and multi_body_split_recommended are deterministic carries from precompute.
        Use `event_type` to choose heading vocabulary that sounds native to the material.
        EXAMPLE: screening
        EXAMPLE: lecture

        INPUT:
        {input_payload}
        """
    ).strip()


def _clean_layout_plan(
    payload: dict[str, Any],
    *,
    title: str,
    pack: dict[str, Any],
    lead_payload: dict[str, Any],
    precompute: dict[str, Any],
) -> dict[str, Any]:
    all_fact_ids = list(precompute.get("all_fact_ids") or [])
    logistics_ids = set(precompute.get("logistics_ids") or [])
    catalog = _fact_catalog(pack)
    used: set[str] = set()
    cleaned_blocks: list[dict[str, Any]] = []
    deferred_program_refs: list[str] = []
    lead_ids = [lead_payload["lead_fact_id"]]
    if lead_payload.get("lead_support_id"):
        support_id = lead_payload["lead_support_id"]
        if _is_literal_program_fact(catalog.get(support_id)):
            deferred_program_refs.append(support_id)
        else:
            lead_ids.append(support_id)
    cleaned_blocks.append({"role": "lead", "fact_refs": [item for item in lead_ids if item], "style": "narrative", "heading": None})
    used.update(cleaned_blocks[0]["fact_refs"])

    raw_blocks = [block for block in list(payload.get("blocks") or []) if isinstance(block, dict)]
    for block in raw_blocks:
        role = str(block.get("role") or "").strip()
        if role in {"lead", ""}:
            continue
        refs = [fact_id for fact_id in list(block.get("fact_refs") or []) if fact_id in all_fact_ids and fact_id not in used]
        if role == "infoblock":
            refs = [fact_id for fact_id in refs if fact_id in logistics_ids]
        else:
            refs = [fact_id for fact_id in refs if fact_id not in logistics_ids]
        if role != "infoblock":
            literal_program_refs = [fact_id for fact_id in refs if _is_literal_program_fact(catalog.get(fact_id))]
            for fact_id in literal_program_refs:
                if fact_id not in deferred_program_refs:
                    deferred_program_refs.append(fact_id)
            refs = [fact_id for fact_id in refs if fact_id not in literal_program_refs]
            if role == "program":
                role = "body"
        if not refs:
            continue
        heading = _clean_heading(block.get("heading"))
        if role in {"lead", "infoblock"} or not precompute.get("allow_semantic_headings"):
            heading = None
        cleaned_blocks.append(
            {
                "role": role,
                "fact_refs": _reorder_body_refs_for_opening(refs, catalog) if role == "body" and str(block.get("style") or "") != "structured" else refs,
                "style": str(block.get("style") or ("structured" if role == "infoblock" else "narrative")),
                "heading": heading,
            }
        )
        used.update(refs)

    remaining_body: list[str] = []
    for fact_id in all_fact_ids:
        if fact_id in used or fact_id in logistics_ids:
            continue
        if _is_literal_program_fact(catalog.get(fact_id)):
            if fact_id not in deferred_program_refs:
                deferred_program_refs.append(fact_id)
            continue
        remaining_body.append(fact_id)
    if remaining_body:
        cleaned_blocks.append(
            {
                "role": "body",
                "fact_refs": _reorder_body_refs_for_opening(remaining_body, catalog),
                "style": "narrative",
                "heading": None,
            }
        )
        used.update(remaining_body)

    program_refs = [fact_id for fact_id in deferred_program_refs if fact_id in all_fact_ids and fact_id not in used]
    if program_refs:
        insert_at = _program_insert_index(cleaned_blocks)
        cleaned_blocks.insert(
            insert_at,
            {
                "role": "program",
                "fact_refs": program_refs,
                "style": "list",
                "heading": "Программа" if precompute.get("allow_semantic_headings") else None,
            },
        )
        used.update(program_refs)

    remaining_logistics = [fact_id for fact_id in all_fact_ids if fact_id not in used and fact_id in logistics_ids]
    if remaining_logistics:
        cleaned_blocks.append({"role": "infoblock", "fact_refs": remaining_logistics, "style": "structured", "heading": None})
        used.update(remaining_logistics)

    body_blocks = [block for block in cleaned_blocks if block["role"] == "body"]
    split_applied = False
    if len(body_blocks) == 1 and precompute.get("body_block_floor") == 2:
        block = body_blocks[0]
        pivot = None
        refs = list(block["fact_refs"])
        for idx in range(1, len(refs)):
            prev_bucket = str(catalog.get(refs[idx - 1], {}).get("bucket") or "").strip()
            curr_bucket = str(catalog.get(refs[idx], {}).get("bucket") or "").strip()
            if prev_bucket != curr_bucket:
                pivot = idx
                break
        if pivot and 0 < pivot < len(block["fact_refs"]):
            first_refs = block["fact_refs"][:pivot]
            second_refs = block["fact_refs"][pivot:]
            insert_at = cleaned_blocks.index(block)
            cleaned_blocks[insert_at : insert_at + 1] = [
                {"role": "body", "fact_refs": first_refs, "style": "narrative", "heading": block.get("heading")},
                {"role": "body", "fact_refs": second_refs, "style": "narrative", "heading": None},
            ]
            split_applied = True

    title_strategy = "enhance" if str(payload.get("title_strategy") or "").strip() == "enhance" else "keep"
    title_hint_ref = str(payload.get("title_hint_ref") or "").strip() or None
    if title_strategy == "keep":
        title_hint_ref = None

    return {
        "title_strategy": title_strategy,
        "title_hint_ref": title_hint_ref,
        "blocks": cleaned_blocks,
        "cleaning_stats": {
            "body_split_floor_applied": split_applied,
            "program_block_inserted": bool(program_refs),
            "deferred_program_fact_ids": program_refs,
        },
    }


def _audit_layout(
    *,
    plan_payload: dict[str, Any],
    pack: dict[str, Any],
    precompute: dict[str, Any],
    lead_payload: dict[str, Any],
    title: str,
) -> dict[str, Any]:
    flags: list[str] = []
    headings = [
        str(block.get("heading") or "").strip()
        for block in list(plan_payload.get("blocks") or [])
        if str(block.get("role") or "").strip() in {"body", "program"} and str(block.get("heading") or "").strip()
    ]
    if precompute.get("heading_guardrail_recommended") and not headings:
        flags.append("missing_headings_for_dense_case")
    return {
        "flags": flags,
        "metrics": {
            "heading_guardrail_recommended": bool(precompute.get("heading_guardrail_recommended")),
            "heading_count": len(headings),
        },
    }
