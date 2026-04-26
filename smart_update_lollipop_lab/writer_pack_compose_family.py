from __future__ import annotations

import re
from typing import Any


def _fact_priority(weight: str) -> int:
    return {"high": 3, "medium": 2, "low": 1}.get(weight, 2)


def _canonical_label(value: str) -> str:
    text = (value or "").strip()
    lowered = text.lower()
    if "бесплат" in lowered or re.search(r"\b₽\b", text):
        return "Цена"
    if "пушкин" in lowered or "билет" in lowered:
        return "Билеты"
    if "галере" in lowered or "театр" in lowered or "музей" in lowered or "филиал" in lowered:
        return "Локация"
    return "Прочее"


def _canonical_value(value: str, label: str) -> str:
    return (value or "").strip()


def _uses_literal_list_mode(item: dict[str, Any]) -> bool:
    return bool(item.get("literal_items")) and str(item.get("bucket") or "").strip() == "program_list"


def _compose_standard_section(block: dict[str, Any], catalog: dict[str, dict[str, Any]]) -> dict[str, Any]:
    fact_id = block["fact_refs"][0]
    item = catalog[fact_id]
    facts = []
    coverage_plan = []
    literal_items = list(item.get("literal_items") or []) if _uses_literal_list_mode(item) else []
    literal_item_source_fact_ids = [fact_id] if literal_items else []
    if literal_items:
        cleaned_text = re.sub(r"(?iu):\s*.+$", ".", str(item["text"]))
        facts.append({"fact_id": fact_id, "text": cleaned_text, "priority": _fact_priority(item["weight"])})
        coverage_plan.append({"fact_id": fact_id, "mode": "narrative_plus_literal_list"})
    else:
        facts.append({"fact_id": fact_id, "text": item["text"], "priority": _fact_priority(item["weight"])})
        coverage_plan.append({"fact_id": fact_id, "mode": "narrative"})
    return {
        "role": block["role"],
        "style": block["style"],
        "heading": block.get("heading"),
        "fact_ids": [fact_id],
        "facts": facts,
        "coverage_plan": coverage_plan,
        "literal_items": literal_items,
        "literal_item_source_fact_ids": literal_item_source_fact_ids,
        "literal_list_is_partial": bool(literal_items and re.search(r"(?iu)\b(и другие|и др\.?|среди которых|в том числе)\b", str(item["text"]))),
    }


def _compose_program_section(block: dict[str, Any], catalog: dict[str, dict[str, Any]]) -> dict[str, Any]:
    literal_items: list[str] = []
    facts: list[dict[str, Any]] = []
    coverage_plan: list[dict[str, str]] = []
    partial = False
    seen_literal: set[str] = set()
    for fact_id in list(block.get("fact_refs") or []):
        item = catalog[fact_id]
        if _uses_literal_list_mode(item):
            for literal in item["literal_items"]:
                if literal not in seen_literal:
                    seen_literal.add(literal)
                    literal_items.append(literal)
            facts.append({"fact_id": fact_id, "text": item["text"], "priority": _fact_priority(item["weight"])})
            coverage_plan.append({"fact_id": fact_id, "mode": "narrative_plus_literal_list"})
            partial = partial or bool(re.search(r"(?iu)\b(и другие|и др\.?|среди которых|в том числе)\b", str(item["text"])))
            continue
        coverage_plan.append({"fact_id": fact_id, "mode": "narrative"})
        facts.append({"fact_id": fact_id, "text": item["text"], "priority": _fact_priority(item["weight"])})
    return {
        "role": block["role"],
        "style": block["style"],
        "heading": block.get("heading"),
        "fact_ids": list(block.get("fact_refs") or []),
        "facts": facts,
        "coverage_plan": coverage_plan,
        "literal_items": literal_items,
        "literal_item_source_fact_ids": [fact_id for fact_id in block.get("fact_refs") or [] if _uses_literal_list_mode(catalog[fact_id])],
        "literal_list_is_partial": partial,
    }


def _compose_writer_pack(*, event_id: int, title: str, layout_result: dict[str, Any], prioritize_result: dict[str, Any]) -> dict[str, Any]:
    layout_payload = dict(layout_result["layout_result"]["payload"])
    precompute = dict(layout_result["layout_result"]["precompute"])
    pack = dict(prioritize_result["weight_result"]["payload"])
    catalog = {
        item["fact_id"]: item
        for bucket in ("event_core", "program_list", "people_and_roles", "forward_looking", "support_context", "logistics_infoblock")
        for item in list(pack.get(bucket) or [])
        if item.get("narrative_policy") != "suppress"
    }
    sections: list[dict[str, Any]] = []
    for block in list(layout_payload.get("blocks") or []):
        role = str(block.get("role") or "").strip()
        if role == "infoblock":
            continue
        refs = [fact_id for fact_id in list(block.get("fact_refs") or []) if fact_id in catalog]
        if not refs:
            continue
        if role == "program":
            section = _compose_program_section({**block, "fact_refs": refs}, catalog)
        elif len(refs) == 1:
            section = _compose_standard_section({**block, "fact_refs": refs}, catalog)
        else:
            facts = []
            coverage_plan = []
            literal_items: list[str] = []
            literal_sources: list[str] = []
            partial = False
            for fact_id in refs:
                item = catalog[fact_id]
                if _uses_literal_list_mode(item):
                    literal_items.extend(list(item["literal_items"]))
                    literal_sources.append(fact_id)
                    coverage_plan.append({"fact_id": fact_id, "mode": "narrative_plus_literal_list"})
                    partial = partial or bool(re.search(r"(?iu)\b(и другие|и др\.?|среди которых|в том числе)\b", str(item["text"])))
                else:
                    coverage_plan.append({"fact_id": fact_id, "mode": "narrative"})
                facts.append({"fact_id": fact_id, "text": item["text"], "priority": _fact_priority(item["weight"])})
            section = {
                "role": role,
                "style": block.get("style") or "narrative",
                "heading": block.get("heading"),
                "fact_ids": refs,
                "facts": facts,
                "coverage_plan": coverage_plan,
                "literal_items": literal_items,
                "literal_item_source_fact_ids": literal_sources,
                "literal_list_is_partial": partial,
            }
        sections.append(section)

    infoblock = [
        {
            "fact_id": item["fact_id"],
            "label": _canonical_label(item["text"]),
            "value": _canonical_value(item["text"], _canonical_label(item["text"])),
        }
        for item in list(pack.get("logistics_infoblock") or [])
        if item.get("narrative_policy") != "suppress"
    ]
    infoblock.sort(key=lambda row: ["Дата", "Время", "Локация", "Цена", "Билеты", "Возраст", "Прочее"].index(row["label"]) if row["label"] in ["Дата", "Время", "Локация", "Цена", "Билеты", "Возраст", "Прочее"] else 6)

    return {
        "event_id": event_id,
        "payload": {
            "event_type": layout_result["event_type"],
            "title_context": {
                "original_title": title,
                "strategy": layout_payload["title_strategy"],
                "hint_fact_id": layout_payload.get("title_hint_ref"),
                "hint_fact_text": catalog.get(layout_payload.get("title_hint_ref") or "", {}).get("text"),
                "is_bare": bool(precompute.get("title_is_bare")),
            },
            "sections": sections,
            "infoblock": infoblock,
            "constraints": {
                "must_cover_fact_ids": [fact_id for fact_id in precompute.get("all_fact_ids") or [] if not fact_id.startswith("LG") and fact_id in catalog],
                "infoblock_fact_ids": [row["fact_id"] for row in infoblock],
                "headings": [str(section.get("heading")).strip() for section in sections if section.get("heading")],
                "list_required": any(section.get("literal_items") for section in sections),
                "no_logistics_in_narrative": True,
            },
        },
    }
