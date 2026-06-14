from __future__ import annotations

import json
from json import JSONDecodeError
from pathlib import Path

from .config import RunConfig
from .contracts import LineGroup, MaskBundle, SemanticPlan
from .llm_gateway import image_part, run_gateway_json_call


CHUNK_SIZE = 10


def review_line_groups_with_gemini(
    groups: list[LineGroup],
    *,
    original_image: str | Path,
    overlay_image: str | Path,
    semantic_plan: SemanticPlan,
    masks: MaskBundle,
    config: RunConfig,
    out_path: str | Path,
) -> dict[str, dict[str, object]]:
    if not config.gemini.enabled or not config.candidate_search.allow_gemini_line_review:
        raise RuntimeError("Gemini line-group verdict is required before primitive final rendering")
    if not groups:
        raise RuntimeError("Gemini line-group verdict requires at least one line group")

    selected = groups[: config.candidate_search.max_gemini_line_groups]
    actions: dict[str, dict[str, object]] = {}
    reports: list[dict[str, object]] = []
    for start in range(0, len(selected), CHUNK_SIZE):
        chunk = selected[start : start + CHUNK_SIZE]
        ids = [group.id for group in chunk]
        schema = _line_group_schema(ids)
        prompt = _line_group_prompt(chunk, semantic_plan, masks)
        text = run_gateway_json_call(
            model=config.gemini.model,
            prompt=[
                image_part(original_image, mime_type="image/png"),
                image_part(overlay_image, mime_type="image/png"),
                {"text": prompt},
            ],
            generation_config={
                "response_mime_type": "application/json",
                "response_schema": schema,
                "temperature": 0,
            },
            default_env_var_name=config.gemini.api_key_env,
            max_output_tokens=5000,
        )
        try:
            data = json.loads(text or "{}")
        except JSONDecodeError as exc:
            raw_path = Path(out_path).with_name(f"{Path(out_path).stem}_chunk_{start // CHUNK_SIZE:02d}.raw.txt")
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_text(text or "", encoding="utf-8")
            raise RuntimeError(f"Gemini line-group verdict returned invalid JSON; raw response: {raw_path}") from exc
        seen: set[str] = set()
        for row in data.get("group_actions", []) if isinstance(data, dict) else []:
            group_id = str(row.get("group_id") or "")
            if group_id not in ids:
                raise RuntimeError(f"Gemini line verdict returned unknown group_id={group_id}")
            seen.add(group_id)
            actions[group_id] = dict(row)
        missing = sorted(set(ids) - seen)
        if missing:
            raise RuntimeError(f"Gemini line verdict did not cover groups: {missing}")
        reports.append(data)

    output = Path(out_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps({"reports": reports, "actions": actions}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return actions


def _line_group_schema(group_ids: list[str]) -> dict[str, object]:
    return {
        "type": "object",
        "properties": {
            "overall_assessment": {"type": "string"},
            "noise_risk_score": {"type": "number", "minimum": 0, "maximum": 1},
            "recognizability_score": {"type": "number", "minimum": 0, "maximum": 1},
            "group_actions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "group_id": {"type": "string", "enum": group_ids},
                        "action": {
                            "type": "string",
                            "enum": [
                                "keep",
                                "drop",
                                "merge_with",
                                "simplify",
                                "extend_across_occluder",
                                "lower_priority",
                            ],
                        },
                        "target_group_id": {"type": "string"},
                        "semantic_label": {"type": "string"},
                        "reason": {"type": "string"},
                        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    },
                    "required": ["group_id", "action", "semantic_label", "reason", "confidence"],
                },
            },
        },
        "required": ["overall_assessment", "noise_risk_score", "recognizability_score", "group_actions"],
    }


def _line_group_prompt(groups: list[LineGroup], semantic_plan: SemanticPlan, masks: MaskBundle) -> str:
    summaries = [
        {
            "group_id": group.id,
            "semantic_label": group.semantic_label,
            "importance": group.importance,
            "confidence": group.confidence,
            "object_visible_overlap": group.object_visible_overlap,
            "occluder_overlap": group.occluder_overlap,
            "background_overlap": group.background_overlap,
            "member_count": len(group.members),
            "length": round(group.merged_geometry.length, 2),
        }
        for group in groups
    ]
    payload = {
        "task": "Edit line groups before primitive SVG rendering.",
        "hard_rules": [
            "Keep only meaningful contour lines of the primary object.",
            "Drop trees, foliage, foreground fence, sky, pavement, wires, shadows and random texture.",
            "Prefer global silhouette, roof, facade corner, cornices, key openings and stairs before decorative detail.",
            "Use extend_across_occluder only for confirmed architecture continuation, not for invented hidden features.",
            "Return one action for every allowed group id; do not invent group ids.",
        ],
        "allowed_group_ids": [group.id for group in groups],
        "semantic_plan": semantic_plan.to_dict(),
        "mask_bundle": masks.to_dict(),
        "line_groups": summaries,
    }
    return (
        "Return exactly one valid JSON object matching the schema. "
        "All confidence/risk scores must be in 0..1. "
        f"{json.dumps(payload, ensure_ascii=False)}"
    )
