from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import RunConfig
from .contracts import SemanticPlan
from .llm_gateway import image_part, run_gateway_json_call


def _semantic_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "primary_object": {
                "type": "object",
                "properties": {
                    "label": {"type": "string"},
                    "visual_summary": {"type": "string"},
                    "bbox_hint_xyxy": {"type": "array", "items": {"type": "number"}},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                },
                "required": ["label", "visual_summary", "bbox_hint_xyxy", "confidence"],
            },
            "style_relevant_features": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "geometry_type": {"type": "string"},
                        "location_hint": {"type": "string"},
                        "importance": {"type": "number", "minimum": 0, "maximum": 1},
                        "line_art_instruction": {"type": "string"},
                    },
                    "required": ["name", "geometry_type", "location_hint", "importance", "line_art_instruction"],
                },
            },
            "occluders": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "location_hint": {"type": "string"},
                        "mask_prompt": {"type": "string"},
                        "priority": {"type": "number", "minimum": 0, "maximum": 1},
                    },
                    "required": ["name", "location_hint", "mask_prompt", "priority"],
                },
            },
            "should_ignore": {"type": "array", "items": {"type": "string"}},
            "completion_policy": {
                "type": "object",
                "properties": {
                    "preserve": {"type": "array", "items": {"type": "string"}},
                    "complete_conservatively": {"type": "array", "items": {"type": "string"}},
                    "reject_as_background": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["preserve", "complete_conservatively", "reject_as_background"],
            },
            "notes_for_generation": {"type": "string"},
        },
        "required": ["primary_object", "style_relevant_features", "occluders", "should_ignore", "completion_policy"],
    }


def analyze_with_gemini(image_path: str | Path, config: RunConfig) -> tuple[SemanticPlan, list[str]]:
    if not config.gemini.enabled:
        raise RuntimeError("Gemini semantic planning is required for contour_svg")
    prompt = """
Analyze this image for a neural contour SVG generator.
Select the primary object, prioritizing architecture/buildings/towers.
Return one valid JSON object only, with double-quoted keys and no markdown.
Identify important contour/line-art features, occluders,
background elements to ignore, and conservative completion policy.
Never invent hidden parts outside the visible or inferred object envelope.
"""
    text = run_gateway_json_call(
        model=config.gemini.model,
        prompt=[
            {"text": prompt},
            image_part(image_path, mime_type="image/png"),
        ],
        generation_config={
            "response_mime_type": "application/json",
            "response_schema": _semantic_schema(),
            "temperature": 0,
        },
        default_env_var_name=config.gemini.api_key_env,
        max_output_tokens=2000,
    )
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        debug_dir = config.resolved_output_dir() / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        raw_path = debug_dir / "gemini_semantic_raw.txt"
        raw_path.write_text(text or "", encoding="utf-8")
        raise RuntimeError(f"Gemini semantic plan returned invalid JSON; raw response: {raw_path}") from exc
    primary = data.get("primary_object")
    if not isinstance(primary, dict) or not primary:
        raise RuntimeError("Gemini semantic plan did not return primary_object")
    return SemanticPlan(
        primary_object=primary,
        style_relevant_features=list(data.get("style_relevant_features") or []),
        occluders=list(data.get("occluders") or []),
        should_ignore=list(data.get("should_ignore") or []),
        completion_policy=dict(data.get("completion_policy") or {}),
        notes_for_generation=str(data.get("notes_for_generation") or ""),
        source="gemini",
    ), []
