from __future__ import annotations

import json
from pathlib import Path

from .config import RunConfig
from .contracts import Candidate, SemanticPlan
from .llm_gateway import image_part, run_gateway_json_call


def _judge_schema(candidate_ids: list[str]) -> dict:
    return {
        "type": "object",
        "properties": {
            "scores": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "candidate_id": {"type": "string", "enum": candidate_ids},
                        "score": {"type": "number", "minimum": 0, "maximum": 10},
                        "coarse_to_fine_present": {"type": "boolean"},
                        "parasite_foliage_detected": {"type": "boolean"},
                        "embedded_raster_detected": {"type": "boolean"},
                        "reason": {"type": "string"},
                    },
                    "required": [
                        "candidate_id",
                        "score",
                        "coarse_to_fine_present",
                        "parasite_foliage_detected",
                        "embedded_raster_detected",
                        "reason",
                    ],
                },
            },
            "best_candidate_id": {"type": "string", "enum": candidate_ids},
            "global_notes": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["scores", "best_candidate_id", "global_notes"],
    }


def judge_candidates_with_gemini(
    candidates: list[Candidate],
    *,
    contact_sheet: Path | None,
    original_image: Path,
    semantic_plan: SemanticPlan,
    config: RunConfig,
    out_path: Path,
) -> list[str]:
    warnings: list[str] = []
    if not config.gemini.enabled or not config.candidate_search.allow_gemini_review:
        raise RuntimeError("Gemini candidate review is required for E1 ranking")
    if not contact_sheet or not contact_sheet.exists():
        raise RuntimeError("Gemini candidate review requires a contact sheet")
    style_reference = _resolved_style_reference(config)
    prompt = {
        "task": "Rank clean vector line-art candidates for postcard-like SVG output.",
        "rubric": [
            "global silhouette / main building shell is present before details",
            "coarse-to-fine architecture: roof, corner mass, facade planes, stairs, then windows/details",
            "recognizable primary object",
            "preserve the original source building identity; do not reward candidates that look like a different building",
            "clean geometry and line economy",
            "background/occluder removal",
            "distinctive architectural features",
            "if a style reference image is provided, use it only for visual approach: line economy, graphic contrast, and architectural simplification",
            "low hallucination risk",
            "reject candidates that appear dominated by raster tracing, foliage, fence, sky, pavement, or texture",
        ],
        "semantic_plan": semantic_plan.to_dict(),
        "candidate_ids": [c.candidate_id for c in candidates],
        "style_reference_provided": bool(style_reference),
        "return_json": {
            "scores": [
                {
                    "candidate_id": "string from candidate_ids",
                    "score": "number 0..10",
                    "coarse_to_fine_present": "boolean",
                    "parasite_foliage_detected": "boolean",
                    "embedded_raster_detected": "boolean",
                    "reason": "short string",
                }
            ],
            "best_candidate_id": "string",
            "global_notes": ["string"],
        },
    }
    try:
        parts = [image_part(original_image, mime_type="image/png")]
        if style_reference:
            parts.append(image_part(style_reference, mime_type=_mime_type(style_reference)))
        parts.extend(
            [
                image_part(contact_sheet, mime_type="image/png"),
                {
                    "text": (
                        "Return exactly one valid JSON object matching the schema. "
                        "The first image is the source object identity. "
                        "A second image may be a style reference only; never copy its object identity. "
                        "The contact sheet contains candidate SVG previews to rank.\n"
                        f"{json.dumps(prompt, ensure_ascii=False)}"
                    )
                },
            ]
        )
        text = run_gateway_json_call(
            model=config.gemini.model,
            prompt=parts,
            generation_config={
                "response_mime_type": "application/json",
                "response_schema": _judge_schema([c.candidate_id for c in candidates]),
                "temperature": 0,
            },
            default_env_var_name=config.gemini.api_key_env,
            max_output_tokens=1200,
        )
        data = json.loads(text or "{}")
        score_map = {}
        for row in data.get("scores", []) if isinstance(data, dict) else []:
            try:
                score_map[str(row["candidate_id"])] = float(row["score"])
            except Exception:
                continue
        for candidate in candidates:
            if candidate.candidate_id in score_map:
                candidate.gemini_score = max(0.0, min(10.0, score_map[candidate.candidate_id]))
        missing = [c.candidate_id for c in candidates if c.gemini_score is None]
        if missing:
            raise RuntimeError(f"Gemini judge did not score candidates: {missing}")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        raise RuntimeError(f"Gemini gateway judge failed: {type(exc).__name__}: {exc}") from exc
    return warnings


def _resolved_style_reference(config: RunConfig) -> Path | None:
    raw = config.input.style_reference_path
    if not raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        path = Path.cwd() / path
    return path if path.exists() else None


def _mime_type(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if suffix == ".webp":
        return "image/webp"
    return "image/png"
