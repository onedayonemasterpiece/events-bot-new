from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "region_talk_operator_feedback.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_operator_feedback", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_operator_visual_approval_is_manifest_bound_and_does_not_bypass_other_gates() -> None:
    mod = load_module()
    row = {
        "image_model_input_type": "actual_image",
        "image_queue_status": "actual_scored",
        "image_acquisition_status": "complete",
        "expected_image_count": 8,
        "fetched_image_count": 8,
        "images_scored_actual_count": 8,
        "input_media_manifest_hash": "gallery-hash",
        "cv_publication_safety_score": 0.98,
        "vector_gate_status": "vector_accept_candidate",
    }
    result = mod.apply_decision(
        "image_queue_item",
        row,
        decision="approve_visual",
        reason="professional architecture/interior editorial gallery",
        at="2026-07-20T12:00:00+00:00",
    )

    assert result["image_quality_decision"] == "operator_visual_accept"
    assert result["operator_visual_media_manifest_hash"] == "gallery-hash"
    assert result["next_action"] == "publication_verification"
    assert result["vector_gate_status"] == "vector_accept_candidate"
    assert "manual_decision" not in result


def test_operator_visual_approval_waits_for_complete_safe_actual_gallery() -> None:
    mod = load_module()
    result = mod.apply_decision(
        "image_queue_item",
        {
            "image_model_input_type": "actual_image",
            "image_acquisition_status": "partial",
            "expected_image_count": 8,
            "fetched_image_count": 2,
            "images_scored_actual_count": 2,
            "input_media_manifest_hash": "partial-hash",
            "cv_publication_safety_score": 0.98,
        },
        decision="approve_visual",
        reason="good page",
        at="2026-07-20T12:00:00+00:00",
    )

    assert result.get("image_quality_decision") != "operator_visual_accept"
    assert result["next_action"] == "complete_safe_actual_gallery_before_operator_visual_accept"
