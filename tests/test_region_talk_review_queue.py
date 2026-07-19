from __future__ import annotations

import base64
import importlib.util
import struct
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "region_talk_review_queue.py"


def load_module():
    spec = importlib.util.spec_from_file_location("region_talk_review_queue", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def row(url: str, quality: float, vector: list[float] | None, *, source: str = "s", topics: list[str] | None = None) -> dict:
    value = {
        "post_url": url,
        "publication_score": quality,
        "source_title": source,
        "diversity_topics": topics or [],
    }
    if vector is not None:
        value.update({
            "embedding_vector_f16_b64": base64.b64encode(struct.pack("<" + "e" * len(vector), *vector)).decode("ascii"),
            "embedding_vector_encoding": "f16_le_base64",
            "embedding_dim": len(vector),
            "model_id": "BAAI/bge-m3",
            "encoder_contract": "sentence_transformers_normalized_v1",
        })
    return value


def test_mmr_places_semantically_different_item_between_near_duplicates() -> None:
    mod = load_module()
    ranked = mod.rank_publication_queue([
        row("https://example.org/a", 1.0, [1.0, 0.0], source="one"),
        row("https://example.org/b", 0.98, [0.99, 0.01], source="two"),
        row("https://example.org/c", 0.90, [0.0, 1.0], source="three"),
    ], diversity_weight=0.3, adjacency_threshold=0.86)
    assert [item["post_url"] for item in ranked] == [
        "https://example.org/a",
        "https://example.org/c",
        "https://example.org/b",
    ]
    assert ranked[1]["previous_similarity_mode"] == "bge_m3_vector"


def test_missing_vectors_use_explicit_heuristic_fallback() -> None:
    mod = load_module()
    ranked = mod.rank_publication_queue([
        row("https://example.org/a", 0.9, None, source="same", topics=["кант"]),
        row("https://example.org/b", 0.8, None, source="same", topics=["кант"]),
    ])
    assert ranked[1]["diversity_mode"] == "heuristic_fallback"
    assert ranked[1]["fallback_reasons"]


def test_adjacency_relaxation_is_visible_when_no_alternative_exists() -> None:
    mod = load_module()
    ranked = mod.rank_publication_queue([
        row("https://example.org/a", 1.0, [1.0, 0.0]),
        row("https://example.org/b", 0.9, [1.0, 0.0]),
    ], adjacency_threshold=0.8)
    assert ranked[1]["adjacency_relaxed"] is True
    assert ranked[1]["adjacency_relax_reason"] == "all_remaining_candidates_exceed_threshold"


def test_incompatible_models_are_not_compared_as_vectors() -> None:
    mod = load_module()
    first = row("https://example.org/a", 1.0, [1.0, 0.0])
    second = row("https://example.org/b", 0.9, [1.0, 0.0])
    second["model_id"] = "other-model"
    similarity, status = mod.cosine_if_compatible(first, second)
    assert similarity is None
    assert status == "incompatible_vector_contract"


def test_durable_history_does_not_penalize_candidate_against_itself() -> None:
    mod = load_module()
    candidate = row("https://example.org/a", 0.9, [1.0, 0.0])
    ranked = mod.rank_publication_queue([candidate], history=[dict(candidate)])
    assert ranked[0]["max_similarity_to_selected_or_history"] == 0.0
    assert ranked[0]["rank_score"] == 0.9


def test_queue_snapshot_messages_include_rank_evidence() -> None:
    mod = load_module()
    ranked = mod.rank_publication_queue([row("https://example.org/a", 0.8, [1.0, 0.0])])
    snapshot = mod.queue_snapshot(ranked, generated_at="2026-07-19T12:00:00+00:00")
    messages = mod.queue_messages(snapshot)
    assert snapshot["snapshot_id"].startswith("rtqueue_")
    assert "rank=" in messages[0]
    assert "max_sim=" in messages[0]


def test_nested_external_contract_fields_render_without_flattening() -> None:
    mod = load_module()
    external = {
        "canonical_url": "https://example.org/paper",
        "publication": {"title": "Исследование берега", "source_name": "Научный журнал"},
        "quality_assessment": {"normalized_score": 0.75},
        "editorial_pack": {
            "source_overview": "Нерегиональный научный журнал.",
            "why_selected": "Понятное научно-популярное зерно.",
        },
    }
    assert mod.quality_score(external) == 0.75
    card = mod.review_card(external)
    assert "Научный журнал" in card
    assert "Исследование берега" in card
    assert "Нерегиональный научный журнал" in card
