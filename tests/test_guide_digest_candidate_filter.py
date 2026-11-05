from __future__ import annotations

from guide_excursions.service import _display_rows_excluding_published_reference_clusters


def test_digest_preview_suppresses_candidates_clustered_with_published_references() -> None:
    display, suppressed = _display_rows_excluding_published_reference_clusters(
        [
            {"id": 140, "canonical_title": "Огонь Брюстерорта"},
            {"id": 141, "canonical_title": "Прогулка по Зеленоградску"},
        ],
        coverage_by_display_id={
            140: [140, 120],
            141: [141],
        },
        published_reference_ids={120},
        limit=24,
    )

    assert [row["id"] for row in display] == [141]
    assert suppressed == [140]


def test_digest_candidate_query_requires_iso_dates() -> None:
    import inspect

    from guide_excursions import service

    source = inspect.getsource(service._fetch_digest_candidates)

    assert "go.date GLOB '????-??-??'" in source



def test_same_post_paraphrase_reaches_model_and_published_reference(monkeypatch) -> None:
    import asyncio
    from guide_excursions import dedup

    rows = [
        {"id": 718, "canonical_title": "Кинематографический Советск"},
        {"id": 716, "canonical_title": "Кинопрогулка по Советску"},
    ]
    for row in rows:
        row.update(source_username="altin39", source_kind="guide_personal",
                   date="2026-11-06", time="14:00",
                   channel_url="https://vk.com/wall394310405_3914",
                   booking_url="https://example.org/sovetsk-walk")
    calls = []

    async def judge(left, right, features):
        calls.append((left["id"], right["id"]))
        return {"decision": "same_occurrence", "canonical_side": "right",
                "confidence": 0.99, "relation": "same_walk", "reason_short": "One walk in the same post."}

    monkeypatch.setattr(dedup, "_ask_pair_judge_llm", judge)
    monkeypatch.setattr(dedup, "GUIDE_EXCURSIONS_DEDUP_ENABLED", True)
    monkeypatch.setattr(dedup, "GUIDE_EXCURSIONS_DEDUP_LLM_ENABLED", True)
    monkeypatch.setattr(dedup, "_PAIR_DECISION_CACHE", {})
    result = asyncio.run(dedup.deduplicate_occurrence_rows(rows, family="new_occurrences", limit=2))
    display, suppressed = _display_rows_excluding_published_reference_clusters(
        result.display_rows, coverage_by_display_id=result.coverage_by_display_id,
        published_reference_ids={716}, limit=2,
    )
    assert calls == [(718, 716)]
    assert display == []
    assert suppressed


def test_distinct_products_with_shared_booking_keep_model_or_separate_post_verdict(monkeypatch) -> None:
    import asyncio
    from guide_excursions import dedup

    left = {"id": 801, "canonical_title": "Каяки Преголи", "source_username": "organizer",
            "date": "2026-11-06", "time": "14:00", "channel_url": "https://t.me/organizer/10",
            "booking_url": "https://example.org/contact"}
    right = dict(left, id=802, canonical_title="Архитектура Амалиенау")
    calls = []

    async def judge(*args):
        calls.append(True)
        return {"decision": "distinct", "canonical_side": "neither", "confidence": 0.99,
                "relation": "different_routes", "reason_short": "Independent simultaneous tours."}

    monkeypatch.setattr(dedup, "_ask_pair_judge_llm", judge)
    monkeypatch.setattr(dedup, "GUIDE_EXCURSIONS_DEDUP_LLM_ENABLED", True)
    monkeypatch.setattr(dedup, "_PAIR_DECISION_CACHE", {})
    same_post = asyncio.run(dedup._decide_pair(left, right, dedup._build_pair_features(left, right)))
    assert same_post["decision"] == "distinct"
    assert calls == [True]
    right["channel_url"] = "https://t.me/organizer/11"
    different_post = asyncio.run(dedup._decide_pair(left, right, dedup._build_pair_features(left, right)))
    assert different_post["decision"] == "distinct"
    assert different_post["decided_by"] == "heuristic"
    assert calls == [True]
