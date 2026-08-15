from __future__ import annotations

import json
from pathlib import Path

import pytest

import main
import smart_event_update as seu
import vk_intake
from db import Database
from models import Event
from smart_update_state import ProductExclusionReason


FIXTURE = (
    Path(__file__).parent
    / "replays"
    / "INC-2026-08-15-ingestion-retry-stall-and-wal-growth"
    / "vk_location_grounding.json"
)


@pytest.mark.asyncio
async def test_positive_and_opposite_raw_vk_controls_cross_persist_boundary(
    tmp_path,
    monkeypatch,
) -> None:
    """INC-2026-08-15: replay both controls through VK -> Smart -> DB."""

    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    provider_by_url = {
        str(payload["source_url"]): dict(payload["provider_result"])
        for payload in fixture.values()
    }
    reviewed_urls: list[str] = []

    async def fake_ask(prompt, _schema, *, max_tokens, label):  # noqa: ANN001
        assert max_tokens > 0
        if label == "eventness_review":
            return {
                "decision": "event",
                "confidence": 0.96,
                "reason_short": "the source announces a dated attendee-facing programme",
                "grounded_title": "Праздничная программа",
                "has_single_concrete_event": True,
                "missing_anchors": [],
            }
        assert label == "location_grounding_review"
        for source_url, result in provider_by_url.items():
            if source_url in prompt:
                reviewed_urls.append(source_url)
                return result
        raise AssertionError("location review prompt lost the raw source identity")

    async def fake_bundle(_candidate, *_args, **_kwargs):  # noqa: ANN001
        return {
            "description": "Презентация экологического маршрута в библиотеке Чехова.",
            "short_description": "Презентация экологического маршрута.",
            "search_digest": "экологический маршрут библиотека Чехова",
            "facts": ["18 августа, 15:00", "Московский проспект, 39"],
        }

    async def fake_text(*_args, label, **_kwargs):  # noqa: ANN001
        if label == "rewrite_full":
            return "Презентация экологического маршрута в библиотеке Чехова."
        if label == "short_description":
            return "Встреча о редких растениях и природных памятниках на новом городском маршруте."
        raise AssertionError(f"unexpected text LLM stage: {label}")

    async def grounded_bundle(*_args, **_kwargs):  # noqa: ANN001
        return True, "llm_grounded", []

    async def no_topics(*_args, **_kwargs):  # noqa: ANN001
        return None

    async def no_jobs(*_args, **_kwargs):  # noqa: ANN001
        return {}

    monkeypatch.setattr(seu, "SMART_UPDATE_LLM_DISABLED", False)
    monkeypatch.setattr(seu, "SMART_UPDATE_G4_SPLIT_CREATE", False)
    monkeypatch.setattr(seu, "SMART_UPDATE_G4_LOLLIPOP_LIGHT_CREATE", False)
    monkeypatch.setattr(seu, "SMART_UPDATE_FACT_FIRST", False)
    monkeypatch.setattr(seu, "SMART_UPDATE_DEDUP_ADJUDICATOR", False)
    monkeypatch.setattr(seu, "SMART_UPDATE_IDENTITY_GATE_MODE", seu.IdentityGateMode.OFF)
    monkeypatch.setattr(seu, "SMART_UPDATE_MERGE_IDENTITY_GATE_MODE", seu.IdentityGateMode.OFF)
    monkeypatch.setattr(seu, "_ask_gemma_json", fake_ask)
    monkeypatch.setattr(seu, "_ask_gemma_text", fake_text)
    monkeypatch.setattr(seu, "_llm_create_description_facts_and_digest", fake_bundle)
    monkeypatch.setattr(seu, "_llm_review_create_bundle_grounding", grounded_bundle)
    monkeypatch.setattr(seu, "_classify_topics", no_topics)
    monkeypatch.setattr(main, "schedule_event_update_tasks", no_jobs)
    monkeypatch.setenv("SMART_UPDATE_SKIP_PAST_EVENTS", "0")

    db = Database(str(tmp_path / "shadow.sqlite"))
    await db.init()
    try:
        results = {}
        for control_name in ("positive", "negative"):
            raw = fixture[control_name]
            draft = vk_intake.EventDraft(
                title=raw["title"],
                date=raw["date"],
                time=raw.get("time"),
                venue=raw.get("location_name"),
                festival=raw.get("festival"),
                location_address=raw.get("location_address"),
                city=raw.get("city"),
                source_text=raw["source_text"],
                description=raw["source_text"],
            )
            results[control_name] = await vk_intake.persist_event_and_pages(
                draft,
                [],
                db,
                source_post_url=raw["source_url"],
                wait_for_telegraph_url=False,
            )

        positive = results["positive"]
        assert positive.smart_result is not None
        assert positive.smart_result.status == "created"
        assert positive.event_id is not None
        async with db.get_session() as session:
            saved = await session.get(Event, positive.event_id)
        assert saved is not None
        assert saved.source_post_url == fixture["positive"]["source_url"]
        assert saved.location_name == fixture["positive"]["location_name"]
        assert saved.location_address == fixture["positive"]["location_address"]

        negative = results["negative"]
        assert negative.event_id is None
        assert negative.smart_result is not None
        assert negative.smart_result.status == "rejected_product_policy"
        assert (
            negative.smart_result.product_exclusion_reason
            is ProductExclusionReason.MISSING_LOCATION
        )

        assert reviewed_urls == [
            fixture["positive"]["source_url"],
            fixture["negative"]["source_url"],
        ]
    finally:
        await db.close()
