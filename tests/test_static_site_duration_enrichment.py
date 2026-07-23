from __future__ import annotations

import asyncio
import importlib.util
import json
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "site/scripts/enrich-event-duration-estimates.py"
SPEC = importlib.util.spec_from_file_location("enrich_event_duration_estimates", SCRIPT)
assert SPEC and SPEC.loader
ENRICHER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ENRICHER)


def test_strict_duration_result_validation_and_conservative_rounding() -> None:
    result = ENRICHER.validate_provider_result({
        "most_likely_minutes": 120,
        "plausible_min_minutes": 90,
        "plausible_max_minutes": 180,
        "confidence": "medium",
    })
    assert result["conservative_routing_minutes"] == 150

    with pytest.raises(ValueError):
        ENRICHER.validate_provider_result({
            "most_likely_minutes": 120,
            "plausible_min_minutes": 180,
            "plausible_max_minutes": 90,
            "confidence": "medium",
        })
    with pytest.raises(ValueError):
        ENRICHER.validate_provider_result({
            "most_likely_minutes": 120,
            "plausible_min_minutes": 90,
            "plausible_max_minutes": 180,
            "confidence": "medium",
            "public_rationale": "must not be accepted",
        })


def test_enrichment_is_bounded_cached_and_records_api_gateway(tmp_path: Path) -> None:
    events_path = tmp_path / "events.json"
    schedules_path = tmp_path / "schedules.json"
    output_path = tmp_path / "estimates.json"
    events_path.write_text(json.dumps({
        "events": [{
            "id": 6529,
            "title": "Декоративное мини-панно «Тканые узоры»",
            "event_type": "мастер-класс",
            "description_html": "<p>Создание панно на рамке для новичков.</p>",
            "venue_name": "Музей курортной моды",
            "city": "Зеленоградск",
            "start_date": "2026-07-26",
            "end_date": None,
            "start_time": "15:00",
            "time_range_end": None,
        }],
    }, ensure_ascii=False), encoding="utf-8")
    schedules_path.write_text(json.dumps({
        "routes": [{"city": "Зеленоградск"}],
    }, ensure_ascii=False), encoding="utf-8")
    calls = 0

    async def provider(_packet: dict[str, object]) -> str:
        nonlocal calls
        calls += 1
        return json.dumps({
            "most_likely_minutes": 120,
            "plausible_min_minutes": 90,
            "plausible_max_minutes": 180,
            "confidence": "medium",
        })

    kwargs = {
        "events_path": events_path,
        "schedules_path": schedules_path,
        "output_path": output_path,
        "model": "gemini-3.1-flash-lite",
        "key_envs": ["GOOGLE_API_KEY5"],
        "max_events": 1,
        "event_ids": {6529},
        "provider_call": provider,
        "require_complete": True,
    }
    first = asyncio.run(ENRICHER.enrich(**kwargs))
    second = asyncio.run(ENRICHER.enrich(**kwargs))

    assert calls == 1
    assert first["scope"] == "build_time"
    assert first["candidate_limit"] == 1
    assert second["estimates"][0]["generation_method"] == "provider_api"
    assert second["estimates"][0]["model"] == {
        "provider": "Google Gemini API",
        "gateway": "google_ai.client.GoogleAIClient",
        "id": "gemini-3.1-flash-lite",
    }
    assert second["estimates"][0]["conservative_routing_minutes"] == 150
    assert "public_rationale" not in second["estimates"][0]


def test_invalid_provider_response_fails_closed_without_an_estimate(tmp_path: Path) -> None:
    events_path = tmp_path / "events.json"
    schedules_path = tmp_path / "schedules.json"
    output_path = tmp_path / "estimates.json"
    events_path.write_text(json.dumps({
        "events": [{
            "id": 6529,
            "title": "Панно",
            "event_type": "мастер-класс",
            "description_html": "",
            "venue_name": "Музей",
            "city": "Зеленоградск",
            "start_date": "2026-07-26",
            "end_date": None,
            "start_time": "15:00",
            "time_range_end": None,
        }],
    }), encoding="utf-8")
    schedules_path.write_text(json.dumps({"routes": [{"city": "Зеленоградск"}]}), encoding="utf-8")

    async def invalid_provider(_packet: dict[str, object]) -> str:
        return '{"most_likely_minutes": 10}'

    payload = asyncio.run(ENRICHER.enrich(
        events_path=events_path,
        schedules_path=schedules_path,
        output_path=output_path,
        model="gemini-3.1-flash-lite",
        key_envs=["GOOGLE_API_KEY5"],
        max_events=1,
        event_ids={6529},
        provider_call=invalid_provider,
    ))
    assert payload["estimates"] == []
    assert payload["failures"] == [{"event_id": 6529, "error": "ValueError"}]
