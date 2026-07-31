import asyncio
import json
from pathlib import Path

import pytest

import smart_event_update as su


REPLAY = (
    Path(__file__).parent
    / "replays"
    / "INC-2026-07-31-poster-candidate-url"
    / "source.json"
)


def test_poster_candidate_evidence_url_prefers_managed_then_source() -> None:
    assert (
        su._poster_candidate_evidence_url(
            su.PosterCandidate(
                catbox_url="https://source.example/poster.jpg",
                supabase_url="https://static.example/poster.webp",
            )
        )
        == "https://static.example/poster.webp"
    )
    assert (
        su._poster_candidate_evidence_url(
            su.PosterCandidate(catbox_url="https://source.example/poster.jpg")
        )
        == "https://source.example/poster.jpg"
    )
    assert su._poster_candidate_evidence_url(su.PosterCandidate()) is None


def test_vk_smart_update_boundary_builds_poster_grounding_evidence(monkeypatch) -> None:
    payload = json.loads(REPLAY.read_text(encoding="utf-8"))
    candidate = su.EventCandidate(
        source_type=payload["source_type"],
        source_url=payload["source_url"],
        source_text=payload["source_text"],
        festival=payload["festival"],
        posters=[su.PosterCandidate(**item) for item in payload["posters"]],
    )
    captured: dict[str, object] = {}

    class BoundaryReached(Exception):
        pass

    def capture_grounding(festival, *, source_evidence, curated_festival_series):
        captured["festival"] = festival
        captured["source_evidence"] = source_evidence
        captured["curated_festival_series"] = curated_festival_series
        raise BoundaryReached

    monkeypatch.setattr(su, "ground_kgd80_festival", capture_grounding)

    with pytest.raises(BoundaryReached):
        asyncio.run(
            su._smart_event_update_impl(
                object(),
                candidate,
                schedule_tasks=False,
            )
        )

    poster_evidence = captured["source_evidence"][-1]
    assert poster_evidence == [
        {
            "ocr_text": None,
            "ocr_title": None,
            "url": "https://static.example/managed-poster.webp",
        },
        {"ocr_text": None, "ocr_title": None, "url": None},
    ]
