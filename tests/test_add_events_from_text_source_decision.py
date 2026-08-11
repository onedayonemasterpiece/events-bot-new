from __future__ import annotations

from pathlib import Path

import pytest

import main
import smart_event_update as smart_update_module
from db import Database
from poster_media import PosterMedia
from source_parse_contract import (
    EvidenceManifest,
    SourceDisposition,
    SourceParseDecision,
    SourceParseRetryReason,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["retry", "confirmed_no_event"])
async def test_typed_empty_source_decision_never_enters_smart_update(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    terminal: str,
) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    manifest = EvidenceManifest.complete_source("Полный исходный текст")
    if terminal == "retry":
        decision = SourceParseDecision.retry(
            SourceParseRetryReason.TECHNICAL_ERROR,
            evidence_manifest=manifest,
        )
    else:
        decision = SourceParseDecision(
            [],
            disposition=SourceDisposition.CONFIRMED_NO_EVENT,
            evidence_manifest=manifest,
            evidence_complete=True,
        )

    async def fake_parse(*_args, **_kwargs):
        return decision

    async def forbidden_smart_update(*_args, **_kwargs):
        raise AssertionError("typed empty source terminal reached Smart Update")

    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse)
    monkeypatch.setattr(
        smart_update_module, "smart_event_update", forbidden_smart_update
    )
    try:
        result = await main.add_events_from_text(
            db,
            "Полный исходный текст",
            "https://example.test/source/1",
        )
    finally:
        await db.close()

    assert result == []
    assert result.source_decision is decision
    assert result.disposition is decision.disposition
    assert result.evidence_complete is decision.evidence_complete


@pytest.mark.asyncio
async def test_direct_poster_media_passes_truthful_attachment_cardinality(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    seen: dict[str, object] = {}

    async def fake_recognize(*_args, **_kwargs):
        return [], 0, None

    async def fake_parse(text, *_args, **kwargs):
        seen.update(kwargs)
        manifest = EvidenceManifest.complete_source(
            text,
            kwargs.get("poster_texts"),
            attachment_count=int(kwargs["attachment_count"]),
        )
        return SourceParseDecision(
            [],
            disposition=SourceDisposition.CONFIRMED_NO_EVENT,
            evidence_manifest=manifest,
            evidence_complete=True,
        )

    monkeypatch.setattr(main.poster_ocr, "recognize_posters", fake_recognize)
    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse)
    try:
        result = await main.add_events_from_text(
            db,
            "Подпись",
            "https://example.test/source/poster",
            poster_media=[PosterMedia(data=b"poster", name="poster.jpg")],
        )
    finally:
        await db.close()

    assert seen["attachment_count"] == 1
    assert result.source_decision.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.source_decision.retry_reason is SourceParseRetryReason.EVIDENCE_INCOMPLETE
    assert result.source_decision.evidence_manifest.unavailable_attachment_count == 1
