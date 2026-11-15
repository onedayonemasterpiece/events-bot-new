"""Packet29477: source OCR survives an empty publishing-media assignment."""
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import main
import smart_event_update as seu
import vk_intake
from poster_media import PosterMedia


@pytest.mark.asyncio
async def test_unassigned_source_ocr_reaches_identity_without_publishing_gallery(monkeypatch):
    raw = json.loads((Path(__file__).parent / 'fixtures/vk-source-ocr29477.json').read_text())

    async def parse(*args, **kwargs):
        return raw['decision']

    async def assign(**kwargs):
        return {i: [] for i in range(len(kwargs['posters']))}

    monkeypatch.setattr(main, 'parse_event_via_llm', parse)
    monkeypatch.setattr(vk_intake, '_llm_assign_source_posters_to_drafts', assign)
    drafts, _ = await vk_intake.build_event_drafts_from_vk(
        raw['source_text'],
        publish_ts=datetime(2026, 11, 15, tzinfo=timezone.utc),
        poster_media=[PosterMedia(data=b'', name=f'{i}.jpg', digest=p['sha256'],
                                  ocr_text=p['text'], ocr_title=p['title'])
                      for i, p in enumerate(raw['ocr'])],
    )
    restored = vk_intake.DraftParseResult.from_receipt_payload(drafts.to_receipt_payload())
    assert {d.date for d in restored} == {'2026-11-20', '2026-11-21'}
    captured = []

    async def identity(db, candidate, **kwargs):
        captured.append(candidate)
        return SimpleNamespace(is_accepted=False)

    monkeypatch.setattr(seu, 'smart_event_update', identity)
    for draft in restored:
        assert not draft.poster_media and not draft.allow_raw_photo_fallback
        await vk_intake.persist_event_and_pages(
            draft, ['https://example.invalid/unassigned.jpg'], None,
            source_post_url=raw['provenance']['source_url'],
        )
    candidate = next(c for c in captured if c.date == '2026-11-21')
    assert candidate.posters == []
    assert candidate.occurrence_scope_text is None  # No automatic semantic selection.
    assert raw['source_text'] in candidate.source_text
    assert '21.11' in candidate.source_text
    assert seu._candidate_needs_llm_occurrence_scope_review(candidate)

    async def scope(prompt, schema, **kwargs):
        assert '21.11' in prompt
        return {'decision': 'scoped', 'confidence': .95,
                'selected_excerpts': [raw['ocr'][0]['text']]}

    monkeypatch.setattr(seu, 'SMART_UPDATE_LLM_DISABLED', False)
    monkeypatch.setattr(seu, '_ask_gemma_json', scope)
    assert await seu._llm_scope_candidate_occurrence(candidate) == (True, 'llm_scoped')
    assert '20 ноября' not in candidate.occurrence_scope_text
    sibling = next(c for c in captured if c.date == '2026-11-20')
    assert await seu._llm_scope_candidate_occurrence(sibling) == (False, 'llm_reject_missing_date')
    assert sibling.occurrence_scope_text is None
    # Preserving evidence must not turn an invalid relation-as-reason into acceptance.
    decision = {'action': 'keep', 'relation': 'distinct_occurrence',
                'reason_code': 'distinct_occurrence', 'confidence': .95,
                'source_grounded_evidence': ['21.11'], 'blocking_conflicts': ['different_date']}
    assert seu._dedup_adjudicator_final_result(candidate, [], decision).action.value == 'FINAL_RETRY'


@pytest.mark.asyncio
@pytest.mark.parametrize('event_count,with_ocr', [(1, True), (2, False)])
async def test_existing_single_child_and_text_only_carriers_stay_unchanged(monkeypatch, event_count, with_ocr):
    raw = json.loads((Path(__file__).parent / 'fixtures/vk-source-ocr29477.json').read_text())

    async def parse(*args, **kwargs):
        return {**raw['decision'], 'events': raw['decision']['events'][:event_count]}

    monkeypatch.setattr(main, 'parse_event_via_llm', parse)
    drafts, _ = await vk_intake.build_event_drafts_from_vk(
        raw['source_text'], publish_ts=datetime(2026, 11, 15, tzinfo=timezone.utc),
        poster_media=[PosterMedia(data=b'', name='source.jpg', ocr_text=raw['ocr'][0]['text'])]
                     if with_ocr else [],
    )
    assert len(drafts) == event_count
    assert all(draft.source_text == raw['source_text'] for draft in drafts)
