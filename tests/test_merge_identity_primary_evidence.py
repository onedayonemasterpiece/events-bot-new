"""A generated summary cannot prove original-artist versus tribute identity."""
import json
from copy import deepcopy
from pathlib import Path

import pytest

import smart_event_update as seu
from models import Event


CASES = json.loads((Path(__file__).parent / 'fixtures/merge-identity-primary-evidence.json').read_text())['cases']


@pytest.mark.asyncio
@pytest.mark.parametrize('case', CASES, ids=lambda case: case['case'])
async def test_all_merge_identity_citations_must_have_primary_source_evidence(monkeypatch, case):
    payload = deepcopy(case['candidate_payload'])
    payload['posters'] = [seu.PosterCandidate(**p) for p in payload.get('posters', [])]
    candidate = seu.EventCandidate(**payload)
    existing = Event(**case['existing_event'])

    async def ask(*args, **kwargs):
        assert kwargs['label'] == 'merge_identity_gate'
        return deepcopy(case['model_response'])

    monkeypatch.setattr(seu, 'SMART_UPDATE_LLM_DISABLED', False)
    monkeypatch.setattr(seu, '_ask_gemma_json', ask)
    result = await seu._llm_merge_identity_gate(
        candidate, existing, conflicting_anchor_fields=case['conflicting_anchor_fields'],
        poster_texts=case['poster_texts'],
    )
    assert (result is not None) == case['expected_after_is_decision']
    if result:
        assert result['source_grounded_evidence'] == case['model_response']['source_grounded_evidence']
