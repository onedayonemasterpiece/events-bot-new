"""A sibling caption must not replace the LLM-selected occurrence evidence."""
import json
from copy import deepcopy
from pathlib import Path
import pytest
import smart_event_update as su
from models import Event
CASES=json.loads((Path(__file__).parent/'fixtures/newyear-occurrence-identity.json').read_text())['cases']

@pytest.mark.asyncio
@pytest.mark.parametrize('case',CASES,ids=lambda x:x['case'])
async def test_merge_identity_consumes_selected_occurrence_instead_of_sibling_caption(monkeypatch,case):
    packet=deepcopy(case['candidate_payload'])
    packet['posters']=[su.PosterCandidate(**x) for x in packet['posters']]
    candidate=su.EventCandidate(**packet)
    candidate.occurrence_scope_text=case['selected_scope']
    async def ask(prompt,*args,**kwargs):
        assert kwargs['label']=='merge_identity_gate'
        payload=json.loads(prompt.split('Данные:\n',1)[1])
        assert payload['candidate']['source_text']==case['selected_scope']
        assert payload['candidate']['occurrence_scope_text']==case['selected_scope']
        assert payload['candidate']['source_context']==packet['source_text']
        assert payload['candidate']['date']=='2026-12-26'
        assert payload['candidate']['time']=='12:30'
        return {'action':'allow_merge','relation':'source_update','confidence':0.99,
                'source_grounded_evidence':['Агропарк «Некрасово поле»']}
    monkeypatch.setattr(su,'SMART_UPDATE_LLM_DISABLED',False)
    monkeypatch.setattr(su,'_ask_gemma_json',ask)
    result=await su._llm_merge_identity_gate(candidate,Event(**case['existing_event']),conflicting_anchor_fields={})
    assert result['action']=='allow_merge'
