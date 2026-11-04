import pytest

import vk_intake
from source_parse_contract import SourceParseDecision, SourceParseRetryReason


@pytest.mark.asyncio
@pytest.mark.parametrize('instruction', ['Импортируется только сеанс 2027-01-04 в 11:00.', None])
async def test_operator_scope_reaches_semantic_parser_without_changing_source(monkeypatch, instruction):
    source = 'Дата: 2027-01-04\nВремя: 11:00\nОписание: с 24 декабря по 6 января в 11:00 и 13:00.'
    captured = {}
    original = vk_intake.require_main_attr

    async def parse(prompt, **kwargs):
        captured.update(prompt=prompt, **kwargs)
        return SourceParseDecision.retry(SourceParseRetryReason.TECHNICAL_ERROR)

    monkeypatch.setattr(vk_intake, 'require_main_attr', lambda name: parse if name == 'parse_event_via_llm' else original(name))
    await vk_intake.build_event_drafts_from_vk(source, source_name='theatre:tretyakov', operator_extra=instruction)

    assert (captured.get('source_channel'), captured.get('source_context')) == (
        'theatre:tretyakov', {'operator_extra': instruction} if instruction else None,
    )
    assert captured['semantic_source_text'] == source
    if instruction:
        assert instruction in captured['prompt']
        assert instruction not in captured['semantic_source_text']
