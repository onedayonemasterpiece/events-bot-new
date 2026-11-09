from datetime import datetime, timezone

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


@pytest.mark.asyncio
@pytest.mark.parametrize('published', [1794085560, datetime.fromtimestamp(1794085560, timezone.utc), None])
async def test_source_publication_anchor_reaches_primary_and_verifier(monkeypatch, published):
    source = '[Репост $.copy_history[0]]\nПриглашаем завтра на фестиваль Garazhka в Понарте'
    captured = {}
    original = vk_intake.require_main_attr

    async def parse(prompt, **kwargs):
        captured.update(prompt=prompt, **kwargs)
        return SourceParseDecision.retry(SourceParseRetryReason.TECHNICAL_ERROR)

    monkeypatch.setattr(vk_intake, 'require_main_attr', lambda name: parse if name == 'parse_event_via_llm' else original(name))
    await vk_intake.build_event_drafts_from_vk(source, publish_ts=published)

    assert captured['semantic_source_text'] == source
    if published is None:
        assert 'published_at' not in captured
        assert 'Дата публикации источника:' not in captured['prompt']
    else:
        anchor = datetime.fromisoformat(captured['published_at'])
        assert int(anchor.timestamp()) == 1794085560
        assert captured['published_at'] in captured['prompt']
        assert 'Относительные даты считай от даты публикации' in captured['prompt']
