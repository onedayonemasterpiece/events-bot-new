"""One normal producer/router/reviewer regression; no model, DB or external I/O."""
import copy
from types import SimpleNamespace

import pytest
import smart_event_update as su
from location_reference import match_known_venue
from source_parsing.telegram.handlers import _build_candidate



@pytest.mark.asyncio
async def test_original29589_inferred_unconfigured_venue_reaches_existing_reviewer(monkeypatch):
    # Source: https://t.me/klassster/20197. Extraction fields were recorded
    # before producer normalization; the response was saved from the existing
    # reviewer. Keep this fixture independent of live services and local artifacts.
    payload = {'title': 'Творческая встреча, посвященная 100-летию женской моды',
     'date': '2026-11-21',
     'time': '18:00',
     'city': 'Калининград',
     'event_type': 'встреча',
     'source_text': '🌟Друзья, мы продолжаем серию светских вечеров!\n'
                    '\n'
                    '🌟Приглашаем вас на творческую встречу, посвященную 100-летию женской моды и '
                    'величайшим дизайнерам прошлого столетия. \n'
                    '\n'
                    'В программе: \n'
                    '•показ моделей 40-60-х годов с бесценными комментариями Евгения Соколовского '
                    '(профессиональный портной, легендарный учитель, историк моды)\n'
                    '\n'
                    '•фото для всех желающих в фотостудии автопортрета ФОТКАЙ ДЕТКА\n'
                    '\n'
                    '🎫Вход 1500₽\n'
                    'Парный билет 1+1= 2000₽\n'
                    '21 ноября (суббота)\n'
                    'Начало 18:00\n'
                    '\n'
                    'Для записи пишите в личные сообщения. \n'
                    'Количество мест ограничено 🙌🏼\n'
                    '\n'
                    'Тел. 50-70-30\n'
                    '\n'
                    '#афишакалининград #кудасходить #калининград #историямоды #фото',
     'source_url': 'https://t.me/klassster/20197',
     'trust_level': 'low',
     'location_name': 'творческую встречу',
     'posters': [{'sha256': 'd600c7628dfd4e074d0d68e408d5fdc14870c4ef2b3a208a9a36b09ada52566a',
                  'ocr_text': 'СВЕТСКИЙ ВЕЧЕР 100-ЛЕТИЕ ЖЕНСКОЙ МОДЫ 21 НОЯБРЯ 18:00 ВХОД 1500Р '
                              'ПАРНЫЙ БИЛЕТ 2000Р В ПРОГРАММЕ: ПОКАЗ МОДЕЛЕЙ ПЕРИОДА 40-60-Х С '
                              'КОММЕНТАРИЯМИ ЕВГЕНИЯ СОКОЛОВСКОГО ФОТО ДЛЯ ВСЕХ ЖЕЛАЮЩИХ В '
                              'ФОТОСТУДИИ АВТОПОРТРЕТА ФОТКАЙ ДЕТКА @FOTKAIDETKASTUDIO тел. '
                              '50-70-30 ФОТКАЙ ДЕТКА ГАРАЖНАЯ, 2 КОРПУС А 3 ЭТАЖ КАБ. А309',
                  'ocr_title': 'СВЕТСКИЙ ВЕЧЕР'}]}
    observed_name = 'ФОТКАЙ ДЕТКА'
    observed_address = 'Гаражная, 2, корпус А, 3 этаж, каб. А309'
    event = {key: value for key, value in payload.items() if key not in ('posters', 'metrics')}
    event['location_name'] = observed_name
    event['location_address'] = observed_address
    message = {'source_username': 'klassster', 'message_id': 20197,
               'source_link': payload['source_url'], 'text': payload['source_text'],
               'events': [event], 'posters': payload['posters']}
    source = SimpleNamespace(default_location=None, default_ticket_link=None,
                             trust_level=payload['trust_level'])
    candidate = _build_candidate(source, message, event)
    # Existing producer behavior and diagnostics remain unchanged by routing.
    assert candidate.location_name == payload['location_name'] == 'творческую встречу'
    assert candidate.location_address is None
    assert 'tg_extracted_location_name' in candidate.metrics
    assert candidate.metrics['tg_extracted_location_name'] is None
    assert not candidate.metrics['tg_default_location']
    assert match_known_venue(candidate.location_name, city=candidate.city) is None
    before_metrics = copy.deepcopy(candidate.metrics)
    before_source = candidate.source_text
    before_posters = copy.deepcopy(candidate.posters)
    accepted_response = {'decision': 'repair',
     'confidence': 1.0,
     'location_name': 'ФОТКАЙ ДЕТКА',
     'location_address': 'ГАРАЖНАЯ, 2 КОРПУС А 3 ЭТАЖ КАБ. А309',
     'city': 'Калининград',
     'evidence_quote': 'ФОТКАЙ ДЕТКА ГАРАЖНАЯ, 2 КОРПУС А 3 ЭТАЖ КАБ. А309',
     'reason_short': 'В источнике указано конкретное место проведения (фотостудия) и адрес, '
                     'которые отсутствовали в кандидате.'}
    calls = []

    async def recorded_response(prompt, schema, **kwargs):
        # The actual saved response repairs the actual inferred prose venue;
        # the fixture does not replace producer output with the desired venue.
        assert kwargs['label'] == 'location_grounding_review'
        assert 'творческую встречу' in prompt
        assert accepted_response['evidence_quote'] in candidate.posters[0].ocr_text
        calls.append(kwargs['label'])
        return copy.deepcopy(accepted_response)

    monkeypatch.setattr(su, 'SMART_UPDATE_LLM_DISABLED', False)
    monkeypatch.setattr(su, '_ask_gemma_json', recorded_response)
    needs_review, trigger = su._candidate_needs_llm_location_grounding_review(candidate)
    assert needs_review, (needs_review, trigger)
    accepted, reason = await su._llm_review_candidate_location_grounding(candidate, trigger_reason=trigger)
    assert (accepted, reason) == (True, 'llm_repair')
    assert calls == ['location_grounding_review']
    assert candidate.location_name == accepted_response['location_name'] == 'ФОТКАЙ ДЕТКА'
    assert candidate.location_address == accepted_response['location_address']
    assert candidate.city == accepted_response['city'] == 'Калининград'
    assert candidate.metrics == before_metrics
    assert candidate.source_text == before_source
    assert candidate.posters == before_posters
