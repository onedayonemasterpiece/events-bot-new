"""Mixed album photos reach existing visual review instead of an OCR-only loss."""
import hashlib
import io
from types import SimpleNamespace

import pytest
from PIL import Image
from sqlmodel import select

import event_media
from db import Database
from models import Event, EventPoster
from smart_event_update import _apply_posters
from source_parsing.telegram.handlers import _build_candidate


def build_album(mode='mixed'):
    source = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level='high')
    child = dict(title='Линии глины', date='2099-11-20', time='15:00', location_name='Мастерская')
    payloads = {}
    posters = []
    for index, text in enumerate(['Лекция о мигрени 3 марта 10:00',
                                  'Левитан художественная мастерская 20 ноября 15:00', '', '']):
        out = io.BytesIO()
        Image.new('RGB', (160 + index * 20, 240), (index * 65, 60, 80)).save(out, format='PNG')
        url = f'https://static.example/album-{index}.png'
        payloads[url] = out.getvalue()
        posters.append(dict(supabase_url=url, sha256=hashlib.sha256(out.getvalue()).hexdigest(),
                            ocr_text=text, ocr_title=None))
    if mode == 'photos':
        posters = posters[2:]
    elif mode == 'wrong_text':
        posters = posters[:2]
    if mode == 'assigned':
        child['posters'] = [posters[2]]
    message = dict(source_username='workshop', message_id=1, text='Линии глины и лекция',
                   posters=posters, events=[child] if mode == 'single' else [child, dict(title='Лекция')])
    return _build_candidate(source, message, child), payloads, message


@pytest.mark.parametrize('mode,count', [('mixed', 2), ('photos', 2), ('single', 4),
                                         ('assigned', 1), ('wrong_text', 0)])
def test_album_intake_preserves_photo_candidates_and_existing_ocr_assignment(mode, count):
    candidate, _, _ = build_album(mode)
    assert len(candidate.posters) == count
    if mode in ('mixed', 'photos', 'assigned'):
        assert all(not p.ocr_text for p in candidate.posters)


@pytest.mark.asyncio
@pytest.mark.parametrize("unavailable", [False, True])
async def test_mixed_album_photo_enters_smartupdate_and_existing_review_rejects_foreign(tmp_path, monkeypatch, unavailable):
    candidate, payloads, _ = build_album()
    assert len(candidate.posters) == 2  # Before fix: zero candidates reach SmartUpdate.
    db = Database(str(tmp_path / 'mixed.sqlite'))
    await db.init()
    monkeypatch.setattr(event_media, 'event_media_require_cdn', lambda: False)
    async def download(poster):
        return event_media.DownloadedPoster(payloads[poster.supabase_url], 'image/png', poster.supabase_url)
    calls = []
    async def review(**kwargs):
        calls.append(kwargs)
        assert kwargs['event'].title == 'Линии глины'
        if unavailable:
            from google_ai.exceptions import ProviderError
            raise ProviderError('provider unavailable', status_code=503)
        # Existing pair contract: the second no-text photograph is unrelated.
        return dict(decision='distinct', duplicate_kind='none', confidence=0.99,
                    semantic_conflict=True, canonical_side='left', reason_code='right_unrelated_to_event')
    monkeypatch.setattr(event_media, '_download_poster', download)
    monkeypatch.setattr(event_media, '_call_reviewer', review)
    try:
        async with db.get_session() as session:
            event = Event(title=candidate.title, date=candidate.date, time=candidate.time,
                          description='Мастер-класс керамики', source_text=candidate.source_text,
                          location_name=candidate.location_name)
            session.add(event)
            await session.flush()
            eid = event.id
            await _apply_posters(session, eid, candidate.posters)
            await session.commit()
        await event_media.review_next_event_media_pair(eid, db)
        assert len(calls) == 1
        async with db.get_session() as session:
            rows = (await session.execute(select(EventPoster).where(EventPoster.event_id == eid)
                                          .order_by(EventPoster.id))).scalars().all()
            assert [p.review_status for p in rows] == [event_media.APPROVED,
                event_media.APPROVED if unavailable else event_media.REJECTED]
            expected = candidate.posters if unavailable else candidate.posters[:1]
            assert (await session.get(Event, eid)).photo_urls == [p.supabase_url for p in expected]
            if unavailable:
                # Existing owner-approved fail-open cannot guarantee event relevance.
                assert all(p.review_reason == 'optional_visual_review_unavailable' for p in rows)
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('assigned', [False, True, 'photos'])
async def test_normal_import_assignment_keeps_photos_without_overriding_monitor(tmp_path, monkeypatch, assigned):
    import json
    from smart_event_update import SmartUpdateResult
    from source_parsing.telegram import handlers
    from models import TelegramSource
    _, payloads, message = build_album()
    message['events'][1].update(title='Левитан художественная мастерская',
                               date='2099-11-20', time='15:00', location_name='Мастерская')
    if assigned == 'photos':
        message['posters'] = message['posters'][2:]
    if assigned is True:
        message['events'][0]['posters'] = [message['posters'][2]]
        message['events'][1]['posters'] = [message['posters'][1]]
    path = tmp_path / 'telegram_results.json'
    path.write_text(json.dumps(dict(schema_version=2, run_id='mixed-test', messages=[message])))
    db = Database(str(tmp_path / 'intake.sqlite'))
    await db.init()
    async with db.get_session() as session:
        session.add(TelegramSource(username='workshop', title='Мастерская', enabled=True))
        await session.commit()
    import google_ai
    import main
    sends = []
    class Client:
        def __init__(self, **kwargs):
            assert kwargs['consumer'] == 'event_media_review'
        async def generate_content_async(self, **kwargs):
            sends.append(kwargs)
            kwargs['attempt_observer']({'requested_model': kwargs['model']})
            ids = [int(p.split('=')[1]) for p in kwargs['prompt']
                   if isinstance(p, str) and p.startswith('poster_id=')]
            assert len(ids) == 3
            assert len([p for p in kwargs['prompt'] if isinstance(p, dict)]) == 3
            assert 'images' in kwargs['generation_config']['response_json_schema']['properties']
            return json.dumps({'images': [dict(poster_id=pid, duplicate_of_id=None,
                decision='distinct', duplicate_kind='none', confidence=0.99,
                semantic_conflict=i == 2, canonical_side='unknown', reason_code='foreign' if i == 2 else 'relevant')
                for i, pid in enumerate(ids)]}), None
    async def download(poster):
        return event_media.DownloadedPoster(payloads[poster.supabase_url], 'image/png', poster.supabase_url)
    monkeypatch.setattr(google_ai, 'GoogleAIClient', Client)
    monkeypatch.setattr(google_ai, 'SecretsProvider', lambda: object())
    monkeypatch.setattr(main, 'get_supabase_client', lambda: None)
    monkeypatch.setattr(event_media, '_download_poster', download)
    captured = []
    saved_ids = []
    async def smart_update(db_arg, candidate, **kwargs):
        captured.append(candidate)
        async with db_arg.get_session() as session:
            event = Event(title=candidate.title, date=candidate.date, time=candidate.time,
                          description='Описание', source_text=candidate.source_text,
                          location_name=candidate.location_name)
            session.add(event)
            await session.flush()
            await _apply_posters(session, event.id, candidate.posters)
            saved_ids.append(event.id)
            await session.commit()
        return SmartUpdateResult(status='created', event_id=event.id)
    async def no_network(**kwargs):
        return []
    monkeypatch.setattr(handlers, 'smart_event_update', smart_update)
    monkeypatch.setattr(handlers, '_fallback_fetch_posters_from_public_tg_page', no_network)
    try:
        report = await handlers.process_telegram_results(path, db)
        assert report.events_errored == 0
        assert len(captured) == 2
        urls = [{p.supabase_url for p in c.posters} for c in captured]
        base = 'https://static.example/album-'
        if assigned == 'photos':
            assert urls == [{base + '2.png'}, {base + '3.png'}]
        else:
            expected_first = {base + '2.png'} if assigned else {base + '2.png', base + '3.png'}
            assert urls == [expected_first, {base + '1.png', base + '2.png', base + '3.png'}]
        if assigned != 'photos':
            await event_media.review_next_event_media_pair(saved_ids[1], db)
            assert len(sends) == 1
            async with db.get_session() as session:
                assert (await session.get(Event, saved_ids[1])).photo_urls == [base + '1.png', base + '2.png']
                rows = (await session.execute(select(EventPoster).where(
                    EventPoster.event_id == saved_ids[1]).order_by(EventPoster.id))).scalars().all()
                assert [p.review_status for p in rows] == [event_media.APPROVED, event_media.APPROVED,
                                                         event_media.REJECTED]
    finally:
        await db.close()
