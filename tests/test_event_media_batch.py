"""Five source images share one existing provider request and pair ledger."""
import hashlib
import io
import json
import re

import pytest
from PIL import Image, ImageDraw
from sqlmodel import select

from db import Database
import event_media
from models import Event, EventPoster, EventMediaPairReview
from smart_event_update import PosterCandidate, _apply_posters


@pytest.mark.asyncio
@pytest.mark.parametrize('conflict', [False, True])
@pytest.mark.parametrize('count', [5, 8])
@pytest.mark.parametrize('model', ['gemini-3.1-flash-lite', 'models/gemma-4-31b-it'])
async def test_gallery_one_send_source_images(tmp_path, monkeypatch, conflict, count, model):
    import google_ai
    import main
    monkeypatch.setenv('EVENT_MEDIA_REVIEW_PRIMARY_MODEL', model)
    db = Database(str(tmp_path / 'batch.sqlite'))
    await db.init()
    payloads = {}
    for index in range(count):
        image = Image.new('RGB', (200 + index * 20, 300), (index * 25, 50, 80))
        ImageDraw.Draw(image).rectangle((index * 10, 20, index * 10 + 60, 100), fill='white')
        out = io.BytesIO()
        image.save(out, format='PNG')
        payloads[f'https://static.example/{index}.png'] = out.getvalue()
    sends = []
    ids = []
    class Client:
        def __init__(self, **kwargs):
            assert kwargs['consumer'] == 'event_media_review'
        async def generate_content_async(self, **kwargs):
            sends.append(kwargs)
            if model.startswith('models/gemma'):
                item = kwargs['generation_config']['response_schema']['properties']['images']['items']
                assert 'additionalProperties' not in item
                assert item['properties']['duplicate_of_id'] == {'type': 'integer', 'nullable': True}
            kwargs['attempt_observer']({'requested_model': kwargs['model']})
            results = []
            for pid in [int(p.split('=')[1]) for p in kwargs['prompt'] if isinstance(p, str) and p.startswith('poster_id=')]:
                duplicate = pid == ids[1]
                results.append({'poster_id': pid, 'duplicate_of_id': ids[4] if duplicate else None,
                    'decision': 'duplicate' if duplicate else 'distinct',
                    'duplicate_kind': 'mirror_reencode' if duplicate else 'none',
                    'confidence': 0.99, 'semantic_conflict': conflict and pid == ids[3],
                    'canonical_side': 'unknown', 'reason_code': 'same_photo' if duplicate else 'different_photos'})
            return json.dumps({'images': results}), None
    async def download(poster):
        return event_media.DownloadedPoster(payloads[poster.supabase_url], 'image/png', poster.supabase_url)
    monkeypatch.setattr(google_ai, 'GoogleAIClient', Client)
    monkeypatch.setattr(google_ai, 'SecretsProvider', lambda: object())
    monkeypatch.setattr(main, 'get_supabase_client', lambda: None)
    monkeypatch.setattr(event_media, '_download_poster', download)
    async with db.get_session() as session:
        event = Event(title='Театральная постановка', date='2099-01-09', time='19:00', description='Описание',
                      location_name='Театр', source_text='Афиша и фотографии спектакля')
        session.add(event)
        await session.flush()
        eid = event.id
        await _apply_posters(session, eid, [PosterCandidate(supabase_url=url,
            sha256=hashlib.sha256(data).hexdigest()) for url, data in payloads.items()])
        await session.commit()
        posters = (await session.execute(select(EventPoster).where(EventPoster.event_id == eid)
                                        .order_by(EventPoster.id))).scalars().all()
        ids.extend(p.id for p in posters)
    try:
        await event_media.review_next_event_media_pair(eid, db)
        assert len(sends) == 1
        images = [p['inline_data'] for p in sends[0]['prompt'] if isinstance(p, dict)]
        assert len(images) == count
        for image in images:
            assert image['mime_type'] == 'image/jpeg'
            with Image.open(io.BytesIO(image['data'])) as decoded:
                assert max(decoded.size) <= 1200
        async with db.get_session() as session:
            pairs = (await session.execute(select(EventMediaPairReview).where(
                EventMediaPairReview.event_id == eid))).scalars().all()
            assert pairs and all(p.status == 'resolved' for p in pairs)
            assert all(p.provider_calls == 1 for p in pairs)
            posters = (await session.execute(select(EventPoster).where(EventPoster.event_id == eid)
                                            .order_by(EventPoster.id))).scalars().all()
            assert posters[1].review_status == event_media.DUPLICATE
            assert posters[1].duplicate_of_id == posters[4].id
            assert posters[4].review_status == event_media.APPROVED
            assert posters[3].review_status == ('rejected' if conflict else event_media.APPROVED)
            saved = await session.get(Event, eid)
            assert len(saved.photo_urls) == count - 1 - int(conflict)
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['provider', 'download', 'cached_decode'])
async def test_fourteen_sources_transport_failure_does_not_hold_selected_gallery(tmp_path, monkeypatch, failure):
    from models import JobOutbox, JobTask, JobStatus
    from google_ai.exceptions import ProviderError
    from unittest.mock import AsyncMock
    import main
    db = Database(str(tmp_path / 'fourteen.sqlite'))
    await db.init()
    monkeypatch.setenv('EVENT_MEDIA_REQUIRE_CDN', '0')
    monkeypatch.setattr(main, '_event_has_existing_managed_vk_post', AsyncMock(return_value=False))
    async def download(poster):
        if failure == 'download':
            raise TimeoutError('source download timed out')
        if failure == 'cached_decode':
            return event_media.DownloadedPoster(b'not an image', 'image/jpeg', poster.supabase_url)
        index = int(poster.supabase_url.rsplit('/', 1)[1].split('.')[0])
        image = Image.new('RGB', (100, 150), (index * 15, 10, 80))
        out = io.BytesIO()
        image.save(out, format='PNG')
        return event_media.DownloadedPoster(out.getvalue(), 'image/png', poster.supabase_url)
    if failure == 'cached_decode':
        out = io.BytesIO()
        Image.new('RGB', (100, 150)).save(out, format='PNG')
        cached = event_media.compute_image_fingerprints(out.getvalue())
        monkeypatch.setattr(event_media, '_review_fingerprints', lambda *_: cached)
    calls = []
    async def provider(**kwargs):
        calls.append(kwargs['model'])
        kwargs['attempt_observer']({'requested_model': kwargs['model']})
        raise ProviderError(error_type='ServerError', status_code=503, retryable=True)
    monkeypatch.setattr(event_media, '_download_poster', download)
    monkeypatch.setattr(event_media, '_call_reviewer', provider)
    try:
        async with db.get_session() as session:
            ev = Event(title='Театр', description='', source_text='', date='2099-01-09',
                       time='19:00', location_name='Театр', linked_event_ids=[1234])
            session.add(ev)
            await session.flush()
            eid = ev.id
            await _apply_posters(session, eid, [PosterCandidate(supabase_url=f'https://static.example/{i}.png',
                sha256=hashlib.sha256(str(i).encode()).hexdigest()) for i in range(14)])
            await session.commit()
        await event_media.review_next_event_media_pair(eid, db)
        async with db.get_session() as session:
            posters = (await session.execute(select(EventPoster).where(EventPoster.event_id == eid)
                                            .order_by(EventPoster.id))).scalars().all()
            ev = await session.get(Event, eid)
            if failure in ('download', 'cached_decode'):
                assert not calls
                assert len(ev.photo_urls) == 1
                assert all(p.review_status == event_media.PENDING_REVIEW for p in posters[1:])
                return
            assert len(calls) == 2  # Existing primary then configured fallback.
            assert len(ev.photo_urls) == 8
            assert all(p.review_status == event_media.APPROVED for p in posters[:8])
            assert all(p.review_status == event_media.PENDING_REVIEW for p in posters[8:])
            assert all(p.media_semantic_status != 'classified' for p in posters)
            for job in (await session.execute(select(JobOutbox).where(
                JobOutbox.event_id == eid, JobOutbox.task == JobTask.event_media_review))).scalars():
                job.status = JobStatus.done
                session.add(job)
            await session.commit()
        await main.schedule_event_update_tasks(db, ev)
        async with db.get_session() as session:
            vk = (await session.execute(select(JobOutbox).where(
                JobOutbox.event_id == eid, JobOutbox.task == JobTask.vk_sync))).scalar_one()
            assert f'event_media_review:{eid}' not in (vk.depends_on or '')
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('limit,source_count', [(10, 7), (9, 7), (3, 1)])
async def test_initial_gallery_reserves_both_cards_inside_nine(monkeypatch, limit, source_count):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    import main
    import vk_event_relations
    monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '1')
    monkeypatch.setattr(main, 'VK_PHOTOS_ENABLED', True)
    monkeypatch.setattr(main, 'VK_MAX_ATTACHMENTS', limit)
    monkeypatch.setattr(main, '_resolve_event_festival', AsyncMock(return_value=None))
    monkeypatch.setattr(main, '_event_primary_publish_source_url', AsyncMock(return_value='https://example/source'))
    monkeypatch.setattr(main, 'resolve_vk_location_marker_for_event', AsyncMock(return_value=SimpleNamespace(applied=False)))
    monkeypatch.setattr(main, '_event_media_ledger_intentionally_empty', AsyncMock(return_value=False))
    monkeypatch.setattr(vk_event_relations, 'render_card', lambda *args, **kwargs: b'card')
    sources = [f'https://static.example/{i}.png' for i in range(14)]
    class ReachedUpload(Exception):
        pass
    async def upload(group, urls, *args, relation_cards, **kwargs):
        assert urls == sources[:source_count]
        assert len(relation_cards) == 2
        assert len(urls) + len(relation_cards) == min(9, limit)
        raise ReachedUpload
    monkeypatch.setattr(main, '_upload_vk_event_gallery', upload)
    ev = Event(id=99, title='Theatre', description='', source_text='', date='2099-01-09',
               time='19:00', location_name='Theatre', photo_urls=sources)
    with pytest.raises(ReachedUpload):
        await main.sync_vk_source_post(ev, 'Text', None, None,
                                      relations={'dates': [{'id': 1}], 'related': [{'id': 2}]})


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['low_confidence', 'low_confidence_failure', 'invalid_duplicate', 'duplicate_ids', 'exact_semantic', 'drift'])
async def test_gallery_unconfirmed_verdicts_and_current_source_identity(tmp_path, monkeypatch, mode):
    db = Database(str(tmp_path / 'verdict.sqlite'))
    await db.init()
    payloads = []
    for i in range(5):
        out = io.BytesIO()
        Image.new('RGB', (100, 150), (i * 30, 80, 20)).save(out, format='PNG')
        payloads.append(out.getvalue())
    if mode == 'exact_semantic':
        payloads[1] = payloads[0]
    async def download(poster):
        i = int(poster.supabase_url.rsplit('/', 1)[1].split('.')[0])
        return event_media.DownloadedPoster(payloads[i], 'image/png', poster.supabase_url)
    calls = []
    async def review(**kwargs):
        calls.append(kwargs['model'])
        kwargs['attempt_observer']({'requested_model': kwargs['model']})
        if mode == 'low_confidence_failure' and len(calls) == 2:
            from google_ai.exceptions import ProviderError
            raise ProviderError(error_type='ServerError', status_code=503, retryable=True)
        gallery = kwargs['gallery']
        assert len(gallery) == (4 if mode == 'exact_semantic' else 5)
        if mode == 'drift':
            async with db.get_session() as session:
                row = await session.get(EventPoster, gallery[1][0].id)
                row.supabase_url = 'https://static.example/upgraded-original.png'
                row.pixel_sha256 = 'a' * 64
                session.add(row)
                await session.commit()
        images = [dict(poster_id=row.id, duplicate_of_id=None,
            decision='duplicate' if mode == 'invalid_duplicate' else 'distinct', duplicate_kind='none',
            confidence=0.2 if mode in ('low_confidence', 'low_confidence_failure') else 0.99,
            semantic_conflict=mode in ('exact_semantic', 'drift', 'low_confidence_failure') and i == 0,
            canonical_side='unknown', reason_code='visual_result') for i, (row, media) in enumerate(gallery)]
        if mode == 'duplicate_ids':
            images.append(images[0])
        return {'images': images}
    monkeypatch.setattr(event_media, '_download_poster', download)
    monkeypatch.setattr(event_media, '_call_reviewer', review)
    try:
        async with db.get_session() as session:
            ev = Event(title='Theatre', description='', source_text='', date='2099-01-09',
                       time='19:00', location_name='Theatre')
            session.add(ev)
            await session.flush()
            eid = ev.id
            await _apply_posters(session, eid, [PosterCandidate(supabase_url=f'https://static.example/{i}.png',
                sha256=hashlib.sha256(f'source-{i}'.encode()).hexdigest()) for i in range(5)])
            await session.commit()
        await event_media.review_next_event_media_pair(eid, db)
        async with db.get_session() as session:
            rows = (await session.execute(select(EventPoster).where(EventPoster.event_id == eid)
                                          .order_by(EventPoster.id))).scalars().all()
            ev = await session.get(Event, eid)
            if mode == 'exact_semantic':
                assert len(calls) == 1
                assert rows[0].review_status == rows[1].review_status == event_media.REJECTED
                assert len(ev.photo_urls) == 3
            elif mode == 'drift':
                assert rows[1].supabase_url.endswith('upgraded-original.png')
                assert rows[1].pixel_sha256 == 'a' * 64
                assert all(row.review_status == event_media.PENDING_REVIEW for row in rows[1:])
                assert rows[0].review_status == event_media.APPROVED
            else:
                assert len(calls) == 2
                assert len(ev.photo_urls) == 5
                assert all(row.review_status == event_media.APPROVED for row in rows)
                assert all(row.review_reason == ('optional_visual_review_unavailable' if mode == 'low_confidence_failure'
                                                  else 'optional_visual_review_uncertain') for row in rows)
    finally:
        await db.close()
