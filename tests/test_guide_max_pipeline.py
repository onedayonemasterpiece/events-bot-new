import json
from datetime import datetime, timezone

import pytest

from guide_excursions import max_digest as md
from guide_excursions import visual_digest as vd
from guide_excursions.vibepublish_client import VibePublishClient, html_content, SnapshotUnavailable
from test_guide_max_delivery import DB, Transport


@pytest.mark.asyncio
async def test_http_lost_reply_replays_exact_original_admission_then_get_only():
    client = VibePublishClient(base_url='https://vibe.example', token='fixture')
    payload = {'alias': 'max_guide', 'native_id': '123', 'binding_revision': 1,
               'content': html_content('<b>Дайджест</b>\n<a href="https://t.me/g/1">Экскурсия</a>'),
               'image_b64': 'aW1hZ2U='}
    calls = []
    async def request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        if path == '/v1/bootstrap':
            return {'destinations': [{'alias': 'max_guide', 'kind': 'destination', 'provider': 'max',
                                      'native_id': '123', 'revision': 1}]}
        if path == '/v1/assets':
            return {'asset_id': 'asset_same'}
        return {'operation_id': 'op_same', 'state': 'accepted', 'deliveries': []}
    client._request = request
    first = await client.submit(payload, 'same-key')
    replay = await client.observe('same-key', None)
    assert first == replay
    assert calls[0:3] == calls[3:6]
    assert calls[1][2]['key'] == 'same-key-image'
    assert calls[2][2]['key'] == calls[2][2]['body']['request_key'] == 'same-key'
    assert calls[2][2]['body']['media'][0]['source']['id'] == 'asset_same'
    before = len(calls)
    await client.observe('same-key', 'op_same')
    assert calls[before:] == [('GET', '/v1/operations/op_same', {})]


@pytest.mark.asyncio
async def test_max_renderer_failure_does_not_block_native_tg_vk(tmp_path, monkeypatch):
    db = DB(tmp_path / 'isolation.sqlite')
    await seed(db)
    enable(monkeypatch)
    calls = []
    async def load(db, issue_id):
        return {'items': ROWS}
    async def tg(*args, **kwargs):
        calls.append('telegram')
        return {'published': True}
    async def vk(*args, **kwargs):
        calls.append('vk')
        return {'published': True}
    async def broken(*args, **kwargs):
        raise SnapshotUnavailable('fixture_renderer_failure')
    monkeypatch.setattr(vd, 'load_visual_digest_issue', load)
    monkeypatch.setattr(vd, 'publish_visual_digest_to_telegram', tg)
    monkeypatch.setattr(vd, 'publish_visual_digest_to_vk', vk)
    monkeypatch.setattr(md, 'freeze_visual_snapshot', broken)
    result = await vd.publish_visual_digest_daily(db, object(), vk_group_id=238875824)
    assert calls == ['telegram', 'vk']
    assert result['telegram']['published'] and result['vk']['published']
    assert not result['max']['published']
    assert result['state'] == 'partial'



@pytest.mark.asyncio
async def test_http_domain_error_keeps_only_safe_code(monkeypatch):
    from guide_excursions import vibepublish_client as module
    class Response:
        status = 200
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def json(self):
            return {'error': {'code': 'access_denied', 'message': 'private provider details'}}
    class Session:
        def __init__(self, **kwargs): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        def request(self, *args, **kwargs): return Response()
    monkeypatch.setattr(module.aiohttp, 'ClientSession', Session)
    client = VibePublishClient(base_url='https://vibe.example', token='fixture')
    with pytest.raises(SnapshotUnavailable) as caught:
        await client._request('GET', '/v1/bootstrap')
    assert str(caught.value) == 'access_denied'



ROWS = [{"id": 77, "canonical_title": "Мистическая мини-экспедиция", "date": "2026-10-31",
         "time": "10:00", "booking_url": "https://t.me/guide/123", "source_post_url": "https://t.me/guide/123"}]
TG = {"message_ids": [11], "media_message_ids": [11], "target_chat": "@youwillsee39", "transport": "telegram_photo"}
VK = {"message_ids": [12], "post_urls": ["https://vk.com/wall-238875824_12"], "group_id": 238875824,
      "transport": "vk_wall", "publish_date": 123}


async def seed(db):
    async with db.raw_conn() as conn:
        await conn.execute("CREATE TABLE guide_digest_issue (id INTEGER PRIMARY KEY, family TEXT, status TEXT, created_at TEXT, "
                           "published_targets_json TEXT, target_chat TEXT, text TEXT, published_at TEXT, published_message_ids_json TEXT)")
        await conn.execute("CREATE TABLE guide_occurrence (id INTEGER PRIMARY KEY, published_visual_digest_issue_id INTEGER)")
        await conn.execute("INSERT INTO guide_occurrence VALUES(77,289)")
        await conn.execute("INSERT INTO guide_digest_issue (id,family,status,created_at,published_targets_json) VALUES(289,'visual_schedule','published',?,?)",
                           (datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'), json.dumps({"tg:@youwillsee39:visual": TG, "vk:uhtykaliningrad:visual": VK})))
        await conn.commit()


def enable(monkeypatch):
    monkeypatch.setenv("ENABLE_GUIDE_VISUAL_DIGEST_MAX", "1")
    monkeypatch.setenv("GUIDE_VISUAL_DIGEST_MAX_TARGETS", json.dumps([{"alias": "max_guide", "native_id": "native-123", "binding_revision": 1, "canonical_url": "https://max.ru/channel_uh_kaliningrad"}]))


@pytest.mark.asyncio
async def test_native_daily_entrypoint_resumes_backend_edition_only_missing_max(tmp_path, monkeypatch):
    db = DB(tmp_path / 'pipeline.sqlite')
    await seed(db)
    enable(monkeypatch)
    calls = {"render": 0, "load": [], "tg": 0, "vk": 0}

    async def load(db, issue_id):
        calls['load'].append(issue_id)
        return {"id": issue_id, "items": ROWS}
    async def no_schema(conn):
        pass
    async def forbidden(*args, **kwargs):
        raise AssertionError("Must not select new occurrences or create another edition")
    def render(rows, *, issue_id):
        assert rows == ROWS and issue_id == 289
        calls['render'] += 1
        return [b'canonical-backend-jpeg']
    class Bot:
        async def send_photo(self, *args, **kwargs):
            calls['tg'] += 1
            raise AssertionError('Telegram already delivered')
    async def vk_publish(*args, **kwargs):
        calls['vk'] += 1
        raise AssertionError('VK already delivered')
    monkeypatch.setattr(vd, 'load_visual_digest_issue', load)
    monkeypatch.setattr(vd, 'ensure_visual_digest_schema', no_schema)
    monkeypatch.setattr(vd, 'build_visual_digest_issue', forbidden)
    monkeypatch.setattr(vd, 'render_visual_digest_cards', render)
    monkeypatch.setattr(vd, '_publish_visual_digest_to_vk_once', vk_publish)
    monkeypatch.setattr(vd, 'VISUAL_DIGEST_TG_TARGET_CHATS', ['@youwillsee39'])
    transport = Transport()
    monkeypatch.setattr(md, 'VibePublishClient', lambda payload=None: transport)
    first = await vd.publish_visual_digest_daily(db, Bot(), vk_group_id=238875824)
    second = await vd.publish_visual_digest_daily(db, Bot(), vk_group_id=238875824)
    assert first['complete'] and second['complete']
    assert first['issue_id'] == second['issue_id'] == 289
    assert calls['tg'] == calls['vk'] == 0
    assert calls['render'] >= 1
    assert len(transport.submitted) == 1
    payload = transport.submitted[0][0]
    assert payload['image_b64']
    assert any(n.get('url') == 'https://t.me/guide/123' for p in payload['content']['paragraphs'] for n in p)
    assert 'vk.cc' not in json.dumps(payload)
    links = [n for p in payload['content']['paragraphs'] for n in p if n.get('kind') == 'link']
    subscribe = [n for n in links if n.get('label') == 'Подписаться']
    assert subscribe == [{'kind': 'link', 'label': 'Подписаться', 'url': 'https://max.ru/channel_uh_kaliningrad'}]
    assert [n['label'] for n in payload['content']['paragraphs'][-1] if n.get('kind') == 'link'] == ['Подписаться', 'Telegram', 'Вконтакте']
    assert not any(n.get('label') == 'Max' for n in links)
    async with db.raw_conn() as conn:
        cur = await conn.execute('SELECT published_targets_json FROM guide_digest_issue WHERE id=289')
        targets = json.loads((await cur.fetchone())[0])
        cur = await conn.execute('SELECT count(*) FROM guide_digest_issue')
        assert (await cur.fetchone())[0] == 1
    assert targets['tg:@youwillsee39:visual'] == TG
    assert targets['vk:uhtykaliningrad:visual'] == VK
    assert targets['max:native-123:visual']['message_ids'] == [123]


@pytest.mark.asyncio
async def test_independent_max_destinations_preserve_success_on_partial_failure(tmp_path, monkeypatch):
    db = DB(tmp_path / 'multi.sqlite')
    await seed(db)
    enable(monkeypatch)
    monkeypatch.setenv('GUIDE_VISUAL_DIGEST_MAX_TARGETS', json.dumps([
        {'alias': 'max_a', 'native_id': 'a', 'binding_revision': 1, 'canonical_url': 'https://max.ru/channel_a'},
        {'alias': 'max_b', 'native_id': 'b', 'binding_revision': 2, 'canonical_url': 'https://max.ru/channel_b'}]))
    await md.freeze_visual_snapshot(db, issue_id=289, caption_html='<b>Дайджест</b>\n<a href="https://t.me/g/1">Экскурсия</a>', card=b'image')
    a, b = Transport(), Transport()
    b.raise_submit = True
    def factory(payload=None):
        return a if payload['alias'] == 'max_a' else b
    first = await md.publish_visual_digest_to_max(db, issue_id=289, client_factory=factory)
    assert not first['published'] and first['deliveries'][0]['published']
    second = await md.publish_visual_digest_to_max(db, issue_id=289, client_factory=factory)
    assert second['published']
    assert len(a.submitted) == len(b.submitted) == 1
    assert len(b.observed) == 1
    assert a.submitted[0][1] != b.submitted[0][1]
    for transport, url in [(a, 'https://max.ru/channel_a'), (b, 'https://max.ru/channel_b')]:
        footer = transport.submitted[0][0]['content']['paragraphs'][-1]
        assert footer[0] == {'kind': 'link', 'label': 'Подписаться', 'url': url}


@pytest.mark.asyncio
async def test_alias_drift_fails_before_asset_upload_or_publish():
    client = VibePublishClient(base_url='https://vibe.example', token='fixture')
    calls = []
    async def request(method, path, **kwargs):
        calls.append(path)
        return {'destinations': [{'alias': 'max_guide', 'kind': 'destination', 'provider': 'max', 'native_id': 'WRONG', 'revision': 1}]}
    client._request = request
    with pytest.raises(SnapshotUnavailable, match='max_binding_changed'):
        await client.submit({'alias': 'max_guide', 'native_id': 'right', 'binding_revision': 1, 'image_b64': 'aW1hZ2U=', 'content': {}}, 'key')
    assert calls == ['/v1/bootstrap']


def test_named_links_and_bold_preserved_without_html_or_shortlinks():
    content = html_content('<b>Заголовок</b>\n1. <a href="https://t.me/guide/123?a=1&amp;b=2">Экскурсия</a>')
    nodes = content['paragraphs'][0]
    assert nodes[0] == {'kind': 'text', 'text': 'Заголовок', 'style': 'bold'}
    assert nodes[-1] == {'kind': 'link', 'label': 'Экскурсия', 'url': 'https://t.me/guide/123?a=1&b=2'}
    with pytest.raises(SnapshotUnavailable):
        html_content('<a href="https://vk.cc/short">Экскурсия</a>')


def test_top_level_failure_is_not_reported_as_accepted():
    client = VibePublishClient({'alias': 'max_guide'}, base_url='https://vibe.example', token='fixture')
    result = client._publication_result({'receipts': [{'operation_id': 'op_1', 'state': 'blocked', 'operation_complete': True, 'deliveries': []}]})
    assert result['state'] == 'failed' and result['error_code'] == 'operation_blocked'


def test_publish_requires_complete_exact_destination_receipt():
    client = VibePublishClient({'alias': 'max_guide'}, base_url='https://vibe.example', token='fixture')
    receipt = {'operation_id': 'op_1', 'operation_complete': False,
               'deliveries': [{'destination': 'max_guide', 'provider': 'max', 'state': 'verified', 'observed': 'published', 'item_ref': 'item_123'}]}
    assert client._publication_result({'receipts': [receipt]})['state'] == 'accepted'
    receipt['operation_complete'] = True
    assert client._publication_result({'receipts': [receipt]})['state'] == 'published'
