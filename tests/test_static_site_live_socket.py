"""Isolated aiohttp transport contracts; no bot/database/model credentials.

Run alongside test_static_site_live_search.py with --noconftest so unrelated
Telegram/database global fixtures do not replace this local socket transport.
"""
import base64
import json
import struct
import unittest
from unittest.mock import AsyncMock, patch

from aiohttp import WSServerHandshakeError, WSMsgType, web
from aiohttp.test_utils import TestClient, TestServer
from live_interaction import LiveSocketSessionHost

from static_site_live_search import RESOURCE_ID, _token_hash, register


class Adapter:
    def initialize(self, *, access_token, **kwargs):
        return {'state': {'token_hash': _token_hash(access_token)}, 'context': {}, 'configuration': {}}

    async def execute_tool(self, session, call):
        return {'ok': True}


class LiveSocketRoutes(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.inputs = []
        async def provider(*, reader, on_event, **kwargs):
            self.inputs.append(json.loads(await reader.readline()))
            on_event({'type': 'ready'})
            while raw := await reader.readline():
                message = json.loads(raw)
                self.inputs.append(message)
                if message['type'] == 'stop':
                    return
                if message['type'] == 'text':
                    on_event({'type': 'output_transcript', 'text': 'fixture answer'})
                    on_event({'type': 'audio', 'data': base64.b64encode(b'\x01\x00\x02\x00').decode(), 'mime_type': 'audio/pcm;rate=24000'})
                    on_event({'type': 'turn_complete'})
        self.host = LiveSocketSessionHost(adapter_factory=lambda **_: Adapter(),
            key_resolver=lambda *_: 'fixture-no-provider-key', provider_run=provider, ready_timeout_ms=1000)
        self.auth = patch('static_site_live_search.verify_supabase_user', new=AsyncMock(return_value={'id': 'owner'}))
        self.auth.start()
        app = web.Application()
        register(app, host_override=self.host, env={
            'ENABLE_STATIC_SITE_LIVE_SEARCH': '1',
            'PERSONALIZATION_SUPABASE_URL': 'https://example.supabase.co',
            'PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY': 'fixture-public',
            'STATIC_SITE_LIVE_SEARCH_ALLOWED_ORIGINS': 'https://kenigevents.ru',
        })
        self.client = TestClient(TestServer(app))
        await self.client.start_server()
        self.headers = {'Origin': 'https://kenigevents.ru', 'Authorization': 'Bearer fixture-owner'}

    async def asyncTearDown(self):
        await self.client.close()
        self.auth.stop()

    async def start(self):
        response = await self.client.post('/api/live-search', json={'attempt_id': 'attempt_test'}, headers=self.headers)
        self.assertEqual(response.status, 200, await response.text())
        return await response.json()

    async def connect(self, value, origin='https://kenigevents.ru', suffix=''):
        return await self.client.ws_connect(value['socket_url'] + suffix,
            headers={'Origin': origin}, protocols=['wl-live-v1', 'wl-ticket.' + value['socket_ticket']])

    async def hello(self, ws, value):
        await ws.send_json({'type': 'hello', 'protocol': 'wl-live-v1', 'attempt_id': value['attempt_id'], 'cursor': 0, 'connection_generation': 1})
        self.assertEqual((await ws.receive_json(timeout=2))['type'], 'hello_ack')

    async def test_binary_audio_and_pushed_results_use_shared_host(self):
        value = await self.start()
        self.assertEqual(self.host.sessions[value['session_id']].resource_id, RESOURCE_ID)
        ws = await self.connect(value)
        try:
            await self.hello(ws, value)
            denied = await self.client.post('/api/live-search/' + value['session_id'] + '/input',
                json={'text': 'no HTTP fallback'}, headers=self.headers)
            self.assertEqual(denied.status, 409)
            await ws.send_bytes(struct.pack('!III', 0x574c4131, 1, 20) + b'\x01\x00\x02\x00')
            for _ in range(10):
                message = await ws.receive_json(timeout=2)
                if message['type'] == 'audio_ack':
                    break
            else:
                self.fail('No audio acknowledgement')
            await ws.send_json({'type': 'input', 'message': {'audio_stream_end': True}})
            await ws.send_json({'type': 'input', 'message': {'text': 'fixture turn'}})
            for _ in range(15):
                message = await ws.receive(timeout=2)
                if message.type == WSMsgType.BINARY:
                    self.assertEqual(struct.unpack('!III', message.data[:12])[::2], (0x574c4f31, 24000))
                    break
            else:
                self.fail('No pushed binary model audio')
            await ws.send_json({'type': 'ping'})
            for _ in range(10):
                message = await ws.receive_json(timeout=2)
                if message['type'] == 'pong':
                    break
            else:
                self.fail('No application heartbeat')
            await ws.send_json({'type': 'stop'})
            await ws.receive(timeout=2)
        finally:
            await ws.close()
        stop = await self.client.post('/api/live-search/' + value['session_id'] + '/stop', json={}, headers=self.headers)
        self.assertEqual(stop.status, 200)
        self.assertEqual(self.host.size(), 0)
        self.assertEqual([item['type'] for item in self.inputs][:4], ['start', 'audio', 'audio_stream_end', 'text'])

    async def test_origin_query_ticket_renewal_and_actor_binding(self):
        value = await self.start()
        for origin, suffix in [('https://evil.invalid', ''), ('https://kenigevents.ru', '?ticket=forbidden')]:
            with self.assertRaises(WSServerHandshakeError):
                await self.connect(value, origin, suffix)
        ticket_url = '/api/live-search/' + value['session_id'] + '/socket-ticket'
        foreign = {**self.headers, 'Authorization': 'Bearer different-owner'}
        denied = await self.client.post(ticket_url, json={}, headers=foreign)
        self.assertEqual(denied.status, 403)
        response = await self.client.post(ticket_url, json={}, headers=self.headers)
        self.assertEqual(response.status, 200)
        renewed = await response.json()
        self.assertNotEqual(value['socket_ticket'], renewed['socket_ticket'])
        with self.assertRaises(WSServerHandshakeError):
            await self.connect(value)
        ws = await self.connect({**value, **renewed})
        try:
            await self.hello(ws, value)
            await ws.send_json({'type': 'stop'})
            await ws.receive(timeout=2)
        finally:
            await ws.close()
