import test from 'node:test';
import assert from 'node:assert/strict';
import { verifyLiveSearchWss } from '../../.github/scripts/live-search-wss.mjs';

const base = 'https://api.example.test/api/live-search';
const started = { socket_url: '/api/live-search/live_fixture/socket', socket_ticket: 'a'.repeat(32),
  attempt_id: 'attempt_test', transport_protocol: 'wl-live-v1' };
function harness(events) {
  let options, closed = 0, connects = 0;
  return {
    arguments: { base, started, timeoutMs: 30, WebSocketImpl: class {},
      transportFactory(callbacks) {
        options = callbacks;
        return {
          async connect(args) { connects++; assert.equal(args.url, 'https://api.example.test/api/live-search/live_fixture/socket'); },
          async send(message) { assert.ok(message.text); for (const event of events) options.onEvent(event); },
          close() { closed++; },
        };
      },
    },
    get closed() { return closed; }, get connects() { return connects; },
  };
}
const success = [
  { type: 'tool_call', calls: [{ name: 'search_events' }] },
  { type: 'search_results', data: { items: [{ id: 7 }], has_more: true } },
  { type: 'tool_result', name: 'search_events', status: 'ok' },
  { type: 'audio', pcm: new Uint8Array(80) },
  { type: 'turn_complete' },
];
test('canary uses installed shared WSS and proves search, cards and answer', async () => {
  const h = harness(success);
  const receipt = await verifyLiveSearchWss(h.arguments);
  assert.equal(receipt.transport, 'wss');
  assert.equal(receipt.cards_observed, 1);
  assert.equal(receipt.model_audio_bytes, 80);
  assert.equal(receipt.http_audio_requests, 0);
  assert.equal(receipt.http_event_polls, 0);
  assert.equal(receipt.physical_mic, false);
  assert.equal(h.closed, 1);
});
test('HTTP-only bootstrap and foreign sockets cannot pass a WSS canary', async () => {
  for (const override of [{ transport_protocol: 'http' }, { socket_url: 'https://foreign.invalid/socket' }, { socket_url: '/socket?ticket=leak' }]) {
    const h = harness(success);
    await assert.rejects(verifyLiveSearchWss({ ...h.arguments, started: { ...started, ...override } }));
    assert.equal(h.connects, 0);
  }
});
test('missing answer audio or failed tool stays failed and releases the socket', async () => {
  const h = harness(success.filter(event => event.type !== 'audio'));
  await assert.rejects(verifyLiveSearchWss(h.arguments), /LIVE_SEARCH_WSS_TIMEOUT/);
  assert.equal(h.closed, 1);
  const failed = harness([{ type: 'tool_result', name: 'search_events', status: 'error', code: 'SEARCH_RELEVANCE_UNAVAILABLE' }]);
  await assert.rejects(verifyLiveSearchWss(failed.arguments), /SEARCH_RELEVANCE_UNAVAILABLE/);
  assert.equal(failed.closed, 1);
});
test('old pre-tool turn_complete is not accepted as a completed search response', async () => {
  const h = harness([{ type: 'turn_complete' }, ...success.slice(0, -1)]);
  await assert.rejects(verifyLiveSearchWss(h.arguments), /LIVE_SEARCH_WSS_TIMEOUT/);
  assert.equal(h.closed, 1);
});
