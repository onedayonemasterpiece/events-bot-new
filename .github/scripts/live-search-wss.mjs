import { createRequire } from 'node:module';
import { createLiveSocketTransport } from '../../site/node_modules/@onedayonemasterpiece/live-interaction/browser/socket-transport.js';

const requireSite = createRequire(new URL('../../site/package.json', import.meta.url));

/** Exercise the installed browser transport; do not implement a second relay. */
export async function verifyLiveSearchWss({ base, started, timeoutMs = 90000,
  transportFactory = createLiveSocketTransport, WebSocketImpl } = {}) {
  const origin = new URL(base);
  const target = new URL(started?.socket_url, origin);
  if (origin.protocol !== 'https:' || target.origin !== origin.origin || target.username || target.password || target.search || target.hash) {
    throw new Error('LIVE_SOCKET_ORIGIN');
  }
  if (started?.transport_protocol !== 'wl-live-v1' || !started.socket_ticket || !started.attempt_id) {
    throw new Error('LIVE_SOCKET_REQUIRED');
  }
  if (!WebSocketImpl) {
    const WebSocket = requireSite('ws');
    WebSocketImpl = class extends WebSocket {
      constructor(url, protocols) { super(url, protocols, { origin: 'https://kenigevents.ru', handshakeTimeout: 10000 }); }
    };
  }
  let resultEvent = null, sawSearchTool = false, toolOk = false, modelAudioBytes = 0;
  let resolveDone, rejectDone, finished = false;
  const done = new Promise((resolve, reject) => { resolveDone = resolve; rejectDone = reject; });
  // A network callback can fail while connect() is still awaited.
  void done.catch(() => {});
  const fail = code => { if (!finished) { finished = true; rejectDone(new Error(String(code).slice(0, 100))); } };
  const transport = transportFactory({
    WebSocketImpl,
    onEvent(event) {
      if (event?.type === 'tool_call' && (event.calls || []).some(call => call?.name === 'search_events')) sawSearchTool = true;
      if (event?.type === 'tool_result' && event.name === 'search_events') {
        if (event.status !== 'ok') { fail(event.code || 'LIVE_SEARCH_TOOL_FAILED'); return; }
        toolOk = true;
      }
      if (event?.type === 'search_results') resultEvent = event;
      if (event?.type === 'audio') modelAudioBytes += event.pcm?.byteLength || 0;
      if (event?.type === 'error') fail(event.code || 'LIVE_PROVIDER_ERROR');
      if (event?.type === 'turn_complete' && sawSearchTool && toolOk && resultEvent && modelAudioBytes > 0 && !finished) {
        const items = Array.isArray(resultEvent.data?.items) ? resultEvent.data.items : [];
        if (!items.length) { fail('LIVE_SEARCH_CARDS_EMPTY'); return; }
        finished = true;
        resolveDone({
          transport: 'wss', protocol: 'wl-live-v1', hello_ack: true,
          functional_call: 'search_events', tool_result: 'ok',
          cards_observed: items.length, has_more: Boolean(resultEvent.data?.has_more),
          model_audio_bytes: modelAudioBytes, turn_complete: true,
          http_audio_requests: 0, http_event_polls: 0, physical_mic: false,
        });
      }
    },
    onError(error) { fail(error?.code || 'LIVE_SOCKET_IO'); },
    onClose(event) { if (!event.expected) fail('LIVE_SOCKET_CLOSED'); },
  });
  const timer = setTimeout(() => fail('LIVE_SEARCH_WSS_TIMEOUT'), timeoutMs);
  try {
    await transport.connect({ url: target.href, ticket: started.socket_ticket,
      attempt_id: started.attempt_id, cursor: 0, connection_generation: 1 });
    await transport.send({ text: 'Покажи несколько интересных событий на ближайшие дни' });
    return await done;
  } finally {
    clearTimeout(timer);
    transport.close({ sendStop: true, reason: 'canary_complete' });
  }
}
