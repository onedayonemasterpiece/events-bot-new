import { createLiveClient } from '@onedayonemasterpiece/live-interaction/browser';

function liveError(code, message) {
  const error = new Error(message || code);
  error.code = code;
  return error;
}

export function createLiveEventSearchController({
  endpoint,
  getAccessToken,
  onResults = () => {},
  onState = () => {},
  onNotice = () => {},
  onError = () => {},
  followupMs = 15000,
  clientFactory = createLiveClient,
  fetchImpl = globalThis.fetch?.bind(globalThis),
  setTimer = globalThis.setTimeout?.bind(globalThis),
  clearTimer = globalThis.clearTimeout?.bind(globalThis),
} = {}) {
  if (!endpoint) throw liveError('LIVE_ENDPOINT_MISSING');
  if (typeof getAccessToken !== 'function') throw liveError('LIVE_AUTH_PROVIDER_MISSING');
  if (typeof fetchImpl !== 'function') throw liveError('LIVE_FETCH_MISSING');

  let followupTimer = null;
  let resultPendingTurn = false;
  let lastQuery = '';

  function clearFollowup() {
    if (followupTimer != null) clearTimer(followupTimer);
    followupTimer = null;
  }

  function scheduleFollowupStop() {
    clearFollowup();
    followupTimer = setTimer(() => {
      followupTimer = null;
      client.stop({ reason: 'followup_timeout' });
      onState('followup_timeout');
    }, followupMs);
  }

  async function request(url, options = {}) {
    const token = String(await getAccessToken() || '');
    if (!token) throw liveError('LIVE_AUTH_REQUIRED', 'Сессия закончилась. Войдите через Яндекс ещё раз.');
    const headers = new Headers(options.headers || {});
    headers.set('Authorization', 'Bearer ' + token);
    if (options.body && !headers.has('Content-Type')) headers.set('Content-Type', 'application/json');
    const response = await fetchImpl(url, { cache: 'no-store', ...options, headers });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const code = String(payload?.error?.code || payload?.error || 'LIVE_REQUEST_FAILED');
      const error = liveError(code, payload?.error?.message || payload?.detail || code);
      error.status = response.status;
      throw error;
    }
    return payload;
  }

  const client = clientFactory({
    request,
    onEvent(event) {
      if (event?.type === 'search_results') {
        resultPendingTurn = true;
        const query = String(event.query || lastQuery || '').trim();
        const offset = Math.max(0, Number(event.offset) || 0);
        onResults({ query, offset, append: offset > 0, data: event.data || {} });
        return;
      }
      if (event?.type === 'turn_complete' && resultPendingTurn) {
        resultPendingTurn = false;
        scheduleFollowupStop();
      }
    },
    onState(state, detail) {
      onState(state, detail);
    },
    onNotice(kind, error) {
      onNotice(kind, error);
      if (kind === 'start_error' || kind === 'connection_error' || kind === 'transport_error') {
        onError(error || liveError(kind));
      }
    },
  });

  async function ensureStarted({ microphone = false } = {}) {
    clearFollowup();
    if (client.sessionId) {
      if (microphone && !client.microphoneEnabled) return Boolean(await client.enableMicrophone());
      return true;
    }
    await client.start({ url: endpoint, body: {}, microphone });
    if (!client.sessionId) return false;
    return !microphone || Boolean(client.microphoneEnabled);
  }

  async function search(query) {
    const value = String(query || '').trim();
    if (!value) throw liveError('INVALID_SEARCH_QUERY');
    lastQuery = value;
    resultPendingTurn = false;
    if (!await ensureStarted({ microphone: false })) return false;
    await client.input({ text: value });
    return true;
  }

  async function startVoice() {
    resultPendingTurn = false;
    return ensureStarted({ microphone: true });
  }

  async function more() {
    if (!lastQuery || !client.sessionId) throw liveError('SEARCH_CONTEXT_MISSING');
    clearFollowup();
    resultPendingTurn = false;
    await client.input({ text: 'Покажи ещё варианты' });
  }

  function stopVoice() {
    if (!client.sessionId || !client.microphoneEnabled) return false;
    client.disableMicrophone();
    scheduleFollowupStop();
    onState('microphone_off');
    return true;
  }

  function stop(reason = 'user_stop', { keepalive = false } = {}) {
    clearFollowup();
    resultPendingTurn = false;
    client.stop({ reason, keepalive });
  }

  return {
    search,
    more,
    startVoice,
    stopVoice,
    stop,
    clearFollowup,
    get active() { return Boolean(client.sessionId || client.starting); },
    get microphoneEnabled() { return Boolean(client.microphoneEnabled); },
    get sessionId() { return client.sessionId; },
    get lastQuery() { return lastQuery; },
  };
}
