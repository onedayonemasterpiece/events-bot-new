import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';

import { createLiveEventSearchController } from '../src/lib/liveEventSearch.js';

function harness() {
  const calls = [];
  const results = [];
  const states = [];
  const stops = [];
  const transcripts = [];
  const completedTurns = [];
  let handlers;
  let sessionId = null;
  let microphoneEnabled = false;
  let timerFn = null;

  const controller = createLiveEventSearchController({
    endpoint: 'https://api.example.test/api/live-search',
    getAccessToken: async () => 'token-1',
    onResults: (payload) => results.push(payload),
    onTranscript: (payload) => transcripts.push(payload),
    onTurnComplete: (event) => completedTurns.push(event),
    onState: (state) => states.push(state),
    fetchImpl: async (url, options) => {
      calls.push({ url, options });
      return { ok: true, json: async () => ({ session_id: 'server-session', model: 'gemini-3.8-live' }) };
    },
    setTimer: (fn, ms) => {
      timerFn = { fn, ms };
      return 1;
    },
    clearTimer: () => {
      timerFn = null;
    },
    clientFactory: (options) => {
      handlers = options;
      return {
        get sessionId() { return sessionId; },
        get starting() { return false; },
        get microphoneEnabled() { return microphoneEnabled; },
        async start({ url, microphone = true }) {
          calls.push({ start: { url, microphone } });
          await options.request(url, { method: 'POST', body: '{}' });
          sessionId = 'live-1';
          microphoneEnabled = Boolean(microphone);
          options.onState('started');
          if (microphoneEnabled) options.onState('listening');
          return { session_id: sessionId, model: 'gemini-3.8-live' };
        },
        async enableMicrophone() {
          calls.push({ enableMicrophone: true });
          microphoneEnabled = true;
          options.onState('listening');
          return true;
        },
        async input(message) {
          calls.push({ input: message });
        },
        disableMicrophone() {
          calls.push({ disableMicrophone: true });
          microphoneEnabled = false;
        },
        stop({ reason }) {
          stops.push(reason);
          sessionId = null;
          microphoneEnabled = false;
          options.onState('off', { reason });
        },
      };
    },
  });
  return { controller, calls, results, states, stops, transcripts, completedTurns, get handlers() { return handlers; }, get timerFn() { return timerFn; } };
}

test('Live search sends authorized text through the shared session and surfaces canonical card payload', async () => {
  const h = harness();
  assert.equal(await h.controller.search('джаз завтра'), true);
  const start = h.calls.find((call) => call.options?.method === 'POST');
  assert.equal(new Headers(start.options.headers).get('Authorization'), 'Bearer token-1');
  assert.deepEqual(h.calls.find((call) => call.input)?.input, { text: 'джаз завтра' });

  h.handlers.onEvent({ type: 'search_results', query: 'джаз завтра', offset: 0, data: { items: [{ id: 7 }], has_more: true } });
  assert.equal(h.results.length, 1);
  assert.equal(h.results[0].data.items[0].id, 7);
  assert.equal(h.results[0].append, false);
});

test('continue stays in the same Live session and follow-up timeout releases it', async () => {
  const h = harness();
  await h.controller.search('театр');
  await h.controller.more();
  assert.deepEqual(h.calls.at(-1).input, { text: 'Покажи ещё варианты' });

  h.handlers.onEvent({ type: 'search_results', query: 'театр', offset: 9, data: { items: [], has_more: false } });
  h.handlers.onEvent({ type: 'turn_complete' });
  assert.equal(h.timerFn.ms, 15000);
  h.timerFn.fn();
  assert.deepEqual(h.stops, ['followup_timeout']);
  assert.ok(h.states.includes('followup_timeout'));
});

test('AuthorizedEventSearch selects old adapter only when Live is not configured', async () => {
  const source = await readFile(new URL('../src/components/AuthorizedEventSearch.astro', import.meta.url), 'utf8');
  assert.match(source, /if \(liveEnabled\) await runLiveSearch\(\{ append: false \}\);\s*else await runSearch/u);
  assert.match(source, /if \(liveEnabled\) await runLiveSearch\(\{ append: true \}\);\s*else await runSearch/u);
  assert.doesNotMatch(source, /catch[\s\S]{0,300}runSearch\(\{ append:/u);
});

test('typed Live search stays microphone-free and voice enables mic in the same session', async () => {
  const h = harness();
  assert.equal(await h.controller.search('лекция сегодня'), true);
  assert.equal(h.calls.find((call) => call.start)?.start.microphone, false);
  assert.equal(h.controller.microphoneEnabled, false);

  assert.equal(await h.controller.startVoice(), true);
  assert.equal(h.controller.sessionId, 'live-1');
  assert.equal(h.controller.microphoneEnabled, true);
  assert.equal(h.calls.filter((call) => call.start).length, 1);
  assert.equal(h.calls.filter((call) => call.enableMicrophone).length, 1);
});


test('Live controller surfaces both user and model transcripts and mute preserves the session', async () => {
  const h = harness();
  await h.controller.startVoice();
  h.handlers.onEvent({ type: 'input_transcript', text: 'хочу камерный концерт' });
  h.handlers.onEvent({ type: 'output_transcript', text: 'Сейчас поищу подходящие варианты.' });
  assert.deepEqual(h.transcripts.map(({ role, text }) => [role, text]), [
    ['user', 'хочу камерный концерт'],
    ['assistant', 'Сейчас поищу подходящие варианты.'],
  ]);

  assert.equal(h.controller.mute(), true);
  assert.equal(h.controller.sessionId, 'live-1');
  assert.equal(h.controller.microphoneEnabled, false);
  assert.equal(h.calls.filter((call) => call.disableMicrophone).length, 1);

  h.handlers.onEvent({ type: 'turn_complete' });
  assert.equal(h.completedTurns.length, 1);
});
