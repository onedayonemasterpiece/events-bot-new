import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';

import { SEARCH_CANARY_VARIANTS } from '../e2e/search/canary-manifest.mjs';
import { runSearchJourney } from '../e2e/search/journey.mjs';

function fakeAdapter() {
  const activity = { requests: [], responses: [], routes: [] };
  let policy = null;
  let value = '';
  let rendered = [];
  let invalid = false;
  let showMore = false;
  const indexes = new Map();
  const cases = new Map([['first varied query', 1], ['second varied query', 2], ['third varied query', 3]]);
  const dispatch = (append = false) => {
    const caseNo = cases.get(value);
    const mode = policy.execution_mode;
    const id = append ? `${caseNo}02` : `${caseNo}01`;
    activity.requests.push({ method: 'POST', path: '/functions/v1/event-search' });
    activity.responses.push({
      response_ids: [id], response_families: [`event:${id}`], item_count: 1, fallback_count: 0,
      has_more: caseNo === 1 && !append && mode !== 'cached_vector',
      result_cache_status: mode === 'cached_vector' ? 'hit' : 'stored', served_from_cache: mode === 'cached_vector',
      requested_execution_mode: mode, actual_execution_mode: mode,
      provider_attempts: mode === 'cached_vector' ? { embedding: 0, vector: 0, llm: 0 } : { embedding: 1, vector: 1, llm: 0 },
      provider_attempts_present: true,
    });
    activity.routes.push({ policy: 'selected-once', route: 'direct', status: 200 });
    if (append) rendered.push(id); else rendered = [id];
    showMore = caseNo === 1 && !append && mode !== 'cached_vector';
    indexes.set(value, (indexes.get(value) || 0) + 1);
  };
  return {
    open: async () => {},
    inspectSurface: async () => ({ enabled: true, authorized: true, input_tag: 'textarea', enter_key_hint: 'search' }),
    configureRequestPolicy: async (next) => { policy = next; },
    activity: async () => structuredClone(activity),
    typeQuery: async (next) => { value = next; }, clearQuery: async () => { value = ''; },
    readQueryState: async () => ({ length: value.length }),
    submitWithSearchIntent: async () => { if (value === 'x') invalid = true; else dispatch(false); },
    waitForTerminal: async () => {},
    snapshotResults: async () => ({ terminal: true, error: false, cards_visible: true, visible_card_count: 1,
      rendered_ids: [...rendered], rendered_families: rendered.map((id) => `event:${id}`), card_renderer_unavailable: false }),
    realScrollResults: async () => ({ performed: true, delta_y: 640, card_visible_after: true }),
    showMoreState: async () => ({ visible: showMore, enabled: showMore }),
    activateShowMore: async () => dispatch(true),
    waitForValidation: async () => ({ visible: invalid, kind: 'error' }),
  };
}

test('semantic journey proves three varied queries, pagination, cache and zero-post validation', async () => {
  const result = await runSearchJourney({
    adapter: fakeAdapter(), targetUrl: 'https://kenigevents.ru/preview-secret-token-123/poisk/',
    variant: SEARCH_CANARY_VARIANTS.cold_vector,
    queryCases: [
      { id: 'case_one', value: 'first varied query', paginate: true },
      { id: 'case_two', value: 'second varied query' },
      { id: 'case_three', value: 'third varied query' },
    ],
  });
  assert.equal(result.status, 'PASS');
  assert.equal(result.query_cases.length, 3);
  assert.equal(result.query_cases[0].pages.length, 2);
  assert.equal(result.query_cases.every((item) => item.cache_repeat.response.served_from_cache), true);
  assert.equal(result.counters.requests, 10);
  assert.equal(result.counters.validation_posts, 0);
  assert.equal(result.target_path, '/preview-<redacted>/poisk/');
  assert.equal(JSON.stringify(result).includes('varied query'), false);
});

test('journey is mechanics-neutral and mobile adapters own native keyboard/touch mechanics', async () => {
  const journey = await readFile(new URL('../e2e/search/journey.mjs', import.meta.url), 'utf8');
  assert.doesNotMatch(journey, /playwright|appium|webdriver|mouse|touch|keyboard\.press/iu);
  const mobile = await readFile(new URL('../e2e/search/adapters/appium-base.mjs', import.meta.url), 'utf8');
  assert.match(mobile, /isKeyboardShown/u);
  assert.match(mobile, /driver\.keys\('\\uE007'\)/u);
  assert.match(mobile, /mobile: scrollGesture/u);
  assert.match(mobile, /mobile: scroll/u);
  assert.doesNotMatch(mobile, /screenshot|pageSource|getPageSource|\bhar\b|trace|video/iu);
});

test('runner is fail-closed on exact secret target SHA and carries three incident queries', async () => {
  const runner = await readFile(new URL('../e2e/search/run.mjs', import.meta.url), 'utf8');
  assert.match(runner, /E2E_EXPECTED_REPO_SHA/u);
  assert.match(runner, /candidate-build\.json/u);
  assert.match(runner, /static_secret_candidate_build_v1/u);
  assert.match(runner, /\{43\}/u);
  assert.match(runner, /На природу с детьми/u);
  assert.match(runner, /искусство у моря/u);
  assert.match(runner, /в пятницу бесплатно/u);
  assert.doesNotMatch(runner, /focus-email|mailbox|real-mail|otp/iu);
});
