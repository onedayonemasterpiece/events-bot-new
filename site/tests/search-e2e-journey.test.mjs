import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';

import { SEARCH_CANARY_VARIANTS } from '../e2e/search/canary-manifest.mjs';
import { runSearchJourney } from '../e2e/search/journey.mjs';
import { runRealWheelScroll } from '../e2e/search/adapters/playwright.mjs';
import { runRealTouchScroll } from '../e2e/search/adapters/appium-base.mjs';

function fakeAdapter({ coldCachedKeys = false } = {}) {
  const activity = { requests: [], responses: [], routes: [] };
  let policy = null;
  let value = '';
  let rendered = [];
  let invalid = false;
  let showMore = false;
  const indexes = new Map();
  const cases = new Map([['first varied query', 1], ['second varied query', 2], ['third varied query', 3]]);
  const cachedKeys = new Set();
  const dispatch = (append = false) => {
    const caseNo = cases.get(value);
    const mode = policy.execution_mode;
    const id = append ? `${caseNo}02` : `${caseNo}01`;
    const cacheKey = `${value}:${append ? 'next' : 'first'}`;
    const cacheHit = mode === 'cached_vector' && (!coldCachedKeys || cachedKeys.has(cacheKey));
    if (mode === 'cached_vector') cachedKeys.add(cacheKey);
    activity.requests.push({ method: 'POST', path: '/functions/v1/event-search' });
    activity.responses.push({
      response_ids: [id], response_families: [`event:${id}`], item_count: 1, fallback_count: 0,
      has_more: caseNo === 1 && !append,
      result_cache_status: mode === 'cached_vector' ? (cacheHit ? 'hit' : 'stored') : 'stored',
      served_from_cache: cacheHit,
      requested_execution_mode: mode, actual_execution_mode: mode === 'cached_vector' && !cacheHit ? 'cold_vector' : mode,
      provider_attempts: mode === 'cached_vector' && cacheHit
        ? { embedding: 0, vector: 0, llm: 0 }
        : { embedding: 1, vector: 1, llm: 0 },
      provider_attempts_present: true,
    });
    activity.routes.push({ policy: 'selected-once', route: 'direct', status: 200 });
    if (append) rendered.push(id); else rendered = [id];
    showMore = caseNo === 1 && !append;
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

test('Playwright wheel scrolling reaches a production-length final card beyond five gestures', async () => {
  let scrollY = 2300;
  let positiveWheels = 0;
  const receipt = await runRealWheelScroll({
    readScrollY: async () => scrollY,
    lastCardVisible: async () => scrollY >= 6900,
    wheel: async (delta) => {
      scrollY = Math.max(0, scrollY + delta);
      if (delta > 0) positiveWheels += 1;
    },
    wait: async () => {},
    step: 576,
  });
  assert.equal(receipt.card_visible_after, true);
  assert.equal(receipt.delta_y >= 6900, true);
  assert.equal(positiveWheels > 5, true);
});

test('mobile touch scrolling reaches a production-length final card beyond one gesture', async () => {
  let scrollY = 0;
  let gestures = 0;
  const receipt = await runRealTouchScroll({
    readScrollY: async () => scrollY,
    lastCardVisible: async () => scrollY >= 3600,
    gesture: async () => { gestures += 1; scrollY += 480; },
    wait: async () => {},
  });
  assert.equal(receipt.card_visible_after, true);
  assert.equal(receipt.delta_y >= 3600, true);
  assert.equal(gestures > 5, true);
});

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

test('live cached debug may bootstrap an invalidated cache and must then prove a zero-provider hit', async () => {
  const result = await runSearchJourney({
    adapter: fakeAdapter({ coldCachedKeys: true }), targetUrl: 'https://kenigevents.ru/preview-secret-token-123/poisk/',
    variant: SEARCH_CANARY_VARIANTS.cached_vector,
    cacheBootstrap: true,
    queryCases: [
      { id: 'case_one', value: 'first varied query', paginate: true },
      { id: 'case_two', value: 'second varied query' },
      { id: 'case_three', value: 'third varied query' },
    ],
  });
  assert.equal(result.status, 'PASS');
  assert.equal(result.query_cases.every((item) => item.cache_bootstrap.initial), true);
  assert.equal(result.query_cases[0].cache_bootstrap.pagination, 1);
  assert.equal(result.query_cases.every((item) => item.cache_repeat.response.served_from_cache), true);
  assert.deepEqual(result.query_cases.map((item) => item.cache_repeat.response.provider_attempts), [
    { embedding: 0, vector: 0, llm: 0 },
    { embedding: 0, vector: 0, llm: 0 },
    { embedding: 0, vector: 0, llm: 0 },
  ]);
  assert.equal(result.counters.requests, 10);
});

test('strict cached release rejects the same initial cache miss instead of bootstrapping it', async () => {
  await assert.rejects(() => runSearchJourney({
    adapter: fakeAdapter({ coldCachedKeys: true }), targetUrl: 'https://kenigevents.ru/preview-secret-token-123/poisk/',
    variant: SEARCH_CANARY_VARIANTS.cached_vector,
    queryCases: [
      { id: 'case_one', value: 'first varied query', paginate: true },
      { id: 'case_two', value: 'second varied query' },
      { id: 'case_three', value: 'third varied query' },
    ],
  }), /search_cache_state:case_one:initial:stored/u);
});

test('journey is mechanics-neutral and mobile adapters own native keyboard/touch mechanics', async () => {
  const journey = await readFile(new URL('../e2e/search/journey.mjs', import.meta.url), 'utf8');
  assert.doesNotMatch(journey, /playwright|appium|webdriver|mouse|touch|keyboard\.press/iu);
  const mobile = await readFile(new URL('../e2e/search/adapters/appium-base.mjs', import.meta.url), 'utf8');
  const [android, ios] = await Promise.all([
    readFile(new URL('../e2e/search/adapters/appium-android.mjs', import.meta.url), 'utf8'),
    readFile(new URL('../e2e/search/adapters/appium-ios.mjs', import.meta.url), 'utf8'),
  ]);
  assert.match(mobile, /observeNativeKeyboard/u);
  assert.match(mobile, /search_auth_callback_session_not_restored/u);
  assert.match(mobile, /data-authorized-search.*is-authorized/su);
  assert.match(mobile, /driver\.keys\('\\uE007'\)/u);
  assert.match(mobile, /focusIosSafariWebInput/u);
  assert.match(mobile, /performNativeTouchSwipe/u);
  assert.doesNotMatch(mobile, /mobile: scrollGesture/u);
  assert.match(mobile, /mobile: scroll/u);
  assert.match(mobile, /connectionRetryTimeout: Number\(options\.connectionRetryTimeout \|\| 300_000\)/u);
  assert.match(mobile, /connectionRetryCount: 0/u);
  assert.doesNotMatch(mobile, /screenshot|pageSource|getPageSource|\bhar\b|trace|video/iu);
  assert.match(android, /buildAppiumCapabilities/u);
  assert.match(ios, /buildAppiumCapabilities/u);
  assert.match(mobile, /prepareIosSafariWebContext/u);
  assert.doesNotMatch(ios, /webviewConnectTimeout|webviewConnectRetries|browserName:\s*'Safari'/u);
});

test('runner is fail-closed on exact secret target SHA and carries three incident queries', async () => {
  const runner = await readFile(new URL('../e2e/search/run.mjs', import.meta.url), 'utf8');
  const mobileIssuer = await readFile(new URL('../../.github/scripts/issue-static-search-session.mjs', import.meta.url), 'utf8');
  assert.match(runner, /E2E_EXPECTED_REPO_SHA/u);
  assert.match(runner, /candidate-build\.json/u);
  assert.match(runner, /static_secret_candidate_build_v1/u);
  assert.match(runner, /\{43\}/u);
  assert.match(runner, /На природу с детьми/u);
  assert.match(runner, /искусство у моря/u);
  assert.match(runner, /в пятницу бесплатно/u);
  assert.match(runner, /revisionPolicy === 'live_consistent' && mode === 'cached_vector'/u);
  assert.doesNotMatch(runner, /focus-email|mailbox|real-mail|otp/iu);
  assert.match(mobileIssuer, /createBrowserVerificationCallback/u);
  assert.doesNotMatch(mobileIssuer, /E2E_AUTH_ACTION_LINK=\$\{actionLink\}/u);
});
