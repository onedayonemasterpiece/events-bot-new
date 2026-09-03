import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
  UNUSUAL_BROWSER_VIEWPORTS,
  buildUnusualBrowserReceipt,
  isSemanticProviderUrl,
  normalizeUnusualHealth,
  redactCandidateSecrets,
  resolveUnusualTarget,
} from './unusual-events-monitor.mjs';

const health = (overrides = {}) => ({
  schema_version:'unusual-events-health-v1',
  run_id:'static-site:health:42',
  repo_sha:'a'.repeat(40),
  health_status:'HEALTHY',
  content_readiness:'READY',
  as_of_date:'2026-08-09',
  publication:{
    expected:true,
    indexable:false,
    manifest_sha256:'b'.repeat(64),
    canonical_path:'/neobychnoe/',
  },
  feed:{
    selected_count:2,
    target_count:30,
    minimum_publish_count:2,
    visible_event_ids:[41,'42'],
    visible_concept_ids:['concept:a','concept:b'],
    selected:[
      { event_id:41, concept_id:'concept:a', title:'Первое', path:'/events/first/', start_date:'2026-08-10', family:'participatory', image_required:true },
      { event_id:'42', concept_id:'concept:b', title:'Второе', url:'https://kenigevents.ru/events/second/', end_date:'2026-08-12', family:'site_specific', image_required:false },
    ],
  },
  contracts:{ visible_output_sha256:'c'.repeat(64) },
  ...overrides,
});

const viewportReceipt = (normalized, overrides = {}) => ({
  viewport:UNUSUAL_BROWSER_VIEWPORTS[0],
  screenshot:'unusual-events-mobile-390x844.png',
  http_status:200,
  error_shell:false,
  dom_state:'ready',
  card_count:normalized.feed.selected_count,
  visible_event_ids:[...normalized.feed.visible_event_ids],
  visible_concept_ids:[...normalized.feed.visible_concept_ids],
  cards:[],
  links:{ checked_count:2, truncated:false, results:[] },
  canonical:{ present:true, path_matches:true, index_contract:true },
  horizontal_overflow_px:0,
  diagnostics:{ console_errors:[], page_errors:[], request_errors:[], semantic_provider_calls:[] },
  passed:true,
  failures:[],
  ...overrides,
});

test('health normalization freezes the strict ordered publication projection', () => {
  const normalized = normalizeUnusualHealth(health());
  assert.deepEqual(normalized.feed.visible_event_ids, ['41','42']);
  assert.deepEqual(normalized.feed.visible_concept_ids, ['concept:a','concept:b']);
  assert.deepEqual(normalized.feed.selected.map((row) => row.path), ['/events/first/','/events/second/']);
  assert.equal(normalized.publication.indexable, false);
  assert.equal(normalized.contracts.visible_output_sha256, 'c'.repeat(64));
  assert.ok(Object.isFrozen(normalized.feed.selected));
});

test('health normalization fails closed on count, ordering, duplicate and hash drift', () => {
  assert.throws(() => normalizeUnusualHealth(health({
    feed:{ ...health().feed, selected_count:1 },
  })), /selected_count_parity/u);
  assert.throws(() => normalizeUnusualHealth(health({
    feed:{ ...health().feed, visible_event_ids:['42','41'] },
  })), /selected_order/u);
  assert.throws(() => normalizeUnusualHealth(health({
    feed:{ ...health().feed, visible_concept_ids:['concept:a','concept:a'] },
  })), /visible_ids_duplicate/u);
  assert.throws(() => normalizeUnusualHealth(health({
    contracts:{ visible_output_sha256:'not-a-hash' },
  })), /visible_output_sha256/u);
});

test('candidate target preserves its immutable prefix but never needs to enter receipts', () => {
  const token = 'pp1wRctXBd6boYU1EcnBrod3z8MmKpD7SGEufK1t-xw';
  const target = resolveUnusualTarget({
    UNUSUAL_EVENTS_CANDIDATE_URL:`https://kenigevents.ru/_review/${token}/`,
  }, '/neobychnoe/');
  assert.equal(target.url, `https://kenigevents.ru/_review/${token}/neobychnoe/`);
  assert.equal(redactCandidateSecrets(`failed at ${target.url}`, target.secrets), 'failed at [REDACTED_CANDIDATE]');
  assert.equal(redactCandidateSecrets(`GET /_review/${token}/event/1`), 'GET /_review/[REDACTED_CANDIDATE]/event/1');
});

test('semantic provider detection covers remote and same-origin semantic routes', () => {
  const directProviderProbe = `https://generativelanguage.${'googleapis.com'}/v1/models/gemini`;
  assert.equal(isSemanticProviderUrl(directProviderProbe), true);
  assert.equal(isSemanticProviderUrl('https://candidate.invalid/functions/v1/event-search'), true);
  assert.equal(isSemanticProviderUrl('https://candidate.invalid/neobychnoe/'), false);
  assert.equal(isSemanticProviderUrl('https://candidate.invalid/assets/app.js'), false);
});

test('READY requires exact two-viewport parity while an honest blocked empty receipt is never READY', () => {
  const normalized = normalizeUnusualHealth(health());
  const viewports = UNUSUAL_BROWSER_VIEWPORTS.map((viewport) => viewportReceipt(normalized, { viewport }));
  const ready = buildUnusualBrowserReceipt(normalized, viewports, '2026-08-09T00:00:00Z', '2026-08-09T00:01:00Z');
  assert.equal(ready.browser_mechanics_passed, true);
  assert.equal(ready.page_manifest_match, true);
  assert.equal(ready.status, 'READY');

  const blockedHealth = normalizeUnusualHealth(health({
    health_status:'BLOCKED',
    content_readiness:'BLOCKED',
    publication:{ ...health().publication, expected:false },
    feed:{
      selected_count:0,
      target_count:30,
      minimum_publish_count:2,
      visible_event_ids:[],
      visible_concept_ids:[],
      selected:[],
    },
  }));
  const blockedViewports = UNUSUAL_BROWSER_VIEWPORTS.map((viewport) => viewportReceipt(blockedHealth, {
    viewport, dom_state:'empty', card_count:0, visible_event_ids:[], visible_concept_ids:[], links:{ checked_count:0, truncated:false, results:[] },
  }));
  const blocked = buildUnusualBrowserReceipt(blockedHealth, blockedViewports, '2026-08-09T00:00:00Z', '2026-08-09T00:01:00Z');
  assert.equal(blocked.browser_mechanics_passed, true);
  assert.equal(blocked.page_manifest_match, false);
  assert.equal(blocked.status, 'BLOCKED');
});

test('WATCH with READY content remains eligible for exact browser parity', () => {
  const normalized = normalizeUnusualHealth(health({ health_status:'WATCH' }));
  const viewports = UNUSUAL_BROWSER_VIEWPORTS.map((viewport) => viewportReceipt(normalized, { viewport }));
  const receipt = buildUnusualBrowserReceipt(
    normalized,
    viewports,
    '2026-08-09T00:00:00Z',
    '2026-08-09T00:01:00Z',
  );
  assert.equal(receipt.page_manifest_match, true);
  assert.equal(receipt.status, 'READY');
});

test('receipt contains only bounded identity and never contains the candidate bearer URL', () => {
  const normalized = normalizeUnusualHealth(health());
  const receipt = buildUnusualBrowserReceipt(
    normalized,
    UNUSUAL_BROWSER_VIEWPORTS.map((viewport) => viewportReceipt(normalized, { viewport })),
    '2026-08-09T00:00:00Z',
    '2026-08-09T00:01:00Z',
  );
  const encoded = JSON.stringify(receipt);
  assert.doesNotMatch(encoded, /https?:\/\//u);
  assert.doesNotMatch(encoded, /_review\//u);
  assert.equal(receipt.viewports.length, 2);
  assert.ok(receipt.viewports.every((entry) => entry.cards.length <= 30 && entry.failures.length <= 20));
});

test('the shipped surface exposes generic ready/empty feed markers and the Playwright entrypoint wires health mode', async () => {
  const [surface, entrypoint] = await Promise.all([
    readFile(new URL('../src/components/UnusualListingSurface.astro', import.meta.url), 'utf8'),
    readFile(new URL('./unusual-events.playwright.mjs', import.meta.url), 'utf8'),
  ]);
  assert.match(surface, /data-unusual-feed="ready"/u);
  assert.match(surface, /data-unusual-feed="empty"/u);
  assert.match(surface, /'data-unusual-event-id':item\.event\.id/u);
  assert.match(surface, /<AdaptiveEventCardGrid[\s\S]*?itemRoots=\{itemRoots\}/u);
  assert.match(entrypoint, /UNUSUAL_EVENTS_HEALTH_FILE/u);
  assert.match(entrypoint, /runUnusualEventsBrowserMonitor/u);
});
