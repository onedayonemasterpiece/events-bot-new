import assert from 'node:assert/strict';
import test from 'node:test';

import { buildTargetPresenterPlanV1 } from './presenter-plan.ts';
import { resolvePersonalizationRuntimeMode } from './runtime-mode.ts';
import { resolveRouteSurfaceV1, resolveSurfacePolicyV1 } from './surface-policy.ts';
import { buildPersonalizationTestSnapshotV1 } from './test-api.ts';

test('target policy unknown fails closed without signal or network', () => {
  const policy = resolveSurfacePolicyV1('not-a-surface');
  assert.equal(policy.id, 'unknown-static');
  assert.equal(policy.rankingMode, 'identity');
  assert.equal(policy.reorderScope, 'none');
  assert.equal(policy.signalCollection, 'none');
  assert.equal(policy.networkOnPageView, false);
});

test('calendar route stays identity while personal tail is a separate policy', () => {
  assert.equal(resolveRouteSurfaceV1('/segodnya/').policy.id, 'calendar-exact-only');
  assert.equal(resolveRouteSurfaceV1('/date-2026-08-03/').policy.id, 'calendar-exact-only');
  assert.equal(resolveSurfacePolicyV1('calendar_personal_tail').id, 'calendar-personal-tail');
});

test('related, search, popular and thematic policies remain distinct target intents', () => {
  assert.equal(resolveRouteSurfaceV1('/sobytiya/example/').policy.id, 'related-anchor-first');
  assert.equal(resolveRouteSurfaceV1('/poisk/').policy.id, 'search-query-first');
  assert.equal(resolveRouteSurfaceV1('/populyarnoe/').policy.id, 'popular-tiebreak');
  assert.equal(resolveRouteSurfaceV1('/vystavki/').policy.id, 'thematic-weak');
});

test('presenter target shadow keeps frozen positions and never duplicates or drops', () => {
  const plan = buildTargetPresenterPlanV1({
    policyId: 'thematic-weak',
    currentOrder: ['a', 'b', 'c', 'd'],
    targetRanks: [
      { eventId: 'd', targetRank: 0 },
      { eventId: 'c', targetRank: 1 },
      { eventId: 'a', targetRank: 2 },
    ],
    frozenIds: ['a', 'b'],
  });
  assert.deepEqual(plan.plannedOrder, ['a', 'b', 'd', 'c']);
  assert.deepEqual([...plan.plannedOrder].sort(), ['a', 'b', 'c', 'd']);
  assert.equal(plan.applied, false);
});

test('calendar and unknown presenter plans are identity regardless of target ranks', () => {
  for (const policyId of ['calendar-exact-only', 'unknown-static'] as const) {
    const plan = buildTargetPresenterPlanV1({
      policyId,
      currentOrder: ['1', '2', '3'],
      targetRanks: [{ eventId: '3', targetRank: 0 }],
      frozenIds: [],
    });
    assert.deepEqual(plan.plannedOrder, ['1', '2', '3']);
  }
});

test('runtime modes default safely and unknown values switch off', () => {
  assert.equal(resolvePersonalizationRuntimeMode('', 'production').mode, 'off');
  assert.equal(resolvePersonalizationRuntimeMode('', 'preview').mode, 'characterize');
  assert.equal(resolvePersonalizationRuntimeMode('local-shadow', 'production').mode, 'local-shadow');
  assert.deepEqual(resolvePersonalizationRuntimeMode('on', 'preview'), { mode: 'off', diagnostic: 'p13n_mode.invalid_off' });
});

test('test API is bounded and contains no identity or raw profile fields', () => {
  const route = resolveRouteSurfaceV1('/poisk/');
  const snapshot = buildPersonalizationTestSnapshotV1({
    mode: 'characterize',
    route,
    legacyProfileByteSize: 123,
    legacyParity: { ids: ['1'], scores: [0.123456] },
    networkCounters: { total: 4, reads: 4, writes: 0 },
  });
  assert.equal(snapshot.legacy_parity_plan.scores[0], 0.1235);
  assert.equal(snapshot.network_request_counters.writes, 0);
  assert.equal(snapshot.network_request_counters.harness_supplied, true);
  const serialized = JSON.stringify(snapshot);
  for (const secret of ['anon_id', 'session_id', 'email', 'token', 'raw_profile', 'action_log']) {
    assert.equal(serialized.includes(secret), false);
  }
});
