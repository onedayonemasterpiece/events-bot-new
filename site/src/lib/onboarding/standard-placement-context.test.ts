import assert from 'node:assert/strict';
import test from 'node:test';
import {
  resolveStandardOnboardingPlacementContext,
} from './standard-placement-context.ts';

test('standard onboarding route contexts expose one inert page-end slot', () => {
  const cases = [
    ['/', 'home'],
    ['/preview-secret.1/segodnya/', 'listing'],
    ['/sobytiya/example-1/', 'event_detail'],
    ['/poisk/', 'search'],
    ['/dlya-menya/', 'personal'],
    ['/partners/', 'information'],
  ] as const;
  for (const [path, expected] of cases) {
    const result = resolveStandardOnboardingPlacementContext(path);
    assert.equal(result.routeContext, expected);
    assert.equal(result.placementSlot, 'page_end');
    assert.equal(result.runtimeMode, 'inert');
    assert.deepEqual(
      [result.artifactProgram, result.clubProgram, result.raffleProgram],
      ['disabled', 'disabled', 'disabled'],
    );
  }
});
