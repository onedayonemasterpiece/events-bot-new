import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildFocusRecommendations,
  FOCUS_STORAGE_KEY,
  focusInterestIdsForEvent,
} from './focus-personalization.ts';
import type { PreviewEvent } from './types';

function specimen(id: number, title: string, topics: string[]): PreviewEvent {
  return { id, title, topics } as PreviewEvent;
}

test('presentation mapping consumes only already-projected topics', () => {
  const untaggedJazz = specimen(1, 'Большой джазовый концерт', []);
  assert.deepEqual(focusInterestIdsForEvent(untaggedJazz), []);

  const projected = specimen(2, 'Нейтральное название', [
    'THEATRE_CLASSIC',
    'THEATRE',
    'LECTURES',
    'UNKNOWN_TOPIC',
  ]);
  assert.deepEqual(focusInterestIdsForEvent(projected), ['theatre', 'talks']);
});

test('recommendation explanation is honest when a projected topic is unavailable', () => {
  const [recommendation] = buildFocusRecommendations([
    specimen(3, 'Событие без подходящей темы', ['CONCERTS']),
  ]);
  assert.deepEqual(recommendation.interestIds, []);
  assert.match(recommendation.fallbackReason, /нет достаточно точного совпадения/u);
  assert.doesNotMatch(recommendation.fallbackReason, /персональн(?:о|ая) подходит/u);
});

test('personalization storage is namespaced independently from focus access', () => {
  assert.equal(FOCUS_STORAGE_KEY, 'kenigevents.focus-personalization.prototype.v1');
  assert.doesNotMatch(FOCUS_STORAGE_KEY, /focus-preview/u);
});
