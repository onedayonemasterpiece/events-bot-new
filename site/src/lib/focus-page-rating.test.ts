import assert from 'node:assert/strict';
import test from 'node:test';
import {
  FOCUS_PAGE_RATING_TTL_MS,
  focusPageRatingStorageKey,
  focusPageRatingType,
  readFocusPageRating,
  writeFocusPageRating,
} from './focus-page-rating.ts';

class MemoryStorage {
  values = new Map<string, string>();
  getItem(key: string) { return this.values.get(key) ?? null; }
  setItem(key: string, value: string) { this.values.set(key, value); }
  removeItem(key: string) { this.values.delete(key); }
}

test('calendar routes share one page-type rating', () => {
  assert.equal(focusPageRatingType('today'), 'calendar');
  assert.equal(focusPageRatingType('tomorrow'), 'calendar');
  assert.equal(focusPageRatingType('calendar_date'), 'calendar');
  assert.equal(focusPageRatingStorageKey('u-1', 'today'), focusPageRatingStorageKey('u-1', 'calendar_date'));
});

test('rating survives navigation for 24 hours and can be replaced', () => {
  const storage = new MemoryStorage();
  const now = Date.UTC(2026, 6, 29, 12);
  writeFocusPageRating(storage, 'u-1', 'today', 7, now);
  assert.equal(readFocusPageRating(storage, 'u-1', 'tomorrow', now + FOCUS_PAGE_RATING_TTL_MS - 1)?.score, 7);
  writeFocusPageRating(storage, 'u-1', 'calendar_date', 9, now + 1_000);
  assert.equal(readFocusPageRating(storage, 'u-1', 'today', now + 2_000)?.score, 9);
  assert.equal(readFocusPageRating(storage, 'u-1', 'today', now + 1_000 + FOCUS_PAGE_RATING_TTL_MS), null);
});

test('rating is isolated by user and page type', () => {
  const storage = new MemoryStorage();
  writeFocusPageRating(storage, 'u-1', 'event_detail', 10, 100);
  assert.equal(readFocusPageRating(storage, 'u-2', 'event_detail', 200), null);
  assert.equal(readFocusPageRating(storage, 'u-1', 'home', 200), null);
  assert.equal(readFocusPageRating(storage, 'u-1', 'event_detail', 200)?.score, 10);
});
