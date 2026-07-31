import assert from 'node:assert/strict';
import test from 'node:test';
import {
  FOCUS_FEEDBACK_SCORE_MAX_BYTES,
  FOCUS_FEEDBACK_SCORE_STORAGE_KEY,
  FOCUS_FEEDBACK_SCORE_TTL_MS,
  readFocusFeedbackScores,
  rememberFocusFeedbackScore,
} from './focus-feedback-state.ts';

class MemoryStorage {
  values = new Map<string, string>();
  getItem(key: string) { return this.values.get(key) ?? null; }
  setItem(key: string, value: string) { this.values.set(key, value); }
  removeItem(key: string) { this.values.delete(key); }
}

test('page-family score survives navigation for 24 hours and can be changed', () => {
  const storage = new MemoryStorage();
  rememberFocusFeedbackScore(storage, 'calendar_date', 7, 1_000);
  assert.equal(readFocusFeedbackScores(storage, 2_000).calendar_date?.score, 7);
  rememberFocusFeedbackScore(storage, 'calendar_date', 9, 3_000);
  assert.equal(readFocusFeedbackScores(storage, 4_000).calendar_date?.score, 9);
});

test('expired and malformed state is removed', () => {
  const storage = new MemoryStorage();
  rememberFocusFeedbackScore(storage, 'home', 8, 1_000);
  assert.deepEqual(readFocusFeedbackScores(storage, 1_000 + FOCUS_FEEDBACK_SCORE_TTL_MS), {});
  storage.setItem(FOCUS_FEEDBACK_SCORE_STORAGE_KEY, '{oops');
  assert.deepEqual(readFocusFeedbackScores(storage, 2_000), {});
  assert.equal(storage.getItem(FOCUS_FEEDBACK_SCORE_STORAGE_KEY), null);
});

test('storage remains bounded and rejects invalid families and scores', () => {
  const storage = new MemoryStorage();
  assert.equal(rememberFocusFeedbackScore(storage, '../bad', 2), null);
  assert.equal(rememberFocusFeedbackScore(storage, 'home', 11), null);
  for (let index = 0; index < 80; index += 1) {
    rememberFocusFeedbackScore(storage, `family_${index}`, index % 11, 10_000 + index);
  }
  const raw = storage.getItem(FOCUS_FEEDBACK_SCORE_STORAGE_KEY) || '';
  assert.ok(new TextEncoder().encode(raw).byteLength <= FOCUS_FEEDBACK_SCORE_MAX_BYTES);
  assert.ok(Object.keys(readFocusFeedbackScores(storage, 20_000)).length <= 24);
});
