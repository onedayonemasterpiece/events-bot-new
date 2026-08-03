import assert from 'node:assert/strict';
import test from 'node:test';
import { assertPopularOccurrenceCollapse } from './popular-occurrence-contract.mjs';

const event = (id, otherDateIds = []) => ({ id, other_date_ids: otherDateIds });

test('Popular preview accepts a ranked selection with no repeated-date family', () => {
  assert.doesNotThrow(() => assertPopularOccurrenceCollapse({
    desktopIds: ['10', '20'],
    temporalLabels: ['3 августа · 18:00', '4 августа · 19:00'],
    events: [event(10), event(20)],
  }));
});

test('Popular preview requires one card and the matching summary for a repeated-date family', () => {
  assert.doesNotThrow(() => assertPopularOccurrenceCollapse({
    desktopIds: ['10', '30'],
    temporalLabels: ['3 августа · 18:00 · ещё 2 показа', '5 августа · 20:00'],
    events: [event(10, [11, 12]), event(11, [10, 12]), event(12, [10, 11]), event(30)],
  }));

  assert.throws(() => assertPopularOccurrenceCollapse({
    desktopIds: ['10', '11'],
    temporalLabels: ['3 августа · 18:00 · ещё 1 показ', '4 августа · 18:00 · ещё 1 показ'],
    events: [event(10, [11]), event(11, [10])],
  }), /renders linked occurrence 11 as a second card/u);

  assert.throws(() => assertPopularOccurrenceCollapse({
    desktopIds: ['10'],
    temporalLabels: ['3 августа · 18:00'],
    events: [event(10, [11]), event(11, [10])],
  }), /must summarize 1 linked occurrence/u);

  assert.throws(() => assertPopularOccurrenceCollapse({
    desktopIds: ['10'],
    temporalLabels: ['3 августа · 18:00 · ещё 10 показов'],
    events: [event(10, [11]), event(11, [10])],
  }), /must summarize 1 linked occurrence/u);
});
