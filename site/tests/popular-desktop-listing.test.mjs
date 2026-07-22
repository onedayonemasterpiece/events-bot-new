import test from 'node:test';
import assert from 'node:assert/strict';
import {
  isCompatiblePopularProfile,
  popularProfileSignalCount,
  selectPopularPersonalizedCandidates,
} from '../src/lib/popularPersonalization.mjs';

const baseProfile = {
  consent_ok: true,
  profile_version: 'anon-profile-v1',
  feature_schema_version: 'event-detail-related-v1',
  taxonomy_version: 'event-taxonomy-v1',
  anon_id: '018f47a2-a011-4e73-8f73-111111111111',
  session_id: '018f47a2-a011-4e73-8f73-222222222222',
  positive_tags: { music: 1 },
  positive_categories: {},
  negative_interest_tags: {},
  liked_event_ids: ['90', '91', '92'],
  not_interested_event_ids: [],
  hidden_event_ids: [],
  share_counts: {},
};

const candidates = [
  { id: 1, category: 'concert', tags: ['music'], baseScore: 10 },
  { id: 2, category: 'concert', tags: ['music'], baseScore: 9 },
  { id: 3, category: 'festival', tags: ['music'], baseScore: 8 },
  { id: 4, category: 'club', tags: ['music'], baseScore: 7 },
  { id: 5, category: 'lecture', tags: ['education'], baseScore: 6 },
  { id: 6, category: 'concert', tags: ['music'], baseScore: 5 },
];

test('Popular personal shelf stays absent without compatible warm consent', () => {
  assert.equal(isCompatiblePopularProfile({ ...baseProfile, consent_ok: false }), false);
  assert.equal(popularProfileSignalCount({ ...baseProfile, liked_event_ids: ['90', '91'] }), 2);
  assert.deepEqual(selectPopularPersonalizedCandidates(candidates, { ...baseProfile, liked_event_ids: ['90', '91'] }), []);
});

test('Popular personal shelf selects four affinity cards and one anti-bubble card', () => {
  const selected = selectPopularPersonalizedCandidates(candidates, baseProfile);
  assert.equal(selected.length, 5);
  assert.deepEqual(selected.slice(0, 4), ['1', '2', '3', '4']);
  assert.equal(selected[4], '5');
});

test('Popular personal shelf excludes hidden events and never underfills visibly', () => {
  const selected = selectPopularPersonalizedCandidates(candidates, {
    ...baseProfile,
    hidden_event_ids: ['1', '2'],
  });
  assert.deepEqual(selected, []);
});
