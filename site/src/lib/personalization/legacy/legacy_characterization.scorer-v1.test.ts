import assert from 'node:assert/strict';
import test from 'node:test';

import type { LegacyProfileV1, LegacyRankCandidateV1, LegacyRankManifestV1 } from '../contract.ts';
import {
  LEGACY_SCORING_CONFIG_V1,
  legacyRankEventDetailRelatedV1,
  legacyRankPersonalFeedV1,
} from './scorer-v1.ts';

function candidate(id: number, overrides: Partial<LegacyRankCandidateV1> = {}): LegacyRankCandidateV1 {
  return {
    event_id: id,
    category: 'music',
    tags: ['jazz'],
    static_score: 0.5,
    lifecycle_status: 'active',
    venue_id: `venue-${id}`,
    reason_codes: [`fixture:${id}`],
    display: { likes_count: 0, shares_count: 0 },
    ...overrides,
  };
}

function profile(overrides: Partial<LegacyProfileV1> = {}): LegacyProfileV1 {
  return {
    consent_ok: true,
    profile_version: 'anon-profile-v1',
    feature_schema_version: 'event-detail-related-v1',
    taxonomy_version: 'event-taxonomy-v1',
    anon_id: '11111111-1111-4111-8111-111111111111',
    session_id: '22222222-2222-4222-8222-222222222222',
    positive_tags: {},
    negative_interest_tags: {},
    liked_event_ids: [],
    not_interested_event_ids: [],
    hidden_event_ids: [],
    seen_event_ids: [],
    seen_venue_ids: [],
    price_preferences: { prefer_free: false },
    share_counts: {},
    ...overrides,
  };
}

function manifest(items: LegacyRankCandidateV1[], current = 99): LegacyRankManifestV1 {
  return { event_id: current, feature_schema_version: 'event-detail-related-v1', taxonomy_version: 'event-taxonomy-v1', related_static: items };
}

test('legacy_characterization no profile preserves static score and stable numeric tie', () => {
  const plan = legacyRankEventDetailRelatedV1(manifest([
    candidate(3, { static_score: 0.7 }),
    candidate(2, { static_score: 0.7 }),
    candidate(4, { static_score: 0.6 }),
  ]), null, LEGACY_SCORING_CONFIG_V1);
  assert.deepEqual(plan.items.map((item) => item.event_id), [2, 3, 4]);
  assert.deepEqual(plan.items.map((item) => item.personal_score), [0.7, 0.7, 0.6]);
});

test('legacy_characterization excludes current, hidden, cancelled and postponed events', () => {
  const plan = legacyRankEventDetailRelatedV1(manifest([
    candidate(99),
    candidate(2),
    candidate(3, { lifecycle_status: 'cancelled' }),
    candidate(4, { ticket_status: 'postponed' }),
  ]), profile({ hidden_event_ids: ['2'] }), LEGACY_SCORING_CONFIG_V1);
  assert.deepEqual(plan.items, []);
});

test('legacy_characterization incompatible manifest profile falls back to static scoring', () => {
  const plan = legacyRankEventDetailRelatedV1(manifest([
    candidate(2, { static_score: 0.4 }),
    candidate(3, { static_score: 0.8 }),
  ]), profile({ feature_schema_version: 'other', liked_event_ids: ['2'], positive_tags: { jazz: 4 } }), LEGACY_SCORING_CONFIG_V1);
  assert.deepEqual(plan.items.map((item) => item.event_id), [3, 2]);
  assert.deepEqual(plan.items.map((item) => item.personal_score), [0.8, 0.4]);
});

test('legacy_characterization reproduces like-heavy affinity, negative hard filter and free preference', () => {
  const jazz = candidate(2, { static_score: 0.4, is_free: true });
  const theatre = candidate(3, { static_score: 0.5, category: 'theatre', tags: ['drama'] });
  const positive = legacyRankEventDetailRelatedV1(manifest([theatre, jazz]), profile({
    positive_tags: { jazz: 2 }, liked_event_ids: ['2'], price_preferences: { prefer_free: true },
  }), LEGACY_SCORING_CONFIG_V1);
  assert.deepEqual(positive.items.map((item) => item.event_id), [2, 3]);
  assert.equal(positive.items[0].personal_score, 1.06);
  const negative = legacyRankEventDetailRelatedV1(manifest([jazz, theatre]), profile({ negative_interest_tags: { jazz: 1 } }), LEGACY_SCORING_CONFIG_V1);
  assert.deepEqual(negative.items.map((item) => item.event_id), [3]);
});

test('legacy_characterization reproduces event and venue fatigue and diversity postponement', () => {
  const items = [
    candidate(1, { static_score: 0.9, venue_id: 'same' }),
    candidate(2, { static_score: 0.8, venue_id: 'same' }),
    candidate(3, { static_score: 0.7, venue_id: 'same' }),
    candidate(4, { static_score: 0.6, venue_id: 'other', category: 'theatre', tags: ['drama'] }),
  ];
  const plan = legacyRankEventDetailRelatedV1(manifest(items), profile({ seen_event_ids: ['1'], seen_venue_ids: ['same'] }), {
    ...LEGACY_SCORING_CONFIG_V1,
    maxSameVenue: 2,
  });
  assert.equal(plan.items.some((item) => item.diversity_postponed), true);
  assert.equal(plan.items.find((item) => item.event_id === 1)?.reason_codes.includes('profile:fatigue_penalty'), true);
});

test('legacy_characterization personal feed requires three signals and uses quarantined formula', () => {
  const data = manifest([
    candidate(2, { static_score: 0.4, display: { likes_count: 50, shares_count: 3 } }),
    candidate(3, { static_score: 0.8, category: 'theatre', tags: ['drama'] }),
  ]);
  assert.deepEqual(legacyRankPersonalFeedV1(data, profile({ liked_event_ids: ['2'] }), LEGACY_SCORING_CONFIG_V1).items, []);
  const ready = profile({ liked_event_ids: ['8', '9', '10'], positive_tags: { jazz: 1 } });
  const plan = legacyRankPersonalFeedV1(data, ready, LEGACY_SCORING_CONFIG_V1);
  assert.deepEqual(plan.items.map((item) => item.event_id), [2, 3]);
  assert.equal(plan.items[0].reason_codes.includes('catalog:popularity'), true);
});

test('legacy_characterization config is labeled legacy and cannot be mistaken for target scorer', () => {
  assert.equal(LEGACY_SCORING_CONFIG_V1.related.staticContext, 0.8);
  assert.equal(Object.isFrozen(LEGACY_SCORING_CONFIG_V1), true);
  assert.equal(Object.isFrozen(LEGACY_SCORING_CONFIG_V1.related), true);
});
