import assert from 'node:assert/strict';
import test from 'node:test';

import {
  LEGACY_PROFILE_ARRAY_CAP_V1,
  LEGACY_PROFILE_MAP_CAP_V1,
  LEGACY_PROFILE_MAX_BYTES_V1,
  parseLegacyProfileV1,
} from './profile-v1.ts';

const uuidA = '11111111-1111-4111-8111-111111111111';
const uuidB = '22222222-2222-4222-8222-222222222222';

function valid(overrides: Record<string, unknown> = {}) {
  return JSON.stringify({
    consent_ok: true,
    profile_version: 'anon-profile-v1',
    feature_schema_version: 'event-detail-related-v1',
    taxonomy_version: 'event-taxonomy-v1',
    anon_id: uuidA,
    session_id: uuidB,
    positive_tags: { jazz: 1 },
    negative_interest_tags: {},
    liked_event_ids: ['10'],
    not_interested_event_ids: [],
    hidden_event_ids: [],
    seen_event_ids: [],
    seen_venue_ids: [],
    price_preferences: { prefer_free: false },
    share_counts: {},
    ...overrides,
  });
}

test('legacy_characterization profile parser accepts only current compatible profile', () => {
  const result = parseLegacyProfileV1(valid(), {
    featureSchemaVersion: 'event-detail-related-v1',
    taxonomyVersion: 'event-taxonomy-v1',
  });
  assert.equal(result.diagnostic, 'legacy_profile_v1.valid');
  assert.equal(result.profile?.anon_id, uuidA);
  assert.deepEqual(result.profile?.liked_event_ids, ['10']);
});

test('legacy_characterization profile parser rejects empty, corrupt and invalid UUID', () => {
  assert.equal(parseLegacyProfileV1(null).diagnostic, 'legacy_profile_v1.empty');
  assert.equal(parseLegacyProfileV1('{').diagnostic, 'legacy_profile_v1.malformed_json');
  assert.equal(parseLegacyProfileV1(valid({ anon_id: 'anon-1' })).diagnostic, 'legacy_profile_v1.invalid_uuid');
});

test('legacy_characterization profile parser rejects incompatible versions and obsolete negative_tags', () => {
  assert.equal(parseLegacyProfileV1(valid({ profile_version: 'v2' })).diagnostic, 'legacy_profile_v1.incompatible_profile_version');
  assert.equal(parseLegacyProfileV1(valid(), { featureSchemaVersion: 'other' }).diagnostic, 'legacy_profile_v1.incompatible_feature_schema');
  assert.equal(parseLegacyProfileV1(valid(), { taxonomyVersion: 'other' }).diagnostic, 'legacy_profile_v1.incompatible_taxonomy');
  assert.equal(parseLegacyProfileV1(valid({ negative_tags: { jazz: 1 } })).diagnostic, 'legacy_profile_v1.obsolete_negative_tags');
});

test('legacy_characterization profile parser fails bounded arrays, maps and bytes closed', () => {
  const oversizedArray = Array.from({ length: LEGACY_PROFILE_ARRAY_CAP_V1 + 1 }, (_, index) => String(index));
  assert.equal(parseLegacyProfileV1(valid({ liked_event_ids: oversizedArray })).diagnostic, 'legacy_profile_v1.collection_cap_exceeded');
  const oversizedMap = Object.fromEntries(Array.from({ length: LEGACY_PROFILE_MAP_CAP_V1 + 1 }, (_, index) => [`tag-${index}`, 1]));
  assert.equal(parseLegacyProfileV1(valid({ positive_tags: oversizedMap })).diagnostic, 'legacy_profile_v1.collection_cap_exceeded');
  const huge = `${valid().slice(0, -1)},"padding":"${'x'.repeat(LEGACY_PROFILE_MAX_BYTES_V1)}"}`;
  assert.equal(parseLegacyProfileV1(huge).diagnostic, 'legacy_profile_v1.oversized_bytes');
});

test('legacy_characterization parser is read-only and never synthesizes consent', () => {
  const source = valid({ consent_ok: false });
  const result = parseLegacyProfileV1(source);
  assert.equal(result.diagnostic, 'legacy_profile_v1.missing_consent');
  assert.equal(result.profile, null);
  assert.equal(source.includes('"consent_ok":false'), true);
});
