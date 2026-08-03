import type {
  LegacyProfileHorizonV1,
  LegacyProfileParseResultV1,
  LegacyProfileV1,
  LegacyProfileV1Diagnostic,
} from '../contract.ts';

export const LEGACY_PROFILE_STORAGE_KEY_V1 = 'ke_personalization_profile' as const;
export const LEGACY_PROFILE_MAX_BYTES_V1 = 64 * 1024;
export const LEGACY_PROFILE_ARRAY_CAP_V1 = 384;
export const LEGACY_PROFILE_MAP_CAP_V1 = 128;

export interface LegacyProfileCompatibilityV1 {
  featureSchemaVersion?: string;
  taxonomyVersion?: string;
}

const UUID_V1 = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu;
const MAP_FIELDS = ['positive_tags', 'positive_categories', 'negative_interest_tags', 'positive_time_tags'] as const;
const ARRAY_FIELDS = ['liked_event_ids', 'not_interested_event_ids', 'hidden_event_ids', 'seen_event_ids', 'seen_venue_ids'] as const;
const HORIZON_FIELDS = ['session', 'short', 'mid', 'long'] as const;

function byteLength(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function boundedStringArray(value: unknown): string[] | null {
  if (value == null) return [];
  if (!Array.isArray(value) || value.length > LEGACY_PROFILE_ARRAY_CAP_V1) return null;
  const result: string[] = [];
  for (const item of value) {
    if (typeof item !== 'string' && typeof item !== 'number') return null;
    const normalized = String(item);
    if (!normalized || normalized.length > 160) return null;
    result.push(normalized);
  }
  return result;
}

function boundedNumberMap(value: unknown): Record<string, number> | null {
  if (value == null) return {};
  if (!isRecord(value)) return null;
  const entries = Object.entries(value);
  if (entries.length > LEGACY_PROFILE_MAP_CAP_V1) return null;
  const result: Record<string, number> = {};
  for (const [key, item] of entries) {
    const numeric = Number(item);
    if (!key || key.length > 160 || !Number.isFinite(numeric)) return null;
    result[key] = numeric;
  }
  return result;
}

function sanitizeHorizon(value: unknown): LegacyProfileHorizonV1 | null {
  if (value == null) return {};
  if (!isRecord(value)) return null;
  const result: LegacyProfileHorizonV1 = {};
  for (const field of MAP_FIELDS) {
    if (!(field in value)) continue;
    const map = boundedNumberMap(value[field]);
    if (!map) return null;
    result[field] = map;
  }
  return result;
}

function reject(diagnostic: LegacyProfileV1Diagnostic, bytes: number): LegacyProfileParseResultV1 {
  return { profile: null, diagnostic, byteSize: bytes };
}

export function parseLegacyProfileV1(
  serialized: string | null | undefined,
  compatibility: LegacyProfileCompatibilityV1 = {},
): LegacyProfileParseResultV1 {
  if (!serialized) return reject('legacy_profile_v1.empty', 0);
  const bytes = byteLength(serialized);
  if (bytes > LEGACY_PROFILE_MAX_BYTES_V1) return reject('legacy_profile_v1.oversized_bytes', bytes);
  let value: unknown;
  try {
    value = JSON.parse(serialized);
  } catch {
    return reject('legacy_profile_v1.malformed_json', bytes);
  }
  if (!isRecord(value)) return reject('legacy_profile_v1.invalid_shape', bytes);
  if (value.consent_ok !== true) return reject('legacy_profile_v1.missing_consent', bytes);
  if (value.profile_version !== 'anon-profile-v1') return reject('legacy_profile_v1.incompatible_profile_version', bytes);
  if (compatibility.featureSchemaVersion && value.feature_schema_version !== compatibility.featureSchemaVersion) {
    return reject('legacy_profile_v1.incompatible_feature_schema', bytes);
  }
  if (compatibility.taxonomyVersion && value.taxonomy_version !== compatibility.taxonomyVersion) {
    return reject('legacy_profile_v1.incompatible_taxonomy', bytes);
  }
  if (typeof value.feature_schema_version !== 'string' || typeof value.taxonomy_version !== 'string') {
    return reject('legacy_profile_v1.invalid_shape', bytes);
  }
  if (typeof value.anon_id !== 'string' || typeof value.session_id !== 'string'
      || !UUID_V1.test(value.anon_id) || !UUID_V1.test(value.session_id)) {
    return reject('legacy_profile_v1.invalid_uuid', bytes);
  }
  if (Object.prototype.hasOwnProperty.call(value, 'negative_tags')) {
    return reject('legacy_profile_v1.obsolete_negative_tags', bytes);
  }

  const maps: Record<string, Record<string, number>> = {};
  for (const field of [...MAP_FIELDS, 'share_counts'] as const) {
    const map = boundedNumberMap(value[field]);
    if (!map) return reject('legacy_profile_v1.collection_cap_exceeded', bytes);
    maps[field] = map;
  }
  const arrays: Record<string, string[]> = {};
  for (const field of ARRAY_FIELDS) {
    const items = boundedStringArray(value[field]);
    if (!items) return reject('legacy_profile_v1.collection_cap_exceeded', bytes);
    arrays[field] = items;
  }
  const horizons: Partial<Record<(typeof HORIZON_FIELDS)[number], LegacyProfileHorizonV1>> = {};
  for (const field of HORIZON_FIELDS) {
    if (!(field in value)) continue;
    const horizon = sanitizeHorizon(value[field]);
    if (!horizon) return reject('legacy_profile_v1.collection_cap_exceeded', bytes);
    horizons[field] = horizon;
  }
  const price = isRecord(value.price_preferences) ? value.price_preferences : {};
  const profile: LegacyProfileV1 = {
    consent_ok: true,
    profile_version: 'anon-profile-v1',
    feature_schema_version: value.feature_schema_version,
    taxonomy_version: value.taxonomy_version,
    anon_id: value.anon_id,
    session_id: value.session_id,
    positive_tags: maps.positive_tags,
    positive_categories: maps.positive_categories,
    negative_interest_tags: maps.negative_interest_tags,
    positive_time_tags: maps.positive_time_tags,
    liked_event_ids: arrays.liked_event_ids,
    not_interested_event_ids: arrays.not_interested_event_ids,
    hidden_event_ids: arrays.hidden_event_ids,
    seen_event_ids: arrays.seen_event_ids,
    seen_venue_ids: arrays.seen_venue_ids,
    price_preferences: { prefer_free: price.prefer_free === true },
    share_counts: maps.share_counts,
    ...horizons,
    ...(typeof value.updated_at === 'string' ? { updated_at: value.updated_at } : {}),
  };
  return { profile, diagnostic: 'legacy_profile_v1.valid', byteSize: bytes };
}
