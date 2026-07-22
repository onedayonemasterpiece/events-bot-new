export const POPULAR_PROFILE_VERSION = 'anon-profile-v1';
export const POPULAR_FEATURE_SCHEMA_VERSION = 'event-detail-related-v1';
export const POPULAR_TAXONOMY_VERSION = 'event-taxonomy-v1';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu;

function values(value) {
  return Array.isArray(value) ? value : [];
}

function weightedMap(profile, fieldName) {
  const result = { ...(profile?.[fieldName] || {}) };
  for (const [horizon, weight] of [['session', 1], ['short', 0.65], ['mid', 0.35], ['long', 0.2]]) {
    for (const [key, raw] of Object.entries(profile?.[horizon]?.[fieldName] || {})) {
      result[key] = Number(result[key] || 0) + Number(raw || 0) * weight;
    }
  }
  return result;
}

export function isCompatiblePopularProfile(profile) {
  return Boolean(
    profile?.consent_ok === true
    && profile.profile_version === POPULAR_PROFILE_VERSION
    && profile.feature_schema_version === POPULAR_FEATURE_SCHEMA_VERSION
    && profile.taxonomy_version === POPULAR_TAXONOMY_VERSION
    && UUID_RE.test(String(profile.anon_id || ''))
    && UUID_RE.test(String(profile.session_id || ''))
    && !Object.prototype.hasOwnProperty.call(profile, 'negative_tags')
  );
}

export function popularProfileSignalCount(profile) {
  if (!isCompatiblePopularProfile(profile)) return 0;
  const explicitIds = new Set([
    ...values(profile.liked_event_ids),
    ...values(profile.not_interested_event_ids),
    ...values(profile.hidden_event_ids),
  ].map(String).filter(Boolean));
  const shares = Object.values(profile.share_counts || {})
    .reduce((sum, raw) => sum + Math.max(0, Number(raw || 0)), 0);
  return explicitIds.size + Math.min(3, shares);
}

function candidateAffinity(candidate, profile) {
  const positiveTags = weightedMap(profile, 'positive_tags');
  const positiveCategories = weightedMap(profile, 'positive_categories');
  const negativeTags = weightedMap(profile, 'negative_interest_tags');
  const tags = new Set([candidate.category, ...values(candidate.tags)].filter(Boolean));
  let affinity = Number(positiveCategories[candidate.category] || 0);
  let negative = 0;
  tags.forEach((tag) => {
    affinity += Number(positiveTags[tag] || 0);
    negative += Math.max(0, Number(negativeTags[tag] || 0));
  });
  return { affinity, net: affinity - negative * 1.25 };
}

/**
 * Warm-only 4+1 selection. It is intentionally evaluated once: subsequent
 * feedback must not reorder the row under the visitor's pointer.
 */
export function selectPopularPersonalizedCandidates(candidates, profile) {
  if (popularProfileSignalCount(profile) < 3) return [];
  const hiddenIds = new Set([
    ...values(profile.hidden_event_ids),
    ...values(profile.not_interested_event_ids),
  ].map(String));
  const ranked = candidates
    .filter((candidate) => candidate?.id != null && !hiddenIds.has(String(candidate.id)))
    .map((candidate) => ({ ...candidate, ...candidateAffinity(candidate, profile) }))
    .sort((left, right) => right.net - left.net || right.baseScore - left.baseScore || Number(left.id) - Number(right.id));
  const affinity = ranked.filter((candidate) => candidate.net > 0).slice(0, 4);
  if (affinity.length < 4) return [];

  const used = new Set(affinity.map((candidate) => String(candidate.id)));
  const affinityCategories = new Set(affinity.map((candidate) => candidate.category));
  const remaining = ranked.filter((candidate) => !used.has(String(candidate.id)));
  const explorationPool = remaining.filter((candidate) => !affinityCategories.has(candidate.category));
  const exploration = (explorationPool.length ? explorationPool : remaining)
    .sort((left, right) => left.net - right.net || right.baseScore - left.baseScore || Number(left.id) - Number(right.id))[0];
  return exploration ? [...affinity, exploration].map((candidate) => String(candidate.id)) : [];
}
