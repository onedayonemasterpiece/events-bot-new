const PROFILE_VERSION = 'anon-profile-v1';
const FEATURE_SCHEMA_VERSION = 'event-detail-related-v1';
const TAXONOMY_VERSION = 'event-taxonomy-v1';

function values(value) {
  return Array.isArray(value) ? value : [];
}

function id(value) {
  const normalized = String(value ?? '').trim();
  return normalized ? normalized : null;
}

function numericMap(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return Object.fromEntries(Object.entries(value).map(([key, score]) => [String(key), Number(score) || 0]));
}

export function compatibleHomeProfile(profile) {
  return Boolean(
    profile
    && profile.consent_ok === true
    && profile.profile_version === PROFILE_VERSION
    && profile.feature_schema_version === FEATURE_SCHEMA_VERSION
    && profile.taxonomy_version === TAXONOMY_VERSION
    && !Object.prototype.hasOwnProperty.call(profile, 'negative_tags'),
  );
}

export function homeProfileSignalCount(profile) {
  if (!compatibleHomeProfile(profile)) return 0;
  const shares = Object.values(numericMap(profile.share_counts)).reduce((sum, value) => sum + Math.max(0, value), 0);
  return (
    values(profile.liked_event_ids).length
    + values(profile.not_interested_event_ids).length
    + values(profile.hidden_event_ids).length
    + Math.min(8, shares)
  );
}

export function rankHomeFeedItems(items, profile) {
  const source = Array.isArray(items) ? items : [];
  if (!compatibleHomeProfile(profile)) return source.map((item, index) => ({ ...item, rank: index, score: -index }));
  const liked = new Set(values(profile.liked_event_ids).map(id).filter(Boolean));
  const hidden = new Set([
    ...values(profile.not_interested_event_ids),
    ...values(profile.hidden_event_ids),
  ].map(id).filter(Boolean));
  const positive = numericMap(profile.positive_tags);
  const negative = numericMap(profile.negative_interest_tags);
  return source
    .filter((item) => !hidden.has(id(item.id)))
    .map((item, index) => {
      const tags = new Set([
        String(item.category || ''),
        ...values(item.tags).map(String),
      ].filter(Boolean));
      const affinity = [...tags].reduce((sum, tag) => sum + (positive[tag] || 0) - 1.35 * (negative[tag] || 0), 0);
      const likedBoost = liked.has(id(item.id)) ? 8 : 0;
      const score = likedBoost + affinity - Number(item.baseRank ?? index) * 0.018;
      return { ...item, originalIndex: index, score };
    })
    .sort((left, right) => right.score - left.score || left.originalIndex - right.originalIndex || Number(left.id) - Number(right.id))
    .map((item, rank) => ({ ...item, rank }));
}
