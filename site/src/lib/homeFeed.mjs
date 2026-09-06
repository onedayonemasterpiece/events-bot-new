import { parseLegacyProfileV1 } from './personalization/legacy/profile-v1.ts';
import {
  LEGACY_SCORING_CONFIG_V1,
  legacyPersonalFeedSignalCountV1,
  legacyRankEventDetailRelatedV1,
  legacyRankPersonalFeedV1,
} from './personalization/legacy/scorer-v1.ts';

export const HOME_FEED_LIMIT = 30;
export const HOME_FEED_COMPATIBILITY = Object.freeze({
  featureSchemaVersion: 'event-detail-related-v1', taxonomyVersion: 'event-taxonomy-v1',
});
export const homeCandidateId = (candidate) => String(candidate.event_id ?? candidate.id);
export const homeCandidateFamily = (candidate) => [...new Set([
  homeCandidateId(candidate), ...(candidate.display?.occurrence_member_ids || []).map(String),
])];

export function parseHomeProfile(serialized) {
  return parseLegacyProfileV1(serialized, HOME_FEED_COMPATIBILITY).profile;
}
export function compatibleHomeProfile(profile) {
  try { return Boolean(parseHomeProfile(JSON.stringify(profile))); } catch { return false; }
}
export function homeProfileSignalCount(profile) {
  return compatibleHomeProfile(profile) ? legacyPersonalFeedSignalCountV1(profile) : 0;
}
export function homeHiddenIds(profile) {
  return new Set(compatibleHomeProfile(profile)
    ? [...profile.hidden_event_ids, ...profile.not_interested_event_ids].map(String) : []);
}

/** The shared legacy scorer is an applied compatibility path, not the shadow target presenter. */
export function rankHomeCandidates(candidates, profile = null) {
  let active = null;
  try { active = parseHomeProfile(JSON.stringify(profile)); } catch {}
  const hidden = homeHiddenIds(active);
  const seen = new Set();
  const eligible = candidates.filter((candidate) => {
    const family = homeCandidateFamily(candidate);
    if (family.some((id) => seen.has(id))) return false;
    family.forEach((id) => seen.add(id));
    return !family.some((id) => hidden.has(id));
  });
  const manifest = { feature_schema_version: HOME_FEED_COMPATIBILITY.featureSchemaVersion,
    taxonomy_version: HOME_FEED_COMPATIBILITY.taxonomyVersion, related_static: eligible };
  const personalized = Boolean(active && legacyPersonalFeedSignalCountV1(active) >= 3);
  const plan = personalized
    ? legacyRankPersonalFeedV1(manifest, active, LEGACY_SCORING_CONFIG_V1)
    : legacyRankEventDetailRelatedV1(manifest, null, LEGACY_SCORING_CONFIG_V1);
  return { items: plan.items, personalized };
}

/** Old metadata adapter retained for non-runtime consumers; no separate scoring formula. */
export function rankHomeFeedItems(items, profile) {
  const byId = new Map(items.map((item) => [String(item.id), item]));
  return rankHomeCandidates(items.map((item, index) => ({
    ...item, event_id: item.id, static_score: Math.max(0, 1 - index / Math.max(1, items.length)),
  })), profile).items.map((item) => ({ ...byId.get(String(item.event_id)), rank:item.rank, score:item.personal_score }));
}

/** Preserve the observed prefix, including hidden slots so Undo returns to the exact place.
 * Re-rank only the unseen suffix; hidden slots do not consume the 30 visible-card budget.
 */
export function reconcileHomeOrder({ previous = [], locked = 0, ranked = [], candidates = [], hidden = new Set(), limit = HOME_FEED_LIMIT }) {
  const byId = new Map(candidates.map((candidate) => [homeCandidateId(candidate), candidate]));
  const isHidden = (id) => homeCandidateFamily(byId.get(id)).some((member) => hidden.has(member));
  const prefix = [...new Set(previous.slice(0, locked).map(String))].filter((id) => byId.has(id));
  const order = [...prefix];
  const overflow = new Set();
  let visible = 0;
  for (const id of prefix) {
    if (isHidden(id)) continue;
    if (visible >= limit) overflow.add(id);
    else visible += 1;
  }
  for (const item of ranked) {
    const id = String(item.event_id);
    if (!byId.has(id) || order.includes(id) || isHidden(id) || visible >= limit) continue;
    order.push(id); visible += 1;
  }
  return { order, visible, hidden:order.filter((id) => isHidden(id) || overflow.has(id)), locked:prefix.length };
}
