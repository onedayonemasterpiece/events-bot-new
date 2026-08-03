import type {
  LegacyProfileHorizonV1,
  LegacyProfileV1,
  LegacyRankCandidateV1,
  LegacyRankManifestV1,
  LegacyRankPlanItemV1,
  LegacyRankPlanV1,
  LegacyScoringConfigV1,
} from '../contract.ts';

// Characterization baseline copied from the inline prototype. These numbers are
// quarantined migration evidence, not target defaults or a quality winner.
export const LEGACY_SCORING_CONFIG_V1: Readonly<LegacyScoringConfigV1> = Object.freeze({
  related: Object.freeze({
    staticContext: 0.80,
    profileAffinity: 0.10,
    priceMatch: 0.04,
    timeMatch: 0.03,
    explicitLike: 0.60,
    exploration: 0.02,
    negativeInterest: 0.55,
    fatigue: 0.18,
    soldOut: 0.20,
  }),
  personalFeed: Object.freeze({
    affinity: 0.60,
    popularity: 0.16,
    price: 0.09,
    time: 0.06,
    staticScore: 0.05,
    exploration: 0.04,
    negativeInterest: 0.62,
    fatigue: 0.16,
  }),
  negativeHardFilterThreshold: 0.95,
  maxSameCategory: 3,
  maxSameVenue: 2,
});

function asArray<T>(value: T[] | undefined): T[] {
  return Array.isArray(value) ? value : [];
}

function asId(value: unknown): string | null {
  return value == null ? null : String(value);
}

function unique(values: unknown[]): string[] {
  return Array.from(new Set(values.filter((value) => value != null).map(String).filter(Boolean)));
}

function mapScore(map: Record<string, number> | undefined, key: unknown): number {
  if (!map || key == null) return 0;
  const numeric = Number(map[String(key)] || 0);
  return Number.isFinite(numeric) ? numeric : 0;
}

function weightedMap(profile: LegacyProfileV1, field: keyof LegacyProfileHorizonV1): Record<string, number> {
  const top = profile[field];
  const result: Record<string, number> = top && typeof top === 'object' ? { ...top } : {};
  const horizons: Array<[LegacyProfileHorizonV1 | undefined, number]> = [
    [profile.session, 1.0],
    [profile.short, 0.65],
    [profile.mid, 0.35],
    [profile.long, 0.20],
  ];
  for (const [horizon, weight] of horizons) {
    const map = horizon?.[field];
    if (!map) continue;
    for (const [key, value] of Object.entries(map)) {
      result[key] = mapScore(result, key) + Number(value || 0) * weight;
    }
  }
  return result;
}

export function legacyCandidateIdV1(candidate: LegacyRankCandidateV1): string | number | undefined {
  return candidate.event_id != null ? candidate.event_id : candidate.id;
}

function currentEventId(manifest: LegacyRankManifestV1): string | number | undefined {
  return manifest.current_event?.event_id != null ? manifest.current_event.event_id : manifest.event_id;
}

function displayOf(candidate: LegacyRankCandidateV1): Record<string, unknown> {
  return candidate.display || candidate as unknown as Record<string, unknown>;
}

function matchingTags(candidate: LegacyRankCandidateV1): string[] {
  const display = displayOf(candidate);
  const values: unknown[] = [];
  for (const key of ['category', 'event_type'] as const) {
    if (candidate[key]) values.push(candidate[key]);
    if (display[key]) values.push(display[key]);
  }
  for (const field of ['tags', 'audience_tags', 'format_tags', 'time_tags', 'price_tags'] as const) {
    values.push(...asArray(candidate[field]));
  }
  for (const reason of asArray(candidate.reason_codes)) {
    if (reason.startsWith('tag:')) values.push(reason.slice(4));
    if (reason.startsWith('same_category:')) values.push(reason.slice('same_category:'.length));
  }
  return unique(values);
}

function hiddenSet(profile: LegacyProfileV1 | null): Set<string | null> {
  return new Set([
    ...asArray(profile?.hidden_event_ids).map(asId),
    ...asArray(profile?.not_interested_event_ids).map(asId),
  ]);
}

export function legacyStaticCandidateScoreV1(candidate: LegacyRankCandidateV1): number {
  const raw = candidate.static_score != null ? candidate.static_score : candidate.base_similarity;
  const score = Number(raw || 0);
  return Number.isFinite(score) ? Math.max(0, Math.min(1, score)) : 0;
}

export function legacyTagAffinityV1(candidate: LegacyRankCandidateV1, profile: LegacyProfileV1): number {
  const positive = weightedMap(profile, 'positive_tags');
  const categories = weightedMap(profile, 'positive_categories');
  let sum = 0;
  for (const tag of matchingTags(candidate)) sum += mapScore(positive, tag);
  if (candidate.category) sum += mapScore(categories, candidate.category);
  return Math.min(1.5, sum / 2);
}

export function legacyNegativeInterestPenaltyV1(candidate: LegacyRankCandidateV1, profile: LegacyProfileV1): number {
  const negative = weightedMap(profile, 'negative_interest_tags');
  let penalty = 0;
  for (const tag of matchingTags(candidate)) penalty += Math.max(0, mapScore(negative, tag));
  return Math.min(1.5, penalty);
}

export function legacyPriceMatchV1(candidate: LegacyRankCandidateV1, profile: LegacyProfileV1): number {
  return profile.price_preferences.prefer_free && candidate.is_free ? 1 : 0;
}

export function legacyTimeMatchV1(candidate: LegacyRankCandidateV1, profile: LegacyProfileV1): number {
  const preferences = weightedMap(profile, 'positive_time_tags');
  let score = 0;
  for (const tag of asArray(candidate.time_tags)) score += mapScore(preferences, tag);
  for (const tag of matchingTags(candidate)) {
    if (tag === 'evening' || tag === 'daytime') score += mapScore(preferences, tag);
  }
  return Math.min(1, score);
}

export function legacyFatiguePenaltyV1(candidate: LegacyRankCandidateV1, profile: LegacyProfileV1): number {
  const id = asId(legacyCandidateIdV1(candidate));
  const venue = asId(candidate.venue_id ?? candidate.location_name);
  let penalty = 0;
  if (new Set(asArray(profile.seen_event_ids).map(asId)).has(id)) penalty += 0.7;
  if (venue && new Set(asArray(profile.seen_venue_ids).map(asId)).has(venue)) penalty += 0.35;
  return Math.min(1, penalty);
}

function cancelledLike(candidate: LegacyRankCandidateV1): boolean {
  const lifecycle = String(candidate.lifecycle_status || '').toLowerCase();
  const status = String(candidate.status || candidate.ticket_status || '').toLowerCase();
  return ['cancelled', 'postponed', 'duplicate', 'merged'].includes(lifecycle)
    || ['cancelled', 'postponed'].includes(status);
}

export function legacyIsEligibleCandidateV1(
  candidate: LegacyRankCandidateV1,
  manifest: LegacyRankManifestV1,
  profile: LegacyProfileV1 | null,
  config: LegacyScoringConfigV1,
): boolean {
  const id = legacyCandidateIdV1(candidate);
  if (id == null || asId(id) === asId(currentEventId(manifest)) || cancelledLike(candidate)) return false;
  if (!profile) return true;
  if (hiddenSet(profile).has(asId(id))) return false;
  return legacyNegativeInterestPenaltyV1(candidate, profile) < config.negativeHardFilterThreshold;
}

function scoreRelated(
  candidate: LegacyRankCandidateV1,
  profile: LegacyProfileV1 | null,
  config: LegacyScoringConfigV1,
): { score: number; base: number; reasons: string[] } {
  const base = legacyStaticCandidateScoreV1(candidate);
  if (!profile) return { score: base, base, reasons: asArray(candidate.reason_codes).slice() };
  const affinity = legacyTagAffinityV1(candidate, profile);
  const negative = legacyNegativeInterestPenaltyV1(candidate, profile);
  const fatigue = legacyFatiguePenaltyV1(candidate, profile);
  const price = legacyPriceMatchV1(candidate, profile);
  const time = legacyTimeMatchV1(candidate, profile);
  const soldOut = String(candidate.status || candidate.ticket_status || '').toLowerCase() === 'sold_out' ? 1 : 0;
  const exploration = candidate.exploration_candidate ? 1 : 0;
  const explicitLike = new Set(asArray(profile.liked_event_ids).map(asId)).has(asId(legacyCandidateIdV1(candidate))) ? 1 : 0;
  const weights = config.related;
  const score = weights.staticContext * base
    + weights.profileAffinity * affinity
    + weights.priceMatch * price
    + weights.timeMatch * time
    + weights.explicitLike * explicitLike
    + weights.exploration * exploration
    - weights.negativeInterest * negative
    - weights.fatigue * fatigue
    - weights.soldOut * soldOut;
  const reasons = asArray(candidate.reason_codes).slice();
  if (affinity > 0) reasons.push('profile:positive_affinity');
  if (explicitLike > 0) reasons.push('profile:explicit_like');
  if (negative > 0) reasons.push('profile:negative_interest_penalty');
  if (fatigue > 0) reasons.push('profile:fatigue_penalty');
  if (price > 0) reasons.push('profile:price_match');
  return { score, base, reasons: unique(reasons) };
}

function stableSort(scored: Array<Omit<LegacyRankPlanItemV1, 'rank' | 'personal_score' | 'base_similarity' | 'diversity_postponed'> & { score: number; base: number }>) {
  return scored.sort((left, right) => right.score - left.score
    || right.base - left.base
    || Number(legacyCandidateIdV1(left.candidate) || 0) - Number(legacyCandidateIdV1(right.candidate) || 0));
}

function applyLegacyDiversityV1(
  sorted: ReturnType<typeof stableSort>,
  config: LegacyScoringConfigV1,
): Array<(typeof sorted)[number] & { diversity_postponed?: boolean }> {
  const result: Array<(typeof sorted)[number] & { diversity_postponed?: boolean }> = [];
  const postponed: Array<(typeof sorted)[number] & { diversity_postponed?: boolean }> = [];
  const categories = new Map<string, number>();
  const venues = new Map<string, number>();
  for (const item of sorted) {
    const category = item.candidate.category || item.candidate.event_type || 'unknown';
    const venue = String(item.candidate.venue_id || item.candidate.location_name || 'unknown');
    const categoryCount = categories.get(category) || 0;
    const venueCount = venues.get(venue) || 0;
    if (categoryCount >= config.maxSameCategory || venueCount >= config.maxSameVenue) {
      postponed.push({ ...item, diversity_postponed: true });
      continue;
    }
    result.push(item);
    categories.set(category, categoryCount + 1);
    venues.set(venue, venueCount + 1);
  }
  return result.concat(postponed);
}

function toPlan(items: ReturnType<typeof applyLegacyDiversityV1>): LegacyRankPlanV1 {
  return {
    diagnostic: 'legacy_rank_plan_v1',
    items: items.map((item, rank) => ({
      event_id: legacyCandidateIdV1(item.candidate) as string | number,
      candidate: item.candidate,
      rank,
      personal_score: Number(item.score.toFixed(4)),
      base_similarity: Number(item.base.toFixed(4)),
      reason_codes: item.reason_codes,
      diversity_postponed: Boolean(item.diversity_postponed),
    })),
  };
}

export function legacyRankEventDetailRelatedV1(
  manifest: LegacyRankManifestV1,
  profile: LegacyProfileV1 | null,
  config: LegacyScoringConfigV1,
): LegacyRankPlanV1 {
  const active = profile
    && (!manifest.feature_schema_version || profile.feature_schema_version === manifest.feature_schema_version)
    && (!manifest.taxonomy_version || profile.taxonomy_version === manifest.taxonomy_version)
    ? profile
    : null;
  const candidates = manifest.related_static || manifest.candidates || manifest.events || [];
  const scored = candidates.flatMap((candidate) => {
    if (!legacyIsEligibleCandidateV1(candidate, manifest, active, config)) return [];
    const item = scoreRelated(candidate, active, config);
    return [{ candidate, event_id: legacyCandidateIdV1(candidate) as string | number, score: item.score, base: item.base, reason_codes: item.reasons }];
  });
  return toPlan(applyLegacyDiversityV1(stableSort(scored), config));
}

function popularity(candidate: LegacyRankCandidateV1): number {
  const display = displayOf(candidate);
  const likes = Math.max(0, Number(display.likes_count || 0));
  const shares = Math.max(0, Number(display.shares_count || 0));
  return Math.min(1, Math.log1p(likes + shares * 1.5) / Math.log(80));
}

export function legacyPersonalFeedSignalCountV1(profile: LegacyProfileV1): number {
  const explicit = new Set([
    ...profile.liked_event_ids.map(asId),
    ...profile.not_interested_event_ids.map(asId),
    ...profile.hidden_event_ids.map(asId),
  ].filter(Boolean));
  const shares = Object.values(profile.share_counts).reduce((sum, value) => sum + Math.max(0, Number(value || 0)), 0);
  return explicit.size + Math.min(3, shares);
}

export function legacyRankPersonalFeedV1(
  manifest: LegacyRankManifestV1,
  profile: LegacyProfileV1,
  config: LegacyScoringConfigV1,
): LegacyRankPlanV1 {
  if (legacyPersonalFeedSignalCountV1(profile) < 3) return { diagnostic: 'legacy_rank_plan_v1', items: [] };
  const weights = config.personalFeed;
  const scored = asArray(manifest.related_static).flatMap((candidate) => {
    if (!legacyIsEligibleCandidateV1(candidate, manifest, profile, config)) return [];
    const affinity = legacyTagAffinityV1(candidate, profile);
    const negative = legacyNegativeInterestPenaltyV1(candidate, profile);
    const fatigue = legacyFatiguePenaltyV1(candidate, profile);
    const price = legacyPriceMatchV1(candidate, profile);
    const time = legacyTimeMatchV1(candidate, profile);
    const popularityScore = popularity(candidate);
    const base = legacyStaticCandidateScoreV1(candidate);
    const score = weights.affinity * affinity
      + weights.popularity * popularityScore
      + weights.price * price
      + weights.time * time
      + weights.staticScore * base
      + weights.exploration * (candidate.exploration_candidate ? 1 : 0)
      - weights.negativeInterest * negative
      - weights.fatigue * fatigue;
    const reasons = asArray(candidate.reason_codes).slice();
    if (affinity > 0) reasons.push('profile:positive_affinity');
    if (popularityScore > 0) reasons.push('catalog:popularity');
    if (price > 0) reasons.push('profile:price_match');
    return [{ candidate, event_id: legacyCandidateIdV1(candidate) as string | number, score, base, reason_codes: unique(reasons) }];
  });
  return toPlan(applyLegacyDiversityV1(stableSort(scored), config));
}
