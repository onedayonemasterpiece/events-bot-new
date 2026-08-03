export const P13N_SURFACE_REGISTRY_VERSION = 'collection-surfaces-v1' as const;

export type PersonalizationRuntimeMode = 'off' | 'characterize' | 'local-shadow';

export type PersonalizationSurfaceIdV1 =
  | 'unknown'
  | 'static_only'
  | 'calendar_primary'
  | 'today_primary'
  | 'tomorrow_primary'
  | 'weekend_primary'
  | 'calendar_personal_tail'
  | 'thematic_collection'
  | 'popular_primary'
  | 'search_results'
  | 'event_detail_related'
  | 'for_me'
  | 'free_primary'
  | 'children_primary';

export type PersonalizationPolicyIdV1 =
  | 'unknown-static'
  | 'calendar-exact-only'
  | 'calendar-personal-tail'
  | 'thematic-weak'
  | 'popular-tiebreak'
  | 'search-query-first'
  | 'related-anchor-first'
  | 'for-me-strong'
  | 'free-weak'
  | 'children-weak';

export type PersonalizationActionV1 =
  | 'like_set'
  | 'like_unset'
  | 'hide_commit'
  | 'hide_restore'
  | 'save_set'
  | 'save_unset'
  | 'interest_profile_change'
  | 'personal_mode_enabled';

export type LegacyProfileV1Diagnostic =
  | 'legacy_profile_v1.empty'
  | 'legacy_profile_v1.valid'
  | 'legacy_profile_v1.malformed_json'
  | 'legacy_profile_v1.oversized_bytes'
  | 'legacy_profile_v1.invalid_shape'
  | 'legacy_profile_v1.missing_consent'
  | 'legacy_profile_v1.incompatible_profile_version'
  | 'legacy_profile_v1.incompatible_feature_schema'
  | 'legacy_profile_v1.incompatible_taxonomy'
  | 'legacy_profile_v1.invalid_uuid'
  | 'legacy_profile_v1.obsolete_negative_tags'
  | 'legacy_profile_v1.collection_cap_exceeded';

export interface LegacyProfileHorizonV1 {
  positive_tags?: Record<string, number>;
  positive_categories?: Record<string, number>;
  negative_interest_tags?: Record<string, number>;
  positive_time_tags?: Record<string, number>;
}

export interface LegacyProfileV1 {
  consent_ok: true;
  profile_version: 'anon-profile-v1';
  feature_schema_version: string;
  taxonomy_version: string;
  anon_id: string;
  session_id: string;
  positive_tags: Record<string, number>;
  positive_categories?: Record<string, number>;
  negative_interest_tags: Record<string, number>;
  positive_time_tags?: Record<string, number>;
  liked_event_ids: string[];
  not_interested_event_ids: string[];
  hidden_event_ids: string[];
  seen_event_ids: string[];
  seen_venue_ids: string[];
  price_preferences: { prefer_free: boolean };
  share_counts: Record<string, number>;
  session?: LegacyProfileHorizonV1;
  short?: LegacyProfileHorizonV1;
  mid?: LegacyProfileHorizonV1;
  long?: LegacyProfileHorizonV1;
  updated_at?: string;
}

export interface LegacyProfileParseResultV1 {
  profile: LegacyProfileV1 | null;
  diagnostic: LegacyProfileV1Diagnostic;
  byteSize: number;
}

export interface LegacyRankCandidateV1 {
  event_id?: string | number;
  id?: string | number;
  title?: string;
  category?: string;
  event_type?: string;
  tags?: string[];
  audience_tags?: string[];
  format_tags?: string[];
  time_tags?: string[];
  price_tags?: string[];
  reason_codes?: string[];
  venue_id?: string | number;
  location_name?: string;
  lifecycle_status?: string;
  status?: string;
  ticket_status?: string;
  is_free?: boolean;
  exploration_candidate?: boolean;
  static_score?: number;
  base_similarity?: number;
  display?: Record<string, unknown>;
}

export interface LegacyRankManifestV1 {
  feature_schema_version?: string;
  taxonomy_version?: string;
  event_id?: string | number;
  current_event?: { event_id?: string | number };
  related_static?: LegacyRankCandidateV1[];
  candidates?: LegacyRankCandidateV1[];
  events?: LegacyRankCandidateV1[];
}

export interface LegacyRankPlanItemV1 {
  event_id: string | number;
  candidate: LegacyRankCandidateV1;
  rank: number;
  personal_score: number;
  base_similarity: number;
  reason_codes: string[];
  diversity_postponed: boolean;
}

export interface LegacyRankPlanV1 {
  diagnostic: 'legacy_rank_plan_v1';
  items: LegacyRankPlanItemV1[];
}

export interface LegacyScoringWeightsV1 {
  staticContext: number;
  profileAffinity: number;
  priceMatch: number;
  timeMatch: number;
  explicitLike: number;
  exploration: number;
  negativeInterest: number;
  fatigue: number;
  soldOut: number;
}

export interface LegacyPersonalFeedWeightsV1 {
  affinity: number;
  popularity: number;
  price: number;
  time: number;
  staticScore: number;
  exploration: number;
  negativeInterest: number;
  fatigue: number;
}

export interface LegacyScoringConfigV1 {
  related: LegacyScoringWeightsV1;
  personalFeed: LegacyPersonalFeedWeightsV1;
  negativeHardFilterThreshold: number;
  maxSameCategory: number;
  maxSameVenue: number;
}

export interface SurfacePolicyV1 {
  id: PersonalizationPolicyIdV1;
  registryVersion: typeof P13N_SURFACE_REGISTRY_VERSION;
  rankingMode:
    | 'identity'
    | 'chronological'
    | 'profile'
    | 'baseline-plus-profile-tiebreak'
    | 'popularity-first-profile-tiebreak'
    | 'query-first-profile-tiebreak'
    | 'anchor-first-profile-tiebreak'
    | 'eligibility-first-profile-tiebreak';
  reorderScope: 'none' | 'invisible-tail' | 'unseen-results-only' | 'whole-unseen-list';
  exactHide: 'compatible-local-only' | 'global';
  signalCollection: 'none' | 'explicit-actions-only';
  networkOnPageView: false;
  fallback: string;
  source: 'personalization-to-be.md';
}

export interface TargetRankInputV1 {
  eventId: string;
  targetRank: number;
}

export interface TargetPresenterPlanV1 {
  policyId: PersonalizationPolicyIdV1;
  registryVersion: typeof P13N_SURFACE_REGISTRY_VERSION;
  currentOrder: string[];
  plannedOrder: string[];
  frozenIds: string[];
  applied: false;
  diagnosticCodes: string[];
}
