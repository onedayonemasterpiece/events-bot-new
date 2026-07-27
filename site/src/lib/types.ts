export type TicketKind = 'ticket' | 'registration' | 'free' | 'phone' | 'source' | 'status';

export interface ImageBox {
  /** Normalized 0..1 left coordinate. */
  x: number;
  /** Normalized 0..1 top coordinate. */
  y: number;
  /** Normalized 0..1 width. */
  w: number;
  /** Normalized 0..1 height. */
  h: number;
  /** Model confidence when the producer supplies one (valuable regions do). */
  confidence?: number;
}

export interface ImageFocalPoint {
  /** Normalized 0..1 x coordinate recommended for object-position. */
  x: number;
  /** Normalized 0..1 y coordinate recommended for object-position. */
  y: number;
}

export type EventImageMediaRole =
  | 'event_identity_poster'
  | 'event_photo'
  | 'attendee_information'
  | 'program_or_schedule'
  | 'wayfinding'
  | 'sponsor_or_brand'
  | 'unknown_document'
  | 'unknown_visual';

export interface EventImageDerivative {
  src: string;
  width: number;
  height: number;
}

export interface EventImageAsset {
  src: string;
  width: number;
  height: number;
  alt: string;
  image_text_mode: 'ocr_text' | 'visual_only' | 'unknown';
  /** LLM-authored event-relative purpose. Missing/unknown values never unlock poster UI. */
  media_role?: EventImageMediaRole;
  media_role_confidence?: number;
  media_semantic_status?: 'pending' | 'classified' | 'error' | 'stale';
  image_kind?: 'poster' | 'photo' | 'mixed' | 'fallback';
  /** Content-addressed rail/card derivatives, smallest first. */
  thumbnail_sources?: EventImageDerivative[];
  asset_key?: string;
  ocr_boxes?: ImageBox[];
  face_boxes?: ImageBox[];
  /** Smallest coherent viewer-value region, in the same normalized space as face_boxes. */
  valuable_region?: ImageBox;
  saliency_boxes?: ImageBox[];
  focal_point?: ImageFocalPoint;
  recommended_object_position?: string;
  recommended_hero_fit?: 'contain' | 'cover';
  safe_crop?: boolean;
  /** Geometry is usable only when these two hashes are present and exactly equal. */
  current_pixel_sha256?: string;
  geometry_id?: number;
  geometry_pixel_sha256?: string;
  geometry_model?: string;
  geometry_prompt_version?: string;
  geometry_status?: 'classified';
  /** All x/y/w/h values are fractions of source pixels in [0, 1], origin top-left. */
  geometry_coordinate_space?: 'normalized_0_1';
  geometry_source_width?: number;
  geometry_source_height?: number;
  geometry_reason_code?: string;
  /** Build-time technical quality signal; it never replaces semantic LLM media classification. */
  quality_score?: number;
  /** Exact source-review evidence used by listing/card presentation overrides. */
  listing_crop_evidence?: string;
  /** Explicit source review that this individual asset contains no OCR text. */
  listing_no_ocr_review?: boolean;
  /** Preserve authored geometry for a reviewed listing asset. */
  listing_use_natural?: boolean;
}

export interface TicketInfo {
  kind: TicketKind;
  label: string;
  href: string | null;
  note?: string;
  status: string | null;
  is_free: boolean;
  price_label: string | null;
}

export type AgeRestriction = '0+' | '6+' | '12+' | '16+' | '18+';
export type AgeRestrictionStatus =
  | 'declared'
  | 'assessed'
  | 'conflict'
  | 'insufficient_evidence'
  | 'unknown'
  | 'budget_deferred';

export interface PreviewEvent {
  id: number;
  title: string;
  slug: string;
  event_type: string | null;
  festival: string | null;
  organizer_names?: string[];
  status_label: string;
  lifecycle_status: string;
  starts_at: string | null;
  start_date: string;
  start_time: string | null;
  end_date: string | null;
  end_at: string | null;
  time_range_end: string | null;
  duration_forecast_minutes?: number | null;
  transport_end_basis?: 'source_duration' | 'forecast';
  timezone: string;
  display_date: string;
  display_time: string | null;
  city: string | null;
  venue_name: string | null;
  address: string | null;
  map_query: string | null;
  ticket: TicketInfo;
  age_restriction: AgeRestriction | null;
  age_restriction_status: AgeRestrictionStatus;
  age_restriction_provenance: string | null;
  age_restriction_decision_version: string | null;
  age_recommendation: AgeRestriction | null;
  age_recommendation_label: string | null;
  source_url: string | null;
  source_urls?: string[];
  source_count?: number;
  telegraph_url: string | null;
  image_url: string | null;
  image_alt: string;
  image_text_mode: 'ocr_text' | 'visual_only' | 'unknown';
  image_media_role?: EventImageMediaRole;
  /** Future multi-image/face-aware export contract; first item should match image_url when present. */
  image_assets?: EventImageAsset[];
  face_boxes?: ImageBox[];
  valuable_region?: ImageBox | null;
  ocr_boxes?: ImageBox[];
  focal_point?: ImageFocalPoint;
  image_object_position?: string | null;
  safe_crop?: boolean;
  summary: string;
  meta_description: string;
  description_html: string;
  topics: string[];
  /** Total visible likes: source_likes_count + service_likes_count. */
  likes_count: number;
  /** Aggregated public reactions from source posts (TG/VK post metrics when available). */
  source_likes_count: number;
  /** First-party likes already persisted by KenigEvents. Static preview keeps it 0 until backend ingest exists. */
  service_likes_count: number;
  source_views_count?: number;
  source_engagement_sources_count?: number;
  shares_count?: number;
  /** Explainable batch-derived reasons; absent/empty means legacy aggregate fallback. */
  popularity_reason_codes?: Array<'fast_growth' | 'frequently_shared' | 'discussed' | 'multi_source'>;
  popularity_signal_score?: number;
  pushkin_card: boolean;
  other_date_ids: number[];
  source_prod_id: number;
  data_quality_notes: string[];
  updated_at: string | null;
}

export interface EventFeatureSummary {
  event_id: number;
  title: string;
  category: string;
  tags: string[];
  audience_exclusion_tags: string[];
  city: string | null;
  location_name: string | null;
  date: string;
  age_restriction: AgeRestriction | null;
  age_restriction_status: AgeRestrictionStatus;
}

export interface DiscoveryDisplayPayload {
  href: string;
  absolute_url: string;
  event_type: string | null;
  image_url: string | null;
  image_alt: string;
  image_text_mode: PreviewEvent['image_text_mode'];
  image_media_role?: EventImageMediaRole;
  image_width?: number | null;
  image_height?: number | null;
  focal_y?: number | null;
  display_date: string;
  display_time: string | null;
  display_date_time: string;
  occurrence_aria_label: string;
  occurrence_member_ids: number[];
  city: string | null;
  venue_name: string | null;
  place: string;
  status_label: string;
  price_label: string | null;
  likes_count: number;
  shares_count: number;
  calendar_href: string;
  calendar_eligible: boolean;
  age_restriction: AgeRestriction | null;
  age_restriction_status: AgeRestrictionStatus;
  age_recommendation: AgeRestriction | null;
  age_recommendation_label: string | null;
}

export interface RelatedManifestCandidate extends EventFeatureSummary {
  status: string;
  lifecycle_status: string;
  is_free: boolean;
  base_similarity: number;
  static_score: number;
  reason_codes: string[];
  exploration_candidate: boolean;
  slot_type?: 'pure_related' | 'adjacent_discovery' | 'promo';
  lexical_similarity?: number;
  vector_similarity?: number;
  deterministic_score?: number;
  llm_semantic_score?: number;
  llm_confidence?: number;
  related_score?: number;
  similarity_class?: string;
  retrieval_sources?: string[];
  display: DiscoveryDisplayPayload;
}

export interface EventDetailRelatedManifest {
  version: number;
  schema_version: 'event-detail-related-v1';
  feature_schema_version: 'event-detail-related-v1';
  taxonomy_version: 'event-taxonomy-v1';
  surface: 'event_detail_related';
  algorithm_id: 'static_related_v1' | 'event_sparse_related_chain_v1' | 'event_vector_related_chain_v2' | 'event_pgvector_related_chain_v1' | 'event_pgvector_related_chain_v2_two_doc';
  generated_at: string;
  event_id: number;
  strategy: 'static_related_manifest_v1' | 'event_sparse_related_chain_v1_manifest' | 'event_related_chain_v2_manifest' | 'event_pgvector_related_chain_v1_manifest' | 'event_pgvector_related_chain_v2_manifest';
  preload_target: number;
  page_size: number;
  current_event: EventFeatureSummary;
  related_static: RelatedManifestCandidate[];
}

export interface PreviewData {
  build: {
    generated_at: string;
    source: string;
    current_date: string;
    notes: string[];
  };
  events: PreviewEvent[];
}

export interface RelatedData {
  schema_version?: string;
  generated_at: string;
  algorithm: string;
  retrieval_method?: string;
  semantic_embeddings?: boolean;
  embedding_model?: string;
  gemma_verification?: unknown;
  strict_verified_related?: boolean;
  cache?: unknown;
  related: Record<string, { similar: number[]; pure_related?: number[]; explore: number[]; adjacent_discovery?: number[]; chain?: Array<Record<string, unknown>>; underfilled?: boolean; strict_verified?: boolean }>;
}

export interface InterestClubMeeting {
  event_id: number;
  title: string;
  start_date: string;
  start_time: string | null;
  display_time: string | null;
  city: string | null;
  venue_name: string | null;
  event_path: string | null;
  source_url: string | null;
}

export interface InterestClubActivity {
  meeting_count: number;
  distinct_date_count: number;
  first_observed_date: string;
  last_observed_date: string;
  future_meeting_count: number;
}

export interface InterestClub {
  id: number;
  slug: string;
  name: string;
  topic: string;
  description: string | null;
  city: string | null;
  typical_venue: string | null;
  activity: InterestClubActivity;
  future_meetings: InterestClubMeeting[];
  updated_at: string | null;
}

export interface InterestClubsData {
  schema_version: 'interest-clubs-static-v1';
  projection_version: 1;
  generated_at: string;
  current_date: string;
  source: string;
  clubs: InterestClub[];
}

export type UnusualQualityGateStatus = 'approved' | 'shadow' | 'migration' | 'failed' | 'unavailable';

export interface UnusualManifestQualityGate {
  status: UnusualQualityGateStatus | string;
  metrics: Record<string, number | string | boolean | null>;
  rollout_baseline_at?: string | null;
}

export interface UnusualManifestItem {
  event_id: number;
  concept_id: string;
  representative_event_id: number;
  tier: string;
  unusual_score: number;
  confidence: number;
  families: string[];
  reason_codes: string[];
  prototype_evidence: unknown[];
  first_published_at: string | null;
  notify_eligible: boolean;
  content_hash: string;
  date: string;
  lifecycle: string;
  path?: string | null;
  event_snapshot?: PreviewEvent | null;
}

export interface UnusualEventsManifest {
  schema_version: string;
  build_id: string;
  generated_at: string;
  source_snapshot_id: string;
  hash: string;
  taxonomy_version: string;
  policy_version: string;
  embedding_model: string;
  revision: string;
  dim: number;
  doc_kind: string;
  document_version: string;
  prototype_bank_hash: string;
  classifier_hash: string;
  rollout_baseline_at?: string | null;
  notification_baseline_at?: string | null;
  rollout_baseline?: string | null;
  quality_gate: UnusualManifestQualityGate;
  items: UnusualManifestItem[];
}
