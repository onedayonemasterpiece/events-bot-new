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
  confidence: number;
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
  saliency_boxes?: ImageBox[];
  focal_point?: ImageFocalPoint;
  recommended_object_position?: string;
  recommended_hero_fit?: 'contain' | 'cover';
  safe_crop?: boolean;
  /** Build-time technical quality signal; it never replaces semantic LLM media classification. */
  quality_score?: number;
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

export interface PreviewEvent {
  id: number;
  title: string;
  slug: string;
  event_type: string | null;
  festival: string | null;
  status_label: string;
  lifecycle_status: string;
  starts_at: string | null;
  start_date: string;
  start_time: string | null;
  end_date: string | null;
  end_at: string | null;
  time_range_end: string | null;
  timezone: string;
  display_date: string;
  display_time: string | null;
  city: string | null;
  venue_name: string | null;
  address: string | null;
  map_query: string | null;
  ticket: TicketInfo;
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
}

export interface DiscoveryDisplayPayload {
  href: string;
  absolute_url: string;
  event_type: string | null;
  image_url: string | null;
  image_alt: string;
  image_text_mode: PreviewEvent['image_text_mode'];
  image_media_role?: EventImageMediaRole;
  focal_y?: number | null;
  display_date: string;
  display_time: string | null;
  display_date_time: string;
  city: string | null;
  venue_name: string | null;
  place: string;
  status_label: string;
  price_label: string | null;
  likes_count: number;
  shares_count: number;
  calendar_href: string;
  calendar_eligible: boolean;
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
