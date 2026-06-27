export type TicketKind = 'ticket' | 'registration' | 'free' | 'phone' | 'source' | 'status';

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
  telegraph_url: string | null;
  image_url: string | null;
  image_alt: string;
  image_text_mode: 'ocr_text' | 'visual_only' | 'unknown';
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
  generated_at: string;
  algorithm: string;
  related: Record<string, { similar: number[]; explore: number[] }>;
}
