export type ArtistArrivalKind = 'russia' | 'international';

export interface ArtistArrivalItem {
  item_key: string;
  artist_name: string;
  arrival_kind: ArtistArrivalKind;
  project_title: string;
  role?: string | null;
  event_ids: number[];
  dates: string[];
  venues: string[];
  municipalities: string[];
  event_url?: string | null;
  photo_url?: string | null;
  photo_credit_text?: string | null;
  photo_source_url?: string | null;
  media_ready: boolean;
}

export interface ArtistArrivalsProjection {
  schema_version: 'kenigevents.artist_arrivals.v1';
  manifest_hash: string | null;
  generated_at: string | null;
  expires_at: string | null;
  eligible: boolean;
  shadow_eligible: boolean;
  publication_mode: 'shadow' | 'auto';
  social_threshold_met: boolean;
  items: ArtistArrivalItem[];
}

export function artistArrivalsProjection(value: unknown): ArtistArrivalsProjection {
  const raw = (value && typeof value === 'object' ? value : {}) as Partial<ArtistArrivalsProjection>;
  const items = Array.isArray(raw.items)
    ? raw.items.filter((item): item is ArtistArrivalItem => Boolean(
        item &&
        typeof item.item_key === 'string' &&
        typeof item.artist_name === 'string' &&
        (item.arrival_kind === 'russia' || item.arrival_kind === 'international') &&
        typeof item.project_title === 'string' &&
        Array.isArray(item.dates)
      ))
    : [];
  return {
    schema_version: 'kenigevents.artist_arrivals.v1',
    manifest_hash: typeof raw.manifest_hash === 'string' ? raw.manifest_hash : null,
    generated_at: typeof raw.generated_at === 'string' ? raw.generated_at : null,
    expires_at: typeof raw.expires_at === 'string' ? raw.expires_at : null,
    eligible: raw.eligible === true && items.length > 0,
    shadow_eligible: raw.shadow_eligible === true && items.length > 0,
    publication_mode: raw.publication_mode === 'auto' ? 'auto' : 'shadow',
    social_threshold_met: raw.social_threshold_met === true,
    items,
  };
}
