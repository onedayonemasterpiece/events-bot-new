import organizerMedallions from '../data/organizerMedallions.json';
import festivalMedallions from '../data/festivalMedallions.json';
import { matchMedallionAlias } from './eventMedallions';
import type { EventImageAsset, PreviewEvent } from './types';

export interface ListingMedallionEvidence {
  field: 'venue_name' | 'festival' | 'event_id';
  value: string;
  match: 'exact' | 'bounded' | 'curated_event';
}

export interface ListingVenueMedallion {
  slug: string;
  name: string;
  shortName: string;
  avatarUrl: string;
  fallbackPngUrl?: string;
  background?: string;
  ring?: string;
  ariaLabel: string;
  evidence: ListingMedallionEvidence;
}

interface ManifestMedallion {
  slug: string;
  name: string;
  shortName?: string;
  aliases?: string[];
  avatarUrl: string;
  fallbackPngUrl?: string;
  background?: string;
  ring?: string;
  ariaLabel?: string;
  listingStatus?: 'listing_ready' | 'detail_only' | 'blocked_missing_binding';
  listingBinding?: 'venue' | 'festival' | 'organizer';
  listingEventIds?: number[];
}

function namesFor(item: ManifestMedallion): string[] {
  return Array.from(new Set([item.name, item.shortName, ...(item.aliases || [])]
    .filter(Boolean)
    .map(String)));
}

/** Exact or bounded phrase match; deliberately no fuzzy prose matching. */
function structuredEvidence(
  item: ManifestMedallion,
  field: 'venue_name' | 'festival',
  value: unknown,
): ListingMedallionEvidence | null {
  for (const candidate of namesFor(item)) {
    const match = matchMedallionAlias(value, candidate);
    if (match) return { field, value:String(value), match };
  }
  return null;
}

const allItems: ManifestMedallion[] = [
  ...((organizerMedallions as { items?: ManifestMedallion[] }).items || []),
  ...((festivalMedallions as { items?: ManifestMedallion[] }).items || []),
];

interface ListingCandidate {
  item: ManifestMedallion;
  evidence: ListingMedallionEvidence;
}

function listingCandidates(event: PreviewEvent): ListingCandidate[] {
  const ready = allItems.filter((item) => item.listingStatus === 'listing_ready');
  const matched: ListingCandidate[] = [];
  for (const item of ready) {
    const evidence = item.listingBinding === 'venue'
      ? structuredEvidence(item, 'venue_name', event.venue_name)
      : item.listingBinding === 'festival'
        ? structuredEvidence(item, 'festival', event.festival)
        : item.listingBinding === 'organizer' && item.listingEventIds?.includes(event.id)
          ? { field:'event_id' as const, value:String(event.id), match:'curated_event' as const }
          : null;
    if (evidence) matched.push({ item, evidence });
  }
  return matched.filter(({ item }, index) => (
    matched.findIndex((candidate) => candidate.item.slug === item.slug) === index
  ));
}

function toListingMedallion({ item, evidence }: ListingCandidate): ListingVenueMedallion {
  return {
    slug: item.slug,
    name: item.name,
    shortName: item.shortName || item.name,
    avatarUrl: item.avatarUrl,
    fallbackPngUrl: item.fallbackPngUrl,
    background: item.background,
    ring: item.ring,
    ariaLabel: item.ariaLabel || `Локация: ${item.name}`,
    evidence,
  };
}

/**
 * Structured identity rail candidates. Unlike an on-image overlay this list
 * is safe for OCR because the rail is rendered outside the media frame.
 */
export function getListingIdentityMedallions(event: PreviewEvent): ListingVenueMedallion[] {
  return listingCandidates(event).slice(0, 3).map(toListingMedallion);
}

/**
 * Listing medallions are bound only through structured event fields and only
 * to the image that actually won presentation selection. Visual-only media is
 * the hard OCR safety gate; at most one medallion is returned.
 */
export function getListingVenueMedallion(event: PreviewEvent, selectedAsset?: EventImageAsset | null): ListingVenueMedallion | null {
  // The selected listing presentation is the only media that may grant an
  // overlay. Falling back to a rejected raw first asset made the rule appear
  // random and could cover OCR/unknown posters that were not actually shown.
  const primaryAsset = selectedAsset ?? null;
  const isNeutralFallback = !event.image_url && !primaryAsset;
  const ratio = primaryAsset?.width && primaryAsset?.height
    ? primaryAsset.width / primaryAsset.height
    : 0;
  const isSafeWidePhoto = Boolean(
    primaryAsset
    && primaryAsset.image_text_mode === 'visual_only'
    && primaryAsset.media_semantic_status === 'classified'
    && primaryAsset.media_role === 'event_photo'
    && Number(primaryAsset.media_role_confidence || 0) >= 0.9
    && primaryAsset.safe_crop === true
    && primaryAsset.focal_point
    && ratio >= 1.2
  );
  if (!isNeutralFallback && !isSafeWidePhoto) return null;
  const candidate = listingCandidates(event)[0];
  if (!candidate) return null;
  return toListingMedallion(candidate);
}
