import organizerMedallions from '../data/organizerMedallions.json';
import festivalMedallions from '../data/festivalMedallions.json';
import type { PreviewEvent } from './types';

export interface ListingVenueMedallion {
  slug: string;
  name: string;
  shortName: string;
  avatarUrl: string;
  fallbackPngUrl?: string;
  background?: string;
  ring?: string;
  ariaLabel: string;
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
  category?: string;
}

function normalize(value: unknown): string {
  return String(value || '')
    .toLowerCase()
    .replace(/<[^>]*>/gu, ' ')
    .replace(/[«»"'`´’‘.,!?()[\]{}:;—–-]+/gu, ' ')
    .replace(/\s+/gu, ' ')
    .trim();
}

function namesFor(item: ManifestMedallion): string[] {
  return Array.from(new Set([item.name, item.shortName, ...(item.aliases || [])]
    .map(normalize)
    .filter((value) => value.length >= 4)));
}

function matchesVenue(item: ManifestMedallion, venueName: string): boolean {
  const venue = normalize(venueName);
  if (!venue) return false;
  return namesFor(item).some((candidate) => (
    candidate === venue
    || (candidate.length >= 6 && venue.includes(candidate))
    || (venue.length >= 6 && candidate.includes(venue))
  ));
}

const organizerItems = (organizerMedallions as { items?: ManifestMedallion[] }).items || [];
const venueBrandItems = ((festivalMedallions as { items?: ManifestMedallion[] }).items || [])
  .filter((item) => item.category === 'venue_brand');

/**
 * Listing overlays fail closed: they are allowed only for a primary image that
 * the image pipeline explicitly classified as a visual-only event photo and for
 * a venue that has a curated, source-grounded medallion. This additional
 * semantic gate matters because the legacy short-OCR heuristic can label a
 * poster with a small wordmark as visual_only. safe_crop is deliberately not
 * used as an OCR proxy.
 */
export function getListingVenueMedallion(event: PreviewEvent): ListingVenueMedallion | null {
  if (!event.image_url || event.image_text_mode !== 'visual_only' || !event.venue_name) return null;
  const primaryAsset = event.image_assets?.[0];
  if (
    !primaryAsset
    || primaryAsset.image_text_mode !== 'visual_only'
    || primaryAsset.media_role !== 'event_photo'
    || primaryAsset.media_semantic_status !== 'classified'
    || primaryAsset.image_kind !== 'photo'
  ) return null;

  const item = [...organizerItems, ...venueBrandItems]
    .find((candidate) => matchesVenue(candidate, event.venue_name || ''));
  if (!item) return null;

  return {
    slug: item.slug,
    name: item.name,
    shortName: item.shortName || item.name,
    avatarUrl: item.avatarUrl,
    fallbackPngUrl: item.fallbackPngUrl,
    background: item.background,
    ring: item.ring,
    ariaLabel: item.ariaLabel || `Локация: ${item.name}`,
  };
}
