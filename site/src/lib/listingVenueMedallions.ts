import organizerMedallions from '../data/organizerMedallions.json';
import festivalMedallions from '../data/festivalMedallions.json';
import type { EventImageAsset, PreviewEvent } from './types';

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
  listingStatus?: 'listing_ready' | 'detail_only' | 'blocked_missing_binding';
  listingBinding?: 'venue' | 'festival' | 'organizer';
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
    .filter((value) => value.length >= 3)));
}

/** Exact or bounded phrase match; deliberately no fuzzy prose matching. */
function matchesStructuredValue(item: ManifestMedallion, value: unknown): boolean {
  const normalized = normalize(value);
  if (!normalized) return false;
  return namesFor(item).some((candidate) => {
    if (candidate === normalized) return true;
    if (candidate.length < 5) return false;
    return (` ${normalized} `).includes(` ${candidate} `)
      || (` ${candidate} `).includes(` ${normalized} `);
  });
}

const allItems: ManifestMedallion[] = [
  ...((organizerMedallions as { items?: ManifestMedallion[] }).items || []),
  ...((festivalMedallions as { items?: ManifestMedallion[] }).items || []),
];

function listingCandidates(event: PreviewEvent): ManifestMedallion[] {
  const ready = allItems.filter((item) => item.listingStatus === 'listing_ready');
  const matched = [
    ...ready.filter((item) => item.listingBinding === 'venue' && matchesStructuredValue(item, event.venue_name)),
    ...ready.filter((item) => item.listingBinding === 'festival' && matchesStructuredValue(item, event.festival)),
  ];
  return matched.filter((item, index) => matched.findIndex((candidate) => candidate.slug === item.slug) === index);
}

function toListingMedallion(item: ManifestMedallion): ListingVenueMedallion {
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
  const primaryAsset = selectedAsset || event.image_assets?.[0];
  if (!event.image_url || !primaryAsset || primaryAsset.image_text_mode !== 'visual_only') return null;
  const item = listingCandidates(event)[0];
  if (!item) return null;
  return toListingMedallion(item);
}
