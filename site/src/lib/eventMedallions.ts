import type { PreviewEvent } from './types';

export type MedallionIdentityCategory = 'venue_brand' | 'festival_brand' | 'organizer';
export type MedallionEvidenceField = 'venue_name' | 'festival' | 'source_url' | 'festival_policy';

export interface OrganizerMedallionDefinition {
  slug: string;
  name: string;
  shortName?: string;
  aliases?: string[];
  avatarUrl: string;
  fallbackPngUrl?: string;
  background?: string;
  ring?: string;
  ariaLabel?: string;
  category?: MedallionIdentityCategory;
  impliedByFestivalAliases?: string[];
}

export interface MedallionEvidence {
  field: MedallionEvidenceField;
  value: string;
  alias: string;
  match: 'exact' | 'bounded' | 'source_identity' | 'curated_relation';
}

export interface ResolvedOrganizerMedallion {
  item: OrganizerMedallionDefinition;
  evidence: MedallionEvidence;
}

export interface EventMedallionResolution {
  identities: ResolvedOrganizerMedallion[];
  failClosedReason?: 'conflicting_source_identity' | 'ambiguous_venue_identity';
  conflictEvidence?: string[];
}

export function normalizeMedallionText(value: unknown): string {
  return String(value || '')
    .normalize('NFKC')
    .toLocaleLowerCase('ru-RU')
    .replace(/<[^>]*>/gu, ' ')
    .replace(/[«»"'`´’‘.,!?()[\]{}:;—–_/\\-]+/gu, ' ')
    .replace(/\s+/gu, ' ')
    .trim();
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
}

/**
 * Match one curated alias without allowing short abbreviations to hide inside
 * another Unicode word (for example, `ММО` in `программой`).
 */
export function matchMedallionAlias(value: unknown, alias: unknown): 'exact' | 'bounded' | null {
  const haystack = normalizeMedallionText(value);
  const needle = normalizeMedallionText(alias);
  if (!haystack || !needle) return null;
  if (haystack === needle) return 'exact';
  const pattern = new RegExp(`(?<![\\p{L}\\p{N}])${escapeRegExp(needle).replace(/\\ /gu, '\\s+')}(?![\\p{L}\\p{N}])`, 'u');
  return pattern.test(haystack) ? 'bounded' : null;
}

function aliasesFor(item: OrganizerMedallionDefinition): string[] {
  return Array.from(new Set([item.name, item.shortName, ...(item.aliases || [])].filter(Boolean).map(String)));
}

function firstAliasEvidence(
  item: OrganizerMedallionDefinition,
  field: Extract<MedallionEvidenceField, 'venue_name' | 'festival'>,
  value: string | null | undefined,
): MedallionEvidence | null {
  for (const alias of aliasesFor(item)) {
    const match = matchMedallionAlias(value, alias);
    if (match) return { field, value:String(value), alias, match };
  }
  return null;
}

function urlIdentity(value: string): { host: string; eventKey?: string } | null {
  try {
    const url = new URL(value);
    const identityMatch = `${url.pathname}/${url.hash}`.match(/(?:^|\/)event\/(\d+)(?:\/|$)/u);
    return { host:url.hostname.toLocaleLowerCase('en-US'), eventKey:identityMatch?.[1] };
  } catch {
    return null;
  }
}

function conflictingStructuredSourceIdentities(sourceUrls: string[]): string[] {
  const eventKeysByHost = new Map<string, Set<string>>();
  for (const value of sourceUrls) {
    const identity = urlIdentity(value);
    if (!identity?.eventKey) continue;
    const keys = eventKeysByHost.get(identity.host) || new Set<string>();
    keys.add(identity.eventKey);
    eventKeysByHost.set(identity.host, keys);
  }
  return Array.from(eventKeysByHost.entries())
    .filter(([, keys]) => keys.size > 1)
    .map(([host, keys]) => `${host}:event/${Array.from(keys).sort().join(',event/')}`)
    .sort();
}

function sourceEvidence(item: OrganizerMedallionDefinition, sourceUrls: string[]): MedallionEvidence | null {
  // Source evidence is deliberately limited to URL-shaped aliases. Ordinary
  // organization words in an arbitrary URL are not identity evidence.
  const urlAliases = aliasesFor(item).filter((alias) => /[._/]|\.[a-z]{2,}$/iu.test(alias));
  for (const value of sourceUrls) {
    const normalizedUrl = normalizeMedallionText(value);
    for (const alias of urlAliases) {
      const match = matchMedallionAlias(normalizedUrl, alias);
      if (match) return { field:'source_url', value, alias, match:'source_identity' };
    }
  }
  return null;
}

function evidenceRank(evidence: MedallionEvidence): number {
  if (evidence.field === 'venue_name' && evidence.match === 'exact') return 50;
  if (evidence.field === 'venue_name') return 40;
  if (evidence.field === 'festival' && evidence.match === 'exact') return 35;
  if (evidence.field === 'festival') return 30;
  if (evidence.field === 'source_url') return 20;
  return 10;
}

export function resolveEventMedallions(
  event: Pick<PreviewEvent, 'venue_name' | 'festival' | 'source_url' | 'source_urls'>,
  items: OrganizerMedallionDefinition[],
): EventMedallionResolution {
  const sourceUrls = Array.from(new Set([...(event.source_urls || []), event.source_url].filter(Boolean).map(String)));
  const conflictEvidence = conflictingStructuredSourceIdentities(sourceUrls);
  if (conflictEvidence.length > 0) {
    return { identities:[], failClosedReason:'conflicting_source_identity', conflictEvidence };
  }

  const candidates: ResolvedOrganizerMedallion[] = [];
  for (const item of items) {
    const category = item.category || 'organizer';
    let evidence: MedallionEvidence | null = null;
    if (category === 'venue_brand') {
      evidence = firstAliasEvidence(item, 'venue_name', event.venue_name) || sourceEvidence(item, sourceUrls);
    } else if (category === 'festival_brand') {
      evidence = firstAliasEvidence(item, 'festival', event.festival) || sourceEvidence(item, sourceUrls);
    } else {
      evidence = firstAliasEvidence(item, 'venue_name', event.venue_name) || sourceEvidence(item, sourceUrls);
      if (!evidence && event.festival) {
        for (const alias of item.impliedByFestivalAliases || []) {
          const match = matchMedallionAlias(event.festival, alias);
          if (match) {
            evidence = { field:'festival_policy', value:event.festival, alias, match:'curated_relation' };
            break;
          }
        }
      }
    }
    if (evidence) candidates.push({ item, evidence });
  }

  const venueCandidates = candidates
    .filter(({ item }) => (item.category || 'organizer') === 'venue_brand')
    .sort((left, right) => evidenceRank(right.evidence) - evidenceRank(left.evidence) || left.item.slug.localeCompare(right.item.slug));
  const strongestVenueRank = venueCandidates[0] ? evidenceRank(venueCandidates[0].evidence) : 0;
  const equallyStrongVenues = venueCandidates.filter(({ evidence }) => evidenceRank(evidence) === strongestVenueRank);
  if (equallyStrongVenues.length > 1) {
    return {
      identities:candidates.filter(({ item }) => (item.category || 'organizer') !== 'venue_brand'),
      failClosedReason:'ambiguous_venue_identity',
      conflictEvidence:equallyStrongVenues.map(({ item, evidence }) => `${item.slug}:${evidence.field}:${evidence.alias}`),
    };
  }

  const selectedVenue = venueCandidates[0];
  const selected = candidates
    .filter(({ item }) => (item.category || 'organizer') !== 'venue_brand')
    .concat(selectedVenue ? [selectedVenue] : [])
    .sort((left, right) => evidenceRank(right.evidence) - evidenceRank(left.evidence) || left.item.slug.localeCompare(right.item.slug));
  return { identities:selected.slice(0, 3) };
}
