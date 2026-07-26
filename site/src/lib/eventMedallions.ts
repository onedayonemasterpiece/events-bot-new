import type { PreviewEvent } from './types';
import type { EventTransportSuggestion } from './eventTransport';

export type MedallionIdentityCategory = 'venue_brand' | 'festival_brand' | 'festival' | 'organizer';
export type MedallionEvidenceField = 'venue_name' | 'venue_address' | 'festival' | 'source_url' | 'festival_policy';

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

export interface EventMedallionLayout {
  main?: ResolvedOrganizerMedallion;
  secondary: ResolvedOrganizerMedallion[];
}

export interface RailTransportMedallionDefinition {
  slug: 'rzd-lastochka';
  name: string;
  avatarUrl: string;
  fallbackPngUrl: string;
  ariaLabel: string;
}

const RZD_LASTOCHKA_MEDALLION: RailTransportMedallionDefinition = {
  slug:'rzd-lastochka',
  name:'Электропоезд «Ласточка»',
  avatarUrl:'/assets/transport/rzd-lastochka-medallion.webp',
  fallbackPngUrl:'/assets/transport/rzd-lastochka-medallion.png',
  ariaLabel:'Транспортная подсказка: электропоезд «Ласточка»',
};

/**
 * Project the accepted transport artwork only from the same grounded payload
 * that renders EventTransportSchedule. City/title/venue prose is deliberately
 * not accepted here: a null transport suggestion must remain a null token.
 */
export function resolveRailTransportMedallion(
  suggestion: EventTransportSuggestion | null | undefined,
): RailTransportMedallionDefinition | null {
  return suggestion ? RZD_LASTOCHKA_MEDALLION : null;
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
  field: Extract<MedallionEvidenceField, 'venue_name' | 'venue_address' | 'festival'>,
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
  if (evidence.field === 'venue_address' && evidence.match === 'exact') return 38;
  if (evidence.field === 'venue_address') return 36;
  if (evidence.field === 'festival' && evidence.match === 'exact') return 35;
  if (evidence.field === 'festival') return 30;
  if (evidence.field === 'source_url') return 20;
  return 10;
}

/**
 * Split already-resolved structured identities into the product layout roles.
 *
 * A structured festival is the strongest principal identity, followed by an
 * explicit organizer. A venue becomes Main only when no festival/organizer
 * resolved (for example the current MUMOD event). This keeps extra locations
 * secondary without inventing organizer identity from title or description.
 */
export function classifyEventMedallionLayout(
  resolution: EventMedallionResolution,
): EventMedallionLayout {
  if (resolution.identities.length === 0) return { secondary:[] };

  const categoryRank = (item: OrganizerMedallionDefinition): number => {
    const category = item.category || 'organizer';
    if (category === 'festival_brand') return 400;
    if (category === 'festival') return 390;
    if (category === 'organizer') return 300;
    return 200;
  };
  const ordered = resolution.identities
    .map((identity, sourceOrder) => ({ identity, sourceOrder }))
    .sort((left, right) => (
      categoryRank(right.identity.item) - categoryRank(left.identity.item)
      || evidenceRank(right.identity.evidence) - evidenceRank(left.identity.evidence)
      || left.sourceOrder - right.sourceOrder
      || left.identity.item.slug.localeCompare(right.identity.item.slug)
    ));
  const main = ordered[0]?.identity;
  return {
    main,
    secondary:resolution.identities.filter((identity) => identity !== main),
  };
}

export function resolveEventMedallions(
  event: Pick<PreviewEvent, 'venue_name' | 'address' | 'festival' | 'source_url' | 'source_urls'>,
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
      evidence = firstAliasEvidence(item, 'venue_name', event.venue_name)
        || firstAliasEvidence(item, 'venue_address', event.address)
        || sourceEvidence(item, sourceUrls);
    } else if (category === 'festival_brand' || category === 'festival') {
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
