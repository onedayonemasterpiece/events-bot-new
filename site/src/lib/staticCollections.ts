import registryPayload from '../data/static-collection-registry.json';
import gastronomyPayload from '../data/gastronomy-collection-v1.json';
import productionCatalog from '../data/production-catalog.json';
import { createHash } from 'node:crypto';
import type { PreviewEvent } from './types';

export type CollectionRegistryStatus = 'public' | 'repair' | 'blocked' | 'deferred';
export type GastronomyLifecycle = 'active' | 'low_supply' | 'recent_empty' | 'dormant' | 'blocked' | 'last_good';

export interface CollectionRegistryEntry {
  key: string;
  title: string;
  description: string;
  path: string | null;
  status: CollectionRegistryStatus;
  catalog: boolean;
  navigation: boolean;
  sitemap: boolean;
}

export interface GastronomyDecision {
  event_id: number;
  family_id: string;
  role: 'core' | 'co_core';
  occurrence: 'future' | 'recent';
}

export interface GastronomyManifest {
  schema_version: 'gastronomy-collection-v1';
  policy_version: 'gastronomy_v1';
  generated_at: string;
  current_date: string;
  catalog_hash: string;
  manifest_hash: string;
  provider_calls: number;
  compute_status: 'pass' | 'failed' | 'blocked';
  quality_status: 'pass' | 'failed' | 'blocked' | 'not_evaluated';
  publication_status: 'ready' | 'shadow' | 'blocked';
  failure_reason?: string | null;
  accepted: GastronomyDecision[];
  last_good: { status: 'absent' | 'available'; manifest: GastronomyManifest | null };
}

const VALID_STATUSES = new Set<CollectionRegistryStatus>(['public', 'repair', 'blocked', 'deferred']);

function checkedRegistry(): CollectionRegistryEntry[] {
  const raw = registryPayload as { schema_version?: string; entries?: CollectionRegistryEntry[] };
  if (raw.schema_version !== 'static-collection-registry-v1' || !Array.isArray(raw.entries)) return [];
  const seen = new Set<string>();
  return raw.entries.filter((entry) => {
    if (!entry || !entry.key || seen.has(entry.key) || !VALID_STATUSES.has(entry.status)) return false;
    seen.add(entry.key);
    return entry.status === 'blocked'
      ? !entry.catalog && !entry.navigation && !entry.sitemap
      : true;
  });
}

export const staticCollectionRegistry = checkedRegistry();
export const collectionCatalogEntries = staticCollectionRegistry.filter((entry) => entry.catalog && entry.status !== 'blocked');
export const collectionNavigationEntries = staticCollectionRegistry.filter((entry) => entry.navigation && entry.status !== 'blocked');
export const collectionSitemapEntries = staticCollectionRegistry.filter((entry) => entry.sitemap && entry.status === 'public' && Boolean(entry.path));

function withoutDormantGastronomy(entries: CollectionRegistryEntry[], lifecycle?: GastronomyLifecycle): CollectionRegistryEntry[] {
  return lifecycle === 'dormant' ? entries.filter((entry) => entry.key !== 'gastronomy') : entries;
}

export const getCollectionCatalogEntries = (lifecycle?: GastronomyLifecycle) => withoutDormantGastronomy(collectionCatalogEntries, lifecycle);
export const getCollectionNavigationEntries = (lifecycle?: GastronomyLifecycle) => withoutDormantGastronomy(collectionNavigationEntries, lifecycle);
export const getCollectionSitemapEntries = (lifecycle?: GastronomyLifecycle) => withoutDormantGastronomy(collectionSitemapEntries, lifecycle);

export interface ResolvedGastronomyCollection {
  lifecycle: GastronomyLifecycle;
  publicationStatus: 'public' | 'shadow' | 'blocked';
  future: Array<{ event: PreviewEvent; familyId: string }>;
  recent: Array<{ event: PreviewEvent; familyId: string }>;
  manifest: GastronomyManifest;
  reason: string | null;
}

function dateSixMonthsBefore(value: string): string | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/u.exec(value);
  if (!match) return null;
  const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
  date.setUTCMonth(date.getUTCMonth() - 6);
  return date.toISOString().slice(0, 10);
}

function stableStringify(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  if (value && typeof value === 'object') {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record).sort().map((key) => `${JSON.stringify(key)}:${stableStringify(record[key])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

const currentCatalogHash = createHash('sha256').update(stableStringify(productionCatalog)).digest('hex');

function manifestIsUsable(manifest: GastronomyManifest, catalogIds: Set<number>): boolean {
  if (manifest.schema_version !== 'gastronomy-collection-v1' || manifest.policy_version !== 'gastronomy_v1') return false;
  if (manifest.provider_calls !== 0 || manifest.compute_status !== 'pass' || manifest.quality_status !== 'pass') return false;
  if (manifest.catalog_hash !== currentCatalogHash) return false;
  const { manifest_hash: claimedHash, ...unhashed } = manifest;
  const actualHash = createHash('sha256').update(stableStringify(unhashed)).digest('hex');
  if (!claimedHash || claimedHash !== actualHash) return false;
  const eventIds = new Set<number>();
  const familyOccurrences = new Set<string>();
  for (const item of manifest.accepted || []) {
    const eventId = Number(item.event_id);
    if (!Number.isInteger(eventId) || eventId <= 0 || !catalogIds.has(eventId) || eventIds.has(eventId)) return false;
    if (!item.family_id || !['core', 'co_core'].includes(item.role) || !['future', 'recent'].includes(item.occurrence)) return false;
    const familyOccurrence = `${item.family_id}:${item.occurrence}`;
    if (familyOccurrences.has(familyOccurrence)) return false;
    eventIds.add(eventId);
    familyOccurrences.add(familyOccurrence);
  }
  return true;
}

function selectManifest(raw: GastronomyManifest, catalogIds: Set<number>): { manifest: GastronomyManifest; usedLastGood: boolean } | null {
  if (manifestIsUsable(raw, catalogIds)) return { manifest: raw, usedLastGood: false };
  const lastGood = raw.last_good?.status === 'available' ? raw.last_good.manifest : null;
  return lastGood && manifestIsUsable(lastGood, catalogIds) ? { manifest: lastGood, usedLastGood: true } : null;
}

export function resolveGastronomyCollection(
  events: PreviewEvent[],
  rawManifest: GastronomyManifest = gastronomyPayload as unknown as GastronomyManifest,
): ResolvedGastronomyCollection {
  const byId = new Map(events.map((event) => [Number(event.id), event]));
  const selected = selectManifest(rawManifest, new Set(byId.keys()));
  if (!selected) {
    return {
      lifecycle: 'blocked', publicationStatus: 'blocked', future: [], recent: [], manifest: rawManifest,
      reason: rawManifest.failure_reason || 'manifest_failed_closed',
    };
  }
  const manifest = selected.manifest;
  const cutoff = dateSixMonthsBefore(manifest.current_date);
  const future = manifest.accepted
    .filter((item) => item.occurrence === 'future')
    .map((item) => ({ event: byId.get(item.event_id)!, familyId: item.family_id }))
    .filter((item) => item.event.start_date >= manifest.current_date)
    .sort((a, b) => a.event.start_date.localeCompare(b.event.start_date) || a.event.id - b.event.id);
  const recent = manifest.accepted
    .filter((item) => item.occurrence === 'recent')
    .map((item) => ({ event: byId.get(item.event_id)!, familyId: item.family_id }))
    .filter((item) => Boolean(cutoff) && item.event.start_date < manifest.current_date && item.event.start_date >= cutoff!)
    .sort((a, b) => b.event.start_date.localeCompare(a.event.start_date) || a.event.id - b.event.id);
  const lifecycle: GastronomyLifecycle = selected.usedLastGood
    ? 'last_good'
    : future.length >= 3 ? 'active'
      : future.length >= 1 ? 'low_supply'
        : recent.length ? 'recent_empty' : 'dormant';
  return {
    lifecycle,
    publicationStatus: manifest.publication_status === 'ready' && lifecycle !== 'dormant' ? 'public' : 'shadow',
    future,
    recent,
    manifest,
    reason: selected.usedLastGood ? 'using_last_good_manifest' : null,
  };
}
