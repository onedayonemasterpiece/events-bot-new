export interface StorageRegistryEntry {
  key: string;
  prefix?: boolean;
  maxBytes: number;
  ttlMs?: number;
  version?: number;
}

/** Hard ceiling for KenigEvents state, excluding the Supabase-owned auth token. */
export const NON_AUTH_STORAGE_BUDGET_BYTES = 64 * 1024;

export const APP_STORAGE_REGISTRY: readonly StorageRegistryEntry[] = [
  { key: 'ke_personalization_profile', maxBytes: 12 * 1024, version: 1 },
  { key: 'ke_event_feedback_log_v2', maxBytes: 7 * 1024, ttlMs: 14 * 86_400_000, version: 2 },
  { key: 'ke_search_feedback_queue_v2', maxBytes: 5 * 1024, ttlMs: 7 * 86_400_000, version: 2 },
  { key: 'ke_idempotent_outbox_v1', maxBytes: 12 * 1024, ttlMs: 86_400_000, version: 1 },
  { key: 'ke_calendar_saved_v1', maxBytes: 4 * 1024, version: 1 },
  { key: 'ke_listing_personal_feed_hint_v2', maxBytes: 1024, ttlMs: 30 * 60_000, version: 2 },
  { key: 'ke_saved_event_reconciliation_v2', maxBytes: 768, version: 2 },
  { key: 'ke_artifact_collection_v1', maxBytes: 3 * 1024, version: 1 },
  { key: 'ke_unusual_seen_v1', maxBytes: 2 * 1024, version: 1 },
  { key: 'ke_festival_likes_v1', maxBytes: 2 * 1024, version: 1 },
  { key: 'ke_experiment_', prefix: true, maxBytes: 3 * 1024 },
  { key: 'kenigevents:pwa-', prefix: true, maxBytes: 512 },
  { key: 'ke_', prefix: true, maxBytes: 8 * 1024 },
] as const;

const LEGACY_DROP_KEYS = new Set([
  'ke_event_feedback_log_v1',
  'ke_search_feedback_queue_v1',
  'ke_saved_event_reconciliation_v1',
]);
const LEGACY_DROP_PREFIXES = [
  'ke_listing_personal_feed_cache_v1:',
  'ke_event_continuation_recent_v1:/preview-',
  'ke_supabase_transport_route_v1',
];

function bytes(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function isSupabaseAuthKey(key: string): boolean {
  // Never let application cleanup rewrite authentication state. Project refs
  // are usually hyphenless, but older/self-hosted refs need not be.
  return /^sb-.+-auth-token(?:-code-verifier)?$/u.test(key);
}

function keysOf(storage: Pick<Storage, 'length' | 'key'>): string[] {
  const keys: string[] = [];
  try {
    for (let index = 0; index < storage.length; index += 1) {
      const key = storage.key(index);
      if (key) keys.push(key);
    }
  } catch { return []; }
  return keys;
}

function registryEntry(key: string): StorageRegistryEntry | null {
  const exact = APP_STORAGE_REGISTRY.find((item) => !item.prefix && item.key === key);
  if (exact) return exact;
  return APP_STORAGE_REGISTRY
    .filter((item) => item.prefix && key.startsWith(item.key))
    .sort((left, right) => right.key.length - left.key.length)[0] || null;
}

function expiryOf(raw: string): number | null {
  try {
    const parsed = JSON.parse(raw);
    const expiry = Number(parsed?.expiresAt ?? parsed?.expires_at ?? 0);
    return Number.isFinite(expiry) && expiry > 0 ? expiry : null;
  } catch { return null; }
}

export function registeredWorstCaseBudget(): number {
  return APP_STORAGE_REGISTRY.reduce((total, item) => total + item.maxBytes, 0);
}

export function cleanupAppStorage(
  storage: Pick<Storage, 'length' | 'key' | 'getItem' | 'removeItem'>,
  options: { now?: number; currentBasePath?: string } = {},
): { removed: string[]; bytes: number } {
  const now = Number(options.now ?? Date.now());
  const currentBasePath = String(options.currentBasePath || '/').replace(/\/+$/u, '') || '/';
  const removed: string[] = [];
  for (const key of keysOf(storage)) {
    if (isSupabaseAuthKey(key)) continue;
    let remove = LEGACY_DROP_KEYS.has(key) || LEGACY_DROP_PREFIXES.some((prefix) => key.startsWith(prefix));
    if (/^ke_.+:(?:\/)?preview-/u.test(key) && !key.includes(currentBasePath)) remove = true;
    let raw = '';
    try { raw = storage.getItem(key) || ''; } catch { raw = ''; }
    const entry = registryEntry(key);
    if (!remove && entry && bytes(raw) > entry.maxBytes) remove = true;
    if (!remove && entry?.ttlMs) {
      const expiry = expiryOf(raw);
      if (expiry !== null && expiry <= now) remove = true;
    }
    if (!remove) continue;
    try { storage.removeItem(key); removed.push(key); } catch { /* best effort */ }
  }
  let total = 0;
  for (const key of keysOf(storage)) {
    if (isSupabaseAuthKey(key)) continue;
    try { total += bytes(key) + bytes(storage.getItem(key) || ''); } catch { /* ignored */ }
  }
  return { removed, bytes: total };
}

export function boundedJsonWrite(
  storage: Pick<Storage, 'setItem' | 'removeItem'>,
  key: string,
  value: unknown,
  maxBytes: number,
): boolean {
  try {
    const serialized = JSON.stringify(value);
    if (bytes(serialized) > maxBytes) return false;
    storage.setItem(key, serialized);
    return true;
  } catch {
    try { storage.removeItem(key); } catch { /* ignored */ }
    return false;
  }
}

export function boundedJsonRead<T>(
  storage: Pick<Storage, 'getItem' | 'removeItem'>,
  key: string,
  fallback: T,
  maxBytes: number,
): T {
  try {
    const raw = storage.getItem(key);
    if (!raw) return fallback;
    if (bytes(raw) > maxBytes) {
      storage.removeItem(key);
      return fallback;
    }
    return JSON.parse(raw) as T;
  } catch {
    try { storage.removeItem(key); } catch { /* ignored */ }
    return fallback;
  }
}

function boundedIds(value: unknown, max: number): number[] {
  const values = Array.isArray(value) ? value : [];
  return [...new Set(values.map(Number).filter((id) => Number.isSafeInteger(id) && id > 0))].slice(-max);
}

function boundedScoreMap(value: unknown, max: number): Record<string, number> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return Object.fromEntries(Object.entries(value as Record<string, unknown>)
    .map(([key, score]) => [String(key).slice(0, 80), Math.max(-20, Math.min(20, Number(score || 0)))] as const)
    .filter(([key, score]) => Boolean(key) && Number.isFinite(score) && score !== 0)
    .sort((left, right) => Math.abs(right[1]) - Math.abs(left[1]))
    .slice(0, max));
}

export function compactPersonalizationProfile<T>(profile: T): T {
  if (!profile || typeof profile !== 'object' || Array.isArray(profile)) return profile;
  const source = profile as Record<string, unknown>;
  const compact: Record<string, unknown> = {
    ...source,
    liked_event_ids: boundedIds(source.liked_event_ids, 80),
    not_interested_event_ids: boundedIds(source.not_interested_event_ids, 100),
    hidden_event_ids: boundedIds(source.hidden_event_ids, 100),
    seen_event_ids: boundedIds(source.seen_event_ids, 100),
    seen_venue_ids: boundedIds(source.seen_venue_ids, 64),
    positive_tags: boundedScoreMap(source.positive_tags, 48),
    negative_interest_tags: boundedScoreMap(source.negative_interest_tags, 48),
    share_counts: boundedScoreMap(source.share_counts, 64),
  };
  for (const horizon of ['session', 'short', 'mid', 'long']) {
    const value = source[horizon];
    if (!value || typeof value !== 'object' || Array.isArray(value)) continue;
    const horizonSource = value as Record<string, unknown>;
    compact[horizon] = {
      ...horizonSource,
      positive_tags: boundedScoreMap(horizonSource.positive_tags, 32),
      negative_interest_tags: boundedScoreMap(horizonSource.negative_interest_tags, 32),
    };
  }
  return compact as T;
}
