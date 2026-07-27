export const FOCUS_PREVIEW_STORAGE_KEY = 'kenigevents:focus-preview:v1';
export const FOCUS_PREVIEW_MARKER_VERSION = 1;
export const FOCUS_PREVIEW_TTL_MS = 72 * 60 * 60 * 1000;
export const FOCUS_PREVIEW_MAX_BYTES = 384;
export const FOCUS_PARTICIPATION_STORAGE_KEY = 'kenigevents:focus-participation:v1';
export const FOCUS_PARTICIPATION_MARKER_VERSION = 1;
export const FOCUS_PARTICIPATION_PROGRAM_ID = 'static-site-focus-group-2026';
// Browser-local continuity is deliberately much longer than the old 72-hour
// preview hint. The programme can still end earlier through server policy.
export const FOCUS_PARTICIPATION_SAFETY_TTL_MS = 366 * 24 * 60 * 60 * 1000;
export const FOCUS_PARTICIPATION_MAX_BYTES = 640;

const INVITE_TOKEN_PATTERN = /^[A-Za-z0-9_-]{16,128}$/u;

export type FocusInviteFragmentStatus = 'accepted' | 'invalid' | 'missing';

export interface FocusInviteFragmentResult {
  status: FocusInviteFragmentStatus;
  cleanHref: string;
}

export interface FocusPreviewMarker {
  version: 1;
  kind: 'focus_preview_hint';
  source: 'invite_fragment';
  createdAt: number;
  expiresAt: number;
}

export type FocusParticipationStatus = 'joining' | 'active';
export type FocusIdentityChoice = 'undecided' | 'email_intent' | 'yandex_intent' | 'skipped';

export interface FocusParticipationMarker {
  version: 1;
  kind: 'focus_participation_hint';
  programId: typeof FOCUS_PARTICIPATION_PROGRAM_ID;
  status: FocusParticipationStatus;
  source: 'invite_fragment' | 'legacy_preview';
  identityChoice: FocusIdentityChoice;
  createdAt: number;
  joinedAt: number | null;
  expiresAt: number;
}

export interface FocusStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

/**
 * Inspect an invite fragment without returning or retaining its bearer value.
 *
 * The caller must immediately replace the browser URL with `cleanHref` before
 * it performs any other page logic. A fragment is not sent in HTTP requests,
 * but removing it also keeps it out of later copied URLs and browser history.
 */
export function inspectFocusInviteUrl(rawUrl: string): FocusInviteFragmentResult {
  const url = new URL(rawUrl, 'https://focus-preview.invalid');
  const cleanHref = `${url.pathname}${url.search}`;

  if (!url.hash) return { status: 'missing', cleanHref };

  const fragment = url.hash.slice(1);
  const candidate = fragment.startsWith('invite=')
    ? fragment.slice('invite='.length)
    : fragment;
  const status: FocusInviteFragmentStatus = INVITE_TOKEN_PATTERN.test(candidate)
    ? 'accepted'
    : 'invalid';

  return { status, cleanHref };
}

export function createFocusPreviewMarker(now = Date.now()): FocusPreviewMarker {
  return {
    version: FOCUS_PREVIEW_MARKER_VERSION,
    kind: 'focus_preview_hint',
    source: 'invite_fragment',
    createdAt: now,
    expiresAt: now + FOCUS_PREVIEW_TTL_MS,
  };
}

export function serializeFocusPreviewMarker(marker: FocusPreviewMarker): string {
  const serialized = JSON.stringify(marker);
  if (new TextEncoder().encode(serialized).byteLength > FOCUS_PREVIEW_MAX_BYTES) {
    throw new Error('Focus preview marker exceeds its size limit.');
  }
  return serialized;
}

export function parseFocusPreviewMarker(
  raw: string | null,
  now = Date.now(),
): FocusPreviewMarker | null {
  if (!raw || new TextEncoder().encode(raw).byteLength > FOCUS_PREVIEW_MAX_BYTES) return null;

  try {
    const value = JSON.parse(raw) as Partial<FocusPreviewMarker>;
    if (
      value.version !== FOCUS_PREVIEW_MARKER_VERSION
      || value.kind !== 'focus_preview_hint'
      || value.source !== 'invite_fragment'
      || !Number.isFinite(value.createdAt)
      || !Number.isFinite(value.expiresAt)
      || Number(value.createdAt) > now
      || Number(value.expiresAt) <= now
      || Number(value.expiresAt) - Number(value.createdAt) > FOCUS_PREVIEW_TTL_MS
    ) {
      return null;
    }
    return value as FocusPreviewMarker;
  } catch {
    return null;
  }
}

export function storeFocusPreviewMarker(storage: FocusStorage, now = Date.now()): boolean {
  try {
    const marker = createFocusPreviewMarker(now);
    storage.setItem(FOCUS_PREVIEW_STORAGE_KEY, serializeFocusPreviewMarker(marker));
    return true;
  } catch {
    return false;
  }
}

export function readFocusPreviewMarker(
  storage: FocusStorage,
  now = Date.now(),
): FocusPreviewMarker | FocusParticipationMarker | null {
  try {
    const participation = readFocusParticipationMarker(storage, now);
    if (participation?.status === 'active') return participation;

    const marker = parseFocusPreviewMarker(storage.getItem(FOCUS_PREVIEW_STORAGE_KEY), now);
    if (!marker) storage.removeItem(FOCUS_PREVIEW_STORAGE_KEY);
    return marker;
  } catch {
    return null;
  }
}

export function clearFocusPreviewMarker(storage: FocusStorage): void {
  try {
    storage.removeItem(FOCUS_PREVIEW_STORAGE_KEY);
    storage.removeItem(FOCUS_PARTICIPATION_STORAGE_KEY);
  } catch {
    // Storage can be disabled; clearing a missing UX hint remains a no-op.
  }
}

export function createFocusParticipationMarker(
  now = Date.now(),
  source: FocusParticipationMarker['source'] = 'invite_fragment',
): FocusParticipationMarker {
  return {
    version: FOCUS_PARTICIPATION_MARKER_VERSION,
    kind: 'focus_participation_hint',
    programId: FOCUS_PARTICIPATION_PROGRAM_ID,
    status: source === 'legacy_preview' ? 'active' : 'joining',
    source,
    identityChoice: source === 'legacy_preview' ? 'skipped' : 'undecided',
    createdAt: now,
    joinedAt: source === 'legacy_preview' ? now : null,
    expiresAt: now + FOCUS_PARTICIPATION_SAFETY_TTL_MS,
  };
}

export function serializeFocusParticipationMarker(marker: FocusParticipationMarker): string {
  const serialized = JSON.stringify(marker);
  if (new TextEncoder().encode(serialized).byteLength > FOCUS_PARTICIPATION_MAX_BYTES) {
    throw new Error('Focus participation marker exceeds its size limit.');
  }
  return serialized;
}

export function parseFocusParticipationMarker(
  raw: string | null,
  now = Date.now(),
): FocusParticipationMarker | null {
  if (!raw || new TextEncoder().encode(raw).byteLength > FOCUS_PARTICIPATION_MAX_BYTES) return null;

  try {
    const value = JSON.parse(raw) as Partial<FocusParticipationMarker>;
    if (
      value.version !== FOCUS_PARTICIPATION_MARKER_VERSION
      || value.kind !== 'focus_participation_hint'
      || value.programId !== FOCUS_PARTICIPATION_PROGRAM_ID
      || !['joining', 'active'].includes(String(value.status))
      || !['invite_fragment', 'legacy_preview'].includes(String(value.source))
      || !['undecided', 'email_intent', 'yandex_intent', 'skipped'].includes(String(value.identityChoice))
      || !Number.isFinite(value.createdAt)
      || !Number.isFinite(value.expiresAt)
      || Number(value.createdAt) > now
      || Number(value.expiresAt) <= now
      || Number(value.expiresAt) - Number(value.createdAt) > FOCUS_PARTICIPATION_SAFETY_TTL_MS
      || (value.joinedAt !== null && !Number.isFinite(value.joinedAt))
      || (value.status === 'active' && value.joinedAt === null)
      || (value.status === 'joining' && value.joinedAt !== null)
    ) {
      return null;
    }
    return value as FocusParticipationMarker;
  } catch {
    return null;
  }
}

export function storeFocusParticipationMarker(
  storage: FocusStorage,
  now = Date.now(),
  source: FocusParticipationMarker['source'] = 'invite_fragment',
): FocusParticipationMarker | null {
  try {
    const marker = createFocusParticipationMarker(now, source);
    storage.setItem(
      FOCUS_PARTICIPATION_STORAGE_KEY,
      serializeFocusParticipationMarker(marker),
    );
    return marker;
  } catch {
    return null;
  }
}

export function readFocusParticipationMarker(
  storage: FocusStorage,
  now = Date.now(),
): FocusParticipationMarker | null {
  try {
    const marker = parseFocusParticipationMarker(
      storage.getItem(FOCUS_PARTICIPATION_STORAGE_KEY),
      now,
    );
    if (marker) return marker;
    storage.removeItem(FOCUS_PARTICIPATION_STORAGE_KEY);

    // Preserve continuity for browsers that joined through the first 72-hour
    // prototype. The bearer itself was never stored, and the old hint is
    // removed immediately after this one-time migration.
    const legacy = parseFocusPreviewMarker(storage.getItem(FOCUS_PREVIEW_STORAGE_KEY), now);
    if (!legacy) return null;
    const migrated = storeFocusParticipationMarker(storage, now, 'legacy_preview');
    if (migrated) storage.removeItem(FOCUS_PREVIEW_STORAGE_KEY);
    return migrated;
  } catch {
    return null;
  }
}

export function updateFocusParticipationIdentityChoice(
  storage: FocusStorage,
  choice: Exclude<FocusIdentityChoice, 'undecided'>,
  now = Date.now(),
): FocusParticipationMarker | null {
  try {
    const marker = readFocusParticipationMarker(storage, now);
    if (!marker || marker.status !== 'joining') return null;
    const updated = { ...marker, identityChoice: choice };
    storage.setItem(
      FOCUS_PARTICIPATION_STORAGE_KEY,
      serializeFocusParticipationMarker(updated),
    );
    return updated;
  } catch {
    return null;
  }
}

export function activateFocusParticipation(
  storage: FocusStorage,
  choice: Exclude<FocusIdentityChoice, 'undecided'>,
  now = Date.now(),
): FocusParticipationMarker | null {
  try {
    const marker = readFocusParticipationMarker(storage, now);
    if (!marker) return null;
    if (marker.status === 'active') return marker;
    const activated: FocusParticipationMarker = {
      ...marker,
      status: 'active',
      identityChoice: choice,
      joinedAt: now,
    };
    storage.setItem(
      FOCUS_PARTICIPATION_STORAGE_KEY,
      serializeFocusParticipationMarker(activated),
    );
    storage.removeItem(FOCUS_PREVIEW_STORAGE_KEY);
    return activated;
  } catch {
    return null;
  }
}

export function clearFocusParticipationMarker(storage: FocusStorage): void {
  try {
    storage.removeItem(FOCUS_PARTICIPATION_STORAGE_KEY);
  } catch {
    // Explicit programme exit remains a no-op when storage is unavailable.
  }
}
