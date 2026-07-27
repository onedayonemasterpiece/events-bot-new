export const FOCUS_PREVIEW_STORAGE_KEY = 'kenigevents:focus-preview:v1';
export const FOCUS_PREVIEW_MARKER_VERSION = 1;
export const FOCUS_PREVIEW_TTL_MS = 72 * 60 * 60 * 1000;
export const FOCUS_PREVIEW_MAX_BYTES = 384;

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

export function readFocusPreviewMarker(storage: FocusStorage, now = Date.now()): FocusPreviewMarker | null {
  try {
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
  } catch {
    // Storage can be disabled; clearing a missing UX hint remains a no-op.
  }
}
