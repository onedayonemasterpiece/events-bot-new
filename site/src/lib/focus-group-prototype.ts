export const FOCUS_PARTICIPATION_STORAGE_KEY = 'kenigevents:focus-participation:v1';
export const FOCUS_PARTICIPATION_MARKER_VERSION = 1;
export const FOCUS_PARTICIPATION_PROGRAM_ID = 'static-site-focus-group-2026';
export const FOCUS_GROUP_MASS_INVITE_TOKEN = 'focus-group-2026-announcements';
export const FOCUS_PARTICIPATION_DURATION_MS = 30 * 24 * 60 * 60 * 1000;
export const FOCUS_PARTICIPATION_MAX_BYTES = 640;
export const FOCUS_CONTINUING_CONSENT_STORAGE_KEY = 'kenigevents:focus-continuing-consent:v1';
export const FOCUS_CONTINUING_CONSENT_VERSION = 1;
export const FOCUS_CONTINUING_CONSENT_MAX_BYTES = 320;

const INVITE_TOKEN_PATTERN = /^[A-Za-z0-9_-]{16,128}$/u;

export type FocusInviteFragmentStatus = 'accepted' | 'invalid' | 'missing';

export interface FocusInviteFragmentResult {
  status: FocusInviteFragmentStatus;
  cleanHref: string;
}

export type FocusParticipationStatus = 'joining' | 'active';
export type FocusIdentityChoice = 'undecided' | 'email_intent' | 'yandex_intent' | 'skipped';

export interface FocusParticipationMarker {
  version: 1;
  kind: 'focus_participation_hint';
  programId: typeof FOCUS_PARTICIPATION_PROGRAM_ID;
  status: FocusParticipationStatus;
  source: 'invite_fragment';
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

export interface FocusContinuingConsent {
  version: 1;
  kind: 'focus_continuing_consent';
  acceptedAt: number;
  purposes: ['focus_updates', 'prize_result', 'service_updates'];
}

/**
 * Inspect an invite fragment without returning or retaining its bearer value.
 *
 * The caller must immediately replace the browser URL with `cleanHref` before
 * it performs any other page logic. A fragment is not sent in HTTP requests,
 * but removing it also keeps it out of later copied URLs and browser history.
 */
export function inspectFocusInviteUrl(rawUrl: string): FocusInviteFragmentResult {
  const url = new URL(rawUrl, 'https://focus-invite.invalid');
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

export function createFocusParticipationMarker(
  now = Date.now(),
): FocusParticipationMarker {
  return {
    version: FOCUS_PARTICIPATION_MARKER_VERSION,
    kind: 'focus_participation_hint',
    programId: FOCUS_PARTICIPATION_PROGRAM_ID,
    status: 'joining',
    source: 'invite_fragment',
    identityChoice: 'undecided',
    createdAt: now,
    joinedAt: null,
    expiresAt: now + FOCUS_PARTICIPATION_DURATION_MS,
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
      || value.source !== 'invite_fragment'
      || !['undecided', 'email_intent', 'yandex_intent', 'skipped'].includes(String(value.identityChoice))
      || !Number.isFinite(value.createdAt)
      || !Number.isFinite(value.expiresAt)
      || Number(value.createdAt) > now
      || Number(value.expiresAt) <= now
      || (value.joinedAt !== null && !Number.isFinite(value.joinedAt))
      || (value.status === 'active' && value.joinedAt === null)
      || (value.status === 'joining' && value.joinedAt !== null)
      || (value.joinedAt !== null && Number(value.joinedAt) < Number(value.createdAt))
      || Number(value.expiresAt) - Number(
        value.status === 'active' ? value.joinedAt : value.createdAt,
      ) > FOCUS_PARTICIPATION_DURATION_MS
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
): FocusParticipationMarker | null {
  try {
    const marker = createFocusParticipationMarker(now);
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
    return null;
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
      expiresAt: now + FOCUS_PARTICIPATION_DURATION_MS,
    };
    storage.setItem(
      FOCUS_PARTICIPATION_STORAGE_KEY,
      serializeFocusParticipationMarker(activated),
    );
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

export function readFocusContinuingConsent(
  storage: FocusStorage,
): FocusContinuingConsent | null {
  try {
    const raw = storage.getItem(FOCUS_CONTINUING_CONSENT_STORAGE_KEY);
    if (!raw || new TextEncoder().encode(raw).byteLength > FOCUS_CONTINUING_CONSENT_MAX_BYTES) {
      storage.removeItem(FOCUS_CONTINUING_CONSENT_STORAGE_KEY);
      return null;
    }
    const value = JSON.parse(raw) as Partial<FocusContinuingConsent>;
    if (
      value.version !== FOCUS_CONTINUING_CONSENT_VERSION
      || value.kind !== 'focus_continuing_consent'
      || !Number.isFinite(value.acceptedAt)
      || !Array.isArray(value.purposes)
      || value.purposes.join('|') !== 'focus_updates|prize_result|service_updates'
    ) {
      storage.removeItem(FOCUS_CONTINUING_CONSENT_STORAGE_KEY);
      return null;
    }
    return value as FocusContinuingConsent;
  } catch {
    try {
      storage.removeItem(FOCUS_CONTINUING_CONSENT_STORAGE_KEY);
    } catch {
      // The preference remains unchanged when browser storage is unavailable.
    }
    return null;
  }
}

export function clearFocusContinuingConsent(storage: FocusStorage): void {
  try {
    storage.removeItem(FOCUS_CONTINUING_CONSENT_STORAGE_KEY);
  } catch {
    // An operator reset remains best-effort when browser storage is unavailable.
  }
}

export function setFocusContinuingConsent(
  storage: FocusStorage,
  accepted: boolean,
  now = Date.now(),
): FocusContinuingConsent | null {
  try {
    if (!accepted) {
      storage.removeItem(FOCUS_CONTINUING_CONSENT_STORAGE_KEY);
      return null;
    }
    const consent: FocusContinuingConsent = {
      version: FOCUS_CONTINUING_CONSENT_VERSION,
      kind: 'focus_continuing_consent',
      acceptedAt: now,
      purposes: ['focus_updates', 'prize_result', 'service_updates'],
    };
    const raw = JSON.stringify(consent);
    if (new TextEncoder().encode(raw).byteLength > FOCUS_CONTINUING_CONSENT_MAX_BYTES) {
      return null;
    }
    storage.setItem(FOCUS_CONTINUING_CONSENT_STORAGE_KEY, raw);
    return consent;
  } catch {
    return null;
  }
}
