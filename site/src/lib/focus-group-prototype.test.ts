import assert from 'node:assert/strict';
import test from 'node:test';

import {
  FOCUS_PARTICIPATION_MAX_BYTES,
  FOCUS_PARTICIPATION_SAFETY_TTL_MS,
  FOCUS_PARTICIPATION_STORAGE_KEY,
  FOCUS_PREVIEW_MAX_BYTES,
  FOCUS_PREVIEW_STORAGE_KEY,
  FOCUS_PREVIEW_TTL_MS,
  activateFocusParticipation,
  createFocusPreviewMarker,
  inspectFocusInviteUrl,
  parseFocusParticipationMarker,
  parseFocusPreviewMarker,
  readFocusParticipationMarker,
  readFocusPreviewMarker,
  serializeFocusPreviewMarker,
  storeFocusParticipationMarker,
  storeFocusPreviewMarker,
  updateFocusParticipationIdentityChoice,
  type FocusStorage,
} from './focus-group-prototype.ts';

class MemoryStorage implements FocusStorage {
  private readonly values = new Map<string, string>();

  getItem(key: string) {
    return this.values.get(key) ?? null;
  }

  setItem(key: string, value: string) {
    this.values.set(key, value);
  }

  removeItem(key: string) {
    this.values.delete(key);
  }
}

test('invite fragment is accepted without exposing its value and clean URL drops the hash', () => {
  const result = inspectFocusInviteUrl(
    'https://example.test/fokus-gruppa/priglashenie/?campaign=seed#invite=abcdefghijklmnopqrstuvwxyz_ABCDEFG123456',
  );
  assert.deepEqual(result, {
    status: 'accepted',
    cleanHref: '/fokus-gruppa/priglashenie/?campaign=seed',
  });
  assert.equal(JSON.stringify(result).includes('abcdefghijklmnopqrstuvwxyz'), false);
});

test('missing and malformed fragments fail closed and still provide a clean URL', () => {
  assert.deepEqual(
    inspectFocusInviteUrl('https://example.test/fokus-gruppa/priglashenie/'),
    { status: 'missing', cleanHref: '/fokus-gruppa/priglashenie/' },
  );
  assert.deepEqual(
    inspectFocusInviteUrl('https://example.test/fokus-gruppa/priglashenie/#invite=too-short'),
    { status: 'invalid', cleanHref: '/fokus-gruppa/priglashenie/' },
  );
});

test('preview marker is small, contains no invite and expires after 72 hours', () => {
  const now = Date.UTC(2026, 6, 27, 20, 0, 0);
  const marker = createFocusPreviewMarker(now);
  const serialized = serializeFocusPreviewMarker(marker);

  assert.ok(new TextEncoder().encode(serialized).byteLength <= FOCUS_PREVIEW_MAX_BYTES);
  assert.equal(serialized.includes('invite='), false);
  assert.deepEqual(parseFocusPreviewMarker(serialized, now + FOCUS_PREVIEW_TTL_MS - 1), marker);
  assert.equal(parseFocusPreviewMarker(serialized, now + FOCUS_PREVIEW_TTL_MS), null);
});

test('storage helper removes invalid state and preserves only a valid bounded marker', () => {
  const storage = new MemoryStorage();
  const now = Date.UTC(2026, 6, 27, 20, 0, 0);

  storage.setItem(FOCUS_PREVIEW_STORAGE_KEY, '{"version":99}');
  assert.equal(readFocusPreviewMarker(storage, now), null);
  assert.equal(storage.getItem(FOCUS_PREVIEW_STORAGE_KEY), null);

  assert.equal(storeFocusPreviewMarker(storage, now), true);
  assert.ok(readFocusPreviewMarker(storage, now + 1));
});

test('programme participation stays pending until an explicit choice and then survives the research period', () => {
  const storage = new MemoryStorage();
  const now = Date.UTC(2026, 6, 27, 20, 0, 0);
  const pending = storeFocusParticipationMarker(storage, now);

  assert.equal(pending?.status, 'joining');
  assert.equal(pending?.identityChoice, 'undecided');
  assert.equal(readFocusPreviewMarker(storage, now + 1), null);
  assert.ok(
    new TextEncoder().encode(storage.getItem(FOCUS_PARTICIPATION_STORAGE_KEY) || '').byteLength
      <= FOCUS_PARTICIPATION_MAX_BYTES,
  );

  const chosen = updateFocusParticipationIdentityChoice(storage, 'email_intent', now + 10);
  assert.equal(chosen?.identityChoice, 'email_intent');
  const active = activateFocusParticipation(storage, 'email_intent', now + 20);
  assert.equal(active?.status, 'active');
  assert.equal(active?.joinedAt, now + 20);
  assert.ok(readFocusPreviewMarker(storage, now + FOCUS_PREVIEW_TTL_MS + 1));
  assert.ok(readFocusParticipationMarker(storage, now + FOCUS_PARTICIPATION_SAFETY_TTL_MS - 1));
  assert.equal(
    readFocusParticipationMarker(storage, now + FOCUS_PARTICIPATION_SAFETY_TTL_MS),
    null,
  );
});

test('personalization reset cannot remove the independent focus participation marker', () => {
  const storage = new MemoryStorage();
  const now = Date.UTC(2026, 6, 27, 20, 0, 0);
  const personalizationKey = 'kenigevents.focus-personalization.prototype.v1';
  storage.setItem(personalizationKey, '{"consented":true}');
  storeFocusParticipationMarker(storage, now);
  activateFocusParticipation(storage, 'skipped', now + 1);

  storage.removeItem(personalizationKey);

  assert.ok(readFocusParticipationMarker(storage, now + 2));
  const raw = storage.getItem(FOCUS_PARTICIPATION_STORAGE_KEY);
  assert.equal(raw?.includes('invite='), false);
  assert.equal(raw?.includes('@'), false);
});

test('legacy 72-hour preview hint migrates once to programme participation without retaining bearer data', () => {
  const storage = new MemoryStorage();
  const now = Date.UTC(2026, 6, 27, 20, 0, 0);
  storeFocusPreviewMarker(storage, now);

  const migrated = readFocusParticipationMarker(storage, now + 1);

  assert.equal(migrated?.source, 'legacy_preview');
  assert.equal(migrated?.status, 'active');
  assert.equal(storage.getItem(FOCUS_PREVIEW_STORAGE_KEY), null);
  assert.ok(parseFocusParticipationMarker(storage.getItem(FOCUS_PARTICIPATION_STORAGE_KEY), now + 2));
});
