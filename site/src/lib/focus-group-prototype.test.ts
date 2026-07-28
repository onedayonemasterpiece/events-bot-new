import assert from 'node:assert/strict';
import test from 'node:test';

import {
  FOCUS_PARTICIPATION_DURATION_MS,
  FOCUS_PARTICIPATION_MAX_BYTES,
  FOCUS_PARTICIPATION_STORAGE_KEY,
  activateFocusParticipation,
  clearFocusParticipationMarker,
  inspectFocusInviteUrl,
  parseFocusParticipationMarker,
  readFocusParticipationMarker,
  storeFocusParticipationMarker,
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

test('storage helper removes invalid state and preserves only a valid participation marker', () => {
  const storage = new MemoryStorage();
  const now = Date.UTC(2026, 6, 27, 20, 0, 0);

  storage.setItem(FOCUS_PARTICIPATION_STORAGE_KEY, '{"version":99}');
  assert.equal(readFocusParticipationMarker(storage, now), null);
  assert.equal(storage.getItem(FOCUS_PARTICIPATION_STORAGE_KEY), null);

  assert.ok(storeFocusParticipationMarker(storage, now));
  assert.ok(readFocusParticipationMarker(storage, now + 1));
});

test('programme participation starts a full 30-day access window on activation', () => {
  const storage = new MemoryStorage();
  const now = Date.UTC(2026, 6, 27, 20, 0, 0);
  const pending = storeFocusParticipationMarker(storage, now);

  assert.equal(pending?.status, 'joining');
  assert.equal(pending?.identityChoice, 'undecided');
  assert.ok(
    new TextEncoder().encode(storage.getItem(FOCUS_PARTICIPATION_STORAGE_KEY) || '').byteLength
      <= FOCUS_PARTICIPATION_MAX_BYTES,
  );

  const chosen = updateFocusParticipationIdentityChoice(storage, 'email_intent', now + 10);
  assert.equal(chosen?.identityChoice, 'email_intent');
  const active = activateFocusParticipation(storage, 'email_intent', now + 20);
  assert.equal(active?.status, 'active');
  assert.equal(active?.joinedAt, now + 20);
  assert.equal(active?.expiresAt, now + 20 + FOCUS_PARTICIPATION_DURATION_MS);
  const relaunched = activateFocusParticipation(
    storage,
    'email_intent',
    now + 24 * 60 * 60 * 1000,
  );
  assert.equal(
    relaunched?.expiresAt,
    active?.expiresAt,
    'ordinary PWA relaunch must neither shorten nor silently slide the window',
  );
  assert.ok(
    readFocusParticipationMarker(
      storage,
      now + 20 + FOCUS_PARTICIPATION_DURATION_MS - 1,
    ),
  );
  assert.equal(
    readFocusParticipationMarker(
      storage,
      now + 20 + FOCUS_PARTICIPATION_DURATION_MS,
    ),
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

test('explicit focus exit removes only participation and preserves personalization', () => {
  const storage = new MemoryStorage();
  const now = Date.UTC(2026, 6, 27, 20, 0, 0);
  const personalizationKey = 'kenigevents.focus-personalization.prototype.v1';
  storage.setItem(personalizationKey, '{"consented":true,"interests":["theatre"]}');
  storeFocusParticipationMarker(storage, now);
  activateFocusParticipation(storage, 'skipped', now + 1);

  clearFocusParticipationMarker(storage);

  assert.equal(storage.getItem(FOCUS_PARTICIPATION_STORAGE_KEY), null);
  assert.equal(
    storage.getItem(personalizationKey),
    '{"consented":true,"interests":["theatre"]}',
  );
});

test('participation payload never retains invite, email or identity credentials', () => {
  const storage = new MemoryStorage();
  const now = Date.UTC(2026, 6, 27, 20, 0, 0);
  storeFocusParticipationMarker(storage, now);
  activateFocusParticipation(storage, 'skipped', now + 1);

  const raw = storage.getItem(FOCUS_PARTICIPATION_STORAGE_KEY) || '';
  assert.ok(parseFocusParticipationMarker(raw, now + 2));
  assert.equal(raw.includes('invite='), false);
  assert.equal(raw.includes('@'), false);
  assert.equal(raw.includes('token'), false);
});
