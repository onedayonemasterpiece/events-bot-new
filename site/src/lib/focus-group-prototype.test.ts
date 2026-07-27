import assert from 'node:assert/strict';
import test from 'node:test';

import {
  FOCUS_PREVIEW_MAX_BYTES,
  FOCUS_PREVIEW_STORAGE_KEY,
  FOCUS_PREVIEW_TTL_MS,
  createFocusPreviewMarker,
  inspectFocusInviteUrl,
  parseFocusPreviewMarker,
  readFocusPreviewMarker,
  serializeFocusPreviewMarker,
  storeFocusPreviewMarker,
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
