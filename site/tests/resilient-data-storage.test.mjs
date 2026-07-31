import assert from 'node:assert/strict';
import test from 'node:test';
import {
  NON_AUTH_STORAGE_BUDGET_BYTES,
  boundedJsonRead,
  boundedJsonWrite,
  cleanupAppStorage,
  compactPersonalizationProfile,
  registeredWorstCaseBudget,
} from '../src/lib/browserStorage.ts';
import { BoundedIdempotentOutbox } from '../src/lib/idempotentOutbox.ts';

function storageFixture(initial = {}) {
  const values = new Map(Object.entries(initial));
  return {
    get length() { return values.size; },
    key(index) { return [...values.keys()][index] || null; },
    getItem(key) { return values.has(key) ? values.get(key) : null; },
    setItem(key, value) { values.set(key, String(value)); },
    removeItem(key) { values.delete(key); },
    values,
  };
}

test('registered worst-case non-auth state remains below the 64 KiB budget', () => {
  assert.ok(registeredWorstCaseBudget() <= NON_AUTH_STORAGE_BUDGET_BYTES);
});

test('cleanup drops expired, oversized and orphaned preview state without touching Supabase Auth', () => {
  const authKey = 'sb-project-auth-token';
  const storage = storageFixture({
    [authKey]: 'x'.repeat(80_000),
    'ke_listing_personal_feed_cache_v1:/preview-old': JSON.stringify({ manifest:{ huge:'x'.repeat(4000) } }),
    'ke_event_feedback_log_v2': JSON.stringify({ expires_at:10, entries:[] }),
    'ke_search_feedback_queue_v2': 'x'.repeat(7000),
  });
  const result = cleanupAppStorage(storage, { now:20, currentBasePath:'/preview-new' });
  assert.equal(storage.getItem(authKey)?.length, 80_000);
  assert.equal(storage.getItem('ke_listing_personal_feed_cache_v1:/preview-old'), null);
  assert.equal(storage.getItem('ke_event_feedback_log_v2'), null);
  assert.equal(storage.getItem('ke_search_feedback_queue_v2'), null);
  assert.ok(result.removed.length >= 3);
});


test('cleanup enforces one aggregate app budget across many individually valid prefix keys', () => {
  const authKey = 'sb-project-auth-token';
  const initial = {
    [authKey]: 'a'.repeat(90_000),
    'kenigevents:focus-participation:v1': JSON.stringify({ version:1, status:'active' }),
  };
  for (let index = 0; index < 40; index += 1) {
    initial[`ke_cache_${String(index).padStart(2, '0')}`] = 'x'.repeat(1_900);
  }
  const storage = storageFixture(initial);
  const result = cleanupAppStorage(storage);
  assert.ok(result.bytes <= NON_AUTH_STORAGE_BUDGET_BYTES);
  assert.ok(result.removed.length > 0);
  assert.notEqual(storage.getItem('kenigevents:focus-participation:v1'), null);
  assert.equal(storage.getItem(authKey)?.length, 90_000);
});

test('profile collections and maps are deterministically capped', () => {
  const ids = Array.from({ length:300 }, (_, index) => index + 1);
  const tags = Object.fromEntries(ids.map((id) => [`tag-${id}`, id / 10]));
  const profile = compactPersonalizationProfile({
    liked_event_ids:ids,
    not_interested_event_ids:ids,
    hidden_event_ids:ids,
    seen_event_ids:ids,
    seen_venue_ids:ids,
    positive_tags:tags,
    negative_interest_tags:tags,
    share_counts:tags,
  });
  assert.equal(profile.liked_event_ids.length, 80);
  assert.equal(profile.not_interested_event_ids.length, 100);
  assert.equal(profile.seen_venue_ids.length, 64);
  assert.equal(Object.keys(profile.positive_tags).length, 48);
  assert.equal(Object.keys(profile.share_counts).length, 64);
});

test('bounded JSON and outbox tolerate corruption, quota failure, TTL and dedupe', async () => {
  const storage = storageFixture({ broken:'{' });
  assert.deepEqual(boundedJsonRead(storage, 'broken', { ok:false }, 100), { ok:false });
  assert.equal(storage.getItem('broken'), null);
  const quota = { setItem() { throw new Error('quota'); }, removeItem() {}, getItem() { return null; } };
  assert.equal(boundedJsonWrite(quota, 'x', { ok:true }, 100), false);

  let now = 1_000;
  const outbox = new BoundedIdempotentOutbox({ indexedDBRef:null, storage, now:() => now, ttlMs:60_000, maxEntries:2, maxBytes:4096 });
  assert.equal(await outbox.enqueue({ id:'event:00000001', channel:'test-v1', payload:{ n:1 } }), true);
  assert.equal(await outbox.enqueue({ id:'event:00000001', channel:'test-v1', payload:{ n:2 } }), true);
  assert.equal((await outbox.inspect()).length, 1);
  const beforeSkip = await outbox.inspect();
  assert.equal(await outbox.flush(async () => 'skip'), 0);
  const afterSkip = await outbox.inspect();
  assert.deepEqual(afterSkip, beforeSkip, 'a channel-safe skip must not burn an attempt');
  let sends = 0;
  assert.equal(await outbox.flush(async () => { sends += 1; return 'sent'; }), 1);
  assert.equal(sends, 1);
  await outbox.enqueue({ id:'event:00000002', channel:'test-v1', payload:{ n:3 } });
  now += 61_000;
  assert.equal((await outbox.inspect()).length, 0);
});
