import assert from 'node:assert/strict';
import { webcrypto } from 'node:crypto';
import test from 'node:test';
import { createSiteIdentityController, maskEmail, siteIdentityStorageKeys } from '../../site/src/lib/site-identity.js';

class Storage {
  constructor(seed = {}) { this.map = new Map(Object.entries(seed)); }
  getItem(k) { return this.map.has(k) ? this.map.get(k) : null; }
  setItem(k, v) { this.map.set(k, String(v)); }
  removeItem(k) { this.map.delete(k); }
}
function windowFixture(url = 'https://kenigevents.test/segodnya/') {
  const handlers = new Map();
  return {
    location: { href: url },
    history: { replaceState(_a, _b, next) { this.replaced = next; } },
    addEventListener(kind, fn) { handlers.set(kind, fn); },
    dispatchStorage(key) { handlers.get('storage')?.({ key }); },
  };
}
function supabaseFixture({ user = null } = {}) {
  let session = user ? { access_token: `token-${user.id}`, refresh_token: 'refresh', user } : null;
  let count = 0;
  const calls = [];
  let authListener = null;
  return {
    calls,
    auth: {
      async getSession() { return { data: { session } }; },
      async signInWithOAuth(args) { calls.push(['oauth', args]); return { data: {} }; },
      async signInWithOtp(args) { calls.push(['otp', args]); return { data: {} }; },
      async verifyOtp(args) { calls.push(['verify', args]); return { data: { session }, error: session ? null : new Error('invalid') }; },
      async exchangeCodeForSession(code) { calls.push(['exchange', code]); return { data: { session }, error: session ? null : new Error('invalid') }; },
      async setSession(tokens) { calls.push(['setSession', tokens]); return { data: { session } }; },
      async signOut() { session = null; authListener?.('SIGNED_OUT', null); },
      onAuthStateChange(fn) { authListener = fn; return { data: { subscription: { unsubscribe() {} } } }; },
      switchUser(next) { session = next ? { access_token: `token-${next.id}`, refresh_token: 'r', user: next } : null; authListener?.('SIGNED_IN', session); },
    },
    async rpc(name, args) {
      calls.push(['rpc', name, args]);
      if (name === 'personalization_saved_count_v1') return { data: count, error: null };
      if (name === 'personalization_save_occurrence_v1') {
        count = args.p_saved ? 1 : 0;
        return { data: [{ saved: args.p_saved, unique_saved_event_count: count, lifecycle_status: 'upcoming' }], error: null };
      }
      return { data: true, error: null };
    },
  };
}
function controller({ storage = new Storage(), now = { value: 1_000_000 }, user = null, url, fetch } = {}) {
  const supabase = supabaseFixture({ user });
  const win = windowFixture(url);
  const instance = createSiteIdentityController({ supabase, storage, window: win, crypto: webcrypto, now: () => now.value, identityControlUrl: 'https://example.supabase.co/functions/v1/identity-control', fetch: fetch || globalThis.fetch });
  return { instance, supabase, storage, win, now };
}

test('maskEmail never exposes the full local part', () => assert.equal(maskEmail('Person.Name@Example.test'), 'p***@example.test'));

test('email code and link share one bounded transaction: cooldown, TTL, attempts and replay', async () => {
  const ctx = controller();
  await ctx.instance.requestEmailVerification('USER@example.test');
  assert.equal(ctx.instance.snapshot().rememberedEmailMasked, 'u***@example.test');
  await assert.rejects(() => ctx.instance.requestEmailVerification('user@example.test'), /email_rate_limited/);
  for (let i = 0; i < 5; i += 1) await assert.rejects(() => ctx.instance.verifyEmailCode('111111'), /invalid/);
  await assert.rejects(() => ctx.instance.verifyEmailCode('111111'), /email_attempts_exhausted/);
  const txn = JSON.parse(ctx.storage.getItem(siteIdentityStorageKeys.emailTxn));
  txn.attempts = 0;
  txn.expires_at = ctx.now.value - 1;
  ctx.storage.setItem(siteIdentityStorageKeys.emailTxn, JSON.stringify(txn));
  await assert.rejects(() => ctx.instance.verifyEmailCode('111111'), /email_code_expired/);
  txn.expires_at = ctx.now.value + 100;
  txn.consumed_at = ctx.now.value;
  ctx.storage.setItem(siteIdentityStorageKeys.emailTxn, JSON.stringify(txn));
  await assert.rejects(() => ctx.instance.verifyEmailCode('111111'), /email_code_replayed/);
  const otp = ctx.supabase.calls.find((call) => call[0] === 'otp')[1];
  assert.equal(otp.email, 'user@example.test');
  assert.equal(otp.options.emailRedirectTo, 'https://kenigevents.test/segodnya/');
});

test('remembered email restores on reload and can be forgotten without logout', async () => {
  const storage = new Storage();
  const first = controller({ storage });
  await first.instance.requestEmailVerification('restore@example.test');
  const second = controller({ storage });
  assert.equal(second.instance.snapshot().rememberedEmailMasked, 'r***@example.test');
  second.instance.forgetEmailOnDevice();
  assert.equal(second.instance.snapshot().rememberedEmailMasked, '');
  assert.equal(storage.getItem(siteIdentityStorageKeys.email), null);
});

test('account switch resets count and never carries account A count to account B', async () => {
  const ctx = controller({ user: { id: 'account-a', email: 'a@example.test' } });
  await ctx.instance.init();
  await ctx.instance.saveOccurrence({ eventId: 10, occurrenceKey: '10@2026-07-20', saved: true });
  assert.equal(ctx.instance.snapshot().savedCount, 1);
  ctx.supabase.auth.switchUser({ id: 'account-b', email: 'b@example.test' });
  assert.equal(ctx.instance.snapshot().user.id, 'account-b');
  assert.equal(ctx.instance.snapshot().savedCount, 0);
});

test('save/repeat/undo uses one idempotent occurrence RPC and unique count contract', async () => {
  const ctx = controller({ user: { id: 'account-a' } });
  await ctx.instance.init();
  const one = await ctx.instance.saveOccurrence({ eventId: 42, occurrenceKey: '42@2026-08-01T16:00:00Z', occurrenceStartsAt: '2026-08-01T16:00:00Z' });
  const repeat = await ctx.instance.saveOccurrence({ eventId: 42, occurrenceKey: '42@2026-08-01T16:00:00Z', occurrenceStartsAt: '2026-08-01T16:00:00Z' });
  assert.equal(one.unique_saved_event_count, 1);
  assert.equal(repeat.unique_saved_event_count, 1);
  const undone = await ctx.instance.saveOccurrence({ eventId: 42, occurrenceKey: '42@2026-08-01T16:00:00Z', saved: false });
  assert.equal(undone.unique_saved_event_count, 0);
});

test('consented device merge sends device proof and stable request id without local identity claims', async () => {
  const requests = [];
  const fetch = async (_url, init) => { requests.push(JSON.parse(init.body)); return { ok: true, async json() { return { ok: true, merge: { merge_status: 'merged' } }; } }; };
  const ctx = controller({ user: { id: 'account-a' }, fetch });
  await ctx.instance.init();
  await ctx.instance.materializeAnonymous([{ event_id: 77, occurrence_key: '77@2026-09-01', saved: true }], 'personalization-v1');
  await ctx.instance.mergeAfterAuth('personalization-v1', '11111111-1111-4111-8111-111111111111');
  assert.equal(requests[0].action, 'materialize_device');
  assert.equal(requests[1].action, 'merge_device');
  assert.equal(requests[0].device_id, requests[1].device_id);
  assert.equal(requests[1].request_id, '11111111-1111-4111-8111-111111111111');
  assert.ok(!('user_id' in requests[1]));
});

test('callback URL is cleaned before rendering and a consumed link is rejected as replay', async () => {
  const storage = new Storage({ [siteIdentityStorageKeys.emailTxn]: JSON.stringify({ consumed_at: 99 }) });
  const ctx = controller({ storage, url: 'https://kenigevents.test/vystavki/?code=secret&state=x' });
  await ctx.instance.init();
  assert.equal(ctx.win.history.replaced, 'https://kenigevents.test/vystavki/');
  assert.equal(ctx.instance.snapshot().error, 'auth_callback_replayed');
  assert.equal(ctx.supabase.calls.some((call) => call[0] === 'exchange'), false);
});
