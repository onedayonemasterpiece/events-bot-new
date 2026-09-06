import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { spawnSync } from 'node:child_process';
import { DevCoveerReceiptStore, RECEIPT_LIMITS } from '../../scripts/voice/receiptStore.mjs';

const owner = randomUUID(), other = randomUUID();
const code = expected => error => error.code === expected;
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const pcm = (overrides = {}) => { const bytes = new Uint8Array([0, 0, 1, 0]); return {index: 0, firstFrame: 0, frameCount: 2, sampleRate: 16000, bytes, digest: sha(bytes), ...overrides}; };
function fixture(t, limits) {
  const root = mkdtempSync(join(tmpdir(), 'voice-receipts-'));
  const filename = join(root, 'private', 'receipts.sqlite');
  const store = new DevCoveerReceiptStore(filename, limits);
  const handles = [store];
  t.after(() => { for (const handle of handles) handle.close(); rmSync(root, {recursive: true, force: true}); });
  return {root, filename, store, reopen: () => { store.close(); const next = new DevCoveerReceiptStore(filename, limits); handles.push(next); return next; },
    connect: () => { const next = new DevCoveerReceiptStore(filename, limits); handles.push(next); return next; },
    sql: work => { const db = new DatabaseSync(filename); try { return work(db); } finally { db.close(); } }};
}
async function completed(store, kind = 'search', who = owner) {
  const id = randomUUID(); await store.admit(who, id, kind, {});
  const claim = await store.claim(who, id);
  const outcome = {result: {title: id, items: []}, accounting: {pending: true, reservation: 'receipt-not-ledger'}};
  await store.checkpoint(who, id, claim.claim_id, 'completed', outcome);
  return {id, claim: claim.claim_id, outcome};
}

test('durable admission, JSONB key-order idempotency, global ID owner isolation and private WAL', async t => {
  const f = fixture(t); const id = randomUUID();
  await f.store.admit(owner, id, 'interpret', {b: 2, a: {z: 1, x: [3, 4]}});
  assert.equal((await f.store.admit(owner, id, 'interpret', {a: {x: [3, 4], z: 1}, b: 2})).state, 'accepted');
  await assert.rejects(f.store.admit(owner, id, 'interpret', {a: 1}), code('payload_conflict'));
  await assert.rejects(f.store.admit(other, id, 'interpret', {}), code('operation_not_found'));
  assert.equal(await f.store.get(other, id), null);
  for (const suffix of ['', '-wal', '-shm']) assert.equal(statSync(f.filename + suffix).mode & 0o777, 0o600);
  assert.equal(statSync(join(f.root, 'private')).mode & 0o777, 0o700);
  assert.equal(f.sql(db => db.prepare('PRAGMA journal_mode').get().journal_mode), 'wal');
  const reopened = f.reopen(); assert.equal((await reopened.get(owner, id)).state, 'accepted');
  assert.equal('claim_id' in await reopened.get(owner, id), false);
});

test('two connections and concurrent requests yield exactly one claim; all checkpoint paths isolate owner', async t => {
  const f = fixture(t), second = f.connect(), id = randomUUID();
  await f.store.admit(owner, id, 'search', {});
  const claims = await Promise.all(Array.from({length: 16}, (_, i) => (i % 2 ? f.store : second).claim(owner, id)));
  assert.equal(claims.filter(c => c.claimed).length, 1);
  const winner = claims.find(c => c.claimed).claim_id;
  await assert.rejects(f.store.claim(other, id), code('operation_not_found'));
  await assert.rejects(f.store.checkpoint(other, id, winner, 'completed', {}), code('revision_conflict'));
  await assert.rejects(f.store.checkpoint(owner, id, randomUUID(), 'dispatched'), code('revision_conflict'));
  await f.store.checkpoint(owner, id, winner, 'dispatched');
  await assert.rejects(f.store.checkpoint(owner, id, winner, 'accepted'), code('revision_conflict'));
  assert.equal((await second.get(owner, id)).dispatched, true);
});

test('stale before-dispatch reclaim fences old worker; stale post-dispatch never replays after reopening', async t => {
  const f = fixture(t); const id = randomUUID();
  await f.store.admit(owner, id, 'asr', {}); const first = await f.store.claim(owner, id);
  const age = () => f.sql(db => db.prepare('UPDATE operations SET updated_at=? WHERE id=?').run('2000-01-01T00:00:00.000Z', id));
  age(); const second = await f.store.claim(owner, id);
  assert.equal(second.claimed, true); assert.notEqual(first.claim_id, second.claim_id);
  await assert.rejects(f.store.checkpoint(owner, id, first.claim_id, 'dispatched'), code('revision_conflict'));
  await f.store.checkpoint(owner, id, second.claim_id, 'dispatched'); age();
  const reopened = f.reopen();
  assert.deepEqual(await reopened.claim(owner, id), {claimed: false, state: 'outcome_unknown'});
  assert.equal((await reopened.get(owner, id)).state, 'outcome_unknown');
  await assert.rejects(reopened.checkpoint(owner, id, second.claim_id, 'completed', {}), code('revision_conflict'));
  assert.equal((await reopened.claim(owner, id)).claimed, false);
});

test('abrupt process exit after committed dispatch preserves receipt in WAL without close', async t => {
  const f = fixture(t); f.store.close(); const id = randomUUID();
  const url = new URL('../../scripts/voice/receiptStore.mjs', import.meta.url).href;
  const script = `import {DevCoveerReceiptStore} from ${JSON.stringify(url)}; const s=new DevCoveerReceiptStore(${JSON.stringify(f.filename)}); await s.admit('${owner}','${id}','search',{}); const c=await s.claim('${owner}','${id}'); await s.checkpoint('${owner}','${id}',c.claim_id,'dispatched'); process.exit(73);`;
  assert.equal(spawnSync(process.execPath, ['--input-type=module', '-e', script], {encoding: 'utf8'}).status, 73);
  const reopened = f.reopen(); assert.equal((await reopened.get(owner, id)).dispatched, true);
  f.sql(db => db.prepare('UPDATE operations SET updated_at=? WHERE id=?').run('2000-01-01T00:00:00.000Z', id));
  assert.equal((await reopened.claim(owner, id)).state, 'outcome_unknown');
});

test('completed is immutable including accounting retries, wrong owners and mismatched results', async t => {
  const f = fixture(t); const {id, claim, outcome} = await completed(f.store);
  await assert.rejects(f.store.checkpoint(owner, id, claim, 'failed'), code('revision_conflict'));
  await assert.rejects(f.store.accounted(other, id, claim, outcome), code('accounting_checkpoint_pending'));
  await assert.rejects(f.store.accounted(owner, id, claim, {...outcome, result: {title: 'rewrite'}}), code('accounting_checkpoint_pending'));
  await f.store.accounted(owner, id, claim, outcome); await f.store.accounted(owner, id, claim, outcome);
  const saved = await f.reopen().get(owner, id);
  assert.equal(saved.state, 'completed'); assert.deepEqual(saved.outcome.result, outcome.result); assert.equal(saved.outcome.accounting.pending, false);
});

test('history pages only completed owned searches, stable timestamp+UUID ties without omissions', async t => {
  const f = fixture(t); const ids = [];
  for (let i = 0; i < 24; i++) ids.push((await completed(f.store)).id);
  await completed(f.store, 'search', other); await completed(f.store, 'asr');
  await f.store.admit(owner, randomUUID(), 'search', {});
  f.sql(db => db.prepare('UPDATE operations SET created_at=?').run('2026-09-06T00:00:00.000Z'));
  const page1 = await f.store.history(owner);
  const last = page1.at(-1); const page2 = await f.store.history(owner, `${last.created_at}|${last.id}`);
  assert.equal(page1.length, 20); assert.equal(page2.length, 4);
  assert.deepEqual([...page1, ...page2].map(r => r.id), ids.sort().reverse());
  assert.equal((await f.store.history(other)).length, 1);
  await assert.rejects(f.store.history(owner, 'bogus|oops'), code('invalid_cursor'));
});

test('PCM blobs survive reopen, exact retries are immutable after completion, manifest/digest conflicts reject', async t => {
  const f = fixture(t), id = randomUUID(), part = pcm();
  await f.store.admit(owner, id, 'asr', {frames: 4, sampleRate: 16000, partCount: 2});
  await f.store.putAudio(owner, id, part); await f.store.putAudio(owner, id, part);
  await assert.rejects(f.store.putAudio(other, id, part), code('operation_not_found'));
  await assert.rejects(f.store.audio(other, id), code('operation_not_found'));
  await assert.rejects(f.store.putAudio(owner, id, {...part, firstFrame: 1}), code('payload_conflict'));
  await assert.rejects(f.store.putAudio(owner, id, {...part, digest: 'a'.repeat(64)}), code('audio_digest_mismatch'));
  await assert.rejects(f.store.putAudio(owner, id, {...part, index: 2}), code('invalid_manifest'));
  await f.store.putAudio(owner, id, {...part, index: 1, firstFrame: 2});
  const claim = await f.store.claim(owner, id); await f.store.checkpoint(owner, id, claim.claim_id, 'completed', {result: {text: 'private'}});
  const reopened = f.reopen(); await reopened.putAudio(owner, id, part);
  const audio = await reopened.audio(owner, id); assert.deepEqual(audio.map(p => p.index), [0, 1]); assert.deepEqual(audio[0].bytes, part.bytes);
  assert.equal(f.sql(db => db.prepare('SELECT typeof(audio) AS type FROM audio_parts LIMIT 1').get().type), 'blob');
});

test('compressed media owner scoping, byte+digest+MIME immutability, late upload rejection and PCM separation', async t => {
  const f = fixture(t), id = randomUUID(), bytes = new Uint8Array([1, 2, 3]);
  const media = {mimeType: 'audio/webm;codecs=opus', digest: sha(bytes), bytes};
  await f.store.admit(owner, id, 'asr', {codec: 'webm-opus', partCount: 1});
  assert.equal(await f.store.media(owner, id), null);
  await f.store.putMedia(owner, id, media); await f.store.putMedia(owner, id, media);
  await assert.rejects(f.store.putMedia(other, id, media), code('operation_not_found'));
  await assert.rejects(f.store.media(other, id), code('operation_not_found'));
  await assert.rejects(f.store.putMedia(owner, id, {...media, mimeType: 'audio/ogg'}), code('payload_conflict'));
  const changed = new Uint8Array([3, 2, 1]);
  await assert.rejects(f.store.putMedia(owner, id, {...media, bytes: changed}), code('audio_digest_mismatch'));
  await assert.rejects(f.store.putMedia(owner, id, {...media, bytes: changed, digest: sha(changed)}), code('payload_conflict'));
  await assert.rejects(f.store.putAudio(owner, id, pcm()), code('invalid_manifest'));
  const claim = await f.store.claim(owner, id); await f.store.checkpoint(owner, id, claim.claim_id, 'completed', {});
  const reopened = f.reopen(); await reopened.putMedia(owner, id, media); assert.deepEqual(await reopened.media(owner, id), media);
  const late = randomUUID(); await reopened.admit(owner, late, 'asr', {codec: 'webm-opus', partCount: 1}); await reopened.claim(owner, late);
  await assert.rejects(reopened.putMedia(owner, late, media), code('invalid_manifest'));
});

test('write failures rollback atomically without losing prior dispatch and release transaction locks', async t => {
  const f = fixture(t), id = randomUUID(); await f.store.admit(owner, id, 'search', {}); const c = await f.store.claim(owner, id);
  await f.store.checkpoint(owner, id, c.claim_id, 'dispatched');
  f.sql(db => db.exec(`CREATE TRIGGER fault BEFORE UPDATE ON operations WHEN NEW.state='completed' BEGIN SELECT RAISE(ABORT, 'injected'); END;`));
  await assert.rejects(f.store.checkpoint(owner, id, c.claim_id, 'completed', {result: {text: 'never logged'}}), code('receipt_store_unavailable'));
  assert.equal((await f.store.get(owner, id)).state, 'processing'); assert.equal((await f.store.get(owner, id)).dispatched, true);
  f.sql(db => db.exec('DROP TRIGGER fault'));
  await f.store.checkpoint(owner, id, c.claim_id, 'completed', {result: {text: 'saved'}});
  assert.equal((await f.reopen().get(owner, id)).outcome.result.text, 'saved');
});

test('bounded payload/outcome/audio/operation count, fail-closed paths and idempotency at capacity', async t => {
  const f = fixture(t, {ownerOperations: 1, operations: 2, audioBytes: 4}); const id = randomUUID();
  await f.store.admit(owner, id, 'asr', {frames: 4, sampleRate: 16000, partCount: 2});
  await f.store.putAudio(owner, id, pcm());
  await assert.rejects(f.store.putAudio(owner, id, pcm({index: 1, firstFrame: 2})), code('audio_capacity'));
  await assert.rejects(f.store.admit(owner, randomUUID(), 'search', {}), code('receipt_capacity'));
  await f.store.admit(owner, id, 'asr', {frames: 4, sampleRate: 16000, partCount: 2});
  await assert.rejects(f.store.admit(other, randomUUID(), 'search', {text: 'x'.repeat(RECEIPT_LIMITS.payloadBytes)}), code('receipt_capacity'));
  const c = await f.store.claim(owner, id);
  await assert.rejects(f.store.checkpoint(owner, id, c.claim_id, 'completed', {text: 'x'.repeat(RECEIPT_LIMITS.outcomeBytes)}), code('receipt_capacity'));
  assert.equal((await f.store.get(owner, id)).state, 'processing');
  assert.throws(() => new DevCoveerReceiptStore('relative.sqlite'), code('absolute_receipt_path_required'));
  assert.throws(() => new DevCoveerReceiptStore('/tmp/receipts.sqlite'), code('private_receipt_root_required'));
  const target = join(f.root, 'target'); writeFileSync(target, 'keep'); const link = join(f.root, 'private', 'link.sqlite'); symlinkSync(target, link);
  assert.throws(() => new DevCoveerReceiptStore(link), code('unsafe_receipt_path'));
});
