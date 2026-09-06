import { DatabaseSync } from 'node:sqlite';
import { createHash, randomUUID } from 'node:crypto';
import { chmodSync, closeSync, constants, existsSync, fsyncSync, lstatSync, mkdirSync, openSync, realpathSync } from 'node:fs';
import { dirname, isAbsolute, resolve } from 'node:path';

// Private, single-host preview receipts only. Not a quota ledger or a replica of
// Supabase. Authenticate/validate eligibility before binding the owner argument.
// node:sqlite Node 22 API; WAL + synchronous=FULL commits precede dispatch.
export const RECEIPT_LIMITS = Object.freeze({payloadBytes: 65536, outcomeBytes: 2097152,
  partBytes: 786432, audioBytes: 32 * 1024 * 1024, databaseBytes: 512 * 1024 * 1024,
  operations: 10000, ownerOperations: 1000});
const staleMs = 300000;
const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;
const digestPattern = /^[a-f0-9]{64}$/;
export class ReceiptStoreError extends Error {
  constructor(code, status = 503) { super(code); this.name = 'ReceiptStoreError'; this.code = code; this.status = status; }
}
const fail = (code, status) => { throw new ReceiptStoreError(code, status); };
const identity = value => { if (typeof value !== 'string' || !uuid.test(value)) fail('invalid_id', 400); return value.toLowerCase(); };
// JSONB equality ignores object key order, while array order remains significant.
function canonical(value, limit) {
  let json;
  try {
    json = JSON.stringify(value);
    if (typeof json !== 'string') fail('invalid_json', 400);
    if (Buffer.byteLength(json) > limit) fail('receipt_capacity', 413);
    const sort = value => Array.isArray(value) ? value.map(sort) : value && typeof value === 'object'
      ? Object.fromEntries(Object.keys(value).sort().map(key => [key, sort(value[key])])) : value;
    return JSON.stringify(sort(JSON.parse(json)));
  } catch (error) { if (error instanceof ReceiptStoreError) throw error; fail('invalid_json', 400); }
}
function output(row) {
  if (!row) return null;
  const {claim_id: _claim, ...rest} = row;
  return {...rest, dispatched: Boolean(row.dispatched), payload: JSON.parse(row.payload),
    outcome: row.outcome === null ? null : JSON.parse(row.outcome)};
}
function blob(bytes, digest, limit) {
  if (!(bytes instanceof Uint8Array) || !bytes.byteLength || bytes.byteLength > limit) fail('audio_capacity', 413);
  const copy = Buffer.from(bytes);
  if (typeof digest !== 'string' || !digestPattern.test(digest) || createHash('sha256').update(copy).digest('hex') !== digest)
    fail('audio_digest_mismatch', 409);
  return copy;
}
function privateFile(path) {
  if (!existsSync(path)) return;
  const stat = lstatSync(path);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.nlink !== 1) fail('unsafe_receipt_path');
  chmodSync(path, 0o600);
}

export class DevCoveerReceiptStore {
  #db;
  #filename;
  #limits;
  constructor(filename, limits = {}) {
    if (typeof filename !== 'string' || !isAbsolute(filename)) fail('absolute_receipt_path_required');
    this.#filename = resolve(filename);
    const root = dirname(this.#filename);
    // Use a dedicated private root, never /tmp, HOME, or another shared directory.
    if (root === '/' || root === '/tmp' || root === process.env.HOME) fail('private_receipt_root_required');
    mkdirSync(root, {recursive: true, mode: 0o700});
    if (realpathSync(root) !== root || !lstatSync(root).isDirectory()) fail('unsafe_receipt_path');
    chmodSync(root, 0o700);
    this.#limits = {...RECEIPT_LIMITS};
    for (const [key, value] of Object.entries(limits)) {
      if (!(key in RECEIPT_LIMITS) || !Number.isSafeInteger(value) || value < 1 || value > RECEIPT_LIMITS[key]) fail('invalid_receipt_limit');
      this.#limits[key] = value;
    }
    try {
      privateFile(this.#filename);
      const fd = openSync(this.#filename, constants.O_CREAT | constants.O_RDWR | constants.O_NOFOLLOW, 0o600);
      fsyncSync(fd); closeSync(fd);
      for (const suffix of ['-wal', '-shm', '-journal']) privateFile(`${this.#filename}${suffix}`);
      this.#db = new DatabaseSync(this.#filename);
      this.#db.exec('PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=128; PRAGMA journal_size_limit=8388608;');
      const pageSize = this.#db.prepare('PRAGMA page_size').get().page_size;
      const maxPages = Math.floor(this.#limits.databaseBytes / pageSize);
      if (maxPages < 16) fail('invalid_receipt_limit');
      const actual = this.#db.prepare(`PRAGMA max_page_count=${maxPages}`).get().max_page_count;
      if (actual > maxPages) fail('receipt_capacity', 507);
      this.#db.exec(`
        CREATE TABLE IF NOT EXISTS operations (
          id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('asr','interpret','search')),
          payload TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'accepted' CHECK(state IN ('accepted','processing','completed','failed','outcome_unknown')),
          claim_id TEXT, dispatched INTEGER NOT NULL DEFAULT 0 CHECK(dispatched IN (0,1)),
          outcome TEXT, error_code TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        ) STRICT;
        CREATE INDEX IF NOT EXISTS owner_history ON operations(owner_id,created_at DESC,id DESC);
        CREATE TABLE IF NOT EXISTS audio_parts (
          operation_id TEXT NOT NULL REFERENCES operations(id), part_index INTEGER NOT NULL,
          first_frame INTEGER NOT NULL, frame_count INTEGER NOT NULL, sample_rate INTEGER NOT NULL,
          digest TEXT NOT NULL, audio BLOB NOT NULL, PRIMARY KEY(operation_id,part_index)
        ) STRICT;
        CREATE TABLE IF NOT EXISTS media (
          operation_id TEXT PRIMARY KEY REFERENCES operations(id), mime_type TEXT NOT NULL,
          digest TEXT NOT NULL, audio BLOB NOT NULL
        ) STRICT;
      `);
      for (const suffix of ['', '-wal', '-shm']) privateFile(`${this.#filename}${suffix}`);
      const dirfd = openSync(root, constants.O_RDONLY); fsyncSync(dirfd); closeSync(dirfd);
    } catch (error) { this.#db?.close(); this.#db = undefined; if (error instanceof ReceiptStoreError) throw error; fail('receipt_store_unavailable'); }
  }
  #transaction(work) {
    if (!this.#db) fail('receipt_store_unavailable');
    try {
      this.#db.exec('BEGIN IMMEDIATE');
      const result = work();
      this.#db.exec('COMMIT');
      return result;
    } catch (error) {
      if (this.#db.isTransaction) this.#db.exec('ROLLBACK');
      if (error instanceof ReceiptStoreError) throw error;
      fail('receipt_store_unavailable');
    }
  }
  #row(owner, id) {
    if (!this.#db) fail('receipt_store_unavailable');
    return this.#db.prepare('SELECT * FROM operations WHERE owner_id=? AND id=?').get(identity(owner), identity(id));
  }
  #required(owner, id) { const row = this.#row(owner, id); if (!row) fail('operation_not_found', 404); return row; }
  async admit(owner, id, kind, payload) {
    owner = identity(owner); id = identity(id);
    if (!['asr', 'interpret', 'search'].includes(kind)) fail('invalid_operation', 400);
    const json = canonical(payload, this.#limits.payloadBytes);
    return this.#transaction(() => {
      const old = this.#db.prepare('SELECT * FROM operations WHERE id=?').get(id);
      if (old) {
        if (old.owner_id !== owner) fail('operation_not_found', 404);
        if (old.kind !== kind || old.payload !== json) fail('payload_conflict', 409);
        return output(old);
      }
      const count = this.#db.prepare('SELECT COUNT(*) AS total, COUNT(CASE WHEN owner_id=? THEN 1 END) AS own FROM operations').get(owner);
      if (count.total >= this.#limits.operations || count.own >= this.#limits.ownerOperations) fail('receipt_capacity', 507);
      const now = new Date().toISOString();
      this.#db.prepare('INSERT INTO operations(id,owner_id,kind,payload,created_at,updated_at) VALUES(?,?,?,?,?,?)').run(id, owner, kind, json, now, now);
      return output(this.#required(owner, id));
    });
  }
  async get(owner, id) { return output(this.#row(owner, id)); }
  async history(owner, before) {
    owner = identity(owner);
    let date = null, id = null;
    if (before !== undefined) {
      const pair = typeof before === 'string' ? before.split('|') : [];
      if (pair.length !== 2 || !Number.isFinite(Date.parse(pair[0]))) fail('invalid_cursor', 400);
      date = new Date(pair[0]).toISOString(); id = identity(pair[1]);
    }
    if (!this.#db) fail('receipt_store_unavailable');
    return this.#db.prepare(`SELECT * FROM operations WHERE owner_id=? AND kind='search' AND state='completed'
      AND (? IS NULL OR created_at<? OR (created_at=? AND id<?)) ORDER BY created_at DESC,id DESC LIMIT 20`)
      .all(owner, date, date, date, id).map(output);
  }
  async claim(owner, id) {
    return this.#transaction(() => {
      const row = this.#required(owner, id);
      const now = new Date().toISOString();
      if (row.state === 'processing' && Date.now() - Date.parse(row.updated_at) > staleMs) {
        row.state = row.dispatched ? 'outcome_unknown' : 'accepted';
        this.#db.prepare('UPDATE operations SET state=?,claim_id=NULL,updated_at=? WHERE id=?').run(row.state, now, row.id);
      }
      if (row.state !== 'accepted') return {claimed: false, state: row.state};
      const claim = randomUUID();
      this.#db.prepare("UPDATE operations SET state='processing',claim_id=?,updated_at=? WHERE id=?").run(claim, now, row.id);
      return {claimed: true, claim_id: claim};
    });
  }
  async checkpoint(owner, id, claim, state, outcome = null, error = undefined) {
    if (!['dispatched', 'completed', 'failed', 'outcome_unknown', 'accepted'].includes(state) ||
      (error != null && (typeof error !== 'string' || !/^[a-z0-9_]{1,80}$/.test(error)))) fail('invalid_manifest', 400);
    const json = state === 'completed' ? canonical(outcome, this.#limits.outcomeBytes) : null;
    return this.#transaction(() => {
      const row = this.#row(owner, id);
      if (!row || row.claim_id !== claim || row.state !== 'processing' || (state === 'accepted' && row.dispatched)) fail('revision_conflict', 409);
      this.#db.prepare('UPDATE operations SET state=?,dispatched=?,outcome=?,error_code=?,updated_at=? WHERE id=?')
        .run(state === 'dispatched' ? 'processing' : state, state === 'dispatched' ? 1 : row.dispatched,
          state === 'completed' ? json : row.outcome, error ?? null, new Date().toISOString(), row.id);
    });
  }
  async accounted(owner, id, claim, outcome) {
    return this.#transaction(() => {
      const row = this.#row(owner, id);
      if (!row || row.claim_id !== claim || row.state !== 'completed') fail('accounting_checkpoint_pending');
      const stored = JSON.parse(row.outcome);
      const clear = value => ({...value, accounting: value?.accounting ? {...value.accounting, pending: false} : null});
      const json = canonical(clear(stored), this.#limits.outcomeBytes);
      // Accounting can only clear the pending flag, never replace a terminal result.
      if (json !== canonical(clear(outcome), this.#limits.outcomeBytes)) fail('accounting_checkpoint_pending');
      this.#db.prepare('UPDATE operations SET outcome=? WHERE id=?').run(json, row.id);
    });
  }
  async putAudio(owner, id, part) {
    return this.#transaction(() => {
      const row = this.#required(owner, id);
      if (row.kind !== 'asr') fail('operation_not_found', 404);
      if (!part || !Number.isInteger(part.index) || part.index < 0 || part.index > 255 ||
        !Number.isSafeInteger(part.firstFrame) || part.firstFrame < 0 || !Number.isSafeInteger(part.frameCount) || part.frameCount < 1 ||
        !Number.isInteger(part.sampleRate) || part.sampleRate < 8000 || part.sampleRate > 96000) fail('invalid_manifest', 400);
      const bytes = blob(part.bytes, part.digest, this.#limits.partBytes);
      if (bytes.length !== part.frameCount * 2) fail('invalid_manifest', 400);
      const old = this.#db.prepare('SELECT * FROM audio_parts WHERE operation_id=? AND part_index=?').get(row.id, part.index);
      if (old) {
        if (old.digest !== part.digest || !Buffer.from(old.audio).equals(bytes) || old.first_frame !== part.firstFrame ||
          old.frame_count !== part.frameCount || old.sample_rate !== part.sampleRate) fail('payload_conflict', 409);
        return;
      }
      const manifest = JSON.parse(row.payload);
      if (row.state !== 'accepted' || manifest.codec || !Number.isInteger(manifest.partCount) || part.index >= manifest.partCount ||
        part.sampleRate !== manifest.sampleRate || !Number.isSafeInteger(manifest.frames) || part.firstFrame + part.frameCount > manifest.frames)
        fail('invalid_manifest', 400);
      const total = this.#db.prepare('SELECT coalesce(SUM(length(audio)),0) AS size FROM audio_parts WHERE operation_id=?').get(row.id).size;
      if (total + bytes.length > this.#limits.audioBytes) fail('audio_capacity', 413);
      this.#db.prepare('INSERT INTO audio_parts VALUES(?,?,?,?,?,?,?)').run(row.id, part.index, part.firstFrame, part.frameCount, part.sampleRate, part.digest, bytes);
    });
  }
  async audio(owner, id) {
    const row = this.#required(owner, id);
    return this.#db.prepare('SELECT * FROM audio_parts WHERE operation_id=? ORDER BY part_index').all(row.id).map(part => ({
      index: part.part_index, firstFrame: part.first_frame, frameCount: part.frame_count, sampleRate: part.sample_rate,
      digest: part.digest, bytes: new Uint8Array(part.audio)}));
  }
  async putMedia(owner, id, part) {
    return this.#transaction(() => {
      const row = this.#required(owner, id);
      if (row.kind !== 'asr') fail('operation_not_found', 404);
      if (!part || typeof part.mimeType !== 'string' || part.mimeType.length > 128 || !/^audio\/[a-z0-9.+-]+(?:;[a-z0-9= .,+-]+)*$/i.test(part.mimeType)) fail('invalid_manifest', 400);
      const bytes = blob(part.bytes, part.digest, this.#limits.audioBytes);
      const old = this.#db.prepare('SELECT * FROM media WHERE operation_id=?').get(row.id);
      if (old) {
        if (old.mime_type !== part.mimeType || old.digest !== part.digest || !Buffer.from(old.audio).equals(bytes)) fail('payload_conflict', 409);
        return;
      }
      const manifest = JSON.parse(row.payload);
      if (row.state !== 'accepted' || typeof manifest.codec !== 'string' || !manifest.codec || manifest.partCount !== 1 ||
        this.#db.prepare('SELECT 1 FROM audio_parts WHERE operation_id=? LIMIT 1').get(row.id)) fail('invalid_manifest', 400);
      this.#db.prepare('INSERT INTO media VALUES(?,?,?,?)').run(row.id, part.mimeType, part.digest, bytes);
    });
  }
  async media(owner, id) {
    const row = this.#required(owner, id);
    const part = this.#db.prepare('SELECT * FROM media WHERE operation_id=?').get(row.id);
    return part ? {mimeType: part.mime_type, digest: part.digest, bytes: new Uint8Array(part.audio)} : null;
  }
  close() { if (this.#db) { this.#db.close(); this.#db = undefined; } }
}
