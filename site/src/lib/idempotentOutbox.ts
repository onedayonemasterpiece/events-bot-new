export interface IdempotentOutboxRecord {
  id: string;
  channel: string;
  payload: unknown;
  createdAt: number;
  expiresAt: number;
  attempts: number;
}

export interface IdempotentOutboxConfig {
  indexedDBRef?: IDBFactory | null;
  storage?: Pick<Storage, 'getItem' | 'setItem' | 'removeItem'> | null;
  now?: () => number;
  maxEntries?: number;
  maxBytes?: number;
  ttlMs?: number;
}

const DB_NAME = 'ke-resilient-outbox-v1';
const STORE_NAME = 'events';
const FALLBACK_KEY = 'ke_idempotent_outbox_v1';
const DEFAULT_MAX_ENTRIES = 16;
const DEFAULT_MAX_BYTES = 12 * 1024;
const DEFAULT_TTL_MS = 24 * 60 * 60 * 1000;

function browserStorage(): Pick<Storage, 'getItem' | 'setItem' | 'removeItem'> | null {
  try { return typeof window === 'undefined' ? null : window.localStorage; } catch { return null; }
}

function browserIndexedDb(): IDBFactory | null {
  try { return typeof indexedDB === 'undefined' ? null : indexedDB; } catch { return null; }
}

function byteLength(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength;
}

function validRecord(value: unknown, now: number): value is IdempotentOutboxRecord {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const item = value as Partial<IdempotentOutboxRecord>;
  return /^[a-z0-9][a-z0-9._:-]{7,159}$/iu.test(String(item.id || ''))
    && /^[a-z0-9][a-z0-9._:-]{1,79}$/iu.test(String(item.channel || ''))
    && Number.isFinite(item.createdAt)
    && Number.isFinite(item.expiresAt)
    && Number(item.expiresAt) > now
    && Number.isInteger(item.attempts)
    && Number(item.attempts) >= 0
    && byteLength(item.payload) <= 4096;
}

function compact(records: IdempotentOutboxRecord[], now: number, maxEntries: number, maxBytes: number) {
  const unique = new Map<string, IdempotentOutboxRecord>();
  for (const item of records) {
    if (validRecord(item, now) && item.attempts < 5) unique.set(item.id, item);
  }
  const kept = [...unique.values()].sort((left, right) => left.createdAt - right.createdAt).slice(-maxEntries);
  while (kept.length && byteLength({ v: 1, e: kept }) > maxBytes) kept.shift();
  return kept;
}

function openDatabase(factory: IDBFactory): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = factory.open(DB_NAME, 1);
    request.onupgradeneeded = () => {
      if (!request.result.objectStoreNames.contains(STORE_NAME)) request.result.createObjectStore(STORE_NAME, { keyPath: 'id' });
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error || new Error('outbox_indexeddb_open_failed'));
  });
}

async function idbReadAll(factory: IDBFactory): Promise<IdempotentOutboxRecord[]> {
  const db = await openDatabase(factory);
  try {
    return await new Promise((resolve, reject) => {
      const request = db.transaction(STORE_NAME, 'readonly').objectStore(STORE_NAME).getAll();
      request.onsuccess = () => resolve(request.result as IdempotentOutboxRecord[]);
      request.onerror = () => reject(request.error || new Error('outbox_indexeddb_read_failed'));
    });
  } finally { db.close(); }
}

async function idbReplace(factory: IDBFactory, records: IdempotentOutboxRecord[]): Promise<void> {
  const db = await openDatabase(factory);
  try {
    await new Promise<void>((resolve, reject) => {
      const tx = db.transaction(STORE_NAME, 'readwrite');
      const store = tx.objectStore(STORE_NAME);
      store.clear();
      records.forEach((item) => store.put(item));
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error || new Error('outbox_indexeddb_write_failed'));
      tx.onabort = () => reject(tx.error || new Error('outbox_indexeddb_write_aborted'));
    });
  } finally { db.close(); }
}

export class BoundedIdempotentOutbox {
  private readonly indexedDBRef: IDBFactory | null;
  private readonly storage: Pick<Storage, 'getItem' | 'setItem' | 'removeItem'> | null;
  private readonly now: () => number;
  private readonly maxEntries: number;
  private readonly maxBytes: number;
  private readonly ttlMs: number;
  private serial: Promise<unknown> = Promise.resolve();

  constructor(config: IdempotentOutboxConfig = {}) {
    this.indexedDBRef = config.indexedDBRef === undefined ? browserIndexedDb() : config.indexedDBRef;
    this.storage = config.storage === undefined ? browserStorage() : config.storage;
    this.now = config.now || (() => Date.now());
    this.maxEntries = Math.min(32, Math.max(1, Number(config.maxEntries || DEFAULT_MAX_ENTRIES)));
    this.maxBytes = Math.min(24 * 1024, Math.max(4096, Number(config.maxBytes || DEFAULT_MAX_BYTES)));
    this.ttlMs = Math.min(7 * DEFAULT_TTL_MS, Math.max(60_000, Number(config.ttlMs || DEFAULT_TTL_MS)));
  }

  private fallbackRead(): IdempotentOutboxRecord[] {
    try {
      const parsed = JSON.parse(this.storage?.getItem(FALLBACK_KEY) || 'null');
      if (parsed?.v !== 1 || !Array.isArray(parsed.e)) return [];
      return compact(parsed.e, this.now(), this.maxEntries, this.maxBytes);
    } catch { return []; }
  }

  private fallbackWrite(records: IdempotentOutboxRecord[]): void {
    try {
      const next = compact(records, this.now(), this.maxEntries, this.maxBytes);
      if (next.length) this.storage?.setItem(FALLBACK_KEY, JSON.stringify({ v: 1, e: next }));
      else this.storage?.removeItem(FALLBACK_KEY);
    } catch {
      // Quota/private mode drops disposable telemetry rather than blocking UI.
    }
  }

  private async read(): Promise<IdempotentOutboxRecord[]> {
    if (this.indexedDBRef) {
      try { return compact(await idbReadAll(this.indexedDBRef), this.now(), this.maxEntries, this.maxBytes); }
      catch { /* fall through */ }
    }
    return this.fallbackRead();
  }

  private async write(records: IdempotentOutboxRecord[]): Promise<void> {
    const next = compact(records, this.now(), this.maxEntries, this.maxBytes);
    if (this.indexedDBRef) {
      try {
        await idbReplace(this.indexedDBRef, next);
        this.fallbackWrite([]);
        return;
      } catch { /* compact fallback below */ }
    }
    this.fallbackWrite(next);
  }

  enqueue(input: { id: string; channel: string; payload: unknown }): Promise<boolean> {
    const task = this.serial.then(async () => {
      const now = this.now();
      const record: IdempotentOutboxRecord = {
        id: String(input.id || '').slice(0, 160),
        channel: String(input.channel || '').slice(0, 80),
        payload: input.payload,
        createdAt: now,
        expiresAt: now + this.ttlMs,
        attempts: 0,
      };
      if (!validRecord(record, now)) return false;
      const records = await this.read();
      records.push(record);
      await this.write(records);
      return true;
    });
    this.serial = task.catch(() => undefined);
    return task;
  }

  flush(sender: (record: IdempotentOutboxRecord) => Promise<'sent' | 'retry' | 'drop' | 'skip'>): Promise<number> {
    const task = this.serial.then(async () => {
      const records = await this.read();
      const remaining: IdempotentOutboxRecord[] = [];
      let sent = 0;
      for (const record of records) {
        let outcome: 'sent' | 'retry' | 'drop' | 'skip' = 'retry';
        try { outcome = await sender(record); } catch { outcome = 'retry'; }
        if (outcome === 'sent') sent += 1;
        else if (outcome === 'retry') remaining.push({ ...record, attempts: record.attempts + 1 });
        else if (outcome === 'skip') remaining.push(record);
      }
      await this.write(remaining);
      return sent;
    });
    this.serial = task.catch(() => undefined);
    return task;
  }

  inspect(): Promise<IdempotentOutboxRecord[]> {
    return this.serial.then(() => this.read());
  }
}

let shared: BoundedIdempotentOutbox | null = null;

export function getIdempotentOutbox(): BoundedIdempotentOutbox {
  if (!shared) shared = new BoundedIdempotentOutbox();
  return shared;
}
