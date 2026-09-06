import type { State } from './conversationState.ts';
import type { Command } from './assistantClient.ts';
import type { AudioPart } from './audioSegments.ts';
import type { CaptureReceipt } from './microphoneCapture.ts';
import type { CompressedPart } from './compressedCapture.ts';

export type Recording = { id: string; owner: string; createdAt: string; state: 'recording' | 'saved' | 'partial';
  receipt?: CaptureReceipt; transcript?: string; partCount: number; bytes: number };
export type StoredAnswer = { id: string; owner: string; createdAt: string; payload: Record<string, unknown> };
type CompressedPartRow = CompressedPart & { owner: string; recordingId: string; digest: string };
type PartRow = AudioPart & { owner: string; recordingId: string; digest: string };
const key = (owner: string, id: string) => [owner, id];
function identity(owner: string, id: string): void {
  if (![owner, id].every(value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,128}$/.test(value))) throw new Error('invalid_identity');
}
const digest = async (bytes: Uint8Array) => Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', bytes as BufferSource)), x => x.toString(16).padStart(2, '0')).join('');

/** Local, owner-scoped recovery storage. Audio is NOT placed in the generic
 * transport outbox or localStorage. No logout/upgrade path deletes this DB.
 * Every write resolves on transaction completion, not request success.
 */
export class VoiceStore {
  private db: IDBDatabase;
  constructor(db: IDBDatabase) { this.db = db; }
  static open(): Promise<VoiceStore> {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open('kenigevents-voice-v1', 3);
      request.onupgradeneeded = () => {
        const db = request.result;
        for (const name of ['recordings', 'answers', 'commands']) {
          if (db.objectStoreNames.contains(name)) continue;
          const store = db.createObjectStore(name, { keyPath: ['owner', 'id'] });
          store.createIndex('owner_created', ['owner', 'createdAt', 'id']);
        }
        if (!db.objectStoreNames.contains('parts')) db.createObjectStore('parts', { keyPath: ['owner', 'recordingId', 'index'] });
        if (!db.objectStoreNames.contains('compressedParts')) db.createObjectStore('compressedParts', { keyPath: ['owner', 'recordingId', 'index'] });
        if (!db.objectStoreNames.contains('conversations')) db.createObjectStore('conversations', { keyPath: 'owner' });
      };
      request.onblocked = () => reject(new Error('voice_storage_upgrade_blocked'));
      request.onerror = () => reject(new Error('voice_storage_unavailable'));
      request.onsuccess = () => {
        request.result.onversionchange = () => request.result.close();
        resolve(new VoiceStore(request.result));
      };
    });
  }
  close(): void { this.db.close(); }
  private write<T>(stores: string[], apply: (tx: IDBTransaction, result: (value: T) => void) => void): Promise<T> {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(stores, 'readwrite', { durability: 'strict' });
      let value: T; let reason: unknown;
      const abort = (error: unknown) => { reason = error; tx.abort(); };
      tx.oncomplete = () => resolve(value);
      tx.onabort = tx.onerror = () => reject(reason || new Error('voice_storage_write_failed'));
      // Keep application validation errors attached to the transaction without
      // throwing from an asynchronous IDB callback into the window error handler.
      tx.addEventListener('voice-abort', (event: Event) => abort((event as CustomEvent).detail));
      try { apply(tx, item => { value = item; }); } catch (error) { abort(error); }
    });
  }
  private abort(tx: IDBTransaction, code: string): void {
    tx.dispatchEvent(new CustomEvent('voice-abort', { detail: new Error(code) }));
  }
  async create(owner: string, id: string, createdAt = new Date().toISOString()): Promise<Recording> {
    identity(owner, id);
    const row: Recording = { owner, id, createdAt, state: 'recording', partCount: 0, bytes: 0 };
    return this.write(['recordings'], (tx, result) => {
      const store = tx.objectStore('recordings'); const get = store.get(key(owner, id));
      get.onsuccess = () => {
        if (get.result) { this.abort(tx, 'recording_exists'); return; }
        store.add(row); result(row);
      };
    });
  }
  async putPart(owner: string, id: string, part: AudioPart): Promise<void> {
    identity(owner, id);
    const hash = await digest(part.bytes);
    const row: PartRow = { ...part, owner, recordingId: id, digest: hash };
    await this.write<void>(['recordings', 'parts'], (tx, result) => {
      const recordings = tx.objectStore('recordings'); const parts = tx.objectStore('parts');
      const getRecording = recordings.get(key(owner, id));
      getRecording.onsuccess = () => {
        const recording: Recording | undefined = getRecording.result;
        if (!recording) { this.abort(tx, 'recording_not_found'); return; }
        const get = parts.get([owner, id, part.index]);
        get.onsuccess = () => {
          const existing: PartRow | undefined = get.result;
          if (existing) {
            if (existing.digest !== hash || existing.firstFrame !== part.firstFrame || existing.frameCount !== part.frameCount || existing.sampleRate !== part.sampleRate) this.abort(tx, 'audio_payload_conflict');
            else result();
            return;
          }
          parts.add(row);
          recordings.put({ ...recording, partCount: recording.partCount + 1, bytes: recording.bytes + part.bytes.byteLength });
          result();
        };
      };
    });
  }
  async putCompressedPart(owner: string, id: string, part: CompressedPart): Promise<void> {
    identity(owner, id);
    if (!Number.isSafeInteger(part.index) || part.index < 0 || !part.bytes.byteLength ||
        !/^audio\/(webm|ogg|mp4)(;[^\r\n]*)?$/.test(part.mimeType)) throw new Error('invalid_compressed_part');
    const hash = await digest(part.bytes);
    const row: CompressedPartRow = { ...part, owner, recordingId: id, digest: hash };
    await this.write<void>(['recordings', 'compressedParts'], (tx, result) => {
      const recordings = tx.objectStore('recordings'); const parts = tx.objectStore('compressedParts');
      const getRecording = recordings.get(key(owner, id));
      getRecording.onsuccess = () => {
        const recording: Recording | undefined = getRecording.result;
        if (!recording) { this.abort(tx, 'recording_not_found'); return; }
        const get = parts.get([owner, id, part.index]);
        get.onsuccess = () => {
          const existing: CompressedPartRow | undefined = get.result;
          if (existing) {
            if (existing.digest !== hash || existing.mimeType !== part.mimeType) this.abort(tx, 'audio_payload_conflict');
            else result();
            return;
          }
          if (recording.receipt?.compressed?.complete) { this.abort(tx, 'compressed_recording_sealed'); return; }
          parts.add(row); result();
        };
      };
    });
  }
  /** Only a fully sealed, contiguous original container is usable. An abrupt
   * reload retains fragments and PCM, but never presents fragments as a file. */
  async compressed(owner: string, id: string): Promise<{ mimeType: string; bytes: Uint8Array } | null> {
    identity(owner, id);
    const { recording, parts } = await new Promise<{ recording?: Recording; parts: CompressedPartRow[] }>((resolve, reject) => {
      const tx = this.db.transaction(['recordings', 'compressedParts']);
      const recording = tx.objectStore('recordings').get(key(owner, id));
      const parts = tx.objectStore('compressedParts').getAll(IDBKeyRange.bound([owner, id, 0], [owner, id, Number.MAX_SAFE_INTEGER]));
      tx.oncomplete = () => resolve({ recording: recording.result, parts: parts.result });
      tx.onabort = tx.onerror = () => reject(new Error('voice_storage_read_failed'));
    });
    const seal = recording?.receipt?.compressed;
    if (!seal?.complete || !parts.length || parts.length !== seal.partCount ||
        parts.some((part, index) => part.index !== index || part.mimeType !== seal.mimeType) ||
        parts.reduce((sum, part) => sum + part.bytes.byteLength, 0) !== seal.bytes) return null;
    for (const part of parts) if (await digest(part.bytes) !== part.digest) return null;
    const bytes = new Uint8Array(seal.bytes); let offset = 0;
    for (const part of parts) { bytes.set(part.bytes, offset); offset += part.bytes.byteLength; }
    return { mimeType: seal.mimeType, bytes };
  }
  async finish(owner: string, id: string, receipt: CaptureReceipt): Promise<void> {
    await this.patchRecording(owner, id, row => ({ ...row, receipt,
      state: receipt.complete && receipt.savedFrames === receipt.frames && row.partCount === receipt.partCount ? 'saved' : 'partial' }));
  }
  async setTranscript(owner: string, id: string, transcript: string): Promise<void> {
    if (typeof transcript !== 'string' || transcript.length > 65536) throw new Error('invalid_transcript');
    await this.patchRecording(owner, id, row => ({ ...row, transcript }));
  }
  private async patchRecording(owner: string, id: string, patch: (row: Recording) => Recording): Promise<void> {
    identity(owner, id);
    await this.write<void>(['recordings'], (tx, result) => {
      const store = tx.objectStore('recordings'); const get = store.get(key(owner, id));
      get.onsuccess = () => {
        if (!get.result) { this.abort(tx, 'recording_not_found'); return; }
        store.put(patch(get.result)); result();
      };
    });
  }
  async parts(owner: string, id: string): Promise<AudioPart[]> {
    identity(owner, id);
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction('parts');
      const get = tx.objectStore('parts').getAll(IDBKeyRange.bound([owner, id, 0], [owner, id, Number.MAX_SAFE_INTEGER]));
      get.onsuccess = () => resolve(get.result.map(({ owner: _owner, recordingId: _id, digest: _digest, ...part }) => part));
      get.onerror = () => reject(new Error('voice_storage_read_failed'));
    });
  }
  async saveAnswer(owner: string, id: string, payload: Record<string, unknown>): Promise<void> {
    identity(owner, id);
    await this.write<void>(['answers'], (tx, result) => {
      const store = tx.objectStore('answers'); const get = store.get(key(owner, id));
      get.onsuccess = () => {
        if (!get.result) store.add({ id, owner, createdAt: new Date().toISOString(), payload });
        result();
      };
    });
  }
  command(owner:string,id:string):Promise<Command|null> {
    identity(owner,id);return new Promise((resolve,reject)=>{const tx=this.db.transaction('commands');const get=tx.objectStore('commands').get([owner,id]);
      get.onsuccess=()=>resolve(get.result||null);get.onerror=()=>reject(new Error('voice_storage_read_failed'));});
  }
  conversation(owner:string):Promise<State|null> {
    identity(owner,'active');
    return new Promise((resolve,reject)=>{const tx=this.db.transaction('conversations');const request=tx.objectStore('conversations').get(owner);
      request.onsuccess=()=>resolve(request.result?.state||null);request.onerror=()=>reject(new Error('voice_storage_read_failed'));});
  }
  /** Accepted input and the new kernel revision commit atomically. Two tabs
   * cannot overwrite each other; a conflict requires a fresh read. */
  async checkpoint(owner:string,state:State,expectedRevision:number,command?:Command):Promise<void> {
    identity(owner,'active');
    await this.write<void>(['conversations','commands'],(tx,result)=>{
      const store=tx.objectStore('conversations');const get=store.get(owner);
      get.onsuccess=()=>{
        if((get.result?.state?.revision||0)!==expectedRevision){this.abort(tx,'voice_revision_conflict');return;}
        if(command)tx.objectStore('commands').add({...command,owner});
        store.put({owner,state});result();
      };
    });
  }
  /** Stable bounded keyset pagination; loading a page never deletes old rows. */
  page<T extends Recording | StoredAnswer | (Command & {owner:string})>(kind: 'recordings' | 'answers' | 'commands', owner: string, before?: [string, string], limit = 20): Promise<T[]> {
    identity(owner, 'page');
    if (!Number.isInteger(limit) || limit < 1 || limit > 50) throw new Error('invalid_page_size');
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(kind); const index = tx.objectStore(kind).index('owner_created');
      const range = IDBKeyRange.bound([owner, '', ''], [owner, ...(before || ['\uffff', '\uffff'])], false, !!before);
      const request = index.openCursor(range, 'prev'); const rows: T[] = [];
      request.onsuccess = () => {
        const cursor = request.result;
        if (!cursor || rows.length === limit) { resolve(rows); return; }
        rows.push(cursor.value); cursor.continue();
      };
      request.onerror = () => reject(new Error('voice_storage_read_failed'));
    });
  }
}
