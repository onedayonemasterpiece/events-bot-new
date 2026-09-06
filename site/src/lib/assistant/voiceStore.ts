import { HOME_HANDOFF_VERSION, validateHandoff, type HomeHandoff, type HandoffScope } from './searchHandoff.ts';
import { initialState, resetTask, acceptInput } from './conversationState.ts';
import { voiceTrace, voiceErrorName } from './voiceDiagnostics.ts';
import type { State } from './conversationState.ts';
import type { Command } from './assistantClient.ts';
import type { AudioPart } from './audioSegments.ts';
import type { CaptureReceipt } from './microphoneCapture.ts';
import type { CompressedPart } from './compressedCapture.ts';

export type Recording = { id: string; owner: string; createdAt: string; state: 'recording' | 'saved' | 'partial';
  compressedStorage?: 'parts-v1' | 'compressedParts';
  receipt?: CaptureReceipt; transcript?: string; partCount: number; bytes: number };
export type StoredAnswer = { id: string; owner: string; createdAt: string; payload: Record<string, unknown> };
type CompressedPartRow = CompressedPart & { owner: string; recordingId: string; digest: string };
type CompatibleCompressedRow = CompressedPartRow & { kind: 'compressed-part-v1'; originalIndex: number };
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
let pendingOpen: Promise<VoiceStore> | null = null;
let openAttempt = 0;
export class VoiceStore {
  private db: IDBDatabase;
  constructor(db: IDBDatabase, private scope = '') { this.db = db; }
  private storageOwner(owner:string):string { return this.scope ? `${owner}:${this.scope}` : owner; }
  scoped(scope:string):VoiceStore { if(!/^[a-zA-Z0-9-]{1,48}$/.test(scope))throw Error('voice_scope_invalid');return new VoiceStore(this.db,scope); }
  static open({ timeoutMs = 8000 }: { timeoutMs?: number } = {}): Promise<VoiceStore> {
    if (pendingOpen) { voiceTrace('open_reused', { attempt: openAttempt }); return pendingOpen; }
    const attempt = ++openAttempt;
    voiceTrace('open_requested', { attempt });
    let terminal = false;
    const opening = new Promise<VoiceStore>((resolve, reject) => {
      const trace = (event: string) => voiceTrace(event, { attempt });
      const release = () => { terminal = true; pendingOpen = null; };
      let settled = false;
      const fail = (code: string) => { if (!settled) { settled = true; clearTimeout(timer); reject(new Error(code)); } };
      const timer = setTimeout(() => { trace('open_timeout'); fail('voice_storage_open_timeout'); }, timeoutMs);
      let request: IDBOpenDBRequest;
      try { request = indexedDB.open('kenigevents-voice-v1'); }
      catch (error) { voiceTrace('open_throw', { attempt, error: voiceErrorName(error) }); release(); fail('voice_storage_unavailable'); return; }
      request.onupgradeneeded = event => {
        voiceTrace('open_upgrade', { attempt, oldVersion: event?.oldVersion, newVersion: event?.newVersion });
        request.transaction?.addEventListener?.('complete', () => trace('upgrade_complete'));
        request.transaction?.addEventListener?.('abort', () => trace('upgrade_abort'));
        request.transaction?.addEventListener?.('error', () => trace('upgrade_error'));
        // Open requests cannot be cancelled. A timed-out/blocked attempt must
        // not later retain a connection or run an orphan upgrade after retry.
        if (settled) { trace('late_upgrade_abort'); request.transaction?.abort(); return; }
        const db = request.result;
        // Versionless open only upgrades a brand-new database (oldVersion 0).
        // Existing v2/v3 schemas are never upgraded on the microphone path.
        if (event.oldVersion !== 0) { request.transaction?.abort(); return; }
        for (const name of ['recordings', 'answers', 'commands']) {
          if (db.objectStoreNames.contains(name)) continue;
          const store = db.createObjectStore(name, { keyPath: ['owner', 'id'] });
          store.createIndex('owner_created', ['owner', 'createdAt', 'id']);
        }
        if (!db.objectStoreNames.contains('parts')) db.createObjectStore('parts', { keyPath: ['owner', 'recordingId', 'index'] });
        if (!db.objectStoreNames.contains('compressedParts')) db.createObjectStore('compressedParts', { keyPath: ['owner', 'recordingId', 'index'] });
        if (!db.objectStoreNames.contains('conversations')) db.createObjectStore('conversations', { keyPath: 'owner' });
      };
      request.onblocked = event => { voiceTrace('open_blocked', { attempt, oldVersion: event?.oldVersion, newVersion: event?.newVersion }); fail('voice_storage_upgrade_blocked'); };
      request.onerror = () => { voiceTrace('open_error', { attempt, error: voiceErrorName(request.error) }); release(); fail('voice_storage_unavailable'); };
      request.onsuccess = () => {
        voiceTrace(settled ? 'open_late_success' : 'open_success', { attempt, version: request.result.version }); release();
        if (settled) { request.result.close(); trace('late_connection_closed'); return; }
        try { VoiceStore.validate(request.result); }
        catch { trace('open_schema_unsupported'); request.result.close(); fail('voice_storage_schema_unsupported'); return; }
        settled = true; clearTimeout(timer);
        request.result.onversionchange = event => { voiceTrace('connection_versionchange', { attempt, oldVersion: event?.oldVersion, newVersion: event?.newVersion }); request.result.close(); trace('connection_closed'); };
        resolve(new VoiceStore(request.result));
      };
    });
    // Timeout does not cancel IDBOpenDBRequest. Reuse it until a terminal event
    // instead of adding more opens to the same database's connection queue.
    if (!terminal) pendingOpen = opening;
    return opening;
  }
  private static validate(db: IDBDatabase): void {
    const layouts: Record<string,string | string[]> = {
      recordings:['owner','id'], answers:['owner','id'], commands:['owner','id'],
      parts:['owner','recordingId','index'], conversations:'owner',
    };
    if (db.objectStoreNames.contains('compressedParts')) layouts.compressedParts = ['owner','recordingId','index'];
    if (Object.keys(layouts).some(name => !db.objectStoreNames.contains(name))) throw new Error('voice_storage_schema_unsupported');
    const tx = db.transaction(Object.keys(layouts));
    for (const [name,keyPath] of Object.entries(layouts)) {
      const store = tx.objectStore(name);
      if (JSON.stringify(store.keyPath) !== JSON.stringify(keyPath)) throw new Error('voice_storage_schema_unsupported');
      if (['recordings','answers','commands'].includes(name) &&
          (!store.indexNames.contains('owner_created') || JSON.stringify(store.index('owner_created').keyPath) !== JSON.stringify(['owner','createdAt','id']))) throw new Error('voice_storage_schema_unsupported');
    }
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
    identity(owner, id); owner=this.storageOwner(owner);
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
    identity(owner, id); owner=this.storageOwner(owner);
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
    identity(owner, id); owner=this.storageOwner(owner);
    // -(index+1) must remain a safe integer in the v2-compatible key domain.
    if (!Number.isSafeInteger(part.index) || part.index < 0 || part.index >= Number.MAX_SAFE_INTEGER || !part.bytes.byteLength ||
        !/^audio\/(webm|ogg|mp4)(;[^\r\n]*)?$/.test(part.mimeType)) throw new Error('invalid_compressed_part');
    const hash = await digest(part.bytes);
    const native = this.db.objectStoreNames.contains('compressedParts');
    await this.write<void>(['recordings','parts', ...(native ? ['compressedParts'] : [])], (tx, result) => {
      const recordings = tx.objectStore('recordings');
      const getRecording = recordings.get(key(owner, id));
      getRecording.onsuccess = () => {
        const recording: Recording | undefined = getRecording.result;
        if (!recording) { this.abort(tx, 'recording_not_found'); return; }
        // Pin each recording's format atomically. If another application later
        // upgrades v2 to v3, its existing compatible fragments stay together.
        const layout = recording.compressedStorage || (native ? 'compressedParts' : 'parts-v1');
        if (!['compressedParts','parts-v1'].includes(layout) || layout === 'compressedParts' && !native) { this.abort(tx, 'voice_storage_schema_unsupported'); return; }
        const compatible = layout === 'parts-v1';
        const parts = tx.objectStore(compatible ? 'parts' : 'compressedParts');
        const index = compatible ? -(part.index + 1) : part.index;
        const row: CompressedPartRow | CompatibleCompressedRow = { ...part, owner, recordingId:id, digest:hash,
          ...(compatible ? { index, originalIndex:part.index, kind:'compressed-part-v1' as const } : {}) };
        const get = parts.get([owner,id,index]);
        get.onsuccess = () => {
          const existing = get.result as CompatibleCompressedRow | undefined;
          if (existing) {
            if (existing.digest !== hash || existing.mimeType !== part.mimeType ||
                compatible && (existing.kind !== 'compressed-part-v1' || existing.originalIndex !== part.index)) this.abort(tx, 'audio_payload_conflict');
            else result();
            return;
          }
          if (recording.receipt?.compressed?.complete) { this.abort(tx, 'compressed_recording_sealed'); return; }
          parts.add(row); recordings.put({ ...recording, compressedStorage:layout }); result();
        };
      };
    });
  }
  /** Only a sealed, contiguous original container is usable. Compatible v2
   * rows have negative keys; old and new PCM readers only select nonnegative
   * keys. Existing v3 fragments are neither migrated nor overwritten. */
  async compressed(owner: string, id: string): Promise<{ mimeType: string; bytes: Uint8Array } | null> {
    identity(owner, id); owner=this.storageOwner(owner);
    const native = this.db.objectStoreNames.contains('compressedParts');
    const { recording, parts } = await new Promise<{ recording?: Recording; parts: CompressedPartRow[] }>((resolve, reject) => {
      const tx = this.db.transaction(['recordings','parts', ...(native ? ['compressedParts'] : [])]);
      const recording = tx.objectStore('recordings').get(key(owner,id));
      const compatible = tx.objectStore('parts').getAll(IDBKeyRange.bound([owner,id,-Number.MAX_SAFE_INTEGER],[owner,id,-1]));
      const existing = native ? tx.objectStore('compressedParts').getAll(IDBKeyRange.bound([owner,id,0],[owner,id,Number.MAX_SAFE_INTEGER])) : null;
      tx.oncomplete = () => {
        const row = recording.result as Recording | undefined;
        const layout = row?.compressedStorage || (existing?.result.length ? 'compressedParts' : 'parts-v1');
        let parts: CompressedPartRow[] = [];
        if (layout === 'compressedParts') parts = existing?.result || [];
        if (layout === 'parts-v1') {
          const rows = compatible.result as CompatibleCompressedRow[];
          if (rows.every(part => part.kind === 'compressed-part-v1' && Number.isSafeInteger(part.originalIndex) &&
              part.originalIndex >= 0 && part.index === -(part.originalIndex + 1))) {
            parts = rows.map(({originalIndex,kind:_kind,...part}) => ({...part,index:originalIndex})).sort((a,b) => a.index-b.index);
          }
        }
        resolve({recording:row,parts});
      };
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
    identity(owner, id); owner=this.storageOwner(owner);
    await this.write<void>(['recordings'], (tx, result) => {
      const store = tx.objectStore('recordings'); const get = store.get(key(owner, id));
      get.onsuccess = () => {
        if (!get.result) { this.abort(tx, 'recording_not_found'); return; }
        store.put(patch(get.result)); result();
      };
    });
  }
  async parts(owner: string, id: string): Promise<AudioPart[]> {
    identity(owner, id); owner=this.storageOwner(owner);
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction('parts');
      const get = tx.objectStore('parts').getAll(IDBKeyRange.bound([owner, id, 0], [owner, id, Number.MAX_SAFE_INTEGER]));
      get.onsuccess = () => resolve(get.result.map(({ owner: _owner, recordingId: _id, digest: _digest, ...part }) => part));
      get.onerror = () => reject(new Error('voice_storage_read_failed'));
    });
  }
  async saveAnswer(owner: string, id: string, payload: Record<string, unknown>): Promise<void> {
    identity(owner, id); owner=this.storageOwner(owner);
    await this.write<void>(['answers'], (tx, result) => {
      const store = tx.objectStore('answers'); const get = store.get(key(owner, id));
      get.onsuccess = () => {
        if (!get.result) store.add({ id, owner, createdAt: new Date().toISOString(), payload });
        result();
      };
    });
  }
  command(owner:string,id:string):Promise<Command|null> {
    identity(owner,id); owner=this.storageOwner(owner);return new Promise((resolve,reject)=>{const tx=this.db.transaction('commands');const get=tx.objectStore('commands').get([owner,id]);
      get.onsuccess=()=>resolve(get.result||null);get.onerror=()=>reject(new Error('voice_storage_read_failed'));});
  }
  conversation(owner:string):Promise<State|null> {
    identity(owner,'active'); owner=this.storageOwner(owner);
    return new Promise((resolve,reject)=>{const tx=this.db.transaction('conversations');const request=tx.objectStore('conversations').get(owner);
      request.onsuccess=()=>resolve(request.result?.state||null);request.onerror=()=>reject(new Error('voice_storage_read_failed'));});
  }
  /** Accepted input and the new kernel revision commit atomically. Two tabs
   * cannot overwrite each other; a conflict requires a fresh read. */
  async checkpoint(owner:string,state:State,expectedRevision:number,command?:Command):Promise<void> {
    identity(owner,'active'); owner=this.storageOwner(owner);
    await this.write<void>(['conversations','commands'],(tx,result)=>{
      const store=tx.objectStore('conversations');const get=store.get(owner);
      get.onsuccess=()=>{
        if((get.result?.state?.revision||0)!==expectedRevision){this.abort(tx,'voice_revision_conflict');return;}
        if(command)tx.objectStore('commands').add({...command,owner});
        store.put({owner,state});result();
      };
    });
  }
  recording(owner:string,id:string):Promise<Recording|null> {
    identity(owner,id);owner=this.storageOwner(owner);
    return new Promise((resolve,reject)=>{const tx=this.db.transaction('recordings');const get=tx.objectStore('recordings').get([owner,id]);
      tx.oncomplete=()=>resolve(get.result||null);tx.onabort=tx.onerror=()=>reject(Error('voice_storage_read_failed'));});
  }
  /** Same schema/store, never a second queue/database. Logical owner stays explicit. */
  async prepareHandoff(owner:string,scope:HandoffScope,payload:HomeHandoff['payload'],id:string=crypto.randomUUID()):Promise<HomeHandoff>{
    if(this.scope!==scope.storageScope)throw Error('handoff_scope_invalid');
    if(payload.kind==='audio'){
      const recording=await this.recording(owner,payload.recordingId);
      if(recording?.state!=='saved'||!recording.receipt?.complete||recording.receipt.speechEvidence!==true)throw Error('handoff_audio_incomplete');
    }
    const createdAt=new Date().toISOString();
    const row:HomeHandoff={id,kind:'home-handoff-v1',owner,createdAt,version:HOME_HANDOFF_VERSION,origin:scope.origin,prefix:scope.prefix,submittedAt:createdAt,
      taskId:crypto.randomUUID(),interpretationId:crypto.randomUUID(),searchId:crypto.randomUUID(),asrId:crypto.randomUUID(),payload,status:'prepared'};
    validateHandoff(row,owner,scope);
    await this.write<void>(['commands'],(tx,result)=>{const store=tx.objectStore('commands');const get=store.get([this.storageOwner(owner),id]);
      get.onsuccess=()=>{if(get.result){const old=get.result.handoff;if(JSON.stringify(old?.payload)!==JSON.stringify(payload)){this.abort(tx,'handoff_payload_conflict');return;}result();return;}
        store.add({id,owner:this.storageOwner(owner),createdAt,kind:'home-handoff-v1',handoff:row});result();};});
    const saved=await this.handoff(owner,id);return validateHandoff(saved,owner,scope);
  }
  handoff(owner:string,id:string):Promise<HomeHandoff|null>{
    identity(owner,id);return new Promise((resolve,reject)=>{const tx=this.db.transaction('commands');const get=tx.objectStore('commands').get([this.storageOwner(owner),id]);
      tx.oncomplete=()=>resolve(get.result?.kind==='home-handoff-v1'?get.result.handoff:null);tx.onabort=tx.onerror=()=>reject(Error('voice_storage_read_failed'));});
  }
  async saveHomeDraft(owner:string,payload:{text:string;recordingId:string|null;pendingId:string|null}):Promise<void>{
    identity(owner,'home-entry-draft');
    await this.write<void>(['commands'],(tx,result)=>{tx.objectStore('commands').put({id:'home-entry-draft',owner:this.storageOwner(owner),createdAt:new Date().toISOString(),kind:'home-entry-draft',payload});result();});
  }
  async homeDraft(owner:string):Promise<{text:string;recordingId:string|null;pendingId:string|null}|null>{
    const row=await this.command(owner,'home-entry-draft') as unknown as {kind:string;payload:any}|null;
    return row?.kind==='home-entry-draft'?row.payload:null;
  }
  async markHandoff(owner:string,id:string,status:'completed'|'empty'|'cancelled'):Promise<void>{
    await this.write<void>(['commands'],(tx,result)=>{const store=tx.objectStore('commands');const get=store.get([this.storageOwner(owner),id]);
      get.onsuccess=()=>{const row=get.result;if(!row?.handoff){this.abort(tx,'handoff_missing');return;}store.put({...row,handoff:{...row.handoff,status}});result();};});
  }
  /** New task + accepted command + adoption marker are ONE atomic CAS owner transaction. */
  async adoptHandoff(owner:string,id:string,scope:HandoffScope,text:string):Promise<{state:State;command:Command;fresh:boolean}>{
    if(this.scope!==scope.storageScope||!text.trim()||text.length>8192)throw Error('handoff_invalid');
    return this.write(['commands','conversations'],(tx,result)=>{
      const commands=tx.objectStore('commands'),conversations=tx.objectStore('conversations'),physical=this.storageOwner(owner);
      const get=commands.get([physical,id]);get.onsuccess=()=>{
        const stored=get.result;let handoff:HomeHandoff;
        try{handoff=validateHandoff(stored?.handoff,owner,scope);}catch{this.abort(tx,'handoff_invalid');return;}
        if(['empty','cancelled'].includes(handoff.status)){this.abort(tx,'handoff_terminal');return;}
        const stateGet=conversations.get(physical);stateGet.onsuccess=()=>{
          const state:State=stateGet.result?.state||initialState();
          const existing=commands.get([physical,handoff.interpretationId]);existing.onsuccess=()=>{
            if(existing.result){if(existing.result.payload.text!==text){this.abort(tx,'handoff_payload_conflict');return;}result({state,command:existing.result,fresh:false});return;}
            if(handoff.status!=='prepared'){this.abort(tx,'handoff_receipt_missing');return;}
            const working=resetTask(state,false);
            const input={id:handoff.interpretationId,sequence:working.acceptedThrough+1,epoch:working.epoch,previousId:null,mode:'new_search' as const,parentId:null,text};
            const command:Command={id:input.id,searchId:handoff.searchId,input,createdAt:handoff.submittedAt,
              payload:{text,mode:'new_search',parentId:null,previousId:null,anchor:handoff.submittedAt,visibleIds:[]}};
            const next=acceptInput(working,input);
            commands.add({...command,owner:physical});conversations.put({owner:physical,state:next});
            commands.put({...stored,handoff:{...handoff,status:'adopted'}});result({state:next,command,fresh:true});
          };
        };
      };
    });
  }
  /** Stable bounded keyset pagination; loading a page never deletes old rows. */
  page<T extends Recording | StoredAnswer | (Command & {owner:string})>(kind: 'recordings' | 'answers' | 'commands', owner: string, before?: [string, string], limit = 20): Promise<T[]> {
    identity(owner, 'page'); owner=this.storageOwner(owner);
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
