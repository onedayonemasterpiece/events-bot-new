import test from 'node:test';import assert from 'node:assert/strict';
import {VoiceStore} from '../src/lib/assistant/voiceStore.ts';
const schema = close => ({close,objectStoreNames:{contains:n=>['recordings','answers','commands','parts','conversations'].includes(n)},transaction:()=>({objectStore:n=>({keyPath:n==='conversations'?'owner':n==='parts'?['owner','recordingId','index']:['owner','id'],indexNames:{contains:()=>true},index:()=>({keyPath:['owner','createdAt','id']})})})});
test('opening a silent IndexedDB request has a bounded failure and closes its late connection',async()=>{
 const request={};let closes=0;globalThis.indexedDB={open:()=>request};
 await assert.rejects(VoiceStore.open({timeoutMs:20}),/voice_storage_open_timeout/);
 request.result={close:()=>closes++};request.onsuccess();assert.equal(closes,1);delete globalThis.indexedDB;
});
test('blocked upgrade fails explicitly; late upgrade aborts instead of retaining an orphan DB',async()=>{
 const request={};globalThis.indexedDB={open:()=>request};let aborted=0;
 const opening=VoiceStore.open({timeoutMs:100});request.onblocked();await assert.rejects(opening,/voice_storage_upgrade_blocked/);
 request.transaction={abort:()=>aborted++};request.onupgradeneeded();assert.equal(aborted,1);request.onerror();delete globalThis.indexedDB;
});
test('normal connection resolves once and remains versionchange-closeable',async()=>{
 const request={};globalThis.indexedDB={open:()=>request};let closes=0;
 const opening=VoiceStore.open({timeoutMs:100});request.result=schema(()=>closes++);request.onsuccess();const store=await opening;
 request.result.onversionchange();assert.equal(closes,1);store.close();assert.equal(closes,2);delete globalThis.indexedDB;
});

test('timed out request stays single-flight until a native terminal event',async()=>{
 const request={};let calls=0;globalThis.indexedDB={open:()=>{calls++;return request;}};
 const first=VoiceStore.open({timeoutMs:10});assert.equal(VoiceStore.open({timeoutMs:10}),first);
 await assert.rejects(first,/voice_storage_open_timeout/);await assert.rejects(VoiceStore.open(),/voice_storage_open_timeout/);assert.equal(calls,1);
 request.onerror();const second=VoiceStore.open({timeoutMs:10});request.result=schema(()=>{});request.onsuccess();await second;assert.equal(calls,2);delete globalThis.indexedDB;
});

test('existing schema opens without a version and missing stores fail closed without repair',async()=>{
 const request={};let args,closed=0;globalThis.indexedDB={open:(...values)=>{args=values;return request;}};
 const opening=VoiceStore.open({timeoutMs:100});assert.deepEqual(args,['kenigevents-voice-v1']);request.result={close:()=>closed++,objectStoreNames:{contains:()=>false}};request.onsuccess();await assert.rejects(opening,/voice_storage_schema_unsupported/);assert.equal(closed,1);delete globalThis.indexedDB;
});
