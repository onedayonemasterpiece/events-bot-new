import test from 'node:test';import assert from 'node:assert/strict';
import {VoiceStore} from '../src/lib/assistant/voiceStore.ts';
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
 const opening=VoiceStore.open({timeoutMs:100});request.result={close:()=>closes++};request.onsuccess();const store=await opening;
 request.result.onversionchange();assert.equal(closes,1);store.close();assert.equal(closes,2);delete globalThis.indexedDB;
});

test('timed out request stays single-flight until a native terminal event',async()=>{
 const request={};let calls=0;globalThis.indexedDB={open:()=>{calls++;return request;}};
 const first=VoiceStore.open({timeoutMs:10});assert.equal(VoiceStore.open({timeoutMs:10}),first);
 await assert.rejects(first,/voice_storage_open_timeout/);await assert.rejects(VoiceStore.open(),/voice_storage_open_timeout/);assert.equal(calls,1);
 request.onerror();const second=VoiceStore.open({timeoutMs:10});request.result={close(){}};request.onsuccess();await second;assert.equal(calls,2);delete globalThis.indexedDB;
});
