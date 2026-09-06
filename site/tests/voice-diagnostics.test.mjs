import test from 'node:test';import assert from 'node:assert/strict';
import {probeVoiceStorage} from '../src/lib/assistant/voiceDiagnostics.ts';
test('explicit diagnostic is single-flight and bounded; late success deletes only its own disposable database',async()=>{
 let request,opened=[];const deleted=[];
 globalThis.indexedDB={open(name,version){opened.push({name,version});return request={};},databases:()=>new Promise(()=>{}),deleteDatabase(name){deleted.push(name);return {};}};
 const first=probeVoiceStorage(15);assert.equal(probeVoiceStorage(15),first);await first;
 assert.equal(opened.length,1);assert.match(opened[0].name,/^kenigevents-voice-diagnostic-[a-f0-9-]+$/);assert.equal(opened[0].version,1);assert.equal(deleted.length,0);
 let closed=0;request.result={version:1,close(){closed++;}};request.onsuccess();assert.equal(closed,1);assert.deepEqual(deleted,[opened[0].name]);assert.ok(!deleted.includes('kenigevents-voice-v1'));delete globalThis.indexedDB;
});
test('diagnostic handles unsupported metadata API and explicit open rejection without deleting any database',async()=>{
 let deletes=0;globalThis.indexedDB={open(){throw new DOMException('private message','SecurityError');},deleteDatabase(){deletes++;}};
 await probeVoiceStorage(15);assert.equal(deletes,0);delete globalThis.indexedDB;
});
