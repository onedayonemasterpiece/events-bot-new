import test from 'node:test';
import assert from 'node:assert/strict';
import {AssistantClient} from '../src/lib/assistant/assistantClient.ts';
import {loadPreviewPublicConfig} from '../scripts/preview-public-env.mjs';
test('capture-only denies all assistant routes before session or network access',async()=>{
 let sessions=0,requests=0;
 const auth={client:{auth:{getSession:async()=>{sessions++;throw Error('must not read session');}}},dataClient:{request:async()=>{requests++;throw Error('must not send');}}};
 const api=new AssistantClient(auth,'http://localhost','public',()=> 'owner',()=>false);
 for(const operation of [()=>api.status('owner','old'),()=>api.history('owner'),()=>api.control('owner','id','asr',{},true),()=>api.request('audio','owner',{}),()=>api.execute('owner','id','interpret',{})]) await assert.rejects(operation,/voice_capture_only/);
 assert.equal(sessions,0);assert.equal(requests,0);
});
test('capture-only flag is browser-safe and rejects malformed configuration',()=>{
 const site=new URL('../',import.meta.url).pathname;
 const config=loadPreviewPublicConfig(site,{PUBLIC_EVENT_SEARCH_ASSISTANT_CAPTURE_ONLY:'1'});
 assert.equal(config.values.PUBLIC_EVENT_SEARCH_ASSISTANT_CAPTURE_ONLY,'1');
 assert.throws(()=>loadPreviewPublicConfig(site,{PUBLIC_EVENT_SEARCH_ASSISTANT_CAPTURE_ONLY:'yes'}),/must be 0 or 1/);
});

test('control gives full-window semantic verification a bounded deadline; reads retain the old limit',async()=>{
 const original=AbortSignal.timeout,deadlines=[];let requests=0;
 AbortSignal.timeout=(ms)=>{deadlines.push(ms);return new AbortController().signal;};
 try{
  const auth={client:{auth:{getSession:async()=>({data:{session:{access_token:'fixture-token',user:{id:'owner'}}}})}},dataClient:{request:async()=>{requests++;return new Response(JSON.stringify({contract:'kenigevents-assistant-v1'}));}}};
  const api=new AssistantClient(auth,'https://fixture.invalid','public',()=> 'owner');
  await api.control('owner','id','search',{},true);await api.status('owner','id');
  assert.deepEqual(deadlines,[90000,60000]);assert.equal(requests,2);
 }finally{AbortSignal.timeout=original;}
});
