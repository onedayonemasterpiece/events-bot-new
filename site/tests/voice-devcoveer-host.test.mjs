// HTTP/storage/media validation fixtures. Provider/Auth stubs here are NOT live ASR.
import test from 'node:test';import assert from 'node:assert/strict';import{mkdtempSync,rmSync}from'node:fs';import{join,resolve}from'node:path';import{execFileSync}from'node:child_process';import{randomUUID,createHash}from'node:crypto';import{pathToFileURL}from'node:url';import{DevCoveerReceiptStore}from'../../scripts/voice/receiptStore.mjs';
const root=resolve(import.meta.dirname,'../..'),tmp=mkdtempSync(join(root,'artifacts/voice-host-test-'));
execFileSync('node',['scripts/voice/build-runtime.mjs',tmp+'/runtime.mjs'],{cwd:root});
const{createVoiceServer,validateMedia}=await import(pathToFileURL(tmp+'/runtime.mjs'));
const owner=randomUUID();const origin='https://kenigevents.ru';let sends=0;
const {AssistantError}=await import('../../supabase/functions/event-search/assistant-intent.ts');
// Dependency failures require cross-bundle-safe error conversion in factory.
const deps=repo=>({enabled:true,allowedOrigins:[origin],maxAudioBytes:16*1024*1024,
 authenticate:async r=>{if(r.headers.get('authorization')!=='Bearer fixture')throw Object.assign(new Error('auth_required'),{code:'auth_required',status:401});return{owner,repo};},
 generate:async o=>{sends++;await o.dispatched();const value=o.validate({text:'Бесплатные события',uncertain:[]});await o.completed(value,null);await o.accounted();return value;},search:async()=>({items:[]}),currentCards:async()=>[]});
const store=new DevCoveerReceiptStore(tmp+'/state/receipts.sqlite');const server=createVoiceServer({store,dependencyFactory:deps});await new Promise(r=>server.listen(0,'127.0.0.1',r));const base=`http://127.0.0.1:${server.address().port}/kenig-audio/event-search/assistant/`;
const headers={Origin:origin,Authorization:'Bearer fixture','Content-Type':'application/json'};
test.after(()=>{server.close();store.close();rmSync(tmp,{recursive:true,force:true});});
test('exact routes, CORS, denied auth and bounded upload',async()=>{
 assert.equal((await fetch(base+'status',{headers:{Origin:'https://foreign.invalid'}})).status,403);
 assert.equal((await fetch(base+'status',{method:'OPTIONS',headers:{Origin:origin}})).headers.get('access-control-allow-origin'),origin);
 assert.equal((await fetch(base+'../other',{headers})).status,404);
 const denied=await fetch(base+'status',{headers:{Origin:origin}});assert.equal(denied.status,401);
 const tooBig=await fetch(base+'control',{method:'POST',headers,body:' '.repeat(65537)});assert.equal(tooBig.status,413);
});
test('real Opus binary admission, durable receipt, same-ID no provider replay',async()=>{
 const bytes=execFileSync((process.env.VOICE_FFMPEG||'ffmpeg'),['-v','error','-f','lavfi','-i','sine=frequency=440:duration=2:sample_rate=48000','-ac','1','-c:a','libopus','-b:a','32k','-f','webm','pipe:1']);
 const payload={frames:96000,sampleRate:48000,partCount:1,mimeType:'audio/webm;codecs=opus',digest:createHash('sha256').update(bytes).digest('hex'),byteLength:bytes.length};const id=randomUUID();
 const control=run=>fetch(base+'control',{method:'POST',headers,body:JSON.stringify({id,kind:'asr',payload,run})});
 assert.equal((await control(false)).status,202);
 const put=()=>fetch(base+'media?id='+id,{method:'POST',headers:{...headers,'Content-Type':payload.mimeType},body:bytes});
 const uploaded=await put();assert.equal(uploaded.status,200,await uploaded.text());assert.equal((await put()).status,200);
 const result=await(await control(true)).json();assert.equal(result.state,'completed');assert.equal(result.result.text,'Бесплатные события');
 assert.equal((await(await control(true)).json()).state,'completed');assert.equal(sends,1);
 assert.equal((await store.media(owner,id)).bytes.length,bytes.length);assert.ok(bytes.length<96000*2/5);
});
test('AAC real container decoded; bogus input and mismatched duration rejected',async()=>{
 const bytes=execFileSync((process.env.VOICE_FFMPEG||'ffmpeg'),['-v','error','-f','lavfi','-i','sine=frequency=440:duration=2:sample_rate=48000','-ac','1','-c:a','aac','-b:a','32k','-movflags','frag_keyframe+empty_moov','-f','mp4','pipe:1']);
 const r=await validateMedia(bytes,'audio/mp4',{frames:96000,sampleRate:48000},{ffmpeg:(process.env.VOICE_FFMPEG||'ffmpeg'),ffprobe:(process.env.VOICE_FFPROBE||'ffprobe')});assert.equal(r.codec,'aac');
 await assert.rejects(()=>validateMedia(bytes,'audio/mp4',{frames:960000,sampleRate:48000},{ffmpeg:(process.env.VOICE_FFMPEG||'ffmpeg'),ffprobe:(process.env.VOICE_FFPROBE||'ffprobe')}),/duration_mismatch/);
 await assert.rejects(()=>validateMedia(Buffer.from('not audio'),'audio/webm',{frames:1,sampleRate:48000},{ffprobe:(process.env.VOICE_FFPROBE||'ffprobe')}),/invalid_audio/);
});
