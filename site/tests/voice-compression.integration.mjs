// Native Chromium compressed companion + IndexedDB; synthetic device, NOT physical phone or ASR.
import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { build } from 'esbuild';
import { chromium } from 'playwright';
const root = new URL('../', import.meta.url).pathname;
let server, browser, origin, bundle;
test.before(async () => {
  bundle = (await build({ stdin: { contents: `
    import { MicrophoneCapture } from './src/lib/assistant/microphoneCapture.ts';
    import { VoiceStore } from './src/lib/assistant/voiceStore.ts';
    window.modules = { MicrophoneCapture, VoiceStore };
    `, resolveDir: root, loader: 'ts' }, bundle: true, write: false, format: 'esm' })).outputFiles[0].text;
  const worklet = await readFile(new URL('../public/voice/pcm-capture-worklet.js', import.meta.url));
  server = createServer((req, res) => {
    if (req.url === '/bundle.js') { res.setHeader('Content-Type','text/javascript'); res.end(bundle); }
    else if (req.url === '/voice/pcm-capture-worklet.js') { res.setHeader('Content-Type','text/javascript'); res.end(worklet); }
    else { res.setHeader('Content-Type','text/html'); res.end('<!doctype html><button id="start">Запись</button><button id="stop">Стоп</button><script type="module" src="/bundle.js"></script>'); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  origin = `http://127.0.0.1:${server.address().port}`;
  browser = await chromium.launch({ channel: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH ? undefined : 'chromium', executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || undefined,
    args: ['--use-fake-device-for-media-stream', '--no-sandbox'] });
});
test.after(async () => { await browser?.close(); await new Promise(resolve => server?.close(resolve)); });
async function setup(permissions = ['microphone']) {
  const context = await browser.newContext({ permissions }); const page = await context.newPage();
  await page.goto(origin); await page.waitForFunction(() => !!window.modules);
  return { context, page };
}
async function record(page, id, {failCompressed = false, noRecorder = false, reason = 'user'} = {}) {
 await page.evaluate(async ({id,failCompressed,noRecorder}) => {
  const {VoiceStore,MicrophoneCapture}=window.modules; window.store = await VoiceStore.open(); await store.create('owner',id);
  if(noRecorder)window.MediaRecorder=undefined;window.compressedCheckpoints=0;
  window.capture=new MicrophoneCapture({workletUrl:'/voice/pcm-capture-worklet.js',budget:{maxWireBytes:1048576,envelopeBytes:8192,encoding:'base64'},
   onPart:part=>store.putPart('owner',id,part),onCompressedPart:async part=>{if(failCompressed)throw Error('quota fixture');await store.putCompressedPart('owner',id,part);window.compressedCheckpoints++;},onStatus:()=>{}});
  document.querySelector('#start').onclick=()=>{window.started=capture.start();};
 },{id,failCompressed,noRecorder});
 await page.click('#start'); await page.waitForFunction(()=>capture.status==='recording');
 if(!failCompressed&&!noRecorder)await page.waitForFunction(()=>window.compressedCheckpoints>=2);
 else await page.waitForTimeout(2250);
 return page.evaluate(async ({id,reason})=>{
  const unsealed=await store.compressed('owner',id); const receipt=await capture.stop(reason); const before=await store.compressed('owner',id);
  await store.finish('owner',id,receipt);
  return {receipt,unsealed,before,pcm:(await store.parts('owner',id)).reduce((n,p)=>n+p.bytes.length,0)};
 },{id,reason});
}
test('native 32kbps Opus has final tail, remains one decodable original after reload, and is far smaller than PCM',async()=>{
 const {context,page}=await setup(); const result=await record(page,'native');
 assert.equal(result.receipt.complete,true); assert.equal(result.receipt.compressed.complete,true);
 assert.match(result.receipt.compressed.mimeType,/^audio\/webm;codecs=opus$/);
 assert.ok(result.receipt.compressed.partCount>=3); assert.ok(result.receipt.compressed.bytes<result.pcm/8);
 assert.equal(result.unsealed,null);assert.equal(result.before,null);
 await page.reload();await page.waitForFunction(()=>window.modules);
 const decoded=await page.evaluate(async()=>{
  const store=await window.modules.VoiceStore.open();const audio=await store.compressed('owner','native');const pcm=await store.parts('owner','native');
  const ac=new AudioContext();const decoded=await ac.decodeAudioData(audio.bytes.buffer.slice(0));await ac.close();
  const other=await store.compressed('other','native'); const row=(await store.page('recordings','owner'))[0];
  return {duration:decoded.duration,bytes:audio.bytes.length,other,state:row.state,pcmDuration:pcm.reduce((n,p)=>n+p.frameCount,0)/pcm[0].sampleRate};
 });
 assert.ok(decoded.duration>=decoded.pcmDuration-0.10,JSON.stringify(decoded));assert.ok(decoded.duration<decoded.pcmDuration+0.25,JSON.stringify(decoded));
 assert.equal(decoded.state,'saved');assert.equal(decoded.bytes,result.receipt.compressed.bytes);assert.equal(decoded.other,null);
 console.log('synthetic native compression evidence',JSON.stringify({receipt:result.receipt,pcmBytes:result.pcm,decoded}));
 await context.close();
});
for(const [id,options,reason] of [['unsupported',{noRecorder:true},'media_recorder_unavailable'],['quota',{failCompressed:true},'compressed_storage_or_data_failed']])test(`native PCM remains complete when compressed ${id}`,async()=>{
 const {context,page}=await setup();const result=await record(page,id,options);
 assert.equal(result.receipt.complete,true);assert.equal(result.receipt.compressed.complete,false);assert.equal(result.receipt.compressed.reason,reason);
 assert.ok(result.pcm>0);assert.equal(await page.evaluate(id=>store.compressed('owner',id),id),null);
 await context.close();
});
test('additive v2 upgrade retains PCM, answers, commands and conversations; incomplete/gapped compressed rows never substitute for original',async()=>{
 const {context,page}=await setup();
 const result=await page.evaluate(async()=>{
  await new Promise((resolve,reject)=>{
   const request=indexedDB.open('kenigevents-voice-v1',2);
   request.onupgradeneeded=()=>{const db=request.result;for(const name of ['recordings','answers','commands']){const s=db.createObjectStore(name,{keyPath:['owner','id']});s.createIndex('owner_created',['owner','createdAt','id']);}db.createObjectStore('parts',{keyPath:['owner','recordingId','index']});db.createObjectStore('conversations',{keyPath:'owner'});};
   request.onsuccess=()=>{const db=request.result;const tx=db.transaction(['recordings','answers','commands','parts','conversations'],'readwrite');
    tx.objectStore('recordings').put({owner:'owner',id:'old',createdAt:'2026',partCount:1,bytes:2,state:'saved'});
    tx.objectStore('parts').put({owner:'owner',recordingId:'old',index:0,bytes:Uint8Array.of(7,8)});
    for(const name of ['answers','commands'])tx.objectStore(name).put({owner:'owner',id:name,createdAt:'2026',payload:{keep:true}});
    tx.objectStore('conversations').put({owner:'owner',state:{revision:4}});
    tx.oncomplete=()=>{db.close();resolve();};tx.onerror=reject;};request.onerror=reject;
  });
  const store=await window.modules.VoiceStore.open();const old=await store.parts('owner','old');
  await store.create('owner','gap');await store.putCompressedPart('owner','gap',{index:1,mimeType:'audio/webm;codecs=opus',bytes:Uint8Array.of(1)});
  await store.finish('owner','gap',{frames:0,savedFrames:0,partCount:0,complete:false,compressed:{mimeType:'audio/webm;codecs=opus',partCount:1,bytes:1,complete:true}});
  return {old:[...old[0].bytes],answers:(await store.page('answers','owner')).length,commands:(await store.page('commands','owner')).length,
   conversation:await store.conversation('owner'),gap:await store.compressed('owner','gap'),oldCompressed:await store.compressed('owner','old')};
 });
 assert.deepEqual(result,{old:[7,8],answers:1,commands:1,conversation:{revision:4},gap:null,oldCompressed:null});await context.close();
});
test('background stop seals its container but remains partial; later capture does not inherit it',async()=>{
 const {context,page}=await setup();const background=await record(page,'background',{reason:'background'});
 assert.equal(background.receipt.complete,false);assert.equal(background.receipt.reason,'background');assert.equal(background.receipt.compressed.complete,true);
 const next=await record(page,'next');assert.equal(next.receipt.complete,true);
 const rows=await page.evaluate(async()=>{const rows=await store.page('recordings','owner');return rows.map(row=>({id:row.id,state:row.state,compressed:row.receipt.compressed}));});
 assert.equal(rows.find(r=>r.id==='background').state,'partial');assert.equal(rows.find(r=>r.id==='next').state,'saved');await context.close();
});
test('abrupt close preserves compressed checkpoints without inventing a sealed file',async()=>{
 const {context,page}=await setup();
 await page.evaluate(async()=>{
  const {VoiceStore,MicrophoneCapture}=window.modules;window.store=await VoiceStore.open();await store.create('owner','abrupt');window.checkpoints=0;
  window.capture=new MicrophoneCapture({workletUrl:'/voice/pcm-capture-worklet.js',budget:{maxWireBytes:1048576,envelopeBytes:8192,encoding:'base64'},onPart:part=>store.putPart('owner','abrupt',part),onCompressedPart:async part=>{await store.putCompressedPart('owner','abrupt',part);window.checkpoints++;},onStatus:()=>{}});
  document.querySelector('#start').onclick=()=>capture.start();
 });
 await page.click('#start');await page.waitForFunction(()=>window.checkpoints>=2);await page.close();
 const reopened=await context.newPage();await reopened.goto(origin);await reopened.waitForFunction(()=>window.modules);
 const result=await reopened.evaluate(async()=>{
  const store=await window.modules.VoiceStore.open();const rows=await store.page('recordings','owner');
  const count=await new Promise((resolve,reject)=>{const request=indexedDB.open('kenigevents-voice-v1');request.onsuccess=()=>{const db=request.result;const get=db.transaction('compressedParts').objectStore('compressedParts').count();get.onsuccess=()=>{resolve(get.result);db.close();};get.onerror=reject;};});
  return {state:rows[0].state,count,compressed:await store.compressed('owner','abrupt'),pcm:(await store.parts('owner','abrupt')).length};
 });
 assert.equal(result.state,'recording');assert.ok(result.count>=2);assert.ok(result.pcm>=1);assert.equal(result.compressed,null);await context.close();
});
test('cancel during final AudioContext resume drains native/PCM writes and never resurrects recording',async()=>{
 const {context,page}=await setup();
 await page.evaluate(async()=>{
  const {VoiceStore,MicrophoneCapture}=window.modules;window.store=await VoiceStore.open();await store.create('owner','cancel-resume');window.states=[];window.writes=0;
  const resume=AudioContext.prototype.resume;let resumes=0;
  AudioContext.prototype.resume=function(){const resumed=resume.call(this);if(++resumes===2)return resumed.then(()=>new Promise(resolve=>{window.releaseResume=resolve;}));return resumed;};
  window.capture=new MicrophoneCapture({workletUrl:'/voice/pcm-capture-worklet.js',budget:{maxWireBytes:1048576,envelopeBytes:8192,encoding:'base64'},onPart:part=>store.putPart('owner','cancel-resume',part),onCompressedPart:async part=>{await store.putCompressedPart('owner','cancel-resume',part);window.writes++;},onStatus:state=>states.push(state)});
  document.querySelector('#start').onclick=()=>{window.started=capture.start();};
 });
 await page.click('#start');await page.waitForFunction(()=>window.releaseResume);
 const result=await page.evaluate(async()=>{const stopped=capture.stop();releaseResume();await started;const receipt=await stopped;await store.finish('owner','cancel-resume',receipt);return {receipt,states,writes,status:capture.status};});
 assert.equal(result.receipt.reason,'cancelled');assert.equal(result.receipt.complete,false);assert.equal(result.status,'partial');assert.equal(result.states.includes('recording'),false);
 if(result.receipt.compressed.complete)assert.equal(result.writes,result.receipt.compressed.partCount);
 await context.close();
});
