// Real Chromium + IndexedDB + production mount/client; injected Auth snapshot and
// synthetic persisted WAV fixture. NO physical microphone, real login or ASR claim.
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createServer} from 'node:http';
import {build} from 'esbuild';
import {chromium} from 'playwright';
let server,browser,origin,assistantRequests=0;
const root=new URL('../',import.meta.url).pathname;
const controls=['record','stop','submit','new','resume','latest','save-retry','history-load','login'];
const labels=['processing','capture','base','answers','history-list','recording-list','auth'];
const html=`<!doctype html><div data-authorized-search><div data-assistant data-assistant-capture-only="true" data-assistant-worklet="/voice/pcm-capture-worklet.js"><div data-assistant-dock><span data-assistant-live></span><span data-assistant-timer hidden></span><button data-assistant-launcher>Микрофон</button><button data-assistant-details>Записи</button></div><div data-assistant-composer hidden><button data-assistant-close>Свернуть</button>${controls.map(n=>`<button type="button" data-assistant-${n} ${['submit','new','history-load'].includes(n)?'disabled':''}>${n}</button>`).join('')}${labels.map(n=>`<div data-assistant-${n}></div>`).join('')}<form data-assistant-form><textarea data-assistant-text disabled></textarea></form></div></div></div><script type="module" src="/bundle.js"></script>`;
test.before(async()=>{
 const bundle=(await build({stdin:{contents:`
 import {mountComposerPresentation} from './src/lib/assistant/composerPresentation.ts';
 import {mountConversationalSearch} from './src/lib/assistant/conversationalSearch.ts';
 import {VoiceStore} from './src/lib/assistant/voiceStore.ts';
 import {segmentPcm16} from './src/lib/assistant/audioSegments.ts';
 window.sessionReads=0;window.snap={status:'signed_in',user:{id:'test-owner',is_anonymous:false}};
 window.auth={signIn:async()=>false,client:{auth:{getSession:async()=>{window.sessionReads++;return {data:{session:{access_token:'fixture',user:window.snap.user}}};}}},dataClient:{request:(...args)=>fetch(...args)},subscribe:fn=>{window.notifyAuth=fn;},initialize:async()=>window.notifyAuth(window.snap)};
 const store=await VoiceStore.open();
 if(!(await store.page('recordings','test-owner')).length){
 await store.create('test-owner','synthetic-recording');
 const parts=segmentPcm16(new Float32Array(1600),16000,{maxWireBytes:65536,envelopeBytes:4096,encoding:'base64'});
 for(const part of parts)await store.putPart('test-owner','synthetic-recording',part);
 await store.finish('test-owner','synthetic-recording',{complete:true,captureComplete:true,frames:1600,savedFrames:1600,sampleRate:16000,partCount:parts.length});
 await store.saveAnswer('test-owner','old-server-answer',{id:'old-server-answer',items:[]});
 }
 document.querySelector('[data-authorized-search]').dataset.supabaseUrl=location.origin;
 window.VoiceStore=VoiceStore;window.store=store;window.ui=mountComposerPresentation(document.querySelector('[data-assistant]'));await mountConversationalSearch(document.querySelector('[data-assistant]'),window.ui);window.ready=true;
 `,resolveDir:root,loader:'ts'},bundle:true,write:false,format:'esm',plugins:[{name:'auth-fixture-only',setup(b){b.onResolve({filter:/^\.\.\/staticSiteAuth$/},()=>({path:'auth-fixture',namespace:'fixture'}));b.onLoad({filter:/.*/,namespace:'fixture'},()=>({contents:'export function getStaticSiteAuth(){return window.auth;}',loader:'js'}));}}]})).outputFiles[0].text;
 server=createServer((req,res)=>{if(req.url==='/voice/pcm-capture-worklet.js'){res.setHeader('Content-Type','text/javascript');res.end(readFileSync(root+'/public/voice/pcm-capture-worklet.js'));return;}if(req.url.startsWith('/functions/v1/event-search/assistant')){assistantRequests++;res.statusCode=500;res.end('{}');return;}res.setHeader('Content-Type',req.url==='/bundle.js'?'text/javascript':'text/html');res.end(req.url==='/bundle.js'?bundle:html);});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));origin=`http://127.0.0.1:${server.address().port}`;
 browser=await chromium.launch({channel:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH?undefined:'chromium',executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox','--use-fake-device-for-media-stream']});
});
test.after(async()=>{await browser?.close();await new Promise(r=>server?.close(r));});
test('capture-only mount/reload keeps local playback, denies submit/history/status and respects signout',async()=>{
 const context=await browser.newContext();const page=await context.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));
 let observed=0;page.on('request',r=>{if(r.url().includes('/assistant/'))observed++;});
 await page.goto(origin);await page.waitForFunction(()=>window.ready);
 for(let pass=0;pass<2;pass++){
  await page.evaluate(()=>window.ui.open());
  await page.waitForFunction(()=>document.querySelector('[data-assistant-recording-list] button'));
  assert.equal(await page.locator('[data-assistant-record]').isDisabled(),false);
  for(const n of ['text','submit','new','history-load'])assert.equal(await page.locator(`[data-assistant-${n}]`).isDisabled(),true);
  assert.equal(await page.getByRole('button',{name:'Распознавание в этом превью отключено'}).isDisabled(),true);
  await page.getByRole('button',{name:'Прослушать сохранённое аудио'}).click();
  await page.waitForFunction(()=>document.querySelector('audio')?.readyState>=1);
  assert.equal(await page.locator('audio').evaluate(a=>a.duration),0.1);
  await page.evaluate(()=>{document.querySelector('[data-assistant-text]').value='should not send';document.querySelector('[data-assistant-form]').dispatchEvent(new Event('submit',{cancelable:true}));for(const n of ['history-load','resume','new'])document.querySelector(`[data-assistant-${n}]`).dispatchEvent(new MouseEvent('click'));});
  assert.equal(await page.evaluate(()=>window.sessionReads),0);
  if(pass===0){await page.reload();await page.waitForFunction(()=>window.ready);}
 }
 await page.evaluate(()=>window.notifyAuth({status:'signed_out',user:null}));
 assert.equal(await page.locator('[data-assistant-record]').isDisabled(),false);
 await page.locator('[data-assistant-record]').evaluate(button=>button.click());
 assert.match(await page.locator('[data-assistant-auth]').textContent(),/Войдите/);
 await page.locator('[data-assistant-login]').click();
 await page.waitForFunction(()=>document.querySelector('[data-assistant-auth]').textContent.includes('Не удалось открыть вход'));
 assert.equal(await page.locator('[data-assistant-recording-list]').textContent(),'');
 assert.deepEqual(errors,[]);assert.equal(observed,0);assert.equal(assistantRequests,0);
 await context.close();
});

test('signed-in orb records twice without opening a panel; durable seal gates restart and records survive reload',async()=>{
 const context=await browser.newContext({permissions:['microphone']});const p=await context.newPage();const errors=[];p.on('pageerror',e=>errors.push(e.message));await p.goto(origin);await p.waitForFunction(()=>window.ready);
 const mic=p.locator('[data-assistant-launcher]'),panel=p.locator('[data-assistant-composer]');
 for(let i=0;i<2;i++){
  await mic.click();await p.waitForFunction(()=>document.querySelector('[data-assistant-dock]').dataset.captureState==='recording');
  assert.equal(await panel.isVisible(),false);assert.equal(await mic.getAttribute('aria-pressed'),'true');
  await p.waitForTimeout(1200);
  await p.evaluate(()=>{const original=window.VoiceStore.prototype.finish;window.originalFinish=original;window.VoiceStore.prototype.finish=async function(...args){await new Promise(r=>window.releaseSeal=r);return original.apply(this,args);};});
  await mic.click();await p.waitForFunction(()=>window.releaseSeal);
  assert.equal(await p.locator('[data-assistant-dock]').getAttribute('data-capture-state'),'stopping');
  const rect=await mic.boundingBox();await p.mouse.click(rect.x+rect.width/2,rect.y+rect.height/2);assert.match(await p.locator('[data-assistant-live]').textContent(),/Сохраняю/);
  await p.evaluate(()=>{window.VoiceStore.prototype.finish=window.originalFinish;window.releaseSeal();window.releaseSeal=null;});
  await p.waitForFunction(()=>document.querySelector('[data-assistant-dock]').dataset.captureState==='saved');assert.equal(await panel.isVisible(),false);
 }
 const before=await p.evaluate(async()=>{const rows=await window.store.page('recordings','test-owner');return rows.map(r=>({id:r.id,state:r.state,bytes:r.bytes}));});assert.equal(before.length,3);assert.ok(before.every(r=>r.state==='saved'&&r.bytes>0));
 await p.reload();await p.waitForFunction(()=>window.ready);const after=await p.evaluate(async()=>{const rows=await window.store.page('recordings','test-owner');return rows.map(r=>({id:r.id,state:r.state,bytes:r.bytes}));});assert.deepEqual(after,before);assert.deepEqual(errors,[]);assert.equal(assistantRequests,0);await context.close();
});
test('failed final receipt is not reported saved; retry seals the same audio without a new recording',async()=>{
 const context=await browser.newContext({permissions:['microphone']});const p=await context.newPage();await p.goto(origin);await p.waitForFunction(()=>window.ready);
 await p.evaluate(()=>{window.originalFinish=window.VoiceStore.prototype.finish;window.VoiceStore.prototype.finish=async()=>{throw Error('injected terminal storage failure');};});
 await p.locator('[data-assistant-launcher]').click();await p.waitForFunction(()=>document.querySelector('[data-assistant-dock]').dataset.captureState==='recording');await p.waitForTimeout(1200);await p.locator('[data-assistant-launcher]').click();
 await p.waitForFunction(()=>document.querySelector('[data-assistant-dock]').dataset.captureState==='error');assert.equal(await p.locator('[data-assistant-save-retry]').isVisible(),true);
 const interrupted=await p.evaluate(async()=>{const rows=await window.store.page('recordings','test-owner');return rows.filter(r=>r.id!=='synthetic-recording').map(r=>({id:r.id,state:r.state,bytes:r.bytes}));});assert.equal(interrupted.length,1);assert.equal(interrupted[0].state,'recording');assert.ok(interrupted[0].bytes>0);
 assert.equal(await p.evaluate(()=>{const event=new Event('beforeunload',{cancelable:true});window.dispatchEvent(event);return event.defaultPrevented;}),true);
 await p.evaluate(()=>{window.VoiceStore.prototype.finish=window.originalFinish;});await p.locator('[data-assistant-save-retry]').click();await p.waitForFunction(()=>document.querySelector('[data-assistant-dock]').dataset.captureState==='saved');
 const recovered=await p.evaluate(async()=>{const rows=await window.store.page('recordings','test-owner');return rows.filter(r=>r.id!=='synthetic-recording').map(r=>({id:r.id,state:r.state,bytes:r.bytes}));});assert.deepEqual(recovered,[{...interrupted[0],state:'saved'}]);assert.equal(assistantRequests,0);await context.close();
});
