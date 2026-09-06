// Real IndexedDB/native Chromium plus explicit Auth snapshot fixture. No provider,
// live identity or physical Android claims; browser UI + lifecycle regression.
import test from 'node:test';import assert from 'node:assert/strict';
import {createServer} from 'node:http';import {readFile} from 'node:fs/promises';
import {build} from 'esbuild';import {chromium,devices} from 'playwright';
const root=new URL('../',import.meta.url).pathname;let server,browser,origin;
const controls=['record','stop','submit','new','resume','latest','save-retry','history-load','login'];
const labels=['processing','capture','base','answers','history-list','recording-list','auth'];
const html=`<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><style>[hidden]{display:none!important}button{min-height:44px;margin:5px}</style><div data-authorized-search><div data-assistant data-assistant-capture-only="true" data-assistant-worklet="/voice/pcm-capture-worklet.js"><div data-assistant-dock><span data-assistant-live></span><span data-assistant-timer hidden></span><button data-assistant-launcher>Микрофон</button><button data-assistant-details>Запрос и записи</button></div><div data-assistant-composer hidden><button data-assistant-close>Свернуть</button>${controls.map(n=>`<button type="button" data-assistant-${n}>${n}</button>`).join('')}${labels.map(n=>`<div data-assistant-${n}></div>`).join('')}<form data-assistant-form><textarea data-assistant-text></textarea></form></div></div></div><script type="module" src="/bundle.js"></script>`;
test.before(async()=>{
 const bundle=(await build({stdin:{contents:`
 import{mountComposerPresentation}from'./src/lib/assistant/composerPresentation.ts';import{mountConversationalSearch}from'./src/lib/assistant/conversationalSearch.ts';
 import{VoiceStore}from'./src/lib/assistant/voiceStore.ts';
 window.VoiceStore=VoiceStore;window.micCalls=0;window.requests=0;window.signIns=0;
 const gum=navigator.mediaDevices.getUserMedia.bind(navigator.mediaDevices);navigator.mediaDevices.getUserMedia=(...a)=>{window.micCalls++;return gum(...a);};
 const mode=new URLSearchParams(location.search).get('mode');const root=document.querySelector('[data-assistant]');
 const current={status:mode==='checking'?'checking':'signed_in',user:mode==='checking'?null:{id:'owner',is_anonymous:false},message:'Проверяю вход…'};
 window.auth={subscribe(fn){window.authNotify=fn;fn(current);},async initialize(){},async signIn(){window.signIns++;return false;},client:{auth:{async getSession(){return {data:{session:null}};}}},dataClient:{async request(){window.requests++;throw Error('no network expected');}}};
 if(mode==='restore'){root.dataset.assistantCaptureOnly='false';const original=VoiceStore.prototype.conversation;VoiceStore.prototype.conversation=function(owner){return new Promise(resolve=>{window.restoreGates ||= {};window.restoreGates[owner]=()=>original.call(this,owner).then(resolve);});};}
 const open=VoiceStore.open.bind(VoiceStore);VoiceStore.open=()=>open({timeoutMs:window.voiceOpenTimeoutMs||8000});
 window.ui=mountComposerPresentation(root);window.mounted=mountConversationalSearch(root,window.ui);window.loaded=true;
 `,resolveDir:root,loader:'ts'},bundle:true,write:false,format:'esm',plugins:[{name:'explicit-auth-fixture',setup(b){b.onResolve({filter:/^\.\.\/staticSiteAuth$/},()=>({path:'auth',namespace:'fixture'}));b.onLoad({filter:/.*/,namespace:'fixture'},()=>({contents:'export const getStaticSiteAuth=()=>window.auth;',loader:'js'}));}}]})).outputFiles[0].text;
 const worklet=await readFile(root+'public/voice/pcm-capture-worklet.js');
 server=createServer((req,res)=>{res.setHeader('Content-Type',req.url==='/bundle.js'||req.url==='/voice/pcm-capture-worklet.js'?'text/javascript':'text/html');res.end(req.url==='/bundle.js'?bundle:req.url==='/voice/pcm-capture-worklet.js'?worklet:html);});await new Promise(r=>server.listen(0,'127.0.0.1',r));origin=`http://127.0.0.1:${server.address().port}`;
 browser=await chromium.launch({executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox','--use-fake-device-for-media-stream']});
});
test.after(async()=>{await browser?.close();await new Promise(r=>server?.close(r));});
async function setup(mode='',init){const context=await browser.newContext({...devices['Pixel 7'],permissions:['microphone']});const page=await context.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));if(init)await page.addInitScript(init);await page.goto(origin+'/?mode='+mode);await page.waitForFunction(()=>window.loaded);return{context,page,errors};}
test('delayed open becomes retryable; late connection closes; next gesture records once without reload or data deletion',async()=>{
 const {context,page,errors}=await setup('',()=>{
  window.voiceOpenTimeoutMs=150;const original=indexedDB.open.bind(indexedDB);let calls=0;window.lateClosed=0;
  indexedDB.open=function(name,...args){const request=original(name,...args);if(name==='kenigevents-voice-v1'&&++calls===1){let handler;Object.defineProperty(request,'onsuccess',{get:()=>handler,set:fn=>handler=fn});request.addEventListener('success',event=>{const close=request.result.close.bind(request.result);request.result.close=()=>{window.lateClosed++;close();};window.releaseOpen=()=>handler.call(request,event);});}return request;};
 });
 await page.waitForFunction(()=>document.querySelector('[data-assistant]').dataset.assistantStartup==='storage_error');
 assert.match(await page.locator('[data-assistant-live]').textContent(),/хранилище не ответило/);assert.equal(await page.evaluate(()=>window.micCalls),0);
 await page.evaluate(()=>window.releaseOpen());assert.equal(await page.evaluate(()=>window.lateClosed),1);
 await page.locator('[data-assistant-launcher]').click();await page.waitForFunction(()=>document.querySelector('[data-assistant]').dataset.assistantStartup==='ready');
 assert.equal(await page.evaluate(()=>window.micCalls),0);assert.equal(await page.locator('[data-assistant-dock]').getAttribute('data-capture-state'),'idle');
 await page.locator('[data-assistant-launcher]').click();await page.waitForFunction(()=>document.querySelector('[data-assistant-dock]').dataset.captureState==='recording');
 await page.waitForTimeout(250);await page.locator('[data-assistant-launcher]').click();await page.waitForFunction(()=>document.querySelector('[data-assistant-dock]').dataset.captureState==='saved');
 assert.equal(await page.evaluate(()=>window.micCalls),1);assert.equal(await page.evaluate(()=>window.requests),0);assert.deepEqual(errors,[]);await context.close();
});
test('real old v2 tab blocks upgrade; closing only its connection enables manual retry and preserves original audio',async()=>{
 const context=await browser.newContext({...devices['Pixel 7'],permissions:['microphone']});const old=await context.newPage();
 await old.route('**/bundle.js',route=>route.fulfill({contentType:'text/javascript',body:''}));await old.goto(origin);
 await old.evaluate(()=>new Promise((resolve,reject)=>{const request=indexedDB.open('kenigevents-voice-v1',2);request.onupgradeneeded=()=>{const db=request.result;for(const n of ['recordings','answers','commands']){const s=db.createObjectStore(n,{keyPath:['owner','id']});s.createIndex('owner_created',['owner','createdAt','id']);}db.createObjectStore('parts',{keyPath:['owner','recordingId','index']});db.createObjectStore('conversations',{keyPath:'owner'});};request.onsuccess=()=>{window.oldDb=request.result;const tx=oldDb.transaction(['recordings','parts'],'readwrite');tx.objectStore('recordings').put({id:'legacy',owner:'owner',createdAt:'2026-09-05',state:'recording',partCount:1,bytes:2});tx.objectStore('parts').put({owner:'owner',recordingId:'legacy',index:0,bytes:Uint8Array.of(17,29)});tx.oncomplete=resolve;tx.onerror=reject;};}));
 const page=await context.newPage();await page.goto(origin);await page.waitForFunction(()=>document.querySelector('[data-assistant]').dataset.assistantStartupError==='voice_storage_upgrade_blocked');
 assert.match(await page.locator('[data-assistant-live]').textContent(),/другой вкладке/);
 await old.evaluate(()=>window.oldDb.close());await page.locator('[data-assistant-launcher]').click();await page.waitForFunction(()=>document.querySelector('[data-assistant]').dataset.assistantStartup==='ready');
 const result=await page.evaluate(async()=>{const store=await VoiceStore.open();return{bytes:[...(await store.parts('owner','legacy'))[0].bytes],rows:(await store.page('recordings','owner')).length,micCalls};});assert.deepEqual(result,{bytes:[17,29],rows:1,micCalls:0});await context.close();
});
test('restore gate rejects early submit and stale owner completion cannot enable another owner',async()=>{
 const {context,page,errors}=await setup('restore');await page.waitForFunction(()=>window.restoreGates?.owner);
 await page.locator('[data-assistant-details]').click();assert.equal(await page.locator('[data-assistant-submit]').isDisabled(),true);
 await page.evaluate(()=>{document.querySelector('[data-assistant-text]').value='не отправлять';document.querySelector('[data-assistant-form]').dispatchEvent(new Event('submit',{cancelable:true}));authNotify({status:'signed_in',user:{id:'owner-b',is_anonymous:false}});restoreGates.owner();});
 await page.waitForFunction(()=>window.restoreGates['owner-b']);assert.equal(await page.locator('[data-assistant-submit]').isDisabled(),true);
 await page.evaluate(()=>restoreGates['owner-b']());await page.waitForFunction(()=>document.querySelector('[data-assistant]').dataset.assistantStartup==='ready');assert.equal(await page.locator('[data-assistant-submit]').isDisabled(),false);assert.equal(await page.evaluate(()=>window.requests),0);assert.deepEqual(errors,[]);await context.close();
});
test('checking to signed-out and signed-in updates readiness without automatic microphone access',async()=>{
 const {context,page,errors}=await setup('checking');await page.waitForFunction(()=>document.querySelector('[data-assistant]').dataset.assistantStartup==='checking_auth');
 await page.locator('[data-assistant-launcher]').click();assert.match(await page.locator('[data-assistant-live]').textContent(),/Проверяю вход/);assert.equal(await page.evaluate(()=>micCalls),0);
 await page.evaluate(()=>authNotify({status:'signed_out',user:null,message:'Войдите'}));assert.equal(await page.locator('[data-assistant]').getAttribute('data-assistant-startup'),'signed_out');
 await page.evaluate(()=>authNotify({status:'signed_in',user:{id:'owner',is_anonymous:false}}));await page.waitForFunction(()=>document.querySelector('[data-assistant]').dataset.assistantStartup==='ready');assert.equal(await page.evaluate(()=>micCalls),0);assert.deepEqual(errors,[]);await context.close();
});
