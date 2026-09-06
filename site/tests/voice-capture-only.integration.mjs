// Real Chromium + IndexedDB + production mount/client; injected Auth snapshot and
// synthetic persisted WAV fixture. NO physical microphone, real login or ASR claim.
import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {build} from 'esbuild';
import {chromium} from 'playwright';
let server,browser,origin,assistantRequests=0;
const root=new URL('../',import.meta.url).pathname;
const controls=['record','stop','submit','new','resume','latest','save-retry','history-load','login'];
const labels=['processing','capture','base','answers','history-list','recording-list','auth'];
const html=`<!doctype html><div data-authorized-search><div data-assistant data-assistant-capture-only="true"><div data-assistant-composer>${controls.map(n=>`<button type="button" data-assistant-${n} ${n==='login'?'':'disabled'}>${n}</button>`).join('')}${labels.map(n=>`<div data-assistant-${n}></div>`).join('')}<form data-assistant-form><textarea data-assistant-text disabled></textarea></form></div></div></div><script type="module" src="/bundle.js"></script>`;
test.before(async()=>{
 const bundle=(await build({stdin:{contents:`
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
 await mountConversationalSearch(document.querySelector('[data-assistant]'));window.ready=true;
 `,resolveDir:root,loader:'ts'},bundle:true,write:false,format:'esm',plugins:[{name:'auth-fixture-only',setup(b){b.onResolve({filter:/^\.\.\/staticSiteAuth$/},()=>({path:'auth-fixture',namespace:'fixture'}));b.onLoad({filter:/.*/,namespace:'fixture'},()=>({contents:'export function getStaticSiteAuth(){return window.auth;}',loader:'js'}));}}]})).outputFiles[0].text;
 server=createServer((req,res)=>{if(req.url.startsWith('/functions/v1/event-search/assistant')){assistantRequests++;res.statusCode=500;res.end('{}');return;}res.setHeader('Content-Type',req.url==='/bundle.js'?'text/javascript':'text/html');res.end(req.url==='/bundle.js'?bundle:html);});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));origin=`http://127.0.0.1:${server.address().port}`;
 browser=await chromium.launch({channel:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH?undefined:'chromium',executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox']});
});
test.after(async()=>{await browser?.close();await new Promise(r=>server?.close(r));});
test('capture-only mount/reload keeps local playback, denies submit/history/status and respects signout',async()=>{
 const context=await browser.newContext();const page=await context.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));
 let observed=0;page.on('request',r=>{if(r.url().includes('/assistant/'))observed++;});
 await page.goto(origin);await page.waitForFunction(()=>window.ready);
 for(let pass=0;pass<2;pass++){
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
