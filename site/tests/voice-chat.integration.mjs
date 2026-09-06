// Native browser + real durable controller; explicitly gated provider/Auth fixtures.
// Proves stage ordering/UI, NOT actual ASR or physical microphone quality.
import test from 'node:test';import assert from 'node:assert/strict';
import {createServer} from 'node:http';import {readFile} from 'node:fs/promises';
import {build} from 'esbuild';import {chromium} from 'playwright';
let browser,server,origin;
const root=new URL('../',import.meta.url).pathname;
const controls=['record','stop','submit','new','resume','latest','save-retry','history-load','login'];
const labels=['processing','capture','base','history-list','recording-list','auth'];
const html=`<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><style>[hidden]{display:none!important}[data-assistant-turn]{min-height:900px;scroll-margin-top:120px}button{min-height:44px}</style><div data-authorized-search><div data-assistant data-assistant-worklet="/worklet.js"><div data-assistant-dock><span data-assistant-live></span><span data-assistant-timer hidden></span><button data-assistant-launcher>Микрофон</button><button data-assistant-details>Написать</button></div><div data-assistant-composer hidden><button data-assistant-close>Свернуть</button>${controls.map(n=>`<button type="button" data-assistant-${n}>${n}</button>`).join('')}${labels.map(n=>`<div data-assistant-${n}></div>`).join('')}<form data-assistant-form><textarea data-assistant-text></textarea></form></div><div data-assistant-answers></div></div></div><script type="module" src="/bundle.js"></script>`;
test.before(async()=>{
 const bundle=(await build({stdin:{contents:`
 import{mountComposerPresentation}from'./src/lib/assistant/composerPresentation.ts';import{mountConversationalSearch}from'./src/lib/assistant/conversationalSearch.ts';
 import{initialState}from'./src/lib/assistant/conversationState.ts';
 window.calls=[];window.gates={};window.intent={...initialState().activeIntent,goal:'джаз',dateFrom:'2026-09-06',dateTo:null,timeOfDay:null,audience:[],timezone:'Europe/Kaliningrad'};
 window.auth={subscribe(fn){fn({status:'signed_in',user:{id:'owner'},message:''});},async initialize(){}};
 window.KenigEventsCreateEventCard=({candidate})=>{const card=document.createElement('article');card.dataset.eventCard='';card.dataset.eventId=String(candidate.event_id);card.textContent=candidate.title;return card;};window.KenigEventsResolveMobileEventCardMedia=()=>({});
 window.api={async history(){return{items:[]};},async status(){throw Error('unused');},async control(o,id,kind,payload){calls.push({id,kind,payload});return{state:'accepted'};},async execute(o,id,kind){calls.push({id,kind});if(kind==='interpret'){await new Promise(r=>gates.interpret=r);return{state:'completed',result:{intent:window.intent}};}await new Promise((r,j)=>{gates.answer=r;gates.fail=j;});return{state:'completed',result:{id,title:'Джаз на выходных',question:'server question',answer:'Короткий ответ по запросу.',items:[{event_id:42,title:'Джазовый вечер'}],catalog_revision:'fixture-v1'}};},async transcribe(){calls.push({kind:'asr'});await new Promise(r=>gates.asr=r);return{state:'completed',result:{text:'Я бы хотел сходить на джаз на выходных',uncertain:[]}};}};
 const root=document.querySelector('[data-assistant]');await mountConversationalSearch(root,mountComposerPresentation(root));
 `,resolveDir:root,loader:'ts'},bundle:true,write:false,format:'esm',plugins:[{name:'explicit-fixtures',setup(b){b.onResolve({filter:/^(\.\.\/staticSiteAuth|\.\/assistantClient\.ts)$/},a=>({path:a.path,namespace:'fixture'}));b.onLoad({filter:/.*/,namespace:'fixture'},a=>({contents:a.path.includes('staticSiteAuth')?'export const getStaticSiteAuth=()=>window.auth;':'export class AssistantClient{constructor(){return window.api;}}',loader:'js'}));}}]})).outputFiles[0].text;
 const worklet=await readFile(root+'public/voice/pcm-capture-worklet.js');
 server=createServer((req,res)=>{res.setHeader('Content-Type',req.url.endsWith('.js')?'text/javascript':'text/html');res.end(req.url==='/bundle.js'?bundle:req.url==='/worklet.js'?worklet:html);});await new Promise(r=>server.listen(0,'127.0.0.1',r));origin=`http://127.0.0.1:${server.address().port}`;
 browser=await chromium.launch({channel:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH?undefined:'chromium',executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH||undefined,args:['--use-fake-device-for-media-stream']});
});
test.after(async()=>{await browser?.close();await new Promise(r=>server?.close(r));});
test('ASR immediately becomes bubble + lines/cards skeleton before interpretation; next turn appends; failed turn stops shimmer',async()=>{
 const context=await browser.newContext({permissions:['microphone'],viewport:{width:390,height:844}});const p=await context.newPage();const errors=[];p.on('console',m=>{if(m.type()==='error')console.log(m.text());});p.on('pageerror',e=>errors.push(e.message));
 try{
 await p.goto(origin);await p.waitForFunction(()=>document.querySelector('[data-assistant]').dataset.assistantStartup==='ready');
 await p.locator('[data-assistant-launcher]').click();await p.waitForFunction(()=>document.querySelector('[data-assistant-dock]').dataset.captureState==='recording');await p.waitForTimeout(350);await p.locator('[data-assistant-launcher]').click();
 await p.waitForFunction(()=>window.gates.asr);assert.equal(await p.locator('[data-assistant-question]').count(),0);
 await p.evaluate(()=>gates.asr());await p.waitForFunction(()=>window.gates.interpret);
 assert.match(await p.locator('[data-assistant-question]').textContent(),/Я бы хотел/);
 assert.equal(await p.locator('[data-assistant-skeleton]').count(),1);assert.equal(await p.locator('.assistant__skeleton-lines > *').count(),3);assert.equal(await p.locator('.assistant__skeleton-card').count(),3);
 assert.equal(await p.locator('[data-assistant-composer]').isVisible(),false);assert.equal(await p.locator('[data-event-card]').count(),0);
 await p.evaluate(()=>gates.interpret());await p.waitForFunction(()=>window.gates.answer);await p.evaluate(()=>gates.answer());await p.waitForSelector('[data-event-card]');
 assert.equal(await p.locator('[data-assistant-skeleton]').count(),0);assert.equal(await p.locator('[data-assistant-question]').count(),1);assert.equal(await p.locator('[data-assistant-response]').getAttribute('aria-busy'),'false');
 await p.locator('[data-assistant-details]').click();await p.locator('[data-assistant-text]').fill('А на побережье?');await p.locator('[data-assistant-form]').evaluate(f=>f.requestSubmit());await p.waitForFunction(()=>document.querySelectorAll('[data-assistant-question]').length===2);
 assert.equal(await p.locator('[data-event-card]').count(),1);assert.equal(await p.locator('[data-assistant-skeleton]').count(),1);
 await p.waitForFunction(()=>window.calls.filter(x=>x.kind==='interpret').length===4);await p.evaluate(()=>gates.interpret());await p.waitForFunction(()=>window.calls.filter(x=>x.kind==='search').length===2);await p.evaluate(()=>gates.fail(Error('voice_outcome_unknown')));
 await p.waitForSelector('[data-assistant-turn="error"]');assert.equal(await p.locator('[data-assistant-skeleton]').count(),0);assert.equal(await p.locator('[data-assistant-question]').count(),2);assert.equal(await p.locator('[data-event-card]').count(),1);assert.deepEqual(errors,[]);
 }catch(error){console.log(JSON.stringify({errors,body:await p.locator('body').innerText(),calls:await p.evaluate(()=>window.calls)}));throw error;}finally{await context.close();}
});
