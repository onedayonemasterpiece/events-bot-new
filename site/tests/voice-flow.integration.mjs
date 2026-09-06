// Production mount/IndexedDB with synthetic capture/provider/controller fixtures.
// Tests orchestration only; does not claim live ASR, Auth, retrieval or phone.
import test from 'node:test';import assert from 'node:assert/strict';
import {createServer} from 'node:http';import {build} from 'esbuild';import {chromium} from 'playwright';
let server,browser,origin;
const fixture={
 '../staticSiteAuth':`export const getStaticSiteAuth=()=>({subscribe(fn){window.authNotify=fn;},async initialize(){window.authNotify({status:'signed_in',user:{id:'owner',is_anonymous:false}});},async signIn(){return false;}});`,
 './microphoneCapture.ts':`export class MicrophoneCapture{status='idle';constructor(options){this.options=options;}unsavedParts(){return [];}async start(){this.status='recording';this.options.onStatus('recording');}async stop(reason='user'){this.status='saved';this.options.onStatus('saved');const receipt={complete:reason==='user',reason,frames:0,savedFrames:0,sampleRate:16000,partCount:0};this.options.onStopped(receipt);return receipt;}}`,
 './assistantClient.ts':`export class AssistantClient{async transcribe(){window.asrCalls++;return new Promise(resolve=>window.resolveAsr=resolve);}async status(){return {state:'pending'};}}`,
 './conversationController.ts':`export class ConversationController{async initialize(){}async newTask(){}async rememberSection(){}}`
};
test.before(async()=>{
 const bundle=(await build({stdin:{contents:`import{mountConversationalSearch}from'./src/lib/assistant/conversationalSearch.ts';window.asrCalls=0;await mountConversationalSearch(document.querySelector('[data-assistant]'));window.ready=true;`,resolveDir:new URL('../',import.meta.url).pathname,loader:'ts'},bundle:true,write:false,format:'esm',plugins:[{name:'explicit-flow-fixtures',setup(b){b.onResolve({filter:/.*/},args=>Object.hasOwn(fixture,args.path)?{path:args.path,namespace:'fixture'}:null);b.onLoad({filter:/.*/,namespace:'fixture'},args=>({contents:fixture[args.path],loader:'js'}));}}]})).outputFiles[0].text;
 const controls=['record','stop','submit','new','resume','latest','save-retry','history-load','login'];const labels=['processing','capture','base','answers','history-list','recording-list','auth'];
 const html=`<!doctype html><div data-authorized-search><div data-assistant><div data-assistant-composer>${controls.map(n=>`<button type="button" data-assistant-${n}>${n}</button>`).join('')}${labels.map(n=>`<div data-assistant-${n}></div>`).join('')}<details data-assistant-recordings></details><form data-assistant-form><textarea data-assistant-text></textarea></form></div></div></div><script type="module" src="/bundle.js"></script>`;
 server=createServer((req,res)=>{res.setHeader('Content-Type',req.url==='/bundle.js'?'text/javascript':'text/html');res.end(req.url==='/bundle.js'?bundle:html);});await new Promise(r=>server.listen(0,'127.0.0.1',r));origin=`http://127.0.0.1:${server.address().port}`;browser=await chromium.launch({executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH||undefined});
});
test.after(async()=>{await browser?.close();await new Promise(r=>server?.close(r));});
for(const scenario of ['normal','edited','interrupted'])test(`capture to editable transcript: ${scenario}`,async()=>{
 const context=await browser.newContext();const p=await context.newPage();const errors=[];p.on('pageerror',e=>errors.push(e.message));await p.goto(origin);await p.waitForFunction(()=>window.ready);await p.locator('[data-assistant-record]').click();
 if(scenario==='interrupted'){await p.evaluate(()=>window.KenigEventsSearchAdapterV1.beforeOverlayOpen());await p.waitForFunction(()=>document.querySelector('[data-assistant-recording-list]').children.length>0);assert.equal(await p.evaluate(()=>window.asrCalls),0);}
 else{await p.locator('[data-assistant-stop]').click();await p.waitForFunction(()=>window.asrCalls===1);if(scenario==='edited')await p.locator('textarea').fill('Мой исправленный запрос');await p.evaluate(()=>window.resolveAsr({state:'completed',result:{text:'завтра в Светлогорске',uncertain:[]}}));await p.waitForFunction(()=>document.querySelector('[data-assistant-processing]').textContent.includes('готов')||document.querySelector('[data-assistant-processing]').textContent.includes('Поздняя'));assert.equal(await p.locator('textarea').inputValue(),scenario==='edited'?'Мой исправленный запрос':'завтра в Светлогорске');assert.equal(await p.evaluate(()=>window.asrCalls),1);}
 assert.deepEqual(errors,[]);await context.close();
});
