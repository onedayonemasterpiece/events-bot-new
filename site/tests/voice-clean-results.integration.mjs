// L1 mocked_ui: real conversational mount + IndexedDB, synthetic Auth/controller/cards.
// Proves result chrome and composer context, not live Auth, retrieval or ASR quality.
import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {build} from 'esbuild';
import {chromium} from 'playwright';
let browser,server,origin;
const fixture={
  '../staticSiteAuth':`export const getStaticSiteAuth=()=>({subscribe(fn){window.authNotify=fn;},async initialize(){window.authNotify({status:'signed_in',user:{id:'owner',is_anonymous:false}});}});`,
  './assistantClient.ts':`export class AssistantClient{async history(){return {items:[{id:'history',title:'Сохранённая подборка'}]};}async status(owner,id){return {state:'completed',result:window.result(id)};}}`,
  './conversationController.ts':`export class ConversationController{
    constructor(owner,store,api,callbacks){window.controller=this;this.owner=owner;this.callbacks=callbacks;}
    async initialize(){} async newTask(){} async rememberSection(){}
    async submit(raw,mode,parent,ids){window.submissions.push({owner:this.owner,raw,mode,parent,ids});return {searchId:'next-'+window.submissions.length};}
  }`,
};
test.before(async()=>{
  const bundle=(await build({stdin:{contents:`
    import {mountConversationalSearch} from './src/lib/assistant/conversationalSearch.ts';
    window.submissions=[];window.cardActions=[];window.registrations=[];
    window.result=(id,extra={})=>({id,title:'Джаз на выходных',question:'Что послушать?',answer:'Подборка концертов.',items:Array.from({length:15},(_,i)=>({event_id:i+1,title:'Концерт '+(i+1)})),membership_complete:false,explanationKind:'none',catalog_revision:'fixture',...extra});
    window.KenigEventsCreateEventCard=({candidate},variant)=>{
      const card=document.createElement('article');card.dataset.eventCard='';card.dataset.eventId=String(candidate.event_id);card.dataset.variant=variant;
      card.append(document.createTextNode(candidate.title));
      for(const action of ['like','share','tickets']){const button=document.createElement('button');button.textContent=action;button.dataset.cardAction=action;button.onclick=()=>window.cardActions.push(action);card.append(button);}
      return card;
    };
    window.KenigEventsResolveMobileEventCardMedia=()=>({});
    window.KenigEventsSearchCardHost={hiddenIds:()=>[],register:(grid,items,context)=>window.registrations.push({count:items.length,context}),sync:async()=>{}};
    await mountConversationalSearch(document.querySelector('[data-assistant]'));
  `,resolveDir:new URL('../',import.meta.url).pathname,loader:'ts'},bundle:true,write:false,format:'esm',plugins:[{name:'explicit-clean-results-fixtures',setup(b){
    b.onResolve({filter:/.*/},a=>Object.hasOwn(fixture,a.path)?{path:a.path,namespace:'fixture'}:null);
    b.onLoad({filter:/.*/,namespace:'fixture'},a=>({contents:fixture[a.path],loader:'js'}));
  }}]})).outputFiles[0].text;
  const buttons=['record','stop','submit','new','resume','latest','save-retry','history-load','login'];
  const labels=['processing','capture','base','history-list','recording-list','auth'];
  const html=clean=>`<!doctype html><meta charset="utf-8"><style>[hidden]{display:none!important}</style><div data-authorized-search><div data-assistant data-assistant-clean-ui="${clean}"><div data-assistant-composer>${buttons.map(n=>`<button type="button" data-assistant-${n}>${n}</button>`).join('')}${labels.map(n=>`<div data-assistant-${n}></div>`).join('')}<form data-assistant-form><textarea data-assistant-text></textarea></form><form data-assistant-quick-form><input data-assistant-quick-input></form></div><div data-assistant-answers></div></div></div><script type="module" src="/bundle.js"></script>`;
  server=createServer((req,res)=>{res.setHeader('Content-Type',req.url==='/bundle.js'?'text/javascript':'text/html');res.end(req.url==='/bundle.js'?bundle:html(req.url!=='/legacy'));});
  await new Promise(r=>server.listen(0,'127.0.0.1',r));origin=`http://127.0.0.1:${server.address().port}`;
  browser=await chromium.launch({executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH||undefined});
});
test.after(async()=>{await browser?.close();if(server)await new Promise(r=>server.close(r));});

async function open(path='/'){
  const context=await browser.newContext();const page=await context.newPage();const errors=[];
  page.on('pageerror',e=>errors.push(e.message));await page.goto(origin+path);
  await page.waitForFunction(()=>document.querySelector('[data-assistant]').dataset.assistantStartup==='ready');
  return {context,page,errors};
}
function noFooter(page){return page.locator('[data-assistant-response] > button, [data-assistant-response] > .assistant__controls, [data-assistant-response] > details');}

test('clean results have no footer or coverage details, keep all 15 cards and card actions',async()=>{
  const {context,page,errors}=await open();try{
    await page.evaluate(()=>controller.callbacks.answer(result('first'),true));
    assert.equal(await noFooter(page).count(),0);
    assert.equal(await page.getByText('Об этой подборке',{exact:true}).count(),0);
    assert.equal(await page.locator('[data-assistant-more]').count(),0);
    assert.equal(await page.locator('[data-event-card]').count(),15);
    assert.equal(await page.locator('[data-variant="split-actions"]').count(),15);
    for(const action of ['like','share','tickets'])await page.locator(`[data-card-action="${action}"]`).first().click();
    assert.deepEqual(await page.evaluate(()=>cardActions),['like','share','tickets']);
    assert.equal(await page.evaluate(()=>registrations[0].count),15);
    await page.locator('[data-assistant-quick-input]').fill('А на побережье?');
    await page.locator('[data-assistant-quick-form]').evaluate(f=>f.requestSubmit());
    await page.waitForFunction(()=>submissions.length===1);
    assert.deepEqual(await page.evaluate(()=>submissions[0]),{owner:'owner',raw:'А на побережье?',mode:'expand_selection',parent:'first',ids:Array.from({length:15},(_,i)=>String(i+1))});
    assert.deepEqual(errors,[]);
  }finally{await context.close();}
});

test('clarification and history need no footer; composer keeps active context and owner switch clears answers',async()=>{
  const {context,page,errors}=await open();try{
    await page.evaluate(()=>controller.callbacks.answer(result('clarify',{items:[],clarification:true,answer:'Какой город вам удобнее?'}),true));
    assert.equal(await noFooter(page).count(),0);
    await page.locator('[data-assistant-text]').fill('Светлогорск');
    await page.locator('[data-assistant-form]').evaluate(f=>f.requestSubmit());
    await page.waitForFunction(()=>submissions.length===1);
    assert.deepEqual(await page.evaluate(()=>submissions[0]),{owner:'owner',raw:'Светлогорск',mode:'expand_selection',parent:'clarify',ids:[]});
    await page.locator('[data-assistant-history-load]').click();
    await page.getByRole('button',{name:'Сохранённая подборка',exact:true}).click();
    await page.waitForSelector('[data-assistant-section="history"]');
    assert.equal(await noFooter(page).count(),0);
    assert.equal(await page.locator('[data-assistant-section="history"] [data-event-card]').count(),15);
    assert.equal(await page.evaluate(()=>window.KenigEventsSearchAdapterV1.getState().refinementBaseId),'clarify');
    await page.evaluate(()=>authNotify({status:'signed_in',user:{id:'other-owner',is_anonymous:false}}));
    await page.waitForFunction(()=>document.querySelector('[data-assistant]').dataset.assistantStartup==='ready');
    assert.equal(await page.locator('[data-assistant-section]').count(),0);
    assert.equal(await page.locator('[data-assistant-history-list]').textContent(),'');
    assert.equal(await page.evaluate(()=>window.KenigEventsSearchAdapterV1.getState().refinementBaseId),null);
    assert.deepEqual(errors,[]);
  }finally{await context.close();}
});

test('non-clean prototype retains explicit selection controls and pagination',async()=>{
  const {context,page,errors}=await open('/legacy');try{
    await page.evaluate(()=>controller.callbacks.answer(result('legacy'),true));
    assert.equal(await page.locator('[data-event-card]').count(),12);
    assert.equal(await page.locator('.assistant__controls button').count(),3);
    assert.equal(await page.getByText('Об этой подборке',{exact:true}).count(),1);
    await page.getByRole('button',{name:'Показать ещё',exact:true}).click();
    assert.equal(await page.locator('[data-event-card]').count(),15);
    assert.equal(await page.locator('[data-assistant-more]').isVisible(),false);
    assert.deepEqual(errors,[]);
  }finally{await context.close();}
});
