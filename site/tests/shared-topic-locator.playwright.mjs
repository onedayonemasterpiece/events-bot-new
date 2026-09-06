// Shared-shell boundary fixture on a REAL built /poisk/ route, not ASR E2E.
import {chromium} from 'playwright';
import assert from 'node:assert/strict';
import {mkdirSync,writeFileSync} from 'node:fs';
const base=process.env.CHECK_BASE,out=process.env.CHECK_OUTPUT;
if(!base||!out)throw Error('CHECK_BASE and CHECK_OUTPUT are required');
mkdirSync(out,{recursive:true});const browser=await chromium.launch({executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH||undefined}),checks=[];
for(const width of [320,390,430,1440]) {
  const page=await browser.newPage({viewport:{width,height:900}});
  // Isolate the shell adapter fixture from the real Search state owner.
  await page.route('**/ConversationalSearch*.js',route=>route.fulfill({contentType:'text/javascript',body:''}));
  await page.goto(`${base}/poisk/`);await page.evaluate(()=>document.fonts.ready);
  const locator=page.locator('[data-search-topic-locator]');assert.equal(await locator.count(),1);
  const first='Джаз в Калининграде завтра вечером',second='Бесплатные выставки в Светлогорске в следующие выходные';
  await page.evaluate(({first,second})=>{
    const main=document.querySelector('main');
    const fixture=document.createElement('div');fixture.dataset.topicFixture='';fixture.style.paddingTop='300px';
    for(const [id,title] of [['topic-a',first],['topic-b',second]]) {
      const question=document.createElement('p');question.dataset.questionBubble='';question.textContent='Полный исходный вопрос пользователя сохраняется отдельным сообщением, не заменяется коротким названием темы.';
      const section=document.createElement('section');section.dataset.assistantSection=id;section.id=`assistant-answer-${id}`;section.style.minHeight='1000px';
      const heading=document.createElement('h2');heading.textContent=title;section.append(heading);fixture.append(question,section);
    }
    main.append(fixture);
    window.__topicState={viewedSectionId:'topic-a',viewedTitle:first,refinementBaseId:null,pendingDraftId:null,capture:'idle'};
    window.KenigEventsSearchAdapterV1={version:'1.0.0',element:fixture,getState:()=>window.__topicState,showComposer(){},showSection(){},async beforeOverlayOpen(){},diagnostic(){return{};}};
    window.dispatchEvent(new CustomEvent('kenigevents:search-adapter-ready'));
  },{first,second});
  await page.waitForTimeout(100);assert.equal(await locator.isVisible(),false,'no premature fixed topic');
  for(const [id,title] of [['topic-a',first],['topic-b',second]]) {
    await page.evaluate(({id,title})=>{
      window.__topicState={...window.__topicState,viewedSectionId:id,viewedTitle:title};
      document.getElementById(`assistant-answer-${id}`).scrollIntoView({behavior:'instant'});
      window.dispatchEvent(new CustomEvent('kenigevents:search-context-changed',{detail:window.__topicState}));
    },{id,title});await page.waitForTimeout(150);
    if(width<760){
      assert.equal(await locator.isVisible(),true);assert.equal(await locator.textContent(),title);
      assert.equal(await locator.getAttribute('title'),title);
      const r=await locator.boundingBox(),brand=await page.locator('[data-reference4-fullscreen] > summary').boundingBox();
      assert.ok(r.x>=brand.x+brand.width+11);assert.ok(r.x+r.width<=width-11);assert.ok(r.height<=64);
      assert.equal(await locator.evaluate(n=>getComputedStyle(n).position),'absolute');
    }
    assert.equal(await page.locator(`#assistant-answer-${id} > h2`).textContent(),title);
  }
  await page.screenshot({path:`${out}/${width}-topic.png`});
  if(width<760){await page.locator('[data-reference4-fullscreen] > summary').click();await page.waitForTimeout(100);assert.equal(await locator.isVisible(),false);await page.keyboard.press('Escape');}
  await page.evaluate(()=>window.dispatchEvent(new CustomEvent('kenigevents:search-context-changed',{detail:{viewedSectionId:'topic-b',viewedTitle:null}})));
  await page.waitForTimeout(100);assert.equal(await locator.isVisible(),false,'missing LLM title never substitutes question');
  checks.push({width,semanticHeadingsIntact:true,rawQuestionSeparate:true,contextBoundary:true,mobileGeometry:width<760});await page.close();
}
await browser.close();writeFileSync(`${out}/checks.json`,JSON.stringify(checks,null,2));console.log('PASS shared topic locator boundary',checks.length);
