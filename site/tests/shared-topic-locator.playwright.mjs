// Shared-shell boundary fixture on a REAL built /poisk/ route, not ASR E2E.
import {chromium} from 'playwright';
import assert from 'node:assert/strict';
import {mkdirSync,writeFileSync,readFileSync} from 'node:fs';
import {stripTypeScriptTypes} from 'node:module';
const base=process.env.CHECK_BASE,out=process.env.CHECK_OUTPUT;
if(!base||!out)throw Error('CHECK_BASE and CHECK_OUTPUT are required');
const sourceFixture=process.env.CHECK_LOCATOR_SOURCE==='1';
const component=sourceFixture?readFileSync(new URL('../src/components/SharedSearchTopicLocator.astro',import.meta.url),'utf8'):'';
const source=sourceFixture?stripTypeScriptTypes(readFileSync(new URL('../src/lib/assistant/sharedTopicLocator.ts',import.meta.url),'utf8')).replaceAll('export ',''):'';
const css=component.match(/<style>([\s\S]*?)<\/style>/)?.[1].replaceAll('.shared-search-topic-locator','.shared-search-topic-locator[data-search-topic-locator]').replace(/:global\(([^)]+)\)/g,'$1')||'';
mkdirSync(out,{recursive:true});const browser=await chromium.launch({executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH||undefined}),checks=[];
for(const width of [320,390,430,1440]) {
  const page=await browser.newPage({viewport:{width,height:900}});
  // Isolate the shell adapter fixture from the real Search state owner.
  await page.route('**/ConversationalSearch*.js',route=>route.fulfill({contentType:'text/javascript',body:''}));
  // Remove the old inline locator controller; reset old linked CSS constraints
  // before applying the actual local component CSS (no synthetic replacement shell).
  if(sourceFixture)await page.route('**/poisk/',async route=>{const response=await route.fetch();const html=(await response.text()).replace(/<script\b[^>]*>[\s\S]*?<\/script>/g,tag=>tag.includes('data-search-topic-locator')?'':tag).replace(/\.shared-search-topic-locator[^{}]*\{[^}]*\}/g,'');await route.fulfill({response,body:html.replace('</body>',`<style>.shared-search-topic-locator[data-search-topic-locator] {max-height:none;-webkit-line-clamp:unset;}${css}</style><script type="module">${source};bindSharedTopicLocator(document.querySelector('[data-search-topic-locator]'));</script></body>`)});});
  await page.goto(`${base}/poisk/`);await page.evaluate(()=>document.fonts.ready);
  const locator=page.locator('[data-search-topic-locator]');assert.equal(await locator.count(),1);
  const first='Джаз в Калининграде завтра вечером',second='Бесплатные выставки современного искусства, экскурсии и вечерние концерты в Калининграде и Светлогорске в следующие выходные без предварительной регистрации';
  await page.evaluate(({first,second})=>{
    const main=document.querySelector('main');
    const fixture=document.createElement('div');fixture.dataset.topicFixture='';fixture.style.paddingTop='300px';
    for(const [id,title] of [['topic-a',first],['topic-b',second]]) {
      const question=document.createElement('p');question.dataset.questionBubble='';question.textContent='Полный исходный вопрос пользователя сохраняется отдельным сообщением, не заменяется коротким названием темы.';
      const section=document.createElement('section');section.dataset.assistantSection=id;section.dataset.assistantTurn='ready';section.className='assistant__turn';section.id=`assistant-answer-${id}`;section.style.minHeight='1000px';
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
      document.getElementById(`assistant-answer-${id}`).scrollIntoView({behavior:'instant'});window.scrollBy(0,32);
      window.dispatchEvent(new CustomEvent('kenigevents:search-context-changed',{detail:window.__topicState}));
    },{id,title});await page.waitForTimeout(150);
    if(width<760){
      assert.equal(await locator.isVisible(),true);assert.equal(await locator.textContent(),title);
      assert.equal(await locator.getAttribute('title'),title);
      const r=await locator.boundingBox(),brand=await page.locator('[data-reference4-fullscreen] > summary').boundingBox();
      assert.ok(r.x>=brand.x+brand.width+11||r.y>=brand.y+brand.height+11);assert.ok(r.x+r.width<=width-11);
      assert.equal(await locator.evaluate(n=>n.scrollHeight<=n.clientHeight&&n.scrollWidth<=n.clientWidth),true,'full title has no internal clipping or scrolling');
      assert.ok(await locator.evaluate(n=>parseFloat(getComputedStyle(document.documentElement).getPropertyValue('--ke-assistant-locator-edge'))>=n.getBoundingClientRect().bottom),'shared geometry includes measured island');
      assert.equal(await locator.evaluate(n=>getComputedStyle(n).position),'absolute');
    }
    if(width>=760){
      assert.equal(await locator.isVisible(),true);assert.equal(await locator.textContent(),title);
      const toggle=page.locator('[data-search-nav-toggle]');assert.equal(await toggle.isVisible(),true);
      await toggle.click();assert.equal(await page.locator('.site-nav').isVisible(),true);
      await page.keyboard.press('Escape');assert.equal(await page.locator('.site-nav').isVisible(),false);
    }
    assert.equal(await locator.evaluate(n=>n.scrollHeight<=n.clientHeight&&n.scrollWidth<=n.clientWidth),true,'desktop/mobile full title without internal scroll');
    assert.equal(await page.locator(`#assistant-answer-${id} > h2`).textContent(),title);
    assert.ok(await page.locator(`#assistant-answer-${id}`).evaluate(n=>parseFloat(getComputedStyle(n).scrollMarginTop)>=parseFloat(getComputedStyle(document.documentElement).getPropertyValue('--ke-assistant-locator-edge'))+11),'real turn scroll margin clears island');
  }
  await page.screenshot({path:`${out}/${width}-topic-normal.png`});
  if(width<760){
    await page.addStyleTag({content:'.shared-search-topic-locator[data-search-topic-locator] {font-size:28px!important;line-height:40px!important;}'});
    await page.waitForTimeout(150);
    const r=await locator.boundingBox(),brand=await page.locator('[data-reference4-fullscreen] > summary').boundingBox();
    assert.ok(r.x>=brand.x+brand.width+11||r.y>=brand.y+brand.height+11,'200% text clears leather tag');
    assert.ok(r.y+r.height<800,'200% title remains readable above bottom navigation');
    assert.equal(await locator.textContent(),second);
    assert.equal(await locator.evaluate(n=>n.scrollHeight<=n.clientHeight&&n.scrollWidth<=n.clientWidth),true,'200% text fully visible');
  }
  await page.screenshot({path:`${out}/${width}-topic.png`});
  // A shorter next title must not shrink the selection threshold back over its
  // own heading. Exercise the same geometry -> context feedback as Search.
  const boundary=await page.evaluate(async first=>{
    const root=document.documentElement,edge=()=>parseFloat(getComputedStyle(root).getPropertyValue('--ke-assistant-locator-edge'));
    const prior=edge(),section=document.createElement('section');section.dataset.assistantSection='topic-c';
    section.style.cssText=`position:absolute;top:${scrollY+prior-2}px;height:1000px`;
    document.querySelector('main').append(section);
    let updates=0;
    const update=()=>{updates++;const next=section.getBoundingClientRect().top<=edge()+1?'topic-c':'topic-b';
      if(window.__topicState.viewedSectionId!==next){window.__topicState={...window.__topicState,viewedSectionId:next,viewedTitle:next==='topic-c'?first:document.querySelector('#assistant-answer-topic-b h2').textContent};window.dispatchEvent(new CustomEvent('kenigevents:search-context-changed',{detail:window.__topicState}));}};
    window.addEventListener('kenigevents:search-locator-geometry',update);
    update();await new Promise(resolve=>setTimeout(resolve,300));
    const result={updates,id:window.__topicState.viewedSectionId,edge:edge(),prior};
    window.removeEventListener('kenigevents:search-locator-geometry',update);section.remove();return result;
  },first);
  assert.equal(boundary.id,'topic-c');assert.ok(boundary.updates<=2,'no geometry/context oscillation');assert.ok(boundary.edge>=boundary.prior);
  if(width<760){await page.locator('[data-reference4-fullscreen] > summary').click();await page.waitForTimeout(100);assert.equal(await locator.isVisible(),false);await page.keyboard.press('Escape');}
  await page.evaluate(()=>window.dispatchEvent(new CustomEvent('kenigevents:search-context-changed',{detail:{viewedSectionId:'topic-b',viewedTitle:null}})));
  await page.waitForTimeout(100);assert.equal(await locator.isVisible(),false,'missing LLM title never substitutes question');
  checks.push({width,sourceFixture,titleLength:second.length,fullTitle:true,textEnlargement200:width<760,boundary,semanticHeadingsIntact:true,rawQuestionSeparate:true,contextBoundary:true,mobileGeometry:width<760});await page.close();
}
await browser.close();writeFileSync(`${out}/checks.json`,JSON.stringify(checks,null,2));console.log('PASS shared topic locator boundary',checks.length);
