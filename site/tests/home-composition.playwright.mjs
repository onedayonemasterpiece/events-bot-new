// Focused shared-controller fixture by default. CHECK_BASE adds actual integrated
// route acceptance; a fixture result never stands in for a published home render.
import {chromium} from 'playwright';
import {readFileSync,mkdirSync,writeFileSync} from 'node:fs';
import {stripTypeScriptTypes} from 'node:module';
import assert from 'node:assert/strict';
const out=process.env.CHECK_OUTPUT||'artifacts/codex/home-composition';
mkdirSync(out,{recursive:true});
const browser=await chromium.launch({executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH||undefined});
const checks=[];
try{
 const page=await browser.newPage();
 await page.setContent('<main><section data-home-search-entry data-search-enabled="true" data-home-search-state="idle"></section><section data-hero-talk-page-end data-hero-talk-route="home"><a href="/preview/poisk/">Перейти к поиску</a></section></main>');
 const module=stripTypeScriptTypes(readFileSync(new URL('../src/lib/heroTalkPlacement.ts',import.meta.url),'utf8')).replaceAll('export ','');
 await page.addScriptTag({type:'module',content:`${module};window.unbind=bindHeroTalkPageEnd(document.querySelector('[data-hero-talk-page-end]'));window.fixtureReady=true;`});
 await page.waitForFunction(()=>window.fixtureReady);
 const end=page.locator('[data-hero-talk-page-end]');
 for(const [state,enabled,visible] of [['idle','true',true],['requesting','true',false],['recording','true',false],['saving','true',false],['submitted','true',false],['disabled','false',false],['signed-out','false',true],['error','false',false],['error','true',true]]){
  await page.evaluate(({state,enabled})=>{const entry=document.querySelector('[data-home-search-entry]');entry.dataset.homeSearchState=state;entry.dataset.searchEnabled=enabled;},{state,enabled});
  await page.waitForTimeout(20);assert.equal(await end.isVisible(),visible,state);
 }
 await end.evaluate(n=>n.dataset.heroTalkSuppressed='true');await page.waitForTimeout(20);assert.equal(await end.isVisible(),false);
 await end.evaluate(n=>n.dataset.heroTalkSuppressed='false');await page.waitForTimeout(20);assert.equal(await end.isVisible(),true);
 await page.evaluate(()=>window.unbind());
 checks.push({kind:'controller-fixture',auth_mode:'mocked_ui',states:9,suppression:true,pass:true});await page.close();
 if(process.env.CHECK_BASE){
  const base=process.env.CHECK_BASE.replace(/\/$/,'');
  for(const [width,height] of [[1440,900],[1920,1080],[390,844],[360,844]]){
   const page=await browser.newPage({viewport:{width,height},reducedMotion:'reduce'});
   await page.goto(`${base}/`);await page.evaluate(()=>document.fonts.ready);
   assert.equal(await page.locator('body').getAttribute('data-shell-composition'),'home-navigation-only');
   assert.equal(await page.locator('[data-floating-top-band],[data-floating-page-context]').count(),0);
   assert.equal(await page.locator('[data-mobile-bottom-nav]').count(),1);
   assert.equal(await page.locator('[data-mobile-nav-section="afisha"]').getAttribute('aria-current'),'page');
   const order=await page.evaluate(()=>['[data-home-hero-talk]','[data-home-search-entry]','[data-home-quick-nav]','[data-home-cold-start-feed]','[data-hero-talk-page-end]'].map(selector=>{const el=document.querySelector(selector);return el&&!el.hidden?el.getBoundingClientRect().top:null;}));
   for(let i=1;i<order.length;i++)if(order[i]!==null&&order[i-1]!==null)assert.ok(order[i]>=order[i-1],`block order ${i}`);
   assert.equal(await page.locator('[data-reference4-fullscreen]').count(),1);
   if(width>=760) { assert.equal(await page.locator('.site-nav').evaluate(n=>getComputedStyle(n).position),'fixed'); }
   else { assert.equal(await page.locator('.mobile-discovery-menu__summary').isVisible(),true); }
   assert.equal(await page.locator('[data-home-quick-nav] a').count(),6);
   for(const href of await page.locator('[data-home-quick-nav] a').evaluateAll(nodes=>nodes.map(n=>n.getAttribute('href'))))assert.ok(href.startsWith(new URL(base).pathname.replace(/\/$/,'')+'/'));
   await page.screenshot({path:`${out}/home-${width}-top.png`});
   const pageEnd=page.locator('[data-hero-talk-page-end]');
   if(await pageEnd.isVisible()){
    await pageEnd.locator('a').focus();await pageEnd.scrollIntoViewIfNeeded();
    const rect=await pageEnd.boundingBox(),dock=await page.locator('[data-mobile-bottom-nav]').boundingBox();
    assert.ok(rect.y+rect.height<=dock.y,'page-end remains above lower navigation');
   }else await page.evaluate(()=>window.scrollTo(0,document.body.scrollHeight));
   assert.equal(await page.locator('[data-floating-top-band],[data-floating-page-context]').count(),0);
   await page.screenshot({path:`${out}/home-${width}-end.png`});
   checks.push({kind:'integrated-route',width,height,order,pageEndVisible:await pageEnd.isVisible(),pass:true});await page.close();
  }
 }
 writeFileSync(`${out}/checks.json`,JSON.stringify({checks,integrated:!!process.env.CHECK_BASE},null,2));
 console.log(`PASS home composition: ${checks.length} checks; integrated=${!!process.env.CHECK_BASE}`);
}finally{await browser.close();}
