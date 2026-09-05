#!/usr/bin/env node
/** Generated-route diagnostic only. No publication, private sessions or product writes. */
import assert from 'node:assert/strict';
import {readFileSync,writeFileSync,mkdirSync} from 'node:fs';
import {dirname,resolve,join} from 'node:path';
import {chromium} from 'playwright';
import {startReleaseServer} from '../../scripts/check-browser-release-gate.mjs';
const [receiptPath, outputArg]=process.argv.slice(2);
if(!receiptPath||!outputArg)throw new Error('Expected receipt and output directory');
const receipt=JSON.parse(readFileSync(receiptPath,'utf8'));
assert.equal(receipt.schema,'kenigevents.local-focused-preview-result.v2');
const output=resolve(outputArg);mkdirSync(output,{recursive:true});
const {buildId,selection}=receipt;
assert.match(buildId,/^preview-islands-[a-z0-9-]+$/);
assert.ok(['/populyarnoe/','/podborki/besplatnye-sobytiya/'].includes(selection.exactRoute));
const server=await startReleaseServer(resolve(dirname(receiptPath),'..'),`/${buildId}`);
let browser;const results=[];let browserVersion=null;
const selectors=['.site-header__brand-tag','.mobile-discovery-menu__summary'];
try{
  browser=await chromium.launch({headless:true});browserVersion=browser.version();
  for(const viewport of [{width:390,height:844},{width:1280,height:800},{width:1920,height:1080}]){
    const context=await browser.newContext({viewport,reducedMotion:'reduce'});
    await context.route('**/*',route=>{
      const r=route.request(),u=new URL(r.url());
      if(u.origin===server.origin)return route.continue();
      if(r.method()==='GET'&&['kenigevents.ru','static.kenigevents.ru'].includes(u.hostname)&&['image','font','stylesheet'].includes(r.resourceType()))return route.continue();
      return route.abort();
    });
    const page=await context.newPage();
    const result={viewport,checks:[],pageErrors:[],screenshots:[]};
    const check=(name,ok,detail=null)=>result.checks.push({name,ok:Boolean(ok),detail});
    page.on('pageerror',e=>result.pageErrors.push(e.message));
    const url=`${server.origin}/${buildId}${selection.exactRoute}`;
    const capture=async name=>{const file=`${viewport.width}-${name}.png`;await page.screenshot({path:join(output,file)});result.screenshots.push(file);};
    const snapshot=()=>page.evaluate(selectors=>({
      brand:selectors.flatMap(selector=>[...document.querySelectorAll(selector)].map(el=>{
        const r=el.getBoundingClientRect(),s=getComputedStyle(el);
        return {selector,x:r.x,y:r.y,width:r.width,height:r.height,display:s.display,visibility:s.visibility,transform:s.transform};
      })),
      nav:(()=>{const el=document.querySelector('[data-mobile-bottom-nav]');if(!el)return null;
        const r=el.getBoundingClientRect(),s=getComputedStyle(el);
        return {x:r.x,y:r.y,width:r.width,height:r.height,background:s.backgroundColor,border:s.border,shadow:s.boxShadow,
          icons:[...el.querySelectorAll('.ke-icon-role')].map(e=>e.getBoundingClientRect().width),
          links:[...el.querySelectorAll('a')].map(e=>({href:e.getAttribute('href'),label:e.textContent.trim(),current:e.getAttribute('aria-current')}))};})(),
      headings:[...document.querySelectorAll('h1')].map(e=>e.textContent.trim()),
      cityCount:document.querySelectorAll('[data-listing-city-input]').length,
      overflow:document.documentElement.scrollWidth-document.documentElement.clientWidth
    }),selectors);
    try{
      let r=await page.goto(url+'?islands=off',{waitUntil:'networkidle'});
      await page.evaluate(()=>document.fonts.ready);await page.waitForTimeout(200);
      check('baseline-http',r?.status()===200);const baseline=await snapshot();await capture('baseline-top');
      let beforeScroll;
      if(selection.exactRoute.includes('besplatnye')){
        await page.evaluate(()=>scrollTo(0,600));await page.waitForTimeout(150);beforeScroll=await snapshot();
      }
      r=await page.goto(url,{waitUntil:'networkidle'});await page.evaluate(()=>document.fonts.ready);await page.waitForTimeout(200);
      const candidate=await snapshot();await capture('candidate-top');
      check('candidate-http',r?.status()===200);
      check('candidate-enabled',Boolean(await page.locator('html').getAttribute('data-fi-review')));
      check('brand-unchanged',JSON.stringify(baseline.brand)===JSON.stringify(candidate.brand),{baseline:baseline.brand,candidate:candidate.brand});
      check('same-h1',JSON.stringify(baseline.headings)===JSON.stringify(candidate.headings));
      check('lower-island-restored-exactly',JSON.stringify(baseline.nav)===JSON.stringify(candidate.nav),{baseline:baseline.nav,candidate:candidate.nav});
      check('no-horizontal-overflow',candidate.overflow<=1,candidate.overflow);
      check('same-city-control-set',baseline.cityCount===candidate.cityCount);
      if(selection.exactRoute==='/populyarnoe/'&&viewport.width===390){
        const toggle=page.locator('[data-fi-city-toggle]').first(),panel=page.locator('[data-fi-city-panel]').first();
        check('city-trigger',await toggle.isVisible());await toggle.click();await page.waitForTimeout(100);
        check('city-panel',await panel.isVisible());await capture('cities-open');
        const field=page.locator('[data-listing-city-input]').first();await field.locator('..').click();await page.waitForTimeout(100);
        check('real-selection',await field.isChecked());check('summary-updated',!(await toggle.innerText()).includes('Все города'));
        await page.locator('[data-fi-city-close]').first().click();
        check('close-focus',await toggle.evaluate(el=>el===document.activeElement));
        check('close-state',await toggle.getAttribute('aria-expanded')==='false');
        await toggle.click();await page.keyboard.press('Escape');
        check('escape-close',await toggle.getAttribute('aria-expanded')==='false');
      }
      if(selection.exactRoute.includes('besplatnye')){
        await page.evaluate(()=>scrollTo(0,600));await page.waitForTimeout(150);
        const mark=await page.locator('[data-fi-free-mark]').boundingBox(),island=await page.locator('[data-fi-free-context]').boundingBox();
        check('free-mark-visible',mark&&mark.x>=0&&mark.y>=0&&mark.x+mark.width<=viewport.width&&mark.y+mark.height<=viewport.height,mark);
        check('no-empty-title-plane',mark&&island&&Math.abs(mark.width-island.width)<=1,{mark,island});
        check('no-duplicate-free-title',await page.locator('[data-floating-page-title]').evaluate(el=>getComputedStyle(el).display==='none'));
        const after=await snapshot();check('brand-scroll-unchanged',JSON.stringify(beforeScroll?.brand)===JSON.stringify(after.brand));
        await capture('free-scrolled');
      }
    }catch(e){check('execution',false,e.message);await capture('failure').catch(()=>{});}
    check('no-pageerrors',result.pageErrors.length===0);results.push(result);await context.close();
  }
}finally{if(browser)await browser.close();await server.close();}
const pass=results.length===3&&results.every(r=>r.checks.every(c=>c.ok));
const summary={schema:'kenigevents.floating-islands-diagnostic.v1',verdict:pass?'DIAGNOSTIC_PASS_NOT_ACCEPTANCE':'DIAGNOSTIC_FAIL',sourceSha:receipt.sourceSha,buildId,target:selection.exactRoute,dataIdentity:receipt.dataIdentity,authMode:'anonymous',platform:'desktop-chromium-with-mobile-viewports',kaggle:false,published:false,nativePenpotVerified:false,noProductionWrites:true,browserVersion,results};
writeFileSync(join(output,'qa-summary.json'),JSON.stringify(summary,null,2)+'\n');
console.log(JSON.stringify({verdict:summary.verdict,sourceSha:summary.sourceSha,target:summary.target}));
if(!pass)process.exitCode=1;
