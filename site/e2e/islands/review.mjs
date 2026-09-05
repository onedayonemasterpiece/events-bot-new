#!/usr/bin/env node
/** Focused generated-page diagnostic; no publication or authenticated actions. */
import assert from 'node:assert/strict';
import {readFileSync,writeFileSync,mkdirSync} from 'node:fs';
import {dirname,resolve,join} from 'node:path';
import {chromium} from 'playwright';
import {startReleaseServer} from '../../scripts/check-browser-release-gate.mjs';
const [receiptPath, outputArg] = process.argv.slice(2);
if (!receiptPath || !outputArg) throw new Error('Usage: node site/e2e/islands/review.mjs <local-focused-receipt.json> <output-dir>');
const receipt=JSON.parse(readFileSync(receiptPath,'utf8'));
assert.equal(receipt.schema,'kenigevents.local-focused-preview-result.v2');
const output=resolve(outputArg); mkdirSync(output,{recursive:true});
const buildRoot=resolve(dirname(receiptPath),'..');
const {buildId,selection}=receipt;
assert.match(buildId,/^preview-islands-[a-z0-9-]+$/);
assert.ok(['/populyarnoe/','/podborki/besplatnye-sobytiya/'].includes(selection.exactRoute));
const server=await startReleaseServer(buildRoot,`/${buildId}`);
const browser=await chromium.launch({headless:true});
const results=[];
const allowedMedia=new Set(['kenigevents.ru','static.kenigevents.ru']);
const brandSelectors=['.site-header__brand-tag','.mobile-discovery-menu__summary'];
try {
  for(const viewport of [{width:390,height:844},{width:1280,height:800},{width:1920,height:1080}]) {
    const context=await browser.newContext({viewport,reducedMotion:'reduce'});
    await context.route('**/*',route=>{
      const request=route.request(),url=new URL(request.url());
      if(url.origin===server.origin)return route.continue();
      if(request.method()==='GET' && allowedMedia.has(url.hostname) && ['image','font','stylesheet'].includes(request.resourceType()))return route.continue();
      return route.abort(); // No product writes, Auth, provider or optional telemetry.
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
        const r=el.getBoundingClientRect();return {width:r.width,height:r.height,
        icons:[...el.querySelectorAll('.ke-icon-role')].map(e=>e.getBoundingClientRect().width),
        links:[...el.querySelectorAll('a')].map(e=>({href:e.getAttribute('href'),label:e.textContent.trim(),current:e.getAttribute('aria-current')}))};})(),
      headings:[...document.querySelectorAll('h1')].map(e=>e.textContent.trim()),
      cityCount:document.querySelectorAll('[data-listing-city-input]').length,
      overflow:document.documentElement.scrollWidth-document.documentElement.clientWidth,
    }),brandSelectors);
    try {
      let response=await page.goto(url+'?islands=off',{waitUntil:'networkidle'});
      await page.evaluate(()=>document.fonts.ready); await page.waitForTimeout(250);
      check('baseline-http',response?.status()===200);const baseline=await snapshot();await capture('baseline-top');
      response=await page.goto(url,{waitUntil:'networkidle'});
      await page.evaluate(()=>document.fonts.ready);await page.waitForTimeout(250);
      check('candidate-http',response?.status()===200);
      check('candidate-enabled',await page.locator('html').getAttribute('data-fi-review')==='floating-islands.owner-review.v1.2');
      const candidate=await snapshot(); await capture('candidate-top');
      check('brand-unchanged',JSON.stringify(baseline.brand)===JSON.stringify(candidate.brand),{baseline:baseline.brand,candidate:candidate.brand});
      check('same-h1',JSON.stringify(baseline.headings)===JSON.stringify(candidate.headings));
      check('same-nav-links',JSON.stringify(baseline.nav?.links)===JSON.stringify(candidate.nav?.links));
      check('no-page-overflow',candidate.overflow<=1,candidate.overflow);
      check('one-city-control-set',baseline.cityCount===candidate.cityCount,{before:baseline.cityCount,after:candidate.cityCount});
      if(viewport.width>=1024){
        check('larger-desktop-dock',candidate.nav?.height>baseline.nav?.height,{baseline:baseline.nav,candidate:candidate.nav});
        check('canonical-feature-icons',candidate.nav?.icons.every(n=>Math.abs(n-32)<=1),candidate.nav?.icons);
      }else check('mobile-dock-preserved',candidate.nav?.height===baseline.nav?.height && candidate.nav?.width===baseline.nav?.width);
      if(selection.exactRoute==='/populyarnoe/' && viewport.width===390){
        const toggle=page.locator('[data-fi-city-toggle]').first();
        check('city-trigger-visible',await toggle.isVisible());
        if(await toggle.isVisible()){
          await toggle.click();const panel=page.locator('[data-fi-city-panel]').first();
          await page.waitForTimeout(100);check('rectangle-visible',await panel.isVisible());await capture('cities-open');
          const field=page.locator('[data-listing-city-input]').first();
          await field.locator('..').click();
          await page.waitForTimeout(100);
          check('real-checkbox-selected',await field.isChecked());
          const text=await toggle.innerText();check('selection-summary',!text.includes('Все города'),text);
          await page.locator('[data-fi-city-close]').first().click();
          check('close-restores-focus',await toggle.evaluate(el=>el===document.activeElement));
          check('closed-state',await toggle.getAttribute('aria-expanded')==='false');
          await toggle.click();await page.keyboard.press('Escape');
          check('escape-closes',await toggle.getAttribute('aria-expanded')==='false');
        }
      }
      if(selection.exactRoute.includes('besplatnye-sobytiya')){
        await page.evaluate(()=>window.scrollTo(0,600));await page.waitForTimeout(150);
        check('free-equivalent-marker',await page.locator('[data-fi-free-mark]').count()===1);
        check('free-title-island-not-duplicated',await page.locator('[data-floating-page-title]').evaluate(el=>getComputedStyle(el).display==='none'));
        check('semantic-h1-retained',await page.locator('h1').count()>0);
        await capture('free-scrolled');
      }
    } catch(error){check('execution',false,error.message);await capture('failure').catch(()=>{});}
    check('no-uncaught-errors',result.pageErrors.length===0);
    results.push(result);await context.close();
  }
}finally{await browser.close();await server.close();}
const pass=results.every(r=>r.checks.every(c=>c.ok));
const summary={schema:'kenigevents.floating-islands-diagnostic.v1',
 verdict:pass?'DIAGNOSTIC_PASS_NOT_ACCEPTANCE':'DIAGNOSTIC_FAIL',sourceSha:receipt.sourceSha,buildId,
 target:selection.exactRoute,dataIdentity:receipt.dataIdentity,authMode:'anonymous',
 platform:'desktop-chromium-with-mobile-viewports',kaggle:false,published:false,nativePenpotVerified:false,
 noProductionWrites:true,browserVersion:browser.version(),results};
writeFileSync(join(output,'qa-summary.json'),JSON.stringify(summary,null,2)+'\n');
console.log(JSON.stringify({verdict:summary.verdict,sourceSha:summary.sourceSha,buildId,target:summary.target}));
if(!pass)process.exitCode=1;
