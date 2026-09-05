#!/usr/bin/env node
/** Adversarial presentation checks on generated Astro, not native keyboard/OS acceptance. */
import {readFileSync,writeFileSync,mkdirSync} from 'node:fs';
import {resolve,dirname,join} from 'node:path';
import assert from 'node:assert/strict';
import {chromium} from 'playwright';
import {startReleaseServer} from '../../scripts/check-browser-release-gate.mjs';
const [receiptPath,outArg]=process.argv.slice(2);
const receipt=JSON.parse(readFileSync(receiptPath,'utf8'));
assert.equal(receipt.schema,'kenigevents.local-focused-preview-result.v2');
const output=resolve(outArg);mkdirSync(output,{recursive:true});
const server=await startReleaseServer(resolve(dirname(receiptPath),'..'),`/${receipt.buildId}`);
const url=`${server.origin}/${receipt.buildId}${receipt.selection.exactRoute}`;
let browser;const checks=[],errors=[];let browserVersion;
const check=(name,ok,detail=null)=>checks.push({name,ok:Boolean(ok),detail});
try{
  browser=await chromium.launch({headless:true});browserVersion=browser.version();
  for(const fallback of [false,true]){
    const context=await browser.newContext({viewport:{width:390,height:844},reducedMotion:'reduce'});
    const popular=receipt.selection.exactRoute==='/populyarnoe/';
    if(fallback&&popular)await context.addInitScript(()=>{HTMLElement.prototype.showPopover=undefined;});
    await context.route('**/*',route=>{
      const r=route.request(),u=new URL(r.url());
      if(fallback&&!popular&&u.pathname.endsWith('/free-listing-medallion.svg'))return route.abort();
      if(u.origin===server.origin)return route.continue();
      if(r.method()==='GET'&&['kenigevents.ru','static.kenigevents.ru'].includes(u.hostname)&&['image','font','stylesheet'].includes(r.resourceType()))return route.continue();
      return route.abort();
    });
    const page=await context.newPage();page.on('pageerror',e=>errors.push(e.message));
    try{
      await page.goto(url,{waitUntil:'networkidle'});await page.evaluate(()=>document.fonts.ready);await page.waitForTimeout(200);
      if(popular){
        const trigger=page.locator('[data-fi-city-toggle]').first(),panel=page.locator('[data-fi-city-panel]').first(),controls=page.locator('[data-fi-city-root]').first();
        await page.evaluate(()=>{window.__fiOriginal=document.querySelector('[data-listing-city-filter]');});
        await trigger.click();await page.waitForTimeout(100);
        check(`open-${fallback}`,await panel.isVisible());
        check(`presentation-${fallback}`,await controls.getAttribute('data-fi-city-placement')===(fallback?'inline':'popover'));
        const input=page.locator('[data-listing-city-input]').first();await input.locator('..').click();await input.focus();
        const selection=await input.isChecked();
        await page.setViewportSize({width:390,height:220});await page.waitForTimeout(200);
        check(`small-height-inline-${fallback}`,await controls.getAttribute('data-fi-city-placement')==='inline');
        check(`same-controls-small-${fallback}`,await page.evaluate(()=>window.__fiOriginal===document.querySelector('[data-listing-city-filter]')));
        check(`selection-preserved-small-${fallback}`,await input.isChecked()===selection);
        await page.screenshot({path:join(output,`390-city-small-height-${fallback}.png`)});
        await page.setViewportSize({width:390,height:844});await page.waitForTimeout(200);
        check(`restored-presentation-${fallback}`,await controls.getAttribute('data-fi-city-placement')===(fallback?'inline':'popover'));
        check(`selection-preserved-return-${fallback}`,await input.isChecked()===selection);
        check(`focus-preserved-return-${fallback}`,await input.evaluate(el=>el===document.activeElement));
        await page.keyboard.press('Escape');
        check(`escape-after-reflow-${fallback}`,await trigger.getAttribute('aria-expanded')==='false');
        check(`focus-after-reflow-${fallback}`,await trigger.evaluate(el=>el===document.activeElement));
        for(let i=0;i<3;i++){await trigger.click();await page.locator('[data-fi-city-close]').first().click();}
        check(`repeated-open-one-owner-${fallback}`,await page.locator('[data-fi-city-panel]').count()===1&&await page.locator('[data-listing-city-filter]').count()===1);
        await trigger.click();await page.locator('h1').first().click();await page.waitForTimeout(50);
        check(`outside-dismiss-${fallback}`,await trigger.getAttribute('aria-expanded')==='false');
        await page.evaluate(()=>document.querySelector('[data-fi-review-seed]').__fiReview.destroy());
        check(`cleanup-original-controls-${fallback}`,await page.evaluate(()=>window.__fiOriginal===document.querySelector('[data-listing-city-filter]')&&!document.querySelector('[data-fi-city-panel]')));
        check(`cleanup-flag-${fallback}`,await page.locator('html').getAttribute('data-fi-review')===null);
        await page.setViewportSize({width:1280,height:800});await page.waitForTimeout(100);
        check(`no-resurrection-after-cleanup-${fallback}`,await page.locator('[data-fi-city-panel]').count()===0);
      }else{
        await page.evaluate(()=>scrollTo(0,600));await page.waitForTimeout(200);
        if(fallback){
          check('asset-failure-retains-text',await page.locator('[data-floating-page-title]').evaluate(el=>getComputedStyle(el).display!=='none'));
          check('asset-failure-no-empty-identity',await page.locator('[data-fi-free-context]').count()===0);
        }else{
          check('loaded-medallion-identity',await page.locator('[data-fi-free-context]').count()===1);
          await page.locator('[data-fi-free-context] button').click();await page.waitForTimeout(100);
          check('medallion-return-to-heading',await page.evaluate(()=>scrollY<2&&document.activeElement?.tagName==='H1'));
        }
        check(`h1-retained-${fallback}`,await page.locator('h1').count()===1);
        await page.screenshot({path:join(output,`390-free-asset-fallback-${fallback}.png`)});
      }
    }catch(e){check(`execution-${fallback}`,false,e.message);await page.screenshot({path:join(output,`recovery-failure-${fallback}.png`)}).catch(()=>{});}
    await context.close();
  }
}finally{if(browser)await browser.close();await server.close();}
check('no-pageerrors',errors.length===0,errors);
const pass=checks.every(c=>c.ok);
writeFileSync(join(output,'recovery-summary.json'),JSON.stringify({sourceSha:receipt.sourceSha,buildId:receipt.buildId,target:receipt.selection.exactRoute,verdict:pass?'DIAGNOSTIC_PASS_NOT_ACCEPTANCE':'DIAGNOSTIC_FAIL',browserVersion,checks,errors,keyboard:'viewport-resize-only-not-native',published:false,nativePenpotVerified:false},null,2)+'\n');
console.log(JSON.stringify({checks:checks.length,pass,target:receipt.selection.exactRoute}));
if(!pass)process.exitCode=1;
