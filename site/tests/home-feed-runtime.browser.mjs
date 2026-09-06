// Synthetic DOM state regression only. Real card/media/Auth acceptance is the integrated preview gate.
import assert from 'node:assert/strict';
import test from 'node:test';
import { build } from 'esbuild';
import { chromium } from 'playwright';
import { fileURLToPath } from 'node:url';

const candidates=Array.from({length:45},(_,i)=>({event_id:i+1,category:`cat-${i%7}`,static_score:1-i/45,
  tags:i===40?['jazz']:[],display:{title:`Synthetic ${i+1}`,occurrence_member_ids:i===40?[41,141]:[i+1]}}));
const profile={consent_ok:true,profile_version:'anon-profile-v1',feature_schema_version:'event-detail-related-v1',taxonomy_version:'event-taxonomy-v1',
  anon_id:'11111111-1111-4111-8111-111111111111',session_id:'22222222-2222-4222-8222-222222222222',
  liked_event_ids:['501','502','503'],hidden_event_ids:[],not_interested_event_ids:[],positive_tags:{jazz:2},share_counts:{}};

test('synthetic DOM: outside-30 materialization, stable observed prefix, family hide/Undo, return, storage failure fallback',async()=>{
  const bundle=await build({stdin:{contents:"import {bindHomeFeeds} from './site/src/lib/homeFeedRuntime.ts'; bindHomeFeeds();",resolveDir:fileURLToPath(new URL('../..',import.meta.url))},bundle:true,write:false,format:'iife',platform:'browser'});
  const browser=await chromium.launch({headless:true,executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE || '/home/dev/.cache/ms-playwright/chromium-1200/chrome-linux64/chrome'});
  try {
    const page=await browser.newPage({viewport:{width:1200,height:700}});
    const errors=[];page.on('pageerror',e=>errors.push(e.message));
    await page.route('http://home.test/**',route=>route.fulfill({contentType:'text/html',body:`<!doctype html><style>body{margin:0}.spacer{height:1000px}.grid{display:grid;grid-template-columns:repeat(3,1fr)}article{height:160px}article[hidden]{display:none}</style><div class=spacer></div><section data-home-cold-start-feed><p data-home-feed-status>Общая подборка</p><script type=application/json data-home-feed-candidates>${JSON.stringify({v:1,currentDate:'2026-09-06',candidates})}</script><p data-home-feed-empty hidden></p><div class=grid data-home-feed-grid>${candidates.slice(0,30).map(x=>`<article data-home-feed-item data-event-id=${x.event_id}>${x.event_id}</article>`).join('')}</div></section>`}));
    await page.goto('http://home.test/review/');
    await page.evaluate(profile=>localStorage.setItem('ke_personalization_profile',JSON.stringify(profile)),profile);
    const bind=async()=>{
      await page.evaluate(()=>{
        window.KenigEventsCreateEventCard=item=>{const card=document.createElement('article');card.dataset.eventId=String(item.event_id);card.textContent=String(item.event_id);return card;};
        window.KenigEventsSearchCardHost={hiddenIds:()=>[],register:()=>{}};
      });
      await page.addScriptTag({content:bundle.outputFiles[0].text});
    };
    await bind();
    const ids=()=>page.locator('[data-home-feed-item]:not([hidden])').evaluateAll(nodes=>nodes.map(x=>x.dataset.eventId));
    assert.equal((await ids())[0],'41');assert.equal((await ids()).length,30);
    assert.equal(await page.locator('[data-home-feed-status]').textContent(),'По вашим действиям на сайте');
    await page.evaluate(()=>scrollTo(0,1100));await page.waitForTimeout(80);
    const before=await ids();
    await page.evaluate(()=>{
      window.__originalFirst=document.querySelector('[data-home-feed-item]');
      const p=JSON.parse(localStorage.getItem('ke_personalization_profile'));p.hidden_event_ids=['141'];localStorage.setItem('ke_personalization_profile',JSON.stringify(p));
      window.KenigEventsHomeCardHost.sync();
    });
    assert.ok(!(await ids()).includes('41'));assert.equal((await ids()).length,30);
    await page.evaluate(()=>{const p=JSON.parse(localStorage.getItem('ke_personalization_profile'));p.hidden_event_ids=[];localStorage.setItem('ke_personalization_profile',JSON.stringify(p));window.KenigEventsHomeCardHost.sync();});
    assert.deepEqual((await ids()).slice(0,9),before.slice(0,9));
    assert.ok(await page.evaluate(()=>window.__originalFirst===document.querySelector('[data-home-feed-item]')));
    const snapshot=await ids();
    await page.evaluate(()=>dispatchEvent(new PageTransitionEvent('pagehide')));
    await page.reload();await bind();
    assert.deepEqual(await ids(),snapshot);
    // A fresh isolated browser state with storage denied retains the honest common feed.
    const isolated=await browser.newContext();const denied=await isolated.newPage();
    await denied.route('http://home.test/**',route=>route.fulfill({contentType:'text/html',body:`<section data-home-cold-start-feed><p data-home-feed-status>Общая подборка</p><script type=application/json data-home-feed-candidates>${JSON.stringify({v:1,candidates:[]})}</script><p data-home-feed-empty></p><div data-home-feed-grid></div></section>`}));
    await denied.goto('http://home.test/');
    await denied.evaluate(()=>{Object.defineProperty(window,'localStorage',{get:()=>{throw new Error('denied')}});Object.defineProperty(window,'sessionStorage',{get:()=>{throw new Error('denied')}});window.KenigEventsCreateEventCard=()=>null;window.KenigEventsSearchCardHost={hiddenIds:()=>[],register:()=>{}};});
    await denied.addScriptTag({content:bundle.outputFiles[0].text});
    assert.equal(await denied.locator('[data-home-feed-status]').textContent(),'Общая подборка');
    assert.equal(await denied.locator('[data-home-cold-start-feed]').getAttribute('data-ds-state'),'general empty');
    await isolated.close();assert.deepEqual(errors,[]);
  } finally {await browser.close();}
});
