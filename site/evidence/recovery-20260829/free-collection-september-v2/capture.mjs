import { chromium } from 'playwright';
import { writeFile } from 'node:fs/promises';
const out = new URL('./', import.meta.url);
const browser = await chromium.launch({ headless:true, executablePath:'/home/dev/.cache/ms-playwright/chromium-1228/chrome-linux64/chrome', args:['--no-sandbox'] });
const receipt={schema_version:'free-collection-browser-receipt.v3',route:'/podborki/besplatnye-sobytiya/',scenario:'free-collection-september-desktop-v2',captured_at:new Date().toISOString(),views:{}};
for(const [viewport,width,height] of [['desktop',1280,1200],['mobile',390,844]]){
 const page=await browser.newPage({viewport:{width,height},deviceScaleFactor:1});const errors=[];page.on('console',m=>{if(m.type()==='error')errors.push(m.text())});page.on('pageerror',e=>errors.push(String(e)));
 await page.goto('http://127.0.0.1:4173/podborki/besplatnye-sobytiya/',{waitUntil:'networkidle'});
 const expectedCardCount=5;
 const cardCount=await page.locator('[data-event-card]').count();
 if(cardCount!==expectedCardCount)throw new Error(`${viewport}: expected ${expectedCardCount} cards, found ${cardCount}`);
 // A full-page screenshot does not guarantee that native lazy images below the
 // viewport were requested. Visit the complete document first, then require
 // every card image to decode before capturing any proof bitmap.
 const documentHeight=await page.evaluate(()=>document.documentElement.scrollHeight);
 for(let y=0;y<documentHeight;y+=Math.max(320,Math.floor(height*.65))){
   await page.evaluate(value=>scrollTo(0,value),y);
   await page.waitForTimeout(80);
 }
 await page.waitForFunction(()=>{
   const images=[...document.querySelectorAll('[data-event-card] [data-card-image]')];
   return images.length===5&&images.every(image=>image.complete&&image.naturalWidth>0);
 },null,{timeout:30000});
 await page.evaluate(async()=>Promise.all([...document.querySelectorAll('[data-event-card] [data-card-image]')].map(image=>image.decode())));
 const imageReadiness=await page.evaluate(()=>[...document.querySelectorAll('[data-event-card]')].map(card=>{const image=card.querySelector('[data-card-image]');return{event_id:Number(card.getAttribute('data-event-id')),complete:image?.complete===true,natural_width:image?.naturalWidth||0,current_src:image?.currentSrc||''}}));
 if(imageReadiness.some(image=>!image.complete||image.natural_width===0||!image.current_src))throw new Error(`${viewport}: incomplete event-card image readiness`);
 await page.evaluate(()=>{document.documentElement.style.scrollBehavior='auto';document.body.style.scrollBehavior='auto';scrollTo(0,0)});
 await page.waitForFunction(()=>scrollY===0,null,{timeout:5000});
 const geometry=()=>page.evaluate(()=>{const rect=s=>{const e=document.querySelector(s);if(!e)return null;const r=e.getBoundingClientRect();return{x:r.x,y:r.y,width:r.width,height:r.height}};return{hero:rect('[data-free-collection-hero]'),results:rect('[data-free-collection-event-group="events"]'),exhibitions:rect('[data-free-collection-event-group="exhibitions"]'),nav:rect('[data-mobile-bottom-nav]')}});
 const topGeometry=await geometry();
 await page.screenshot({path:new URL(`astro-${viewport}-top.png`,out).pathname});
 await page.screenshot({path:new URL(`astro-${viewport}-full.png`,out).pathname,fullPage:true});
 await page.evaluate(()=>scrollTo(0,700));await page.waitForTimeout(250);await page.screenshot({path:new URL(`astro-${viewport}-sticky.png`,out).pathname});
 const stickyGeometry=await geometry();
 const observed=await page.evaluate(()=>{const cards=[...document.querySelectorAll('[data-event-card]')];return{document:{width:document.documentElement.scrollWidth,height:document.documentElement.scrollHeight,horizontal_overflow:Math.max(0,document.documentElement.scrollWidth-innerWidth)},scenario:document.querySelector('[data-ui-fixture-scenario]')?.getAttribute('data-ui-fixture-scenario'),event_ids:cards.map(c=>Number(c.getAttribute('data-event-id'))),calendar_event_ids:cards.filter(c=>c.querySelector('[data-calendar-action]')).map(c=>Number(c.getAttribute('data-event-id'))),zero_count_labels:[...document.querySelectorAll('[data-share-count],[data-feedback-count]')].filter(e=>e.textContent.trim()==='0').length,updated_text:document.querySelector('.free-collection__copy .muted-note')?.textContent.trim(),mobile_current:document.querySelector('[data-mobile-bottom-nav] [aria-current="page"]')?.getAttribute('data-mobile-nav-section')||null}});
 receipt.views[viewport]={viewport:{width,height,dpr:1},...observed,image_readiness:imageReadiness,all_card_images_decoded:true,top_geometry:topGeometry,sticky_geometry:stickyGeometry,console_errors:errors};await page.close();
}
await browser.close();await writeFile(new URL('astro-browser-receipt.v2.json',out),JSON.stringify(receipt,null,2)+'\n');
console.log(JSON.stringify(receipt,null,2));
