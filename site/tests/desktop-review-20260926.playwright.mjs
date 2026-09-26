import assert from 'node:assert/strict';
import {chromium} from 'playwright';
import {mkdir,writeFile} from 'node:fs/promises';
const base=process.env.CHECK_BASE?.replace(/\/$/u,''),out=process.env.CHECK_OUTPUT;
if(!base||!out)throw new Error('CHECK_BASE and CHECK_OUTPUT are required');
await mkdir(out,{recursive:true});
const browser=await chromium.launch({executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH||undefined});
const results=[];
const box=async(page,selector)=>page.locator(selector).first().evaluate(e=>{const r=e.getBoundingClientRect(),c=getComputedStyle(e);return {x:r.x,y:r.y,width:r.width,height:r.height,radius:c.borderRadius,font:c.font,fontWeight:c.fontWeight,background:c.backgroundColor,color:c.color};});
try{
 const manifest=await(await fetch(base+'/data/event-dates.json')).json();
 assert.equal(manifest.current_date,'2026-09-26');
 for(const width of [1920,1440]){
  let expectedNav;
  const routeGeometry={};
  for(const route of ['', 'segodnya/', 'zavtra/', 'vyhodnye/', 'vystavki/', 'festivali/']){
   const page=await browser.newPage({viewport:{width,height:1080}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
   assert.equal((await page.goto(base+'/'+route,{waitUntil:'domcontentloaded'})).status(),200);
   await page.waitForTimeout(650);
   const nav=await box(page,'.site-nav'),link=await box(page,'.site-nav a');
   assert.equal(nav.y,20);assert.equal(nav.height,56);assert.equal(nav.radius,'20px');assert.equal(link.radius,'14px');assert.equal(link.height,44);
   expectedNav??={font:link.font,height:nav.height,radius:nav.radius};assert.deepEqual({font:link.font,height:nav.height,radius:nav.radius},expectedNav);
   await page.locator('.site-nav a').first().hover();assert.equal((await box(page,'.site-nav a')).radius,'14px');
   let homeBrandTop=null;
   if(!route){
    assert.equal(await page.getByRole('button',{name:'Пауза',exact:true}).count(),0);
    for(const scene of await page.locator('[data-home-hero-scene]').all())assert.equal(await scene.locator('.home-hero-talk__cursor').count(),1);
    assert.equal((await box(page,'[data-home-hero-copy]')).fontWeight,'500');
    const mosaicStyle=await page.locator('[data-home-hero-mosaic]').first().evaluate(e=>({columnGap:getComputedStyle(e).columnGap,rowGap:getComputedStyle(e).rowGap}));
    assert.deepEqual(mosaicStyle,{columnGap:'2px',rowGap:'2px'});
    const cursorVertical=await page.locator('.home-hero-talk__cursor').first().evaluate(e=>getComputedStyle(e).verticalAlign);
    assert.notEqual(cursorVertical,'baseline');
    homeBrandTop=(await box(page,'.site-header__brand-tag')).y;
    const end=page.locator('[data-hero-talk-page-end]');assert.ok(parseFloat(await end.evaluate(e=>getComputedStyle(e).marginTop))>=48);
    const gap=await page.evaluate(()=>{const end=document.querySelector('[data-hero-talk-page-end]').getBoundingClientRect(),main=document.querySelector('main').getBoundingClientRect();return main.bottom-end.bottom;});assert.ok(gap<10,`page-end trailing gap ${gap}`);
   }
   if(route==='vyhodnye/'){
    const heads=await page.locator('.ke-weekend-day__head').evaluateAll(es=>es.map(e=>({bg:getComputedStyle(e).backgroundColor,color:getComputedStyle(e.querySelector('.ke-weekend-day__label')).color,weight:getComputedStyle(e.querySelector('.ke-weekend-day__label')).fontWeight})));
    assert.deepEqual(heads.map(h=>h.bg),['rgb(152, 64, 31)','rgb(15, 93, 87)']);assert.ok(heads.every(h=>h.color==='rgb(255, 255, 255)'&&h.weight==='500'));
   }
   if(route==='vystavki/'){
    assert.equal(await page.locator('body').evaluate(e=>getComputedStyle(e).backgroundColor),'rgb(36, 33, 31)');
    assert.equal((await box(page,'.ex-deck__frame')).radius,'14px');
   }
   await page.screenshot({path:`${out}/${width}-${route.replaceAll('/','')||'home'}-top.png`});
   await page.evaluate(()=>scrollTo(0,1000));await page.waitForTimeout(550);
   if(!route){
    const brandAfter=(await box(page,'.site-header__brand-tag')).y;
    assert.ok(Math.abs(brandAfter-homeBrandTop)<=1,`home brand tag moved ${homeBrandTop} → ${brandAfter}`);
   }
   if(route==='segodnya/'){
    const context=page.locator('[data-floating-page-context]');
    assert.equal(await context.isVisible(),false,'Today must not duplicate timeline time in a floating context');
    routeGeometry.todayCity=await box(page,'[data-listing-controls]');
   }
   if(route==='zavtra/'){
    const context=await box(page,'[data-floating-page-context]');assert.ok(context.width>=70&&context.width<=120,`time context width ${context.width}`);
    routeGeometry.tomorrowCity=await box(page,'[data-listing-controls]');
   }
   if(route==='festivali/'){
    assert.equal(await page.locator('.festival-month-nav').count(),1);assert.equal(Math.round((await box(page,'.festival-month-nav')).y),88);
    assert.equal(await page.locator('[data-floating-page-context]').isVisible(),false);
    const ends=await page.locator('[data-festival-card][data-festival-end]').evaluateAll(es=>es.map(e=>e.dataset.festivalEnd));assert.ok(ends.every(d=>d>='2026-09-26'));
    const archive=page.locator('[data-festival-archive]');assert.equal(await archive.getAttribute('open'),null);await archive.locator('summary').click();assert.equal(await archive.locator('[data-archived-festival]').count(),13);
    assert.ok((await archive.locator('[data-festival-end]').evaluateAll(es=>es.map(e=>e.dataset.festivalEnd))).every(d=>d<'2026-09-26'));await archive.locator('summary').click();
   }
   const dateLink=page.locator('[data-mobile-bottom-nav] [data-mobile-nav-section="dates"]');
   const priorUrl=page.url();await dateLink.click();const calendar=page.locator('[data-calendar-sheet]');await calendar.waitFor({state:'visible'});assert.equal(page.url(),priorUrl);
   assert.equal(await calendar.getByRole('dialog').count(),1);await page.keyboard.press('Escape');assert.equal(await calendar.isVisible(),false);assert.equal(await dateLink.evaluate(e=>e===document.activeElement),true);
   await dateLink.click();
   const date=calendar.locator('[data-calendar-month]:not([hidden]) a[href*="/date-"]').first();const href=await date.getAttribute('href');assert.ok(href.includes(new URL(base).pathname+'/date-'));await date.click();await page.waitForURL('**/date-*/');assert.equal((await page.request.get(page.url())).status(),200);
   assert.deepEqual(errors,[]);results.push({width,route:route||'home',navigation:true,calendarExactDate:href,errors});await page.close();
  }
  assert.ok(routeGeometry.todayCity&&routeGeometry.tomorrowCity);
  assert.ok(routeGeometry.todayCity.x+40<routeGeometry.tomorrowCity.x,`Today did not reclaim section-context width: ${routeGeometry.todayCity.x} vs ${routeGeometry.tomorrowCity.x}`);

  const eventPage=await browser.newPage({viewport:{width,height:1080}}),eventErrors=[];eventPage.on('pageerror',e=>eventErrors.push(e.message));
  await eventPage.goto(base+'/segodnya/',{waitUntil:'domcontentloaded'});await eventPage.waitForTimeout(350);
  const eventHref=await eventPage.locator('a[href*="/sobytiya/"]').first().getAttribute('href');assert.ok(eventHref);
  await eventPage.goto(new URL(eventHref,base).href,{waitUntil:'domcontentloaded'});await eventPage.waitForTimeout(650);
  assert.equal(await eventPage.locator('body').getAttribute('data-shell-composition'),'event-navigation-only');
  assert.equal(await eventPage.locator('[data-floating-top-band]').count(),0);
  assert.equal(Math.round((await box(eventPage,'.site-header')).height),0);
  assert.equal(Math.round((await box(eventPage,'.site-nav')).y),20);
  assert.equal(await eventPage.locator('main#main').count(),1);assert.ok(await eventPage.locator('main#main h1').count()>=1);
  await eventPage.screenshot({path:`${out}/${width}-event-detail-r5.png`});assert.deepEqual(eventErrors,[]);await eventPage.close();

  const feedPage=await browser.newPage({viewport:{width,height:1080}}),feedErrors=[];feedPage.on('pageerror',e=>feedErrors.push(e.message));
  await feedPage.goto(base+'/dlya-menya/',{waitUntil:'domcontentloaded'});await feedPage.waitForTimeout(950);
  const feedCount=await feedPage.locator('[data-personal-feed-slot] [data-event-card]').count();
  assert.ok(feedCount>=12,`For Me first desktop hydration rendered only ${feedCount}`);
  results.push({width,forMeInitialCards:feedCount});assert.deepEqual(feedErrors,[]);await feedPage.close();

  if(width===1440){
   const clubsPage=await browser.newPage({viewport:{width,height:1080}}),clubErrors=[];clubsPage.on('pageerror',e=>clubErrors.push(e.message));
   await clubsPage.goto(base+'/kluby-po-interesam/',{waitUntil:'domcontentloaded'});await clubsPage.waitForTimeout(350);
   assert.equal(await clubsPage.locator('[data-club-list]').count(),1);
   const clubText=await clubsPage.locator('[data-club-list]').innerText();
   assert.match(clubText,/Game Vibes/u);assert.match(clubText,/Клуб исследователей нейронок/u);
   results.push({clubs:true});assert.deepEqual(clubErrors,[]);await clubsPage.close();
  }
 }
 // Delay modules to compare server paint against enhanced geometry, including
 // the city control whose original reparenting produced center/left jumps.
 const page=await browser.newPage({viewport:{width:1920,height:1080}});let release;const gate=new Promise(r=>release=r);
 await page.route('**/*.js',async r=>{await gate;await r.continue();});await page.goto(base+'/zavtra/',{waitUntil:'commit'});await page.locator('.site-nav').waitFor({state:'visible'});await page.waitForTimeout(200);
 const before={nav:await box(page,'.site-nav'),city:await box(page,'[data-listing-controls]')};release();await page.waitForFunction(()=>document.body.dataset.fiMotion==='ready');
 const after={nav:await box(page,'.site-nav'),city:await box(page,'[data-listing-controls]')};
 for(const key of ['nav','city'])for(const axis of ['x','y','width','height'])assert.ok(Math.abs(before[key][axis]-after[key][axis])<=1,`${key}.${axis} changed ${before[key][axis]} → ${after[key][axis]}`);
 results.push({firstPaint:{before,after}});await page.close();
 // Responsive regression only. This is not native-mobile or owner acceptance.
 for(const width of [320,390,430]){
  const page=await browser.newPage({viewport:{width,height:844},hasTouch:true});const errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.goto(base+'/');await page.locator('[data-home-quick-nav] a[href$="/segodnya/"]').first().click();await page.waitForURL('**/segodnya/');await page.locator('[data-date-dock-surface]').waitFor();
  assert.equal(await page.locator('[data-mobile-bottom-nav]').count(),1);await page.locator('[data-calendar-open]').click();await page.locator('[data-calendar-sheet]').waitFor({state:'visible'});await page.keyboard.press('Escape');assert.equal(await page.locator('[data-calendar-sheet]').isVisible(),false);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);assert.deepEqual(errors,[]);results.push({mobileRegressionOnly:width,errors});await page.close();
 }
 await writeFile(out+'/checks.json',JSON.stringify({passed:true,ownerReviewScope:'desktop-only',results},null,2));console.log('PASS desktop review:',results.length,'checks');
}finally{await browser.close();}
