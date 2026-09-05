import assert from 'node:assert/strict';
import { chromium } from 'playwright';

const origin = process.env.HYDRATED_FRAMING_BASE_URL || 'http://127.0.0.1:4387';
const browser = await chromium.launch({ headless:true });
const page = await browser.newPage({ viewport:{ width:1440, height:1100 } });
const pageErrors = [];
page.on('pageerror', (error) => pageErrors.push(error.message));
const svg = (width, height) => `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}"><rect width="100%" height="100%" fill="#d8b88f"/><rect x="${width*.1}" y="${height*.2}" width="${width*.8}" height="${height*.6}" fill="#fff4d8"/><text x="50%" y="50%" text-anchor="middle" font-size="64">protected bbox</text></svg>`;
await page.route('http://fixture.local/**', (route) => {
  const square = route.request().url().includes('square');
  return route.fulfill({ status:200, contentType:'image/svg+xml', body:svg(square ? 1000 : 800, 1000) });
});

function specs(kind) {
  if (kind === 'positive') return [{id:101,ratio:.8},{id:102,ratio:1},{id:103,ratio:1}];
  if (kind === 'horizontal') return [{id:201,ratio:.8,proof:false},{id:202,ratio:.8,proof:false},{id:203,ratio:1}];
  return [{id:301,ratio:1},{id:302,ratio:1},{id:303,ratio:.8,stale:true}];
}

async function render(kind) {
  await page.evaluate(({ kind, specs }) => {
    const hash = 'a'.repeat(64);
    const ranked = specs.map((spec, rank) => {
      const display = {
        id:spec.id, title:`Synthetic ${kind} ${spec.id}`, href:'#', absolute_url:location.href,
        event_type:'fixture', image_url:`http://fixture.local/${spec.ratio === 1 ? 'square' : 'portrait'}-${spec.id}.svg`,
        image_alt:'Synthetic framing fixture', image_text_mode:'ocr_text', image_media_role:'event_identity_poster',
        image_width:spec.ratio === 1 ? 1000 : 800, image_height:1000,
        ...(spec.proof === false ? {} : {
          safe_crop:true, current_pixel_sha256:hash,
          geometry_pixel_sha256:spec.stale ? 'b'.repeat(64) : hash,
          geometry_status:'classified', geometry_coordinate_space:'normalized_0_1',
          ocr_boxes:[{ x:.1, y:.2, w:.8, h:.6 }],
        }),
        display_date:'5 сентября', display_time:'18:00', display_date_time:'5 сентября · 18:00',
        occurrence_aria_label:'5 сентября в 18:00', occurrence_member_ids:[spec.id],
        city:'Калининград', venue_name:'Fixture', place:'Калининград · Fixture', status_label:'fixture',
        price_label:'Бесплатно', likes_count:0, shares_count:0, calendar_href:'#', calendar_eligible:false,
      };
      return { event_id:spec.id, id:spec.id, rank, personal_score:1-rank*.01,
        candidate:{ id:spec.id, title:display.title, lifecycle_status:'active', display } };
    });
    const planned = window.KenigEventsPlanRelatedCardRows(ranked, { rowSize:3, presentation:'flow' });
    const grid = document.querySelector('[data-free-collection-grid]');
    grid.replaceChildren();
    planned.forEach(({ item, layout }) => grid.append(window.KenigEventsCreateEventCard(item, 'split-actions', layout)));
    window.__hydratedProtectedPlan = planned.map(({ item, layout }) => ({ id:item.event_id, layout }));
  }, { kind, specs:specs(kind) });
  await page.waitForFunction(() => [...document.querySelectorAll('[data-free-collection-grid] > [data-event-card]')]
    .every((card) => card.querySelector('[data-card-image]')?.complete));
  await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  return page.evaluate(() => ({
    plan:window.__hydratedProtectedPlan,
    cards:[...document.querySelectorAll('[data-free-collection-grid] > [data-event-card]')].map((card) => {
      const shell = card.querySelector('[data-card-media-shell]');
      const image = card.querySelector('[data-card-image]');
      const box = shell.getBoundingClientRect();
      const style = getComputedStyle(image);
      const scale = style.objectFit === 'cover'
        ? Math.max(box.width/image.naturalWidth, box.height/image.naturalHeight)
        : Math.min(box.width/image.naturalWidth, box.height/image.naturalHeight);
      const paintedWidth = image.naturalWidth*scale;
      const paintedHeight = image.naturalHeight*scale;
      const offsetX = (box.width-paintedWidth)/2;
      const offsetY = (box.height-paintedHeight)/2;
      const visibleWidth = Math.min(box.width, offsetX+paintedWidth)-Math.max(0,offsetX);
      const visibleHeight = Math.min(box.height, offsetY+paintedHeight)-Math.max(0,offsetY);
      return {
        id:Number(card.dataset.eventId), shellRatio:box.width/box.height,
        fit:shell.dataset.mediaFrameFit, permission:shell.dataset.mediaFrameCropPermission,
        computedFit:style.objectFit,
        crop:1-(visibleWidth/paintedWidth)*(visibleHeight/paintedHeight),
        unused:1-(visibleWidth*visibleHeight)/(box.width*box.height),
        protectedInside:offsetX+paintedWidth*.1>=-.5 && offsetY+paintedHeight*.2>=-.5
          && offsetX+paintedWidth*.9<=box.width+.5 && offsetY+paintedHeight*.8<=box.height+.5,
      };
    }),
  }));
}

try {
  await page.goto(`${origin}/podborki/besplatnye-sobytiya/`, { waitUntil:'domcontentloaded' });
  await page.waitForFunction(() => typeof window.KenigEventsPlanRelatedCardRows === 'function'
    && typeof window.KenigEventsCreateEventCard === 'function'
    && typeof window.KenigEventsRelatedCardMediaFrameBinding === 'function');
  const catalogIds = await page.evaluate(() => JSON.parse(document.querySelector('#free-collection-catalog').textContent)
    .related_static.map((item) => String(item.event_id)));
  const catalogTotal = catalogIds.length;
  const initialIds = await page.locator('[data-free-collection-grid] > [data-event-card]:not([hidden])').evaluateAll((cards) => cards.map((card) => card.dataset.eventId));
  assert.equal(initialIds.length, 12);
  for (let attempt=0; attempt<16; attempt+=1) {
    const before = await page.locator('[data-free-collection-grid] > [data-event-card]:not([hidden])').count();
    if (before >= catalogTotal) break;
    await page.locator('[data-free-collection-load-more]').evaluate((button) => button.click());
    await page.waitForFunction((previous) => document.querySelectorAll('[data-free-collection-grid] > [data-event-card]:not([hidden])').length > previous, before);
  }
  const allIds = await page.locator('[data-free-collection-grid] > [data-event-card]:not([hidden])').evaluateAll((cards) => cards.map((card) => card.dataset.eventId));
  assert.equal(allIds.length, catalogTotal);
  assert.equal(new Set(allIds).size, catalogTotal);
  assert.deepEqual(allIds, catalogIds);
  assert.deepEqual(allIds.slice(0,initialIds.length), initialIds);
  const positive = await render('positive');
  const accepted = positive.cards.find(({id}) => id===101);
  assert.ok(positive.plan.every(({layout}) => layout.framingStatus==='satisfied' && layout.rowRatio===1));
  assert.equal(positive.plan.find(({id})=>id===101).layout.mediaTreatment,'document-protected-cover');
  assert.deepEqual([accepted.fit,accepted.permission,accepted.computedFit],['cover','reviewed-bounded','cover']);
  assert.ok(accepted.crop>0 && accepted.crop<=.200001 && Math.abs(accepted.unused)<1e-6 && accepted.protectedInside);

  const horizontal = await render('horizontal');
  const horizontalRejected = horizontal.cards.find(({id})=>id===203);
  assert.deepEqual([horizontalRejected.fit,horizontalRejected.permission,horizontalRejected.computedFit],['contain','forbidden','contain']);
  const stale = await render('stale');
  const staleRejected = stale.cards.find(({id})=>id===303);
  assert.deepEqual([staleRejected.fit,staleRejected.permission,staleRejected.computedFit],['contain','forbidden','contain']);
  assert.notEqual(stale.plan.find(({id})=>id===303).layout.mediaTreatment,'document-protected-cover');

  await page.setViewportSize({width:390,height:844});
  const mobile = await render('positive');
  const natural = mobile.cards.find(({id})=>id===101);
  assert.deepEqual([natural.fit,natural.permission,natural.computedFit],['contain','forbidden','contain']);
  assert.ok(Math.abs(natural.shellRatio-.8)<.002 && Math.abs(natural.unused)<.002);
  assert.deepEqual(pageErrors, []);
} finally {
  await browser.close();
}
