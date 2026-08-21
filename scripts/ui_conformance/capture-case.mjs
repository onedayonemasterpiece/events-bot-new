#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import {
  captureStableLocatorPng, collectBoundedElementFacts, loadPinnedPlaywrightImageComparator, startSpecimenServer,
} from '../current_ui_resource_graph/v1/specimens/capture.mjs';
import { resolveEventCardArchetypeContext, validateCapturedArchetypeEvidence } from './archetype-specimen.mjs';

function parseArgs(argv) { const out = {}; for (let i = 0; i < argv.length; i += 1) { const key = argv[i].replace(/^--/u, ''); const next = argv[i + 1]; if (!next || next.startsWith('--')) out[key] = true; else { out[key] = next; i += 1; } } return out; }
const sha = (value) => createHash('sha256').update(value).digest('hex');
const args = parseArgs(process.argv.slice(2));
if (!args.resolved || !args.harness || !args.output) throw new Error('--resolved, --harness and --output are required');
const data = JSON.parse(readFileSync(resolve(args.resolved), 'utf8')); const archetype = resolveEventCardArchetypeContext(data); const harness = resolve(args.harness); const output = resolve(args.output); mkdirSync(output, { recursive: true });
const modules = resolve(args['node-modules'] || join(harness, 'node_modules')); const playwrightPath = join(modules, 'playwright/index.mjs');
if (!existsSync(playwrightPath)) throw new Error('Exact Playwright entrypoint missing');
const { chromium } = await import(pathToFileURL(playwrightPath).href); const server = await startSpecimenServer({ dist: join(harness, 'dist') });
const browser = await chromium.launch({ headless: true });
try {
  const context = await browser.newContext({
    viewport: { width: data.container_geometry.viewport_width, height: data.container_geometry.viewport_height }, deviceScaleFactor: data.container_geometry.device_scale_factor,
    locale: 'ru-RU', timezoneId: 'Europe/Kaliningrad', reducedMotion: 'reduce', colorScheme: 'light',
  });
  const page = await context.newPage();
  await page.addInitScript(({ iso }) => {
    const NativeDate = Date;
    const FrozenDate = class extends NativeDate { constructor(...args) { super(...(args.length ? args : [iso])); } static now(){ return new NativeDate(iso).getTime(); } };
    FrozenDate.parse = NativeDate.parse; FrozenDate.UTC = NativeDate.UTC; window.Date = FrozenDate;
    window.__UI_CONFORMANCE__ = { fixtureNetwork: false, frozenClock: iso };
  }, { iso: data.reference_clock?.reference_iso || '2026-08-21T09:00:00+02:00' });
  await page.goto(`${server.baseUrl}/specimens/${data.case_id}/`, { waitUntil: 'networkidle' });
  await page.addStyleTag({ content: '*,*::before,*::after{animation:none!important;transition:none!important;caret-color:transparent!important;scroll-behavior:auto!important}' });
  await page.evaluate(async () => { await document.fonts.ready; await Promise.all([...document.images].map(async (image) => { if (!image.complete) await new Promise((done) => { image.addEventListener('load', done, { once:true }); image.addEventListener('error', done, { once:true }); }); if (image.complete && image.naturalWidth > 0) await image.decode().catch(() => {}); })); });
  const selector = data.component_id === 'core.button' ? '[data-specimen-root] .ke-button' : archetype?.selected_card_selector || '[data-event-card]';
  const parentSelector = archetype?.parent_selector || selector;
  const locator = page.locator(selector); const parentLocator = page.locator(parentSelector);
  await parentLocator.waitFor({ state: 'visible' }); await locator.waitFor({ state: 'visible' });
  // Locator screenshots include fixed/sticky page chrome that overlays a tall
  // component after Playwright scrolls it into view. Hide only unrelated
  // overlaying chrome; never crop, scale, or alter the selected component.
  await page.evaluate((rootSelector) => {
    const root = document.querySelector(rootSelector); if (!root) return;
    for (const node of document.body.querySelectorAll('*')) {
      if (node === root || node.contains(root) || root.contains(node)) continue;
      const position = getComputedStyle(node).position;
      if (position === 'fixed' || position === 'sticky') node.setAttribute('data-ui-conformance-external-overlay', '');
    }
  }, parentSelector);
  await page.addStyleTag({ content: '[data-ui-conformance-external-overlay]{display:none!important}' });
  const imageComparator = loadPinnedPlaywrightImageComparator(modules);
  const parentScreenshot = archetype ? await captureStableLocatorPng({ locator:parentLocator, path: join(output, 'astro-archetype.png'), imageComparator, label: `UI conformance archetype ${data.case_id}` }) : null;
  const screenshot = await captureStableLocatorPng({ locator, path: join(output, 'astro.png'), imageComparator, label: `UI conformance selected card ${data.case_id}` });
  const partSelectors = data.component_id === 'core.button' ? [] : ['[data-card-media-shell]', '[data-card-title]', '[data-card-meta]', '[data-card-status]', '[data-card-place]', '[data-feedback-action="not_interested"]', '[data-calendar-action]', '[data-native-share]', '[data-feedback-action="like"]'];
  const bounded = await collectBoundedElementFacts(locator, partSelectors);
  const facts = await locator.evaluate((root, selectors) => {
    const base=root.getBoundingClientRect();
    const box = (node) => { const r = node.getBoundingClientRect(); return { x:r.x-base.x, y:r.y-base.y, width:r.width, height:r.height }; };
    const regions = {}; for (const selector of selectors) { const node = root.querySelector(selector); if (node) regions[selector] = box(node); }
    const styleFacts=(node)=>{const s=getComputedStyle(node);return {padding:[s.paddingTop,s.paddingRight,s.paddingBottom,s.paddingLeft],gap:{row:s.rowGap,column:s.columnGap},border_radius:s.borderRadius,border:{top_width:s.borderTopWidth,top_style:s.borderTopStyle,top_color:s.borderTopColor},background_color:s.backgroundColor,color:s.color,box_shadow:s.boxShadow,opacity:s.opacity};};
    const regionStyles={}; for(const selector of selectors){const node=root.querySelector(selector);if(node)regionStyles[selector]=styleFacts(node);}
    const typography = {}; for (const selector of ['[data-card-title]','[data-card-meta]','[data-card-status]','[data-card-place]']) { const node = root.querySelector(selector); if (!node) continue; const s=getComputedStyle(node); const range=document.createRange(); range.selectNodeContents(node); const lines=new Set([...range.getClientRects()].map(r=>Math.round(r.y*2)/2)); typography[selector]={font_family:s.fontFamily,font_weight:s.fontWeight,font_size:s.fontSize,line_height:s.lineHeight,line_count:lines.size,overflow:s.overflow}; }
    const icons=[...root.querySelectorAll('svg')].map((node)=>{
      const explicit=node.getAttribute('data-icon'); if(explicit) return explicit;
      const owner=node.closest('[data-feedback-action],[data-native-share],[data-calendar-action]');
      if(!owner) return 'unknown';
      if(owner.hasAttribute('data-native-share')) return 'share';
      if(owner.hasAttribute('data-calendar-action')) return 'calendar';
      return owner.getAttribute('data-feedback-action')||'unknown';
    });
    const media=root.querySelector('[data-card-image]');
    const mediaShell=root.querySelector('[data-card-media-shell]'); const mediaState=mediaShell?.classList.contains('is-image-loaded')?'loaded':mediaShell?.classList.contains('is-image-missing')?'error':mediaShell?.classList.contains('is-image-loading')?'loading':null;
    return { root:box(root), box_model:styleFacts(root), regions, region_styles:regionStyles, typography, icon_ids:icons, region_order:[...root.querySelectorAll('[data-card-media-shell],[data-card-title],[data-card-meta],[data-card-status],[data-card-place],[data-feedback-action],[data-calendar-action],[data-native-share]')].map(n=>n.getAttribute('data-card-title')!==null?'title':n.getAttribute('data-card-meta')!==null?'meta':n.getAttribute('data-card-status')!==null?'status':n.getAttribute('data-card-place')!==null?'place':n.getAttribute('data-calendar-action')!==null?'calendar':n.getAttribute('data-native-share')!==null?'share':n.getAttribute('data-feedback-action')||'media'), nested_component_ids:[], media_fit:media?getComputedStyle(media).objectFit:null, media_position:media?getComputedStyle(media).objectPosition:null, crop_window:media?box(media):null, state_markers:{media:mediaState}, forbidden_consumer_overrides:[] };
  }, partSelectors);
  let archetypeEvidence = null;
  let archetypeValidation = null;
  if (archetype) {
    archetypeEvidence = await parentLocator.evaluate((root) => {
      const parentRect = root.getBoundingClientRect();
      const relativeRect = (node) => { const rect=node.getBoundingClientRect(); return {x:rect.x-parentRect.x,y:rect.y-parentRect.y,width:rect.width,height:rect.height}; };
      const style=getComputedStyle(root);
      const cards=[...root.querySelectorAll(':scope > [data-event-card]')].map((card)=>{
        const media=card.querySelector('[data-card-media-shell]'); const image=card.querySelector('[data-card-image]'); const cardStyle=getComputedStyle(card);
        const mediaState=media?.classList.contains('is-image-loaded')?'loaded':media?.classList.contains('is-image-missing')?'error':media?.classList.contains('is-image-loading')?'loading':null;
        return {
          event_id:Number(card.dataset.eventId), event_title:card.dataset.eventTitle||null,
          row_index:Number(card.dataset.labRowIndex), column_index:Number(card.dataset.labRowColumn),
          row_ratio:Number(card.style.getPropertyValue('--lab-row-media-ratio')||0), row_mode:card.dataset.labRowMedia||null,
          media_kind:card.dataset.labMediaKind||null, media_treatment:card.dataset.labMediaTreatment||null,
          crop_reason:card.dataset.labCropReason||null, row_cost:Number(card.dataset.labRowCost||0),
          rect:relativeRect(card), computed_grid_row:cardStyle.gridRowStart, computed_grid_column:cardStyle.gridColumnStart,
          computed_height:cardStyle.height, computed_grid_template_rows:cardStyle.gridTemplateRows,
          media_rect:media?relativeRect(media):null, media_state:mediaState,
          media_fit:image?getComputedStyle(image).objectFit:null, media_position:image?getComputedStyle(image).objectPosition:null,
        };
      });
      return {
        root:{x:parentRect.x,y:parentRect.y,width:parentRect.width,height:parentRect.height},
        grid:{display:style.display,template_columns:style.gridTemplateColumns,column_count:style.gridTemplateColumns.split(/\s+/u).filter(Boolean).length,column_gap:Number.parseFloat(style.columnGap),row_gap:Number.parseFloat(style.rowGap),align_items:style.alignItems},
        ordered_event_ids:cards.map((card)=>card.event_id), cards,
      };
    });
    archetypeValidation=validateCapturedArchetypeEvidence(archetypeEvidence,archetype);
    if(archetypeValidation.status!=='PASS')throw new Error(`Archetype geometry/placement evidence failed: ${JSON.stringify(archetypeValidation.checks)}`);
  }
  const font = await locator.evaluate(async (node) => {
    await document.fonts.ready; const s=getComputedStyle(node); const faces=[...document.fonts].map(f=>({family:f.family,weight:f.weight,style:f.style,status:f.status}));
    const canvas=document.createElement('canvas'); const ctx=canvas.getContext('2d'); const probe='mmmmmmmmmmWWWWiiiiЖЩ';
    ctx.font='32px Inter'; const expectedWidth=ctx.measureText(probe).width; ctx.font='32px sans-serif'; const fallbackWidth=ctx.measureText(probe).width;
    const faceLoaded=faces.some(f=>String(f.family).replace(/["']/g,'').toLowerCase()==='inter'&&f.status==='loaded');
    const metricDistinct=Math.abs(expectedWidth-fallbackWidth)>0.01;
    return { expected_family:'Inter', expected_weight:s.fontWeight, expected_style:s.fontStyle, computed_family:s.fontFamily, document_fonts_check:document.fonts.check(`${s.fontSize} Inter`), face_loaded:faceLoaded, metric_distinct_from_sans:metricDistinct, versioned_font_face_required:true, font_loaded:faceLoaded, fonts:faces };
  });
  const outputFacts = { ...facts, bounded_browser_evidence: bounded, capture: screenshot, parent_capture:parentScreenshot, archetype_context:archetype, archetype_evidence:archetypeEvidence, archetype_validation:archetypeValidation, locale:'ru-RU', timezone:'Europe/Kaliningrad', device_scale_factor:data.container_geometry.device_scale_factor, animations_disabled:true, caret_hidden:true, reduced_motion:true, fixture_network_bound:false, frozen_clock:data.reference_clock };
  writeFileSync(join(output, 'astro-facts.json'), `${JSON.stringify(outputFacts, null, 2)}\n`); writeFileSync(join(output, 'font-preflight.json'), `${JSON.stringify(font, null, 2)}\n`);
  const selectedScreenshotSha=sha(readFileSync(join(output,'astro.png'))); const parentScreenshotSha=parentScreenshot?sha(readFileSync(join(output,'astro-archetype.png'))):null;
  writeFileSync(join(output, 'astro-capture-receipt.json'), `${JSON.stringify({ schema_version:'ui_conformance_astro_capture_v1', case_id:data.case_id, resolved_render_case_sha256:data.resolved_render_case_sha256, screenshot_sha256:selectedScreenshotSha, selected_card_screenshot_sha256:selectedScreenshotSha, parent_archetype_screenshot_sha256:parentScreenshotSha, root_selector:selector, parent_root_selector:parentSelector, archetype_validation:archetypeValidation, font, captured_at:new Date().toISOString() }, null, 2)}\n`);
  process.stdout.write(`${JSON.stringify({ ok:true, screenshot, parent_screenshot:parentScreenshot, archetype_validation:archetypeValidation, font },null,2)}\n`); await context.close();
} finally { await browser.close(); await server.close(); }
