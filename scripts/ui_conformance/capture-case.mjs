#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import {
  captureStableLocatorPng, collectBoundedElementFacts, loadPinnedPlaywrightImageComparator, startSpecimenServer,
} from '../current_ui_resource_graph/v1/specimens/capture.mjs';

function parseArgs(argv) { const out = {}; for (let i = 0; i < argv.length; i += 1) { const key = argv[i].replace(/^--/u, ''); const next = argv[i + 1]; if (!next || next.startsWith('--')) out[key] = true; else { out[key] = next; i += 1; } } return out; }
const sha = (value) => createHash('sha256').update(value).digest('hex');
const args = parseArgs(process.argv.slice(2));
if (!args.resolved || !args.harness || !args.output) throw new Error('--resolved, --harness and --output are required');
const data = JSON.parse(readFileSync(resolve(args.resolved), 'utf8')); const harness = resolve(args.harness); const output = resolve(args.output); mkdirSync(output, { recursive: true });
const modules = resolve(args['node-modules'] || join(harness, 'node_modules')); const playwrightPath = join(modules, 'playwright/index.mjs');
if (!existsSync(playwrightPath)) throw new Error('Exact Playwright entrypoint missing');
const { chromium } = await import(pathToFileURL(playwrightPath).href); const server = await startSpecimenServer({ dist: join(harness, 'dist') });
const browser = await chromium.launch({ headless: true });
try {
  const context = await browser.newContext({
    viewport: { width: data.viewport.width, height: data.viewport.height }, deviceScaleFactor: data.viewport.device_scale_factor,
    locale: 'ru-RU', timezoneId: 'Europe/Kaliningrad', reducedMotion: 'reduce', colorScheme: 'light',
  });
  const page = await context.newPage();
  await page.addInitScript(() => { window.__UI_CONFORMANCE__ = { fixtureNetwork: true }; });
  await page.goto(`${server.baseUrl}/specimens/${data.case_id}/`, { waitUntil: 'networkidle' });
  await page.addStyleTag({ content: '*,*::before,*::after{animation:none!important;transition:none!important;caret-color:transparent!important;scroll-behavior:auto!important}' });
  await page.evaluate(async () => { await document.fonts.ready; await Promise.all([...document.images].map(async (image) => { if (!image.complete) await new Promise((done) => { image.addEventListener('load', done, { once:true }); image.addEventListener('error', done, { once:true }); }); if (image.complete && image.naturalWidth > 0) await image.decode().catch(() => {}); })); });
  const selector = data.component_id === 'core.button' ? '[data-specimen-root] .ke-button' : '[data-event-card]'; const locator = page.locator(selector); await locator.waitFor({ state: 'visible' });
  const imageComparator = loadPinnedPlaywrightImageComparator(modules); const screenshot = await captureStableLocatorPng({ locator, path: join(output, 'astro.png'), imageComparator, label: `UI conformance ${data.case_id}` });
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
  const font = await locator.evaluate(async (node) => {
    await document.fonts.ready; const s=getComputedStyle(node); const faces=[...document.fonts].map(f=>({family:f.family,weight:f.weight,style:f.style,status:f.status}));
    const canvas=document.createElement('canvas'); const ctx=canvas.getContext('2d'); const probe='mmmmmmmmmmWWWWiiiiЖЩ';
    ctx.font='32px Inter'; const expectedWidth=ctx.measureText(probe).width; ctx.font='32px sans-serif'; const fallbackWidth=ctx.measureText(probe).width;
    const faceLoaded=faces.some(f=>String(f.family).replace(/["']/g,'').toLowerCase()==='inter'&&f.status==='loaded');
    const metricDistinct=Math.abs(expectedWidth-fallbackWidth)>0.01;
    return { expected_family:'Inter', expected_weight:s.fontWeight, expected_style:s.fontStyle, computed_family:s.fontFamily, document_fonts_check:document.fonts.check(`${s.fontSize} Inter`), face_loaded:faceLoaded, metric_distinct_from_sans:metricDistinct, versioned_font_face_required:true, font_loaded:faceLoaded, fonts:faces };
  });
  const outputFacts = { ...facts, bounded_browser_evidence: bounded, capture: screenshot, locale:'ru-RU', timezone:'Europe/Kaliningrad', device_scale_factor:data.viewport.device_scale_factor, animations_disabled:true, caret_hidden:true, reduced_motion:true, fixture_network_bound:true };
  writeFileSync(join(output, 'astro-facts.json'), `${JSON.stringify(outputFacts, null, 2)}\n`); writeFileSync(join(output, 'font-preflight.json'), `${JSON.stringify(font, null, 2)}\n`);
  writeFileSync(join(output, 'astro-capture-receipt.json'), `${JSON.stringify({ schema_version:'ui_conformance_astro_capture_v1', case_id:data.case_id, resolved_render_case_sha256:data.resolved_render_case_sha256, screenshot_sha256:sha(readFileSync(join(output,'astro.png'))), root_selector:selector, font, captured_at:new Date().toISOString() }, null, 2)}\n`);
  process.stdout.write(`${JSON.stringify({ ok:true, screenshot, font },null,2)}\n`); await context.close();
} finally { await browser.close(); await server.close(); }
