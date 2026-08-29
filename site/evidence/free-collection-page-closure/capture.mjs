import { chromium } from 'playwright';
import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

const baseUrl = process.env.BASE_URL || 'http://127.0.0.1:4173';
const route = '/podborki/besplatnye-sobytiya/';
const kind = process.env.VIEWPORT_KIND || 'desktop';
const expectedScenario = process.env.SCENARIO_ID || `free-collection-september-${kind}-v3`;
const referenceIso = process.env.REFERENCE_ISO || '2026-08-29T14:00:00+02:00';
const outDir = path.resolve(process.env.OUT_DIR || new URL('./capture-output', import.meta.url).pathname, kind);
const viewport = kind === 'mobile' ? { width:390, height:844 } : { width:1280, height:1200 };
const expectedOrder = [8006, 8200, 2182, 6711, 7609];
const expectedGroups = { events:[8006, 8200], exhibitions:[2182, 6711, 7609] };
const fixedNow = Date.parse(referenceIso);

await mkdir(outDir, { recursive:true });
const browser = await chromium.launch({
  headless:true,
  executablePath:process.env.CHROMIUM_EXECUTABLE_PATH || '/home/dev/.cache/ms-playwright/chromium-1228/chrome-linux64/chrome',
  args:['--no-sandbox'],
});
const context = await browser.newContext({
  viewport,
  deviceScaleFactor:1,
  locale:'ru-RU',
  timezoneId:'Europe/Kaliningrad',
  reducedMotion:'reduce',
});
await context.addInitScript(({ now }) => {
  const NativeDate = Date;
  class FrozenDate extends NativeDate {
    constructor(...args) { super(...(args.length ? args : [now])); }
    static now() { return now; }
  }
  Object.setPrototypeOf(FrozenDate, NativeDate);
  globalThis.Date = FrozenDate;
  globalThis.__UI_REFERENCE_NOW__ = now;
}, { now:fixedNow });
const page = await context.newPage();
const errors = [];
page.on('console', (message) => { if (message.type() === 'error') errors.push(message.text()); });
page.on('pageerror', (error) => errors.push(String(error)));

await page.goto(`${baseUrl}${route}`, { waitUntil:'domcontentloaded', timeout:30_000 });
await page.locator('[data-free-collection-surface]').waitFor({ state:'visible', timeout:15_000 });
await page.addStyleTag({ content:'*,*::before,*::after{animation:none!important;transition:none!important;scroll-behavior:auto!important}html{scroll-behavior:auto!important}' });
await page.evaluate(async () => document.fonts.ready);

const cards = page.locator('[data-event-card]');
if (await cards.count() !== 5) throw new Error(`${kind}: expected exactly five canonical cards`);
const identity = await page.evaluate(() => {
  const root = document.querySelector('[data-free-collection-surface]');
  const groupIds = (name) => [...document.querySelectorAll(`[data-free-collection-event-group="${name}"] [data-event-card]`)]
    .map((node) => Number(node.getAttribute('data-event-id')));
  return {
    scenario:root?.getAttribute('data-ui-fixture-scenario'),
    reference_clock:root?.getAttribute('data-reference-clock'),
    event_ids:[...document.querySelectorAll('[data-event-card]')].map((node) => Number(node.getAttribute('data-event-id'))),
    groups:{ events:groupIds('events'), exhibitions:groupIds('exhibitions') },
  };
});
if (identity.scenario !== expectedScenario) throw new Error(`scenario mismatch: ${identity.scenario} != ${expectedScenario}`);
if (identity.reference_clock !== referenceIso) throw new Error(`reference clock mismatch: ${identity.reference_clock} != ${referenceIso}`);
if (JSON.stringify(identity.event_ids) !== JSON.stringify(expectedOrder)) throw new Error(`render order mismatch: ${identity.event_ids}`);
if (JSON.stringify(identity.groups) !== JSON.stringify(expectedGroups)) throw new Error(`group mismatch: ${JSON.stringify(identity.groups)}`);

const documentHeight = await page.evaluate(() => document.documentElement.scrollHeight);
for (let y = 0; y < documentHeight; y += Math.max(320, Math.floor(viewport.height * .65))) {
  await page.evaluate((value) => scrollTo(0, value), y);
  await page.waitForTimeout(40);
}
await page.waitForFunction(() => {
  const images = [...document.querySelectorAll('[data-event-card] [data-card-image]')];
  return images.length === 5 && images.every((image) => image.complete && image.naturalWidth > 0);
}, null, { timeout:30_000 });
await page.evaluate(async () => Promise.all([...document.querySelectorAll('[data-event-card] [data-card-image]')].map((image) => image.decode())));
const imageReadiness = await page.evaluate(() => [...document.querySelectorAll('[data-event-card]')].map((card) => {
  const image = card.querySelector('[data-card-image]');
  return {
    event_id:Number(card.getAttribute('data-event-id')),
    complete:image?.complete === true,
    natural_width:image?.naturalWidth || 0,
    natural_height:image?.naturalHeight || 0,
    current_src:image?.currentSrc || '',
  };
}));
if (imageReadiness.some((image) => !image.complete || !image.natural_width || !image.current_src)) throw new Error(`${kind}: incomplete event-card image readiness`);

const snapshot = () => page.evaluate(() => {
  const selectors = {
    root:'[data-free-collection-surface]', hero:'[data-free-collection-hero]',
    stickyIdentity:'[data-free-collection-sticky-identity]',
    events:'[data-free-collection-event-group="events"]', exhibitions:'[data-free-collection-event-group="exhibitions"]',
    mobileNav:'[data-mobile-bottom-nav]',
  };
  const inspect = (node) => {
    if (!(node instanceof Element)) return null;
    const rect = node.getBoundingClientRect();
    const style = getComputedStyle(node);
    return {
      geometry:{ x:rect.x, y:rect.y, width:rect.width, height:rect.height, top:rect.top, right:rect.right, bottom:rect.bottom, left:rect.left },
      style:{ display:style.display, position:style.position, boxSizing:style.boxSizing, color:style.color, backgroundColor:style.backgroundColor, borderColor:style.borderColor, borderWidth:style.borderWidth, borderRadius:style.borderRadius, boxShadow:style.boxShadow, fontFamily:style.fontFamily, fontSize:style.fontSize, fontWeight:style.fontWeight, lineHeight:style.lineHeight, letterSpacing:style.letterSpacing, padding:style.padding, margin:style.margin, gap:style.gap },
    };
  };
  const named = Object.fromEntries(Object.entries(selectors).map(([key, selector]) => [key, inspect(document.querySelector(selector))]));
  const cards = [...document.querySelectorAll('[data-event-card]')].map((card) => ({
    event_id:Number(card.getAttribute('data-event-id')), card:inspect(card), media:inspect(card.querySelector('[data-card-media-shell]')),
    title:inspect(card.querySelector('[data-card-title]')), body:inspect(card.querySelector('.event-card__body')),
    utility:inspect(card.querySelector('.event-card__utility-row')), feedback:inspect(card.querySelector('.event-card__feedback--under')),
  }));
  return { scroll:{ x:scrollX, y:scrollY }, viewport:{ width:innerWidth, height:innerHeight, dpr:devicePixelRatio }, document:{ width:document.documentElement.scrollWidth, height:document.documentElement.scrollHeight }, named, cards };
});

await page.evaluate(() => scrollTo(0, 0));
await page.waitForFunction(() => scrollY === 0, null, { timeout:5_000 });
const topEvidence = await snapshot();
await page.screenshot({ path:path.join(outDir, 'astro-top.png') });
await page.screenshot({ path:path.join(outDir, 'astro-full.png'), fullPage:true });

const scrollTarget = kind === 'mobile' ? 520 : 700;
await page.evaluate((value) => scrollTo(0, value), scrollTarget);
await page.waitForFunction((value) => scrollY === value, scrollTarget, { timeout:5_000 });
await page.waitForTimeout(100);
const scrolledEvidence = await snapshot();
await page.screenshot({ path:path.join(outDir, 'astro-scrolled.png') });

await page.evaluate(() => scrollTo(0, 0));
for (const group of Object.keys(expectedGroups)) {
  await page.locator(`[data-free-collection-event-group="${group}"]`).screenshot({ path:path.join(outDir, `astro-group-${group}.png`) });
}
for (const id of expectedOrder) {
  await page.locator(`[data-event-card][data-event-id="${id}"]`).screenshot({ path:path.join(outDir, `astro-card-${id}.png`) });
}

await writeFile(path.join(outDir, 'geometry-and-style.json'), `${JSON.stringify({ top:topEvidence, scrolled:scrolledEvidence }, null, 2)}\n`);
const receipt = {
  schema_version:'free-collection-browser-receipt.v3', route, scenario:expectedScenario,
  reference_clock:{ iso:referenceIso, timezone:'Europe/Kaliningrad', locale:'ru-RU' }, viewport:{ kind, ...viewport, dpr:1 },
  event_ids:identity.event_ids, groups:identity.groups, all_card_images_decoded:true, image_readiness:imageReadiness,
  scroll_target:scrollTarget, document:topEvidence.document, console_errors:errors,
  capture_files:['astro-top.png','astro-scrolled.png','astro-full.png',...Object.keys(expectedGroups).map((group) => `astro-group-${group}.png`),...expectedOrder.map((id) => `astro-card-${id}.png`)],
};
for (const file of receipt.capture_files) {
  const bytes = await readFile(path.join(outDir, file));
  receipt[`${file}_sha256`] = createHash('sha256').update(bytes).digest('hex');
}
await writeFile(path.join(outDir, 'astro-browser-receipt.v3.json'), `${JSON.stringify(receipt, null, 2)}\n`);
console.log(JSON.stringify(receipt, null, 2));
await context.close();
await browser.close();
