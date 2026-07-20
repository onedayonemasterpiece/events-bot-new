import { createReadStream, existsSync, readFileSync, readdirSync, renameSync, statSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { dirname, extname, join, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const SCRIPT_PATH = fileURLToPath(import.meta.url);
const SITE_DIR = dirname(dirname(SCRIPT_PATH));
const REQUIRED_CHECKS = Object.freeze([
  'related_geometry_crop',
  'canonical_event_cards',
  'gallery_cross_document',
  'footer_shortcuts',
]);

function invariant(condition, message) {
  if (!condition) throw new Error(`Browser release gate failed: ${message}`);
}

function parseArgs(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (!value.startsWith('--')) throw new Error(`Unexpected argument: ${value}`);
    const [rawKey, inline] = value.slice(2).split('=', 2);
    const next = inline ?? argv[++index];
    if (!next || next.startsWith('--')) throw new Error(`Missing value for --${rawKey}`);
    args[rawKey.replaceAll('-', '_')] = next;
  }
  return args;
}

export function releaseRootMetadata(rootPath, manifestOverride = '') {
  const root = resolve(rootPath);
  invariant(existsSync(root) && statSync(root).isDirectory(), `release root is missing: ${root}`);
  const candidates = manifestOverride
    ? [resolve(manifestOverride)]
    : [join(root, 'secret-candidate-manifest.json'), join(root, 'static-release-manifest.json')];
  const manifestPath = candidates.find((path) => existsSync(path)) || null;
  let manifest = null;
  if (manifestPath) manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
  const previewPath = join(root, 'preview-build.json');
  const preview = existsSync(previewPath) ? JSON.parse(readFileSync(previewPath, 'utf8')) : null;
  const rawBase = String(manifest?.base_path ?? preview?.basePath ?? '/').trim();
  const basePath = rawBase && rawBase !== '/' ? `/${rawBase.replace(/^\/+|\/+$/gu, '')}` : '';
  invariant(!basePath.includes('..') && !basePath.includes('\\'), 'unsafe release base path');
  return { root, manifestPath, manifest, basePath };
}

export function recordBrowserVisualSuccess(manifestPath, report) {
  invariant(manifestPath, 'release manifest is required for browser_visual receipt');
  invariant(report?.ok === true, 'refusing to record browser_visual before a successful gate');
  for (const check of REQUIRED_CHECKS) invariant(report.checks?.[check] === 'ok', `browser check incomplete: ${check}`);
  const path = resolve(manifestPath);
  const manifest = JSON.parse(readFileSync(path, 'utf8'));
  manifest.checks = manifest.checks && typeof manifest.checks === 'object' ? manifest.checks : {};
  manifest.checks.browser_visual = 'ok';
  const temporary = `${path}.browser-${process.pid}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(manifest, null, 2)}\n`, { encoding: 'utf8', flag: 'wx' });
  renameSync(temporary, path);
  return manifest;
}

function mimeType(path) {
  return ({
    '.css': 'text/css; charset=utf-8', '.gif': 'image/gif', '.html': 'text/html; charset=utf-8',
    '.ico': 'image/x-icon', '.ics': 'text/calendar; charset=utf-8', '.jpeg': 'image/jpeg',
    '.jpg': 'image/jpeg', '.js': 'text/javascript; charset=utf-8', '.json': 'application/json; charset=utf-8',
    '.mjs': 'text/javascript; charset=utf-8', '.png': 'image/png', '.svg': 'image/svg+xml', '.webp': 'image/webp',
    '.woff': 'font/woff', '.woff2': 'font/woff2', '.xml': 'application/xml; charset=utf-8',
  })[extname(path).toLowerCase()] || 'application/octet-stream';
}

export async function startReleaseServer(rootPath, basePath = '') {
  const root = resolve(rootPath);
  const server = createServer((request, response) => {
    try {
      const url = new URL(request.url || '/', 'http://127.0.0.1');
      let pathname = decodeURIComponent(url.pathname);
      if (basePath && (pathname === basePath || pathname.startsWith(`${basePath}/`))) pathname = pathname.slice(basePath.length) || '/';
      const parts = pathname.split('/').filter(Boolean);
      if (parts.some((part) => part === '.' || part === '..' || part.includes('\\'))) throw new Error('unsafe path');
      let path = resolve(root, ...parts);
      if (path !== root && !path.startsWith(`${root}${sep}`)) throw new Error('path escaped root');
      if (existsSync(path) && statSync(path).isDirectory()) path = join(path, 'index.html');
      if (!existsSync(path) || !statSync(path).isFile()) {
        response.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
        response.end('Not found');
        return;
      }
      response.writeHead(200, {
        'content-type': mimeType(path),
        'content-length': statSync(path).size,
        'cache-control': 'no-store',
      });
      if (request.method === 'HEAD') response.end();
      else createReadStream(path).pipe(response);
    } catch (error) {
      response.writeHead(400, { 'content-type': 'text/plain; charset=utf-8' });
      response.end(String(error));
    }
  });
  await new Promise((accept, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', accept);
  });
  const address = server.address();
  invariant(address && typeof address === 'object', 'static server did not bind');
  return {
    origin: `http://127.0.0.1:${address.port}`,
    close: () => new Promise((accept, reject) => server.close((error) => error ? reject(error) : accept())),
  };
}

function eventRoutes(root, basePath) {
  const eventsRoot = join(root, 'sobytiya');
  invariant(existsSync(eventsRoot), 'generated sobytiya directory is missing');
  const routes = readdirSync(eventsRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && existsSync(join(eventsRoot, entry.name, 'index.html')))
    .map((entry) => `${basePath}/sobytiya/${entry.name}/`);
  invariant(routes.length > 0, 'generated release contains no event pages');
  return routes.sort();
}

async function waitForContinuation(page) {
  await page.evaluate(() => window.scrollTo({ top: document.documentElement.scrollHeight, behavior: 'instant' }));
  await page.waitForFunction(() => document.querySelectorAll('[data-personal-feed-section][data-listing-context="event-detail"] [data-personal-feed-slot] > [data-event-card]').length > 0, null, { timeout: 15_000 });
}

async function chooseSpecimen(browser, origin, routes) {
  const page = await browser.newPage({ viewport: { width: 1536, height: 864 } });
  try {
    for (const route of routes.slice(0, 80)) {
      await page.goto(`${origin}${route}`, { waitUntil: 'domcontentloaded' });
      if (await page.locator('[data-desktop-clean-event]').count() !== 1) continue;
      const relatedCount = await page.locator('[data-related-start] [data-event-card]').count();
      const gallery = page.locator('[data-hero-gallery]').first();
      const slideCount = await gallery.locator('[data-hero-gallery-slide][data-gallery-slide-kind="image"]').count();
      const target = await gallery.locator('[data-hero-gallery-slide][data-gallery-slide-kind="cta"] a[href]').getAttribute('href').catch(() => null);
      if (relatedCount < 3 || slideCount < 2 || !target) continue;
      const targetUrl = new URL(target, `${origin}${route}`);
      if (targetUrl.origin !== origin) continue;
      const probe = await browser.newPage({ viewport: { width: 1536, height: 864 } });
      try {
        await probe.goto(targetUrl.href, { waitUntil: 'domcontentloaded' });
        const targetSlides = await probe.locator('[data-hero-gallery]').first().locator('[data-hero-gallery-slide][data-gallery-slide-kind="image"]').count();
        if (targetSlides >= 2 && await probe.locator('[data-clean-hero-image]').count() === 1) return { route, targetPath: targetUrl.pathname };
      } finally {
        await probe.close();
      }
    }
  } finally {
    await page.close();
  }
  throw new Error('Browser release gate failed: no real event pair exercises a multi-image gallery recommendation');
}

async function assertRecommendationGeometry(page, selector, expectedCount = 1) {
  const metrics = await page.locator(selector).evaluateAll((cards) => cards.filter((card) => !card.hidden).map((card) => {
    const shell = card.querySelector('[data-card-media-shell]');
    const image = card.querySelector('[data-card-image]');
    const cardRect = card.getBoundingClientRect();
    const shellRect = shell?.getBoundingClientRect();
    const imageRect = image?.getBoundingClientRect();
    const treatment = card.getAttribute('data-lab-media-treatment') || '';
    const mediaKind = card.getAttribute('data-lab-media-kind') || '';
    const actualCrop = image?.naturalWidth > 0 && image?.naturalHeight > 0 && shellRect?.width > 0 && shellRect?.height > 0
      ? Math.max(0, 1 - Math.min((image.naturalWidth / image.naturalHeight) / (shellRect.width / shellRect.height), (shellRect.width / shellRect.height) / (image.naturalWidth / image.naturalHeight)))
      : 0;
    return {
      id: card.getAttribute('data-event-id'), row: card.getAttribute('data-lab-row-index'), treatment, mediaKind,
      coverCrop: Number(card.getAttribute('data-lab-cover-crop') || 0), rowWorstCrop: Number(card.getAttribute('data-lab-row-worst-crop') || 0), actualCrop,
      card: { top: cardRect.top, left: cardRect.left, right: cardRect.right, width: cardRect.width },
      shell: shellRect ? { top: shellRect.top, left: shellRect.left, right: shellRect.right, width: shellRect.width, height: shellRect.height } : null,
      image: imageRect ? { left: imageRect.left, right: imageRect.right, width: imageRect.width, height: imageRect.height } : null,
      objectFit: image ? getComputedStyle(image).objectFit : '',
      href: card.getAttribute('data-card-href'), titleHref: card.querySelector('[data-card-title]')?.getAttribute('href'),
      mediaHref: card.querySelector('[data-card-media-link]')?.getAttribute('href'), variant: card.getAttribute('data-feed-card-variant'),
      tabIndex: card.getAttribute('tabindex'), actions: {
        like: card.querySelectorAll('[data-feedback-action="like"]').length,
        dislike: card.querySelectorAll('[data-feedback-action="not_interested"]').length,
        share: card.querySelectorAll('[data-native-share]').length,
      },
    };
  }));
  invariant(metrics.length >= expectedCount, `${selector} has ${metrics.length} cards, expected at least ${expectedCount}`);
  for (const item of metrics) {
    invariant(item.shell && item.shell.width > 100 && item.shell.height > 80, `card ${item.id} has invalid media shell geometry`);
    invariant(item.image && item.image.left >= item.shell.left - 1 && item.image.right <= item.shell.right + 1, `card ${item.id} image escapes its shell`);
    invariant(item.variant === 'split-actions' && item.tabIndex === '0', `card ${item.id} bypasses canonical split EventCard behavior`);
    invariant(item.href && item.href === item.titleHref && item.href === item.mediaHref, `card ${item.id} has divergent canonical links`);
    invariant(item.actions.like === 1 && item.actions.dislike === 1 && item.actions.share === 1, `card ${item.id} has a non-canonical action set`);
    const expectedFit = item.treatment === 'document-contain' ? 'contain' : 'cover';
    invariant(item.objectFit === expectedFit, `card ${item.id} crop mode ${item.objectFit} != ${expectedFit}`);
    if (item.mediaKind === 'document') {
      invariant(item.coverCrop <= 0.2001 && item.rowWorstCrop <= 0.2001, `card ${item.id} exceeds the 20% document crop contract`);
      if (item.objectFit === 'cover') invariant(item.actualCrop <= 0.205, `card ${item.id} visually crops ${item.actualCrop}`);
    }
  }
  const rows = new Map();
  for (const item of metrics) {
    const row = item.row ?? Math.round(item.shell.top);
    rows.set(row, [...(rows.get(row) || []), item]);
  }
  for (const cards of rows.values()) {
    const shellHeights = cards.map((item) => item.shell.height);
    invariant(Math.max(...shellHeights) - Math.min(...shellHeights) <= 2, 'recommendation row media shells do not share one geometry');
    const sorted = [...cards].sort((left, right) => left.card.left - right.card.left);
    for (let index = 1; index < sorted.length; index += 1) invariant(sorted[index - 1].card.right <= sorted[index].card.left + 1, 'recommendation cards overlap');
  }
  return metrics;
}

async function assertRealCardEnter(page, origin, route, zoneSelector) {
  await page.goto(`${origin}${route}`, { waitUntil: 'domcontentloaded' });
  if (zoneSelector.includes('personal-feed')) await waitForContinuation(page);
  const card = page.locator(`${zoneSelector} [data-event-card]`).first();
  const href = await card.getAttribute('data-card-href');
  invariant(href, `missing canonical EventCard href in ${zoneSelector}`);
  const expected = new URL(href, page.url()).href;
  await card.focus();
  await Promise.all([
    page.waitForURL((url) => url.href === expected, { timeout: 12_000 }),
    page.keyboard.press('Enter'),
  ]);
  invariant(page.url() === expected && await page.locator('[data-desktop-clean-event]').count() === 1, `real Enter navigation failed in ${zoneSelector}`);
}

async function assertGalleryCrossDocument(page, origin, route) {
  await page.goto(`${origin}${route}`, { waitUntil: 'domcontentloaded' });
  const gallery = page.locator('[data-hero-gallery]').first();
  await page.locator('[data-hero-gallery-open]').first().click();
  await gallery.waitFor({ state: 'visible' });
  const slides = gallery.locator('[data-hero-gallery-slide]');
  const slideCount = await slides.count();
  invariant(slideCount >= 3, 'gallery specimen does not include images plus recommendation');
  for (let index = 1; index < slideCount; index += 1) await page.keyboard.press('ArrowRight');
  const recommendation = gallery.locator('[data-hero-gallery-slide][data-gallery-slide-kind="cta"][aria-hidden="false"] a[href]');
  const href = await recommendation.getAttribute('href');
  invariant(href, 'gallery final recommendation has no real link');
  const expected = new URL(href, page.url()).href;
  // Do not intercept or prevent the click: this must create a fresh document.
  await Promise.all([
    page.waitForURL((url) => url.href === expected, { timeout: 12_000, waitUntil: 'domcontentloaded' }),
    page.keyboard.press('Enter'),
  ]);
  invariant(page.url() === expected, 'gallery Enter did not follow its real recommendation URL');
  const active = await page.evaluate(() => document.activeElement?.tagName);
  invariant(active === 'BODY' || active === 'HTML', `cross-document focus should land on BODY/HTML, got ${active}`);
  const hero = page.locator('[data-clean-hero-image]');
  const initial = await hero.getAttribute('src');
  await page.keyboard.press('ArrowRight');
  await page.waitForTimeout(100);
  const next = await hero.getAttribute('src');
  invariant(next && next !== initial, 'BODY ArrowRight did not advance the destination hero');
  await page.keyboard.press('ArrowLeft');
  await page.waitForTimeout(100);
  invariant(await hero.getAttribute('src') === initial, 'BODY ArrowLeft did not restore the destination hero');
}

async function assertFooterShortcuts(page, origin, route) {
  await page.goto(`${origin}${route}`, { waitUntil: 'domcontentloaded' });
  const footer = page.locator('[data-service-share-root][data-service-share-surface="footer"]');
  await page.evaluate(() => window.scrollTo({ top: document.documentElement.scrollHeight, behavior: 'instant' }));
  await footer.waitFor({ state: 'visible' });
  await page.waitForFunction(() => document.querySelector('[data-service-share-surface="footer"]')?.getAttribute('data-service-share-ready') === 'file', null, { timeout: 12_000 });
  await page.evaluate(() => document.activeElement?.blur());
  invariant(['BODY', 'HTML'].includes(await page.evaluate(() => document.activeElement?.tagName)), 'footer BODY specimen unexpectedly owns focused control');
  await page.keyboard.press('KeyP');
  await page.waitForFunction(() => window.__releaseGateClipboard.images.length === 1, null, { timeout: 8_000 });
  await page.waitForFunction(() => /Карточка скопирована/iu.test(document.querySelector('[data-keyboard-action-toast]')?.textContent || ''), null, { timeout: 8_000 });
  await page.keyboard.press('KeyS');
  await page.waitForFunction(() => window.__releaseGateClipboard.text.at(-1)?.endsWith('\nhttps://kenigevents.ru/'), null, { timeout: 8_000 });
  await page.waitForFunction(() => /Текст и ссылка скопированы/iu.test(document.querySelector('[data-keyboard-action-toast]')?.textContent || ''), null, { timeout: 8_000 });

  // Keep a real event owner focused but offscreen. The footer viewport owns the
  // service shortcuts; the test never focuses either footer button.
  await page.locator('[data-keyboard-event-surface]').focus();
  await page.evaluate(() => window.scrollTo({ top: document.documentElement.scrollHeight, behavior: 'instant' }));
  invariant(await page.evaluate(() => {
    const rect = document.activeElement?.getBoundingClientRect();
    return Boolean(rect && rect.bottom < 0);
  }), 'offscreen-focus footer specimen was not established');
  await page.keyboard.press('KeyP');
  await page.waitForFunction(() => window.__releaseGateClipboard.images.length === 2, null, { timeout: 8_000 });
  await page.keyboard.press('KeyS');
  await page.waitForFunction(() => window.__releaseGateClipboard.text.length >= 2 && window.__releaseGateClipboard.text.at(-1)?.endsWith('\nhttps://kenigevents.ru/'), null, { timeout: 8_000 });
  invariant(await footer.locator('[data-service-share-intent="image"]:focus, [data-service-share-intent="text"]:focus').count() === 0, 'browser gate must not focus footer controls');
}

async function runBrowserGate({ root, basePath, origin, browser }) {
  const routes = eventRoutes(root, basePath);
  const specimen = await chooseSpecimen(browser, origin, routes);
  const context = await browser.newContext({ viewport: { width: 1536, height: 864 } });
  await context.addInitScript(() => {
    window.__releaseGateClipboard = { text: [], images: [] };
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: {
        writeText: async (value) => { window.__releaseGateClipboard.text.push(String(value)); },
        write: async (items) => { window.__releaseGateClipboard.images.push(items.map((item) => [...item.types])); },
      },
    });
    Object.defineProperty(navigator, 'share', { configurable: true, value: undefined });
  });
  const page = await context.newPage();
  const checks = {};
  try {
    const dogRoute = routes.find((route) => /-6408\/$/u.test(route));
    const cropRoutes = [...new Set([specimen.route, dogRoute].filter(Boolean))];
    let related = [];
    let continuation = [];
    for (const route of cropRoutes) {
      await page.goto(`${origin}${route}`, { waitUntil: 'domcontentloaded' });
      await page.waitForLoadState('networkidle').catch(() => {});
      await waitForContinuation(page);
      const routeRelated = await assertRecommendationGeometry(page, '[data-related-start] [data-event-card]', 3);
      const routeContinuation = await assertRecommendationGeometry(page, '[data-personal-feed-section][data-listing-context="event-detail"] [data-personal-feed-slot] > [data-event-card]', 1);
      invariant(routeContinuation.length <= 6, `Ещё события is unbounded on ${route}: ${routeContinuation.length}`);
      related.push(...routeRelated);
      continuation.push(...routeContinuation);
    }
    checks.related_geometry_crop = 'ok';
    invariant(await page.evaluate(() => typeof window.KenigEventsCreateEventCard === 'function'), 'canonical EventCard runtime renderer is missing');
    await assertRealCardEnter(page, origin, specimen.route, '[data-related-start]');
    await assertRealCardEnter(page, origin, specimen.route, '[data-personal-feed-section][data-listing-context="event-detail"] [data-personal-feed-slot]');
    checks.canonical_event_cards = 'ok';
    await assertGalleryCrossDocument(page, origin, specimen.route);
    checks.gallery_cross_document = 'ok';
    await assertFooterShortcuts(page, origin, specimen.route);
    checks.footer_shortcuts = 'ok';
    return {
      ok: true, checks,
      specimen: { route: specimen.route, gallery_target: specimen.targetPath, crop_routes: cropRoutes },
      related_cards: related.length, continuation_cards: continuation.length,
    };
  } finally {
    await context.close();
  }
}

export async function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  const metadata = releaseRootMetadata(args.root || join(SITE_DIR, 'dist'), args.manifest || '');
  const server = await startReleaseServer(metadata.root, metadata.basePath);
  const { chromium } = await import('playwright');
  const browser = await chromium.launch({ headless: true });
  try {
    const report = await runBrowserGate({ root: metadata.root, basePath: metadata.basePath, origin: server.origin, browser });
    if (metadata.manifestPath) recordBrowserVisualSuccess(metadata.manifestPath, report);
    if (args.report) writeFileSync(resolve(args.report), `${JSON.stringify(report, null, 2)}\n`);
    console.log(`Browser release gate passed: ${JSON.stringify(report)}`);
    return report;
  } finally {
    await browser.close();
    await server.close();
  }
}

if (process.argv[1] && resolve(process.argv[1]) === resolve(SCRIPT_PATH)) {
  main().catch((error) => {
    console.error(error?.stack || error);
    process.exitCode = 1;
  });
}
