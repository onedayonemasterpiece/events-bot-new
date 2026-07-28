import { createReadStream, existsSync, mkdirSync, readFileSync, readdirSync, renameSync, statSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { dirname, extname, join, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const SCRIPT_PATH = fileURLToPath(import.meta.url);
const SITE_DIR = dirname(dirname(SCRIPT_PATH));
const REQUIRED_CHECKS = Object.freeze([
  'hero_gallery_crop',
  'related_geometry_crop',
  'related_loaded_media',
  'canonical_event_cards',
  'spatial_card_keyboard',
  'cold_and_pointer_keyboard',
  'gallery_cross_document',
  'footer_shortcuts',
  'festival_calendar',
]);
export const BROWSER_GATE_ACTION_TIMEOUT_MS = 8_000;
export const BROWSER_GATE_NAVIGATION_TIMEOUT_MS = 12_000;

function invariant(condition, message) {
  if (!condition) throw new Error(`Browser release gate failed: ${message}`);
}

export function expectedObjectFitForTreatment(treatment) {
  return String(treatment || '').endsWith('-contain') ? 'contain' : 'cover';
}

export function localReleaseAssetPath(value) {
  let url;
  try {
    url = new URL(String(value || ''));
  } catch (_) {
    return null;
  }
  if (url.hostname !== 'static.kenigevents.ru') return null;
  const marker = url.pathname.indexOf('/_astro/');
  return marker >= 0 ? url.pathname.slice(marker) : null;
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
    close: () => new Promise((accept, reject) => {
      server.close((error) => error ? reject(error) : accept());
      // A release page can still be streaming an image when Chromium closes.
      // Do not let that socket keep the mandatory gate alive indefinitely.
      server.closeIdleConnections?.();
      server.closeAllConnections?.();
    }),
  };
}

function configureGatePage(page) {
  page.setDefaultTimeout(BROWSER_GATE_ACTION_TIMEOUT_MS);
  page.setDefaultNavigationTimeout(BROWSER_GATE_NAVIGATION_TIMEOUT_MS);
  return page;
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

function routeFile(root, basePath, route) {
  let pathname = new URL(route, 'https://release.invalid').pathname;
  if (basePath && (pathname === basePath || pathname.startsWith(`${basePath}/`))) {
    pathname = pathname.slice(basePath.length) || '/';
  }
  const parts = pathname.split('/').filter(Boolean);
  invariant(!parts.some((part) => part === '.' || part === '..' || part.includes('\\')), `unsafe generated route: ${route}`);
  return join(root, ...parts, 'index.html');
}

function galleryTargetFromHtml(html) {
  const match = /data-gallery-slide-kind="cta"[\s\S]{0,6000}?<a[^>]+href="([^"]+)"/u.exec(html);
  return match?.[1]?.replaceAll('&amp;', '&') || null;
}

function hasClosedHeroCarousel(html) {
  // Event pages contain separate mobile/desktop gallery DOM, so counting raw
  // gallery slides mistakes one duplicated image for a multi-image hero. The
  // canonical desktop renderer only emits this owner id when photoCount > 1.
  return /<main\b[^>]*data-desktop-clean-event[^>]*data-closed-hero-gallery="[^"]+"/u.test(html);
}

export function staticSpecimenCandidates(root, basePath, routes) {
  const routeSet = new Set(routes);
  const candidates = [];
  for (const route of routes) {
    const path = routeFile(root, basePath, route);
    if (!existsSync(path)) continue;
    const html = readFileSync(path, 'utf8');
    if (!html.includes('data-desktop-clean-event') || !html.includes('data-related-start')) continue;
    if ((html.match(/data-event-card(?:=|\s|>)/gu) || []).length < 3) continue;
    if (!hasClosedHeroCarousel(html)) continue;
    const target = galleryTargetFromHtml(html);
    if (!target) continue;
    const targetPath = new URL(target, `https://release.invalid${route}`).pathname;
    if (!routeSet.has(targetPath)) continue;
    const targetFile = routeFile(root, basePath, targetPath);
    if (!existsSync(targetFile)) continue;
    const targetHtml = readFileSync(targetFile, 'utf8');
    if (!targetHtml.includes('data-clean-hero-image')) continue;
    if (!hasClosedHeroCarousel(targetHtml)) continue;
    candidates.push({ route, targetPath });
  }
  // Keep the user-reported production journey as the first canary while it is
  // present, but remain data-driven when that event leaves the active catalog.
  return candidates.sort((left, right) => Number(!/-6408\/$/u.test(left.route)) - Number(!/-6408\/$/u.test(right.route)));
}

function singleImageSpecimen(root, basePath, routes) {
  return routes.find((route) => {
    const html = readFileSync(routeFile(root, basePath, route), 'utf8');
    return html.includes('data-desktop-clean-event')
      && html.includes('data-clean-hero-image')
      && !hasClosedHeroCarousel(html);
  }) || null;
}

export function staticHeroCropCandidates(root, basePath, routes) {
  const byPresentation = new Map();
  for (const route of routes) {
    const html = readFileSync(routeFile(root, basePath, route), 'utf8');
    const main = /<main\b[^>]*data-desktop-clean-event[^>]*>/u.exec(html)?.[0] || '';
    if (!main.includes('data-selected-media-policy="visual_only"')) continue;
    const family = /data-desktop-family="([^"]+)"/u.exec(main)?.[1] || 'unknown';
    const splitFit = /data-split-media-fit="([^"]+)"/u.exec(main)?.[1] || 'none';
    const key = `${family}:${splitFit}`;
    if (!byPresentation.has(key) || /-6408\/$/u.test(route)) byPresentation.set(key, route);
  }
  return [...byPresentation.values()].sort((left, right) => Number(!/-6408\/$/u.test(left)) - Number(!/-6408\/$/u.test(right)));
}

async function waitForContinuation(page) {
  // Hydration intentionally starts when the end-of-related marker approaches
  // the viewport. Jumping straight to scrollHeight can skip that marker in one
  // frame and never fire IntersectionObserver. Exercise the same boundary a
  // wheel/touchpad user crosses, then wait on the rendered-card contract.
  const trigger = page.locator('[data-hide-sticky-after]:visible').first();
  invariant(await trigger.count() === 1, 'event continuation trigger is missing');
  await trigger.scrollIntoViewIfNeeded();
  await page.mouse.wheel(0, 320);
  await page.waitForFunction(() => {
    const section = document.querySelector('[data-personal-feed-section][data-listing-context="event-detail"]');
    const cards = section?.querySelectorAll('[data-personal-feed-slot] > [data-event-card]').length || 0;
    return cards > 0 || section?.getAttribute('data-personal-feed-mode') === 'unavailable';
  }, null, { timeout: 15_000 });
  const state = await page.locator('[data-personal-feed-section][data-listing-context="event-detail"]').evaluate((section) => ({
    cards: section.querySelectorAll('[data-personal-feed-slot] > [data-event-card]').length,
    mode: section.getAttribute('data-personal-feed-mode'),
    status: section.querySelector('[data-personal-feed-status]')?.textContent?.trim() || '',
  }));
  invariant(state.cards > 0, `event continuation did not render cards: mode=${state.mode} status=${state.status}`);
}

async function chooseSpecimen(browser, origin, candidates) {
  const page = configureGatePage(await browser.newPage({ viewport: { width: 1536, height: 864 } }));
  try {
    for (const candidate of candidates.slice(0, 12)) {
      const { route, targetPath } = candidate;
      await page.goto(`${origin}${route}`, { waitUntil: 'domcontentloaded' });
      if (await page.locator('[data-desktop-clean-event]').count() !== 1) continue;
      const relatedCount = await page.locator('[data-related-start] [data-event-card]').count();
      const opener = page.locator('[data-desktop-clean-event] [data-hero-gallery-open]').first();
      const galleryId = await opener.getAttribute('data-hero-gallery-open').catch(() => null);
      if (!galleryId) continue;
      const gallery = page.locator(`[id="${galleryId}"]`);
      const slideCount = await gallery.locator('[data-hero-gallery-slide][data-gallery-slide-kind="image"]').count();
      const target = await gallery.locator('[data-hero-gallery-slide][data-gallery-slide-kind="cta"] a[href]').getAttribute('href').catch(() => null);
      if (relatedCount < 3 || slideCount < 2 || !target) continue;
      const targetUrl = new URL(target, `${origin}${route}`);
      if (targetUrl.origin !== origin) continue;
      if (targetUrl.pathname !== targetPath) continue;
      const probe = configureGatePage(await browser.newPage({ viewport: { width: 1536, height: 864 } }));
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
  await page.waitForFunction((cardSelector) => Array.from(document.querySelectorAll(cardSelector))
    .filter((card) => !card.hidden)
    .every((card) => {
      const shell = card.querySelector('[data-card-media-shell]');
      const image = card.querySelector('[data-card-image]');
      if (!(image instanceof HTMLImageElement) || !image.getAttribute('src')) return true;
      return (image.complete && image.naturalWidth > 0 && shell?.classList.contains('is-image-loaded'))
        || shell?.classList.contains('is-image-missing');
    }), selector, { timeout: 12_000 });
  // `complete`/`naturalWidth` prove fetch success but not that Chromium has
  // decoded the lazy image into a paintable frame. Evidence screenshots must
  // capture the settled pixels, not a transient neutral shell.
  await page.locator(selector).evaluateAll(async (cards) => {
    const images = cards.flatMap((card) => Array.from(card.querySelectorAll('[data-card-image]')));
    await Promise.all(images.map((image) => image instanceof HTMLImageElement && image.currentSrc
      ? image.decode().catch(() => undefined)
      : Promise.resolve()));
  });
  const metrics = await page.locator(selector).evaluateAll((cards) => cards.filter((card) => !card.hidden).map((card) => {
    const shell = card.querySelector('[data-card-media-shell]');
    const image = card.querySelector('[data-card-image]');
    const fallback = card.querySelector('[data-card-image-fallback]');
    const body = card.querySelector('.event-card__body');
    const cardRect = card.getBoundingClientRect();
    const shellRect = shell?.getBoundingClientRect();
    const imageRect = image?.getBoundingClientRect();
    const bodyRect = body?.getBoundingClientRect();
    const bodyContentBottom = body
      ? Math.max(bodyRect?.top || 0, ...Array.from(body.children).map((child) => child.getBoundingClientRect().bottom))
      : 0;
    const treatment = card.getAttribute('data-lab-media-treatment') || '';
    const cropReason = card.getAttribute('data-lab-crop-reason') || '';
    const mediaKind = card.getAttribute('data-lab-media-kind') || '';
    const actualCrop = image?.naturalWidth > 0 && image?.naturalHeight > 0 && shellRect?.width > 0 && shellRect?.height > 0
      ? Math.max(0, 1 - Math.min((image.naturalWidth / image.naturalHeight) / (shellRect.width / shellRect.height), (shellRect.width / shellRect.height) / (image.naturalWidth / image.naturalHeight)))
      : 0;
    const objectFit = image ? getComputedStyle(image).objectFit : '';
    // Independent product-visible empty-frame budget: `cover` fills the shell;
    // `contain` leaves the ratio loss the owner saw as 22%/32% bands.
    const unusedFrameRatio = objectFit === 'contain' ? actualCrop : 0;
    return {
      id: card.getAttribute('data-event-id'), row: card.getAttribute('data-lab-row-index'), treatment, cropReason, mediaKind,
      coverCrop: Number(card.getAttribute('data-lab-cover-crop') || 0), rowWorstCrop: Number(card.getAttribute('data-lab-row-worst-crop') || 0), actualCrop, unusedFrameRatio,
      card: { top: cardRect.top, left: cardRect.left, right: cardRect.right, bottom:cardRect.bottom, width: cardRect.width, height:cardRect.height },
      shell: shellRect ? { top: shellRect.top, left: shellRect.left, right: shellRect.right, width: shellRect.width, height: shellRect.height } : null,
      body: bodyRect ? { height:bodyRect.height, unused:Math.max(0, bodyRect.bottom - bodyContentBottom) } : null,
      image: imageRect ? { left: imageRect.left, right: imageRect.right, width: imageRect.width, height: imageRect.height } : null,
      objectFit,
      imageOpacity: image ? Number(getComputedStyle(image).opacity || 1) : 0,
      imageVisibility: image ? getComputedStyle(image).visibility : '',
      imageLoaded: Boolean(image?.complete && image?.naturalWidth > 0 && shell?.classList.contains('is-image-loaded')),
      imageMissing: Boolean(shell?.classList.contains('is-image-missing')),
      fallbackVisible: fallback ? (() => {
        const style = getComputedStyle(fallback);
        const rect = fallback.getBoundingClientRect();
        return !fallback.hidden && style.display !== 'none' && style.visibility !== 'hidden'
          && Number(style.opacity || 1) > 0 && rect.width > 0 && rect.height > 0;
      })() : false,
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
    invariant(item.imageLoaded || item.imageMissing, `card ${item.id} media never reached loaded or missing state`);
    if (item.imageLoaded) {
      invariant(!item.fallbackVisible, `card ${item.id} exposes fallback text behind a loaded ${item.objectFit} image`);
      invariant(item.imageOpacity > 0.99 && item.imageVisibility === 'visible', `card ${item.id} loaded image is not paint-visible`);
    }
    if (item.imageMissing) invariant(item.fallbackVisible, `card ${item.id} lost its fallback after image failure`);
    // The serialized treatment is the source-of-truth contract produced by
    // resolveRelatedCardMediaTreatment(). Classified OCR/documents may cover
    // only inside their explicit 20% budget. Unknown/error media has no
    // positive crop evidence and must instead remain whole, fail closed.
    const expectedFit = expectedObjectFitForTreatment(item.treatment);
    invariant(item.objectFit === expectedFit, `card ${item.id} crop mode ${item.objectFit} != ${expectedFit}`);
    if (item.mediaKind === 'visual') {
      invariant(item.treatment === 'visual-cover' && item.objectFit === 'cover', `visual card ${item.id} is letterboxed as ${item.treatment}/${item.objectFit}`);
      invariant(item.unusedFrameRatio <= 0.001, `visual card ${item.id} leaves ${(item.unusedFrameRatio * 100).toFixed(1)}% of its media frame unused`);
    }
    if (item.mediaKind === 'document') {
      if (item.treatment === 'document-safe-cover') {
        invariant(item.objectFit === 'cover', `bounded document card ${item.id} does not cover its frame`);
        invariant(item.unusedFrameRatio <= 0.001, `bounded document card ${item.id} leaves ${(item.unusedFrameRatio * 100).toFixed(1)}% of its media frame unused`);
        invariant(item.coverCrop <= 0.2001, `card ${item.id} exceeds the 20% document crop contract: cover=${item.coverCrop}`);
        invariant(item.actualCrop <= 0.205, `card ${item.id} visually crops ${item.actualCrop}`);
      } else {
        invariant(item.treatment === 'document-contain' && item.objectFit === 'contain', `fail-closed document card ${item.id} has invalid ${item.treatment}/${item.objectFit}`);
        invariant(
          ['semantic_error_fail_closed', 'unknown_media_fail_closed', 'document_dimensions_unknown'].includes(item.cropReason),
          `document card ${item.id} contains without a fail-closed reason: ${item.cropReason || '(missing)'}`,
        );
        invariant(item.coverCrop === 0, `fail-closed document card ${item.id} exposes a numeric crop claim`);
      }
    }
  }
  const rows = new Map();
  for (const item of metrics) {
    const row = item.row ?? Math.round(item.shell.top);
    rows.set(row, [...(rows.get(row) || []), item]);
  }
  const orderedRows = [...rows.entries()].sort(([left], [right]) => Number(left) - Number(right));
  for (const [rowOffset, [, cards]] of orderedRows.entries()) {
    invariant(cards.length === 3 || rowOffset === orderedRows.length - 1, `recommendation row ${rowOffset} is incomplete before the final row (${cards.length}/3)`);
    const shellHeights = cards.map((item) => item.shell.height);
    const cardHeights = cards.map((item) => item.card.height);
    invariant(Math.max(...shellHeights) - Math.min(...shellHeights) <= 2, 'recommendation row media shells do not share one geometry');
    invariant(Math.max(...cardHeights) - Math.min(...cardHeights) <= 2, 'recommendation row cards do not share one total height');
    const chromeHeights = cards.map((item) => item.card.height - item.shell.height);
    invariant(Math.max(...chromeHeights) - Math.min(...chromeHeights) <= 2, 'recommendation cards do not share row-local chrome height');
    invariant(Math.min(...cards.map((item) => item.body?.unused ?? Number.POSITIVE_INFINITY)) <= 32, `recommendation row ${rowOffset} reserves excessive body whitespace`);
    const sorted = [...cards].sort((left, right) => left.card.left - right.card.left);
    for (let index = 1; index < sorted.length; index += 1) invariant(sorted[index - 1].card.right <= sorted[index].card.left + 1, 'recommendation cards overlap');
  }
  return metrics;
}

async function assertHeroGalleryCrop(page) {
  const hero = page.locator('[data-clean-hero-image]').first();
  await hero.waitFor({ state:'visible' });
  await hero.evaluate(async (image) => {
    if (image instanceof HTMLImageElement && image.currentSrc) await image.decode().catch(() => undefined);
  });
  const result = await page.evaluate(() => {
    const root = document.querySelector('[data-desktop-family]');
    const image = document.querySelector('[data-clean-hero-image]');
    if (!(image instanceof HTMLImageElement)) return null;
    const mode = root?.getAttribute('data-selected-media-policy') || '';
    const fit = getComputedStyle(image).objectFit;
    const gallery = Array.from(document.querySelectorAll('[data-hero-gallery-slide][data-gallery-slide-kind="image"] img')).map((entry) => ({
      mode:entry.getAttribute('data-image-text-mode') || '',
      fit:getComputedStyle(entry).objectFit,
    }));
    return { mode, fit, declared:image.getAttribute('data-protected-crop-fit') || '', gallery };
  });
  invariant(result, 'desktop hero image is missing');
  if (result.mode === 'visual_only') {
    invariant(result.fit === 'cover' && result.declared === 'cover', `non-OCR hero is letterboxed as ${result.declared}/${result.fit}`);
  }
  for (const item of result.gallery) {
    if (item.mode === 'visual_only') invariant(item.fit === 'cover', `non-OCR gallery slide is letterboxed as ${item.fit}`);
    else invariant(item.fit === 'contain', `OCR/document gallery slide is unexpectedly ${item.fit}`);
  }
  return result;
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

async function dispatchPhysicalKey(page, code, key) {
  return page.evaluate(({ physicalCode, logicalKey }) => {
    const target = document.activeElement === document.body || document.activeElement === document.documentElement
      ? document.body
      : document.activeElement;
    const down = new KeyboardEvent('keydown', {
      code:physicalCode, key:logicalKey, bubbles:true, cancelable:true,
    });
    const accepted = target.dispatchEvent(down);
    target.dispatchEvent(new KeyboardEvent('keyup', {
      code:physicalCode, key:logicalKey, bubbles:true, cancelable:true,
    }));
    return { accepted, target:target?.tagName || '' };
  }, { physicalCode:code, logicalKey:key });
}

async function clickInertCurrentEventPoint(page) {
  const scope = page.locator('[data-desktop-clean-event] .desktop-clean-description').first();
  invariant(await scope.count() === 1, 'current event has no inert description scope for mixed-input acceptance');
  await scope.scrollIntoViewIfNeeded();
  const point = await scope.evaluate((element) => {
    const rect = element.getBoundingClientRect();
    const blocked = 'a,button,input,textarea,select,[contenteditable="true"],[role="dialog"],[data-service-share-root]';
    const left = Math.max(1, Math.ceil(rect.left + 4));
    const right = Math.min(innerWidth - 2, Math.floor(rect.right - 4));
    const top = Math.max(1, Math.ceil(rect.top + 4));
    const bottom = Math.min(innerHeight - 2, Math.floor(rect.bottom - 4));
    for (let y = top; y <= bottom; y += 16) {
      for (let x = left; x <= right; x += 16) {
        const target = document.elementFromPoint(x, y);
        if (target && element.contains(target) && !target.closest(blocked)) return { x, y, tag:target.tagName };
      }
    }
    return null;
  });
  invariant(point, 'could not find a real inert pointer point inside the current event');
  await page.mouse.click(point.x, point.y);
  invariant(['BODY', 'HTML'].includes(await page.evaluate(() => document.activeElement?.tagName)), `inert current-event click unexpectedly focused ${await page.evaluate(() => document.activeElement?.tagName)}`);
  return point;
}

async function clickInertWithin(page, selector, label) {
  const scope = page.locator(selector).first();
  invariant(await scope.count() === 1, `${label} scope is missing`);
  await scope.evaluate((element) => element.scrollIntoView({ block:'nearest', inline:'nearest' }));
  const point = await scope.evaluate((element) => {
    const rect = element.getBoundingClientRect();
    const blocked = 'a,button,input,textarea,select,[contenteditable="true"]';
    for (let y = Math.max(1, Math.ceil(rect.top + 3)); y <= Math.min(innerHeight - 2, Math.floor(rect.bottom - 3)); y += 8) {
      for (let x = Math.max(1, Math.ceil(rect.left + 3)); x <= Math.min(innerWidth - 2, Math.floor(rect.right - 3)); x += 8) {
        const target = document.elementFromPoint(x, y);
        if (target && element.contains(target) && !target.closest(blocked)) return { x, y, tag:target.tagName };
      }
    }
    return null;
  });
  invariant(point, `could not find a real inert pointer point inside ${label}`);
  await page.mouse.click(point.x, point.y);
  return point;
}

async function visualCardGrid(page, selector) {
  return page.locator(selector).evaluateAll((cards) => {
    const measured = cards.map((card) => {
      const rect = card.getBoundingClientRect();
      return { id:card.getAttribute('data-event-id'), top:rect.top, left:rect.left, centerX:rect.left + rect.width / 2 };
    }).sort((left, right) => left.top - right.top || left.left - right.left);
    const rows = [];
    for (const entry of measured) {
      const row = rows.at(-1);
      if (!row || Math.abs(row.top - entry.top) > 16) rows.push({ top:entry.top, cards:[entry] });
      else row.cards.push(entry);
    }
    rows.forEach((row) => row.cards.sort((left, right) => left.left - right.left));
    return rows.map((row) => row.cards);
  });
}

async function assertSpatialCardKeyboard(page, origin, route) {
  const relatedSelector = '[data-related-start] [data-event-card]';
  const continuationSelector = '[data-personal-feed-section][data-listing-context="event-detail"] [data-personal-feed-slot] > [data-event-card]';
  await page.goto(`${origin}${route}`, { waitUntil:'domcontentloaded' });
  await waitForContinuation(page);
  const relatedRows = await visualCardGrid(page, relatedSelector);
  const continuationRows = await visualCardGrid(page, continuationSelector);
  invariant(relatedRows.length >= 2 && relatedRows[0].length >= 2, 'spatial keyboard specimen has no multi-row related grid');
  invariant(continuationRows.length > 0, 'spatial keyboard specimen has no continuation grid');
  const card = (id) => page.locator(`${relatedSelector}[data-event-id="${id}"]`).first();
  const continuationCard = (id) => page.locator(`${continuationSelector}[data-event-id="${id}"]`).first();
  const activeId = () => page.evaluate(() => document.activeElement?.closest?.('[data-event-card]')?.getAttribute('data-event-id') || '');
  const visibleHints = () => page.locator('[data-related-calendar-shortcut]:visible').count();

  invariant(await visibleHints() === 0, 'unfocused cards expose K hints');
  await card(relatedRows[0][0].id).hover();
  invariant(await visibleHints() === 0, 'hovered card exposes a K hint without focus');
  await card(relatedRows[0][0].id).focus();
  invariant(await visibleHints() === 1, 'focused card must expose exactly one K hint');
  invariant(await card(relatedRows[0][0].id).locator('[data-related-calendar-shortcut]:visible').count() === 1, 'visible K hint belongs to a different card');

  await page.keyboard.press('ArrowRight');
  invariant(await activeId() === relatedRows[0][1].id, 'ArrowRight did not follow the visual row');
  await card(relatedRows[0].at(-1).id).focus();
  await page.keyboard.press('ArrowRight');
  invariant(await activeId() === relatedRows[1][0].id, 'ArrowRight did not wrap to the first card of the next visual row');

  const finalRow = relatedRows.at(-1);
  const previousRow = relatedRows.at(-2);
  if (finalRow.length < previousRow.length) {
    const source = previousRow.at(-1);
    const nearestFinal = finalRow.reduce((best, entry) => Math.abs(entry.centerX - source.centerX) < Math.abs(best.centerX - source.centerX) ? entry : best);
    await card(source.id).focus();
    await page.keyboard.press('ArrowDown');
    invariant(await activeId() === nearestFinal.id, 'ArrowDown did not choose the nearest card in the ragged final row');
    const nearestPrevious = previousRow.reduce((best, entry) => Math.abs(entry.centerX - nearestFinal.centerX) < Math.abs(best.centerX - nearestFinal.centerX) ? entry : best);
    await page.keyboard.press('ArrowUp');
    invariant(await activeId() === nearestPrevious.id, 'ArrowUp did not return from the ragged row by visual center');
  }

  const lastRelated = finalRow.at(-1);
  const firstContinuationRow = continuationRows[0];
  const nearestContinuation = firstContinuationRow.reduce((best, entry) => Math.abs(entry.centerX - lastRelated.centerX) < Math.abs(best.centerX - lastRelated.centerX) ? entry : best);
  await card(lastRelated.id).focus();
  await page.keyboard.press('ArrowDown');
  invariant(await activeId() === nearestContinuation.id, 'ArrowDown did not bridge to the visually nearest continuation card');
  const continuationOwnsCalendar = await continuationCard(nearestContinuation.id).locator('[data-calendar-action]:not([hidden])').count() === 1;
  const expectedContinuationHints = continuationOwnsCalendar ? 1 : 0;
  invariant(
    await visibleHints() === expectedContinuationHints
      && await continuationCard(nearestContinuation.id).locator('[data-related-calendar-shortcut]:visible').count() === expectedContinuationHints,
    continuationOwnsCalendar
      ? 'calendar-eligible continuation focus does not own the single visible K hint'
      : 'calendar-ineligible continuation focus exposes a misleading K hint',
  );
  await page.keyboard.press('ArrowUp');
  invariant(await activeId() === lastRelated.id, 'ArrowUp did not bridge back to the final related row');

  const eligible = await page.locator(`${relatedSelector}:has([data-calendar-action])`).first().getAttribute('data-event-id');
  invariant(eligible, 'spatial keyboard specimen has no calendar-eligible related card');
  await page.evaluate(() => {
    window.__releaseGateCardCalendar = [];
    document.querySelectorAll('[data-event-card] [data-calendar-action]').forEach((action) => action.addEventListener('click', (event) => {
      event.preventDefault();
      event.stopImmediatePropagation();
      window.__releaseGateCardCalendar.push(action.closest('[data-event-card]')?.getAttribute('data-event-id'));
    }, { capture:true }));
  });
  await card(eligible).focus();
  await page.keyboard.press('KeyK');
  invariant(await page.evaluate(() => window.__releaseGateCardCalendar.at(-1)) === eligible, 'KeyK acted on a card other than the visually focused owner');
  invariant(await visibleHints() === 1 && await card(eligible).locator('[data-related-calendar-shortcut]:visible').count() === 1,
    'KeyK result lost the focused card hint invariant');
  return { relatedRows:relatedRows.map((row) => row.map(({ id }) => id)), continuationEntry:nearestContinuation.id, calendarOwner:eligible };
}

async function assertColdAndPointerKeyboard(page, origin, route, { expectMultipleImages = null } = {}) {
  await page.goto(`${origin}${route}`, { waitUntil:'domcontentloaded' });
  await page.locator('[data-keyboard-event-surface]').waitFor({ state:'attached' });
  invariant(['BODY', 'HTML'].includes(await page.evaluate(() => document.activeElement?.tagName)), 'cold event page must retain BODY/HTML focus');
  const hero = page.locator('[data-clean-hero-image]');
  const actualImageCount = await page.locator('[data-desktop-clean-event]').evaluate((root) => {
    const galleryId = root.getAttribute('data-closed-hero-gallery');
    return galleryId
      ? document.querySelectorAll(`#${CSS.escape(galleryId)} [data-hero-gallery-slide][data-gallery-slide-kind="image"]`).length
      : 1;
  });
  const hasMultipleImages = expectMultipleImages === null ? actualImageCount > 1 : expectMultipleImages;
  const before = await hero.getAttribute('src');
  await page.keyboard.press('ArrowRight');
  await page.waitForTimeout(100);
  const after = await hero.getAttribute('src');
  if (hasMultipleImages) {
    invariant(after && after !== before, `cold BODY ArrowRight did not advance ${route}`);
    invariant(await page.locator('[data-keyboard-event-surface]:focus').count() === 1, 'cold hero arrow did not establish current-event focus');
    await page.keyboard.press('ArrowLeft');
    invariant(await hero.getAttribute('src') === before, 'cold ArrowLeft did not restore the original hero');
  } else {
    invariant(after === before, `single-image cold ArrowRight changed ${route}`);
  }

  await page.reload({ waitUntil:'domcontentloaded' });
  await page.locator('[data-keyboard-event-surface]').waitFor({ state:'attached' });
  await page.evaluate(() => {
    window.__releaseGateEventActions = { like:0, calendar:0, primary:0 };
    const surface = document.querySelector('[data-keyboard-event-surface]');
    const observe = (selector, key) => surface?.querySelector(selector)?.addEventListener('click', (event) => {
      window.__releaseGateEventActions[key] += 1;
      event.preventDefault();
      event.stopImmediatePropagation();
    }, { capture:true });
    observe('[data-feedback-action="like"]', 'like');
    observe('[data-calendar-action]', 'calendar');
    observe('.desktop-prototype__primary-action:not(.is-disabled)', 'primary');
  });
  const pointer = await clickInertCurrentEventPoint(page);
  // Explicit Cyrillic `key` values with stable physical `code` prove that the
  // router is layout-independent rather than accidentally matching Latin text.
  await dispatchPhysicalKey(page, 'KeyL', 'д');
  invariant(await page.evaluate(() => window.__releaseGateEventActions.like) === 1, 'inert click + Russian-layout KeyL did not reach current-event Like');
  await dispatchPhysicalKey(page, 'KeyK', 'л');
  invariant(await page.evaluate(() => window.__releaseGateEventActions.calendar) === 1, 'inert click + Russian-layout KeyK did not reach current-event calendar');
  const textWrites = await page.evaluate(() => window.__releaseGateClipboard.text.length);
  await dispatchPhysicalKey(page, 'KeyS', 'ы');
  await page.waitForFunction((count) => window.__releaseGateClipboard.text.length === count + 1, textWrites, { timeout:8_000 });
  invariant((await page.evaluate(() => window.__releaseGateClipboard.text.at(-1)))?.includes('/sobytiya/'), 'current-event KeyS copied the wrong payload');
  await dispatchPhysicalKey(page, 'Enter', 'Enter');
  invariant(await page.evaluate(() => window.__releaseGateEventActions.primary) === 1, 'inert click + Enter did not reach current-event CTA');

  // Unrelated pointer provenance must revoke BODY recovery.
  await page.reload({ waitUntil:'domcontentloaded' });
  await page.locator('[data-keyboard-event-surface]').waitFor({ state:'attached' });
  await page.evaluate(() => {
    window.__releaseGateNegativeLikes = 0;
    document.querySelector('[data-keyboard-event-surface] [data-feedback-action="like"]')?.addEventListener('click', (event) => {
      window.__releaseGateNegativeLikes += 1;
      event.preventDefault();
      event.stopImmediatePropagation();
    }, { capture:true });
  });
  const headerPointer = await clickInertWithin(page, '.site-header', 'site header');
  await page.evaluate(() => document.activeElement?.blur());
  await dispatchPhysicalKey(page, 'KeyL', 'д');
  invariant(await page.evaluate(() => window.__releaseGateNegativeLikes) === 0, 'header provenance leaked current-event KeyL ownership');
  await page.evaluate(() => {
    const input = document.createElement('input');
    input.dataset.releaseGateEditor = '';
    document.querySelector('[data-desktop-clean-event]')?.append(input);
    input.focus({ preventScroll:true });
  });
  await page.keyboard.type('д');
  invariant(await page.locator('[data-release-gate-editor]').inputValue() === 'д', 'editor did not retain native Cyrillic input');
  invariant(await page.evaluate(() => window.__releaseGateNegativeLikes) === 0, 'editable input leaked current-event shortcut ownership');
  await page.evaluate(() => {
    document.querySelector('[data-release-gate-editor]')?.remove();
    const dialog = document.createElement('dialog');
    dialog.dataset.releaseGateDialog = '';
    dialog.innerHTML = '<p>Проверка модального контекста</p>';
    document.body.append(dialog);
    dialog.showModal();
  });
  const dialogPointer = await clickInertWithin(page, '[data-release-gate-dialog]', 'modal dialog');
  await dispatchPhysicalKey(page, 'KeyL', 'д');
  invariant(await page.evaluate(() => window.__releaseGateNegativeLikes) === 0, 'modal dialog leaked current-event KeyL ownership');
  await page.evaluate(() => document.querySelector('[data-release-gate-dialog]')?.remove());
  return {
    coldHeroChanged:after !== before,
    imageCount:actualImageCount,
    inertPointer:pointer,
    headerPointer,
    dialogPointer,
  };
}

async function assertGalleryCrossDocument(page, origin, route) {
  await page.goto(`${origin}${route}`, { waitUntil: 'domcontentloaded' });
  // Event pages intentionally render separate mobile and desktop galleries.
  // Exercise the visible desktop owner rather than relying on DOM order: the
  // first gallery in markup may be the hidden mobile dialog.
  const opener = page.locator('[data-desktop-clean-event] [data-hero-gallery-open]:visible').first();
  const galleryId = await opener.getAttribute('data-hero-gallery-open');
  invariant(galleryId, 'visible desktop gallery opener has no target');
  const gallery = page.locator(`[id="${galleryId}"]`);
  await opener.click();
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
  // Let the continuation insert its cards before targeting the footer. A raw
  // scrollHeight jump races that insertion and can leave the footer above the
  // viewport, which does not reproduce a person actually looking at it.
  await waitForContinuation(page);
  await footer.evaluate((element) => element.scrollIntoView({ block:'center', behavior:'instant' }));
  await footer.waitFor({ state: 'visible' });
  await page.waitForFunction(() => {
    const element = document.querySelector('[data-service-share-surface="footer"]');
    const rect = element?.getBoundingClientRect();
    return Boolean(rect && rect.bottom > 72 && rect.top < window.innerHeight - 72);
  }, null, { timeout: 8_000 });
  await page.waitForFunction(() => document.querySelector('[data-service-share-surface="footer"]')?.getAttribute('data-service-share-ready') === 'file', null, { timeout: 12_000 });
  await page.evaluate(() => document.activeElement?.blur());
  invariant(['BODY', 'HTML'].includes(await page.evaluate(() => document.activeElement?.tagName)), 'footer BODY specimen unexpectedly owns focused control');
  await page.keyboard.press('KeyP');
  await page.waitForFunction(() => window.__releaseGateClipboard.images.length === 1, null, { timeout: 8_000 });
  await page.waitForFunction(() => /Карточка скопирована/iu.test(document.querySelector('[data-mobile-toast-message]')?.textContent || ''), null, { timeout: 8_000 });
  await page.keyboard.press('KeyS');
  await page.waitForFunction(() => window.__releaseGateClipboard.text.at(-1)?.endsWith('\nhttps://kenigevents.ru/'), null, { timeout: 8_000 });
  await page.waitForFunction(() => /Текст и ссылка скопированы/iu.test(document.querySelector('[data-mobile-toast-message]')?.textContent || ''), null, { timeout: 8_000 });

  // Keep a real event owner focused but offscreen. The footer viewport owns the
  // service shortcuts; the test never focuses either footer button.
  await page.locator('[data-keyboard-event-surface]').focus();
  await footer.evaluate((element) => element.scrollIntoView({ block:'center', behavior:'instant' }));
  invariant(await page.evaluate(() => {
    const rect = document.activeElement?.getBoundingClientRect();
    return Boolean(rect && rect.bottom < 0);
  }), 'offscreen-focus footer specimen was not established');
  await page.keyboard.press('KeyP');
  await page.waitForFunction(() => window.__releaseGateClipboard.images.length === 2, null, { timeout: 8_000 });
  // Clipboard write resolves before the async service-share handler finishes
  // its success state. Wait on that concrete UI signal before sending S;
  // otherwise the still-busy P transaction can legitimately reject the next
  // shortcut on slower/prefixed candidate assets.
  await page.waitForFunction(() => /Карточка скопирована/iu.test(document.querySelector('[data-mobile-toast-message]')?.textContent || ''), null, { timeout: 8_000 });
  await page.keyboard.press('KeyS');
  await page.waitForFunction(() => window.__releaseGateClipboard.text.length >= 2 && window.__releaseGateClipboard.text.at(-1)?.endsWith('\nhttps://kenigevents.ru/'), null, { timeout: 8_000 });
  invariant(await footer.locator('[data-service-share-intent="image"]:focus, [data-service-share-intent="text"]:focus').count() === 0, 'browser gate must not focus footer controls');
}

async function assertFestivalCalendar(page, origin, basePath) {
  const route = `${basePath}/festivali/`.replace(/\/+/gu, '/');
  const reports = [];
  for (const viewport of [
    { width: 1440, height: 900 },
    { width: 390, height: 844 },
  ]) {
    await page.setViewportSize(viewport);
    await page.goto(`${origin}${route}`, { waitUntil:'domcontentloaded' });
    const root = page.locator('[data-festival-timeline][data-festival-count="21"]');
    invariant(await root.count() === 1, `festival calendar is missing at ${viewport.width}px`);
    invariant(await root.locator('[data-festival-card]').count() === 21, `festival calendar lost cards at ${viewport.width}px`);
    const images = root.locator('[data-festival-card] img');
    invariant(await images.count() === 21, `festival calendar image inventory is incomplete at ${viewport.width}px`);
    for (let index = 0; index < 21; index += 1) {
      const image = images.nth(index);
      await image.scrollIntoViewIfNeeded();
      await image.evaluate((node) => node.decode?.().catch(() => undefined));
    }
    const geometry = await root.evaluate((element) => ({
      cards: element.querySelectorAll('[data-festival-card]').length,
      broken: [...element.querySelectorAll('[data-festival-card] img')]
        .filter((image) => !image.complete || image.naturalWidth <= 0).length,
      overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
    }));
    invariant(geometry.broken === 0, `festival calendar has ${geometry.broken} broken images at ${viewport.width}px`);
    invariant(geometry.overflow <= 1, `festival calendar overflows horizontally by ${geometry.overflow}px at ${viewport.width}px`);
    reports.push({ viewport, ...geometry });
  }
  await page.setViewportSize({ width:1536, height:864 });
  return reports;
}

async function runBrowserGate({ root, basePath, origin, browser, artifactDir = '' }) {
  const routes = eventRoutes(root, basePath);
  const candidates = staticSpecimenCandidates(root, basePath, routes);
  invariant(candidates.length > 0, 'generated release has no static multi-image recommendation journey');
  console.log(`[browser-release-gate] static candidates=${candidates.length}`);
  const specimen = await chooseSpecimen(browser, origin, candidates);
  console.log(`[browser-release-gate] specimen=${specimen.route} target=${specimen.targetPath}`);
  const context = await browser.newContext({ viewport: { width: 1536, height: 864 } });
  context.setDefaultTimeout(BROWSER_GATE_ACTION_TIMEOUT_MS);
  context.setDefaultNavigationTimeout(BROWSER_GATE_NAVIGATION_TIMEOUT_MS);
  // Production HTML intentionally points at the immutable CDN build prefix,
  // but the prefix is published only after this fail-closed gate succeeds.
  // Serve its generated Astro runtime from the checked tree itself; otherwise
  // every script is a pre-publication 404 and the gate tests static markup
  // rather than the real interactions it is supposed to certify.
  await context.route('https://static.kenigevents.ru/**', async (route) => {
    const localPath = localReleaseAssetPath(route.request().url());
    if (!localPath) {
      await route.continue();
      return;
    }
    const response = await fetch(`${origin}${localPath}`);
    invariant(response.ok, `generated local asset is missing: ${localPath}`);
    await route.fulfill({
      status: response.status,
      contentType: response.headers.get('content-type') || undefined,
      body: Buffer.from(await response.arrayBuffer()),
    });
  });
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
    const heroRoutes = [...new Set([...cropRoutes, ...staticHeroCropCandidates(root, basePath, routes)])];
    let related = [];
    let continuation = [];
    const heroGallery = [];
    for (const route of heroRoutes) {
      await page.goto(`${origin}${route}`, { waitUntil: 'domcontentloaded' });
      heroGallery.push({ route, ...await assertHeroGalleryCrop(page) });
      if (artifactDir) {
        const slug = route.split('/').filter(Boolean).at(-1)?.replace(/[^a-z0-9_-]+/giu, '-') || 'event';
        await page.locator('[data-desktop-family] [data-scroll-stage]').first().screenshot({
          path:join(artifactDir, `${slug}-hero-loaded.png`),
          animations:'disabled',
        });
      }
    }
    for (const route of cropRoutes) {
      await page.goto(`${origin}${route}`, { waitUntil: 'domcontentloaded' });
      await waitForContinuation(page);
      const routeRelated = await assertRecommendationGeometry(page, '[data-related-start] [data-event-card]', 3);
      const routeContinuation = await assertRecommendationGeometry(page, '[data-personal-feed-section][data-listing-context="event-detail"] [data-personal-feed-slot] > [data-event-card]', 1);
      invariant(routeContinuation.length <= 6, `Ещё события is unbounded on ${route}: ${routeContinuation.length}`);
      invariant([...routeRelated, ...routeContinuation].some((item) => item.mediaKind === 'document' && item.objectFit === 'cover' && item.actualCrop <= 0.205), `crop canary ${route} has no bounded-cover document card`);
      related.push(...routeRelated);
      continuation.push(...routeContinuation);
      if (artifactDir) {
        const slug = route.split('/').filter(Boolean).at(-1)?.replace(/[^a-z0-9_-]+/giu, '-') || 'event';
        const relatedSection = page.locator('[data-related-start]');
        await relatedSection.screenshot({
          path:join(artifactDir, `${slug}-related-loaded.png`),
          animations:'disabled',
        });
        await relatedSection.evaluate((element) => element.scrollIntoView({ block:'start', inline:'nearest' }));
        await page.screenshot({
          path:join(artifactDir, `${slug}-related-viewport.png`),
          animations:'disabled',
          fullPage:false,
        });
      }
    }
    checks.hero_gallery_crop = 'ok';
    console.log('[browser-release-gate] hero_gallery_crop=ok');
    checks.related_geometry_crop = 'ok';
    console.log('[browser-release-gate] related_geometry_crop=ok');
    checks.related_loaded_media = 'ok';
    console.log('[browser-release-gate] related_loaded_media=ok');
    invariant(await page.evaluate(() => typeof window.KenigEventsCreateEventCard === 'function'), 'canonical EventCard runtime renderer is missing');
    await assertRealCardEnter(page, origin, specimen.route, '[data-related-start]');
    await assertRealCardEnter(page, origin, specimen.route, '[data-personal-feed-section][data-listing-context="event-detail"] [data-personal-feed-slot]');
    checks.canonical_event_cards = 'ok';
    console.log('[browser-release-gate] canonical_event_cards=ok');
    invariant(dogRoute, 'generated release has no 6408 spatial keyboard specimen');
    const spatialKeyboard = await assertSpatialCardKeyboard(page, origin, dogRoute);
    checks.spatial_card_keyboard = 'ok';
    console.log('[browser-release-gate] spatial_card_keyboard=ok');
    const keyboardRoutes = [...new Set([
      routes.find((route) => /-6408\/$/u.test(route)),
      routes.find((route) => /-6593\/$/u.test(route)),
      specimen.route,
    ].filter(Boolean))];
    const keyboardReports = [];
    for (const route of keyboardRoutes) keyboardReports.push({ route, ...await assertColdAndPointerKeyboard(page, origin, route) });
    const singleRoute = singleImageSpecimen(root, basePath, routes);
    invariant(singleRoute, 'generated release has no single-image negative keyboard specimen');
    keyboardReports.push({ route:singleRoute, ...await assertColdAndPointerKeyboard(page, origin, singleRoute, { expectMultipleImages:false }) });
    checks.cold_and_pointer_keyboard = 'ok';
    console.log('[browser-release-gate] cold_and_pointer_keyboard=ok');
    await assertGalleryCrossDocument(page, origin, specimen.route);
    checks.gallery_cross_document = 'ok';
    console.log('[browser-release-gate] gallery_cross_document=ok');
    await assertFooterShortcuts(page, origin, specimen.route);
    checks.footer_shortcuts = 'ok';
    console.log('[browser-release-gate] footer_shortcuts=ok');
    const festivalCalendar = await assertFestivalCalendar(page, origin, basePath);
    checks.festival_calendar = 'ok';
    console.log('[browser-release-gate] festival_calendar=ok');
    return {
      ok: true, checks,
      specimen: { route: specimen.route, gallery_target: specimen.targetPath, crop_routes: cropRoutes, hero_routes:heroRoutes },
      hero_gallery:heroGallery,
      keyboard:keyboardReports,
      spatial_keyboard:spatialKeyboard,
      festival_calendar:festivalCalendar,
      media: [...related, ...continuation].map((item) => ({
        id:item.id,
        row:item.row,
        treatment:item.treatment,
        object_fit:item.objectFit,
        cover_crop:item.coverCrop,
        actual_crop:item.actualCrop,
        unused_frame_ratio:item.unusedFrameRatio,
        card_height:item.card.height,
        media_height:item.shell.height,
        image_loaded:item.imageLoaded,
        image_missing:item.imageMissing,
        fallback_visible:item.fallbackVisible,
      })),
      related_cards: related.length, continuation_cards: continuation.length,
    };
  } finally {
    await context.close();
  }
}

export async function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  const metadata = releaseRootMetadata(args.root || join(SITE_DIR, 'dist'), args.manifest || '');
  const browserName = args.browser || 'chromium';
  invariant(['chromium', 'firefox', 'webkit'].includes(browserName), `unsupported browser engine: ${browserName}`);
  const playwright = await import('playwright');
  const browserType = playwright[browserName];
  let server = null;
  let browser = null;
  try {
    browser = await browserType.launch({ headless: true });
    server = await startReleaseServer(metadata.root, metadata.basePath);
    const artifactDir = args.artifact_dir ? resolve(args.artifact_dir) : '';
    if (artifactDir) mkdirSync(artifactDir, { recursive:true });
    const report = await runBrowserGate({ root: metadata.root, basePath: metadata.basePath, origin: server.origin, browser, artifactDir });
    report.browser = browserName;
    if (metadata.manifestPath) recordBrowserVisualSuccess(metadata.manifestPath, report);
    if (args.report) writeFileSync(resolve(args.report), `${JSON.stringify(report, null, 2)}\n`);
    console.log(`Browser release gate passed: ${JSON.stringify(report)}`);
    return report;
  } finally {
    // Launch failures are release failures too.  Always tear down whichever
    // resource was acquired so a missing CI library exits immediately instead
    // of leaving the local fixture alive until the outer five-minute watchdog.
    if (browser) await browser.close().catch(() => {});
    if (server) await server.close().catch(() => {});
  }
}

if (process.argv[1] && resolve(process.argv[1]) === resolve(SCRIPT_PATH)) {
  main().catch((error) => {
    console.error(error?.stack || error);
    process.exitCode = 1;
  });
}
