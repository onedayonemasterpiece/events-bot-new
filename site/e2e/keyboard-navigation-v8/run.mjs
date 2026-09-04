import { mkdir, readdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium, firefox, webkit } from 'playwright';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteDir = resolve(scriptDir, '../..');
const distDir = join(siteDir, 'dist');
const browserName = process.env.KEYBOARD_NAVIGATION_BROWSER || 'chromium';
const baseUrl = (process.env.KEYBOARD_NAVIGATION_BASE_URL || 'http://127.0.0.1:4321').replace(/\/+$/u, '');
const requestedBuildId = String(process.env.KEYBOARD_NAVIGATION_BUILD_ID || '').trim();
const outputDir = resolve(
  process.env.KEYBOARD_NAVIGATION_EVIDENCE_DIR
    || join(siteDir, '../artifacts/keyboard-navigation-v8', browserName),
);
const browserTypes = { chromium, firefox, webkit };
const browserType = browserTypes[browserName];
if (!browserType) throw new Error(`Unsupported browser: ${browserName}`);

await mkdir(outputDir, { recursive: true });
await mkdir(join(outputDir, 'screenshots'), { recursive: true });
await mkdir(join(outputDir, 'traces'), { recursive: true });

function boundedMessage(value) {
  return String(value || '').replace(/\s+/gu, ' ').slice(0, 480);
}

async function discoverBuildId() {
  if (requestedBuildId) return requestedBuildId;
  const entries = await readdir(distDir, { withFileTypes: true });
  const builds = entries.filter((entry) => entry.isDirectory() && entry.name.startsWith('preview-'));
  if (builds.length !== 1) {
    throw new Error(`Expected one preview build in ${distDir}, found ${builds.map((entry) => entry.name).join(', ')}`);
  }
  return builds[0].name;
}

function attr(tag, name) {
  const match = tag.match(new RegExp(`${name}=["']([^"']*)["']`, 'iu'));
  return match?.[1] || '';
}

async function collectFixtureCandidates(buildId) {
  const eventsRoot = join(distDir, buildId, 'sobytiya');
  const slugs = await readdir(eventsRoot, { withFileTypes: true });
  const candidates = [];
  for (const entry of slugs) {
    if (!entry.isDirectory()) continue;
    const file = join(eventsRoot, entry.name, 'index.html');
    let html = '';
    try { html = await readFile(file, 'utf8'); } catch { continue; }
    const family = html.match(/data-desktop-family=["'](editorial|split)["']/iu)?.[1];
    if (!family || !html.includes('data-desktop-clean-event')) continue;
    const heroTag = html.match(/<img\b[^>]*data-clean-hero-image[^>]*>/iu)?.[0] || '';
    const width = Number(attr(heroTag, 'width') || 0);
    const height = Number(attr(heroTag, 'height') || 0);
    const ratio = width > 0 && height > 0 ? width / height : 0;
    const reason = html.match(/data-presentation-reason=["']([^"']+)["']/iu)?.[1] || '';
    candidates.push({
      family,
      slug: entry.name,
      route: `/${buildId}/sobytiya/${entry.name}/`,
      file: relative(siteDir, file),
      width,
      height,
      ratio,
      reason,
    });
  }
  return candidates;
}

function chooseFixtures(candidates) {
  const editorial = candidates
    .filter((candidate) => candidate.family === 'editorial')
    .sort((left, right) => right.ratio - left.ratio || right.width * right.height - left.width * left.height)[0];
  const splitCandidates = candidates.filter((candidate) => candidate.family === 'split');
  const split = [
    ...splitCandidates.filter((candidate) => candidate.ratio > 0 && candidate.ratio < 1.15),
    ...splitCandidates,
  ][0];
  if (!editorial || !split) {
    throw new Error(`Missing required family: editorial=${Boolean(editorial)} split=${Boolean(split)}`);
  }
  return [editorial, split];
}

async function focusBody(page) {
  await page.evaluate(() => {
    const active = document.activeElement;
    if (active instanceof HTMLElement) active.blur();
    document.body.setAttribute('tabindex', '-1');
    document.body.focus({ preventScroll: true });
    document.body.removeAttribute('tabindex');
  });
}

async function snapshot(page) {
  return page.evaluate(() => {
    const active = document.activeElement;
    const activeElement = active instanceof Element ? active : null;
    const card = activeElement?.closest?.('[data-event-card]');
    const surface = activeElement?.closest?.('[data-keyboard-event-surface]');
    const gallery = activeElement?.closest?.('[data-hero-gallery],[data-efficient-viewer]');
    const reading = activeElement?.closest?.('.desktop-clean-description,.desktop-clean-practical,[data-desktop-transport]');
    const owner = card ? 'card'
      : surface ? 'event'
        : gallery ? 'gallery'
          : reading ? 'reading'
            : activeElement === document.body || activeElement === document.documentElement ? 'body'
              : activeElement?.tagName?.toLowerCase() || 'none';
    const rect = activeElement instanceof HTMLElement ? activeElement.getBoundingClientRect() : null;
    return {
      path: location.pathname,
      hash: location.hash,
      owner,
      activeTag: activeElement?.tagName || null,
      activeId: activeElement?.id || null,
      cardId: card?.getAttribute('data-event-id') || null,
      surface: Boolean(surface),
      gallery: Boolean(gallery),
      scrollY: window.scrollY,
      viewportHeight: innerHeight,
      visible: Boolean(rect && rect.width > 0 && rect.height > 0 && rect.bottom > 0 && rect.top < innerHeight),
      rect: rect ? { top:rect.top, left:rect.left, right:rect.right, bottom:rect.bottom, width:rect.width, height:rect.height } : null,
    };
  });
}

function rowsFromCards(cards, rowTolerance = 16) {
  const sorted = [...cards].sort((left, right) => left.top - right.top || left.left - right.left);
  const rows = [];
  for (const card of sorted) {
    const row = rows.at(-1);
    if (!row || Math.abs(row.top - card.top) > rowTolerance) rows.push({ top:card.top, cards:[card] });
    else row.cards.push(card);
  }
  rows.forEach((row) => row.cards.sort((left, right) => left.left - right.left));
  return rows;
}

async function visualCards(page) {
  const measured = await page.locator('[data-related-start] [data-event-card]').evaluateAll((cards) => cards.map((card) => {
    const rect = card.getBoundingClientRect();
    return {
      id: card.getAttribute('data-event-id'),
      href: card.getAttribute('data-card-href'),
      top: rect.top,
      left: rect.left,
      width: rect.width,
      height: rect.height,
    };
  }));
  return rowsFromCards(measured).flatMap((row) => row.cards);
}

async function attachNavigationProbe(page, selector) {
  await page.locator(selector).evaluate((node) => {
    window.__knNavigationProbe = null;
    const link = node.querySelector('[data-card-title][href], [data-card-media-link][href]');
    if (!link) return;
    link.addEventListener('click', (event) => {
      window.__knNavigationProbe = { href: link.href };
      event.preventDefault();
      event.stopImmediatePropagation();
    }, { capture: true, once: true });
  });
}

function result(id, status, details = {}) {
  return { id, status, ...details };
}

async function probeBodyStart(page, id, selector, nearestOwners) {
  const target = page.locator(selector).first();
  if (!(await target.count())) return result(id, 'SKIPPED_NOT_APPLICABLE', { selector });
  await target.scrollIntoViewIfNeeded();
  await focusBody(page);
  const before = await snapshot(page);
  await page.keyboard.press('ArrowDown');
  await page.waitForTimeout(80);
  const after = await snapshot(page);
  const started = after.owner !== 'body' && after.visible;
  return result(id, started ? 'PASS' : 'FAIL', {
    contextRecovery: nearestOwners.includes(after.owner) ? 'PASS' : 'GAP_TARGET_NOT_IMPLEMENTED',
    before,
    after,
  });
}

async function runFixture(browser, fixture) {
  const fixtureDir = join(outputDir, fixture.family);
  await mkdir(fixtureDir, { recursive: true });
  const context = await browser.newContext({
    viewport: { width: 1536, height: 864 },
    locale: 'ru-RU',
    colorScheme: 'light',
  });
  await context.tracing.start({ screenshots: true, snapshots: true, sources: true });
  const page = await context.newPage();
  const consoleErrors = [];
  page.on('console', (message) => {
    if (message.type() === 'error') consoleErrors.push({ type:'console', text:boundedMessage(message.text()) });
  });
  page.on('pageerror', (error) => consoleErrors.push({ type:'pageerror', text:boundedMessage(error.message) }));
  await page.route('**/*.ics', (route) => route.fulfill({
    status: 200,
    contentType: 'text/calendar',
    body: 'BEGIN:VCALENDAR\r\nVERSION:2.0\r\nEND:VCALENDAR\r\n',
  }));
  await page.addInitScript(() => {
    window.__knClipboard = [];
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: {
        writeText: async (value) => { window.__knClipboard.push({ kind:'text', size:String(value).length }); },
        write: async (items) => { window.__knClipboard.push({ kind:'image', types:items.flatMap((item) => [...item.types]) }); },
      },
    });
    Object.defineProperty(navigator, 'share', {
      configurable: true,
      value: async () => { window.__knNativeShare = Number(window.__knNativeShare || 0) + 1; },
    });
  });

  const results = [];
  const url = `${baseUrl}${fixture.route}`;
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45_000 });
  await page.waitForSelector('[data-keyboard-event-navigation-mounted]', { timeout: 15_000 });
  await page.waitForSelector('[data-keyboard-event-surface]', { state: 'visible', timeout: 15_000 });
  await page.screenshot({ path:join(outputDir, 'screenshots', `${browserName}-${fixture.family}-initial.png`), fullPage:false });

  const initial = await snapshot(page);
  results.push(result('KN-001-router-and-no-autofocus', initial.owner !== 'event' ? 'PASS' : 'FAIL', { initial }));

  const family = await page.locator('[data-desktop-clean-event]').getAttribute('data-desktop-family');
  results.push(result('KN-001-family', family === fixture.family ? 'PASS' : 'FAIL', { expected:fixture.family, actual:family }));

  const surface = page.locator('[data-keyboard-event-surface]');
  const cards = page.locator('[data-related-start] [data-event-card]');
  const cardCount = await cards.count();
  results.push(result('KN-005-related-cards-present', cardCount > 1 ? 'PASS' : 'FAIL', { cardCount }));

  await page.evaluate(() => window.scrollTo({ top:0, behavior:'instant' }));
  await focusBody(page);
  const coldBefore = await snapshot(page);
  await page.keyboard.press('ArrowDown');
  await page.waitForTimeout(80);
  const coldAfter = await snapshot(page);
  results.push(result('KN-002-start-cold-body', coldAfter.owner !== 'body' && coldAfter.visible ? 'PASS' : 'FAIL', {
    contextRecovery:coldAfter.owner === 'event' ? 'PASS' : 'GAP_TARGET_NOT_IMPLEMENTED',
    before:coldBefore,
    after:coldAfter,
  }));

  results.push(await probeBodyStart(page, 'KN-002-start-description-middle', '.desktop-clean-description', ['reading']));
  results.push(await probeBodyStart(page, 'KN-002-start-practical', '.desktop-clean-practical', ['reading']));
  results.push(await probeBodyStart(page, 'KN-002-start-related', '[data-related-start]', ['card']));

  await page.evaluate(() => window.scrollTo({ top:0, behavior:'instant' }));
  await surface.focus();
  const surfaceShortcuts = await surface.getAttribute('aria-keyshortcuts');
  results.push(result('KN-003-command-grammar', /ArrowLeft/u.test(surfaceShortcuts || '') && /Enter/u.test(surfaceShortcuts || '') ? 'PASS' : 'FAIL', { surfaceShortcuts }));

  const readingBefore = await snapshot(page);
  await page.keyboard.press('ArrowDown');
  await page.waitForTimeout(80);
  const readingAfter = await snapshot(page);
  results.push(result('KN-004-event-to-reading', readingAfter.owner === 'reading' ? 'PASS' : 'GAP_TARGET_NOT_IMPLEMENTED', { before:readingBefore, after:readingAfter }));

  const visualBefore = await visualCards(page);
  const firstCardId = visualBefore[0]?.id || null;
  const firstCard = page.locator(`[data-related-start] [data-event-card][data-event-id="${firstCardId}"]`).first();
  if (await firstCard.count()) await firstCard.focus();
  const cardBefore = await snapshot(page);
  await page.keyboard.press('ArrowRight');
  await page.waitForTimeout(80);
  const cardAfter = await snapshot(page);
  const expectedNextId = visualBefore[1]?.id || null;
  results.push(result('KN-005-card-arrow-right', cardAfter.cardId === expectedNextId ? 'PASS' : 'FAIL', {
    expectedNextId,
    before:cardBefore,
    after:cardAfter,
  }));
  await page.screenshot({ path:join(outputDir, 'screenshots', `${browserName}-${fixture.family}-card-focus.png`), fullPage:false });

  const selectedCard = page.locator(`[data-related-start] [data-event-card][data-event-id="${cardAfter.cardId}"]`).first();
  let selectedHref = '';
  if (await selectedCard.count()) {
    selectedHref = String(await selectedCard.getAttribute('data-card-href') || '');
    await attachNavigationProbe(page, `[data-related-start] [data-event-card][data-event-id="${cardAfter.cardId}"]`);
    await selectedCard.focus();
    await page.keyboard.press('Enter');
    await page.waitForTimeout(40);
    const navigationProbe = await page.evaluate(() => window.__knNavigationProbe);
    const expectedAbsolute = selectedHref ? new URL(selectedHref, url).href : '';
    results.push(result('KN-006-enter-selected-card', navigationProbe?.href === expectedAbsolute ? 'PASS' : 'FAIL', {
      expected:expectedAbsolute,
      actual:navigationProbe?.href || null,
    }));
  }

  await page.evaluate(() => window.scrollTo({ top:0, behavior:'instant' }));
  await surface.focus();
  await page.keyboard.press('ArrowUp');
  const gallery = page.locator('[data-hero-gallery]:not([hidden]).is-open, [data-efficient-viewer]:not([hidden])').first();
  let galleryOpened = false;
  try {
    await gallery.waitFor({ state:'visible', timeout:4_000 });
    galleryOpened = true;
  } catch {}
  if (galleryOpened) {
    const galleryState = await snapshot(page);
    await page.screenshot({ path:join(outputDir, 'screenshots', `${browserName}-${fixture.family}-gallery.png`), fullPage:false });
    await page.keyboard.press('Escape');
    await page.waitForTimeout(220);
    const restored = await snapshot(page);
    results.push(result('KN-008-gallery-focus-return', restored.owner === 'event' && galleryState.owner === 'gallery' ? 'PASS' : 'FAIL', { galleryState, restored }));
  } else {
    results.push(result('KN-008-gallery-focus-return', 'FAIL', { reason:'gallery or efficient viewer did not open' }));
  }

  await surface.focus();
  await page.keyboard.press('Shift+Slash');
  await page.waitForTimeout(80);
  const helpCount = await page.locator('[data-keyboard-help-dialog]:visible').count();
  results.push(result('KN-009-context-help', helpCount > 0 ? 'PASS' : 'GAP_TARGET_NOT_IMPLEMENTED', { helpCount }));
  if (helpCount) await page.keyboard.press('Escape');

  const artifactCount = await page.locator('[data-keyboard-artifact-bridge]').count();
  results.push(result('KN-012-keyboard-artifact', artifactCount > 0 ? 'PASS' : 'GAP_TARGET_NOT_IMPLEMENTED', { artifactCount }));

  if (selectedHref) {
    const sourcePath = new URL(url).pathname;
    const targetUrl = new URL(selectedHref, url).href;
    await selectedCard.focus();
    const destinationPromise = page.waitForURL((candidate) => candidate.pathname !== sourcePath, { timeout:15_000 });
    await page.keyboard.press('Enter');
    try {
      await destinationPromise;
      await page.waitForSelector('[data-keyboard-event-surface]', { state:'visible', timeout:15_000 });
      await focusBody(page);
      await page.keyboard.press('ArrowDown');
      await page.waitForTimeout(80);
      const destinationStart = await snapshot(page);
      results.push(result('KN-006-destination-start', destinationStart.owner !== 'body' && destinationStart.visible ? 'PASS' : 'FAIL', { targetUrl, destinationStart }));
      await page.goBack({ waitUntil:'domcontentloaded', timeout:15_000 });
      await page.waitForSelector('[data-keyboard-event-surface]', { state:'visible', timeout:15_000 });
      const historyBefore = await snapshot(page);
      await page.keyboard.press('ArrowRight');
      await page.waitForTimeout(80);
      const historyAfter = await snapshot(page);
      const restoredCard = historyBefore.owner === 'card' || historyAfter.owner === 'card';
      results.push(result('KN-007-history-back-owner', restoredCard ? 'PASS' : 'GAP_TARGET_NOT_IMPLEMENTED', { historyBefore, historyAfter }));
    } catch (error) {
      results.push(result('KN-007-history-back-owner', 'FAIL', { targetUrl, error:boundedMessage(error?.message) }));
      await page.goto(url, { waitUntil:'domcontentloaded', timeout:45_000 });
      await page.waitForSelector('[data-keyboard-event-surface]', { state:'visible', timeout:15_000 });
    }
  }

  await page.reload({ waitUntil:'domcontentloaded' });
  await page.waitForSelector('[data-keyboard-event-surface]', { state:'visible', timeout:15_000 });
  await focusBody(page);
  await page.keyboard.press('ArrowDown');
  await page.waitForTimeout(80);
  const reloadAfter = await snapshot(page);
  results.push(result('KN-002-start-after-reload', reloadAfter.owner !== 'body' && reloadAfter.visible ? 'PASS' : 'FAIL', { after:reloadAfter }));

  await page.evaluate(() => window.dispatchEvent(new Event('blur')));
  await focusBody(page);
  await page.keyboard.press('ArrowDown');
  await page.waitForTimeout(80);
  const blurAfter = await snapshot(page);
  results.push(result('KN-002-start-after-blur', blurAfter.owner !== 'body' && blurAfter.visible ? 'PASS' : 'FAIL', { after:blurAfter }));

  const blockingIds = new Set([
    'KN-001-router-and-no-autofocus',
    'KN-001-family',
    'KN-005-related-cards-present',
    'KN-005-card-arrow-right',
    'KN-006-enter-selected-card',
    'KN-006-destination-start',
    'KN-008-gallery-focus-return',
    'KN-002-start-cold-body',
    'KN-002-start-after-reload',
  ]);
  const blockingFailures = results.filter((entry) => entry.status === 'FAIL' && blockingIds.has(entry.id));
  await writeFile(join(fixtureDir, 'results.json'), JSON.stringify({ fixture, results, consoleErrors, blockingFailures }, null, 2));
  await context.tracing.stop({ path:join(outputDir, 'traces', `${browserName}-${fixture.family}.zip`) });
  await context.close();
  return { fixture, results, consoleErrors, blockingFailures };
}

const buildId = await discoverBuildId();
const candidates = await collectFixtureCandidates(buildId);
const fixtures = chooseFixtures(candidates);
await writeFile(join(outputDir, 'fixture-selection.json'), JSON.stringify({ buildId, browserName, fixtures, candidateCount:candidates.length }, null, 2));

const browser = await browserType.launch({ headless: true });
const fixtureReports = [];
try {
  for (const fixture of fixtures) fixtureReports.push(await runFixture(browser, fixture));
} finally {
  await browser.close();
}

const allResults = fixtureReports.flatMap((report) => report.results.map((entry) => ({ family:report.fixture.family, ...entry })));
const allConsoleErrors = fixtureReports.flatMap((report) => report.consoleErrors.map((entry) => ({ family:report.fixture.family, ...entry })));
const blockingFailures = fixtureReports.flatMap((report) => report.blockingFailures.map((entry) => ({ family:report.fixture.family, ...entry })));
const startResults = allResults.filter((entry) => entry.id.startsWith('KN-002-start-'));
const startPasses = startResults.filter((entry) => entry.status === 'PASS').length;
const contextResults = startResults.filter((entry) => entry.contextRecovery);
const contextPasses = contextResults.filter((entry) => entry.contextRecovery === 'PASS').length;
const summary = {
  schema_version:'keyboard-navigation-evidence-v1',
  buildId,
  browserName,
  generatedAt:new Date().toISOString(),
  fixtures,
  metric:{
    keyboard_start_reliability:startResults.length ? startPasses / startResults.length : 0,
    startPassed:startPasses,
    startTested:startResults.length,
    context_recovery_accuracy:contextResults.length ? contextPasses / contextResults.length : 0,
    contextPassed:contextPasses,
    contextTested:contextResults.length,
  },
  status:blockingFailures.length ? 'FAIL' : 'PASS_WITH_TARGET_GAPS',
  blockingFailures,
  results:allResults,
};
await writeFile(join(outputDir, 'keyboard-navigation-evidence.json'), JSON.stringify(summary, null, 2));
await writeFile(join(outputDir, 'console-errors.json'), JSON.stringify(allConsoleErrors, null, 2));
const markdown = [
  '# Keyboard navigation V8 evidence',
  '',
  `- Browser: \`${browserName}\``,
  `- Build: \`${buildId}\``,
  `- Status: **${summary.status}**`,
  `- Start reliability: **${startPasses}/${startResults.length} (${(summary.metric.keyboard_start_reliability * 100).toFixed(1)}%)**`,
  `- Context recovery accuracy: **${contextPasses}/${contextResults.length} (${(summary.metric.context_recovery_accuracy * 100).toFixed(1)}%)**`,
  `- Console/page errors: **${allConsoleErrors.length}**`,
  '',
  '## Fixtures',
  ...fixtures.map((fixture) => `- ${fixture.family}: \`${fixture.route}\` — ${fixture.width}×${fixture.height}, ratio ${fixture.ratio.toFixed(3)}, ${fixture.reason || 'no reason'}`),
  '',
  '## Results',
  ...allResults.map((entry) => `- ${entry.status === 'PASS' ? '✅' : entry.status === 'FAIL' ? '❌' : '◻️'} **${entry.family} / ${entry.id}** — ${entry.status}${entry.contextRecovery ? `; context=${entry.contextRecovery}` : ''}`),
  '',
  '## Blocking failures',
  ...(blockingFailures.length ? blockingFailures.map((entry) => `- ${entry.family} / ${entry.id}`) : ['- none']),
  '',
  'Target gaps remain advisory in K0. Current V7 regressions and inability to select both page families are blocking.',
].join('\n');
await writeFile(join(outputDir, 'keyboard-navigation-summary.md'), markdown);

if (blockingFailures.length) process.exitCode = 1;
