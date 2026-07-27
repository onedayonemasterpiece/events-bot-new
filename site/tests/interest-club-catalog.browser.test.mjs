import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import path from 'node:path';
import test, { after, before } from 'node:test';
import { fileURLToPath } from 'node:url';

import { chromium } from 'playwright';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const previewBuildId = process.env.PREVIEW_BUILD_ID || '';
const distRoot = path.join(siteRoot, 'dist');
const builtRoot = previewBuildId ? path.join(distRoot, previewBuildId) : distRoot;
const routeBase = previewBuildId ? `/${previewBuildId}` : '';
const contentTypes = new Map([
  ['.css', 'text/css; charset=utf-8'],
  ['.html', 'text/html; charset=utf-8'],
  ['.js', 'text/javascript; charset=utf-8'],
  ['.json', 'application/json; charset=utf-8'],
  ['.svg', 'image/svg+xml'],
  ['.webp', 'image/webp'],
]);

let server;
let baseUrl;
let browser;

before(async () => {
  assert.ok((await stat(path.join(builtRoot, 'kluby-po-interesam/index.html'))).isFile(),
    'run a clubs-enabled build, setting PREVIEW_BUILD_ID here when testing build:preview');
  server = createServer(async (request, response) => {
    try {
      const pathname = decodeURIComponent(new URL(request.url, 'http://localhost').pathname);
      let target = path.join(distRoot, pathname);
      const targetStat = await stat(target).catch(() => null);
      if (targetStat?.isDirectory()) target = path.join(target, 'index.html');
      const body = await readFile(target);
      response.writeHead(200, { 'content-type': contentTypes.get(path.extname(target)) || 'application/octet-stream' });
      response.end(body);
    } catch {
      response.writeHead(404);
      response.end('Not found');
    }
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  baseUrl = `http://127.0.0.1:${server.address().port}`;
  browser = await chromium.launch({ headless: true });
});

after(async () => {
  await browser?.close();
  await new Promise((resolve) => server?.close(resolve));
});

test('club catalog keeps full desktop rows, scoped hotkeys, fallback and mobile overflow contract', async () => {
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  await page.goto(`${baseUrl}${routeBase}/kluby-po-interesam/`, { waitUntil: 'networkidle' });

  const cards = page.locator('[data-club-card]');
  const cardCount = await cards.count();
  assert.equal(cardCount, 3);
  for (const selector of ['#club-catalog-keyboard-instructions', '[data-club-keyboard-status]']) {
    assert.deepEqual(await page.locator(selector).evaluate((node) => {
      const style = getComputedStyle(node);
      return { width: style.width, height: style.height, overflow: style.overflow, clip: style.clip };
    }), { width: '1px', height: '1px', overflow: 'hidden', clip: 'rect(0px, 0px, 0px, 0px)' });
  }
  const geometry = await page.evaluate(() => {
    const list = document.querySelector('[data-club-list]');
    const cards = [...document.querySelectorAll('[data-club-card]')];
    const listRect = list.getBoundingClientRect();
    const rects = cards.map((card) => card.getBoundingClientRect());
    return {
      columns: getComputedStyle(list).gridTemplateColumns.split(' ').length,
      sameRow: Math.abs(rects[0].top - rects[1].top) <= 1,
      equalWidth: Math.abs(rects[0].width - rects[1].width) <= 1,
      fillsRow: Math.abs(rects.at(-1).right - listRect.right) <= 1,
      overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
    };
  });
  assert.deepEqual(geometry, {
    columns: Math.min(cardCount, 3),
    sameRow: true,
    equalWidth: true,
    fillsRow: true,
    overflow: 0,
  });
  assert.equal(await page.locator('[data-club-mobile-shelf]').isHidden(), true);

  const desktopMeetingBadge = page.locator('[data-club-future-badge="desktop"]:visible');
  assert.equal(await desktopMeetingBadge.count(), 1);
  const desktopBadgeLayout = await desktopMeetingBadge.evaluate((badge) => {
    const card = badge.closest('[data-club-card]');
    const media = card.querySelector('.club-card__media');
    const badgeRect = badge.getBoundingClientRect();
    const cardRect = card.getBoundingClientRect();
    const mediaRect = media.getBoundingClientRect();
    const style = getComputedStyle(badge);
    const glow = getComputedStyle(badge, '::after');
    return {
      topInset: badgeRect.top - mediaRect.top,
      rightInset: mediaRect.right - badgeRect.right,
      insideMedia:
        badgeRect.top >= mediaRect.top
        && badgeRect.right <= mediaRect.right
        && badgeRect.bottom <= mediaRect.bottom,
      insideCard:
        badgeRect.left >= cardRect.left
        && badgeRect.right <= cardRect.right,
      position: style.position,
      whiteSpace: style.whiteSpace,
      lowerGlow: style.boxShadow.includes('rgba(255, 194, 96')
        && style.boxShadow.includes('rgba(244, 145, 37')
        && glow.backgroundImage.includes('radial-gradient')
        && glow.mixBlendMode === 'screen'
        && Number.parseFloat(glow.top) >= badgeRect.height - 5,
    };
  });
  assert.equal(desktopBadgeLayout.position, 'absolute');
  assert.equal(desktopBadgeLayout.whiteSpace, 'nowrap');
  assert.equal(desktopBadgeLayout.insideMedia, true);
  assert.equal(desktopBadgeLayout.insideCard, true);
  assert.equal(desktopBadgeLayout.lowerGlow, true);
  assert.ok(desktopBadgeLayout.topInset >= 12 && desktopBadgeLayout.topInset <= 24);
  assert.ok(desktopBadgeLayout.rightInset >= 12 && desktopBadgeLayout.rightInset <= 24);
  assert.equal(await page.locator('[data-club-future-badge="mobile"]:visible').count(), 0);

  const firstHint = cards.first().locator('.club-card__keyboard-hint');
  assert.equal(await firstHint.evaluate((node) => getComputedStyle(node).visibility), 'hidden');
  await cards.first().focus();
  assert.equal(await firstHint.evaluate((node) => getComputedStyle(node).visibility), 'visible');
  assert.equal(await cards.nth(1).locator('.club-card__keyboard-hint').evaluate((node) => getComputedStyle(node).visibility), 'hidden');
  await page.keyboard.press('ArrowRight');
  assert.equal(await page.evaluate(() => document.activeElement?.getAttribute('data-club-name')), 'Клуб исследователей нейронок');
  await page.keyboard.press('ArrowLeft');
  assert.equal(await page.evaluate(() => document.activeElement?.getAttribute('data-club-name')), 'Game Vibes');

  // Add a second visual row without reinitializing the controller. Dynamic
  // measurement must send Down to the nearest column, not DOM adjacency.
  await page.evaluate(() => {
    const list = document.querySelector('[data-club-list]');
    const cards = [...list.querySelectorAll('[data-club-card]')];
    cards.forEach((card, index) => {
      card.dataset.testVisualIndex = String(index);
      const clone = card.cloneNode(true);
      clone.dataset.testVisualIndex = String(index + cards.length);
      list.append(clone);
    });
  });
  await page.locator('[data-test-visual-index="0"]').focus();
  await page.keyboard.press('ArrowDown');
  assert.equal(await page.evaluate(() => document.activeElement?.getAttribute('data-test-visual-index')), String(cardCount));

  await page.evaluate(() => {
    window.__clubActivation = [];
    document.querySelectorAll('[data-club-primary-action]').forEach((link) => {
      link.addEventListener('click', (event) => {
        event.preventDefault();
        event.stopImmediatePropagation();
        window.__clubActivation.push(link.getAttribute('href'));
      }, { capture: true });
    });
  });
  const secondPrimaryHref = await cards.nth(1).locator('[data-club-primary-action]').getAttribute('href');
  await page.locator('[data-test-visual-index="1"]').focus();
  await page.keyboard.press('Enter');
  assert.deepEqual(await page.evaluate(() => window.__clubActivation), [secondPrimaryHref]);

  const photoCard = page.locator('[data-test-visual-index="0"]');
  const photo = photoCard.locator('[data-club-cover]');
  assert.ok(await photo.evaluate((image) => image.naturalWidth > 0));
  await photo.evaluate((image) => {
    image.removeAttribute('srcset');
    image.src = '/missing-club-cover.webp';
  });
  await photoCard.waitFor({ state: 'visible' });
  await page.waitForFunction(() => document.querySelector('[data-test-visual-index="0"]')?.getAttribute('data-cover-state') === 'fallback');
  assert.equal(await photo.isHidden(), true);

  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(`${baseUrl}${routeBase}/kluby-po-interesam/`, { waitUntil: 'networkidle' });
  const mobile = await page.evaluate(() => {
    const list = document.querySelector('[data-club-list]');
    const hint = document.querySelector('.club-card__keyboard-hint');
    const desktopBadge = document.querySelector('[data-club-future-badge="desktop"]');
    const mobileBadge = document.querySelector('[data-club-future-badge="mobile"]');
    const shelf = document.querySelector('[data-club-mobile-shelf]');
    return {
      columns: getComputedStyle(list).gridTemplateColumns.split(' ').length,
      overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
      hintVisibility: getComputedStyle(hint).visibility,
      desktopBadgeDisplay: getComputedStyle(desktopBadge).display,
      mobileBadgeDisplay: getComputedStyle(mobileBadge).display,
      mobileBadgeInHead: mobileBadge.parentElement.classList.contains('club-card__head'),
      mobileBadgeWithinViewport:
        mobileBadge.getBoundingClientRect().right <= document.documentElement.clientWidth + 1,
      cardsWithinViewport: [...document.querySelectorAll('[data-club-card]')]
        .every((card) => card.getBoundingClientRect().right <= document.documentElement.clientWidth + 1),
      shelfVisible: getComputedStyle(shelf).display === 'flex',
      shelfTitle: shelf.querySelector('strong')?.textContent,
    };
  });
  assert.deepEqual(mobile, {
    columns: 1,
    overflow: 0,
    hintVisibility: 'hidden',
    desktopBadgeDisplay: 'block',
    mobileBadgeDisplay: 'none',
    mobileBadgeInHead: true,
    mobileBadgeWithinViewport: true,
    cardsWithinViewport: true,
    shelfVisible: true,
    shelfTitle: 'Клубы по интересам',
  });
  await page.evaluate(() => scrollTo(0, 900));
  await page.waitForFunction(() => scrollY > 800);
  const stickyShelf = await page.locator('[data-club-mobile-shelf]').evaluate((shelf) => {
    const box = shelf.getBoundingClientRect();
    return {
      top:box.top,
      height:box.height,
      headerBottom:document.querySelector('.site-header').getBoundingClientRect().bottom,
      overflow:document.documentElement.scrollWidth - document.documentElement.clientWidth,
    };
  });
  assert.ok(Math.abs(stickyShelf.top - stickyShelf.headerBottom) <= 1);
  assert.ok(stickyShelf.height >= 44);
  assert.equal(stickyShelf.overflow, 0);
  await page.close();
});
