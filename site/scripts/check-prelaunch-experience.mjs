import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { chromium } from 'playwright';

function option(name, fallback = '') {
  const index = process.argv.indexOf(name);
  return index >= 0 ? String(process.argv[index + 1] || '') : fallback;
}

function check(condition, message, failures) {
  if (!condition) failures.push(message);
}

const url = option('--url');
const artifactDir = resolve(option('--artifact-dir', 'artifacts/prelaunch-browser'));
if (!url || !/^https?:\/\//u.test(url)) {
  throw new Error('Usage: check-prelaunch-experience.mjs --url <http(s) URL> [--artifact-dir <path>]');
}
mkdirSync(artifactDir, { recursive: true });

const executablePath = String(process.env.PRELAUNCH_CHROMIUM_EXECUTABLE_PATH || '').trim() || undefined;
const browser = await chromium.launch({ headless: true, executablePath });
const failures = [];

try {
  const context = await browser.newContext({
    viewport: { width: 390, height: 844 },
    reducedMotion: 'reduce',
    colorScheme: 'dark',
  });
  const page = await context.newPage();
  const response = await page.goto(url, { waitUntil: 'networkidle' });
  check(response?.ok(), `idle: HTTP ${response?.status() || 'unknown'}`, failures);
  await page.locator('[data-prelaunch-page]').waitFor({ state: 'visible' });
  await page.locator('.prelaunch-form__promise').waitFor({ state: 'visible' });

  const idle = await page.evaluate(() => {
    const root = document.querySelector('[data-prelaunch-page]');
    const mosaic = document.querySelector('[data-prelaunch-mosaic]');
    const tile = document.querySelector('[data-prelaunch-tile]');
    const surface = tile ? getComputedStyle(tile, '::before') : null;
    const rootBefore = root ? getComputedStyle(root, '::before') : null;
    const rootAfter = root ? getComputedStyle(root, '::after') : null;
    const button = document.querySelector('[data-prelaunch-submit]');
    const form = document.querySelector('[data-prelaunch-form]');
    const promise = document.querySelector('.prelaunch-form__promise');
    const complete = document.querySelector('.prelaunch-form__complete');
    const tiles = [...document.querySelectorAll('[data-prelaunch-tile]')];
    const scrolling = document.scrollingElement || document.documentElement;
    const gridColumns = String(mosaic ? getComputedStyle(mosaic).gridTemplateColumns : '')
      .split(/\s+/u)
      .filter(Boolean).length;
    return {
      experienceReady: root?.getAttribute('data-experience-ready'),
      formState: form?.getAttribute('data-experience-state'),
      promise: promise?.textContent || '',
      completeHidden: complete?.hasAttribute('hidden'),
      tileCount: tiles.length,
      gridColumns,
      windowCount: tiles.filter((node) => node.getAttribute('data-window') === 'true').length,
      accentCount: tiles.filter((node) => node.getAttribute('data-accent') === 'true').length,
      edgeBands: [...new Set(tiles.map((node) => node.getAttribute('data-edge')))].filter(Boolean),
      tileOverflow: tile ? getComputedStyle(tile).overflow : '',
      tileRadius: tile ? getComputedStyle(tile).borderRadius : '',
      surfaceRadius: surface?.borderRadius || '',
      surfaceShadow: surface?.boxShadow || '',
      surfaceBackground: surface?.backgroundImage || '',
      sharedLight: rootBefore?.backgroundImage || '',
      dust: rootAfter?.backgroundImage || '',
      buttonBackground: button ? getComputedStyle(button).backgroundImage : '',
      verticalOverflow: scrolling.scrollHeight - window.innerHeight,
      horizontalOverflow: scrolling.scrollWidth - window.innerWidth,
    };
  });

  check(idle.experienceReady === 'true', 'idle: enhancement module did not initialize', failures);
  check(idle.formState === 'idle', `idle: unexpected form state ${idle.formState}`, failures);
  check(idle.promise.includes('приятный сюрприз'), 'idle: surprise promise is missing', failures);
  check(idle.completeHidden === true, 'idle: completion panel must be hidden', failures);
  check(idle.tileCount === 72, `idle: expected 72 panes, got ${idle.tileCount}`, failures);
  check(idle.gridColumns === 6, `idle: mobile grid must use six smaller columns, got ${idle.gridColumns}`, failures);
  check(idle.windowCount === 8, `idle: expected eight coherent reveal windows, got ${idle.windowCount}`, failures);
  check(idle.accentCount === 3, `idle: expected three edge accents, got ${idle.accentCount}`, failures);
  check(
    ['ambient', 'soft', 'warm', 'hot'].every((edge) => idle.edgeBands.includes(edge)),
    `idle: incomplete edge bands ${idle.edgeBands.join(',')}`,
    failures,
  );
  check(idle.tileOverflow === 'hidden', `idle: corner mask container overflow=${idle.tileOverflow}`, failures);
  check(idle.tileRadius === '0px', `idle: square mask container radius=${idle.tileRadius}`, failures);
  check(idle.surfaceRadius !== '0px', 'idle: rounded glass surface lost its radius', failures);
  check(idle.surfaceShadow.includes('rgb(7, 9, 13)'), 'idle: opaque corner spread mask is missing', failures);
  check(!idle.surfaceBackground.includes('radial-gradient'), 'idle: pane paints a local radial spotlight', failures);
  check(idle.sharedLight.includes('radial-gradient'), 'idle: shared upper-right emitter is missing', failures);
  check((idle.dust.match(/radial-gradient/gu) || []).length >= 12, 'idle: golden powder layer is too sparse', failures);
  check(idle.buttonBackground.includes('radial-gradient') && idle.buttonBackground.includes('linear-gradient'), 'idle: premium CTA gradient is missing', failures);
  check(idle.verticalOverflow <= 1, `idle: vertical overflow ${idle.verticalOverflow}px`, failures);
  check(idle.horizontalOverflow <= 1, `idle: horizontal overflow ${idle.horizontalOverflow}px`, failures);

  await page.screenshot({
    path: resolve(artifactDir, 'prelaunch-experience-idle-390x844.png'),
    fullPage: true,
    animations: 'disabled',
  });

  await page.evaluate(() => {
    window.localStorage.setItem('ke_prelaunch_notification_v1', 'registered');
  });
  await page.reload({ waitUntil: 'networkidle' });
  await page.locator('.prelaunch-form__complete').waitFor({ state: 'visible' });

  const registered = await page.evaluate(() => {
    const form = document.querySelector('[data-prelaunch-form]');
    const row = document.querySelector('.prelaunch-form__row');
    const consent = document.querySelector('.prelaunch-form__consent');
    const promise = document.querySelector('.prelaunch-form__promise');
    const complete = document.querySelector('.prelaunch-form__complete');
    const scrolling = document.scrollingElement || document.documentElement;
    return {
      state: form?.getAttribute('data-experience-state'),
      rowHidden: row?.hasAttribute('hidden'),
      consentHidden: consent?.hasAttribute('hidden'),
      promiseHidden: promise?.hasAttribute('hidden'),
      completeHidden: complete?.hasAttribute('hidden'),
      completeText: complete?.textContent || '',
      verticalOverflow: scrolling.scrollHeight - window.innerHeight,
      horizontalOverflow: scrolling.scrollWidth - window.innerWidth,
    };
  });

  check(registered.state === 'registered', `registered: state=${registered.state}`, failures);
  check(registered.rowHidden === true, 'registered: input row remains visible', failures);
  check(registered.consentHidden === true, 'registered: consent remains visible', failures);
  check(registered.promiseHidden === true, 'registered: idle promise remains visible', failures);
  check(registered.completeHidden === false, 'registered: completion panel is hidden', failures);
  check(registered.completeText.includes('Вы уже записаны'), 'registered: confirmation headline is missing', failures);
  check(registered.completeText.includes('приятный сюрприз'), 'registered: surprise promise is missing', failures);
  check(registered.verticalOverflow <= 1, `registered: vertical overflow ${registered.verticalOverflow}px`, failures);
  check(registered.horizontalOverflow <= 1, `registered: horizontal overflow ${registered.horizontalOverflow}px`, failures);

  await page.screenshot({
    path: resolve(artifactDir, 'prelaunch-experience-registered-390x844.png'),
    fullPage: true,
    animations: 'disabled',
  });

  writeFileSync(
    resolve(artifactDir, 'prelaunch-experience-summary.json'),
    `${JSON.stringify({ ok: failures.length === 0, idle, registered, failures }, null, 2)}\n`,
  );
  await context.close();
} finally {
  await browser.close();
}

if (failures.length > 0) {
  throw new Error(`Prelaunch experience gate failed after preserving evidence:\n- ${failures.join('\n- ')}`);
}
console.log(JSON.stringify({ ok: true, artifactDir }, null, 2));
