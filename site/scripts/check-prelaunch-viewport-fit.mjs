import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { chromium } from 'playwright';

function option(name, fallback = '') {
  const index = process.argv.indexOf(name);
  return index >= 0 ? String(process.argv[index + 1] || '') : fallback;
}

const url = option('--url');
const artifactDir = resolve(option('--artifact-dir', 'artifacts/prelaunch-browser'));
if (!url || !/^https?:\/\//u.test(url)) {
  throw new Error('Usage: check-prelaunch-viewport-fit.mjs --url <http(s) URL> [--artifact-dir <path>]');
}
mkdirSync(artifactDir, { recursive: true });

const viewports = [
  { name: 'fit-desktop-medium', width: 1536, height: 864 },
  { name: 'fit-desktop-small', width: 1024, height: 600 },
  { name: 'fit-tablet-portrait', width: 768, height: 1024 },
  { name: 'fit-mobile-medium', width: 360, height: 640 },
  { name: 'fit-mobile-landscape', width: 844, height: 390 },
  { name: 'fit-mobile-landscape-compact', width: 667, height: 375 },
];

const executablePath = String(process.env.PRELAUNCH_CHROMIUM_EXECUTABLE_PATH || '').trim() || undefined;
const browser = await chromium.launch({ headless: true, executablePath });
const failures = [];
const evidence = [];

function check(condition, message, localFailures) {
  if (!condition) localFailures.push(message);
}

function boxesOverlap(left, right, gap = 0) {
  if (!left || !right) return true;
  return !(
    left.right + gap <= right.left
    || right.right + gap <= left.left
    || left.bottom + gap <= right.top
    || right.bottom + gap <= left.top
  );
}

try {
  for (const viewport of viewports) {
    const localFailures = [];
    const context = await browser.newContext({
      viewport: { width: viewport.width, height: viewport.height },
      reducedMotion: 'reduce',
      colorScheme: 'dark',
    });
    try {
      const page = await context.newPage();
      const response = await page.goto(url, { waitUntil: 'networkidle' });
      check(response?.ok(), `${viewport.name}: HTTP ${response?.status() || 'unknown'}`, localFailures);
      await page.locator('[data-prelaunch-page]').waitFor({ state: 'visible' });
      await page.waitForTimeout(250);

      const scene = await page.evaluate(() => {
        const rect = (selector) => {
          const node = document.querySelector(selector);
          if (!(node instanceof Element)) return null;
          const value = node.getBoundingClientRect();
          return {
            top: value.top,
            right: value.right,
            bottom: value.bottom,
            left: value.left,
            width: value.width,
            height: value.height,
          };
        };
        const scrolling = document.scrollingElement || document.documentElement;
        const documentHeight = Math.max(
          scrolling.scrollHeight,
          document.documentElement.scrollHeight,
          document.body?.scrollHeight || 0,
        );
        const documentWidth = Math.max(
          scrolling.scrollWidth,
          document.documentElement.scrollWidth,
          document.body?.scrollWidth || 0,
        );
        const consent = document.querySelector('.prelaunch-form__consent');
        const rootStyle = getComputedStyle(document.querySelector('[data-prelaunch-page]'));
        return {
          viewport: { width: window.innerWidth, height: window.innerHeight },
          documentHeight,
          documentWidth,
          verticalOverflow: documentHeight - window.innerHeight,
          horizontalOverflow: documentWidth - window.innerWidth,
          scrollTop: scrolling.scrollTop,
          scrollLeft: scrolling.scrollLeft,
          rootPosition: rootStyle.position,
          rootOverflow: rootStyle.overflow,
          root: rect('[data-prelaunch-page]'),
          brand: rect('.prelaunch__brand'),
          heading: rect('#prelaunch-title'),
          description: rect('.prelaunch__copy p'),
          notify: rect('.prelaunch__notify'),
          consent: rect('.prelaunch-form__consent'),
          consentFontSize: consent ? Number.parseFloat(getComputedStyle(consent).fontSize) : 0,
        };
      });

      check(scene.verticalOverflow <= 1, `${viewport.name}: vertical overflow ${scene.verticalOverflow}px`, localFailures);
      check(scene.horizontalOverflow <= 1, `${viewport.name}: horizontal overflow ${scene.horizontalOverflow}px`, localFailures);
      check(scene.scrollTop === 0 && scene.scrollLeft === 0, `${viewport.name}: non-zero initial scroll`, localFailures);
      check(scene.rootPosition === 'fixed', `${viewport.name}: root is ${scene.rootPosition}, expected fixed`, localFailures);
      check(scene.rootOverflow === 'hidden', `${viewport.name}: root overflow is ${scene.rootOverflow}`, localFailures);
      check(scene.root && Math.abs(scene.root.height - viewport.height) <= 1, `${viewport.name}: root height ${scene.root?.height}`, localFailures);
      for (const [name, box] of Object.entries({
        brand: scene.brand,
        heading: scene.heading,
        description: scene.description,
        notify: scene.notify,
        consent: scene.consent,
      })) {
        check(box, `${viewport.name}: missing ${name}`, localFailures);
        if (!box) continue;
        check(box.top >= -1, `${viewport.name}: ${name} top ${box.top}`, localFailures);
        check(box.left >= -1, `${viewport.name}: ${name} left ${box.left}`, localFailures);
        check(box.right <= viewport.width + 1, `${viewport.name}: ${name} right ${box.right}`, localFailures);
        check(box.bottom <= viewport.height + 1, `${viewport.name}: ${name} bottom ${box.bottom}`, localFailures);
      }
      check(
        !boxesOverlap(scene.description, scene.notify, 6),
        `${viewport.name}: description/form rectangles overlap`,
        localFailures,
      );
      check(scene.consentFontSize >= 8.5, `${viewport.name}: consent font ${scene.consentFontSize}px is unreadable`, localFailures);

      const screenshotPath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}.png`);
      const domPath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}-dom.html`);
      const scenePath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}-scene.json`);
      await page.screenshot({ path: screenshotPath, fullPage: true, animations: 'disabled' });
      writeFileSync(domPath, await page.content());
      writeFileSync(scenePath, `${JSON.stringify({ ...scene, failures: localFailures }, null, 2)}\n`);
      evidence.push({ viewport, screenshotPath, domPath, scenePath, failures: localFailures });
    } catch (error) {
      localFailures.push(`${viewport.name}: capture exception: ${String(error?.stack || error)}`);
      writeFileSync(
        resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}-capture-error.txt`),
        `${localFailures.join('\n\n')}\n`,
      );
    } finally {
      failures.push(...localFailures);
      await context.close();
    }
  }
} finally {
  await browser.close();
}

const summary = { ok: failures.length === 0, url, viewports, evidence, failures };
writeFileSync(resolve(artifactDir, 'prelaunch-viewport-fit-summary.json'), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify(summary, null, 2));
if (failures.length > 0) {
  throw new Error(`Prelaunch viewport-fit gate failed after preserving evidence:\n- ${failures.join('\n- ')}`);
}
