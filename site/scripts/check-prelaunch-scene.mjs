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
  throw new Error('Usage: check-prelaunch-scene.mjs --url <http(s) URL> [--artifact-dir <path>]');
}
mkdirSync(artifactDir, { recursive: true });

const viewports = [
  { name: 'reference-square', width: 1200, height: 1200, source: 'mobile', naturalWidth: 853, naturalHeight: 1844 },
  { name: 'visual-wide', width: 1728, height: 900, source: 'desktop', naturalWidth: 1738, naturalHeight: 905 },
  { name: 'visual-desktop', width: 1440, height: 900, source: 'desktop', naturalWidth: 1738, naturalHeight: 905 },
  { name: 'mobile', width: 390, height: 844, source: 'mobile', naturalWidth: 853, naturalHeight: 1844 },
  { name: 'mobile-small', width: 320, height: 568, source: 'mobile', naturalWidth: 853, naturalHeight: 1844 },
  { name: 'mobile-landscape', width: 844, height: 390, source: 'mobile', naturalWidth: 853, naturalHeight: 1844 },
];

const executablePath = String(process.env.PRELAUNCH_CHROMIUM_EXECUTABLE_PATH || '').trim() || undefined;
const browser = await chromium.launch({ headless: true, executablePath });
const failures = [];
const evidence = [];

try {
  for (const viewport of viewports) {
    const localFailures = [];
    const context = await browser.newContext({
      viewport: { width: viewport.width, height: viewport.height },
      reducedMotion: 'reduce',
      colorScheme: 'dark',
      deviceScaleFactor: 1,
    });
    try {
      const page = await context.newPage();
      const response = await page.goto(url, { waitUntil: 'networkidle' });
      check(response?.ok(), `${viewport.name}: HTTP ${response?.status() || 'unknown'}`, localFailures);
      await page.locator('[data-prelaunch-page]').waitFor({ state: 'visible' });
      await page.waitForFunction(() => {
        const image = document.querySelector('[data-prelaunch-static-image]');
        return image instanceof HTMLImageElement && image.complete && image.naturalWidth > 0 && image.naturalHeight > 0;
      }, undefined, { timeout: 12_000 });
      await page.waitForTimeout(180);

      const scene = await page.evaluate(() => {
        const rect = (node) => {
          if (!(node instanceof Element)) return null;
          const value = node.getBoundingClientRect();
          return { top: value.top, right: value.right, bottom: value.bottom, left: value.left, width: value.width, height: value.height };
        };
        const root = document.querySelector('[data-prelaunch-page]');
        const picture = document.querySelector('[data-prelaunch-static-picture]');
        const image = document.querySelector('[data-prelaunch-static-image]');
        const imageStyle = image ? getComputedStyle(image) : null;
        const rootStyle = root ? getComputedStyle(root) : null;
        const scrolling = document.scrollingElement || document.documentElement;
        const viewportMeta = document.querySelector('meta[name="viewport"]')?.getAttribute('content') || '';
        const jsonLd = [...document.querySelectorAll('script[type="application/ld+json"]')]
          .map((node) => { try { return JSON.parse(node.textContent || '{}'); } catch { return null; } })
          .filter(Boolean);
        const jsonTypes = jsonLd.flatMap((entry) => {
          const graph = Array.isArray(entry?.['@graph']) ? entry['@graph'] : [entry];
          return graph.map((item) => item?.['@type']).filter(Boolean);
        });
        const allElements = [...document.querySelectorAll('*')];
        const backdropFiltered = allElements.filter((node) => {
          const style = getComputedStyle(node);
          const value = style.backdropFilter || style.webkitBackdropFilter || '';
          return value && value !== 'none';
        }).map((node) => node.className || node.tagName).slice(0, 20);
        return {
          viewport: { width: innerWidth, height: innerHeight },
          staticBackground: root?.getAttribute('data-static-background') || '',
          rootPosition: rootStyle?.position || '',
          rootOverflow: rootStyle?.overflow || '',
          picture: rect(picture),
          image: {
            rect: rect(image),
            currentSrc: image instanceof HTMLImageElement ? image.currentSrc : '',
            naturalWidth: image instanceof HTMLImageElement ? image.naturalWidth : 0,
            naturalHeight: image instanceof HTMLImageElement ? image.naturalHeight : 0,
            objectFit: imageStyle?.objectFit || '',
            objectPosition: imageStyle?.objectPosition || '',
            opacity: imageStyle?.opacity || '',
            filter: imageStyle?.filter || '',
            mixBlendMode: imageStyle?.mixBlendMode || '',
            animationName: imageStyle?.animationName || '',
          },
          reconstructionLayerCount: document.querySelectorAll('[data-prelaunch-static-composite], [data-prelaunch-static-artwork], .prelaunch__static-depth, .prelaunch__static-pane-field, .prelaunch__static-grid, .prelaunch__static-light, .prelaunch__static-dust').length,
          dynamicTileCount: document.querySelectorAll('[data-prelaunch-tile]').length,
          dynamicSeamCount: document.querySelectorAll('[data-prelaunch-seams]').length,
          backdropFiltered,
          verticalOverflow: Math.max(scrolling.scrollHeight, document.documentElement.scrollHeight, document.body?.scrollHeight || 0) - innerHeight,
          horizontalOverflow: Math.max(scrolling.scrollWidth, document.documentElement.scrollWidth, document.body?.scrollWidth || 0) - innerWidth,
          viewportMeta,
          title: document.title,
          description: document.querySelector('meta[name="description"]')?.getAttribute('content') || '',
          canonical: document.querySelector('link[rel="canonical"]')?.getAttribute('href') || '',
          jsonTypes,
          text: {
            heading: document.querySelector('#prelaunch-title')?.textContent?.trim() || '',
            promise: document.querySelector('.prelaunch-form__promise')?.textContent?.trim() || '',
            consent: document.querySelector('.prelaunch-form__consent')?.textContent?.trim() || '',
          },
        };
      });

      check(scene.staticBackground === 'approved-desktop-mobile-v2', `${viewport.name}: static background marker=${scene.staticBackground}`, localFailures);
      check(scene.rootPosition === 'fixed', `${viewport.name}: root position=${scene.rootPosition}`, localFailures);
      check(scene.rootOverflow === 'hidden', `${viewport.name}: root overflow=${scene.rootOverflow}`, localFailures);
      check(scene.picture && scene.picture.width >= viewport.width - 1 && scene.picture.height >= viewport.height - 1, `${viewport.name}: background picture does not cover viewport`, localFailures);
      check(scene.image.naturalWidth === viewport.naturalWidth && scene.image.naturalHeight === viewport.naturalHeight, `${viewport.name}: selected ${viewport.source} source is ${scene.image.naturalWidth}x${scene.image.naturalHeight}`, localFailures);
      check(scene.image.currentSrc.includes(`prelaunch-scene-${viewport.source}.webp`), `${viewport.name}: unexpected image source ${scene.image.currentSrc}`, localFailures);
      check(scene.image.objectFit === 'cover', `${viewport.name}: object-fit=${scene.image.objectFit}`, localFailures);
      check(scene.image.opacity === '1', `${viewport.name}: image opacity=${scene.image.opacity}`, localFailures);
      check(scene.image.filter === 'none', `${viewport.name}: image filter=${scene.image.filter}`, localFailures);
      check(scene.image.mixBlendMode === 'normal', `${viewport.name}: image blend=${scene.image.mixBlendMode}`, localFailures);
      check(scene.image.animationName === 'none', `${viewport.name}: image animation=${scene.image.animationName}`, localFailures);
      check(scene.reconstructionLayerCount === 0, `${viewport.name}: reconstruction layers=${scene.reconstructionLayerCount}`, localFailures);
      check(scene.dynamicTileCount === 0, `${viewport.name}: live tile count=${scene.dynamicTileCount}`, localFailures);
      check(scene.dynamicSeamCount === 0, `${viewport.name}: live seam count=${scene.dynamicSeamCount}`, localFailures);
      check(scene.backdropFiltered.length <= 3, `${viewport.name}: too many backdrop-filter surfaces ${scene.backdropFiltered.join(', ')}`, localFailures);
      check(scene.verticalOverflow <= 1, `${viewport.name}: vertical overflow=${scene.verticalOverflow}`, localFailures);
      check(scene.horizontalOverflow <= 1, `${viewport.name}: horizontal overflow=${scene.horizontalOverflow}`, localFailures);
      check(scene.viewportMeta.includes('maximum-scale=1'), `${viewport.name}: maximum-scale missing`, localFailures);
      check(scene.viewportMeta.includes('user-scalable=no'), `${viewport.name}: zoom lock missing`, localFailures);
      check(scene.viewportMeta.includes('viewport-fit=cover'), `${viewport.name}: viewport-fit missing`, localFailures);
      check(scene.title.includes('Полюбить Калининград') && scene.title.includes('1 сентября'), `${viewport.name}: title incomplete`, localFailures);
      check(scene.description.includes('Калининградской области'), `${viewport.name}: description incomplete`, localFailures);
      check(Boolean(scene.canonical), `${viewport.name}: canonical missing`, localFailures);
      for (const type of ['Organization', 'ImageObject', 'WebSite', 'WebPage', 'Service']) {
        check(scene.jsonTypes.includes(type), `${viewport.name}: JSON-LD ${type} missing`, localFailures);
      }
      check(scene.text.heading.includes('Запуск') && scene.text.heading.includes('1 сентября'), `${viewport.name}: heading missing`, localFailures);
      check(scene.text.promise.includes('приятный сюрприз'), `${viewport.name}: surprise copy missing`, localFailures);
      check(scene.text.consent.includes('Отписаться можно'), `${viewport.name}: unsubscribe copy missing`, localFailures);

      const screenshotPath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}.png`);
      const domPath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}-dom.html`);
      const scenePath = resolve(artifactDir, `prelaunch-${viewport.name}-${viewport.width}x${viewport.height}-scene.json`);
      await page.screenshot({ path: screenshotPath, fullPage: false, animations: 'disabled' });
      writeFileSync(domPath, await page.content());
      writeFileSync(scenePath, `${JSON.stringify({ viewport, scene, failures: localFailures }, null, 2)}\n`);
      evidence.push({ viewport, scene, failures: localFailures, screenshotPath, domPath, scenePath });
    } catch (error) {
      const message = `${viewport.name}: ${String(error?.stack || error)}`;
      localFailures.push(message);
      writeFileSync(resolve(artifactDir, `prelaunch-${viewport.name}-capture-error.txt`), `${message}\n`);
    } finally {
      failures.push(...localFailures);
      await context.close();
    }
  }
} finally {
  await browser.close();
}

const summary = {
  schema_version: 'prelaunch_approved_background_evidence_v2',
  ok: failures.length === 0,
  url,
  viewports,
  evidence,
  failures,
  manual_review_required: true,
  manual_review_axes: [
    'exact_approved_backgrounds',
    'composition_against_user_reference',
    'foreground_legibility',
    'desktop_mobile_equivalence',
    'consent_prominence',
    'no_scroll',
  ],
};
writeFileSync(resolve(artifactDir, 'prelaunch-scene-summary.json'), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify(summary, null, 2));
if (failures.length) throw new Error(failures.join('\n'));
