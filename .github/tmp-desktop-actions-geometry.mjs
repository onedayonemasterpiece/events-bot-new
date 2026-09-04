import fs from 'node:fs';
import { chromium } from 'playwright';

const base = (process.env.STATIC_SITE_REVIEW_BASE_URL || '').replace(/\/$/u, '');
if (!base) throw new Error('STATIC_SITE_REVIEW_BASE_URL is required');

const executablePath = [
  '/usr/bin/google-chrome',
  '/usr/bin/google-chrome-stable',
  '/usr/bin/chromium',
  '/usr/bin/chromium-browser',
].find((candidate) => fs.existsSync(candidate));
if (!executablePath) throw new Error('No system Chromium executable found');

const cases = [
  ['lab/event-desktop/examples/cta-phone-invariant', 'split', 'split-inline', 3],
  ['lab/event-desktop/examples/cta-registration-invariant', 'split', 'split-inline', 3],
  ['lab/event-desktop/examples/cta-free-calendar-invariant', 'split', 'split-inline', 2],
  ['lab/event-desktop/examples/footer-service-v1', 'editorial', 'editorial-side', 3],
];

const browser = await chromium.launch({ headless: true, executablePath, args: ['--no-sandbox'] });
const page = await browser.newPage({ viewport: { width: 1536, height: 864 } });

for (const [route, family, variant, expectedControlCount] of cases) {
  await page.goto(`${base}/${route}/`, { waitUntil: 'networkidle' });
  await page.waitForSelector('[data-desktop-action-panel]');
  await page.waitForTimeout(120);
  const result = await page.evaluate(({ family, variant, expectedControlCount }) => {
    const root = document.querySelector('[data-desktop-clean-event]');
    const panel = document.querySelector('[data-desktop-action-panel]');
    if (!(root instanceof HTMLElement) || !(panel instanceof HTMLElement)) {
      return { ok: false, reason: 'root or panel missing' };
    }
    const admission = panel.querySelector(':scope > p:first-child');
    const primary = panel.querySelector(':scope > .desktop-prototype__primary-action');
    const row = panel.querySelector(':scope > [data-desktop-action-row="calendar-share-like"]');
    const controls = row ? [...row.children].filter((item) => item instanceof HTMLElement) : [];
    if (!(admission instanceof HTMLElement) || !(primary instanceof HTMLElement) || !(row instanceof HTMLElement)) {
      return { ok: false, reason: 'action anatomy missing' };
    }

    const panelRect = panel.getBoundingClientRect();
    const admissionRect = admission.getBoundingClientRect();
    const primaryRect = primary.getBoundingClientRect();
    const rowRect = row.getBoundingClientRect();
    const controlRects = controls.map((item) => item.getBoundingClientRect());
    const contained = [admissionRect, primaryRect, rowRect, ...controlRects].every((rect) =>
      rect.left >= panelRect.left - 1 && rect.right <= panelRect.right + 1
      && rect.top >= panelRect.top - 1 && rect.bottom <= panelRect.bottom + 1);
    const targetFloor = [primaryRect, ...controlRects].every((rect) => rect.width >= 44 && rect.height >= 44);
    const icons = [...panel.querySelectorAll('.desktop-prototype__primary-action .icon, .desktop-prototype__icon-action .icon')]
      .map((icon) => icon.getBoundingClientRect());
    const centralIcons = icons.length > 0 && icons.every((rect) =>
      Math.abs(rect.width - 24) <= 0.5 && Math.abs(rect.height - 24) <= 0.5);
    const centers = [admissionRect, primaryRect, rowRect].map((rect) => (rect.top + rect.bottom) / 2);
    const splitGeometry = Math.max(...centers) - Math.min(...centers) <= 2
      && admissionRect.right <= primaryRect.left + 1 && primaryRect.right <= rowRect.left + 1;
    const editorialGeometry = admissionRect.bottom <= primaryRect.top + 1 && primaryRect.bottom <= rowRect.top + 1;

    return {
      ok: root.dataset.desktopFamily === family
        && panel.dataset.actionFamily === family
        && panel.dataset.actionVariant === variant
        && Boolean(panel.dataset.actionState)
        && panel.dataset.actionLayout === (family === 'split' ? 'inline' : 'stacked')
        && controls.length === expectedControlCount
        && contained
        && targetFloor
        && centralIcons
        && panel.scrollWidth <= panel.clientWidth + 1
        && row.scrollWidth <= row.clientWidth + 1
        && (family === 'split' ? splitGeometry : editorialGeometry),
      rootFamily: root.dataset.desktopFamily,
      panelFamily: panel.dataset.actionFamily,
      variant: panel.dataset.actionVariant,
      state: panel.dataset.actionState,
      layout: panel.dataset.actionLayout,
      density: panel.dataset.actionDensity,
      fit: panel.dataset.actionFit,
      controls: controlRects.map((rect) => ({ width: rect.width, height: rect.height })),
      icons: icons.map((rect) => ({ width: rect.width, height: rect.height })),
      contained,
      targetFloor,
      centralIcons,
      splitGeometry,
      editorialGeometry,
    };
  }, { family, variant, expectedControlCount });

  if (!result.ok) throw new Error(`${route}: ${JSON.stringify(result)}`);
  console.log(route, JSON.stringify(result));
}

await browser.close();
