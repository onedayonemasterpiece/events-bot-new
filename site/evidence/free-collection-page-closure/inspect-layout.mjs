import { chromium } from 'playwright';
import { writeFile } from 'node:fs/promises';

const browser = await chromium.launch({
  headless: true,
  executablePath: '/home/dev/.cache/ms-playwright/chromium-1228/chrome-linux64/chrome',
  args: ['--no-sandbox'],
});
const report = {};

for (const [viewport, width, height] of [['desktop', 1280, 1200], ['mobile', 390, 844]]) {
  const page = await browser.newPage({ viewport: { width, height }, deviceScaleFactor: 1 });
  await page.goto('http://127.0.0.1:4173/podborki/besplatnye-sobytiya/', { waitUntil: 'networkidle' });
  report[viewport] = await page.evaluate(() => {
    const read = (element) => {
      if (!(element instanceof Element)) return null;
      const r = element.getBoundingClientRect();
      const s = getComputedStyle(element);
      return {
        tag: element.tagName.toLowerCase(),
        classes: element.className,
        text: element.textContent?.trim().replace(/\s+/g, ' ') || '',
        rect: { x: r.x, y: r.y, width: r.width, height: r.height },
        style: {
          display: s.display,
          position: s.position,
          gap: s.gap,
          padding: s.padding,
          margin: s.margin,
          border: s.border,
          borderRadius: s.borderRadius,
          background: s.background,
          boxShadow: s.boxShadow,
          color: s.color,
          fontFamily: s.fontFamily,
          fontSize: s.fontSize,
          fontWeight: s.fontWeight,
          lineHeight: s.lineHeight,
          letterSpacing: s.letterSpacing,
          opacity: s.opacity,
          visibility: s.visibility,
          overflow: s.overflow,
          objectFit: s.objectFit,
        },
      };
    };
    const one = (selector) => read(document.querySelector(selector));
    const all = (selector) => [...document.querySelectorAll(selector)].map(read);
    return {
      shell: one('.free-collection__shell'),
      breadcrumb: one('.breadcrumbs'),
      hero: one('[data-free-collection-hero]'),
      heroChildren: {
        eyebrow: one('.free-collection__hero .eyebrow'),
        title: one('.free-collection__hero h1'),
        lead: one('.free-collection__hero .lead'),
        criteria: one('.free-collection__hero .collection-criteria'),
        note: one('.free-collection__hero .muted-note'),
        medallion: one('.free-collection__medallion--hero'),
      },
      eventGroup: one('[data-free-collection-event-group="events"]'),
      eventHeading: one('[data-free-collection-event-group="events"] h2'),
      exhibitionGroup: one('[data-free-collection-event-group="exhibitions"]'),
      exhibitionEyebrow: one('[data-free-collection-event-group="exhibitions"] .eyebrow'),
      exhibitionHeading: one('[data-free-collection-event-group="exhibitions"] h2'),
      exhibitionNote: one('[data-free-collection-event-group="exhibitions"] .free-collection__group-note'),
      cards: [...document.querySelectorAll('[data-event-card]')].map(card => ({
        id: card.getAttribute('data-event-id'),
        card: read(card),
        mediaShell: read(card.querySelector('[data-card-media-shell]')),
        image: read(card.querySelector('[data-card-image]')),
        body: read(card.querySelector('.event-card__body')),
        title: read(card.querySelector('[data-card-title]')),
        meta: read(card.querySelector('.event-card__meta-row')),
        place: read(card.querySelector('[data-card-place]')),
        utility: read(card.querySelector('.event-card__utility-row')),
        feedback: read(card.querySelector('.event-card__feedback')),
        calendar: read(card.querySelector('[data-calendar-action]')),
      })),
      desktopNavItems: all('.desktop-nav a'),
      mobileNav: one('[data-mobile-bottom-nav]'),
      mobileNavItems: all('[data-mobile-bottom-nav] [data-mobile-nav-section]'),
      footerShare: one('[data-footer-share-strip]'),
      footer: one('footer'),
    };
  });
  await page.close();
}

await browser.close();
const target = new URL('./astro-layout-inspection.v1.json', import.meta.url);
await writeFile(target, JSON.stringify(report, null, 2) + '\n');
console.log(target.pathname);
