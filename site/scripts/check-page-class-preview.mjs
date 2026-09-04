import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import {
  normalizeStaticSiteFocusedRoutes,
  normalizeStaticSitePageClasses,
} from './page-class-build-filter.mjs';

const siteDir = resolve(new URL('..', import.meta.url).pathname);
const buildId = String(process.env.PREVIEW_BUILD_ID || '').trim();
if (!/^preview-[A-Za-z0-9][A-Za-z0-9._-]*$/u.test(buildId)) {
  throw new Error('check:preview-slice requires the exact PREVIEW_BUILD_ID');
}
const pageClasses = normalizeStaticSitePageClasses(process.env.STATIC_SITE_PAGE_CLASSES || '');
const focusedRoutes = normalizeStaticSiteFocusedRoutes(process.env.STATIC_SITE_FOCUSED_ROUTES || '');
if (pageClasses[0] === 'all') throw new Error('check:preview-slice requires at least one named page class');
const root = join(siteDir, 'dist', buildId);
const required = [
  '__preview/index.html',
  'robots.txt',
  'preview-build.json',
];
const representatives = {
  event: 'sobytiya',
  date: 'segodnya/index.html',
  weekend: 'vyhodnye/index.html',
  collection: 'vystavki/index.html',
  personal: 'poisk/index.html',
  focus: 'fokus-gruppa/index.html',
  partner: 'partnerstvo/index.html',
  lab: 'lab/design-system/index.html',
};
for (const item of required) {
  if (!existsSync(join(root, item))) throw new Error(`page-class preview missing ${item}`);
}
if (focusedRoutes.length) {
  const routeTarget = (route) => {
    const local = route.replace(/^\/+|\/+$/gu, '');
    if (!local) return 'index.html';
    return /\.[a-z0-9]+$/iu.test(local) ? local : `${local}/index.html`;
  };
  for (const route of focusedRoutes) {
    const relative = routeTarget(route);
    if (!existsSync(join(root, relative))) {
      throw new Error(`focused page-class preview missing ${route}: ${relative}`);
    }
  }
} else {
  for (const pageClass of pageClasses) {
    const relative = representatives[pageClass];
    const target = join(root, relative);
    if (!existsSync(target)) throw new Error(`page-class preview missing representative for ${pageClass}: ${relative}`);
    if (pageClass === 'event') {
      const eventPages = readdirSync(target).filter((name) => (
        statSync(join(target, name)).isDirectory() && existsSync(join(target, name, 'index.html'))
      ));
      if (!eventPages.length) throw new Error('event page-class preview contains no event page');
    }
  }
}
const manifest = JSON.parse(readFileSync(join(root, 'preview-build.json'), 'utf8'));
if (manifest.buildId !== buildId || JSON.stringify(manifest.pageClasses) !== JSON.stringify(pageClasses)) {
  throw new Error('page-class preview manifest identity mismatch');
}
const index = readFileSync(join(root, '__preview/index.html'), 'utf8');
if (!/name=["']robots["'][^>]+noindex/iu.test(index)) {
  throw new Error('page-class preview shell must remain noindex');
}
console.log(`Page-class preview check passed: ${buildId}; classes=${pageClasses.join(',')}`);
