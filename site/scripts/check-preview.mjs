import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import eventsData from '../src/data/preview-events.json' with { type: 'json' };

const siteDir = resolve(new URL('..', import.meta.url).pathname);
const distDir = join(siteDir, 'dist');
const buildId = process.env.PREVIEW_BUILD_ID || readdirSync(distDir).find((name) => name.startsWith('preview-'));
if (!buildId) throw new Error('No preview-* folder found in dist');
const root = join(distDir, buildId);
const required = [
  '__preview/index.html',
  'sitemap.xml',
  'robots.txt',
  'preview-build.json',
  `sobytiya/${eventsData.events[0].slug}/index.html`,
  `sobytiya/${eventsData.events[0].slug}/event.ics`,
];
for (const rel of required) {
  const path = join(root, rel);
  if (!existsSync(path) || !statSync(path).isFile()) throw new Error(`Missing required file: ${rel}`);
}
const control = eventsData.events.find((event) => event.id === 5878);
if (!control) throw new Error('Missing control event 5878');
if (control.slug !== 'pesni-sssr-svetlogorsk-5878') throw new Error(`Unexpected control slug: ${control.slug}`);
const controlHtml = readFileSync(join(root, `sobytiya/${control.slug}/index.html`), 'utf8');
if (!controlHtml.includes('noindex,nofollow,noarchive')) throw new Error('Missing preview robots meta');
if (controlHtml.includes('https://kenigevents.ru/sobytiya/pesni-sssr-svetlogorsk-5878/')) throw new Error('Production canonical leaked into preview page');
if (!controlHtml.includes(`https://kenigevents.ru/${buildId}/sobytiya/pesni-sssr-svetlogorsk-5878/`)) throw new Error('Preview canonical missing for control page');
if (/\bnull\b/.test(controlHtml)) throw new Error('Rendered HTML contains literal null');
const ics = readFileSync(join(root, `sobytiya/${control.slug}/event.ics`), 'utf8');
for (const needle of ['BEGIN:VCALENDAR', 'VERSION:2.0', 'BEGIN:VEVENT', 'DTSTART:20260711T193000Z', 'SUMMARY:Песни СССР', 'END:VCALENDAR']) {
  if (!ics.includes(needle)) throw new Error(`Control ICS missing ${needle}`);
}
if (/^DTEND:/m.test(ics)) throw new Error('Control ICS must not include DTEND without reliable duration');
const robots = readFileSync(join(root, 'robots.txt'), 'utf8').trim();
if (robots !== 'User-agent: *\nDisallow: /') throw new Error(`Unexpected robots.txt: ${robots}`);
const sitemap = readFileSync(join(root, 'sitemap.xml'), 'utf8');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/__preview/`)) throw new Error('Sitemap misses preview index URL');
if (!sitemap.includes(`https://kenigevents.ru/${buildId}/sobytiya/${control.slug}/`)) throw new Error('Sitemap misses control event URL');
console.log(`Preview checks passed for ${buildId}`);
