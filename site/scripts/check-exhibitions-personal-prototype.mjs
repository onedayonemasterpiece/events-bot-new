import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';

const siteRoot = path.resolve(import.meta.dirname, '..');
const pageSource = fs.readFileSync(path.join(siteRoot, 'src/pages/lab/exhibitions-personal/index.astro'), 'utf8');
const rowSource = fs.readFileSync(path.join(siteRoot, 'src/components/ExhibitionPrototypeRow.astro'), 'utf8');
const layoutSource = fs.readFileSync(path.join(siteRoot, 'src/layouts/EventLayout.astro'), 'utf8');
const outputPath = path.join(siteRoot, 'dist/lab/exhibitions-personal/index.html');

if (!fs.existsSync(outputPath)) throw new Error(`Build output is missing: ${outputPath}`);
const html = fs.readFileSync(outputPath, 'utf8');
const eventIds = [...html.matchAll(/<article[^>]+data-exhibition-row[^>]+data-event-id="(\d+)"/gu)].map((match) => match[1]);
const uniqueIds = new Set(eventIds);

const checks = [
  ['12 curated exhibition rows', eventIds.length === 12],
  ['no duplicate event rows', uniqueIds.size === eventIds.length],
  ['3 new inbox rows', (html.match(/data-new="true"/gu) || []).length === 3],
  ['personal mode precedes all mode', html.indexOf('data-mode="personal"') < html.indexOf('data-mode="all"')],
  ['new inbox precedes priority and tail', html.indexOf('data-list-section="new"') < html.indexOf('data-list-section="priority"') && html.indexOf('data-list-section="priority"') < html.indexOf('data-list-section="tail"')],
  ['tail is progressively disclosed', /data-tail-toggle[^>]+aria-expanded="false"/u.test(html) && /data-list-section="tail" hidden/u.test(html)],
  ['mode switch is a complete radio group', /role="radiogroup"/u.test(html) && /role="radio"/u.test(html) && pageSource.includes("['ArrowLeft','ArrowRight','Home','End']")],
  ['keyboard safeguards editable controls', pageSource.includes("input,textarea,select,button,[contenteditable=\"true\"]")],
  ['like and reject expose pressed state', rowSource.includes('data-like aria-pressed="false"') && rowSource.includes('data-reject aria-pressed="false"')],
  ['rejection keeps an undo stub', rowSource.includes('data-hidden-stub') && rowSource.includes('data-undo')],
  ['gallery uses native modal dialog', /<dialog[^>]+data-gallery/u.test(html) && pageSource.includes('showModal()')],
  ['shared header is selected for exhibitions', pageSource.includes('headerCurrent="exhibitions"') && !pageSource.includes('<header class="ex-header"')],
  ['shared header owns the responsive badge', pageSource.includes("headerBadge={{ key: 'exhibitions'") && layoutSource.includes('data-header-badge={item.key}') && html.includes('data-header-badge="exhibitions"') && html.includes('3 новых')],
  ['responsive and reduced-motion contracts exist', pageSource.includes('@media (max-width:740px)') && pageSource.includes('@media (prefers-reduced-motion:reduce)')],
  ['mobile uses the shared immersive discovery drawer', layoutSource.includes('data-mobile-discovery-menu') && pageSource.includes('heroChrome="immersive"') && !pageSource.includes('>.site-header .site-nav { display:none; }')],
  ['shared mobile navigation exposes current section for badge extension', layoutSource.includes("aria-current={headerBadge && headerCurrent === 'exhibitions' ? 'page' : undefined}")],
  ['photo deck keeps real source geometry', rowSource.includes('width={asset.width}') && rowSource.includes('height={asset.height}') && rowSource.includes('normalizedRatio')],
  ['photo deck avoids poster and OCR cropping', pageSource.includes('.ex-deck__frame img') && pageSource.includes('object-fit:contain') && rowSource.includes('data-image-text-mode')],
  ['single-image deck does not reserve a missing stack', pageSource.includes('.ex-deck__images--1 { right:7px; }') && pageSource.includes('.ex-deck__images--1 { right:8px; }')],
  ['deck exposes distinct card and edge layers', rowSource.includes('ex-deck__frame') && rowSource.includes('ex-deck__edge-stack') && pageSource.includes('.ex-deck__edge-stack i:nth-child(4)')],
  ['row halo has hover and keyboard parity', rowSource.includes('ex-row__halo') && rowSource.includes('ex-row__edge-light') && pageSource.includes('.ex-row:focus-within .ex-row__halo')],
  ['metadata motion avoids grid-row reflow', pageSource.includes('.ex-row__expanded { display:grid; opacity:') && !pageSource.includes('transition:grid-template-rows')],
  ['cinematic motion and reduced-motion scrolling exist', pageSource.includes('cubic-bezier(.16,1,.3,1)') && pageSource.includes("behavior:reducedMotion.matches ? 'auto' : 'smooth'")],
  ['reduced motion keeps positioning transforms intact', !pageSource.includes('animation:none!important; transform:none!important') && pageSource.includes('.ex-action--like[aria-pressed="true"] svg')],
  ['roving tabindex follows arrow focus', pageSource.includes('links.forEach((link) => { link.tabIndex = -1; })') && pageSource.includes('next.tabIndex = 0')],
];

const failures = checks.filter(([, passed]) => !passed);
for (const [label, passed] of checks) console.log(`${passed ? 'PASS' : 'FAIL'} ${label}`);
if (failures.length) {
  console.error(`\n${failures.length} exhibitions prototype contract check(s) failed.`);
  process.exit(1);
}
console.log(`\nPASS exhibitions prototype contract (${checks.length} checks, ${uniqueIds.size} unique events)`);
