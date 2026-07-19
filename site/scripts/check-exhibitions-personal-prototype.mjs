import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';

const siteRoot = path.resolve(import.meta.dirname, '..');
const pageSource = fs.readFileSync(path.join(siteRoot, 'src/pages/lab/exhibitions-personal/index.astro'), 'utf8');
const rowSource = fs.readFileSync(path.join(siteRoot, 'src/components/ExhibitionPrototypeRow.astro'), 'utf8');
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
  ['notification has visible accessible copy', /data-exhibition-badge[^>]*aria-live="polite"/u.test(html) && html.includes('3 новых')],
  ['responsive and reduced-motion contracts exist', pageSource.includes('@media (max-width:740px)') && pageSource.includes('@media (prefers-reduced-motion:reduce)')],
  ['mobile restores all five navigation targets', pageSource.includes('.ex-header__nav>a:not(.is-current) { display:flex; }')],
  ['photo deck keeps the lead image on top', pageSource.includes('z-index:calc(10 - var(--deck-index))')],
  ['collapsed hover metadata leaves the accessibility tree', pageSource.includes('.ex-row__expanded { visibility:hidden;') && pageSource.includes('visibility:visible; grid-template-rows:1fr')],
  ['roving tabindex follows arrow focus', pageSource.includes('links.forEach((link) => { link.tabIndex = -1; })') && pageSource.includes('next.tabIndex = 0')],
];

const failures = checks.filter(([, passed]) => !passed);
for (const [label, passed] of checks) console.log(`${passed ? 'PASS' : 'FAIL'} ${label}`);
if (failures.length) {
  console.error(`\n${failures.length} exhibitions prototype contract check(s) failed.`);
  process.exit(1);
}
console.log(`\nPASS exhibitions prototype contract (${checks.length} checks, ${uniqueIds.size} unique events)`);
