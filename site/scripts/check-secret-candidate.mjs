import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import eventsData from '../src/data/preview-events.json' with { type: 'json' };
import festivalTimelineData from '../src/data/festival-timeline.json' with { type: 'json' };
import templateContract from '../src/data/eventTemplateContract.json' with { type: 'json' };
import {
  CANDIDATE_MANIFEST_SCHEMA, candidateBasePath, fileInventory, safeCandidateToken, sha256, treeHash,
} from './release-contract.mjs';

const siteDir = dirname(dirname(fileURLToPath(import.meta.url)));
const token = safeCandidateToken(process.env.SECRET_CANDIDATE_TOKEN || '');
const basePath = candidateBasePath(token);
const root = join(siteDir, 'dist', basePath.slice(1));
function fail(message) { throw new Error(`Secret-candidate check failed: ${message}`); }
function source(key) { try { return readFileSync(join(root, key), 'utf8'); } catch { fail(`missing ${key}`); } }
const manifest = JSON.parse(source('secret-candidate-manifest.json'));
if (manifest.schema_version !== CANDIDATE_MANIFEST_SCHEMA || manifest.site_mode !== 'secret_candidate' || manifest.publication_mode !== 'secret_link') fail('wrong candidate manifest profile');
if (manifest.base_path !== basePath || manifest.token_sha256 !== sha256(token)) fail('candidate token/base mismatch');
for (const route of [
  'segodnya',
  'zavtra',
  'vyhodnye',
  'vystavki',
  'festivali',
  'populyarnoe',
  'poisk',
  'dlya-menya',
  'kluby-po-interesam',
  'partners',
]) source(`${route}/index.html`);
for (const slug of ['dzhaz-na-vyhodnyh', 'besplatno-s-detmi', 'stendap-na-etoy-nedele']) {
  source(`podborki/${slug}/index.html`);
}
if (
  festivalTimelineData.schema_version !== 'festival-timeline-static-v1'
  || festivalTimelineData.source !== 'sqlite-festival-calendar-v1'
  || festivalTimelineData.database_row_count < 21
) fail('DB-backed festival timeline receipt is incomplete');
const transportExperiment = manifest.experiments?.transport_timetable_layout;
const transportQaRoute = 'lab/event-desktop/examples/editorial-ocr-companion-arrival';
const footerRegressionRoute = 'lab/event-desktop/examples/footer-service-v1';
const splitCtaRegressionRoute = 'lab/event-desktop/examples/cta-phone-invariant';
const registrationCtaRegressionRoute = 'lab/event-desktop/examples/cta-registration-invariant';
const freeCalendarCtaRegressionRoute = 'lab/event-desktop/examples/cta-free-calendar-invariant';
const retainedLabRoutes = [
  transportQaRoute,
  footerRegressionRoute,
  splitCtaRegressionRoute,
  registrationCtaRegressionRoute,
  freeCalendarCtaRegressionRoute,
];
if (!transportExperiment || !['qa', 'focus_group'].includes(transportExperiment.mode)) fail('transport timetable experiment mode missing');
if (transportExperiment.config_hash !== 'sha256:bf9a8a80e35c8699a26993ae25ac83313d4b6923900f9e51688d2dad7d92cdf2') fail('transport timetable experiment config mismatch');
if (transportExperiment.mode === 'qa' && transportExperiment.trusted_telemetry !== false) fail('QA experiment telemetry must be untrusted');
if (transportExperiment.qa_route !== `/${transportQaRoute}/`) fail('transport QA route contract mismatch');
const files = fileInventory(root, { exclude: ['secret-candidate-manifest.json'], secretCandidate: true });
const byKey = new Map(manifest.files.map((file) => [file.key, file]));
if (files.length !== byKey.size || manifest.counts.file_count !== files.length) fail('candidate file count mismatch');
for (const file of files) {
  const expected = byKey.get(file.key);
  if (!expected || expected.sha256 !== file.sha256 || expected.size !== file.size || expected.cache_control !== 'private, no-store, max-age=0') fail(`candidate inventory mismatch ${file.key}`);
}
if (manifest.tree_sha256 !== treeHash(files)) fail('candidate tree hash mismatch');
for (const key of files.map((file) => file.key)) {
  if (/^__preview(?:\/|$)/u.test(key) || (/^lab(?:\/|$)/u.test(key) && !retainedLabRoutes.some((route) => key.startsWith(`${route}/`))) || key === 'partnerstvo/index.html') fail(`QA route leaked ${key}`);
}
source(`${transportQaRoute}/index.html`);
const footerRegressionHtml = source(`${footerRegressionRoute}/index.html`);
const splitCtaRegressionHtml = source(`${splitCtaRegressionRoute}/index.html`);
const registrationCtaRegressionHtml = source(`${registrationCtaRegressionRoute}/index.html`);
const freeCalendarCtaRegressionHtml = source(`${freeCalendarCtaRegressionRoute}/index.html`);
if (!footerRegressionHtml.includes('data-site-footer="service-v1"')) fail('accepted service footer marker missing');
const footerServiceHtml = footerRegressionHtml.match(/<footer\b[^>]*data-site-footer="service-v1"[^>]*>[\s\S]*?<\/footer>/u)?.[0] || '';
if ((footerServiceHtml.match(/>Партнёры</gu) || []).length !== 1 || (footerServiceHtml.match(/>Стать партнёром</gu) || []).length !== 1) fail('accepted service footer must expose distinct Partners and partnership links exactly once');
if (!footerServiceHtml.includes('Пользовательское соглашение') || !footerServiceHtml.includes('Политика обработки персональных данных')) fail('accepted service footer legal links missing');
if (!footerRegressionHtml.includes('data-desktop-family="editorial"') || !footerRegressionHtml.includes('data-action-layout="stacked"')) fail('accepted Editorial CTA regression marker missing');
if (!splitCtaRegressionHtml.includes('data-desktop-family="split"') || !splitCtaRegressionHtml.includes('data-action-layout="inline"')) fail('accepted Split CTA regression marker missing');
if (!registrationCtaRegressionHtml.includes('data-action-layout="inline"') || !registrationCtaRegressionHtml.includes('Зарегистрироваться')) fail('registration CTA regression marker missing');
if (!freeCalendarCtaRegressionHtml.includes('data-action-layout="inline"') || !freeCalendarCtaRegressionHtml.includes('В календарь')) fail('calendar-primary CTA regression marker missing');
for (const event of eventsData.events) {
  const eventHtml = source(`sobytiya/${event.slug}/index.html`);
  source(`sobytiya/${event.slug}/event.ics`); source(`data/discovery/${event.id}.json`);
  if (!eventHtml.includes(`data-event-template-contract="${templateContract.contract_id}"`)) fail(`event ${event.id} misses accepted template contract marker`);
  if (!eventHtml.includes(`data-event-template-source="${templateContract.accepted_source_sha}"`)) fail(`event ${event.id} misses accepted template source marker`);
  if (!/data-desktop-family="(?:editorial|split)"/u.test(eventHtml)) fail(`event ${event.id} bypasses the accepted desktop family router`);
}
for (const file of files.filter((item) => item.key.endsWith('.html'))) {
  const html = source(file.key);
  if (html.includes('{buildId}') || /https:\/\/static\.kenigevents\.ru\/[^"']*\/_astro\//u.test(html)) fail(`external or unresolved Astro asset prefix in ${file.key}`);
  if (!/<meta\s+name="robots"\s+content="noindex,nofollow,noarchive,nosnippet"/iu.test(html)) fail(`noindex policy missing ${file.key}`);
  if (!/<meta\s+name="referrer"\s+content="no-referrer"/iu.test(html)) fail(`no-referrer policy missing ${file.key}`);
  const internalAttributes = [...html.matchAll(/(?:href|src|data-card-href|data-share-url)="(\/[^"]*)"/gu)].map((match) => match[1]);
  for (const url of internalAttributes) {
    if (url.startsWith('//')) continue;
    if (!url.startsWith(`${basePath}/`) && !url.startsWith(`${basePath}?`) && url !== basePath) fail(`out-of-prefix internal URL in ${file.key}: ${url}`);
  }
  if (html.includes('https://static.kenigevents.ru/ics/') || /href="\/ics\//u.test(html)) fail(`stable ICS leaked ${file.key}`);
}
const eventHtml = source(`sobytiya/${eventsData.events[0].slug}/index.html`);
if (!eventHtml.includes(`${basePath}/_astro/`)) fail('candidate Astro assets are not self-contained under the bearer prefix');
if (!source('index.html').includes('data-secret-candidate-root-listing')) fail('candidate root is not production-family listing');
const robots = source('robots.txt');
if (robots !== 'User-agent: *\nDisallow: /\n') fail('candidate robots artifact must remain disallow');
const sitemap = source('sitemap.xml');
if (sitemap.includes('/__preview/') || sitemap.includes('/lab/') || !sitemap.includes(`${manifest.base_path}/sobytiya/`)) fail('candidate sitemap route isolation failed');
console.log(`ADD-BUILD-07/10 secret candidate check passed: ${manifest.build_id}, ${files.length} files`);
