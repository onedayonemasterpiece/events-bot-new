import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import eventsData from '../src/data/preview-events.json' with { type: 'json' };
import catalogData from '../src/data/production-catalog.json' with { type: 'json' };
import festivalTimelineData from '../src/data/festival-timeline.json' with { type: 'json' };
import templateContract from '../src/data/eventTemplateContract.json' with { type: 'json' };
import {
  CHECK_CONTRACT_VERSION, RELEASE_MANIFEST_SCHEMA, fileInventory, sha256, treeHash, validateCatalogLedger,
} from './release-contract.mjs';

const siteDir = dirname(dirname(fileURLToPath(import.meta.url)));
const root = join(siteDir, 'dist');
const manifestPath = join(root, 'static-release-manifest.json');
const buildPath = join(root, 'production-build.json');
function fail(message) { throw new Error(`Production check failed: ${message}`); }
function required(path) { if (!existsSync(join(root, path))) fail(`missing ${path}`); }
function html(path) { return readFileSync(join(root, path), 'utf8'); }

required('static-release-manifest.json'); required('production-build.json');
const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
const build = JSON.parse(readFileSync(buildPath, 'utf8'));
if (manifest.schema_version !== RELEASE_MANIFEST_SCHEMA || manifest.site_mode !== 'production' || manifest.publication_mode !== 'artifact_only') fail('wrong manifest profile');
if (build.validation_contract !== CHECK_CONTRACT_VERSION || build.site_mode !== 'production' || build.base_path !== '/') fail('wrong build profile');
if (manifest.build_id !== build.build_id || manifest.run_id !== build.run_id || manifest.repo_sha !== build.repo_sha) fail('build/manifest identity mismatch');
if (process.env.PRODUCTION_BUILD_ID && manifest.build_id !== process.env.PRODUCTION_BUILD_ID) fail('build id differs from requested id');
validateCatalogLedger(catalogData, { repo_sha: manifest.repo_sha, run_id: manifest.run_id, build_id: manifest.build_id, 'snapshot.sha256': manifest.snapshot.sha256 });
if (manifest.catalog.sha256 !== sha256(readFileSync(join(siteDir, 'src/data/production-catalog.json')))) fail('catalog ledger hash mismatch');
if (
  manifest.versions?.template !== templateContract.contract_id
  || manifest.versions?.template_source_sha !== templateContract.accepted_source_sha
  || manifest.versions?.template_contract_schema !== templateContract.schema_version
  || manifest.checks?.template_matrix !== 'ok'
) fail('accepted v11 template contract is not pinned in the checked manifest');

const files = fileInventory(root, { exclude: ['static-release-manifest.json'] });
const manifestByKey = new Map(manifest.files.map((file) => [file.key, file]));
if (files.length !== manifest.files.length || manifestByKey.size !== files.length) fail('tree/manifest file count mismatch');
for (const file of files) {
  const expected = manifestByKey.get(file.key);
  if (!expected || expected.sha256 !== file.sha256 || expected.size !== file.size || expected.content_type !== file.content_type) fail(`file inventory mismatch: ${file.key}`);
}
if (manifest.tree_sha256 !== treeHash(files)) fail('tree hash mismatch');
if (manifest.counts.file_count !== files.length || manifest.counts.bytes !== files.reduce((sum, file) => sum + file.size, 0)) fail('manifest aggregate counts mismatch');

for (const path of [
  'index.html',
  'segodnya/index.html',
  'zavtra/index.html',
  'vyhodnye/index.html',
  'vystavki/index.html',
  'festivali/index.html',
  'populyarnoe/index.html',
  'poisk/index.html',
  'dlya-menya/index.html',
  'kluby-po-interesam/index.html',
  'partners/index.html',
  'partnerstvo/index.html',
  'podborki/besplatnye-sobytiya/index.html',
  'podborki/dzhaz-na-vyhodnyh/index.html',
  'podborki/besplatno-s-detmi/index.html',
  'podborki/stendap-na-etoy-nedele/index.html',
  'robots.txt',
  'sitemap.xml',
]) required(path);
const productionWeekendSource = html('vyhodnye/index.html');
const productionArtifactMarker = /\bdata-amber-artifact(?:\s|=|>)/u;
if (
  productionArtifactMarker.test(productionWeekendSource)
  || productionWeekendSource.includes('kenigevents:artifact-collected')
) fail('artifact research leaked into production weekend listing');
const productionArtifactSource = html('artefakty/index.html');
if (
  productionArtifactSource.includes('data-artifact-collection')
  || productionArtifactSource.includes('artifact-detail-title')
  || !productionArtifactSource.includes('Коллекция пока недоступна')
) fail('artifact collection leaked into production');
const freeCollectionSource = html('podborki/besplatnye-sobytiya/index.html');
const freeCollectionResults = freeCollectionSource.slice(
  freeCollectionSource.indexOf('data-search-collection-results'),
  freeCollectionSource.indexOf('</section>', freeCollectionSource.indexOf('data-search-collection-results')),
);
const freeCollectionIds = [...freeCollectionResults.matchAll(/data-event-id="(\d+)"/gu)].map((match) => Number(match[1]));
const eventByIdForCollections = new Map(eventsData.events.map((event) => [Number(event.id), event]));
if (!freeCollectionIds.length) fail('general free collection is unexpectedly empty');
if (freeCollectionIds.some((id) => !eventByIdForCollections.get(id)?.ticket?.is_free)) fail('general free collection contains a non-free exported event');
const ongoingFreeIds = new Set(eventsData.events
  .filter((event) => event.ticket?.is_free && event.start_date < eventsData.build.current_date && event.end_date >= eventsData.build.current_date)
  .map((event) => Number(event.id)));
if (ongoingFreeIds.size && !freeCollectionIds.some((id) => ongoingFreeIds.has(id))) fail('general free collection lost all ongoing free events');
const jazzCollectionSource = html('podborki/dzhaz-na-vyhodnyh/index.html');
if (!jazzCollectionSource.includes('data-search-collection-results')) {
  if (!jazzCollectionSource.includes('data-search-collection-empty-window')) fail('empty Jazz collection misses its explicit weekend explanation');
  const referenceDate = jazzCollectionSource.match(/подборка рассчитана на (\d{4}-\d{2}-\d{2})/u)?.[1];
  const reference = referenceDate ? new Date(`${referenceDate}T00:00:00Z`) : null;
  if (!reference || !Number.isFinite(reference.getTime())) fail('Jazz collection reference date is not inspectable');
  const day = reference.getUTCDay();
  reference.setUTCDate(reference.getUTCDate() + (day === 6 ? 0 : day === 0 ? -1 : 6 - day) + 2);
  const firstDayAfterWeekend = reference.toISOString().slice(0, 10);
  const laterJazzExists = eventsData.events.some((event) => event.lifecycle_status === 'active'
    && (event.end_date || event.start_date) >= referenceDate
    && event.start_date >= firstDayAfterWeekend
    && /джаз/iu.test(event.title));
  if (laterJazzExists && !jazzCollectionSource.includes('data-search-collection-fallback')) fail('empty Jazz collection hides later exported Jazz events');
}
if (!files.some((file) => /^date-\d{4}-\d{2}-\d{2}\/index\.html$/u.test(file.key))) fail('generated date page family is missing');
if (!files.some((file) => /^vyhodnye\/\d{4}-\d{2}-\d{2}\/index\.html$/u.test(file.key))) fail('generated weekend page family is missing');
if (
  festivalTimelineData.schema_version !== 'festival-timeline-static-v1'
  || festivalTimelineData.source !== 'sqlite-festival-calendar-v1'
  || !Array.isArray(festivalTimelineData.festivals)
  || festivalTimelineData.database_row_count < 21
) fail('DB-backed festival timeline receipt is incomplete');
if (
  manifest.versions?.festival_calendar?.schema_version !== festivalTimelineData.schema_version
  || manifest.versions?.festival_calendar?.source !== festivalTimelineData.source
  || manifest.versions?.festival_calendar?.projection_sha256
    !== sha256(readFileSync(join(siteDir, 'src/data/festival-timeline.json')))
  || manifest.versions?.festival_calendar?.rendered_count !== festivalTimelineData.festivals.length
) fail('festival timeline release receipt/hash mismatch');
const festivalSlugs = festivalTimelineData.festivals.map((item) => item.slug);
if (festivalSlugs.length !== new Set(festivalSlugs).size) fail('festival timeline contains duplicate slugs');
for (const key of files.map((file) => file.key)) {
  if (/^(?:__preview|lab)(?:\/|$)/u.test(key) || /^preview-[^/]+\//u.test(key)) fail(`preview/fixture route leaked: ${key}`);
}
const eventById = new Map(eventsData.events.map((event) => [Number(event.id), event]));
const eligibleIds = catalogData.eligible.map((item) => Number(item.event_id));
if (eventById.size !== eligibleIds.length || eligibleIds.some((id) => !eventById.has(id))) fail('ADD-BUILD-09 eligible/exported catalog parity failed');
for (const id of eventById.keys()) if (!eligibleIds.includes(id)) fail(`ineligible event leaked: ${id}`);
for (const event of eventsData.events) {
  required(`sobytiya/${event.slug}/index.html`);
  required(`sobytiya/${event.slug}/event.ics`);
  required(`data/discovery/${event.id}.json`);
  const eventSource = html(`sobytiya/${event.slug}/index.html`);
  if (!eventSource.includes(`data-event-template-contract="${templateContract.contract_id}"`)) fail(`event ${event.id} misses accepted template contract marker`);
  if (!eventSource.includes(`data-event-template-source="${templateContract.accepted_source_sha}"`)) fail(`event ${event.id} misses accepted template source marker`);
  if (!/data-desktop-family="(?:editorial|split)"/u.test(eventSource)) fail(`event ${event.id} bypasses the accepted desktop family router`);
  const source = catalogData.eligible.find((item) => Number(item.event_id) === Number(event.id));
  if ((source?.age_restriction || null) !== (event.age_restriction || null)) fail(`accepted age lost for event ${event.id}`);
  const linked = [...new Set((event.other_date_ids || []).map(Number))];
  if (linked.includes(Number(event.id))) fail(`self linked occurrence ${event.id}`);
  for (const linkedId of linked) {
    const target = eventById.get(linkedId);
    if (!target) fail(`dangling linked occurrence ${event.id}->${linkedId}`);
    if (!(target.other_date_ids || []).map(Number).includes(Number(event.id))) fail(`asymmetric linked occurrence ${event.id}<->${linkedId}`);
  }
}

const siteOrigin = manifest.site_origin;
for (const file of files.filter((item) => item.key.endsWith('.html'))) {
  const source = html(file.key);
  const intentionallyUnindexed = file.key === 'dlya-menya/index.html'
    || /^podborki\/[^/]+\/index\.html$/u.test(file.key);
  if (intentionallyUnindexed) {
    if (!/<meta\s+name="robots"\s+content="noindex,nofollow,noarchive"/iu.test(source)) fail(`private/personal noindex policy missing from ${file.key}`);
  } else {
    if (/<meta\s+name="robots"\s+content="[^"]*\bnoindex\b[^"]*"/iu.test(source)) fail(`noindex leaked into ${file.key}`);
    if (!/<meta\s+name="robots"\s+content="index,follow"/iu.test(source)) fail(`index,follow missing from ${file.key}`);
  }
  if (/<meta\s+name="referrer"\s+content="no-referrer"/iu.test(source)) fail(`secret-candidate policy leaked into ${file.key}`);
  if (source.includes('/__preview/') || source.includes('/_review/') || source.includes('Preview · noindex')) fail(`preview/candidate reference leaked into ${file.key}`);
  const canonical = file.key === 'index.html' ? `${siteOrigin}/` : (file.key.endsWith('/index.html') ? `${siteOrigin}/${file.key.slice(0, -'index.html'.length)}` : null);
  if (canonical && !source.includes(`<link rel="canonical" href="${canonical}">`)) fail(`canonical mismatch ${file.key}`);
  if (/storage\.yandexcloud\.net\/(?:kenigevents|kenigevents\.ru)/u.test(source)) fail(`raw Object Storage URL leaked into ${file.key}`);
  if (/(?:src|href)="https?:\/\/[^"]+\.(?:png|jpe?g|gif)(?:[?#"])/iu.test(source)) fail(`external runtime raster fallback leaked into ${file.key}`);
}
if (!html('index.html').includes('data-production-root-listing')) fail('root is not the today listing');
const robots = readFileSync(join(root, 'robots.txt'), 'utf8');
if (robots !== `User-agent: *\nAllow: /\nSitemap: ${siteOrigin}/sitemap.xml\n`) fail('robots policy is not production indexable');
const sitemap = readFileSync(join(root, 'sitemap.xml'), 'utf8');
const locs = [...sitemap.matchAll(/<loc>([^<]+)<\/loc>/gu)].map((match) => match[1]);
if (new Set(locs).size !== locs.length || locs.some((url) => !url.startsWith(`${siteOrigin}/`) || /\/(?:__preview|lab|_review)(?:\/|$)/u.test(url))) fail('sitemap contains duplicate/off-origin/QA URLs');
for (const event of eventsData.events) if (!locs.includes(`${siteOrigin}/sobytiya/${event.slug}/`)) fail(`event missing from sitemap ${event.id}`);
if (!locs.includes(`${siteOrigin}/festivali/`)) fail('festival calendar missing from sitemap');
if (!locs.includes(`${siteOrigin}/partnerstvo/`)) fail('partnership page missing from sitemap');
if (!Array.isArray(manifest.stable_ics) || manifest.stable_ics.length !== eventsData.events.length) fail('stable ICS manifest parity failed');
for (const item of manifest.stable_ics) {
  if (item.target_key !== `ics/${item.event_id}.ics` || !manifestByKey.has(item.source_key)) fail(`invalid stable ICS mapping ${item.event_id}`);
}
console.log(`ADD-BUILD-07/09 production check passed: ${manifest.build_id}, ${eligibleIds.length} events, ${files.length} files`);
