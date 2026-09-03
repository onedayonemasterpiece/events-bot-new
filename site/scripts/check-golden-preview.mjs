import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { goldenEventMap, loadGoldenCorpus, sha256 } from './golden-review-corpus.mjs';

const siteDir = resolve(new URL('..', import.meta.url).pathname);
const distDir = join(siteDir, 'dist');
const buildId = process.env.PREVIEW_BUILD_ID
  || readdirSync(distDir).filter((name) => name.startsWith('preview-golden-')).sort().at(-1);
if (!buildId) throw new Error('No PREVIEW_BUILD_ID and no dist/preview-golden-* folder found');
if (!/^preview-golden-[a-zA-Z0-9._-]+$/u.test(buildId)) {
  throw new Error(`Golden checker refuses non-Golden build id: ${buildId}`);
}
const root = join(distDir, buildId);
if (!existsSync(root) || !statSync(root).isDirectory()) throw new Error(`Missing Golden build directory: ${root}`);

const source = loadGoldenCorpus();
const corpus = source.corpus;
const eventMap = goldenEventMap(corpus);
const copiedCorpusPath = join(root, 'golden-review-corpus.v1.json');
const buildMetadataPath = join(root, 'preview-build.json');
const evidencePath = join(root, 'data', 'golden', 'evidence.json');
for (const path of [copiedCorpusPath, buildMetadataPath, evidencePath, join(root, '__preview', 'index.html')]) {
  if (!existsSync(path) || !statSync(path).isFile()) throw new Error(`Missing Golden evidence file: ${path.slice(root.length + 1)}`);
}

const copiedCorpusRaw = readFileSync(copiedCorpusPath);
const copiedCorpus = JSON.parse(copiedCorpusRaw.toString('utf8'));
const buildMetadata = JSON.parse(readFileSync(buildMetadataPath, 'utf8'));
const evidence = JSON.parse(readFileSync(evidencePath, 'utf8'));
if (sha256(copiedCorpusRaw) !== source.digest) throw new Error('Published Golden corpus bytes differ from the source corpus');
if (JSON.stringify(copiedCorpus.route_contract) !== JSON.stringify(corpus.route_contract)) throw new Error('Published route contract differs from source');
if (buildMetadata.buildId !== buildId || buildMetadata.basePath !== `/${buildId}`) throw new Error('preview-build.json has a mismatched build identity');
if (!/^[0-9a-f]{40}$/u.test(String(buildMetadata.repo_sha || ''))) throw new Error('preview-build.json lacks a full repo_sha');
if (buildMetadata.dataMode !== 'golden') throw new Error(`Expected dataMode=golden, received ${buildMetadata.dataMode}`);
if (buildMetadata.goldenCorpusId !== corpus.corpus_id) throw new Error('preview-build.json has a mismatched Golden corpus id');
if (buildMetadata.goldenCorpusDigest !== source.digest) throw new Error('preview-build.json has a mismatched Golden corpus digest');
if (buildMetadata.currentDate !== corpus.frozen_clock.current_date) throw new Error('preview-build.json currentDate is not the frozen Friday');
if (buildMetadata.referenceIso !== corpus.frozen_clock.reference_iso) throw new Error('preview-build.json referenceIso is not the frozen clock');
if (evidence.build_id !== buildId || evidence.repo_sha !== buildMetadata.repo_sha) throw new Error('Golden evidence/build ancestry mismatch');
if (evidence.corpus_id !== corpus.corpus_id || evidence.corpus_sha256 !== source.digest) throw new Error('Golden evidence corpus mismatch');

const realEventsPath = join(siteDir, 'src', 'data', 'preview-events.json');
const restoredDigest = sha256(readFileSync(realEventsPath));
if (evidence.source_data_restore?.real_preview_events_sha256_before !== restoredDigest
  || evidence.source_data_restore?.real_preview_events_sha256_after !== restoredDigest) {
  throw new Error('The real preview dataset was not restored byte-for-byte after Golden generation');
}
if (evidence.source_data_restore?.golden_events !== corpus.events.length) throw new Error('Golden evidence event count mismatch');

for (const asset of corpus.pinned_assets) {
  const localPath = join(siteDir, 'public', asset.path.replace(/^\/+/, ''));
  if (!existsSync(localPath) || !statSync(localPath).isFile()) throw new Error(`Pinned asset is missing: ${asset.path}`);
  const gitHash = spawnSync('git', ['hash-object', localPath], { cwd:siteDir, encoding:'utf8' });
  if (gitHash.status !== 0 || gitHash.stdout.trim() !== asset.git_blob_sha) {
    throw new Error(`Pinned Git blob identity changed for ${asset.id}`);
  }
  if (asset.sha256 && sha256(readFileSync(localPath)) !== asset.sha256) {
    throw new Error(`Pinned SHA-256 identity changed for ${asset.id}`);
  }
}
for (const event of corpus.events) {
  for (const path of event.media?.asset_paths || []) {
    if (existsSync(join(siteDir, 'public', path.replace(/^\/+/, '')))) {
      throw new Error(`Intentional missing-media probe unexpectedly exists: ${path}`);
    }
  }
}

function routeFile(routePath) {
  const relative = routePath.replace(/^\/+|\/+$/gu, '');
  return join(root, relative, 'index.html');
}

function routeHtml(routeName, family) {
  const contract = corpus.route_contract[routeName];
  const path = routeFile(contract.path);
  if (!existsSync(path)) throw new Error(`Golden route is missing: ${contract.path}`);
  const html = readFileSync(path, 'utf8');
  if (!html.includes(`data-ds-family="${family}"`)) {
    throw new Error(`${contract.path} misses canonical ${family} identity`);
  }
  const expectedIds = contract.event_ids;
  const expected = new Set(expectedIds);
  for (const id of expectedIds) {
    const event = eventMap.get(id);
    if (!event || !html.includes(`/sobytiya/${event.slug}/`)) {
      throw new Error(`${contract.path} misses Golden event ${id}`);
    }
  }
  for (const event of corpus.events) {
    if (!expected.has(event.id) && html.includes(`/sobytiya/${event.slug}/`)) {
      throw new Error(`${contract.path} contains out-of-contract Golden event ${event.id}`);
    }
  }
  return html;
}

const todayHtml = routeHtml('today', 'DateListingSurface');
const tomorrowHtml = routeHtml('tomorrow', 'DateListingSurface');
const sundayHtml = routeHtml('sunday', 'DateListingSurface');
const weekendHtml = routeHtml('weekend', 'WeekendListingSurface');
const datedWeekendHtml = routeHtml('dated_weekend', 'WeekendListingSurface');
const freeHtml = routeHtml('free_collection', 'FreeCollectionSurface');

if (!sameMembers(corpus.route_contract.weekend.event_ids, corpus.route_contract.dated_weekend.event_ids)) {
  throw new Error('Current and dated Weekend contracts do not reuse one occurrence set');
}
if (!weekendHtml.includes(`data-weekend-start="${corpus.frozen_clock.saturday}"`)
  || !weekendHtml.includes(`data-weekend-end="${corpus.frozen_clock.sunday}"`)) {
  throw new Error('Current Weekend does not expose the frozen Saturday/Sunday range');
}
if (!datedWeekendHtml.includes(`data-weekend-start="${corpus.frozen_clock.saturday}"`)
  || !datedWeekendHtml.includes(`data-weekend-end="${corpus.frozen_clock.sunday}"`)) {
  throw new Error('Dated Weekend does not expose the frozen Saturday/Sunday range');
}
if (!freeHtml.includes('data-ds-family="AdaptiveEventCardGrid"')) throw new Error('Free collection misses the canonical AdaptiveEventCardGrid');
if (!freeHtml.includes('/assets/badges/free-listing-medallion.svg')) throw new Error('Free collection misses the pinned medallion SVG');
if (existsSync(join(root, 'lab', 'golden'))) throw new Error('Golden review must not create an owner-facing lab route');

const combinedDateHtml = `${todayHtml}\n${tomorrowHtml}\n${sundayHtml}\n${weekendHtml}`;
for (const value of ['data-media-frame-fit="cover"', 'data-media-frame-fit="contain"', 'data-media-frame-kind="fallback"']) {
  if (!combinedDateHtml.includes(value)) throw new Error(`Golden date surfaces miss MediaFrame stress evidence ${value}`);
}
for (const value of ['Перенесено', 'Отменено']) {
  if (!combinedDateHtml.includes(value)) throw new Error(`Golden date surfaces miss lifecycle label ${value}`);
}
if (!todayHtml.includes(corpus.events.find((event) => event.stress_tags.includes('long-copy')).title)) {
  throw new Error('Today route misses the long-copy specimen');
}

for (const event of corpus.events) {
  for (const relative of [
    `sobytiya/${event.slug}/index.html`,
    `sobytiya/${event.slug}/event.ics`,
    `data/discovery/${event.id}.json`,
  ]) {
    const path = join(root, relative);
    if (!existsSync(path) || !statSync(path).isFile()) throw new Error(`Golden event ${event.id} misses ${relative}`);
  }
}

function sameMembers(left, right) {
  if (left.length !== right.length) return false;
  const a = [...left].sort((x, y) => x - y);
  const b = [...right].sort((x, y) => x - y);
  return a.every((value, index) => value === b[index]);
}

console.log(JSON.stringify({
  status:'PASS',
  buildId,
  repo_sha:buildMetadata.repo_sha,
  corpus_id:corpus.corpus_id,
  corpus_sha256:source.digest,
  frozen_clock:corpus.frozen_clock,
  counts:{
    friday:corpus.route_contract.today.event_ids.length,
    saturday:corpus.route_contract.tomorrow.event_ids.length,
    sunday:corpus.route_contract.sunday.event_ids.length,
    weekend:corpus.route_contract.weekend.event_ids.length,
    free:corpus.route_contract.free_collection.event_ids.length,
  },
  exact_route_membership:true,
  weekend_occurrence_reuse:true,
  pinned_assets_verified:corpus.pinned_assets.length,
  ordinary_routes_only:true,
  real_preview_data_restored:true,
}, null, 2));
