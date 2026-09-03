import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import {
  GOLDEN_CORPUS_PATH,
  loadGoldenCorpus,
  materializeGoldenPreviewData,
  sha256,
} from '../scripts/golden-review-corpus.mjs';

const testsDir = dirname(fileURLToPath(import.meta.url));
const siteDir = resolve(testsDir, '..');
const repositoryRoot = resolve(siteDir, '..');
const corpusRelativePath = 'site/scripts/golden-review-corpus.v1.json';

const read = (relative) => readFileSync(join(siteDir, relative), 'utf8');

function idsFor(corpus, date) {
  return corpus.events.filter((event) => event.date === date).map((event) => event.id);
}

function sorted(values) {
  return [...values].sort((left, right) => left - right);
}

test('Golden corpus has a frozen Kaliningrad Friday, exact 5/6/5 density and one reused Weekend occurrence set', () => {
  const { corpus, digest } = loadGoldenCorpus();
  assert.match(digest, /^[0-9a-f]{64}$/u);
  assert.equal(corpus.frozen_clock.timezone, 'Europe/Kaliningrad');
  assert.equal(corpus.frozen_clock.current_date, corpus.frozen_clock.friday);
  assert.equal(corpus.events.length, 16);
  assert.equal(idsFor(corpus, corpus.frozen_clock.friday).length, 5);
  assert.equal(idsFor(corpus, corpus.frozen_clock.saturday).length, 6);
  assert.equal(idsFor(corpus, corpus.frozen_clock.sunday).length, 5);
  assert.deepEqual(
    sorted(corpus.route_contract.weekend.event_ids),
    sorted([...corpus.route_contract.tomorrow.event_ids, ...corpus.route_contract.sunday.event_ids]),
  );
  assert.deepEqual(
    sorted(corpus.route_contract.dated_weekend.event_ids),
    sorted(corpus.route_contract.weekend.event_ids),
  );
  assert.equal(new Set(corpus.events.map((event) => event.id)).size, 16);
  assert.equal(new Set(corpus.events.map((event) => event.slug)).size, 16);
  assert.equal(new Set(corpus.events.map((event) => event.source_identity)).size, 16);
});

test('Golden stress coverage includes framing, long copy, admission, calendar and lifecycle cells', () => {
  const { corpus } = loadGoldenCorpus();
  const tags = new Set(corpus.events.flatMap((event) => event.stress_tags));
  for (const tag of [
    'media-cover', 'media-contain', 'media-multiple', 'media-missing', 'media-error',
    'long-copy', 'admission-free', 'admission-ticket', 'admission-registration',
    'admission-phone', 'admission-source', 'calendar-single', 'calendar-range',
    'lifecycle-cancelled', 'lifecycle-rescheduled',
  ]) {
    assert.equal(tags.has(tag), true, `missing Golden stress tag ${tag}`);
  }
  assert.ok(corpus.events.some((event) => event.title.length > 120));
  assert.ok(corpus.events.some((event) => event.description_html?.length > 300));
  assert.ok(corpus.events.some((event) => event.lifecycle_status === 'cancelled'));
  assert.ok(corpus.events.some((event) => event.status_label === 'Перенесено'));
  assert.ok(corpus.events.some((event) => event.end_date === corpus.frozen_clock.sunday));
});

test('materialization overlays Golden dates while retaining only historical real-data canaries', () => {
  const { corpus } = loadGoldenCorpus();
  const base = JSON.parse(read('src/data/preview-events.json'));
  const materialized = materializeGoldenPreviewData(corpus, base);
  const goldenIds = new Set(corpus.events.map((event) => event.id));
  const materializedGolden = materialized.events.filter((event) => goldenIds.has(Number(event.id)));
  const retainedReal = materialized.events.filter((event) => !goldenIds.has(Number(event.id)));
  assert.equal(materializedGolden.length, 16);
  assert.ok(retainedReal.length > 0, 'real canaries must remain available to existing lab/build contracts');
  assert.ok(retainedReal.every((event) => String(event.end_date || event.start_date || '') < corpus.frozen_clock.friday));
  assert.equal(materialized.build.current_date, corpus.frozen_clock.current_date);
  assert.equal(materialized.build.generated_at, corpus.frozen_clock.reference_iso);
  assert.equal(materialized.build.source, `golden-review-corpus:${corpus.corpus_id}`);
  assert.equal(new Set(materialized.events.map((event) => Number(event.id))).size, materialized.events.length);
});

test('pinned image and SVG identities resolve to the committed Git blobs', () => {
  const { corpus } = loadGoldenCorpus();
  for (const asset of corpus.pinned_assets) {
    const path = join(siteDir, 'public', asset.path.replace(/^\/+/, ''));
    assert.equal(existsSync(path) && statSync(path).isFile(), true, `missing ${asset.path}`);
    const gitHash = spawnSync('git', ['hash-object', path], { cwd:repositoryRoot, encoding:'utf8' });
    assert.equal(gitHash.status, 0, gitHash.stderr);
    assert.equal(gitHash.stdout.trim(), asset.git_blob_sha, `Git blob drift for ${asset.id}`);
    if (asset.sha256) assert.equal(sha256(readFileSync(path)), asset.sha256, `SHA-256 drift for ${asset.id}`);
  }
});

test('the two-week stability window permits additions but forbids mutation or deletion of existing identities', () => {
  const { corpus } = loadGoldenCorpus();
  if (Date.now() > Date.parse(corpus.stability.append_only_until)) return;
  const history = spawnSync('git', [
    'log', '--format=%H', '--follow', '--', corpusRelativePath,
  ], { cwd:repositoryRoot, encoding:'utf8' });
  assert.equal(history.status, 0, history.stderr);
  const commits = history.stdout.trim().split(/\r?\n/u).filter(Boolean);
  if (commits.length < 2) return;
  const previousRaw = spawnSync('git', [
    'show', `${commits[1]}:${corpusRelativePath}`,
  ], { cwd:repositoryRoot, encoding:'utf8' });
  assert.equal(previousRaw.status, 0, previousRaw.stderr);
  const previous = JSON.parse(previousRaw.stdout);
  const currentEvents = new Map(corpus.events.map((event) => [event.id, event]));
  for (const event of previous.events) {
    assert.deepEqual(currentEvents.get(event.id), event, `event ${event.id} changed inside append-only window`);
  }
  const currentAssets = new Map(corpus.pinned_assets.map((asset) => [asset.id, asset]));
  for (const asset of previous.pinned_assets) {
    assert.deepEqual(currentAssets.get(asset.id), asset, `asset ${asset.id} changed inside append-only window`);
  }
});

test('Golden local generation is diagnostic-only and full publication is the canonical Kaggle rail', () => {
  const packageJson = JSON.parse(read('package.json'));
  const builder = read('scripts/build-golden-preview.mjs');
  const previewBuilder = read('scripts/build-preview.mjs');
  const checker = read('scripts/check-golden-preview.mjs');
  const actionChecker = read('scripts/check-golden-actions.mjs');
  const acceptance = JSON.parse(read('scripts/n0-successor-acceptance.v1.json'));
  const goldenDoc = readFileSync(
    resolve(repositoryRoot, 'docs', 'features', 'static-site-pages', 'design-system', 'golden-review-preview-v1.md'),
    'utf8',
  );

  assert.equal(packageJson.scripts['build:golden-preview'], 'node scripts/build-golden-preview.mjs');
  assert.equal(
    packageJson.scripts['check:golden-preview'],
    'node scripts/check-golden-preview.mjs && node scripts/check-golden-actions.mjs',
  );
  assert.equal(
    packageJson.scripts['test:golden-preview-contract'],
    'node --test tests/golden-review-preview-contract.test.mjs tests/golden-review-actions.test.mjs',
  );
  assert.equal(packageJson.scripts['deploy:preview'], undefined);
  assert.equal(packageJson.scripts['deploy:golden-preview'], undefined);
  assert.equal(packageJson.scripts['test:golden-deploy-contract'], undefined);
  assert.equal(existsSync(join(siteDir, 'scripts', 'deploy-golden-preview.mjs')), false);
  assert.equal(existsSync(join(siteDir, 'tests', 'golden-review-deploy-contract.test.mjs')), false);

  assert.match(builder, /PREVIEW_DATA_MODE:'golden'/u);
  assert.match(builder, /STATIC_SITE_CURRENT_DATE:corpus\.frozen_clock\.current_date/u);
  assert.match(builder, /PUBLIC_SEARCH_COLLECTION_REFERENCE_DATE:corpus\.frozen_clock\.current_date/u);
  assert.match(builder, /finally \{[\s\S]*atomicWrite\(eventsPath, originalRaw\)[\s\S]*rmSync\(lockPath/u);
  assert.match(builder, /restoredDigest !== originalDigest/u);
  assert.match(previewBuilder, /dataMode: previewDataMode/u);
  assert.match(previewBuilder, /goldenCorpusDigest/u);
  assert.match(checker, /DateListingSurface/u);
  assert.match(checker, /WeekendListingSurface/u);
  assert.match(checker, /FreeCollectionSurface/u);
  assert.match(checker, /ordinary_routes_only:true/u);
  assert.match(actionChecker, /goldenActionContract/u);
  assert.match(actionChecker, /free_and_cancelled_external_actions:false/u);

  const realGate = acceptance.gate_graph.find((item) => item.id === 'FULL_REAL_KAGGLE_PREVIEW');
  assert.ok(realGate, 'full-real Kaggle gate is missing');
  assert.ok(realGate.requires.includes('canonical events-bot-new StaticSiteBuilder'));
  assert.ok(realGate.requires.includes('--preview-data-mode real'));
  assert.ok(realGate.requires.includes('--page-class all'));
  assert.ok(realGate.requires.includes('create-only immutable-prefix publication'));
  assert.ok(acceptance.prohibitions.includes('full or published preview outside the canonical Kaggle pipeline'));

  assert.match(goldenDoc, /same Kaggle runner/u);
  assert.match(goldenDoc, /Local diagnostic/u);
  assert.match(goldenDoc, /one existing `events-bot-new` Kaggle `StaticSiteBuilder`/u);
  assert.doesNotMatch(goldenDoc, /npm run deploy:golden-preview/u);
  assert.equal(existsSync(join(siteDir, 'src', 'pages', 'golden')), false, 'a second Golden UI route is forbidden');
  assert.equal(existsSync(join(siteDir, 'src', 'pages', 'lab', 'golden')), false, 'an owner-facing Golden lab is forbidden');
  assert.equal(existsSync(GOLDEN_CORPUS_PATH), true);
});
