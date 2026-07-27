import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  CHECK_CONTRACT_VERSION, RELEASE_MANIFEST_SCHEMA, fileInventory, pageCounts,
  safeBuildId, safeRunId, sha256, treeHash, validateCatalogLedger,
} from './release-contract.mjs';
import { loadPreviewPublicConfig, requirePreviewAuthorizedSearch } from './preview-public-env.mjs';

const siteDir = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = resolve(siteDir, '..');
const distDir = join(siteDir, 'dist');
const catalogPath = join(siteDir, 'src/data/production-catalog.json');
const eventsPath = join(siteDir, 'src/data/preview-events.json');
const relatedPath = join(siteDir, 'src/data/preview-related.json');
const festivalTimelinePath = join(siteDir, 'src/data/festival-timeline.json');
const artifactRegistryPath = join(siteDir, 'src/data/artifactRegistry.json');
const templateContractPath = join(siteDir, 'src/data/eventTemplateContract.json');
const manifestPath = join(distDir, 'static-release-manifest.json');
const buildPath = join(distDir, 'production-build.json');

function gitSha() {
  const result = spawnSync('git', ['rev-parse', 'HEAD'], { cwd: repoRoot, encoding: 'utf8' });
  return result.status === 0 ? result.stdout.trim() : '';
}
function replaceRequired(source, before, after, label) {
  if (!source.includes(before)) throw new Error(`Cannot construct production root: missing ${label}`);
  return source.replace(before, after);
}
function normalizeOrigin(value) {
  const parsed = new URL(value || 'https://kenigevents.ru');
  if (parsed.protocol !== 'https:' || parsed.pathname !== '/' || parsed.search || parsed.hash || parsed.username || parsed.password) throw new Error('PUBLIC_SITE_ORIGIN must be one HTTPS origin');
  return parsed.origin;
}

if (!existsSync(catalogPath)) throw new Error('production-catalog.json is required; run a full-catalog export');
const catalog = JSON.parse(readFileSync(catalogPath, 'utf8'));
const repoSha = String(process.env.STATIC_SITE_REPO_SHA || catalog.repo_sha || gitSha());
if (!/^[0-9a-f]{40}$/u.test(repoSha)) throw new Error('STATIC_SITE_REPO_SHA must be a full commit SHA');
const buildId = safeBuildId(process.env.PRODUCTION_BUILD_ID || catalog.build_id);
const runId = safeRunId(process.env.STATIC_SITE_RUN_ID || catalog.run_id);
validateCatalogLedger(catalog, { repo_sha: repoSha, run_id: runId, build_id: buildId });
const currentGitSha = gitSha();
const allowDirty = /^(?:1|true|yes|on)$/iu.test(String(process.env.PRODUCTION_ALLOW_DIRTY || ''));
if (currentGitSha && currentGitSha !== repoSha && !allowDirty) throw new Error(`Checked-out SHA ${currentGitSha} differs from build SHA ${repoSha}`);
if (currentGitSha) {
  const dirty = spawnSync('git', ['status', '--porcelain', '--untracked-files=normal'], { cwd: repoRoot, encoding: 'utf8' }).stdout.trim();
  if (dirty && !allowDirty) throw new Error('Refusing production build from a dirty worktree');
}
const siteOrigin = normalizeOrigin(process.env.PUBLIC_SITE_ORIGIN || 'https://kenigevents.ru');
const icsBaseUrl = (process.env.PUBLIC_ICS_BASE_URL || 'https://static.kenigevents.ru/ics').replace(/\/+$/u, '');
const publicSearchConfig = loadPreviewPublicConfig(siteDir, process.env);
requirePreviewAuthorizedSearch(publicSearchConfig, {
  ...process.env,
  PREVIEW_REQUIRE_AUTHORIZED_SEARCH: process.env.PRODUCTION_REQUIRE_AUTHORIZED_SEARCH || '',
});

rmSync(distDir, { recursive: true, force: true });
const env = {
  ...process.env,
  ...publicSearchConfig.values,
  PUBLIC_SITE_MODE: 'production', PUBLIC_SITE_ORIGIN: siteOrigin, PUBLIC_ICS_BASE_URL: icsBaseUrl, SITE_BASE_PATH: '/',
  PUBLIC_ASSET_BASE_URL: process.env.PUBLIC_ASSET_BASE_URL || 'https://static.kenigevents.ru',
  PUBLIC_TRANSPORT_TIMETABLE_EXPERIMENT_MODE: 'off', PUBLIC_STATIC_RELEASE_ID: buildId,
};
delete env.PUBLIC_PREVIEW_BUILD_ID;
delete env.PUBLIC_ROOT_PREVIEW_HREF;
const astro = spawnSync(process.platform === 'win32' ? 'astro.cmd' : 'astro', ['build'], { cwd: siteDir, env, stdio: 'inherit', shell: process.platform === 'win32' });
if (astro.status !== 0) process.exit(astro.status || 1);

rmSync(join(distDir, '__preview'), { recursive: true, force: true });
rmSync(join(distDir, 'lab'), { recursive: true, force: true });
const todayPath = join(distDir, 'segodnya/index.html');
let rootHtml = readFileSync(todayPath, 'utf8');
rootHtml = replaceRequired(rootHtml, `<link rel="canonical" href="${siteOrigin}/segodnya/">`, `<link rel="canonical" href="${siteOrigin}/">`, 'today canonical');
rootHtml = replaceRequired(rootHtml, `<meta property="og:url" content="${siteOrigin}/segodnya/">`, `<meta property="og:url" content="${siteOrigin}/">`, 'today og:url');
rootHtml = replaceRequired(rootHtml, '<main id="main"', '<main id="main" data-production-root-listing', 'root marker');
writeFileSync(join(distDir, 'index.html'), rootHtml);

const eventsData = JSON.parse(readFileSync(eventsPath, 'utf8'));
const relatedData = JSON.parse(readFileSync(relatedPath, 'utf8'));
const festivalTimelineData = JSON.parse(readFileSync(festivalTimelinePath, 'utf8'));
const artifactRegistryData = JSON.parse(readFileSync(artifactRegistryPath, 'utf8'));
const templateContract = JSON.parse(readFileSync(templateContractPath, 'utf8'));
const desktopContract = spawnSync(process.execPath, [join(siteDir, 'scripts/check-production-desktop-contract.mjs')], { cwd: siteDir, env, stdio: 'inherit' });
if (desktopContract.status !== 0) process.exit(desktopContract.status || 1);
const buildMetadata = {
  schema_version: 'static_production_build_v2', site_mode: 'production', publication_mode: 'artifact_only',
  build_id: buildId, run_id: runId, repo_sha: repoSha, generated_at: new Date().toISOString(),
  site_origin: siteOrigin, base_path: '/', ics_base_url: icsBaseUrl,
  snapshot_id: catalog.snapshot.snapshot_id, snapshot_sha256: catalog.snapshot.sha256,
  validation_contract: CHECK_CONTRACT_VERSION,
};
writeFileSync(buildPath, `${JSON.stringify(buildMetadata, null, 2)}\n`);
const files = fileInventory(distDir, { exclude: ['static-release-manifest.json'] });
const counts = pageCounts(files, catalog.eligible_count);
const idBySlug = new Map(eventsData.events.map((event) => [String(event.slug), Number(event.id)]));
const stableIcs = files.flatMap((file) => {
  const match = /^sobytiya\/([^/]+)\/event\.ics$/u.exec(file.key);
  if (!match) return [];
  const eventId = idBySlug.get(match[1]);
  if (!eventId) throw new Error(`Cannot map stable ICS source ${file.key}`);
  return [{ event_id: eventId, source_key: file.key, target_key: `ics/${eventId}.ics`, sha256: file.sha256 }];
});
const manifest = {
  schema_version: RELEASE_MANIFEST_SCHEMA,
  publication_mode: 'artifact_only', site_mode: 'production', build_id: buildId, run_id: runId, repo_sha: repoSha,
  generated_at: buildMetadata.generated_at, site_origin: siteOrigin, base_path: '/', hash_algorithm: 'sha256',
  snapshot: catalog.snapshot,
  catalog: {
    schema_version: catalog.schema_version, eligibility_predicate_version: catalog.eligibility_predicate_version,
    sha256: sha256(readFileSync(catalogPath)), eligible_count: catalog.eligible_count, excluded_count: catalog.excluded_count,
  },
  versions: {
    exporter: 'prod-sqlite-static-site-export-v2',
    template: templateContract.contract_id,
    template_source_sha: templateContract.accepted_source_sha,
    template_contract_schema: templateContract.schema_version,
    related: relatedData.schema_version || relatedData.algorithm || null,
    festival_calendar: {
      schema_version: festivalTimelineData.schema_version,
      source: festivalTimelineData.source,
      catalog_versions: festivalTimelineData.catalog_versions,
      projection_sha256: sha256(readFileSync(festivalTimelinePath)),
      database_row_count: festivalTimelineData.database_row_count,
      rendered_count: festivalTimelineData.festivals.length,
    },
    artifact_registry: {
      schema_version: artifactRegistryData.schema_version,
      registry_version: artifactRegistryData.registry_version,
      projection_sha256: sha256(readFileSync(artifactRegistryPath)),
      rendered_count: artifactRegistryData.artifacts.length,
    },
    transport: 'event-transport-projection-v1', media: 'event-media-role-v1', age: 'event-age-projection-v1', occurrence: 'linked-event-ids-v1',
    transport_timetable_experiment: {
      experiment_key: 'transport_timetable_layout', experiment_version: 1, mode: 'off',
      assignment_unit: 'browser_subject', allocation_algorithm: 'sha256-u32be-bucket-10000-v1',
      config_hash: 'sha256:bf9a8a80e35c8699a26993ae25ac83313d4b6923900f9e51688d2dad7d92cdf2',
      variants: ['departure_board_v1', 'route_strips_v1', 'next_departure_queue_v1'],
    },
  },
  counts, tree_sha256: treeHash(files), files, stable_ics: stableIcs,
  checks: {
    astro_build: 'ok', template_matrix: 'ok', production_contract: 'pending', catalog_parity: 'pending', fixture_isolation: 'pending',
    canonical_and_indexing: 'pending', tree_hashes: 'pending', related_freshness: relatedData.strict_verified_related ? 'verified' : 'optional_degraded',
  },
  intended_immutable_release_prefix: `_static/releases/${buildId}/root`, previous_release: null, rollback_release: null,
};
writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
const check = spawnSync(process.execPath, [join(siteDir, 'scripts/check-production.mjs')], { cwd: siteDir, env: { ...env, PRODUCTION_BUILD_ID: buildId, STATIC_SITE_RUN_ID: runId, STATIC_SITE_REPO_SHA: repoSha, PRODUCTION_ALLOW_DIRTY: process.env.PRODUCTION_ALLOW_DIRTY || '' }, stdio: 'inherit' });
if (check.status !== 0) process.exit(check.status || 1);
for (const key of ['production_contract','catalog_parity','fixture_isolation','canonical_and_indexing','tree_hashes']) manifest.checks[key] = 'ok';
writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
console.log(`Checked root-form production artifact ready: ${buildId} events=${counts.event_count} files=${counts.file_count}`);
