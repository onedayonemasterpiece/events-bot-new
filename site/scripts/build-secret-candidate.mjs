import { spawnSync } from 'node:child_process';
import { mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  CANDIDATE_MANIFEST_SCHEMA, candidateBasePath, fileInventory, pageCounts, safeCandidateToken,
  sha256, treeHash, validateCatalogLedger,
} from './release-contract.mjs';
import { loadPreviewPublicConfig, requirePreviewAuthorizedSearch } from './preview-public-env.mjs';
import { assertTransportFaultBuildDisabled, removeTransportFaultBuildEnv } from './transport-fault-build-contract.mjs';

const siteDir = dirname(dirname(fileURLToPath(import.meta.url)));
const distDir = join(siteDir, 'dist');
const catalogPath = join(siteDir, 'src/data/production-catalog.json');
const eventsPath = join(siteDir, 'src/data/preview-events.json');
const productionManifestPath = join(distDir, 'static-release-manifest.json');
const token = safeCandidateToken(process.env.SECRET_CANDIDATE_TOKEN || '');
const basePath = candidateBasePath(token);
const candidateRoot = join(distDir, basePath.slice(1));
assertTransportFaultBuildDisabled(process.env, 'secret-candidate');
const siteOrigin = (process.env.PUBLIC_SITE_ORIGIN || 'https://kenigevents.ru').replace(/\/+$/u, '');
const productionManifestBytes = readFileSync(productionManifestPath);
const productionManifest = JSON.parse(productionManifestBytes);
if (typeof productionManifest.prelaunch_mode !== 'boolean') {
  throw new Error('Checked production manifest is missing boolean prelaunch_mode');
}
const prelaunchMode = productionManifest.prelaunch_mode;
const publicSurface = prelaunchMode ? 'prelaunch' : 'full_catalog';
const publicSearchConfig = loadPreviewPublicConfig(siteDir, process.env);
requirePreviewAuthorizedSearch(publicSearchConfig, {
  ...process.env,
  PREVIEW_REQUIRE_AUTHORIZED_SEARCH: process.env.SECRET_CANDIDATE_REQUIRE_AUTHORIZED_SEARCH || '',
});
const transportExperimentMode = process.env.SECRET_CANDIDATE_TRANSPORT_EXPERIMENT_MODE || 'qa';
const transportQaRoute = 'lab/event-desktop/examples/editorial-ocr-companion-arrival';
const footerPrototypeRoute = 'lab/event-desktop/examples/footer-service-v1';
// The compact Split CTA is retained as an expiry-proof browser-acceptance
// surface. Real event URLs are still checked during review, but they cannot be
// the only executable contract because an event can legitimately leave the
// active catalog between two Smart Update builds.
const splitCtaRegressionRoute = 'lab/event-desktop/examples/cta-phone-invariant';
const registrationCtaRegressionRoute = 'lab/event-desktop/examples/cta-registration-invariant';
const freeCalendarCtaRegressionRoute = 'lab/event-desktop/examples/cta-free-calendar-invariant';
const retainedLabRoutes = [
  transportQaRoute,
  footerPrototypeRoute,
  splitCtaRegressionRoute,
  registrationCtaRegressionRoute,
  freeCalendarCtaRegressionRoute,
];
if (!['qa', 'focus_group'].includes(transportExperimentMode)) throw new Error('Secret candidate transport experiment mode must be qa or focus_group');
for (const key of ['template_matrix','production_contract','catalog_parity','fixture_isolation','canonical_and_indexing','tree_hashes']) {
  if (productionManifest.checks?.[key] !== 'ok') throw new Error(`Production artifact is not checked: ${key}`);
}
const catalog = JSON.parse(readFileSync(catalogPath, 'utf8'));
validateCatalogLedger(catalog, { repo_sha: productionManifest.repo_sha, run_id: productionManifest.run_id, build_id: productionManifest.build_id });

rmSync(distDir, { recursive: true, force: true });
const env = {
  ...process.env,
  ...publicSearchConfig.values,
  PUBLIC_SITE_MODE: 'secret_candidate', PUBLIC_SITE_ORIGIN: siteOrigin, SITE_BASE_PATH: basePath,
  PUBLIC_ICS_BASE_URL: '', PUBLIC_PREVIEW_BUILD_ID: '', PUBLIC_ROOT_PREVIEW_HREF: '',
  PUBLIC_ASTRO_ASSET_BASE_URL: '',
  PUBLIC_TRANSPORT_TIMETABLE_EXPERIMENT_MODE: transportExperimentMode,
  PUBLIC_STATIC_RELEASE_ID: productionManifest.build_id,
  // A secret candidate is a review projection of the checked production
  // artifact. It must not silently replace a prelaunch root with the catalog.
  PUBLIC_PRELAUNCH_MODE: prelaunchMode ? 'on' : 'off',
  // Mirror the checked production projection instead of degrading a review
  // candidate to the empty-state merely because the shell omitted the flag.
  PUBLIC_INTEREST_CLUBS_ENABLED: process.env.PUBLIC_INTEREST_CLUBS_ENABLED || '1',
};
removeTransportFaultBuildEnv(env);
const astro = spawnSync(process.platform === 'win32' ? 'astro.cmd' : 'astro', ['build'], { cwd: siteDir, env, stdio: 'inherit', shell: process.platform === 'win32' });
if (astro.status !== 0) process.exit(astro.status || 1);
rmSync(join(distDir, '__preview'), { recursive: true, force: true });
// Secret candidates retain only the explicit noindex review specimens: the
// transport experiment, isolated footer prototype and compact Split CTA
// regression surfaces (phone, long registration label and calendar-primary).
// Every other lab route stays excluded from the immutable candidate.
const retainedLabStaging = retainedLabRoutes.map((route, index) => {
  const staged = join(siteDir, `.secret-lab-${process.pid}-${index}`);
  rmSync(staged, { recursive: true, force: true });
  renameSync(join(distDir, route), staged);
  return { route, staged };
});
rmSync(join(distDir, 'lab'), { recursive: true, force: true });
for (const { route, staged } of retainedLabStaging) {
  mkdirSync(dirname(join(distDir, route)), { recursive: true });
  renameSync(staged, join(distDir, route));
}
// Preserve the dedicated home surface; the candidate prefix is already
// applied by Astro and must not be replaced with the Today listing.
let rootHtml = readFileSync(join(distDir, 'index.html'), 'utf8');
if (!rootHtml.includes(`<link rel="canonical" href="${siteOrigin}${basePath}/">`)
  || !rootHtml.includes(`<meta property="og:url" content="${siteOrigin}${basePath}/">`)) {
  throw new Error('Cannot package candidate root: home canonical metadata is missing');
}
rootHtml = rootHtml.replace('<main id="main"', '<main id="main" data-secret-candidate-root-home');
writeFileSync(join(distDir, 'index.html'), rootHtml);
const staged = join(siteDir, `.secret-candidate-dist-${process.pid}`);
rmSync(staged, { recursive: true, force: true });
renameSync(distDir, staged);
mkdirSync(dirname(candidateRoot), { recursive: true });
renameSync(staged, candidateRoot);

const buildMetadata = {
  schema_version: 'static_secret_candidate_build_v1', site_mode: 'secret_candidate', publication_mode: 'secret_link',
  build_id: productionManifest.build_id, run_id: productionManifest.run_id, repo_sha: productionManifest.repo_sha,
  generated_at: new Date().toISOString(), base_path: basePath, token_sha256: sha256(token),
  prelaunch_mode: prelaunchMode, public_surface: publicSurface,
  production_manifest_sha256: sha256(productionManifestBytes), production_tree_sha256: productionManifest.tree_sha256,
  snapshot: productionManifest.snapshot,
};
writeFileSync(join(candidateRoot, 'candidate-build.json'), `${JSON.stringify(buildMetadata, null, 2)}\n`);
const files = fileInventory(candidateRoot, { exclude: ['secret-candidate-manifest.json'], secretCandidate: true });
const events = JSON.parse(readFileSync(eventsPath, 'utf8')).events;
const manifest = {
  schema_version: CANDIDATE_MANIFEST_SCHEMA, site_mode: 'secret_candidate', publication_mode: 'secret_link',
  build_id: productionManifest.build_id, run_id: productionManifest.run_id, repo_sha: productionManifest.repo_sha,
  generated_at: buildMetadata.generated_at, base_path: basePath, token_sha256: sha256(token),
  prelaunch_mode: prelaunchMode, public_surface: publicSurface,
  production_manifest_sha256: buildMetadata.production_manifest_sha256, production_tree_sha256: productionManifest.tree_sha256,
  snapshot: productionManifest.snapshot, catalog: productionManifest.catalog, versions: productionManifest.versions,
  experiments: {
    transport_timetable_layout: {
      ...productionManifest.versions.transport_timetable_experiment,
      mode: env.PUBLIC_TRANSPORT_TIMETABLE_EXPERIMENT_MODE,
      trusted_telemetry: env.PUBLIC_TRANSPORT_TIMETABLE_EXPERIMENT_MODE === 'focus_group',
      qa_route: `/${transportQaRoute}/`,
    },
  },
  counts: pageCounts(files, events.length), tree_sha256: treeHash(files), files,
  checks: { astro_build: 'ok', candidate_contract: 'pending', catalog_parity: 'pending', noindex: 'pending', no_referrer: 'pending', prefix_containment: 'pending', root_isolation: 'pending' },
};
writeFileSync(join(candidateRoot, 'secret-candidate-manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`);
const check = spawnSync(process.execPath, [join(siteDir, 'scripts/check-secret-candidate.mjs')], { cwd: siteDir, env: { ...env, SECRET_CANDIDATE_TOKEN: token }, stdio: 'inherit' });
if (check.status !== 0) process.exit(check.status || 1);
for (const key of ['candidate_contract','catalog_parity','noindex','no_referrer','prefix_containment','root_isolation']) manifest.checks[key] = 'ok';
writeFileSync(join(candidateRoot, 'secret-candidate-manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`);
console.log(`Checked secret candidate ready: ${basePath}/ token_sha256=${sha256(token)}`);
