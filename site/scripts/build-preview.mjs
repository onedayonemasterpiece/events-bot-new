import { spawnSync } from 'node:child_process';
import { mkdirSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { loadPreviewPublicConfig, requirePreviewAuthorizedSearch } from './preview-public-env.mjs';
import { selectedTransportFaultProfile } from './transport-fault-build-contract.mjs';
import { normalizeStaticSitePageClasses } from './page-class-build-filter.mjs';

function safeBuildId(value) {
  if (!value || !/^preview-[a-zA-Z0-9._-]+$/.test(value) || value.includes('/')) {
    throw new Error(`Invalid preview build id: ${value || '(empty)'}`);
  }
  return value;
}

function gitShortSha() {
  const result = spawnSync('git', ['rev-parse', '--short=8', 'HEAD'], { encoding: 'utf8' });
  return result.status === 0 ? result.stdout.trim() : 'nogit';
}

function gitFullSha() {
  const configured = String(process.env.STATIC_SITE_REPO_SHA || '').trim().toLowerCase();
  if (configured) {
    if (!/^[0-9a-f]{40}$/u.test(configured)) {
      throw new Error('STATIC_SITE_REPO_SHA must be a full commit SHA');
    }
    return configured;
  }
  const result = spawnSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' });
  const value = result.status === 0 ? result.stdout.trim().toLowerCase() : '';
  if (!/^[0-9a-f]{40}$/u.test(value)) throw new Error('Cannot record full repo SHA in preview-build.json');
  return value;
}

const now = new Date();
function dateInKaliningrad(value) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Kaliningrad',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(value);
  const part = (type) => parts.find((item) => item.type === type)?.value || '';
  return `${part('year')}-${part('month')}-${part('day')}`;
}
const effectiveCurrentDate = process.env.STATIC_SITE_CURRENT_DATE || dateInKaliningrad(now);
const effectiveReferenceIso = process.env.STATIC_SITE_CURRENT_DATETIME || now.toISOString();
const stamp = now.toISOString().replace(/[-:]/g, '').replace(/\.\d+Z$/, 'Z').slice(0, 15).toLowerCase();
const buildId = safeBuildId(process.env.PREVIEW_BUILD_ID || `preview-${stamp}-${gitShortSha()}`);
const siteDir = resolve(new URL('..', import.meta.url).pathname);
const distDir = join(siteDir, 'dist');
const stagedDistDir = join(siteDir, `.preview-dist-${process.pid}`);
const astroAssetBaseUrl = (process.env.PUBLIC_ASTRO_ASSET_BASE_URL || '')
  .replace(/\{buildId\}/g, buildId)
  .replace(/\{BUILD_ID\}/g, buildId)
  .replace(/\/+$/u, '');
const publicSearchConfig = loadPreviewPublicConfig(siteDir, process.env);
requirePreviewAuthorizedSearch(publicSearchConfig, process.env);
const transportFault = selectedTransportFaultProfile(process.env);
const pageClasses = normalizeStaticSitePageClasses(process.env.STATIC_SITE_PAGE_CLASSES || 'all');

rmSync(distDir, { recursive: true, force: true });
const env = {
  ...process.env,
  ...publicSearchConfig.values,
  SITE_BASE_PATH: `/${buildId}`,
  PUBLIC_PREVIEW_BUILD_ID: buildId,
  PUBLIC_SITE_ORIGIN: process.env.PUBLIC_SITE_ORIGIN || 'https://kenigevents.ru',
  PUBLIC_STATIC_SITE_CURRENT_DATE: effectiveCurrentDate,
  PUBLIC_STATIC_SITE_REFERENCE_ISO: effectiveReferenceIso,
  PUBLIC_INTEREST_CLUBS_ENABLED: process.env.PUBLIC_INTEREST_CLUBS_ENABLED || '1',
  ENABLE_INTEREST_CLUB_STATIC_PROJECTION: process.env.ENABLE_INTEREST_CLUB_STATIC_PROJECTION || '1',
  ...(astroAssetBaseUrl ? { PUBLIC_ASTRO_ASSET_BASE_URL: astroAssetBaseUrl } : {}),
};
const astroBin = process.platform === 'win32' ? 'astro.cmd' : 'astro';
const result = spawnSync(astroBin, ['build'], {
  cwd: siteDir,
  env,
  stdio: 'inherit',
  shell: process.platform === 'win32',
});
if (result.status !== 0) {
  process.exit(result.status || 1);
}

rmSync(stagedDistDir, { recursive: true, force: true });
renameSync(distDir, stagedDistDir);
mkdirSync(join(distDir, buildId), { recursive: true });
renameSync(stagedDistDir, join(distDir, buildId));
writeFileSync(join(distDir, buildId, 'preview-build.json'), JSON.stringify({
  buildId,
  repo_sha: gitFullSha(),
  generatedAt: now.toISOString(),
  basePath: `/${buildId}`,
  astroAssetBaseUrl: astroAssetBaseUrl || null,
  authorizedSearchConfigured: publicSearchConfig.configured,
  currentDate: effectiveCurrentDate,
  referenceIso: effectiveReferenceIso,
  transportFaultProfile: transportFault.id,
  transportFaultRegistryDigest: transportFault.registry_digest,
  pageClasses,
}, null, 2));
console.log(`Preview build ready: dist/${buildId}/`);
console.log(`Preview URL: https://kenigevents.ru/${buildId}/__preview/`);
if (astroAssetBaseUrl) console.log(`Astro asset CDN: ${astroAssetBaseUrl}/_astro/`);
console.log(`Authorized Search: ${publicSearchConfig.configured ? 'configured with browser-safe public values' : 'disabled (public config unavailable)'}`);
