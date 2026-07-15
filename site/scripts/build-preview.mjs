import { spawnSync } from 'node:child_process';
import { mkdirSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

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

const now = new Date();
const stamp = now.toISOString().replace(/[-:]/g, '').replace(/\.\d+Z$/, 'Z').slice(0, 15).toLowerCase();
const buildId = safeBuildId(process.env.PREVIEW_BUILD_ID || `preview-${stamp}-${gitShortSha()}`);
const siteDir = resolve(new URL('..', import.meta.url).pathname);
const distDir = join(siteDir, 'dist');
const stagedDistDir = join(siteDir, `.preview-dist-${process.pid}`);
const astroAssetBaseUrl = (process.env.PUBLIC_ASTRO_ASSET_BASE_URL || '')
  .replace(/\{buildId\}/g, buildId)
  .replace(/\{BUILD_ID\}/g, buildId)
  .replace(/\/+$/u, '');

rmSync(distDir, { recursive: true, force: true });
const env = {
  ...process.env,
  SITE_BASE_PATH: `/${buildId}`,
  PUBLIC_PREVIEW_BUILD_ID: buildId,
  PUBLIC_SITE_ORIGIN: process.env.PUBLIC_SITE_ORIGIN || 'https://kenigevents.ru',
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
writeFileSync(join(distDir, buildId, 'preview-build.json'), JSON.stringify({ buildId, generatedAt: now.toISOString(), basePath: `/${buildId}`, astroAssetBaseUrl: astroAssetBaseUrl || null }, null, 2));
console.log(`Preview build ready: dist/${buildId}/`);
console.log(`Preview URL: https://kenigevents.ru/${buildId}/__preview/`);
if (astroAssetBaseUrl) console.log(`Astro asset CDN: ${astroAssetBaseUrl}/_astro/`);
