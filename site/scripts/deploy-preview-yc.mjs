import { spawnSync } from 'node:child_process';
import { existsSync, readdirSync, statSync, readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

const siteDir = resolve(new URL('..', import.meta.url).pathname);
const repoRoot = resolve(siteDir, '..');
const envPath = join(repoRoot, '.env');

function loadDotEnv(path) {
  if (!existsSync(path)) return {};
  const text = readFileSync(path, 'utf8');
  const env = {};
  for (const line of text.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#') || !trimmed.includes('=')) continue;
    const idx = trimmed.indexOf('=');
    const key = trimmed.slice(0, idx).trim();
    let value = trimmed.slice(idx + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    env[key] = value;
  }
  return env;
}

const fileEnv = loadDotEnv(envPath);
const env = { ...fileEnv, ...process.env };
const distDir = join(siteDir, 'dist');
const buildId = env.PREVIEW_BUILD_ID || readdirSync(distDir).find((name) => name.startsWith('preview-'));
if (!buildId) throw new Error('No PREVIEW_BUILD_ID and no dist/preview-* folder found');
if (!/^preview-[a-z0-9][a-z0-9._-]*$/u.test(buildId)) {
  throw new Error(`Preview deploy refuses a non-preview build id: ${buildId}`);
}
const sourceDir = join(distDir, buildId);
if (!existsSync(sourceDir) || !statSync(sourceDir).isDirectory()) throw new Error(`Missing preview build folder: ${sourceDir}`);
const bucket = env.KENIGEVENTS_SITE_YC_BUCKET;
const endpoint = env.KENIGEVENTS_SITE_YC_ENDPOINT || 'https://storage.yandexcloud.net';
const region = env.KENIGEVENTS_SITE_YC_REGION || 'ru-central1';
const accessKey = env.KENIGEVENTS_SITE_YC_ACCESS_KEY_ID;
const secretKey = env.KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY;
if (!bucket || !accessKey || !secretKey) {
  throw new Error('Missing KENIGEVENTS_SITE_YC_BUCKET / KENIGEVENTS_SITE_YC_ACCESS_KEY_ID / KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY');
}
const awsEnv = {
  ...process.env,
  AWS_ACCESS_KEY_ID: accessKey,
  AWS_SECRET_ACCESS_KEY: secretKey,
  AWS_DEFAULT_REGION: region,
};
const dryRun = ['1', 'true', 'yes', 'on'].includes(String(env.KENIGEVENTS_SITE_DEPLOY_DRY_RUN || '').toLowerCase());
const dryRunArgs = dryRun ? ['--dryrun'] : [];
const target = `s3://${bucket}/${buildId}/`;
console.log(`${dryRun ? 'Planning' : 'Uploading'} ${sourceDir} -> ${target}`);
console.log(`Preview-only destination prefix: ${target}`);
const cp = spawnSync('aws', ['--endpoint-url', endpoint, 's3', 'cp', sourceDir, target, '--recursive', '--cache-control', 'public, max-age=300', '--no-progress', ...dryRunArgs], { env: awsEnv, stdio: 'inherit' });
if (cp.status !== 0) process.exit(cp.status || 1);
if (dryRun) {
  console.log('Preview-only safety: every planned object is below the preview build prefix');
  console.log('Preview-only dry run complete; no objects were uploaded');
  process.exit(0);
}
// Astro output filenames are content-hashed and the preview prefix is versioned,
// so code assets are safe to cache aggressively through the CDN.
const astroAssetsDir = join(sourceDir, '_astro');
if (existsSync(astroAssetsDir)) {
  const findAstroAssets = spawnSync('find', [astroAssetsDir, '-type', 'f'], { encoding: 'utf8' });
  for (const file of findAstroAssets.stdout.split(/\r?\n/).filter(Boolean)) {
    const rel = file.slice(sourceDir.length + 1);
    const put = spawnSync('aws', ['--endpoint-url', endpoint, 's3', 'cp', file, `s3://${bucket}/${buildId}/${rel}`, '--cache-control', 'public, max-age=31536000, immutable', '--no-progress', ...dryRunArgs], { env: awsEnv, stdio: 'inherit' });
    if (put.status !== 0) process.exit(put.status || 1);
  }
}
// Service-share versions are content-addressed. Re-upload them with exact MIME
// and immutable caching; keep only the mutable current pointer uncached.
const serviceShareVersionsDir = join(sourceDir, 'service-share', 'versions');
if (existsSync(serviceShareVersionsDir)) {
  const versionedFiles = spawnSync('find', [serviceShareVersionsDir, '-type', 'f'], { encoding: 'utf8' });
  for (const file of versionedFiles.stdout.split(/\r?\n/).filter(Boolean)) {
    const rel = file.slice(sourceDir.length + 1);
    const extension = file.toLowerCase().split('.').pop();
    const contentType = extension === 'webp'
      ? 'image/webp'
      : extension === 'png'
        ? 'image/png'
        : extension === 'json'
          ? 'application/json; charset=utf-8'
          : 'application/octet-stream';
    const put = spawnSync('aws', ['--endpoint-url', endpoint, 's3', 'cp', file, `s3://${bucket}/${buildId}/${rel}`, '--content-type', contentType, '--cache-control', 'public, max-age=31536000, immutable', '--no-progress', ...dryRunArgs], { env: awsEnv, stdio: 'inherit' });
    if (put.status !== 0) process.exit(put.status || 1);
  }
}
const serviceShareCurrentManifest = join(sourceDir, 'service-share', 'current', 'manifest.json');
if (existsSync(serviceShareCurrentManifest)) {
  const rel = serviceShareCurrentManifest.slice(sourceDir.length + 1);
  const put = spawnSync('aws', ['--endpoint-url', endpoint, 's3', 'cp', serviceShareCurrentManifest, `s3://${bucket}/${buildId}/${rel}`, '--content-type', 'application/json; charset=utf-8', '--cache-control', 'no-cache, max-age=0', '--no-progress', ...dryRunArgs], { env: awsEnv, stdio: 'inherit' });
  if (put.status !== 0) process.exit(put.status || 1);
}
const pwaManifest = join(sourceDir, 'manifest.webmanifest');
if (existsSync(pwaManifest)) {
  const rel = pwaManifest.slice(sourceDir.length + 1);
  const put = spawnSync('aws', [
    '--endpoint-url', endpoint, 's3', 'cp', pwaManifest, `s3://${bucket}/${buildId}/${rel}`,
    '--content-type', 'application/manifest+json; charset=utf-8',
    '--cache-control', 'public, max-age=300, must-revalidate',
    '--no-progress', ...dryRunArgs,
  ], { env: awsEnv, stdio: 'inherit' });
  if (put.status !== 0) process.exit(put.status || 1);
}
const labDir = join(sourceDir, 'lab', 'pwa-capabilities');
const labManifest = join(labDir, 'manifest.webmanifest');
if (existsSync(labManifest)) {
  const rel = labManifest.slice(sourceDir.length + 1);
  const put = spawnSync('aws', [
    '--endpoint-url', endpoint, 's3', 'cp', labManifest, `s3://${bucket}/${buildId}/${rel}`,
    '--content-type', 'application/manifest+json; charset=utf-8',
    '--cache-control', 'public, max-age=300, must-revalidate',
    '--no-progress', ...dryRunArgs,
  ], { env: awsEnv, stdio: 'inherit' });
  if (put.status !== 0) process.exit(put.status || 1);
}
const labWorker = join(labDir, 'sw.js');
if (existsSync(labWorker)) {
  const rel = labWorker.slice(sourceDir.length + 1);
  const put = spawnSync('aws', [
    '--endpoint-url', endpoint, 's3', 'cp', labWorker, `s3://${bucket}/${buildId}/${rel}`,
    '--content-type', 'application/javascript; charset=utf-8',
    '--cache-control', 'no-cache, no-store, must-revalidate',
    '--no-progress', ...dryRunArgs,
  ], { env: awsEnv, stdio: 'inherit' });
  if (put.status !== 0) process.exit(put.status || 1);
}
// Ensure calendar endpoints have the right metadata in one bounded AWS process.
// Spawning one SDK process per event makes large previews exceed the launcher
// lifetime even though every destination is the same accepted prefix.
const icsPut = spawnSync('aws', [
  '--endpoint-url', endpoint, 's3', 'cp', sourceDir, target, '--recursive',
  '--exclude', '*', '--include', '*.ics',
  '--content-type', 'text/calendar; charset=utf-8',
  '--content-disposition', 'inline; filename="event.ics"',
  '--cache-control', 'public, max-age=300', '--no-progress',
], { env: awsEnv, stdio: 'inherit' });
if (icsPut.status !== 0) process.exit(icsPut.status || 1);
const publicBase = (env.KENIGEVENTS_SITE_PUBLIC_BASE_URL || `https://kenigevents.ru`).replace(/\/+$/u, '');
console.log(`Preview URL: ${publicBase}/${buildId}/__preview/`);
console.log(`Website endpoint fallback: http://${bucket}.website.yandexcloud.net/${buildId}/__preview/`);
console.log('Preview-only safety: stable s3://<bucket>/ics/* objects were not modified');

const requirePublicVerify = ['1', 'true', 'yes', 'on'].includes(String(env.KENIGEVENTS_SITE_REQUIRE_PUBLIC_VERIFY || '').toLowerCase());
const verifyTargets = [
  [`${publicBase}/${buildId}/__preview/`, 'main-domain preview index'],
  [`http://${bucket}.website.yandexcloud.net/${buildId}/__preview/`, 'website-endpoint preview index'],
];
if (existsSync(pwaManifest)) {
  verifyTargets.push([
    `${publicBase}/${buildId}/manifest.webmanifest`,
    'PWA manifest',
    'application/manifest+json',
  ]);
  for (const size of [192, 512]) {
    verifyTargets.push([
      `${publicBase}/${buildId}/assets/pwa/announcements-${size}.png`,
      `PWA ${size} icon`,
      'image/png',
    ]);
  }
}
if (existsSync(labManifest) && existsSync(labWorker)) {
  const labPublicUrl = `${publicBase}/${buildId}/lab/pwa-capabilities/`;
  verifyTargets.push(
    [labPublicUrl, 'PWA capabilities lab'],
    [`${labPublicUrl}manifest.webmanifest`, 'PWA capabilities lab manifest', 'application/manifest+json'],
    [`${labPublicUrl}sw.js`, 'PWA capabilities lab worker', 'application/javascript'],
  );
  for (const asset of [
    'assets/pwa/announcements-brand-v2-192.png',
    'assets/pwa/announcements-brand-v2-512.png',
    'assets/pwa/announcements-brand-v2-maskable-192.png',
    'assets/pwa/announcements-brand-v2-maskable-512.png',
    'assets/pwa/announcements-brand-192.png',
    'assets/pwa/focus-group-icon.png',
  ]) {
    verifyTargets.push([
      `${publicBase}/${buildId}/${asset}`,
      `PWA capabilities lab asset ${asset.split('/').pop()}`,
      'image/png',
    ]);
  }
}
if (existsSync(serviceShareCurrentManifest)) {
  const manifestPublicUrl = `${publicBase}/${buildId}/service-share/current/manifest.json`;
  verifyTargets.push([manifestPublicUrl, 'F18 current manifest', 'application/json']);
  const manifest = JSON.parse(readFileSync(serviceShareCurrentManifest, 'utf8'));
  for (const [kind, expectedType] of [['webp', 'image/webp'], ['png', 'image/png']]) {
    const value = manifest?.assets?.[kind]?.url;
    if (!value) continue;
    verifyTargets.push([new URL(String(value), manifestPublicUrl).href, `F18 ${kind} asset`, expectedType]);
  }
}
const publicFailures = [];
for (const [url, label, expectedType] of verifyTargets) {
  try {
    const response = await fetch(url, { method: 'HEAD', redirect: 'follow' });
    if (!response.ok) {
      publicFailures.push(`${label}: ${response.status} ${response.statusText} at ${url}`);
    } else if (expectedType && !String(response.headers.get('content-type') || '').toLowerCase().startsWith(expectedType)) {
      publicFailures.push(`${label}: expected Content-Type ${expectedType}, got ${response.headers.get('content-type') || '(missing)'} at ${url}`);
    }
  } catch (error) {
    publicFailures.push(`${label}: ${error?.message || error} at ${url}`);
  }
}
if (publicFailures.length) {
  console.error(`Public preview verification failed:\n- ${publicFailures.join('\n- ')}`);
  console.error('Objects were uploaded, but the bucket/CDN is not publicly serving them. Check bucket public-read policy/ACL or CDN origin access.');
  if (requirePublicVerify) process.exit(1);
} else {
  console.log('Public preview verification: ok');
}
