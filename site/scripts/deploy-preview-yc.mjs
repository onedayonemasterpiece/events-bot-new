import { spawnSync } from 'node:child_process';
import { existsSync, readdirSync, statSync, readFileSync } from 'node:fs';
import { basename, join, resolve } from 'node:path';
import { eventIcsDownloadFilename, transportIcsDownloadFilename } from '../src/lib/icsFilenames.mjs';

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
function loadPreviewEventsBySlug() {
  const dataPath = join(siteDir, 'src', 'data', 'preview-events.json');
  if (!existsSync(dataPath)) return new Map();
  const parsed = JSON.parse(readFileSync(dataPath, 'utf8'));
  const out = new Map();
  for (const event of parsed.events || []) {
    if (event?.slug && event?.id) out.set(String(event.slug), event);
  }
  return out;
}

const target = `s3://${bucket}/${buildId}/`;
console.log(`Uploading ${sourceDir} -> ${target}`);
const cp = spawnSync('aws', ['--endpoint-url', endpoint, 's3', 'cp', sourceDir, target, '--recursive', '--cache-control', 'public, max-age=300', '--no-progress'], { env: awsEnv, stdio: 'inherit' });
if (cp.status !== 0) process.exit(cp.status || 1);
// Astro output filenames are content-hashed and the preview prefix is versioned,
// so code assets are safe to cache aggressively through the CDN.
const astroAssetsDir = join(sourceDir, '_astro');
if (existsSync(astroAssetsDir)) {
  const findAstroAssets = spawnSync('find', [astroAssetsDir, '-type', 'f'], { encoding: 'utf8' });
  for (const file of findAstroAssets.stdout.split(/\r?\n/).filter(Boolean)) {
    const rel = file.slice(sourceDir.length + 1);
    const put = spawnSync('aws', ['--endpoint-url', endpoint, 's3', 'cp', file, `s3://${bucket}/${buildId}/${rel}`, '--cache-control', 'public, max-age=31536000, immutable', '--no-progress'], { env: awsEnv, stdio: 'inherit' });
    if (put.status !== 0) process.exit(put.status || 1);
  }
}
// Ensure calendar endpoints have the right metadata for clients that care about content-type.
const eventsBySlug = loadPreviewEventsBySlug();
let stableIcsUploaded = 0;
const find = spawnSync('find', [sourceDir, '-name', '*.ics', '-type', 'f'], { encoding: 'utf8' });
for (const file of find.stdout.split(/\r?\n/).filter(Boolean)) {
  const rel = file.slice(sourceDir.length + 1);
  const eventMatch = /^sobytiya\/([^/]+)\/event\.ics$/u.exec(rel);
  const transportMatch = /^sobytiya\/([^/]+)\/transport\/([^/]+)\.ics$/u.exec(rel);
  const eventSlug = eventMatch?.[1] || transportMatch?.[1] || null;
  const event = eventSlug ? eventsBySlug.get(eventSlug) : null;
  const downloadFilename = eventMatch && event
    ? eventIcsDownloadFilename(event)
    : transportMatch && event
      ? transportIcsDownloadFilename(event, transportMatch[2])
      : basename(file);
  const put = spawnSync('aws', ['--endpoint-url', endpoint, 's3', 'cp', file, `s3://${bucket}/${buildId}/${rel}`, '--content-type', 'text/calendar; charset=utf-8', '--content-disposition', `inline; filename="${downloadFilename}"`, '--cache-control', 'public, max-age=300', '--no-progress'], { env: awsEnv, stdio: 'inherit' });
  if (put.status !== 0) process.exit(put.status || 1);
  const slugEventId = eventMatch ? /-(\d+)$/u.exec(eventMatch[1])?.[1] : null;
  const eventId = eventMatch ? (event?.id || (slugEventId ? Number(slugEventId) : null)) : null;
  if (eventId) {
    const stablePut = spawnSync('aws', ['--endpoint-url', endpoint, 's3', 'cp', file, `s3://${bucket}/ics/${eventId}.ics`, '--content-type', 'text/calendar; charset=utf-8', '--content-disposition', `inline; filename="${downloadFilename}"`, '--cache-control', 'public, max-age=300', '--no-progress'], { env: awsEnv, stdio: 'inherit' });
    if (stablePut.status !== 0) process.exit(stablePut.status || 1);
    stableIcsUploaded += 1;
  }
}
const publicBase = (env.KENIGEVENTS_SITE_PUBLIC_BASE_URL || `https://kenigevents.ru`).replace(/\/+$/u, '');
console.log(`Preview URL: ${publicBase}/${buildId}/__preview/`);
console.log(`Website endpoint fallback: http://${bucket}.website.yandexcloud.net/${buildId}/__preview/`);
if (stableIcsUploaded) console.log(`Stable CDN ICS uploaded: ${stableIcsUploaded} -> s3://${bucket}/ics/<event_id>.ics`);

const requirePublicVerify = ['1', 'true', 'yes', 'on'].includes(String(env.KENIGEVENTS_SITE_REQUIRE_PUBLIC_VERIFY || '').toLowerCase());
const verifyTargets = [
  [`${publicBase}/${buildId}/__preview/`, 'main-domain preview index'],
  [`http://${bucket}.website.yandexcloud.net/${buildId}/__preview/`, 'website-endpoint preview index'],
];
if (stableIcsUploaded) {
  const firstIcs = find.stdout.split(/\r?\n/).find((file) => /\/event\.ics$/u.test(file));
  const match = firstIcs ? /^sobytiya\/([^/]+)\/event\.ics$/u.exec(firstIcs.slice(sourceDir.length + 1)) : null;
  const eventId = match ? eventsBySlug.get(match[1])?.id || /-(\d+)$/u.exec(match[1])?.[1] : null;
  if (eventId && env.PUBLIC_ICS_BASE_URL) {
    verifyTargets.push([`${String(env.PUBLIC_ICS_BASE_URL).replace(/\/+$/u, '')}/${eventId}.ics`, 'stable ICS CDN sample']);
  }
}
const publicFailures = [];
for (const [url, label] of verifyTargets) {
  try {
    const response = await fetch(url, { method: 'HEAD', redirect: 'follow' });
    if (!response.ok) {
      publicFailures.push(`${label}: ${response.status} ${response.statusText} at ${url}`);
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
