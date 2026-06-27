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
const target = `s3://${bucket}/${buildId}/`;
console.log(`Uploading ${sourceDir} -> ${target}`);
const cp = spawnSync('aws', ['--endpoint-url', endpoint, 's3', 'cp', sourceDir, target, '--recursive', '--no-progress'], { env: awsEnv, stdio: 'inherit' });
if (cp.status !== 0) process.exit(cp.status || 1);
// Ensure calendar endpoints have the right metadata for clients that care about content-type.
const find = spawnSync('find', [sourceDir, '-name', 'event.ics', '-type', 'f'], { encoding: 'utf8' });
for (const file of find.stdout.split(/\r?\n/).filter(Boolean)) {
  const rel = file.slice(sourceDir.length + 1);
  const put = spawnSync('aws', ['--endpoint-url', endpoint, 's3', 'cp', file, `s3://${bucket}/${buildId}/${rel}`, '--content-type', 'text/calendar; charset=utf-8', '--content-disposition', 'attachment; filename="event.ics"', '--cache-control', 'public, max-age=300', '--no-progress'], { env: awsEnv, stdio: 'inherit' });
  if (put.status !== 0) process.exit(put.status || 1);
}
const publicBase = (env.KENIGEVENTS_SITE_PUBLIC_BASE_URL || `https://kenigevents.ru`).replace(/\/+$/u, '');
console.log(`Preview URL: ${publicBase}/${buildId}/__preview/`);
console.log(`Website endpoint fallback: http://${bucket}.website.yandexcloud.net/${buildId}/__preview/`);
