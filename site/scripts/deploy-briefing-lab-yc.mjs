import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const siteDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const buildId = process.env.PREVIEW_BUILD_ID || '';
const strictBuildId = /^preview-\d{8}t\d{4}-briefing-lab-[0-9a-f]{8}$/u;
if (!strictBuildId.test(buildId)) {
  throw new Error('PREVIEW_BUILD_ID must match preview-YYYYMMDDtHHMM-briefing-lab-<sha8>');
}

const sourceDir = path.join(siteDir, 'dist-lab', buildId);
if (!existsSync(sourceDir) || !statSync(sourceDir).isDirectory()) {
  throw new Error(`Missing isolated lab build: ${sourceDir}`);
}

const check = spawnSync(process.execPath, [path.join(siteDir, 'scripts/check-briefing-lab.mjs')], {
  cwd: siteDir,
  env: { ...process.env, PREVIEW_BUILD_ID: buildId },
  stdio: 'inherit',
});
if (check.status !== 0) process.exit(check.status || 1);

const manifest = JSON.parse(readFileSync(path.join(sourceDir, 'lab-manifest.json'), 'utf8'));
if (manifest.kind !== 'briefing-lab' || manifest.buildId !== buildId || manifest.publicPath !== `/${buildId}/lab/briefing/`) {
  throw new Error('Refusing to deploy an invalid briefing-lab manifest');
}

const bucket = process.env.KENIGEVENTS_SITE_YC_BUCKET;
const endpoint = process.env.KENIGEVENTS_SITE_YC_ENDPOINT || 'https://storage.yandexcloud.net';
const region = process.env.KENIGEVENTS_SITE_YC_REGION || 'ru-central1';
const accessKey = process.env.KENIGEVENTS_SITE_YC_ACCESS_KEY_ID;
const secretKey = process.env.KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY;
if (!bucket || !accessKey || !secretKey) {
  throw new Error('Missing KENIGEVENTS_SITE_YC_BUCKET / KENIGEVENTS_SITE_YC_ACCESS_KEY_ID / KENIGEVENTS_SITE_YC_SECRET_ACCESS_KEY');
}

const target = `s3://${bucket}/${buildId}/`;
const awsEnv = {
  ...process.env,
  AWS_ACCESS_KEY_ID: accessKey,
  AWS_SECRET_ACCESS_KEY: secretKey,
  AWS_DEFAULT_REGION: region,
};
console.log(`Uploading immutable lab prefix ${sourceDir} -> ${target}`);
const upload = spawnSync('aws', [
  '--endpoint-url', endpoint,
  's3', 'cp', sourceDir, target,
  '--recursive',
  '--cache-control', 'public, max-age=300',
  '--no-progress',
], { env: awsEnv, stdio: 'inherit' });
if (upload.status !== 0) process.exit(upload.status || 1);

const publicBase = (process.env.KENIGEVENTS_SITE_PUBLIC_BASE_URL || 'https://kenigevents.ru').replace(/\/+$/u, '');
const labUrl = `${publicBase}/${buildId}/lab/briefing/`;
const websiteUrl = `http://${bucket}.website.yandexcloud.net/${buildId}/lab/briefing/`;
const failures = [];

for (const variant of ['a', 'b', 'c']) {
  const url = `${labUrl}?variant=${variant}&scenario=exhibitions_count`;
  try {
    const response = await fetch(url, { redirect: 'follow' });
    const html = await response.text();
    if (!response.ok) failures.push(`${response.status} ${url}`);
    if (!/<meta name="robots" content="noindex,nofollow,noarchive"/u.test(html)) failures.push(`noindex missing: ${url}`);
    if (!html.includes(`/${buildId}/_astro/`)) failures.push(`versioned CSS missing: ${url}`);
    if (/<[^>]+data-personal-feed-slot/iu.test(html)) failures.push(`personal feed leaked: ${url}`);
  } catch (error) {
    failures.push(`${url}: ${error?.message || error}`);
  }
}

try {
  const response = await fetch(websiteUrl, { redirect: 'follow' });
  if (!response.ok) failures.push(`${response.status} ${websiteUrl}`);
} catch (error) {
  failures.push(`${websiteUrl}: ${error?.message || error}`);
}

if (failures.length) {
  throw new Error(`Public lab verification failed:\n- ${failures.join('\n- ')}`);
}

console.log(`Lab URL: ${labUrl}?variant=a`);
console.log(`Variants: ${labUrl}?variant=a | ?variant=b | ?variant=c`);
console.log(`Website endpoint fallback: ${websiteUrl}`);
console.log('Public lab verification: ok');
