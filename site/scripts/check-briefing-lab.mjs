import { execFileSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const siteDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const sha = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: path.resolve(siteDir, '..'), encoding: 'utf8' }).trim();
const root = path.join(siteDir, 'dist-lab');
const buildId = process.env.PREVIEW_BUILD_ID || `briefing-lab-${sha.slice(0, 12)}`;
if (!/^[a-z0-9][a-z0-9._-]{0,63}$/u.test(buildId) || buildId.includes('..')) throw new Error('Unsafe PREVIEW_BUILD_ID');
const buildRoot = path.join(root, buildId);
if (!existsSync(buildRoot)) throw new Error(`Missing lab build: ${buildRoot}`);

const files = [];
const visit = (dir) => readdirSync(dir).forEach((name) => {
  const target = path.join(dir, name);
  if (statSync(target).isDirectory()) visit(target);
  else files.push(path.relative(buildRoot, target).split(path.sep).join('/'));
});
visit(buildRoot);
const allowed = (name) => name === 'lab/briefing/index.html' || name === 'lab-manifest.json' || name === 'favicon.svg' || name === 'brand/announcements-wordmark-ui.svg' || name.startsWith('_astro/');
const rejected = files.filter((name) => !allowed(name));
if (rejected.length) throw new Error(`Unexpected lab artifacts:\n${rejected.join('\n')}`);
for (const forbidden of ['sobytiya/', 'ics/', '__preview/', 'sitemap', 'robots']) {
  if (files.some((name) => name.startsWith(forbidden) || name.includes(forbidden))) {
    throw new Error(`Forbidden artifact emitted: ${forbidden}`);
  }
}
if (files.includes('index.html')) throw new Error('Forbidden root index.html emitted');
if (!files.includes('lab/briefing/index.html')) throw new Error('Missing lab/briefing/index.html');
const manifest = JSON.parse(readFileSync(path.join(buildRoot, 'lab-manifest.json'), 'utf8'));
if (manifest.kind !== 'briefing-lab' || manifest.buildId !== buildId || manifest.gitSha !== sha || !manifest.generatedAt) {
  throw new Error('Invalid lab manifest');
}
const html = readFileSync(path.join(buildRoot, 'lab/briefing/index.html'), 'utf8');
if (!html.includes('data-briefing-lab') || /<[^>]+data-personal-feed-slot/iu.test(html)) throw new Error('Lab page contract missing or personalization slot leaked');
for (const route of ['segodnya', 'zavtra', 'vyhodnye', 'vystavki', 'populyarnoe']) {
  if (html.includes(`href="/${buildId}/${route}/`)) throw new Error(`Production navigation was incorrectly versioned: ${route}`);
}
if (html.includes(`"/${buildId}/sobytiya/`)) throw new Error('Production event links were incorrectly versioned');
for (const asset of [`/${buildId}/_astro/`, `/${buildId}/favicon.svg`, `/${buildId}/brand/announcements-wordmark-ui.svg`]) {
  if (!html.includes(asset)) throw new Error(`Versioned lab asset URL missing: ${asset}`);
}
console.log(`Briefing lab allowlist OK (${files.length} files): ${buildId}`);
