import { execFileSync } from 'node:child_process';
import { copyFileSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const siteDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const gitRoot = path.resolve(siteDir, '..');
const sha = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: gitRoot, encoding: 'utf8' }).trim();
const buildId = process.env.PREVIEW_BUILD_ID || `briefing-lab-${sha.slice(0, 12)}`;
if (!/^[a-z0-9][a-z0-9._-]{0,63}$/u.test(buildId) || buildId.includes('..')) {
  throw new Error(`Unsafe PREVIEW_BUILD_ID: ${JSON.stringify(buildId)}`);
}
const outputDir = path.join(siteDir, 'dist-lab', buildId);
rmSync(outputDir, { recursive: true, force: true });
mkdirSync(outputDir, { recursive: true });
const env = {
  ...process.env,
  LAB_BUILD_ID: buildId,
  PUBLIC_PREVIEW_BUILD_ID: buildId,
  PUBLIC_SITE_ORIGIN: process.env.PUBLIC_SITE_ORIGIN || 'http://127.0.0.1:4177',
  PUBLIC_PERSONALIZATION_SUPABASE_URL: '',
  PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY: '',
};
execFileSync(process.execPath, [path.join(siteDir, 'node_modules/astro/bin/astro.mjs'), 'build', '--config', 'astro.lab.config.mjs'], {
  cwd: siteDir,
  env,
  stdio: 'inherit',
});
const htmlPath = path.join(outputDir, 'lab/briefing/index.html');
let html = readFileSync(htmlPath, 'utf8');
for (const route of ['segodnya', 'zavtra', 'vyhodnye', 'vystavki', 'populyarnoe', 'poisk', 'partners', '__preview', 'partnerstvo']) {
  html = html.replaceAll(`href="/${buildId}/${route}/`, `href="/${route}/`);
}
html = html.replaceAll(`"/${buildId}/sobytiya/`, '"/sobytiya/');
writeFileSync(htmlPath, html);
mkdirSync(path.join(outputDir, 'brand'), { recursive: true });
copyFileSync(path.join(siteDir, 'public/favicon.svg'), path.join(outputDir, 'favicon.svg'));
copyFileSync(path.join(siteDir, 'public/brand/announcements-wordmark-ui.svg'), path.join(outputDir, 'brand/announcements-wordmark-ui.svg'));
const manifest = {
  kind: 'briefing-lab',
  buildId,
  gitSha: sha,
  generatedAt: new Date().toISOString(),
  publicPath: `/${buildId}/lab/briefing/`,
};
writeFileSync(path.join(outputDir, 'lab-manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`);
console.log(`Briefing lab: ${outputDir}`);
