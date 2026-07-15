import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { lstatSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const MANIFEST_SCHEMA = 'static_release_manifest_v1';
const MANIFEST_FILE = 'static-release-manifest.json';
const BUILD_FILE = 'production-build.json';
const siteDir = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = resolve(siteDir, '..');
const distDir = join(siteDir, 'dist');

function runGit(args) {
  const result = spawnSync('git', args, { cwd: repoRoot, encoding: 'utf8' });
  if (result.status !== 0) throw new Error(`git ${args.join(' ')} failed: ${result.stderr.trim()}`);
  return result.stdout.trim();
}

function safeBuildId(value) {
  if (!value || !/^production-[a-zA-Z0-9][a-zA-Z0-9._-]*$/u.test(value) || value.includes('/') || value.includes('..')) {
    throw new Error(`Invalid production build id: ${value || '(empty)'}`);
  }
  return value;
}

function normalizeOrigin(value) {
  const parsed = new URL(value || 'https://kenigevents.ru');
  if (parsed.protocol !== 'https:' || parsed.username || parsed.password || parsed.search || parsed.hash || parsed.pathname !== '/') {
    throw new Error(`PUBLIC_SITE_ORIGIN must be an HTTPS origin without path/query: ${value}`);
  }
  return parsed.origin;
}

function sha256(buffer) {
  return createHash('sha256').update(buffer).digest('hex');
}

function contentType(key) {
  const ext = key.slice(key.lastIndexOf('.')).toLowerCase();
  return ({
    '.css': 'text/css; charset=utf-8',
    '.html': 'text/html; charset=utf-8',
    '.ics': 'text/calendar; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.map': 'application/json; charset=utf-8',
    '.svg': 'image/svg+xml',
    '.txt': 'text/plain; charset=utf-8',
    '.xml': 'application/xml; charset=utf-8',
    '.webp': 'image/webp',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.gif': 'image/gif',
    '.woff': 'font/woff',
    '.woff2': 'font/woff2',
  })[ext] || 'application/octet-stream';
}

function cacheControl(key) {
  if (key.startsWith('_astro/')) return 'public, max-age=31536000, immutable';
  if (key.endsWith('.html') || key === 'robots.txt' || key === 'sitemap.xml') return 'public, max-age=60, must-revalidate';
  if (key.endsWith('.json') || key.endsWith('.ics')) return 'public, max-age=300, must-revalidate';
  return 'public, max-age=3600, must-revalidate';
}

function promotionClass(key) {
  if (key.startsWith('_astro/')) return 'immutable_asset';
  if (key === 'index.html') return 'root_html';
  if (key.endsWith('.html')) return 'html';
  return 'supporting';
}

function walkFiles(root) {
  const files = [];
  function walk(dir) {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name);
      const stat = lstatSync(path);
      if (stat.isSymbolicLink()) throw new Error(`Production artifact must not contain symlinks: ${path}`);
      if (entry.isDirectory()) walk(path);
      else if (entry.isFile()) files.push(path);
      else throw new Error(`Unsupported artifact entry: ${path}`);
    }
  }
  walk(root);
  return files.sort();
}

const now = new Date();
const gitSha = runGit(['rev-parse', 'HEAD']);
if (!/^[0-9a-f]{40}$/u.test(gitSha)) throw new Error(`Invalid git SHA: ${gitSha}`);
const gitDirty = Boolean(runGit(['status', '--porcelain', '--untracked-files=normal']));
const allowDirty = ['1', 'true', 'yes', 'on'].includes(String(process.env.PRODUCTION_ALLOW_DIRTY || '').toLowerCase());
if (gitDirty && !allowDirty) throw new Error('Refusing production build from a dirty tracked worktree (set PRODUCTION_ALLOW_DIRTY=1 only for local validation)');

const stamp = now.toISOString().replace(/[-:]/gu, '').replace(/\.\d+Z$/u, 'Z').toLowerCase();
const buildId = safeBuildId(process.env.PRODUCTION_BUILD_ID || `production-${stamp}-${gitSha.slice(0, 8)}`);
const siteOrigin = normalizeOrigin(process.env.PUBLIC_SITE_ORIGIN || 'https://kenigevents.ru');
const icsBaseUrl = (process.env.PUBLIC_ICS_BASE_URL || 'https://static.kenigevents.ru/ics').replace(/\/+$/u, '');

rmSync(distDir, { recursive: true, force: true });
const env = {
  ...process.env,
  PUBLIC_SITE_MODE: 'production',
  PUBLIC_SITE_ORIGIN: siteOrigin,
  PUBLIC_ICS_BASE_URL: icsBaseUrl,
  SITE_BASE_PATH: '/',
};
delete env.PUBLIC_PREVIEW_BUILD_ID;
delete env.PUBLIC_ROOT_PREVIEW_HREF;

const astroBin = process.platform === 'win32' ? 'astro.cmd' : 'astro';
const build = spawnSync(astroBin, ['build'], {
  cwd: siteDir,
  env,
  stdio: 'inherit',
  shell: process.platform === 'win32',
});
if (build.status !== 0) process.exit(build.status || 1);

// QA-only routes can remain available in prefixed preview builds, but they are
// never part of a production-root artifact.
rmSync(join(distDir, '__preview'), { recursive: true, force: true });
rmSync(join(distDir, 'lab'), { recursive: true, force: true });

// Keep the long-standing preview root byte-compatible. Production promotes the
// real today listing to `/` and rewrites only its root SEO identity.
const todayListingPath = join(distDir, 'segodnya', 'index.html');
let productionRootHtml = readFileSync(todayListingPath, 'utf8');
function replaceRequired(source, before, after, label) {
  if (!source.includes(before)) throw new Error(`Cannot construct production root: missing ${label}`);
  return source.replace(before, after);
}
productionRootHtml = replaceRequired(
  productionRootHtml,
  `<link rel="canonical" href="${siteOrigin}/segodnya/">`,
  `<link rel="canonical" href="${siteOrigin}/">`,
  'today canonical',
);
productionRootHtml = replaceRequired(
  productionRootHtml,
  `<meta property="og:url" content="${siteOrigin}/segodnya/">`,
  `<meta property="og:url" content="${siteOrigin}/">`,
  'today Open Graph URL',
);
productionRootHtml = replaceRequired(productionRootHtml, '<main id="main">', '<main id="main" data-production-root-listing>', 'root listing marker');
writeFileSync(join(distDir, 'index.html'), productionRootHtml);

const buildMetadata = {
  schema_version: 'static_production_build_v1',
  site_mode: 'production',
  build_id: buildId,
  git_sha: gitSha,
  git_dirty: gitDirty,
  generated_at: now.toISOString(),
  site_origin: siteOrigin,
  base_path: '/',
  ics_base_url: icsBaseUrl,
  validation_contract: 'check-production-v1',
};
writeFileSync(join(distDir, BUILD_FILE), `${JSON.stringify(buildMetadata, null, 2)}\n`);

const files = walkFiles(distDir)
  .filter((path) => relative(distDir, path).split(sep).join('/') !== MANIFEST_FILE)
  .map((path) => {
    const key = relative(distDir, path).split(sep).join('/');
    const bytes = readFileSync(path);
    return {
      key,
      sha256: sha256(bytes),
      size: bytes.length,
      content_type: contentType(key),
      cache_control: cacheControl(key),
      promotion_class: promotionClass(key),
    };
  });

const eventsData = JSON.parse(readFileSync(join(siteDir, 'src', 'data', 'preview-events.json'), 'utf8'));
const eventIdBySlug = new Map((eventsData.events || []).map((event) => [String(event.slug), Number(event.id)]));
const stableIcs = files.flatMap((file) => {
  const match = /^sobytiya\/([^/]+)\/event\.ics$/u.exec(file.key);
  if (!match) return [];
  const suffixId = /-(\d+)$/u.exec(match[1])?.[1];
  const eventId = eventIdBySlug.get(match[1]) || (suffixId ? Number(suffixId) : 0);
  if (!Number.isSafeInteger(eventId) || eventId <= 0) throw new Error(`Cannot resolve stable ICS event id for ${file.key}`);
  return [{ event_id: eventId, source_key: file.key, target_key: `ics/${eventId}.ics`, sha256: file.sha256 }];
});

const manifest = {
  schema_version: MANIFEST_SCHEMA,
  site_mode: 'production',
  build_id: buildId,
  git_sha: gitSha,
  git_dirty: gitDirty,
  generated_at: now.toISOString(),
  site_origin: siteOrigin,
  base_path: '/',
  hash_algorithm: 'sha256',
  manifest_file: MANIFEST_FILE,
  build_metadata_file: BUILD_FILE,
  managed_root_keys: files.map((file) => file.key),
  immutable_asset_keys: files.filter((file) => file.promotion_class === 'immutable_asset').map((file) => file.key),
  stable_ics: stableIcs,
  protected_bucket_prefixes: ['ics/', '_static/releases/', 'preview-'],
  files,
};
writeFileSync(join(distDir, MANIFEST_FILE), `${JSON.stringify(manifest, null, 2)}\n`);

const check = spawnSync(process.execPath, [join(siteDir, 'scripts', 'check-production.mjs')], {
  cwd: siteDir,
  env: { ...process.env, PRODUCTION_BUILD_ID: buildId },
  stdio: 'inherit',
});
if (check.status !== 0) process.exit(check.status || 1);

console.log(`Checked production artifact ready: dist/ (${files.length} managed root keys)`);
console.log(`Build id: ${buildId}`);
console.log(`Git SHA: ${gitSha}${gitDirty ? ' (dirty local validation only)' : ''}`);
