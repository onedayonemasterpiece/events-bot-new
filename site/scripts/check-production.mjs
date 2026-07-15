import { createHash } from 'node:crypto';
import { existsSync, lstatSync, readFileSync, readdirSync } from 'node:fs';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import eventsData from '../src/data/preview-events.json' with { type: 'json' };

const MANIFEST_SCHEMA = 'static_release_manifest_v1';
const MANIFEST_FILE = 'static-release-manifest.json';
const BUILD_FILE = 'production-build.json';
const siteDir = dirname(dirname(fileURLToPath(import.meta.url)));
const distDir = join(siteDir, 'dist');

function fail(message) {
  throw new Error(`Production check failed: ${message}`);
}

function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

function walkFiles(root) {
  const out = [];
  function walk(dir) {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name);
      if (lstatSync(path).isSymbolicLink()) fail(`symlink is forbidden: ${path}`);
      if (entry.isDirectory()) walk(path);
      else if (entry.isFile()) out.push(relative(root, path).split(sep).join('/'));
      else fail(`unsupported filesystem entry: ${path}`);
    }
  }
  walk(root);
  return out.sort();
}

function readJson(path, label) {
  try {
    return JSON.parse(readFileSync(path, 'utf8'));
  } catch (error) {
    fail(`${label} is not valid JSON: ${error.message}`);
  }
}

function assertSafeKey(key) {
  if (typeof key !== 'string' || !key || key.startsWith('/') || key.includes('\\') || key.split('/').some((part) => !part || part === '.' || part === '..')) {
    fail(`unsafe managed key: ${JSON.stringify(key)}`);
  }
  if (/^(?:__preview|lab|ics)(?:\/|$)/u.test(key) || /^preview-[^/]+\//u.test(key) || key.startsWith('_static/releases/')) {
    fail(`protected/QA key cannot be managed at production root: ${key}`);
  }
}

function expectedCanonicalForHtmlKey(key, siteOrigin) {
  if (key === 'index.html') return `${siteOrigin}/`;
  if (!key.endsWith('/index.html')) return null;
  return `${siteOrigin}/${key.slice(0, -'index.html'.length)}`;
}

if (!existsSync(distDir)) fail('dist/ does not exist; run npm run build:production');
const manifestPath = join(distDir, MANIFEST_FILE);
const buildPath = join(distDir, BUILD_FILE);
if (!existsSync(manifestPath)) fail(`${MANIFEST_FILE} is missing`);
if (!existsSync(buildPath)) fail(`${BUILD_FILE} is missing`);

const manifest = readJson(manifestPath, MANIFEST_FILE);
const build = readJson(buildPath, BUILD_FILE);
if (manifest.schema_version !== MANIFEST_SCHEMA) fail(`manifest schema must be ${MANIFEST_SCHEMA}`);
if (manifest.site_mode !== 'production' || build.site_mode !== 'production') fail('artifact site_mode must be production');
if (manifest.base_path !== '/' || build.base_path !== '/') fail('production base_path must be /');
if (manifest.hash_algorithm !== 'sha256') fail('hash_algorithm must be sha256');
if (!/^production-[a-zA-Z0-9][a-zA-Z0-9._-]*$/u.test(manifest.build_id || '')) fail('invalid production build_id');
if (process.env.PRODUCTION_BUILD_ID && process.env.PRODUCTION_BUILD_ID !== manifest.build_id) fail('PRODUCTION_BUILD_ID does not match artifact manifest');
if (!/^[0-9a-f]{40}$/u.test(manifest.git_sha || '')) fail('manifest git_sha must be a full commit SHA');
if (manifest.git_sha !== build.git_sha || manifest.build_id !== build.build_id || manifest.site_origin !== build.site_origin) fail('build metadata does not match release manifest');
if (manifest.git_dirty || build.git_dirty) {
  const allowDirty = ['1', 'true', 'yes', 'on'].includes(String(process.env.PRODUCTION_ALLOW_DIRTY || '').toLowerCase());
  if (!allowDirty) fail('dirty-build metadata is never publishable');
}
const siteOrigin = new URL(manifest.site_origin).origin;
if (siteOrigin !== manifest.site_origin || !siteOrigin.startsWith('https://')) fail('site_origin must be an HTTPS origin');

if (!Array.isArray(manifest.files) || !manifest.files.length) fail('manifest files must be non-empty');
if (!Array.isArray(manifest.managed_root_keys) || !manifest.managed_root_keys.length) fail('managed_root_keys must be non-empty');
const fileByKey = new Map();
for (const file of manifest.files) {
  assertSafeKey(file?.key);
  if (fileByKey.has(file.key)) fail(`duplicate file key: ${file.key}`);
  if (!/^[0-9a-f]{64}$/u.test(file.sha256 || '')) fail(`invalid sha256 for ${file.key}`);
  if (!Number.isSafeInteger(file.size) || file.size < 0) fail(`invalid size for ${file.key}`);
  if (!file.content_type || !file.cache_control || !['immutable_asset', 'supporting', 'html', 'root_html'].includes(file.promotion_class)) fail(`incomplete publication metadata for ${file.key}`);
  const path = join(distDir, file.key);
  if (!existsSync(path) || !lstatSync(path).isFile()) fail(`manifest file missing from artifact: ${file.key}`);
  const bytes = readFileSync(path);
  if (bytes.length !== file.size || sha256(bytes) !== file.sha256) fail(`hash/size mismatch: ${file.key}`);
  fileByKey.set(file.key, file);
}

const managedKeys = manifest.managed_root_keys;
if (new Set(managedKeys).size !== managedKeys.length) fail('managed_root_keys contains duplicates');
for (const key of managedKeys) {
  assertSafeKey(key);
  if (!fileByKey.has(key)) fail(`managed_root_key has no file record: ${key}`);
}
if (managedKeys.length !== fileByKey.size || [...fileByKey.keys()].some((key) => !managedKeys.includes(key))) fail('managed_root_keys must exactly match files[]');

const diskFiles = walkFiles(distDir);
const expectedDiskFiles = [...fileByKey.keys(), MANIFEST_FILE].sort();
if (diskFiles.length !== expectedDiskFiles.length || diskFiles.some((key, index) => key !== expectedDiskFiles[index])) {
  fail('artifact contains unmanifested or missing files');
}

const required = [
  'index.html',
  'segodnya/index.html',
  'zavtra/index.html',
  'vyhodnye/index.html',
  'vystavki/index.html',
  'populyarnoe/index.html',
  'poisk/index.html',
  'robots.txt',
  'sitemap.xml',
  BUILD_FILE,
  ...eventsData.events.flatMap((event) => [
    `sobytiya/${event.slug}/index.html`,
    `sobytiya/${event.slug}/event.ics`,
    `data/discovery/${event.id}.json`,
  ]),
];
for (const key of required) if (!fileByKey.has(key)) fail(`required production file missing: ${key}`);

const htmlKeys = [...fileByKey.keys()].filter((key) => key.endsWith('.html'));
for (const key of htmlKeys) {
  const html = readFileSync(join(distDir, key), 'utf8');
  if (/<meta\s+name=["']robots["']\s+content=["'][^"']*noindex/iu.test(html) || /<meta\s+content=["'][^"']*noindex[^"']*["']\s+name=["']robots["']/iu.test(html)) fail(`noindex leaked into ${key}`);
  if (!/<meta\s+name=["']robots["']\s+content=["']index,follow["']/iu.test(html)) fail(`index,follow robots meta missing from ${key}`);
  if (html.includes('/__preview/') || html.includes('noindex,nofollow,noarchive')) fail(`preview route/policy leaked into ${key}`);
  const expectedCanonical = expectedCanonicalForHtmlKey(key, siteOrigin);
  if (expectedCanonical && !html.includes(`<link rel="canonical" href="${expectedCanonical}">`)) fail(`wrong or missing root canonical in ${key}: ${expectedCanonical}`);
}
const rootHtml = readFileSync(join(distDir, 'index.html'), 'utf8');
if (!rootHtml.includes('data-production-root-listing')) fail('root page is not the production event listing');

const robotsExpected = `User-agent: *\nAllow: /\nSitemap: ${siteOrigin}/sitemap.xml\n`;
if (readFileSync(join(distDir, 'robots.txt'), 'utf8') !== robotsExpected) fail('robots.txt is not the production allow/sitemap policy');

const sitemap = readFileSync(join(distDir, 'sitemap.xml'), 'utf8');
const sitemapLocs = [...sitemap.matchAll(/<loc>([^<]+)<\/loc>/gu)].map((match) => match[1]);
if (!sitemapLocs.length || new Set(sitemapLocs).size !== sitemapLocs.length) fail('sitemap is empty or contains duplicate URLs');
const requiredSitemapUrls = [
  `${siteOrigin}/`,
  ...['segodnya', 'zavtra', 'vyhodnye', 'vystavki', 'populyarnoe', 'poisk', 'partners'].map((path) => `${siteOrigin}/${path}/`),
  ...eventsData.events.map((event) => `${siteOrigin}/sobytiya/${event.slug}/`),
];
for (const url of requiredSitemapUrls) if (!sitemapLocs.includes(url)) fail(`required URL missing from sitemap: ${url}`);
for (const url of sitemapLocs) {
  if (!url.startsWith(`${siteOrigin}/`)) fail(`off-origin sitemap URL: ${url}`);
  if (/\/(?:__preview|lab)(?:\/|$)/u.test(url) || /\/preview-[^/]+\//u.test(url) || url === `${siteOrigin}/partnerstvo/`) fail(`QA/preview URL leaked into sitemap: ${url}`);
}

if (!Array.isArray(manifest.stable_ics) || !manifest.stable_ics.length) fail('stable_ics mapping is missing');
const stableTargets = new Set();
for (const item of manifest.stable_ics) {
  if (!Number.isSafeInteger(item.event_id) || item.event_id <= 0 || item.target_key !== `ics/${item.event_id}.ics`) fail('invalid stable ICS mapping');
  if (stableTargets.has(item.target_key)) fail(`duplicate stable ICS target: ${item.target_key}`);
  stableTargets.add(item.target_key);
  const source = fileByKey.get(item.source_key);
  if (!source || source.sha256 !== item.sha256 || source.content_type !== 'text/calendar; charset=utf-8') fail(`invalid stable ICS source: ${item.source_key}`);
}
if (manifest.managed_root_keys.some((key) => key.startsWith('ics/'))) fail('stable /ics must not be part of the managed-root deletion set');

console.log(`Production check passed: ${manifest.build_id}, ${fileByKey.size} managed root files, ${manifest.stable_ics.length} stable ICS mappings`);
