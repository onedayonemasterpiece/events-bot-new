import { createHash, randomBytes } from 'node:crypto';
import { existsSync, lstatSync, readFileSync, readdirSync } from 'node:fs';
import { join, relative, sep } from 'node:path';

export const RELEASE_MANIFEST_SCHEMA = 'static_release_manifest_v1';
export const CANDIDATE_MANIFEST_SCHEMA = 'static_secret_candidate_manifest_v1';
export const CATALOG_LEDGER_SCHEMA = 'static_event_catalog_ledger_v1';
export const ELIGIBILITY_PREDICATE_VERSION = 'static_event_public_projection_v2';
export const CHECK_CONTRACT_VERSION = 'static-production-check-v2';
export const REVIEW_PREFIX = '_review';

export function sha256(value) {
  return createHash('sha256').update(value).digest('hex');
}

export function safeBuildId(value) {
  if (!/^production-[a-zA-Z0-9][a-zA-Z0-9._-]*$/u.test(value || '') || value.includes('/') || value.includes('..')) {
    throw new Error(`Invalid production build id: ${value || '(empty)'}`);
  }
  return value;
}

export function safeRunId(value) {
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._:-]{7,159}$/u.test(value || '') || value.includes('..')) {
    throw new Error(`Invalid static-site run id: ${value || '(empty)'}`);
  }
  return value;
}

export function safeCandidateToken(value) {
  if (!/^[A-Za-z0-9_-]{43}$/u.test(value || '')) throw new Error('Candidate token must be one 256-bit base64url value');
  return value;
}

export function generateCandidateToken() {
  return safeCandidateToken(randomBytes(32).toString('base64url'));
}

export function candidateBasePath(token) {
  return `/${REVIEW_PREFIX}/${safeCandidateToken(token)}`;
}

export function assertSafeRelativeKey(key) {
  if (typeof key !== 'string' || !key || key.startsWith('/') || key.includes('\\') || key.split('/').some((part) => !part || part === '.' || part === '..')) {
    throw new Error(`Unsafe artifact key: ${JSON.stringify(key)}`);
  }
}

export function assertCandidateObjectKey(key, token) {
  assertSafeRelativeKey(key);
  const prefix = `${REVIEW_PREFIX}/${safeCandidateToken(token)}/`;
  if (!key.startsWith(prefix)) throw new Error(`Secret candidate publisher refuses out-of-prefix key: ${key}`);
  const suffix = key.slice(prefix.length);
  if (!suffix || /^(?:_static|ics)(?:\/|$)/u.test(suffix)) throw new Error(`Secret candidate publisher refuses protected key: ${key}`);
  if (/^(?:current|previous|promotion-lease)\.json$/u.test(suffix)) throw new Error(`Secret candidate publisher refuses release-control key: ${key}`);
  return key;
}

export function readJson(path, label = path) {
  try {
    return JSON.parse(readFileSync(path, 'utf8'));
  } catch (error) {
    throw new Error(`${label} is not valid JSON: ${error.message}`);
  }
}

export function walkFiles(root) {
  const files = [];
  function walk(dir) {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name);
      const stat = lstatSync(path);
      if (stat.isSymbolicLink()) throw new Error(`Artifact must not contain symlinks: ${path}`);
      if (entry.isDirectory()) walk(path);
      else if (entry.isFile()) files.push(path);
      else throw new Error(`Unsupported artifact entry: ${path}`);
    }
  }
  walk(root);
  return files.sort();
}

export function contentType(key) {
  const ext = key.includes('.') ? key.slice(key.lastIndexOf('.')).toLowerCase() : '';
  return ({
    '.css': 'text/css; charset=utf-8', '.html': 'text/html; charset=utf-8',
    '.ics': 'text/calendar; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
    '.json': 'application/json; charset=utf-8', '.map': 'application/json; charset=utf-8',
    '.svg': 'image/svg+xml', '.txt': 'text/plain; charset=utf-8', '.xml': 'application/xml; charset=utf-8',
    '.webp': 'image/webp', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
    '.gif': 'image/gif', '.woff': 'font/woff', '.woff2': 'font/woff2',
  })[ext] || 'application/octet-stream';
}

export function cacheControl(key, { secretCandidate = false } = {}) {
  if (secretCandidate) return 'private, no-store, max-age=0';
  if (key.startsWith('_astro/')) return 'public, max-age=31536000, immutable';
  if (key.endsWith('.html') || key === 'robots.txt' || key === 'sitemap.xml') return 'public, max-age=60, must-revalidate';
  if (key.endsWith('.json') || key.endsWith('.ics')) return 'public, max-age=300, must-revalidate';
  return 'public, max-age=3600, must-revalidate';
}

export function fileInventory(root, { exclude = [], secretCandidate = false } = {}) {
  const excluded = new Set(exclude);
  return walkFiles(root).flatMap((path) => {
    const key = relative(root, path).split(sep).join('/');
    if (excluded.has(key)) return [];
    assertSafeRelativeKey(key);
    const bytes = readFileSync(path);
    return [{ key, sha256: sha256(bytes), size: bytes.length, content_type: contentType(key), cache_control: cacheControl(key, { secretCandidate }) }];
  });
}

export function treeHash(files) {
  const payload = [...files]
    .sort((a, b) => a.key.localeCompare(b.key))
    .map((file) => `${file.key}\0${file.sha256}\0${file.size}\n`)
    .join('');
  return sha256(payload);
}

export function validateCatalogLedger(catalog, expected = {}) {
  if (!catalog || catalog.schema_version !== CATALOG_LEDGER_SCHEMA) throw new Error(`Catalog ledger must use ${CATALOG_LEDGER_SCHEMA}`);
  if (catalog.eligibility_predicate_version !== ELIGIBILITY_PREDICATE_VERSION) throw new Error(`Catalog ledger must use ${ELIGIBILITY_PREDICATE_VERSION}`);
  if (!Array.isArray(catalog.eligible) || !Array.isArray(catalog.excluded)) throw new Error('Catalog ledger lists are missing');
  if (catalog.eligible_count !== catalog.eligible.length || catalog.excluded_count !== catalog.excluded.length) throw new Error('Catalog ledger counts disagree');
  const ids = catalog.eligible.map((item) => Number(item.event_id));
  if (ids.some((id) => !Number.isSafeInteger(id) || id <= 0) || new Set(ids).size !== ids.length) throw new Error('Catalog eligible ids are invalid or duplicated');
  const excludedIds = catalog.excluded.map((item) => Number(item.event_id));
  if (excludedIds.some((id) => !Number.isSafeInteger(id) || id <= 0) || new Set(excludedIds).size !== excludedIds.length) throw new Error('Catalog excluded ids are invalid or duplicated');
  if (excludedIds.some((id) => ids.includes(id))) throw new Error('Catalog eligible/excluded sets overlap');
  if (catalog.excluded.some((item) => typeof item.reason !== 'string' || !/^[a-z0-9_:.-]{2,120}$/u.test(item.reason))) throw new Error('Catalog exclusion reason is invalid');
  if (!/^[0-9a-f]{40}$/u.test(catalog.repo_sha || '')) throw new Error('Catalog repo_sha must be a full SHA');
  if (!/^[0-9a-f]{64}$/u.test(catalog.snapshot?.sha256 || '')) throw new Error('Catalog snapshot SHA-256 is invalid');
  for (const [key, value] of Object.entries(expected)) {
    const actual = key.startsWith('snapshot.') ? catalog.snapshot?.[key.slice(9)] : catalog[key];
    if (value !== undefined && value !== null && String(actual) !== String(value)) throw new Error(`Catalog ${key} does not match: ${actual} != ${value}`);
  }
  return { catalog, eligibleIds: ids };
}

export function pageCounts(files, eventCount) {
  const html = files.filter((file) => file.key.endsWith('.html'));
  return {
    event_count: eventCount,
    event_page_count: files.filter((file) => /^sobytiya\/[^/]+\/index\.html$/u.test(file.key)).length,
    html_count: html.length,
    page_count: html.length + files.filter((file) => /(?:\.json|\.xml|\.txt|\.ics)$/u.test(file.key)).length,
    file_count: files.length,
    bytes: files.reduce((sum, file) => sum + file.size, 0),
  };
}

export function assertFileInventory(root, files) {
  const actual = fileInventory(root);
  const byKey = new Map(files.map((file) => [file.key, file]));
  if (actual.length !== files.length || byKey.size !== files.length) throw new Error('Manifest file inventory count/duplicates mismatch');
  for (const item of actual) {
    const expected = byKey.get(item.key);
    if (!expected || expected.sha256 !== item.sha256 || expected.size !== item.size) throw new Error(`Manifest file mismatch: ${item.key}`);
  }
}

export function objectExists(path) {
  return existsSync(path) && lstatSync(path).isFile();
}
