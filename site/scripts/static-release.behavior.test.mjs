import assert from 'node:assert/strict';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import {
  assertCandidateObjectKey, candidateBasePath, generateCandidateToken, safeCandidateToken,
  cacheControl, contentType, treeHash, validateCatalogLedger,
} from './release-contract.mjs';
import { assertAnonymousListDisabled, publicationObjects } from './deploy-secret-candidate-yc.mjs';

test('ADD-BUILD-07 production/preview/candidate profiles use an explicit 256-bit review path', () => {
  const token = generateCandidateToken();
  assert.equal(token.length, 43);
  assert.equal(safeCandidateToken(token), token);
  assert.equal(candidateBasePath(token), `/_review/${token}`);
  assert.throws(() => safeCandidateToken('timestamp-build-id'));
});

test('production and candidate root aliases preserve listing classes while adding release markers', () => {
  const production = readFileSync(new URL('./build-production.mjs', import.meta.url), 'utf8');
  const candidate = readFileSync(new URL('./build-secret-candidate.mjs', import.meta.url), 'utf8');
  assert.match(production, /replaceRequired\(rootHtml, '<main id="main"', '<main id="main" data-production-root-listing'/u);
  assert.match(candidate, /replace\('<main id="main"', '<main id="main" data-secret-candidate-root-listing'/u);
  assert.doesNotMatch(production, /'<main id="main">'/u);
  assert.doesNotMatch(candidate, /'<main id="main">'/u);
});

test('production and candidate builds forward only normalized browser-safe search config', () => {
  const production = readFileSync(new URL('./build-production.mjs', import.meta.url), 'utf8');
  const candidate = readFileSync(new URL('./build-secret-candidate.mjs', import.meta.url), 'utf8');
  for (const source of [production, candidate]) {
    assert.match(source, /loadPreviewPublicConfig\(siteDir, process\.env\)/u);
    assert.match(source, /\.\.\.publicSearchConfig\.values/u);
    assert.doesNotMatch(source, /PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY:\s*''/u);
  }
});

test('production and secret-candidate profiles retain the production-ready partnership route', () => {
  const production = readFileSync(new URL('./build-production.mjs', import.meta.url), 'utf8');
  const candidate = readFileSync(new URL('./build-secret-candidate.mjs', import.meta.url), 'utf8');
  const productionCheck = readFileSync(new URL('./check-production.mjs', import.meta.url), 'utf8');
  const candidateCheck = readFileSync(new URL('./check-secret-candidate.mjs', import.meta.url), 'utf8');
  for (const source of [production, candidate]) {
    assert.doesNotMatch(source, /rmSync\(join\(distDir, 'partnerstvo', 'index\.html'\)/u);
  }
  assert.match(productionCheck, /'partnerstvo\/index\.html'/u);
  assert.match(candidateCheck, /source\('partnerstvo\/index\.html'\)/u);
});

test('secret-candidate robots policy overrides page-local noindex without losing nosnippet', () => {
  const layout = readFileSync(new URL('../src/layouts/EventLayout.astro', import.meta.url), 'utf8');
  assert.match(layout, /const robots = IS_SECRET_CANDIDATE\s*\?\s*'noindex,nofollow,noarchive,nosnippet'/u);
});

test('ADD-BUILD-09 catalog ledger rejects duplicate, missing, or unversioned eligibility evidence', () => {
  const base = {
    schema_version: 'static_event_catalog_ledger_v1', repo_sha: 'a'.repeat(40), run_id: 'static:run:123', build_id: 'production-test',
    eligibility_predicate_version: 'static_event_public_projection_v2',
    snapshot: { snapshot_id: 'snap-1', sha256: 'b'.repeat(64) }, eligible_count: 1, excluded_count: 0,
    eligible: [{ event_id: 7 }], excluded: [],
  };
  assert.deepEqual(validateCatalogLedger(base).eligibleIds, [7]);
  assert.throws(() => validateCatalogLedger({ ...base, eligible_count: 2 }));
  assert.throws(() => validateCatalogLedger({ ...base, eligible: [{ event_id: 7 }, { event_id: 7 }], eligible_count: 2 }));
  assert.throws(() => validateCatalogLedger({ ...base, eligibility_predicate_version: undefined }));
  assert.throws(() => validateCatalogLedger({ ...base, excluded: [{ event_id: 7, reason: 'silent' }], excluded_count: 1 }));
});

test('ADD-BUILD-09 tree hash is order-independent and content-addressed', () => {
  const left = [{ key: 'b', sha256: '2'.repeat(64), size: 2 }, { key: 'a', sha256: '1'.repeat(64), size: 1 }];
  assert.equal(treeHash(left), treeHash([...left].reverse()));
  assert.notEqual(treeHash(left), treeHash([{ ...left[0], size: 3 }, left[1]]));
});

test('PWA manifest publishes with installable MIME and bounded revalidation', () => {
  assert.equal(contentType('manifest.webmanifest'), 'application/manifest+json; charset=utf-8');
  assert.equal(cacheControl('manifest.webmanifest'), 'public, max-age=300, must-revalidate');
});

test('ADD-BUILD-12 capacity/privacy preflight refuses public listing and accepts only AccessDenied', () => {
  assert.throws(() => assertAnonymousListDisabled({ status: 200, body: '<ListBucketResult />' }), /anonymous ListObjects/u);
  assert.throws(() => assertAnonymousListDisabled({ status: 500, body: 'unknown' }), /Cannot prove/u);
  assert.equal(assertAnonymousListDisabled({ status: 403, body: 'AccessDenied' }), true);
});

test('ADD-BUILD-10 ADD-BUILD-11 publisher is create-only, prefix-contained, MIME/cache-bound and has no root/current/stable ICS targets', () => {
  const token = generateCandidateToken();
  const dir = mkdtempSync(join(tmpdir(), 'candidate-publisher-test-'));
  mkdirSync(join(dir, 'sobytiya/e'), { recursive: true });
  writeFileSync(join(dir, 'index.html'), '<html></html>');
  writeFileSync(join(dir, 'sobytiya/e/event.ics'), 'BEGIN:VCALENDAR');
  writeFileSync(join(dir, 'secret-candidate-manifest.json'), '{}');
  const objects = publicationObjects(dir, {
    site_mode: 'secret_candidate', publication_mode: 'secret_link',
    checks: {
      candidate_contract: 'ok', catalog_parity: 'ok', noindex: 'ok', no_referrer: 'ok',
      prefix_containment: 'ok', root_isolation: 'ok',
    },
    files: [
      { key: 'index.html', sha256: '1'.repeat(64), size: 13, content_type: 'text/html; charset=utf-8' },
      { key: 'sobytiya/e/event.ics', sha256: '2'.repeat(64), size: 15, content_type: 'text/calendar; charset=utf-8' },
    ],
  }, token);
  assert(objects.every((item) => item.object_key.startsWith(`_review/${token}/`)));
  assert(objects.every((item) => !/^ics\//u.test(item.object_key) && !item.object_key.includes('/current.json')));
  assert(objects.every((item) => item.cache_control === 'private, no-store, max-age=0'));
  assert.throws(() => publicationObjects(dir, {
    site_mode: 'secret_candidate', publication_mode: 'secret_link', checks: {}, files: [],
  }, token), /unchecked secret candidate/u);
  assert.throws(() => assertCandidateObjectKey('index.html', token));
  assert.throws(() => assertCandidateObjectKey(`_review/${token}/ics/7.ics`, token));
  assert.throws(() => assertCandidateObjectKey(`_review/${token}/current.json`, token));
});
