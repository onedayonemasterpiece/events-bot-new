import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const siteRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const repoRoot = resolve(siteRoot, '..');
const docRoot = join(repoRoot, 'docs/features/static-site-pages/personalizaion');

test('research guard preserves to-be as target and legacy only as migration evidence', () => {
  const traceability = readFileSync(join(docRoot, 'personalization-research-traceability.md'), 'utf8');
  const delta = readFileSync(join(docRoot, 'p13n-00-research-delta.md'), 'utf8');
  assert.match(traceability, /personalization-to-be\.md.*какой должна стать система/su);
  assert.match(traceability, /legacy code → inferred product truth.*запрещён/su);
  assert.match(delta, /не является доказательством качества/u);
  assert.match(delta, /Golden personas.*`not started`/u);
  assert.match(delta, /exploration, anti-bubble.*`not started`/u);
});

test('research guard reserves target scorer and keeps legacy scorer quarantined', () => {
  assert.equal(existsSync(join(siteRoot, 'src/lib/personalization/scorer.ts')), false);
  assert.equal(existsSync(join(siteRoot, 'src/lib/personalization/model.ts')), false);
  assert.equal(existsSync(join(siteRoot, 'src/lib/personalization/legacy/scorer-v1.ts')), true);
  for (const file of ['contract.ts', 'surface-policy.ts', 'presenter-plan.ts', 'test-api.ts', 'runtime-mode.ts']) {
    const source = readFileSync(join(siteRoot, 'src/lib/personalization', file), 'utf8');
    assert.doesNotMatch(source, /legacy\/scorer-v1/u, `${file} must not import legacy scorer`);
  }
});

test('storage report documents zero Wave-0 writes and no new key', () => {
  const report = JSON.parse(readFileSync(join(siteRoot, 'src/lib/personalization/fixtures/legacy_characterization-storage-report-v1.json'), 'utf8'));
  assert.deepEqual(report.wave0_new_storage_keys, []);
  assert.equal(report.wave0_storage_writes, 0);
  assert.equal(report.profile_parser_budget_bytes, 64 * 1024);
});
