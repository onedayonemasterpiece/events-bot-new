import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const testsDir = dirname(fileURLToPath(import.meta.url));
const siteDir = resolve(testsDir, '..');
const read = (relative) => readFileSync(join(siteDir, relative), 'utf8');

test('Golden publication always uses its exact immutable prefix and strict public verification', () => {
  const packageJson = JSON.parse(read('package.json'));
  const wrapper = read('scripts/deploy-golden-preview.mjs');
  const deployer = read('scripts/deploy-preview-yc.mjs');

  assert.equal(packageJson.scripts['deploy:golden-preview'], 'node scripts/deploy-golden-preview.mjs');
  assert.match(wrapper, /PREVIEW_BUILD_ID/u);
  assert.match(wrapper, /\^preview-golden-/u);
  assert.match(wrapper, /buildId\.includes\('\/'\)/u);
  assert.match(wrapper, /KENIGEVENTS_SITE_REQUIRE_PUBLIC_VERIFY = '1'/u);
  assert.match(wrapper, /await import\('\.\/deploy-preview-yc\.mjs'\)/u);
  assert.ok(
    wrapper.indexOf("KENIGEVENTS_SITE_REQUIRE_PUBLIC_VERIFY = '1'")
      < wrapper.indexOf("await import('./deploy-preview-yc.mjs')"),
    'strict public verification must be enabled before the deploy module starts',
  );

  assert.ok(deployer.includes('const target = `s3://${bucket}/${buildId}/`;'));
  assert.match(deployer, /Preview-only destination prefix/u);
  assert.match(deployer, /stable s3:\/\/<bucket>\/ics\/\* objects were not modified/u);
  assert.match(deployer, /if \(requirePublicVerify\) process\.exit\(1\)/u);
  assert.doesNotMatch(wrapper, /build:production|check:production|publish:secret-candidate/u);
});

test('Golden publication refuses an implicit or non-Golden build id', () => {
  const wrapper = read('scripts/deploy-golden-preview.mjs');
  assert.match(wrapper, /requires an exact PREVIEW_BUILD_ID beginning with preview-golden-/u);
  assert.doesNotMatch(wrapper, /readdirSync|find\(\(name\).*preview-/u);
});
