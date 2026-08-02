import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

test('Appium adapter uses real browser capabilities and ordinary digit input without raw native hierarchy', async () => {
  const source = await readFile(new URL('../../e2e/focus-email/adapters/appium-ui.mjs', import.meta.url), 'utf8');
  assert.match(source, /browserName: 'Chrome'/u);
  assert.match(source, /browserName: 'Safari'/u);
  assert.match(source, /UiAutomator2/u);
  assert.match(source, /XCUITest/u);
  assert.match(source, /for \(const digit of value\).*addValue\(digit\)/su);
  assert.doesNotMatch(source, /getPageSource|pageSource|input\.value\s*=\s*value/u);
});

test('immutable preview metadata records the full repository SHA used by the E2E gate', async () => {
  const source = await readFile(new URL('../../scripts/build-preview.mjs', import.meta.url), 'utf8');
  assert.match(source, /repo_sha: gitFullSha\(\)/u);
  assert.match(source, /git.*rev-parse.*HEAD/su);
});
