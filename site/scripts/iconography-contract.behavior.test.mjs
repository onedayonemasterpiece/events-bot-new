import assert from 'node:assert/strict';
import { mkdtemp, readFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import test from 'node:test';

const siteRoot = path.resolve(import.meta.dirname, '..');

test('inline SVG census reports rendered Astro source, not serialized source excerpts', async () => {
  const outputRoot = await mkdtemp(path.join(os.tmpdir(), 'ke-iconography-contract-'));
  const outputPath = path.join(outputRoot, 'inventory.json');
  const result = spawnSync(
    process.execPath,
    ['scripts/check-design-system-iconography-contract.mjs', '--out', outputPath],
    { cwd: siteRoot, encoding: 'utf8' },
  );

  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const report = JSON.parse(await readFile(outputPath, 'utf8'));
  const sources = report.gaps.inlineSvgOutsideCanonical.map((item) => item.source);

  assert.ok(sources.length > 0, 'the census must retain real inline Astro owners');
  assert.ok(sources.every((source) => source.endsWith('.astro')), sources.join('\n'));
  assert.ok(sources.includes('src/components/DesktopEventPage.astro'));
  assert.ok(!sources.includes('src/components/MobileBottomNav.astro'));
  assert.ok(!sources.includes('src/components/ServiceShareAction.astro'));
  assert.ok(!sources.includes('src/design-system/token-impact.generated.v1.json'));
  assert.ok(!sources.includes('src/components/design-system/f0-candidate-consumer-census.mjs'));
  assert.ok(!sources.includes('src/lib/focus-invite-qr.ts'));

  assert.ok(report.consumers.ui.spark.includes('src/components/MobileBottomNav.astro'));
  assert.ok(report.consumers.ui.check.includes('src/components/ServiceShareAction.astro'));
  assert.ok(!report.gaps.unusedUiNames.includes('spark'));
  assert.ok(!report.gaps.unusedUiNames.includes('check'));
});
