import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { resolve } from 'node:path';
import { spawnSync } from 'node:child_process';

const root = resolve(import.meta.dirname, '..');
const workflow = readFileSync(resolve(root, '.github/workflows/ui-three-way-conformance.yml'), 'utf8');
for (const path of ['site/src/components/**', 'site/src/layouts/**', 'site/src/styles/**', 'site/src/data/preview-events.json', 'scripts/ui_conformance/**']) assert(workflow.includes(path));
assert(workflow.includes('workflow_dispatch'));
assert(workflow.includes("github.event_name == 'workflow_dispatch'"), 'Telegram must remain off for ordinary pull requests');
assert(workflow.includes('trusted_source:'));

assert(readFileSync(resolve(root, 'AGENTS.md'), 'utf8').includes('ui-three-way-conformance'), 'root AGENTS must route material visual changes');

const productionDiff = spawnSync('git', ['diff', '--name-only', '22ebe3c5e92b13684cca32c14357ef7b91834977', '--', 'site/src'], { cwd: root, encoding: 'utf8' });
assert.equal(productionDiff.status, 0, productionDiff.stderr);
assert.equal(productionDiff.stdout.trim(), '', 'UI conformance implementation must not mutate production UI source');

// Refreshing an existing corpus version must fail before any payload write.
const scratch = mkdtempSync(join(tmpdir(), 'ui-corpus-immutability-'));
const design = join(scratch, 'design'); const target = join(design, 'catalog/fixtures/ui-reference-events/v1');
mkdirSync(target, { recursive: true }); writeFileSync(join(target, 'sentinel.txt'), 'unchanged\n');
const preview = join(scratch, 'preview.json'); writeFileSync(preview, '{"events":[]}\n');
const cache = join(scratch, 'cache'); mkdirSync(cache);
const before = readFileSync(join(target, 'sentinel.txt'), 'utf8');
const immutable = spawnSync('python3', [resolve(root, 'scripts/ui_conformance/extract-reference-corpus.py'),
  '--preview-events', preview, '--design-system-root', design, '--version', 'v1', '--event-ids', '1',
  '--current-date', '2026-08-21', '--reference-iso', '2026-08-21T09:00:00+02:00', '--timezone', 'Europe/Kaliningrad',
  '--source-repository-sha', 'a'.repeat(40), '--source-static-build-id', 'test',
  '--source-database-snapshot-fingerprint', 'b'.repeat(64), '--extracted-at', '2026-08-21T00:00:00Z', '--asset-cache', cache,
], { cwd: root, encoding: 'utf8' });
assert.notEqual(immutable.status, 0);
assert.match(immutable.stderr + immutable.stdout, /Refusing to rewrite immutable corpus/u);
assert.equal(readFileSync(join(target, 'sentinel.txt'), 'utf8'), before);

process.stdout.write('ui-conformance routing tests: PASS\n');
