import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { spawnSync } from 'node:child_process';

const root = resolve(import.meta.dirname, '..');
const workflow = readFileSync(resolve(root, '.github/workflows/ui-three-way-conformance.yml'), 'utf8');
for (const path of ['site/src/components/**', 'site/src/layouts/**', 'site/src/styles/**', 'site/src/data/preview-events.json', 'scripts/ui_conformance/**']) assert(workflow.includes(path));
assert(workflow.includes('workflow_dispatch'));
assert(workflow.includes("github.event_name == 'workflow_dispatch'"), 'Telegram must remain off for ordinary pull requests');
assert(workflow.includes('trusted_source:'));

for (const file of ['AGENTS.md', 'docs/AGENTS.md', 'site/AGENTS.md', '.codex/skills/static-site-design-system/SKILL.md', '.codex/skills/static-site-autotest/SKILL.md', '.codex/skills/static-listing-visual-lab/SKILL.md']) {
  assert(readFileSync(resolve(root, file), 'utf8').includes('ui-three-way-conformance'), `${file} must route material visual changes`);
}

const productionDiff = spawnSync('git', ['diff', '--name-only', 'a68c7f23c4e014c6e9f66e95f394656e9cb0f411', '--', 'site/src'], { cwd: root, encoding: 'utf8' });
assert.equal(productionDiff.status, 0, productionDiff.stderr);
assert.equal(productionDiff.stdout.trim(), '', 'UI conformance implementation must not mutate production UI source');

process.stdout.write('ui-conformance routing tests: PASS\n');
