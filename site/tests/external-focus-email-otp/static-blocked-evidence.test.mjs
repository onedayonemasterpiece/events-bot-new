import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';
import test from 'node:test';

const execFileAsync = promisify(execFile);

test('static fallback reports unknown side-effect counts without running the journey', async () => {
  const root = await mkdtemp(join(tmpdir(), 'ke-static-blocked-'));
  try {
    await execFileAsync(process.execPath, ['scripts/write-static-otp-blocked-evidence.mjs'], { cwd: new URL('../..', import.meta.url), env: {
      ...process.env, E2E_EVIDENCE_DIR: join(root, 'evidence'), E2E_PLATFORM: 'ios',
      E2E_SCENARIO_ID: 'focus.otp.browser_tab', E2E_RUN_ID: 'fixture',
    } });
    const summary = JSON.parse(await readFile(join(root, 'evidence', 'qa-summary.json'), 'utf8'));
    assert.equal(summary.status, 'BLOCKED');
    assert.deepEqual(summary.counts, { issue: null, verify: null, registration: null });
    assert.equal(summary.redaction_status, 'passed');
    assert.match(await readFile(join(root, 'evidence', 'README.md'), 'utf8'), /automatic rerun is forbidden/u);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
