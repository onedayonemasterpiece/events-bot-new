import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import test from 'node:test';

function run(command, args, options) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, options);
    let stderr = '';
    child.stderr.on('data', (chunk) => { stderr += chunk; });
    child.on('error', reject);
    child.on('close', (code) => resolve({ code, stderr }));
  });
}

test('missing external configuration produces a sanitized BLOCKED artifact', async () => {
  const root = await mkdtemp(join(tmpdir(), 'focus-email-e2e-blocked-'));
  const evidenceDir = join(root, 'evidence');
  try {
    const result = await run(process.execPath, ['e2e/focus-email/run.mjs'], {
      cwd: new URL('../../', import.meta.url),
      env: {
        ...process.env,
        E2E_RUN_ID: 'blocked-config-test',
        E2E_EVIDENCE_DIR: evidenceDir,
        E2E_TARGET_URL: '',
        E2E_RECIPIENT_TEMPLATE: '',
        E2E_IMAP_USERNAME: '',
        E2E_IMAP_PASSWORD: '',
      },
      stdio: ['ignore', 'ignore', 'pipe'],
    });
    assert.equal(result.code, 1);
    assert.match(result.stderr, /BLOCKED: BLOCKED_INFRASTRUCTURE/u);
    const report = JSON.parse(await readFile(join(evidenceDir, 'result.json'), 'utf8'));
    assert.equal(report.status, 'BLOCKED');
    assert.equal(report.failure_domain, 'BLOCKED_INFRASTRUCTURE');
    assert.equal(report.redaction_audit_passed, true);
    assert.equal(await readFile(join(evidenceDir, '.redaction-ok'), 'utf8'), 'safe\n');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
