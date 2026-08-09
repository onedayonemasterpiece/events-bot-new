import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { chmod, cp, mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';
import test from 'node:test';

import { buildSearchRuntimeDeployDispatch } from '../../scripts/search-runtime-deploy-dispatch.mjs';

const execFileAsync = promisify(execFile);
const helper = new URL('../../scripts/search-runtime-deploy-dispatch.mjs', import.meta.url);
const deployScript = new URL('../../scripts/deploy_fly_main.sh', import.meta.url);
const sha = '0123456789abcdef0123456789abcdef01234567';

const payload = {
  site_runtime_sha: sha,
  search_backend_revision: 'event-search:89abcdef',
  validation_profile: 'standard',
  changed_surfaces: ['site_runtime', 'search_backend'],
  deployment_run_id: 'fly-main-123.1',
};

test('dispatch helper emits the exact repository_dispatch envelope and no URL or secret fields', () => {
  const dispatch = buildSearchRuntimeDeployDispatch(payload);
  assert.deepEqual(Object.keys(dispatch), ['event_type', 'client_payload']);
  assert.equal(dispatch.event_type, 'search-runtime-deployed');
  assert.deepEqual(Object.keys(dispatch.client_payload).sort(), [
    'changed_surfaces', 'deployment_run_id', 'search_backend_revision',
    'site_runtime_sha', 'validation_profile',
  ]);
  assert.deepEqual(dispatch.client_payload.changed_surfaces, ['search_backend', 'site_runtime']);
  assert.doesNotMatch(JSON.stringify(dispatch), /https?:\/\/|token|secret|url/iu);
});

test('dispatch CLI rejects none and emits deterministic standard/full payloads', async () => {
  const baseArgs = [
    '--site-runtime-sha', sha,
    '--search-backend-revision', 'event-search:89abcdef',
    '--changed-surface', 'site_runtime',
    '--deployment-run-id', 'fly-main-123.1',
  ];
  for (const profile of ['standard', 'full']) {
    const first = await execFileAsync(process.execPath, [helper.pathname, ...baseArgs, '--validation-profile', profile]);
    const second = await execFileAsync(process.execPath, [helper.pathname, ...baseArgs, '--validation-profile', profile]);
    assert.equal(first.stdout, second.stdout);
    assert.equal(JSON.parse(first.stdout).client_payload.validation_profile, profile);
  }
  await assert.rejects(
    execFileAsync(process.execPath, [helper.pathname, ...baseArgs, '--validation-profile', 'none']),
    /search_runtime_validation_profile_invalid/u,
  );
});

test('canonical Fly deploy consumes marker arguments and dispatches exactly once after success', async () => {
  const source = await readFile(deployScript, 'utf8');
  assert.match(source, /SEARCH_VALIDATION_PROFILE="\$\{SEARCH_VALIDATION_PROFILE:-none\}"/u);
  assert.match(source, /none\|standard\|full/u);
  assert.ok(source.includes('FLY_ARGS+=("$1")'));
  assert.equal((source.match(/"\$GH_BIN" api --method POST/gu) || []).length, 1);
  assert.equal((source.match(/scripts\/search-runtime-deploy-dispatch\.mjs/gu) || []).length, 1);
  const preflightAt = source.indexOf('"$GH_BIN" auth status');
  const deployAt = source.indexOf('"$FLYCTL" deploy');
  const noneAt = source.indexOf('if [[ "$SEARCH_VALIDATION_PROFILE" == none ]]');
  const dispatchAt = source.indexOf('"$GH_BIN" api --method POST');
  assert.ok(preflightAt > 0 && deployAt > preflightAt && noneAt > deployAt && dispatchAt > noneAt,
    'Fly success must precede none exit and the only dispatch');
  assert.doesNotMatch(source.slice(deployAt, noneAt), /search-runtime-deployed/u);
});

async function makeFakeDeployRepo() {
  const root = await mkdtemp(join(tmpdir(), 'search-deploy-marker-'));
  await mkdir(join(root, 'scripts'), { recursive: true });
  await mkdir(join(root, 'site/e2e/search'), { recursive: true });
  await mkdir(join(root, 'home/.fly/bin'), { recursive: true });
  await mkdir(join(root, 'bin'), { recursive: true });
  await cp(deployScript, join(root, 'scripts/deploy_fly_main.sh'));
  await cp(helper, join(root, 'scripts/search-runtime-deploy-dispatch.mjs'));
  for (const name of ['production-health-planner.mjs', 'production-health-contract.mjs']) {
    await cp(new URL(`../e2e/search/${name}`, import.meta.url), join(root, 'site/e2e/search', name));
  }
  const fly = join(root, 'home/.fly/bin/flyctl');
  const gh = join(root, 'bin/gh');
  await writeFile(fly, `#!/usr/bin/env bash\nset -euo pipefail\necho "fly:$*" >> "$CALL_LOG"\nif [[ "\${1:-}" == deploy ]]; then exit "\${FLY_DEPLOY_EXIT:-0}"; fi\n`);
  await writeFile(gh, `#!/usr/bin/env bash\nset -euo pipefail\necho "gh:$*" >> "$CALL_LOG"\nwhile (($#)); do if [[ "$1" == --input ]]; then cat "$2" >> "$CALL_LOG"; exit 0; fi; shift; done\n`);
  await chmod(fly, 0o755);
  await chmod(gh, 0o755);
  await execFileAsync('git', ['init', '-q', '-b', 'main'], { cwd: root });
  await execFileAsync('git', ['config', 'user.email', 'test@example.invalid'], { cwd: root });
  await execFileAsync('git', ['config', 'user.name', 'Search Test'], { cwd: root });
  await execFileAsync('git', ['add', '.'], { cwd: root });
  await execFileAsync('git', ['commit', '-qm', 'fixture'], { cwd: root });
  const bare = `${root}.git`;
  await execFileAsync('git', ['init', '-q', '--bare', bare]);
  await execFileAsync('git', ['remote', 'add', 'origin', bare], { cwd: root });
  await execFileAsync('git', ['push', '-q', '-u', 'origin', 'main'], { cwd: root });
  const callLog = `${root}.calls.log`;
  return { root, callLog, env: {
    ...process.env,
    HOME: join(root, 'home'),
    PATH: `${join(root, 'bin')}:${process.env.PATH}`,
    CALL_LOG: callLog,
  } };
}

test('default none and failed Fly deploy emit zero dispatches; successful standard emits one', async () => {
  const fixture = await makeFakeDeployRepo();
  const script = join(fixture.root, 'scripts/deploy_fly_main.sh');
  await execFileAsync('bash', [script], { cwd: fixture.root, env: fixture.env });
  let calls = await readFile(fixture.callLog, 'utf8');
  assert.equal((calls.match(/^gh:/gmu) || []).length, 0);

  await writeFile(fixture.callLog, '');
  await assert.rejects(execFileAsync('bash', [script,
    '--search-validation-profile', 'standard',
    '--search-backend-revision', 'event-search:89abcdef',
    '--search-deployment-run-id', 'fly-main-123.1',
    '--search-changed-surface', 'site_runtime',
  ], { cwd: fixture.root, env: { ...fixture.env, FLY_DEPLOY_EXIT: '42' } }));
  calls = await readFile(fixture.callLog, 'utf8');
  assert.equal((calls.match(/^gh:api --method POST/gmu) || []).length, 0);
  assert.match(calls, /^gh:auth status$/mu);

  await writeFile(fixture.callLog, '');
  await execFileAsync('bash', [script,
    '--search-validation-profile', 'standard',
    '--search-backend-revision', 'event-search:89abcdef',
    '--search-deployment-run-id', 'fly-main-123.1',
    '--search-changed-surface', 'site_runtime',
  ], { cwd: fixture.root, env: fixture.env });
  calls = await readFile(fixture.callLog, 'utf8');
  assert.equal((calls.match(/^gh:api --method POST/gmu) || []).length, 1);
  assert.match(calls, /"event_type":"search-runtime-deployed"/u);
  assert.match(calls, /"validation_profile":"standard"/u);

  await writeFile(fixture.callLog, '');
  await execFileAsync('bash', [script], { cwd: fixture.root, env: {
    ...fixture.env,
    SEARCH_VALIDATION_PROFILE: 'standard',
    SEARCH_BACKEND_REVISION: 'event-search:89abcdef',
    SEARCH_DEPLOYMENT_RUN_ID: 'fly-main-env-123.1',
    SEARCH_CHANGED_SURFACES: 'site_runtime,search_backend',
  } });
  calls = await readFile(fixture.callLog, 'utf8');
  assert.equal((calls.match(/^gh:api --method POST/gmu) || []).length, 1);
  const envelope = JSON.parse(calls.split('\n').find((line) => line.startsWith('{')));
  assert.deepEqual(envelope.client_payload.changed_surfaces, ['search_backend', 'site_runtime']);
});
