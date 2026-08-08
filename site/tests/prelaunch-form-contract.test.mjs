import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { classifyBackendOperation, policyForOperation } from '../src/lib/backendOperationCatalog.ts';

const siteDir = dirname(dirname(fileURLToPath(import.meta.url)));
const repoRoot = dirname(siteDir);
const source = (path) => readFileSync(join(repoRoot, path), 'utf8');

test('prelaunch client keeps a permanent backend-confirmed first/repeat state', () => {
  const runtime = source('site/src/scripts/prelaunchForm.ts');
  assert.match(runtime, /payload\.status !== 'registered' && payload\.status !== 'already_registered'/u);
  assert.match(runtime, /alreadyRegistered \? 'registered' : 'success'/u);
  assert.match(runtime, /title: 'Готово, вы записаны'/u);
  assert.match(runtime, /title: 'Вы уже записаны'/u);
  assert.match(runtime, /localStorage\.setItem\(STORAGE_KEY, 'registered'\)/u);
  assert.match(runtime, /localStorage\.removeItem\(STORAGE_KEY\)/u);
  assert.match(runtime, /if \(requestInFlight\) return/u);
  assert.match(runtime, /requestInFlight = true/u);
  assert.match(runtime, /requestInFlight = false/u);
  assert.match(runtime, /finally \{/u);
  assert.doesNotMatch(runtime, /email\.value\s*=\s*['"]{2}/u);
});

test('prelaunch RPC is replay-safe in the resilient transport catalog', () => {
  const operation = classifyBackendOperation(
    'https://project.supabase.co/rest/v1/rpc/register_prelaunch_notification_v1',
    { method: 'POST' },
  );
  assert.equal(operation.semantics, 'idempotent-replay');
  assert.deepEqual(operation.routeSupport, ['direct', 'relay']);
  assert.equal(policyForOperation(operation), 'idempotent-replay');
});

test('v3 RPC returns server truth and closes the final insert race', () => {
  const migration = source('supabase/migrations/20260808143744_prelaunch_registration_result_and_race_safe_dedup.sql');
  assert.match(migration, /create unique index if not exists[\s\S]*\(email\)/u);
  assert.match(migration, /on conflict \(email\) do nothing[\s\S]*returning true into v_inserted/u);
  assert.match(migration, /if not coalesce\(v_inserted, false\)[\s\S]*update personalization\.prelaunch_launch_subscription/u);
  assert.match(migration, /'status', 'registered'/u);
  assert.match(migration, /'status', 'already_registered'/u);
  assert.match(migration, /revoke execute[\s\S]*from public, anon, authenticated/u);
  assert.match(migration, /grant execute[\s\S]*to anon, authenticated, service_role/u);
});
