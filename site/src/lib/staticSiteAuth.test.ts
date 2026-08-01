import assert from 'node:assert/strict';
import test from 'node:test';

import { purgeStaticAuthStorage } from './staticAuthReset.ts';

function memoryStorage(initial: Record<string, string>) {
  const values = new Map(Object.entries(initial));
  return {
    get length() { return values.size; },
    key(index: number) { return [...values.keys()][index] || null; },
    getItem(key: string) { return values.get(key) || null; },
    removeItem(key: string) { values.delete(key); },
    entries() { return [...values.entries()]; },
  };
}

test('onboarding reset removes every auth fragment for the current project only', () => {
  const storage = memoryStorage({
    'sb-project-auth-token': '{session}',
    'sb-project-auth-token.0': 'chunk-zero',
    'sb-project-auth-token-code-verifier': 'pkce',
    'ke_yandex_auth_intent_v1': '{intent}',
    'sb-other-auth-token': '{other-session}',
    'ke_personalization_profile': '{profile}',
  });

  assert.equal(purgeStaticAuthStorage('https://project.supabase.co', storage), true);
  assert.deepEqual(storage.entries(), [
    ['sb-other-auth-token', '{other-session}'],
    ['ke_personalization_profile', '{profile}'],
  ]);
});

test('onboarding reset fails closed when browser storage cannot be changed', () => {
  const storage = {
    length: 1,
    key: () => 'sb-project-auth-token',
    getItem: () => '{session}',
    removeItem: () => { throw new Error('blocked'); },
  };
  assert.equal(purgeStaticAuthStorage('https://project.supabase.co', storage), false);
});
