import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import vm from 'node:vm';

const source = await readFile(
  new URL('../control/auth-storage.js', import.meta.url),
  'utf8',
);
const context = vm.createContext({});
vm.runInContext(source, context);
const access = context.AutopresenterControlAuth;

class MemoryStorage {
  constructor(entries = {}) {
    this.values = new Map(Object.entries(entries));
  }

  getItem(key) {
    return this.values.get(key) ?? null;
  }

  setItem(key, value) {
    this.values.set(key, String(value));
  }

  removeItem(key) {
    this.values.delete(key);
  }
}

test('fragment onboarding survives a fresh installed-app page session', () => {
  const onboardingSession = new MemoryStorage();
  const sameOriginPersistent = new MemoryStorage();
  access.remember('owner-token', onboardingSession, sameOriginPersistent);

  const relaunchedSession = new MemoryStorage();
  assert.equal(
    access.restore(relaunchedSession, sameOriginPersistent),
    'owner-token',
  );
  assert.equal(
    relaunchedSession.getItem('autopresenter-control-token'),
    'owner-token',
  );
});

test('access reset removes only the Autopresenter key from both stores', () => {
  const session = new MemoryStorage({
    'autopresenter-control-token': 'owner-token',
    unrelated: 'keep-session',
  });
  const persistent = new MemoryStorage({
    'autopresenter-control-token': 'owner-token',
    unrelated: 'keep-persistent',
  });

  access.forget(session, persistent);

  assert.equal(session.getItem('autopresenter-control-token'), null);
  assert.equal(persistent.getItem('autopresenter-control-token'), null);
  assert.equal(session.getItem('unrelated'), 'keep-session');
  assert.equal(persistent.getItem('unrelated'), 'keep-persistent');
});
