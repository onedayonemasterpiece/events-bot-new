import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';

const IMMUTABLE_SHA = /^[0-9a-f]{40}$/u;

export function assertImmutableSha(value, label) {
  if (!IMMUTABLE_SHA.test(String(value || ''))) {
    throw new Error(`${label} must be an immutable lowercase 40-hex Git SHA`);
  }
  return value;
}

export function readCheckoutHead(root, label = 'checkout') {
  const checkout = resolve(root);
  const result = spawnSync('git', ['rev-parse', 'HEAD'], {
    cwd: checkout,
    encoding: 'utf8',
  });
  if (result.status !== 0) {
    throw new Error(`${label} is not a readable Git checkout: ${result.stderr.trim()}`);
  }
  return result.stdout.trim();
}

export function assertImmutableCheckout({ root, expectedSha, label }) {
  assertImmutableSha(expectedSha, `${label} SHA`);
  const actualSha = readCheckoutHead(root, label);
  if (actualSha !== expectedSha) {
    throw new Error(`${label} SHA mismatch: expected ${expectedSha}, got ${actualSha}`);
  }
  return { root: resolve(root), sha: actualSha };
}
