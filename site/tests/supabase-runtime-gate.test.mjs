import assert from 'node:assert/strict';
import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';

const root = new URL('../src/', import.meta.url);

async function files(directory) {
  const entries = await readdir(directory, { withFileTypes:true });
  const nested = await Promise.all(entries.map((entry) => {
    const target = path.join(directory, entry.name);
    return entry.isDirectory() ? files(target) : [target];
  }));
  return nested.flat();
}

test('Supabase runtime endpoints are reachable only through the shared client or diagnostics', async () => {
  const allowRaw = new Set([
    path.normalize(new URL('../src/lib/resilientSupabaseTransport.ts', import.meta.url).pathname),
    path.normalize(new URL('../src/components/FocusConnectivityDiagnostic.astro', import.meta.url).pathname),
  ]);
  const offenders = [];
  for (const filename of await files(root.pathname)) {
    if (!/\.(?:astro|[cm]?[jt]s)$/u.test(filename) || filename.includes('.test.')) continue;
    const source = await readFile(filename, 'utf8');
    if (!/(?:auth|rest|functions)\/v1/u.test(source) || allowRaw.has(path.normalize(filename))) continue;
    const ownsSharedRoute = /getResilientDataClient|dataClient\.(?:request|safeRead|selectedOnce|idempotentReplay)|authController\?\.transport/u.test(source);
    if (!ownsSharedRoute) offenders.push(path.relative(root.pathname, filename));
  }
  assert.deepEqual(offenders, []);
});

test('only the shared Auth controller constructs a browser Supabase SDK client', async () => {
  const offenders = [];
  for (const filename of await files(root.pathname)) {
    if (!/\.(?:astro|[cm]?[jt]s)$/u.test(filename) || filename.includes('.test.')) continue;
    const source = await readFile(filename, 'utf8');
    if (/\bcreateClient\s*\(/u.test(source) && !filename.endsWith('/lib/staticSiteAuth.ts')) {
      offenders.push(path.relative(root.pathname, filename));
    }
  }
  assert.deepEqual(offenders, []);
});
