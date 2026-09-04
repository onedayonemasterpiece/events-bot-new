import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const siteRoot = new URL('../', import.meta.url);

test('owner Preview maps every required production archetype exactly once', async () => {
  const [hub, contract, checker] = await Promise.all([
    readFile(new URL('src/pages/[preview]/index.astro', siteRoot), 'utf8'),
    readFile(new URL('src/data/design-system-production-surface-contract.v1.json', siteRoot), 'utf8').then(JSON.parse),
    readFile(new URL('scripts/check-unified-prototype.mjs', siteRoot), 'utf8'),
  ]);

  const registry = /const ownerFacingArchetypes = \[([\s\S]*?)\]\s+as const;/u.exec(hub)?.[1];
  assert.ok(registry, 'ownerFacingArchetypes registry must remain explicit');
  const ownerIds = [...registry.matchAll(/\bid:\s*'([a-z0-9-]+)'/gu)].map((match) => match[1]);
  const contractIds = [...registry.matchAll(/\bcontractId:\s*'([a-z0-9-]+)'/gu)].map((match) => match[1]);
  const requiredIds = contract.archetypes.filter((archetype) => archetype.required).map((archetype) => archetype.id);

  assert.equal(ownerIds.length, 18);
  assert.equal(new Set(ownerIds).size, ownerIds.length);
  assert.equal(new Set(contractIds).size, contractIds.length);
  assert.deepEqual([...contractIds].sort(), [...requiredIds].sort());
  assert.ok(ownerIds.includes('date'), 'arbitrary Date remains the additional owner representative');
  assert.match(registry, /id:\s*'interest-clubs',[^\n]*href:\s*'\/kluby-po-interesam\/'/u);
  assert.match(registry, /id:\s*'unusual-events',[^\n]*href:\s*'\/neobychnoe\/'/u);
  assert.match(checker, /design-system-production-surface-contract\.v1\.json/u);
  assert.match(checker, /missingProductionContractArchetypes/u);
});
