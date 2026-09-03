import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const readJson = async (relativePath) => JSON.parse(await readFile(path.join(siteRoot, relativePath), 'utf8'));

test('A0 successor boundary selects the restored net tree and current root ownership', async () => {
  const boundary = await readJson('src/data/a0-candidate-acceptance-boundary.v1.json');

  assert.equal(boundary.schema, 'kenigevents.a0.candidate-acceptance-boundary.v1');
  assert.equal(boundary.status, 'SUCCESSOR_INCLUSION_REQUIRED');
  assert.equal(boundary.review_outcome.branch_local_convergence_is_acceptance, false);
  assert.equal(boundary.review_outcome.source_binding_is_browser_acceptance, false);

  const inclusionIds = boundary.include_from_current_a0.map((item) => item.id);
  assert.deepEqual(inclusionIds, [
    'A0-COLLECTION-CONSUMERS',
    'A0-PERSONAL-CONSUMERS',
    'A0-FOCUS-AND-ARTIFACT-CONSUMERS',
    'A0-INFORMATION-AND-CLUB-CARD-CONSUMERS',
    'A0-POPULAR-BEHAVIOR-CORRECTION',
    'A0-DOWNSTREAM-CONTRACTS',
  ]);

  const rejected = boundary.must_not_select_as_final_net_effect;
  assert.ok(rejected.some((item) => item.historical_commit === '5e466d65bc2b71a814c26c063f90aa07709de08f'));
  assert.ok(rejected.some((item) => item.pattern?.includes('ListingMobileDensitySwitch')));
  assert.ok(rejected.some((item) => item.pattern?.includes('OptimizedEventCardGrid')));

  assert.deepEqual(boundary.integration_order.map((item) => item.step), [1, 2, 3, 4, 5, 6, 7]);
  assert.equal(boundary.integration_order[1].action.includes('F0'), true);
  assert.equal(boundary.integration_order[2].action.includes('M0'), true);
  assert.equal(boundary.integration_order[3].action.includes('A0'), true);
  assert.equal(boundary.integration_order[4].action.includes('a0-r0-mechanical-batch.v1.json'), true);

  const m0Protected = boundary.conflict_rules.filter((rule) => rule.winner?.includes('M0'));
  assert.ok(m0Protected.length >= 2);
  assert.ok(m0Protected.every((rule) => rule.a0_edit_allowed === false));
});
