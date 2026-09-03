import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const raw = await readFile(new URL('../src/data/a0-route-family-normalization.v1.json', import.meta.url), 'utf8');
const matrix = JSON.parse(raw);
const focusCollection = await readFile(new URL('../src/pages/fokus-gruppa/kollektsiya/index.astro', import.meta.url), 'utf8');
const closedFocusHub = await readFile(new URL('../src/pages/zakrytaya-afisha/index.astro', import.meta.url), 'utf8');

test('A0 grouped route-family fraction has an exact production-contract denominator', () => {
  assert.equal(matrix.schema_version, 'a0-route-family-normalization-v1');
  assert.equal(matrix.source_contract, 'site/src/data/design-system-production-surface-contract.v1.json');
  assert.equal(matrix.denominator, 9);
  assert.equal(matrix.families.length, matrix.denominator);
  const converged = matrix.families.filter((family) => family.status === 'source_converged');
  assert.equal(converged.length, matrix.source_converged);
  assert.equal(matrix.fraction, `${matrix.source_converged}/${matrix.denominator}`);
  assert.equal(matrix.fraction, '9/9');
  assert.equal(matrix.browser_verdict_owner, 'V0');
});

test('every grouped family has concrete source evidence and an honest remaining boundary', () => {
  const expectedIds = [
    'collections',
    'festivals',
    'exhibitions',
    'for_me',
    'focus_group',
    'artifacts',
    'event_detail',
    'interest_clubs',
    'information_pages',
  ];
  assert.deepEqual(matrix.families.map((family) => family.id), expectedIds);
  for (const family of matrix.families) {
    assert.ok(['source_converged', 'partial'].includes(family.status));
    assert.ok(Array.isArray(family.evidence) && family.evidence.length > 0, `${family.id} lacks evidence`);
    assert.ok(typeof family.remaining === 'string' && family.remaining.length > 0, `${family.id} lacks remaining boundary`);
  }
});

test('all grouped families are source-converged without claiming the V0 browser verdict', () => {
  assert.deepEqual(matrix.families.filter((family) => family.status === 'partial'), []);
  for (const family of matrix.families) assert.match(family.remaining, /V0/u);
});

test('final focus routes expose accepted identity and synchronize their runtime state', () => {
  assert.match(focusCollection, /data-ds-family="FocusEggCollectionRouteComposition"[\s\S]*data-ds-version="1"[\s\S]*data-ds-variant="collection-prototype"/u);
  assert.match(focusCollection, /data-ds-state=\{`found-\$\{collectionProgress\.found\}-of-\$\{collectionProgress\.eligible\}`\}/u);
  assert.match(focusCollection, /root\.dataset\.dsState = `found-\$\{found\}-of-\$\{eligible\}`/u);

  assert.match(closedFocusHub, /data-ds-family="ClosedFocusHubRouteComposition"[\s\S]*data-ds-version="1"[\s\S]*data-ds-variant="participant-hub"[\s\S]*data-ds-state="checking"/u);
  assert.match(closedFocusHub, /root\.dataset\.dsState = 'available'/u);
  assert.match(closedFocusHub, /root\.dataset\.dsState = 'locked'/u);
});
