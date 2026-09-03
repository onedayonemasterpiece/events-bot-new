import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const raw = await readFile(new URL('../src/data/a0-route-family-normalization.v1.json', import.meta.url), 'utf8');
const matrix = JSON.parse(raw);

test('A0 grouped route-family fraction has an exact production-contract denominator', () => {
  assert.equal(matrix.schema_version, 'a0-route-family-normalization-v1');
  assert.equal(matrix.source_contract, 'site/src/data/design-system-production-surface-contract.v1.json');
  assert.equal(matrix.denominator, 9);
  assert.equal(matrix.families.length, matrix.denominator);
  const converged = matrix.families.filter((family) => family.status === 'source_converged');
  assert.equal(converged.length, matrix.source_converged);
  assert.equal(matrix.fraction, `${matrix.source_converged}/${matrix.denominator}`);
  assert.equal(matrix.fraction, '5/9');
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

test('partial families name exact A0/F0 work instead of receiving false completion credit', () => {
  const partial = Object.fromEntries(matrix.families.filter((family) => family.status === 'partial').map((family) => [family.id, family]));
  assert.deepEqual(Object.keys(partial), ['festivals', 'exhibitions', 'event_detail', 'interest_clubs']);
  assert.match(partial.festivals.remaining, /atomic source patch/u);
  assert.match(partial.exhibitions.remaining, /local --ex-\* visible palette\/geometry ownership/u);
  assert.match(partial.event_detail.remaining, /DesktopEventPage handwritten rail anatomies/u);
  assert.match(partial.event_detail.remaining, /EventLayout orphan EventCard shell\/media style owner/u);
  assert.match(partial.interest_clubs.remaining, /route-local club hero\/deck theme ownership/u);
});
