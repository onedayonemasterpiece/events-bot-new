import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import test from 'node:test';

const profile=JSON.parse(await readFile(new URL('../src/data/design-system-reference-fixtures.json',import.meta.url),'utf8'));
const festivals=JSON.parse(await readFile(new URL('../src/data/festival-timeline.json',import.meta.url),'utf8'));
const page=await readFile(new URL('../src/pages/festivali/index.astro',import.meta.url),'utf8');

test('design-system fixture profile is a factual seven-festival 1/4/2 subset',()=>{
  assert.equal(profile.profile_id,'design-system-reference-v1');
  assert.deepEqual(profile.festivals.rows.map(row=>row.slugs.length),[1,4,2]);
  const selected=profile.festivals.rows.flatMap(row=>row.slugs);
  assert.equal(selected.length,7);
  assert.equal(new Set(selected).size,7);
  const actual=new Set(festivals.festivals.map(item=>item.slug));
  assert.ok(selected.every(slug=>actual.has(slug)));
});

test('festival route activates the bounded profile only by explicit preview environment',()=>{
  assert.match(page,/PUBLIC_DESIGN_FIXTURE_PROFILE/u);
  assert.match(page,/requestedFixtureProfile === designFixtureProfiles\.profile_id/u);
  assert.match(page,/filter\(\(item\) => !activeFixtureProfile \|\| festivalSlugSet\.has\(item\.slug\)\)/u);
  assert.match(page,/data-design-fixture-profile/u);
});
