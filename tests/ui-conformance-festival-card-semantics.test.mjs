import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import {
  buildFestivalSemanticCensus,
  extractFestivalCategoryIcons,
} from '../scripts/ui_conformance/festival-card-semantics.mjs';

const root = resolve(import.meta.dirname, '..');
const source = readFileSync(resolve(root, 'site/src/pages/festivali/index.astro'), 'utf8');
const fixture = JSON.parse(readFileSync(resolve(root, 'tests/fixtures/ui-conformance/festival-card-semantic-census.v1.json'), 'utf8'));

test('category icon vocabulary is extracted from the actual Astro renderer', () => {
  const mapping = extractFestivalCategoryIcons(source);
  assert.equal(Object.keys(mapping).length, 16);
  assert.deepEqual(mapping['Современное искусство'], ['palette']);
  assert.deepEqual(mapping['Кино'], ['camera', 'ticket']);
  assert.deepEqual(mapping['Море и техника'], ['anchor', 'history']);
});

test('actual generated FestivalCard corpus has one semantic theme component with variable content', () => {
  const report = buildFestivalSemanticCensus({ root });
  assert.equal(report.festival_count, 21);
  assert.equal(report.category_count, 16);
  assert.deepEqual(report.categories, fixture.categories);
  assert.deepEqual(report.component_contract.theme, {
    component_id: 'festival.meta.theme',
    component_variant: 'default',
    label_role: 'content',
    icon_slots: ['primary', 'secondary'],
    secondary_icon_presence: 'optional',
  });
  assert.equal(new Set(report.festivals.map((row) => row.theme.component_id)).size, 1);
  assert.equal(new Set(report.festivals.map((row) => row.theme.component_variant)).size, 1);
});

test('FestivalCard reuses shared Like action and social proof rather than a private Favorite family', () => {
  const report = buildFestivalSemanticCensus({ root });
  assert.deepEqual(report.component_contract.like, {
    action_component_id: 'event.action.like',
    proof_component_id: 'event.social-proof.like',
    local_selection_state: 'behavior-only',
    festival_specific_component: false,
  });
  const more = report.festivals.find((row) => row.slug === 'more-vnutri');
  assert.equal(more.internal_event_id, 4211);
  assert.deepEqual(more.like, {
    action_component_id: 'event.action.like',
    proof_component_id: 'event.social-proof.like',
    base_count: 84,
    count_label: '84',
    proof_present: true,
  });
  assert.equal(report.joined_event_count, 4);
  assert.equal(report.festivals.filter((row) => row.like.proof_present).length, 4);
});

test('committed census is a deterministic read-back of current renderer plus data', () => {
  const rebuilt = buildFestivalSemanticCensus({ root });
  assert.deepEqual(rebuilt, fixture);
  assert.equal(fixture.source_facts.renderer_like_model, 'festival-local-favorite/no-aggregate-count');
  assert.equal(fixture.target_model.owner_override, 'shared-like-action/aggregate-social-proof');
  assert.equal(fixture.production_source_mutated, false);
});
