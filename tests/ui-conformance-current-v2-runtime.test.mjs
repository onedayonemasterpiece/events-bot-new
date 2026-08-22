import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { assertImmutableCheckout, assertImmutableSha } from '../scripts/ui_conformance/immutable-checkout.mjs';
import { extractGeneratedChipState, summarizeInventory } from '../scripts/ui_conformance/inventory-event-card-chips.mjs';

const root = resolve(import.meta.dirname, '..');

test('immutable checkout identity rejects branch names and a mismatched SHA', () => {
  assert.throws(() => assertImmutableSha('main', 'Design SHA'), /40-hex/u);
  assert.match(readFileSync(resolve(root, '.git'), 'utf8').trim(), /gitdir:/u);
  assert.throws(() => assertImmutableCheckout({ root, expectedSha: '0'.repeat(40), label: 'Tooling checkout' }), /SHA mismatch/u);
});

test('generated chip extraction stays inside the selected EventCard article', () => {
  const html = `<!doctype html><article data-event-card data-calendar-eligible="false">
    <span data-card-type>выставка</span><span data-card-status>Билеты</span>
    <span aria-label="10 июля в 19:00" data-occurrence-label data-occurrence-label-variant="inline" data-occurrence-complex="false"><span>10 июля 19:00</span></span>
    <button data-feedback-action="not_interested"><span>Не интересно</span></button>
    <button data-native-share data-share-base-count="5"><span data-share-label>Поделиться</span><span data-share-count>5</span></button>
    <button data-feedback-action="like" data-base-count="0"><span data-feedback-count></span></button>
  </article><article data-event-card data-calendar-eligible="true"><a data-calendar-action><span data-calendar-label>В календарь</span></a></article>`;
  const row = extractGeneratedChipState(html, { fixture_id: 'event.real.1', event_id: 1, preview_event_sha256: 'a'.repeat(64) });
  assert.deepEqual(row.event_type, { present: true, label: 'выставка' });
  assert.deepEqual(row.admission, { present: true, label: 'Билеты' });
  assert.equal(row.occurrence.label, '10 июля 19:00');
  assert.deepEqual(row.calendar, { eligible: false, present: false, label: null });
  assert.equal(row.actions.share.base_count, 5);
  assert.equal(row.actions.share.count_label, '5');
  assert.equal(row.actions.like.base_count, 0);
  assert.equal(row.actions.like.count_label, '');
  assert(row.branch_families.includes('calendar:absent'));
});

test('runtime scripts keep Astro source and tooling identities distinct', () => {
  const materialize = readFileSync(resolve(root, 'scripts/ui_conformance/materialize-case.mjs'), 'utf8');
  for (const flag of ['astro-source-site', 'astro-source-sha', 'tooling-root', 'tooling-sha']) assert(materialize.includes(flag));
  assert(materialize.includes('astro_source_repository_sha'));
  assert(materialize.includes('conformance_tooling_repository_sha'));
  assert(!materialize.includes('const site=resolve(args.site)'), 'materializer must not infer source identity from tooling HEAD');
  const inventory = readFileSync(resolve(root, 'scripts/ui_conformance/inventory-event-card-chips.mjs'), 'utf8');
  assert(inventory.includes("generation_mode: 'real-astro-static-build/exact-event-card/split-actions'"));
  assert(inventory.includes('production_source_mutated: false'));
  assert(!inventory.includes("writeFileSync(join(site, 'src"), 'inventory must not write into production site/src');
});

test('inventory summary preserves every rendered label and count branch', () => {
  const rows = [
    { event_type:{label:'выставка'}, admission:{label:'Билеты'}, occurrence:{label:'10 июля'}, actions:{not_interested:{label:'Не интересно'},calendar:{label:null},share:{label:'Поделиться',base_count:0},like:{label:null,base_count:5}}, branch_families:['calendar:absent'] },
    { event_type:{label:'лекция'}, admission:{label:'Бесплатно · регистрация'}, occurrence:{label:'22 августа 12:00'}, actions:{not_interested:{label:'Не интересно'},calendar:{label:'В календарь'},share:{label:'Поделиться',base_count:5},like:{label:null,base_count:0}}, branch_families:['calendar:present'] },
  ];
  assert.deepEqual(summarizeInventory(rows).event_type_labels, ['выставка', 'лекция']);
  assert.deepEqual(summarizeInventory(rows).share_count_values, [0, 5]);
  assert.deepEqual(summarizeInventory(rows).branch_families, ['calendar:absent', 'calendar:present']);
});
