import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { applyGoldenActionFixtures, goldenActionContract, goldenActionHref } from '../scripts/golden-review-actions.mjs';
import { loadGoldenCorpus, materializeGoldenPreviewData } from '../scripts/golden-review-corpus.mjs';

const testsDir = dirname(fileURLToPath(import.meta.url));
const siteDir = resolve(testsDir, '..');

function materializedGolden() {
  const { corpus } = loadGoldenCorpus();
  const base = JSON.parse(readFileSync(join(siteDir, 'src', 'data', 'preview-events.json'), 'utf8'));
  return {
    corpus,
    data:applyGoldenActionFixtures(materializeGoldenPreviewData(corpus, base), corpus),
  };
}

test('Golden action hrefs are deterministic, inert and cover all admission kinds', () => {
  const { corpus } = loadGoldenCorpus();
  const contract = goldenActionContract(corpus);
  assert.equal(contract.length, corpus.events.length);
  assert.deepEqual(new Set(contract.map((item) => item.kind)), new Set(['free', 'ticket', 'registration', 'phone', 'source']));

  for (const item of contract) {
    const spec = corpus.events.find((event) => event.id === item.event_id);
    assert.ok(spec);
    assert.equal(item.href, goldenActionHref(spec));
    if (item.lifecycle_status === 'cancelled' || item.kind === 'free') {
      assert.equal(item.href, null);
    } else if (item.kind === 'phone') {
      assert.equal(item.href, 'tel:+74012000000');
    } else {
      assert.match(item.href, /^https:\/\/example\.invalid\/kenigevents-golden\/(ticket|registration|source)\/9700\d{2}$/u);
    }
  }
});

test('Golden materialization exposes CTA hrefs without changing real canaries', () => {
  const { corpus, data } = materializedGolden();
  const specs = new Map(corpus.events.map((event) => [event.id, event]));
  for (const event of data.events) {
    const spec = specs.get(Number(event.id));
    if (!spec) continue;
    const expectedHref = goldenActionHref(spec);
    assert.equal(event.ticket.href, expectedHref, `event ${event.id} action mismatch`);
    if (spec.admission.kind === 'source') {
      assert.equal(event.source_url, expectedHref);
      assert.deepEqual(event.source_urls, expectedHref ? [expectedHref] : []);
    } else {
      assert.equal(event.source_url, null);
      assert.deepEqual(event.source_urls, []);
    }
  }

  const realIds = new Set(corpus.events.map((event) => Number(event.id)));
  const sourceBase = JSON.parse(readFileSync(join(siteDir, 'src', 'data', 'preview-events.json'), 'utf8'));
  const sourceById = new Map(sourceBase.events.map((event) => [Number(event.id), event]));
  for (const event of data.events.filter((item) => !realIds.has(Number(item.id)))) {
    const original = sourceById.get(Number(event.id));
    assert.ok(original);
    assert.deepEqual(event.ticket, original.ticket, `real canary ${event.id} ticket mutated`);
    assert.equal(event.source_url, original.source_url, `real canary ${event.id} source URL mutated`);
  }
});
