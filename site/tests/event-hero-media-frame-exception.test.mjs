import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('EventHero has one explicit temporary media-frame exception and no unbounded family waiver', async () => {
  const [source, raw] = await Promise.all([
    read('src/components/EventHero.astro'),
    read('src/components/design-system/a0-media-frame-exceptions.v1.json'),
  ]);
  const registry = JSON.parse(raw);

  assert.equal(registry.schema_version, 'a0-media-frame-exceptions-v1');
  assert.equal(registry.owner, 'A0');
  assert.equal(registry.contract_version, '1.9.0');
  assert.equal(registry.exceptions.length, 1);
  const exception = registry.exceptions[0];
  assert.equal(exception.path, 'site/src/components/EventHero.astro');
  assert.equal(exception.selector, '.event-hero__image');
  assert.equal(exception.status, 'intentional_temporary_exception');
  assert.match(exception.review_trigger, /atomic patch-capable A0 writer|direct M0 EventHeroMediaFrame adapter/u);
  assert.match(exception.acceptance_boundary, /EventLayout, DesktopEventPage, mobile rails, cards or galleries/u);

  assert.match(source, /class="event-hero__image"/u);
  assert.match(source, /data-image-mode=\{imageMode\}/u);
  assert.match(source, /--event-hero-focal-y/u);
  assert.equal((source.match(/object-fit:/gu) || []).length, 2,
    'temporary exception remains bounded to contain and cover hero states');
  assert.equal((source.match(/object-position:/gu) || []).length, 2,
    'temporary exception remains bounded to centered and focal hero states');
});
