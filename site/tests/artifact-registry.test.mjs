import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(path, import.meta.url), 'utf8');
const registry = JSON.parse(await read('../src/data/artifactRegistry.json'));
const page = await read('../src/pages/artefakty/index.astro');
const library = await read('../src/lib/artifacts.ts');
const endpoint = await read('../src/pages/data/artifacts.json.ts');
const sitemap = await read('../src/pages/sitemap.xml.ts');

test('Telegram idea registry is complete, stable and collection-linked', () => {
  assert.equal(registry.schema_version, 'artifact_registry_v1');
  assert.equal(registry.source_scope.anchor_message_id, 484);
  assert.ok(registry.source_scope.retrieved_through_message_id >= 707);
  assert.doesNotMatch(registry.registry_version, /telegram|message|through/iu);
  assert.ok(registry.artifacts.length >= 100);

  const ids = registry.artifacts.map((item) => item.id);
  const slugs = registry.artifacts.map((item) => item.slug);
  assert.equal(new Set(ids).size, ids.length);
  assert.equal(new Set(slugs).size, slugs.length);
  assert.ok(ids.every((id) => /^[a-z0-9]+(?:-[a-z0-9]+)*$/u.test(id)));

  const collection = registry.collections.find((item) => item.id === 'signs-of-kaliningrad-001');
  assert.ok(collection);
  assert.equal(collection.status, 'draft');
  assert.equal(collection.simultaneous_availability, true);
  assert.equal(collection.artifact_ids.length, 8);
  assert.equal(collection.unlock.threshold_percent, 60);
  assert.equal(collection.unlock.required_count, 5);
  assert.equal(collection.unlock.total_count, 8);
  assert.equal(collection.unlock.automatic_entry, false);
  assert.equal(collection.unlock.extra_finds_increase_odds, false);
  assert.equal(collection.unlock.share_increases_odds, false);
  assert.ok(collection.artifact_ids.every((id) => ids.includes(id)));
  assert.equal(new Set(collection.artifact_ids).size, collection.artifact_ids.length);
  for (const artifact of registry.artifacts) {
    const isInFirstCollection = collection.artifact_ids.includes(artifact.id);
    assert.equal(artifact.collection_ids.includes(collection.id), isInFirstCollection);
    assert.equal(artifact.planned_difficulty !== null, isInFirstCollection);
  }
});

test('public page uses hybrid naming and states the drawing boundary', () => {
  assert.match(page, /Пасхалки интерфейса/u);
  assert.match(page, /тайных местах интерфейса/u);
  assert.match(page, /становятся <strong>артефактами Калининградской области<\/strong>/u);
  assert.match(page, /ограниченный набор[\s\S]*<strong>коллекцию<\/strong>/u);
  assert.match(page, /не означает[\s\S]*автоматическое участие или выигрыш/u);
  assert.match(page, /скорость сбора не повышают шанс/u);
  assert.match(page, /Публикация, лайк, покупка/u);
  assert.match(page, /Розыгрыш ещё не открыт/u);
  assert.match(page, /data-artifact-registry-page/u);
  assert.match(page, /href=\{withBase\('\/data\/artifacts\.json'\)\}/u);
  assert.doesNotMatch(page, /Telegram|message №|сообщения №/iu);
});

test('public projection strips private thread and hidden placement metadata', () => {
  assert.match(library, /getPublicArtifactRegistry/u);
  assert.match(endpoint, /getPublicArtifactRegistry/u);
  assert.doesNotMatch(
    library.slice(library.indexOf('export function getPublicArtifactRegistry')),
    /source_refs|review_flags|source_scope|anchor_message_id|retrieved_through_message_id/u,
  );
  const serialized = JSON.stringify(registry);
  assert.doesNotMatch(serialized, /actor_id|participant_id|found_at|placement_url|placement_id/u);
});

test('artifact registry is a first-class public static route', () => {
  assert.match(sitemap, /absoluteUrl\('\/artefakty\/'\)/u);
  assert.match(page, /absoluteUrl\('\/artefakty\/'\)/u);
  assert.match(page, /<main id="main"/u);
  assert.match(page, /aria-label="Хлебные крошки"/u);
  assert.match(page, /mailto:info@kenigevents\.ru/u);
});

test('personal prototype leads with a compact honest collection teaser', async () => {
  const personalPage = await read('../src/pages/dlya-menya/index.astro');
  assert.match(personalPage, /data-artifact-collection-teaser/u);
  assert.match(personalPage, /data-prototype-progress/u);
  assert.match(personalPage, /slice\(0, 3\)/u);
  assert.match(personalPage, /Ещё <strong>\{artifactTeaserRemaining\}<\/strong> скрыто в интерфейсе/u);
  assert.match(personalPage, /Открыть коллекцию/u);
  assert.match(personalPage, /Сбор ещё не запущен/u);
  assert.match(personalPage, /grid-template-columns: repeat\(3, minmax\(0, 1fr\)\)/u);
  assert.ok(
    personalPage.indexOf('data-artifact-collection-teaser')
      < personalPage.indexOf('class="personal-page__intro"'),
  );
});
