import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('Free remains top-level while Children moves into the Collections submenu with Unusual, Free and Clubs', async () => {
  const menu = await read('src/components/Reference4MobileMenu.astro');
  const main = menu.slice(menu.indexOf('data-reference4-main'), menu.indexOf('data-reference4-collections aria-hidden'));
  const collections = menu.slice(menu.indexOf('data-reference4-collections aria-hidden'), menu.indexOf('data-reference4-service aria-hidden'));
  assert.match(main, /podborki\/besplatnye-sobytiya/u);
  assert.match(main, /data-reference4-collections-open/u);
  assert.doesNotMatch(main, />Детям</u);
  for (const label of ['Детям','Необычное','Бесплатно','Клубы по интересам']) assert.match(collections, new RegExp(`>${label}<`, 'u'));
  assert.match(collections, /route\('\/neobychnoe\/'\)/u);
  assert.match(collections, /route\('\/kluby-po-interesam\/'\)/u);
  assert.match(menu, /showPlane\('collections'\)/u);
  assert.match(menu, /route\('\/izbrannoe\/'\)/u);
});

test('Collections icons are one sourced Phosphor Thin family with durable provenance', async () => {
  const attribution = await read('public/assets/icons/reference4-v8/ATTRIBUTION.md');
  for (const [asset, id] of [['squares-four-thin.svg','365765'],['sparkle-thin.svg','365749'],['chats-thin.svg','365240']]) {
    assert.match(attribution, new RegExp(`${asset.replace('.', '\\.')}.+${id}`, 'u'));
  }
  assert.match(attribution, /Squares Four.+selected over `Folder`/u);
});
