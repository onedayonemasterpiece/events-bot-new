import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

test('For me page title consumes the canonical page-title role', async () => {
  const source = await readFile(path.join(siteRoot, 'src/pages/dlya-menya/index.astro'), 'utf8');

  assert.match(source, /\.personal-page__head h1\s*\{[^}]*font:\s*var\(--ke-type-h1\);[^}]*letter-spacing:\s*var\(--ke-type-h1-letter\)/u);
  assert.doesNotMatch(source, /\.personal-page__head h1\s*\{[^}]*font-size:\s*clamp\(/u);
});

test('For me account heading remains its compact functional role', async () => {
  const source = await readFile(path.join(siteRoot, 'src/pages/dlya-menya/index.astro'), 'utf8');

  assert.match(source, /\.personal-page__account h2\s*\{[^}]*font-size:\s*clamp\(1\.45rem,\s*3vw,\s*2rem\)/u);
  assert.doesNotMatch(source, /\.personal-page__account h2\s*\{[^}]*font:\s*var\(--ke-type-h2\)/u);
});


test('ordinary Search, Favorites and Partners titles do not retain local scale forks', async () => {
  const favorites = await readFile(path.join(siteRoot,'src/components/FavoritesSurface.astro'),'utf8');
  const partners = await readFile(path.join(siteRoot,'src/pages/partners/index.astro'),'utf8');
  const layout = await readFile(path.join(siteRoot,'src/layouts/EventLayout.astro'),'utf8');
  assert.match(favorites, /\.favorites-surface__head h1\s*\{[^}]*font:\s*var\(--ke-type-h1\)/u);
  assert.doesNotMatch(favorites, /\.favorites-surface__head h1\s*\{[^}]*font-size:\s*clamp\(/u);
  assert.match(partners, /\.partners-heading h1\s*\{[^}]*font:\s*var\(--ke-type-h1\)/u);
  assert.doesNotMatch(partners, /font-size:\s*var\(--ke-partners-(?:mobile-)?heading-size\)/u);
  assert.doesNotMatch(layout, /\.authorized-search--standalone \.authorized-search__head h1\s*\{[^}]*font-size:\s*clamp\(/u);
  const authority=JSON.parse(await readFile(path.join(siteRoot,'src/components/design-system/f0-typography-authority.v1.json'),'utf8'));
  for(const route of ['/poisk/','/izbrannoe/','/partners/','/partnerstvo/'])assert.ok(authority.owner_review_roles.page_title.routes.includes(route));
});
