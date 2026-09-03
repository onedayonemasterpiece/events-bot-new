import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');
const semanticRoles = (source) => [...source.matchAll(/<SemanticIcon\b[^>]*\brole="([^"]+)"/gu)].map((match) => match[1]);

test('M0 card and mobile-rail roots consume SemanticIcon rather than bare Icon', async () => {
  const roots = [
    ['src/components/EventCard.astro', "./design-system/SemanticIcon.astro"],
    ['src/components/listings/ListingEventCard.astro', "../design-system/SemanticIcon.astro"],
    ['src/components/listings/MobileListingRailRow.astro', "../design-system/SemanticIcon.astro"],
  ];

  for (const [file, semanticImport] of roots) {
    const source = await read(file);
    assert.ok(source.includes(`import SemanticIcon from '${semanticImport}';`), `${file} misses SemanticIcon`);
    assert.doesNotMatch(source, /import Icon from ['"][^'"]*Icon\.astro['"]/u, `${file} must not bypass the four-role wrapper`);
    assert.doesNotMatch(source, /<Icon\b/u, `${file} must not render a bare Icon`);
  }
});

test('EventCard action controls use the action role without changing semantic glyphs', async () => {
  const source = await read('src/components/EventCard.astro');

  for (const name of ['dislike', 'share', 'heart', 'calendar']) {
    assert.match(source, new RegExp(`<SemanticIcon name="${name}" role="action" \\/>`, 'u'), `${name} misses action role`);
  }
  assert.equal(semanticRoles(source).filter((role) => role === 'action').length, 7,
    'overlay and split-actions variants must expose seven action-icon instances');
});

test('ListingEventCard proof and mobile rail states use only the intended four-role values', async () => {
  const [listing, mobile, foundations] = await Promise.all([
    read('src/components/listings/ListingEventCard.astro'),
    read('src/components/listings/MobileListingRailRow.astro'),
    read('src/components/design-system/foundations.ts'),
  ]);

  assert.match(listing, /<SemanticIcon name="share" role="inline" \/>/u);
  assert.match(listing, /<SemanticIcon name="heart" role="inline" \/>/u);
  for (const role of ['feature', 'inline', 'action']) {
    assert.match(mobile, new RegExp(`<SemanticIcon name="heart" role="${role}" \\/>`, 'u'));
  }

  const roleBlock = foundations.match(/export const ICON_SIZE_ROLES = \{([\s\S]*?)\} as const;/u)?.[1] || '';
  const declaredRoles = [...roleBlock.matchAll(/^\s*([a-z-]+):\s*\d+,?\s*$/gmu)].map((match) => match[1]);
  assert.deepEqual(declaredRoles, ['inline', 'control', 'action', 'feature']);

  const usedRoles = new Set([...semanticRoles(listing), ...semanticRoles(mobile)]);
  for (const role of usedRoles) assert.ok(declaredRoles.includes(role), `undeclared icon role leaked: ${role}`);
});
