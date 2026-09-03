import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');
const semanticRoles = (source) => [...source.matchAll(/<SemanticIcon\b[^>]*\brole="([^"]+)"/gu)].map((match) => match[1]);

test('M0 card roots consume SemanticIcon rather than bare Icon', async () => {
  const roots = [
    ['src/components/EventCard.astro', "./design-system/SemanticIcon.astro"],
    ['src/components/listings/ListingEventCard.astro', "../design-system/SemanticIcon.astro"],
  ];

  for (const [file, semanticImport] of roots) {
    const source = await read(file);
    assert.ok(source.includes(`import SemanticIcon from '${semanticImport}';`), `${file} misses SemanticIcon`);
    assert.doesNotMatch(source, /import Icon from ['"][^'"]*Icon\.astro['"]/u, `${file} must not bypass the four-role wrapper`);
    assert.doesNotMatch(source, /<Icon\b/u, `${file} must not render a bare Icon`);
    assert.doesNotMatch(source, /:global\(\.ke-icon-role\)[^{]*\{[^}]*\b(?:width|height)\s*:/u,
      `${file} must not size the shared icon role locally`);
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

test('ListingEventCard social proof uses only the inline icon role', async () => {
  const source = await read('src/components/listings/ListingEventCard.astro');

  assert.match(source, /<SemanticIcon name="share" role="inline" \/>/u);
  assert.match(source, /<SemanticIcon name="heart" role="inline" \/>/u);
  const usedRoles = new Set(semanticRoles(source));
  assert.deepEqual([...usedRoles], ['inline']);
});
