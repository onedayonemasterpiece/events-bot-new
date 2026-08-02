import assert from 'node:assert/strict';
import test from 'node:test';

import {
  duplicateValues,
  normalizeBaseUrl,
  normalizeResourcePath,
  normalizeRoutePath,
  redactText,
  redactedBaseDescriptor,
  resolveResourceUrl,
  resolveRouteUrl,
  routeIsListed,
  stripBasePrefix,
} from './check-static-collections-e2e.mjs';

test('normalizes collection routes but preserves resource filenames', () => {
  assert.equal(normalizeRoutePath('/nauka'), '/nauka/');
  assert.equal(normalizeRoutePath('//nauka///'), '/nauka/');
  assert.equal(normalizeResourcePath('/sitemap-index.xml'), '/sitemap-index.xml');
  assert.throws(() => normalizeRoutePath('/../secret/'), /Unsafe route/u);
});

test('resolves routes and resources inside a bearer candidate prefix', () => {
  const base = normalizeBaseUrl('https://kenigevents.ru/_review/secret-token');
  assert.equal(
    resolveRouteUrl(base, '/nauka/').href,
    'https://kenigevents.ru/_review/secret-token/nauka/',
  );
  assert.equal(
    resolveResourceUrl(base, '/sitemap-index.xml').href,
    'https://kenigevents.ru/_review/secret-token/sitemap-index.xml',
  );
  assert.equal(
    stripBasePrefix(
      base,
      'https://kenigevents.ru/_review/secret-token/nauka/',
    ),
    '/nauka/',
  );
});

test('matches both candidate-prefixed and canonical production sitemap URLs', () => {
  const base = normalizeBaseUrl('https://kenigevents.ru/_review/secret-token/');
  assert.equal(
    routeIsListed(
      base,
      ['https://kenigevents.ru/_review/secret-token/nauka/'],
      '/nauka/',
    ),
    true,
  );
  assert.equal(
    routeIsListed(
      base,
      ['https://kenigevents.ru/nauka/'],
      '/nauka/',
    ),
    true,
  );
  assert.equal(
    routeIsListed(
      base,
      ['https://kenigevents.ru/neobychnoe/'],
      '/nauka/',
    ),
    false,
  );
});

test('reports duplicate event/family values deterministically', () => {
  assert.deepEqual(
    duplicateValues(['2', '1', '2', '', '1', '3']),
    ['1', '2'],
  );
});

test('redacted base descriptor never contains the bearer token', () => {
  const descriptor = redactedBaseDescriptor(
    'https://kenigevents.ru/_review/super-secret-token/',
  );
  const serialized = JSON.stringify(descriptor);
  assert.equal(descriptor.value, '[REDACTED]');
  assert.equal(descriptor.path_prefix_depth, 2);
  assert.equal(serialized.includes('super-secret-token'), false);
  assert.equal(
    redactText(
      '<a href="/_review/super-secret-token/nauka/">x</a>',
      'https://kenigevents.ru/_review/super-secret-token/',
    ).includes('super-secret-token'),
    false,
  );
});
