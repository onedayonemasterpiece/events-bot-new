import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

const donor = readFileSync(
  new URL('../src/components/AuthorizedEventSearch.astro', import.meta.url),
  'utf8',
);

function extractFunction(source, name) {
  const start = source.indexOf(`function ${name}(`);
  assert.notEqual(start, -1, `${name} must exist`);
  const openingBrace = source.indexOf('{', start);
  let depth = 0;
  for (let index = openingBrace; index < source.length; index += 1) {
    if (source[index] === '{') depth += 1;
    if (source[index] === '}') depth -= 1;
    if (depth === 0) return source.slice(start, index + 1);
  }
  assert.fail(`${name} must have a balanced function body`);
}

test('Search skeleton is hidden in initial markup independently of backend configuration', () => {
  assert.match(donor, /<div class="authorized-search__skeletons" data-search-skeletons hidden aria-hidden="true">/u);
  assert.doesNotMatch(donor, /data-search-skeletons hidden=\{enabled\}/u);
  assert.doesNotMatch(donor, /Образец состояния загрузки результатов|authorized-search__prototype-label/u);
});

test('runtime loading alone reveals the skeleton and restores the hidden state', () => {
  const source = extractFunction(donor, 'setSkeletonLoading');
  const skeletons = { hidden: true };
  const classState = new Map();
  const root = {
    classList: {
      toggle(name, enabled) {
        classState.set(name, enabled);
      },
    },
  };
  const setSkeletonLoading = new Function(
    'skeletons',
    'root',
    `${source}; return setSkeletonLoading;`,
  )(skeletons, root);

  setSkeletonLoading(true);
  assert.equal(skeletons.hidden, false);
  assert.equal(classState.get('is-search-skeleton-loading'), true);

  setSkeletonLoading(false);
  assert.equal(skeletons.hidden, true);
  assert.equal(classState.get('is-search-skeleton-loading'), false);

  assert.equal((donor.match(/skeletons\.hidden\s*=/gu) || []).length, 1, 'no other script path writes skeleton visibility');
  assert.match(donor, /setSearchLoading\(true, \{ showSkeleton: !append \}\)/u, 'a validated first-page runtime request reveals it');
  assert.doesNotMatch(donor, /setSkeletonLoading\(true\)/u, 'no non-loading call reveals it directly');
});

test('a validated first-page submit releases the mobile input before Search traffic', () => {
  const start = donor.indexOf('async function runSearch');
  const end = donor.indexOf("form?.addEventListener('submit'", start);
  assert.ok(start >= 0 && end > start, 'runSearch source boundary must exist');
  const source = donor.slice(start, end);
  const prepared = source.indexOf('const prepared = prepareSearchRequest');
  const validationReturn = source.indexOf('if (!prepared.ok)', prepared);
  const blur = source.indexOf('if (!append) input?.blur()', validationReturn);
  const request = source.indexOf('const searchRequest = invokeEventSearch', blur);

  assert.ok(prepared >= 0, 'request validation must exist');
  assert.ok(validationReturn > prepared, 'invalid input must return before focus changes');
  assert.ok(blur > validationReturn, 'validated first-page input must be blurred');
  assert.ok(request > blur, 'focus must be released before the cost-bearing POST');
});
