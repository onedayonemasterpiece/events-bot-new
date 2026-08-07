import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

const search = readFileSync(
  new URL('../src/components/AuthorizedEventSearch.astro', import.meta.url),
  'utf8',
);

function extractFunction(source, name) {
  const start = source.indexOf(`function ${name}(`);
  assert.notEqual(start, -1, `${name} must exist`);
  const parameterEnd = source.indexOf(') {', start);
  const openingBrace = parameterEnd >= 0 ? parameterEnd + 2 : source.indexOf('{', start);
  let depth = 0;
  for (let index = openingBrace; index < source.length; index += 1) {
    if (source[index] === '{') depth += 1;
    if (source[index] === '}') depth -= 1;
    if (depth === 0) return source.slice(start, index + 1);
  }
  assert.fail(`${name} must have a balanced function body`);
}

function variantHelpers() {
  const source = [
    extractFunction(search, 'normalizeSearchExecutionMode'),
    extractFunction(search, 'searchExecutionModeRequestOptions'),
  ].join('\n');
  return new Function(
    'searchExecutionModes',
    `${source}; return { normalizeSearchExecutionMode, searchExecutionModeRequestOptions };`,
  )(new Set(['cached_vector', 'cold_vector', 'cold_vector_llm', 'degraded_vector_fallback']));
}

test('Search execution control is secret-candidate-only and accepts a closed vocabulary', () => {
  assert.match(search, /const searchExecutionModeControl = import\.meta\.env\.PUBLIC_SITE_MODE === 'secret_candidate'/u);
  assert.match(search, /data-search-execution-mode-control=\{searchExecutionModeControl \? 'true' : 'false'\}/u);
  assert.match(search, /params\.get\('search_variant'\)/u);

  const { normalizeSearchExecutionMode } = variantHelpers();
  for (const mode of ['cached_vector', 'cold_vector', 'cold_vector_llm', 'degraded_vector_fallback']) {
    assert.equal(normalizeSearchExecutionMode(mode), mode);
  }
  for (const unsafe of ['', 'vector', 'cold_vector_llm_extra', 'COLD_VECTOR', 'cold_vector?cache=off']) {
    assert.equal(normalizeSearchExecutionMode(unsafe), '');
  }

  const resolverSource = extractFunction(search, 'resolveSearchExecutionMode');
  const resolver = (dataset, searchValue) => new Function(
    'root',
    'window',
    'normalizeSearchExecutionMode',
    `${resolverSource}; return resolveSearchExecutionMode();`,
  )({ dataset }, { location: { search: searchValue } }, normalizeSearchExecutionMode);

  const productionDataset = { searchExecutionModeControl: 'false', searchExecutionMode: 'cold_vector' };
  assert.equal(resolver(productionDataset, '?search_variant=cold_vector'), '');
  assert.equal(Object.hasOwn(productionDataset, 'searchExecutionMode'), false, 'production removes injected mode state');

  const candidateDataset = { searchExecutionModeControl: 'true' };
  assert.equal(resolver(candidateDataset, '?search_variant=cold_vector_llm'), 'cold_vector_llm');
  assert.equal(candidateDataset.searchExecutionMode, 'cold_vector_llm');

  const rejectedDataset = { searchExecutionModeControl: 'true' };
  assert.equal(resolver(rejectedDataset, '?search_variant=vector'), '');
  assert.equal(Object.hasOwn(rejectedDataset, 'searchExecutionMode'), false);
});

test('vector-only modes retain the legacy no-LLM guard while regular Search keeps its default', () => {
  const { searchExecutionModeRequestOptions } = variantHelpers();
  assert.deepEqual(searchExecutionModeRequestOptions('cached_vector'), {
    use_llm_verifier: false,
    allow_llm_fallback: false,
  });
  assert.deepEqual(searchExecutionModeRequestOptions('cold_vector'), {
    use_llm_verifier: false,
    allow_llm_fallback: false,
  });
  assert.deepEqual(searchExecutionModeRequestOptions('cold_vector_llm'), {
    use_llm_verifier: true,
    allow_llm_fallback: false,
  });
  assert.deepEqual(searchExecutionModeRequestOptions('degraded_vector_fallback'), {
    use_llm_verifier: true,
    allow_llm_fallback: false,
  });
  assert.deepEqual(searchExecutionModeRequestOptions(''), {
    use_llm_verifier: true,
    allow_llm_fallback: false,
  });
});

test('validated request preparation yields zero POST opportunity and variants persist through pagination', () => {
  const prepare = extractFunction(search, 'prepareSearchRequest');
  const prepareValidated = extractFunction(search, 'prepareValidatedSearchRequest');
  assert.match(prepare, /const validation = validateSearchInput\(value\)/u);
  assert.match(prepare, /if \(!validation\.ok\) return \{ \.\.\.validation, body: null \}/u);
  assert.match(prepareValidated, /client_request_id: createSearchClientRequestId\(\)/u);
  assert.match(prepareValidated, /body\.execution_mode = mode/u);

  const run = extractFunction(search, 'runSearch');
  assert.match(run, /const prepared = prepareSearchRequest\(input\?\.value \|\| '', append \? offset : 0, append \? activeExecutionMode : resolveSearchExecutionMode\(\)\)/u);
  assert.match(run, /if \(!prepared\.ok\) \{[\s\S]*?setSearchLifecycle\('validation'\)[\s\S]*?return;/u);
  assert.match(run, /activeExecutionMode = prepared\.executionMode/u);
  assert.match(run, /invokeEventSearch\(\s*prepared\.body,/u);
  assert.doesNotMatch(run, /execution_mode:\s*(?:root|window|location|params)/u);

  const validationIndex = run.indexOf('if (!prepared.ok)');
  const invokeIndex = run.indexOf('invokeEventSearch(');
  assert.ok(validationIndex >= 0 && validationIndex < invokeIndex, 'validation must terminate before the only POST path');

  const executable = [
    extractFunction(search, 'normalizeSearchExecutionMode'),
    extractFunction(search, 'searchExecutionModeRequestOptions'),
    extractFunction(search, 'createSearchClientRequestId'),
    prepareValidated,
    prepare,
  ].join('\n');
  const prepareSearchRequest = new Function(
    'searchExecutionModes',
    'validateSearchInput',
    `${executable}; return prepareSearchRequest;`,
  )(
    new Set(['cached_vector', 'cold_vector', 'cold_vector_llm', 'degraded_vector_fallback']),
    (value) => String(value).trim().length < 3
      ? { ok: false, message: 'Введите хотя бы 3 символа.' }
      : { ok: true, query: String(value).trim() },
  );

  let postCount = 0;
  const invalid = prepareSearchRequest('x', 0, 'cold_vector');
  if (invalid.body) postCount += 1;
  assert.equal(invalid.ok, false);
  assert.equal(invalid.body, null);
  assert.equal(postCount, 0, 'validation cannot produce a cost-bearing POST body');

  const first = prepareSearchRequest('джаз на выходных', 0, 'cold_vector');
  const next = prepareSearchRequest('джаз на выходных', 8, first.executionMode);
  assert.equal(first.body.execution_mode, 'cold_vector');
  assert.equal(next.body.execution_mode, 'cold_vector', 'pagination carries the selected mode');
  assert.equal(first.body.use_llm_verifier, false);
  assert.equal(next.body.offset, 8);
  assert.match(first.body.client_request_id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u);
  assert.notEqual(first.body.client_request_id, next.body.client_request_id, 'each gesture owns one request identity');

  const regular = prepareSearchRequest('джаз на выходных', 0, 'not_allowed');
  assert.equal(Object.hasOwn(regular.body, 'execution_mode'), false, 'regular product requests omit the canary override');
  assert.equal(regular.body.use_llm_verifier, true, 'default product behavior remains unchanged');
});

test('DOM evidence exposes only lifecycle and opaque identity markers', () => {
  assert.match(search, /data-search-state="idle" data-search-terminal="false"/u);
  assert.match(search, /root\.dataset\.searchState = state/u);
  assert.match(search, /results\.dataset\.responseIds/u);
  assert.match(search, /results\.dataset\.responseFamilyIds/u);
  assert.match(search, /node\.dataset\.searchResponseId/u);
  assert.match(search, /node\.dataset\.searchFamilyId/u);
  assert.doesNotMatch(search, /dataset\.searchQuery|dataset\.responseTitles|dataset\.searchCardContent/u);
});
