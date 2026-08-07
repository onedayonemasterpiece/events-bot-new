import {
  activityDelta,
  assertCacheState,
  assertExecutionReceipt,
  assertOneSubmitOnePost,
  assertProviderAttempts,
  assertRealScroll,
  assertResponseRenderedIds,
  assertRoute,
  assertTerminalState,
  assertUniqueCards,
  assertZeroPost,
} from './acceptance.mjs';
import { sanitizedTargetPath } from './evidence.mjs';

function validCases(cases) {
  if (!Array.isArray(cases) || cases.length < 3) throw new Error('search_query_cases_require_variety');
  const ids = new Set();
  return cases.map((item, index) => {
    const id = String(item?.id || '').trim();
    const value = String(item?.value || '');
    if (!/^[a-z0-9][a-z0-9_.-]{2,63}$/iu.test(id) || ids.has(id)) throw new Error(`search_query_case_id:${index}`);
    if (value.trim().length < 3 || value.length > 180) throw new Error(`search_query_case_value:${id}`);
    ids.add(id);
    return { id, value, paginate: item.paginate === true };
  });
}

function requiredAdapter(adapter) {
  const methods = [
    'open', 'inspectSurface', 'configureRequestPolicy', 'activity', 'typeQuery', 'clearQuery',
    'readQueryState', 'submitWithSearchIntent', 'waitForTerminal', 'snapshotResults',
    'realScrollResults', 'showMoreState', 'activateShowMore', 'waitForValidation',
  ];
  for (const method of methods) if (typeof adapter?.[method] !== 'function') throw new Error(`search_adapter_method_missing:${method}`);
}

async function acceptedSubmit({ adapter, variant, label, prefixIds = [] }) {
  const before = await adapter.activity();
  await adapter.submitWithSearchIntent();
  await adapter.waitForTerminal({ minimumResponseCount: before.responses.length + 1 });
  const after = await adapter.activity();
  const delta = activityDelta(before, after);
  const counters = assertOneSubmitOnePost(delta, label);
  const response = delta.responses[0];
  const state = await adapter.snapshotResults();
  assertTerminalState(state, label);
  assertUniqueCards(state, label);
  assertResponseRenderedIds(response, state, { prefixIds });
  const route = assertRoute(delta, variant, label);
  return { before, after, delta, counters, response, state, route };
}

/**
 * Semantic Search journey. The adapter owns every input, browser/device,
 * network-observation and scrolling mechanic; this module only sequences user
 * intentions and validates observable product contracts.
 */
export async function runSearchJourney({ adapter, targetUrl, variant, queryCases }) {
  requiredAdapter(adapter);
  const cases = validCases(queryCases);
  await adapter.configureRequestPolicy(variant.request_policy);
  await adapter.open(targetUrl);
  const surface = await adapter.inspectSurface();
  if (!surface.enabled || !surface.authorized) throw new Error('search_surface_not_authorized');
  if (!['input', 'textarea'].includes(surface.input_tag)) throw new Error(`search_surface_input:${surface.input_tag}`);
  if (surface.enter_key_hint !== 'search') throw new Error(`search_surface_enter_key_hint:${surface.enter_key_hint}`);

  const baseline = await adapter.activity();
  await adapter.typeQuery(cases[0].value);
  await adapter.clearQuery();
  const empty = await adapter.readQueryState();
  if (empty.length !== 0) throw new Error(`search_typed_empty_not_empty:${empty.length}`);
  assertZeroPost(baseline, await adapter.activity(), 'typed_empty');

  const results = [];
  for (const queryCase of cases) {
    await adapter.clearQuery();
    await adapter.typeQuery(queryCase.value);
    const initial = await acceptedSubmit({ adapter, variant, label: `${queryCase.id}:initial` });
    assertCacheState(initial.response, variant.expected_cache_state, `${queryCase.id}:initial`);
    assertExecutionReceipt(initial.response, variant.request_policy.execution_mode, `${queryCase.id}:initial`);
    assertProviderAttempts(initial.response, variant.allowed_provider_attempts);

    const scroll = await adapter.realScrollResults();
    assertRealScroll(scroll);
    const pageReceipts = [{
      request_count: initial.counters.request_count,
      response_count: initial.counters.response_count,
      route: initial.route,
      response: initial.response,
      rendered_ids: initial.state.rendered_ids,
    }];

    if (queryCase.paginate) {
      const more = await adapter.showMoreState();
      if (!more.visible || !more.enabled) throw new Error(`search_show_more_missing:${queryCase.id}`);
      const prefixIds = [...initial.state.rendered_ids];
      const before = await adapter.activity();
      await adapter.activateShowMore();
      await adapter.waitForTerminal({ minimumResponseCount: before.responses.length + 1, minimumCardCount: prefixIds.length + 1 });
      const after = await adapter.activity();
      const delta = activityDelta(before, after);
      const counters = assertOneSubmitOnePost(delta, `${queryCase.id}:show_more`);
      const response = delta.responses[0];
      assertExecutionReceipt(response, variant.request_policy.execution_mode, `${queryCase.id}:show_more`);
      assertProviderAttempts(response, variant.allowed_provider_attempts);
      const state = await adapter.snapshotResults();
      assertTerminalState(state, `${queryCase.id}:show_more`);
      assertUniqueCards(state, `${queryCase.id}:show_more`);
      assertResponseRenderedIds(response, state, { prefixIds });
      const route = assertRoute(delta, variant, `${queryCase.id}:show_more`);
      pageReceipts.push({ request_count: counters.request_count, response_count: counters.response_count, route,
        response, rendered_ids: state.rendered_ids });
    }

    await adapter.clearQuery();
    await adapter.typeQuery(queryCase.value);
    const cachePolicy = { ...variant.request_policy, execution_mode: 'cached_vector', cache: 'prefer', llm: 'forbid' };
    await adapter.configureRequestPolicy(cachePolicy);
    const warm = await acceptedSubmit({ adapter, variant, label: `${queryCase.id}:cache_warm` });
    assertCacheState(warm.response, ['hit', 'miss', 'stored'], `${queryCase.id}:cache_warm`);
    assertExecutionReceipt(warm.response, 'cached_vector', `${queryCase.id}:cache_warm`);
    assertProviderAttempts(warm.response, { embedding: 1, vector: 1, llm: 0 });
    await adapter.clearQuery();
    await adapter.typeQuery(queryCase.value);
    const repeat = await acceptedSubmit({ adapter, variant, label: `${queryCase.id}:cache_repeat` });
    assertCacheState(repeat.response, ['hit'], `${queryCase.id}:cache_repeat`);
    assertExecutionReceipt(repeat.response, 'cached_vector', `${queryCase.id}:cache_repeat`);
    if (!repeat.response.served_from_cache) throw new Error(`search_cache_receipt_missing:${queryCase.id}`);
    assertProviderAttempts(repeat.response, { embedding: 0, vector: 0, llm: 0 });
    await adapter.configureRequestPolicy(variant.request_policy);

    results.push({
      query_id: queryCase.id,
      pagination_required: queryCase.paginate,
      pages: pageReceipts,
      cache_repeat: {
        request_count: repeat.counters.request_count,
        response_count: repeat.counters.response_count,
        route: repeat.route,
        response: repeat.response,
        rendered_ids: repeat.state.rendered_ids,
      },
      cache_warm: {
        request_count: warm.counters.request_count,
        response_count: warm.counters.response_count,
        route: warm.route,
        response: warm.response,
        rendered_ids: warm.state.rendered_ids,
      },
      scroll,
    });
  }

  await adapter.clearQuery();
  await adapter.typeQuery('x');
  const invalidBefore = await adapter.activity();
  await adapter.submitWithSearchIntent();
  const validation = await adapter.waitForValidation();
  const invalidAfter = await adapter.activity();
  assertZeroPost(invalidBefore, invalidAfter, 'invalid_query');
  if (!validation.visible || validation.kind !== 'error') throw new Error('search_validation_terminal_missing');
  await adapter.clearQuery();

  const finalActivity = await adapter.activity();
  return {
    status: 'PASS',
    target_origin: new URL(targetUrl).origin,
    target_path: sanitizedTargetPath(new URL(targetUrl).pathname),
    surface,
    query_cases: results,
    counters: {
      requests: finalActivity.requests.length - baseline.requests.length,
      responses: finalActivity.responses.length - baseline.responses.length,
      routes: finalActivity.routes.length - baseline.routes.length,
      validation_posts: 0,
    },
  };
}
