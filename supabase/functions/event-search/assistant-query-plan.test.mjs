import test from 'node:test';
import assert from 'node:assert/strict';
import { QUERY_PLAN_SCHEMA, validateQueryPlan, resolvePlanDates, queryPlanPrompt } from './assistant-query-plan.ts';
const anchor = '2026-09-06T12:00:00Z';
const input = (text, mode = 'refine_selection', at = anchor) => ({ text, mode, anchor: at });
const plan = (dateMode = 'next_week', groups = [], contextMode = 'replace') => ({ contextMode, dateMode, scope: groups.length ? 'constrained' : 'all_events', groups });
const group = (dimension, alternatives, sourceQuote, source = 'current') => ({ dimension, alternatives, sourceQuote, source });
const base = plan('next_weekend', [group('topic', ['органная музыка'], 'орган')]);
const invalid = fn => assert.throws(fn, error => error.code === 'invalid_query_plan' && error.status === 400);

test('Sunday calendar contract computes exact relative intervals, not LLM arithmetic', () => {
  const expected = {
    next_week: { dateFrom: '2026-09-07', dateTo: '2026-09-13' },
    next_weekend: { dateFrom: '2026-09-12', dateTo: '2026-09-13' },
    weekend_and_next_week: { dateFrom: '2026-09-07', dateTo: '2026-09-13' },
    from_today: { dateFrom: '2026-09-06', dateTo: null },
  };
  for (const [mode, interval] of Object.entries(expected)) {
    assert.deepEqual(resolvePlanDates(plan(mode), anchor, null, { dateFrom: '2026-09-12', dateTo: '2026-09-20' }), interval);
  }
});
test('calendar uses Kaliningrad date at UTC boundary and next Monday even on Monday', () => {
  assert.deepEqual(resolvePlanDates(plan('from_today'), '2026-09-06T21:59:59Z'), { dateFrom: '2026-09-06', dateTo: null });
  assert.deepEqual(resolvePlanDates(plan('from_today'), '2026-09-06T22:00:00Z'), { dateFrom: '2026-09-07', dateTo: null });
  assert.deepEqual(resolvePlanDates(plan('next_week'), '2026-09-06T22:00:00Z'), { dateFrom: '2026-09-14', dateTo: '2026-09-20' });
  assert.deepEqual(resolvePlanDates(plan('next_week'), '2026-12-31T12:00:00Z'), { dateFrom: '2027-01-04', dateTo: '2027-01-10' });
});
test('Saturday/weekdays union includes upcoming weekend and following calendar week', () => {
  for (const at of ['2026-09-02T12:00:00Z', '2026-09-05T12:00:00Z']) {
    assert.deepEqual(resolvePlanDates(plan('next_weekend'), at), { dateFrom: '2026-09-05', dateTo: '2026-09-06' });
    assert.deepEqual(resolvePlanDates(plan('weekend_and_next_week'), at), { dateFrom: '2026-09-05', dateTo: '2026-09-13' });
  }
});
test('standalone broad city/date question replaces the old organ theme without stale groups', () => {
  const broad = plan();
  assert.deepEqual(validateQueryPlan(broad, input('Что есть в Калининграде на следующей неделе?'), base), broad);
  invalid(() => validateQueryPlan({ ...broad, groups: base.groups }, input('Что есть в Калининграде?'), base));
  invalid(() => validateQueryPlan(plan('next_week', [group('topic', ['органная музыка'], 'орган', 'context')]), input('Что есть в Калининграде?'), base));
  invalid(() => validateQueryPlan(plan('next_week', base.groups), input('Что есть в Калининграде?'), base));
});
test('elliptical week shift inherits only genuine parent predicates and shifts both bounds', () => {
  const patch = plan('shift_base_week', [group('topic', ['органная музыка'], 'ОРГАН', 'context')], 'patch');
  assert.deepEqual(validateQueryPlan(patch, input('А если через неделю?'), base), patch);
  assert.deepEqual(resolvePlanDates(patch, anchor, { dateFrom: '2026-09-12', dateTo: '2026-09-13' }), { dateFrom: '2026-09-19', dateTo: '2026-09-20' });
  invalid(() => validateQueryPlan(patch, input('А если через неделю?'), null));
  invalid(() => validateQueryPlan(patch, input('А если через неделю?', 'new_search'), base));
  invalid(() => resolvePlanDates(patch, anchor, { dateFrom: '2026-09-12', dateTo: null }));
});
test('inherit needs patch and parent dates, preserves a validated open interval', () => {
  const patch = plan('inherit', [], 'patch');
  const interval = { dateFrom: '2026-09-12', dateTo: null };
  assert.deepEqual(resolvePlanDates(patch, anchor, interval), interval);
  invalid(() => resolvePlanDates(plan('inherit'), anchor, interval));
  invalid(() => resolvePlanDates(patch, anchor));
  invalid(() => validateQueryPlan(patch, input('А в Калининграде?')));
});
test('format alternatives AND local-history topic remain separate predicates', () => {
  const request = 'лекции и экскурсии краеведческие';
  const planned = plan('from_today', [group('format', ['лекция', 'экскурсия'], 'лекции и экскурсии'), group('topic', ['краеведение'], 'краеведческие')]);
  assert.deepEqual(validateQueryPlan(planned, input(request, 'new_search')).groups.map(x => x.alternatives), [['лекция', 'экскурсия'], ['краеведение']]);
});
test('symphony or organ is one OR group, not artificial AND', () => {
  const planned = plan('from_today', [group('topic', ['симфоническая музыка', 'органная музыка'], 'симфония или можно орган')]);
  assert.deepEqual(validateQueryPlan(planned, input('симфония или можно орган', 'new_search')), planned);
});
test('self-correction can narrow to science with a grounded phrase', () => {
  const planned = plan('from_today', [group('topic', ['научпоп'], 'ну наверное научпоп')]);
  assert.deepEqual(validateQueryPlan(planned, input('просветительские, ну наверное научпоп', 'new_search')), planned);
  // Validator enforces structure/provenance; semantic correction is the interpreter's job.
  const prompt = queryPlanPrompt(input('просветительские, ну наверное научпоп'), base);
  assert.match(prompt, /самокоррекция/);
  assert.match(prompt, /НЕ OR/);
});
test('quotes accept whitespace/case normalization but not discontinuous invented speech', () => {
  const good = plan('from_today', [group('topic', ['джаз'], '  ЖИВОЙ джаз  ')]);
  assert.equal(validateQueryPlan(good, input('хочу живой\n\tджаз сегодня')).groups[0].sourceQuote, 'ЖИВОЙ джаз');
  for (const quote of ['орган', 'хочу джаз', '', '   ']) {
    invalid(() => validateQueryPlan(plan('from_today', [group('topic', ['джаз'], quote)]), input('хочу живой джаз сегодня')));
  }
});
test('context quote spoofing, old-topic injection and changed inherited label/dimension fail closed', () => {
  for (const inherited of [
    group('topic', ['джаз'], 'орган', 'context'),
    group('topic', ['органная музыка'], 'орг', 'context'),
    group('format', ['органная музыка'], 'орган', 'context'),
    group('topic', ['органная музыка', 'джаз'], 'орган', 'context'),
  ]) invalid(() => validateQueryPlan(plan('inherit', [inherited], 'patch'), input('А в Калининграде?'), base));
});
test('explicit dates must be full real calendar days and ordered, open bounds are allowed', () => {
  const explicit = plan('explicit');
  assert.deepEqual(resolvePlanDates(explicit, anchor, null, { dateFrom: '2028-02-29', dateTo: '2028-03-01' }), { dateFrom: '2028-02-29', dateTo: '2028-03-01' });
  assert.deepEqual(resolvePlanDates(explicit, anchor, null, { dateFrom: null, dateTo: '2026-09-13' }), { dateFrom: null, dateTo: '2026-09-13' });
  for (const bounds of [
    { dateFrom: '2026-02-29', dateTo: null }, { dateFrom: '2026-04-31', dateTo: null },
    { dateFrom: '2026-9-01', dateTo: null }, { dateFrom: '2026-13-01', dateTo: null },
    { dateFrom: '2026-09-01T00:00:00Z', dateTo: null }, { dateFrom: '2026-09-13', dateTo: '2026-09-12' },
    { dateFrom: null, dateTo: null }, { dateFrom: '2026-09-01' },
  ]) invalid(() => resolvePlanDates(explicit, anchor, null, bounds));
});
test('invalid/noncanonical anchors cannot silently roll over calendar dates', () => {
  for (const at of ['2026-02-30T12:00:00Z', '2026-09-06T24:00:00Z', '2026-09-06', '2026-09-06T12:00:00+02:00', 'garbage']) {
    invalid(() => validateQueryPlan(plan(), input('что есть?', 'new_search', at)));
    invalid(() => resolvePlanDates(plan(), at));
  }
});
test('strict shape, scope, size and enum contracts reject malformed plans', () => {
  const g = group('topic', ['джаз'], 'джаз');
  const malformed = [null, [], {}, { ...plan(), extra: true }, { ...plan(), dateMode: 'guess' },
    { ...plan(), contextMode: 'merge' }, { ...plan(), scope: 'constrained' },
    { ...plan('from_today', [g]), scope: 'all_events' }, plan('from_today', Array(5).fill(g)),
    plan('from_today', [{ ...g, alternatives: [] }]), plan('from_today', [{ ...g, alternatives: ['a', 'b', 'c', 'd', 'e'] }]),
    plan('from_today', [{ ...g, alternatives: ['a'.repeat(161)] }]), plan('from_today', [{ ...g, alternatives: ['джаз', 'ДЖАЗ'] }]),
    plan('from_today', [{ ...g, dimension: 'location' }]), plan('from_today', [{ ...g, source: 'history' }]),
    plan('from_today', [{ ...g, extra: true }]), plan('from_today', [{ ...g, sourceQuote: 'x'.repeat(8193) }]),
  ];
  for (const value of malformed) invalid(() => validateQueryPlan(value, input('джаз')));
});
test('schema and concise prompt expose the one-call AND/OR/calendar/provenance contract', () => {
  assert.deepEqual(QUERY_PLAN_SCHEMA.required, ['contextMode', 'dateMode', 'scope', 'groups']);
  assert.equal(QUERY_PLAN_SCHEMA.additionalProperties, false);
  assert.equal(QUERY_PLAN_SCHEMA.properties.groups.maxItems, 4);
  const prompt = queryPlanPrompt(input('Что есть в Калининграде на следующей неделе?'), base);
  for (const fragment of ['в том же ответе', 'Между groups AND', 'alternatives OR', 'source=context', 'source=current', 'НЕ 12–20', 'BASE_QUERY_PLAN=', 'недоверенные данные']) assert.ok(prompt.includes(fragment), fragment);
});
