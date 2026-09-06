import test from 'node:test';import assert from 'node:assert/strict';
import {structuredInterpretation,structuredInterpretationSchema,sourceFragments,STRUCTURED_INTERPRETATION_SCHEMA} from './assistant-intent.ts';
import {initialState} from './assistant-dialogue.ts';
const base={...initialState().activeIntent,goal:'экскурсии краеведческие',dateFrom:'2026-09-07',dateTo:'2026-09-13',timeOfDay:null,audience:[],timezone:'Europe/Kaliningrad'};
const input={text:'Какие события пройдут в Светлогорске на следующей неделе?',anchor:'2026-09-06T15:09:37.000Z',mode:'expand_selection',parentId:crypto.randomUUID(),previousId:null,visibleIds:[]};
const value={intent:{...base,localityIds:['svetlogorsk'],dateFrom:'2026-09-14',dateTo:'2026-09-20'},title:'События в Светлогорске',responseSummary:'На 14–20 сентября',clarification:null,explanationKind:'none',ordinal:null,queryPlan:{contextMode:'replace',dateMode:'next_week',scope:'all_events',groups:[]}};
test('structured calendar overrides LLM date arithmetic and embedding goal cannot retain prior topic',()=>{
 const r=structuredInterpretation(value,input,base);assert.equal(r.intent.dateFrom,'2026-09-07');assert.equal(r.intent.dateTo,'2026-09-13');assert.equal(r.intent.goal,'события');assert.equal(r.responseSummary,null);assert.match(r.title,/7 сентября/);assert.doesNotMatch(r.title,/14/);assert.equal(r.queryPlan.scope,'all_events');
});
test('production structured schema requires plan and rejects missing or hidden audience predicates',()=>{
 assert.ok(STRUCTURED_INTERPRETATION_SCHEMA.required.includes('queryPlan'));
 for(const v of [{...value,queryPlan:undefined},{...value,intent:{...value.intent,audience:['family']}}]) assert.throws(()=>structuredInterpretation(v,input,base),e=>e.code==='invalid_query_plan');
});
test('full predicates survive independent from bounded embedding hint',()=>{
 const speech='Лекции и экскурсии краеведческие на следующей неделе';
 const queryPlan={contextMode:'replace',dateMode:'next_week',scope:'constrained',groups:[{dimension:'format',alternatives:['лекция','экскурсия'],source:'current',sourceQuote:'Лекции и экскурсии'},{dimension:'topic',alternatives:['краеведение'],source:'current',sourceQuote:'краеведческие'}]};
 const r=structuredInterpretation({...value,queryPlan},{...input,text:speech},base);assert.equal(r.intent.goal,'(лекция ИЛИ экскурсия) И (краеведение)');assert.deepEqual(r.queryPlan,queryPlan);
});

test('relative mode is authoritative even if model emits reversed or malformed date strings',()=>{
 const r=structuredInterpretation({...value,intent:{...value.intent,dateFrom:'nonsense',dateTo:'2026-01-01'}},input,base);
 assert.equal(r.intent.dateFrom,'2026-09-07');assert.equal(r.intent.dateTo,'2026-09-13');
});

test('provider quote enum covers literal input chunks; examples cannot leak into copied evidence',()=>{
 const q='Какие просветительские мероприятия, ну, наверное, научпоп мероприятия, будут на следующей неделе?';
 const schema=structuredInterpretationSchema({...input,text:q},null);assert.deepEqual(schema.properties.queryPlan.properties.groups.items.properties.sourceQuote.enum,[q]);
 const long='много разных слов '.repeat(80);assert.equal(sourceFragments(long).join(' '),long.trim());assert.ok(sourceFragments(long).every(p=>p.length<=240));
});

test('open date bounds are visible as from/until, not a misleading single-day title',()=>{
 const r=structuredInterpretation({...value,queryPlan:{...value.queryPlan,dateMode:'from_today'}},input,base);assert.match(r.title,/с 6 сентября/);assert.equal(r.intent.dateTo,null);
});

test('model may replace topical groups yet carry the previous period as a conversational continuation',()=>{
 const q='Подбери экскурсии и лекции краеведческого характера.';
 const parentPlan={contextMode:'replace',dateMode:'next_week',scope:'constrained',groups:[{dimension:'topic',alternatives:['научпоп'],source:'current',sourceQuote:'научпоп'}]};
 const groups=[{dimension:'format',alternatives:['экскурсия','лекция'],source:'current',sourceQuote:q},{dimension:'topic',alternatives:['краеведение'],source:'current',sourceQuote:q}];
 const r=structuredInterpretation({...value,queryPlan:{contextMode:'patch',dateMode:'inherit',scope:'constrained',groups}},{...input,text:q},base,parentPlan);
 assert.equal(r.intent.dateFrom,'2026-09-07');assert.equal(r.intent.dateTo,'2026-09-13');assert.doesNotMatch(r.intent.goal,/научпоп/);assert.deepEqual(r.queryPlan.groups,groups);
});
