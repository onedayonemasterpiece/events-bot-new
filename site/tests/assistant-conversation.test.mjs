import { test } from 'node:test';
import assert from 'node:assert/strict';
import { initialState, applyIntentPatch, acceptInput, interpretInput, retrievalTicket,
  commitAnswer, resetTask, failDraft, visibleMembers, resolveOrdinal } from '../src/lib/assistant/conversationState.ts';
const input = (s, text = 'бесплатно', mode = s.draft ? 'continue_draft' : 'new_search', parentId = s.draft?.parentId ?? null) => ({
  id: `u${s.epoch}:${s.acceptedThrough + 1}`, epoch: s.epoch, sequence: s.acceptedThrough + 1,
  previousId: s.receipts.at(-1)?.id ?? null, mode, parentId, text });
const heard = (s, text, patch, mode, parent) => {
  const i = input(s, text, mode, parent); s = acceptInput(s, i); return interpretInput(s, i.id, patch, s.revision);
};
const ready = () => heard(initialState(), 'бесплатно', {freeOnly:true});
const answer = (s, id='s1', eventIds=['fixture-a','fixture-b']) => commitAnswer(s, retrievalTicket(s), {
  id, title:'Бесплатные события', answer:'Данные тестового корпуса', eventIds, catalogRevision:'fixture-v1'
}, ['fixture-a','fixture-b','fixture-c']);
const throws = (fn, code) => assert.throws(fn, e => e.code === code);

test('price and city replacements retain exclusions, input not mutated', () => {
  const base = {goal:'вечером', localityIds:['svetlogorsk'], excludedFormats:['concert'], freeOnly:true, maxPrice:null};
  const next = applyIntentPatch(base, {localityIds:['kaliningrad'], freeOnly:false, maxPrice:500});
  assert.deepEqual(next.localityIds, ['kaliningrad']); assert.deepEqual(next.excludedFormats,['concert']);
  assert.equal(base.freeOnly,true); assert.equal(next.maxPrice,500);
});
for (const patch of [{freeOnly:1},{maxPrice:NaN},{maxPrice:-1},{maxPrice:Infinity},{goal:7},{admin:true}, {localityIds:['../secret']}, {goal:undefined}]) {
  test(`reject malformed patch ${JSON.stringify(patch)}`, () => assert.throws(() => applyIntentPatch(initialState().activeIntent,patch)));
}
test('conflicting price is explicit failure, no silently removed constraint', () => throws(() => applyIntentPatch(ready().activeIntent,{maxPrice:500}),'conflicting_price'));
test('known ID lists are replaced and deduplicated',()=>assert.deepEqual(applyIntentPatch(initialState().activeIntent,{localityIds:['a','a']}).localityIds,['a']));
test('idempotent logical input returns same state, payload conflict rejected', () => {
  const s=initialState(), i=input(s); const a=acceptInput(s,i);
  assert.equal(acceptInput(a,{...i}),a); throws(()=>acceptInput(a,{...i,text:'paid'}),'payload_conflict'); assert.equal(s.receipts.length,0);
});
test('out-of-order intake rejects without claiming acceptance',()=>throws(()=>acceptInput(initialState(),{...input(initialState()),sequence:2}),'sequence_conflict'));
test('wrong predecessor rejected',()=>throws(()=>acceptInput(initialState(),{...input(initialState()),previousId:'missing'}),'predecessor_conflict'));
test('foreign/missing parent rejected',()=>throws(()=>acceptInput(initialState(),input(initialState(),'coast','refine_selection','foreign')),'parent_not_found'));
test('new search cannot silently inherit parent',()=>{
 const s=answer(ready()); throws(()=>acceptInput(s,input(s,'new','new_search','s1')),'new_search_has_parent');
});
test('dogon during interpretation retains each utterance and order',()=>{
 let s=initialState(),i1=input(s);s=acceptInput(s,i1);const oldRevision=s.revision;
 const i2=input(s,'не концерт');s=acceptInput(s,i2);
 throws(()=>interpretInput(s,i1.id,{freeOnly:true},oldRevision),'revision_conflict');
 throws(()=>interpretInput(s,i2.id,{excludedFormats:['concert']},s.revision),'interpretation_order');
 s=interpretInput(s,i1.id,{freeOnly:true},s.revision); throws(()=>retrievalTicket(s),'not_ready');
 s=interpretInput(s,i2.id,{excludedFormats:['concert']},s.revision); s=answer(s);
 assert.equal(s.sections[0].question,'бесплатно\nне концерт');assert.equal(s.sections[0].intent.freeOnly,true);
 assert.deepEqual(s.sections[0].intent.excludedFormats,['concert']);
});
test('dogon after retrieval launch invalidates its result',()=>{
 let s=ready();const old=retrievalTicket(s);s=heard(s,'на побережье',{localityIds:['coast']});
 throws(()=>commitAnswer(s,old,{id:'s1',title:'x',answer:'',eventIds:[],catalogRevision:'v1'},[]),'stale_result');
 assert.equal(answer(s).sections.length,1);
});
test('committed answer is retained, old-section branch appends chronologically',()=>{
 let s=answer(ready()); const first=structuredClone(s.sections[0]);
 s=heard(s,'на побережье',{localityIds:['coast']},'refine_selection','s1'); s=answer(s,'s2',['fixture-b']);
 s=heard(s,'не концерт',{excludedFormats:['concert']},'refine_selection','s1');s=answer(s,'s3',['fixture-a']);
 assert.deepEqual(s.sections[0],first);assert.deepEqual(s.sections.map(x=>x.parentId),[null,'s1','s1']);
});
test('subset covers parent beyond visible page but cannot introduce foreign member',()=>{
 let s=answer(ready());s=heard(s,'coast',{},'refine_selection','s1');assert.deepEqual(answer(s,'s2',['fixture-b']).sections[1].eventIds,['fixture-b']);
 throws(()=>answer(s,'s2',['fixture-c']),'subset_expanded');
});
test('parent catalog revision mismatch demands explicit refresh',()=>{
 let s=heard(answer(ready()),'coast',{},'refine_selection','s1');
 throws(()=>commitAnswer(s,retrievalTicket(s),{id:'s2',title:'x',answer:'',eventIds:[],catalogRevision:'new-v'},[]),'parent_revision_changed');
});
test('model IDs require independent eligible allowlist',()=>{
 const s=ready();throws(()=>commitAnswer(s,retrievalTicket(s),{id:'s1',title:'x',answer:'',eventIds:['foreign'],catalogRevision:'v1'},[]),'untrusted_result_id');
});
test('explanation is valid with no event grid',()=>{
 let s=heard(answer(ready()),'адрес',{},'explain_selection','s1');s=answer(s,'s2',[]);assert.equal(s.sections.length,2);assert.deepEqual(s.sections[1].eventIds,[]);
});
test('completed result cannot append twice',()=>{
 const s=ready(), t=retrievalTicket(s);const a={id:'s1',title:'x',answer:'',eventIds:[],catalogRevision:'v1'};
 const n=commitAnswer(s,t,a,[]);throws(()=>commitAnswer(n,t,a,[]),'stale_result');
});
test('global hide overlay and ordinal source are explicit, history is immutable',()=>{
 const s=answer(ready()), section=s.sections[0];const shown=visibleMembers(section,new Set(['fixture-a']));
 assert.deepEqual(shown,['fixture-b']);assert.equal(resolveOrdinal(shown,1),'fixture-b');assert.equal(section.eventIds.length,2);
 throws(()=>resolveOrdinal(shown,2),'invalid_ordinal');assert.deepEqual(visibleMembers(section,new Set()),section.eventIds);
});
test('unknown dispatch does not silently reopen work',()=>{
 let s=ready();s=failDraft(s,retrievalTicket(s),'provider_outcome_unknown',true);
 throws(()=>retrievalTicket(s),'not_ready');throws(()=>acceptInput(s,input(s)),'draft_needs_resolution');
});
test('reset invalidates all late work; delete removes history',()=>{
 let s=ready(),t=retrievalTicket(s),d=s.draft.id;s=resetTask(s);
 assert.equal(failDraft(s,t,'late',false),s);throws(()=>commitAnswer(s,t,{id:'s1',title:'x',answer:'',eventIds:[],catalogRevision:'v1'},[]),'stale_result');
 let full=answer(ready());assert.equal(resetTask(full).sections.length,1);assert.equal(resetTask(full,true).sections.length,0);
});
test('mutating output cannot mutate prior state or accepted input',()=>{
 const s=initialState(),i=input(s);const a=acceptInput(s,i);i.text='hacked';assert.equal(a.receipts[0].text,'бесплатно');
 const b=interpretInput(a,a.receipts[0].id,{localityIds:['coast']},a.revision);b.receipts[0].text='other';assert.equal(a.receipts[0].text,'бесплатно');
});
test('property: every accepted prefix survives reordered retrieval completions',()=>{
 for(let n=1;n<=50;n++){
  let s=ready();const old=retrievalTicket(s);
  for(let j=0;j<n;j++)s=heard(s,`refinement ${j}`,{goal:`goal ${j}`});
  throws(()=>commitAnswer(s,old,{id:'stale',title:'x',answer:'',eventIds:[],catalogRevision:'v1'},[]),'stale_result');
  const result=answer(s);assert.equal(result.receipts.length,n+1);assert.equal(result.sections.length,1);
  assert.equal(result.sections[0].intent.goal,`goal ${n-1}`);
 }
});

test('explicit expansion inherits intent but may broaden parent membership',()=>{
 let s=answer(ready());s=heard(s,'можно платно',{freeOnly:false,maxPrice:500},'expand_selection','s1');
 s=answer(s,'s2',['fixture-c']);assert.equal(s.sections[1].intent.maxPrice,500);assert.equal(s.sections[1].parentId,'s1');
});
test('stale stage failure cannot invalidate the newer draft revision',()=>{
 let s=ready();const t=retrievalTicket(s);s=heard(s,'coast',{localityIds:['coast']});
 assert.equal(failDraft(s,t,'late_timeout',true),s);
});
