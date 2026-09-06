import test from 'node:test';import assert from 'node:assert/strict';
import {planVerifierSchema,planVerifierPrompt,classifyPlanPayload} from './assistant-plan-verification.ts';
const rows=[{event_id:1,title:'Историческая выставка'},{event_id:2,title:'Краеведческая лекция'},{event_id:3,title:'Бытовые советы'}];
const plan={scope:'constrained',groups:[{dimension:'format',alternatives:['лекция','экскурсия']},{dimension:'topic',alternatives:['краеведение']} ]};
const facts=new Map(rows.map(c=>[c.event_id,'Тип: событие\nОписание: факты']));
const v=(verdict)=>({verdict,fact_index:1,relation:'direct'});
test('AND across format/topic rejects thematic exhibition; OR alternatives live inside each group',()=>{
 const result=classifyPlanPayload({classifications:{1:{g0:v('rejected'),g1:v('exact')},2:{g0:v('exact'),g1:v('exact')},3:{g0:v('exact'),g1:v('rejected')}}},rows,plan,facts);
 assert.equal(result.status,'ok');assert.deepEqual(result.exact.map(c=>c.event_id),[2]);assert.deepEqual(result.rejected_ids,[1,3]);assert.ok(result.group_evidence['1']);
});
test('missing predicate or fabricated source line never becomes exact',()=>{
 for(const item of [{g0:v('exact')},{g0:v('exact'),g1:{verdict:'exact',fact_index:-1}},{g0:v('exact'),g1:{verdict:'exact',fact_index:999}}]){
  const r=classifyPlanPayload({classifications:{1:item}},rows.slice(0,1),plan,facts);assert.notEqual(r.status,'ok');
 }
});
test('schema requires every candidate and every independent group; prompt does not recalculate dates',()=>{
 const s=planVerifierSchema(rows,plan).properties.classifications;assert.deepEqual(s.required,['1','2','3']);assert.deepEqual(s.properties['1'].required,['g0','g1']);
 const prompt=planVerifierPrompt(plan,{},[]);assert.match(prompt,/groups соединены И/);assert.match(prompt,/не всякая полезная лекция/);assert.match(prompt,/Не вычисляй/);
});

test('parent festival context cannot admit a subevent as a direct festival',()=>{
 const r=classifyPlanPayload({classifications:{1:{g0:{...v('exact'),relation:'context_only'},g1:v('exact')}}},rows.slice(0,1),plan,facts);assert.equal(r.status,'ok');assert.equal(r.exact.length,0);assert.deepEqual(r.rejected_ids,[1]);
});

test('schema complexity cap preserves every group and the complete candidate window',async()=>{
 const {planVerifierBatchSize}=await import('./assistant-plan-verification.ts');
 const {verifyVoiceWindow}=await import('./assistant-verification.ts');
 assert.deepEqual([0,1,2,3,4].map(n=>planVerifierBatchSize({groups:Array(n).fill({dimension:'topic',alternatives:['тема']})})),[20,20,20,13,10]);
 const candidates=Array.from({length:59},(_,i)=>({event_id:i+1}));
 for(const n of [3,4]){
  const batches=[];const r=await verifyVoiceWindow(candidates,async batch=>{batches.push(batch.length);return{used:true,status:'ok',exact:batch,possible:[],rejected_ids:[]};},{batchSize:planVerifierBatchSize({groups:Array(n).fill({})})});
  assert.deepEqual(batches,n===3?[13,13,13,13,7]:[10,10,10,10,10,9]);assert.equal(r.verification.checked_count,59);assert.equal(r.exact.length,59);
 }
 let calls=0;const failed=await verifyVoiceWindow(candidates,async batch=>++calls===2?{used:false,status:'provider_failed'}:{used:true,status:'ok',exact:batch,possible:[],rejected_ids:[]},{batchSize:13});
 assert.equal(failed.verification.status,'unavailable');assert.equal(failed.verification.checked_count,13);assert.equal(failed.verification.unchecked_ids.length,46);assert.deepEqual(failed.exact,[]);
});
