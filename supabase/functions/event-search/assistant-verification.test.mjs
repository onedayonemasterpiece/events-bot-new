import test from 'node:test';
import assert from 'node:assert/strict';
import { classifyVoicePayload, classifyVoiceSchemaPayload, voiceVerifierSchema, verifyVoiceWindow, voiceVerifierPrompt } from './assistant-verification.ts';
const candidates=[{event_id:7422,title:'Swing standards',category:'concert'},{event_id:8680,title:'Rap concert',place:'City Jazz Club'},{event_id:7410,title:'Exhibition'},{event_id:4,title:'Classical music'},{event_id:5,title:'Related music festival'}];
const payload={query_interpretation:'Jazz repertoire on the weekend',exact_event_ids:[7422],possible_event_ids:[5],rejected_event_ids:[8680,7410,4]};
test('verifier contract accepts nonliteral jazz evidence and rejects rap in Jazz venue, exhibition, classical; possible stays separate',async()=>{
 const result=await verifyVoiceWindow(candidates,async batch=>classifyVoicePayload(payload,batch));
 assert.equal(result.verification.status,'complete');assert.deepEqual(result.exact.map(x=>x.event_id),[7422]);assert.deepEqual(result.verification.rejected_ids,[8680,7410,4]);assert.deepEqual(result.verification.possible_ids,[5]);assert.deepEqual(result.verification.unchecked_ids,[]);
});
test('complete window is batched without silent prefix success',async()=>{
 const rows=Array.from({length:50},(_,i)=>({event_id:i+1}));const sizes=[];
 const r=await verifyVoiceWindow(rows,async batch=>{sizes.push(batch.length);return classifyVoicePayload({exact_event_ids:batch.map(x=>x.event_id),possible_event_ids:[],rejected_event_ids:[]},batch)});
 assert.deepEqual(sizes,[20,20,10]);assert.equal(r.exact.length,50);assert.equal(r.verification.checked_count,50);
});
test('partial/conflicting/unknown classification is unavailable, never false zero or unchecked exact',async()=>{
 for(const p of [{exact_event_ids:[7422],possible_event_ids:[],rejected_event_ids:[]},{...payload,possible_event_ids:[7422,5]},{...payload,exact_event_ids:[999999]}]){
  const r=await verifyVoiceWindow(candidates,async b=>classifyVoicePayload(p,b));
  assert.equal(r.verification.status,'unavailable');assert.ok(r.verification.unchecked_ids.length);assert.equal(r.exact.length,0);
 }
});
test('disabled, quota and timeout fail closed preserving full unchecked membership',async()=>{
 for(const status of ['disabled','degraded:quota_rpm','degraded:llm_provider_timeout']){
  const r=await verifyVoiceWindow(candidates,async()=>({used:false,status,exact:[],possible:candidates,rejected_ids:[]}));
  assert.equal(r.verification.status,'unavailable');assert.equal(r.verification.failure_reason,status);assert.deepEqual(r.verification.unchecked_ids,candidates.map(x=>x.event_id));assert.deepEqual(r.exact,[]);assert.deepEqual(r.verification.possible_ids,[]);
 }
});
test('later batch timeout preserves first batch accounting but admits no partially verified result set',async()=>{
 const rows=Array.from({length:21},(_,i)=>({event_id:i+1}));let calls=0;
 const r=await verifyVoiceWindow(rows,async batch=>++calls===1?classifyVoicePayload({exact_event_ids:batch.map(x=>x.event_id),possible_event_ids:[],rejected_event_ids:[]},batch):{used:false,status:'timeout'});
 assert.equal(r.verification.checked_count,20);assert.deepEqual(r.verification.unchecked_ids,[21]);assert.equal(r.verification.exact_ids.length,20);assert.deepEqual(r.exact,[]);
});
test('empty current candidate window is a complete checked empty set',async()=>{
 const r=await verifyVoiceWindow([],async()=>{throw Error('must not send')});assert.equal(r.verification.status,'complete');assert.equal(r.verification.checked_count,0);
});
test('prompt contains complete typed intent, explicit genre-vs-venue guard and uncertainty rule',()=>{
 const p=voiceVerifierPrompt({goal:'джаз',dateFrom:'2026-09-12',dateTo:'2026-09-13'},candidates);
 assert.match(p,/2026-09-13/);assert.match(p,/название площадки/);assert.match(p,/possible_event_ids/);assert.match(p,/недоверенные данные/);
});

test('duplicate IDs within one bucket and malformed arrays cannot become complete exact results',async()=>{
 for(const p of [{...payload,exact_event_ids:[7422,7422]},{...payload,possible_event_ids:null},{...payload,rejected_event_ids:[8680,7410,4,999999]}]){
  const r=await verifyVoiceWindow(candidates,async b=>classifyVoicePayload(p,b));assert.equal(r.verification.status,'unavailable');assert.deepEqual(r.exact,[]);
 }
});
test('invalid duplicate input window fails before any classifier send',async()=>{
 const r=await verifyVoiceWindow([{event_id:1},{event_id:1}],async()=>{throw Error('unexpected')});assert.equal(r.status,'invalid_candidate_window');assert.equal(r.verification.checked_count,0);
});
test('expired total budget does not start another classifier call',async()=>{
 const r=await verifyVoiceWindow(candidates,async()=>{throw Error('unexpected')},{budgetMs:0});assert.equal(r.verification.failure_reason,'verification_budget_exhausted');assert.equal(r.verification.checked_count,0);
});

test('provider schema requires every ID once; parser still rejects missing/unknown/invalid verdicts',()=>{
 const rows=[{event_id:7422},{event_id:8580}];
 const schema=voiceVerifierSchema(rows).properties.classifications;
 assert.deepEqual(schema.required,['7422','8580']);assert.equal(schema.additionalProperties,false);
 assert.equal(classifyVoiceSchemaPayload({classifications:{7422:'exact',8580:'possible'}},rows).status,'ok');
 for(const classifications of [{7422:'exact'},{7422:'exact',8580:'maybe'},{7422:'exact',8580:'exact',999:'exact'},null,[]]) {
  assert.notEqual(classifyVoiceSchemaPayload({classifications},rows).status,'ok');
 }
});
