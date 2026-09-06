// Real Request/Response/domain path with an injected provider and repository.
// These tests DO NOT claim external ASR, deployed Auth, SQL or phone evidence.
import test from 'node:test';import assert from 'node:assert/strict';
import {handleAssistant,publicOperation} from '../../supabase/functions/event-search/assistant-handler.ts';
import {sha256} from '../../supabase/functions/event-search/assistant-media.ts';
import {initialState} from '../src/lib/assistant/conversationState.ts';
import {segmentPcm16} from '../src/lib/assistant/audioSegments.ts';
import {AssistantError,AUDIO_BUDGET,interpretation,eligible,kaliningradDay} from '../../supabase/functions/event-search/assistant-intent.ts';
const uid=()=>crypto.randomUUID(),owner=uid(),other=uid(),origin='https://preview.example';
const intent=()=>({...initialState().activeIntent,goal:'лекции',dateFrom:'2026-09-06',dateTo:null,timeOfDay:null,audience:[],timezone:'Europe/Kaliningrad'});
const input=(extra={})=>({text:'Хочу на лекцию',mode:'new_search',parentId:null,previousId:null,anchor:'2026-09-05T23:30:00.000Z',visibleIds:[],...extra});
const parsed=(extra={})=>({intent:intent(),title:'Лекции',clarification:null,explanationKind:'none',ordinal:null,...extra});
function harness(){
 const rows=new Map(),parts=new Map(),calls=[];let activeOwner=owner;
 const repo={
  async admit(who,id,kind,payload){const old=rows.get(id);if(old){if(old.owner_id!==who)throw new AssistantError('operation_not_found',404);if(old.kind!==kind||JSON.stringify(old.payload)!==JSON.stringify(payload))throw new AssistantError('payload_conflict',409);return old;}const row={id,kind,payload,owner_id:who,state:'accepted',dispatched:false,created_at:new Date().toISOString()};rows.set(id,row);return row;},
  async get(who,id){const row=rows.get(id);return row?.owner_id===who?row:null;},
  async history(who){return [...rows.values()].filter(r=>r.owner_id===who&&r.kind==='search'&&r.state==='completed').slice(0,20);},
  async claim(who,id){const r=await this.get(who,id);if(r.state!=='accepted')return{claimed:false};r.state='processing';r.claim_id=uid();return{claimed:true,claim_id:r.claim_id};},
  async checkpoint(who,id,claim,state,outcome,error){const r=await this.get(who,id);assert.equal(r.claim_id,claim);assert.equal(r.state,'processing');if(state==='dispatched')r.dispatched=true;else r.state=state;if(outcome)r.outcome=outcome;if(error)r.error_code=error;},
  async accounted(who,id){const r=await this.get(who,id);if(r.outcome?.accounting)r.outcome.accounting.pending=false;},
  async putAudio(who,id,part){const r=await this.get(who,id);if(!r)throw new AssistantError('operation_not_found',404);const list=parts.get(id)||[];const old=list.find(p=>p.index===part.index);if(old&&old.digest!==part.digest)throw new AssistantError('audio_payload_conflict',409);if(!old)list.push(part);parts.set(id,list);},
  async audio(who,id){if(!await this.get(who,id))return[];return parts.get(id)||[];}
 };
 const deps={enabled:true,allowedOrigins:[origin],maxAudioBytes:16*1024*1024,
  async authenticate(req){if(req.headers.get('authorization')!=='Bearer test')throw new AssistantError('auth_required',401);return{owner:activeOwner,repo};},
  async generate(o){calls.push(o.kind);await o.dispatched();const value=o.validate(o.kind==='asr'?{text:'Не концерт, лучше лекция',uncertain:[]}:parsed());await o.completed(value,{pending:true,lease:'private'});await o.accounted();return value;},
  async search(){calls.push('search');return{items:[{event_id:1,title:'Лекция',start_date:'2026-09-06',category:'lecture'}],catalog_revision:'facts-v1'};},
  async currentCards(who,ids){return ids.map(event_id=>({event_id:Number(event_id),title:'Актуальная лекция',start_date:'2026-09-06',category:'lecture'}));}
 };
 const request=(route,body,headers={})=>handleAssistant(new Request(`https://api.example/event-search/assistant/${route}`,{method:body?'POST':'GET',headers:{origin,authorization:'Bearer test',...headers},...(body?{body:JSON.stringify(body)}:{})}),deps);
 const control=(id,kind,payload,run=true)=>request('control',{id,kind,payload,run});
 return{rows,parts,calls,repo,deps,request,control,setOwner:value=>{activeOwner=value;}};
}
test('HTTP intake is durable before provider dispatch and retains the full raw request',async()=>{
 const h=harness(),id=uid(),payload=input({text:'не концерт '.repeat(500)});
 const r=await h.control(id,'interpret',payload,false);assert.equal(r.body.state,'accepted');assert.equal(h.calls.length,0);assert.equal(h.rows.get(id).payload.text,payload.text);
});
test('same confirmed ID executes once; private accounting never appears in public receipt',async()=>{
 const h=harness(),id=uid(),payload=input();const first=await h.control(id,'interpret',payload);const second=await h.control(id,'interpret',payload);
 assert.equal(first.body.state,'completed');assert.equal(second.body.state,'completed');assert.deepEqual(h.calls,['interpret']);assert.ok(!JSON.stringify(first.body).includes('private'));assert.equal(second.body.result.question,payload.text);
});
test('completed checkpoint survives accounting failure and is never regenerated',async()=>{
 const h=harness();h.repo.accounted=async()=>{throw new Error('accounting down');};const id=uid(),payload=input();
 const first=await h.control(id,'interpret',payload);assert.equal(first.body.state,'completed');assert.equal(first.body.accounting_pending,true);await h.control(id,'interpret',payload);assert.equal(h.calls.length,1);
});
test('ambiguous sent operation does not replay on POST, read or explicit retry',async()=>{
 const h=harness();h.deps.generate=async o=>{h.calls.push('sent');await o.dispatched();throw new Error('connection lost');};const id=uid(),payload=input();
 assert.equal((await h.control(id,'interpret',payload)).body.state,'outcome_unknown');await h.control(id,'interpret',payload);await h.request(`status?id=${id}`);assert.deepEqual(h.calls,['sent']);
});
test('missing auth, wrong origin, disabled lane cannot create rows or call provider',async()=>{
 const h=harness(),body={id:uid(),kind:'interpret',payload:input(),run:true};
 assert.equal((await h.request('control',body,{authorization:''})).status,401);assert.equal((await h.request('control',body,{origin:'https://other.example'})).status,403);
 h.deps.enabled=false;assert.equal((await h.request('control',body)).status,404);assert.equal(h.rows.size,0);assert.equal(h.calls.length,0);
});
test('another user cannot read or reuse an operation ID',async()=>{
 const h=harness(),id=uid(),payload=input();await h.control(id,'interpret',payload,false);h.setOwner(other);
 assert.equal((await h.request(`status?id=${id}`)).status,404);assert.equal((await h.control(id,'interpret',payload)).status,404);assert.equal(h.calls.length,0);
});
test('same ID with another payload conflicts, unknown fields and oversized text fail closed',async()=>{
 const h=harness(),id=uid(),payload=input();await h.control(id,'interpret',payload,false);
 assert.equal((await h.control(id,'interpret',input({text:'другое'}))).status,409);
 assert.equal((await h.control(uid(),'interpret',input({ownerId:other}))).status,400);
 assert.equal((await h.control(uid(),'interpret',input({text:'я'.repeat(8193)}))).status,400);
});
test('standalone WAV parts uploaded out of order assemble into ONE complete ASR request',async()=>{
 const h=harness(),id=uid(),samples=Float32Array.from({length:900000},(_,i)=>i%100===0?-0.0001:0.1);const parts=segmentPcm16(samples,48000,AUDIO_BUDGET);
 const manifest={frames:samples.length,sampleRate:48000,partCount:parts.length};await h.control(id,'asr',manifest,false);
 assert.equal((await h.control(id,'asr',manifest)).status,409);assert.equal(h.calls.length,0);
 for(const part of [...parts].reverse()){
  const digest=await sha256(part.bytes);const {bytes,...meta}=part;const body={id,part:{...meta,digest,data:Buffer.from(bytes).toString('base64')}};
  assert.equal((await h.request('audio',body)).status,200);assert.equal((await h.request('audio',body)).status,200);
 }
 let length;const original=h.deps.generate;h.deps.generate=async o=>{length=o.audio.length;return original(o);};
 assert.equal((await h.control(id,'asr',manifest)).body.state,'completed');assert.equal(length,44+samples.length*2);assert.deepEqual(h.calls,['asr']);
});
test('wrong digest never reaches ASR; truncated body is rejected',async()=>{
 const h=harness(),id=uid();const [part]=segmentPcm16(new Float32Array(20),44100,AUDIO_BUDGET);await h.control(id,'asr',{frames:20,sampleRate:44100,partCount:1},false);
 const {bytes,...meta}=part;assert.equal((await h.request('audio',{id,part:{...meta,digest:'0'.repeat(64),data:Buffer.from(bytes).toString('base64')}})).status,409);assert.equal(h.calls.length,0);
});
test('search uses completed interpretation; status refreshes current facts without mutating frozen membership',async()=>{
 const h=harness(),i=uid(),s=uid();await h.control(i,'interpret',input());const done=await h.control(s,'search',{interpretationId:i});assert.equal(done.body.result.items.length,1);
 h.deps.currentCards=async()=>[];const read=await h.request(`status?id=${s}`);assert.deepEqual(read.body.result.items,[]);assert.deepEqual(read.body.result.logical_event_ids,['1']);assert.equal(h.rows.get(s).outcome.result.items.length,1);
});
test('U2 is admitted before U1 completes but cannot interpret across a missing predecessor',async()=>{
 const h=harness(),i=uid(),j=uid();const payload=input({mode:'continue_draft',previousId:i,text:'нет, можно платные'});
 const waiting=await h.control(j,'interpret',payload);assert.equal(waiting.body.waiting,'previous');assert.equal(h.calls.length,0);
 await h.control(i,'interpret',input());assert.equal((await h.control(j,'interpret',payload)).body.state,'completed');assert.deepEqual(h.calls,['interpret','interpret']);assert.match(h.rows.get(j).outcome.result.question,/можно платные/);
});
test('stale dispatched status is reported as unknown, not accepted',()=>{const out=publicOperation({id:uid(),kind:'asr',state:'processing',dispatched:true,updated_at:'2020-01-01T00:00:00Z'});assert.equal(out.state,'outcome_unknown');});
test('Kaliningrad anchor crosses UTC midnight without depending on machine timezone',()=>{assert.equal(kaliningradDay('2026-09-05T23:30:00.000Z'),'2026-09-06');});
test('typed unknown filters, conflicting price and omitted fields are rejected',()=>{
 assert.throws(()=>interpretation(parsed({intent:{...intent(),localityIds:['invented']}})));
 assert.throws(()=>interpretation(parsed({intent:{...intent(),freeOnly:true,maxPrice:100}})));
 const i=intent();delete i.dateFrom;assert.throws(()=>interpretation(parsed({intent:i})));
});
test('hard filters operate on canonical facts; unknown price/date/audience never mean match',()=>{
 const i={...intent(),freeOnly:true,localityIds:['zelenogradsk'],excludedFormats:['concert'],timeOfDay:'evening',audience:['family']};
 const event={event_id:7,city:'Зеленоградск',category:'lecture',is_free:true,start_date:'2026-09-06',start_time:'19:00',audience_tags:['family']};
 assert.equal(eligible(event,i),true);for(const patch of [{category:'concert'},{is_free:false},{city:'Калининград'},{start_date:undefined},{audience_tags:[]},{lifecycle_status:'cancelled'}])assert.equal(eligible({...event,...patch},i),false);
});
