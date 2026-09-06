// Real Request/Response/domain path with an injected provider and repository.
// These tests DO NOT claim external ASR, deployed Auth, SQL or phone evidence.
import test from 'node:test';import assert from 'node:assert/strict';
import {handleAssistant,publicOperation} from '../../supabase/functions/event-search/assistant-handler.ts';
import {sha256} from '../../supabase/functions/event-search/assistant-media.ts';
import {initialState} from '../src/lib/assistant/conversationState.ts';
import {segmentPcm16} from '../src/lib/assistant/audioSegments.ts';
import {AssistantError,AUDIO_BUDGET,interpretation,eligible,kaliningradDay,interpreterPrompt,INTERPRETATION_SCHEMA} from '../../supabase/functions/event-search/assistant-intent.ts';
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
  async search(){calls.push('search');return{items:[{event_id:1,title:'Лекция',start_date:'2026-09-06',category:'lecture'}],catalog_revision:'facts-v1',semantic_verification:{status:'complete',exact_ids:[1],possible_ids:[],rejected_ids:[],unchecked_ids:[],checked_count:1}};},
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
test('ASR gets bounded regional hints as data and stamps the durable vocabulary version',async()=>{
 const h=harness(),id=uid();const [part]=segmentPcm16(new Float32Array(20),44100,AUDIO_BUDGET);const manifest={frames:20,sampleRate:44100,partCount:1};await h.control(id,'asr',manifest,false);
 const {bytes,...meta}=part;await h.request('audio',{id,part:{...meta,digest:await sha256(bytes),data:Buffer.from(bytes).toString('base64')}});
 const original=h.deps.generate;let prompt;h.deps.generate=async o=>{prompt=o.prompt;return original(o);};
 const done=await h.control(id,'asr',manifest);assert.equal(done.body.result.vocabulary_version,'kenigevents-regional-places-v1');
 assert.match(prompt,/Зеленоградск/);assert.match(prompt,/акустической и контекстной совместимости/);assert.match(prompt,/Не добавляй названия/);assert.ok(prompt.length<4096);
});
test('bounded search membership is never relabelled as a complete catalogue',async()=>{
 const h=harness(),i=uid(),s=uid();await h.control(i,'interpret',input());
 const original=h.deps.search;h.deps.search=async()=>({...await original(),has_more:true});
 const result=(await h.control(s,'search',{interpretationId:i})).body.result;
 assert.equal(result.has_more,true);assert.equal(result.membership_complete,false);assert.equal(result.membership_scope,'bounded_canonical_search_window');
});

// Same interpreter call produces an intent acknowledgement, not invented card facts.
function summaryProvider(h, extra={}) {
 h.deps.generate=async o=>{h.calls.push(o.kind);await o.dispatched();const value=o.validate(parsed({responseSummary:'Вы хотите лекцию в Калининграде на 6 сентября.',...extra}));await o.completed(value,{pending:true});await o.accounted();return value;};
}
test('optional response summary preserves old receipts and enforces a short nonempty string when supplied',()=>{
 assert.equal(interpretation(parsed()).responseSummary,undefined);
 assert.equal(interpretation(parsed({responseSummary:null})).responseSummary,null);
 assert.equal(interpretation(parsed({responseSummary:'Вы хотите лекцию.'})).responseSummary,'Вы хотите лекцию.');
 for(const responseSummary of ['', '  ', 5, {}, 'я'.repeat(321)]) assert.throws(()=>interpretation(parsed({responseSummary})));
 assert.ok(!INTERPRETATION_SCHEMA.required.includes('responseSummary'));
 assert.equal(INTERPRETATION_SCHEMA.properties.responseSummary.maxLength,320);
 const prompt=interpreterPrompt(input(),intent(),[]);
 assert.match(prompt,/title — короткое осмысленное название/);assert.match(prompt,/что \+ где \+ когда/);
 assert.match(prompt,/Экскурсии по востоку области на 5–6 сентября/);
 assert.match(prompt,/поиск ещё не выполнен/);assert.match(prompt,/Фактическое количество и отсутствие результатов сообщит сервер/);
});
test('one existing interpretation call creates a compact acknowledgement and actual bounded-window count without replay',async()=>{
 const h=harness(),i=uid(),s=uid();summaryProvider(h,{title:'Лекции в Калининграде на 6 сентября'});
 await h.control(i,'interpret',input());
 const done=await h.control(s,'search',{interpretationId:i});
 assert.equal(done.body.result.title,'Лекции в Калининграде на 6 сентября');
 assert.equal(done.body.result.answer,'Вы хотите лекцию в Калининграде на 6 сентября.\nСобытий в текущей выдаче: 1.');
 await h.control(i,'interpret',input());await h.control(s,'search',{interpretationId:i});
 assert.deepEqual(h.calls,['interpret','search']);
});
test('summary never masks zero matches and refreshed projections recount without mutating the completed receipt',async()=>{
 const h=harness(),i=uid(),s=uid();summaryProvider(h);
 await h.control(i,'interpret',input());await h.control(s,'search',{interpretationId:i});
 h.deps.currentCards=async()=>[];
 const refreshed=await h.request(`status?id=${s}`);
 assert.match(refreshed.body.result.answer,/Вы хотите лекцию/);
 assert.match(refreshed.body.result.answer,/нет событий с подтверждёнными условиями/);
 assert.match(h.rows.get(s).outcome.result.answer,/выдаче: 1/);
 assert.deepEqual(h.calls,['interpret','search']);
 const empty=harness(),j=uid(),k=uid();summaryProvider(empty);empty.deps.search=async()=>({items:[],semantic_verification:{status:'complete',exact_ids:[],possible_ids:[],rejected_ids:[],unchecked_ids:[],checked_count:0}});
 await empty.control(j,'interpret',input());
 const done=await empty.control(k,'search',{interpretationId:j});
 assert.match(done.body.result.answer,/нет событий с подтверждёнными условиями/);
});
test('clarification and canonical card explanations take precedence over generated acknowledgements',async()=>{
 const h=harness(),i=uid(),s=uid();summaryProvider(h,{clarification:'На какой день искать лекцию?'});
 await h.control(i,'interpret',input());const done=await h.control(s,'search',{interpretationId:i});
 assert.equal(done.body.result.answer,'На какой день искать лекцию?');assert.deepEqual(h.calls,['interpret']);
 const fresh=harness(),a=uid(),b=uid();await fresh.control(a,'interpret',input());await fresh.control(b,'search',{interpretationId:a});
 summaryProvider(fresh,{explanationKind:'address',ordinal:1});
 fresh.deps.currentCards=async()=>[{event_id:1,title:'Лекция',address:'Проверенный адрес'}];
 const explain=uid(),answer=uid();
 await fresh.control(explain,'interpret',input({mode:'explain_selection',parentId:b,visibleIds:['1'],text:'Где первая лекция?'}));
 const reply=await fresh.control(answer,'search',{interpretationId:explain});
 assert.equal(reply.body.result.answer,'Проверенный адрес');
 const status=await fresh.request(`status?id=${answer}`);assert.equal(status.body.result.answer,'Проверенный адрес');
});

// Strict voice admission uses a known classifier receipt; these are contract fixtures, not live semantic evidence.
test('vector-only/missing verifier and timeout are explicit unavailable, not jazz matches or a false zero; no same-ID replay',async()=>{
 for(const semantic_verification of [undefined,{status:'unavailable',failure_reason:'timeout',unchecked_ids:[8680]}]){
  const h=harness(),i=uid(),s=uid();let searches=0;
  h.deps.search=async()=>{searches++;return {items:[{event_id:8680,title:'Rap at Jazz Club'}],semantic_verification};};
  await h.control(i,'interpret',input());const r=await h.control(s,'search',{interpretationId:i});
  assert.equal(r.body.state,'completed');assert.deepEqual(r.body.result.items,[]);assert.equal(r.body.result.verification_unavailable,true);
  assert.match(r.body.result.answer,/Не удалось завершить проверку/);assert.doesNotMatch(r.body.result.answer,/нет событий с подтверждёнными/);
  await h.control(s,'search',{interpretationId:i});assert.equal(searches,1);
 }
});
test('refine semantically verifies changed intent over refreshed full parent subset, never current visible page or catalog expansion',async()=>{
 const h=harness(),i=uid(),s=uid();await h.control(i,'interpret',input());await h.control(s,'search',{interpretationId:i});
 const j=uid(),k=uid();h.deps.generate=async o=>{await o.dispatched();await o.completed(o.validate(parsed({intent:{...intent(),goal:'джаз'}})),null);await o.accounted();};
 let captured;
 h.deps.search=async(_req,nextIntent,_id,parentCandidates)=>{captured={nextIntent,parentCandidates};return{items:[{event_id:999,title:'Injected outside parent'},{event_id:1,title:'Confirmed parent'}],semantic_verification:{status:'complete',exact_ids:[1],possible_ids:[],rejected_ids:[],unchecked_ids:[]}};};
 await h.control(j,'interpret',input({mode:'refine_selection',parentId:s,visibleIds:[]}));
 const r=await h.control(k,'search',{interpretationId:j});
 assert.equal(captured.nextIntent.goal,'джаз');assert.deepEqual(captured.parentCandidates.map(x=>x.event_id),[1]);assert.deepEqual(r.body.result.items.map(x=>x.event_id),[1]);assert.equal(r.body.result.has_more,false);
});
test('expand semantically checks the new intent without a parent-only candidate bound',async()=>{
 const h=harness(),i=uid(),s=uid();await h.control(i,'interpret',input());await h.control(s,'search',{interpretationId:i});
 const j=uid(),k=uid();let captured;
 h.deps.search=async(_req,nextIntent,_id,parentCandidates)=>{captured=parentCandidates;return{items:[],semantic_verification:{status:'complete',exact_ids:[],unchecked_ids:[]}};};
 await h.control(j,'interpret',input({mode:'expand_selection',parentId:s}));await h.control(k,'search',{interpretationId:j});assert.equal(captured,undefined);
});
test('interpreter has no default-city narrowing when base and question provide no city',()=>{
 const emptyBase={...intent(),localityIds:[]};const prompt=interpreterPrompt(input({text:'Джаз на выходных'}),emptyBase,[]);
 assert.match(prompt,/BASE.localityIds пуст/);assert.match(prompt,/intent.localityIds должен остаться \[\]/);assert.match(prompt,/Не добавляй kaliningrad по умолчанию/);
 assert.deepEqual(interpretation(parsed({intent:emptyBase})).intent.localityIds,[]);
 assert.ok(eligible({event_id:8580,title:'FOXTROT JAZZ BAND',city:'Светлогорск',start_date:'2026-09-12'}, {...emptyBase,dateFrom:'2026-09-12',dateTo:'2026-09-13'}));
});

test('nearest weekend calendar grounding spans Saturday/Sunday with local timezone and month/year rollover',async()=>{
 const {nearestWeekend,interpreterPrompt}=await import('../../supabase/functions/event-search/assistant-intent.ts');
 assert.deepEqual(nearestWeekend('2026-09-06T14:00:00Z'),{dateFrom:'2026-09-12',dateTo:'2026-09-13'});
 assert.deepEqual(nearestWeekend('2026-09-05T12:00:00Z'),{dateFrom:'2026-09-05',dateTo:'2026-09-06'});
 assert.deepEqual(nearestWeekend('2026-09-05T22:30:00Z'),{dateFrom:'2026-09-12',dateTo:'2026-09-13'});
 assert.deepEqual(nearestWeekend('2026-12-31T12:00:00Z'),{dateFrom:'2027-01-02',dateTo:'2027-01-03'});
 const prompt=interpreterPrompt({anchor:'2026-09-06T14:00:00Z'}, {}, null);
 assert.match(prompt,/2026-09-12/);assert.match(prompt,/воскресенье–понедельник/);
});

test('missing audience projection is deferred to strict semantic verifier, not silently accepted or prefiltered',()=>{
 const event={event_id:7,start_date:'2026-09-12',category:'theatre'};const i={...intent(),audience:['children']};
 assert.equal(eligible(event,i),false);assert.equal(eligible(event,i,true),true);
 assert.equal(eligible({...event,lifecycle_status:'cancelled'},i,true),false);
});
test('calendar next week starts tomorrow on Sunday; context prompt supports union and topic replacement',async()=>{
 const {nextCalendarWeek}=await import('../../supabase/functions/event-search/assistant-intent.ts');
 assert.deepEqual(nextCalendarWeek('2026-09-06T14:00:00Z'),{dateFrom:'2026-09-07',dateTo:'2026-09-13'});
 assert.deepEqual(nextCalendarWeek('2026-09-07T14:00:00Z'),{dateFrom:'2026-09-14',dateTo:'2026-09-20'});
 const p=interpreterPrompt(input(),intent(),[]);assert.match(p,/неизменяемый фильтр/);assert.match(p,/ИЛИ/);assert.match(p,/сдвигает интервал BASE на 7/);
});
function enableEditorial(h,{fail=false,accountingFailure=false}={}) {
 h.deps.editorialEnabled=true;
 h.deps.editorialFacts=async()=>[{event_id:1,title:'Лекция',search_digest:'Разговор об архитектуре старого города.'}];
 const original=h.deps.generate;
 h.deps.generate=async o=>{
  if(o.kind!=='editorial')return original(o);
  h.calls.push('editorial');await o.dispatched();if(fail)throw Error('quota_or_timeout');
  const value=o.validate({intro:'Я бы начал с этого варианта.',recommendations:[{event_id:1,comment:'Можно глубже познакомиться с архитектурой города.',evidence_index:0}]});
  await o.completed(value,{pending:true});if(accountingFailure)throw Error('finalize unavailable');await o.accounted();return value;
 };
}
test('editorial follows verified results, is durable and never regenerated on retry/status',async()=>{
 const h=harness();enableEditorial(h);const i=uid(),s=uid();await h.control(i,'interpret',input());const r=await h.control(s,'search',{interpretationId:i});
 assert.equal(r.body.result.editorial.status,'complete');assert.match(r.body.result.answer,/Лекция —/);assert.doesNotMatch(r.body.result.answer,/Событий в текущей/);
 await h.control(s,'search',{interpretationId:i});const status=await h.request(`status?id=${s}`);
 assert.equal(status.body.result.answer,r.body.result.answer);assert.equal(h.calls.filter(x=>x==='editorial').length,1);
});
test('optional editorial failure preserves verified cards, and finalize failure preserves generated reply',async()=>{
 for(const config of [{fail:true},{accountingFailure:true}]){
  const h=harness();enableEditorial(h,config);const i=uid(),s=uid();await h.control(i,'interpret',input());const r=await h.control(s,'search',{interpretationId:i});
  assert.equal(r.body.state,'completed');assert.equal(r.body.result.items.length,1);assert.equal(r.body.result.editorial.status,config.fail?'unavailable':'complete');
  await h.control(s,'search',{interpretationId:i});assert.equal(h.calls.filter(x=>x==='editorial').length,1);
 }
});
test('no editorial provider spend on unverified or empty result sets',async()=>{
 for(const search of [{items:[],semantic_verification:{status:'complete',exact_ids:[],unchecked_ids:[]}},{items:[{event_id:1}],verification_unavailable:true}]){
  const h=harness();enableEditorial(h);h.deps.search=async()=>search;const i=uid(),s=uid();await h.control(i,'interpret',input());await h.control(s,'search',{interpretationId:i});assert.ok(!h.calls.includes('editorial'));
 }
});
test('changed editorial public facts invalidate commentary on refresh without regenerating',async()=>{
 const h=harness();enableEditorial(h);const i=uid(),s=uid();await h.control(i,'interpret',input());await h.control(s,'search',{interpretationId:i});
 h.deps.editorialFacts=async()=>[{event_id:1,title:'Лекция',search_digest:'Программа изменилась.'}];
 const r=await h.request(`status?id=${s}`);assert.equal(r.body.result.editorial.status,'stale');assert.doesNotMatch(r.body.result.answer,/архитектур/);assert.equal(r.body.result.items.length,1);assert.equal(h.calls.filter(c=>c==='editorial').length,1);
});
test('editorial failure plus unresolved quota finalize preserves cards AND pending accounting',async()=>{
 const {SharedGoogleQuotaError}=await import('../../supabase/functions/event-search/google-quota.ts');
 const h=harness();enableEditorial(h);const original=h.deps.generate;h.deps.generate=async o=>{if(o.kind==='editorial')throw new SharedGoogleQuotaError('finalize','synthetic');return original(o);};
 const i=uid(),s=uid();await h.control(i,'interpret',input());const r=await h.control(s,'search',{interpretationId:i});
 assert.equal(r.body.state,'completed');assert.equal(r.body.result.items.length,1);assert.equal(r.body.accounting_pending,true);assert.equal(r.body.result.editorial.failure_reason,'shared_quota_finalize');
});

test('structured plan is durably passed to search; ordinary conversational continuation does not force subset',async()=>{
 const h=harness();h.deps.structuredPlanEnabled=true;
 const queryPlan={contextMode:'replace',dateMode:'next_week',scope:'all_events',groups:[]};
 h.deps.generate=async o=>{await o.dispatched();const r=o.validate(parsed({queryPlan,title:'События в Светлогорске',intent:{...intent(),goal:'старое краеведение',localityIds:['svetlogorsk']}}));await o.completed(r);return r;};
 let received;h.deps.search=async(...args)=>{received=args;return {items:[],semantic_verification:{status:'complete',exact_ids:[],unchecked_ids:[]}};};
 const id=uid();await h.control(id,'interpret',input({text:'Какие события пройдут в Светлогорске на следующей неделе?'}));
 const r=await h.control(uid(),'search',{interpretationId:id});
 assert.equal(r.body.state,'completed');assert.deepEqual(received[4],queryPlan);assert.equal(received[3],undefined);assert.equal(received[1].goal,'события');assert.deepEqual(r.body.result.queryPlan,queryPlan);
});

test('model-led interpretation receives the owner-checked actual predecessor message, not only its shortened goal',async()=>{
 const h=harness(),parentId=uid();h.deps.structuredPlanEnabled=true;
 const original='Хочется спланировать всю следующую неделю, начнём с научных мероприятий.';
 const queryPlan={contextMode:'replace',dateMode:'next_week',scope:'constrained',groups:[{dimension:'topic',alternatives:['научпоп'],sourceQuote:original,source:'current'}]};
 h.rows.set(parentId,{id:parentId,owner_id:owner,kind:'search',state:'completed',outcome:{result:{question:original,title:'Научпоп',intent:{...intent(),dateFrom:'2026-09-07',dateTo:'2026-09-13'},queryPlan,items:[]}}});
 h.deps.generate=async o=>{assert.ok(o.prompt.includes('CONVERSATION_CONTEXT='));assert.ok(o.prompt.includes(original));await o.dispatched();const result=o.validate(parsed({intent:{...intent(),dateFrom:null,dateTo:null},queryPlan:{contextMode:'patch',dateMode:'inherit',scope:'constrained',groups:[{dimension:'format',alternatives:['лекция'],sourceQuote:'Подбери лекции',source:'current'}]}}));await o.completed(result);return result;};
 const r=await h.control(uid(),'interpret',input({text:'Подбери лекции',mode:'expand_selection',parentId}));assert.equal(r.body.state,'completed');assert.equal(r.body.result.intent.dateFrom,'2026-09-07');assert.equal(r.body.result.intent.dateTo,'2026-09-13');assert.doesNotMatch(r.body.result.intent.goal,/научпоп/);
});

// Adaptive strategy stays inside the existing interpreter and authenticated lane.
const adaptive=(extra={})=>({knowledgeAction:'internal',externalNeed:null,externalQuery:null,clarification:'none',question:null,assumptions:[],refinementOpportunity:null,...extra});
function adaptiveProvider(h,plan,queryPlan={contextMode:'replace',dateMode:'from_today',scope:'constrained',groups:[{dimension:'format',alternatives:['лекция'],sourceQuote:'Хочу на лекцию',source:'current'}]}){
 h.deps.structuredPlanEnabled=true;h.deps.adaptivePlanEnabled=true;
 h.deps.generate=async o=>{h.calls.push(o.kind);assert.ok(o.schema.required.includes('adaptivePlan'));await o.dispatched();const value=o.validate(parsed({adaptivePlan:plan,queryPlan}));await o.completed(value);return value;};
}
test('adaptive internal plan uses exactly one interpret call and does not force questions',async()=>{
 const h=harness();adaptiveProvider(h,adaptive());const i=uid(),s=uid();const r=await h.control(i,'interpret',input());
 assert.equal(r.body.state,'completed');assert.equal(r.body.result.clarification,null);
 const found=await h.control(s,'search',{interpretationId:i});assert.equal(found.body.result.items.length,1);assert.deepEqual(h.calls,['interpret','search']);
});
test('blocking clarification does not run retrieval, verification or editorial',async()=>{
 const h=harness();h.deps.editorialEnabled=true;h.deps.editorialFacts=async()=>{throw Error('must not fetch');};
 adaptiveProvider(h,adaptive({clarification:'blocking',question:'Какую именно лекцию вы имеете в виду?'}));
 const i=uid();await h.control(i,'interpret',input());const r=await h.control(uid(),'search',{interpretationId:i});
 assert.equal(r.body.result.answer,'Какую именно лекцию вы имеете в виду?');assert.deepEqual(r.body.result.items,[]);assert.deepEqual(h.calls,['interpret']);
});
test('optional question does not block cards, appears once, survives readback',async()=>{
 const h=harness();const question='Вам интереснее история или искусство?';adaptiveProvider(h,adaptive({clarification:'optional',question}));
 const i=uid(),s=uid();await h.control(i,'interpret',input());const r=await h.control(s,'search',{interpretationId:i});
 assert.equal(r.body.result.items.length,1);assert.equal(r.body.result.clarification,null);assert.equal(r.body.result.answer.split(question).length,2);
 const read=await h.request(`status?id=${s}`);assert.equal(read.body.result.answer,r.body.result.answer);assert.deepEqual(h.calls,['interpret','search']);
});
test('unavailable web reference asks rather than pretends to search externally',async()=>{
 const h=harness();adaptiveProvider(h,adaptive({knowledgeAction:'web_lookup',externalNeed:'Неизвестный исполнитель',externalQuery:'исполнитель жанр',clarification:'blocking',question:'Какой жанр у этого исполнителя?'}));
 const i=uid();await h.control(i,'interpret',input());const r=await h.control(uid(),'search',{interpretationId:i});
 assert.equal(r.body.result.adaptivePlan.knowledgeAction,'web_lookup');assert.deepEqual(r.body.result.items,[]);assert.deepEqual(h.calls,['interpret']);
});
test('answer to blocking question retains original request as grounded input without replacing raw speech',async()=>{
 const h=harness();adaptiveProvider(h,adaptive({clarification:'blocking',question:'На какую тему?'}),null);const i=uid(),s=uid();
 await h.control(i,'interpret',input());await h.control(s,'search',{interpretationId:i});
 let prompt,schema;adaptiveProvider(h,adaptive());const generate=h.deps.generate;h.deps.generate=o=>{prompt=o.prompt;schema=o.schema;return generate(o);};
 const next=uid();const r=await h.control(next,'interpret',input({text:'История края',mode:'expand_selection',parentId:s}));
 assert.equal(r.body.state,'completed');assert.equal(h.rows.get(next).payload.text,'История края');assert.equal(r.body.result.question,'Хочу на лекцию\nИстория края');
 assert.ok(prompt.includes('На какую тему?'));assert.ok(schema.properties.queryPlan.properties.groups.items.properties.sourceQuote.enum.some(q=>q.includes('Хочу на лекцию')));
});
test('optional question context is available to next interpreter, without forcing inheritance',async()=>{
 const h=harness();adaptiveProvider(h,adaptive({clarification:'optional',question:'На какую тему?'}));const i=uid(),s=uid();await h.control(i,'interpret',input());await h.control(s,'search',{interpretationId:i});
 let prompt;adaptiveProvider(h,adaptive(),{contextMode:'replace',dateMode:'from_today',scope:'all_events',groups:[]});const generate=h.deps.generate;h.deps.generate=o=>{prompt=o.prompt;return generate(o);};
 const r=await h.control(uid(),'interpret',input({text:'Нет, любые события',mode:'expand_selection',parentId:s}));
 assert.equal(r.body.state,'completed');assert.equal(r.body.result.question,'Нет, любые события');assert.equal(r.body.result.queryPlan.scope,'all_events');assert.ok(prompt.includes('На какую тему?'));
});
test('adaptive editorial uses actual candidates and optional question is composed exactly once',async()=>{
 const h=harness(),question='Какую тему предпочитаете?';adaptiveProvider(h,adaptive({clarification:'optional',question,refinementOpportunity:'Уточнить тему при необходимости'}));
 h.deps.editorialEnabled=true;h.deps.editorialFacts=async()=>[{event_id:1,title:'Лекция',search_digest:'История региона'}];
 const interpret=h.deps.generate;h.deps.generate=async o=>{if(o.kind==='interpret')return interpret(o);h.calls.push(o.kind);assert.match(o.prompt,/"resultCount":1/);assert.deepEqual(o.schema.properties.refinement.enum,[null]);await o.dispatched();const value=o.validate({intro:'Есть историческая лекция.',recommendations:[{event_id:1,comment:'Познакомит с прошлым региона.',evidence_index:0}],refinement:null});await o.completed(value);};
 const i=uid(),s=uid();await h.control(i,'interpret',input());const r=await h.control(s,'search',{interpretationId:i});
 assert.equal(r.body.result.editorial.status,'complete');assert.equal(r.body.result.answer.split(question).length,2);assert.equal(r.body.result.items.length,1);assert.deepEqual(h.calls,['interpret','search','editorial']);
 assert.equal((await h.request(`status?id=${s}`)).body.result.answer,r.body.result.answer);
 h.deps.editorialFacts=async()=>[];const stale=await h.request(`status?id=${s}`);assert.equal(stale.body.result.editorial.status,'stale');assert.equal(stale.body.result.answer.split(question).length,2);
});

test('blocking is not rejected by an unexecutable query plan and cannot carry it into next search',async()=>{
 const h=harness();adaptiveProvider(h,adaptive({clarification:'blocking',question:'О каком событии речь?'}),{contextMode:'patch',dateMode:'inherit',scope:'constrained',groups:[]});
 const i=uid();const r=await h.control(i,'interpret',input());assert.equal(r.body.state,'completed');assert.equal(r.body.result.queryPlan,null);
 const found=await h.control(uid(),'search',{interpretationId:i});assert.equal(found.body.result.answer,'О каком событии речь?');assert.deepEqual(h.calls,['interpret']);
});
test('nonblocking null plan still fails closed before retrieval',async()=>{
 const h=harness();adaptiveProvider(h,adaptive(),null);const r=await h.control(uid(),'interpret',input());assert.equal(r.body.error,'invalid_query_plan');assert.deepEqual(h.calls,['interpret']);
});
