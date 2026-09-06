// Actual Search verifier + actual shared-reservation wrapper, injected network/metadata only. No live provider/DB calls.
import test from 'node:test';import assert from 'node:assert/strict';import {readFile} from 'node:fs/promises';import {build} from '../../../site/node_modules/esbuild/lib/main.js';
const source=await readFile(new URL('./index.ts',import.meta.url),'utf8');
const built=await build({stdin:{contents:source.replace(/import \{ createClient \} from "https:[^\n]+/, 'const createClient = () => {throw Error("unexpected database client")};')+'\nexport {llmVerify};',resolveDir:new URL('.',import.meta.url).pathname,loader:'ts'},bundle:true,write:false,platform:'node',format:'esm',logLevel:'silent'});
const {llmVerify}=await import('data:text/javascript;base64,'+Buffer.from(built.outputFiles[0].text).toString('base64'));
const key='11111111-1111-4111-8111-111111111111',keyEnv='VOICE_TEST_KEY';
const rows=[{event_id:7422,title:'Swing standards'},{event_id:8680,title:'Rap at Jazz Club'}];
const digests=new Map([[7422,'Jazz standards and improvisation'],[8680,'Rap concert; the venue is City Jazz Club']]);
const intent={goal:'джаз на выходных',localityIds:[],dateFrom:'2026-09-12',dateTo:'2026-09-13'};
async function harness(fn,{enabled=true,deny=false,partial=false,timeout=false}={}){
 const names=['EVENT_SEARCH_LLM_ENABLED','EVENT_SEARCH_LLM_KEY_ENVS','EVENT_SEARCH_LLM_RESERVE_KEY_ENVS',keyEnv];const saved=Object.fromEntries(names.map(n=>[n,process.env[n]]));
 process.env.EVENT_SEARCH_LLM_ENABLED=enabled?'1':'0';process.env.EVENT_SEARCH_LLM_KEY_ENVS=keyEnv;process.env.EVENT_SEARCH_LLM_RESERVE_KEY_ENVS='VOICE_TEST_UNUSED';process.env[keyEnv]='synthetic-test-secret';
 const oldFetch=globalThis.fetch;const calls=[];const counters={llm_provider_attempts:0};
 const backend={async listActiveKeys(){calls.push('metadata');return[{id:key,env_var_name:keyEnv,quota_scope:'google:test'}]},async rpc(name){calls.push(name);if(name==='google_ai_reserve')return deny?{ok:false,blocked_reason:'rpm'}:{ok:true,api_key_id:key,env_var_name:keyEnv,quota_scope:'google:test',minute_bucket:'2026-09-06T00:00:00Z',day_bucket:'2026-09-06',limiter_contract:'google_ai_project_model_atomic_v1',bucket_strategy:'rolling_60s_pacific_day_v2'};return{ok:true}}};
 globalThis.fetch=async(url,opts)=>{assert.match(String(url),/generateContent/);assert.equal(calls.at(-1),'google_ai_mark_sent');calls.push('provider');if(timeout)throw Error('llm_provider_timeout');
 const body=JSON.parse(opts.body);assert.match(body.contents[0].parts[0].text,/название площадки/);assert.match(body.contents[0].parts[0].text,/2026-09-13/);assert.equal(body.generationConfig.maxOutputTokens,2048);
 return new Response(JSON.stringify({candidates:[{content:{parts:[{text:JSON.stringify({query_interpretation:'jazz',exact_event_ids:[7422],possible_event_ids:[],rejected_event_ids:partial?[]:[8680]})}]}}],usageMetadata:{promptTokenCount:50,candidatesTokenCount:30,totalTokenCount:80}}),{status:200});};
 try{return await fn({backend,counters,calls,verify:(facts=digests)=>llmVerify(intent.goal,rows,facts,{voiceIntent:intent,deadline:Date.now()+2000,gemma_overflow_allowed:false,quota_backend:backend,counters})});}
 finally{globalThis.fetch=oldFetch;for(const n of names){if(saved[n]===undefined)delete process.env[n];else process.env[n]=saved[n];}}
}
test('voice verifier sends only after real shared reserve + mark-sent and finalizes actual usage',()=>harness(async({verify,calls,counters})=>{const r=await verify();assert.equal(r.used,true);assert.equal(r.status,'ok');assert.deepEqual(r.exact.map(x=>x.event_id),[7422]);assert.deepEqual(r.rejected_ids,[8680]);assert.equal(counters.llm_provider_attempts,1);assert.deepEqual(calls,['metadata','google_ai_reserve','google_ai_mark_sent','provider','google_ai_finalize']);}));
test('disabled cannot send or fabricate an admitted exact result',()=>harness(async({verify,calls})=>{const r=await verify();assert.equal(r.used,false);assert.equal(r.status,'disabled');assert.deepEqual(calls,[])},{enabled:false}));
test('shared quota denial prevents provider send',()=>harness(async({verify,calls,counters})=>{const r=await verify();assert.equal(r.used,false);assert.equal(counters.llm_provider_attempts,0);assert.ok(!calls.includes('provider'));assert.ok(calls.includes('google_ai_reserve'))},{deny:true}));
test('strict voice needs facts for every candidate, not legacy fifty percent coverage',()=>harness(async({verify,calls})=>{const r=await verify(new Map([[7422,'Jazz']]));assert.equal(r.status,'degraded:digest_insufficient');assert.deepEqual(calls,[])}));
test('partial classifier response stays explicit and is never silently extended with unchecked possible tail',()=>harness(async({verify,calls})=>{const r=await verify();assert.equal(r.status,'degraded:incomplete_classification');assert.deepEqual(r.unchecked_ids,[8680]);assert.equal(r.possible.length,0);assert.equal(calls.filter(x=>x==='provider').length,1)},{partial:true}));
test('transport timeout is finalized and unavailable, without canary admission or provider fallback',()=>harness(async({verify,calls})=>{const r=await verify();assert.equal(r.used,false);assert.match(r.status,/timeout/);assert.ok(calls.includes('google_ai_finalize'));assert.equal(calls.filter(x=>x==='provider').length,1)},{timeout:true}));
test('internal voice admission is distinct from legacy quota yet still lease-wrapped; ordinary fail-open remains',()=>{
 assert.match(source,/useLlmVerifier && \(Boolean\(assistantIntent\) \|\| isCanary \|\| llmQuotaReserved\)/);
 assert.match(source,/assistantIntent \? llmResult.exact : llmResult.used \? llmResult.exact : llmResult.possible/);
 assert.match(source,/include_fallback: false, use_llm_verifier: true, allow_llm_fallback: false/);
 assert.match(source,/withSharedGoogleQuotaAttempt\(\{/);assert.doesNotMatch(source,/llm_reserved: true/);
});
