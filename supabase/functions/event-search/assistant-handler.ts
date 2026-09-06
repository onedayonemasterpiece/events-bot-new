import { ASR_VOCABULARY_VERSION, transcriptionPrompt } from './assistant-vocabulary.ts';
import { assemble, sha256 } from './assistant-media.ts';
export { assemble, sha256 } from './assistant-media.ts';
import { validateAudioParts } from './assistant-audio.ts';
import { initialState } from './assistant-dialogue.ts';
import { AUDIO_BUDGET, ASSISTANT_CONTRACT, AssistantError, confirmedInput, eligible, interpretation,
  interpreterPrompt, INTERPRETATION_SCHEMA, TRANSCRIPT_SCHEMA, object, reject, text, uuid,
  type Intent, type ConfirmedInput } from './assistant-intent.ts';
export type Operation = {id:string;owner_id:string;kind:string;payload:any;state:string;dispatched:boolean;outcome?:any;error_code?:string;updated_at?:string;created_at?:string};
export interface AssistantRepository {
  admit(owner:string,id:string,kind:string,payload:unknown):Promise<Operation>;
  get(owner:string,id:string):Promise<Operation|null>;
  history(owner:string,before?:string):Promise<Operation[]>;
  claim(owner:string,id:string):Promise<{claimed:boolean;claim_id?:string;state?:string}>;
  checkpoint(owner:string,id:string,claim:string,state:string,outcome?:unknown,error?:string):Promise<void>;
  accounted(owner:string,id:string,claim:string,outcome:unknown):Promise<void>;
  putAudio(owner:string,id:string,part:any):Promise<void>;
  audio(owner:string,id:string):Promise<any[]>;
}
export interface AssistantDependencies {
  authenticate(request:Request):Promise<{owner:string;repo:AssistantRepository}>;
  enabled:boolean;
  allowedOrigins:readonly string[];
  generate(options:{kind:'asr'|'interpret';prompt:string;schema:unknown;audio?:Uint8Array;audioMimeType?:string;sampleRate?:number;frames?:number;
    validate:(value:unknown)=>unknown;dispatched:()=>Promise<void>;completed:(value:unknown,accounting:unknown)=>Promise<void>;accounted:()=>Promise<void>}):Promise<unknown>;
  search(request:Request,intent:Intent,operationId:string):Promise<Record<string,any>>;
  currentCards(owner:string,ids:string[]):Promise<Record<string,any>[]>;
  maxAudioBytes:number;
  loadCompressedAudio?:(repo:AssistantRepository,owner:string,id:string,manifest:any)=>Promise<{bytes:Uint8Array;mimeType:string}>;
}
export function publicOperation(row:Operation):Record<string,unknown> {
  const stale=row.state==='processing' && Date.now()-Date.parse(row.updated_at || '')>300000;
  return {contract:ASSISTANT_CONTRACT,id:row.id,kind:row.kind,state:stale&&row.dispatched?'outcome_unknown':row.state,
    result:row.state==='completed'?row.outcome?.result:null,error:row.error_code || null,
    accounting_pending:row.outcome?.accounting?.pending===true};
}
export async function boundedJson(request:Request,limit:number):Promise<any> {
  const declared=Number(request.headers.get('content-length'));
  if (Number.isFinite(declared)&&declared>limit) reject('request_too_large',413);
  if (!request.body) reject('missing_body');
  const reader=request.body.getReader();const chunks:Uint8Array[]=[];let size=0;
  try { while(true) { const item=await reader.read();if(item.done) break;
    size+=item.value.length;if(size>limit) { await reader.cancel();reject('request_too_large',413); }chunks.push(item.value); }
  } finally { reader.releaseLock(); }
  const bytes=new Uint8Array(size);let at=0;for(const chunk of chunks){bytes.set(chunk,at);at+=chunk.length;}
  try { return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes)); } catch { return reject('invalid_json'); }
}
const fromBase64=(value:string):Uint8Array=>{try{return Uint8Array.from(atob(value),c=>c.charCodeAt(0));}catch{return reject('invalid_audio_encoding');}};
function manifest(value:unknown,maxBytes:number):any {
  const row=object(value,['frames','sampleRate','partCount','mimeType','digest','byteLength']);
  if(row.mimeType!==undefined){
    if(!['audio/webm;codecs=opus','audio/ogg;codecs=opus','audio/webm','audio/ogg','audio/mp4','audio/mp4;codecs=mp4a.40.2'].includes(row.mimeType)||typeof row.digest!=='string'||!/^[a-f0-9]{64}$/.test(row.digest)||!Number.isSafeInteger(row.byteLength)||row.byteLength<1||row.byteLength>16*1024*1024||row.partCount!==1)reject('invalid_compressed_manifest');
  } else if(row.digest!==undefined||row.byteLength!==undefined)reject('invalid_manifest');
  if(!Number.isSafeInteger(row.frames)||row.frames<1||row.frames*2+44>maxBytes)reject('audio_capacity',413);
  if(!Number.isInteger(row.sampleRate)||row.sampleRate<8000||row.sampleRate>96000||!Number.isInteger(row.partCount)||row.partCount<1||row.partCount>256)reject('invalid_manifest');
  return row;
}
function validPart(value:any):any {
  const row=object(value,['index','firstFrame','frameCount','sampleRate','digest','data']);
  if(!Number.isSafeInteger(row.index)||row.index<0||row.index>255||!Number.isSafeInteger(row.firstFrame)||row.firstFrame<0||typeof row.digest!=='string'||!/^[a-f0-9]{64}$/.test(row.digest))reject('invalid_audio_part');
  const bytes=fromBase64(text(row.data,AUDIO_BUDGET.maxWireBytes));
  validateAudioParts([{index:0,firstFrame:0,frameCount:row.frameCount,sampleRate:row.sampleRate,bytes}],row.frameCount,AUDIO_BUDGET);
  return {...row,bytes};
}
function searchAnswer(result:any,count:number):string {
  const summary=typeof result.responseSummary==='string'?result.responseSummary.trim():'';
  const facts=count?`Событий в текущей выдаче: ${count}.`:'В текущем поисковом окне нет событий с подтверждёнными условиями.';
  return summary?`${summary}\n${facts}`:count?'Подобрал события по указанным условиям.':facts;
}
const itemsOf=(row:Operation|null):any[]=>Array.isArray(row?.outcome?.result?.items)?row!.outcome.result.items:[];
async function completedDependency(repo:AssistantRepository,owner:string,id:string,kind:string):Promise<Operation|null>{
  const row=await repo.get(owner,id);
  if(!row)return null;
  if(row.kind!==kind)reject('dependency_kind',409);
  if(['failed','outcome_unknown'].includes(row.state))reject('dependency_unresolved',409);
  return row.state==='completed'?row:null;
}
export async function handleAssistant(request:Request,deps:AssistantDependencies):Promise<{status:number;body:Record<string,any>}> {
  try {
    if(!deps.enabled)reject('assistant_disabled',404);
    if(!deps.allowedOrigins.includes(request.headers.get('origin')||''))reject('origin_not_allowed',403);
    const {owner,repo}=await deps.authenticate(request);uuid(owner);
    const url=new URL(request.url);const route=url.pathname.split('/').at(-1);
    if(route==='status'&&request.method==='GET'){
      const id=url.searchParams.get('id');
      if(id){const row=await repo.get(owner,uuid(id));if(!row)reject('operation_not_found',404);const projection=publicOperation(row);
        if(row.state==='completed'&&row.kind==='search'){
          const original=itemsOf(row);const fresh=await deps.currentCards(owner,original.map(i=>String(i.event_id??i.id)));
          const lookup=new Map(fresh.map(i=>[String(i.event_id??i.id),i]));
          projection.result={...row.outcome.result,logical_event_ids:original.map(i=>String(i.event_id??i.id)),
            items:original.map(i=>lookup.get(String(i.event_id??i.id))).filter(Boolean),facts_refreshed_at:new Date().toISOString()};
          if(row.outcome.result.responseSummary&&!row.outcome.result.clarification&&row.outcome.result.explanationKind==='none')
            projection.result.answer=searchAnswer(row.outcome.result,projection.result.items.length);
        }
        return {status:200,body:projection};}
      const before=url.searchParams.get('before')||undefined;
      if(before){const pair=before.split('|');if(pair.length!==2||!/^\d{4}-\d{2}-\d{2}T[0-9:.+-]+Z?$/.test(pair[0])||!Number.isFinite(Date.parse(pair[0])))reject('invalid_cursor');uuid(pair[1]);}
      const rows=await repo.history(owner,before);
      return {status:200,body:{contract:ASSISTANT_CONTRACT,items:rows.map(r=>({id:r.id,createdAt:r.created_at,title:r.outcome?.result?.title||'Ответ'})),next:rows.length===20?`${rows.at(-1)?.created_at}|${rows.at(-1)?.id}`:null}};
    }
    if(request.method!=='POST'||!['control','audio'].includes(route||''))reject('method_not_allowed',405);
    const body=await boundedJson(request,route==='audio'?AUDIO_BUDGET.maxWireBytes:65536);
    if(route==='audio'){
      object(body,['id','part']);const id=uuid(body.id);const part=validPart(body.part);
      if(await sha256(part.bytes)!==part.digest)reject('audio_digest_mismatch',409);
      await repo.putAudio(owner,id,part);return{status:200,body:{id,index:part.index,state:'accepted',digest:part.digest}};
    }
    object(body,['id','kind','payload','run']);const id=uuid(body.id);
    if(!['asr','interpret','search'].includes(body.kind)||typeof body.run!=='boolean')reject('invalid_operation');
    let payload:any;
    if(body.kind==='asr')payload=manifest(body.payload,deps.maxAudioBytes);
    else if(body.kind==='interpret')payload=confirmedInput(body.payload);
    else {payload=object(body.payload,['interpretationId']);uuid(payload.interpretationId);}
    let row=await repo.admit(owner,id,body.kind,payload);
    if(!body.run||row.state==='completed'||['failed','outcome_unknown'].includes(row.state))return{status:row.state==='completed'?200:202,body:publicOperation(row)};
    // Admission is durable even if an earlier upload/interpretation has not yet
    // arrived. An explicit run of this same ID can resume without losing U2.
    let parent:Operation|null=null,previous:Operation|null=null,interpreted:Operation|null=null;
    if(body.kind==='interpret'){
      if(payload.parentId){parent=await completedDependency(repo,owner,payload.parentId,'search');if(!parent)return{status:202,body:{...publicOperation(row),waiting:'parent'}};}
      if(payload.mode==='continue_draft'){
        previous=await completedDependency(repo,owner,payload.previousId,'interpret');
        if(!previous)return{status:202,body:{...publicOperation(row),waiting:'previous'}};
        if(previous.outcome.result.parentId!==payload.parentId)reject('parent_conflict',409);
      }
    }
    if(body.kind==='search'){
      interpreted=await completedDependency(repo,owner,payload.interpretationId,'interpret');
      if(!interpreted)return{status:202,body:{...publicOperation(row),waiting:'interpretation'}};
      const parentId=interpreted.outcome.result.parentId;
      if(parentId){parent=await completedDependency(repo,owner,parentId,'search');if(!parent)return{status:202,body:{...publicOperation(row),waiting:'parent'}};}
    }
    const compressed=body.kind==='asr'&&payload.mimeType?(deps.loadCompressedAudio?await deps.loadCompressedAudio(repo,owner,id,payload):reject('compressed_audio_unavailable',415)):undefined;
    const audio=body.kind==='asr'?(compressed?.bytes||await assemble(await repo.audio(owner,id),payload)):undefined;
    const claim=await repo.claim(owner,id);
    if(!claim.claimed){row=(await repo.get(owner,id))!;return{status:202,body:publicOperation(row)};}
    let sent=false;let durable=false;let savedOutcome:any;
    const dispatch=async()=>{await repo.checkpoint(owner,id,claim.claim_id!,'dispatched');sent=true;};
    const complete=async(result:unknown,accounting:unknown=null)=>{
      savedOutcome={result,accounting};await repo.checkpoint(owner,id,claim.claim_id!,'completed',savedOutcome);durable=true;
    };
    const accounted=async()=>{await repo.accounted(owner,id,claim.claim_id!,savedOutcome);};
    try{
      if(body.kind==='asr'){
        await deps.generate({kind:'asr',prompt:transcriptionPrompt(),
          schema:TRANSCRIPT_SCHEMA,audio,audioMimeType:compressed?.mimeType,sampleRate:payload.sampleRate,frames:payload.frames,dispatched:dispatch,completed:complete,accounted,
          validate:value=>{const r=object(value,['text','uncertain']);text(r.text,65536,true);if(!Array.isArray(r.uncertain)||r.uncertain.length>256||r.uncertain.some((v:unknown)=>typeof v!=='string'||v.length>512))reject('invalid_transcript');return {...r,vocabulary_version:ASR_VOCABULARY_VERSION};}});
      } else if(body.kind==='interpret'){
        const input=payload as ConfirmedInput;
        const base=previous?.outcome.result.intent||parent?.outcome.result.intent||initialState().activeIntent;
        const parentItems=itemsOf(parent);
        if(input.visibleIds.some(v=>!parentItems.some(item=>String(item.event_id??item.id)===v)))reject('untrusted_visible_ids',409);
        const question=previous?`${previous.outcome.result.question}\n${input.text}`:input.text;
        if(question.length>65536)reject('draft_capacity',413);
        await deps.generate({kind:'interpret',prompt:interpreterPrompt(input,base,parentItems.map(item=>({id:item.event_id,title:item.title}))),schema:INTERPRETATION_SCHEMA,
          dispatched:dispatch,completed:complete,accounted,validate:value=>({...interpretation(value,base),question,parentId:input.parentId,
            mode:previous?.outcome.result.mode||input.mode,anchor:input.anchor,visibleIds:input.visibleIds})});
      } else {
        const result=interpreted!.outcome.result;let answer:string='';let search:any={items:[],catalog_revision:parent?.outcome?.result?.catalog_revision||'facts-v1'};
        if(result.clarification)answer=result.clarification;
        else if(result.explanationKind!=='none'){
          if(!parent)reject('parent_required');
          const id=result.ordinal?result.visibleIds[result.ordinal-1]:null;
          if(!id)answer='Уточните, о каком событии рассказать: выберите карточку или назовите её номер.';
          else{
            const current=(await deps.currentCards(owner,[id]))[0];
            if(!current)answer='Событие больше не доступно в текущем каталоге.';
            else {const d=current.display||{};const address=d.address||current.address;
              answer=result.explanationKind==='address'?(address?String(address):'В карточке нет подтверждённого адреса.'):
                [current.title,d.display_date_time,d.place,d.address].filter(v=>typeof v==='string'&&v).join(' · ');}
          }
        } else if(result.mode==='refine_selection'){
          const parentItems=itemsOf(parent);const fresh=await deps.currentCards(owner,parentItems.map(i=>String(i.event_id??i.id)));
          const byId=new Map(fresh.map(i=>[String(i.event_id??i.id),i]));
          // Full parent membership, NOT the currently rendered page; preserve rank.
          search={...parent!.outcome.result,items:parentItems.map(i=>byId.get(String(i.event_id??i.id))).filter(i=>i&&eligible(i,result.intent)),has_more:false};
        } else {
          await dispatch();search=await deps.search(request,result.intent,id);
          if(search.error)reject('search_failed',502);
        }
        const items=Array.isArray(search.items)?search.items:[];
        await complete({...search,...result,id,items,parentId:result.parentId,
          title:result.title,answer:answer||searchAnswer(result,items.length),
          served_list_id:crypto.randomUUID(),served_list_hash:await sha256(new TextEncoder().encode(JSON.stringify(items.map((i:any)=>i.event_id??i.id)))),
          source_served_list_id:search.served_list_id||null,has_more:search.has_more===true,
          membership_complete:search.membership_complete===true, membership_scope:'bounded_canonical_search_window'});
      }
    }catch(error){
      if(!durable){
        const code=error instanceof AssistantError?error.code:'provider_or_storage_failed';
        await repo.checkpoint(owner,id,claim.claim_id!,sent?'outcome_unknown':'failed',null,code).catch(()=>{});
      }
      // A durable provider outcome is authoritative even if quota finalization
      // or HTTP acknowledgement was lost. Never replace it with another call.
    }
    row=(await repo.get(owner,id))!;
    return{status:row.state==='completed'?200:202,body:publicOperation(row)};
  }catch(error){return{status:error instanceof AssistantError?error.status:503,body:{contract:ASSISTANT_CONTRACT,error:error instanceof AssistantError?error.code:'assistant_unavailable'}};}
}
