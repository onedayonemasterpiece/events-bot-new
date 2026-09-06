import { googleModelActionUrl, resolveStrictGoogleQuotaPool, withSharedGoogleQuotaAttempt,
  GoogleProviderAttemptError, type GoogleQuotaBackend, type GoogleQuotaKeyCandidate, type GoogleTokenUsage } from './google-quota.ts';
import { AssistantError, reject } from './assistant-intent.ts';
import type { AssistantDependencies } from './assistant-handler.ts';
const encoded=(bytes:Uint8Array):string=>{let s='';for(let i=0;i<bytes.length;i+=8192)s+=String.fromCharCode(...bytes.subarray(i,i+8192));return btoa(s);};
async function responseJson(response:Response,maxBytes:number):Promise<any>{
  const reader=response.body?.getReader();if(!reader)reject('provider_empty_response',502);
  let total=0;const chunks:Uint8Array[]=[];
  try{while(true){const {done,value}=await reader!.read();if(done)break;total+=value.length;if(total>maxBytes){await reader!.cancel();reject('provider_response_too_large',502);}chunks.push(value);}}
  finally{reader!.releaseLock();}
  const bytes=new Uint8Array(total);let offset=0;for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.length;}
  try{return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));}catch{return reject('provider_invalid_json',502);}
}
/** Website consumer of the EXISTING shared limiter. No donor owner account,
 * arbitrary model URL, fallback model, per-key bypass or raw-key browser path.
 * The outcome is checkpointed before limiter finalize, as in voice-intake v2.
 */
export function assistantGenerator(config:{backend:GoogleQuotaBackend|null;keys:GoogleQuotaKeyCandidate[];env:(key:string)=>string;fetchImpl?:typeof fetch}):AssistantDependencies['generate']{
  return async options=>{
    const model=config.env('EVENT_SEARCH_ASSISTANT_MODEL')||'gemini-3.1-flash-lite';
    const approved=(config.env('EVENT_SEARCH_ASSISTANT_APPROVED_MODELS')||'').split(',').map(v=>v.trim());
    if(!approved.includes(model)||!model.includes('flash-lite'))reject('assistant_model_not_approved',503);
    const pool=await resolveStrictGoogleQuotaPool(config.backend,config.keys);
    if(!pool.length)reject('assistant_provider_unavailable',503);
    const outputTokens=options.kind==='asr'?8192:2048;
    const inputTokens=Math.ceil(options.prompt.length/2)+(options.frames&&options.sampleRate?Math.ceil(options.frames/options.sampleRate*32):0);
    return withSharedGoogleQuotaAttempt({backend:config.backend,key:pool[0],model,reservedTpm:inputTokens+outputTokens,
      consumer:`kenigevents.voice.${options.kind}.v1`,accountName:'kenigevents',readEnv:config.env,
      execute:async(apiKey,lease)=>{
        await options.dispatched();
        const parts:any[]=[{text:options.prompt}];if(options.audio)parts.push({inlineData:{mimeType:options.audioMimeType||'audio/wav',data:encoded(options.audio)}});
        const controller=new AbortController();const timer=setTimeout(()=>controller.abort(),25000);
        try{
          const response=await (config.fetchImpl||fetch)(googleModelActionUrl(model,'generateContent'),{
            method:'POST',headers:{'Content-Type':'application/json','x-goog-api-key':apiKey},signal:controller.signal,
            body:JSON.stringify({contents:[{role:'user',parts}],generationConfig:{temperature:0,maxOutputTokens:outputTokens,responseMimeType:'application/json',responseJsonSchema:options.schema}})});
          const payload=await responseJson(response,512*1024);
          const count=(v:unknown)=>typeof v==='number'&&Number.isFinite(v)&&v>=0?Math.floor(v):null;
          const u=payload.usageMetadata||{};const usage:GoogleTokenUsage={input_tokens:count(u.promptTokenCount),output_tokens:count(u.candidatesTokenCount),total_tokens:count(u.totalTokenCount)};
          if(!response.ok)throw new GoogleProviderAttemptError('assistant_provider_http',{provider_status:`http_${response.status}`,error_type:'provider_http',error_code:String(response.status),usage});
          const candidate=payload.candidates?.[0];
          if(candidate?.finishReason!=='STOP')reject('provider_incomplete_output',502);
          const raw=(candidate.content?.parts||[]).filter((p:any)=>typeof p.text==='string'&&!p.thought).map((p:any)=>p.text).join('');
          let parsed;try{parsed=JSON.parse(raw);}catch{reject('provider_invalid_schema',502);}
          const value=options.validate(parsed);
          await options.completed(value,{pending:true,lease,usage});
          return{value,provider_status:'succeeded',usage};
        }finally{clearTimeout(timer);}
      }}).then(async value=>{await options.accounted().catch(()=>{});return value;});
  };
}
