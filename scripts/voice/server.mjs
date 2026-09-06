/** DevCoveer HOST ADAPTER for the existing Search/assistant core, not another
 * search engine/Auth/provider/limiter. Raw audio and receipts stay on local disk. */
import {createServer} from 'node:http';
import {spawn} from 'node:child_process';
import {createHash} from 'node:crypto';
import {createAssistantDependencies} from '../../supabase/functions/event-search/index.ts';
import {handleAssistant} from '../../supabase/functions/event-search/assistant-handler.ts';
import {AssistantError,ASSISTANT_CONTRACT,uuid,reject} from '../../supabase/functions/event-search/assistant-intent.ts';
import {DevCoveerReceiptStore} from './receiptStore.mjs';
const PREFIX='/kenig-audio/event-search/assistant/';
const MAX_UPLOAD=16*1024*1024;
function digest(bytes){return createHash('sha256').update(bytes).digest('hex');}
function mediaProcess(program,args,input,maxBytes){
 return new Promise((resolve,rejectPromise)=>{
  const p=spawn(program,args,{stdio:['pipe','pipe','pipe']});let size=0;const chunks=[];let settled=false;
  const finish=(error,result)=>{if(settled)return;settled=true;clearTimeout(timer);if(error){p.kill('SIGKILL');rejectPromise(new AssistantError(error,422));}else resolve(result);};
  const timer=setTimeout(()=>finish('audio_decode_timeout'),20000);
  p.on('error',()=>finish('audio_decoder_unavailable'));p.stdin.on('error',()=>{});
  p.stdout.on('data',chunk=>{size+=chunk.length;if(size>maxBytes)finish('decoded_audio_capacity');else chunks.push(chunk);});
  p.stderr.resume();p.on('close',code=>finish(code?'invalid_audio_container':null,Buffer.concat(chunks)));p.stdin.end(input);
 });
}
export async function validateMedia(bytes,mimeType,manifest,options={}){
 const bare=mimeType.split(';')[0].trim().toLowerCase();
 const probe=await mediaProcess(options.ffprobe||process.env.VOICE_FFPROBE||'ffprobe',['-v','error','-show_streams','-of','json','-i','pipe:0'],bytes,65536);
 const streams=JSON.parse(probe.toString()).streams||[];const stream=streams[0];
 if(streams.length!==1||stream?.codec_type!=='audio'||stream.channels!==1||!['opus','aac'].includes(stream.codec_name))reject('unsupported_audio_codec',415);
 if((bare==='audio/mp4')!==(stream.codec_name==='aac'))reject('audio_mime_mismatch',415);
 const rate=Number(stream.sample_rate);if(!Number.isInteger(rate)||rate<8000||rate>96000)reject('invalid_sample_rate');
 const pcm=await mediaProcess(options.ffmpeg||process.env.VOICE_FFMPEG||'ffmpeg',['-v','error','-i','pipe:0','-map','0:a:0','-vn','-acodec','pcm_s16le','-f','s16le','pipe:1'],bytes,64*1024*1024);
 const duration=pcm.length/2/rate,expected=manifest.frames/manifest.sampleRate;
 // Native encoder priming/padding and independent worklet start clocks differ.
 // Never trim/resample original compressed bytes to force equality.
 if(!duration||Math.abs(duration-expected)>Math.max(.75,expected*.02))reject('compressed_duration_mismatch',409);
 return {mimeType:bare,duration,codec:stream.codec_name,bytes:bytes.length};
}
export function repositoryAdapter(store){
 return new Proxy(store,{get(target,name){const value=target[name];if(typeof value!=='function')return value;return async(...args)=>{try{return await value.apply(target,args);}catch(e){if(e?.code&&Number.isInteger(e.status))throw new AssistantError(e.code,e.status);throw e;}};}});
}
export function createVoiceServer({store,dependencyFactory=createAssistantDependencies,sourceSha=process.env.VOICE_SOURCE_SHA||'development'}={}){
 if(!store)throw Error('voice_store_required');const repo=repositoryAdapter(store);
 const origins=(process.env.EVENT_SEARCH_ASSISTANT_ORIGINS||'https://kenigevents.ru').split(',').map(s=>s.trim());
 return createServer(async(req,res)=>{
  const origin=req.headers.origin||'';
  const cors={'Cache-Control':'no-store','Content-Type':'application/json; charset=utf-8','Vary':'Origin',...(origins.includes(origin)?{'Access-Control-Allow-Origin':origin,'Access-Control-Allow-Headers':'authorization,apikey,content-type','Access-Control-Allow-Methods':'GET,POST,OPTIONS','Access-Control-Max-Age':'300'}:{})};
  const reply=(status,body)=>{res.writeHead(status,cors);res.end(JSON.stringify(body));};
  try{
   const url=new URL(req.url,'http://127.0.0.1');
   if(url.pathname==='/kenig-audio/healthz'&&req.method==='GET')return reply(200,{contract:ASSISTANT_CONTRACT,source_sha:sourceSha,host:'devcoveer',media:'opus-or-aac',raw_media_supabase:false});
   if(!['control','status','audio','media'].some(route=>url.pathname===PREFIX+route))return reply(404,{error:'not_found'});
   if(!origins.includes(origin))return reply(403,{error:'origin_not_allowed'});
   if(req.method==='OPTIONS')return reply(204,{});
   const headers=new Headers();for(const [k,v] of Object.entries(req.headers))if(v!==undefined)headers.set(k,Array.isArray(v)?v.join(','):v);
   const deps=dependencyFactory(repo);
   if(!deps.enabled)return reply(404,{error:'assistant_disabled'});
   // Verify ordinary Supabase identity before accepting potentially large media.
   const authenticated=await deps.authenticate(new Request(url,{headers}));
   deps.authenticate=async()=>authenticated;
   const limit=url.pathname.endsWith('/media')?MAX_UPLOAD:url.pathname.endsWith('/audio')?1024*1024:65536;
   if(Number(req.headers['content-length'])>limit)return reply(413,{error:'request_too_large'});
   const chunks=[];let total=0;
   for await(const chunk of req){total+=chunk.length;if(total>limit){reply(413,{error:'request_too_large'});return;}chunks.push(chunk);}
   const bytes=Buffer.concat(chunks);
   if(url.pathname.endsWith('/media')){
    if(req.method!=='POST')return reply(405,{error:'method_not_allowed'});
    const id=uuid(url.searchParams.get('id')),op=await repo.get(authenticated.owner,id);
    if(!op||op.kind!=='asr')reject('operation_not_found',404);
    const manifest=op.payload,mimeType=headers.get('content-type')||'';
    if(mimeType!==manifest.mimeType||bytes.length!==manifest.byteLength||digest(bytes)!==manifest.digest)reject('audio_digest_mismatch',409);
    const old=await repo.media(authenticated.owner,id);
    if(!old){await validateMedia(bytes,mimeType,manifest);await repo.putMedia(authenticated.owner,id,{mimeType,digest:manifest.digest,bytes});}
    else if(old.digest!==manifest.digest)reject('audio_payload_conflict',409);
    return reply(200,{id,digest:manifest.digest,byteLength:bytes.length,state:'accepted'});
   }
   deps.loadCompressedAudio=async(_repo,owner,id,manifest)=>{const media=await repo.media(owner,id);if(!media||media.digest!==manifest.digest||media.bytes.length!==manifest.byteLength)reject('audio_incomplete',409);return {bytes:media.bytes,mimeType:media.mimeType.split(';')[0]};};
   const request=new Request(url,{method:req.method,headers,...(['GET','HEAD'].includes(req.method)?{}:{body:bytes})});
   const result=await handleAssistant(request,deps);reply(result.status,result.body);
  }catch(error){const known=error instanceof AssistantError||(Number.isInteger(error?.status)&&error.status>=400&&error.status<=599&&/^[a-z0-9_]{1,80}$/.test(error?.code||''));reply(known?error.status:503,{contract:ASSISTANT_CONTRACT,error:known?error.code:'voice_host_unavailable'});}
 });
}
if(process.env.VOICE_SERVER_AUTOSTART==='1'){
 if(!process.env.VOICE_STATE_FILE||!process.env.EVENT_SEARCH_ASSISTANT_PREVIEW_USER_IDS||!process.env.EVENT_SEARCH_ASSISTANT_POLICY_REF)throw Error('protected_voice_configuration_required');
 const store=new DevCoveerReceiptStore(process.env.VOICE_STATE_FILE);
 const server=createVoiceServer({store});server.requestTimeout=120000;server.headersTimeout=15000;
 server.listen(Number(process.env.PORT||14320),'127.0.0.1',()=>console.log(JSON.stringify({event:'voice_host_ready',source_sha:process.env.VOICE_SOURCE_SHA,port:Number(process.env.PORT||14320)})));
 let closing=false;for(const signal of ['SIGTERM','SIGINT'])process.on(signal,()=>{if(closing)return;closing=true;server.close(()=>{store.close();process.exit(0);});});
}
