import { ASSISTANT_CONTRACT, AUDIO_BUDGET } from '../../../../supabase/functions/event-search/assistant-intent.ts';
import { validateAudioParts, type AudioPart } from './audioSegments.ts';
export type RemoteReceipt = {id:string;kind:string;state:string;result:any;error?:string;waiting?:string;accounting_pending?:boolean};
export type Command = {id:string;searchId:string;payload:Record<string,any>;input:any;createdAt:string};
export interface AuthPort {client:{auth:{getSession():Promise<any>}};dataClient:{request(input:string,init?:RequestInit):Promise<Response>};}
/** No new transport, auth session, URL selector or replay loop. */
export class AssistantClient {
  private auth:AuthPort;private base:string;private key:string;private owner:()=>string;private networkEnabled:()=>boolean;
  constructor(auth:AuthPort,base:string,key:string,owner:()=>string,networkEnabled:()=>boolean=()=>true,devcoveer=false){this.networkEnabled=networkEnabled;this.auth=auth;this.base=devcoveer?'https://mcp-datahub.kenigevents.ru/kenig-audio/event-search/assistant':base.replace(/\/+$/,'')+'/functions/v1/event-search/assistant';this.key=key;this.owner=owner;}
  async request(route:string,owner:string,body?:unknown,mediaType?:string):Promise<any>{
    if(!this.networkEnabled())throw new Error('voice_capture_only');
    if(!owner||this.owner()!==owner)throw new Error('voice_identity_changed');
    const {data,error}=await this.auth.client.auth.getSession();
    const session=data?.session;
    if(error||!session?.access_token||session.user?.id!==owner||session.user?.is_anonymous)throw new Error('voice_auth_required');
    if(this.owner()!==owner)throw new Error('voice_identity_changed');
    const response=await this.auth.dataClient.request(`${this.base}/${route}`,{method:body===undefined?'GET':'POST',
      headers:{Authorization:`Bearer ${session.access_token}`,apikey:this.key,'Content-Type':mediaType||'application/json',Accept:'application/json'},
      // A search control includes bounded semantic verification of the full window.
      signal:AbortSignal.timeout(route==='control'?90000:60000),...(body===undefined?{}:{body:mediaType?body as BodyInit:JSON.stringify(body)})});
    const value=await response.json();
    if(this.owner()!==owner)throw new Error('voice_identity_changed');
    if(!response.ok)throw new Error(typeof value.error==='string'?value.error:'voice_request_failed');
    if(route.startsWith('status')||route==='control'){
      if(value.contract!==ASSISTANT_CONTRACT)throw new Error('voice_contract_mismatch');
    }
    return value;
  }
  status(owner:string,id:string):Promise<RemoteReceipt>{return this.request(`status?id=${encodeURIComponent(id)}`,owner);}
  history(owner:string,before?:string){return this.request(`status${before?`?before=${encodeURIComponent(before)}`:''}`,owner);}
  control(owner:string,id:string,kind:string,payload:unknown,run:boolean):Promise<RemoteReceipt>{return this.request('control',owner,{id,kind,payload,run});}
  /** Explicit execution only. A lost response is reconciled by safe read, never
   * by issuing a second POST. Polling is bounded and pauses in hidden documents.
   */
  async execute(owner:string,id:string,kind:string,payload:unknown):Promise<RemoteReceipt>{
    let receipt:RemoteReceipt;
    try{receipt=await this.control(owner,id,kind,payload,true);}
    catch(error){try{return await this.status(owner,id);}catch{throw error;}}
    for(let attempt=0;receipt.state==='processing'&&attempt<6&&!document.hidden;attempt++){
      await new Promise(resolve=>setTimeout(resolve,Math.min(5000,1000*(attempt+1))));
      receipt=await this.status(owner,id);
    }
    return receipt;
  }
  async transcribe(owner:string,id:string,parts:AudioPart[],compressed?:{mimeType:string;bytes:Uint8Array}|null):Promise<RemoteReceipt>{
    if(!parts.length)throw new Error('voice_audio_empty');
    // Validate the full continuous manifest without the batch helper's 4M-frame
    // limit. The server enforces its separately configured full-utterance cap.
    let frames=0;
    for(const [index,part]of parts.entries()){
      if(part.index!==index||part.firstFrame!==frames||part.sampleRate!==parts[0].sampleRate)throw new Error('voice_audio_incomplete');
      validateAudioParts([{...part,index:0,firstFrame:0}],part.frameCount,AUDIO_BUDGET);frames+=part.frameCount;
    }
    const mediaDigest=compressed?Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',compressed.bytes as BufferSource)),x=>x.toString(16).padStart(2,'0')).join(''):undefined;
    const payload={frames,sampleRate:parts[0].sampleRate,partCount:compressed?1:parts.length,...(compressed?{mimeType:compressed.mimeType,digest:mediaDigest,byteLength:compressed.bytes.length}:{})};
    let receipt:RemoteReceipt;
    try{receipt=await this.control(owner,id,'asr',payload,false);}
    catch(error){try{receipt=await this.status(owner,id);}catch{throw error;}}
    if(receipt.state==='completed'||receipt.state==='outcome_unknown'||receipt.state==='failed')return receipt;
    if(receipt.state!=='accepted')return this.status(owner,id);
    if(compressed){
      const accepted=await this.request(`media?id=${encodeURIComponent(id)}`,owner,compressed.bytes,compressed.mimeType);
      if(accepted.id!==id||accepted.digest!==mediaDigest||accepted.byteLength!==compressed.bytes.length||accepted.state!=='accepted')throw new Error('voice_audio_receipt_invalid');
    }else for(const part of parts){
      const digest=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',part.bytes as BufferSource)),x=>x.toString(16).padStart(2,'0')).join('');
      let binary='';for(let i=0;i<part.bytes.length;i+=8192)binary+=String.fromCharCode(...part.bytes.subarray(i,i+8192));
      const accepted=await this.request('audio',owner,{id,part:{index:part.index,firstFrame:part.firstFrame,frameCount:part.frameCount,sampleRate:part.sampleRate,digest,data:btoa(binary)}});
      if(accepted.id!==id||accepted.index!==part.index||accepted.digest!==digest||accepted.state!=='accepted')throw new Error('voice_audio_receipt_invalid');
    }
    return this.execute(owner,id,'asr',payload);
  }
}
