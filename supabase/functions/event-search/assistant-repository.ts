import { reject } from './assistant-intent.ts';
import type { AssistantRepository, Operation } from './assistant-handler.ts';
/** The caller constructs this adapter only after auth.getUser + eligibility.
 * All queries bind the verified owner, never an owner field from JSON.
 */
export function assistantRepository(client: any): AssistantRepository {
  async function rpc(name: string, args: Record<string, unknown>): Promise<any> {
    const {data,error}=await client.rpc(name,args);
    if(error){
      if(error.code==='23505')reject('payload_conflict',409);
      if(error.code==='42501')reject('operation_not_found',404);
      if(error.code==='40001')reject('revision_conflict',409);
      if(error.code==='22023')reject('invalid_manifest',400);
      if(error.code==='28000')reject('eligible_user_required',401);
      reject('receipt_store_unavailable',503);
    }
    return data;
  }
  const operations=()=>client.from('event_search_assistant_operations');
  return {
    admit:(owner,id,kind,payload)=>rpc('event_search_assistant_admit_v1',{p_owner:owner,p_id:id,p_kind:kind,p_payload:payload}),
    async get(owner,id){const {data,error}=await operations().select('*').eq('owner_id',owner).eq('id',id).maybeSingle();if(error)reject('receipt_store_unavailable',503);return data;},
    async history(owner,before){
      let query=operations().select('id,created_at,outcome').eq('owner_id',owner).eq('kind','search').eq('state','completed');
      if(before){const [date,id]=before.split('|');query=query.or(`created_at.lt.${date},and(created_at.eq.${date},id.lt.${id})`);}
      const {data,error}=await query.order('created_at',{ascending:false}).order('id',{ascending:false}).limit(20);
      if(error)reject('receipt_store_unavailable',503);return data||[];
    },
    claim:(owner,id)=>rpc('event_search_assistant_claim_v1',{p_owner:owner,p_id:id}),
    async checkpoint(owner,id,claim,state,outcome=null,error){await rpc('event_search_assistant_checkpoint_v1',{p_owner:owner,p_id:id,p_claim:claim,p_state:state,p_outcome:outcome,p_error:error??null});},
    async accounted(owner,id,claim,outcome:any){
      const value={...outcome,accounting:outcome.accounting?{...outcome.accounting,pending:false}:null};
      const {data,error}=await operations().update({outcome:value}).eq('owner_id',owner).eq('id',id).eq('claim_id',claim).eq('state','completed').select('id');
      if(error||data?.length!==1)reject('accounting_checkpoint_pending',503);
    },
    async putAudio(owner,id,part){await rpc('event_search_assistant_audio_part_v1',{p_owner:owner,p_id:id,p_index:part.index,p_first:part.firstFrame,p_frames:part.frameCount,p_rate:part.sampleRate,p_digest:part.digest,p_audio:part.data});},
    async audio(owner,id){
      const op=await this.get(owner,id);if(!op)reject('operation_not_found',404);
      const {data,error}=await client.from('event_search_assistant_audio_parts').select('*').eq('operation_id',id).order('part_index');
      if(error)reject('audio_store_unavailable',503);
      return (data||[]).map((row:any)=>{
        if(typeof row.audio!=='string'||!/^\\x(?:[0-9a-f]{2})*$/i.test(row.audio))reject('audio_store_invalid',503);
        const bytes=Uint8Array.from(row.audio.slice(2).match(/../g)||[],(hex:any)=>parseInt(hex,16));
        return {index:row.part_index,firstFrame:Number(row.first_frame),frameCount:row.frame_count,sampleRate:row.sample_rate,digest:row.digest,bytes};
      });
    },
  };
}
