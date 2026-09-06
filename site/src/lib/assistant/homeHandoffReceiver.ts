import {validateHandoff,type HandoffScope} from './searchHandoff.ts';
import type {VoiceStore} from './voiceStore.ts';
import type {AssistantClient} from './assistantClient.ts';
import type {ConversationController} from './conversationController.ts';
type Options={owner:string;id:string;scope:HandoffScope;store:VoiceStore;api:AssistantClient;controller:ConversationController;isCurrent:()=>boolean;status:(copy:string)=>void};
const active=new Map<string,Promise<void>>();
/** Route startup only recovers this explicit submitted envelope, never arbitrary drafts. */
export function receiveHomeHandoff(options:Options):Promise<void>{
 const key=`${options.scope.storageScope}:${options.owner}:${options.id}`;
 const existing=active.get(key);if(existing)return existing;
 const work=async()=>{
  const {owner,id,scope,store,api,controller,isCurrent,status}=options;
  const check=()=>{if(!isCurrent())throw Error('voice_identity_changed');};check();
  const row=validateHandoff(await store.handoff(owner,id),owner,scope);check();
  if(row.status==='cancelled'||row.status==='empty'){status('Запрос не отправлен. Можно написать новый.');return;}
  let text=row.payload.kind==='text'?row.payload.text:'';
  if(row.payload.kind==='audio'){
   const recording=await store.recording(owner,row.payload.recordingId);check();
   if(recording?.state!=='saved'||!recording.receipt?.complete||recording.receipt.speechEvidence!==true)throw Error('voice_audio_incomplete');
   if(recording.transcript!==undefined)text=recording.transcript;
   else {
    status('Запись принята. Распознаю запрос…');
    const parts=await store.parts(owner,recording.id),compressed=await store.compressed(owner,recording.id);check();
    const receipt=await api.transcribe(owner,row.asrId,parts,compressed);check();
    if(receipt.state!=='completed')throw Error(receipt.state==='outcome_unknown'?'voice_outcome_unknown':`voice_${receipt.state}`);
    if(typeof receipt.result?.text!=='string')throw Error('voice_transcript_invalid');
    text=receipt.result.text;await store.setTranscript(owner,recording.id,text);check();
   }
  }
  if(!text.trim()){await store.markHandoff(owner,id,'empty');status('Не удалось услышать запрос. Поиск не запускался; запись сохранена.');return;}
  status('Запрос сохранён. Подбираю события…');
  const command=await controller.acceptHomeHandoff(id,scope,text);check();
  await controller.processHomeHandoff(command);check();
  await store.markHandoff(owner,id,'completed');
 };
 // Web Locks reduce duplicate transport, server receipts remain the authority if unavailable.
 const task:Promise<void>=(async()=>{if(globalThis.navigator?.locks)await navigator.locks.request(`ke-home-${key}`,work);else await work();})();
 active.set(key,task);void task.finally(()=>active.delete(key)).catch(()=>{});return task;
}
