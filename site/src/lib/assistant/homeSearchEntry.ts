import {getStaticSiteAuth} from '../staticSiteAuth';
import {VoiceStore} from './voiceStore.ts';
import {MicrophoneCapture,type CaptureReceipt} from './microphoneCapture.ts';
import {handoffScope,handoffUrl,validateHandoff,type HomeHandoff} from './searchHandoff.ts';
import {AUDIO_BUDGET} from './domain/assistant-intent.ts';
const mounted=new WeakMap<HTMLElement,Promise<void>>();
type Dependencies={auth?:ReturnType<typeof getStaticSiteAuth>;openStore?:typeof VoiceStore.open;Capture?:typeof MicrophoneCapture;navigate?:(url:string)=>void};
/** Presentation adapter only. Durable commands/voice and adoption belong to Search. */
export function mountHomeSearchEntry(root:HTMLElement,deps:Dependencies={}):Promise<void>{
 const prior=mounted.get(root);if(prior)return prior;
 const task=mount(root,deps);mounted.set(root,task);return task;
}
async function mount(root:HTMLElement,deps:Dependencies):Promise<void>{
 const get=<T extends HTMLElement>(name:string):T=>{const host=root.querySelector<HTMLElement>(`[data-home-${name}]`)!;return (host.matches('button,input,textarea,a,form,p')?host:host.querySelector('button,input,textarea,a')||host) as T;};
 const text=get<HTMLTextAreaElement>('text'),record=get<HTMLButtonElement>('record'),stop=get<HTMLButtonElement>('stop'),cancel=get<HTMLButtonElement>('cancel'),submit=get<HTMLButtonElement>('submit'),login=get<HTMLButtonElement>('login'),next=get<HTMLAnchorElement>('continue'),fresh=get<HTMLButtonElement>('new'),status=get('status');
 const events=new AbortController();let alive=true,owner='',generation=0,ready=false,busy=false,submitted=false;
 let capture:MicrophoneCapture|null=null,recordingId:string|null=null,pendingId:string|null=null;
 let saving=Promise.resolve(),finalization=Promise.resolve();let store:VoiceStore;
 const scope=await handoffScope(location.origin,root.dataset.prefix||'/');
 const state=(value:string,copy?:string)=>{root.dataset.homeSearchState=value;root.dataset.dsState=value;if(copy!==undefined)status.textContent=copy;};
 text.maxLength=8192;
 const controls=()=>{root.dataset.homeReady='true';const enabled=ready&&!!owner&&root.dataset.capability==='true';root.dataset.searchEnabled=String(enabled);
  const running=!!capture&&['requesting','recording','stopping'].includes(capture.status);const unsaved=!!capture?.unsavedParts?.().length;
  text.disabled=!enabled||busy||submitted||running;record.disabled=!enabled||busy||submitted||running||unsaved;submit.disabled=!enabled||busy||submitted||running;
  submit.hidden=!text.value.trim()&&!recordingId;stop.hidden=cancel.hidden=!running&&!unsaved;stop.textContent=unsaved?'Повторить сохранение':'Завершить и искать';stop.disabled=busy;submit.textContent=recordingId&&!text.value.trim()?'Искать по записи':'Найти события';login.hidden=!!owner||root.dataset.capability!=='true';next.hidden=fresh.hidden=!submitted;
 };
 const fail=()=>{busy=false;state('error','Данные не удалены. Проверьте подключение и повторите действие.');controls();};
 const auth=deps.auth||getStaticSiteAuth({supabaseUrl:root.dataset.supabaseUrl||'',relayUrl:root.dataset.supabaseRelayUrl||'',publishableKey:root.dataset.supabaseKey||'',provider:root.dataset.yandexProvider});
 const persist=()=>{if(!owner||!ready)return Promise.resolve();const own=owner,payload={text:text.value,recordingId,pendingId};const task=saving.catch(()=>{}).then(()=>store.saveHomeDraft(own,payload));saving=task;return task;};
 const handoff=async(payload:HomeHandoff['payload'])=>{
  if(!owner||!ready||submitted)return;const own=owner,epoch=generation;
  busy=true;state('saving','Сохраняю запрос…');controls();pendingId ||= crypto.randomUUID();
  await persist();await finalization;await saving;
  const row=await store.prepareHandoff(own,scope,payload,pendingId);
  // Readback is deliberately after the strict write transaction, before navigation.
  validateHandoff(await store.handoff(own,row.id),own,scope);
  if(!alive||epoch!==generation)throw Error('voice_identity_changed');
  submitted=true;busy=false;next.href=handoffUrl(row,scope);state('submitted','Запрос сохранён. Продолжение — в поиске.');controls();
  (deps.navigate||((url:string)=>location.assign(url)))(next.href);
 };
 const send=async()=>{if(busy||submitted||!owner)return;const value=text.value.trim();
  if(value){await handoff({kind:'text',text:value});return;}
  if(recordingId){const row=await store.recording(owner,recordingId);if(row?.receipt?.complete&&row.receipt.speechEvidence){await handoff({kind:'audio',recordingId});return;}}
  state('idle','Напишите запрос или запишите голос. Пустая запись не отправляется.');
 };
 const finish=async(search:boolean)=>{if(!capture||busy)return;busy=true;state('saving','Завершаю и сохраняю запись…');controls();let receipt=await capture.stop(search?'user':'cancelled');await finalization;if(search&&capture.unsavedParts?.().length){await capture.retryUnsaved();receipt=capture.receipt()!;if(recordingId)await store.finish(owner,recordingId,receipt);}busy=false;
  if(!search){recordingId=null;pendingId=null;await persist();state('idle','Запись отменена. Поиск не запускался.');controls();return;}
  if(!receipt.complete||!receipt.speechEvidence){state('idle','Речь не обнаружена или запись не завершена. Поиск не запускался.');controls();return;}
  if(recordingId)await handoff({kind:'audio',recordingId});controls();
 };
 const start=()=>{if(!ready||!owner||busy||submitted)return;const own=owner,epoch=generation;recordingId=crypto.randomUUID();pendingId=null;const id=recordingId;
  const created=store.create(own,id);void created.catch(fail);const Capture=deps.Capture||MicrophoneCapture;
  capture=new Capture({workletUrl:root.dataset.worklet||'',budget:AUDIO_BUDGET,
   onPart:async part=>{await created;await store.putPart(own,id,part);},onCompressedPart:async part=>{await created;await store.putCompressedPart(own,id,part);},
   onStatus:value=>{if(epoch!==generation||!alive)return;if(value==='requesting'||value==='recording')state(value,value==='recording'?'Слушаю. Завершите запись, чтобы искать.':'Ожидаю разрешение микрофона…');if(value==='error')fail();controls();},
   onStopped:(receipt:CaptureReceipt)=>{finalization=(async()=>{await created;await store.finish(own,id,receipt);if(epoch!==generation||!alive)return;await persist();if(!busy){state('idle',receipt.complete&&receipt.speechEvidence?'Запись сохранена. Нажмите «Искать по записи».':'Запись сохранена без отправки.');controls();}})();void finalization.catch(fail);},
  });
  // Call start synchronously from the user gesture, not after IDB promises.
  void capture.start().catch(fail);void persist().catch(fail);
 };
 const on=(el:EventTarget,type:string,handler:EventListener)=>el.addEventListener(type,handler,{signal:events.signal});
 on(record,'click',()=>start());on(stop,'click',()=>{void finish(true).catch(fail);});on(cancel,'click',()=>{void finish(false).catch(fail);});
 on(get('form'),'submit',event=>{event.preventDefault();void send().catch(fail);});on(text,'input',()=>{pendingId=null;controls();void persist().catch(fail);});
 on(login,'click',()=>{void auth.signIn().catch(fail);});
 on(fresh,'click',()=>{submitted=false;pendingId=null;recordingId=null;text.value='';state('idle','Новый запрос.');controls();void persist().catch(fail);});
 let unsubscribe=()=>{};
 const teardown=()=>{alive=false;generation++;events.abort();unsubscribe();if(capture)void capture.stop('background');mounted.delete(root);window.addEventListener('pageshow',event=>{if(event.persisted)void mountHomeSearchEntry(root,deps);},{once:true});};
 on(window,'pagehide',teardown);
 if(root.dataset.capability!=='true'){state('disabled');controls();return;}
 try{store=(await (deps.openStore||VoiceStore.open)()).scoped(scope.storageScope);ready=true;}catch{fail();return;}
 if(!alive)return;
 unsubscribe=auth.subscribe(snapshot=>{
  const nextOwner=snapshot.status==='signed_in'&&snapshot.user&&!snapshot.user.is_anonymous?snapshot.user.id:'';
  if(nextOwner===owner&&root.dataset.homeSearchState!=='signed-out')return;
  owner=nextOwner;const epoch=++generation;submitted=false;busy=!!owner;pendingId=null;recordingId=null;text.value='';
  if(capture)void capture.stop('interrupted');capture=null;
  state(owner?'idle':'signed-out',owner?'Напишите или расскажите, что хочется.':'Войдите, чтобы начать. Вход не отправляет запрос.');controls();
  if(owner)void store.homeDraft(owner).then(async draft=>{
   if(!alive||epoch!==generation)return;if(!draft){busy=false;controls();return;}text.value=draft.text;recordingId=draft.recordingId;pendingId=draft.pendingId;
   if(pendingId){const row=await store.handoff(owner,pendingId);if(!alive||epoch!==generation)return;if(row){validateHandoff(row,owner,scope);submitted=true;next.href=handoffUrl(row,scope);state('submitted','Ранее отправленный запрос сохранён. Откройте продолжение в поиске.');}}
   busy=false;if(!submitted&&(text.value||recordingId))state('idle','Черновик сохранён. Отправьте его явно, когда будете готовы.');controls();
  }).catch(fail);
 });
 await auth.initialize();
}
