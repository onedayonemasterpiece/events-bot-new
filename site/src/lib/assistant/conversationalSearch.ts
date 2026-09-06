import { createConversationTurn, settleConversationTurn, type ConversationTurn } from './conversationTurn.ts';
import { mountVoiceDiagnostics } from './voiceDiagnostics.ts';
import { getStaticSiteAuth } from '../staticSiteAuth';
import { MicrophoneCapture, type CaptureStatus } from './microphoneCapture.ts';
import { VoiceStore, type Recording, type StoredAnswer } from './voiceStore.ts';
import { AssistantClient } from './assistantClient.ts';
import { ConversationController } from './conversationController.ts';
import { initialState, type Section, type Mode } from './conversationState.ts';
import { AUDIO_BUDGET } from '../../../../supabase/functions/event-search/assistant-intent.ts';
import { announceAssistantSurface } from './assistantSurface.ts';
import type { mountComposerPresentation } from './composerPresentation.ts';
import { AssistantMeasurement } from './measurement.ts';
type CardHost={hiddenIds:()=>string[];register:(element:HTMLElement,items:any[],context:any)=>void;sync:()=>Promise<void>};
type Host=Window&{KenigEventsSearchCardHost?:CardHost;KenigEventsCreateEventCard?:(item:any,variant:string,layout:any)=>HTMLElement;
  KenigEventsResolveMobileEventCardMedia?:(item:any)=>any};
const node=<K extends keyof HTMLElementTagNameMap>(tag:K,text?:string)=>{const result=document.createElement(tag);if(text!==undefined)result.textContent=text;return result;};
const button=(label:string,fn:()=>unknown)=>{const el=node('button',label);el.type='button';el.className='secondary-button';el.addEventListener('click',()=>{void fn();});return el;};
const errorText=(error:unknown):string=>{
  const code=error instanceof Error?error.message:'';
  if(code.includes('identity')||code.includes('auth'))return 'Для этого действия нужен тот же вошедший аккаунт. Запись и подтверждённый ввод сохранены.';
  if(code.includes('outcome_unknown'))return 'Ответ провайдера не подтверждён. Повторного запуска не было; сохранённые данные доступны.';
  if(code.includes('preview_access')||code.includes('disabled')||code.includes('model_not_approved'))return 'Окружение голосового прототипа ещё не включено для этого аккаунта. Обычный поиск доступен.';
  if(code.includes('capacity')||code.includes('too_large'))return 'Достигнут технический предел этого запроса. Данные не удалены; запись можно сохранить и отправить допустимую часть отдельно.';
  if(code.includes('revision_conflict'))return 'Этот поиск изменён в другом окне. Обновите страницу: подтверждённый ввод сохранён.';
  if(code.includes('quota'))return 'Сейчас нет доступной ёмкости модели. Ввод сохранён; микрофон и история остаются доступны.';
  return 'Обработка не подтверждена. Ввод сохранён; воспользуйтесь проверкой статуса. Обычный поиск остаётся доступным.';
};
export async function mountConversationalSearch(root:HTMLElement,presentation?:ReturnType<typeof mountComposerPresentation>):Promise<void>{
  if(root.dataset.assistantBound==='true')return;root.dataset.assistantBound='true';
  if(root.dataset.assistantCatalog==='true')return;
  const captureOnly=root.dataset.assistantCaptureOnly==='true';
  const search=root.closest<HTMLElement>('[data-authorized-search]');if(!search)return;
  const get=<T extends HTMLElement>(name:string)=>root.querySelector<T>(`[data-assistant-${name}]`)!;
  const record=get<HTMLButtonElement>('record'),stop=get<HTMLButtonElement>('stop'),submit=get<HTMLButtonElement>('submit');
  const text=get<HTMLTextAreaElement>('text'),processing=get('processing'),captureStatus=get('capture'),baseLabel=get('base');
  const answers=get('answers'),historyList=get('history-list'),recordings=get('recording-list'),resume=get<HTMLButtonElement>('resume');
  mountVoiceDiagnostics(root,get('composer'));
  const host=window as Host;let owner='';let authSeen=false;let authStatus='';let generation=0;let store:VoiceStore;
  let controllerReady=false;let controller:ConversationController|null=null;let capture:MicrophoneCapture|null=null;let recordingId:string|null=null;
  let pendingCommit=false;let receiptRetry=false;
  let recordingOwner='';let selectedBase:string|null=null;let mode:Mode='new_search';let viewed:string|null=null;let latest:string|null=null;
  let cursor:string|undefined;let state=initialState();let transition=Promise.resolve();let voicePipeline=Promise.resolve();let accepting=false;
  let measurement=new AssistantMeasurement();const rendered=new Map<string,{element:HTMLElement;result:any;count:number;ids:string[]}>();
  const objectUrls=new Set<string>();
  const pendingTurns=new Map<string,ConversationTurn>();
  const auth=getStaticSiteAuth({supabaseUrl:search.dataset.supabaseUrl||'',relayUrl:search.dataset.supabaseRelayUrl||'',publishableKey:search.dataset.supabaseKey||'',provider:search.dataset.yandexProvider});
  const api=new AssistantClient(auth,search.dataset.supabaseUrl||'',search.dataset.supabaseKey||'',()=>owner,()=>!captureOnly,root.dataset.assistantHost==='devcoveer');
  const showError=(error:unknown,searchId?:string)=>{const turn=searchId?pendingTurns.get(searchId):undefined;if(turn){settleConversationTurn(turn,'error',errorText(error));turn.body.append(button('Проверить обработку',()=>controller?.resume()));}processing.textContent=errorText(error);if(!capture||!['requesting','recording'].includes(capture.status))presentation?.message(processing.textContent);resume.hidden=!state.draft;};
  const showComposer=()=>presentation?presentation.open():get('composer').scrollIntoView({block:'start'});
  const stopForOverlay=async()=>{if(capture&&['requesting','recording','stopping'].includes(capture.status))await capture.stop('interrupted');};
  root.dataset.assistantStartup='opening_storage';delete root.dataset.assistantStartupError;
  const openingCopy='Открываю локальные записи… Микрофон пока выключен.';
  record.dataset.recordLabel ||= record.textContent || 'Начать запись';
  record.textContent=record.dataset.recordLabel;record.onclick=null;record.disabled=true;
  get('auth').textContent=openingCopy;
  presentation?.setCapture('idle',openingCopy);
  presentation?.bind(()=>presentation.message(openingCopy),async()=>{});
  try{store=await VoiceStore.open();}catch(error){
    const code=error instanceof Error?error.message:'voice_storage_unavailable';
    root.dataset.assistantStartup='storage_error';
    root.dataset.assistantStartupError=['voice_storage_open_timeout','voice_storage_upgrade_blocked'].includes(code)?code:'voice_storage_unavailable';
    const copy=code==='voice_storage_upgrade_blocked'
      ?'Локальные записи открыты в другой вкладке. Закройте другую вкладку этого сайта и повторите подключение. Записи не удалены.'
      :code==='voice_storage_open_timeout'
        ?'Локальное хранилище не ответило. Нажмите микрофон, чтобы повторить подключение. Записи не удалены; обычный поиск доступен.'
        :'Не удалось открыть локальные записи. Нажмите микрофон, чтобы повторить подключение. Запись не начата; данные не удалены.';
    processing.textContent=copy;get('auth').textContent=copy;root.dataset.assistantBound='false';
    const retry=()=>{void mountConversationalSearch(root,presentation).catch(()=>presentation?.fail());};
    record.textContent='Повторить подключение';record.disabled=false;record.hidden=false;record.onclick=retry;
    presentation?.setCapture('error',copy);presentation?.bind(retry,async()=>{});
    presentation?.launcher.setAttribute('aria-label','Повторить подключение голосового поиска');
    return;
  }
  root.dataset.assistantStartup='checking_auth';processing.textContent='';
  get('login')?.addEventListener('click',()=>{
    get('auth').textContent='Открываю вход через Яндекс…';
    void auth.signIn().then(ok=>{if(!ok)get('auth').textContent='Не удалось открыть вход через Яндекс. Попробуйте ещё раз.';}).catch(()=>{get('auth').textContent='Не удалось открыть вход через Яндекс. Попробуйте ещё раз.';});
  });
  const notify=()=>window.dispatchEvent(new CustomEvent('kenigevents:search-context-changed',{detail:{viewedSectionId:viewed,viewedTitle:viewed?rendered.get(viewed)?.result.title||'':'',refinementBaseId:selectedBase,pendingDraftId:state.draft?.id||null,capture:capture?.status||'idle'}}));
  let scrollFrame=0;
  const updateViewed=()=>{
    scrollFrame=0;
    const edge=parseFloat(getComputedStyle(document.documentElement).getPropertyValue('--ke-assistant-locator-edge'))||110;
    let next:string|null=null;
    for(const section of answers.querySelectorAll<HTMLElement>('[data-assistant-section]')){
      const rect=section.getBoundingClientRect();
      if(rect.top<=edge+8&&rect.bottom>edge){next=section.dataset.assistantSection||null;break;}
    }
    if(next!==viewed){viewed=next;notify();}
  };
  const scheduleViewed=()=>{if(!scrollFrame)scrollFrame=requestAnimationFrame(updateViewed);};
  window.addEventListener('scroll',scheduleViewed,{passive:true});window.addEventListener('resize',scheduleViewed);
  window.addEventListener('kenigevents:search-locator-geometry',scheduleViewed);
  const visibility=new ResizeObserver(scheduleViewed);
  // Test/preview exposure uses real viewport visibility and a 1s dwell; it never
  // invents a product conversion. The production analytics sink stays separate.
  const dwell=new Map<Element,ReturnType<typeof setTimeout>>();
  const exposure=new IntersectionObserver(entries=>{
    for(const entry of entries){const prior=dwell.get(entry.target);if(prior)clearTimeout(prior);dwell.delete(entry.target);
      if(entry.intersectionRatio<0.5||document.hidden||(entry.target as HTMLElement).hidden)continue;
      dwell.set(entry.target,setTimeout(()=>{
        dwell.delete(entry.target);if(document.hidden||(entry.target as HTMLElement).hidden)return;
        const rect=entry.target.getBoundingClientRect();const top=document.elementFromPoint(rect.left+rect.width/2,Math.max(0,rect.top)+Math.min(rect.height/2,innerHeight/2));
        if(top&&entry.target.contains(top))measurement.expose((entry.target as HTMLElement).dataset.eventId||'');
      },1000));
    }
  },{threshold:[0,0.5,1]});
  const visibleIds=(id:string|null)=>id?Array.from(rendered.get(id)?.element.querySelectorAll<HTMLElement>('[data-event-card]')||[]).filter(card=>!card.hidden).map(card=>card.dataset.eventId||''):[];
  const chooseBase=async(result:any,nextMode:Mode)=>{
    if(state.draft){processing.textContent='Сначала завершите текущий запрос или нажмите «Новый поиск». Просмотр истории не меняет его базу.';return;}
    const section:Section={id:result.id,parentId:result.parentId||null,mode:result.mode||'new_search',title:result.title,question:result.question,answer:result.answer,
      eventIds:result.items.map((item:any)=>String(item.event_id??item.id)),catalogRevision:result.catalog_revision,intent:result.intent,through:0,epoch:state.epoch};
    transcriptRevision++;await controller?.rememberSection(section);selectedBase=result.id;mode=nextMode;baseLabel.textContent=`База: ${result.title}`;showComposer();text.focus();notify();
  };
  const syncOverlay=()=>{
    const hidden=new Set(host.KenigEventsSearchCardHost?.hiddenIds()||[]);
    root.querySelectorAll<HTMLElement>('[data-event-card]').forEach(card=>{
      // The active action retains the common undo plate. Other sections hide it.
      const pinned=card.dataset.assistantUndoPinned==='true';
      card.hidden=hidden.has(card.dataset.eventId||'')&&!pinned;
    });
  };
  function appendCards(entry:{element:HTMLElement;result:any;count:number;ids:string[]}){
    const grid=entry.element.querySelector<HTMLElement>('[data-assistant-cards]')!;
    const maker=host.KenigEventsCreateEventCard;const media=host.KenigEventsResolveMobileEventCardMedia;
    if(!maker||!media){processing.textContent='Ответ сохранён, но общий компонент карточек не загрузился. Обновите страницу.';return;}
    const items=entry.result.items.slice(entry.count,entry.count+12);
    for(const [index,candidate]of items.entries()){
      const rank=entry.count+index;const card=maker({candidate,rank,personal_score:candidate.semantic_score||candidate.base_similarity||0},'split-actions',media(candidate));
      if(!card)continue;card.dataset.assistantSectionId=entry.result.id;card.dataset.servedListId=entry.result.served_list_id||'';
      grid.append(card);exposure.observe(card);entry.ids.push(String(candidate.event_id??candidate.id));
    }
    entry.count+=items.length;
    const more=entry.element.querySelector<HTMLButtonElement>('[data-assistant-more]');if(more)more.hidden=entry.count>=entry.result.items.length;
    host.KenigEventsSearchCardHost?.register(grid,entry.result.items,{sectionId:entry.result.id,servedListId:entry.result.served_list_id,servedListHash:entry.result.served_list_hash,algorithmId:entry.result.algorithm_id});
    measurement.render(entry.result.id,entry.ids);syncOverlay();void host.KenigEventsSearchCardHost?.sync();
  }
  function renderAnswer(result:any,scroll=false){
    if(!Array.isArray(result.items)||typeof result.id!=='string')return;
    if(rendered.has(result.id)){if(scroll)rendered.get(result.id)?.element.scrollIntoView({block:'start'});return;}
    const turn=pendingTurns.get(result.id)||createConversationTurn(result.question||'');
    settleConversationTurn(turn,'ready');pendingTurns.delete(result.id);
    const section=turn.section,body=turn.body;section.id=`assistant-answer-${result.id}`;section.dataset.assistantSection=result.id;
    const heading=node('h2',result.title);heading.tabIndex=-1;body.append(heading);
    if(result.parentId)body.append(node('p',`Уточнение предыдущей подборки`));
    if(result.membership_complete!==true&&!result.clarification&&result.explanationKind==='none')body.append(node('p','Это ограниченное поисковое окно, не весь каталог. «Уточнить» работает с этой подборкой; для расширения выберите «Изменить условия и искать заново».'));
    body.append(node('p',result.answer||''));
    const grid=node('div');grid.className='cards-grid cards-grid--immersive';grid.dataset.assistantCards='';body.append(grid);
    const entry={element:section,result,count:0,ids:[] as string[]};rendered.set(result.id,entry);
    const more=button('Показать ещё',()=>appendCards(entry));more.dataset.assistantMore='';body.append(more);
    const controls=node('div');controls.className='assistant__controls';
    controls.append(button(result.clarification?'Ответить на уточнение':'Уточнить эту подборку',()=>chooseBase(result,result.clarification?'expand_selection':'refine_selection')),
      button('Изменить условия и искать заново',()=>chooseBase(result,'expand_selection')),button('Спросить о событии',()=>chooseBase(result,'explain_selection')));
    body.append(controls);if(!section.isConnected)answers.append(section);appendCards(entry);visibility.observe(section);scheduleViewed();
    latest=result.id;get('latest').hidden=false;get('history')?.removeAttribute('hidden');
    if(scroll)section.scrollIntoView({block:'start'});
  }
  let transcriptRevision=0;
  text.addEventListener('input',()=>{transcriptRevision++;});
  async function transcribeRecording(own:string,id:string,revision=transcriptRevision,autoSend=false,anchor=new Date().toISOString()){
    if(captureOnly||owner!==own)return;
    processing.textContent='Распознаю речь…';presentation?.message(processing.textContent);
    try{
      const result=await api.transcribe(own,id,await store.parts(own,id),root.dataset.assistantHost==='devcoveer'?await store.compressed(own,id):null);
      if(result.state!=='completed')throw new Error(result.error||`voice_${result.state}`);
      await store.setTranscript(own,id,result.result.text);
      if(owner!==own)return;
      if(autoSend&&result.result.text?.trim()){
        // Show the actual ASR text + anticipated answer BEFORE durable intake/network.
        await sendQuestion(result.result.text,anchor);await loadRecordings();return;
      }
      if(revision===transcriptRevision){text.value=text.value?`${text.value}\n${result.result.text}`:result.result.text;transcriptRevision++;}
      else {processing.textContent='Текст уже изменён. Поздняя расшифровка доступна в «Аудио и восстановление» и не заменяет ваш запрос.';await loadRecordings();return;}
      get('composer').hidden=false;
      processing.textContent=result.result.uncertain?.length?'Есть неразборчивые места — поправьте запрос.':'Запрос готов. Можно исправить текст и найти события.';
      presentation?.message(processing.textContent);await loadRecordings();
    }catch(error){if(owner!==own)return;showError(error);get<HTMLDetailsElement>('recordings')?.setAttribute('open','');}
  }
  async function loadRecordings(){
    const own=owner;if(!own)return;
    const rows=await store.page<Recording>('recordings',own);if(owner!==own)return;
    recordings.replaceChildren();
    const recovery=get('recordings');if(recovery)recovery.hidden=rows.length===0;
    for(const row of rows){
      const panel=node('div');const label=node('p',`${new Date(row.createdAt).toLocaleString('ru-RU')} · ${row.state==='saved'?'сохранена':row.state==='partial'?'сохранена частично':'незавершённая запись'} · ${Math.round(row.bytes/1024)} КБ`);panel.append(label);
      const transcribe=button(captureOnly?'Распознавание в этом превью отключено':'Распознать / проверить ответ',async()=>{
        if(captureOnly)return;
        if(row.state!=='saved'){processing.textContent='Запись прервана; сохранённая часть доступна для прослушивания.';return;}
        await transcribeRecording(own,row.id);
      });
      transcribe.disabled=captureOnly;panel.append(transcribe);
      if(row.transcript!==undefined)panel.append(button('Вставить распознанный текст',()=>{if(owner===own){transcriptRevision++;text.value=text.value?`${text.value}\n${row.transcript}`:row.transcript||'';text.focus();}}));
      panel.append(button('Прослушать сохранённое аудио',async()=>{
        try{const parts=await store.parts(own,row.id);if(owner!==own)return;
          const {assemble,sha256}=await import('../../../../supabase/functions/event-search/assistant-media.ts');
          const prepared=await Promise.all(parts.map(async part=>({...part,digest:await sha256(part.bytes)})));
          const bytes=await assemble(prepared,{frames:parts.reduce((sum,part)=>sum+part.frameCount,0),sampleRate:parts[0]?.sampleRate,partCount:parts.length});
          const url=URL.createObjectURL(new Blob([bytes as BlobPart],{type:'audio/wav'}));objectUrls.add(url);
          const audio=node('audio');audio.controls=true;audio.src=url;panel.append(audio);
        }catch(error){showError(error);}
      }));
      recordings.append(panel);
    }
  }
  record.addEventListener('click',()=>{
    if(!owner&&authStatus==='checking'){presentation?.message(get('auth').textContent||'Проверяю сохранённый вход…');return;}
    if(!owner){showComposer();get('auth').textContent='Войдите, чтобы начать голосовой поиск. Микрофон не включится автоматически после входа.';get<HTMLButtonElement>('login')?.focus();return;}
    if(pendingCommit){presentation?.message('Сохраняю запись. Дождитесь завершения.');return;}
    if(receiptRetry){showComposer();presentation?.message('Не удалось завершить сохранение. Повторите локальное сохранение, не закрывая вкладку.');return;}
    if(!store){processing.textContent='Локальное хранилище недоступно. Запись не начата.';return;}
    if(capture?.unsavedParts().length){showComposer();processing.textContent='Не всё аудио сохранено. Сначала сохраните оставшуюся запись.';presentation?.message(processing.textContent);return;}
    if(capture&&['requesting','recording','stopping'].includes(capture.status)){processing.textContent='Запись уже начата. Нажмите «Остановить запись».';return;}
    const revision=++transcriptRevision;
    const own=owner,id=crypto.randomUUID(),anchor=new Date().toISOString();recordingId=id;recordingOwner=own;
    const created=store.create(own,id); // AudioContext starts in this user gesture.
    let finished=false;
    const finalize=async(receipt:any)=>{
      if(finished)return;finished=true;pendingCommit=true;
      try{
        await created;await store.finish(own,id,receipt);
        if(owner===own){
          receiptRetry=false;await loadRecordings();pendingCommit=false;
          if(receipt.frames===0&&receipt.reason==='capture_failed'){presentation?.setCapture('error',captureStatus.textContent||'Не удалось включить микрофон.');return;}
          presentation?.setCapture(receipt.complete?'saved':receipt.reason==='cancelled'?'idle':'partial',receipt.complete?(captureOnly?'Запись сохранена на устройстве. Распознавание в этом превью пока отключено.':'Запись сохранена. Распознаю речь…'):receipt.reason==='cancelled'?'Микрофон выключен.':'Запись прервана. Сохранённая часть доступна в записях.');
          if(receipt.complete&&['user','silence'].includes(receipt.reason)&&!captureOnly){voicePipeline=voicePipeline.then(()=>transcribeRecording(own,id,revision,true,anchor)).catch(showError);}
        }
      }catch(error){if(owner===own){receiptRetry=true;get('save-retry').hidden=false;presentation?.setCapture('error','Не удалось завершить сохранение. Аудио не удалено; повторите сохранение.');processing.textContent='Не удалось завершить сохранение. Повторите локальное сохранение, не закрывая вкладку.';showComposer();}}
      finally{pendingCommit=false;}
    };
    capture=new MicrophoneCapture({workletUrl:root.dataset.assistantWorklet||'',budget:AUDIO_BUDGET,onPart:async part=>{await created;await store.putPart(own,id,part);},onCompressedPart:async part=>{await created;await store.putCompressedPart(own,id,part);},
      onStatus:(status:CaptureStatus,reason)=>{
        if(owner!==own)return;
        stop.hidden=!['requesting','recording','stopping'].includes(status);stop.disabled=status==='stopping';record.disabled=!stop.hidden;record.hidden=!stop.hidden;
        const labels:Record<string,string>={requesting:'Ожидаю разрешение микрофона. Можно отменить.',recording:'Слушаю. После паузы отправлю запрос. Можно остановить кнопкой или Escape.',stopping:'Сохраняю конец записи.',saved:captureOnly?'Запись сохранена в этом браузере. Её можно прослушать ниже.':'Запись завершена.',partial:'Запись прервана; сохранённая часть доступна ниже.',error:reason==='microphone_denied'?'Доступ к микрофону запрещён. Разрешите его в настройках сайта.':reason==='microphone_not_found'?'Микрофон не найден. Подключите его и повторите.':reason==='microphone_unavailable'?'В этом браузере запись недоступна. Нужны HTTPS и поддержка микрофона.':'Не удалось включить микрофон. Проверьте устройство и разрешения браузера.',idle:'Микрофон выключен.'};
        captureStatus.textContent=labels[status]||status;get('save-retry').hidden=reason!=='storage_failed';
        const visual=status==='saved'||status==='partial'?'stopping':status;
        const brief=status==='recording'?'Слушаю · после паузы отправлю':status==='requesting'?'Разрешите микрофон · круг отменит запрос':visual==='stopping'?'Сохраняю запись на устройстве…':captureStatus.textContent;
        presentation?.setCapture(visual,brief);notify();
      },onStopped:receipt=>{void finalize(receipt);}});
    void capture.start().catch(()=>{void finalize({complete:false,reason:'capture_failed',frames:0,savedFrames:0,sampleRate:0,partCount:0});});void created.catch(showError);
  });
  stop.addEventListener('click',()=>{void capture?.stop().catch(showError);});
  get('save-retry').addEventListener('click',()=>{
    const active=capture,id=recordingId,own=recordingOwner;if(!active||!id)return;
    const retry=get<HTMLButtonElement>('save-retry');retry.disabled=true;
    void active.retryUnsaved().then(async left=>{
      const receipt=active.receipt();if(receipt)await store.finish(own,id,receipt);
      if(owner!==own||capture!==active)return;
      receiptRetry=false;retry.hidden=left===0;record.disabled=false;
      presentation?.setCapture(left?'error':receipt?.complete?'saved':'partial',left?'Часть аудио ещё не сохранена.':'Локальное сохранение завершено.');
      captureStatus.textContent=left?'Часть аудио пока остаётся только в памяти.':receipt?.complete?(captureOnly?'Запись полностью сохранена в этом браузере. Её можно прослушать ниже.':'Запись полностью сохранена. Её можно распознать ниже.'):'Сохранённая часть записи доступна ниже. Прерывание записи не отменено.';
      await loadRecordings();
    }).catch(showError).finally(()=>{retry.disabled=false;});
  });
  document.addEventListener('keydown',event=>{if(event.key==='Escape'&&capture&&['requesting','recording'].includes(capture.status)){event.preventDefault();void capture.stop().catch(showError);}});
  window.addEventListener('beforeunload',event=>{if(pendingCommit||receiptRetry||capture?.unsavedParts().length||capture&&['requesting','recording','stopping'].includes(capture.status)){event.preventDefault();event.returnValue='';}});
  async function sendQuestion(raw:string,anchor=new Date().toISOString()){
    if(captureOnly||!raw.trim())return;
    // No mandatory confirmation surface: the question and skeleton are visible
    // immediately, before waiting for the controller's durable checkpoint.
    void presentation?.close();
    const turn=createConversationTurn(raw);answers.append(turn.section);
    turn.section.scrollIntoView({block:'start',behavior:matchMedia('(prefers-reduced-motion: reduce)').matches?'instant':'smooth'});
    presentation?.message('Ищу события · можно дополнить голосом');
    const active=controller,own=owner,currentMode=mode,parent=selectedBase,ids=visibleIds(parent);
    if(!controllerReady||!active){settleConversationTurn(turn,'error','Поиск ещё подключается. Запрос сохранён в записи; повторите после подключения.');return;}
    try{
      const command=await active.submit(raw,currentMode,parent,ids,anchor);
      if(owner!==own||controller!==active)return;
      pendingTurns.set(command.searchId,turn);
      if(text.value===raw){text.value='';transcriptRevision++;}
    }catch(error){if(owner===own){settleConversationTurn(turn,'error',errorText(error));showError(error);}}
  }
  get<HTMLFormElement>('form').addEventListener('submit',event=>{
    event.preventDefault();if(captureOnly||!controllerReady||!controller||!text.value.trim()||accepting)return;
    accepting=true;transcriptRevision++;const raw=text.value;
    transition=transition.then(()=>sendQuestion(raw)).finally(()=>{accepting=false;});
  });
  get('new').addEventListener('click',()=>{if(!controllerReady)return;transcriptRevision++;void controller?.newTask().then(()=>{selectedBase=null;mode='new_search';baseLabel.textContent='Новый поиск';notify();}).catch(showError);});
  resume.addEventListener('click',()=>{if(controllerReady)void controller?.resume();});
  get('latest').addEventListener('click',()=>{if(latest)rendered.get(latest)?.element.scrollIntoView({block:'start'});});
  get('history-load').addEventListener('click',async()=>{
    const own=owner;if(captureOnly||!own)return;
    try{const result=await api.history(own,cursor);if(owner!==own)return;
      for(const row of result.items){historyList.append(button(row.title,async()=>{
        try{const receipt=await api.status(own,row.id);if(owner!==own)return;if(receipt.state==='completed'){await store.saveAnswer(own,row.id,receipt.result);renderAnswer(receipt.result,true);}}
        catch(error){showError(error);}
      }));}
      cursor=result.next||undefined;get('history-load').textContent=cursor?'Загрузить предыдущие':'История загружена';get<HTMLButtonElement>('history-load').disabled=!cursor;
    }catch(error){showError(error);}
  });
  root.addEventListener('click',event=>{
    if(!(event.target instanceof Element))return;const card=event.target.closest<HTMLElement>('[data-event-card]');if(!card)return;
    // Attribution is captured on intent, but commitment arrives from the owner.
    if(event.target.closest('[data-feedback-action="like"],[data-calendar-action],[data-saved-event-action]'))lastAction.set(card.dataset.eventId||'',Date.now());
  });
  const lastAction=new Map<string,number>();
  window.addEventListener('kenigevents:saved-event-state',event=>{
    if(!(event instanceof CustomEvent))return;const id=String(event.detail?.eventId);const at=lastAction.get(id);
    if(at&&Date.now()-at<60000&&['favorite','calendar'].includes(event.detail?.source))measurement.committed(id,event.detail.source,event.detail.saved===true);
  });
  window.addEventListener('kenigevents:profile-changed',event=>{
    if(event instanceof CustomEvent){const source=event.detail?.sourceCard;
      if(source instanceof HTMLElement&&root.contains(source))source.dataset.assistantUndoPinned=event.detail.action==='not_interested'?'true':'false';
    }syncOverlay();
  });
  window.addEventListener('storage',event=>{if(event.key==='ke_personalization_profile')syncOverlay();});
  document.addEventListener('visibilitychange',()=>{if(document.hidden){for(const timer of dwell.values())clearTimeout(timer);dwell.clear();}});
  announceAssistantSurface({version:'1.0.0',element:get('composer'),getState:()=>({viewedSectionId:viewed,viewedTitle:viewed?rendered.get(viewed)?.result.title||'':'',refinementBaseId:selectedBase,pendingDraftId:state.draft?.id||null,capture:capture?.status||'idle'}),
    showComposer,showSection:id=>rendered.get(id)?.element.scrollIntoView({block:'start'}),
    beforeOverlayOpen:stopForOverlay,
    diagnostic:()=>({contract:'kenigevents.voice-diagnostic.v1',startup:root.dataset.assistantStartup,controllerReady,capture:capture?.status||'idle',pending:!!state.draft,acceptedThrough:state.acceptedThrough,processedThrough:state.processedThrough,readout:measurement.readout()})});
  auth.subscribe(snapshot=>{
    if(snapshot.status==='checking'||snapshot.status==='error')get('auth').textContent=snapshot.message;
    const next=snapshot.status==='signed_in'&&snapshot.user&&!snapshot.user.is_anonymous?snapshot.user.id:'';
    if(authSeen&&next===owner&&authStatus===snapshot.status)return;authSeen=true;authStatus=snapshot.status;owner=next;transcriptRevision++;const epoch=++generation;
    if(capture&&['requesting','recording'].includes(capture.status))void capture.stop('interrupted');
    selectedBase=null;mode='new_search';viewed=null;latest=null;cursor=undefined;state=initialState();controllerReady=false;controller=null;text.value='';
    for(const url of objectUrls)URL.revokeObjectURL(url);objectUrls.clear();for(const timer of dwell.values())clearTimeout(timer);dwell.clear();exposure.disconnect();visibility.disconnect();rendered.clear();pendingTurns.clear();answers.replaceChildren();historyList.replaceChildren();recordings.replaceChildren();measurement=new AssistantMeasurement();lastAction.clear();
    for(const name of ['submit','new','text','history-load'])(get(name) as HTMLButtonElement).disabled=true;
    record.disabled=false;record.hidden=!owner;captureStatus.hidden=!owner;
    const login=get('login');if(login)login.hidden=Boolean(owner);
    const history=get('history');if(history)history.hidden=captureOnly||!owner;
    const recovery=get('recordings');if(recovery)recovery.hidden=true;
    get('auth').textContent=(snapshot.status==='checking'||snapshot.status==='error')?snapshot.message:owner?'':'Войдите, чтобы начать голосовой поиск. Микрофон не включится автоматически после входа.';resume.hidden=true;
    if(!owner){root.dataset.assistantStartup=snapshot.status==='checking'?'checking_auth':snapshot.status==='error'?'auth_error':'signed_out';presentation?.setCapture('idle','Голосовой поиск · нужен вход');return;}
    presentation?.setCapture('idle',captureOnly?'Микрофон · пока без распознавания':'Голосовой поиск');
    if(captureOnly){root.dataset.assistantStartup='ready';processing.textContent='';void loadRecordings().catch(showError);return;}
    const active=new ConversationController(owner,store,api,{change:value=>{if(epoch!==generation)return;state=value;resume.hidden=!state.draft;notify();},superseded:searchId=>{if(epoch!==generation)return;const turn=pendingTurns.get(searchId);if(turn){settleConversationTurn(turn,'superseded','Дополнение принято. Общий ответ появится под следующим сообщением.');pendingTurns.delete(searchId);}},answer:(result,visible)=>{if(epoch!==generation)return;if(visible){renderAnswer(result);selectedBase=result.id;mode=result.clarification?'expand_selection':'refine_selection';baseLabel.textContent=`База: ${result.title}`;presentation?.message('Ответ готов · можно дополнить голосом');notify();}else{const turn=pendingTurns.get(result.id);if(turn){settleConversationTurn(turn,'superseded','Учтено в следующем запросе. Предыдущий ответ сохранён в истории.');pendingTurns.delete(result.id);}}},status:(message,searchId)=>{if(epoch===generation){processing.textContent=message;const turn=searchId?pendingTurns.get(searchId):undefined;if(turn){turn.status.textContent=message;}}},error:(error,searchId)=>{if(epoch===generation)showError(error,searchId);}});
    controller=active;root.dataset.assistantStartup='restoring_conversation';
    void active.initialize().then(async()=>{
      if(epoch!==generation)return;controllerReady=true;root.dataset.assistantStartup='ready';
      for(const name of ['submit','new','text','history-load'])(get(name) as HTMLButtonElement).disabled=false;
      const saved=await store.page<StoredAnswer>('answers',owner,undefined,10);
      if(epoch!==generation)return;
      for(const row of saved.reverse()){
        try{const fresh=await api.status(owner,row.id);if(epoch!==generation)return;if(fresh.state==='completed')renderAnswer(fresh.result);}
        catch{if(epoch!==generation)return;processing.textContent='История сохранена на устройстве, но актуальность событий пока не проверена. Карточки не показаны как текущие.';}
      }
      await loadRecordings();if(state.draft)processing.textContent='Найден сохранённый незавершённый запрос. Обработка возобновится только по кнопке.';
    }).catch(error=>{if(epoch===generation){if(!controllerReady)root.dataset.assistantStartup='restore_error';showError(error);}});
  });
  presentation?.bind(()=>{
    if(capture&&['requesting','recording'].includes(capture.status)){void capture.stop().catch(showError);return;}
    if(capture?.status==='stopping'||pendingCommit){presentation.message('Сохраняю запись…');return;}
    if(receiptRetry||capture?.unsavedParts().length){showComposer();presentation.message('Нужно завершить локальное сохранение записи.');return;}
    record.click();
  },stopForOverlay);
  await auth.initialize();
}
