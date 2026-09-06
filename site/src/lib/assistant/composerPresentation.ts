/** WL-style single mic toggle: no modal or focus theft during capture.
 * The existing composer is an explicit, in-flow request/recovery surface. */
export function mountComposerPresentation(root: HTMLElement) {
  const panel = root.querySelector<HTMLElement>('[data-assistant-composer]')!;
  const launcher = root.querySelector<HTMLButtonElement>('[data-assistant-launcher]')!;
  const details = root.querySelector<HTMLButtonElement>('[data-assistant-details]')!;
  const dock = root.querySelector<HTMLElement>('[data-assistant-dock]')!;
  const live = root.querySelector<HTMLElement>('[data-assistant-live]')!;
  const timer = root.querySelector<HTMLElement>('[data-assistant-timer]')!;
  const status = root.querySelector<HTMLElement>('[data-assistant-processing]')!;
  let activate = () => { live.textContent = 'Поиск подключается. Попробуйте через несколько секунд.'; };
  let captureState = 'idle', started = 0;
  let clock: ReturnType<typeof setInterval> | undefined;
  const tick = () => { const seconds = Math.floor((performance.now() - started) / 1000); timer.textContent = `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, '0')}`; };
  const open = () => { panel.hidden = false; details.setAttribute('aria-expanded','true'); panel.scrollIntoView({block:'nearest',behavior:'instant'}); };
  const close = async () => { panel.hidden = true; details.setAttribute('aria-expanded','false'); };
  const message = (value: string) => { live.textContent = value; };
  launcher.addEventListener('click', () => activate());
  details.addEventListener('click', () => { if(panel.hidden)open();else void close(); });
  root.querySelector('[data-assistant-close]')!.addEventListener('click', () => { void close(); (root.dataset.assistantCleanUi==='true'?root.querySelector<HTMLElement>('[data-assistant-recovery-open]')||launcher:details).focus({preventScroll:true}); });
  const enterConversation=()=>{
    if(root.dataset.assistantPhase==='conversation')return;
    const before=launcher.getBoundingClientRect();root.dataset.assistantPhase='conversation';root.dataset.dsState='conversation';
    const after=launcher.getBoundingClientRect();
    if(root.dataset.assistantCleanUi==='true'&&!matchMedia('(prefers-reduced-motion: reduce)').matches&&before.width&&after.width){
      launcher.animate([{transform:`translate(${before.left+before.width/2-after.left-after.width/2}px,${before.top+before.height/2-after.top-after.height/2}px) scale(${before.width/after.width})`},{transform:'translate(0,0) scale(1)'}],{duration:650,easing:'cubic-bezier(.22,.72,.18,1)'});
    }
  };
  return { open, close, launcher, message, enterConversation,
    bind: (action: () => void, _stopForOverlay: () => Promise<void>) => { activate = action; },
    setCapture: (state: string, copy?: string) => {
      if(state === 'recording' && captureState !== state){started=performance.now();tick();clock=setInterval(tick,250);}
      if(state !== 'recording' && clock){clearInterval(clock);clock=undefined;}
      captureState=state;dock.dataset.captureState=state;
      const active=state==='requesting'||state==='recording';
      launcher.setAttribute('aria-pressed',String(active));
      launcher.setAttribute('aria-label',state==='requesting'?'Отменить запрос микрофона':active?'Остановить запись':state==='stopping'?'Сохраняю запись':'Начать запись голосом');
      launcher.title=launcher.getAttribute('aria-label')!;
      launcher.setAttribute('aria-disabled',String(state==='stopping'));
      timer.hidden=state!=='recording';
      if(copy)message(copy);
      if(active){panel.hidden=true;details.setAttribute('aria-expanded','false');}
      details.hidden=active||state==='stopping';
    },
    fail: () => { const copy='Голосовой поиск не подключился. Обновите страницу; записи не удалены.';status.textContent=copy;message(copy);activate=()=>message(copy);root.querySelector<HTMLButtonElement>('[data-assistant-record]')!.disabled=true; }
  };
}
