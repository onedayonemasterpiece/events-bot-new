import type { getStaticSiteAuth } from './staticSiteAuth';
import { EMAIL_OTP_LENGTH, canSubmitEmailOtp, normalizeEmailOtp } from './emailOtp';

/** UI only: the host supplies its existing origin-scoped Auth controller. */
export type StaticSiteSignInAuth = Pick<ReturnType<typeof getStaticSiteAuth>,
  'subscribe' | 'signIn' | 'signInWithEmailOtp' | 'verifyEmailOtp'>;
export type StaticSiteSignIn = { open:(trigger?:HTMLElement)=>void; close:()=>void; destroy:()=>void };
const mounted = new WeakMap<HTMLElement,StaticSiteSignIn>();
let nextId = 0;
export function mountStaticSiteSignIn(container:HTMLElement, auth:StaticSiteSignInAuth, options:{variant?:'compact'|'card'}={}):StaticSiteSignIn {
  const existing=mounted.get(container);if(existing)return existing;
  const id=`static-sign-in-${++nextId}`;
  const make=<K extends keyof HTMLElementTagNameMap>(tag:K,copy?:string)=>{const el=document.createElement(tag);if(copy)el.textContent=copy;return el;};
  const panel=make('section');panel.className=`static-sign-in static-sign-in--${options.variant==='compact'?'compact':'card'}`;panel.setAttribute('aria-label','Вход');
  const head=make('div');head.className='static-sign-in__head';head.append(make('h2','Войти, чтобы продолжить'));
  const closeButton=make('button','×');closeButton.type='button';closeButton.className='static-sign-in__close';closeButton.setAttribute('aria-label','Закрыть вход');head.append(closeButton);panel.append(head);
  const yandex=make('button','Войти через Яндекс');yandex.type='button';yandex.className='static-sign-in__yandex';panel.append(yandex);
  const divider=make('p','или');divider.className='static-sign-in__divider';panel.append(divider);
  const emailForm=make('form');const emailLabel=make('label','Email');emailLabel.htmlFor=`${id}-email`;
  const email=make('input');email.id=emailLabel.htmlFor;email.type='email';email.autocomplete='email';email.inputMode='email';email.required=true;email.maxLength=254;email.placeholder='you@example.com';
  const send=make('button','Получить код');send.type='submit';emailForm.append(emailLabel,email,send);panel.append(emailForm);
  const codeForm=make('form');codeForm.hidden=true;const destination=make('p');destination.className='static-sign-in__destination';
  const codeLabel=make('label','Код из письма');codeLabel.htmlFor=`${id}-code`;
  const code=make('input');code.id=codeLabel.htmlFor;code.type='text';code.inputMode='numeric';code.autocomplete='one-time-code';code.maxLength=EMAIL_OTP_LENGTH;code.pattern='[0-9]{6}';code.required=true;
  const verify=make('button','Войти');verify.type='submit';
  const resend=make('button','Отправить код ещё раз');resend.type='button';resend.className='static-sign-in__secondary';
  const change=make('button','Другой email');change.type='button';change.className='static-sign-in__secondary';
  codeForm.append(destination,codeLabel,code,verify,resend,change);panel.append(codeForm);
  const status=make('p');status.className='static-sign-in__status';status.setAttribute('role','status');status.setAttribute('aria-live','polite');status.setAttribute('aria-atomic','true');panel.append(status);
  container.append(panel);container.hidden=true;
  let busy:''|'oauth'|'send'|'verify'='',pendingEmail='',lastSubmitted='',signedIn=false,disposed=false,epoch=0,cooldownUntil=0;
  let trigger:HTMLElement|undefined;let timer:ReturnType<typeof setInterval>|undefined;
  const abort=new AbortController();
  const render=()=>{
    const seconds=Math.max(0,Math.ceil((cooldownUntil-Date.now())/1000));
    yandex.disabled=Boolean(busy);email.disabled=Boolean(busy);send.disabled=Boolean(busy)||seconds>0;
    send.textContent=busy==='send'?'Отправляю…':seconds>0?`Повторить через ${seconds} с`:'Получить код';
    code.disabled=Boolean(busy);verify.disabled=Boolean(busy)||!canSubmitEmailOtp(code.value,{lastSubmitted});verify.textContent=busy==='verify'?'Проверяю…':'Войти';
    resend.disabled=Boolean(busy)||seconds>0;resend.textContent=seconds>0?`Отправить ещё раз через ${seconds} с`:'Отправить код ещё раз';change.disabled=Boolean(busy);
    panel.setAttribute('aria-busy',String(Boolean(busy)));
    if(!seconds&&timer){clearInterval(timer);timer=undefined;}
  };
  const cooldown=()=>{cooldownUntil=Date.now()+60000;if(!timer)timer=setInterval(render,1000);};
  const focus=()=>{if(!container.hidden)(codeForm.hidden?email:code).focus();};
  const close=()=>{container.hidden=true;if(trigger?.isConnected)trigger.focus({preventScroll:true});};
  const reset=()=>{++epoch;busy='';pendingEmail='';lastSubmitted='';email.value='';code.value='';destination.textContent='';status.textContent='';codeForm.hidden=true;emailForm.hidden=false;render();};
  const requestCode=async(address:string)=>{
    if(disposed||signedIn||busy||Date.now()<cooldownUntil)return;
    const operation=epoch;pendingEmail=address;busy='send';status.textContent='Отправляю код…';render();
    try{
      const result=await auth.signInWithEmailOtp(address);
      if(disposed||operation!==epoch)return;
      if(result.accepted||result.status==='ambiguous'){
        lastSubmitted='';code.value='';emailForm.hidden=true;codeForm.hidden=false;destination.textContent=`Код для ${pendingEmail}`;
        status.textContent=result.accepted?'Введите шесть цифр из письма.':result.message;cooldown();
      }else{status.textContent=result.message;if(result.status==='rate_limited')cooldown();}
    }catch{if(!disposed&&operation===epoch){status.textContent='Не получили подтверждение отправки. Проверьте почту, прежде чем повторять.';emailForm.hidden=true;codeForm.hidden=false;destination.textContent=`Код для ${pendingEmail}`;cooldown();}}
    finally{if(!disposed&&operation===epoch){busy='';render();focus();}}
  };
  const verifyCode=async()=>{
    const token=normalizeEmailOtp(code.value);code.value=token;
    if(disposed||signedIn||busy||!pendingEmail||!canSubmitEmailOtp(token,{lastSubmitted}))return;
    const operation=epoch;busy='verify';lastSubmitted=token;status.textContent='Проверяю код…';render();
    try{const result=await auth.verifyEmailOtp(pendingEmail,token);if(disposed||operation!==epoch)return;status.textContent=result.message;}
    catch{if(!disposed&&operation===epoch)status.textContent='Подтверждение не получено. Проверьте состояние входа; повторной отправки кода не было.';}
    finally{if(!disposed&&operation===epoch){busy='';render();}}
  };
  closeButton.addEventListener('click',close,{signal:abort.signal});
  yandex.addEventListener('click',()=>{
    if(disposed||signedIn||busy)return;const operation=epoch;busy='oauth';status.textContent='Открываю вход через Яндекс…';render();
    void auth.signIn().then(ok=>{if(!disposed&&operation===epoch&&!ok)status.textContent='Не удалось открыть вход через Яндекс. Попробуйте ещё раз.';}).catch(()=>{if(!disposed&&operation===epoch)status.textContent='Не удалось открыть вход через Яндекс. Проверьте соединение.';}).finally(()=>{if(!disposed&&operation===epoch){busy='';render();}});
  },{signal:abort.signal});
  emailForm.addEventListener('submit',event=>{event.preventDefault();if(emailForm.reportValidity())void requestCode(email.value.trim());},{signal:abort.signal});
  resend.addEventListener('click',()=>{if(pendingEmail)void requestCode(pendingEmail);},{signal:abort.signal});
  change.addEventListener('click',()=>{if(busy)return;codeForm.hidden=true;emailForm.hidden=false;code.value='';lastSubmitted='';status.textContent='';render();focus();},{signal:abort.signal});
  codeForm.addEventListener('submit',event=>{event.preventDefault();void verifyCode();},{signal:abort.signal});
  code.addEventListener('input',()=>{code.value=normalizeEmailOtp(code.value);render();if(canSubmitEmailOtp(code.value,{inFlight:Boolean(busy),lastSubmitted}))void verifyCode();},{signal:abort.signal});
  const unsubscribe=auth.subscribe(snapshot=>{
    const next=snapshot.status==='signed_in'&&Boolean(snapshot.user)&&!snapshot.user?.is_anonymous;
    if(next){signedIn=true;reset();close();}
    else if(signedIn){signedIn=false;reset();}
  });
  const api:StaticSiteSignIn={
    open:(origin)=>{if(disposed||signedIn)return;trigger=origin||(document.activeElement instanceof HTMLElement?document.activeElement:undefined);container.hidden=false;render();focus();},
    close,
    destroy:()=>{if(disposed)return;disposed=true;++epoch;abort.abort();unsubscribe();if(timer)clearInterval(timer);email.value='';code.value='';pendingEmail='';lastSubmitted='';panel.remove();container.hidden=true;mounted.delete(container);},
  };
  mounted.set(container,api);render();return api;
}
