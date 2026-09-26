/** One event lower surface: move the existing CTA and nav, never clone actions. */
export function initMobileEventDock(doc=document,win=window){
 const cta=doc.querySelector('[data-sticky-cta]'),nav=doc.querySelector('[data-mobile-bottom-nav]');
 if(!cta||!nav||!doc.querySelector('[data-mobile-event-production]')||cta.__eventDock)return;
 const media=win.matchMedia('(max-width:720px)'),reduced=()=>win.matchMedia('(prefers-reduced-motion: reduce)').matches;
 const ctaOrigin=doc.createComment('event-dock-cta-origin'),navOrigin=doc.createComment('event-dock-nav-origin');cta.before(ctaOrigin);nav.before(navOrigin);
 const dock=doc.createElement('div');dock.dataset.eventDockSurface='';dock.setAttribute('aria-label','Действие события и навигация');
 let active=false,shown=null,motion=null;
 const emit=()=>win.dispatchEvent(new CustomEvent('kenigevents:event-dock-change'));
 function sync(animate=true){
  if(!active)return;const next=!cta.classList.contains('is-hidden');if(next===shown)return;
  const before=dock.getBoundingClientRect().height;motion?.cancel();motion=null;shown=next;dock.dataset.activeCta=String(next);cta.inert=!next;
  const height=next?110:82;dock.style.height=`${height}px`;
  if(animate&&!reduced()&&before){cta.inert=true;motion=dock.animate([{height:`${before}px`},{height:`${height}px`}],{duration:340,easing:'cubic-bezier(.25,.1,.25,1)'});motion.onfinish=()=>{motion=null;cta.inert=!shown;emit();};}
  emit();
 }
 function mount(){
  if(media.matches===active)return;
  if(media.matches){active=true;doc.body.append(dock);dock.append(cta,nav);doc.body.dataset.eventDock='';shown=null;sync(false);}
  else{active=false;motion?.cancel();motion=null;cta.inert=false;ctaOrigin.after(cta);navOrigin.after(nav);dock.remove();delete doc.body.dataset.eventDock;emit();}
 }
 // The existing hero/feed IntersectionObservers remain the ONLY visibility
 // owner. This observer merely composes their already-decided state.
 const observer=new MutationObserver(()=>sync());observer.observe(cta,{attributes:true,attributeFilter:['class']});
 const sizeObserver=new ResizeObserver(emit);sizeObserver.observe(dock);
 const onPageShow=()=>sync(false);media.addEventListener('change',mount);win.addEventListener('pageshow',onPageShow);mount();
 cta.__eventDock={destroy(){if(active){active=false;motion?.cancel();cta.inert=false;ctaOrigin.after(cta);navOrigin.after(nav);dock.remove();delete doc.body.dataset.eventDock;}observer.disconnect();sizeObserver.disconnect();media.removeEventListener('change',mount);win.removeEventListener('pageshow',onPageShow);ctaOrigin.remove();navOrigin.remove();delete cta.__eventDock;}};
 return cta.__eventDock;
}
