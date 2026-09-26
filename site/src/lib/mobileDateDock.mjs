/** One lower surface, reusing the actual date rail, calendar and global nav. */
export function initMobileDateDock(accessory,doc=document,win=window){
 if(accessory.dataset.calendarReady==='true')return;
 accessory.dataset.calendarReady='true';
 const sheet=accessory.nextElementSibling,trigger=accessory.querySelector('[data-calendar-open]'),close=sheet?.querySelector('[data-calendar-close]'),rail=accessory.querySelector('.date-rail');
 const root=sheet?.querySelector('[data-calendar-months]'),months=[...(root?.querySelectorAll('[data-calendar-month]')||[])],label=sheet?.querySelector('[data-calendar-month-label]'),previous=sheet?.querySelector('[data-calendar-month-previous]'),next=sheet?.querySelector('[data-calendar-month-next]'),nav=doc.querySelector('[data-mobile-bottom-nav]');
 if(!sheet||!nav||!months.length)return;
 const media=win.matchMedia('(max-width:720px)'),reduced=()=>win.matchMedia('(prefers-reduced-motion: reduce)').matches;
 const origin=doc.createComment('date-dock-origin');accessory.before(origin);
 const navOrigin=doc.createComment('date-dock-nav-origin');nav.before(navOrigin);
 const dock=doc.createElement('div');dock.className='date-dock';dock.dataset.dateDockSurface='';
 let active=false,opened=false,motion=null,index=Math.max(0,months.findIndex(m=>m.dataset.calendarMonth===root.dataset.selectedMonth)),pointer=null,pull=null,suppressClickUntil=0;
 const emit=()=>win.dispatchEvent(new CustomEvent('kenigevents:date-dock-change'));
 function center(){const selected=rail.querySelector('[aria-current="date"]');if(selected)rail.scrollTo({left:selected.offsetLeft-(rail.clientWidth-selected.clientWidth)/2,behavior:'instant'});}
 function resize(animate=false){if(!active)return;const before=dock.getBoundingClientRect().height;motion?.cancel();motion=null;
  const height=(opened?sheet:accessory).getBoundingClientRect().height+nav.getBoundingClientRect().height+2;
  dock.style.height=`${height}px`;
  if(animate&&!reduced()){motion=dock.animate([{height:`${before}px`},{height:`${height}px`}],{duration:340,easing:'cubic-bezier(.25,.1,.25,1)'});motion.onfinish=()=>{motion=null;emit()};}
  emit();
 }
 function showMonth(value,animate=false){const old=index;index=Math.max(0,Math.min(months.length-1,value));months.forEach((m,i)=>m.hidden=i!==index);label.textContent=months[index].dataset.calendarMonthLabel;previous.disabled=index===0;next.disabled=index===months.length-1;
  if(animate&&old!==index&&!reduced())months[index].animate([{transform:`translateX(${index>old?18:-18}px)`},{transform:'translateX(0)'}],{duration:180,easing:'ease-out'});
  resize(false);
 }
 function setOpen(value,focus=true){if(!active||opened===value)return;const before=dock.getBoundingClientRect().height;opened=value;motion?.cancel();motion=null;
  sheet.hidden=!opened;sheet.setAttribute('aria-hidden',String(!opened));sheet.classList.toggle('is-open',opened);accessory.hidden=opened;trigger.setAttribute('aria-expanded',String(opened));dock.dataset.expanded=String(opened);
  // Nav stays anchored to the bottom while the one shared skin changes height.
  dock.style.height=`${before}px`;resize(true);
  if(focus)(opened?close:trigger).focus({preventScroll:true});
 }
 function mount(){if(media.matches===active)return;
  if(media.matches){active=true;origin.parentNode.insertBefore(dock,origin.nextSibling);dock.append(accessory,sheet,nav);doc.body.dataset.dateDock='';sheet.querySelector('[role="dialog"]').setAttribute('aria-modal','false');showMonth(index);resize();center();}
  else{setOpen(false,false);active=false;motion?.cancel();motion=null;origin.after(accessory,sheet);navOrigin.after(nav);dock.remove();delete doc.body.dataset.dateDock;sheet.hidden=true;accessory.hidden=false;}
 }
 previous.addEventListener('click',()=>showMonth(index-1,true));next.addEventListener('click',()=>showMonth(index+1,true));trigger.addEventListener('click',()=>setOpen(true));close.addEventListener('click',()=>setOpen(false));
 doc.addEventListener('keydown',e=>{if(!active||!opened)return;if(e.key==='Escape'){e.preventDefault();setOpen(false);}if(root.contains(e.target)&&['ArrowLeft','ArrowRight'].includes(e.key)){e.preventDefault();showMonth(index+(e.key==='ArrowRight'?1:-1),true);}});
 doc.addEventListener('pointerdown',e=>{if(opened&&!dock.contains(e.target))setOpen(false,false);});
 root.addEventListener('pointerdown',e=>{if(e.pointerType==='mouse')return;pointer={x:e.clientX,y:e.clientY,id:e.pointerId};});
 root.addEventListener('pointercancel',()=>pointer=null);
 root.addEventListener('pointerup',e=>{if(!pointer||pointer.id!==e.pointerId)return;const dx=e.clientX-pointer.x,dy=e.clientY-pointer.y;pointer=null;if(Math.abs(dx)>55&&Math.abs(dx)>Math.abs(dy)*1.5){suppressClickUntil=Date.now()+400;showMonth(index+(dx<0?1:-1),true);}});
 sheet.addEventListener('click',e=>{if(e.detail>0&&Date.now()<suppressClickUntil){e.preventDefault();e.stopPropagation();}},true);
 // Downward dismissal is a touch gesture, not pointerup: pan-y otherwise
 // cancels pointers as soon as the browser claims scrolling. Only own a
 // downward single-finger pull starting at the panel's scroll top; let the
 // panel scroll normally elsewhere and leave horizontal month swipes alone.
 sheet.addEventListener('touchstart',e=>{
  if(!active||!opened||e.touches.length!==1){pull=null;return;}
  const t=e.touches[0];pull={id:t.identifier,x:t.clientX,y:t.clientY,atTop:sheet.querySelector('.calendar-panel').scrollTop<=0,vertical:false};
 },{passive:true});
 sheet.addEventListener('touchmove',e=>{
  if(!pull||e.touches.length!==1){pull=null;return;}
  const t=e.touches[0],dx=t.clientX-pull.x,dy=t.clientY-pull.y;
  if(pull.atTop&&dy>0&&dy>Math.abs(dx)*1.5){pull.vertical=true;e.preventDefault();}
 },{passive:false});
 sheet.addEventListener('touchend',e=>{
  if(!pull)return;const start=pull;pull=null;const t=[...e.changedTouches].find(t=>t.identifier===start.id);if(!t)return;
  const dx=t.clientX-start.x,dy=t.clientY-start.y;
  if(start.vertical&&dy>56&&dy>Math.abs(dx)*1.5){e.preventDefault();pointer=null;suppressClickUntil=Date.now()+400;setOpen(false);}
 },{passive:false});
 sheet.addEventListener('touchcancel',()=>{pull=null;});
 // Horizontal trackpad/wheel intent scrolls the day strip, never expands it.
 rail.addEventListener('wheel',e=>{if(Math.abs(e.deltaX)>Math.abs(e.deltaY)){e.preventDefault();rail.scrollLeft+=e.deltaX;}},{passive:false});
 media.addEventListener('change',mount);win.addEventListener('resize',()=>resize());win.addEventListener('pageshow',center);win.visualViewport?.addEventListener('resize',()=>resize());
 new ResizeObserver(()=>{if(active){const height=(opened?sheet:accessory).getBoundingClientRect().height+nav.getBoundingClientRect().height+2;if(Math.abs(parseFloat(dock.style.height)-height)>1)resize();}}).observe(nav);
 showMonth(index);mount();win.requestAnimationFrame(center);
}
