import { citySurface, cityEntries, visibleHeading, sectionHeadings, sectionRanges as readSectionRanges } from './islandSurface.mjs';
export function initMobileFloatingIslands(doc=document,win=window){
 const band=doc.querySelector('[data-floating-top-band]');if(!band||band.__islands)return;
 const context=band.querySelector('[data-floating-page-context]'),section=band.querySelector('[data-floating-section-context]'),scope=band.querySelector('[data-floating-section-title]');
 const surface=citySurface(doc,true);if(!surface)return;
 const {controls,panel,field,toggle,closeButton}=surface,nav=doc.querySelector('.site-nav');
 const weekend=controls.closest('[data-mobile-v23-page]')?.dataset.mobileV23Page==='weekend';
 const singleDay=['today','tomorrow','date'].includes(controls.closest('[data-mobile-v23-page]')?.dataset.mobileV23Page);
 if(!context||!section||!field||!nav)return;
 const abort=new AbortController(),on=(n,t,fn,opts={})=>n?.addEventListener(t,fn,{...opts,signal:abort.signal});
 const contextParent=context.parentNode,sectionParent=section.parentNode,contextNext=context.nextSibling,sectionNext=section.nextSibling;
 const originalContextStyle=context.getAttribute('style')||'',originalControlsStyle=controls.getAttribute('style')||'',originalNavStyle=nav.getAttribute('style')||'';
 const controlParent=controls.parentNode,controlNext=controls.nextSibling;
 const prepared=controls.hasAttribute('data-fi-ssr-city');
 const marker=prepared?controls.parentElement:doc.createElement('div');if(!prepared){marker.className='fi-mobile-city-origin';controls.before(marker);marker.append(controls);}
 const shortRail=marker.closest('.ke-listing-discovery-rail'),feed=marker.closest('.feed-head');if(shortRail)shortRail.before(marker);else if(feed)feed.before(marker);
 const title=visibleHeading(doc),titleParent=title.parentNode,titleNext=title.nextSibling,titleStyle=title.getAttribute('style')||'';
 const titleMarker=doc.createElement('div');titleMarker.className='fi-mobile-title-origin';title.before(titleMarker);
 const flow=[titleParent,title.closest('.ke-listing-head')].filter(Boolean);flow.forEach(n=>n.dataset.fiMobileFlow='');
 const titleSkin=doc.createElement('div');titleSkin.className='fi-mobile-title-skin';titleSkin.setAttribute('aria-hidden','true');
 doc.body.dataset.fiMobile='';if(weekend)doc.body.dataset.fiWeekend='';if(singleDay)doc.body.dataset.fiSingleDay='';
 context.append(section);context.hidden=false;section.hidden=false;context.setAttribute('aria-hidden','false');
 const pageButton=context.querySelector('button:not(.site-header__section-context)');if(pageButton)pageButton.hidden=true;
 titleMarker.append(context);context.prepend(titleSkin,title);
 // Weekend retains its real H1 in flow. The SAME context slot becomes two
 // short day anchors, rather than a second title/subtitle or sticky floor.
 let weekendContext=null,weekendNav=null;
 if(weekend){titleMarker.before(title);weekendContext=createWeekendDays(doc,win,on);weekendNav=weekendContext.nav;context.append(weekendNav);}
 band.querySelector('[data-floating-controls-slot]').hidden=true;band.querySelector('[data-floating-utility-slot]').hidden=true;
 const skin=controls.querySelector('.fi-city-skin')||doc.createElement('div');skin.className='fi-city-skin';skin.setAttribute('aria-hidden','true');controls.prepend(skin);
 const row=controls.querySelector('.fi-city-visible')||doc.createElement('div');row.className='fi-city-visible';row.setAttribute('role','group');row.setAttribute('aria-label','Города');controls.append(row);
 const items=cityEntries(field).map(entry=>{
  const {label,input}=entry,button=[...row.querySelectorAll('[data-fi-city-value]')].find(n=>n.dataset.fiCityValue===input.value)||doc.createElement('button');button.type='button';button.className='fi-city-item';button.dataset.fiCityValue=input.value;
  const name=button.querySelector('span')||doc.createElement('span'),count=button.querySelector('small')||doc.createElement('small');name.textContent=singleDay&&input.value==='all'?'Все':entry.name;button.setAttribute('aria-label',entry.name);button.append(name,count);row.append(button);
  on(button,'click',()=>{input.checked=!input.checked;input.dispatchEvent(new Event('change',{bubbles:true}));});return{...entry,button,count};
 });
 row.append(toggle);toggle.hidden=true;toggle.setAttribute('aria-haspopup','dialog');panel.hidden=true;closeButton.hidden=true;panel.setAttribute('role','dialog');panel.setAttribute('aria-label','Остальные города');
 let geometry=null,ready=false,dead=false,frame=0,measureNeeded=true,opened=false,visibleCount=items.length,activeHeading=null,lastScope='',sectionRanges=[],widths=[],docked=null,motions=[],settleTimer=0;
 let selectionLabel='';
 function compactLabel(){const selected=items.filter(i=>i.input.checked&&i.input.value!=='all');return selected.length?selected.map(i=>i.name).join(', '):'Все города';}
 const nativeTimeline=win.CSS?.supports('animation-timeline','scroll(root block)');
 const reduced=()=>win.matchMedia('(prefers-reduced-motion: reduce)').matches;
 function sync(){const label=compactLabel();if(singleDay&&selectionLabel&&selectionLabel!==label)schedule(true);selectionLabel=label;for(const item of items){item.button.setAttribute('aria-pressed',String(item.input.checked));const count=item.countText();if(item.count.textContent!==count)item.count.textContent=count;}}
 function ranges(){sectionRanges=readSectionRanges(doc,win);}
 function cancelMotion(){win.clearTimeout(settleTimer);settleTimer=0;motions.forEach(a=>a.cancel());motions=[];controls.removeAttribute('data-fi-moving');}
 function measure(){
  const savedScroll=row.scrollLeft;close(false);cancelMotion();
  marker.style.position='static';titleMarker.style.position='static';
  row.style.cssText='';toggle.style.cssText='';row.append(toggle);controls.style.cssText=originalControlsStyle+';animation:none;transform:none;width:max-content;max-width:100%;margin:0 auto;';items.forEach(i=>i.button.hidden=false);toggle.hidden=true;panel.hidden=true;
  title.style.cssText=titleStyle+';animation:none;transform:none;';
  const brand=doc.querySelector('[data-mobile-discovery-menu] > summary').getBoundingClientRect(),tr=titleMarker.getBoundingClientRect(),font=parseFloat(win.getComputedStyle(title).fontSize);
  const narrow=win.innerWidth<360,inset=12,gap=narrow?8:10,pad=narrow?8:12;
  const left=brand.right+(narrow?8:12),cityWidth=narrow?44:54,contextWidth=win.innerWidth-left-inset-cityWidth-gap,titleTop=singleDay||weekend?20:8;
  // Reserve enough space for every full section title once per viewport, not
  // on scroll; changing sections never makes the sticky stack jump.
  const probe=doc.createElement('div');probe.style.cssText=`position:fixed;visibility:hidden;width:${contextWidth-2*pad}px;font:600 12px/17px Inter,ui-sans-serif,system-ui,sans-serif;`;doc.body.append(probe);
  let sectionHeight=34;for(const h of sectionHeadings(doc)){if(!h.getClientRects().length&&!h.closest('[data-popular-personalized]'))continue;probe.textContent=h.textContent.trim();sectionHeight=Math.max(sectionHeight,probe.getBoundingClientRect().height);}probe.remove();
  const contextHeight=weekend?44:singleDay?56:Math.max(70,sectionHeight+38);titleMarker.style.height=weekend?'0px':singleDay?'38px':`${contextHeight}px`;
  const titleEnd=Math.max(1,tr.y+win.scrollY-brand.bottom-16),targetY=titleTop;
  widths=items.map(i=>i.button.getBoundingClientRect().width);
  const fullWidth=widths.reduce((a,b)=>a+b,0)+6*(items.length-1)+16;
  const originWidth=Math.min(fullWidth,win.innerWidth-24);
  // Accepted mobile reference: the city surface becomes the ellipsis island
  // beside the title, in the SAME top row; all original choices remain in its
  // picker. One-day pages instead use a named, content-sized picker without
  // a duplicate title. The pre-dock row is a native horizontal scroller containing every city.
  const labelProbe=doc.createElement('span');labelProbe.textContent=compactLabel();labelProbe.style.cssText='position:fixed;visibility:hidden;white-space:nowrap;font:650 12px/1 Inter,ui-sans-serif,system-ui,sans-serif;';doc.body.append(labelProbe);
  const targetWidth=singleDay?Math.min(Math.max(112,Math.ceil(labelProbe.getBoundingClientRect().width)+48),win.innerWidth-left-inset):cityWidth;labelProbe.remove();
  marker.style.height=`${contextHeight}px`;
  const cr=controls.getBoundingClientRect(),mr=marker.getBoundingClientRect();
  geometry={origin:{x:(win.innerWidth-originWidth)/2,y:cr.y+win.scrollY,width:originWidth,height:56},context:{x:left,y:titleTop,width:contextWidth,height:contextHeight},title:{originY:tr.y+win.scrollY,end:titleEnd,top:titleTop,x:left+pad-tr.x,scale:Math.min(1,18/font,(contextWidth-2*pad)/title.getBoundingClientRect().width)},city:{x:win.innerWidth-inset-targetWidth,y:targetY,width:targetWidth,height:singleDay||weekend?44:contextHeight},fullWidth,markerX:mr.x,threshold:Math.max(1,Math.min(48,cr.y+win.scrollY-brand.bottom-12-80)),approachTop:Math.max(brand.bottom,titleTop+contextHeight)+12};
  titleMarker.style.position='';marker.style.position='';marker.style.transition='none';marker.style.setProperty('--fi-city-top',`${geometry.approachTop}px`);
  title.style.cssText=titleStyle;
  if(!nativeTimeline){title.style.animation='none';titleSkin.style.animation='none';}
  context.style.cssText=`--fi-title-height:${singleDay?38:contextHeight}px;--fi-title-x:${left+pad-tr.x}px;--fi-title-scale:${geometry.title.scale};--fi-title-end:${titleEnd}px;--fi-skin-x:${left-tr.x}px;--fi-skin-width:${contextWidth}px;`;
  if(weekend){context.style.setProperty('--fi-title-height','44px');weekendNav.style.cssText=`left:${left-tr.x}px;width:${contextWidth}px;`;title.style.cssText=titleStyle;}
  section.style.cssText=`left:${left+pad-tr.x}px;top:30px;width:${contextWidth-2*pad}px;height:${contextHeight-38}px;`;section.hidden=true;
  controls.style.cssText='';controls.append(toggle);toggle.style.cssText='position:absolute;left:0;top:0;width:44px;';row.style.width=`${originWidth}px`;
  ranges();docked=null;setDocked(win.scrollY>=geometry.threshold,false);row.scrollLeft=savedScroll;measureNeeded=false;
 }
 function fit(){
  // Native scrolling owns overflow before docking; the compact picker owns all
  // choices afterwards. Never hide/recreate row items or reset its scroll offset.
  visibleCount=docked?0:items.length;
  row.inert=!!docked;row.setAttribute('aria-hidden',String(!!docked));
  items.forEach((item,i)=>{item.button.hidden=false;item.button.inert=!!docked;item.label.hidden=opened&&i<visibleCount;});
  toggle.hidden=!docked;toggle.inert=!docked;toggle.textContent=singleDay?compactLabel():'…';
  toggle.setAttribute('aria-label',`Выбрать город. Сейчас: ${items.filter(i=>i.input.checked).map(i=>i.name).join(', ')}`);
  toggle.classList.toggle('has-selected',items.some(i=>i.input.checked&&i.input.value!=='all'));
  controls.dataset.fiVisibleCount=String(visibleCount);controls.dataset.fiOverflowCount=String(items.length-visibleCount);
 }
 function setDocked(next,animate=true){
  if(next===docked)return;
  const before=controls.getBoundingClientRect(),skinWidth=skin.getBoundingClientRect().width,skinHeight=skin.getBoundingClientRect().height;
  const beforeRowClip=win.getComputedStyle(row).clipPath;
  const beforeToggleClip=toggle.hidden?'inset(0px 100% 0px 0px)':win.getComputedStyle(toggle).clipPath;
  const toggleWidth=singleDay?geometry.city.width:44;
  const beforeToggleX=toggle.hidden?skinWidth-toggleWidth:toggle.getBoundingClientRect().x-before.x;
  const beforeToggleY=toggle.hidden?7:toggle.getBoundingClientRect().y-before.y;
  cancelMotion();docked=next;close(false);
  marker.style.transition='none';marker.style.setProperty('--fi-city-top',`${next&&(!animate||reduced())?geometry.city.y:geometry.approachTop}px`);
  const target=next?geometry.city:geometry.origin,x=target.x-geometry.markerX;
  controls.style.width=`${target.width}px`;controls.style.height=`${target.height}px`;controls.style.minHeight=`${target.height}px`;controls.style.transform=`translate3d(${x}px,0,0)`;fit();
  const emptyRow=`inset(0px ${geometry.origin.width}px 0px 0px)`,fullRow='inset(0px 0px 0px 0px)';
  const emptyToggle='inset(0px 100% 0px 0px)',fullToggle='inset(0px 0% 0px 0px)';
  const rowClip=next?emptyRow:fullRow,toggleClip=next?fullToggle:emptyToggle;
  row.style.clipPath=rowClip;toggle.style.width=`${toggleWidth}px`;toggle.style.clipPath=toggleClip;
  const toggleX=next?(target.width-toggleWidth)/2:target.width-toggleWidth,toggleY=(target.height-(next&&singleDay?44:40))/2;
  toggle.style.transform=`translate(${toggleX}px,${toggleY}px)`;controls.dataset.fiDocked=String(next);
  if(!animate||reduced())return;
  toggle.hidden=false;
  const timing={duration:540,easing:'cubic-bezier(.25,.1,.25,1)',fill:'both'};
  controls.dataset.fiMoving='true';
  const move=controls.animate([{transform:`translate3d(${before.x-geometry.markerX}px,0,0)`},{transform:`translate3d(${x}px,0,0)`}],timing);
  // A shared two-phase clipping handoff: the row leaves BEFORE the compact
  // caption enters (the reverse on expansion). No intersecting text layers,
  // opacity fade, font scaling, DOM replacement or scroll-time seeking.
  const rowStart=beforeRowClip==='none'?fullRow:beforeRowClip;
  const toggleStart=beforeToggleClip==='none'?fullToggle:beforeToggleClip;
  const handoff=next ? 0.58 : 0.42;
  motions=[move,skin.animate([{transform:`scale(${skinWidth/target.width},${skinHeight/target.height})`},{transform:'scale(1,1)'}],timing),
   row.animate([{clipPath:rowStart},{clipPath:next?emptyRow:rowStart,offset:handoff},{clipPath:rowClip}],timing),
   toggle.animate([{transform:`translate(${beforeToggleX}px,${beforeToggleY}px)`},{transform:`translate(${toggleX}px,${toggleY}px)`}],timing),
   toggle.animate([{clipPath:toggleStart},{clipPath:next?toggleStart:emptyToggle,offset:handoff},{clipPath:toggleClip}],timing)];
  move.onfinish=()=>{cancelMotion();fit();
   if(next){
    marker.style.transition='top 180ms cubic-bezier(.25,.1,.25,1)';marker.style.setProperty('--fi-city-top',`${geometry.city.y}px`);
    controls.dataset.fiMoving='true';settleTimer=win.setTimeout(()=>{settleTimer=0;controls.removeAttribute('data-fi-moving');},220);
   }
  };
 }
 function close(focus=true){if(!opened)return;opened=false;if(panel.matches(':popover-open'))panel.hidePopover();panel.removeAttribute('popover');panel.style.cssText='';panel.hidden=true;items.forEach(i=>i.label.hidden=false);closeButton.hidden=true;controls.removeAttribute('data-island-city-open');toggle.setAttribute('aria-expanded','false');if(focus)toggle.focus({preventScroll:true});}
 function placePanel(){const r=(docked?controls:toggle).getBoundingClientRect(),bottom=docked?Math.max(r.bottom,titleSkin.getBoundingClientRect().bottom,doc.querySelector('[data-mobile-discovery-menu]>summary').getBoundingClientRect().bottom):r.bottom,width=Math.min(390,win.innerWidth-32);panel.style.cssText=`position:fixed;inset:auto;margin:0;left:${Math.max(16,Math.min(win.innerWidth-width-16,r.right-width))}px;top:${bottom+10}px;width:${width}px;max-height:${Math.max(120,win.innerHeight-bottom-120)}px;overflow:auto;`;}
 function open(){if(opened){close();return;}opened=true;panel.hidden=false;panel.setAttribute('popover','manual');controls.setAttribute('data-island-city-open','');closeButton.hidden=false;toggle.setAttribute('aria-expanded','true');items.forEach((item,i)=>item.label.hidden=i<visibleCount);placePanel();panel.showPopover();panel.querySelector('label:not([hidden]) input')?.focus({preventScroll:true});schedule();}
 function render(){
  frame=0;if(dead||!ready)return;if(measureNeeded)measure();
  const g=geometry,y=win.scrollY;
  if(!docked&&y>=g.threshold)setDocked(true);else if(docked&&y<=Math.max(0,g.threshold-24))setDocked(false);
  if(opened){placePanel();if(motions.length||settleTimer)schedule();}
  const line=y+g.context.y+g.context.height+12;let active=sectionRanges[0];for(const r of sectionRanges)if(r.top<=line)active=r;
  activeHeading=active?.heading;const text=activeHeading?.textContent.trim()||'';
  if(text!==lastScope){scope.textContent=text;lastScope=text;}
  // No short-label map, no text opacity animation, no scroll-position compensation.
  // Older mobile engines: transform-only fallback. Native sticky still owns Y;
  // never resize fonts, reflow controls or correct vertical scroll coordinates.
  if(!nativeTimeline&&!singleDay&&!weekend){const t=Math.min(1,Math.max(0,y/g.title.end)),p=reduced()?1:1-(1-t)**3,{x,scale}=g.title;title.style.transform=`translateX(${x*p}px) scale(${1+(scale-1)*p})`;titleSkin.style.opacity=String(reduced()?(t>=1?1:0):t*t);}
  const arrived=!singleDay&&!weekend&&y>=g.title.end;section.hidden=!arrived;section.tabIndex=arrived?0:-1;section.setAttribute('aria-label',`К разделу: ${text}`);
  weekendContext?.update();
  context.dataset.fiTitleArrived=String(arrived);
  doc.body.dataset.floatingContext=docked?'docked':'morphing';
 }
 function schedule(measureAgain=false){if(measureAgain)measureNeeded=true;if(!frame&&!dead)frame=win.requestAnimationFrame(render);}
 on(toggle,'click',open);on(closeButton,'click',()=>close());on(section,'click',()=>activeHeading&&win.scrollTo({top:activeHeading.getBoundingClientRect().top+win.scrollY-geometry.context.y-geometry.context.height-12,behavior:reduced()?'instant':'smooth'}));
 on(controls,'click',()=>{queueMicrotask(()=>{sync();ranges();schedule();});});
 on(controls,'change',()=>{queueMicrotask(()=>{sync();fit();ranges();schedule();});});
 on(doc,'keydown',e=>{if(e.key==='Escape'){if(opened){e.preventDefault();close();}}});
 on(doc,'pointerdown',e=>{if(opened&&!panel.contains(e.target)&&!controls.contains(e.target))close(false);});
 if(!singleDay&&!weekend)on(title,'click',()=>win.scrollTo({top:0,behavior:reduced()?'instant':'smooth'}));
 on(win,'scroll',()=>schedule(),{passive:true});on(win,'resize',()=>schedule(true),{passive:true});on(win,'pageshow',()=>schedule(true));
 on(win.matchMedia('(prefers-reduced-motion: reduce)'),'change',()=>{cancelMotion();fit();marker.style.transition='none';marker.style.setProperty('--fi-city-top',`${docked?geometry.city.y:geometry.approachTop}px`);schedule();});
 const personal=doc.querySelector('[data-popular-personalized]');
 const personalObserver=new MutationObserver(()=>schedule(true));if(personal)personalObserver.observe(personal,{attributes:true,attributeFilter:['hidden']});
 band.__islands={get geometry(){return geometry},destroy(){dead=true;personalObserver.disconnect();cancelMotion();abort.abort();win.cancelAnimationFrame(frame);close(false);
 titleParent.insertBefore(title,titleNext);title.style.cssText=titleStyle;flow.forEach(n=>delete n.dataset.fiMobileFlow);titleSkin.remove();weekendNav?.remove();
 contextParent.insertBefore(context,contextNext);sectionParent.insertBefore(section,sectionNext);context.style.cssText=originalContextStyle;section.style.cssText='';if(pageButton)pageButton.hidden=false;
 if(!prepared){row.remove();skin.remove();}else{row.style.cssText='';row.inert=false;items.forEach(i=>i.button.inert=false);marker.style.cssText='';toggle.hidden=true;}controls.prepend(toggle);toggle.style.cssText='';controls.style.cssText=originalControlsStyle;controlParent.insertBefore(controls,controlNext);if(!prepared)marker.remove();titleMarker.remove();panel.hidden=prepared;delete doc.body.dataset.fiMobile;delete doc.body.dataset.fiSingleDay;delete doc.body.dataset.fiWeekend;delete band.__islands;}};
 sync();ready=true;measure();render();doc.body.dataset.fiMotion='ready';
 doc.fonts.ready.then(()=>{if(!dead)schedule(true);});return band.__islands;
}

/** Shared day labels use route dates, never the browser's current weekend. */
function createWeekendDays(doc,win,on){
 const nav=doc.createElement('nav');nav.className='fi-weekend-days';nav.setAttribute('aria-label','Дни выходных');
 const dates=doc.querySelector('[data-weekend-start]')?.dataset;
 const reduced=()=>win.matchMedia('(prefers-reduced-motion: reduce)').matches;
 const days=['sat','sun'].map((key,i)=>{
  const date=i?dates?.weekendEnd:dates?.weekendStart,heading=doc.getElementById(`mobile-rail-weekend-${key}`),button=doc.createElement('button');button.type='button';button.dataset.fiWeekendDay=key;
  button.textContent=i?'ВС':'СБ';button.setAttribute('aria-label',`${i?'Воскресенье':'Суббота'}, ${date||''}`);nav.append(button);
  on(button,'click',()=>{if(heading)win.scrollTo({top:heading.getBoundingClientRect().top+win.scrollY-104,behavior:reduced()?'instant':'smooth'});});return{button,heading};
 });
 const update=()=>{
  const progress=Math.min(1,Math.max(0,(win.scrollY-24)/72),Math.max(0,(64-nav.getBoundingClientRect().top)/44));nav.style.opacity=String(reduced()?(progress>=1?1:0):progress*progress);nav.inert=progress<1;
  let current=days.find(d=>d.heading&&!d.heading.closest('[hidden]'));for(const day of days)if(day.heading&&!day.heading.closest('[hidden]')&&day.heading.getBoundingClientRect().top<=116)current=day;
  for(const day of days){day.button.disabled=!day.heading||!!day.heading.closest('[hidden]');if(day===current)day.button.setAttribute('aria-current','date');else day.button.removeAttribute('aria-current');}
 };return{nav,update};
}

/** A one-city/empty weekend has the same short day context, no invented picker. */
export function initMobileWeekendWithoutCities(doc=document,win=window){
 const band=doc.querySelector('[data-floating-top-band]'),context=band.querySelector('[data-floating-page-context]'),title=visibleHeading(doc);
 const parent=context.parentNode,next=context.nextSibling,style=context.getAttribute('style')||'',wasHidden=context.hidden;
 const children=[...context.children],hidden=children.map(n=>n.hidden);children.forEach(n=>n.hidden=true);
 const head=title.closest('.page-head'),marker=doc.createElement('div');marker.className='fi-mobile-title-origin';head.append(marker);marker.append(context);context.hidden=false;
 const abort=new AbortController(),on=(n,t,f,o={})=>n.addEventListener(t,f,{...o,signal:abort.signal}),days=createWeekendDays(doc,win,on);context.append(days.nav);
 doc.body.dataset.fiMobile='';doc.body.dataset.fiWeekend='';
 function measure(){const brand=doc.querySelector('[data-mobile-discovery-menu] > summary').getBoundingClientRect(),x=brand.right+8-marker.getBoundingClientRect().x;context.style.cssText='--fi-title-height:44px;';days.nav.style.left=`${x}px`;days.nav.style.width=`${Math.min(160,win.innerWidth-brand.right-20)}px`;days.update();}
 on(win,'scroll',days.update,{passive:true});on(win,'resize',measure,{passive:true});on(win,'pageshow',measure);measure();doc.body.dataset.fiMotion='ready';
 band.__islands={destroy(){abort.abort();days.nav.remove();parent.insertBefore(context,next);context.style.cssText=style;context.hidden=wasHidden;children.forEach((n,i)=>n.hidden=hidden[i]);marker.remove();delete doc.body.dataset.fiMobile;delete doc.body.dataset.fiWeekend;delete band.__islands;}};return band.__islands;
}
