import { citySurface, cityEntries, visibleHeading, sectionHeadings, sectionRanges as readSectionRanges } from './islandSurface.mjs';
/** Vertical scroll is native sticky. Horizontal docking is a finite transform-only FLIP,
 * not a width animation scrubbed by scroll. Text stays opaque and is never scaled. */
const clamp=n=>Math.max(0,Math.min(1,n));
const ease=n=>{const t=clamp(n);return t*t*(3-2*t)};
export function fitCityItems(width,widths,moreWidths,gap=6,padding=16){
 const available=Math.max(0,width-padding),total=widths.reduce((a,b)=>a+b,0)+gap*Math.max(0,widths.length-1);
 if(total<=available)return widths.length;
 let used=0,count=0;
 for(let i=0;i<widths.length;i++){
  const next=used+(i?gap:0)+widths[i],remaining=widths.length-i-1;
  if(next+(remaining?gap+moreWidths[remaining]:0)>available)break;
  used=next;count=i+1;
 }
 return count;
}
export function initDesktopFloatingIslands(doc=document,win=window){
 const band=doc.querySelector('[data-floating-top-band]');if(!band||band.__islands)return;
 const context=band.querySelector('[data-floating-page-context]'),section=band.querySelector('[data-floating-section-context]'),scope=band.querySelector('[data-floating-section-title]');
 const surface=citySurface(doc,false);if(!surface)return;
 const {controls,panel,field,toggle,closeButton}=surface,nav=doc.querySelector('.site-nav');
 if(!context||!section||!field||!nav)return;
 const abort=new AbortController(),on=(n,t,fn,opts={})=>n?.addEventListener(t,fn,{...opts,signal:abort.signal});
 const contextParent=context.parentNode,sectionParent=section.parentNode,contextNext=context.nextSibling,sectionNext=section.nextSibling;
 const originalContextStyle=context.getAttribute('style')||'',originalControlsStyle=controls.getAttribute('style')||'',originalNavStyle=nav.getAttribute('style')||'';
 const controlParent=controls.parentNode,controlNext=controls.nextSibling;
 const marker=doc.createElement('div');marker.className='fi-desktop-city-origin';controls.before(marker);marker.append(controls);
 const shortRail=marker.closest('.ke-listing-discovery-rail'),feed=marker.closest('.feed-head');if(shortRail)shortRail.before(marker);else if(feed)feed.before(marker);
 const layer=doc.createElement('div');layer.className='fi-desktop-layer';layer.setAttribute('aria-label','Навигация по странице');doc.body.append(layer);
 doc.body.dataset.fiDesktop='';
 context.append(section);context.hidden=false;section.hidden=false;context.style.opacity='0';context.setAttribute('aria-hidden','true');
 const pageButton=context.querySelector('button:not(.site-header__section-context)');if(pageButton)pageButton.hidden=true;
 layer.append(context);
 band.querySelector('[data-floating-controls-slot]').hidden=true;band.querySelector('[data-floating-utility-slot]').hidden=true;
 const skin=doc.createElement('div');skin.className='fi-city-skin';skin.setAttribute('aria-hidden','true');controls.prepend(skin);
 const row=doc.createElement('div');row.className='fi-city-visible';row.setAttribute('role','group');row.setAttribute('aria-label','Города');controls.append(row);
 const items=cityEntries(field).map(entry=>{
  const {label,input}=entry,button=doc.createElement('button');button.type='button';button.className='fi-city-item';button.dataset.fiCityValue=input.value;
  const name=doc.createElement('span'),count=doc.createElement('small');name.textContent=entry.name;button.append(name,count);row.append(button);
  on(button,'click',()=>{input.checked=!input.checked;input.dispatchEvent(new Event('change',{bubbles:true}));});return{...entry,button,count};
 });
 row.append(toggle);toggle.hidden=true;toggle.setAttribute('aria-haspopup','dialog');panel.hidden=true;closeButton.hidden=true;panel.setAttribute('role','dialog');panel.setAttribute('aria-label','Остальные города');
 const navLinks=[...nav.querySelectorAll('[data-island-nav-link]')];
 nav.querySelector('[data-island-nav-more]')?.remove();
 const more=doc.createElement('details');more.dataset.islandNavMore='';more.hidden=true;
 more.innerHTML='<summary aria-label="Открыть остальные разделы" title="Остальные разделы"><svg aria-hidden="true" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"><path d="M4 6h16M4 12h16M4 18h16"/></svg></summary><div data-island-nav-panel></div>';
 nav.append(more);const navPanel=more.querySelector('[data-island-nav-panel]');
 let geometry=null,ready=false,dead=false,frame=0,measureNeeded=true,opened=false,visibleCount=items.length,activeHeading=null,lastScope='',sectionRanges=[],widths=[],moreWidths=[],docked=null,motions=[];
 const reduced=()=>win.matchMedia('(prefers-reduced-motion: reduce)').matches;
 function sync(){for(const item of items){item.button.setAttribute('aria-pressed',String(item.input.checked));const count=item.countText();if(item.count.textContent!==count)item.count.textContent=count;}}
 function ranges(){sectionRanges=doc.querySelector('[data-date-listing="weekend"]')?[]:readSectionRanges(doc,win);}
 function cancelMotion(){motions.forEach(a=>a.cancel());motions=[];controls.removeAttribute('data-fi-moving');}
 function measure(){
  close(false);more.open=false;cancelMotion();
  marker.style.position='static';row.style.cssText='';toggle.style.cssText='';row.append(toggle);controls.style.cssText=originalControlsStyle+';animation:none;transform:none;width:max-content;max-width:100%;margin:0 auto;';items.forEach(i=>i.button.hidden=false);toggle.hidden=true;panel.hidden=true;
  for(const link of navLinks)nav.insertBefore(link,more);more.hidden=true;nav.style.cssText=originalNavStyle;
  ranges();
  // Reserve the longest FULL heading once. Changing sections must not resize the dock.
  const probe=doc.createElement('span');probe.style.cssText='position:fixed;visibility:hidden;white-space:nowrap;font:650 13px/18px Inter,ui-sans-serif,system-ui,sans-serif;';doc.body.append(probe);
  let headingWidth=0;for(const r of sectionRanges){probe.textContent=r.heading?.textContent.trim()||'';headingWidth=Math.max(headingWidth,probe.getBoundingClientRect().width);}probe.remove();
  const brand=doc.querySelector('.site-header__brand-tag').getBoundingClientRect(),left=brand.right+12,right=parseFloat(win.getComputedStyle(nav).right),space=win.innerWidth-right-left;
  const contextWidth=headingWidth===0?0:Math.min(Math.ceil(headingWidth)+44,Math.max(240,win.innerWidth*.28));
  widths=items.map(i=>i.button.getBoundingClientRect().width);
  toggle.hidden=false;for(let n=1;n<=items.length;n++){toggle.textContent=`+${n}`;moreWidths[n]=toggle.getBoundingClientRect().width;}toggle.hidden=true;
  const fullWidth=widths.reduce((a,b)=>a+b,0)+6*(items.length-1)+16;
  const minCity=widths[0]+6+moreWidths[items.length-1]+16;
  // Cities yield space first. Only then move whole non-current destinations into the burger.
  const navWidths=navLinks.map(n=>n.getBoundingClientRect().width),navWidth=count=>12+navWidths.reduce((sum,w,i)=>sum+(visible.has(i)?w:0),0)+2*Math.max(0,count-1)+(count<navLinks.length?46:0);
  const visible=new Set(navLinks.map((_,i)=>i));let nw=navWidth(visible.size);
  const candidates=navLinks.map((_,i)=>i).reverse().filter(i=>navLinks[i].getAttribute('aria-current')!=='page');
  while(contextWidth+minCity+24+nw>space&&candidates.length){visible.delete(candidates.shift());nw=navWidth(visible.size);}
  let secondRow=contextWidth+minCity+24+nw>space;
  if(secondRow){visible.clear();navLinks.forEach((_,i)=>visible.add(i));while(navWidth(visible.size)>space&&candidates.length)visible.delete(candidates.shift());
   // A narrow desktop may need just the current destination plus burger above a second row.
   if(navWidth(visible.size)>space)for(let i=navLinks.length-1;i>=0&&navWidth(visible.size)>space;i--)if(navLinks[i].getAttribute('aria-current')!=='page')visible.delete(i);
  }
  for(let i=0;i<navLinks.length;i++)if(!visible.has(i))navPanel.append(navLinks[i]);more.hidden=visible.size===navLinks.length;
  nav.style.maxWidth=`${Math.max(140,space)}px`;const nr=nav.getBoundingClientRect();
  const targetY=secondRow?Math.max(brand.bottom,nr.bottom)+12:20,targetX=secondRow?brand.left:left;
  const cityX=targetX+contextWidth+12,cityEnd=secondRow?win.innerWidth-brand.left:nr.left-12;
  const cr=controls.getBoundingClientRect(),mr=marker.getBoundingClientRect();
  geometry={origin:{x:cr.x,y:cr.y+win.scrollY,width:cr.width,height:56},context:{x:targetX,y:targetY,width:contextWidth,height:56},city:{x:cityX,y:targetY,width:Math.min(fullWidth,Math.max(minCity,cityEnd-cityX)),height:56},secondRow,fullWidth,markerX:mr.x,threshold:Math.max(24,cr.y+win.scrollY-targetY-140)};
  marker.style.position='';marker.style.setProperty('--fi-city-top',`${targetY}px`);
  controls.style.cssText='';controls.append(toggle);toggle.style.cssText='position:absolute;left:0;top:7px;';row.style.width=`${fullWidth}px`;
  context.style.cssText=`position:absolute;left:${targetX}px;top:${targetY}px;width:${contextWidth}px;height:56px;max-width:none;min-height:0;`;
  docked=null;setDocked(win.scrollY>=geometry.threshold,false);measureNeeded=false;
 }
 function fit(width){
  const count=fitCityItems(width,widths,moreWidths);visibleCount=count;
  items.forEach((item,i)=>{item.button.hidden=i>=count;item.button.inert=i>=count;item.label.hidden=opened&&i<count;});
  const n=items.length-count;toggle.hidden=n===0;toggle.textContent=`+${n}`;toggle.setAttribute('aria-label',`Ещё ${n} ${n===1?'город':'города'}${items.slice(count).some(i=>i.input.checked)?', есть выбранные':''}`);toggle.classList.toggle('has-selected',items.slice(count).some(i=>i.input.checked));
  controls.dataset.fiVisibleCount=String(count);controls.dataset.fiOverflowCount=String(n);
 }
 function setDocked(next,animate=true){
  if(next===docked)return;
  // Preserve the live text nodes. Read the current mask/X before cancellation,
  // including on reversal, rather than cloning/cross-fading an outgoing row.
  const before=controls.getBoundingClientRect(),skinWidth=skin.getBoundingClientRect().width;
  const beforeClip=win.getComputedStyle(row).clipPath;
  const beforeToggleX=toggle.hidden?skinWidth-54:toggle.getBoundingClientRect().x-before.x;
  const shown=items.map(i=>!i.button.hidden);
  cancelMotion();docked=next;close(false);
  const target=next?geometry.city:geometry.origin,x=target.x-geometry.markerX;
  controls.style.width=`${target.width}px`;controls.style.transform=`translate3d(${x}px,0,0)`;fit(target.width);
  const count=visibleCount,hasMore=count<items.length;
  const textEdge=hasMore?8+widths.slice(0,count).reduce((a,b)=>a+b,0)+6*Math.max(0,count-1):geometry.fullWidth;
  const clip=`inset(0px ${Math.max(0,geometry.fullWidth-textEdge)}px 0px 0px)`;
  row.style.clipPath=clip;toggle.style.transform=`translateX(${target.width-54}px)`;
  controls.dataset.fiDocked=String(next);
  if(!animate||reduced())return;
  // Keep outgoing choices in the same row while the edge crops them away.
  // They cannot receive focus/clicks once moved to the overflow picker.
  items.forEach((item,i)=>{item.button.hidden=!(shown[i]||i<count);});
  const movingOverflow=Math.max(items.length-count,items.length-fitCityItems(before.width,widths,moreWidths));toggle.hidden=movingOverflow===0;toggle.textContent=`+${movingOverflow}`;
  const timing={duration:480,easing:'cubic-bezier(.22,1,.36,1)',fill:'both'};
  controls.dataset.fiMoving='true';
  const move=controls.animate([{transform:`translate3d(${before.x-geometry.markerX}px,0,0)`},{transform:`translate3d(${x}px,0,0)`}],timing);
  motions=[move,skin.animate([{transform:`scaleX(${skinWidth/target.width})`},{transform:'scaleX(1)'}],timing),
   row.animate([{clipPath:beforeClip==='none'?'inset(0px 0px 0px 0px)':beforeClip},{clipPath:clip}],timing),
   toggle.animate([{transform:`translateX(${beforeToggleX}px)`},{transform:`translateX(${target.width-54}px)`}],timing)];
  move.onfinish=()=>{cancelMotion();fit(target.width);};
 }
 function close(focus=true){if(!opened)return;opened=false;if(panel.matches(':popover-open'))panel.hidePopover();panel.removeAttribute('popover');panel.style.cssText='';panel.hidden=true;items.forEach(i=>i.label.hidden=false);closeButton.hidden=true;controls.removeAttribute('data-island-city-open');toggle.setAttribute('aria-expanded','false');if(focus)toggle.focus({preventScroll:true});}
 function placePanel(){const r=toggle.getBoundingClientRect(),width=Math.min(390,win.innerWidth-32);panel.style.cssText=`position:fixed;inset:auto;margin:0;left:${Math.max(16,Math.min(win.innerWidth-width-16,r.right-width))}px;top:${r.bottom+10}px;width:${width}px;max-height:${Math.max(120,win.innerHeight-r.bottom-120)}px;overflow:auto;`;}
 function open(){if(opened){close();return;}opened=true;panel.hidden=false;panel.setAttribute('popover','manual');controls.setAttribute('data-island-city-open','');closeButton.hidden=false;toggle.setAttribute('aria-expanded','true');items.forEach((item,i)=>item.label.hidden=i<visibleCount);placePanel();panel.showPopover();panel.querySelector('label:not([hidden]) input')?.focus({preventScroll:true});}
 function render(){
  frame=0;if(dead||!ready)return;if(measureNeeded)measure();
  const g=geometry,y=win.scrollY;
  // Hysteresis prevents boundary chatter. No per-scroll city coordinate/size writes.
  if(!docked&&y>=g.threshold)setDocked(true);else if(docked&&y<=Math.max(0,g.threshold-24))setDocked(false);
  if(opened)placePanel();
  const line=y+g.context.y+70;let active=sectionRanges[0];for(const r of sectionRanges){if(r.top<=line)active=r;}
  activeHeading=active?.heading;const text=activeHeading?.textContent.trim()||'';
  if(text!==lastScope){scope.textContent=text;lastScope=text;}
  const first=sectionRanges[0],alpha=first?ease((y+g.context.y+135-first.top)/110):0;
  context.style.opacity=String(reduced()?(alpha===1?1:0):alpha);context.setAttribute('aria-hidden',String(alpha===0));context.style.pointerEvents=alpha>.95?'auto':'none';section.tabIndex=alpha>.95?0:-1;
  section.setAttribute('aria-label',`К разделу: ${text}`);doc.body.dataset.floatingContext=docked?'docked':'morphing';
 }
 function schedule(measureAgain=false){if(measureAgain)measureNeeded=true;if(!frame&&!dead)frame=win.requestAnimationFrame(render);}
 on(toggle,'click',open);on(closeButton,'click',()=>close());on(section,'click',()=>activeHeading&&win.scrollTo({top:activeHeading.getBoundingClientRect().top+win.scrollY-geometry.context.y-76,behavior:reduced()?'instant':'smooth'}));
 on(controls,'click',()=>{queueMicrotask(()=>{sync();ranges();schedule();});});
 on(controls,'change',()=>{queueMicrotask(()=>{sync();fit(controls.getBoundingClientRect().width);ranges();schedule();});});
 on(doc,'keydown',e=>{if(e.key==='Escape'){if(opened){e.preventDefault();close();}if(more.open){more.open=false;more.querySelector('summary').focus();}}});
 on(doc,'pointerdown',e=>{if(opened&&!panel.contains(e.target)&&!controls.contains(e.target))close(false);if(more.open&&!more.contains(e.target))more.open=false;});
 on(navPanel,'click',e=>{if(e.target.closest('a'))more.open=false;});
 on(win,'scroll',()=>schedule(),{passive:true});on(win,'resize',()=>schedule(true),{passive:true});on(win,'pageshow',()=>schedule(true));
 on(win.matchMedia('(prefers-reduced-motion: reduce)'),'change',()=>{cancelMotion();fit(controls.getBoundingClientRect().width);schedule();});
 const personal=doc.querySelector('[data-popular-personalized]');
 const personalObserver=new MutationObserver(()=>schedule(true));if(personal)personalObserver.observe(personal,{attributes:true,attributeFilter:['hidden']});
 band.__islands={get geometry(){return geometry},destroy(){dead=true;personalObserver.disconnect();cancelMotion();abort.abort();win.cancelAnimationFrame(frame);close(false);for(const link of navLinks)nav.insertBefore(link,more);more.remove();nav.style.cssText=originalNavStyle;contextParent.insertBefore(context,contextNext);sectionParent.insertBefore(section,sectionNext);context.style.cssText=originalContextStyle;if(pageButton)pageButton.hidden=false;row.remove();skin.remove();controls.prepend(toggle);toggle.style.cssText='';controls.style.cssText=originalControlsStyle;controlParent.insertBefore(controls,controlNext);marker.remove();panel.hidden=false;layer.remove();delete doc.body.dataset.fiDesktop;delete band.__islands;}};
 doc.fonts.ready.then(()=>new Promise(r=>win.requestAnimationFrame(()=>win.requestAnimationFrame(r)))).then(()=>{if(dead)return;sync();ready=true;measure();render();doc.body.dataset.fiMotion='ready';});return band.__islands;
}
