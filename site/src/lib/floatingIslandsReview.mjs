/** Preview-only contextual presentation. Does not own filters, brand, navigation or network. */
export const REVIEW_VERSION = 'floating-islands.owner-review.v1.3';
export const REVIEW_PREFIX = /^\/preview-islands-[a-z0-9][a-z0-9-]*$/;
export function reviewEnabled(basePath, production = false) {
  return !production && typeof basePath === 'string' && REVIEW_PREFIX.test(basePath.replace(/\/$/, ''));
}
export function citySummary(all, cities) {
  if (all) return 'Все города';
  const selected = cities.filter(city => city.checked);
  return !selected.length ? 'Города не выбраны' : selected.length === 1 ? selected[0].label : `Города · ${selected.length}`;
}
export function equivalentFreeScope(pathname, basePath) {
  return reviewEnabled(basePath) && pathname === `${basePath.replace(/\/$/, '')}/podborki/besplatnye-sobytiya/`;
}
export function panelPlacement(anchor, viewport, occupiedBottom = 0) {
  const values = [anchor.left, anchor.bottom, viewport.left, viewport.top, viewport.width, viewport.height, occupiedBottom];
  if (values.some(n => !Number.isFinite(n)) || viewport.width <= 0 || viewport.height <= 0) throw new TypeError('Finite positive viewport required');
  const gap = 12, width = Math.max(0, Math.min(420, viewport.width - gap * 2));
  const left = Math.max(viewport.left + gap, Math.min(anchor.left, viewport.left + viewport.width - gap - width));
  const top = Math.max(viewport.top + gap, anchor.bottom + 8);
  const maxHeight = Math.max(0, viewport.top + viewport.height - Math.max(0, occupiedBottom) - gap - top);
  return {left, top, width, maxHeight, inline: maxHeight < 144 || width < 240};
}
/** Only obstacles intersecting the actual panel's horizontal lane constrain it. */
export function panelBottomInset(view, panel, rects = []) {
  const roles = new Set(['navigation','date','event-cta','notification']);
  return Math.max(0, ...rects.filter(r => roles.has(r.role)
    && [r.x,r.y,r.width,r.height].every(Number.isFinite) && r.width > 0 && r.height > 0
    && r.x < panel.left + panel.width && r.x + r.width > panel.left
    && r.y + r.height > view.top && r.y < view.top + view.height)
    .map(r => view.top + view.height - Math.max(view.top, r.y)));
}
export function cityNeedsCompact(available, natural, viewportWidth, wasCompact = false) {
  if (![available,natural,viewportWidth].every(Number.isFinite) || available <= 0) return true;
  return viewportWidth < 980 || natural > available - (wasCompact ? 24 : 0);
}
export function initFloatingIslandsReview(doc = document, win = window) {
  const seed = doc.querySelector('[data-fi-review-seed]');
  if (!seed) return null;
  if (seed.__fiReview) return seed.__fiReview;
  const base = seed.dataset.fiBase || '';
  if (!reviewEnabled(base) || new URLSearchParams(win.location.search).get('islands') === 'off') return null;
  const controller = new win.AbortController(), options = {signal: controller.signal};
  const cleanups = [], tasks = [];
  let frame = 0, pointerHeld = false, composing = false, modalOpen = doc.body.dataset.lowerSurfaceState === 'modal';
  const schedule = () => {
    if (!controller.signal.aborted && !frame) frame = win.requestAnimationFrame(() => {frame = 0; tasks.forEach(fn => fn());});
  };
  const root = doc.documentElement;
  root.dataset.fiReview = REVIEW_VERSION;
  cleanups.push(() => delete root.dataset.fiReview);
  // Preserve the text fallback until the real canonical asset has loaded.
  const free = doc.querySelector('[data-free-collection-surface]');
  const context = doc.querySelector('[data-floating-page-context]');
  const action = context?.querySelector('button');
  const source = free?.querySelector('[data-free-collection-medallion="large"] img');
  if (free && context && action && source && equivalentFreeScope(win.location.pathname, base) && !win.location.search) {
    const image = doc.createElement('img');
    image.alt = ''; image.width = 64; image.height = 64; image.hidden = true; image.dataset.fiFreeMark = '';
    const removeIdentity = () => {image.hidden = true; delete context.dataset.fiFreeContext; delete free.dataset.fiFreeIdentity;};
    const applyIdentity = () => {
      if (controller.signal.aborted || !image.complete || !image.naturalWidth) return;
      image.hidden = false; context.dataset.fiFreeContext = ''; free.dataset.fiFreeIdentity = 'medallion-only'; schedule();
    };
    image.addEventListener('load', applyIdentity, options);image.addEventListener('error', removeIdentity, options);
    image.src = source.getAttribute('src'); action.append(image); applyIdentity();
    cleanups.push(() => {removeIdentity();image.remove();});
  }
  const template = seed.querySelector('template[data-fi-city-template]');
  doc.querySelectorAll('[data-listing-controls]').forEach((controls,index) => {
    const fieldset = controls.querySelector('[data-listing-city-filter]');
    if (!fieldset || !template || fieldset.querySelectorAll('[data-listing-city-input]').length < 2) return;
    const fragment = template.content.cloneNode(true);
    const toggle = fragment.querySelector('[data-fi-city-toggle]'), panel = fragment.querySelector('[data-fi-city-panel]');
    const close = fragment.querySelector('[data-fi-city-close]'), slot = fragment.querySelector('[data-fi-city-options]');
    const summary = toggle.querySelector('[data-fi-city-summary]');
    panel.id = `fi-cities-${index}`;panel.setAttribute('role','region');panel.setAttribute('aria-label','Выбор городов');
    toggle.setAttribute('aria-controls',panel.id);
    const marker = doc.createComment('Original city fieldset position');
    fieldset.before(marker);controls.insertBefore(fragment,fieldset);slot.append(fieldset);
    controls.dataset.fiCityRoot = '';
    const rail = controls.closest('[data-listing-discovery-rail]');if(rail)rail.dataset.fiCityRail = '';
    let compact = false, opened = false, natural = 0, placementMode = 'rail';
    const supportsPopover = typeof panel.showPopover === 'function';
    const nativeOpen = () => supportsPopover && panel.hasAttribute('popover') && panel.matches(':popover-open');
    const setAttr = (el,key,value) => {if(el.getAttribute(key)!==value)el.setAttribute(key,value);};
    const view = () => {const vv=win.visualViewport;return {left:vv?.offsetLeft||0,top:vv?.offsetTop||0,width:vv?.width||win.innerWidth,height:vv?.height||win.innerHeight};};
    const placement = () => {
      const v=view(), anchor=toggle.getBoundingClientRect(), rough=panelPlacement(anchor,v);
      return panelPlacement(anchor,v,panelBottomInset(v,rough,win.KenigEventsShellOccupiedSpace?.()?.rects));
    };
    const clearPosition = () => {for(const name of ['left','top','width','max-height'])panel.style.removeProperty(name);};
    const syncSummary = () => {
      const all=controls.querySelector('[data-listing-city-all]');
      const cities=[...fieldset.querySelectorAll('[data-listing-city-input]')].map(input=>({checked:input.checked,label:input.closest('label')?.querySelector('span')?.textContent?.trim()||input.value}));
      const label=citySummary(!all||all.checked,cities);
      if(summary.textContent!==label)summary.textContent=label;
      setAttr(toggle,'aria-label',`Выбор городов. ${label}`);
    };
    const hide = (restore=false) => {
      if(!opened)return;
      if(restore || panel.contains(doc.activeElement)) toggle.focus({preventScroll:true});
      opened=false;
      if(nativeOpen())panel.hidePopover();
      panel.hidden=true;setAttr(toggle,'aria-expanded','false');
      placementMode='closed';setAttr(controls,'data-fi-city-placement',placementMode);schedule();
    };
    const renderOpen = () => {
      const p=placement();
      const next=supportsPopover&&!p.inline?'popover':'inline';
      // Do not recreate, reparent or blur an actively edited/held control.
      if((pointerHeld||composing) && placementMode!==next && placementMode!=='closed')return;
      const focused=panel.contains(doc.activeElement)?doc.activeElement:null;
      if(next==='popover'){
        panel.setAttribute('popover','manual');toggle.popoverTargetElement=panel;panel.hidden=false;
        for(const [key,value] of Object.entries({left:p.left,top:p.top,width:p.width,maxHeight:p.maxHeight})) {
          if(panel.style[key]!==`${value}px`)panel.style[key]=`${value}px`;
        }
        if(!nativeOpen())panel.showPopover();
      }else{
        if(nativeOpen())panel.hidePopover();
        panel.removeAttribute('popover');toggle.popoverTargetElement=null;clearPosition();panel.hidden=false;
      }
      placementMode=next;setAttr(controls,'data-fi-city-placement',next);setAttr(toggle,'aria-expanded','true');
      if(focused&&doc.activeElement!==focused)focused.focus({preventScroll:true});
    };
    const update = () => {
      syncSummary();
      if(modalOpen){hide(false);return;}
      if(opened){renderOpen();return;}
      if(pointerHeld||composing||controls.contains(doc.activeElement))return;
      if(!compact){
        const labels=[...fieldset.querySelectorAll('label')];
        const gap=Number.parseFloat(win.getComputedStyle(fieldset).columnGap)||8;
        natural=labels.reduce((s,l)=>s+l.getBoundingClientRect().width,0)+Math.max(0,labels.length-1)*gap;
      }
      const available=controls.parentElement?.getBoundingClientRect().width||rail?.getBoundingClientRect().width||0;
      const next=cityNeedsCompact(available,natural,win.innerWidth,compact);
      if(next===compact&&controls.dataset.fiCityMode)return;
      compact=next;setAttr(controls,'data-fi-city-mode',compact?'compact':'inline');
      toggle.hidden=!compact;close.hidden=!compact;
      if(nativeOpen())panel.hidePopover();
      panel.removeAttribute('popover');toggle.popoverTargetElement=null;clearPosition();panel.hidden=compact;
      placementMode=compact?'closed':'rail';setAttr(controls,'data-fi-city-placement',placementMode);
      setAttr(toggle,'aria-expanded','false');
    };
    toggle.addEventListener('click',event=>{
      event.preventDefault();
      if(opened){hide(true);return;}
      if(modalOpen)return;
      opened=true;renderOpen();
    },options);
    close.addEventListener('click',()=>hide(true),options);
    // Browser/light dismissal must not replay a queued close from the previous presentation.
    panel.addEventListener('toggle',()=>{
      if(placementMode==='popover'&&opened&&!nativeOpen())hide(panel.contains(doc.activeElement));
    },options);
    controls.addEventListener('change',()=>win.queueMicrotask(()=>{syncSummary();schedule();}),options);
    controls.addEventListener('focusout',schedule,options);
    controls.addEventListener('keydown',event=>{
      if(event.key==='Escape'&&opened){event.stopPropagation();event.preventDefault();hide(true);}
    },options);
    doc.addEventListener('pointerdown',event=>{if(opened&&!controls.contains(event.target))hide(false);},options);
    doc.addEventListener('focusin',event=>{if(opened&&!controls.contains(event.target))hide(false);},options);
    tasks.push(update);update();
    const resize=typeof win.ResizeObserver==='function'?new win.ResizeObserver(schedule):null;
    resize?.observe(controls.parentElement);resize?.observe(fieldset);
    const changes=new win.MutationObserver(schedule);changes.observe(fieldset,{subtree:true,childList:true,characterData:true});
    cleanups.push(()=>{
      hide(false);resize?.disconnect();changes.disconnect();marker.replaceWith(fieldset);panel.remove();toggle.remove();
      delete controls.dataset.fiCityRoot;delete controls.dataset.fiCityMode;delete controls.dataset.fiCityPlacement;
      if(rail)delete rail.dataset.fiCityRail;
    });
  });
  doc.addEventListener('pointerdown',()=>{pointerHeld=true;},options);
  for(const name of ['pointerup','pointercancel'])doc.addEventListener(name,()=>{pointerHeld=false;schedule();},options);
  win.addEventListener('blur',()=>{pointerHeld=false;composing=false;schedule();},options);
  doc.addEventListener('compositionstart',()=>{composing=true;},options);
  doc.addEventListener('compositionend',()=>{composing=false;schedule();},options);
  win.addEventListener('kenigevents:lower-surface-state',event=>{modalOpen=Boolean(event.detail?.modalOpen);schedule();},options);
  win.addEventListener('storage',schedule,options);
  for(const name of ['resize','scroll']){
    win.addEventListener(name,schedule,{...options,passive:true});
    win.visualViewport?.addEventListener(name,schedule,{...options,passive:true});
  }
  doc.fonts?.ready.then(()=>{if(!controller.signal.aborted)schedule();});
  const api={destroy(){
    controller.abort();if(frame)win.cancelAnimationFrame(frame);frame=0;
    cleanups.reverse().forEach(fn=>fn());delete seed.__fiReview;
  }};
  seed.__fiReview=api;
  win.addEventListener('pagehide',event=>{if(!event.persisted)api.destroy();},options);
  win.addEventListener('pageshow',schedule,options);
  return api;
}
