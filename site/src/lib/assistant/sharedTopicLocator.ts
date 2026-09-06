import type {AssistantSurfaceAdapter,AssistantSurfaceState} from './assistantSurface.ts';

/** Shared header geometry only: no sticky node, H2 clone or search state owner. */
export function topicLocatorGeometry({header,brand,nav,inner}: {header:DOMRect;brand:DOMRect;nav:DOMRect|null;inner:DOMRect}) {
  const left=Math.max(inner.left+12,brand.right+12);
  const right=Math.min(inner.right-12,nav&&nav.width>0?nav.left-12:inner.right-12);
  return {left:left-inner.left,width:Math.max(0,right-left),top:Math.max(8,Math.min(16,(brand.height-56)/2)),edge:Math.max(header.bottom,brand.bottom)};
}

export function bindSharedTopicLocator(locator:HTMLElement,win:Window=window) {
  const doc=locator.ownerDocument,header=locator.closest<HTMLElement>('.site-header'),inner=locator.parentElement;
  if(!header||!inner)return()=>{};
  const owner=win as Window&{KenigEventsSearchAdapterV1?:AssistantSurfaceAdapter};
  let state:Partial<AssistantSurfaceState>={},frame=0,edge=-1;
  const navToggle=header.querySelector<HTMLButtonElement>('[data-search-nav-toggle]');
  const mainNav=header.querySelector<HTMLElement>('.site-nav');if(mainNav&&navToggle)mainNav.id='search-shared-site-nav';
  const menu=header.querySelector<HTMLDetailsElement>('[data-reference4-fullscreen]');
  const visible=(node:Element|null)=>Boolean(node&&node.getBoundingClientRect().width>0&&win.getComputedStyle(node).display!=='none');
  const render=()=>{
    frame=0;
    if(menu?.open){locator.hidden=true;return;}
    const adapter=owner.KenigEventsSearchAdapterV1;
    if(!adapter){locator.hidden=true;return;}
    const brand=Array.from(header.querySelectorAll<HTMLElement>('.site-header__brand-tag, [data-reference4-fullscreen] > summary')).find(visible);
    if(!brand){locator.hidden=true;return;}
    const id=state.viewedSectionId,title=typeof state.viewedTitle==='string'?state.viewedTitle.trim():'';
    const section=id?Array.from(doc.querySelectorAll<HTMLElement>('[data-assistant-section]')).find(n=>n.dataset.assistantSection===id):null;
    const candidateEdge=Math.max(header.getBoundingClientRect().bottom,brand.getBoundingClientRect().bottom);
    const compact=Boolean(doc.querySelector('[data-assistant-clean="true"]')&&win.innerWidth>=760&&title&&section&&section.getBoundingClientRect().top<=candidateEdge+1);
    header.dataset.searchTopicCompact=String(compact);if(navToggle)navToggle.hidden=!compact;
    if(!compact){delete header.dataset.searchNavOpen;navToggle?.setAttribute('aria-expanded','false');}
    const nav=compact?navToggle:mainNav;
    const geometry=topicLocatorGeometry({header:header.getBoundingClientRect(),inner:inner.getBoundingClientRect(),brand:brand.getBoundingClientRect(),nav:visible(nav)?nav!.getBoundingClientRect():null});
    if(edge!==geometry.edge){
      edge=geometry.edge;
      doc.documentElement.style.setProperty('--ke-assistant-locator-edge',`${edge}px`);
      win.dispatchEvent(new CustomEvent('kenigevents:search-locator-geometry',{detail:{edge}}));
    }
    const active=Boolean(title&&section?.matches('[data-assistant-section]')&&section.getBoundingClientRect().top<=edge+1&&geometry.width>=120);
    locator.hidden=!active;
    if(!active)return;
    // Use original LLM title verbatim; never derive it from the user's question.
    if(locator.textContent!==title)locator.textContent=title;
    locator.title=title;
    locator.dataset.viewedSectionId=id!;
    locator.style.left=`${geometry.left}px`;
    locator.style.top=`${geometry.top}px`;
    locator.style.width=`${Math.min(geometry.width,520)}px`;
  };
  const schedule=()=>{if(!frame)frame=win.requestAnimationFrame(render);};
  navToggle?.addEventListener('click',()=>{const open=header.dataset.searchNavOpen!=='true';header.dataset.searchNavOpen=String(open);navToggle.setAttribute('aria-expanded',String(open));});
  header.addEventListener('keydown',event=>{if(event.key==='Escape'&&header.dataset.searchNavOpen==='true'){delete header.dataset.searchNavOpen;navToggle?.setAttribute('aria-expanded','false');navToggle?.focus();}});
  doc.addEventListener('click',event=>{if(event.target instanceof Node&&!mainNav?.contains(event.target)&&!navToggle?.contains(event.target)){delete header.dataset.searchNavOpen;navToggle?.setAttribute('aria-expanded','false');}});
  const ready=()=>{state=owner.KenigEventsSearchAdapterV1?.getState()||{};schedule();};
  const context=(event:Event)=>{state=(event as CustomEvent<AssistantSurfaceState>).detail||{};schedule();};
  win.addEventListener('kenigevents:search-adapter-ready',ready);
  win.addEventListener('kenigevents:search-context-changed',context);
  win.addEventListener('scroll',schedule,{passive:true});win.addEventListener('resize',schedule,{passive:true});
  const resize=new ResizeObserver(schedule);resize.observe(inner);
  header.querySelectorAll<HTMLElement>('.site-header__brand-tag, .site-nav, [data-reference4-fullscreen] > summary').forEach(n=>resize.observe(n));
  const mutation=new MutationObserver(schedule);if(menu)mutation.observe(menu,{attributes:true,attributeFilter:['open']});
  ready();
  return()=>{resize.disconnect();mutation.disconnect();if(frame)win.cancelAnimationFrame(frame);win.removeEventListener('kenigevents:search-adapter-ready',ready);win.removeEventListener('kenigevents:search-context-changed',context);win.removeEventListener('scroll',schedule);win.removeEventListener('resize',schedule);locator.hidden=true;};
}
