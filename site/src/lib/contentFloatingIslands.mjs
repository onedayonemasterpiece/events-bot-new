import { visibleHeading, sectionRanges } from './islandSurface.mjs';
/** Pages without a city owner use the same shell, not a fabricated city filter. */
export function initContentFloatingIslands(doc=document,win=window){
 const band=doc.querySelector('[data-floating-top-band]'),title=visibleHeading(doc);if(!band||!title)return;
 const context=band.querySelector('[data-floating-page-context]'),section=band.querySelector('[data-floating-section-context]'),scope=band.querySelector('[data-floating-section-title]'),nav=doc.querySelector('.site-nav');
 const mobile=win.matchMedia('(max-width:759px)').matches,abort=new AbortController();
 const on=(n,t,f)=>n.addEventListener(t,f,{signal:abort.signal,passive:true});
 const home=context.querySelector('.site-header__context-home'),pageButton=context.querySelector('button'),label=context.querySelector('[data-floating-page-title]');
 const titleParent=title.parentNode,titleNext=title.nextSibling,titleStyle=title.getAttribute('style')||'';
 const placeholder=doc.createElement('div');
 const original={context:context.getAttribute('style'),nav:nav.getAttribute('style'),sectionParent:section.parentNode,sectionNext:section.nextSibling};
 const layer=doc.createElement('div');layer.className='fi-content-layer';doc.body.append(layer);layer.append(context);context.append(section);context.hidden=false;section.hidden=false;home.hidden=true;
 doc.body.dataset.fiContent='';if(mobile)doc.body.dataset.fiContentMobile='';else doc.body.dataset.fiDesktop='';
 label.textContent=title.textContent.trim();pageButton.hidden=!mobile;context.style.opacity='0';
 let frame=0,dead=false,headings=[],g,active;
 const skin=doc.createElement('div');skin.className='fi-content-skin';skin.setAttribute('aria-hidden','true');context.prepend(skin);
 if(mobile){title.before(placeholder);placeholder.style.height=`${title.getBoundingClientRect().height}px`;title.style.margin='0';context.prepend(title);pageButton.hidden=true;}
 const links=[...nav.querySelectorAll('[data-island-nav-link]')],more=doc.createElement('details');more.dataset.islandNavMore='';more.innerHTML='<summary aria-label="Остальные разделы">☰</summary><div data-island-nav-panel></div>';nav.append(more);const panel=more.lastElementChild;
 function measure(){
  if(mobile){titleParent.insertBefore(title,placeholder);placeholder.style.display='none';title.style.cssText=titleStyle;}
  headings=sectionRanges(doc,win);const tr=title.getBoundingClientRect(),brand=doc.querySelector(mobile?'[data-mobile-discovery-menu]>summary':'.site-header__brand-tag').getBoundingClientRect();
  const left=brand.right+12,width=mobile?win.innerWidth-left-12:Math.min(370,Math.max(180,win.innerWidth*.23));
  g={left,width,top:mobile?8:20,end:Math.max(40,title.getBoundingClientRect().bottom+win.scrollY-(mobile?90:100))};
  context.style.cssText=`left:${left}px;top:${g.top}px;width:${width}px;min-height:70px;`;
  if(mobile){
   const typography=win.getComputedStyle(title),font=parseFloat(typography.fontSize),titleFont=typography.font,titleColor=typography.color,spacing=typography.letterSpacing,scale=Math.min(1,18/font,(width-24)/tr.width),height=Math.max(70,tr.height*scale+46);
   placeholder.style.cssText=`height:${tr.height}px;`;context.prepend(title);
   g.origin={x:tr.x-left-12,y:tr.y+win.scrollY-g.top-12,scale,color:titleColor};
   context.style.minHeight=`${height}px`;context.style.setProperty('--fi-content-end',`${g.end}px`);
   context.style.setProperty('--fi-content-x',`${g.origin.x}px`);context.style.setProperty('--fi-content-y',`${g.origin.y}px`);context.style.setProperty('--fi-content-scale',String(scale));context.style.setProperty('--fi-content-original-color',titleColor);
   title.style.cssText=titleStyle+`;position:relative;margin:0;width:${tr.width}px;max-width:none;transform-origin:0 0;font:${titleFont};letter-spacing:${spacing};color:${titleColor};`;
   section.style.marginTop=`${tr.height*scale-tr.height+8}px`;
  }
  for(const a of links)nav.insertBefore(a,more);more.hidden=true;nav.style.cssText=original.nav||'';
  if(!mobile){
   nav.style.maxWidth=`${Math.max(110,win.innerWidth-left-width-40)}px`;nav.style.flexWrap='nowrap';
   const candidates=links.filter(a=>a.getAttribute('aria-current')!=='page').reverse();
   while(nav.scrollWidth>nav.clientWidth+1&&candidates.length){more.hidden=false;panel.prepend(candidates.shift());}
  }
  render();
 }
 function render(){frame=0;if(dead||!g)return;const y=win.scrollY,t=Math.max(0,Math.min(1,y/g.end)),p=t*t*(3-2*t);
  active=headings[0];for(const h of headings)if(h.top<=y+g.top+100)active=h;
  const text=active?.heading.textContent.trim()||'';if(scope.textContent!==text)scope.textContent=text;
  // Desktop never repeats the current page menu label. Mobile surface reveals
  // continuously as the original semantic title leaves the document flow.
  const alpha=mobile?(win.matchMedia('(prefers-reduced-motion: reduce)').matches?1:p):(active?Math.max(0,Math.min(1,(y+150-active.top)/100)):0);
  context.style.opacity=mobile?'1':String(alpha);skin.style.opacity=String(alpha);context.style.transform=mobile?'none':`translateY(${(1-p)*12}px)`;
  if(mobile&&!win.CSS.supports('animation-timeline','scroll(root block)')){const o=g.origin;title.style.transform=`translate(${o.x*(1-p)}px,${o.y*(1-p)}px) scale(${1+(o.scale-1)*p})`;title.style.color=`color-mix(in srgb, ${o.color} ${(1-p)*100}%, #241d17)`;}
  context.style.pointerEvents=alpha>.95?'auto':'none';context.setAttribute('aria-hidden',String(!mobile&&alpha===0));
  pageButton.tabIndex=mobile&&alpha>.95?0:-1;section.tabIndex=alpha>.95&&text?0:-1;section.hidden=!text||(mobile&&t<1);
 }
 const schedule=()=>{if(!frame&&!dead)frame=win.requestAnimationFrame(render)};
 on(doc,'keydown',e=>{if(e.key==='Escape')more.open=false;});on(doc,'pointerdown',e=>{if(!more.contains(e.target))more.open=false;});
 on(win,'scroll',schedule);on(win,'resize',measure);on(pageButton,'click',()=>win.scrollTo({top:0,behavior:win.matchMedia('(prefers-reduced-motion: reduce)').matches?'instant':'smooth'}));
 on(section,'click',()=>active&&win.scrollTo({top:active.heading.getBoundingClientRect().top+win.scrollY-110,behavior:'smooth'}));
 band.__islands={destroy(){dead=true;abort.abort();win.cancelAnimationFrame(frame);for(const a of links)nav.insertBefore(a,more);more.remove();nav.setAttribute('style',original.nav||'');band.prepend(context);original.sectionParent.insertBefore(section,original.sectionNext);context.setAttribute('style',original.context||'');context.hidden=true;if(mobile){titleParent.insertBefore(title,titleNext);title.style.cssText=titleStyle;placeholder.remove();}skin.remove();section.style.marginTop='';layer.remove();delete doc.body.dataset.fiContent;delete doc.body.dataset.fiContentMobile;delete doc.body.dataset.fiDesktop;delete band.__islands;}};
 doc.fonts.ready.then(()=>{if(dead)return;measure();doc.body.dataset.fiMotion='ready'});return band.__islands;
}
