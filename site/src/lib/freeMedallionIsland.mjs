/** Free has one identity: the existing medallion, not a second title island. */
export function initFreeMedallionIsland(doc=document,win=window){
 const band=doc.querySelector('[data-floating-top-band]'),root=doc.querySelector('[data-free-collection-surface]');
 const badge=root?.querySelector('[data-free-collection-medallion]'),copy=root?.querySelector('.free-collection__copy');if(!band||!badge||!copy)return;
 const nodes=[...band.querySelectorAll('[data-floating-page-context],[data-floating-section-context]')],hidden=nodes.map(n=>n.hidden);nodes.forEach(n=>n.hidden=true);
 const original=root.getAttribute('style'),badgeStyle=badge.getAttribute('style'),abort=new AbortController(),on=(n,t,f)=>n.addEventListener(t,f,{signal:abort.signal,passive:true});
 const mobile=win.matchMedia('(max-width:759px)').matches,reduced=win.matchMedia('(prefers-reduced-motion: reduce)'),native=win.CSS?.supports('animation-timeline','scroll(root block)');
 doc.body.dataset.fiFree='';if(!mobile)doc.body.dataset.fiDesktop='';
 let start=0,end=1,frame=0,dead=false;
 function render(){
  frame=0;if(dead)return;const t=Math.max(0,Math.min(1,(win.scrollY-start)/(end-start))),p=t*t*(3-2*t);
  if(!native||reduced.matches)badge.style.transform=`scale(${1-.16*(reduced.matches?(t===1?1:0):p)})`;else badge.style.removeProperty('transform');
  badge.dataset.fiFreeCompact=String(t===1);
 }
 function measure(){
  if(dead)return;const nav=doc.querySelector('.site-nav'),top=mobile?20:Math.max(88,(nav?.getBoundingClientRect().bottom||76)+12);
  root.style.setProperty('--fi-free-top',`${top}px`);end=Math.max(1,copy.getBoundingClientRect().top+win.scrollY-top);start=Math.max(0,end-112);
  root.style.setProperty('--fi-free-start',`${start}px`);root.style.setProperty('--fi-free-end',`${end}px`);render();doc.body.dataset.fiMotion='ready';
 }
 function schedule(){if(!frame&&!dead)frame=win.requestAnimationFrame(render);}
 on(win,'scroll',schedule);on(win,'resize',measure);on(win,'pageshow',measure);on(reduced,'change',measure);measure();doc.fonts.ready.then(measure);
 band.__islands={destroy(){dead=true;abort.abort();win.cancelAnimationFrame(frame);nodes.forEach((n,i)=>n.hidden=hidden[i]);if(original===null)root.removeAttribute('style');else root.setAttribute('style',original);if(badgeStyle===null)badge.removeAttribute('style');else badge.setAttribute('style',badgeStyle);delete badge.dataset.fiFreeCompact;delete doc.body.dataset.fiFree;if(!mobile)delete doc.body.dataset.fiDesktop;delete band.__islands;}};return band.__islands;
}
