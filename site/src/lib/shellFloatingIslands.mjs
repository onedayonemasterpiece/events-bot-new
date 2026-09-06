import { citySurface } from './islandSurface.mjs';
import { initContentFloatingIslands } from './contentFloatingIslands.mjs';
import { initMobileFloatingIslands } from './mobileFloatingIslands.mjs';
import { initDesktopFloatingIslands } from './desktopFloatingIslands.mjs';
/** Isolated Popular review: one geometry owner; measured once, transform-only title. */
export const FLOATING_ISLANDS_VERSION='3.0.0-review-motion';
export const userIslandRoute=path=>!/^\/(?:lab|__preview|admin|api)(?:\/|$)/u.test(path);
export const clamp=n=>Math.max(0,Math.min(1,n));
export const smooth=n=>{const t=clamp(n);return t*t*(3-2*t)};
export const easeIn=n=>clamp(n)**3;
export const easeOut=n=>1-(1-clamp(n))**3;
export const mix=(a,b,p)=>a+(b-a)*p;
export function initShellFloatingIslands(doc=document,win=window){
 const band=doc.querySelector('[data-floating-top-band]');if(!band||band.__islandResponsive)return band?.__islands;
 const media=win.matchMedia('(min-width:760px)');
 const mount=()=>{
  band.__islands?.destroy();delete doc.body.dataset.fiMotion;
  const surface=citySurface(doc,!media.matches),rail=doc.querySelector('[data-mobile-listing-rails]');
  // A one-city/empty day has no city filter to dock and no reason to invent one.
  // Keep its real heading in flow instead of handing it to the content morph.
  if(!media.matches&&!surface&&rail?.getBoundingClientRect().height>0&&['today','tomorrow','date'].includes(rail.dataset.mobileV23Page)){
   const nodes=[...band.querySelectorAll('[data-floating-page-context],[data-floating-section-context]')],hidden=nodes.map(n=>n.hidden);nodes.forEach(n=>n.hidden=true);
   doc.body.dataset.fiMobile='';doc.body.dataset.fiSingleDay='';doc.body.dataset.fiMotion='ready';
   band.__islands={destroy(){nodes.forEach((n,i)=>n.hidden=hidden[i]);delete doc.body.dataset.fiMobile;delete doc.body.dataset.fiSingleDay;delete band.__islands;}};
   return band.__islands;
  }
  return !surface?initContentFloatingIslands(doc,win):media.matches?initDesktopFloatingIslands(doc,win):initMobileFloatingIslands(doc,win);
 };
 media.addEventListener('change',mount);band.__islandResponsive=true;return mount();
}
