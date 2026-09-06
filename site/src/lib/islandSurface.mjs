/** Adapt the two existing city owners; never replace their filtering/storage. */
export function citySurface(doc, mobile) {
 const rail=mobile ? doc.querySelector('[data-mobile-listing-rails]') : null;
 const picker=rail?.getBoundingClientRect().height>0?rail.querySelector('[data-mobile-v23-city-picker]'):null;
 const controls=picker || doc.querySelector('[data-listing-controls]');
 if(!controls || (!picker && !controls.querySelector('[data-listing-city-filter]')))return null;
 return {controls,panel:controls.querySelector('[data-island-city-panel]'),field:controls.querySelector('[data-listing-city-filter]'),toggle:controls.querySelector('[data-island-city-toggle]'),closeButton:controls.querySelector('[data-island-city-close]')};
}
export function cityEntries(field){
 return [...field.querySelectorAll('label,[data-mobile-v23-city]')].map(label=>{
  const original=label.querySelector('input');
  const input=original || {value:label.dataset.mobileV23City,get checked(){return label.getAttribute('aria-pressed')==='true'},set checked(_value){},dispatchEvent(){label.click()}};
  return {label,input,name:label.querySelector('span').textContent,countText:()=>label.querySelector('strong,b')?.textContent||''};
 });
}
export function visibleHeading(doc){return [...doc.querySelectorAll('main h1')].find(n=>n.getClientRects().length&&n.getBoundingClientRect().height>0);}
export function sectionHeadings(doc){
 return [...doc.querySelectorAll('main h2, main [data-mobile-listing-rails] .feed-head__copy > strong')].filter(n=>n.getClientRects().length&&n.getBoundingClientRect().height>0&&!n.closest('dialog,[role="dialog"],[data-keyboard-help],.ke-weekend-day__head'));
}
export function sectionRanges(doc,win){return sectionHeadings(doc).map(heading=>({heading,node:heading.parentElement,top:heading.getBoundingClientRect().top+win.scrollY,bottom:heading.parentElement.getBoundingClientRect().bottom+win.scrollY}));}
