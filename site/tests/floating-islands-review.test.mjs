import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { reviewEnabled, citySummary, equivalentFreeScope, panelPlacement, initFloatingIslandsReview } from '../src/lib/floatingIslandsReview.mjs';
const base = '/preview-islands-owner-20260905';
test('candidate only: explicit prefix, production denied', () => {
  assert.equal(reviewEnabled(base), true);
  assert.equal(reviewEnabled(`${base}/`), true);
  for (const path of ['', '/', '/preview-real-123', '/_review/foo', `${base}/child`, 'https://site/'+base]) assert.equal(reviewEnabled(path), false);
  assert.equal(reviewEnabled(base, true), false);
});
test('exact free scope only; subset labels cannot be replaced', () => {
  assert.equal(equivalentFreeScope(base+'/podborki/besplatnye-sobytiya/', base), true);
  for (const route of ['/podborki/besplatnye-sobytiya/deti/', '/podborki/besplatnye-sobytiya-na-more/', '/izbrannoe/'])
    assert.equal(equivalentFreeScope(base+route, base), false);
  assert.equal(equivalentFreeScope('/podborki/besplatnye-sobytiya/',''), false);
});
test('city summary: all, none, single, multiple', () => {
  const a={label:'Калининград',checked:true},b={label:'Светлогорск',checked:false};
  assert.equal(citySummary(true,[a,b]),'Все города');
  assert.equal(citySummary(false,[{...a,checked:false},b]),'Города не выбраны');
  assert.equal(citySummary(false,[a,b]),'Калининград');
  assert.equal(citySummary(false,[a,{...b,checked:true}]),'Города · 2');
});
test('long city label is not truncated',()=>assert.equal(citySummary(false,[{label:'Посёлок имени Александра Космодемьянского',checked:true}]),'Посёлок имени Александра Космодемьянского'));
test('popover clamped within visual viewport',()=>{
  const p=panelPlacement({left:330,bottom:100},{left:0,top:0,width:390,height:844},84);
  assert.deepEqual(p,{left:12,top:108,width:366,maxHeight:640,inline:false});
});
test('viewport offsets are respected once',()=>{
  const p=panelPlacement({left:500,bottom:120},{left:0+50,top:30,width:390,height:600},80);
  assert.equal(p.left,62);assert.equal(p.width,366);assert.equal(p.top,128);assert.equal(p.maxHeight,410);
});
test('small height reflows instead of clipping essential options',()=>assert.equal(panelPlacement({left:0,bottom:170},{left:0,top:0,width:320,height:240},64).inline,true));
test('tiny width reflows',()=>assert.equal(panelPlacement({left:0,bottom:50},{left:0,top:0,width:200,height:600}).inline,true));
test('invalid geometry rejected',()=>{
  for(const height of [NaN,0,-1,Infinity])assert.throws(()=>panelPlacement({left:0,bottom:10},{left:0,top:0,width:390,height}),TypeError);
});
test('width sweep is inside viewport and never exceeds panel cap',()=>{
  for(let width=240;width<=1920;width++){
    const p=panelPlacement({left:width-5,bottom:64},{left:0,top:0,width,height:900});
    assert.ok(p.left>=12 && p.left+p.width<=width-12);assert.ok(p.width<=420);
  }
});
test('no seed means no initialization or optional side effects',()=>assert.equal(initFloatingIslandsReview({querySelector:()=>null},{}),null));
test('runtime changes the original fieldset, not copied choices',()=>{
  const s=readFileSync(new URL('../src/lib/floatingIslandsReview.mjs',import.meta.url),'utf8');
  assert.ok(s.includes('slot.append(fieldset)'));assert.ok(s.includes('marker.replaceWith(fieldset)'));
  assert.ok(!/fieldset\.cloneNode/.test(s));
  assert.ok(!/\bfetch\s*\(|localStorage\.|sessionStorage\.|sendBeacon\s*\(/.test(s));
});
test('brand menu selectors absent from the new runtime and styles',()=>{
  const files=['../src/lib/floatingIslandsReview.mjs','../src/styles/floating-islands-review.css'];
  for(const f of files){const s=readFileSync(new URL(f,import.meta.url),'utf8');
    assert.ok(!/site-header__brand-tag|mobile-discovery-menu__summary|data-reference4|AnnouncementsLockup|AnnouncementsWordmark/.test(s));
  }
});
test('desktop skin is responsive and does not override suppression/layers',()=>{
  const css=readFileSync(new URL('../src/styles/floating-islands-review.css',import.meta.url),'utf8');
  const desktop=css.split('@media(min-width:1024px)')[1].split('@media(max-width:360px)')[0];
  assert.ok(desktop.includes('var(--fi-nav-feature-size)'));
  assert.ok(!/z-index|position:fixed|opacity:1|visibility:visible/.test(desktop));
  assert.ok(desktop.includes('background:var(--ke-color-background-surface-strong)'));
});
test('free title H1 is not selected for removal',()=>{
  const js=readFileSync(new URL('../src/lib/floatingIslandsReview.mjs',import.meta.url),'utf8');
  assert.ok(!/querySelector\(['"]h1|h1\.remove|innerHTML\s*=/.test(js));
});
