import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { reviewEnabled, citySummary, equivalentFreeScope, panelPlacement, initFloatingIslandsReview } from '../src/lib/floatingIslandsReview.mjs';
const base = '/preview-islands-owner-20260905';
test('candidate only: explicit prefix, production denied', () => {
  assert.equal(reviewEnabled(base), true); assert.equal(reviewEnabled(`${base}/`), true);
  for (const path of ['', '/', '/preview-real-123', '/_review/foo', `${base}/child`, 'https://site/'+base]) assert.equal(reviewEnabled(path), false);
  assert.equal(reviewEnabled(base, true), false);
});
test('exact free scope only; subset labels cannot be replaced', () => {
  assert.equal(equivalentFreeScope(base+'/podborki/besplatnye-sobytiya/', base), true);
  for (const route of ['/podborki/besplatnye-sobytiya/deti/', '/podborki/besplatnye-sobytiya-na-more/', '/izbrannoe/']) assert.equal(equivalentFreeScope(base+route, base), false);
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
test('popover clamped within visual viewport',()=>assert.deepEqual(panelPlacement({left:330,bottom:100},{left:0,top:0,width:390,height:844},84),{left:12,top:108,width:366,maxHeight:640,inline:false}));
test('viewport offsets are respected once',()=>{
  const p=panelPlacement({left:500,bottom:120},{left:50,top:30,width:390,height:600},80);
  assert.equal(p.left,62);assert.equal(p.width,366);assert.equal(p.top,128);assert.equal(p.maxHeight,410);
});
test('small height reflows instead of clipping',()=>assert.equal(panelPlacement({left:0,bottom:170},{left:0,top:0,width:320,height:240},64).inline,true));
test('tiny width reflows',()=>assert.equal(panelPlacement({left:0,bottom:50},{left:0,top:0,width:200,height:600}).inline,true));
test('invalid geometry rejected',()=>{
  for(const height of [NaN,0,-1,Infinity])assert.throws(()=>panelPlacement({left:0,bottom:10},{left:0,top:0,width:390,height}),TypeError);
});
test('width sweep stays inside viewport',()=>{
  for(let width=240;width<=1920;width++){
    const p=panelPlacement({left:width-5,bottom:64},{left:0,top:0,width,height:900});
    assert.ok(p.left>=12 && p.left+p.width<=width-12);assert.ok(p.width<=420);
  }
});
test('no seed means no initialization',()=>assert.equal(initFloatingIslandsReview({querySelector:()=>null},{}),null));
test('reuse original fieldset without network or new storage',()=>{
  const s=readFileSync(new URL('../src/lib/floatingIslandsReview.mjs',import.meta.url),'utf8');
  assert.ok(s.includes('slot.append(fieldset)'));assert.ok(s.includes('marker.replaceWith(fieldset)'));
  assert.ok(!/fieldset\.cloneNode/.test(s));assert.ok(!/\bfetch\s*\(|localStorage\.|sessionStorage\.|sendBeacon\s*\(/.test(s));
});
test('brand mutation selectors absent',()=>{
  for(const f of ['../src/lib/floatingIslandsReview.mjs','../src/styles/floating-islands-review.css']){
    const s=readFileSync(new URL(f,import.meta.url),'utf8');
    assert.ok(!/site-header__brand-tag|mobile-discovery-menu__summary|data-reference4|AnnouncementsLockup|AnnouncementsWordmark/.test(s));
  }
});
test('rejected desktop dock cannot be re-enabled by this stylesheet',()=>{
  const css=readFileSync(new URL('../src/styles/floating-islands-review.css',import.meta.url),'utf8');
  assert.doesNotMatch(css,/\.mobile-bottom-nav|fi-nav-feature-size|data-fi-dock-clearance/);
});
test('semantic H1 is not removed',()=>{
  const js=readFileSync(new URL('../src/lib/floatingIslandsReview.mjs',import.meta.url),'utf8');
  assert.ok(!/querySelector\(['"]h1|h1\.remove|innerHTML\s*=/.test(js));
});
