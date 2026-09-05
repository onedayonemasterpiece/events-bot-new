import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {panelBottomInset,cityNeedsCompact,panelPlacement} from '../src/lib/floatingIslandsReview.mjs';
const view={left:0,top:0,width:1280,height:800},panel={left:20,width:420};
test('disjoint bottom island does not consume city-panel height',()=>assert.equal(panelBottomInset(view,panel,[{role:'navigation',x:700,y:700,width:400,height:80}]),0));
test('overlapping obstacles contribute maximum not summed heights',()=>assert.equal(panelBottomInset(view,panel,[{role:'navigation',x:30,y:700,width:300,height:80},{role:'notification',x:30,y:660,width:300,height:50}]),140));
test('unrelated malformed or offscreen rectangles are ignored',()=>assert.equal(panelBottomInset(view,panel,[{role:'brand',x:20,y:0,width:200,height:80},{role:'navigation',x:20,y:810,width:400,height:80},{role:'navigation',x:NaN,y:700,width:300,height:80}]),0));
test('partial horizontal intersection constrains the panel',()=>assert.equal(panelBottomInset(view,panel,[{role:'date',x:439,y:680,width:300,height:90}]),120));
test('compaction has hysteresis without permanent fallback latch',()=>{
  assert.equal(cityNeedsCompact(510,500,1280,false),false);
  assert.equal(cityNeedsCompact(510,500,1280,true),true);
  assert.equal(cityNeedsCompact(530,500,1280,true),false);
  assert.equal(cityNeedsCompact(1000,500,390,false),true);
});
test('small height and restored height produce different safe presentation',()=>{
  const anchor={left:12,bottom:100};
  assert.equal(panelPlacement(anchor,{left:0,top:0,width:390,height:220},80).inline,true);
  assert.equal(panelPlacement(anchor,{left:0,top:0,width:390,height:844},80).inline,false);
});
test('read-only bridge, lifecycle cleanup and asset fallback are present',()=>{
  const s=readFileSync(new URL('../src/lib/floatingIslandsReview.mjs',import.meta.url),'utf8');
  assert.doesNotMatch(s,/forceInline|\bfetch\s*\(|localStorage\.|sessionStorage\./);
  for(const required of ['panelBottomInset','pointercancel','compositionend','resize?.disconnect()','changes.disconnect()','image.naturalWidth',"image.addEventListener('error'",'marker.replaceWith(fieldset)'])assert.ok(s.includes(required),required);
});
