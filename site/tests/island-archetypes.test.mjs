import test from 'node:test';import assert from 'node:assert/strict';import {readFileSync} from 'node:fs';
import {fitCityItems} from '../src/lib/desktopFloatingIslands.mjs';
import {cityEntries} from '../src/lib/islandSurface.mjs';
import {userIslandRoute} from '../src/lib/shellFloatingIslands.mjs';
import {specimens} from '../scripts/write-island-archetype-index.mjs';
test('fit retains maximal whole items with disclosure budget',()=>{const widths=[50,115,96,88],more=[0,40,40,40,40];for(let available=65;available<480;available++){const n=fitCityItems(available,widths,more);assert.ok(n>=0&&n<=4);if(n<4){const used=16+widths.slice(0,n+1).reduce((a,b)=>a+b,0)+6*n+(n+1<4?6+more[4-n-1]:0);assert.ok(used>available,`maximal fit ${available}/${n}`)}}});
test('mobile adapter delegates to original filter, does not create inputs',()=>{let clicks=0;const label={dataset:{mobileV23City:'city'},querySelector:s=>s==='input'?null:{textContent:s==='span'?'Город':'3'},getAttribute:()=> 'true',click:()=>clicks++};const [entry]=cityEntries({querySelectorAll:()=>[label]});assert.equal(entry.name,'Город');assert.equal(entry.input.checked,true);assert.equal(entry.countText(),'3');entry.input.dispatchEvent();assert.equal(clicks,1)});
test('real archetypes share shell, lab and preview stay excluded',()=>{assert.equal(specimens.length,7);for(const [route]of specimens)assert.equal(userIslandRoute(route),true);for(const route of ['/lab/test','/__preview/','/api/test'])assert.equal(userIslandRoute(route),false)});
test('accepted city motion remains time eased without currentTime seeking',()=>{const s=readFileSync(new URL('../src/lib/mobileFloatingIslands.mjs',import.meta.url),'utf8');assert.match(s,/duration:540/);assert.match(s,/cubic-bezier\(\.25,\.1,\.25,1\)/);assert.doesNotMatch(s,/\.currentTime\s*=/)});
test('content controller guards pre-font scroll and keeps real mobile H1 accessible',()=>{const s=readFileSync(new URL('../src/lib/contentFloatingIslands.mjs',import.meta.url),'utf8');assert.match(s,/if\(dead\|\|!g\)return/);assert.match(s,/String\(!mobile&&alpha===0\)/)});

test('single-day cleanup is scoped and does not add a sticky date or invent a city owner',()=>{
 const mobile=readFileSync(new URL('../src/lib/mobileFloatingIslands.mjs',import.meta.url),'utf8');
 const shell=readFileSync(new URL('../src/lib/shellFloatingIslands.mjs',import.meta.url),'utf8');
 const css=readFileSync(new URL('../src/styles/mobile-floating-islands.css',import.meta.url),'utf8');
 assert.match(mobile,/singleDay=\['today','tomorrow','date'\]/);
 assert.match(mobile,/arrived=!singleDay/);
 assert.match(mobile,/docked&&!singleDay\?0:fitCityItems/);
 assert.match(shell,/!media.matches&&!surface/);
 assert.match(css,/\[data-fi-single-day\].*?page-head h1 \{[^}]*animation:none/);
});
