import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const read=name=>readFileSync(new URL('../src/styles/'+name,import.meta.url),'utf8');
test('one lower elevation owns standalone, date, Buy and pre-JS Weekend shells',()=>{
 const css=read('floating-islands.css');assert.equal((css.match(/--ke-island-lower-shadow:/g)||[]).length,1);
 assert.match(css,/body \.mobile-bottom-nav \{[^}]*box-shadow:var\(--ke-island-lower-shadow\)/);
 for(const name of ['mobile-date-dock.css','mobile-event-dock.css','weekend-first-paint.css'])assert.match(read(name),/box-shadow:var\(--ke-island-lower-shadow\)/);
 for(const name of ['mobile-date-dock.css','mobile-event-dock.css'])assert.match(read(name),/\.mobile-bottom-nav \{[^}]*box-shadow:none/);
 assert.doesNotMatch(read('mobile-floating-islands.css'),/--ke-island-lower-shadow/);
});
