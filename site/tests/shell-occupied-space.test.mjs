import test from 'node:test';import assert from 'node:assert/strict';
import {shellOccupiedSpace} from '../src/lib/shellOccupiedSpace.mjs';
const viewport={x:0,y:0,width:390,height:844};
test('separate islands retain actual occupied rectangles, no bounding-box union',()=>{
 const r=shellOccupiedSpace(viewport,[{id:'brand',rect:{x:12,y:0,width:100,height:84}},{id:'context',rect:{x:142,y:8,width:236,height:44}}]);
 assert.equal(r.rects.length,2);assert.equal(r.rects[0].width,100);assert.equal(r.rects[1].x,142);assert.ok(Object.isFrozen(r.rects));
});
test('visual viewport clips keyboard/offscreen, duplicate updates are stable, invalid input ignored',()=>{
 const r=shellOccupiedSpace({...viewport,height:400},[{id:'nav',rect:{x:12,y:370,width:366,height:66}},{id:'nav',rect:{x:12,y:378,width:366,height:66}},{id:'bad',rect:{x:NaN,y:0,width:1,height:1}}]);
 assert.equal(r.rects.length,1);assert.equal(r.rects[0].height,22);
 assert.deepEqual(shellOccupiedSpace({width:NaN,height:0},[]).rects,[]);
});
