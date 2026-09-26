import test from 'node:test';import assert from 'node:assert/strict';
import {createEventCatalogIndex} from '../src/lib/eventCatalogIndex.mjs';
test('retained details never enter current listings and lookups preserve merge precedence',()=>{
 const current=[{id:2,slug:'next',start_date:'2026-09-27'},{id:1,slug:'today',start_date:'2026-09-26'}];
 const retained=[{id:0,slug:'past',start_date:'2026-09-25'},{...current[1],title:'retained overlap'}];
 const index=createEventCatalogIndex(current,retained);
 assert.deepEqual(index.current().map(e=>e.id),[1,2]);assert.deepEqual(index.details().map(e=>e.id),[0,1,2]);
 assert.equal(index.byId(1),retained[1]);assert.equal(index.bySlug('today'),retained[1]);assert.equal(index.byId(9),undefined);
 const list=index.current();list.pop();list.reverse();assert.deepEqual(index.current().map(e=>e.id),[1,2]);
 assert.deepEqual(current.map(e=>e.id),[2,1]);
});
