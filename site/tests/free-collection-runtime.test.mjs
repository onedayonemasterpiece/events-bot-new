import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {runInNewContext, Script} from 'node:vm';
const source=readFileSync(new URL('../src/layouts/EventLayout.astro',import.meta.url),'utf8');
function fn(name,next,context){const start=source.indexOf(`function ${name}(`),end=source.indexOf(`function ${next}(`,start);assert.ok(start>0&&end>start);return runInNewContext(`(${source.slice(start,end).trim().replace(/async\s*$/u,'')})`,context);}
function asyncFn(name,next,context){const start=source.indexOf(`async function ${name}(`),end=source.indexOf(`function ${next}(`,start);assert.ok(start>0&&end>start);return runInNewContext(`(${source.slice(start,end).trim()})`,context);}

test('hard confirmed-free gate survives profile, rank, retry and fallback admission',()=>{
 const eligible=fn('isEligibleCandidate','scoreRelatedCandidate',{candidateId:c=>c.event_id,asId:Number,currentEventId:()=>0,isCancelledLike:()=>false,profileHasConsent:p=>Boolean(p),hiddenSet:()=>new Set(),negativeInterestPenalty:()=>0,NEGATIVE_HARD_FILTER_THRESHOLD:2});
 for(const profile of [null,{}]) for(const value of [false,null,undefined,'true',1]) assert.equal(eligible({event_id:1,is_free:value},{eligibility_filter:'confirmed-free'},profile),false);
 assert.equal(eligible({event_id:1,is_free:true},{eligibility_filter:'confirmed-free'},null),true);
 assert.equal(eligible({event_id:1,is_free:false},{},null),true,'ordinary paid routes not changed');
});

test('inline catalog shares discovery store, rejects unknown and retries a malformed payload',async()=>{
 const stores=new Map(),feed={dataset:{discoverySrc:'#free-collection-catalog',freeCollectionEligibility:'confirmed-free'}};
 let text='{invalid';let fetches=0;
 const ensure=asyncFn('ensureDiscoveryStore','updateFilteredFeedState',{discoveryStores:stores,document:{getElementById:()=>({type:'application/json',get textContent(){return text;}})},fetch:()=>{fetches++;throw new Error('not an endpoint');},normalizeManifest:x=>x});
 assert.equal((await ensure(feed)).manifest,null);
 text=JSON.stringify({related_static:[{event_id:1,is_free:true},{event_id:2,is_free:false},{event_id:3}],eligibility_filter:'untrusted'});
 const [a,b]=await Promise.all([ensure(feed),ensure(feed)]);
 assert.equal(a,b);assert.equal(a.manifest.eligibility_filter,'confirmed-free');assert.deepEqual(Array.from(a.manifest.related_static,c=>c.event_id),[1]);assert.equal(fetches,0);assert.equal(a.pending,null);
});

test('every loaded card still obeys free eligibility before profile bypass',()=>{
 const eligible=fn('cardEligibleAfterProfile','reorderExistingCards',{candidateById:(store,id)=>store.manifest.related_static.find(c=>c.event_id===Number(id)),asId:Number,sessionPinnedNotInterested:new Set(),hiddenSet:()=>new Set(),negativeInterestPenalty:()=>0,NEGATIVE_HARD_FILTER_THRESHOLD:2});
 const store={manifest:{eligibility_filter:'confirmed-free',related_static:[{event_id:1,is_free:true},{event_id:2,is_free:false}]}};
 assert.equal(eligible({dataset:{eventId:'1'}},store,null,null),true);
 assert.equal(eligible({dataset:{eventId:'2'}},store,null,null),false);
 assert.equal(eligible({dataset:{eventId:'3'}},store,{},3),false,'pinned feedback cannot override hard admission');
});

test('EventLayout inline controllers remain plain executable JavaScript',()=>{
 const scripts=[...source.matchAll(/<script\b(?=[^>]*\bis:inline\b)[^>]*>([\s\S]*?)<\/script>/gu)];
 for(const [,body] of scripts) new Script(body);
});
