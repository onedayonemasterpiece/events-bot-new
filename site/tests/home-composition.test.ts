import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import {stripTypeScriptTypes} from 'node:module';
import { shellCompositionForRoute, lowerNavigationState } from '../src/lib/shellComposition.ts';
import { genericHeroTalkMessage, resolveHeroTalkPlacement, type HeroTalkPlacementContext } from '../src/lib/heroTalkPlacement.ts';
import { buildHomeHeroTalkDeck } from '../src/lib/homeHeroTalk.ts';
import type { PreviewEvent } from '../src/lib/types';
const source=(path:string)=>readFileSync(new URL(path,import.meta.url),'utf8');
const home:HeroTalkPlacementContext={route:'home',placement:'page_end',readyCapabilities:['search']};

test('home alone selects lower-only participants before mount; other archetypes unchanged',()=>{
 for(const path of ['/',''])assert.deepEqual(shellCompositionForRoute(path),{version:'shell-composition-v1',id:'home-navigation-only',topParticipants:false,globalNavigation:true,brandInFlow:false,lowerNavigation:'afisha'});
 for(const path of ['/poisk','/segodnya','/podborki/besplatnye-sobytiya','/vyhodnye','/sobytiya/event','/populyarnoe']) {
  const policy=shellCompositionForRoute(path);assert.equal(policy.id,'contextual');assert.equal(policy.topParticipants,true);assert.equal(policy.brandInFlow,false);
 }
});
test('home does not even query titles or register upper floating listeners',()=>{
 const layout=source('../src/layouts/EventLayout.astro');
 const start=layout.indexOf("if (document.body.dataset.shellComposition !== 'home-navigation-only')");
 const end=layout.indexOf('// AR-17',start);
 const unexpected=()=>{throw Error('upper runtime executed on lower-only home');};
 const state={document:{body:{dataset:{shellComposition:'home-navigation-only'}},querySelector:unexpected,addEventListener:unexpected},window:{addEventListener:unexpected},matchMedia:unexpected};
 vm.runInNewContext(stripTypeScriptTypes(layout.slice(start,end)),state);
 assert.ok(start>=0&&end>start);assert.match(layout.slice(start,end),/window\.addEventListener\('scroll',scheduleContext/);
 assert.match(layout, /shellComposition\.globalNavigation && <Reference4MobileMenu/);
 assert.match(layout, /shellComposition\.globalNavigation && <nav class="site-nav"/);
});
test('shared lower navigation respects modal and real keyboard occupancy, not ordinary focus alone',()=>{
 assert.equal(lowerNavigationState(false,0,true),'ready');assert.equal(lowerNavigationState(false,60,true),'ready');
 assert.equal(lowerNavigationState(false,300,true),'keyboard');assert.equal(lowerNavigationState(false,300,false),'ready');assert.equal(lowerNavigationState(true,300,true),'modal');
});
test('page-end is semantic service continuation and obeys readiness/hide/suppression/completed and upper duplication',()=>{
 const message=resolveHeroTalkPlacement(home)!;assert.equal(message.action.path,'/poisk/');assert.equal(message.contentSource,'service');assert.equal(message.fragments[0].text,'Подобрать точнее?');
 for(const patch of [{readyCapabilities:[]},{suppressed:true},{hiddenMessageIds:[message.id]},{suppressedCapabilities:['search']},{completedActionIds:[message.action.id]},{upperSceneIds:[message.id]}])assert.equal(resolveHeroTalkPlacement({...home,...patch} as HeroTalkPlacementContext),null);
 assert.equal(resolveHeroTalkPlacement({...home,upperSceneIds:['unrelated-upper-editorial']} )?.id,message.id);
 assert.equal(resolveHeroTalkPlacement({...home,route:'event'}),null);
 assert.equal(resolveHeroTalkPlacement(home,{...message,action:{...message.action,path:'//external.invalid'}}),null);
 assert.equal(resolveHeroTalkPlacement(home,{...message,contentSource:'campaign'} as any),null,'service fallback never activates a campaign');
});
test('empty/stale Hero deck has useful generic scene while current photo/text modes stay owned by v2',()=>{
 const old={id:1,start_date:'2020-01-01',title:'Истёкшее событие',lifecycle_status:'active'} as PreviewEvent;
 for(const events of [[],[old]]){
  assert.deepEqual(buildHomeHeroTalkDeck(events,'2026-09-06','fixture'),[]);
  const message=genericHeroTalkMessage('home_intro');assert.equal(message.action.path,'/segodnya/');assert.ok(message.fragments[0].text.length>0);
 }
 const current={...old,id:2,start_date:'2026-09-07',title:'Текущее событие'};
 const deck=buildHomeHeroTalkDeck([current],'2026-09-06','fixture');assert.equal(deck.length,1);assert.equal(deck[0].mode,'text-only');assert.equal(deck[0].event.id,2);
 const hero=source('../src/components/HomeHeroTalk.astro');assert.match(hero,/visibleScenes.length === 0/);assert.match(hero,/data-ds-variant="service-fallback"/);assert.match(hero,/data-home-hero-mosaic/);
});
test('home assembly has five ordered owners, no conversation widget or local card fork',()=>{
 const page=source('../src/pages/index.astro');let prior=-1;
 for(const tag of ['HomeHeroTalk','HomeSearchEntry','HomeQuickNav','HomeColdStartFeed','HeroTalkPageEnd']){const index=page.indexOf(`<${tag} `);const actual=index>=0?index:page.indexOf(`<${tag} />`);assert.ok(actual>prior,tag);prior=actual;}
 assert.doesNotMatch(page,/<ConversationalSearch|<EventCard|position:\s*(sticky|fixed)/);
 const nav=source('../src/components/HomeQuickNav.astro');assert.match(nav,/getCollectionNavigationEntries/);assert.match(nav,/withBase\(item.href\)/);assert.match(nav,/<Button variant="quiet" size="compact"/);assert.doesNotMatch(nav,/sticky|note:|position:fixed/);
 const end=source('../src/components/HeroTalkPageEnd.astro');assert.match(end,/withBase\(message.action.path\)/);assert.doesNotMatch(end,/StandardOnboarding|fixed|sticky|<EventCard/);
});

test('Search/Home contribution bridge resolves exact shared action stores and served-list identity',async()=>{
 const layout=source('../src/layouts/EventLayout.astro');
 const start=layout.indexOf('const searchCardStores = new WeakMap()');
 const end=layout.indexOf('async function handleFeedbackButton',start);
 const served=new Map(),window:any={};
 const context={window,servedListByFeed:served,FEATURE_SCHEMA_VERSION:'schema',TAXONOMY_VERSION:'taxonomy',hiddenSet:()=>new Set(['8']),activeProfile:()=>({}),candidateId:(c:any)=>c.id,applyFeedbackState:()=>{},personalFeedStores:new Map(),ensureDiscoveryStore:()=>{throw Error('must not use unrelated discovery store');}};
 vm.runInNewContext(layout.slice(start,end)+';window.controllerForCard=controllerForCard;',context);
 for(const home of [false,true]){
  const grid:any={dataset:{},matches:(selector:string)=>home?selector.includes('data-home-feed-grid'):selector.includes('data-search-result-host')};
  window.KenigEventsSearchCardHost.register(grid,[{id:8}],{servedListId:'served',sectionId:'section'});
  if(home)assert.equal(grid.dataset.searchResultHost,undefined);else assert.equal(grid.dataset.searchResultHost,'true');
  const result=await window.controllerForCard({closest:(selector:string)=>{assert.ok(selector.includes(home?'data-home-feed-grid':'data-search-result-host'));return grid;}});
  assert.equal(result.store.ranked[0].event_id,8);assert.equal(served.get(grid).surface,home?'home_feed':'search_results');
 }
 assert.equal(window.KenigEventsSearchCardHost.hiddenIds()[0],'8');
});
