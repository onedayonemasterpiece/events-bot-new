import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import { homeHiddenIds, parseHomeProfile, rankHomeCandidates, reconcileHomeOrder } from '../src/lib/homeFeed.mjs';

const profile = (override = {}) => parseHomeProfile(JSON.stringify({
  consent_ok:true, profile_version:'anon-profile-v1', feature_schema_version:'event-detail-related-v1', taxonomy_version:'event-taxonomy-v1',
  anon_id:'11111111-1111-4111-8111-111111111111', session_id:'22222222-2222-4222-8222-222222222222',
  liked_event_ids:['501','502','503'], hidden_event_ids:[], not_interested_event_ids:[],
  positive_tags:{jazz:2}, negative_interest_tags:{}, share_counts:{}, ...override,
}));
const pool = Array.from({length:45}, (_, index) => ({
  event_id:index+1, static_score:1-index/45, category:`category-${index%7}`, location_name:`venue-${index%9}`,
  tags:index===40 ? ['jazz']:[], display:{occurrence_member_ids:[index+1],likes_count:index<30?10:0},
}));

test('full eligible pool shared scoring materializes an affinity candidate beyond popularity top 30', () => {
  const general = rankHomeCandidates(pool);
  assert.equal(general.items.length,45,'diversity must postpone, not drop valid events');
  assert.ok(!general.items.slice(0,30).some(x=>x.event_id===41));
  const personal = rankHomeCandidates(pool,profile());
  assert.equal(personal.personalized,true);
  assert.equal(personal.items[0].event_id,41);
  const visible = reconcileHomeOrder({ranked:personal.items,candidates:pool});
  assert.equal(visible.order.length,30);
  assert.equal(visible.order[0],'41');
});

test('shared static diversity mixes categories/venues without deleting the long tail', () => {
  const ranked = rankHomeCandidates(pool);
  assert.equal(new Set(ranked.items.slice(0,10).map(x=>x.candidate.category)).size,7);
  assert.deepEqual([...ranked.items.map(x=>x.event_id)].sort((a,b)=>a-b),pool.map(x=>x.event_id));
});

test('new affinity never moves observed prefix; exact family hide/Undo restores original position', () => {
  const candidates=pool.map(x=>x.event_id===2?{...x,display:{occurrence_member_ids:[2,202]}}:x);
  const initial=rankHomeCandidates(candidates).items.slice(0,30).map(x=>String(x.event_id));
  const liked=rankHomeCandidates(candidates,profile());
  const changed=reconcileHomeOrder({previous:initial,locked:6,ranked:liked.items,candidates});
  assert.deepEqual(changed.order.slice(0,6),initial.slice(0,6));
  assert.ok(changed.order.includes('41'));
  const hiddenProfile=profile({hidden_event_ids:['202']});
  const hiddenPlan=rankHomeCandidates(candidates,hiddenProfile);
  assert.ok(!hiddenPlan.items.some(x=>x.event_id===2));
  const hidden=reconcileHomeOrder({previous:changed.order,locked:6,ranked:hiddenPlan.items,candidates,hidden:homeHiddenIds(hiddenProfile)});
  assert.equal(hidden.order.indexOf('2'),changed.order.indexOf('2'));
  assert.ok(hidden.hidden.includes('2'));
  assert.equal(hidden.visible,30);
  const undo=reconcileHomeOrder({previous:hidden.order,locked:6,ranked:liked.items,candidates});
  assert.equal(undo.order.indexOf('2'),initial.indexOf('2'));
  assert.equal(undo.visible,30);
  assert.ok(!undo.hidden.includes('2'));
});

test('family duplication and corrupt/incompatible profiles fail closed to honest general feed', () => {
  const duplicate=[{...pool[0],display:{occurrence_member_ids:[1,101]}},{...pool[1],event_id:101},...pool.slice(2)];
  assert.equal(rankHomeCandidates(duplicate).items.filter(x=>[1,101].includes(x.event_id)).length,1);
  assert.equal(parseHomeProfile('{'),null);
  assert.equal(profile({consent_ok:false}),null);
  assert.equal(profile({taxonomy_version:'old'}),null);
  assert.equal(rankHomeCandidates(pool,profile({liked_event_ids:['1']})).personalized,false);
});

test('return order can be replayed without reshuffle and empty pool has no fabricated cards', () => {
  const ranked=rankHomeCandidates(pool,profile());
  const order=reconcileHomeOrder({ranked:ranked.items,candidates:pool}).order;
  const returned=reconcileHomeOrder({previous:order,locked:order.length,ranked:rankHomeCandidates(pool).items,candidates:pool});
  assert.deepEqual(returned.order,order);
  assert.deepEqual(reconcileHomeOrder({}),{order:[],visible:0,hidden:[],locked:0});
});

test('SSR and runtime use common cards/framing/actions; no technical labels, duplicate fetch or recommender', async () => {
  const [component,runtime]=await Promise.all(['src/components/HomeColdStartFeed.astro','src/lib/homeFeedRuntime.ts'].map(path=>readFile(new URL(`../${path}`,import.meta.url),'utf8')));
  assert.match(component,/rankHomeCandidates\(candidates\)/);
  assert.match(component,/generalPlan.items.slice\(0, 30\)/);
  assert.match(component,/<AdaptiveEventCardGrid/);
  assert.match(component,/data-home-feed-candidates/);
  assert.match(component,/Общая подборка/);
  assert.doesNotMatch(component,/Локальных оценок|border:|home-feed-surface/);
  assert.match(runtime,/KenigEventsCreateEventCard/);
  assert.match(runtime,/KenigEventsSearchCardHost.register/);
  assert.match(runtime,/packRelatedCardRows/);
  assert.match(runtime,/sessionStorage.setItem/);
  assert.match(runtime,/keHomeFeed/);
  assert.doesNotMatch(runtime,/\.fetch\(|grid-template-columns|innerHTML\s*=/);
});

test('Undo after observing a hide replacement never exceeds the finite visible budget', () => {
  const initial=rankHomeCandidates(pool).items.slice(0,30).map(x=>String(x.event_id));
  const h=profile({hidden_event_ids:[initial[0]]});
  const hidden=reconcileHomeOrder({previous:initial,locked:30,ranked:rankHomeCandidates(pool,h).items,candidates:pool,hidden:homeHiddenIds(h)});
  assert.equal(hidden.order.length,31);
  const undo=reconcileHomeOrder({previous:hidden.order,locked:31,ranked:rankHomeCandidates(pool).items,candidates:pool});
  assert.equal(undo.visible,30);
  assert.deepEqual(undo.order.filter(id=>!undo.hidden.includes(id)),initial);
});
