import { createHash } from 'node:crypto';

const sha=(value)=>createHash('sha256').update(value).digest('hex');
const clamp=(value,min,max)=>Math.max(min,Math.min(max,value));

const event=(id,slug)=>`/sobytiya/${slug}-${id}/`;
export const SOURCE_CONSUMERS=Object.freeze({
  'src/components/DesktopEventActionPanel.astro':[{component:'DesktopEventActionPanel',route:'/lab/event-desktop/examples/cta-phone-invariant/',root_selector:'[data-desktop-action-panel]'}],
  'src/components/DesktopEventPage.astro':[
    {component:'DesktopEventPage',consumer:'editorial-photo',route:'/lab/event-desktop/examples/editorial-photo/',root_selector:'[data-desktop-clean-event]'},
    {component:'DesktopEventPage',consumer:'split-portrait',route:'/lab/event-desktop/examples/split-portrait/',root_selector:'[data-desktop-clean-event]'},
  ],
  'src/components/EventBusTransportSchedule.astro':[{component:'EventBusTransportSchedule',consumer:'event-6365',route:event(6365,'den-valyaniya-v-sene-romanovo'),root_selector:'[data-event-bus-schedule]',container_name:'event-bus'}],
  'src/components/EventParticipants.astro':[{component:'EventParticipants',route:'/lab/event-participants/',root_selector:'[data-event-participants]'}],
  'src/components/EventQuestionCta.astro':[{component:'EventQuestionCta',route:'/lab/question-cta/',root_selector:'[data-event-question-cta]'}],
  'src/components/EventTransportSchedule.astro':[{component:'EventTransportSchedule',consumer:'event-6939',route:event(6939,'teremok-svetlogorsk'),root_selector:'[data-event-transport-schedule]',container_name:'event-rail'}],
  'src/components/ExhibitionsPersonalSurface.astro':[{component:'ExhibitionsPersonalSurface',route:'/vystavki/',root_selector:'[data-exhibitions-prototype]'}],
  'src/components/FavoritesSurface.astro':[{component:'FavoritesSurface',route:'/izbrannoe/',root_selector:'[data-favorites-surface]'}],
  'src/components/FocusEggArtifact.astro':[{component:'FocusEggArtifact',route:'/fokus-gruppa/kollektsiya/',root_selector:'[data-focus-egg-artifact]'}],
  'src/components/FocusEggSavedListDemo.astro':[{component:'FocusEggSavedListDemo',route:'/fokus-gruppa/kollektsiya/',root_selector:'[data-focus-egg-demo]'}],
  'src/components/FocusGroupInviteIntake.astro':[{component:'FocusGroupInviteIntake',route:'/fokus-gruppa/priglashenie/',root_selector:'[data-focus-intake]'}],
  'src/components/FocusGroupInviteShare.astro':[{component:'FocusGroupInviteShare',route:'/fokus-gruppa/',root_selector:'[data-focus-share]'}],
  'src/components/FocusGroupLabPanel.astro':[{component:'FocusGroupLabPanel',route:'/',root_selector:'[data-focus-lab-panel]',expected_optional:true}],
  'src/components/FocusGroupThankYou.astro':[{component:'FocusGroupThankYou',route:'/fokus-gruppa/',root_selector:'.focus-thanks'}],
  'src/components/FreeCollectionSurface.astro':[{component:'FreeCollectionSurface',route:'/podborki/besplatnye-sobytiya/',root_selector:'[data-free-collection-surface]'}],
  'src/components/HomeHeroTalk.astro':[{component:'HomeHeroTalk',route:'/',root_selector:'[data-home-hero-talk]'}],
  'src/components/InterestClubCard.astro':[{component:'InterestClubCard',route:'/kluby-po-interesam/',root_selector:'[data-club-card]'}],
  'src/components/KaupTransportSchedule.astro':[{component:'KaupTransportSchedule',consumer:'event-5374',route:event(5374,'tribyut-linkin-park-pos-romanovo'),root_selector:'[data-kaup-transport]',container_name:'kaup-transport'}],
  'src/components/MobileBottomNav.astro':[{component:'MobileBottomNav',route:'/',root_selector:'[data-mobile-bottom-nav]'}],
  'src/components/OptimizedEventCardGrid.astro':[{component:'OptimizedEventCardGrid',consumer:'event-6686',route:event(6686,'ekskursiya-oplot-nezavisimosti-i-piva-kaliningrad'),root_selector:'[data-optimized-event-card-grid]'}],
  'src/components/PersonalFeedSlot.astro':[{component:'PersonalFeedSlot',consumer:'event-6686',route:event(6686,'ekskursiya-oplot-nezavisimosti-i-piva-kaliningrad'),root_selector:'[data-personal-feed-section]'}],
  'src/components/Reference4MobileMenu.astro':[{component:'Reference4MobileMenu',route:'/',root_selector:'[data-mobile-discovery-menu][data-reference4-fullscreen]'}],
  'src/components/WeatherDateContext.astro':[{component:'WeatherDateContext',route:'/date-2026-08-08/',root_selector:'[data-weather-date-context]'}],
  'src/components/artifacts/ArtifactCollection.astro':[{component:'ArtifactCollection',route:'/artefakty/',root_selector:'[data-artifact-collection]'}],
  'src/components/listings/AmberRailArtifact.astro':[{component:'AmberRailArtifact',route:'/vyhodnye/',root_selector:'[data-amber-artifact]'}],
  'src/components/listings/MobileListingRailSurface.astro':[{component:'MobileListingRailSurface',route:'/date-2026-08-08/',root_selector:'[data-mobile-listing-rails]'}],
  'src/layouts/EventLayout.astro':[
    {component:'EventLayout',consumer:'home',route:'/',root_selector:'body'},
    {component:'EventLayout',consumer:'listing',route:'/date-2026-08-08/',root_selector:'body'},
    {component:'EventLayout',consumer:'search',route:'/poisk/',root_selector:'body'},
    {component:'EventLayout',consumer:'editorial',route:'/lab/event-desktop/examples/editorial-photo/',root_selector:'body'},
    {component:'EventLayout',consumer:'split',route:'/lab/event-desktop/examples/split-portrait/',root_selector:'body'},
  ],
  'src/pages/festivali/index.astro':[{component:'FestivalIndexPage',route:'/festivali/',root_selector:'[data-festival-timeline]'}],
  'src/pages/lab/design-system/index.astro':[{component:'DesignSystemLabPage',route:'/lab/design-system/',root_selector:'main'}],
  'src/pages/lab/exhibitions-personal/index.astro':[{component:'ExhibitionsPersonalLabPage',route:'/lab/exhibitions-personal/',root_selector:'[data-exhibitions-prototype]'}],
  'src/pages/partners/index.astro':[{component:'PartnersIndexPage',route:'/partners/',root_selector:'.partners-page'}],
  'src/pages/zakrytaya-afisha/index.astro':[{component:'SecretFocusPage',route:'/zakrytaya-afisha/',root_selector:'[data-focus-secret]',expected_optional:true}],
});

function satisfyingValue(feature,target=false){
  if(feature.threshold_px===null)return null;
  if(feature.comparison==='min')return target?feature.threshold_px+64:Math.max(1,feature.threshold_px-64);
  if(feature.comparison==='max')return target?Math.max(1,feature.threshold_px-64):feature.threshold_px+64;
  return feature.threshold_px;
}
export function expectedFeatureBranch(feature,value){
  if(!feature)return true;if(feature.threshold_px===null)return true;
  return feature.comparison==='min'?value>=feature.threshold_px:feature.comparison==='max'?value<=feature.threshold_px:true;
}
export function planProbeEnvironment(row,{defaultWidth=1280,defaultHeight=900}={}){
  let width=defaultWidth,height=defaultHeight,containerWidth=null,containerHeight=null,reducedMotion='reduce',hasTouch=false;
  const target=row.target_feature;
  for(const feature of row.condition_features||[]){
    if(feature===target)continue;let value=satisfyingValue(feature,true);
    if(target&&feature.axis===target.axis&&['width','height'].includes(feature.axis))continue;
    if(feature.axis==='width'){if(row.kind==='container')containerWidth=value;else width=value;}
    else if(feature.axis==='height'){if(row.kind==='container')containerHeight=value;else height=value;}
    else if(feature.axis==='reduced-motion')reducedMotion=feature.value.replace(/\s+/gu,'').toLowerCase()==='no-preference'?'no-preference':'reduce';
    else if(feature.axis==='hover'||feature.axis==='pointer')hasTouch=false;
  }
  if(target?.axis==='width'){if(row.kind==='container')containerWidth=row.probe_px;else width=row.probe_px;}
  else if(target?.axis==='height'){if(row.kind==='container')containerHeight=row.probe_px;else height=row.probe_px;}
  width=clamp(Math.round(width),1,4096);height=clamp(Math.round(height),1,4096);
  if(row.kind==='container'){
    containerWidth=clamp(Math.round(containerWidth??Math.min(width-32,900)),1,4096);
    if(containerHeight!==null)containerHeight=clamp(Math.round(containerHeight),1,4096);
    width=Math.max(width,containerWidth+64);height=Math.max(height,(containerHeight??0)+64,720);
  }
  const targetValue=target?.axis==='height'?(row.kind==='container'?containerHeight:height):target?.axis==='width'?(row.kind==='container'?containerWidth:width):null;
  // Loading-state selectors are otherwise governed by CDN timing: the same
  // exact DOM can leave `loading` between DOMContentLoaded and the CSSOM read.
  // Holding image requests is an environment control (not a DOM mutation) and
  // lets the exact consumer enter and retain the source-authored loading state.
  const holdImageRequests=(row.affected_selectors||[]).some((selector)=>selector.includes('[data-image-state="loading"]'));
  return {viewport:{width,height},container:{name:row.container_name,width:containerWidth,height:containerHeight,box:'content-box'},media:{reduced_motion:reducedMotion,hover:'hover',pointer:'fine',has_touch:hasTouch},resource_control:{image_requests:holdImageRequests?'held-during-observation':'normal',reason:holdImageRequests?'exact-affected-selector-requires-loading-state':null},target_axis:row.axis,target_value:targetValue,expected_branch:target?expectedFeatureBranch(target,targetValue):true,control_values:(row.condition_features||[]).filter((item)=>item!==target).map((feature)=>{const value=feature.axis==='width'?(row.kind==='container'?containerWidth:width):feature.axis==='height'?(row.kind==='container'?containerHeight:height):feature.value;return {feature:feature.name,value,expected_satisfied:feature.threshold_px===null?true:expectedFeatureBranch(feature,value)};})};
}

function selectorTokens(selector){return [...new Set(String(selector).match(/[.#][a-z_][\w-]*|\[data-[\w-]+/giu)||[])].sort();}
function consumerScore(consumer,row){const haystack=(row.affected_selectors||[]).join(' ');const rootTokens=selectorTokens(consumer.root_selector);return rootTokens.filter((token)=>haystack.includes(token.replace(/^\[/u,''))||haystack.includes(token)).length;}
export function consumersForProbe(row){
  const candidates=SOURCE_CONSUMERS[row.path]||[];if(candidates.length<=1)return candidates;
  // EventLayout and DesktopEventPage are selector-aware set-cover inputs. A
  // selector hint wins; otherwise all exact consumers are retained so one
  // missing route cannot be mislabeled as source-wide unreachable.
  const scored=candidates.map((consumer)=>({...consumer,_score:consumerScore(consumer,row)}));const best=Math.max(...scored.map((item)=>item._score));return (best>0?scored.filter((item)=>item._score===best):scored).map(({_score,...item})=>item);
}
export function buildBreakpointProbePlans(rows){
  return rows.map((row)=>{const consumers=consumersForProbe(row);const consumer_coverage_policy=row.path==='src/components/DesktopEventPage.astro'?'all-mapped-consumers-root-required':row.path==='src/layouts/EventLayout.astro'?'selector-aware-first-reconciled-set':'single-exact-consumer';return {...row,environment:planProbeEnvironment(row),consumers,consumer_coverage_policy,plan_fingerprint:sha(`${row.id}\0${row.at_rule_fingerprint}\0${JSON.stringify(consumers)}\0${JSON.stringify(planProbeEnvironment(row))}`)};});
}
