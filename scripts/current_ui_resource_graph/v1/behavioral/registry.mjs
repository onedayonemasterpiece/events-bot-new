export const BEHAVIOR_PACKET_SCHEMA='current_ui_behavior_action_packet_v1_1';
export const PINNED_SOURCE_SHA='ef7aa62e45c60f7a12da6160f490719c0721ec03';

const base={
  schema_version:BEHAVIOR_PACKET_SCHEMA,
  source_sha:PINNED_SOURCE_SHA,
  evidence_plane:'controlled-exact-source-runtime',
  production_state_claimed:false,
  review_status:'planned',
  reduced_motion:true,
  device_scale_factor:1,
  decision:'NOT_MERGED',
  normalization_allowed:false,
};
const step=(phase,actions=[],capture=true)=>({phase,actions,capture});
const plan=(id,family,route,root_selector,steps,extra={})=>Object.freeze({
  ...base,id:`behavior-packet.${id}`,family,route,root_selector,viewport:{width:390,height:844},
  capture_scope:'locator',steps,...extra,
});
const blocker=(id,family,reason,extra={})=>Object.freeze({
  ...base,id:`behavior-packet.${id}`,family,route:null,root_selector:null,viewport:null,
  capture_scope:'none',steps:[],execution_status:'explicit-blocker',blocker_reason:reason,
  review_status:'not-applicable-no-raster',...extra,
});

export const BEHAVIOR_PACKET_PLANS=Object.freeze([
  // Mobile menu and disclosures (8)
  plan('menu-open-close','mobile-menu','/','[data-mobile-discovery-menu][data-reference4-fullscreen]',[
    step('closed'),step('open',[{kind:'click',selector:':scope > summary'}]),step('closed-after-close',[{kind:'click',selector:'[data-reference4-close]'}]),
  ],{capture_scope:'page'}),
  plan('menu-escape','mobile-menu','/','[data-mobile-discovery-menu][data-reference4-fullscreen]',[
    step('closed'),step('open',[{kind:'click',selector:':scope > summary'}]),step('closed-after-escape',[{kind:'press',key:'Escape'}]),
  ],{capture_scope:'page'}),
  plan('menu-collections-plane','mobile-menu','/','[data-mobile-discovery-menu][data-reference4-fullscreen]',[
    step('open',[{kind:'click',selector:':scope > summary'}]),step('collections',[{kind:'click',selector:'[data-reference4-collections-open]'}]),step('main-after-back',[{kind:'click',selector:'[data-reference4-collections-back]'}]),
  ],{capture_scope:'page'}),
  plan('menu-service-plane','mobile-menu','/','[data-mobile-discovery-menu][data-reference4-fullscreen]',[
    step('open',[{kind:'click',selector:':scope > summary'}]),step('service',[{kind:'click',selector:'[data-reference4-service-open]'}]),step('main-after-back',[{kind:'click',selector:'[data-reference4-service-back]'}]),
  ],{capture_scope:'page'}),
  plan('menu-short-scroll','mobile-menu','/','[data-mobile-discovery-menu][data-reference4-fullscreen]',[
    step('open-top',[{kind:'click',selector:':scope > summary'}]),step('open-bottom',[{kind:'scroll-element',selector:'[data-reference4-list]',edge:'end'}]),
  ],{capture_scope:'page',viewport:{width:390,height:678}}),
  plan('time-nav-disclosure','disclosure','/behavior-specimens/time-nav/','[data-listing-time-nav]',[
    step('closed'),step('open',[{kind:'click',selector:'[data-time-nav-disclosure] summary'}]),
  ],{capture_scope:'page'}),
  plan('city-picker','disclosure','/segodnya/','[data-mobile-v23-city-picker]',[
    step('closed'),step('open',[{kind:'click',selector:'summary'}]),step('selected',[{kind:'click',selector:'[data-mobile-v23-city]:not([data-mobile-v23-city="all"])'}]),
  ],{capture_scope:'page'}),
  plan('calendar-sheet','overlay','/vyhodnye/','[data-calendar-sheet]',[
    step('closed'),step('open',[{kind:'click',selector:'[data-calendar-open]',scope:'page'}]),step('next-month',[{kind:'click',selector:'[data-calendar-month-next]',scope:'page'}]),step('closed-after-button',[{kind:'click',selector:'[data-calendar-close]',scope:'page'}]),
  ],{capture_scope:'page'}),

  // Selection controls (4)
  plan('listing-city-filter','selection','/segodnya/','[data-listing-controls]',[
    step('all'),step('city-selected',[{kind:'click',selector:'[data-listing-city-input]:not([value="all"])',scope:'page'}]),
  ],{viewport:{width:1280,height:800}}),
  blocker('popular-category-filter','selection','PopularCategoryFilter has no pinned production consumer; source-only behavior cannot be promoted to runtime-observed.',{source_path:'src/components/listings/PopularCategoryFilter.astro'}),
  blocker('personal-filter-v1','selection','ListingPersonalFilter version=1 has no pinned non-lab consumer; a synthetic wrapper would not prove current consumer behavior.',{source_path:'src/components/ListingPersonalFilter.astro'}),
  plan('personal-filter-v2-floating','selection','/segodnya/','[data-listing-filter][data-listing-filter-version="2"]',[
    step('all'),step('personal',[{kind:'click',selector:'[data-listing-mode-button="personal"]'}]),step('all-restored',[{kind:'click',selector:'[data-listing-mode-button="all"]'}]),
  ],{viewport:{width:1280,height:800},before_navigation:[{kind:'seed-profile',mature:true,hidden_event_ids:[6767]},{kind:'set-listing-mode',mode:'all'}]}),

  // Rails and edge states (4)
  blocker('rail-empty','rail','No exact current route with a rendered zero-row rail exists; source emits an empty region only after client filtering and requires a dedicated reviewed fixture.'),
  blocker('rail-single','rail','No exact pinned current route/section with exactly one rendered MobileListingRailRow is available in the fixed candidate corpus.'),
  plan('rail-many-start-middle-end','rail','/segodnya/','[data-mobile-listing-row] .rail-window',[
    step('edge-start'),step('middle',[{kind:'scroll-element',edge:'middle'}]),step('edge-end',[{kind:'scroll-element',edge:'end'}]),
  ]),
  plan('rail-keyboard-home-end','rail','/segodnya/','[data-mobile-listing-row] .rail-window',[
    step('edge-start'),step('edge-end',[{kind:'focus'},{kind:'press',key:'End'}]),step('edge-start-restored',[{kind:'press',key:'Home'}]),
  ]),

  // Sticky / fixed / collision (7)
  plan('sticky-mobile-title','sticky','/segodnya/','[data-mobile-listing-sticky-title]',[
    step('static'),step('approaching',[{kind:'scroll-window',y:180}]),step('pinned',[{kind:'scroll-to-selector',selector:'[data-mobile-v23-group]',offset:80}]),
  ],{capture_scope:'page'}),
  plan('sticky-mobile-collision','sticky','/populyarnoe/','[data-mobile-listing-sticky-title]',[
    step('static'),step('pinned',[{kind:'scroll-to-selector',selector:'[data-mobile-v23-group]',offset:80}]),step('next-group-collision',[{kind:'scroll-to-selector',selector:'[data-mobile-v23-group]:nth-child(2)',offset:74,optional:true}]),
  ],{capture_scope:'page'}),
  plan('sticky-desktop-discovery','sticky','/segodnya/','[data-listing-discovery-rail]',[
    step('static'),step('pinned',[{kind:'scroll-window',y:340}]),step('leaving-page',[{kind:'scroll-window',edge:'end'}]),
  ],{capture_scope:'page',viewport:{width:1280,height:800}}),
  plan('sticky-weekend-nav','sticky','/vyhodnye/','[data-listing-time-nav]',[
    step('static'),step('pinned',[{kind:'scroll-window',y:420}]),step('group-collision',[{kind:'scroll-to-selector',selector:'[data-mobile-v23-group]',offset:120}]),
  ],{capture_scope:'page'}),
  plan('sticky-editorial-event','sticky','/lab/event-desktop/examples/editorial-photo/','[data-desktop-clean-event]',[
    step('static'),step('media-action-pinned',[{kind:'scroll-window',y:520}]),step('lower-content',[{kind:'scroll-window',y:1100}]),
  ],{capture_scope:'page',viewport:{width:1728,height:900}}),
  plan('sticky-split-event','sticky','/lab/event-desktop/examples/split-portrait/','[data-desktop-clean-event]',[
    step('static'),step('inline-action-pinned',[{kind:'scroll-window',y:520}]),step('lower-content',[{kind:'scroll-window',y:1100}]),
  ],{capture_scope:'page',viewport:{width:1728,height:900}}),
  plan('floating-filter-footer-collision','sticky','/segodnya/','[data-listing-filter][data-listing-filter-version="2"]',[
    step('visible'),step('near-footer',[{kind:'scroll-window',edge:'end'}]),
  ],{capture_scope:'page',viewport:{width:390,height:844},before_navigation:[{kind:'seed-profile',mature:true,hidden_event_ids:[6767]},{kind:'set-listing-mode',mode:'all'}]}),

  // CTA alternatives (6)
  plan('cta-editorial-stacked','cta','/lab/event-desktop/examples/editorial-photo/','[data-desktop-action-panel][data-action-layout="stacked"]',[step('baseline')],{viewport:{width:1728,height:900}}),
  plan('cta-split-inline','cta','/lab/event-desktop/examples/split-portrait/','[data-desktop-action-panel][data-action-layout="inline"]',[step('baseline')],{viewport:{width:1728,height:900}}),
  plan('cta-phone-success','cta','/lab/event-desktop/examples/cta-phone-invariant/','[data-desktop-action-panel]',[
    step('phone-hidden'),step('phone-copied',[{kind:'mock-clipboard',result:'success'},{kind:'click',selector:'[data-desktop-phone-copy]'}]),
  ],{viewport:{width:1728,height:900}}),
  plan('cta-phone-error','cta','/lab/event-desktop/examples/cta-phone-invariant/','[data-desktop-action-panel]',[
    step('phone-hidden'),step('phone-copy-error',[{kind:'mock-clipboard',result:'error'},{kind:'click',selector:'[data-desktop-phone-copy]'}]),
  ],{viewport:{width:1728,height:900}}),
  plan('cta-registration','cta','/lab/event-desktop/examples/cta-registration-invariant/','[data-desktop-action-panel]',[step('registration')],{viewport:{width:1728,height:900}}),
  plan('cta-sold-out','cta','/sobytiya/ekskursiya-zakulise-teatra-kaliningrad-5829/','[data-desktop-action-panel]',[step('sold-out-or-unavailable')],{viewport:{width:1728,height:900}}),

  // Three transport treatments on one exact fixture/environment (6)
  ...['departure_board_v1','route_strips_v1','next_departure_queue_v1'].flatMap((treatment)=>[
    plan(`transport-${treatment.replaceAll('_','-')}-baseline`,'transport','/behavior-specimens/transport/',`[data-transport-treatment="${treatment}"]`,[step('baseline')],{viewport:{width:391,height:900},container_width:391,before_navigation:[{kind:'select-treatment',treatment}]}),
    plan(`transport-${treatment.replaceAll('_','-')}-disclosure`,'transport','/behavior-specimens/transport/',`[data-transport-treatment="${treatment}"]`,[
      step('compact-closed'),step('compact-open',[{kind:'click',selector:'[data-transport-disclosure] summary, [data-transport-queue-details] summary',optional:true}]),
    ],{viewport:{width:389,height:900},container_width:389,before_navigation:[{kind:'select-treatment',treatment}]}),
  ]),

  // Media (6)
  plan('media-real-5x4','media','/segodnya/','[data-mobile-rail-media-reason="safe_visual_landscape_5x4"]',[step('loaded-5x4')],{ratios:['5:4']}),
  plan('media-real-4x5','media','/sobytiya/ekskursiya-oplot-nezavisimosti-i-piva-kaliningrad-6686/','#discovery-feed [data-event-card][data-event-id="7023"] [data-card-media-shell]',[step('loaded-4x5')],{ratios:['4:5'],viewport:{width:390,height:844}}),
  plan('media-broken','media','/segodnya/','[data-mobile-listing-row] .event-media',[
    step('loading'),step('error',[{kind:'release-images',result:'error'}]),
  ],{network_profile:'deferred-images'}),
  plan('media-missing','media','/sobytiya/nauka-vsegda-kstati-progulka-s-uchenym-kaliningrad-6996/','[data-event-hero], [data-desktop-clean-event]',[step('missing-fallback')]),
  plan('media-tiny','media','/sobytiya/fotografii-moego-dedushki-semena-aniskova-kaliningrad-6757/','[data-event-hero], [data-desktop-clean-event]',[step('tiny-source')],{viewport:{width:1728,height:900}}),
  plan('media-primary-and-previews','media','/behavior-specimens/media-rail/','[data-behavior-media-fixture]',[step('large-primary-small-previews')],{viewport:{width:1024,height:900}}),

  // Loading/recovery (9)
  plan('search-loading-result','loading','/poisk/','[data-authorized-search]',[
    step('idle'),step('loading',[{kind:'search-submit',query:'result'}]),step('result',[{kind:'release-search',result:'success'}]),
  ],{runtime_profile:'search-success',ratios:['5:4']}),
  plan('search-loading-empty','loading','/poisk/','[data-authorized-search]',[
    step('idle'),step('loading',[{kind:'search-submit',query:'empty'}]),step('empty',[{kind:'release-search',result:'empty'}]),
  ],{runtime_profile:'search-empty'}),
  plan('search-error-retry-result','loading','/poisk/','[data-authorized-search]',[
    step('idle'),step('loading-error',[{kind:'search-submit',query:'error'}]),step('error',[{kind:'release-search',result:'error'}]),step('retry-loading',[{kind:'search-submit',query:'result'}]),step('result',[{kind:'release-search',result:'success'}]),
  ],{runtime_profile:'search-error-retry'}),
  plan('favorites-loading-result','loading','/izbrannoe/','[data-favorites-surface]',[
    step('loading'),step('result',[{kind:'release-favorites',result:'success'}]),
  ],{runtime_profile:'favorites-result',before_navigation:[{kind:'seed-profile',liked_event_ids:[7023]}]}),
  plan('favorites-loading-empty','loading','/izbrannoe/','[data-favorites-surface]',[
    step('loading'),step('empty',[{kind:'release-favorites',result:'success'}]),
  ],{runtime_profile:'favorites-empty'}),
  plan('favorites-loading-error','loading','/izbrannoe/','[data-favorites-surface]',[
    step('loading'),step('error',[{kind:'release-favorites',result:'error'}]),
  ],{runtime_profile:'favorites-error',before_navigation:[{kind:'seed-profile',liked_event_ids:[7023]}]}),
  plan('personal-feed-popular','loading','/sobytiya/ekskursiya-oplot-nezavisimosti-i-piva-kaliningrad-6686/','[data-personal-feed-section]',[
    step('pending'),step('popular-fallback',[{kind:'release-personal-feed',result:'success'}]),
  ],{viewport:{width:1280,height:800},runtime_profile:'personal-feed-popular'}),
  plan('personal-feed-personal','loading','/sobytiya/ekskursiya-oplot-nezavisimosti-i-piva-kaliningrad-6686/','[data-personal-feed-section]',[
    step('pending'),step('personal',[{kind:'release-personal-feed',result:'success'}]),
  ],{viewport:{width:1280,height:800},runtime_profile:'personal-feed-personal',before_navigation:[{kind:'seed-profile',mature:true,liked_event_ids:[7008,7003,5658]}]}),
  blocker('personal-feed-unavailable','loading','Event-detail source falls back to deterministic popular content on ordinary fetch failure; an honest unavailable state additionally requires an unavailable renderer or empty validated corpus and is not reached by the pinned real corpus.',{source_path:'src/components/PersonalFeedSlot.astro'}),
]);

export function buildBehaviorPacketRegistry(){
  return {schema_version:BEHAVIOR_PACKET_SCHEMA,source_sha:PINNED_SOURCE_SHA,plans:BEHAVIOR_PACKET_PLANS.map((row)=>structuredClone(row))};
}

export function expectedRasterCount(registry=buildBehaviorPacketRegistry()){
  return registry.plans.reduce((sum,row)=>sum+row.steps.filter((item)=>item.capture).length,0);
}
