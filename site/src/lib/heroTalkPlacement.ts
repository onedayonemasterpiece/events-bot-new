import type { HomeHeroTalkEditorialFragment } from '../data/homeHeroTalkEditorial.ts';

/** Typed semantic content shared by introductory and compact continuation placements.
 * Campaign selection/caps remain upstream; this service-only resolver activates no promo.
 */
export const HERO_TALK_PLACEMENT_VERSION = 'hero-talk-placement-v1' as const;
export type HeroTalkRouteContext = 'home' | 'event' | 'listing' | 'collection' | 'search';
export type HeroTalkCapability = 'search' | 'calendar' | 'programme';
export interface HeroTalkMessage {
  id: string;
  contentSource: 'service';
  fragments: HomeHeroTalkEditorialFragment[];
  description: string;
  action: { id: string; label: string; path: string; capability: HeroTalkCapability };
}
export interface HeroTalkPlacementContext {
  route: HeroTalkRouteContext;
  placement: 'home_intro' | 'page_end';
  readyCapabilities: HeroTalkCapability[];
  hiddenMessageIds?: string[];
  suppressedCapabilities?: HeroTalkCapability[];
  completedActionIds?: string[];
  upperSceneIds?: string[];
  suppressed?: boolean;
}
export function genericHeroTalkMessage(placement: HeroTalkPlacementContext['placement']): HeroTalkMessage {
  return placement === 'home_intro' ? {
    id:'service-home-intro', contentSource:'service',
    fragments:[{text:'Как хочется провести день?', accent:true}],
    description:'Выберите дату или расскажите, что вам интересно.',
    action:{id:'open-calendar',label:'Выбрать события на сегодня',path:'/segodnya/',capability:'calendar'},
  } : {
    id:'service-refine-search', contentSource:'service',
    fragments:[{text:'Подобрать точнее?', accent:true}],
    description:'Расскажите, куда и с кем хочется пойти.',
    action:{id:'open-new-search',label:'Перейти к поиску',path:'/poisk/',capability:'search'},
  };
}
export function resolveHeroTalkPlacement(context: HeroTalkPlacementContext, nextStep?: HeroTalkMessage): HeroTalkMessage | null {
  // Other routes supply a truthful, explicit next step; no invented event/programme links.
  const message = nextStep || (context.route === 'home' ? genericHeroTalkMessage(context.placement) : null);
  if (!message || context.suppressed || message.contentSource !== 'service') return null;
  if (!/^\/(?!\/)/u.test(message.action.path)) return null;
  if (!context.readyCapabilities.includes(message.action.capability)
    || context.suppressedCapabilities?.includes(message.action.capability)
    || context.hiddenMessageIds?.includes(message.id)
    || context.upperSceneIds?.includes(message.id)
    || context.completedActionIds?.includes(message.action.id)) return null;
  return message;
}

/** No new storage, campaign exposures or analytics owner: consume actual entry readiness. */
export function bindHeroTalkPageEnd(root: HTMLElement) {
  const entry = root.ownerDocument.querySelector<HTMLElement>('[data-home-search-entry]');
  if (!entry) return () => {};
  const sync = () => {
    const busy = ['requesting','recording','saving','submitted'].includes(entry.dataset.homeSearchState || '');
    const suppressed = root.dataset.heroTalkSuppressed === 'true';
    // Signing in is a useful next step on Search, not unavailable capability.
    const unavailable = entry.dataset.searchEnabled !== 'true' && entry.dataset.homeSearchState !== 'signed-out';
    root.hidden = suppressed || unavailable || busy;
    root.dataset.dsState = root.hidden ? 'suppressed' : 'service-continuation';
  };
  const observer = new MutationObserver(sync);
  observer.observe(entry,{attributes:true,attributeFilter:['data-search-enabled','data-home-search-state']});
  observer.observe(root,{attributes:true,attributeFilter:['data-hero-talk-suppressed']});
  sync();
  return () => observer.disconnect();
}
