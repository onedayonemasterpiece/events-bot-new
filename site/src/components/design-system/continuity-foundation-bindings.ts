export type ContinuityBindingOwner = 'A0' | 'M0';
export type ContinuityBindingKind = 'color' | 'type' | 'spacing' | 'shape' | 'elevation' | 'icon' | 'media';

export interface ContinuityFoundationBinding {
  consumer: string;
  sourceHead: string;
  owner: ContinuityBindingOwner;
  kind: ContinuityBindingKind;
  current: string;
  replacement: string;
  browserExpectation: string;
  negativeControl: string;
}

/**
 * Exact current-head complement to surface-foundation-bindings.ts.
 *
 * Tokens remain authoritative in the CSS registries. This table records the
 * newest A0/M0 literal-to-role handoff so integration does not need to repeat
 * F0 semantic analysis. Source heads are census anchors, never hard gates.
 */
export const CONTINUITY_FOUNDATION_BINDINGS = [
  {
    consumer: 'site/src/components/AdaptiveEventCardGrid.astro',
    sourceHead: '5c6869aa5479564ab03d3a3080905538ed305388',
    owner: 'M0', kind: 'spacing',
    current: 'clamp(.85rem,1.6vw,1.25rem)',
    replacement: 'var(--ke-adaptive-card-grid-gap)',
    browserExpectation: 'flow and packed rows preserve current wrapping and regular-width remainder',
    negativeControl: 'no second grid selector or consumer-local gap owner is introduced',
  },
  {
    consumer: 'site/src/components/AdaptiveEventCardGrid.astro',
    sourceHead: '5c6869aa5479564ab03d3a3080905538ed305388',
    owner: 'M0', kind: 'color',
    current: '#d2c5b7; #e4ddd2 -> #d4c7b9; #15110f',
    replacement: 'var(--ke-color-adaptive-card-media-placeholder); var(--ke-color-adaptive-card-media-visual-start/end); var(--ke-color-adaptive-card-media-contain)',
    browserExpectation: 'visual placeholders and protected contain bands remain pixel-equivalent',
    negativeControl: 'MediaFrame fit/object-position stays owned by media-frame.css and the canonical card root',
  },
  {
    consumer: 'site/src/components/AdaptiveEventCardGrid.astro',
    sourceHead: '5c6869aa5479564ab03d3a3080905538ed305388',
    owner: 'M0', kind: 'spacing',
    current: '58px utility row; 56px feedback row',
    replacement: 'var(--ke-adaptive-card-packed-utility-min-height); var(--ke-adaptive-card-packed-feedback-min-height)',
    browserExpectation: 'packed cards keep equal action-row baselines',
    negativeControl: 'row heights do not become new global control-size roles',
  },
  {
    consumer: 'site/src/components/EventMediaRail.astro',
    sourceHead: '5c6869aa5479564ab03d3a3080905538ed305388',
    owner: 'M0', kind: 'color',
    current: 'gallery/resolved dark shells; item surfaces; alpha borders; inverse labels',
    replacement: 'var(--ke-color-media-rail-gallery/resolved-*); var(--ke-color-media-rail-*-border); var(--ke-color-media-rail-inverse-text)',
    browserExpectation: 'all three variants keep current dark-chrome hierarchy and selected-thumb contrast',
    negativeControl: 'provider/media identity pixels and data-image transport remain unchanged',
  },
  {
    consumer: 'site/src/components/EventMediaRail.astro',
    sourceHead: '5c6869aa5479564ab03d3a3080905538ed305388',
    owner: 'M0', kind: 'spacing',
    current: 'gallery 54px; hero 44..68x48px; poster 88..196px by clamp(104px,13svh,148px)',
    replacement: 'var(--ke-media-rail-gallery-*); var(--ke-media-rail-hero-*); var(--ke-media-rail-poster-*)',
    browserExpectation: 'responsive visible/hidden counts and rail packing remain unchanged',
    negativeControl: 'no fourth rail variant and no component-local icon-size role is created',
  },
  {
    consumer: 'site/src/components/EventMediaRail.astro',
    sourceHead: '5c6869aa5479564ab03d3a3080905538ed305388',
    owner: 'M0', kind: 'elevation',
    current: '0 14px 36px rgba(24,18,14,.2)',
    replacement: 'var(--ke-elevation-media-rail-gallery)',
    browserExpectation: 'gallery thumbnail rail retains current depth',
    negativeControl: 'hero-selector and poster-strip remain intentionally flat',
  },
  {
    consumer: 'site/src/components/HomeHeroTalk.astro',
    sourceHead: '56681a8b89f2e5d765d38d5d665b844ea1cc7d95',
    owner: 'A0', kind: 'type',
    current: 'lead clamp(1rem,1.8vw,1.22rem)/1.48; feature title clamp(1.25rem,2.2vw,1.8rem)/1.05; metadata .9rem',
    replacement: 'var(--ke-home-hero-lead-size/line); var(--ke-home-feature-title-size/line); var(--ke-home-feature-meta-size)',
    browserExpectation: 'headline-supporting copy and event title wrap at the same words',
    negativeControl: 'display-home typography remains a distinct intentional role',
  },
  {
    consumer: 'site/src/components/HomeHeroTalk.astro',
    sourceHead: '56681a8b89f2e5d765d38d5d665b844ea1cc7d95',
    owner: 'A0', kind: 'spacing',
    current: '.65rem action gap; 1.2rem action offset; .24rem feature copy gap; 1rem 1.1rem 1.15rem copy padding',
    replacement: 'var(--ke-home-hero-actions-*); var(--ke-home-feature-copy-*)',
    browserExpectation: 'CTA and feature-copy rhythm remains unchanged',
    negativeControl: 'no generic space-scale step is redefined to fit one hero',
  },
  {
    consumer: 'site/src/components/HomeHeroTalk.astro',
    sourceHead: '56681a8b89f2e5d765d38d5d665b844ea1cc7d95',
    owner: 'A0', kind: 'shape',
    current: '28px feature radius; 20rem/18rem media minima',
    replacement: 'var(--ke-home-feature-radius); var(--ke-home-feature-media-min-height/-compact)',
    browserExpectation: 'feature clipping and desktop/mobile image footprint remain unchanged',
    negativeControl: 'cover/contain and focal-point decisions stay consumer/media owned',
  },
  {
    consumer: 'site/src/components/HomeQuickNav.astro',
    sourceHead: '56681a8b89f2e5d765d38d5d665b844ea1cc7d95',
    owner: 'A0', kind: 'spacing',
    current: '.7rem/.55rem grid gaps; .85rem .9rem/.72rem item padding; .15rem item gap',
    replacement: 'var(--ke-home-quick-nav-gap/-compact); var(--ke-home-quick-nav-item-padding-*); var(--ke-home-quick-nav-item-gap)',
    browserExpectation: 'six/three/two-column layouts retain current density',
    negativeControl: '92px and 82px named item heights remain unchanged',
  },
  {
    consumer: 'site/src/components/HomeQuickNav.astro',
    sourceHead: '56681a8b89f2e5d765d38d5d665b844ea1cc7d95',
    owner: 'A0', kind: 'shape',
    current: '20px item radius; -2px hover offset; 2px focus offset',
    replacement: 'var(--ke-home-quick-nav-item-radius); var(--ke-home-quick-nav-hover-offset); var(--ke-home-quick-nav-focus-offset)',
    browserExpectation: 'card silhouette and interaction movement remain unchanged',
    negativeControl: 'motion does not apply when consumer reduced-motion policy disables it',
  },
  {
    consumer: 'site/src/components/HomeQuickNav.astro',
    sourceHead: '56681a8b89f2e5d765d38d5d665b844ea1cc7d95',
    owner: 'A0', kind: 'type',
    current: '1.02rem/1.08 title; .75rem/680/1.2 note',
    replacement: 'var(--ke-home-quick-nav-title-*); var(--ke-home-quick-nav-meta-*)',
    browserExpectation: 'route labels and notes retain current hierarchy',
    negativeControl: 'note remains readable metadata, not an icon or status label',
  },
  {
    consumer: 'site/src/components/HomeColdStartFeed.astro',
    sourceHead: '56681a8b89f2e5d765d38d5d665b844ea1cc7d95',
    owner: 'A0', kind: 'spacing',
    current: '1rem outer offset; 1rem head gap; 1.1rem head margin; .7rem/.35rem compact padding',
    replacement: 'var(--ke-home-feed-margin-top/head-*); var(--ke-home-feed-padding-compact); var(--ke-home-feed-head-padding-compact)',
    browserExpectation: 'feed shell and compact header keep existing rhythm',
    negativeControl: 'AdaptiveEventCardGrid owns card gap, not this shell',
  },
  {
    consumer: 'site/src/components/HomeColdStartFeed.astro',
    sourceHead: '56681a8b89f2e5d765d38d5d665b844ea1cc7d95',
    owner: 'A0', kind: 'type',
    current: 'status max 32rem; .92rem/720',
    replacement: 'var(--ke-home-feed-status-max-width/size/weight)',
    browserExpectation: 'status line keeps its current desktop wrapping',
    negativeControl: 'runtime status copy and aria-live semantics are untouched',
  },
  {
    consumer: 'site/src/components/PersonalFeedSlot.astro',
    sourceHead: '56681a8b89f2e5d765d38d5d665b844ea1cc7d95',
    owner: 'A0', kind: 'spacing',
    current: '46rem copy width; .45rem 0 1rem copy margin; -.35rem status offset; 1rem/.5rem action offsets; compact 1.15rem 1rem 1.35rem',
    replacement: 'var(--ke-personal-feed-copy-*); var(--ke-personal-feed-status-margin-top); var(--ke-personal-feed-action-*); var(--ke-personal-feed-mobile-*)',
    browserExpectation: 'event-detail personal feed keeps full-bleed desktop and compact mobile rhythm',
    negativeControl: 'AdaptiveEventCardGrid remains sole grid/live-region host',
  },
] as const satisfies readonly ContinuityFoundationBinding[];

export const continuityFoundationBindingsFor = (consumer: string) =>
  CONTINUITY_FOUNDATION_BINDINGS.filter((binding) => binding.consumer === consumer);
