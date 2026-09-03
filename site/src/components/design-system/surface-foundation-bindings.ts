export type FoundationBindingOwner = 'A0' | 'M0';
export type FoundationBindingKind = 'color' | 'type' | 'spacing' | 'shape' | 'elevation' | 'icon' | 'media';

export interface SurfaceFoundationBinding {
  consumer: string;
  owner: FoundationBindingOwner;
  kind: FoundationBindingKind;
  current: string;
  replacement: string;
  browserExpectation: string;
}

/**
 * Exact F0 handoff generated from the current A0/M0 actual-consumer census.
 *
 * This is not an alternate source of visual values. Tokens and icon identities
 * are defined by surface-foundations.css / foundations.ts. Consumer owners use
 * this list to remove local ownership without making semantic choices.
 */
export const SURFACE_FOUNDATION_BINDINGS = [
  {
    consumer: 'site/src/components/HomeHeroTalk.astro', owner: 'A0', kind: 'color',
    current: 'rgba(121,48,20,.13); #fffaf2 -> #f5e4d0',
    replacement: 'var(--ke-color-border-brand-soft); var(--ke-color-home-hero-start) -> var(--ke-color-home-hero-end)',
    browserExpectation: 'same warm hero hierarchy; no new contrast or palette',
  },
  {
    consumer: 'site/src/components/HomeHeroTalk.astro', owner: 'A0', kind: 'type',
    current: 'clamp(3rem,7.5vw,7.2rem) / .86 / -.075em',
    replacement: 'font: var(--ke-type-display-home); letter-spacing: var(--ke-type-display-home-letter)',
    browserExpectation: 'headline wraps at the same words and keeps current vertical rhythm',
  },
  {
    consumer: 'site/src/components/HomeHeroTalk.astro', owner: 'A0', kind: 'shape',
    current: 'clamp(24px,4vw,42px)',
    replacement: 'var(--ke-shape-radius-home-hero)',
    browserExpectation: 'hero clipping radius is unchanged at narrow and wide viewports',
  },
  {
    consumer: 'site/src/components/HomeHeroTalk.astro', owner: 'A0', kind: 'elevation',
    current: '0 24px 65px rgba(72,45,25,.11); 0 18px 46px rgba(35,24,18,.2)',
    replacement: 'var(--ke-elevation-home-hero); var(--ke-elevation-home-feature)',
    browserExpectation: 'same hero and featured-event depth',
  },
  {
    consumer: 'site/src/components/HomeHeroTalk.astro', owner: 'A0', kind: 'media',
    current: '#17120f; #ac6240 -> #211712; #fffaf3; rgba(255,250,243,.74)',
    replacement: 'var(--ke-color-home-feature-surface); var(--ke-color-home-ambient-start) -> var(--ke-color-home-ambient-end); var(--ke-color-home-feature-text); var(--ke-color-home-feature-muted)',
    browserExpectation: 'contain/cover and focal-point behavior remain unchanged',
  },
  {
    consumer: 'site/src/components/HomeQuickNav.astro', owner: 'A0', kind: 'color',
    current: 'rgba(121,48,20,.13/.28); #fffaf3; #fff',
    replacement: 'var(--ke-color-border-brand-soft/hover); var(--ke-color-home-card-surface); var(--ke-color-home-card-hover-surface)',
    browserExpectation: 'cards retain warm rest state and white hover state',
  },
  {
    consumer: 'site/src/components/HomeQuickNav.astro', owner: 'A0', kind: 'spacing',
    current: '92px desktop; 82px compact',
    replacement: 'var(--ke-size-home-nav-item); var(--ke-size-home-nav-item-compact)',
    browserExpectation: 'six/three/two-column navigation keeps existing item heights',
  },
  {
    consumer: 'site/src/components/HomeQuickNav.astro', owner: 'A0', kind: 'color',
    current: 'rgba(165,72,33,.25) focus ring',
    replacement: 'var(--ke-color-focus-ring-brand-subtle)',
    browserExpectation: 'keyboard focus remains visible without a stronger redesign',
  },
  {
    consumer: 'site/src/components/HomeColdStartFeed.astro', owner: 'A0', kind: 'color',
    current: 'rgba(255,250,243,.84); rgba(121,48,20,.13)',
    replacement: 'var(--ke-color-home-feed-surface); var(--ke-color-border-brand-soft)',
    browserExpectation: 'same translucent warm feed panel',
  },
  {
    consumer: 'site/src/components/HomeColdStartFeed.astro', owner: 'A0', kind: 'type',
    current: 'clamp(2rem,4vw,4.2rem) / .92 / -.06em',
    replacement: 'font: var(--ke-type-feature-section); letter-spacing: var(--ke-type-feature-section-letter)',
    browserExpectation: 'section title retains line breaks and density',
  },
  {
    consumer: 'site/src/components/HomeColdStartFeed.astro', owner: 'A0', kind: 'spacing',
    current: 'clamp(1rem,3vw,2rem); clamp(.85rem,1.6vw,1.25rem)',
    replacement: 'var(--ke-space-home-feed-padding); var(--ke-space-home-grid-gap)',
    browserExpectation: 'feed padding and card gaps are unchanged',
  },
  {
    consumer: 'site/src/components/HomeColdStartFeed.astro', owner: 'A0', kind: 'shape',
    current: 'clamp(24px,4vw,38px)',
    replacement: 'var(--ke-shape-radius-home-feed)',
    browserExpectation: 'same responsive outer radius',
  },
  {
    consumer: 'site/src/components/FreeCollectionSurface.astro', owner: 'A0', kind: 'color',
    current: '#fffdf8 -> #f1f6e9; rgba(104,168,83,.18)',
    replacement: 'var(--ke-color-free-hero-start) -> var(--ke-color-free-hero-end); color-mix(in srgb,var(--ke-color-free-hero-glow) 18%,transparent)',
    browserExpectation: 'free identity remains green editorial identity, not success status',
  },
  {
    consumer: 'site/src/components/FreeCollectionSurface.astro', owner: 'A0', kind: 'type',
    current: 'clamp(2.8rem,6vw,5.8rem) / .9 / -.06em',
    replacement: 'font: var(--ke-type-display-collection); letter-spacing: var(--ke-type-display-collection-letter)',
    browserExpectation: 'collection title keeps current scale and wrapping',
  },
  {
    consumer: 'site/src/components/FreeCollectionSurface.astro', owner: 'A0', kind: 'shape',
    current: '26px results; 20px mobile results; local hero radii',
    replacement: 'var(--ke-free-results-radius); var(--ke-free-results-radius-compact); var(--ke-free-hero-radius/compact)',
    browserExpectation: 'hero and result panels preserve their intentionally different radii',
  },
  {
    consumer: 'site/src/components/FreeCollectionSurface.astro', owner: 'A0', kind: 'spacing',
    current: 'clamp(1.5rem,4vw,3rem) results padding; local medallion sizes',
    replacement: 'var(--ke-free-results-padding); var(--ke-free-medallion-hero/sticky-*)',
    browserExpectation: 'hero-to-sticky medallion transition keeps existing size endpoints',
  },
  {
    consumer: 'site/src/components/FreeCollectionSurface.astro', owner: 'A0', kind: 'elevation',
    current: '0 18px 45px rgba(72,45,25,.08); rgba(38,83,34,.18/.22)',
    replacement: 'var(--ke-elevation-free-hero); var(--ke-color-free-medallion-shadow/-strong)',
    browserExpectation: 'free hero and sticky medallion depth remain unchanged',
  },
  {
    consumer: 'site/src/components/UnusualListingSurface.astro', owner: 'A0', kind: 'color',
    current: '#fffaf2 -> #eee1d2; brand .22 and accent .14 radial glows',
    replacement: 'var(--ke-color-unusual-hero-start/end); var(--ke-color-unusual-hero-brand-glow/accent-glow)',
    browserExpectation: 'unusual collection keeps its distinct editorial mix',
  },
  {
    consumer: 'site/src/components/UnusualListingSurface.astro', owner: 'A0', kind: 'type',
    current: 'clamp(4rem,10vw,8.5rem) / .78 / -.075em',
    replacement: 'font: var(--ke-type-display-unusual); letter-spacing: var(--ke-type-display-unusual-letter)',
    browserExpectation: 'oversized title remains the intentional unusual variant',
  },
  {
    consumer: 'site/src/components/UnusualListingSurface.astro', owner: 'A0', kind: 'shape',
    current: '32/24px hero; 28/20px result panels',
    replacement: 'var(--ke-shape-radius-editorial-hero/-compact); var(--ke-unusual-panel-radius/-compact)',
    browserExpectation: 'desktop/mobile clipping and panel grouping remain unchanged',
  },
  {
    consumer: 'site/src/components/GastronomyCollectionSurface.astro', owner: 'A0', kind: 'color',
    current: '#f4f1ea collection-state fallback',
    replacement: 'var(--ke-color-collection-state-surface)',
    browserExpectation: 'neutral loading/empty state does not become a warning or success state',
  },
  {
    consumer: 'site/src/components/GastronomyCollectionSurface.astro', owner: 'A0', kind: 'spacing',
    current: '.85rem 1rem; 1rem radius',
    replacement: 'var(--ke-collection-state-padding-block/inline); var(--ke-collection-state-radius)',
    browserExpectation: 'collection state geometry is unchanged',
  },
  {
    consumer: 'site/src/components/PersonalFeedSlot.astro', owner: 'A0', kind: 'color',
    current: '#f5ede2; #d2c5b7; #15110f',
    replacement: 'var(--ke-color-personal-feed-surface); var(--ke-color-personal-feed-media-placeholder); var(--ke-color-personal-feed-contain-band)',
    browserExpectation: 'full-bleed canvas and contain bands preserve present contrast',
  },
  {
    consumer: 'site/src/components/PersonalFeedSlot.astro', owner: 'A0', kind: 'spacing',
    current: 'clamp(2rem,4vw,4.5rem); clamp(.75rem,1.8vw,1.25rem)',
    replacement: 'var(--ke-personal-feed-section-padding); var(--ke-personal-feed-grid-gap)',
    browserExpectation: 'event-detail section spacing and card gaps remain unchanged',
  },
  {
    consumer: 'site/src/components/PersonalFeedSlot.astro', owner: 'A0', kind: 'type',
    current: 'clamp(2rem,4vw,4rem) section heading',
    replacement: 'var(--ke-personal-feed-heading-size)',
    browserExpectation: 'heading scale remains distinct from home feed maximum',
  },
  {
    consumer: 'site/src/components/FocusLabBadge.astro', owner: 'A0', kind: 'color',
    current: '#ffc10d 13% on white; brand border 25%',
    replacement: 'var(--ke-color-focus-brand-marker-surface); var(--ke-color-focus-brand-marker-border)',
    browserExpectation: 'lab marker keeps yellow prototype identity',
  },
  {
    consumer: 'site/src/components/FocusLabBadge.astro', owner: 'A0', kind: 'icon',
    current: '1.55rem compact image; 2rem hero image',
    replacement: 'Semantic/asset role action=24px; feature=32px via var(--ke-focus-lab-icon-size/-hero)',
    browserExpectation: 'flask image remains contain; no fifth icon-size role',
  },
  {
    consumer: 'site/src/components/FocusGroupFeedback.astro', owner: 'A0', kind: 'color',
    current: '#fff/#fffdf8; #d7f0ec; #f8e5d8; #cdbfad; accent focus mixes',
    replacement: 'var(--ke-color-focus-panel/sheet/control-*); var(--ke-color-border-input); var(--ke-color-focus-ring-accent*)',
    browserExpectation: 'selected response kinds remain visually distinct and keyboard-visible',
  },
  {
    consumer: 'site/src/components/FocusGroupFeedback.astro', owner: 'A0', kind: 'shape',
    current: '1.5rem panel/dialog; .75/.9/1rem controls',
    replacement: 'var(--ke-focus-panel/dialog/control/input/boundary-radius)',
    browserExpectation: 'desktop dialog and mobile sheet retain current silhouettes',
  },
  {
    consumer: 'site/src/components/FocusGroupFeedback.astro', owner: 'A0', kind: 'elevation',
    current: 'shadow-1; shadow-3; 0 4px 14px rgba(72,45,25,.10)',
    replacement: 'var(--ke-elevation-focus-panel); var(--ke-elevation-overlay); var(--ke-elevation-focus-selected)',
    browserExpectation: 'panel, dialog and selected switch depth remain unchanged',
  },
  {
    consumer: 'site/src/components/FocusGroupFeedback.astro', owner: 'A0', kind: 'icon',
    current: 'text × in 44px close control',
    replacement: '<SemanticIcon name="close" role="control" />',
    browserExpectation: 'one canonical close SVG; 20px glyph inside the existing 44px target',
  },
  {
    consumer: 'site/src/components/FocusGroupInviteShare.astro', owner: 'A0', kind: 'color',
    current: '#fff/#fffdf8; #cdbfad; 55% accent focus; warning boundary',
    replacement: 'focus panel/sheet/input tokens; var(--ke-color-focus-ring-accent-medium); status warning tokens',
    browserExpectation: 'URL input, QR block and boundary retain current hierarchy',
  },
  {
    consumer: 'site/src/components/FocusPwaInstallAction.astro', owner: 'A0', kind: 'color',
    current: '#fff panel; accent 13% radial; fallback #166a64 label',
    replacement: 'var(--ke-color-focus-panel-surface); accent glow from action-accent; var(--ke-color-status-success-foreground)',
    browserExpectation: 'panel pixels stay warm/white; label joins canonical success foreground',
  },
  {
    consumer: 'site/src/components/FocusPwaInstallAction.astro', owner: 'A0', kind: 'elevation',
    current: '0 8px 25px rgba(72,45,25,.08)',
    replacement: 'var(--ke-elevation-focus-pwa)',
    browserExpectation: 'same shallow install-card elevation',
  },
  {
    consumer: 'site/src/components/FocusGroupLabPanel.astro', owner: 'A0', kind: 'color',
    current: '#dbc9b5 panel; #d8c8b5 score; #cdbfad input; #fff/#fffdf8 surfaces',
    replacement: 'var(--ke-color-border-panel-warm/score/input); focus panel/sheet/control surfaces',
    browserExpectation: 'panel, score grid and form fields preserve their separate affordances',
  },
  {
    consumer: 'site/src/components/FocusGroupLabPanel.astro', owner: 'A0', kind: 'elevation',
    current: '0 14px 38px rgba(72,45,25,.10); dialog rgba(34,26,20,.22)',
    replacement: 'var(--ke-elevation-focus-lab); var(--ke-elevation-focus-dialog)',
    browserExpectation: 'embedded panel remains shallower than modal dialog',
  },
  {
    consumer: 'site/src/components/FocusGroupLabPanel.astro', owner: 'A0', kind: 'icon',
    current: 'two text × close controls',
    replacement: '<SemanticIcon name="close" role="control" />',
    browserExpectation: 'both dialogs share one close identity and 44px target',
  },
  {
    consumer: 'site/src/components/FocusGroupInviteIntake.astro', owner: 'A0', kind: 'color',
    current: '#fffdf8 card; #fff4d6/#ecd29a/#5d3c12 prize; #f8f2e9 consent; #e6f4ef confirmed',
    replacement: 'focus card/surface tokens; prize border/surface/text; consent/confirmed surface tokens',
    browserExpectation: 'prize, consent and confirmed states remain semantically distinct',
  },
  {
    consumer: 'site/src/components/FocusGroupInviteIntake.astro', owner: 'A0', kind: 'media',
    current: '5.75rem brand image with 24% radius and 0 12px 30px shadow',
    replacement: 'var(--ke-focus-brand-image-size/radius); var(--ke-elevation-focus-brand)',
    browserExpectation: 'brand asset keeps exact UI footprint; intrinsic 192x192 stays media metadata',
  },
  {
    consumer: 'site/src/components/FocusGroupInviteIntake.astro', owner: 'A0', kind: 'icon',
    current: 'text ✓ success; 2rem local spinner',
    replacement: '<SemanticIcon name="check" role="feature" />; feature=32px spinner role',
    browserExpectation: 'success/spinner use the existing feature role, not local dimensions',
  },
  {
    consumer: 'site/src/components/FocusGroupThankYou.astro', owner: 'A0', kind: 'color',
    current: 'brand border 24%; yellow marker #ffc10d; white surface',
    replacement: 'var(--ke-color-border-brand-emphasis); var(--ke-color-focus-brand-marker); var(--ke-color-focus-panel-surface)',
    browserExpectation: 'partner/prize panel remains a gratitude surface, not warning status',
  },
  {
    consumer: 'site/src/components/listings/MobileListingRailSurface.astro', owner: 'A0', kind: 'icon',
    current: '18px trend wrapper; text ⌄ disclosure',
    replacement: 'control=20px wrapper; <SemanticIcon name="chevron-down" role="inline" />',
    browserExpectation: 'group trend and city disclosure use central roles without changing targets',
  },
  {
    consumer: 'site/src/components/listings/MobileDateAccessory.astro', owner: 'A0', kind: 'icon',
    current: 'text ×, ←, →; calendar via className role classes',
    replacement: 'SemanticIcon close/control, arrow-left/control, arrow-right/control; calendar/action',
    browserExpectation: 'calendar sheet controls keep 44px targets and stable focus order',
  },
  {
    consumer: 'site/src/components/lab/MobileEventReviewPage.astro', owner: 'A0', kind: 'color',
    current: '#46372d/#34271f/#8f3517/#7e2d12/#241c17/#b54d22 and brand alpha borders',
    replacement: 'var(--ke-color-event-meta-*)',
    browserExpectation: 'all accepted mobile-review versions retain their metadata hierarchy',
  },
  {
    consumer: 'site/src/components/lab/MobileEventReviewPage.astro', owner: 'A0', kind: 'type',
    current: '.74rem weekday; 1.12rem date; 1.2rem time; .92rem place',
    replacement: 'var(--ke-type-event-meta-weekday/date/time/place-size)',
    browserExpectation: 'metadata baselines and wrap behavior remain unchanged',
  },
  {
    consumer: 'site/src/components/EventMediaRail.astro', owner: 'M0', kind: 'media',
    current: '92px thumbnail contract and layout-owned more-tile styles',
    replacement: 'var(--ke-media-rail-thumbnail-size); media-rail surface/text/type tokens',
    browserExpectation: 'gallery-thumbnails stays current v1 while hero-selector/poster-strip become named variants',
  },
  {
    consumer: 'site/src/components/DesktopEventPage.astro', owner: 'A0', kind: 'media',
    current: 'inline hero-selector/poster-strip lookalikes and local media wrappers',
    replacement: 'consume M0 EventMediaRail named variants; bind surfaces through --ke-color-media-rail-*',
    browserExpectation: 'no duplicate rail root; same asset order, fit, focus and gallery behavior',
  },
] as const satisfies readonly SurfaceFoundationBinding[];

export const surfaceFoundationBindingsFor = (consumer: string) =>
  SURFACE_FOUNDATION_BINDINGS.filter((binding) => binding.consumer === consumer);
