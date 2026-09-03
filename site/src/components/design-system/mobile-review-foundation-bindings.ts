export type MobileReviewFoundationKind = 'color' | 'type' | 'spacing' | 'shape' | 'elevation' | 'icon' | 'media';

export interface MobileReviewFoundationBinding {
  consumer: 'site/src/components/lab/MobileEventReviewPage.astro';
  sourceHead: string;
  owner: 'A0';
  revisions: readonly string[];
  kind: MobileReviewFoundationKind;
  current: string;
  replacement: string;
  browserExpectation: string;
  negativeControl: string;
}

/** Current-head A0 handoff for accepted mobile review compositions. */
export const MOBILE_REVIEW_FOUNDATION_BINDINGS = [
  {
    consumer: 'site/src/components/lab/MobileEventReviewPage.astro',
    sourceHead: 'f991a074715896420d4bb90049ea48239b0a0f9b',
    owner: 'A0', revisions: ['v2'], kind: 'color',
    current: 'brand alpha border; coral 7% panel; #46372d text; #8f3517 date',
    replacement: 'var(--ke-color-mobile-review-meta-v2-border/surface/text/date)',
    browserExpectation: 'v2 metadata panel keeps the same warm grouped treatment',
    negativeControl: 'v3/v4 dense metadata remains a separate accepted composition',
  },
  {
    consumer: 'site/src/components/lab/MobileEventReviewPage.astro',
    sourceHead: 'f991a074715896420d4bb90049ea48239b0a0f9b',
    owner: 'A0', revisions: ['v2'], kind: 'type',
    current: '1.05rem/920/1.15 date; .92rem/780/1.24 place',
    replacement: 'var(--ke-mobile-review-meta-v2-date-*); var(--ke-mobile-review-meta-v2-place-*)',
    browserExpectation: 'date/place baselines and wraps remain unchanged',
    negativeControl: 'no display or generic body type role is redefined',
  },
  {
    consumer: 'site/src/components/lab/MobileEventReviewPage.astro',
    sourceHead: 'f991a074715896420d4bb90049ea48239b0a0f9b',
    owner: 'A0', revisions: ['v2'], kind: 'spacing',
    current: '.2rem .45rem gap; .05rem offset; .72rem .82rem padding; 14px radius',
    replacement: 'var(--ke-mobile-review-meta-v2-gap-*); var(--ke-mobile-review-meta-v2-margin-top); var(--ke-mobile-review-meta-v2-padding-*); var(--ke-mobile-review-meta-v2-radius)',
    browserExpectation: 'v2 metadata footprint remains unchanged',
    negativeControl: 'event occurrence grouping and source order are untouched',
  },
  {
    consumer: 'site/src/components/lab/MobileEventReviewPage.astro',
    sourceHead: 'f991a074715896420d4bb90049ea48239b0a0f9b',
    owner: 'A0', revisions: ['v3', 'v4'], kind: 'color',
    current: '#34271f; brand alpha 22%/14%; #b54d22/#fffaf2; #7e2d12/#241c17',
    replacement: 'var(--ke-color-mobile-review-meta-dense-*); existing --ke-color-event-meta-* roles',
    browserExpectation: 'accepted weekday/date/time hierarchy remains unchanged',
    negativeControl: 'v3/v4 keep their denser anatomy; only ownership moves',
  },
  {
    consumer: 'site/src/components/lab/MobileEventReviewPage.astro',
    sourceHead: 'f991a074715896420d4bb90049ea48239b0a0f9b',
    owner: 'A0', revisions: ['v3', 'v4'], kind: 'type',
    current: '.74rem weekday; 1.12rem date; 1.2rem time; .92rem place; -.025em date/time letter',
    replacement: 'existing --ke-type-event-meta-* roles; var(--ke-mobile-review-meta-letter)',
    browserExpectation: 'metadata baselines and wrap behavior remain unchanged',
    negativeControl: 'no fifth typography hierarchy is created',
  },
  {
    consumer: 'site/src/components/lab/MobileEventReviewPage.astro',
    sourceHead: 'f991a074715896420d4bb90049ea48239b0a0f9b',
    owner: 'A0', revisions: ['v3', 'v4'], kind: 'spacing',
    current: '.58rem panel gap; .78/.08/.82rem padding; .5rem when gap; weekday/place local padding',
    replacement: 'var(--ke-mobile-review-meta-dense-*); var(--ke-mobile-review-meta-when-gap); var(--ke-mobile-review-meta-weekday-padding-*); var(--ke-mobile-review-meta-place-padding-top)',
    browserExpectation: 'dense panel vertical rhythm remains unchanged',
    negativeControl: 'full-bleed hero composition is not altered',
  },
  {
    consumer: 'site/src/components/lab/MobileEventReviewPage.astro',
    sourceHead: 'f991a074715896420d4bb90049ea48239b0a0f9b',
    owner: 'A0', revisions: ['v4', 'accepted-v5', 'accepted-v6', 'accepted-v7', 'accepted-v8'], kind: 'spacing',
    current: '42px poster travel; local medallion/details offsets; 1.5rem continuation overlap',
    replacement: 'var(--ke-mobile-review-poster-travel); var(--ke-mobile-review-medallion/details-*); var(--ke-mobile-review-continuation-*)',
    browserExpectation: 'poster movement and continuation join remain pixel-equivalent',
    negativeControl: 'parallax runtime variables and accepted variant selection are untouched',
  },
  {
    consumer: 'site/src/components/lab/MobileEventReviewPage.astro',
    sourceHead: 'f991a074715896420d4bb90049ea48239b0a0f9b',
    owner: 'A0', revisions: ['accepted-v5'], kind: 'color',
    current: 'rgba(240,225,207,.88) -> rgba(250,240,226,.9) -> rgba(255,249,240,.86) -> transparent',
    replacement: 'var(--ke-color-mobile-review-continuation-v5-*); var(--ke-color-mobile-review-continuation-transparent)',
    browserExpectation: 'v5 foreground fade remains unchanged at 0/6rem/16rem/32rem',
    negativeControl: 'v6-v8 atmospheric rise is not collapsed into v5',
  },
  {
    consumer: 'site/src/components/lab/MobileEventReviewPage.astro',
    sourceHead: 'f991a074715896420d4bb90049ea48239b0a0f9b',
    owner: 'A0', revisions: ['accepted-v6', 'accepted-v7', 'accepted-v8'], kind: 'color',
    current: 'six-stop continuation gradient around clamp(28rem,110vw,32rem)',
    replacement: 'var(--ke-color-mobile-review-continuation-*); var(--ke-mobile-review-continuation-rise/*-offset/*-tail)',
    browserExpectation: 'all accepted continuous-crop versions retain one identical atmosphere',
    negativeControl: 'accepted revisions remain separately addressable for behavior diagnostics',
  },
  {
    consumer: 'site/src/components/lab/MobileEventReviewPage.astro',
    sourceHead: 'f991a074715896420d4bb90049ea48239b0a0f9b',
    owner: 'A0', revisions: ['v2', 'v3', 'v4', 'accepted-v5', 'accepted-v6', 'accepted-v7', 'accepted-v8'], kind: 'shape',
    current: '66ch open prose; .25/1.15/.35rem padding; 16px source gate; rgba warm gate',
    replacement: 'var(--ke-mobile-review-prose-*); var(--ke-mobile-review-source-*); var(--ke-color-mobile-review-source-*)',
    browserExpectation: 'open prose measure and source-gate hierarchy remain unchanged',
    negativeControl: 'source disclosure copy and lock identity are untouched',
  },
  {
    consumer: 'site/src/components/lab/MobileEventReviewPage.astro',
    sourceHead: 'f991a074715896420d4bb90049ea48239b0a0f9b',
    owner: 'A0', revisions: ['v2', 'v3', 'v4'], kind: 'color',
    current: '#292521 dock; white alpha .08/.10/.13/.20; #fffaf2 controls',
    replacement: 'var(--ke-color-mobile-review-dock-*)',
    browserExpectation: 'dark dock and secondary control states remain unchanged',
    negativeControl: 'primary CTA color continues to come from its canonical action component',
  },
  {
    consumer: 'site/src/components/lab/MobileEventReviewPage.astro',
    sourceHead: 'f991a074715896420d4bb90049ea48239b0a0f9b',
    owner: 'A0', revisions: ['v2', 'v3', 'v4'], kind: 'spacing',
    current: '.42rem gap; .48rem dock padding; 18/14/13px radii; 52/48px controls; compact widths',
    replacement: 'var(--ke-mobile-review-dock-*); var(--ke-mobile-review-dock-compact-*); var(--ke-mobile-review-dock-v2-*)',
    browserExpectation: 'dock packing and compact label selection remain unchanged',
    negativeControl: 'container-query action ordering and labels are untouched',
  },
  {
    consumer: 'site/src/components/lab/MobileEventReviewPage.astro',
    sourceHead: 'f991a074715896420d4bb90049ea48239b0a0f9b',
    owner: 'A0', revisions: ['v2', 'v3', 'v4'], kind: 'icon',
    current: 'local .icon width/height 1.28rem',
    replacement: 'width/height: var(--ke-mobile-review-dock-icon-size) -> control=20px',
    browserExpectation: 'heart/calendar/share/ticket glyphs use the central control role inside unchanged targets',
    negativeControl: 'no 1.28rem alias and no fifth central icon role',
  },
  {
    consumer: 'site/src/components/lab/MobileEventReviewPage.astro',
    sourceHead: 'f991a074715896420d4bb90049ea48239b0a0f9b',
    owner: 'A0', revisions: ['v3'], kind: 'icon',
    current: 'text pseudo-element ✓ inside 1.22rem selected receipt',
    replacement: '<SemanticIcon name="check" role="inline" /> inside var(--ke-mobile-review-check-container-size)',
    browserExpectation: 'selected like keeps the existing visible non-colour receipt and screen-reader state',
    negativeControl: 'pseudo text glyph is removed; aria-pressed semantics and heart state remain intact',
  },
  {
    consumer: 'site/src/components/lab/MobileEventReviewPage.astro',
    sourceHead: 'f991a074715896420d4bb90049ea48239b0a0f9b',
    owner: 'A0', revisions: ['v3', 'v4'], kind: 'elevation',
    current: 'dock shadow; v3 selected triple ring/shadow and -1px offset',
    replacement: 'var(--ke-elevation-mobile-review-dock); var(--ke-elevation-mobile-review-like-selected); var(--ke-mobile-review-like-selected-offset)',
    browserExpectation: 'v3 emphasized and v4 simplified selected states remain distinct',
    negativeControl: 'v4 stays shadowless and does not inherit v3 emphasis',
  },
] as const satisfies readonly MobileReviewFoundationBinding[];
