export type ProductThemeContinuityKind = 'color' | 'type' | 'spacing' | 'shape' | 'elevation' | 'icon';

export interface ProductThemeContinuityBinding {
  consumer: string;
  sourceHead: string;
  owner: 'A0';
  kind: ProductThemeContinuityKind;
  current: string;
  replacement: string;
  browserExpectation: string;
  negativeControl: string;
}

/** Current-head A0 handoff for residual product-theme consumers. */
export const PRODUCT_THEME_CONTINUITY_BINDINGS = [
  {
    consumer: 'site/src/components/FocusGroupThankYou.astro',
    sourceHead: 'ede8b0ce65db145a21b4c83cc5b812d1eb2b2f99', owner: 'A0', kind: 'color',
    current: 'brand border 24%; yellow marker/glow; white surface; ink/copy/muted fallbacks',
    replacement: 'var(--ke-color-focus-thanks-*); existing focus/brand semantic roles',
    browserExpectation: 'partner/prize panel remains a gratitude surface, not warning status',
    negativeControl: 'the theatre partner logo remains official identity media',
  },
  {
    consumer: 'site/src/components/FocusGroupThankYou.astro',
    sourceHead: 'ede8b0ce65db145a21b4c83cc5b812d1eb2b2f99', owner: 'A0', kind: 'spacing',
    current: '180px/.42fr split, responsive gap/padding, 260/230px logo bounds and compact 150px split',
    replacement: 'var(--ke-focus-thanks-*)',
    browserExpectation: 'desktop/compact/mobile gratitude composition keeps current footprint',
    negativeControl: 'logo dimensions remain media geometry and never become icon roles',
  },
  {
    consumer: 'site/src/components/FocusGroupThankYou.astro',
    sourceHead: 'ede8b0ce65db145a21b4c83cc5b812d1eb2b2f99', owner: 'A0', kind: 'type',
    current: 'local eyebrow, heading, copy and notice typography',
    replacement: 'var(--ke-focus-thanks-*-size/weight/line/letter)',
    browserExpectation: 'heading and rule disclaimer wrap at the same points',
    negativeControl: 'competition disclaimer copy remains unchanged',
  },
  {
    consumer: 'site/src/components/FocusGroupThankYou.astro',
    sourceHead: 'ede8b0ce65db145a21b4c83cc5b812d1eb2b2f99', owner: 'A0', kind: 'elevation',
    current: 'shadow-1 fallback',
    replacement: 'var(--ke-elevation-focus-thanks)',
    browserExpectation: 'gratitude panel retains current shallow depth',
    negativeControl: 'no dialog-depth shadow is introduced',
  },
  {
    consumer: 'site/src/components/FocusPwaInstallAction.astro',
    sourceHead: 'ede8b0ce65db145a21b4c83cc5b812d1eb2b2f99', owner: 'A0', kind: 'spacing',
    current: '.8rem root gap; .35rem copy gap; local guidance radius/padding/gap',
    replacement: 'var(--ke-focus-pwa-*)',
    browserExpectation: 'checking, install-ready and resolved states keep current geometry',
    negativeControl: 'PWA controller and runtime data hooks are untouched',
  },
  {
    consumer: 'site/src/components/FocusPwaInstallAction.astro',
    sourceHead: 'ede8b0ce65db145a21b4c83cc5b812d1eb2b2f99', owner: 'A0', kind: 'type',
    current: 'local eyebrow, heading, copy, status and guidance typography',
    replacement: 'var(--ke-focus-pwa-*-size/weight/line/letter)',
    browserExpectation: 'install explanation and fallback guidance retain current hierarchy',
    negativeControl: 'button label and browser-specific guidance remain unchanged',
  },
  {
    consumer: 'site/src/components/FocusPwaInstallAction.astro',
    sourceHead: 'ede8b0ce65db145a21b4c83cc5b812d1eb2b2f99', owner: 'A0', kind: 'color',
    current: '13% accent radial; focus/status/info semantic colors already partly bound',
    replacement: 'existing focus/status roles plus var(--ke-focus-pwa-glow-size)',
    browserExpectation: 'warm install glow and info guidance remain unchanged',
    negativeControl: 'do not create a new PWA palette',
  },
  {
    consumer: 'site/src/components/FocusConnectivityDiagnostic.astro',
    sourceHead: 'ede8b0ce65db145a21b4c83cc5b812d1eb2b2f99', owner: 'A0', kind: 'color',
    current: 'warm receipt/action/result/success/error/input literals',
    replacement: 'var(--ke-color-connectivity-*)',
    browserExpectation: 'probe outcomes and copy receipt keep current contrast',
    negativeControl: 'success/error remain connectivity outcomes, not generic badge colors',
  },
  {
    consumer: 'site/src/components/FocusConnectivityDiagnostic.astro',
    sourceHead: 'ede8b0ce65db145a21b4c83cc5b812d1eb2b2f99', owner: 'A0', kind: 'spacing',
    current: 'safe-area page, 520px card, result/receipt/definition/send/input/copy local geometry',
    replacement: 'var(--ke-connectivity-*)',
    browserExpectation: 'one-column diagnostic and receipt remain usable at 320px+',
    negativeControl: 'probe concurrency, cooldown and compact receipt format remain unchanged',
  },
  {
    consumer: 'site/src/components/FocusConnectivityDiagnostic.astro',
    sourceHead: 'ede8b0ce65db145a21b4c83cc5b812d1eb2b2f99', owner: 'A0', kind: 'type',
    current: 'local eyebrow/heading/status/result/receipt/send/input/copy typography',
    replacement: 'var(--ke-connectivity-*-size/weight/line/letter)',
    browserExpectation: 'result labels and machine-readable receipt remain legible without new wraps',
    negativeControl: 'monospace receipt remains intentional technical data',
  },
  {
    consumer: 'site/src/components/FocusConnectivityDiagnostic.astro',
    sourceHead: 'ede8b0ce65db145a21b4c83cc5b812d1eb2b2f99', owner: 'A0', kind: 'elevation',
    current: 'card 0 16px 40px; action 0 10px 26px',
    replacement: 'var(--ke-elevation-connectivity-card/action)',
    browserExpectation: 'diagnostic card and primary probe action retain current depth',
    negativeControl: 'result rows remain flat',
  },
  {
    consumer: 'site/src/components/InterestProfile.astro',
    sourceHead: 'ede8b0ce65db145a21b4c83cc5b812d1eb2b2f99', owner: 'A0', kind: 'color',
    current: 'local --focus-* palette aliases plus repeated raw mint/teal/gold/workspace/map/digest/tag states',
    replacement: 'var(--ke-color-personalization-*) and var(--ke-elevation-personalization-*)',
    browserExpectation: 'consent/profile/confidence evidence language remains unchanged',
    negativeControl: 'teal interaction, gold confidence and coral signal stay semantically distinct',
  },
  {
    consumer: 'site/src/components/InterestProfile.astro',
    sourceHead: 'ede8b0ce65db145a21b4c83cc5b812d1eb2b2f99', owner: 'A0', kind: 'spacing',
    current: 'local notice/consent/workspace/legend/group/interest/choice/meter/map/digest/recommendation/static geometry',
    replacement: 'var(--ke-personalization-*) in product-theme-continuity-foundations.css',
    browserExpectation: 'three-column desktop profile, two-column tablet and one-column mobile layouts retain current behavior',
    negativeControl: 'localStorage state, ordering, eligibility and recommendation logic remain untouched',
  },
  {
    consumer: 'site/src/components/InterestProfile.astro',
    sourceHead: 'ede8b0ce65db145a21b4c83cc5b812d1eb2b2f99', owner: 'A0', kind: 'type',
    current: 'local prototype/kicker/heading/group/choice/meter/map/digest/recommendation/tag typography',
    replacement: 'var(--ke-personalization-*-size/weight/line/letter)',
    browserExpectation: 'explicit choice, interest index and evidence remain visually separable',
    negativeControl: 'no score is represented as an explicit preference',
  },
  {
    consumer: 'site/src/components/InterestProfile.astro',
    sourceHead: 'ede8b0ce65db145a21b4c83cc5b812d1eb2b2f99', owner: 'A0', kind: 'icon',
    current: 'inline local lock SVG at width/height 1.65rem inside 3.25rem container',
    replacement: '<SemanticIcon name="lock" role="action" /> inside var(--ke-personalization-consent-icon-container-size)',
    browserExpectation: 'consent panel keeps a recognizable 24px lock',
    negativeControl: 'remove duplicate inline path; exactly four central icon roles remain',
  },
] as const satisfies readonly ProductThemeContinuityBinding[];

export const productThemeContinuityBindingsFor = (consumer: string) =>
  PRODUCT_THEME_CONTINUITY_BINDINGS.filter((binding) => binding.consumer === consumer);
