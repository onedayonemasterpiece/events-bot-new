export type ServiceContinuityKind = 'color' | 'type' | 'spacing' | 'shape' | 'elevation' | 'icon';

export interface ServiceContinuityBinding {
  consumer: string;
  sourceHead: string;
  owner: 'A0';
  kind: ServiceContinuityKind;
  current: string;
  replacement: string;
  browserExpectation: string;
  negativeControl: string;
}

/** Current-head A0 handoff for public service actions. */
export const SERVICE_CONTINUITY_BINDINGS = [
  {
    consumer: 'site/src/components/ServiceShareAction.astro',
    sourceHead: 'f9c8a0087607707c130b1d0def33f76519721fef', owner: 'A0', kind: 'color',
    current: 'brand alpha border/hover; success #3f8a5b/#2f7449; error #a85645/#8b4537; shortcut local colors',
    replacement: 'var(--ke-color-service-share-*)',
    browserExpectation: 'default, hover, success and error receipts keep current contrast',
    negativeControl: 'share controller state names, aria-live status and fallback URL remain unchanged',
  },
  {
    consumer: 'site/src/components/ServiceShareAction.astro',
    sourceHead: 'f9c8a0087607707c130b1d0def33f76519721fef', owner: 'A0', kind: 'spacing',
    current: 'local root/prompt/controls/button/shortcut gaps, basis, padding and minima',
    replacement: 'var(--ke-service-share-*)',
    browserExpectation: 'mobile single action and desktop three-intent wrapping remain unchanged',
    negativeControl: 'desktop mode marker stays a compatibility marker, not a new visual variant',
  },
  {
    consumer: 'site/src/components/ServiceShareAction.astro',
    sourceHead: 'f9c8a0087607707c130b1d0def33f76519721fef', owner: 'A0', kind: 'type',
    current: 'local prompt/button/shortcut/fallback size, weight and line values',
    replacement: 'var(--ke-service-share-*-size/weight/line)',
    browserExpectation: 'labels and keyboard badges keep current baselines and wrapping',
    negativeControl: 'wordmark remains brand media and does not become text',
  },
  {
    consumer: 'site/src/components/ServiceShareAction.astro',
    sourceHead: 'f9c8a0087607707c130b1d0def33f76519721fef', owner: 'A0', kind: 'icon',
    current: 'three inline success checks; inline share; inline image; external mask link asset; local 1.15rem sizing',
    replacement: '<SemanticIcon name="share|image|link|check" role="control" />; var(--ke-service-share-icon-size)',
    browserExpectation: 'each intent and success receipt keeps one recognizable 20px glyph',
    negativeControl: 'remove duplicate inline paths, linkIconUrl and mask span; exactly four size roles remain',
  },
  {
    consumer: 'site/src/components/ServiceShareAction.astro',
    sourceHead: 'f9c8a0087607707c130b1d0def33f76519721fef', owner: 'A0', kind: 'elevation',
    current: 'shortcut 0 1px 0 rgba(0,0,0,.16)',
    replacement: 'var(--ke-elevation-service-share-shortcut)',
    browserExpectation: 'keyboard badges retain their shallow physical-key cue',
    negativeControl: 'action buttons remain flat outside receipt state',
  },
  {
    consumer: 'site/src/components/PwaInstallAction.astro',
    sourceHead: 'f9c8a0087607707c130b1d0def33f76519721fef', owner: 'A0', kind: 'color',
    current: 'footer inverse alpha panel and fixed #25211e presentation with warm install CTA',
    replacement: 'var(--ke-pwa-install-*); var(--ke-color-pwa-install-*)',
    browserExpectation: 'footer and fixed presentation modes retain their current inverse hierarchy',
    negativeControl: 'PWA readiness/hidden state and controller behavior remain unchanged',
  },
  {
    consumer: 'site/src/components/PwaInstallAction.astro',
    sourceHead: 'f9c8a0087607707c130b1d0def33f76519721fef', owner: 'A0', kind: 'spacing',
    current: 'local panel/button/presentation gap, padding, radius, inset, max-width and pressed offset',
    replacement: 'var(--ke-pwa-install-*); var(--ke-pwa-install-presentation-*)',
    browserExpectation: 'footer panel and fixed card retain current footprints at all safe-area insets',
    negativeControl: 'fixed presentation z-index remains a named product layer, not a generic layer rewrite',
  },
  {
    consumer: 'site/src/components/PwaInstallAction.astro',
    sourceHead: 'f9c8a0087607707c130b1d0def33f76519721fef', owner: 'A0', kind: 'type',
    current: 'local title/copy/status and presentation-title typography',
    replacement: 'var(--ke-pwa-install-*-size/line/weight)',
    browserExpectation: 'install guidance keeps current two-level hierarchy',
    negativeControl: 'guidance copy is unchanged',
  },
  {
    consumer: 'site/src/components/PwaInstallAction.astro',
    sourceHead: 'f9c8a0087607707c130b1d0def33f76519721fef', owner: 'A0', kind: 'icon',
    current: 'Icon install with local width/height 1.25rem',
    replacement: '<SemanticIcon name="install" role="control" />; var(--ke-pwa-install-button-icon-size)',
    browserExpectation: 'install CTA keeps a 20px download-to-device glyph',
    negativeControl: 'no local icon dimension and no fifth icon role',
  },
  {
    consumer: 'site/src/components/PwaInstallAction.astro',
    sourceHead: 'f9c8a0087607707c130b1d0def33f76519721fef', owner: 'A0', kind: 'elevation',
    current: 'footer inset 0 1px 0; presentation 0 24px 64px',
    replacement: 'var(--ke-pwa-install-inset); var(--ke-elevation-pwa-install-presentation)',
    browserExpectation: 'footer panel remains embedded while fixed presentation floats above content',
    negativeControl: 'the two elevations remain intentionally distinct',
  },
] as const satisfies readonly ServiceContinuityBinding[];

export const serviceContinuityBindingsFor = (consumer: string) =>
  SERVICE_CONTINUITY_BINDINGS.filter((binding) => binding.consumer === consumer);
