export type PartnershipFoundationKind = 'color' | 'type' | 'spacing' | 'shape' | 'icon' | 'media';

export interface PartnershipFoundationBinding {
  consumer: string;
  sourceHead: string;
  owner: 'A0';
  kind: PartnershipFoundationKind;
  current: string;
  replacement: string;
  browserExpectation: string;
  negativeControl: string;
}

/** Current-head A0 handoff for partnership routes. */
export const PARTNERSHIP_FOUNDATION_BINDINGS = [
  {
    consumer: 'site/src/pages/partnerstvo/index.astro',
    sourceHead: '170cd7e7408101359ae450667c8c46ac08927ff5', owner: 'A0', kind: 'spacing',
    current: 'partnership-principles margin-top:1rem',
    replacement: 'margin-top: var(--ke-partnership-section-gap)',
    browserExpectation: 'the principles card keeps the current one-rem separation',
    negativeControl: 'canonical Button and generic section-card anatomy remain unchanged',
  },
  {
    consumer: 'site/src/pages/partners/index.astro',
    sourceHead: '170cd7e7408101359ae450667c8c46ac08927ff5', owner: 'A0', kind: 'spacing',
    current: 'local page/header/grid/tile/caption/mobile/narrow dimensions and gaps',
    replacement: 'var(--ke-partners-*); var(--ke-partner-*)',
    browserExpectation: 'dense 4/8-column placement and row-span composition remain unchanged at every breakpoint',
    negativeControl: 'per-partner grid placement custom properties and content order remain data owned',
  },
  {
    consumer: 'site/src/pages/partners/index.astro',
    sourceHead: '170cd7e7408101359ae450667c8c46ac08927ff5', owner: 'A0', kind: 'type',
    current: 'local heading, text-logo, caption and compact responsive typography',
    replacement: 'var(--ke-partners-heading-*); var(--ke-partner-text-logo-*); var(--ke-partner-meta-*); mobile/narrow roles',
    browserExpectation: 'route heading, text-only marks and compact labels wrap at the same points',
    negativeControl: 'partner logo artwork is not converted into text or UI iconography',
  },
  {
    consumer: 'site/src/pages/partners/index.astro',
    sourceHead: '170cd7e7408101359ae450667c8c46ac08927ff5', owner: 'A0', kind: 'media',
    current: '48/50/56px wide, 46px compact and 106/112px tall logo frames with responsive variants',
    replacement: 'var(--ke-partner-logo-*); var(--ke-partner-caption-*); mobile/narrow roles',
    browserExpectation: 'wide, tall, compact and logo-only partner marks retain current optical footprints',
    negativeControl: 'these are media-frame roles, never additions to the four central icon sizes',
  },
  {
    consumer: 'site/src/pages/partners/index.astro',
    sourceHead: '170cd7e7408101359ae450667c8c46ac08927ff5', owner: 'A0', kind: 'color',
    current: 'dynamic --partner-accent from info-partners data; global text/muted/ink UI aliases',
    replacement: 'retain --partner-accent as official partner identity data; use existing semantic text aliases for surrounding UI',
    browserExpectation: 'official partner identities and neutral surrounding copy remain unchanged',
    negativeControl: 'do not merge distinct official brand accents into the product palette',
  },
  {
    consumer: 'site/src/pages/partners/index.astro',
    sourceHead: '170cd7e7408101359ae450667c8c46ac08927ff5', owner: 'A0', kind: 'shape',
    current: '10px focus boundary; -1px hover lift; local focus width/offset',
    replacement: 'var(--ke-partner-focus-*); var(--ke-partner-hover-offset)',
    browserExpectation: 'keyboard focus and hover movement remain unchanged',
    negativeControl: 'logo image clipping stays absent; no decorative tile background is introduced',
  },
] as const satisfies readonly PartnershipFoundationBinding[];

export const partnershipFoundationBindingsFor = (consumer: string) =>
  PARTNERSHIP_FOUNDATION_BINDINGS.filter((binding) => binding.consumer === consumer);
