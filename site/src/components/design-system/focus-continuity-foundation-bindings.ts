export type FocusContinuityKind = 'color' | 'type' | 'spacing' | 'shape' | 'elevation' | 'icon';

export interface FocusContinuityBinding {
  consumer: string;
  sourceHead: string;
  owner: 'A0';
  kind: FocusContinuityKind;
  current: string;
  replacement: string;
  browserExpectation: string;
  negativeControl: string;
}

/**
 * Current-head handoff for the residual focus-group consumers.
 * Source heads are immutable census anchors, not permission gates.
 */
export const FOCUS_CONTINUITY_BINDINGS = [
  {
    consumer: 'site/src/components/FocusGroupFeedback.astro',
    sourceHead: '63e3bd94852f44da93fb491a671095c6a7f06380', owner: 'A0', kind: 'spacing',
    current: 'local panel/dialog/switcher/scale/issue/textarea/status gaps, padding and minima',
    replacement: 'var(--ke-focus-feedback-*)',
    browserExpectation: 'desktop dialog and mobile bottom sheet keep current wrapping, score packing and focus order',
    negativeControl: 'feedback kinds, form names, data hooks and runtime state remain unchanged',
  },
  {
    consumer: 'site/src/components/FocusGroupFeedback.astro',
    sourceHead: '63e3bd94852f44da93fb491a671095c6a7f06380', owner: 'A0', kind: 'type',
    current: 'local switcher, legend, hint, score, issue, textarea, boundary and status type values',
    replacement: 'var(--ke-focus-feedback-*-size/weight/line); existing --ke-type-focus-* roles',
    browserExpectation: 'question hierarchy and 0–10 control labels remain pixel-equivalent',
    negativeControl: 'relationship NPS and page usefulness remain semantically separate',
  },
  {
    consumer: 'site/src/components/FocusGroupInviteShare.astro',
    sourceHead: '63e3bd94852f44da93fb491a671095c6a7f06380', owner: 'A0', kind: 'color',
    current: '#fff/#fffdf8; #cdbfad; #44362d; 55% accent focus; warning fallbacks',
    replacement: 'existing focus/input/status roles plus var(--ke-color-focus-share-*)',
    browserExpectation: 'URL input, QR block and warning boundary retain current hierarchy',
    negativeControl: 'generated invitation URL, QR payload and share/copy behavior are untouched',
  },
  {
    consumer: 'site/src/components/FocusGroupInviteShare.astro',
    sourceHead: '63e3bd94852f44da93fb491a671095c6a7f06380', owner: 'A0', kind: 'spacing',
    current: 'two-column 1.15fr/280px/.85fr shell; local result, QR, buttons, status and boundary geometry',
    replacement: 'var(--ke-focus-share-*)',
    browserExpectation: 'desktop split, tablet QR split and compact stacked layout remain unchanged',
    negativeControl: 'QR remains media geometry and does not become an icon-size role',
  },
  {
    consumer: 'site/src/components/FocusGroupInviteShare.astro',
    sourceHead: '63e3bd94852f44da93fb491a671095c6a7f06380', owner: 'A0', kind: 'type',
    current: 'local monospace URL/input and QR/status/boundary typography',
    replacement: 'var(--ke-focus-share-*-size/weight/line); existing --ke-type-focus-heading',
    browserExpectation: 'long invitation URL and explanatory copy wrap at the same points',
    negativeControl: 'monospace remains intentional data presentation, not body typography',
  },
  {
    consumer: 'site/src/components/FocusGroupLabPanel.astro',
    sourceHead: '63e3bd94852f44da93fb491a671095c6a7f06380', owner: 'A0', kind: 'color',
    current: '#dbc9b5 panel; #d8c8b5 score; #cdbfad input; #fff/#fffdf8 surfaces; selected accent',
    replacement: 'existing --ke-color-border-panel-warm/score/input and focus surface/status roles',
    browserExpectation: 'panel, score grid, upload field and dialogs retain separate affordances',
    negativeControl: 'upload validation and feedback outbox semantics are untouched',
  },
  {
    consumer: 'site/src/components/FocusGroupLabPanel.astro',
    sourceHead: '63e3bd94852f44da93fb491a671095c6a7f06380', owner: 'A0', kind: 'spacing',
    current: '1120px shell; local score/actions/dialog/sheet/textarea/file/QR/mobile geometry',
    replacement: 'var(--ke-focus-lab-*)',
    browserExpectation: 'authenticated panel and both modal dialogs preserve current geometry',
    negativeControl: 'QR dimensions remain media geometry and file input anatomy stays local',
  },
  {
    consumer: 'site/src/components/FocusGroupLabPanel.astro',
    sourceHead: '63e3bd94852f44da93fb491a671095c6a7f06380', owner: 'A0', kind: 'elevation',
    current: 'panel 0 14px 38px; dialog 0 24px 64px',
    replacement: 'var(--ke-elevation-focus-lab); var(--ke-elevation-focus-dialog)',
    browserExpectation: 'embedded panel remains shallower than modal dialogs',
    negativeControl: 'no consumer shadow fallback remains after migration',
  },
  {
    consumer: 'site/src/components/FocusGroupLabPanel.astro',
    sourceHead: '63e3bd94852f44da93fb491a671095c6a7f06380', owner: 'A0', kind: 'icon',
    current: 'two text × close controls styled by font-size:1.8rem',
    replacement: '<SemanticIcon name="close" role="control" /> inside var(--ke-focus-lab-close-size)',
    browserExpectation: 'both dialogs retain 44px targets and one canonical close identity',
    negativeControl: 'remove text glyph and local glyph font sizing; do not change dialog labels',
  },
  {
    consumer: 'site/src/components/FocusGroupInviteIntake.astro',
    sourceHead: '63e3bd94852f44da93fb491a671095c6a7f06380', owner: 'A0', kind: 'color',
    current: '#fffdf8 card; prize, consent, confirmed, OTP and status literals',
    replacement: 'existing focus/prize/consent/confirmed/input/status roles',
    browserExpectation: 'prize, consent, code entry, signed-in and completion states remain distinct',
    negativeControl: 'identity choice, OTP validation and participation storage are untouched',
  },
  {
    consumer: 'site/src/components/FocusGroupInviteIntake.astro',
    sourceHead: '63e3bd94852f44da93fb491a671095c6a7f06380', owner: 'A0', kind: 'spacing',
    current: '560px shell; brand/card/note/consent/input/OTP/status/mobile local geometry',
    replacement: 'existing --ke-focus-intake-* roles plus residual var(--ke-focus-intake-*) in focus-continuity-foundations.css',
    browserExpectation: 'three-stage intake and six-cell OTP layout keep current responsive geometry',
    negativeControl: 'brand image 5.75rem remains media geometry, not a fifth icon role',
  },
  {
    consumer: 'site/src/components/FocusGroupInviteIntake.astro',
    sourceHead: '63e3bd94852f44da93fb491a671095c6a7f06380', owner: 'A0', kind: 'type',
    current: 'local h1/h2/copy/eyebrow/note/consent/link/input/OTP/status typography',
    replacement: 'var(--ke-focus-intake-*-size/weight/line/letter)',
    browserExpectation: 'stage headings, guidance and code cells retain current hierarchy and wrapping',
    negativeControl: 'visible copy and validation messages are unchanged',
  },
  {
    consumer: 'site/src/components/FocusGroupInviteIntake.astro',
    sourceHead: '63e3bd94852f44da93fb491a671095c6a7f06380', owner: 'A0', kind: 'elevation',
    current: 'brand 0 12px 30px; card 0 18px 52px',
    replacement: 'var(--ke-elevation-focus-brand); var(--ke-elevation-focus-card)',
    browserExpectation: 'brand asset remains shallower than intake card',
    negativeControl: 'intrinsic 192x192 asset dimensions remain media metadata',
  },
  {
    consumer: 'site/src/components/FocusGroupInviteIntake.astro',
    sourceHead: '63e3bd94852f44da93fb491a671095c6a7f06380', owner: 'A0', kind: 'icon',
    current: 'text ✓ in 3.5rem completion circle; local 2rem spinner with 3px stroke',
    replacement: '<SemanticIcon name="check" role="feature" />; spinner size var(--ke-focus-intake-spinner-size), stroke var(--ke-focus-intake-spinner-border)',
    browserExpectation: 'completion retains a visible non-colour mark and pending state keeps the same footprint',
    negativeControl: 'remove text glyph/local icon dimension; exactly four central icon roles remain',
  },
] as const satisfies readonly FocusContinuityBinding[];

export const focusContinuityBindingsFor = (consumer: string) =>
  FOCUS_CONTINUITY_BINDINGS.filter((binding) => binding.consumer === consumer);
