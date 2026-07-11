# Event decision-block mobile lab — 2026-07-04…10

Status: design/review lab, not production rollout.

The date is evaluated inside the whole mobile decision space below the approved event hero: parallax hero image → one overlapping light sheet → title/date/place/admission/actions → large medallion shelf. The isolated `/lab/date-block/` page remains the rollback/reference surface from commit `f8aa6bd44e6e2bc475b868f1187bde707cb6fd2d`.

## Public review surface

- Lab route: `/lab/event-decision-block/`.
- First broad A–F exploration: `preview-20260704t-decision-block-ab`.
- Corrected hero-overlap V2: `preview-20260705t-decision-block-v2` at commit `63e88020`.
- Current V3 review target: `preview-20260710t-event-page-ux-v3`.
- The lab itself uses page-local mock HTML/CSS. The real-event implementation is isolated behind the explicit build flag, so the default `EventHero.astro` output and existing auth/share/like/calendar behavior remain the rollback baseline.
- Opt-in real-data trial: `preview-20260711t-real-events-ticket-cluster-d`, compiled with `PUBLIC_EVENT_PAGE_DECISION_VARIANT=ticket-cluster` from a fresh 2026-07-11 Fly SQLite snapshot. The flag leaves the default event-page layout available as rollback and is not a production promotion.

## Immutable product constraints

1. Keep the approved hero mechanic: parallax image first, then one decision sheet overlapping it; H1 remains inside the sheet.
2. Keep one primary ticket/registration CTA. Calendar, share and like remain visible mandatory actions with the shared SVG icon language.
3. Keep medallions large and fast-readable (`90–108px`). Four or more use a horizontal shelf/peek; zero medallions remove the section without a placeholder.
4. Design mobile-first for `390px` and keep a `320px` safety state with no page-level horizontal overflow.
5. Preserve the V2 full-width date candidate as a visible control rather than silently replacing it.

## V3 data and action contract

- Visible secondary-action words are removed from active candidates. Calendar/share/like render as an accessible `icon + number` control with a minimum `44px` touch target and full `aria-label`.
- Calendar count uses `+N` (for example `+24`) because a bare calendar icon plus `24` can be misread as a calendar date.
- Generic `по билетам` is not valid production admission information:
  - known price/range → `от 600 ₽` or `600–1 500 ₽`;
  - free → explicit prominent `Бесплатно` and a registration/get-ticket CTA;
  - paid but price unknown with a real ticket URL → admission slot disappears and CTA becomes the neutral destination label `Билеты`; do not promise that a click will reveal a price.
- Place renders venue plus the exact address when known. It may use up to three calm lines; do not force a destructive one-line truncation.

## Active V3 candidates

| Variant | Pattern | Purpose |
|---|---|---|
| `decision-variant-v3-compact-tile` | Compact Date Tile | Distinctive challenger: strong 82×98 date tile beside H1, price in the top meta row, full address, CTA and icon counts. |
| `decision-variant-v3-calm-bar` | Calm Date Bar | Production-first candidate: H1 then a 50px dark date bar with a large day cell, exact address, price, CTA and icon counts. |
| `decision-variant-v3-split-band` | Split Date / Free Band | Explicit free-event probe: compact date and `Бесплатно` share one band; green registration CTA remains the only primary action. |
| `decision-variant-v3-ticket-cluster` | Ticket Cluster | Lower-bound probe for date strength: compact inline date, full address, then price and purchase merged into one transactional block. |
| `decision-variant-v2-date-card` | V2 full-width control | Preserved control with the strongest/largest date and the old text-labelled secondary actions. Not the current recommendation. |

Older V2 and A–F markers remain hidden regression markers so preview checks retain rollback history; they are not active candidates.

## Research funnel and consultant review

Artifacts are under `artifacts/codex/event-page-ux-v3-20260710/` and are intentionally not committed.

- Pinterest collection: `120` candidates from `12` diversified queries; Codex visually reviewed all and retained `19` (`15.8%`). Reusable mechanics were compact date tiles/rails, icon+count actions, exact-place fact stacks and one dominant booking CTA.
- Gemini 3.1 Pro (Antigravity) and a-opus independently produced four implementable concepts from the same constraints.
- After the HTML/CSS variants were rendered, both consultants reviewed the actual `390px` and `320px` contact sheets. Both ranked **B / Calm Date Bar** first and **A / Compact Date Tile** second.
- Both also found calendar + a bare numeric value ambiguous. The later real-event correction therefore removed the unsupported count and the `+` affordance entirely: calendar uses the established large SVG and an accessible name.

The consultant acceptance ranking remains useful historical evidence, but the 2026-07-11 product shortlist is now:

1. **V3-A / Compact Date Tile** — selected for its ownable compact date anchor.
2. **V3-D / Ticket Cluster** — selected for its calm transaction hierarchy; its bare action row now includes a personalized one-at-a-time onboarding probe.
3. Keep V3-B as the safe fallback and V3-C as the explicit free-event state test.

The onboarding contract, calendar `.ics` + private save + email-follow target, current limitations and Pinterest research are canonical in `event-action-onboarding.md`. After visual product feedback, D’s calendar education was moved below the unchanged icon row, renamed back to the exact action `Добавить в календарь`, and restyled as a light playful pointer callout rather than a graphite decision block.

## QA evidence

Local Astro preview `preview-20260710t-event-page-ux-v3`:

- `433` pages built;
- `npm --prefix site run check:preview` passed;
- Playwright at `390px` and `320px` found no page-level horizontal overflow and no failed image HTTP responses;
- active action controls are `48px` high with three accessible labels;
- medallions measure `104px` at the 390px review viewport and `90px` at the 320px safety viewport.

The lab phone is nested inside the documentation page padding, so its measured inner width is intentionally harsher than a full-bleed production event page.

## Production gate before rollout

- A lab preference is not a production rollout. Real `EventHero.astro` must preserve current Yandex/auth session handling, first-party like state/counts, native-share behavior, ICS calendar behavior and analytics.
- Numeric calendar/share/like values must come from truthful existing counters or an explicitly defined backend field; do not fabricate social proof.
- The real-event trial therefore shows truthful share/like totals including `0`, but renders calendar without a number or `+` because no public calendar-save count exists yet.
- Stress-test long Russian title, three-line address, unknown time, free, known price range, paid-unknown, counts from one to four digits, zero medallions and four-plus medallions.
- Keep `/lab/date-block/`, the V2 control and regression markers until the production event-page implementation is accepted and rollback is no longer needed.
- For V3-D, follow the one-hint-per-session policy in `event-action-onboarding.md`; do not promise email delivery while the unified calendar follow remains incomplete/dry-run.

Real-data acceptance matrix for the corrected successor preview covers known price + phone CTA, free/no-registration → `Добавить в календарь`, paid/unknown price → `Билеты`, long title, zero and non-zero counters, with/without medallions, `320px` and `390px`, optional/forced onboarding, and OCR poster containment. The earlier fixed row measured `0px` absolute movement after dismiss; the successor increases icon size without reducing the `48px+` targets.
