# Event decision-block A/B lab — 2026-07-04

Status: design/review lab, not production rollout.

The date block must now be judged inside the whole first-screen decision space below the hero image: date, weekday, time, venue, primary CTA, calendar/share/like utilities, and event medallions. The previous `/lab/date-block/` page is preserved as the rollback/control reference at commit `f8aa6bd44e6e2bc475b868f1187bde707cb6fd2d`.

## Public review surface

- Lab route: `/lab/event-decision-block/`.
- Build target used for local verification: `preview-20260704t-decision-block-ab`.
- The lab intentionally uses page-local mock HTML/CSS and does not mutate `EventHero.astro`, medallion manifests, or production event pages.

## Variants

| Variant | Pattern | Purpose |
|---|---|---|
| A | `decision-variant-p03` | Editorial oversized date first; strongest visual date anchor. |
| B | `decision-variant-p01` | Compact calendar tile + facts rail; robust and familiar fallback. |
| C | `decision-variant-p04` | Vertical date rail; tests rectangular left-rail composition. |
| D | `decision-variant-baseline` | Current split-badge control; kept for rollback and regression comparison. |
| E | `decision-variant-p03-zero-medallions` | P03 when no organizer/festival/source medallions exist. |
| F | `decision-variant-utility-stack` | One primary CTA plus quieter utility actions; tests action-noise reduction. |

## Review criteria

1. Scan order: date → time → place → primary action → trust tokens.
2. Noise budget: no more than two strong first-screen visual accents.
3. CTA hierarchy: exactly one visually dominant primary action; calendar/share/like must be secondary.
4. Medallion state: 0, 1 and 4+ medallions must not break the first-screen decision task.
5. Mobile fit: 390px and 320px without horizontal scrolling; touch targets at least 44px.

## External guide guardrails

- [NN/g visual hierarchy](https://www.nngroup.com/articles/visual-hierarchy-ux-definition/): define intended reading order first; use contrast, scale, grouping/proximity sparingly so not everything competes for attention.
- [Material Web buttons](https://material-web.dev/components/button/) and [Apple HIG buttons](https://developer.apple.com/design/human-interface-guidelines/buttons): preserve one primary action and make secondary/tertiary actions visibly quieter while retaining accessible touch targets.
- [Baymard Product Page UX](https://baymard.com/research/product-page): use the product-detail-page analogy; an event detail page is a decision page, so the template must survive variable titles, venues, ticket states and trust badges.

## Consultant review outcome

Artifacts are stored under `artifacts/codex/event-decision-block-20260704/` and are not committed.

- Gemini Pro (`gemini-3-pro-preview`) ranking: `F → A → E → B → D → C`. It prioritized CTA hierarchy and recommended an `A+F` hybrid: P03 visual date plus one wide primary CTA, quiet utility row, medallions below.
- a-opus full review ranking: `A → F → B → E → C → D`. It prioritized P03 as the strongest date scan anchor, with F as the cure for the current 4-button noise and B as a 320px safety fallback.

Shared decision: the production candidate should be a **P03-F hybrid**, not the current baseline and not an isolated date-only P13/P03 block. Proposed structure:

```text
hero image
P03 editorial date / weekday / time
H1 or compact title continuation + venue
one wide primary CTA
quiet utility row: calendar / share / like
medallions below, muted and overflow-safe
zero-medallion fact zone when no tokens exist
```

## Production constraints before rollout

- Split production `EventHero` action hierarchy so share/like/calendar are not equal to the primary CTA.
- Keep current `/lab/date-block/` and baseline control until a real event-page A/B candidate is accepted.
- Do not enlarge medallions inside the decision zone; if they move closer to the CTA, they must become smaller/muted or horizontally overflow-safe.
- Stress-test with long Russian titles, long venues, no time, no medallions, 4+ medallions and free/ticket/sold-out states.
