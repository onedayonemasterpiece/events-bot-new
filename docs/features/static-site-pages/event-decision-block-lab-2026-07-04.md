# Event decision-block A/B lab — 2026-07-04/05

Status: design/review lab, not production rollout.

The date block is judged inside the whole first-screen decision space below the approved event hero: hero image → overlapping decision sheet → title/date/place/actions → medallions. The previous `/lab/date-block/` page remains the isolated date rollback/control reference at commit `f8aa6bd44e6e2bc475b868f1187bde707cb6fd2d`.

## Public review surface

- Lab route: `/lab/event-decision-block/`.
- 2026-07-04 build target used for the first broad A–F exploration: `preview-20260704t-decision-block-ab`.
- 2026-07-05 corrected build target: `preview-20260705t-decision-block-v2`.
- The lab intentionally uses page-local mock HTML/CSS and does not mutate `EventHero.astro`, medallion manifests, auth, share, like, calendar or production event pages.

## 2026-07-05 correction after product review

The first A–F page solved the product question too freely: several mocks moved the title into/above the poster image, visually broke the already approved hero-overlap contract, shrank medallions into quiet chips or treated calendar/share/like as optional utilities. That is now explicitly superseded.

Hard constraints for the active lab:

1. Keep the approved mobile hero structure: parallax/hero image first, then one `decision-sheet` overlapping the image with the terracotta handle.
2. Keep `H1`/event title inside the sheet. Do not put the title on top of the hero image.
3. Keep all four mandatory actions visible and icon-led: primary ticket/buy action, calendar, share and like.
4. Keep medallions large and fast-readable. They may move before/after the CTA, but they must not become chips or muted tiny badges.
5. Support both 4+ medallions with horizontal shelf/peek and zero-medallion pages without dashed empty placeholders.

## Active V2 variants

| Variant | Pattern | Purpose |
|---|---|---|
| `decision-variant-v2-date-card` | Date Card Above CTA | Production-first candidate: H1 inside sheet, strong date card, place, mandatory CTA cluster, large medallion shelf immediately after sheet. |
| `decision-variant-v2-medallion-shelf` | Strong Date + Medallions inside sheet | Tests whether 1–3 large medallions can sit inside the decision sheet without burying CTA. |
| `decision-variant-v2-attached-medallions` | Attached shelf + compact date stripe | Experimental identity-heavy layout: medallions visually attach to the sheet edge while date stays a high-contrast stripe. |
| `decision-variant-v2-zero-medallions` | No medallion safety | Ensures the surface collapses cleanly when no medallions exist and uses only compact facts after the CTA. |

The old A–F markers (`decision-variant-p03`, `decision-variant-p01`, `decision-variant-p04`, `decision-variant-baseline`, `decision-variant-p03-zero-medallions`, `decision-variant-utility-stack`) remain on the lab page only as superseded regression markers, not as candidates.

## Review criteria

1. Hero contract: no title above/on the image; one overlapping sheet owns the decision content.
2. Scan order: date → weekday/time → place → primary action → secondary actions → trust tokens.
3. CTA hierarchy: exactly one visually dominant primary action; calendar/share/like must stay present with accessible touch targets.
4. Medallion state: 0, 1 and 4+ medallions must not break the first-screen decision task, and medallions remain large.
5. Mobile fit: 390px primary review and 320px safety review; no page-level horizontal scroll.

## External consultant review outcome

Artifacts are stored under `artifacts/codex/event-decision-block-v2-20260705/` and are not committed.

- Gemini Pro (`gemini-3-pro-preview`) recommended constrained variants rather than the old A–F page: badge/date split as the safest direction, editorial stack as the stronger typographic alternative, and medallion inject as the identity-first experiment. It explicitly preserved the hero-overlap sheet, full mandatory action row and large medallion shelf.
- a-opus recommended `Date Card Above CTA` as the production-first candidate, with an inline-date medallion shelf and attached-medallion shelf as comparison variants. It rejected title/date overlays on the hero image and warned against multiple overlapping objects.

Shared decision: start visual QA from **V1 / Date Card Above CTA**. It keeps the existing production mental model while making date/time/weekday much stronger and restoring the full buy/calendar/share/like cluster. V2/V3 are design probes for events where medallions themselves carry a lot of recognition value.

## Production constraints before rollout

- A lab win is not enough to ship: real `EventHero.astro` must preserve existing share, like, calendar, auth/session and analytics behavior.
- Medallion data provenance and image manifests remain governed by `event-token-medallions.md`; this lab only changes layout.
- Stress-test with long Russian titles, long venues, no time, no medallions, 4+ medallions, free/ticket/sold-out states and 320px width.
- Keep `/lab/date-block/` and the superseded marker list until a real event-page rollout is accepted and rollback is no longer needed.
