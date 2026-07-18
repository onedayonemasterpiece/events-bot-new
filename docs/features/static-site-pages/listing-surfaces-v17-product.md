# Listing surfaces V17: common scan edge and one discovery plane

> **Status:** desktop immutable preview candidate, 2026-07-18. Mobile is research-only in this pass.
> **Surfaces:** `/segodnya/`, `/zavtra/`, `/vyhodnye/`, `/populyarnoe/`.
> **Supersedes:** V16 layout decisions in [`listing-surfaces-v16-product.md`](listing-surfaces-v16-product.md). Its fail-closed OCR/media and truthful Calendar-evidence decisions remain in force.

## Product hierarchy

The listing helps a person scan **time → identity → place → audience evidence** and open a detail page. Share/Like numbers are quiet, non-zero evidence, not listing CTAs. Calendar proof is still absent: the project plans a durable per-user saved-event state, but does not yet export a privacy-safe deduplicated public `saved_event_count`; an ICS request is not a person.

## V17 decisions

### Common left edge and compact evidence

- Every singleton begins at the same left scanning edge as a multi-card row. Its copy envelope may grow to `420px`; neither the card nor only its media is optically centred.
- The actual final card of a multi-card row may use spare copy width only after media geometry and packing settle.
- A media overlay is systemic, not an event-id exception: only the selected `classified` `event_photo` with confidence `>=0.9`, `visual_only`, `safe_crop`, focal evidence and ratio `>=1.2` may receive a lower-right identity medallion. A genuine no-media fallback may contain it. OCR, unknown and rejected raw alternatives fail closed.
- Up to three identity medallions remain visible even with social proof. Three identities plus proof use a compact `56px + 36px` split rail; proof never evicts a festival/venue/Free identity. Event `6811` is the acceptance control.

### One adaptive discovery plane

- Cities and dayparts are one light grid plane, not two stacked subheaders. Direct city toggles remain visible: no dropdown and no horizontal dead end.
- Expanded city labels use `15px`, `52px` targets and `16px` gaps. The pinned state compacts to `12.48px`, `44px`, `8px` and hides count bubbles. Wrapping is measured; the sticky offsets use the measured height.
- Direct hashes and daypart clicks realign after font and discovery-geometry changes. Weekend exact time remains pinned below site header, discovery plane, day heads and a `12px` gap.

### Weekend density and truth

- Weekday chips are neutral gray wayfinding. Light text reads `сб 2 события` / `вс 1 событие`.
- Within one day and one exact time only, a stable greedy pre-calculation may reorder cards when it provably reduces rows at the baseline lane. It never crosses a time/day boundary. The order is viewport-independent; wider lanes only reduce wrapping.
- The listing maximum grows to `1720px` on wide desktop. Saturday 12:00 is the two-row control; Sunday 11:00 may gain at 1920 without changing DOM order.
- Explicitly ended/prior-day cards are past; current-day unknown-end starts older than one hour say `Началось ранее`. Status is real DOM text and only the main media is muted.
- Weekend navigation contains six direct smart ranges plus a separate decorative long-arrow continuation, above and below the schedule.

### Personalization and Popular

- Full list remains default. Immutable `/preview-*` builds expose the `Для меня / Полный список` prototype automatically so it can be reviewed; `?personalization=off` disables the fixture. Stable production does not synthesize qualification.
- The switch uses radio semantics and recomputes city/time/day counts. If a compatible profile would leave fewer than five results, it fails closed to full list.
- Popular remains one continuous score-ordered stream and adds exact event-type filters, not duplicate shelves. Filtering preserves the original score order and recomputes city facets.

## Mobile research decision (not implemented)

The proposed two-density mobile listing should be tested as an explicit `Комфортно / Компактно` control using the same DOM, order and content. V17 does **not** disable browser zoom, set `user-scalable=no`, or intercept native pinch: those approaches conflict with accessibility zoom and make an accidental gesture silently change information density. Pinch may remain a future supplementary gesture only if it mirrors an always-visible accessible control.

## Desktop acceptance

Playwright must cover `1366`, `1536` and `1920` and assert:

- zero document/city horizontal overflow;
- every singleton card/media left edge equals its flow left edge (controls `6685`, `6953`, `6590` included);
- expanded/compact city sizes and a shared cities/daypart plane;
- direct Weekend hash settles to day-head bottom `+12px` after geometry changes;
- event `6811` keeps three identity tokens plus non-zero Share/Like proof with no rail overflow;
- Saturday 12:00 uses at most two rows at 1366/1536 and packing order is stable across widths;
- Popular exact-type filtering returns a score-order subset;
- six Weekend range anchors and a non-anchor decorative continuation appear in both positions;
- past/started-earlier status exists as readable DOM text.

Generated reports and labelled screenshots live in the non-committed `artifacts/codex/listing-surfaces-v17-repair-20260718/` directory.

