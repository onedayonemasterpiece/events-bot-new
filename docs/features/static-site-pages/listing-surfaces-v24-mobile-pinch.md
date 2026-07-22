# Listing surfaces V24 — compact packing and pinch

**Scope:** mobile `Популярное` only. The desktop renderer, ranked dataset and
canonical shared `EventCard` remain the V23/V22 contract.

## Corrections

### Compact icon

The compact-mode icon is two outlined vertical tiles. It communicates the
intended two-up scan density without promising that every source-safe card can
always share a row.

### Packing without scale reduction

V23 kept the accepted media height but inherited the later global listing-shell
gutter: `clamp(3rem, 5vw, 6rem)` removed 48px on common phones. At `390px`, two
ordinary `171.4px` cards plus a `12px` gap therefore needed more than the actual
`342px` row.

V24 does not reduce card/media scale, crop OCR, reorder ranks or introduce
masonry. Only the mobile Popular shell returns to 12px side gutters and the
inter-card gap becomes 10px. Acceptance is based on actual browser rows: more
than half of the 25 ranked events must participate in two-card rows at both
360px and 390px, while singletons remain permitted for genuinely wide cards.

### Pinch density gesture

The always-visible `Крупно / Компактно` dock remains the accessible source of
truth. On this Popular route only, browser zoom is disabled and a two-finger
gesture mirrors the same state machine:

- pinch inward (`distance ratio ≤ 0.84`) → `Компактно`;
- pinch outward (`distance ratio ≥ 1.16`) → `Крупно`.

The handler prevents default behavior only during an active two-touch move;
ordinary one-finger vertical scrolling and taps remain native. Each gesture may
commit once, local preference and the nearest visible event anchor are preserved,
and desktop receives neither the dock nor gesture surface.

## Acceptance

- mobile `360/390/430`: no horizontal overflow; media height exactly matches
  V23 at the same viewport; paired-event share above 50% at 360 and 390;
- compact icon has exactly two outlined tiles;
- synthetic two-touch pinch-in/out changes the same `aria-checked`, `hidden`
  and `inert` states as the dock buttons;
- single-touch move is not cancelled by the density handler;
- `/populyarnoe/` alone emits `maximum-scale=1, user-scalable=no`;
- desktop `1366/1536/1920` preserves V22 event IDs, widths and media heights.

Local Chromium evidence on the ranked 25-event Popular projection measured
`16/25` events in two-card rows (`64%`) at each of `360`, `390` and `430px`.
V23 measured `4/25` (`16%`), `6/25` (`24%`) and `16/25` (`64%`) respectively.
Media heights remained exactly `121.3125`, `131.421875` and `144.90625px` in
both builds. Pinch-in/out updated the same hidden/inert/radio states; a
single-touch move and a synthetic desktop two-touch move were not cancelled.

Review URL:
`https://kenigevents.ru/preview-20260719-date-listings-v24-mobile-pinch/populyarnoe/`
