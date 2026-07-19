# Listing surfaces V23 — mobile Popular density comparison

> **Superseded for mobile packing by V24.** Canonical component reuse and the
> unchanged desktop baseline remain valid; V24 corrects phone gutters, icon and
> adds the owner-requested pinch shortcut.

**Status:** implementation candidate; desktop contract is inherited unchanged
from V22, while the two mobile representations remain a product comparison.

## Product question

The phone page must not force one irreversible card density. It exposes two
finite representations of the same deduplicated, ranked Popular groups:

1. **Крупно** — the exact shared `EventCard.astro` with
   `variant="split-actions"`, i.e. the component used by mobile
   `Смотрите дальше` on an event page. There is no Popular-only reimplementation
   or geometry imitation.
2. **Компактно** — the existing `ListingEventCard.astro` in an ordered,
   intrinsic-width flex wrap. A fixed media height allows two narrow cards on a
   line and leaves a wide/OCR-safe card alone without masonry reordering.

Both representations contain the same event IDs in the same order. Switching
may not change ranking, filters or personalization state. The equal two-column
V22 experiment stays available as a separate immutable comparison; it is not
silently overwritten by the adaptive candidate.

## Packing contract

- mobile breakpoint: `max-width: 720px`; desktop starts at `721px`;
- compact row: ordered `flex-wrap`, left aligned, `20px × 12px` gaps;
- compact media height: `clamp(118px, 33.7vw, 145px)`;
- card width derives from media ratio plus the existing medallion/social rail;
- safe `visual-crop` media may grow only within the existing adaptive crop
  envelope; OCR/document media is not normalized by a new crop rule;
- no masonry, CSS `order`, rank-dependent enlargement or JS packing;
- titles remain under the media and clamp at three lines in compact mode.

This is an efficiency hypothesis, not a claim that adaptive packing is already
proven better. Gemini 3.1 Pro's independent review preferred it for information
fidelity but identified false hierarchy as the main risk: a wide singleton can
look editorially more important. The capped width and immutable rank order are
guardrails; click-through and mode-retention data are needed before choosing a
default beyond the prototype.

## Density dock contract

The `Крупно / Компактно` switch is fixed to the physical bottom edge and spans
the full viewport width. Each half has a 48px minimum target; safe-area inset is
reserved and page bottom padding prevents the last card being covered. The dock
is absent on desktop and leaves when the footer is visible.

Only one representation is accessible at a time: the inactive tree has both
`hidden` and `inert`; `aria-checked`, focus order and persisted local preference
stay synchronized. A density change preserves the nearest visible event anchor.

## Acceptance matrix

- `360×844`, `390×844`, `430×844`: canonical large card, full-width dock,
  no horizontal overflow, identical ordered IDs between modes;
- compact mode: at least one two-card row and one singleton in the actual
  ranked dataset; equal media height within each viewport;
- `720/721`: exactly one mobile/desktop family visible and dock only at `720`;
- `1366`, `1536`, `1920`: desktop card flow/media geometry matches the V22
  baseline for the same snapshot and has no mobile dock;
- city/type filtering counts only the active representation.

## Review links

- V23 adaptive comparison:
  `https://kenigevents.ru/preview-20260719-date-listings-v23-mobile-adaptive/populyarnoe/`
- preserved V22 equal two-column comparison:
  `https://kenigevents.ru/preview-20260719-date-listings-v22-mobile-restore/populyarnoe/`
