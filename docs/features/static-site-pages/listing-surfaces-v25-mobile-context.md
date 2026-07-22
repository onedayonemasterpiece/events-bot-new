# Listing surfaces V25 — mobile context and hierarchy

**Scope:** mobile `Популярное` only (`max-width: 720px`). The V22 desktop
renderer, ranked groups, canonical large `EventCard split-actions` and shared
event-detail card contract remain unchanged.

## Product decisions

### Density changes preserve the viewed event

The switch no longer treats a page offset or the element nearest a generic
sticky-header token as the user's context. Before changing representation it
selects the actually visible event with the largest viewport intersection. A
pinch that begins over a card explicitly prefers that event. After the hidden /
inert trees switch, the same `data-event-id` is placed at the previous viewport
top with an instant, clamped document scroll.

Regression subject `4689`, `Фестиваль добровольчества #МЫВМЕСТЕ`, remains at
the same position in both directions. Local Chromium at `390×844` measured
`90.516px → 90.656px → 90.516px`; pinch-in measured the same event and a
`0.141px` rounding-only delta.

### Mobile chrome is not a desktop stack

Popular on phones reuses the existing mobile discovery drawer as the only
global navigation. The ordinary desktop nav is hidden on this surface. The four
listing routes become one static 44px text-tab row, and the cities-only discovery
rail becomes one non-sticky 48px row instead of inheriting the generic 96px
two-row reservation.

At `390×844`, the listing head falls from about `293px` to about `207px`, the
city row is exactly `48px`, and the first category starts around `340px` instead
of `474px`. Desktop geometry is not selected by these rules.

### Categories must read as categories

Mobile group headings use 24px/900 typography, a quiet count circle, a 52px
minimum head and a 72px semantic break before following groups (40px space plus
32px separated inset). They remain ordinary document headings rather than a
third sticky layer.

### Evidence stays compact, not blindly moved below every image

A measured all-under-photo prototype increased paired events from `64%` to
`80%`, but lengthened the complete compact document by roughly `5%`. That is a
net regression for fast vertical scanning.

V25 therefore keeps the established overlay for reviewed wide non-OCR media and
uses a narrower external evidence spine elsewhere:

- `28px` for non-zero social proof only;
- `44px` when an identity/free medallion is present;
- opaque 40px medallions above subdued icon-over-count proof;
- zero counters remain absent.

This measured `18/25` events in paired rows at `390px` (72%, up from 64%) while
preserving media height, OCR retention and ranked order. A below-photo placement
remains a future packing option only when a group-level pre-calculation proves
that it reduces, rather than increases, total group height.

## Acceptance contract

- `360/390/430`: no horizontal page overflow; date navigation stays one 44px
  row; city rail is 48px and non-sticky; mobile drawer remains available;
- category title is at least 24px/900 and category boundaries are explicit;
- `Крупно ↔ Компактно` and pinch preserve the same event ID and viewport top;
- compact evidence rails are `28/44px`; overlay medallions remain opaque;
- compact paired share is no worse than V24 and media height/crop is unchanged;
- `721/1366/1536/1920`: desktop IDs, widths and media heights equal the accepted
  V22 public baseline; density dock remains hidden.

The discovery and acceptance reviews use `Gemini 3.1 Pro (High)` through `agy`;
raw prompts/replies and browser evidence live under the ignored
`artifacts/codex/listing-surfaces-v25-mobile-context-20260719/` directory.

Public review URL:
`https://kenigevents.ru/preview-20260719-date-listings-v25-mobile-context/populyarnoe/`.
