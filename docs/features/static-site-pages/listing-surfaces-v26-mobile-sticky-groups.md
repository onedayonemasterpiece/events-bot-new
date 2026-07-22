# Listing surfaces V26 — mobile sticky group context

**Scope:** mobile `Популярное` only (`max-width: 720px`). Desktop
ranking, media geometry, V22 rows and the V25 density/evidence contracts are
unchanged.

## Product decision

Long popularity groups need persistent wayfinding, but a third full-width
navigation bar would cost more attention and vertical space than it returns.
V26 keeps the strong 24px/900 section heading at the category boundary and
makes that same semantic header sticky only inside its own section. Once it
sticks, its visual label compacts to a quiet 34px single-line badge aligned to
the right of the existing 128px mobile drawer handle.

The label uses the otherwise free top-right area, never intercepts card taps or
scroll gestures, and is naturally pushed away by the following section. Long
labels ellipsize only in the sticky state; the complete label remains visible
in the original section heading and remains the sole `h2` for accessibility.

## Implementation contract

- CSS `position: sticky` owns physical sticking and section-boundary
  replacement; no scroll listener drives position.
- A 1px section sentinel and the section box are both observed. The sentinel
  handles continuous crossing; the larger box makes a fast fling that skips a
  1px target deterministic. `IntersectionObserver` only toggles the compact
  `.is-stuck` appearance.
- The outer header keeps its 52px flow geometry, so compaction cannot shift the
  event row or alter density packing.
- Once compact, the visual class is not removed at an estimated section-bottom
  threshold. The stable outer header remains compact while its parent boundary
  natively pushes it offscreen, preventing a final-frame jump back to 24px.
- The sticky label is at most `min(100vw - 156px, 288px)`, 34px high, aligned
  right and `pointer-events:none`; the drawer remains the higher layer.
- The existing 72px semantic category gap moves from the next section's outer
  margin/inset into the previous section's bottom padding. Document height is
  unchanged, while native sticky context now survives until the next heading
  reaches it instead of leaving a context-free gap.
- Both large and adaptive representations use the same behavior. Density
  changes dispatch a local refresh event after the active hidden/inert tree is
  switched, while V25 event-anchor preservation remains unchanged.
- `prefers-reduced-motion` removes the visual-state transition.

## Acceptance

- on `360/390/430px`, the current group label remains visible after its source
  heading leaves the viewport, does not touch the drawer handle and creates
  no horizontal overflow;
- the following group natively replaces the previous sticky label without a
  blank or double-label frame;
- both density modes expose the same active category after switching and retain
  the same viewed event within 1px;
- at section start the full heading remains at least 24px/900; only the stuck
  state becomes a one-line 14px context label;
- at `721/1366/1536/1920px`, desktop Popular remains identical to V22/V25.

Discovery and acceptance use `Gemini 3.1 Pro (High)` through `agy`; ignored
evidence is stored under
`artifacts/codex/listing-surfaces-v26-mobile-sticky-groups-20260719/`.

Public review URL:
`https://kenigevents.ru/preview-20260719-date-listings-v26-mobile-sticky-groups/populyarnoe/`.
