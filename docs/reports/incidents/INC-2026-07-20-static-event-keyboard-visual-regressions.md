# INC-2026-07-20 Static event recommendation crop and keyboard ownership regressions

Status: reopened / corrective immutable secret candidate pending
Severity: sev2
Service: immutable secret static-event candidate / desktop event detail
Opened: 2026-07-20
Closed: —
Owners: static-site and keyboard-navigation maintainers
Related incidents: `INC-2026-07-20-image-geometry-pixel-drift`, `INC-2026-07-16-static-event-media-action-regressions`, `INC-2026-07-15-static-production-v2-secondary-surfaces`, `INC-2026-07-15-static-desktop-template-regression`
Related docs: `docs/features/static-site-pages/keyboard-event-navigation-prototype.md`, `docs/features/static-site-pages/image-framing.md`, `docs/features/static-site-pages/astro-preview.md`

## Summary

The immutable secret candidate generated from repository SHA `11cbef17` exposes
three desktop regressions on the event-detail journey:

1. recommendation rows are packed as though images can use `cover`, while the
   canonical card can later fail closed to `contain`, producing large and
   inconsistent bands around event images;
2. a real Enter navigation from the fullscreen gallery recommendation to the
   destination event leaves focus on `BODY`, so closed-hero Left/Right does
   nothing until the event surface is focused manually;
3. scrolling to the footer with a touchpad does not transfer shortcut ownership
   to service sharing, so body-targeted `P`/`S` does nothing; if a prior card
   retains focus, `S` can instead copy the off-screen card.

The earlier Playwright pass did not exercise these user journeys. The recorded
Gemini 3.1 Pro `SHIP` verdict applied only to two frozen noindex V7 prototype
objects and explicitly marked production integration `NOT READY`; it was not a
visual acceptance of this generated candidate.

The first corrective implementation was merged and generated candidate
`RHOg…`, but that candidate failed owner acceptance. Its green
`related_geometry_crop` receipt proved only the selected `object-fit` and box
geometry: it did not prove that the failure fallback behind a loaded
`contain` image was hidden. The same acceptance also omitted cold-load and
real inert-pointer ownership. Therefore the previous `mitigated`/`SHIP`
conclusion is withdrawn. No production-root promotion was performed.

## User / Business Impact

- The `Смотрите дальше` grid can look visibly broken even though individual
  images are correctly protected from an unsafe semantic crop.
- Keyboard navigation stops after the exact cross-event journey it is meant to
  enable, making the interface appear unreliable.
- Footer copy shortcuts provide no result or feedback after normal touchpad
  scrolling, and may act on an off-screen event when focus remains elsewhere.
- Functional green checks incorrectly suggested broader visual and journey
  acceptance than the evidence supported.

The currently stable root site was not changed; impact is confined to the
secret preproduction candidate family.

## Detection

- Reported by the product owner while manually testing event `6408`,
  `Спектакль «Собака на сене»`, on the immutable secret candidate.
- Reproduced in live Chromium at desktop viewports without modifying runtime
  state.
- The current static-build diagnostics were queried read-only for the preceding
  48-hour window.

## Timeline

- 2026-07-20 11:01–11:22 UTC — build
  `production-secret-20260720T130141-937a3d18` generated from SHA `11cbef17`
  and published the `iiHL…` immutable review prefix used for this report.
- 2026-07-20 11:26–11:45 UTC — an already-running parallel Smart Update/bbox
  flow generated and selected candidate
  `production-secret-20260720T132607-9e4818dd` from SHA `ae2336cb`; this
  investigation did not initiate or select it.
- 2026-07-20 — the owner reported the crop, cross-page hero-arrow and footer
  shortcut defects; read-only reproduction confirmed all three.
- 2026-07-20 11:48–11:52 UTC — a fresh agy review with approved
  `gemini-3.1-pro-preview` completed with clean process provenance and returned
  `REJECT` for the current candidate: the cross-page failure is P0 for candidate
  acceptance; recommendation geometry and footer ownership gaps are P1.
- 2026-07-20 18:07 UTC — Fly release `v1734` deployed exact `origin/main` SHA
  `1e7594d22c545f535c131aef3e9f9e5bddddd9f3` with the production browser-gate
  and Kaggle Playwright dependency fixes.
- 2026-07-20 18:12–18:33 UTC — Smart Update job `39104` generated, checked and
  published `production-secret-20260720T201154-77720953`; the coalesced startup
  request `39115` then completed as a fingerprint no-op rather than rebuilding
  the same input.
- 2026-07-20 18:33 UTC — immutable review prefix `RHOg…` became the checked
  current candidate. Both root-form and secret-candidate generated trees carry
  `browser_visual=ok`; the public candidate remains `noindex`, `no-referrer`,
  root-isolated and stable-ICS-isolated.
- 2026-07-20 18:39 UTC — an independent live Chromium run repeated the real
  `6408` → gallery → `6407` → hero-arrow journey, canonical card geometry,
  bounded continuation and footer `P`/`S` clipboard/toast flows successfully.
- 2026-07-20 18:40 UTC — agy Gemini 3.1 Pro reviewed the actual immutable URL
  and retained screenshots and returned `SHIP` with all five acceptance groups
  marked `PASS`.
- 2026-07-20 19:10 UTC — owner review of `RHOg…` disproved that acceptance:
  fallback date/type/city remained visible through `contain` letterboxing;
  fresh-load hero arrows did nothing until a Down/Up ownership cycle; and a
  real click on inert current-event content followed by physical Russian-layout
  `L/K/S/Enter` did nothing.
- 2026-07-20 19:32–19:33 UTC — a new agy `Gemini 3.1 Pro (High)` review over
  the exact reproduction JSON, screenshots and production code returned
  `REJECT`: P0 fallback bleed, P1 cold-load ownership and P2 inert-pointer
  ownership. The review explicitly required fail-closed `contain`, neutral
  bands after load and mixed-input regression coverage rather than a blind
  switch back to `cover`.

## Root Cause

### Recommendation image geometry

1. `site/src/lib/relatedCardLayout.mjs` determines row eligibility and assigns
   `visual-cover` from `image_text_mode` before the final semantic crop policy
   is known.
2. `site/src/components/EventCard.astro` subsequently applies the authoritative
   media-role, exact-geometry and protected-region gates and can correctly fall
   back to `contain`.
3. The row therefore remains sized for `cover` while the image is rendered as
   `contain`. On the reported page, 4 of 10 cards declared `visual-cover` but
   computed to `contain`; 5 cards had at least 20% unused media area.
4. A parallel bbox change also maps `document-safe-cover` to `contain` in the
   desktop page while the same treatment maps to `cover` in the personal-feed
   surface, so one canonical card policy is rendered differently by surface.
5. In `RHOg…` the final `contain` decision itself is valid, but
   `.event-card__image-fallback` remains painted underneath the successfully
   loaded image. The unused letterbox area therefore reveals fallback
   date/type/city/gradient content. The gate checked fit and bounds, not this
   composited loaded state.

### Cross-document hero navigation

1. A full document navigation correctly resets DOM focus to `BODY`.
2. The production keyboard router handles closed-hero Left/Right only when the
   event surface or one of its descendants owns focus. Body-targeted recovery
   is intentionally implemented only for selected actions such as provenance-
   gated `L`.
3. The destination page contains seven valid image slides and responds as soon
   as its event surface is focused manually; the gallery and image-selection
   data are not the failure.
4. The handoff-only correction did not cover a fresh direct load/reload. The
   first body-targeted Left/Right remained unowned until another keyboard path
   focused the event action surface.

### Inert pointer plus keyboard navigation

1. Pointer ownership recognized only the CTA surface or a managed event card.
2. Clicking inert current-event copy correctly leaves DOM focus on `BODY`, but
   also revoked logical ownership, so physical `KeyL`, `KeyK`, `KeyS` and
   `Enter` could not recover the current event.
3. The old Playwright path used programmatic focus or synthetic control
   provenance and therefore did not reproduce the owner's mouse-then-keyboard
   sequence or explicit Cyrillic `KeyboardEvent.key` values with stable
   physical `KeyboardEvent.code`.

### Footer shortcuts

1. Footer `P`/`S` ownership is based on actual focus within a footer share
   control, not on footer visibility or the user's touchpad scroll context.
2. Wheel/touchpad scrolling does not move focus. Body-targeted `P`/`S` falls
   through; retained off-screen card focus can route `S` to that card instead.
3. No viewport-aware ownership handoff or ambiguity guard exists for this
   mixed pointer/keyboard journey.

## Contributing Factors

- The keyboard acceptance script blocks real gallery CTA navigation with
  `preventDefault()` and verifies only the captured destination URL.
- The footer test first asserts body `P` is a no-op, then explicitly focuses
  the footer image and text buttons before testing `P` and `S`; it does not
  model touchpad scroll followed by an unfocused shortcut.
- The Playwright gate checks card count, focus/actions and horizontal overflow,
  but not computed `object-fit`, row/image geometry or crop/letterbox budgets.
- The later `related_geometry_crop` gate still treated a loaded image plus
  correct `object-fit` as sufficient. It neither asserted that the failure
  fallback became non-visible nor retained a settled-pixel viewport screenshot
  for critical review.
- Its single final screenshot is captured after the journey near the
  continuation/footer, is not compared, and does not gate the broken related
  rows.
- The static production contract checks hero/gallery fit, not the effective
  fit of generated recommendation cards.
- The earlier external review covered the frozen V7 prototypes, not the later
  production integration, real candidate data or bbox merge.
- The first candidate Gemini prompt trusted incomplete green checks and
  insufficient visual evidence; it did not force an independent loaded-layer
  inspection. The new review rejects the candidate and requires exact
  screenshots plus DOM/computed-style evidence rather than trusting a `PASS`
  label.

## Automation Contract

### Treat as regression guard when

- changing recommendation row packing, `EventCard` media treatment, semantic
  crop policy or personal-feed rendering;
- changing event keyboard-router activation, full-document navigation, focus
  restoration or hero gallery behavior;
- changing footer service sharing, copy shortcuts or mixed pointer/keyboard
  ownership;
- accepting or publishing a full immutable static-event candidate.

### Affected surfaces

- `site/src/lib/relatedCardLayout.mjs`;
- `site/src/components/EventCard.astro`, `DesktopEventPage.astro` and
  `PersonalFeedSlot.astro`;
- `site/src/lib/keyboardEventNavigation.mjs`;
- production Playwright and generated-candidate acceptance scripts;
- immutable `/_review/<token>/sobytiya/**` pages.

### Mandatory checks before closure or deploy

- Run a live Chromium visual/geometry gate on the actual generated related rows
  at `1536×864` and the reported FHD/125% geometry. Assert that declared
  treatment, computed `object-fit`, final row ratio and crop/letterbox budgets
  agree; retain screenshots of the rows, not only the footer.
- For every loaded recommendation image, wait for `decode()`, assert that the
  media shell is `is-image-loaded`, the real image is paint-visible and the
  fallback layer has no visible box. Missing images must retain the fallback.
  Retain both a related-section capture and an FHD/125%-equivalent viewport
  capture.
- Keep unsafe OCR/protected-region images fail-closed to `contain`; row packing
  must consume that final decision rather than re-enable blind crop.
- Perform the real two-page journey without intercepting navigation:
  `6408` → gallery → final recommendation → Enter → destination → Right/Left;
  assert focus ownership, hero index/source changes and telemetry.
- Cover single-image destinations and non-event/body contexts as negative
  controls.
- On a direct load and reload with natural `BODY` focus, Left/Right must enter
  and move a multi-image hero on first physical intent; a single-image page
  must remain unchanged. After a real mouse click on inert current-event copy,
  physical Russian-layout `KeyL/KeyK/KeyS/Enter` must route to like, calendar,
  event share and primary CTA. A real header click, editor, dialog, browser
  blur and hidden document must disarm that recovery.
- Reproduce touchpad/wheel scroll to a visible footer with both `BODY` focus and
  retained off-screen card focus; verify deterministic `P`/`S` ownership,
  correct clipboard payload and visible success/failure feedback.
- Run the same mixed pointer/keyboard journeys in Firefox and Safari/WebKit and
  complete the already-documented accessibility/zoom/reflow gates.
- Obtain a Gemini Pro-class visual/interaction review of the actual immutable
  production-data candidate and retained screenshots. A prototype-only verdict
  is insufficient.
- Before any publication, verify clean `origin/main` reachability, exact build
  SHA, noindex/no-referrer, root non-mutation and the active incident contracts
  listed above.

### Required evidence

- live Playwright traces, computed geometry JSON and related-row screenshots;
- real cross-document navigation trace and footer clipboard/toast trace;
- exact Gemini Pro model/provenance and verdict for the immutable candidate;
- candidate build/run/manifest/SHA receipt and read-only 48-hour diagnostics;
- deployed SHA reachable from `origin/main` if a later fix is released.

## Immediate Mitigation

- Do not treat the reported `iiHL…` candidate as accepted for rollout.
- Preserve its immutable prefix and evidence for regression testing; publish
  the correction only as a new noindex secret candidate.
- The production root remains unchanged.

## Corrective Actions

- [x] Moved row packing and `EventCard` rendering onto the same authoritative
  protected-crop resolver and serialized its final `visual-cover`,
  `visual-contain` or `document-contain` decision to runtime cards.
- [x] Added the same-origin, 30-second gallery handoff and provenance-gated
  destination `BODY` arrows without making arrows global on ordinary loads.
- [x] Made a visible footer own service `P`/`S` from `BODY`, an off-screen
  managed card or the off-screen current-event action surface, while visible
  editors, dialogs, headers and event controls retain their own context.
- [x] Added a pinned-Chromium generated-tree gate for final computed crop,
  canonical `EventCard` navigation, real cross-document gallery Enter and
  footer clipboard/toast behavior. Build manifests receive `browser_visual=ok`
  only after every browser assertion passes.
- [x] Removed the unbounded/discouraged Playwright `networkidle` wait, bounded
  browser actions/navigation and the Kaggle browser-gate subprocess, and
  force-close residual local HTTP sockets so the visual gate cannot hold the
  production single-flight claim indefinitely.
- [x] Made the generated-tree gate cross the real end-of-related observer
  boundary with wheel-like scrolling before asserting `Ещё события`; a direct
  jump to `scrollHeight` could skip the trigger and was not the reported user
  journey.
- [x] Made the prepublication browser gate serve immutable `/_astro/**` runtime
  requests from the checked tree itself. The CDN build prefix is create-only
  and intentionally does not exist until the gate passes; testing its 404s
  left interaction code unloaded and could never certify the user journey.
- [x] Corrected the Kaggle browser bootstrap after the first exact-main rerun
  proved that binary-only Playwright installation left `libatk-1.0.so.0`
  missing. Production candidates now install the pinned headless shell with
  Playwright's documented Linux dependencies, and launch failure cleans up the
  local fixture without waiting for the outer watchdog.
- [x] Bound related caches to the atomic vector-corpus receipt and added exact-
  normalized-title reciprocity plus fail-closed graph topology checks.
- [x] Published and verified a fresh immutable secret candidate from
  `origin/main` without mutating the stable root or stable ICS namespace.
- [ ] Supersede rejected `RHOg…` with a new immutable noindex candidate whose
  retained browser evidence proves loaded-layer visibility, cold/reload arrows
  and real inert-click Russian-layout shortcuts.
- [ ] Complete native Firefox/Safari, screen-reader, high-contrast and
  zoom/reflow checks before any later root rollout.

## Follow-up Actions

- [x] Unify row packing and card rendering around one final, authoritative media
  treatment derived from semantic role, current geometry and crop feasibility.
- [x] Define and implement a safe post-navigation keyboard-entry contract for
  destination event pages without introducing broad global arrow capture.
- [x] Define footer ownership for touchpad-scroll-plus-keyboard use, including
  the off-screen-focus ambiguity case and mandatory feedback.
- [x] Add the missing live visual and multi-page/mixed-input scenarios to the
  canonical Playwright matrix.
- [ ] Repeat generated-tree plus live immutable-candidate Chromium acceptance
  with the expanded evidence contract and obtain a fresh Gemini 3.1 Pro
  verdict. The previous `SHIP` is explicitly invalidated by owner reproduction.
- [ ] Repair the 28 static-build observability consistency issues reported in
  the final 48-hour diagnostic window. These historical reconciliation issues
  do not invalidate the exact successful receipt, but they remain operational
  debt and must not be hidden by aggregate success counts.

## Superseded Release Evidence

- deployed SHA: `1e7594d22c545f535c131aef3e9f9e5bddddd9f3`, reachable from
  `origin/main`; Fly release `v1734`
- deploy path: Smart Update/Kaggle secret candidate only; immutable URL
  `https://kenigevents.ru/_review/RHOgBCJMl527-JF5Cke3gF-n7Zsmyi_0gkC9tor_Bek/`;
  stable root promotion remains forbidden
- build receipt: build `production-secret-20260720T201154-77720953`, run
  `static-site:production-secret-20260720T201154-77720953:9b135cf41b8c`,
  snapshot `snapshot-20260720T181200-aee8fedca5`, `248` event pages, `937`
  published objects, `root_mutation=false`, `stable_ics_mutation=false`
- generated-tree regression checks: production and secret candidate both
  passed `related_geometry_crop`, canonical `EventCard` Enter navigation, real
  gallery cross-document navigation and footer shortcuts; both manifests have
  `browser_visual=ok`
- live post-publish checks: `16` related/continuation cards inspected, bounded
  continuation count `6`, gallery target `starshiy-syn-kaliningrad-6407`, hero
  Right/Left source change/restore, and footer clipboard/toasts all passed
- consultant acceptance: the earlier agy `Gemini 3.1 Pro` `SHIP` is superseded
  and must not be cited as closure evidence; the corrective review verdict is
  `REJECT`
- production health after completion: `/healthz` reports `ok=true`,
  `ready=true`, no issues; current 48-hour diagnostic reports `60` requests,
  `19` successes, `25` failed attempts, `13` busy deferrals and `2` no-ops,
  plus `28` historical consistency issues requiring follow-up

## Prevention

Static-event acceptance must be journey- and output-based: one shared card
component is not enough when independent row and CSS policies can contradict
it, and one-page keyboard unit flows are not evidence for cross-document or
mixed-input behavior. A production-data candidate is accepted only after its
actual visual geometry and real navigation paths are tested.
