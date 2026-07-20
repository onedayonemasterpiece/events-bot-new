# INC-2026-07-20 Static event recommendation crop and keyboard ownership regressions

Status: mitigated / awaiting immutable-candidate verification
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

The corrective implementation now exists on the integration branch; no
production-root promotion is allowed. Closure still requires the immutable
production-data candidate, its browser receipt and consultant acceptance.

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

### Cross-document hero navigation

1. A full document navigation correctly resets DOM focus to `BODY`.
2. The production keyboard router handles closed-hero Left/Right only when the
   event surface or one of its descendants owns focus. Body-targeted recovery
   is intentionally implemented only for selected actions such as provenance-
   gated `L`.
3. The destination page contains seven valid image slides and responds as soon
   as its event surface is focused manually; the gallery and image-selection
   data are not the failure.

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
- Its single final screenshot is captured after the journey near the
  continuation/footer, is not compared, and does not gate the broken related
  rows.
- The static production contract checks hero/gallery fit, not the effective
  fit of generated recommendation cards.
- The earlier external review covered the frozen V7 prototypes, not the later
  production integration, real candidate data or bbox merge.
- The fresh production-data Gemini review rejects the candidate. Its clarified
  verdict relies on observed ownership/feedback behavior, not on attempting to
  infer PNG clipboard success from `navigator.clipboard.readText()`.

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
- Keep unsafe OCR/protected-region images fail-closed to `contain`; row packing
  must consume that final decision rather than re-enable blind crop.
- Perform the real two-page journey without intercepting navigation:
  `6408` → gallery → final recommendation → Enter → destination → Right/Left;
  assert focus ownership, hero index/source changes and telemetry.
- Cover single-image destinations and non-event/body contexts as negative
  controls.
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
- [x] Bound related caches to the atomic vector-corpus receipt and added exact-
  normalized-title reciprocity plus fail-closed graph topology checks.
- [ ] Publish and verify a fresh immutable secret candidate from `origin/main`.
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
- [ ] Run candidate-wide visual acceptance and Gemini Pro review before any
  future rollout claim.
- [ ] Repair 17 static-build observability consistency warnings identified in
  the 48-hour diagnostics window.

## Release And Closure Evidence

- deployed SHA: pending merge to `origin/main`
- deploy path: pending Fly deployment and Smart Update secret-candidate rebuild;
  stable root promotion remains forbidden
- regression checks: unit/integration and generated-tree Chromium checks are
  required; exact command/report and immutable candidate receipt are recorded
  at release time
- post-deploy verification: pending new secret candidate and Gemini Pro review

## Prevention

Static-event acceptance must be journey- and output-based: one shared card
component is not enough when independent row and CSS policies can contradict
it, and one-page keyboard unit flows are not evidence for cross-document or
mixed-input behavior. A production-data candidate is accepted only after its
actual visual geometry and real navigation paths are tested.
