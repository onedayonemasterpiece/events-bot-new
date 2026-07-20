# INC-2026-07-20 Static event recommendation crop and keyboard ownership regressions

Status: open
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

No fix, rebuild, candidate activation or production-root change was made during
this investigation.

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

- Do not treat the current candidate as accepted for rollout.
- Do not rebuild, activate or deploy from this investigation; preserve the
  immutable failing prefix and evidence for regression testing.
- The production root remains unchanged.

## Corrective Actions

- No corrective code was applied in this investigation, per owner request.

## Follow-up Actions

- [ ] Unify row packing and card rendering around one final, authoritative media
  treatment derived from semantic role, current geometry and crop feasibility.
- [ ] Define and implement a safe post-navigation keyboard-entry contract for
  destination event pages without introducing broad global arrow capture.
- [ ] Define footer ownership for touchpad-scroll-plus-keyboard use, including
  the off-screen-focus ambiguity case and mandatory feedback.
- [ ] Add the missing live visual and multi-page/mixed-input scenarios to the
  canonical Playwright matrix.
- [ ] Run candidate-wide visual acceptance and Gemini Pro review before any
  future rollout claim.
- [ ] Repair 17 static-build observability consistency warnings identified in
  the 48-hour diagnostics window.

## Release And Closure Evidence

- deployed SHA: not applicable; no fix or deploy was requested or performed
- deploy path: none
- regression checks: read-only live reproductions completed; closure checks are
  intentionally pending; fresh Gemini Pro verdict is `REJECT`
- post-deploy verification: not applicable

## Prevention

Static-event acceptance must be journey- and output-based: one shared card
component is not enough when independent row and CSS policies can contradict
it, and one-page keyboard unit flows are not evidence for cross-document or
mixed-input behavior. A production-data candidate is accepted only after its
actual visual geometry and real navigation paths are tested.
