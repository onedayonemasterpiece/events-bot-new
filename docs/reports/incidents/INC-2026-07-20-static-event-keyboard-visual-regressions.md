# INC-2026-07-20 Static event recommendation crop and keyboard ownership regressions

Status: open / focused crop and keyboard automated acceptance passed; owner review pending
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

The second corrective candidate `DJc9…` also failed owner acceptance. It hid
the fallback layer correctly but preserved `visual-contain`, leaving ordinary
photographs visibly letterboxed by 7–32% inside fixed recommendation frames.
The earlier green receipt and Gemini verdict are therefore superseded for crop
acceptance. The actual correction must restore the previously accepted compact
`visual_only` cover policy through the shared `EventCard`, while keeping real
OCR/text/unknown documents contained.

On 2026-07-21 the owner also rejected the later `2BxK…` candidate: a
`visual_only` main gallery image on event `6408` was still letterboxed above and
below because semantic-role uncertainty overrode the OCR classification. The
owner additionally superseded the compact-card document rule: fixed card frames
may not use `contain` at all. Ordinary documents must define the row's natural
ratio without crop; only very tall documents may crop, capped at `20%`, and a
global grouping search must minimize total page height while keeping both media
and card heights equal per row. Therefore the 2026-07-20 `SHIP_SECRET_CANDIDATE`
verdict remains historical evidence, not current acceptance.

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
- 2026-07-20 20:22 UTC — exact `origin/main` SHA
  `1810a12bb6eacce10361c0c05900cecd3708f4d3` was deployed as Fly release
  `v1735`; release `v1736` then changed only the immutable
  `STATIC_SITE_REPO_SHA` pin to that same SHA. The stable web root remained
  untouched.
- 2026-07-20 20:23–20:31 UTC — the already-running old-SHA Kaggle handoff was
  allowed to finish. After the Fly restart, the existing built-in outbox
  reconciliation path rearmed that orphaned runner owner; no second remote
  builder was started for it.
- 2026-07-20 20:34 UTC — static build preflight correctly deferred while the
  Fly volume had less than the configured `1024 MiB` free-space floor. Two
  unreferenced stale local build/snapshot trees were removed after checking
  that no pending/running job referenced them, recovering `563,472,029` bytes.
  After both static jobs became terminal, the last orphaned runner scratch tree
  was also verified unreferenced and removed; total Fly-volume recovery was
  `855,078,330` bytes.
- 2026-07-20 20:37–20:57 UTC — job `39144` built, browser-gated and published
  candidate `DJc9…` from exact SHA `1810a12b`; the older coalesced job `39115`
  then completed as a fingerprint `noop`, proving the single-flight/fingerprint
  path did not publish a stale duplicate.
- 2026-07-20 20:58 UTC — an independent live Chromium pass on the exact public
  secret URL repeated loaded-layer geometry, fresh/reload arrows, inert-click
  Russian physical codes, the `6408` → gallery → `6407` document transition,
  single-image `6593` negative control and footer clipboard/toast flows.
- 2026-07-20 21:00–21:01 UTC — agy `Gemini 3.1 Pro (High)` independently
  inspected the exact candidate, source, computed report and PNG pixels. It
  explicitly confirmed that the production listener is `pointerdown`, not the
  earlier review's mistaken `pointerup`, and returned
  `SHIP_SECRET_CANDIDATE`. Native Firefox/Safari, assistive technology,
  high-contrast and zoom/reflow remain root-rollout blockers.
- 2026-07-20 21:12 UTC — owner review rejected `DJc9…`: ordinary photographs
  still used `contain`, with empty bands of roughly 22% (`Собачье сердце` and
  one `Ромео и Джульетта`), 7% (second `Ромео и Джульетта`) and 32%
  (`Женитьба`). Hiding fallback content had corrected a symptom, not the crop.
- 2026-07-20 22:08–22:13 UTC — the historical compact-card behavior was traced
  to the shared row policy introduced at `621d6f8e`. A local 303-event build
  restored `visual_only` cover for both static and runtime recommendation rows;
  the retained `6408` decoded screenshot has no ordinary-photo bands and keeps
  a real OCR document contained as the negative control.
- 2026-07-20 22:13–22:15 UTC — agy `Gemini 3.1 Pro (High)` inspected the current
  diff, historical implementation and screenshot pixels, returned
  `READY_TO_COMPLETE_LOCAL_GATE`, and required captured production payload
  fixtures plus an explicit computed-style browser guard. Both were added; a
  production-family generated-tree run and immutable candidate review remain
  mandatory before publication.
- 2026-07-20 22:30–22:33 UTC — Fly release `v1738` deployed exact
  `origin/main` crop SHA `2d3d5f3bc5ea4a7f23313ed5687dda697f30ad13`;
  release `v1739` corrected the separately stored `STATIC_SITE_REPO_SHA` secret
  to that same SHA. Public `/healthz` returned ready and the runtime file mirror
  remained enabled under `/data/runtime_logs`.
- 2026-07-20 22:47–22:57 UTC — build
  `production-secret-20260721T004740-5e5fb9bf` ran the complete production and
  secret-candidate generated-tree browser gates from the crop SHA. An unrelated
  guide-monitoring release restarted Fly while the remote kernel was live, but
  the durable handoff retained the exact snapshot/run identity; the kernel
  completed and released its exact `static_site:builder` lease.
- 2026-07-20 23:03–23:14 UTC — host recovery adopted the terminal kernel,
  hash-validated its result and all `937` create-only candidate objects, and
  completed the current-review receipt without overwriting the stable root or
  stable ICS namespace. Stale runner scratch and one unreferenced terminal
  snapshot were removed only after exact-run/reference checks; `/data` finished
  with about `1848 MiB` free and `PRAGMA quick_check=ok`.
- 2026-07-20 23:21 UTC — independent live Chromium acceptance on the actual
  immutable URL verified the four owner-reported ordinary-photo canaries as
  computed `cover` with zero unused frame, retained three real document
  `contain` controls, found no loaded fallback bleed, and passed the cold,
  Cyrillic mixed-input, cross-document gallery and footer shortcut journeys.
- 2026-07-20 23:26–23:27 UTC — agy `Gemini 3.1 Pro (High)` independently
  inspected the immutable URL, source, retained PNG pixels and live report and
  returned `SHIP_SECRET_CANDIDATE`. This is not production-root approval;
  incident closure still requires owner visual acceptance.
- 2026-07-21 — owner acceptance rejected `2BxK…`: event `6408` still had
  non-OCR hero/gallery bands and the compact-card document-`contain` policy did
  not satisfy the required no-fields/equal-row/global-minimum contract. A new
  implementation and a fresh immutable noindex candidate are required; the
  production root remains untouched.
- 2026-07-21 09:21 UTC — PR `#117` merged the replacement implementation into
  `origin/main@58440062e7bab708676c378de345c65f19ce91b1`; both `python-ci` and
  `static-browser-release-gate` completed successfully.
- 2026-07-21 10:04 UTC — Fly release `v1741` deployed that exact main SHA and
  pinned the static builder to the same repository identity. Three subsequent
  `/healthz` probes were ready with no issues.
- 2026-07-21 10:04–10:26 UTC — production-secret build
  `production-secret-20260721T120452-b290f999` completed its production and
  secret generated-output/browser gates and published the create-only `D1qL0…`
  noindex prefix. The receipt reports `root_mutation=false` and
  `stable_ics_mutation=false`.
- 2026-07-21 10:31–10:35 UTC — live Chromium on the exact immutable candidate
  verified the `6408` hero without top/bottom fields, ten related plus six
  hydrated cards with zero unused frame and equal row/card geometry, the real
  reciprocal occurrence family `6686`/`6687`, cold/mixed-input keyboard,
  cross-document gallery and footer shortcuts. Stable-root and sitemap body
  hashes remained byte-identical.
- 2026-07-21 10:38–10:40 UTC — after following the official AGY headless
  permission contract, independent `agy` model `gemini-3.1-pro-high` at high
  effort inspected the exact URL, retained PNG pixels, source and reports and
  returned `SHIP_SECRET_CANDIDATE`. This is acceptance of the secret candidate,
  not permission to promote the stable root; owner acceptance remains pending.
- 2026-07-21 — owner review rejected `D1qL0…` despite the previous automated
  and Gemini verdict: the optimizer emitted middle partial rows (`2,3,2,3` for
  the ten `6408` cards), and fixed `184px` bodies left roughly `63–86px` blank
  per card. The same page also exposed the data coverage gap for Romeo events
  `6318`/`6586`: the occurrence components are present, but both production
  rows export empty explicit links, so the reciprocal-only resolver correctly
  cannot collapse them. The prior acceptance is superseded. Only a fresh
  isolated `preview-*` page for `6408` may be shown next; no all-page candidate
  or stable-root rollout is allowed before owner visual approval.
- 2026-07-21 11:39–11:50 UTC — branch `9dced876` built a focused noindex
  `preview-*` namespace using the production-family renderer and an isolated
  reciprocal review fixture for `6318 ↔ 6586`; canonical production data was
  not mutated. Generated-output checks, `77` Node tests, `10` occurrence tests,
  the `15`-test Dramteatr identity regression and the complete Chromium release
  gate passed. Live `6408` rows are `3,3,3,1`; body height fell from the
  rejected fixed `184px` to about `132.6px`, saving about `206px` over four
  rows, and the single Romeo card exposes `2, 3 ноября 19:00` with aria
  `2 и 3 ноября в 19:00`. Stable root and sitemap SHA-256 remained unchanged.
  The full preview id/link stays in the operator handoff, not Git; owner visual
  approval is still required before canonical repair or all-page generation.
- 2026-07-21 11:55 UTC — mandatory independent agy model
  `gemini-3.1-pro-high` at high effort opened the exact live focused URL,
  inspected the retained full-section screenshot and computed DOM geometry,
  and returned `ACCEPT_FOCUSED_6408`: all five row/compactness/crop/occurrence/
  noindex groups passed with no owner-visible defect. The verdict explicitly
  does not authorize full-site rollout and remains subordinate to owner visual
  acceptance.
- 2026-07-21 — owner found the crop and card dimensions sufficiently correct,
  but rejected keyboard behavior after CSS row reordering: horizontal arrows
  followed DOM adjacency instead of the visible grid. The same review rejected
  ten simultaneous `K` badges as ambiguous because only one focused card owns
  the shortcut. Full-site generation remains paused; the next artifact is
  again a focused `6408` preview only.
- 2026-07-21 12:45 UTC — required agy `gemini-3.1-pro-high` product review
  reproduced the split ordering model (geometric Up/Down versus DOM-indexed
  Left/Right) and accepted a visual 2D matrix plus a single `:focus-within`
  keycap. Its explicit negative controls are zero badges at rest, hover-only
  zero, one focused badge, focused-card `KeyK`, ragged-row navigation and
  related/continuation bridges.
- 2026-07-21 13:11–13:17 UTC — pushed commit `ff4a4950`, then published a new
  create-only focused `preview-*` namespace for event `6408`; stable root and
  sitemap SHA-256 remained byte-identical and the live page retained strict
  `noindex`/`no-referrer`. Live Chromium confirmed visual rows `3,3,3,1`,
  row wrap, nearest-centre ragged-row movement, both section bridges, zero `K`
  badges at rest/hover, exactly one on the focused card, focused-card `KeyK`,
  and one visible Romeo card labelled `2, 3 ноября 19:00`. Required final agy
  `gemini-3.1-pro-high` acceptance returned `PASS` for all five contract groups.
  This remains a focused owner-review artifact only and does not authorize a
  canonical data repair, all-page generation or stable-root rollout.

## Root Cause

### Recommendation image geometry

1. The bbox rollout applied the strict large-surface semantic crop gate to
   compact recommendation previews. It therefore let stale/missing role or
   protected-region metadata demote an already classified `visual_only` photo
   to `contain`.
2. The later unification correctly made `site/src/lib/relatedCardLayout.mjs`
   and `EventCard.astro` agree, but agreed on that wrong surface policy. The
   row stayed compact while ordinary photos were letterboxed.
3. The accepted pre-bbox recommendation mechanism used `visual_only` as the
   compact-card crop boundary. The regression was not lack of unification; it
   was importing a hero-grade fail-closed rule into the wrong surface.
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

### Reordered card navigation and shortcut hint

1. The row optimizer intentionally assigns CSS grid coordinates that can differ
   from source/DOM order.
2. Up/Down already grouped rendered rectangles, while Left/Right, Home/End,
   first-card entry and rerender fallback still indexed DOM arrays. One focus
   graph therefore used two contradictory orders and appeared unpredictable.
3. Every eligible card calendar rendered its decorative `K` keycap at once.
   The action is scoped to one focused card, so the repeated affordance did not
   identify its actual owner and added avoidable visual noise.
4. The correction uses the rendered visual matrix for every card-order
   transition and keeps all `aria-keyshortcuts` semantics while revealing only
   the focused card's decorative `K` via `:focus-within`.

## Contributing Factors

- The keyboard acceptance script blocks real gallery CTA navigation with
  `preventDefault()` and verifies only the captured destination URL.
- The footer test first asserts body `P` is a no-op, then explicitly focuses
  the footer image and text buttons before testing `P` and `S`; it does not
  model touchpad scroll followed by an unfocused shortcut.
- The Playwright gate checks card count, focus/actions and horizontal overflow,
  but historically did not compare focused IDs to rendered row/column geometry
  or count visible shortcut hints.
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
- Keep OCR/text/unknown **hero** media fail-closed to `contain`, but require
  every `visual_only` hero/gallery slide to use `cover` even when semantic-role
  metadata is uncertain. In compact event-detail rows every card must use
  `cover` and have an independently computed unused-frame ratio of zero.
  Ordinary documents must remain uncropped by adapting the row ratio; only very
  tall documents may crop, and their decoded area loss must be `<=20%`.
- Enumerate feasible compact-card groupings globally, allow row reorder, and
  prove that the selected grouping minimizes total normalized page height.
  Assert equal media heights and equal total card heights inside every row.
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
- On CSS-reordered related and continuation cards, build visual rows from
  rendered rectangles and assert Left/Right row progression and wrap, Up/Down
  nearest-center movement, ragged-final-row behavior and both section bridges.
  Assert zero visible card `K` badges at rest and after hover, exactly one on
  the focused card, and that `KeyK` invokes that same card. Keep the keycap's
  reserved layout width and permanent accessible `aria-keyshortcuts` metadata.
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
- [x] Superseded rejected `RHOg…` with `DJc9…`; keyboard and loaded-layer checks
  passed, but owner review rejected its still-letterboxed crop.
- [x] Restored the historical compact `visual_only` cover policy in the single
  shared row/Card resolver, preserved exported focal positions, and added exact
  `6408` payload canaries plus an independent unused-frame browser budget.
- [x] Published and verified a new immutable noindex candidate from that
  correction, including independent live Chromium and Gemini 3.1 Pro review;
  owner review later superseded its crop acceptance.
- [x] Replaced compact document `contain` locally with globally optimized bounded
  cover: no bands, exact natural ratio for ordinary documents, at most 20% crop
  for very tall documents, and equal media/card heights per row. Non-OCR hero
  and gallery slides now cover independently of uncertain semantic role.
- [x] Published and inspected fresh immutable noindex replacement `D1qL0…`;
  repeated generated-output and live-browser gates, then obtained independent
  agy `gemini-3.1-pro-high` verdict `SHIP_SECRET_CANDIDATE` over the exact URL,
  retained screenshots and computed geometry.
- [ ] Replace `D1qL0…` with an owner-reviewed focused `6408` preview whose
  non-final rows are full and whose card chrome is intrinsic per row.
- [x] Replace DOM-indexed horizontal card movement with one rendered visual
  matrix for entry, Home/End, Left/Right, Up/Down, rerender recovery and both
  card-zone bridges; reveal the decorative `K` only on the focused card without
  changing its accessible shortcut or shifting action layout.
- [ ] After owner approval, repair `6318 ↔ 6586` as a durable reciprocal
  explicit occurrence family with provenance/lock, then regenerate all pages;
  do not infer the family from matching copy or venue.
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
- [x] Repeat generated-tree plus live immutable-candidate Chromium acceptance
  with the expanded evidence contract and obtain a fresh Gemini 3.1 Pro
  verdict. The previous `SHIP` is explicitly invalidated by owner reproduction.
- [ ] Repair the 28 static-build observability consistency issues reported in
  the final 48-hour diagnostic window. These historical reconciliation issues
  do not invalidate the exact successful receipt, but they remain operational
  debt and must not be hidden by aggregate success counts.

## Current Replacement Secret-Candidate Evidence

The `D1qL0…` evidence below is superseded by owner rejection. The active
acceptance object is a focused `preview-*` namespace only, produced from branch
SHA `9dced876` plus an isolated explicit Romeo fixture. It did not advance the
`/_review` pointer and did not mutate stable root or stable ICS. It is not a
production-data or all-pages rollout approval.

- immutable prefix: `https://kenigevents.ru/_review/D1qL0…/` (full bearer URL is
  intentionally retained only in ignored acceptance artifacts and the operator
  handoff, not in Git)
- source identity: `origin/main@58440062e7bab708676c378de345c65f19ce91b1`;
  Fly release `v1741`, deployment image
  `deployment-01KY221C04PKKH7WZ9SMR204B1`
- build identity: `production-secret-20260721T120452-b290f999`; run
  `static-site:production-secret-20260721T120452-b290f999:82bfa8e8e4de`;
  snapshot `snapshot-20260721T100452-9c8cd823ac`
- receipt identity: result SHA-256
  `16c57759c57f1d31cd1a84cf5e4e30556a730abc1f71721d871e5bcb6b7b3f16`;
  manifest SHA-256
  `73cb6e4c3ea1ce22e22e29e6974323a17abea2219b34543f6d9e4a247ed5c884`;
  token SHA-256
  `630e6e32605b47e67d14035d396d2210c16ef8dbabbf3b5d2aae37e899d69f1f`
- isolation: candidate responses are `private, no-store`; HTML is `noindex` and
  `no-referrer`; stable root and sitemap remained SHA-256
  `e2ddecb6c2856a94d4579a3091604b7c0804f3545220f43e94eac73e0aab450d`
  and `643f22960e703b91c173d4d52425ca28b6513da9612904047d9930508e329fa7`
- live crop/geometry: `10` related and `6` hydrated cards, every image
  `object-fit:cover`, unused-frame ratio `0`, shared chrome range
  `299.00000–299.01563 px`; generated tall-document canary declares exact
  `20%` crop and measures `20.00235%` only because of Chromium subpixels
- occurrences: detail selectors are rendered for desktop, practical and mobile
  without occurrence cards; real reciprocal family `6686`/`6687` exposes one
  alternative. Exact formatter/rail strings remain enforced before the
  production tree intentionally strips `/lab` routes.
- consultant: `/home/dev/.local/bin/agy`, model id `gemini-3.1-pro-high`, high
  effort, clean provider exit `0`, verdict `SHIP_SECRET_CANDIDATE`
- retained evidence (ignored):
  `artifacts/codex/static-crop-occurrences-20260721/` contains the local and
  live browser reports, PNGs, HTTP headers, root hashes, deploy log and agy
  brief/review/provenance.

The incident stays mitigated rather than closed until the owner accepts the
secret link. Firefox/Safari, assistive-technology, high-contrast and zoom/reflow
checks remain mandatory before any future stable-root rollout.

## Corrected Secret-Candidate Evidence

- immutable URL:
  `https://kenigevents.ru/_review/2BxKLmLKkRXG7uuiNjbvUC1g_dy7Kw1gtaVdnfG5Lj4/`
- source identity:
  `origin/main@2d3d5f3bc5ea4a7f23313ed5687dda697f30ad13`; Fly releases
  `v1738` (image) and `v1739` (exact static-builder SHA secret pin). The SHA
  remains reachable from current `origin/main`.
- build identity: `production-secret-20260721T004740-5e5fb9bf`; run
  `static-site:production-secret-20260721T004740-5e5fb9bf:79c9ed254238`;
  snapshot `snapshot-20260720T224740-cfb8cf9f55`; input fingerprint
  `9f3149691ebd63a49321da1fa780316e0974c05c714846d04518d8f87295a6c1`
- receipt identity: result SHA-256
  `ce294733fd89629ae66c31137149537241b6440c90b4dac644dfc5500ab8fc69`;
  manifest SHA-256
  `aaf69f91a13610afd0009bb081c3f3ab5e3b84414064b7a25cd9be4fbbb96705`;
  token SHA-256
  `12fa09b37d15f42208dc3b3d880cb35f4ede1ab009460fea686a72cfdf6f316b`;
  `937` verified objects
- isolation: `noindex,nofollow,noarchive,nosnippet`, `no-referrer`,
  `root_mutation=false`, `stable_ics_mutation=false`; the stable root body
  remained SHA-256
  `e2ddecb6c2856a94d4579a3091604b7c0804f3545220f43e94eac73e0aab450d`
- live crop evidence: all ordinary-photo cards returned computed
  `visual-cover` / `object-fit:cover` / `unusedFrameRatio=0`, including event
  `5757` (`Собачье сердце`), `6586` and `6318` (the two
  `Ромео и Джульетта` cards) and `5756` (`Женитьба`). Document controls
  `6477`, `3934` and `6610` remained `document-contain`; every decoded image
  had `fallbackVisible=false`.
- live keyboard evidence: cold `6408` Right/Left changed/restored a seven-image
  hero; single-image `6593` remained unchanged; an inert real click followed
  by physical Cyrillic `KeyL`, `KeyK`, `KeyS` and `Enter` reached the intended
  current-event actions; the gallery CTA navigated `6408` → `6407` and the
  destination hero arrows worked; visible-footer `P`/`S` copied `image/png`
  and service text with confirmation toast.
- operational convergence: jobs `39145` and `39146` are terminal, active build
  state is empty, the exact run's `static_site:builder` lease is `released`,
  `PRAGMA quick_check=ok`, runtime file logging is enabled, `/healthz` is ready
  and `/data` has about `1848 MiB` free.
- consultant: agy `Gemini 3.1 Pro (High)`, provider alias
  `gemini-3.1-pro-preview`, clean process exit `0`, verdict
  `SHIP_SECRET_CANDIDATE`. Native Firefox/Safari, assistive-technology,
  high-contrast and zoom/reflow validation remain explicit root-rollout
  blockers.
- retained local evidence (ignored):
  `artifacts/codex/INC-2026-07-20-static-event-keyboard-visual-regressions-v3/live-candidate/`
  contains the live report, exact test script, loaded-row PNGs, HTTP/root hashes,
  production state and Gemini brief/review/provenance.

## Rejected Second Secret-Candidate Evidence

- immutable URL:
  `https://kenigevents.ru/_review/DJc9V0Milp7igVHNO8xPopYxHo3pW0vmy-Eqhhubd54/`
- source identity: `origin/main@1810a12bb6eacce10361c0c05900cecd3708f4d3`;
  Fly releases `v1735` (image) and `v1736` (exact static-builder SHA pin)
- build identity: `production-secret-20260720T223718-960d2ff8`; run
  `static-site:production-secret-20260720T223718-960d2ff8:ae3ac226b597`;
  snapshot `snapshot-20260720T203718-846ed94662`
- receipt identity: result SHA-256
  `cb5508fe97839ea0919dabf43e4a09320f2e391fd4f517f160cdb691c069d4e8`;
  manifest SHA-256
  `a6438021161fdf7cbaddc589a0b37cd487ef76d96517b09c1e370d775c6edccc`;
  token SHA-256
  `99b76b075d8c807f63df5a26941ea0f0002a5708922485619142e421ea9a009c`
- generated output: `248` event pages, `853` pages, `931` files and three
  exact artifacts: `production_root`, `secret_candidate` and fail-closed
  `browser_evidence`; only the secret candidate was published
- indexing/isolation: `noindex,nofollow,noarchive,nosnippet`, `no-referrer`,
  prefix containment, `root_mutation=false`, `stable_ics_mutation=false`
- mandatory generated-tree Chromium checks:
  `related_geometry_crop`, `related_loaded_media`, `canonical_event_cards`,
  `cold_and_pointer_keyboard`, `gallery_cross_document` and
  `footer_shortcuts` all passed
- loaded related media: all `10` tested cards decoded successfully, their
  dynamic shells reached `is-image-loaded`, and every fallback had
  `display:none` / `fallback_visible=false`; the set included `cover`,
  `visual-contain` and `document-contain`
- keyboard evidence: event `6408` had `7` images and changed on the first cold
  `ArrowRight`; event `6593` had one image and stayed unchanged; both passed a
  real inert-description mouse click followed by physical `KeyL/KeyK/KeyS`
  with Cyrillic logical keys plus `Enter`; header, editor and modal dialog were
  negative controls
- journey evidence: the real final-gallery recommendation navigated from
  `6408` to `Старший сын` (`6407`) and destination Right/Left changed/restored
  its hero without a Down/Up primer
- footer evidence: visible-footer `P` copied `image/png`; `S` copied the
  service text and canonical root URL; the toast confirmed success
- consultant: agy `Gemini 3.1 Pro (High)`, provider alias
  `gemini-3.1-pro-preview`, clean exit `0`, verdict
  `SHIP_SECRET_CANDIDATE`; owner crop review superseded this verdict. This is
  neither valid crop acceptance nor approval for root rollout.
- final read-only 48-hour diagnostics at `2026-07-20T21:05:34Z`: `59`
  requests, `44` claims, `17` successes, `26` failed attempts, `13` busy
  deferrals and `2` no-ops. Generated-evidence totals are `1,240` event-page
  renders, `4,265` page renders and `620,931,528` bytes. The diagnostic still
  reports `28` historical ledger/history consistency issues; they are recorded
  debt rather than hidden by the successful current receipt.

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

## 2026-07-23 R7 follow-up: natural BODY copy ownership

Owner testing on event `6529` found one remaining keyboard gap: physical
`KeyC`/`KeyP` worked after focus entered the event action surface, but not from
the natural `BODY` focus of a freshly opened page. Russian layout exposed the
gap clearly (`KeyP` reports logical `key="з"`), although layout decoding was
not the cause.

The R7 correction keeps physical `KeyboardEvent.code` as the sole contract and
does not add Cyrillic `event.key` aliases. On fresh/provenance-armed BODY it
re-enters the event action surface with `preventScroll` and invokes description
or poster copy. Footer service ownership is evaluated first; editable fields,
dialogs, composition, modifiers and repeat remain fail-closed. Regression
acceptance now includes natural BODY `KeyC`/`KeyP` with Cyrillic logical keys,
not only a pre-focused action surface.

## 2026-07-28 release-gate correction: unavailable calendar action

The spatial keyboard gate now follows the same product contract as the page:
focus may move to any visually nearest continuation card, but the `K` hint is
rendered only when that exact card has an available calendar action. A
multi-day/range card that is deliberately calendar-ineligible must expose zero
`K` hints rather than a misleading shortcut. Calendar-eligible cards still own
exactly one visible hint, and `KeyK` remains scoped to the focused card.
