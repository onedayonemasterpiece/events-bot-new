# Current UI Decoder v1 — controlled specimen harness lane

## Result

Implemented an executable, temporary Astro specimen harness without changing any production Astro, CSS, UI, Penpot, token, or normalization source.

### API

- `buildSpecimenRegistry()` — bounded first-wave controlled plans, exact real-route verification bindings, and explicit source-model-only cases.
- `materializeSpecimenHarness()` — copies the pinned candidate `site/src` into a temporary `upstream/` tree using reflink/copy, links exact `node_modules` and `public`, and generates wrapper pages only under the temporary harness.
- `buildSpecimenHarness()` — builds the temporary tree with the exact candidate Astro CLI and forces the production baseline transport experiment mode `off` unless an explicit test environment override is supplied.
- `startSpecimenServer()` — serves only the built temporary `dist` tree on loopback.
- `captureControlledSpecimens()` / `captureWithExactPlaywright()` — component-scoped two-buffer capture with screenshot SHA-256 and decoded-PNG perceptual dHash.
- `resolveExactRealRouteBindings()` / `captureExactRealRoutes()` — resolves `event_id` through the exact catalog to a manifest-relative key and runtime observation, then captures bounded mobile/desktop evidence without retaining the candidate base URL.
- `adaptEvidenceForSnapshot()` — emits stable canonical specimen-observation and page-verification rows with source and route-binding refs; review and production-equivalence claims remain pending/false.
- `assertSpecimenRegistry()`, `assertEvidencePacket()`, `validateTraceIntegrity()` — fail-closed checks for dangling references, fake production claims, unsafe evidence fields, sensitive values, invalid fixture deltas, unbounded first-wave plans, and incomplete six-capsule coverage.

### First wave

- 19 controlled specimens / 20 capture steps:
  - generic Button default/focus/disabled;
  - FocusEgg locked/eligible/found/unavailable, including focus evidence;
  - Amber before/after collection;
  - ArtifactCollection empty and found/open-dialog;
  - rail explicit and schedule-cutoff real rows plus an explicit forecast-only controlled delta;
  - Kaup full/open-details and compact real rows;
  - real-event medallions for zero, one, desktop top, and many/no-top states.
- 26 separate exact real-route bindings and 49 contexts when all routes are reachable: 390px mobile plus 1728px desktop for production routes, desktop-only for desktop CTA labs.
- Event bindings use `/sobytiya/{slug}/`; CTA bindings use exact `/lab/event-desktop/examples/cta-*-invariant/` routes.
- A missing catalog row, manifest key, or runtime observation becomes explicit `explicit-unreachable` evidence instead of a synthetic route or false capture.
- Production artifact routes explicitly verify the unavailable shell and absent Amber state.
- `bus-no-outbound-groups` remains `source-model-only`: the exact pinned data has no reachable real event for the branch, so the harness does not synthesize schedule groups or claim production evidence.

Medallion wrappers accept only a real `PreviewEvent` catalog row plus the real component props `layout` and `allowTopSlot`. They do not accept or render projection-token fixtures. Event fixture deltas are closed to transport-relevant fields and cannot change identity.

### Evidence packet boundary

Captured packets contain bounded DOM attributes (sensitive attribute names redacted), text/media source hashes rather than full content/URLs, ARIA snapshot, computed style, geometry, CSS variables, pseudo state, focus/open/hidden/disabled state, media dimensions, media-query context, aggregate console/network facts, and two element PNG hashes. Full HTML and raw network URLs are forbidden.

Real-route resolution requires the pinned manifest `repo_sha`; the runtime observation content hash and live main-document response bytes must both equal the exact manifest file hash before element evidence is accepted.

All controlled observations remain `captured-not-reviewed`, `pending-human-visual-review`, `production_state_claimed: false`, and `normalization_allowed: false`.

## Verification

- Unit and existing v1 decoder suite:
  - `uv run --with pytest pytest -q --confcutdir=tests/fixtures/current-ui-decoder-v1/specimens tests/test_current_ui_decoder_v1_*.py`
  - **50 passed**.
- Exact local smoke build:
  - Astro **6.4.8** from the exact linked candidate `node_modules`.
  - **19/19** generated temporary wrapper pages built successfully.
- Temporary end-to-end browser smoke:
  - **20/20** evidence packets completed.
  - **20/20** exact sequential screenshot buffers stable.
  - **20/20** perceptual dHashes stable.
  - **0** console errors; **0** failed requests.
  - A contact sheet was manually inspected for obvious empty, clipped, or failed renders. The intentionally absent zero-token medallion host was retained as absence evidence.
- Real-route capture API smoke against the temporary static server:
  - **1/1** bounded element packet captured;
  - exact and perceptual stability both passed;
  - serialized URL leak check was false;
  - review status remained `pending`.

Smoke artifacts were written under `/tmp` and are not committed. These results validate the harness implementation only; they do not constitute the final immutable Actions capture or capsule acceptance.

## Integration notes

- New module entrypoint: `scripts/current_ui_resource_graph/v1/specimens/index.mjs`.
- Real-route implementation: `scripts/current_ui_resource_graph/v1/specimens/real-routes.mjs`.
- The snapshot/integration owner must explicitly consume registry plans and reviewed observation records; this lane does not mutate `decode.mjs` or claim final handoff GO.
- Exact real-route capture remains a separate registry and must be reconciled after route screenshots are visually reviewed.
