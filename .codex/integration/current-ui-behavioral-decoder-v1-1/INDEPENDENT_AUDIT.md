# Current UI Behavioral Decoder v1.1 — independent audit

- Auditor: `/root/behavioral_final_audit` (Archimedes), read-only acceptance reviewer
- Audited at: `2026-08-09T14:55:00Z`
- Exact UI source: `ef7aa62e45c60f7a12da6160f490719c0721ec03`
- Capture decoder: `c9ddf0feafcd80f6fc3aef0f221e8d5e058063ab`
- Reviewed materializer: `ec9ae943675a1098e95515cc8c41f2418c659630`
- Immutable Decoder v1 manifest SHA-256: `f7740f7f533c3f0cda5d4d0b8ebe98b565d7f521368b96462daecbd26522d5cc`
- Immutable Decoder v1 tree: `e77fc2457fadfdffb46ed2d90304ebb91e89a715`
- Actions run: <https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31318132051>
- Actions artifact ID: `9039433060`
- Actions/Release archive SHA-256: `c677f69572ccdbf5b7f1402037a3cb8c164bd2f503fae35eae9168c46eb8d909`
- Durable Release: <https://github.com/onedayonemasterpiece/events-bot-new/releases/tag/current-ui-behavioral-decoder-v1-1-run-31318132051>
- Manual full-resolution review ledger SHA-256: `97c8cbcf2e4bbc34fd7e8c03454f09219bfb723acd4751b89744d6a8eb0f7731`

## Verdict

**PASS for truthful evidence import with final status
`EVIDENCE_COLLECTION_INCOMPLETE`.**

This is not a `READY_FOR_PROJECT_NORMALIZATION_SYNTHESIS` receipt. The evidence
may be imported append-only into the design-system repository only while both
readiness blockers below remain explicit and unique.

## Verified counts and integrity

- 67 behavior packet plans: 57 captured/reviewed and 10 explicit blockers.
- 124 observations, 124 PNG rasters, 124 page-verification rows and 124
  full-resolution visual-review rows.
- 67 action-packet index rows.
- 29 unresolved rows with 29 unique IDs.
- All 124 observation/review/PNG tuples match by relative path, byte count and
  SHA-256.
- All compact outputs match the manifest; the receipt binds the exact manifest
  bytes.
- 124/124 rasters are perceptually stable and 115 are also byte-identical across
  the recorded stable pair. Perceptual or byte stability is not treated as
  human review.
- The manual ledger contains 99 `capture-valid-as-is` rows and 25 explicit
  visual-conflict rows across 13 plans.
- Every inspected decision is `NOT_MERGED`; controlled evidence does not claim
  production observation or equivalence; normalization remains forbidden.
- No production `site/src`, Astro, CSS, JS, Penpot, token, merge/split or
  experiment-winner mutation is part of this delivery.
- A strict independent secret scan found zero private-key, GitHub/AWS key, JWT,
  Bearer or query-token matches in the 58 non-PNG artifact files.
- The durable archive passed `unzip -t`; all 179 archive entries byte-match the
  extracted Actions artifact.

## Retained readiness blockers

1. `unresolved.behavior-blocker.864db42986f38970b1` — the exact focusable mobile
   rail has no working native `End`/`Home` behavior; Chromium remains at
   `scrollLeft=0`.
2. `unresolved.behavior-blocker.fdec1149e1f0d6b359` — the complete 293-row
   breakpoint/container source matrix is not covered by one truthful runtime
   probe per row; bounded representative packets cannot be relabelled as full
   per-probe coverage.

## Visually retained AS-IS findings

The ledger explicitly retains, without repair or normalization, the clipped
TimeNav popover capture boundary, mobile-menu bottom-nav disappearance after
close/Escape, the broken ListingEventCard image/alt overflow, loading/error
media surfaces that are visually indistinguishable, and brand/title overlap in
captured Search, Favorites and continuation states. It also confirms the two
separate desktop Event Detail compositions and CTA anatomies, the distinct
large-primary/small-preview media anatomy, and three separate unresolved
transport treatments.

The 25 conflict rows are fully enumerated as: 19 brand/title-overlap rows, two
loading/error-indistinguishable rows, two bottom-nav-not-restored rows, one
clipped TimeNav popover row and one broken-image/alt-overflow row. The auditor
spot-checked every conflict class against its original PNG. These observations
remain review-ledger facts rather than being hidden by the compact summary.
They are non-readiness findings: only the two explicit records in the preceding
section have `blocks_ready:true`.

One retained CTA unresolved row still says
`semantic-provenance-established-runtime-capture-pending` and requests
`exact-runtime` evidence. Controlled exact-source CTA packets are now captured;
the stale wording may refer to production runtime and must not be used to claim
that no CTA capture exists or to promote controlled evidence to production.

## Stop boundary

The only accepted import status for this audited result is
`EVIDENCE_COLLECTION_INCOMPLETE`. Importing the compact supplement and durable
evidence references does not authorize defragmentation, normalization, token
creation, Penpot mutation, component consolidation, production UI edits or an
experiment winner.
