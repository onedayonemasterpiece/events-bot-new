# INC-2026-08-01-unusual-feed-disabled-by-config Unusual has no current demonstrable build

Status: open
Severity: sev2
Service: KenigEvents static site — `/neobychnoe/` and shared static BGE handoff
Opened: 2026-08-01
Closed: —
Owners: static-site / discovery
Related incidents: `INC-2026-07-19-static-site-stale-builder-lease.md`, `INC-2026-07-18-static-snapshot-disk-pressure.md`
Related docs: `docs/features/unusual-events/README.md`, `docs/operations/kaggle-static-site-builder.md`, `docs/operations/release-governance.md`

## Summary

The owner reported that the previously demonstrable Unusual collection could no
longer be shown and correctly stated that they had never requested it to be
disabled. Investigation confirmed that no tracked change switched the feature
off: `STATIC_SITE_UNUSUAL_ENABLED` was introduced as `0` on 2026-07-27 and has
remained `0`. The pinned-BGE acceptance build was an immutable noindex review
candidate, not a public-root rollout. That URL still returns 200, but its event
set has aged out; `/neobychnoe/` on the public root returns 404.

This is therefore not an outage of a formerly live public route. It is a release
governance/availability defect: optional compute, publication state and route
availability were coupled to one silent opt-in, and no current accepted manifest
or stable demonstration URL was maintained.

## User / Business Impact

- The owner cannot demonstrate a current nonempty Unusual collection.
- Users cannot reach `/neobychnoe/` on the current public root.
- The shared static BGE path is dormant in production-candidate builds, so no
  current NPZ/receipt/cache/last-good exists.
- Source navigation was added before a durable public/data contract, allowing
  implementation state to look like an owner-requested product decision.

## Detection And Evidence

- Owner report on 2026-08-01.
- `https://kenigevents.ru/neobychnoe/` -> 404.
- Historical immutable review URL -> 200/noindex, but product Playwright fails
  `approved canary must contain unusual concepts` because no current items remain.
- Historical candidate identity and URL are recorded in
  `docs/operations/e2e-scenarios.md`; production probe JSON is in the ignored
  `artifacts/codex/podborki-data-audit-20260801/` investigation directory.
- Active Fly config: `STATIC_SITE_RELATED_MODE=pgvector`,
  `STATIC_SITE_UNUSUAL_ENABLED=0`, `STATIC_SITE_UNUSUAL_MIGRATION=1`.
- Configured BGE NPZ/receipt and unusual cache/last-good files are absent.
- Runtime file mirror was checked across active and rotated files by BGE/unusual
  keys, job/run IDs and time window; no production semantic execution exists.
- Production machine is healthy; this record does not attribute the issue to a
  Fly crash or a BGE model failure.

## Timeline

- 2026-07-27 14:44 UTC: `db526dbb` introduces shared BGE handoff and
  `STATIC_SITE_UNUSUAL_ENABLED="0"`; comments keep public cutover behind a real
  canary and owner gate.
- 2026-07-27: a pinned-BGE noindex review candidate passes semantic gates;
  production-root decision remains NO-GO.
- 2026-07-28: later ordinary-boundary regressions make a fresh canary necessary
  before any future promotion.
- 2026-08-01: owner reports loss of a usable demonstration surface; production,
  history, logs, caches and HTTP routes are audited; incident opened.

## Root Cause

The runner and exporter enter the shared semantic block only when related mode
is `bge` or `unusual_enabled=true`. Current production uses pgvector and the
flag was shipped as false, so every normal build skips BGE encoding, scoring,
receipts and cache persistence. Checked-in data remains an empty disabled
fallback and the validation path is skipped by the same condition.

The flag was never a clean publication control: choosing BGE related mode also
runs Unusual while the flag is false. Compute, quality evaluation and
publication were therefore coupled inconsistently. The expired review candidate
was the only demonstrable artifact, while public-root promotion was never made.

## Contributing Factors

- The same opt-in both suppresses computation and makes its validation optional.
- Build checks do not require current delivery status, item count and bound
  manifest hash for an accepted collection.
- A non-approved quality result empties items without first using the compatible
  last-good path; an empty revalidated fallback can still look successful.
- `STATIC_SITE_UNUSUAL_MIGRATION` defaults true and is unsuitable as a permanent
  always-running production state.
- The checked-in empty JSON lets an ordinary build succeed without current
  semantic evidence.

## Automation Contract

### Treat as regression guard when

- changing shared static BGE, `STATIC_SITE_UNUSUAL_*`, StaticSiteBuilder,
  collection manifests, navigation, route promotion or semantic cache retention;
- adding a control that can remove an accepted collection without an explicit
  blocked/incident state.

### Mandatory checks before closure or deploy

- production-candidate requires shared semantic compute even when the legacy
  enable env is absent or explicitly `0`;
- result receipt is bound to repo SHA, snapshot ID/hash, input fingerprint,
  pinned model/document/prototype/classifier/policy/corpus and manifest hashes;
- `provider_calls=0`, event population is complete and cache persistence occurs
  only after validated output;
- accepted delivery is current quality-approved (including explicitly proven
  `approved_empty`) or compatible/revalidated/nonempty last-good;
- disabled/missing status, non-approved empty output and empty fallback block
  promotion while the previously deployed site remains intact;
- normal post-baseline build has migration disabled and fallback never emits
  notification dots;
- focused Python semantic/golden/regression suites pass;
- `unusual-events-source-contract.test.mjs` is updated for the current
  `hasEvents || weekendAvailable` date-rail contract and passes;
- a real pinned-BGE current-catalog build and warm cache-reuse rerun pass;
- stable approved demonstration/public URL returns 200, contains current real
  cards, has the intended index policy and makes no page-view provider calls;
- ledger/logs show start, useful heartbeat and terminal report;
- deployed SHA is reachable from `origin/main`; `/healthz` remains ready.

## Immediate Mitigation

Do not blindly toggle the legacy env and do not promote the expired candidate.
The current review URL can be used only as historical UI evidence, not as a
current product demonstration. A fresh current-catalog canary is required.

## Corrective Actions

- [x] Prove flag/config and route/promotion history.
- [x] Distinguish expired review candidate from public-root rollout.
- [x] Separate mandatory semantic compute/quality receipt from per-label
  publication state in the data-prep candidate.
- [x] Keep non-approved/empty output fail-closed and distinguish it from a
  compatible accepted last-good; no empty result is promoted.
- [x] Repair the stale JS source-contract assertion.
- [x] Add bounded same-pipeline health, warm/cold request identity, durable
  resolver, daily monitor, persistent issue plan and exact two-viewport check.
- [ ] Run fresh production-snapshot BGE candidate and warm cache-reuse run.
- [ ] Run the operator cold/cacheless proof and the merged GitHub workflow.
- [ ] Restore a stable owner-approved demonstration URL.
- [ ] Obtain explicit owner acceptance before any public-root rollout.

## Release And Closure Evidence

### Data-prep candidate 2026-08-01

- branch: `integration/static-collections-data-prep-20260801`; not deployed and
  not yet reachable from `origin/main`;
- implementation: `production-candidate` requires collection compute regardless
  of the legacy Unusual flag/pgvector related mode, validates full catalog
  membership and the ID-only batch, uses evidence-only
  `collection_semantics_v1`, persists validated float32 cache/receipts, keeps
  semantic labels blocked without gold, and records `provider_calls=0`;
- focused regression: collection semantic/export/release suite `123 passed`;
  Kaggle status/handoff/unusual/outbox suite `116 passed`; py_compile and final
  integration checks are recorded in the branch integration report;
- deliberately pending: real current-catalog Kaggle cold/warm run, accepted
  unusual gold/recalibration, compatible nonempty last-good promotion, stale JS
  source-contract repair, stable route and owner approval. Therefore status
  remains `open` and local tests are not closure evidence.

- deployed SHA: pending
- deploy path: pending
- fresh BGE run/receipt: pending
- stable route browser check: pending

### Current-catalog recovery evidence 2026-08-09

- Read-only production SQLite snapshot SHA-256:
  `25e956b38a072c9320ea783540fed12ab57db91ea82ce6311b85af02fa6bfe24`.
- A real local pinned-BGE warm pass over the snapshot covered 401/401 events,
  encoded 0, reused 401, used 75 prototypes and made 0 provider calls.
- The restored explainable head found 249 candidates and a deterministic
  20-concept review shortlist. Publication remained empty (0; target 20,
  minimum 12) because the independent acceptance holdout is absent. Two
  incident hard negatives were excluded by document hash and two duplicate
  occurrences were detected.
- Engineering/editorial review found ordinary-looking boundary candidates,
  including ordinary concerts, a screening, an exhibition and a sports event.
  This evidence is therefore `INCIDENT/BLOCKED`, not an owner baseline or a
  release acceptance.
- Focused integration verification currently passes 145 Python tests and 15
  Node contract/component tests. A real merged Fly/Kaggle warm+cold ledger,
  immutable candidate screenshots, Actions artifacts and issue URL remain
  mandatory before this incident can close.

## Prevention

Every production-candidate build must emit explicit compute, quality and
publication state for each accepted semantic collection. Missing configuration
cannot masquerade as a valid empty feed. Once a route is publicly accepted, its
absence is a release failure: hold the previous valid artifact or fail promotion
and alert, rather than silently removing the page.
