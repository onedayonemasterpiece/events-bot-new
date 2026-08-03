# Lane L3 Results

## Status
committed

## Requirement IDs
- R01 — import normative PR #270 package selectively
- R02 — quarantine legacy profile/scorer and characterize parity
- R03 — pure typed target skeleton, non-applying flags, fail-closed registry and sanitized test API
- R04 — generated route inventory checker
- R05 — source/research quarantine guards
- R06 — tests/build/evidence
- R07 — integration mount handoff

## Branch
`agent/static-unified/l3-p13n00`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/static-site-unified-20260803/l3-p13n`

## Base SHA
`0bc8482dcf9f74b835743f72d57aa5dcce755c28`

## Implementation Head SHA
`976f34e69fe1b52d748926f2d71f115e018a0aff`

Normative PR #270 was brought in as four selective documentation commits before implementation:

- `b3b678c0b` (upstream `726cb139b`)
- `c649e6e9d` (upstream `e1c0c2997`)
- `5f125d542` (upstream `9c33107c7`)
- `3824f3658` (upstream `0f27fb34a`)

## Files changed

- `docs/features/static-site-pages/personalizaion/**` — normative PR package, corrected precedence, research delta and pending-integration status evidence.
- `site/src/lib/personalization/**` — contracts, runtime modes, policy registry, presenter plan, sanitized test API, legacy quarantine and tests.
- `site/src/components/personalization/PersonalizationRuntime.astro` — inert common runtime component.
- `site/scripts/check-personalization-route-inventory.mjs` — real-dist inventory generator/hard gate.
- `site/scripts/check-personalization-source-guard.mjs` — giant-inline/legacy leakage guard.
- `site/scripts/personalization-*.test.mjs` and `tests/test_personalization_implementation_contract.py` — contract/research/component/inventory checks.

Forbidden files were not changed: `site/src/layouts/EventLayout.astro`, `CHANGELOG.md`, `docs/routes.yml`.

## Commands run

1. `git cherry-pick 726cb139b e1c0c2997 9c33107c7 0f27fb34a`
2. `node --experimental-strip-types --test ...personalization...`
3. `node site/scripts/check-personalization-source-guard.mjs`
4. Astro compiler component test through `@astrojs/compiler`
5. `npm run build` using the existing node_modules symlink; no install was run.
6. `node site/scripts/check-personalization-route-inventory.mjs --dist site/dist --out artifacts/personalization-route-inventory.json`
7. Existing personal-feed/search/event-detail regression tests.
8. Contract Python functions invoked directly with `python3` because this image has no `pytest` package.
9. `git diff --check` and explicit forbidden-file diff.

## Tests / verification

### Pass

- New Node personalization suite: **27/27 PASS**.
- Source/quarantine guard: **PASS**.
- Normative Python contract functions: **7/7 PASS** (direct invocation; `python3 -m pytest` unavailable).
- Astro build completed and produced **466 HTML files**.
- Runtime component parses/compiles with `@astrojs/compiler` and contains no fetch/storage/legacy scorer import.
- `EventLayout.astro` is byte-identical to base: **302,718 → 302,718 bytes**, no diff.
- P13N component source: **2,671 bytes**; target modules are isolated and unused until mount.
- Storage fixture states `wave0_new_storage_keys=[]` and `wave0_storage_writes=0`; source guard freezes the current inline key/write/RPC baseline.

### Expected integration gate failure

The real-dist route checker correctly fails before the forbidden common-layout mount:

```text
public_html_total=390
excluded_test_html=76
public_html_missing_runtime=390
public_html_duplicate_runtime=0
public_html_unclassified=390
collections_unknown_surface=9
calendar_primary_non_identity_policy=0
legacy_policy_promoted_to_target=0
```

Artifact: ignored/untracked `artifacts/personalization-route-inventory.json` (about 324 KiB, per-route records required by the contract).

### Existing unrelated baseline failures

Existing targeted regression run: **44/46 PASS**, two failures unchanged by this lane:

1. `event-detail-runtime-regressions`: event 7018 fixture expects a Ruin Keepers organizer medallion absent from the current `origin/main` built row.
2. `personal-feed-surface`: source assertion expects the removed string `current_event_id: personalFeedCurrentEventId(section)`.

This lane did not modify the layout, medallion data, personal-feed runtime or those tests.

## Risks

- P13N-00 is not `done` until the integrator mounts the component once in the common layout and runs production-like browser characterization.
- No browser before/after request/storage/handler evidence is claimed before that mount.
- `implementation-status.yml` intentionally remains `implementation_ready_pending_common_layout_mount_and_browser_acceptance`.
- Legacy parity is explicitly not target quality; target `scorer.ts` and `model.ts` do not exist.
- No DB, transport operation, storage mutation, Supabase/YDB access or remote write was added.

## Merge notes

Integrator-owned minimal mount in `EventLayout.astro`:

```astro
---
import PersonalizationRuntime from '../components/personalization/PersonalizationRuntime.astro';
---
<body ...>
  <PersonalizationRuntime pathname={Astro.url.pathname} />
  ...
</body>
```

Then:

1. build the actual integrated tree;
2. rerun route inventory and require every non-excluded hard count to be zero;
3. run browser characterization for unchanged DOM order, requests, localStorage keys/bytes, like/hide/share/reset and handler count;
4. update `implementation-status.yml` to `done` only with evidence;
5. add the required `[Unreleased]` CHANGELOG entry in the integrator-owned `CHANGELOG.md`.

Do not import `legacy/scorer-v1.ts` into the layout or target paths. Do not turn the old public profile RPC into target transport.
