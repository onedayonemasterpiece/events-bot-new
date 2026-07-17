# Lane L1-core Results

## Status
committed

## Requirement IDs
- R02
- R03
- R05

## Branch
`agent/interest-clubs/l1-core`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/interest-clubs-l1-core`

## Base SHA
`08f2deb881c42d9cb55d56900450685817c19450` (lane-map commit at worker start; merge-base with `origin/main` is `41442a64219d7a39615851c5bdb7f9b8b4340632`)

## Head SHA
`d2b6b6c20b9f4d1572b527acfd8b9678e78850a0` (implementation commit; this results file is committed immediately after it)

## Files changed
- `interest_clubs.py`
- `db.py`
- `models.py`
- `smart_event_update.py`
- `alembic/versions/20260717_interest_clubs.py`
- `tests/test_interest_clubs.py`
- `.codex/lanes/L1-core/RESULTS.md`

## Delivered contract
- Additive/versioned `interest_club`, `interest_club_event`, and compact `interest_club_evaluation` SQLite model, bootstrap DDL, and Alembic migration.
- Explicit idempotent fixture importer; default status is always `shadow`, rejected identities are skipped, and only explicit `--approve-confirmed` can approve confirmed rows. Probable rows remain shadow.
- Exact curated source/name retrieval only. No match deterministically fail-closes without a provider call.
- Separate source/name Gemma 4 31B policies, native `v/q` JSON schema, bounded URL-sanitized packet, 3–8-word exact event-evidence quote gate, no model/Lite fallback, no local/reserve bypass, and provider/no/unclear fail-closed states.
- Canonical eligibility excludes merged/noncanonical/silent/inactive/range rows and does not reinterpret `linked_event_ids` or festival identity.
- Disabled-by-default Smart Update hook launches evaluation after committed create/material merge without awaiting provider work. Static build is coalesced only when the active club-id projection changes.
- Active relation stale removal and input-hash idempotency, including repair when a cached accepted evaluation has no matching active relation.

## Opus consultation blocker evidence
- Worker attempt: `a-opus '<interest-club verifier prompt/schema audit>'` on 2026-07-17 exited `0` after the Antigravity wrapper returned with **empty stdout/stderr**; no usable consultant review was produced.
- Integrator independent attempt: `a-opus --print --print-timeout 10m ...` exited `0` after about one second and returned only `I'll start by locating and reading all the referenced files in parallel.`; no review followed.
- Per project policy, neither is represented as completed Opus review and no lower-class consultant was substituted.

## Required self-audit fixes
- **Source-lane default-yes safety:** retained the audited narrow source rule, but provider `no` remains authoritative fail-close (`review`, never active); added a source/crosspost regression test.
- **Quote validator:** validation uses a separate values-only `quote_corpus`, not labeled/full prompt text, so field labels cannot satisfy evidence; quote must be an exact consecutive 3–8-word substring.
- **Prompt injection boundary:** stable system/lane policy precedes an explicitly marked untrusted JSON payload; URLs are target-sanitized/hashed and input instructions are declared data only.
- **Cached consistency:** cached accepted verdict is reused only when a same-hash, same-policy active relation exists; otherwise the pair is reverified and repaired.

## Commands run
- `uv run --with-requirements requirements.txt python -m pytest tests/test_interest_clubs.py -q`
- `uv run --with-requirements requirements.txt python -m pytest tests/test_smart_update_provider_fallback_safety.py tests/test_google_ai_client.py -q`
- `uv run --with-requirements requirements.txt python -m py_compile interest_clubs.py models.py db.py smart_event_update.py alembic/versions/20260717_interest_clubs.py`
- `uv run --with-requirements requirements.txt python -m pytest tests/test_smart_event_update_duplicate_guards.py -q`
- `git diff --check`

## Tests / verification
- `tests/test_interest_clubs.py`: **10 passed**.
- GoogleAI/Smart Update fallback contracts: **32 passed**.
- Smart Update duplicate guards: **14 passed**.
- The committed 48-case manifest contract is checked for 24 positive / 24 hard-negative labels, 17 deterministic no-match cases, and fail-closed routing. No live paid/provider run was made.

## Risks
- Feature remains disabled unless `ENABLE_INTEREST_CLUB_PIPELINE=1`; no production DB, Smart Update runtime, or public projection was mutated in this lane.
- No live Gemma/provider canary was run. Production enablement still needs exact-key limiter and latency/failure evidence.
- The provider's hosted Gemma path rejects the unsupported thinking config used by some APIs; minimal reasoning is constrained in the prompt instead of sending an unproven `thinking_*` generation field.
- Provider failures persist as `deferred`; replay is currently an explicit evaluator/operator action rather than a new scheduler job. The best-effort immediate Smart Update handoff is process-local and should be covered by release replay/canary before enablement.
- Core does not implement the proposed automatic 90/365-day lifecycle transitions; that remains release/UI policy.
- Only reviewed identities present in the committed review fixture are importable by this CLI; this deliberately does not infer or auto-publish the full audited catalog.

## Merge notes
- Static exporter contract agreed with L2: `interest-clubs-static-v1`; publish only `interest_club.public_status='approved'` plus `interest_club_event.status='active'` joined to a public canonical active event.
- Migration down-revision is `20260715_social_metric_snapshots`; integrator must reconcile if another migration lands on the same base.
- Do not enable the feature flag or run `--approve-confirmed` as part of merge. Release/owner gates remain external.
- No push performed.
