# R02/R03/R05/R09 — static-site orchestration and immutable snapshot audit

Audit date: 2026-07-17 UTC  
Audit base: `origin/main@2822a91d6173883fca36ccf135802280ba4ab09d`  
Lane branch: `agent/static-site-production-pipeline-secret/orchestration-snapshot`  
Scope: read-only implementation audit and clean-port plan. No application code,
schema, configuration, documentation, or production state was changed.

## 1. Sources inspected

The audit used the requirements attached to the user turn and inspected these
current-main implementation surfaces:

- `smart_event_update.py`;
- `main.py`, including `enqueue_job`, `schedule_event_update_tasks`, outbox
  recovery/worker, runtime limits and `job_static_site_build_kaggle`;
- `models.py`, `db.py`;
- `event_vector_sync.py`, `linked_events.py`, `event_media.py`,
  `event_age_bge_service.py`;
- `scripts/run_static_site_builder_kaggle.py`;
- `kaggle/StaticSiteBuilder/static_site_builder.py`;
- `site/scripts/export-production-preview-data.py`;
- `tests/test_static_site_build_handoff.py`,
  `tests/test_static_site_public_gate.py` and relevant generic outbox tests;
- Kaggle status/lease framework and project skills
  `static-site-kaggle-builder`, `events-bot-kaggle-status`, and
  `events-bot-dual-db`.

Canonical requirements that are not yet in the audit base were read directly
from `docs/static-site-release-plan-20260717@8fecf7da`, especially:

- `docs/features/static-site-pages/release-plan.md`;
- `docs/features/static-site-pages/test-scenarios.md`;
- `docs/operations/kaggle-static-site-builder.md`;
- `docs/reports/static-site-release-context-recovery-2026-07-17.md`.

This distinction matters: on `origin/main@2822a91d`,
`docs/features/static-site-pages/release-plan.md` and
`docs/features/static-site-pages/test-scenarios.md` are absent. The named docs
branch is requirement evidence, not current release truth.

Side branches were inspected only as evidence:

- `origin/integration/static-site-production-release-20260715-v2`, including
  `62ba7110`;
- local `agent/static-site-production-publisher/R06`, including hardening commit
  `b0307e32` (a descendant of `62ba7110`).

They were not merged or modified.

## 2. Executive verdict

The current implementation is a useful **preview artifact handoff spike**, not
the required production/secret-release orchestration.

What already exists and should be retained:

- Smart Update create and materially changed merge paths schedule downstream
  work only after an effect; `skipped_nochange` does not enter that path;
- one ordinary pending `static_site_build:prod` row is postponed to 15 minutes
  after the latest scheduling call;
- `static_site_build` already has feature-specific outbox values
  `JOB_TTL=7200` and `JOB_MAX_RUNTIME=5400`, so it no longer inherits the old
  600-second stale check;
- the runner creates a private Kaggle input dataset and the kernel uses the
  status ledger, 60-second alive events, and lease `static_site:builder`;
- Node 22 installation, preview build/check, small output archive, and encrypted
  split secret datasets exist;
- the exporter fingerprints the whole current event set and rebuilds the full
  related graph on a cache miss, which is the right basis for reverse-affected
  anchors.

Release-blocking gaps:

1. `static_site_build` is deliberately missing from the running-owner deferred
   follow-up set. Update B can be merged into running build A and then lost.
2. Pending payload merge replaces the previous payload. It does not retain all
   reasons, event IDs, event revisions, or correlation evidence.
3. There is no durable public-event revision/change ledger. `Event` has
   `added_at` but no general `updated_at` or monotonic public revision.
4. The runner copies `/data/db.sqlite` with `shutil.copy2` while the live WAL
   database may be changing. It does not use SQLite online backup, run
   `quick_check`, hash the snapshot, or record a snapshot watermark.
5. Kaggle does not verify snapshot SHA-256/metadata before export.
6. IDs are not safely unique: the automatic build ID is minute-resolution and
   the input dataset slug is second-resolution. A configured
   `STATIC_SITE_BUILD_ID` can also be reused.
7. The kernel always runs `build:preview`/`check:preview`; its result contains
   only a few fields and the Fly handler does not parse or validate that result.
8. The vector lane and static build are independent coalesced jobs with no
   explicit revision/hash dependency. Priority is not a barrier.
9. There is a direct runner CLI, but no supported durable manual/on-demand
   build-request path through the same outbox/snapshot/retry/correlation state
   machine.
10. Static failures retry indefinitely through the generic backoff, while a
    stale running static job is parked for ten years because it is not in
    `EVENT_PIPELINE_INDEPENDENT_TASKS`. Pending static jobs can also expire
    without a feature-specific catch-up.
11. Restart changes every `running` outbox row to due `error` immediately. It
    does not adopt/check an already-running Kaggle run, so a restarted Fly
    process can attempt a duplicate external run.
12. There is no max-staleness detector, change-vs-release reconciliation, or
    bounded missed-build catch-up.

Therefore R02/R03/R05/R09 are **Partial / implementation required**. Enabling
`ENABLE_STATIC_SITE_KAGGLE_BUILDER=1` in production now would not satisfy the
attachment's Done condition.

## 3. Precise current-state audit

### 3.1 Effectful-only trigger

**Present, but only at selected producers.**

- New Smart Update event: after the event/posters/sources/holiday/topics/linked
  work is committed, `schedule_event_update_tasks` is called. A create is an
  effect, so this is correct.
- Existing event: scheduling is gated by
  `updated_fields || added_posters || (added_sources && !same_source) ||
  holiday_changed`. The returned no-change status is `skipped_nochange` and does
  not schedule a static build.
- Media pair review calls `schedule_event_update_tasks` only when the approved
  public media projection changed.
- The VK cancellation path changes `lifecycle_status` and schedules a rebuild.

This is not yet a system-wide public-fact contract:

- `schedule_event_update_tasks` itself has no `effect_id`, revision, changed
  fields, or idempotency key; any direct caller can create a build even if no
  public projection changed;
- manual `togglefree`, `togglesilent`, and some festival-binding mutations update
  public fields and rebuild old month/weekend pages directly, but do not reliably
  record/schedule the static-site change;
- accepted age-assessment report import commits changed age fields without
  scheduling a static rebuild;
- linked-occurrence recomputation persists backlinks, but standalone/recovery
  callers do not have a central static change hook;
- repair/backfill scripts use ad-hoc direct outbox insertion, so correlation and
  revision coverage varies.

Required correction: every producer of a public projection change must call one
central `record_static_site_change(...)` **inside or immediately after the same
successful mutation boundary**, and no producer should directly construct a
`static_site_build` row. Smart Update remains the semantic owner; this function
only records an already-decided effect.

### 3.2 Fifteen-minute debounce

**Partial.** `schedule_event_update_tasks` computes a common
`now + timedelta(minutes=15)` due time. For an existing pending coalesced job,
`enqueue_job` moves `next_run_at` later when the latest requested time is later.
Thus ordinary sequential updates behave as “last scheduling call + 15 minutes”.

Missing guarantees:

- no stable `ADD-BUILD-01` test name/metadata proves no-op, repeated debounce,
  and exact last-effect semantics;
- payload is replaced rather than unioned;
- the select/update is not protected by a database uniqueness invariant for one
  pending static request, so concurrent producers rely on SQLite timing rather
  than an explicit partial unique index/upsert;
- due time is based on when the scheduler helper runs, not a persisted effect
  timestamp/change sequence.

### 3.3 Running-build follow-up race

**Missing; known data-loss race confirmed.**

`enqueue_job` has a special branch that creates one future row behind a running
coalesced owner, but the allowed task set is only:

- `month_pages`;
- `weekend_pages`;
- `event_vector_sync`;
- `event_age_bge_assessment`.

`static_site_build` is absent. During build A, scheduling update B finds the
latest running `static_site_build:prod`, may overwrite its payload, returns
`"merged"`, and creates no pending B. A completes and is marked done; completion
does not compare its snapshot watermark to later changes. This exactly matches
the race described in the readiness audit.

Adding `static_site_build` to the generic set is necessary but not sufficient:
the generic path still lacks immutable target watermarks, payload union,
transactional uniqueness, and a completion-time repair check.

### 3.4 Persisted reasons, event revisions, and correlation

**Missing.** Current static payload is only:

```json
{"reason":"smart_update","event_id":123}
```

Pending merge replaces it; running merge can mutate the running row's payload.
The outbox-generated `run_id = uuid.uuid4().hex` is used only for logging and is
not stored on the job. Kaggle independently derives
`run_id=static-site-builder:<build_id>`. `last_result` becomes merely `ok`.
There is no durable join from Smart Update effect to outbox row, snapshot,
Kaggle ledger, result, artifact, or release URL.

### 3.5 Immutable SQLite snapshot

**Missing.** With `--export-in-kaggle`, the runner executes:

```python
shutil.copy2(Path(args.db).resolve(), dataset_dir / "events.sqlite")
```

For Fly's WAL-mode live database this is not a consistency mechanism. The copy
can omit committed WAL pages or represent a changing file. Neither runner nor
kernel performs `PRAGMA quick_check`; neither records or verifies snapshot
SHA-256/size/time/change watermark. The runner calls its `--db` parameter a
snapshot, but the automatic handler passes the mutable `db.path` directly.

The online snapshot must include the entire core SQLite database, not a hand
selected event table export, because the exporter consumes event rows plus
sources, approved media, medallion/venue/festival data, age fields, linked
occurrences and other projection inputs.

### 3.6 Kaggle input, status, lease, and result

**Useful basis, incomplete contract.**

Present:

- unique-looking private input dataset per invocation;
- dataset readiness and bound-source checks;
- status dataset and run ledger when status DB/callback are supplied;
- kernel-side `static_site:builder` lease and alive heartbeat;
- Node 22.12, preview export/build/check, tarball, bounded output file set.

Gaps:

- dataset name uses only wall-clock seconds; build ID uses a minute, or an
  operator-provided reusable constant;
- fixed kernel ref `kenigevents-static-site-builder` can be overwritten by a
  second launcher before the kernel-side lease is acquired;
- status resource TTL is 3 hours, but launch/adoption is not tied to the outbox
  run record;
- runner success means Kaggle status `COMPLETE`; it does not require exactly one
  valid result JSON and artifact, verify their hashes/IDs, or classify failure;
- result omits repo SHA, snapshot/run IDs/hash, page/file counts, max revision,
  output hash/size, checks, related/vector revisions, failure class and
  freshness;
- build is preview-only.

### 3.7 Related/vector barrier

**Partial building blocks; no barrier.**

- Smart Update currently schedules `event_vector_sync:prod` after 90 seconds and
  `static_site_build:prod` after 15 minutes when their flags are enabled.
- Static has no `depends_on` relation to vector sync, and the two jobs may have
  different owner event IDs. Sorting vector before static among already-due jobs
  does not prevent static from running while vector sync is still running or
  failed.
- The vector sync writes a useful `ops_run`, and the sync report has completeness
  and counts, but there is no authoritative catalog projection revision that a
  static snapshot can require.
- Current pgvector sync upserts documents/embeddings in chunks. There is no final
  atomic `search_v3`/`related_v1` catalog-revision receipt.
- Exporter fingerprints every event and rebuilds all chains when the event set or
  fingerprint set changes. That correctly recomputes reverse-affected anchors,
  but it does not prove the pgvector rows used for retrieval match the snapshot.
- A provider/vector failure currently fails the whole build when pgvector sync or
  strict verification is enabled. There is no explicit “last-good verified
  projection or omit optional related block” branch, so optional related can
  block base pages.

### 3.8 Manual/on-demand path

**Direct development CLI exists; supported request path does not.**

`scripts/run_static_site_builder_kaggle.py --db ...` can manually launch a run,
but it bypasses effect/change ledger, debounce/follow-up, snapshot creation,
bounded retry, catch-up, and release-channel controls. A few maintenance scripts
write static outbox rows ad hoc. `run_event_update_jobs` can drain a row that
already exists; it does not create a correlated manual request.

Required operator path should be a CLI over the same durable API, for example:

```text
python scripts/request_static_site_build.py \
  --reason operator_request \
  --release-channel secret_preview \
  --not-before now
```

No public HTTP endpoint is needed for this stage. If a Telegram admin command is
later desired, it should call the same function with allowlist, confirmation and
audit evidence; it must not launch Kaggle directly.

### 3.9 Runtime, stale, retry, restart, and catch-up

**Feature-specific runtime is partially correct; recovery is not.**

- `JOB_MAX_RUNTIME[static_site_build] = 5400` is used by both stale inspection and
  `asyncio.wait_for`, so the old 600-second threshold is not applied there.
- `enqueue_job` also consults `JOB_MAX_RUNTIME` when deciding whether a running
  owner is stale.
- However the automatic runner command defaults to
  `STATIC_SITE_KAGGLE_TIMEOUT_MINUTES=60`, so Kaggle is stopped at 3600 seconds,
  not the required allowed 5400 seconds. CLI default is 45 minutes.
- Snapshot creation, dataset readiness and artifact download are included inside
  the 5400-second outbox handler timeout, reducing the actual Kaggle allowance.
- Generic failures use `[30,120,600,3600]` forever; there is no permanent vs
  retryable classification and no max attempts.
- A stale running static job is not in `EVENT_PIPELINE_INDEPENDENT_TASKS`, so it
  is moved ten years into the future rather than retried/caught up.
- A pending static job older than its TTL is expired for ten years; only selected
  per-event publication tasks get stale-pending catch-up.
- startup `reconcile_job_outbox` blindly re-arms every running row, without
  checking whether the associated Kaggle ledger/kernel is alive or has a valid
  terminal result.
- no process periodically compares latest effect watermark, latest valid
  snapshot/artifact/release watermark, pending/running state and freshness SLO.

## 4. Proposed durable state machine

The orchestration should be implemented in one dedicated module rather than
adding more static-specific conditionals to generic outbox code. The existing
outbox can remain the worker wake-up mechanism, but the authoritative state must
be durable, revision-bound records.

### 4.1 Identifiers and watermarks

Generate independently and never reuse:

- `correlation_id`: one effect/manual request; accept an upstream Smart Update or
  repair run ID when available, otherwise UUIDv7;
- `change_seq`: monotonic SQLite AUTOINCREMENT public-change watermark;
- `event_revision`: monotonic per-event public revision;
- `request_id`: coalesced build request identity;
- `run_id`: one execution attempt;
- `snapshot_id`: one immutable SQLite backup;
- `build_id`: one checked site output;
- `release_id`: artifact/publisher identity.

Automatic IDs must not be overridable by a fixed production environment value.
An operator label may be stored separately.

### 4.2 States

```text
NO_EFFECT
  └─ no row, no build

EFFECT_COMMITTED
  └─ append static_site_change(change_seq, event_id, event_revision,
                               reason, correlation_id, changed_fields_hash)

DEBOUNCING
  ├─ one pending request, target_change_seq = max seen
  ├─ due_at = latest effect_at + 15 minutes
  └─ each new effect atomically moves due_at and expands the ledger range

CLAIMED
  ├─ freeze request target_change_seq and all reason/event revision evidence
  ├─ create run_id/snapshot_id/build_id
  └─ mark the execution owner running

SNAPSHOT_PREPARING
  ├─ SQLite online backup into a new temporary path
  ├─ quick_check == ok
  ├─ verify snapshot change_seq >= frozen target_change_seq
  ├─ calculate SHA-256, size, created_at, schema/app version,
  │  max event public revision/update and eligible-catalog summary
  └─ fsync + atomic rename to immutable snapshot path

OPTIONAL_VECTOR_BARRIER
  ├─ if related/vector disabled: record `disabled`, continue base pages
  ├─ derive expected search_v3/related_v1 hashes from this snapshot
  ├─ wait/reconcile until one complete projection receipt matches
  └─ provider failure: select compatible verified last-good or explicitly
     suppress optional related; never label raw/stale candidates verified

KAGGLE_STAGING
  ├─ capacity preflight
  ├─ unique private dataset containing snapshot + metadata + site source
  ├─ unique run-aware kernel/status dataset
  └─ verify mounted snapshot hash and quick_check before export

KAGGLE_RUNNING
  ├─ status ledger + heartbeat + `static_site:builder` lease
  ├─ build/check selected release channel (`secret_preview` now)
  └─ produce bounded signed/hash-bound result and artifact

ARTIFACT_CHECKED
  ├─ Fly downloads and parses exactly one result
  ├─ IDs, repo SHA, snapshot hash, output hash/size, counts and checks match
  └─ hand off only the checked immutable artifact

SECRET_RELEASE_READY
  ├─ upload only to an immutable unguessable noindex prefix
  ├─ no production-root mutation/current activation
  └─ persist hashed secret-link token/evidence, never raw token in logs

SUCCEEDED
  └─ record covered_change_seq = frozen target; retain artifact and evidence
```

### 4.3 Exact running-update rule

Update B during build A must execute this transaction:

1. append B to `static_site_change` and bump B's `event_revision`;
2. observe a running request whose frozen target is older;
3. insert-or-update exactly one pending follow-up request with
   `target_change_seq >= B.change_seq` and `due_at = B.effect_at + 15m`;
4. commit; only after this commit may the producer report scheduling success.

Required database invariants:

- at most one active running execution for the release channel;
- at most one pending coalesced follow-up per release channel;
- one `correlation_id` cannot append the same effect twice;
- request target watermark can only increase.

Build A completion must, in the same terminal transaction:

1. persist A's terminal result and covered snapshot watermark;
2. read latest `static_site_change.change_seq`;
3. if it is newer than A's frozen target, upsert the same one pending follow-up;
4. only then mark the deferred condition reconciled.

The change ledger is the durable deferred marker. It is never cleared before a
follow-up scheduling commit. A periodic reconciler repeats this comparison, so a
crash at either side of the transaction cannot lose B. The pending partial unique
index/upsert makes producer/completion/reconciler races converge to one B.

### 4.4 Failures and adoption

- `retryable`: Kaggle API propagation, transient 429/5xx/network, heartbeat loss
  with no terminal evidence, temporary provider failure when policy requires the
  optional projection. Use bounded exponential backoff + jitter and a maximum
  attempts/time horizon.
- `permanent`: invalid snapshot/hash, failed quick_check, invalid result schema,
  mismatched IDs/SHA, artifact/check/catalog parity failure, unsupported config.
  Quarantine the candidate; do not retry unchanged input.
- `uncertain_external`: Fly restarts while Kaggle may still run. First inspect
  the persisted ledger/kernel/run ID. Adopt/wait for the same run when alive;
  import its terminal result when complete; relaunch only after lease expiry and
  explicit reconciliation proves no valid result exists.
- A failed A does not consume changes. A pending B may widen to cover A+B, but
  there remains one newest-snapshot catch-up, not multiple duplicate jobs.
- Reconciler periodically compares `latest_change_seq` with latest checked/secret
  release coverage and active requests. If uncovered beyond the accepted window,
  emit max-staleness evidence and schedule one catch-up for the newest watermark.

## 5. Exact implementation/edit scope

### 5.1 Main-based orchestration work

Recommended files and ownership:

1. **New `static_site_orchestration.py`**
   - change/revision recording;
   - coalesced pending/running/follow-up transactions;
   - snapshot online-backup creation and validation;
   - run state, failure classification, adoption and catch-up reconciliation;
   - PII-free structured evidence.
2. **`models.py`, `db.py`**
   - durable tables/indexes in section 7;
   - no changes to user/personalization ownership.
3. **`smart_event_update.py`**
   - call the central recorder only for its existing effectful create/merge
     decision; pass changed reason/fields and upstream correlation evidence;
   - no semantic regex/exporter logic.
4. **Other public mutation producers**
   - `event_media.py`, lifecycle/manual/admin mutation handlers,
     `event_age_bge_service.py`, linked-event repair and approved maintenance
     scripts call the same effect recorder when their public projection changes.
5. **`main.py`**
   - replace the static-specific raw `enqueue_job` call with the orchestrator;
   - handler claims a durable run, snapshots first, invokes the runner with
     immutable IDs/metadata, validates result, and terminally reconciles follow-up;
   - retain generic 5400-second awareness but do not rely on generic stale logic;
   - startup and periodic reconciliation/adoption.
6. **New `scripts/request_static_site_build.py`**
   - manual/on-demand request through the same API;
   - default `release_channel=secret_preview`;
   - explicit operator reason/correlation, redacted output;
   - no direct Kaggle launch and no root activation.
7. **`scripts/run_static_site_builder_kaggle.py`**
   - accept `run_id/snapshot_id/build_id`, snapshot metadata and release channel;
   - reject mutable/live DB mode for production/secret pipeline;
   - cryptographically verify snapshot before dataset creation;
   - random/run-bound input and kernel slugs;
   - parse/validate downloaded result and artifact; return a small structured
     receipt to the caller;
   - production timeout defaults must permit the full accepted 5400-second
     kernel window plus bounded staging/download overhead at the outer layer.
8. **`kaggle/StaticSiteBuilder/static_site_builder.py`**
   - re-verify SHA/size/quick_check after mount;
   - select `secret_preview`/production build profile explicitly;
   - emit the complete bounded result contract and failure class;
   - preserve status/heartbeat/lease/Node 22 behavior.
9. **`event_vector_sync.py` and vector sync report**
   - emit a complete hash-bound projection receipt;
   - static orchestration verifies it against snapshot expected hashes;
   - keep base page build available when optional related is disabled/degraded.
10. **Tests**
    - new `tests/test_static_site_orchestration.py` and
      `tests/test_static_site_snapshot.py`;
    - extend `tests/test_static_site_build_handoff.py`;
    - add explicit acceptance ID metadata/names listed below.
11. **Configuration/docs**
    - `.env.example`, controlled Fly config only after tests;
    - canonical README/release-plan/operations/test-scenarios/E2E routes and
      `CHANGELOG.md` in the integration lane;
    - activation must remain secret-only; production-root promotion flag off.

### 5.2 Side-branch clean-port decision

Do **not** cherry-pick `62ba7110` or `b0307e32` wholesale.

- Neither commit implements effect/change ledger, running follow-up, immutable
  SQLite snapshot, vector barrier, manual request, or catch-up state machine.
- Their `build-production.mjs`, `check-production.mjs`, manifest validators and
  behavior-test helpers are useful selective sources for the artifact/profile
  lane after reconciling them with current `origin/main` UI/data contracts.
- `deploy-production-yc.mjs` must not be wired to this task's automatic handler.
  Even after `b0307e32`, it copies files into the live root and only then changes
  a CAS pointer. Readers of ordinary root paths do not resolve through that
  pointer, so they can observe a mixed tree during copying. A metadata pointer is
  not reader-atomic when the delivery layer ignores it.
- The latest user instruction is stricter: publish only an immutable secret URL,
  not root. Therefore root-copy/promotion/rollback code is out of scope for
  activation and remains a redesign/blocker for future canonical root release.

Clean-port only pure, current-main-compatible validators/hash-manifest utilities
and tests that do not mutate root. The artifact/publisher lane should own those
files; orchestration should consume its checked-artifact interface.

## 6. Test plan and stable acceptance IDs

Use both a readable test suffix and explicit marker/metadata, e.g.
`@pytest.mark.acceptance_id("ADD-BUILD-01")`; Python function names use
underscores while reports emit the canonical hyphenated ID.

### ADD-BUILD-01 — effect/debounce/follow-up

- `test_ADD_BUILD_01_noop_smart_update_records_zero_changes_and_zero_builds`
- `test_ADD_BUILD_01_effects_coalesce_to_one_request_due_15m_after_latest`
- `test_ADD_BUILD_01_repeated_correlation_is_idempotent`
- `test_ADD_BUILD_01_update_B_during_build_A_creates_exactly_one_followup`
- `test_ADD_BUILD_01_followup_snapshot_watermark_is_at_least_update_B`
- `test_ADD_BUILD_01_completion_scheduler_race_still_has_one_followup`
- `test_ADD_BUILD_01_deferred_marker_survives_crash_before_followup_commit`

### ADD-BUILD-07 — profile/release-channel isolation

- automatic and manual requests default to `secret_preview`;
- no request can select root promotion without a separate disabled-by-default GO;
- preview remains noindex; production artifact checks remain independent.

### ADD-BUILD-08 — immutable Kaggle handoff

- SQLite online backup captures committed WAL state and all exporter tables;
- `quick_check != ok` blocks Kaggle dataset creation;
- snapshot hash/size/IDs are stable and re-verified in kernel;
- two builds always use different snapshot/build/run/input dataset IDs;
- mounted snapshot/result mismatch fails permanently;
- status ledger and `static_site:builder` lease use the same run ID.

### ADD-BUILD-09 — manifest/tree/catalog correlation

- result/manifest contains repo SHA, run/build/snapshot IDs and hash, target/actual
  change watermark, event/page/file counts, max event revision/update, output
  hash/size, checks, related/vector revisions and freshness;
- runner refuses missing/ambiguous result or artifact;
- orchestration persists the validated receipt, not plain `ok`.

### ADD-BUILD-10 — failed candidate isolation

- Kaggle, build/check, result validation, or secret upload failure never mutates a
  current/root pointer;
- for this stage, success creates only the immutable secret release;
- no root-copy command is invoked by unit/integration tests.

### ADD-BUILD-11 — secret/CDN artifact delivery

- secret prefix serves required MIME/assets/ICS while remaining noindex and
  unlinked;
- token is high entropy, hash-only at rest, absent from referer-producing outbound
  URLs/logs; this is primarily artifact/publisher-lane ownership.

### ADD-BUILD-12 — capacity preflight

- insufficient `/data` space for live DB + temp snapshot + retained snapshot
  blocks before backup/Kaggle;
- dataset/output/object-storage limits fail closed with a classified result;
- cleanup never removes a running snapshot or last-good checked artifact.

### ADD-BUILD-13 — retry/restart/staleness/catch-up

- retryable failures use bounded attempts; permanent failures do not loop;
- restart adopts a still-running Kaggle run rather than pushing a duplicate;
- expired/failed request with uncovered changes produces one newest catch-up;
- stale/missed build emits an alert/evidence row and catch-up clears it only after
  a newer checked secret release;
- retry plus new update widens one follow-up, not duplicate runs.

### ADD-RELATED-01..04

- `ADD-RELATED-01`: Smart Update change → vector receipt → related build → checked
  secret release share one correlation/watermark chain.
- `ADD-RELATED-02`: snapshot expected `search_v3`/`related_v1` hashes must match
  the complete vector receipt; changing event X rebuilds reverse-affected anchors.
- `ADD-RELATED-03`: provider timeout/invalid/partial projection never publishes raw
  candidates as verified; compatible last-good or omitted optional block is used,
  while base pages still build.
- `ADD-RELATED-04`: reconciliation detects missed vector/static changes and
  lifecycle/time expiry without another manual Smart Update.

### ADD-OBS-01 — PII-free end-to-end evidence

Assert one query/report can join:

```text
correlation_id -> change_seq/event_revision -> request_id/outbox row
-> snapshot_id/hash -> run_id/Kaggle ledger -> build_id/result/output hash
-> secret release manifest/url hash
```

Errors must expose phase/failure class/IDs without source text, secrets, raw
personal tokens, callback tokens or credentials.

### Required command-level acceptance after implementation

```text
pytest -q tests/test_static_site_build_handoff.py
pytest -q tests/test_static_site_public_gate.py
pytest -q tests/test_static_site_orchestration.py tests/test_static_site_snapshot.py
npm --prefix site run build:preview
npm --prefix site run check:preview
npm --prefix site run build:production
npm --prefix site run check:production
git diff --check
```

Then one controlled real Kaggle CPU run from an immutable production snapshot,
with status/lease/result evidence and secret-prefix publication only. Production
root activation remains forbidden without a separate GO and reader-atomic
delivery redesign.

Audit note: focused pytest execution was attempted, but this isolated worktree
has neither a `pytest` executable nor an installed `python3 -m pytest`; no green
test claim is made by this read-only lane.

## 7. Production DB and migration impacts

### 7.1 Core Fly SQLite (`/data/db.sqlite`) — required

Per the project's dual-DB ownership contract, event lifecycle, build scheduling,
snapshots and static release correlation belong in core SQLite.

Recommended durable tables (names may be adjusted, semantics should not):

1. `static_site_event_revision`
   - `event_id` primary key;
   - monotonic `public_revision`;
   - `last_change_seq`, `public_projection_hash`, `changed_at`.
2. `static_site_change`
   - AUTOINCREMENT `change_seq` primary key;
   - unique `correlation_id`/idempotency key;
   - `event_id`, `event_revision`, `reason`, `changed_fields_json` or hash,
     `effect_at`, optional upstream run ID;
   - no raw source text/PII.
3. `static_site_build_request`
   - `request_id`, release channel, state, target change range, due time, trigger,
     owner event, attempts/failure class and timestamps;
   - partial unique indexes for one pending and one running request per channel.
4. `static_site_build_run`
   - `run_id`, request/attempt, snapshot/build IDs, frozen target watermark,
     Kaggle run/dataset/kernel references, state/heartbeat/failure/result receipt.
5. `static_site_snapshot`
   - metadata only: snapshot ID/path or object key, SHA-256, size, quick-check,
     created time, change watermark, max event revision/update, schema/catalog
     summary and retention state. Snapshot bytes stay on volume/object storage,
     not in SQLite.

If artifact/release tables are introduced by the publisher lane, reference them
instead of duplicating release manifest/token state here.

Migration requirements:

- additive startup migration in `db.py`, matching existing project practice;
- unique/partial indexes must be created explicitly and tested against an
  upgraded production-like snapshot;
- backfill existing events with revision `1` and a baseline projection hash, or
  create one synthetic baseline change watermark before enabling automatic
  builds;
- no rewrite of canonical event content and no migration of event data out of
  Fly SQLite;
- deploy code/migration with builder disabled, verify DB quick check/indexes,
  then enable secret-only scheduling in a separate controlled config step.

Enhancing only `JobOutbox.payload` is not sufficient: history, watermarks,
completion reconciliation and restart adoption require dedicated durable rows.
`JobOutbox` may continue to carry `request_id` as a lightweight wake-up pointer.

### 7.2 Personalization Supabase/Postgres — conditional for pgvector barrier

Baseline secret static pages with optional related disabled/sparse do not require
a Supabase migration.

For production pgvector/strict-related mode, current chunked document/embedding
upserts lack one authoritative completed catalog revision. Add a backend-only
projection receipt, e.g. `event_vector_projection_revision`, containing:

- projection/run ID and completion state;
- catalog/event-set hash;
- aggregate `search_v3` and `related_v1` hashes;
- model/dimension/policy versions and row counts;
- created/completed timestamps.

The final receipt is written only after all rows are verified. It must be
service-role only: RLS enabled, no `anon`/`authenticated` grants, no browser key
or secret exposure. The static run stores/verifies only the receipt IDs/hashes.
This is personalization-sidecar state, not canonical event ownership.

### 7.3 YDB and object storage

- no YDB schema change is needed for this orchestration;
- SQLite snapshot files and static artifacts belong on volume/object storage with
  bounded retention;
- secret release tokens are high-entropy and hash-only in durable metadata;
- root/current production promotion remains disabled in this stage.

## 8. Integration ordering

1. Port the canonical release/test docs from `8fecf7da` into the main-based
   integration branch without treating them as implementation evidence.
2. Land core SQLite models/migrations and orchestration unit tests with all
   runtime flags off.
3. Centralize effect recording and close producer gaps; prove no-op/debounce and
   the A/B follow-up race.
4. Land immutable snapshot creation/verification and run/result correlation.
5. Add optional vector receipt/barrier and degraded-base-page behavior.
6. Integrate current-main-compatible build/check/artifact utilities from the
   artifact lane; do not port root mutation.
7. Add manual request CLI and periodic reconciliation/catch-up.
8. Run local acceptance, then one real status-aware Kaggle CPU build.
9. Enable only `secret_preview` automatic/manual publication. Keep production
   root activation off pending separate reader-atomic architecture and GO.

## 9. Requirement closure status for this lane

| Requirement | Status | Evidence/conclusion |
|---|---|---|
| R02 effectful trigger + 15m debounce + running follow-up | **Partial** | effect gate/debounce exist for main Smart Update path; static running follow-up and durable evidence are missing |
| R03 immutable SQLite snapshot and Kaggle handoff metadata | **Missing/Partial basis** | private dataset/status exist; live `copy2`, no quick-check/hash/watermark/verification |
| R05 optional related/vector revision barrier | **Partial basis** | full-fingerprint related rebuild and vector ops run exist; no matching revision receipt/barrier/degraded optional path |
| R09 retry, stale, restart, catch-up, manual request and observability | **Partial/Missing** | 5400 outbox max exists; runner timeout, retry classification, adoption, catch-up, request API and end-to-end correlation are missing |

No requirement is marked Done by this audit.
