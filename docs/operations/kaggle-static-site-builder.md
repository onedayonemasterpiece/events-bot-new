# Kaggle static-site builder

Status: immutable secret-candidate pipeline implemented. A reader-atomic
two-root-bucket/Yandex-ALB publisher is implemented default-off; its live
inventory, protected ALB and DNS cutover are not provisioned or approved.
Canonical setup/rollback: [static-site atomic root](static-site-atomic-root.md).

Current event-page release sequencing, top-five platform backlog and the planned
10-day Telegraph coexistence/cutover are canonical in
[`docs/features/static-site-pages/release-plan.md`](../features/static-site-pages/release-plan.md).
The mode names described there are a required implementation contract, not
already-existing production env flags.

## Position

Kaggle is an accepted **batch executor** in this project because the repo already uses Kaggle for monitored parser/video/social jobs. For static pages it may build Astro HTML, related/discovery manifests, golden-facet manifests, share-card artifacts and offline evaluation reports.

R15 unusual events reuse this executor, lease and immutable input dataset. The
builder may produce one hash-bound shared BGE vector artifact, the unusual
manifest/cache/last-good receipt and the daily service-share image in the
coalesced run; it must not create a parallel Kaggle job or move embedding work
to Fly/page views. Exact semantic and rollback rules live in
[`docs/features/unusual-events/README.md`](../features/unusual-events/README.md).
The real Kaggle CPU canary is still pending and must not be inferred from local
fixture tests.

### Static collection data-prep contract (candidate 2026-08-01)

`production-candidate` now requires `collection_semantic_compute=true` even when
legacy Unusual publication is disabled and related results remain pgvector. The
existing kernel passes the same immutable compact SQLite projection to the exporter, which
materializes exact collections/venue/club projections, encodes changed
`collection_semantics_v1` rows and the namespaced prototype union once, and
writes `collection-batch-v1.json`. Astro consumes files only after the kernel
validates exact catalog coverage and hashes. There is no second notebook,
snapshot upload, Supabase event read or page-view provider call.

The Fly runner persists BGE cache/receipt and optional compatible last-good only
after complete exact-run validation. A new/changed semantic label without
approved gold remains `blocked`; missing/partial/mismatched output fails the
production candidate rather than publishing an empty success. Cold and warm
real Kaggle CPU evidence is still mandatory before deploy. Exact implementation
and pending gates: [static selections data-prep](../features/static-site-pages/podborki-to-be.md#0-состояние-реализации-data-prep-mvp).

Kaggle must not be treated as an uncontrolled production publisher. Production trust belongs to the release protocol, not to the notebook itself.

Generated-output canaries must follow catalog lifecycle. A historical event may
remain an immutable focused resolver fixture, but `check:preview` must not
require its expired public row or date route. The crop gate therefore combines
a current generated visual-only rail specimen with the frozen Pianissimo
geometry once the original route has left the active export.

Shared progressive-enhancement controls outside a page's primary `main`
landmark may be intentionally hidden until their browser capability is known.
No-JavaScript content gates therefore inspect the product page's own landmark,
not unrelated focus-group/PWA controls contributed by the shared layout.

It is acceptable for the Kaggle API actor to be a dedicated personal Kaggle user/account, but only as an execution/publisher identity inside this protocol: least-privileged credentials, immutable snapshot input, status ledger, staging-prefix upload, checked manifest, promotion gate and rollback. The personal account must not become the only place where production release state is known.

## Production protocol

### V12 single-flight, no-op and crash adoption

The production rail is a durable state machine, not a local process lock:

1. Smart Update and the operator endpoint coalesce effects into one singleton
   outbox request; a changed effect during a run becomes one follow-up.
2. A read-only immutable static projection and one `Europe/Kaliningrad` build clock feed
   a canonical public-projection SHA-256. Queue attempts, timestamps and elapsed
   past-only churn are excluded; public event/media/config/repo/date/related
   inputs are included.
3. `BEGIN IMMEDIATE` claims `static_site_build_state` before any Kaggle push.
   Equal successful fingerprints no-op for both automatic and ordinary manual
   requests. Only `trigger=operator_request + force_rebuild=true` bypasses it.
4. Before push, the running outbox payload stores the exact build/run/repo,
   candidate-token, snapshot hash/path, fingerprint and clock handoff. The
   Kaggle ledger stores the exact input dataset identity.
5. If Fly dies after push, a retry first pulls the fixed kernel metadata. A
   matching running kernel is deferred; a matching complete kernel is
   downloaded and fully identity/hash validated, published once and adopted.
   It is forbidden to push a replacement merely because the callback is stale.
   Only a failed or different dataset identity releases the old claim.
   If startup or Smart Update requeues the same terminal-looking outbox row
   while it is still the exact `active_job_id`, the queue merge must preserve
   its `remote_handoff` and immutable `snapshot`. Fresh effect reasons are
   unioned into that payload; they never replace the identities required for
   adoption. A stale handoff is discarded only when the state row no longer
   points to that exact job. If a newer pending follow-up already exists, the
   generic coalesce supersession rule must not discard the older exact active
   job: its recovery/adoption runs first, then the follow-up consumes the newly
   accumulated effects. A deploy can make that terminal handoff intentionally
   non-adoptable because its repo/source identity differs from the running
   image. In that case replacement is still forbidden until the exact Kaggle
   ledger proves a terminal status. The host releases only resources owned by
   that run, then validates that the snapshot/manifest pair is a direct child
   of the configured snapshot root, matches the active payload and immutable
   manifest hash/size, and that the output has the recognized exact build
   identity. The active claim stays as a cleanup-pending barrier until strict
   snapshot and output deletion receipts are durable; only then is the failed
   `cross_deploy_recovery_rejected` claim/handoff cleared and a fresh
   current-image run allowed. A cleanup error, live or terminal-unknown handoff
   remains deferred under the same single-flight contract.
6. Review publication remains create-only under a fresh secret prefix. After full
   result/manifest/object verification, the durable internal current-review
   receipt advances atomically. Failed, no-op and artifact-only runs preserve
   its previous value. The optional root publisher runs only after these same
   result/root/candidate checks and successful review publication, and only
   under `ENABLE_STATIC_SITE_ROOT_PROMOTION`; it never mutates stable ICS.

The local `fcntl` lock remains a same-process convenience only. Correctness is
owned by SQLite claim/CAS, Kaggle dataset identity, result receipt and
conditional Object Storage writes.

```text
immutable compact Fly static projection
  -> one Kaggle CPU build with status ledger
  -> checked production-form artifact + release manifest
  -> create-only unlisted secret prefix (current production phase)
  -> default-off checked inactive root bucket -> ALB weight switch (code only)
```

Rules:

- one private input dataset per run;
- one immutable projection per build; new updates during a build queue a later build, they do not mutate the running build;
- resource lease: `static_site:builder`; two production builds must not publish concurrently;
- after a Fly restart, a matching terminal Kaggle ledger row immediately
  re-arms its exact JobOutbox owner for adoption/failure reconciliation; later
  Smart Updates do not wait for the live-run timeout behind completed work;
- the Fly owner and runner share one non-blocking file lock for abandoned
  staging cleanup, so a killed process cannot leave a large dataset that makes
  the host capacity probe prevent the cleanup code itself from starting;
- local Astro dependencies and generated `site/dist`/temporary preview trees
  are excluded from the Fly Docker context; a deploy no longer uploads hundreds
  of megabytes of reproducible frontend output before the remote image build;
- the kernel releases every acquired builder lease before sending its final
  `report_written` callback, so an immediate worker teardown cannot strand an
  exclusive lease; resource-acquisition failures are inside the guarded
  lifecycle and therefore also produce a terminal failure report instead of a
  permanently `running` ledger;
- Kaggle writes only to a unique staging/release prefix, never directly to production root with `--delete`;
- secret publication requires the bounded result, machine-readable release
  manifest and all production/secret checks to pass;
- failed Kaggle jobs must not alter production;
- secrets must be least-privileged for the target bucket/prefix and never printed.
- anonymous Object Storage listing must be disabled before treating an unlisted
  token as a bearer link; `noindex` is not access control;
- the publisher refuses every root/current/stable-ICS key and uses create-only
  writes (`If-None-Match: *`) below one immutable `_review/<token>/`
  prefix.

## Current implementation path

- Runner: `scripts/run_static_site_builder_kaggle.py`.
- Kernel script: `kaggle/StaticSiteBuilder/static_site_builder.py`.
- The runner must close its short-lived status-ledger `Database` immediately
  after writing the callback config. Otherwise the non-daemon `aiosqlite`
  worker can keep a successfully completed process alive at Python interpreter
  shutdown, leaving the durable static-build claim stuck before publication.
- The kernel must locate `kaggle_status_client.py` inside the mounted private
  status dataset under `/kaggle/input`; Kaggle does not add dataset directories
  to `sys.path`. A direct-import-only kernel silently loses all callbacks.
- Kernel callbacks remain authoritative. If callback delivery is absent but
  Fly validates the exact downloaded result and completes publication, the
  host records one idempotent `host_result_validated` terminal event so the
  ledger cannot remain falsely `created`; it does not populate
  `last_heartbeat_at` or pretend a kernel heartbeat occurred. The same
  transaction releases only active resource leases owned by that exact run.
  SQLite lock contention is retried with bounded backoff, and the next static
  build replays this idempotent reconciliation from the immutable current-review
  receipt before remote recovery or push. A late receipt can therefore never
  release a successor run's lease.
- Creating the per-run status ledger also acquires `BEGIN IMMEDIATE` with the
  same bounded lock-only retry. SQLite officially permits `SQLITE_BUSY` when
  another writer owns that slot; timeout expiry is handled as retryable
  contention, while non-lock failures still fail immediately. See
  <https://www.sqlite.org/lang_transaction.html>.
- Callback authentication reads each runner-created token from a fresh SQLite
  connection, and each callback event uses its own bounded
  `BEGIN IMMEDIATE` transaction with guaranteed rollback/close. The aiohttp
  process must not authenticate against the reusable `Database.raw_conn`: a
  stale read snapshot or an earlier failed write can otherwise hide a token
  that a short-lived runner has already committed and make Kaggle report a
  false `invalid token`/busy resource failure.
- Data export: `site/scripts/export-production-preview-data.py`.
- The full exporter requires the current `festival_calendar_item` catalog in
  core Fly SQLite and writes `site/src/data/festival-timeline.json` with source
  `sqlite-festival-calendar-v1`. Production and secret-candidate gates reject a
  fixture/fallback source; the release manifest binds its hash, catalog
  versions and DB/rendered row counts. Public festival-calendar rows
  participate in the static input fingerprint, so a calendar update requests a
  fresh build.
- Duration enrichment is not a StaticSiteBuilder/Kaggle responsibility.
  Smart Update may persist `event.duration_forecast_minutes` before the
  snapshot is handed off, and only for candidates that pass the implemented
  transport eligibility gate. Export validates/copies that nullable value;
  Kaggle/Astro never calls an LLM provider for missing duration and never
  creates a parallel estimate cache.
  Controls are `STATIC_SITE_DURATION_ENRICHMENT`,
  `STATIC_SITE_DURATION_MODEL`, `STATIC_SITE_DURATION_GOOGLE_KEY_ENVS`,
  `STATIC_SITE_DURATION_MAX_EVENTS` (1–50) and optional
  `STATIC_SITE_DURATION_REQUIRE_COMPLETE`. The result receipt stores only
  counts/status and never secrets.
- Fly handoff: `JobTask.static_site_build` and `main.py` `job_static_site_build_kaggle`.
- Feature docs: `docs/features/static-site-pages/astro-preview.md`.

The current production-candidate path produces three hash-checked tarballs:
the root-form proof, the immutable noindex secret candidate and a
`browser_evidence` archive. The latter contains separate root/candidate JSON
reports plus settled related-section and `1536×864` viewport screenshots; the
trusted runner rejects an absent, extra or mismatched artifact kind. Only the
secret-candidate tree can be published. CDN host `static.kenigevents.ru` is configured for
the static-site bucket and also serves mirrored event media `/p/...` plus stable
calendar files `/ics/<event_id>.ics`. Production root activation must never use
an Object Storage pointer or sequential copy. The implemented default-off path
reconciles the inactive one of two complete page-only buckets, verifies it, and
converges Yandex ALB weights. Live buckets/ALB/DNS are still absent, so root
apply remains `NO-GO`. For
preview/focus-group builds pass:

- `PUBLIC_ASTRO_ASSET_BASE_URL=https://static.kenigevents.ru/{buildId}` or runner `--astro-asset-base-url`;
- `PUBLIC_ASSET_BASE_URL=https://static.kenigevents.ru` or runner `--asset-base-url`;
- `PUBLIC_ICS_BASE_URL=https://static.kenigevents.ru/ics` when building locally; the default page logic also derives it from `PUBLIC_ASSET_BASE_URL`.

Before a CDN-enabled build, run/verify `scripts/migrate_static_media_to_cdn_bucket.py --db <snapshot> --active-on <date> --apply` so legacy `s3://kenigevents/p/...` objects referenced by active events exist in `s3://kenigevents.ru/p/...`.

## Implemented automatic and on-demand flow

`effectful Smart Update` coalesces one `static_site_build` request for 15 minutes
after the last public change. A change arriving during a running build records a
deferred marker; completion schedules exactly one follow-up. The worker creates
an online SQLite backup, runs `quick_check`, records SHA-256/size/max event
revision and submits that immutable file as a unique Kaggle input. Retries,
missed-build reconciliation and the feature-specific 14400-second end-to-end
runtime are bounded in `static_site_release.py`/`main.py`. The four-hour budget
is intentionally larger than the 90-minute remote Kaggle wait: it also covers
Fly-side create-only upload and byte/MIME verification of every object in a
full candidate (currently more than 3,300 objects). The remote wait remains
bounded independently.

Immediately before the vector barrier, the worker refreshes revisions for the
event ids already present in the coalesced request from current canonical
SQLite and recomputes the request watermark. This is a mechanical consistency
step, not a semantic rewrite: Smart Update remains the owner of event meaning.
It prevents a deterministic follow-up such as media review from making the
vector receipt newer than the historical intermediate revision originally
queued by Smart Update. A receipt that still differs from the current canonical
revision remains retryable and cannot cross the barrier.

The Smart Update build now also owns the accepted desktop keyboard navigator:
the exact deployed revision is written into `/app/.static-site-repo-sha` at
Docker build time by `scripts/deploy_fly_main.sh`, then packed into the
immutable source dataset. A legacy `STATIC_SITE_REPO_SHA` environment value is
only a local/backwards-compatible fallback; an image value always wins and a
drift is logged. Every event route in the resulting secret candidate mounts the
shared reviewed V7 router while
`PUBLIC_KEYBOARD_EVENT_NAVIGATION_ENABLED != 0`. This is still secret-only
preproduction publication. The checked production-form proof, public root,
`current` and stable `/ics/` objects are never promoted or modified by this
flow.

The same durable request can be made manually without bypassing debounce or the
outbox state machine:

```bash
.venv/bin/python scripts/request_static_site_build.py \
  --db /data/db.sqlite \
  --reason operator_secret_candidate \
  --correlation-id static-site:manual:<ticket>
```

An operator may request the Unusual weekly cacheless proof through this same
command by adding `--semantic-cache-mode cold`; ordinary requests default to
`warm`. Cold is rejected for automatic/Smart Update triggers. The mode is bound
to the request watermark, input fingerprint, recoverable remote handoff,
Kaggle input/config/result and success receipt. Cold omits the prior shared-BGE
NPZ/receipt plus Unusual/collection semantic caches and last-good inputs, so the
kernel must report `encoded_event_count=event_count` and
`reused_event_count=0`. It does not omit the independent related-chain cache and
does not create a second job, kernel lane or lease.

Every production-candidate collection pass also writes bounded
`unusual-events-health.json/.md`, candidates, review pack and manifest diff from
the existing semantic artifacts without another model call. The trusted runner
checks their exact hashes and identity, persists them atomically in
`/data/static_site_builder`, and binds the summary/hashes into
`static_site_build_result_v2` and `static_site_success_receipt_v2` before
scratch cleanup. `scripts/resolve_unusual_events_health.py` reads only that
durable receipt/state and returns pending rather than mixing files from a
different run. The daily GitHub monitor may enqueue through this operator CLI
and poll the resolver; it must never invoke the Kaggle runner directly.

`INC-2026-07-19-static-site-stale-builder-lease` is a mandatory regression
contract for this handoff: successful publication, ledger terminal state and
exact-owner resource release must converge even when terminal callbacks are
lost or Smart Update briefly holds the SQLite writer lock. The same contract
also forbids startup catch-up from erasing an active recoverable handoff when
it rearms an error outbox row, and requires terminal cross-deploy handoffs to
be retired by exact run identity before any replacement push.

All operator/bot link-producing paths must use
`static_site_release.resolve_current_secret_candidate`; historical named
preview URLs are evidence, not routing. The current checked immutable target is
read without enqueueing or mutating the DB:

```bash
.venv/bin/python scripts/request_static_site_build.py \
  --db /data/db.sqlite --show-current-review
```

The resolver requires the exact build/run/repo/snapshot/input fingerprint,
result/manifest/token hashes, verified object count, HTTPS bearer URL and
negative root/stable-ICS mutation evidence. It returns unavailable for legacy
or incomplete receipts. There is deliberately no public stable redirect.

Automatic execution and upload are deliberately opt-in. Required identity and
flags are documented in `.env.example`; defaults stay off:

```text
ENABLE_STATIC_SITE_KAGGLE_BUILDER=0
ENABLE_STATIC_SITE_SECRET_PUBLISH=0
ENABLE_STATIC_SITE_ROOT_PROMOTION=0
STATIC_SITE_ROOT_PROMOTION_MODE=plan
# local-only fallback; production identity is baked into the Fly image
STATIC_SITE_REPO_SHA=<exact clean pushed SHA>
STATIC_SITE_SECRET_CANDIDATE_ARTIFACT_RESEARCH=0
STATIC_SITE_SECRET_CANDIDATE_REQUIRE_AUTHORIZED_SEARCH=0
```

The Kaggle production-candidate invocation must include
`--profile production-candidate --catalog-mode full --snapshot-manifest ...
--repo-sha ... --run-id ... --build-id production-... --candidate-token ...
--export-in-kaggle`. The kernel runs Node 22, full export,
`build:production/check:production`, then
`build:secret-candidate/check:secret-candidate`, and returns the bounded v2
result. The trusted boundary rejects the result unless the complete production
template matrix and candidate noindex/no-referrer/prefix/root-isolation checks
are all `ok`. Local gates are:

When the two secret-candidate review flags are explicitly enabled, Fly includes
them in the immutable input fingerprint and forwards them through the runner
config. Kaggle then sets `PUBLIC_ENABLE_AMBER_ARTIFACT_RESEARCH=tail` and
`SECRET_CANDIDATE_REQUIRE_AUTHORIZED_SEARCH=1` only for the production-candidate
process. The root-form production build still hard-disables the artifact by
site mode and `check:production` verifies its generated absence; the candidate
build fails if browser-safe Search/Auth configuration is missing.

```bash
pytest -q tests/test_static_site_release.py \
  tests/test_static_site_build_handoff.py tests/test_static_site_public_gate.py
npm --prefix site run test:static-release
npm --prefix site run build:preview
npm --prefix site run check:preview
# With full exported catalog + immutable snapshot identity env:
npm --prefix site run build:production
npm --prefix site run check:production
npm --prefix site run build:secret-candidate
npm --prefix site run check:secret-candidate
npm --prefix site run check:browser-release -- \
  --browser chromium --root <generated-root> \
  --report <browser-report.json> --artifact-dir <browser-evidence-dir>
```

Production Fly releases must be made from exact clean `origin/main` with:

```bash
scripts/deploy_fly_main.sh
```

The wrapper supplies `STATIC_SITE_IMAGE_REPO_SHA` as a Docker build argument.
The Dockerfile rejects missing or malformed values, so a release cannot
silently pair new runtime code with an old static-site source revision.

The blocking Chromium gate waits for lazy recommendation images to decode,
asserts that a loaded `contain` image hides the semantic failure fallback,
checks cold/reload and real mouse→Russian-layout keyboard paths, and only then
writes `browser_visual=ok`. For compatibility investigation the same executable
accepts `--browser firefox` and `--browser webkit`; the host must have the
official Playwright system dependencies, and WebKit automation does not replace
the native Safari root-rollout gate.

The Python Fly-side publisher validates/extracts the tar safely, checks the
production-candidate result and manifest identity, rejects a bucket that still
allows anonymous ListObjects, uploads create-only, then verifies every object
hash/MIME plus a public `index.html` probe. It returns a receipt containing the
bearer URL and tree hash. Never put the URL/token in public docs, sitemap,
canonical, Telegram public channels or logs. Revocation in this phase means
deleting that complete immutable prefix; root/current remain untouched.

### Fly artifact and scratch capacity

Persistent builder state under `STATIC_SITE_ARTIFACT_ROOT` (production:
`/data/static_site_builder`) is limited to small validated semantic
caches/receipts and the local runner lock. Large reproducible bytes never use
the Fly volume: projection/dataset work uses `STATIC_SITE_SCRATCH_DIR`, the
immutable projection uses `STATIC_SITE_PROJECTION_SCRATCH_DIR`, and downloaded
result validation uses `STATIC_SITE_OUTPUT_SCRATCH_DIR` (all production
defaults are below `/tmp`). Root scratch is checked with a real create, write,
`fsync` and remove probe before a remote push.

The production input is `static_site_projection_sqlite_v1`, not a full SQLite
backup. A short read-only transaction copies only the explicit Astro-exporter
table allowlist into a compact, index-free SQLite read model, commits exact
row counts, closes the live source connection, then performs `quick_check`,
size cap and SHA-256 binding. Operational relations such as `vk_source_packet`,
`vk_inbox`, `joboutbox`, `ops_run` and Kaggle ledgers are structurally excluded.
The manifest table inventory and row counts are revalidated by the runner and
kernel. This transaction must be closed before dataset upload/polling so the
static build cannot pin the production WAL.

Runner build identities are bounded to `preview-*` or `production-*` before
constructing any filesystem path. Output creation uses one assertion-safe
helper that rejects traversal, a non-directory target and any pre-existing
symlink before download/adoption; it never calls permissive `rmtree(...,
ignore_errors=True)` on a derived path.

After a durable terminal receipt, the runner prunes only recognized
`output-production-*` trees below the configured ephemeral output root. Default
terminal retention is zero because counts/hashes needed for diagnostics are
persisted in SQLite/the receipt. The exact active or recoverable handoff is
always preserved; unknown directories, symlinks and paths outside the root are
fail-closed and never removed. Failed/nonterminal outputs remain available for
explicit incident disposition rather than being mistaken for regenerable
success artifacts.

Downloaded immutable archives remain under ephemeral process scratch until
their terminal receipt is durable. Secret-candidate and optional atomic-root
publishers extract into an
isolated `TemporaryDirectory` on generic process scratch (`/tmp` by default,
or `STATIC_SITE_PUBLICATION_SCRATCH_DIR`) and remove it on both success and
failure. This prevents a roughly 600 MB generated tree from being duplicated
on the 3 GB Fly volume during create-only upload and object verification. The
publication call stays in `asyncio.to_thread`, so the API/event loop remains
responsive while the storage client performs the bounded object walk.

Capacity recovery runs before the scratch-space probe. The Fly owner removes
all complete projection files except the exact paths named by a readable active
handoff; an unreadable active handoff still fails closed. After acquiring the
single local runner lock and before creating a new staging tree, the Kaggle
launcher removes only real, non-symlink `static-site-kaggle-*` directories
inside its configured scratch root. Those directories are owned by
`TemporaryDirectory` during a normal run, so their presence before a new
locked run is evidence of process death. Unknown paths and symlinks are never
followed or removed. This ordering ensures that a regenerable staged SQLite
copy cannot itself cause the next storage preflight to fail. Dataset staging
hard-links the immutable projection when source and dataset scratch share a
filesystem; it never creates another DB-sized allocation merely to satisfy the
directory-oriented Kaggle API.

The mounted projection is read directly under `/kaggle/input`. It is never
copied to `/kaggle/working`, and cleanup removes any legacy/accidental
`*.sqlite*` working file so private SQLite bytes cannot become Kaggle output.
The private dataset reference is content-addressed from the complete staged
payload. Once that exact dataset identity is durable, restart adoption uses its
stored projection manifest/SHA/size even if local process scratch disappeared;
it still requires exact kernel dataset-source identity and result hashes and
never pushes a replacement for a matching live/terminal run.

Production health reports persistent and scratch disk separately. A critical
or unwritable `/tmp` keeps `/healthz` not ready and blocks the static preflight
even during startup grace. The coalesced request is deferred without incrementing
its finite attempt counter until cleanup or deploy restores capacity; it must
not be bypassed by sending another Kaggle attempt.

An exact recoverable remote handoff is adopted and reconciled before a new
build. Active handoff output must not be removed blindly; only the exact
terminal owner is eligible for bounded ephemeral cleanup.

Direct Kaggle-to-Yandex artifact staging remains the next transport phase. It
must use a separate least-privileged staging/review identity and leave Fly as
the manifest-validation and root-promotion authority. Until that gate is
implemented, checked archives may transit through `/tmp` for the existing
create-only Yandex publisher, but never through `/data`.

The adoption runner applies the same ordering internally: it verifies the
fixed remote dataset and completed kernel, removes only the replaceable local
duplicate for that exact build, then runs the capacity probe before downloading
the authoritative output again. New Kaggle submissions still require the
capacity probe before any staging or push.

### Smart Update debounce and historical 2026-07-15 data evidence

Smart Update enqueues `static_site_build:prod` for 15 minutes after the latest
accepted event update. Repeated updates move the one pending job forward only
until `STATIC_SITE_MAX_DEBOUNCE_SECONDS` from the first coalesced update
(production: 30 minutes). This maximum prevents an uninterrupted repair/import
stream from starving publication indefinitely. An operator, calendar-rollover
or startup request keeps immediate priority when later Smart Updates merge. If a
build is already `running`, one deferred pending follow-up is retained instead
of merging the update into the immutable running snapshot; later updates
coalesce into that follow-up. Static-site builds use the task-specific maximum
runtime rather than the historical ten-minute stale-owner constant, so a valid
long Kaggle build is not rearmed as stale.

The data/artifact portion of
`preview-20260715t-production-transport-mobile-real-events-v1` exported `282`
public future/ongoing events, refreshed both pgvector documents for every
anchor, reused `564` unchanged embeddings (`0` provider calls) and generated
`40` non-dangling candidates per event. Its desktop presentation evidence is
**rejected** by `INC-2026-07-15-static-desktop-template-regression`: the build
kept the legacy production DOM and the consultant did not review the generated
mass-event URLs. The replacement
`preview-20260715t-production-desktop-contract-v2` must pass
`check:production-desktop`, full-catalog browser acceptance and exact public
URL review. Either prefix proves only the noindex artifact path; neither removes
the separate atomic production-root promotion gate described above.
Production Fly config enables the builder, full-catalog limit, pgvector
retrieval and CDN/ICS bases, but keeps vector writes owned by the separate
coalesced `event_vector_sync` job. This removes a duplicate full-catalog write
pass and its avoidable Supabase egress from every static build.


## Historical v59 strict pgvector evidence and open gate

`preview-20260629-event-pages-v59-related-gemma50` is historical strict related canary evidence on real production-snapshot data: 50 events focused on 2026-06-30/2026-07-01, CDN-enabled assets/ICS, `npm run check:preview` passed, public Playwright smoke passed, and `/data/discovery/6447.json` shows `6310` “Архитектурно-урбанистическая студия...” as the strict Gemma-approved first related candidate (`llm_semantic_score=0.88`).

Smart Update handoff passes the pgvector/Gemma flags from environment into
`scripts/run_static_site_builder_kaggle.py`: `--related-mode`, `--pgvector-*`,
`--gemma-related-*`, status DB/callback, CDN asset/ICS bases, browser-safe
AuthorizedEventSearch public env and the date-focus controls. Production keeps
`STATIC_SITE_SYNC_PGVECTOR_VECTORS=0`: the dedicated Fly vector owner must
finish first and the build validates its receipt at the vector barrier. Kaggle
then performs only bounded read-only compact related-candidate RPCs. The
optional `--sync-pgvector-vectors` switch remains a manual canary/backfill tool,
not a production-build owner.

The same barrier receipt is the immutable Search revision handoff. The vector
owner writes `event_vector_sync_receipt_v2` with the exact catalog, searchable
corpus and search-document revisions plus complete post-write coverage. For an
authorized Search candidate, Fly validates that v2 receipt and packages it in
the private Kaggle input dataset as `event-search-corpus-receipt.json`; the
kernel copies it into `site/src/data` only after exporting the matching
snapshot. `build:secret-candidate` then refuses a missing, incomplete or
catalog-mismatched receipt. This handoff must not be replaced by enabling
`STATIC_SITE_SYNC_PGVECTOR_VECTORS`: regular static builds remain read-only with
respect to the vector projection.

`INC-2026-07-11-event-vector-sidecar-sync-stalled` restored this optional
handoff after a merge dropped it. Regular production vector ingestion is owned
by the separate full-catalog `event_vector_sync` lane; do not enable a coupled
preview build merely to keep search vectors fresh.

Static related publication policy:

- Astro generation consumes cached strict related manifests and does not call Supabase/LLM on page view;
- a related cache is reusable only when the event id set, search fingerprints and Gemma policy signature match;
- if new/changed events are present, recompute the active/future related graph because the new event can be a candidate for old pages;
- same-day events that already started, past events and cancelled/deleted/duplicate rows are excluded at export/generation time;
- static related generation retries Gemma 4 26B with backoff and does not fall back to Gemini/Flash-Lite, preserving Lite quota for runtime flows.

### P0 Supabase egress guard for related rebuilds

The 24–28 July usage audit attributes approximately `3.10 GB` of Supabase
egress to the full-catalog related-candidate RPC used by static builds. The
daily pattern matches the reported project egress graph. The exporter consumes
only `event_id` and `vector_similarity`, but the current RPC response carries a
much wider row, about `64–65 MB` per complete rebuild.

The repository now uses the dedicated backend-only RPC
`event_related_candidates_compact_by_event_id_v1`. Each per-anchor response has
exactly `event_id` and `vector_similarity`; the function is
revoked from `PUBLIC`, `anon` and `authenticated` and granted only to
`service_role`. It preserves the legacy per-anchor HNSW ordering/top-K contract
without returning titles, tags, dates, distances or `card_snapshot`. The
measured estimate for the same historical run set is about `79 MB`, a `97.45%`
reduction. Applying the accompanying Supabase migration is an activation gate;
the exporter fails closed rather than falling back to the wide legacy RPC.

The exporter reads at most `STATIC_SITE_RELATED_RESPONSE_MAX_BYTES` per RPC
body (default `256 KiB`) before JSON decoding, rejects any row whose keys differ
from the two-field projection and records request count, row count, aggregate
response bytes and maximum single response bytes. A full-catalog pgvector
rebuild also fails when aggregate bytes exceed
`STATIC_SITE_RELATED_TOTAL_RESPONSE_MAX_BYTES` (default `16 MiB`). The same
`static_related_retrieval_receipt_v1` is stored in `preview-related.json` and
copied into `static_site_build_result.json`.

A valid related-cache hit performs **zero** Supabase candidate RPCs and records
zero request/row/byte counters with `source=cache`. A miss still recomputes the
whole active graph once: changed-anchor-only recomputation remains forbidden
because a new event can change old anchors. Duplicate anchor ids fail before
the first request, preventing a second corpus fetch inside one export. Astro
continues to perform the full build for now; it consumes the generated related
manifest and never retrieves the corpus again.

This is independent from browser Auth transport. The Auth/Data relay, if
enabled, is for small user requests only and must never proxy the bulk static
related rebuild.

For production candidates with authorized surfaces the relay is mandatory,
not optional degradation. Fly passes
`STATIC_SITE_PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL` (or its documented
public/personalization aliases) into the runner's
`--public-personalization-supabase-relay-url`; Kaggle copies it only to the
browser-safe `PUBLIC_*` build environment. The required-authorized gate rejects
a candidate that has the Supabase URL/key but omitted the relay.

This still does **not** mean Smart Update publishes the production root. The
ordinary enabled flags build and publish only a checked unlisted candidate.
The separate two-bucket/ALB state machine is default-off and remains `NO-GO`
until its live inventory, SWS, retained previous tree and rollback drill pass
the [atomic-root runbook](static-site-atomic-root.md).

## Static-site Gemma/related secrets

For API-started Kaggle static-site builds, do **not** depend on Kaggle UI Secrets for Gemma verification. The documented project pattern is encrypted split datasets. `scripts/run_static_site_builder_kaggle.py` now creates two short-lived private datasets when `--gemma-related-verify` is enabled:

- cipher dataset: `secrets.enc` plus non-secret `config.json`;
- key dataset: `fernet.key` and `fernet.keys` plus non-secret `config.json`.

Only the minimal runtime env is encrypted: the selected Google key env (default `GOOGLE_API_KEY4`) and Supabase limiter env (`SUPABASE_URL` plus `SUPABASE_KEY`/service key). The Kaggle kernel loads these envs in memory before running the exporter. A waited run deletes the secret datasets in `finally`; `--no-wait` keeps them because the kernel still needs them.

The build must still fail/skip loudly if limiter env is missing. Direct provider calls or local limiter fallback are not allowed for the related-chain Gemma audit. For pgvector related mode the encrypted runtime payload also carries the personalization Supabase URL/secret and selected embedding key env, because the Kaggle exporter must upsert changed search documents/vectors before calling the backend-only related RPC. The kernel copies only browser-safe URL/publishable-key values into `PUBLIC_*` for Astro; it must never expose `PERSONALIZATION_SUPABASE_SECRET_KEY` or service-role keys to the static bundle.

Before claiming the authorized-search browser gate, run:

```bash
python3 scripts/check_authorized_search_readiness.py --env-file .env --probe-edge --probe-yandex-provider --strict
```

Without probes it gives a redacted env-only status and is safe in local operator shells.

## Release manifest contract

Every production candidate build must emit at least:

```json
{
  "schema_version": "static_release_manifest_v1",
  "build_id": "2026-06-28T14-20-00Z",
  "source_snapshot_id": "events_snapshot_2026_06_28_1415",
  "generated_at": "2026-06-28T14:26:13Z",
  "git_sha": "...",
  "paths": {
    "html_count": 842,
    "event_pages_count": 612,
    "listing_pages_count": 18,
    "persona_manifests_count": 96
  },
  "checks": {
    "astro_build": "ok",
    "check_preview": "ok",
    "freshness": "ok",
    "seo": "ok",
    "cdn_urls": "ok"
  },
  "artifacts": {
    "html_prefix": "releases/2026-06-28T14-20-00Z/",
    "card_snapshot": "data/cards/2026-06-28T14-20-00Z/cards.compact.json"
  }
}
```

## Status evidence

A production/Fly run must pass `/data/db.sqlite` as status DB and `/internal/kaggle/run-event` as callback so the existing poller sees:

- `kernel_started`;
- `preflight_ok`;
- meaningful `alive` progress;
- `report_written` or terminal failure;
- resource acquire/release for `static_site:builder`.

A local manual run without callback/status DB is useful build evidence, but it is not production status-ledger evidence.

### Emergency host fallback boundary

If the normal Fly → Kaggle handoff cannot create its short-lived private input
dataset, an operator may build **only an immutable noindex secret candidate** on
the trusted host from the same frozen snapshot and clean `origin/main` SHA. The
host run must reuse the production exporter, vector revision, build profile,
manifest/fingerprint validation and secret-prefix publisher. It may skip
Kaggle-specific status callbacks and OS-package installation only when those
host dependencies are already present.

This fallback is review evidence, not a successful StaticSiteBuilder Kaggle run:

- record the provider failure and the exact snapshot/repo SHA;
- validate both build result and public noindex surface;
- publish only the secret archive, never the root archive;
- do not advance the production current pointer or close the Kaggle
  status-ledger gate;
- retain the ordinary root hash as rollback evidence.

The production leakage check matches the bare `data-amber-artifact` and
`data-artifact-collection` DOM markers with attribute boundaries. The inert
`data-amber-artifact-research="off"` configuration attribute and the
`data-artifact-collection-unavailable` fallback are allowed and must not be
treated as enabled research UI. The unavailable-state gate is tied to that DOM
contract, not to mutable reader-facing Russian copy. `/artefakty/` remains
intentionally `noindex` even while the ordinary production route renders only
the unavailable fallback.

### Restart before remote handoff

The outbox distinguishes a durable remote build from a process orphan. A row
with an exact `static_site_build_state.active_job_id` or a persisted
`remote_handoff` keeps the full 14400-second end-to-end budget. A `running` row with
neither marker is a pre-handoff owner and is recovered after the bounded
`STATIC_SITE_PRE_HANDOFF_STALE_SECONDS` window (600 seconds by default), so a
restart between outbox claim and Kaggle handoff cannot block Smart Update for
the full four-hour budget. When a follow-up loses its CAS to a real owner, it is deferred for
`STATIC_SITE_CLAIM_RETRY_SECONDS` (30 seconds by default) without consuming an
attempt; a two-second log/SQLite hot-spin is forbidden.

Production must not enable `STATIC_SITE_REQUIRE_VECTOR_BARRIER=1` while leaving
`ENABLE_EVENT_VECTOR_SYNC=0`: that configuration makes every new Smart Update
revision permanently unproducible. After the shared atomic Google limiter and
event-vector path have passed their release gate, both flags stay enabled and
the independent vector receipt is allowed to converge before the static build.

The event-vector projection keeps the shared Google AI gateway's fail-fast
NO_WAIT boundary. Its batch caller may smooth only `rpm`/`tpm` admission by
honoring the ledger's bounded `retry_after_ms` (maximum 65 seconds, jittered,
three retries) for the same idempotent embedding input. It never waits or
spills to another key for `rpd`, `no_keys`, unknown admission, or a wait outside
that bound. This avoids restarting an otherwise healthy projection every ten
minutes at the next minute-bucket boundary while preserving fail-closed quota
accounting.
