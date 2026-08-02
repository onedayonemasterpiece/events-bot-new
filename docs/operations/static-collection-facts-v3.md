# Static-collection facts v3: bounded operator runbook

Status: **implemented in a stacked draft; semantic publication and production
apply are blocked until the real-data quality gate passes**.

Canonical design:

- [`static-collections-smart-update-facts-v3-implementation.md`](../features/static-site-pages/static-collections-smart-update-facts-v3-implementation.md)
- [`static-collections-smart-update-facts-v3-real-data-acceptance.md`](../features/static-site-pages/static-collections-smart-update-facts-v3-real-data-acceptance.md)
- [integration report](../../.codex/integration/static-collection-facts-v3-INTEGRATION_REPORT.md)

## Safety boundary

Use the existing `scripts/backfill_static_collection_facts.py`. Do not create a
second extraction pipeline. `--plan` and `--evaluate` never write the requested
SQLite file; `--apply` requires explicit event IDs and is allowed only after the
primary quality gate is green. Audience-only apply may change only
`Event.collection_decisions` for selected events. It must not change `is_free`,
prose, identity, `EventSource`, links or posters.

Never use legacy v2 `audience_decision` as v3 truth. Never enable Astro routes,
navigation, sitemap, manifests or public collection labels from this runbook.

## 1. Fresh Fly snapshot

```bash
set -a
source /home/dev/.config/fly/release.env
set +a
export PATH="$HOME/.fly/bin:$PATH"
RUN="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p artifacts/db/static-collection-facts-v3 \
  artifacts/static-collection-facts-v3
./scripts/sync_prod_db.sh \
  --output "artifacts/db/static-collection-facts-v3/prod-$RUN.sqlite"
sha256sum "artifacts/db/static-collection-facts-v3/prod-$RUN.sqlite"
python3 - <<PY
import sqlite3
p="artifacts/db/static-collection-facts-v3/prod-$RUN.sqlite"
with sqlite3.connect(f"file:{p}?mode=ro&immutable=1", uri=True) as db:
    assert db.execute("PRAGMA quick_check").fetchone()[0] == "ok"
PY
```

Do not commit the database. When local disk is constrained, set
`TMPDIR=/dev/shm`; read-only plan/evaluate makes a disposable online-backup copy.

## 2. Shared Google limiter for a local replay

All real provider sends must use `google_ai_project_model_atomic_v1`. Source the
local approved environment, then alias the already approved personalization
Supabase secret into the dedicated limiter names without printing values:

```bash
set -a
source /home/dev/projects/events-bot-new/.env
set +a
export GOOGLE_AI_LIMITER_SUPABASE_URL="$PERSONALIZATION_SUPABASE_URL"
export GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY="$PERSONALIZATION_SUPABASE_SECRET_KEY"
export GOOGLE_AI_NORMAL_KEY_ENVS="GOOGLE_API_KEY6,GOOGLE_API_KEY,GOOGLE_API_KEY2,GOOGLE_API_KEY3,GOOGLE_API_KEY4,GOOGLE_API_KEY5"
```

A reserve response missing the exact limiter contract or `quota_scope` is a hard
stop. Do not enable the process-local limiter while other Codex/Fly/Kaggle
consumers may run.

## 3. Read-only plan and primary-only evaluate

Freeze exact source bindings before viewing model output. Both event and source
allowlists are mandatory for acceptance:

```bash
python3 scripts/backfill_static_collection_facts.py \
  --db "$DB" --plan --reason audience --current-date 2024-01-01 \
  --event-id-file "$EVENT_IDS" --source-id-file "$SOURCE_IDS" \
  --limit 100 --max-sources-per-event 2 --output "$PLAN_REPORT"

SMART_UPDATE_4O_FALLBACK=0 \
python3 scripts/backfill_static_collection_facts.py \
  --db "$DB" --evaluate --primary-only --reason audience \
  --current-date 2024-01-01 \
  --event-id-file "$EVENT_IDS" --source-id-file "$SOURCE_IDS" \
  --limit 100 --max-sources-per-event 2 --output "$PRIMARY_REPORT"
```

Require: exact event/source bindings, `writes=0`, one physical Gemma send per
routed source, zero GPT-4o calls, exact quotes, zero hard-negative confirmations,
and the recall/count gates from the acceptance document. A provider-deferred
row is not a positive hit.

### Offline Gate-B evaluator

The provider replay does not decide its own quality. Bind its immutable report
to the corrected provisional seed, source-review index, repository revision and
the exact SQLite bytes with the pure offline evaluator:

```bash
SHA="$(git rev-parse HEAD)"
python3 scripts/evaluate_static_collection_facts_v3_gate_b.py \
  --report "$PRIMARY_REPORT" \
  --seed docs/review-data/static_collections_review_seed_v1.json \
  --source-review-index docs/review-data/static-collections-source-reviews-v1/index.json \
  --boundary-manifest "$BOUNDARY_MANIFEST" \
  --db "$DB" \
  --minimum-recall 0.80 \
  --expected-repo-sha "$SHA" \
  --json-output "artifacts/static-collection-facts-v3/gate-b.json" \
  --markdown-output "artifacts/static-collection-facts-v3/gate-b.md"
```

The evaluator opens SQLite as `mode=ro&immutable=1`, performs no provider call,
and fails closed on stale cohorts, repository/DB/index/receipt/source hashes,
source-to-event mismatches, non-exact quotes, fallback, more than one send,
logical writes or a confirmed hard negative. Its output contract is
`docs/review-data/static_collection_facts_v3_gate_b_report.schema.json`.

Recall is occurrence-family weighted and uses only positive rows with all of:
`confidence=high`, `review_decision=keep`, `source_status=sufficient`.
Borderline and source-insufficient rows are WATCH-only and excluded from the
denominator. Every audience label must reach `0.80`; `4/5` passes and `3/5`
blocks. A pass opens only the production-copy gates below. Semantic publication
remains blocked.

`--boundary-manifest` is optional. Use its versioned contract
`docs/review-data/static_collection_facts_v3_boundary_manifest.schema.json`
when the corrected real replay must retain named/removed boundaries outside the
seed supply (including 4648, 6871, 7103, 7307, 7326 and corrected
6562/6898/7102/7172/7176/7258/7290). The manifest binds the exact corrected seed
file SHA and every extra `event_id`/`source_id`/`source_text_sha256`. With it,
the only valid report cohort is exactly `seed bindings + manifest bindings`;
without it, any extra report row is stale and blocks.

Boundary expectations never enter recall:

- `not_confirmed` is hard: runtime `confirmed` is NO-GO;
- `watch` expects no confirmation, but a confirmation is only a categorized
  WATCH warning;
- `confirmed_watch` expects confirmation, but any disagreement is only WATCH.

The JSON/Markdown result reports boundary matches, hard failures and watch
disagreements separately.

## 4. Apply and warm replay

Run only after primary replay and the separate fallback drill pass. Work on a
fresh mutable copy, not the pristine downloaded snapshot:

```bash
cp "$DB" /dev/shm/static-collection-facts-v3-work.sqlite
python3 scripts/backfill_static_collection_facts.py \
  --db /dev/shm/static-collection-facts-v3-work.sqlite \
  --apply --primary-only --reason audience \
  --event-id-file "$COHORT_EVENT_IDS" --source-id-file "$COHORT_SOURCE_IDS" \
  --limit 20 --max-sources-per-event 2 --output "$COPY_APPLY_REPORT"
# Repeat the exact command with a different output path for the warm report.
```

Warm acceptance is exactly `provider_calls=0`, `writes=0`, no logical row
changes. Any prohibited diff or failure erasing accepted truth is a stop.

### Product snapshot after each stage

After copy apply, warm replay and normal-ingestion replay, build a separate
source-bound product snapshot from that exact database state. `source_scope` is
an arbitrary provenance label for the stage; the optional evidence trust filter
is a different argument and must not overwrite provenance:

```bash
python3 site/scripts/static_collection_product_snapshot.py \
  --db /dev/shm/static-collection-facts-v3-work.sqlite \
  --current-date 2026-08-02 \
  --source-scope production-copy-after-apply \
  --evidence-trust-scope all \
  --output artifacts/static-collection-facts-v3/product-after-apply.json

python3 scripts/check_static_collections_product_quality.py \
  --snapshot artifacts/static-collection-facts-v3/product-after-apply.json \
  --expect-status WATCH
```

Use correspondingly explicit scopes such as `production-copy-after-warm` and
`normal-ingestion-replay`. Without owner-accepted baseline, `WATCH` is the
expected honest result. Warm equality is the same `input_fingerprint` plus the
PR #234 runner `normalized_output_sha256` (the adapter persists that same
visible-view hash); `snapshot_sha256` may change with `generated_at`.
The adapter does not call a model (`provider_calls=0`) and never unlocks
publication.

## 5. Normal-ingestion copy replay

Telegram, VK and parser evidence must cross the ordinary production entry
points; calling `apply_collection_decisions` from a test is not acceptance.
Use the bounded harness against a disposable mutable copy:

```bash
python3 scripts/run_static_collection_ingestion_replay.py \
  --db-copy /dev/shm/static-collection-facts-v3-ingestion.sqlite \
  --manifest artifacts/static-collection-facts-v3/ingestion-manifest.json \
  --output artifacts/static-collection-facts-v3/ingestion-report.json \
  --allow-mutable-copy
```

The manifest contract is
`docs/review-data/static_collection_ingestion_replay_manifest.schema.json`.
Freeze the case list and exact `event_id`/`source_id`/`source_url`/
`source_type` bindings **before** looking at provider output. For an acceptance
run, also set `db_logical_sha256_before` to the canonical Event + EventSource
logical hash reported by a dry copy inspection; a mismatch stops the run.
Every expected first-pass call/write count is explicit, and the warm expectation
is always zero calls and no collection write.

The acceptance manifest contains at least 12 artifact-backed cases and retains
the frozen boundary mix from the acceptance design: 3 direct-child, 3 explicit
family, 2 explicit joint-family, plus parents-only, age-only, vague-family and
ordinary-adult negatives; use at least 4 Telegram, 4 VK and 2 parser cases.
Festival extraction/pages and cinema sources are not cases in this run.

Fixture shapes deliberately preserve upstream adapter contracts:

- Telegram: the complete, unchanged `telegram_results.json` packet plus
  `adapter_options.source_username` and `message_id`;
- VK: `{source_post_url, draft, photos}` where `draft` is an `EventDraft` JSON
  object and `photos` contains only reproducible URL strings; serialized
  `poster_media` objects are rejected;
- official parser: `{source, event}` where `event` contains only
  `TheatreEvent` constructor fields.

Acquire these inputs without another extraction run. Download an already
completed Telegram Monitoring `telegram_results.json` from the recorded Kaggle
kernel output and reduce a copy to the selected messages while retaining their
matching `sources_meta` rows and recomputing the four `stats` counters. For
official parsers, take the corresponding event object from an already completed
parser kernel JSON and wrap it with its source name. A VK replay requires the
actual pre-persist `EventDraft` captured by the intake run. The production DB
does not retain original Telegram packets, parser objects, or VK drafts: an
Event/EventSource-derived reconstruction is useful only as a smoke fixture and
must not be labeled source-faithful acceptance. If the exact historical upstream
artifact for a selected row is unavailable, replace that row with another
preselected real binding whose artifact is available; do not invent the packet.

The harness invokes `process_telegram_results`, `persist_event_and_pages` and
`process_source_events` respectively. It keeps Smart Update and collection
adjudication real, but forces `schedule_tasks=False`, disables Telegraph/VK job
scheduling and live web/media fallback, and supplies `bot=None`. Telegram adds
an `INSERT OR IGNORE` force-message control row on the disposable copy so the
same real packet crosses the handler twice; that control write is intentionally
outside the Event/EventSource semantic diff.

The report binds the repository SHA, exact generator command, manifest/fixture
hashes, initial/final DB hashes, redacted adapter receipts, safe provider trace,
facts-v3 receipt hashes, exact source binding and Event/EventSource writes. Raw
source copy, URLs, prompts, responses and credentials are not emitted. PASS
requires the declared first-pass collection call/write behavior, exact persisted
quote grounding, unique binding, SQLite `quick_check=ok`, then an identical warm
replay with no Event or EventSource changes and zero collection calls/writes.
This tool never publishes, deploys, enables routes, or substitutes a direct
facts apply for normal ingestion.

## 6. Live Fly

A Fly canary follows the normal-ingestion copy replay only when the exact SHA is
merged into `origin/main`; deploy exclusively through
`scripts/deploy_fly_main.sh`. Never upload or execute an unmerged implementation
in a production machine.

The live sequence is read-only evaluate (at most 20 IDs), reviewed apply (at
most 12), identical warm replay, then at least six preselected fresh real posts.
Verify `/healthz`, `PRAGMA quick_check`, scheduler, outbox and runtime file logs.
