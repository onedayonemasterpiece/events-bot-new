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

## 5. Normal ingestion and live Fly

Telegram, VK and parser replays through their normal adapters follow only after
the copy apply/warm gate. A Fly canary follows only when the exact SHA is merged
into `origin/main`; deploy exclusively through `scripts/deploy_fly_main.sh`.
Never upload or execute an unmerged implementation in a production machine.

The live sequence is read-only evaluate (at most 20 IDs), reviewed apply (at
most 12), identical warm replay, then at least six preselected fresh real posts.
Verify `/healthz`, `PRAGMA quick_check`, scheduler, outbox and runtime file logs.
