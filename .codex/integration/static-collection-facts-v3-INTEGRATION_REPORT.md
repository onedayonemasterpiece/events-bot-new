# Static-collection facts v3 — integration report

Date: 2026-08-02 UTC

PR-A head: `d9112bd3a547bd8592d42bf3332b5bf69e0fe3e8` (draft PR #222)

Verified extraction/product code SHA: `c625f1809ee60eaf62c16f079f701adb6a9847f1`

Acceptance-harness head before this report: `d5dce869f4a566f78322093bfc931dcfc8a78c26`

Integration branch / draft PR: `integration/static-collection-facts-v3` / #233.

## Executive result

The corrected source truth and real primary-only Gate B are verified. Copy
apply/warm, fallback and the product adapter have strong bounded evidence, but
do not yet meet every mandatory acceptance contract. Ordinary ingestion is
**BLOCKED**, so this stack is not ready for a clean main-based production branch
or Fly canary.

| Gate | Result | Evidence |
|---|---|---|
| Corrected provisional PR A | PASS | review gate PASS, 0 errors/11 supply warnings; strict still fails only because PR-B artifacts do not exist |
| Facts-v3 deterministic suite | PASS | 202 focused tests on the integrated worktree; GitHub facts-v3 and product workflows green on `c625f180` |
| Fresh production snapshot | PASS | 6,998 events, `quick_check=ok`, SHA below |
| Full corrected Gate B | **PASS** | 50 real EventSource rows, 50 Gemma sends, no GPT-4o, no writes, exact binding/quotes; all recalls above 0.80 |
| Fallback/failure drill | PARTIAL | committed offline 4/4 fail-closed drill plus 3 real valid-fallback cases, but the paid-driver is a volatile artifact and malformed/evidence mismatch were not repeated on real sources |
| Production-copy apply/warm | PARTIAL | apply mechanics and an effective 20-source warm no-op are proven, but no exact-cohort copy `plan → evaluate → apply → identical warm` chain exists; one initial defer was replaced |
| Product snapshot + #234 monitor | PARTIAL/WATCH | local copy snapshots after apply/warm have the same normalized output and no FAIL; GitHub live product job and post-ingestion snapshot are absent |
| Ordinary Telegram/VK/parser ingestion | **BLOCKED** | production does not retain the original upstream packets; reconstructed Telegram is rejected before collection adjudication and existing parser replay mutates source metadata while skipping Smart Update |
| Clean main-based integration / Fly | BLOCKED | Gate E is not green; #207/#222/#233/#234 are open and verified SHA is not reachable from `origin/main` |
| Semantic publication | BLOCKED | no owner gold, scores, thresholds, Astro routes, navigation, sitemap or public labels were added |

## 1. Corrected source truth

PR #222 now reflects ontology v2 rather than preserving unsupported supply:

- event 6562 is a high-confidence `family_suitable` positive with exact quote
  `Приходите всей семьёй!`, not a family hard negative;
- 6898, 7102, 7258 and 7290 are no longer unsupported family/joint positives;
- 7172 and 7176 are no longer unsupported child positives;
- the earlier 4648, 6871 and 7103 corrections and split festival scope remain;
- no owner gold, scores, thresholds or replacement rows “for counts” were
  introduced.

The generated seed is reproducible and byte-stable. Review mode is PASS with
0 errors and 11 expected supply warnings. Strict mode remains an expected FAIL
only because PR-B owner-gold/calibration artifacts intentionally do not exist.

## 2. Code and deterministic verification

The implementation extends the existing
`collection_candidate_adjudication`; it does not add a second production LLM
request. It keeps child, family and joint facts independent, requires exact
quotes from the same persisted EventSource, writes per-source receipts and
projects the legacy audience value deterministically. Normal routing recognizes
`приходите всей семьёй`, `всей семьёй` and `всей семьей` only as routing
signals, never as deterministic evidence.

Final local integrated command:

```bash
TMPDIR=/dev/shm /home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q \
  tests/test_smart_event_update.py tests/test_event_update_merge.py \
  tests/test_google_ai_client.py tests/test_static_collection_backfills.py \
  tests/test_static_collection_facts_backfill_report.py \
  tests/test_static_collection_facts_fallback_drill.py \
  tests/test_static_collection_ingestion_replay.py \
  tests/test_static_collection_product_snapshot.py \
  tests/test_static_collection_export.py \
  tests/test_static_collection_semantics.py \
  tests/test_static_collections_product_quality.py \
  tests/test_static_collection_quality_validator.py \
  tests/test_static_collection_review_seed.py \
  tests/test_static_collection_data_quality_reviews.py
# 202 passed in 11.46s
```

GitHub evidence on `c625f180`:

- facts-v3 offline contract: <https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/30758172640> — PASS;
- product-quality workflow: <https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/30758172646> — PASS.

The later commits `cf9d663c` and `d5dce869` add only acceptance harnesses and
tests; they do not change production extraction decisions.

## 3. Fresh production snapshot and Gate B

Fresh online copy:

- artifact: `artifacts/db/static-collection-product-loop/prod-20260802T164757Z.sqlite`;
- size: 316,874,752 bytes;
- events: 6,998;
- `PRAGMA quick_check=ok`;
- SHA-256: `9106d632a48df0d9fd2dfef67044c14837e508f5cbc6a93c5d99d951ee3e3946`.

Exact replay command is persisted in the report and was:

```bash
/home/dev/.codex/venvs/events-bot-new/bin/python \
  scripts/backfill_static_collection_facts.py \
  --db artifacts/db/static-collection-product-loop/prod-20260802T164757Z.sqlite \
  --evaluate --primary-only --current-date 2024-01-01 --reason audience \
  --event-id-file artifacts/static-collection-product-loop/gate-b-event-ids.json \
  --source-id-file artifacts/static-collection-product-loop/gate-b-source-ids.json \
  --limit 100 --max-sources-per-event 2 \
  --output artifacts/static-collection-product-loop/gate-b-primary-final.json
```

Primary report SHA-256:
`7de9f9151b600b4ef19c689862fecc1acad65addaabd1b7c00138053de68d04f`.
Evaluator report:
`artifacts/static-collection-product-loop/gate-b-evaluation.json`, SHA-256
`44939e63e087d33a3fe34ba34c3dd763a257297cbb112fda77639e6e21b375ff`.

Transport and integrity:

- 50 requested and exactly bound real EventSource rows;
- 50 logical calls / 50 physical Gemma sends, maximum one per source;
- requested/actual model: `gemma-4-31b-it` /
  `models/gemma-4-31b-it`;
- GPT-4o fallback calls 0;
- writes and logical DB changes 0;
- 46 strict-valid outputs, 4 provider/validator-deferred;
- every non-empty accepted evidence quote is an exact substring of its bound
  EventSource;
- corrected seed, index, source receipts, SQLite, report, repository SHA and
  boundary manifest are hash-bound by the committed evaluator.

Family-weighted Gate-B result:

| Fact | High/keep/sufficient recall | Confirmed hard negatives |
|---|---:|---:|
| `child_directed` | 11/12 = 0.917 | 0 |
| `family_suitable` | 8/9 = 0.889 | 0 |
| `joint_family_activity` | 1/1 = 1.000 | 0 |

Gate status is PASS, `copy_gates_allowed=true`, errors 0. Seven warnings remain:
six excluded borderline families and one non-blocking 7176 family boundary
disagreement. Publication remains blocked.

## 4. Fallback and failure evidence — partial Gate C

The committed provider-free drill
`scripts/run_static_collection_facts_v3_fallback_drill.py` covers:

- primary failure followed by one fallback;
- malformed fallback;
- exact-evidence mismatch;
- total provider unavailability while preserving accepted truth.

Artifact `fallback-offline.json` is PASS 4/4 with maximum one primary and one
fallback send per case, no DB apply on invalid output and no production
provider calls. SHA-256:
`d0806ebe5aa5cc139fef7bb7b8fedd28bfb93ccc5f769e569e3afe76d6b277a8`.

A separate real-provider drill used the existing GPT-4o fallback on three real
bound sources (events 6562, 6689 and 4648):

- artifact: `artifacts/static-collection-product-loop/fallback-real.json`;
- SHA-256: `8eafacebaa387ddec7436e2bb2245f0c1ff4c55f89a2b9b6e84a8424b913204a`;
- three actual GPT-4o calls after forced primary adapter failure;
- exact model path per case: `[models/gemma-4-31b-it, gpt-4o]`;
- maximum one primary and one fallback send per source;
- all three outputs passed the same strict validator;
- apply was in memory only and DB writes were 0.

The offline cases, rather than extra paid calls, cover malformed output and
complete unavailability. This is useful bounded evidence, but not a fully
reproducible Gate-C PASS: the real-provider driver remains an ignored local
artifact, its report has no generator command/driver hash, and malformed and
evidence-mismatch variants were not repeated against the real corpus. Gate C is
therefore PARTIAL, not a release gate.

## 5. Production-copy apply and warm — partial Gate D

All mutations were confined to
`artifacts/static-collection-product-loop/prod-copy-apply.sqlite`.

Initial bounded command selected 20 current/future rows. It made 20 Gemma sends,
applied 19 rows and deferred event 6797 at validation; only
`Event.collection_decisions` changed and no EventSource or unrelated event did.
Artifact `copy-apply-first.json` SHA-256:
`69c0623841f7729e6f5148cc8ae3ddc9d9eeb84eb037aa14826410a4c3ee593f`.

Because a deferred source cannot demonstrate a warm cache hit, event 6521 /
source 7466597 was applied as a separately disclosed replacement. It made one
Gemma send and one allowed write. Artifact `copy-apply-supplement.json`
SHA-256:
`0fc12fc9f1a7aad4e5df2f0864b40ad326ee80013ebe953416d6fd6b9427d4cb`.

The resulting effective 20-source cohort was repeated unchanged:

```bash
/home/dev/.codex/venvs/events-bot-new/bin/python \
  scripts/backfill_static_collection_facts.py \
  --db artifacts/static-collection-product-loop/prod-copy-apply.sqlite \
  --apply --primary-only --current-date 2026-08-02 --reason audience \
  --event-id-file artifacts/static-collection-product-loop/copy-warm-event-ids.json \
  --source-id-file artifacts/static-collection-product-loop/copy-warm-source-ids.json \
  --max-sources-per-event 1 \
  --output artifacts/static-collection-product-loop/copy-apply-warm.json
```

Warm result: 20 cached sources, provider calls 0, physical sends 0, writes 0,
changed event/source IDs empty, logical database SHA unchanged. Artifact
SHA-256:
`1ef04216ea0bfd695039dbf2a1288cea8bdeb46529861c11bb684398e90705cf`.

This proves bounded apply semantics and a warm no-op for the **effective**
cohort. It does not prove the mandatory exact-cohort sequence
`plan → evaluate → apply → identical warm`: no separate copy-plan/copy-evaluate
artifacts were saved, and the warm cohort substitutes 6521 for the initially
deferred 6797. Gate D is therefore PARTIAL even though the warm mechanics pass.

## 6. Product snapshot and #234 monitor — local PARTIAL/WATCH

One provider-free adapter was added at the existing static exporter /
StaticSiteBuilder boundary. It reads current/future facts-v3 decisions and
their EventSource evidence; it does not create another pipeline or invoke LLM/BGE.
The four outputs are `child_directed`, `family_suitable`,
`joint_family_activity` and the `kids` union. Occurrences are collapsed only by
the existing mutual family identity.

After first apply and after warm:

- adapter `input_fingerprint`:
  `330d57eaa3003b0014d61cf405d92369dbfbe3371facf934bc029265c462b3b6`;
- #234 `normalized_output_sha256`:
  `fc4fe807df56963afe04f77ee8d2aa5edb747d7e947bce760512276857b97f4e`;
- exact normalized output is therefore unchanged by warm;
- provider calls 0;
- monitor overall status WATCH, no FAIL.

Current bounded-copy counts:

| Collection | Families | Status |
|---|---:|---|
| child directed | 11 | HEALTHY |
| family suitable | 7 | HEALTHY |
| joint family activity | 0 | WATCH |
| kids union | 15 | HEALTHY |

The exact WATCH signals are `accepted_baseline_missing` and
`nonpublic_collection_empty` for joint activity. The snapshot stays
`shadow|experimental`; publication stays blocked.

This is local production-copy evidence. In GitHub the live `product` job is
still skipped and only the skeleton/gate jobs are green; no post-ingestion
snapshot exists. The product connection is therefore PARTIAL, not end-to-end
acceptance.

Artifacts:

- `product-after-first-apply.json` and
  `product-quality-after-first-apply.{json,md}`;
- `product-after-warm.json` and `product-quality-after-warm.{json,md}`;
- snapshot file SHA-256 differs only because `generated_at` is part of the full
  artifact, while the normalized product SHA above is identical by contract.

## 7. Ordinary-ingestion Gate E — honest blocker

The committed replay harness crosses the actual production entry points:

- Telegram: `process_telegram_results`;
- VK: `persist_event_and_pages`;
- official parser: `process_source_events`.

It explicitly disables publication/network-media/Telegraph/task side effects,
never calls `apply_collection_decisions` directly, records redacted provider,
write, receipt and quote evidence, and requires an identical warm pass.

### Why the requested full matrix cannot yet be reproduced

Production SQLite retains normalized Event/EventSource evidence, but not the
original Telegram extraction packet, VK `EventDraft` or parser
`TheatreEvent`. A read-only Fly `/data` inspection and current Kaggle output
history found no retained historical packets for the target corpus. Rebuilding
those objects from EventSource is not an exact replay of ordinary ingestion and
must not be reported as one.

Two bounded reconstructed probes were still run to test the harness:

1. Reconstructed Telegram event 6562 reaches the production handler but the
   captured run is rejected before collection adjudication by the existing
   location-grounding guard. The handler has normalized the source-grounded
   `Ленинский пр-кт 83` to `Музей Изобразительных искусств / Ленинский проспект
   83` without carrying the reference-match basis into Smart Update. Two
   semantic guard calls occur; collection calls/writes remain 0. The report's
   exact suffix `invalid:location_grounding_review:llm_keep` is itself not
   reproducible from committed `d5dce869` (a failed keep should be
   `llm_keep_not_grounded`), so it is also a runtime/import-provenance warning,
   not a production-code verdict. Artifact
   `ingestion-smoke-6562-report.json` is FAIL, SHA-256
   `fd289b5e9083ca07eeeace5994d1b3865af70653db2e52778a9c829653e71d20`.
2. Existing official-parser event 6822 is recognized as already existing and
   therefore never enters Smart Update/collection adjudication. Reprocessing
   also changes EventSource bookkeeping, so it is not warm-idempotent under the
   gate. Artifact `ingestion-parser-6822-report.json` is FAIL, SHA-256
   `fef57e056b13aee10790361c67443965f1a7992b1558c1738b417fe332e1bb46`.

After two similar location-review attempts, further prompt guessing was stopped
under the external-tool research gate. Provider documentation confirms that
schema-constrained output cannot guarantee a semantically correct verdict; a
local provenance contract is required. No broad regex, third retry or
production semantic bypass was added to make this acceptance green.

Gate E needs either freshly captured pre-import packets from Telegram/VK/parser
or an explicitly designed, reviewed capture/replay contract. The historical
Telegram source points to ops run `3212`, run id
`5a1f05a2ea9045d0aad6af976b45ab92`; its original packet was not present in DB,
Fly `/data` or the available Kaggle output history. A future, separately
reviewed location fix may carry a handler-owned attestation only for a unique
exact-address/curated-alias match, while preserving the ICAE, island and
programme-label regressions. A broad known-venue bypass or feature flag is not
acceptable, and changing that adjacent semantic guard in this PR would be
scope-unsafe.

Because Gate E is red, no post-ingestion product snapshot is claimed.

## 8. Release boundary and next action

No production migration, backfill or deploy was run. As of this report:

- #207, #222, #233 and #234 are open;
- at the final fetch `origin/main` was `a2b0fa92…`; this branch was 114 commits
  behind and 41 ahead, and does not contain verified code SHA `c625f180…` in
  main history;
- release governance forbids Fly mutation/deploy from this side branch;
- the requested clean main-based integration branch is intentionally deferred
  until Gate E has real packets and passes.

The integration worktree and relevant task lanes are clean. The wider local
repository has many unrelated dirty/stale worktrees and nine remote
`hotfix/*` branches ahead of main, so any later production integration must use
a new clean isolated worktree and repeat the release-governance branch audit.

The smallest next step is not PR B and not Astro. Capture one fresh packet per
real Telegram, VK and official-parser path before normalization/persistence,
run it through the committed harness on a fresh production copy, then expand to
the required child/family/joint/boundary matrix. Only after that gate is PASS:

1. merge #207;
2. rebase/retarget and merge corrected #222;
3. audit and merge facts/product changes from #233/#234 through a clean branch
   based on current `main`;
4. run bounded Fly evaluate/apply/warm from the exact main-reachable SHA.

Until then the precise overall verdict is:

```text
FACTS_V3_CODE              PASS
PRIMARY_REAL_DATA          PASS
FALLBACK_FAILURE_DRILL     PARTIAL
PRODUCTION_COPY_APPLY      PARTIAL
PRODUCTION_COPY_WARM       PARTIAL (mechanics pass; exact sequence absent)
PRODUCT_SNAPSHOT_MONITOR   PARTIAL_WITH_WATCH
REAL_POST_SMART_UPDATE     BLOCKED
BOUNDED_LIVE_FLY           BLOCKED
PUBLICATION                BLOCKED
```
