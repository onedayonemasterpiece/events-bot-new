# Static-collection facts v3 — integration report

Date: 2026-08-02 UTC

PR-A head: `d9112bd3a547bd8592d42bf3332b5bf69e0fe3e8` (draft PR #222)

Verified extraction/product code branch: `integration/static-collection-facts-v3`

Acceptance-harness head before this report: `d5dce869f4a566f78322093bfc931dcfc8a78c26`

Integration branch / draft PR: `integration/static-collection-facts-v3` / #233.

## Executive result

The corrected source truth and real primary-only Gate B are verified. Gate D
now has the requested fixed-cohort `plan → apply → identical warm` evidence.
The bounded capture contract and in-builder product monitor are implemented,
but fresh ordinary ingestion is still **BLOCKED** by external/adjacent runtime
conditions. This stack is therefore not ready for PR-stack merge or Fly canary.

| Gate | Result | Evidence |
|---|---|---|
| Corrected provisional PR A | PASS | review gate PASS, 0 errors/11 supply warnings; strict still fails only because PR-B artifacts do not exist |
| Facts-v3 deterministic suite | PASS | 202 focused tests on the integrated worktree; GitHub facts-v3 and product workflows green on `c625f180` |
| Fresh production snapshot | PASS | 6,998 events, `quick_check=ok`, SHA below |
| Full corrected Gate B | **PASS** | 50 real EventSource rows, 50 Gemma sends, no GPT-4o, no writes, exact binding/quotes; all recalls above 0.80 |
| Fallback/failure drill | PARTIAL | committed offline 4/4 fail-closed drill plus 3 real valid-fallback cases, but the paid-driver is a volatile artifact and malformed/evidence mismatch were not repeated on real sources |
| Production-copy apply/warm | **PASS** | fresh snapshot; fixed 20 exact Event/EventSource bindings from plan onward; first apply 20/20 and identical warm has 0 provider calls/sends/writes and no changed event/source IDs |
| Product snapshot + monitor | PARTIAL/WATCH | StaticSiteBuilder now runs the existing monitor immediately after snapshot generation and durably emits JSON/Markdown/qa-summary; focused tests pass. A completed source-faithful post-ingestion first/warm pair and real builder canary are still absent |
| Ordinary Telegram/VK/parser ingestion | **BLOCKED WITH FRESH CAPTURES** | parser and VK source-faithful packets are captured. Parser first passes but warm rewrites `EventSource.imported_at`; VK replay is blocked before mutation by shared RPD exhaustion; current Telegram Kaggle run has not completed |
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

## 5. Production-copy apply and warm — Gate D PASS

The repeat used a newly downloaded production snapshot, never the previous
mutated copy:

- SQLite: 317,116,416 bytes, 6,998 events, `quick_check=ok`;
- snapshot SHA-256:
  `22790cd3284ca507502be324b93c23812e71f631e2278dcb1f8d6c35d80afc99`;
- fixed 20-event list SHA-256:
  `7e85a72a00d41791b711ef59e8b3dd0c56b8655974c686b0974b945df1a081f5`;
- fixed 20-source list SHA-256:
  `ef5e911d5eda98d9dc243ddfb0503f151e94c33b792857c9e7a0a734ac4dbecd`.

Event 6797 was excluded before plan and its strict-valid replacement 6521 was
already in the immutable initial list. Gate B is the accepted read-only
evaluate evidence, so no duplicate paid evaluate was run.

Results:

| Step | Artifact SHA-256 | Result |
|---|---|---|
| plan | `806fb00e77d21b2c8706824d6f5959769b55d0bf5b3032e7a41a51e1a87c8ce6` | 20 resolved, 0 unresolved, no diff, DB hashes equal |
| first apply | `ed80f5aeddfc0fe3bf590484ffefcb69f85c99598b0949c4ffcd8bbb852d1c83` | 20 attempts/sends/applies/writes; only selected `Event.collection_decisions`; no EventSource changes |
| identical warm | `2c28ec36139ba8d59917d31683f5838e620dd25586df9c16d95e4344103f38f8` | 20 cached; provider calls 0; physical sends 0; writes 0; changed event/source IDs empty |

The warm logical SHA is unchanged before/after. This is the deliberately
shortened Gate-D contract requested for this iteration and is **PASS**.

## 6. Product snapshot and in-builder monitor — PARTIAL/WATCH

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

StaticSiteBuilder now invokes the existing product-quality checker immediately
after validating/copying its generated snapshot. It writes beside the durable
snapshot:

- `static-collections-product-quality.json`;
- `static-collections-product-quality.md`;
- `qa-summary.json`.

`WATCH` is non-blocking; `FAIL` is written as evidence and then blocks the
build. The outer Kaggle runner hash-validates and persists all three artifacts.
Combined capture/replay/builder focused verification passes 53 builder tests
and 22 capture/replay tests. A real Kaggle preview at repo SHA
`6365395ebe6d085aa76dc776f7d91324ee851a9a` then proved the in-builder
connection before an unrelated later check:

- build id: `preview-20260802-collections-product-monitor-shadow-v3`;
- product snapshot SHA-256:
  `95a384094cb0394d5f911ab17860745a591fcc3de943c501a806a34d4c355bf4`;
- product quality JSON/MD SHA-256: `7cafb02f…` / `9cc79137…`;
- `qa-summary.json` SHA-256: `bba46e1e…`;
- product status `WATCH`, QA outcome `PASS`, fail issues 0, provider calls 0;
- normalized output SHA-256:
  `0ac8638e26a5721f48178225094b426ea93e6fc238d10b5614c3d3548adf46fc`.

The overall preview later failed `npm run check:preview` because the bounded
50-event slice omits unrelated mobile rail canary event 4211. The product
artifacts were already written and downloaded. Per scope, mobile routes/checks
were not changed and no third build was launched merely to bypass that canary.
Blocker artifact SHA-256: `af1a04353f864bd31c76153ec21db05b2eb65fddb70420c684671cc7ece9c95f`.

The GitHub live-product job remains off on PR/schedule because it still does not
receive a generated builder artifact; the skeleton is not claimed as live
monitoring. A completed post-ingestion first/warm pair remains required.

Artifacts:

- `product-after-first-apply.json` and
  `product-quality-after-first-apply.{json,md}`;
- `product-after-warm.json` and `product-quality-after-warm.{json,md}`;
- snapshot file SHA-256 differs only because `generated_at` is part of the full
  artifact, while the normalized product SHA above is identical by contract.

## 7. Ordinary-ingestion Gate E — fresh capture implemented, replay blocked

The replay harness crosses the actual production entry points:

- Telegram: `process_telegram_results`;
- VK: `persist_event_and_pages`;
- official parser: `process_source_events`.

It disables publication/network enrichment/Telegraph/task side effects, never
calls `apply_collection_decisions` directly, records redacted provider/write/
receipt/quote evidence and requires an identical warm pass. A new manual pure
capture helper adds the missing source-faithful boundary without a table or
service. Its closed `static-collection-upstream-capture-v1` contract stores one
packet, exact production handler, repo SHA, actual capture time, public source
binding, canonical payload SHA and sanitized binary omissions. It refuses
credentials, `/data`, overwrite and cross-message Telegram dependencies.

Current source-faithful evidence:

1. **Official parser captured.** A live Yantar Hall catalog fetch produced the
   exact `TheatreEvent` for event 7127 / 2026-08-03. Capture artifact SHA-256:
   `61dc05ecba82c367346b788dadb20a92d24d34550451d1af57b70c2784c29192`.
   The first ordinary `process_source_events` pass is PASS with zero collection
   calls/writes. The identical warm pass is FAIL only because the existing
   exact-parser refresh rewrites source 9573658 `imported_at`; changed event IDs
   stay empty and collection calls/writes stay zero. All 65 Yantar Hall, 22
   Estrada and current theatre/philharmonia/Qtickets future packets matched
   existing rows, so no naturally new official occurrence was available.
   Reproducible blocker artifact SHA-256:
   `87dca826b765f28c48312b15d34911b47f715eb7ece40dd8006d230362074e4a`.
2. **VK captured.** A current pending production inbox post was fetched through
   `_process_vk_inbox_row`, OCR/extracted on a disposable copy and captured at
   the exact pre-`persist_event_and_pages` boundary. Source:
   `https://vk.com/wall-149955604_23881`; capture artifact SHA-256:
   `8fc494822c12c77b24224c36ad312c3d8a96797b214126a042d20a66e0b3551c`.
   Replay reached the existing occurrence-scope review, but the shared limiter
   returned `Rate limit exceeded: rpd`; Smart Update failed closed with
   `occurrence_scope_review:llm_unavailable`. Event/EventSource counts and the
   full logical DB SHA remained identical. No bypass/model/prompt change or
   second paid retry was made. Blocker artifact SHA-256:
   `064d22d0782a9b61fc75585a31c5c3ad3f30f5b88411f2fa78937cf5e96827ca`.
3. **Telegram pending.** The authoritative kernel ref is
   `zigomaro/telegram-monitor-bot`; it is still `RUNNING`. No competing run was
   started and the S22 remote auth boundary was not touched. Capture waits for
   the latest completed genuine schema-v2 output; `run_tg_monitor.py` is not
   used because it mutates/imports.

The replay CLI can now optionally emit a product snapshot and quality JSON/MD
after each first/warm pass and fails if normalized output changes or monitor
status is `FAIL`. Adapter exceptions are hashed/redacted, and every failed
first-pass receipt stops the warm retry, preventing an accidental second
provider charge. No source-faithful path has a
complete first+warm PASS yet, so Gate E remains blocked and no release merge is
allowed.

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

The smallest next step is not PR B and not Astro. Finish/download the current
Telegram output, capture one valid message, replay it, retry the immutable VK
capture only after the shared RPD window is available, and retry the parser
capture when a naturally new official occurrence exists. Only after all three
first+warm paths pass:

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
PRODUCTION_COPY_APPLY      PASS
PRODUCTION_COPY_WARM       PASS
PRODUCT_SNAPSHOT_MONITOR   PARTIAL_WITH_WATCH
REAL_POST_SMART_UPDATE     BLOCKED_WITH_FRESH_CAPTURES
BOUNDED_LIVE_FLY           BLOCKED
PUBLICATION                BLOCKED
```
