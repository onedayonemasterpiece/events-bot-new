# Audience collections clean-main integration report

Date: 2026-08-03
Branch: `integration/static-collections-audience-shadow-main-20260803`
Base at integration start: `origin/main@d5e58731a12b378a19bb9f175459b5b3cc153b7a`
Rebased review base: `origin/main@81833f0156321fee90ab467fd0c4069de25309bf`
Scope: data preparation and shadow acceptance only. PR B, owner gold, scores,
thresholds, Astro routes, navigation, sitemap and semantic publication remain
out of scope and blocked.

Merged delivery: PR #299, `origin/main@f44f7fc66ce6f833b7412796de1fb36f53cacec0`.
Production delivery: Fly release `1894`, image digest
`sha256:1fb79eb04f714429d34529205bd5f171b8719af279d06f42f9539412ac07807b`.

## Result

The source-grounded audience facts-v3 contour was selectively reapplied to
current main. Current main remains authoritative for the Google AI limiter,
project scopes, fallback routing and provider-429 behavior. No older limiter
implementation from the stacked collection branches was imported.

Post-fix Gate E is accepted as `PASS_WITH_NON_COLLECTION_DRIFT`:

- Telegram `kulturnaya_chaika/8140`: first PASS; warm collection calls/sends/
  writes `0/0/0`; exact event/source binding stable; collection-decision hash
  stable; product normalized SHA stable and product `WATCH`. Warm-only prose and
  derived age metadata drift is explicitly separated from the collection plane.
- VK `wall-149955604_23881`: first PASS; the previously blocked
  `occurrence_scope_review` and `location_grounding_review` both completed on
  Gemini 3.1 Flash Lite; warm collection calls/sends/writes `0/0/0`; exact
  binding, collection-decision hash and product normalized SHA are stable.
  Warm-only prose drift is separated from the collection plane.
- Yantar Hall parser packet: first PASS; warm
  `PASS_WITH_OPERATIONAL_METADATA`; only `EventSource.imported_at` changed.

The raw replay reports were not rewritten. The derived acceptance receipt binds
their hashes and the narrow non-collection allowlist. General Smart Update warm
prose/idempotency drift is tracked separately in GitHub issue #297.

The full current/future audience universe is accounted, but supply quality is
not yet healthy:

- current/future exported catalog: `377` events;
- audience-routed candidates: `62` with exact primary source bindings;
- evaluated facts-v3 events: `2` (`2781`, `6826`);
- explicit provider/validation deferred: `60`;
- unprocessed: `0`;
- physical provider sends: `2` across `62` logical attempts;
- evaluated rows were `unknown`, so current child/family/joint/kids family
  counts are all `0`;
- provider-free product monitor: `WATCH`, never `FAIL`;
- semantic publication: `BLOCKED`.

The large deferred set is not hidden as success: shared per-project/model TPM
admission allowed two physical Gemma sends and rejected the rest before send.
The coverage contract records every deferred event and no unprocessed event.
This is sufficient as a shadow coverage/accounting proof, not as a healthy
population or publication proof.

## Limiter invariants preserved

Verified in the integrated tree and tests:

- limiter contract `google_ai_project_model_atomic_v1`;
- bucket strategy `rolling_60s_pacific_day_v2`;
- six configured keys map to six distinct project quota scopes;
- provider 429 exclusion/cooldown is quota-scope-aware;
- Smart Update facts stages retain the bounded
  `gemini-3.1-flash-lite -> gemini-3.5-flash-lite -> gemma-4-31b-it` route;
- `collection_candidate_adjudication` keeps Gemma 4 as primary and is limited
  to one physical send for the logical call;
- collection replay instrumentation is an observer/cap on the shared client,
  not a direct-provider bypass.

## Kaggle / StaticSiteBuilder evidence

The first exhaustive preview slice exported all `377` current/future events,
computed the BGE collection batch with `0` LLM provider calls and ran the
collection product monitor as `WATCH`. It then exposed an unrelated packaging
integration regression: current preview scripts require the repo-level
`docs/testing/transport-fault-profiles.v1.yml`, but the Kaggle source tarball
previously contained only `site/`. The build stopped with exact `ENOENT` before
Astro compilation.

The runner now bundles that exact non-secret repo contract and has a regression
test. The next run completed Astro but exposed a second packaging-only issue:
the extracted Kaggle source archive has no `.git`, so preview provenance could
not resolve a full SHA. The runner now binds its exact full SHA into the input
config and the kernel injects it as `STATIC_SITE_REPO_SHA`. The third actual
kernel run is the successful acceptance run recorded below.

- first build id: `preview-20260803-static-collections-full-shadow-post-limiter`;
- first input dataset: `zigomaro/static-site-builder-input-20260803104751-7b7c3e`;
- first exported event count: `377` (therefore exhaustive despite preview/slice
  profile and limit `1000`);
- first BGE batch SHA: `557d5d05c9e5f4bab6b7657dff656e3c2ea7d1d51077166a4e4c4653dfb43d18`;
- first BGE collection compute: PASS, `additional_external_requests=0`,
  `supabase_core_reads=0`, semantic publication blocked;
- first product normalized SHA:
  `0ac8638e26a5721f48178225094b426ea93e6fc238d10b5614c3d3548adf46fc`;
- first terminal status: `ERROR` only because the transport-fault contract was
  absent from the source bundle;
- second build id: `preview-20260803-static-collections-full-shadow-post-limiter-v2`;
- second input dataset: `zigomaro/static-site-builder-input-20260803110021-2651c4`;
- second terminal status: `ERROR` only at preview provenance because the archive
  intentionally has no `.git`; Astro itself completed;
- accepted build id:
  `preview-20260803-static-collections-full-shadow-post-limiter-v3b`;
- accepted input dataset:
  `zigomaro/static-site-builder-input-20260803111804-195187`;
- accepted runner/source SHA:
  `4bacc055c33c87166a5522ffab54e22a54ddf9c3`;
- accepted kernel status: `COMPLETE`, result `ok=true`, `event_count=377`;
- archive SHA: `5bcffc36ad27a39132fa423f938709873668d0185ba131d5f0dce9b42c1fd456`;
- archive verification: required preview/today/tomorrow/weekend/robots/sitemap
  entries present and `377` discovery documents;
- semantic compute: `provider_calls=0`, collection batch contract
  `876dfe7762198db50f17ad4cfd0048ab82308dab1b6dbb8eedde270e53fc02c6`;
- product monitor: `WATCH`, QA `PASS`, `provider_calls=0`, normalized SHA
  `0ac8638e26a5721f48178225094b426ea93e6fc238d10b5614c3d3548adf46fc`;
- hash-bound local acceptance receipt:
  `full-shadow/kaggle-v3b-output/acceptance.json` (`7d55f700…`).

The Kaggle product artifact intentionally sees coverage as `unknown`, because
the manual build input contains the DB/BGE caches rather than the separate local
coverage receipt. The canonical local product snapshot binds the complete
`62 = 2 + 60 + 0` coverage receipt; both outputs have the same normalized
visible SHA. This is reported explicitly rather than presenting remote coverage
as independently embedded.

This manual shadow run intentionally has no production callback/status dataset.
Production ledger/lease assertions belong to the bounded post-merge Fly canary.

## Post-merge Fly canary

The collection data plane passed its bounded production apply/warm acceptance
on six fresh exact event/source bindings (`7283`, `7344`, `7402`, `7404`,
`7411`, `7415`):

- apply: `6` attempted, `6` applied, `0` deferred, `6` provider calls,
  `6` physical sends and `6` writes;
- identical warm: `6` cached, `0` provider calls, `0` physical sends,
  `0` writes and an empty Event/EventSource logical diff;
- the six calls used six distinct API-key IDs and six distinct project quota
  scopes under `google_ai_project_model_atomic_v1` and
  `rolling_60s_pacific_day_v2`; all six requests/attempts reached terminal
  `succeeded`, were finalized and recorded usage;
- event `7402` was confirmed for both `child_directed` and
  `family_suitable` with exact source quotes. The bounded product snapshot has
  one child family, one family-suitable family and one `kids` union family;
  `joint_family_activity` remains empty;
- provider-free product quality is `WATCH`, never `FAIL`, with normalized SHA
  `b91da9d559bbc3699a6958a36df572904adb022b3bdb012835167106a594dc05`;
  semantic publication remains `BLOCKED` for the explicit missing-PR-B reason.

The production StaticSiteBuilder then consumed the exact main SHA and snapshot
`7bfd8f4714cef345293289a1411c004518686faa5a2a0d3a470b5b4849240074`.
It completed preflight, the legacy preview contract, the `377`-event production
root-form build, shared BGE compute and the collection product monitor:

- BGE batch compute: PASS, provider calls `0`, additional external requests
  `0`, Supabase core reads `0`;
- product QA: PASS, product `WATCH`, the same normalized product SHA and the
  same `1/1/0/1` audience family counts;
- vector barrier `ops_run=5159`: complete, `377` events, `754` unchanged
  embedding skips, provider calls `0`;
- status ledger emitted `preflight_ok` and terminal `report_written`; the exact
  `static_site:builder` lease was released at
  `2026-08-03T12:41:03.197854+00:00`.

The overall builder result is deliberately **not** recorded as success. Its
production-root Chromium gate stopped on the already reproduced, unrelated
event `6407` defect: `card 6407 image escapes its shell` (issue #300). This is
the same baseline failure recorded by PR #299 CI and was explicitly excluded
from this collection branch. Therefore no secret candidate receipt advanced,
no candidate was published and no root promotion occurred. Repeating the same
deterministic build without fixing #300 would add cost but no evidence.

Runtime regressions remained green throughout and after the run: SQLite
`quick_check=ok`, Fly machine check `1/1` passing, five external health probes
returned `200` in `0.140-0.223 s`, webhook traffic returned `200`, and the
post-deploy window contains no no-space, database-full or Fly proxy candidate
errors.

## Artifact ledger

Durable local base (not committed):
`artifacts/codex/static-collections-audience-main-integration/`.

| Artifact | SHA-256 |
|---|---|
| `post-limiter/gate-e-acceptance.json` | `1b0d9ca93722e8093f3a1bb3982781702c1c317f2205e80beb5bac63676d4fcc` |
| `post-limiter/telegram-report.json` | `00b44255b26c32458cf45463c4de46d1580ebc58951ad328e01ce019b0fb4163` |
| `post-limiter/vk-report.json` | `7b0eac19e777f002896734dee6fdd9a3c296abe5b9bb252b80ac4026dbaafa01` |
| `post-limiter/parser-report.json` | `0f43989616be0f9fe454461fbc963706267689daaecde79141e7c10511f26dd7` |
| `full-shadow/coverage-before.json` | `1e49fadb54937f96bc779a60bd7412b20b9d340f981e931c725b73ca73a591f7` |
| `full-shadow/coverage-after.json` | `d432169a4741dbcdb89d43b037c27702b4acd7c51d5f0ff8db548c35a85cac6b` |
| `full-shadow/deferred-event-ids.json` | `8fe3f581570800bcde24360b79bc33248f4c40582f9a5820797c29319ce16e47` |
| `full-shadow/applied-warm.json` | `89d214bfdfaabd189f6d76add5405fa18da7d0870e98559eb8643408fd247d0c` |
| `full-shadow/product-snapshot.json` | `5448f87c8a5ebc3ff30766730c68c634e497912a7ef2aecc6b826713ebda5565` |
| `full-shadow/product-quality.json` | `d9b29caeedf49ccec0b4dc51f1303abc5797a125d7c1bebf59d7a77a00697a59` |
| `full-shadow/product-quality.md` | `77f81a390a5ae520c167afcd7f617665493c9705c0fff1de2eb8fcf642df2dac` |
| `full-shadow/kaggle-kernel.log` (first run) | `b18df95d02c402eee54a0d13299a635a589915c5fc32d71853c5d27bfdcc8da1` |
| `full-shadow/kaggle-error-output/collection-batch-v1.json` | `c86432bf1dc7ebff715e6c54a2c6741861ec7049f3b13a2aa1c58fc6ef29f949` |
| `full-shadow/kaggle-kernel-v2.log` | `ba54fe2e33d895bbf987656fbd0c71cf8f3aa867f16a085d5a7e3a312300b434` |
| `full-shadow/kaggle-v3b-output/static_site_build_result.json` | `8d2461a40eb9e18d7d06029dbedd015b9ea3cbce20c6e7e5da11f3b616ecf907` |
| `full-shadow/kaggle-v3b-output/acceptance.json` | `7d55f7008947bf04173a211961ba6f2632635213de6fdd2fdfd0f169de86c89f` |
| `full-shadow/kaggle-v3b-output/preview-…-v3b.tar.gz` | `5bcffc36ad27a39132fa423f938709873668d0185ba131d5f0dce9b42c1fd456` |
| `full-shadow/kaggle-v3b-output/collection-batch-v1.json` | `bfc7b32a94557727645b435229c8a1492447477ef648440e1e77407e55b0330a` |
| `full-shadow/kaggle-v3b-output/static-collection-product-snapshot-v1.json` | `8cb194a57e530c7e1caeae31e9db9186d8ac5387fee8d3cd6bd414464f0cffa5` |
| `full-shadow/kaggle-v3b-output/static-collections-product-quality.json` | `bd5383329fd085623063951cdfc8eaaff069cd544345ca5b90c395b23e11dfeb` |
| `full-shadow/kaggle-v3b-output/qa-summary.json` | `cdba34136b8718620a1a98ea9292e2360d153b5cf356de8911dfd8a1ddb501c8` |
| `fly-canary/fly-canary-acceptance-summary.json` | `e8799e824e18d5c08264d0d95f8304675c7e8800d5162999a63cd62927a5e7a3` |

Disposable full-shadow SQLite SHA-256:
`977386ab730887bc30ee8c74b07c0a8eb6161a507083a851f6a3fe5d2ce119f5`.
It is a production-snapshot copy with only two facts-v3 shadow writes and is not
a production migration artifact.

## Deterministic verification

- exact CI facts/merge/client/backfill suite: `145 passed`;
- additional audience/replay/product/validator suite: `129 passed`;
- limiter/project-scope/fallback/notebook contract suite: `148 passed`;
- Node collection contract tests: `5 passed`;
- StaticSiteBuilder handoff suite: `38 passed`;
- full local Astro preview on the shadow data: `458` pages, `check:preview` PASS;
- review gate: PASS, `0` errors, `11` warnings; warnings are expected supply and
  provisional artifact-binding shortfalls;
- strict gate: expected FAIL because PR-B owner gold, scores, winners,
  thresholds and frozen artifacts are intentionally absent;
- `git diff --check`: PASS;
- Python compile for runner/kernel: PASS.

## Requirement disposition

| Requirement | Status | Evidence |
|---|---|---|
| Clean integration from current main | Done | selective commits; no wholesale limiter replacement |
| Preserve current limiter and model routing | Done | invariant review + 112 tests |
| Do not repeat paid Gate B | Done | prior 50-source evidence reused; semantics unchanged |
| Repeat immutable Telegram/VK/parser Gate E | Done | hash-bound acceptance receipt; issue #297 for unrelated drift |
| Full current/future shadow coverage | Done with quality warning | 62 candidates = 2 evaluated + 60 deferred + 0 unprocessed |
| StaticSiteBuilder product monitor | Done with quality warning | Kaggle `COMPLETE`, 377 events, BGE/QA PASS, product WATCH, 0 provider calls |
| Clean PR and merge | Done | PR #299 merged as `f44f7fc66`; Fly release 1894 |
| Bounded Fly collection apply/warm | Done | 6/6 apply; identical warm 0 calls/sends/writes; six independent scopes |
| Production StaticSiteBuilder collection/product plane | Done | exact main SHA; 377 events; BGE/QA PASS; product WATCH; 0 provider calls |
| Checked secret candidate | Blocked outside collection scope | terminal browser gate reproduced issue #300 on event 6407; no receipt/promotion |
| PR B / owner gold / scores / thresholds | Not started by design | excluded |
| Astro routes/navigation/sitemap/publication | Not started by design | publication blocked |

## Integration lane closure

Read-only mapping lanes and the serial integration are complete. Collection
code, Gate E, full shadow accounting, merge/deploy and bounded production data
acceptance are delivered. The remaining checked-candidate blocker is issue
#300, not collection extraction. PR B and UI/publication remain separate work.
