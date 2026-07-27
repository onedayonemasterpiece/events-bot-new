# R15 execution matrix — unusual/static-site product integration

## Identity and release state

- Original fanout base: `origin/main@551941bf9fc6ec3a647a0801fc704410cfc42761`
- Superseded canary SHA: `11d8c9846432414020cc5201eb650f5cfbf38eba`
- Docs-hardening base: `b9560c111240b1ba46c4291c75402e8c526e0d0d`
- Integration branch: `integration/unusual-static-site-r15-20260727`
- Final exact integration SHA: **to be filled by integrator**
- Final Kaggle run ID: **to be filled by integrator**
- Final immutable candidate URL: **to be filled by integrator**
- Production-root decision: **NO-GO until every final gate below is filled**

The `11d8c984` CPU canary did run. It produced useful real pinned-BGE,
`provider_calls=0`, vector/cache/hash and service-share evidence, so it must not
be described as “never run”. It is nevertheless superseded: it predates the
ordinary-corpus/structured-eligibility hardening, durable notification fix,
unified rail geometry, guaranteed daily enqueue and legacy-preview pre-gate.
Its candidate Playwright incorrectly depended on stripped lab routes, and the
public upload/HTTP acceptance did not complete. It is not final release
evidence.

## Requirement matrix

| ID | Requirement | Integrated contract | Current status | Final evidence still required |
|---|---|---|---|---|
| R01 | Every crop-safe `visual_only` mobile-rail photo is horizontal `140×112` (`5:4`) cover; no bands | One rule for portrait/landscape and single/multi-image galleries. OCR/document/unknown/error, contradictory or unreviewed media fail closed to authored `contain`. | Implementation complete; focused tests and local real portrait gallery passed. | Rebuild and measure the exact final candidate, including event `5297` and multi-image portrait event `6823`. |
| R02 | Free collection keeps large right medallion and compact sticky identity | Materialized Free route; Free remains top-level and inside Collections. | Implementation complete. | Exact final candidate product Playwright. |
| R03 | Service-share refreshes at least once per Kaliningrad local day | Existing sole `static_site_calendar_rollover` at `00:00 Europe/Kaliningrad`, startup catch-up, normal Smart Update coverage, atomic outbox local-day marker and coalesced builder row. | Implementation complete; 62-test builder/release suite passed. | Exact final Kaggle result with current-day service-share manifest/hash/dimensions and preview pre-gate receipt. |
| R04 | Noindex Favorites is future-only, calendar-first, deduplicated and shared-auth/offline safe | Static skeleton plus shared auth; owner-scoped saved-event storage/RLS/RPC. | Implementation complete; production owner/auth acceptance remains release evidence. | Exact final candidate product smoke plus live owner-scoped Yandex/Supabase postflight if production acceptance is requested. |
| R05 | Calendar reaches the furthest public event month; empty dates are inert | Generated availability horizon; empty cells are non-anchor spans with `aria-disabled=true`. | Implementation complete. | Exact final candidate product Playwright. |
| R06 | Home has hero, quick routes and ≤30 useful static cards with local-only rerank | Useful SSR fallback; no page-view model/provider request. | Implementation complete. | Exact final candidate product Playwright. |
| R07 | Collections submenu contains Children/Unusual/Free/Clubs; Free also top-level | One mobile drawer and coherent vendored icons. | Implementation complete. | Exact final candidate product Playwright and overflow check. |
| R08 | Shared-BGE unusual semantics, hard eligibility, ordinary distance, dedup/diversity and hash-bound gates | One event vector supports related/unusual/family/concept; explicit `canonical-event-semantic-v1` fields fail closed; deterministic ordinary-corpus distance/receipt uses the same vectors; deferred rows cannot bypass diversity caps. | Final hardening must be present in the exact integration SHA; old `11d8` metrics are superseded. | Focused Python/source tests and final Kaggle semantic report with model/policy/classifier/artifact/ordinary-corpus hashes, approved metrics and `provider_calls=0`. |
| R09 | Coalesced builder, cache/atomic/last-good, observability and preview/root/candidate gates | Production-candidate first runs ephemeral `build:preview` + `check:preview`, then production-root and secret-candidate build/check/browser gates. Preview output is neither archived nor published and must be cleared before root archive. | Implementation complete; exact final runtime proof pending. | Status-ledger heartbeat/result, downloaded vector/cache/last-good/manifests, preview-contract receipt and both browser-release reports. |
| R10 | Static `/neobychnoe/`, stable concept/red-dot state, no labs in package, no root cutover | `notify_eligible` survives ordinary rebuilds; migration output is false without erasing durable state. Product Playwright and local lab matrix use independent modes/bases; packaged lab route returns `404`. | Implementation complete after final semantic merge; public acceptance pending. | Final product-mode candidate pass, separate local lab-mode ten-state pass, complete upload/HTTP checks and owner decision. |

## Exact final validation order

1. Merge the final semantic hardening and this docs lane; confirm a clean
   integration worktree and record the exact SHA above.
2. Run focused Python/Node/source suites. Required semantic evidence includes:
   explicit canonical eligibility projection, ordinary-corpus policy/corpus
   receipts, cache invalidation on corpus change, notification persistence,
   migration suppression and zero provider calls.
3. Run local Playwright as two independent contracts:
   - `UNUSUAL_EVENTS_PLAYWRIGHT_MODE=product` against the packaged immutable
     candidate base only;
   - `UNUSUAL_EVENTS_PLAYWRIGHT_MODE=lab` against a separately served local
     noindex lab base only.
4. Start one exact-SHA `production-candidate` Kaggle run. It must execute in
   order: legacy `build:preview`/`check:preview` pre-gate; root build/check and
   Chromium browser gate; secret-candidate build/check and Chromium browser
   gate. The preview pre-gate must report `archived=false`, `published=false`.
5. Download and verify all declared hashes/artifacts, complete the immutable
   upload, check candidate/product HTTP `200`, and confirm the packaged lab path
   returns `404`.
6. Fill final SHA, run ID, candidate URL, semantic/cache/service-share hashes and
   pass/fail receipts in this matrix. Do not infer them from the superseded
   canary.
7. Obtain explicit owner acceptance before any production-root promotion.

## Final evidence ledger

| Evidence | Required value | Final value |
|---|---|---|
| Integration SHA | clean exact commit containing final semantic, rail, daily/preview and docs hardening | **to be filled by integrator** |
| Kaggle run | terminal success for that exact SHA with heartbeat/status history | **to be filled by integrator** |
| Legacy preview pre-gate | `status=ok`, `archived=false`, `published=false`; build/check passed | **to be filled by integrator** |
| Semantic contract | pinned BGE hashes, explicit eligibility, ordinary policy/corpus hashes, approved metrics, `provider_calls=0` | **to be filled by integrator** |
| Durable state | notification survives ordinary rebuild; migration manifest false without cache erasure | **to be filled by integrator** |
| Service-share | local date, `1080×1350`, PNG/WebP/manifest hashes, daily marker evidence | **to be filled by integrator** |
| Root browser gate | generated root check and Chromium report pass | **to be filled by integrator** |
| Candidate browser gate | generated noindex candidate check and Chromium report pass | **to be filled by integrator** |
| Product Playwright | product mode passed without visiting lab | **to be filled by integrator** |
| Lab Playwright | separate local ten-state matrix passed | **to be filled by integrator** |
| Public candidate | complete upload; required product URLs `200`; lab path `404` | **to be filled by integrator** |
| Owner/root decision | explicit acceptance; otherwise remain candidate-only/noindex | **to be filled by integrator** |
