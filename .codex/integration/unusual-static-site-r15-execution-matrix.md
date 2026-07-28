# R15 execution matrix — unusual/static-site product integration

## Identity and release state

- Original fanout base: `origin/main@551941bf9fc6ec3a647a0801fc704410cfc42761`
- Superseded canary SHA: `11d8c9846432414020cc5201eb650f5cfbf38eba`
- Docs-hardening base: `b9560c111240b1ba46c4291c75402e8c526e0d0d`
- Integration branch: `integration/unusual-static-site-r15-20260727`
- Final exact code SHA: `123bcee460112ee9fe0b0a0176f51a07c92eed6a`
- Final Kaggle run ID:
  `static-site-builder:r15-bge-final5-20260727T221000Z`
- Final immutable candidate URL:
  <https://kenigevents.ru/_review/pp1wRctXBd6boYU1EcnBrod3z8MmKpD7SGEufK1t-xw/>
- Production-root decision: **NO-GO pending explicit owner acceptance**

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
| R01 | Every crop-safe `visual_only` mobile-rail photo is horizontal `140×112` (`5:4`) cover; no bands | One rule for portrait/landscape and single/multi-image galleries. OCR/document/unknown/error, contradictory or unreviewed media fail closed to authored `contain`. | **Done:** exact generated gate and browser evidence passed; event `5297` has two gallery images in the 5:4 `cover` surface. | Owner visual acceptance only. |
| R02 | Free collection keeps large right medallion and compact sticky identity | Materialized Free route; Free remains top-level and inside Collections. | **Done:** exact product Playwright and public desktop/mobile QA passed. | Owner visual acceptance only. |
| R03 | Service-share refreshes at least once per Kaliningrad local day | Existing sole `static_site_calendar_rollover` at `00:00 Europe/Kaliningrad`, startup catch-up, normal Smart Update coverage, atomic outbox local-day marker and coalesced builder row. | **Done:** candidate emitted ready local-date `2026-07-27`, `1080×1350`, version `20260727-bbf354b38838d4e5`; preview pre-gate passed. | Production scheduler observation remains operational follow-up, not candidate blocker. |
| R04 | Noindex Favorites is future-only, calendar-first, deduplicated and shared-auth/offline safe | Static skeleton plus shared auth; owner-scoped saved-event storage/RLS/RPC. | **Partial:** generated noindex shell and product smoke passed. | Live owner-scoped Yandex/Supabase postflight is still required before production-root acceptance. |
| R05 | Calendar reaches the furthest public event month; empty dates are inert | Generated availability horizon; empty cells are non-anchor spans with `aria-disabled=true`. | **Done:** exact product Playwright passed horizon, weekday alignment and inert empty dates. | Owner visual acceptance only. |
| R06 | Home has hero, quick routes and ≤30 useful static cards with local-only rerank | Useful SSR fallback; no page-view model/provider request. | **Done:** exact product Playwright passed and observed no provider requests. | Owner visual acceptance only. |
| R07 | Collections submenu contains Children/Unusual/Free/Clubs; Free also top-level | One mobile drawer and coherent vendored icons. | **Done:** exact product Playwright and public overflow/link checks passed. | Exact snapshot has no confirmed clubs, so owner must decide whether the fail-closed empty catalog is acceptable for this prototype. |
| R08 | Shared-BGE unusual semantics, hard eligibility, ordinary distance, dedup/diversity and hash-bound gates | One event vector supports related/unusual/family/concept; explicit `canonical-event-semantic-v1` fields fail closed; deterministic ordinary-corpus distance/receipt uses the same vectors; deferred rows cannot bypass diversity caps. | **Done:** exact SHA produced approved hash-bound real-BGE evidence with precision `1.0`, FPR `0.0`, recall `0.8`, 12 families, zero duplicates/ineligible/flip and `provider_calls=0`. | None for secret candidate. |
| R09 | Coalesced builder, cache/atomic/last-good, observability and preview/root/candidate gates | Production-candidate first runs ephemeral `build:preview` + `check:preview`, then production-root and secret-candidate build/check/browser gates. Preview output is neither archived nor published and must be cleared before root archive. | **Done:** terminal Kaggle run, preview receipt, root and secret browser reports, exact artifacts/hashes and immutable upload verified. | Production-root publication deliberately not executed. |
| R10 | Static `/neobychnoe/`, stable concept/red-dot state, no labs in package, no root cutover | `notify_eligible` survives ordinary rebuilds; migration output is false without erasing durable state. Product Playwright and local lab matrix use independent modes/bases; packaged lab route returns `404`. | **Done for candidate:** product Playwright passed, public 36-case matrix passed, lab route is `404`; local red-dot contract remains covered separately. | Explicit owner acceptance before root promotion. |

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
| Integration SHA | clean exact commit containing final semantic, rail, daily/preview and participant code | `123bcee460112ee9fe0b0a0176f51a07c92eed6a` (final documentation is a child commit) |
| Kaggle run | terminal success for that exact SHA with heartbeat/status history | `static-site-builder:r15-bge-final5-20260727T221000Z`, `COMPLETE`; build `production-r15-bge-final5-20260727t221000z` |
| Legacy preview pre-gate | `status=ok`, `archived=false`, `published=false`; build/check passed | `status=ok`, `archived=false`, `published=false` |
| Semantic contract | pinned BGE hashes, explicit eligibility, ordinary policy/corpus hashes, approved metrics, `provider_calls=0` | approved; manifest `6d29b70c…`, vector artifact `08d61c5c…`, vector cache `2da1dbcb…`; precision `1.0`, FPR `0.0`, recall `0.8`, `provider_calls=0` |
| Durable state | notification survives ordinary rebuild; migration manifest false without cache erasure | focused tests passed; final artifact `migration=true` and emits no false notification |
| Service-share | local date, `1080×1350`, PNG/WebP/manifest hashes, daily marker evidence | ready; date `2026-07-27`, `1080×1350`, version `20260727-bbf354b38838d4e5` |
| Root browser gate | generated root check and Chromium report pass | pass; archive retained as evidence only and not published |
| Candidate browser gate | generated noindex candidate check and Chromium report pass | pass |
| Product Playwright | product mode passed without visiting lab | pass against public immutable candidate |
| Lab Playwright | separate local ten-state matrix passed | covered by focused local contract; packaged lab independently returns `404` |
| Public candidate | complete upload; required product URLs `200`; lab path `404` | 1,703 objects create-only and byte/MIME verified; 36 route/viewports green; lab `404` |
| Telegram handoff | all owner-review links delivered to the requested UI-review thread | message `779`, chat `-1004337049383`, reply `548`, read-back verified |
| External consultant | Gemini Pro-class acceptance through agy, or an explicitly labelled allowed fallback | **blocked:** `Gemini 3.1 Pro (High)` failed twice with `account is not eligible … not currently available in your location`; official Antigravity FAQ lists Netherlands as supported. `a-opus` hit the same Antigravity eligibility gate and Claude Code Opus was not logged in. No lower-class model was substituted. |
| Owner/root decision | explicit acceptance; otherwise remain candidate-only/noindex | **pending — production root unchanged** |

Participant UI from `feature/static-event-participants-20260727` is integrated
in the exact code SHA. Its local 1440/390 specimen passed three cards, image,
overflow and reload-persistent-like checks. The exact 326-event production
projection contains zero verified artist/appearance relations, so all
`participants` arrays are intentionally empty; production data readiness is
separate from UI readiness.
