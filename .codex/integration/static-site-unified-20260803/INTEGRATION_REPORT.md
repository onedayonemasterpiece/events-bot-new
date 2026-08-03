# Static Site Unified Integration Report

## Base and isolation

- Initial base: `origin/main@0bc8482dcda5cf16a89f312f9791ecbb6d0e9a3a`.
- Current main merged into integration: `5c359f30fcdae1fd64b1dedc547aa8e0981a60e3`.
- Integration branch: `integration/static-site-unified-20260803`.
- Dirty root checkout and local commit `12ad425e9` were preserved without edits.
- Real OTP/mail sends in this integration: **0**.

## Lane integration

| Lane | Requirements | Status | Integrated head/evidence |
|---|---|---|---|
| L0 | R0a | PR #315 merged; exact-main canary exposed a second deterministic browser assertion | `5c359f30f`; first canary failed at festival-count gate, local fix/revalidation in progress |
| L1 + L1B + L1C + L1D | RYDB | merged locally; live enable blocked | `34e0250ee`, `.codex/lanes/L1{,B,C,D}/RESULTS.md` |
| L2 + L2B + L2C | R5/R6/R7/R8 | merged locally; deterministic acceptance complete | `74897060d`, `.codex/lanes/L2{,B,C}/RESULTS.md` |
| L3 | R3 | merged locally and mounted by L4 | `976f34e69` lineage, `.codex/lanes/L3/RESULTS.md` |
| L4 | R0b/R1/R8 | merged locally; reviewer findings closed | `29bec098c`, `.codex/lanes/L4/RESULTS.md` |
| L5 | R2 | merged locally | `67333baf3`, `.codex/lanes/L5/RESULTS.md` |
| L6 | R4 consumer | merged locally, default-off | `1ba806041`, `.codex/lanes/L6/RESULTS.md` |
| L6P | R4 producer | pushed as green draft PR | cat-weather-new PR #4; PR #2 closed as superseded; baseline repair PR #3 merged as `65cec8bc` |

## Integrated acceptance evidence

- Baseline preview build/check passed before lanes were merged.
- External OTP harness unit suite: **65/65**, no mailbox or OTP issue call.
- Auth fixture + resilient transport + seven-profile matrix: prior combined
  **29/29** plus reviewer follow-up **14/14**. A PASS receipt now requires
  exactly one successful same-origin JWT-bound protected RLS GET; OTP/mail/
  receipt remain `0/0/0`.
- P13N contracts: Node tests and Python contracts pass; source guard passes.
- Final integrated preview: **461 pages**; preview check: **288 events**;
  full production-profile build: **469 pages**.
- Page runtime inventory: **392 eligible HTML**, zero missing/duplicate/
  unclassified runtime; explicit exclusions are 77 lab HTML, 292 JSON, 366 ICS,
  one service worker and one webmanifest. Standard onboarding is inert on
  **385/385** eligible routes; seven focus-product routes are excluded.
- Production/off P13N browser characterization: network 0, localStorage delta 0,
  visible reorder 0, event-handler additions 0.
- Desktop double-click, mobile double-tap, dynamic cards, drag/control/keyboard
  arbitration: browser acceptance passed.
- VK question resolver/rebuild: Python **7/7**; partner CTA requires exact
  live-at-import VK provenance before managed-Afisha fallback. Desktop/mobile
  CTA browser and visual checks passed.
- Collections/gastronomy: registry Node **4/4**, lifecycle Python **6/6**,
  targeted exporter contract passed; preview build/check passed in lane.
- Weather consumer: **20/20**, no overflow/provider browser request, measured
  CLS <= 0.0041. Producer PR #4 has green Python/lint/unit/contract/OpenAPI CI
  and implements location-hash binding, provider policy and pointer-last remote
  adapter without making a live write.
- YDB typed model: final lifecycle suite **16/16**; CandidateReport-focused run
  **337 passed, 1 deselected** (optional `openpyxl` only). Shared aliases,
  due cutoff, deterministic paging, serializable lease/token ACK and two-run
  ACK-only consumer lifecycle are covered. The 20k fixture remains bounded.

## Closure matrix

| ID | Status | Evidence | Remaining gate/risk |
|---|---|---|---|
| RYDB | Partial | typed queue/counters, due cursor/lease, ACK-only consumer lifecycle, exact guard, budgets and 20k tests | no scheduler/RU enable before live YQL/server-RU validation, async complete-producer coverage, alert, approved slot and 24-hour observation |
| R0a | Partial | PR #315 merged and exact-main build reached Chromium; failure reduced to stale hard-coded festival count | data-aware festival fix must pass full local gate, merge/deploy, then obtain a new terminal successful candidate receipt and close incident |
| R0b | Done | 392-page final integrated inventory and desktop/mobile gesture browser acceptance | none for implementation |
| R1 | Done | live-import partner provenance, published managed fallback, UI, SVG provenance and coalesced rebuild tests | publication-trigger production observation follows normal release |
| R2 | Done | checked catalog/manifest/navigation and gastronomy lifecycle | data-blocked collections remain unpublished; the explicit gastronomy repair route stays reachable/noindex by lifecycle policy |
| R3 | Done | P13N-00 mounted once; route/source/browser gates pass | later personalization waves remain deliberately unstarted |
| R4 | Partial | consumer plus green producer draft PR #4; binding/policy/conditional pointer-last adapter implemented | owner-approved production binding, provider usage approval/accounting, bucket/IAM/CDN policy, remote conflict smoke, seven-day observation and bounded consumer canary remain; feature stays off |
| R5 | Partial | seven deterministic fault profiles, mandatory JWT/RLS protected probe and sanitized zero-mail receipts | hosted/browser/mobile and real product-outbox acceptance remains a release gate |
| R6 | Partial | anonymous-first focus v5 and auth modes unified | seed/eligibility/live focus launch gates remain separate |
| R7 | Partial | shared-upstream/no-false-recovery and Yandex degradation contracts executable at L0 | live provider/inbound/OAuth degradation cells remain gated |
| R8 | Done | typed inert page-end placement/context on 385 routes and separate focus boundary | gated artifacts/club/draw promises remain disabled |
| R9 | Superseded | user item was empty | no change |

## Release ordering

1. Builder fix: PR #315 merged; first exact-main canary found a stale festival
   selector. A dequeue/hold race launched one old-SHA retry; deterministic local
   fix and incident closure remain in progress without another manual run.
2. Auth/transport + P13N-00 + shared runtime/CTA + collections/weather consumer:
   open draft PR #316, with final combined validation/review running on its
   latest metadata commit.
3. Weather producer: cat-weather-new draft PR #4 directly on `main`, all checks
   green; prerequisite repair PR #3 is merged.
4. YDB code may merge disabled, but live DDL/cutover/scheduler/RU changes require
   the documented manual canary and observation gates.
