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
| L0 | R0a | merged to main; exact-main canary running | PR #315, `5c359f30f`, `.codex/lanes/L0/RESULTS.md` |
| L1 + L1B | RYDB | merged locally; live enable blocked | `89d841054`, `8585fe2e8`, `.codex/lanes/L1{,B}/RESULTS.md` |
| L2 + L2B | R5/R6/R7/R8 | merged locally; deterministic acceptance complete | `363005371`, `ea96ac68b`, `.codex/lanes/L2{,B}/RESULTS.md` |
| L3 | R3 | merged locally and mounted by L4 | `976f34e69` lineage, `.codex/lanes/L3/RESULTS.md` |
| L4 | R0b/R1 | merged locally | `049fecadb`, `.codex/lanes/L4/RESULTS.md` |
| L5 | R2 | merged locally | `67333baf3`, `.codex/lanes/L5/RESULTS.md` |
| L6 | R4 consumer | merged locally, default-off | `1ba806041`, `.codex/lanes/L6/RESULTS.md` |
| L6P | R4 producer | pushed as draft PR | cat-weather-new PR #2; prerequisite CI repair PR #3 |

## Integrated acceptance evidence

- Baseline preview build/check passed before lanes were merged.
- External OTP harness unit suite: **65/65**, no mailbox or OTP issue call.
- Auth fixture + resilient transport + seven-profile matrix: **29/29** in the
  latest combined run; receipts enforce OTP/mail/receipt `0/0/0`.
- P13N contracts: Node tests and Python contracts pass; source guard passes.
- Final integrated preview: **461 pages**; preview check: **288 events**;
  full production-profile build: **469 pages**.
- Page runtime inventory: **384 eligible HTML**, zero missing/duplicate/
  unclassified runtime; explicit exclusions are 77 lab HTML, 292 JSON, 366 ICS,
  one service worker and one webmanifest.
- Production/off P13N browser characterization: network 0, localStorage delta 0,
  visible reorder 0, event-handler additions 0.
- Desktop double-click, mobile double-tap, dynamic cards, drag/control/keyboard
  arbitration: browser acceptance passed.
- VK question resolver/rebuild: Python **6/6**; desktop/mobile CTA browser and
  visual checks passed.
- Collections/gastronomy: registry Node **4/4**, lifecycle Python **6/6**,
  targeted exporter contract passed; preview build/check passed in lane.
- Weather consumer: **20/20**, build 466, no overflow/provider browser request,
  measured CLS <= 0.0041. Producer: **10/10**, Ruff and installed-package dry-run
  passed; schemas are byte-identical.
- YDB typed model: narrow **10/10** after integration; lane-wide **463 passed,
  1 deselected** (optional `openpyxl` only). The 20k fixture proves exact full
  counters with bounded pages/point joins.

## Closure matrix

| ID | Status | Evidence | Remaining gate/risk |
|---|---|---|---|
| RYDB | Partial | typed queue/counters, exact guard, budgets and 20k tests | no scheduler/RU enable before async-writer coverage, server RU alert, approved slot and 24-hour observation |
| R0a | Partial | fix merged to main; Fly runs exact SHA; one no-publish canary active | terminal successful candidate receipt and incident closure still required |
| R0b | Done | 384-page final integrated inventory and desktop/mobile gesture browser acceptance | none for implementation |
| R1 | Done | exact CTA resolver, UI, SVG provenance and coalesced rebuild tests | publication-trigger production observation follows normal release |
| R2 | Done | checked catalog/manifest/navigation and gastronomy lifecycle | data-blocked collections remain correctly unpublished |
| R3 | Done | P13N-00 mounted once; route/source/browser gates pass | later personalization waves remain deliberately unstarted |
| R4 | Partial | consumer and producer implemented; producer draft PR #2 | provider policy, production location hash, remote adapter/live smoke and seven-day canary; feature remains off |
| R5 | Partial | seven deterministic fault profiles and sanitized zero-mail receipts | hosted/browser/mobile and real product-outbox acceptance remains a release gate |
| R6 | Partial | anonymous-first focus v5 and auth modes unified | seed/eligibility/live focus launch gates remain separate |
| R7 | Partial | shared-upstream/no-false-recovery and Yandex degradation contracts executable at L0 | live provider/inbound/OAuth degradation cells remain gated |
| R8 | Done | typed route contexts and separate standard-onboarding contract | gated artifacts/club/draw promises remain disabled |
| R9 | Superseded | user item was empty | no change |

## Release ordering

1. Builder fix: merged as PR #315; canary/incident closure in progress.
2. Auth/transport + P13N-00 + shared runtime/CTA + collections/weather consumer:
   integration branch, combined validation and review before PR publication.
3. Weather producer: cat-weather-new draft PR #2 stacked on baseline CI repair
   draft PR #3.
4. YDB code may merge disabled, but live DDL/cutover/scheduler/RU changes require
   the documented manual canary and observation gates.
