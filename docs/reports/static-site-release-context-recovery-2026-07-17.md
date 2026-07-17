# Восстановление контекста релиза статического сайта — 2026-07-17

> Status: current recovery map. This report explains where the release requirements
> were recovered from and how they differ from `origin/main`; the executable source
> of requirements is the [full readiness checklist](static-personal-announcements-release-readiness-2026-07-11.md).
>
> Audit base: `origin/main@d169004376c309dc487fa6b48a7aae4a8ed7dea3`.
> Recovery branch before this audit: `docs/static-site-release-plan-20260717@b794825b`.

## Вывод

Предыдущая попытка восстановления вернула только event-page production platform,
десятидневный Telegraph cutover и пять стартовых work packages. Это было полезным
срезом, но одновременно scope regression: из canonical umbrella исчезли F18, H1,
M1–M6, отдельные post-release releases и 153 checklist items.

Главный найденный источник —
`origin/agent/static-release/checklist-cdn-social@fbb5b6a2ad0ba5668a997dfc21ffa3ff0f9b9842`.
В нём readiness checklist имеет 616 строк и 228 checkbox-требований, а umbrella
фиксирует G1/G2, F1–F18, preliminary H1, M1/M1-QA/M2–M6 и отдельные релизы F14,
festivals, operations dashboard и interest clubs.

## Полный объём checklist

| Stage | Checkbox requirements | Смысл |
|---|---:|---|
| 0 | 9 | scope freeze, branches, UI/navigation/H1 decisions |
| 1 | 11 | Smart Update quality, incident replay, M5/M6 audits, 14-day window |
| 2 | 14 | production build/publish, manifest, CDN, atomic promotion/rollback |
| 3 | 20 | related graph, vector/LLM automation, search and public tags |
| 4 | 24 | global identity, telemetry, engagement, favorites and calendar |
| 4A | 8 | Supabase 500 MB budget, compaction, shedding and capacity tests |
| 5 | 13 | exactly-three recommendations, secret page, D-1 reminders, mail safety |
| 5A | 8 | real personalization E2E, golden personas and quality KPIs |
| 5B | 8 | H1 «Городской обзор» explicit `ship|defer` decision |
| 6 | 36 | transport, medallions, admin repair, media, sharing, M2/M5/M6/F18 |
| 6A | 10 | final frozen-SHA SEO/GEO: Codex + Gemini Pro + Opus |
| 7 | 17 | RC, security/load, canary, rollback, 14-day window and hypercare |
| 8 | 5 | post-presentation outward-link transition and feedback research |
| 8A | 9 | separate verified-comment-facts release |
| 9 | 7 | separate festivals release |
| 10 | 5 | separate operations dashboard release |
| 11 | 8 | separate interest-clubs release; now partially superseded by live canary |
| **Stage subtotal** | **212** | 16 additional checkboxes are evidence/product-decision/closure items outside stages |
| **Total** | **228** | full recovered baseline |

The 2026-07-17 reconciliation adds five executable D0–D10 Telegraph rows to
Stage 8 without deleting the baseline, so the current canonical checklist has
**233 checkbox requirements**.

The [event-page release plan](../features/static-site-pages/release-plan.md) is only
one Stage 2/8 workstream and cannot replace this registry.

## Recovered canonical documents

The following contracts were absent from `origin/main` but are required to make the
recovered checklist navigable. They were restored from `fbb5b6a2` unless another
source is named:

- release umbrella, global product decisions, expanded readiness checklist and
  expanded UI release contract;
- responsive navigation, service sharing and desktop clipboard research;
- event age-rating all-surface contract, medallion visual QA, final SEO/GEO gate;
- rail/bus directories and automated event-transport schedule contract;
- typed briefing H1 research contract;
- personalization E2E acceptance and Supabase storage budget;
- consolidated engagement, Region Talk reuse audit, festival release plan,
  operations dashboard and post-event feedback backlog;
- official presentation checklist from
  `origin/fix/static-site-v4-personalization-media-20260716@9e00f9a6`
  (file last materially changed in `6e41b1e0`).

Historical links/claims inside recovered documents remain dated evidence. Current
status must be read from the reconciliation below rather than inferred from an old
`Done` label.

## Three-way reconciliation with work from 15–17 July

| Requirement area | `origin/main` fact | Side/crash evidence | Current classification |
|---|---|---|---|
| Event desktop template | `58abfb19` merged | presentation preview has 282/282 and 12/12 evidence | **Partial** until frozen production RC rerun |
| Age rating M5 | `aa95900a` merged | recovered all-surface contract | **Partial/Blocked**: pipeline exists, full renderer/accepted-value parity unproven |
| Engagement M3 | `fe211a88`, `d25b15d6`, `b34a97d3` merged | consolidated source+site contract recovered | **Partial**: owned metrics exist; one shared source+site projection for every consumer does not |
| Interest clubs | `98180d1e`, `6b234a52`, evidence `6cdae545` merged; canary live | old Stage 11 assumed research had not begun | **Canary/Partial**: research baseline superseded, seven-day observation and stable-release closure remain |
| F5 design system | not in main | `origin/feature/static-design-system-catalog-20260717@128a2d6a` | **Side candidate**; merge plus immutable UI acceptance required |
| Production publisher | not in main | open PR `#43` at `9a6e9876`; 66 commits behind current main and conflicting on 2026-07-17 | **Side candidate/Blocked**; intentionally not folded into the foundation integration without a separate re-port/review |
| Identity/favorites/reminders | not in main | crash work preserved, completed and pushed at `bcd1d118`; accepted into fresh-main `integration/static-release-identity-transport-20260717` | **Integrated code candidate / production unapplied**; SQL/RLS/RPC contracts pass, but migration/Edge/scheduler/mail activation remain gated |
| Transport refresh | not in main | crash work preserved, completed and pushed at `a83704b0`; accepted into the same fresh-main integration branch | **Partial integrated candidate**; provider isolation/LKG/fan-in/coalescing and synthetic mechanics canary pass, while official adapters and production status-ledger/build evidence remain blocked |
| M2 image dedup closure | baseline `79/266` was cleaned; final projection/TG/VK/Telegraph violations were `0`, automatic gate is in main | crash lane downloaded a newer snapshot but did not finish the repeat audit; static canonical URLs were unavailable in earlier verification | **Partial**: cleanup foundation exists; repeat full audit/static verification and 14-day observation remain |
| Telegraph D0–D10 | no resolver/mode implementation in main | documented scenario IDs and cutover plan only | **Designed/Planned** |

The original crash lane map remains historical recovery evidence. The accepted
implementation and validation record is now
`.codex/integration/static-release-identity-transport-20260717-INTEGRATION_REPORT.md`
on `integration/static-release-identity-transport-20260717`. That branch is a
merge candidate, not production evidence and not a substitute for reaching
`origin/main` plus completing the activation gates.

## Test traceability audit

The recovered union first produced **116 unique definitions**: the previous 109-ID
branch plus seven design-system scenarios. This audit then added missing stable IDs
for Stage 0–11 orchestration, exact Stage 6 gates and separate post-release releases.
The current inventory therefore has **208 definitions** (`18 USR-*`, `190 ADD-*`).
At audit time **none of these stable IDs occurs in runnable test/script names**, so
automated release traceability is `0/208` even though lower-level checks exist.

Existing evidence must be classified honestly:

- `check-preview.mjs`, pytest public-gate/build-handoff and exporter tests:
  automated component contracts;
- `static_personalization_contract.spec.ts`: nine-test standalone demo, not public
  Astro/production E2E;
- `static_site_personalization.feature`: draft scenarios without step definitions;
- design-system and production-desktop checks: side-branch component/preview evidence;
- native share/clipboard/mail/calendar/maps and final visual quality: manual/native;
- atomic production promotion, root HTTP/browser release suite and D10 cutover:
  planned, no release evidence.

The old coverage row that mapped Kaggle handoff to `ADD-BUILD-01` was wrong:
`ADD-BUILD-01` is Smart Update debounce. Per-ID test/evidence status is therefore a
P0 documentation and automation task, not a cosmetic cleanup.

## What is still not done

The full unresolved backlog is the unchecked part of the current 233-item checklist, not a
five-row table. The largest blocking families are:

1. repeat M2 full-catalog/static-surface audit plus 14-day zero-critical Smart Update/event-quality window and closure-grade incident replay;
2. production profile, stable URLs, immutable manifest, atomic CDN promotion and rollback;
3. automatic related/search/tag lifecycle and current production catalog;
4. global identity, durable favorites, calendar/reminder state and ecological Supabase storage;
5. exactly-three email/personal-page journey and real Postbox correlation;
6. real personalization E2E with golden personas and the stricter `<=20` RC KPI;
7. H1 `ship|defer`, immutable F5/design-system/UI acceptance;
8. transport refresh/fan-in, medallion QA, M2 zero-duplicate closure, F17 admin repair;
9. F18/event share native matrices, age all-surface parity and linked occurrence cards;
10. final three-lane SEO/GEO gate, security/load, RC/canary/hypercare;
11. D0–D10 Telegraph dual-run/no-create cutover with real channel evidence;
12. separate F14, festivals, operations dashboard and interest-club stable releases.

## Release truth rule

A requirement is `Done` only when its accepted implementation is reachable from
`origin/main`, the stable scenario/evidence contract passes on the frozen SHA, and
current production/canary evidence exists where required. Side branches, dirty
worktrees and historical preview URLs are valuable recovery evidence but never
satisfy this rule on their own.
