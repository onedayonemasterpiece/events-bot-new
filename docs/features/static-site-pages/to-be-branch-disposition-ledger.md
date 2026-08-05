# Реестр разрешения TO-BE-документации из рабочих веток

> **Статус:** закрывающий нормативный ledger для retrospective branch-to-main audit.  
> **Cutoff:** полный remote snapshot 2026-08-05.  
> Машиночитаемый источник: [`to-be-branch-disposition-ledger.manifest.json`](to-be-branch-disposition-ledger.manifest.json); полный machine ledger — `to-be-branch-disposition-ledger.json.gz`.

## Итог

- Просканировано remote branches: **601**.
- Requirement-like/manual branches: **218**.
- Уникальных requirement-like paths: **409**.
- Неразрешённых branches/paths: **0 / 0**.

Это не означает, что каждый исторический файл скопирован в `main`. Исследования, labs, reports, incidents, implementation diaries и backlog остаются evidence. В `main` перенесены только совместимые нормативные решения; конфликтующие варианты получили явный superseded/not-accepted verdict.

## Branch dispositions

| Verdict | Count |
|---|---:|
| `already_canonical` | 1 |
| `conflict_resolved_superseded` | 5 |
| `current_consolidation_pr` | 1 |
| `historical_donor` | 3 |
| `historical_evidence` | 29 |
| `implementation_evidence` | 39 |
| `ported_slice` | 55 |
| `superseded_by_canonical` | 1 |
| `superseded_by_main` | 34 |
| `superseded_or_historical_donor` | 50 |

## Path dispositions

| Verdict | Count |
|---|---:|
| `already_canonical` | 23 |
| `backlog_not_accepted` | 21 |
| `canonical_ported_rewritten` | 15 |
| `canonical_updated_in_pr` | 36 |
| `conflict_resolved_superseded` | 2 |
| `historical_evidence` | 114 |
| `historical_seed` | 2 |
| `implementation_evidence` | 15 |
| `integrated_not_copied` | 1 |
| `main_canonical_branch_variants_superseded` | 130 |
| `not_accepted_historical_donor` | 47 |
| `not_accepted_stale_activation` | 1 |
| `superseded_by_canonical` | 2 |

## Критические противоречия

### focus feedback authorization

**Каноническое решение:** explicit Auth before server-side feedback/NPS/prize actions.

**Отклонено:** anonymous-first focus-control branches.

**Разрешение:** ported current focus-group contracts; stale dashboards marked superseded.

### Favorites / hidden / profile boundaries

**Каноническое решение:** Favorites = calendar/favorite union; hidden recovery stays outside profile; profile owns account/interests/diagnostics.

**Отклонено:** older My events/profile-hidden proposals.

**Разрешение:** transport ecology, user-profile and reminder docs rewritten.

### Search authorization

**Каноническое решение:** current authenticated Search contract.

**Отклонено:** 2026-07-18 public-basic-search recommendations.

**Разрешение:** analysis retained as evidence; contradictory product docs not ported.

### analytics physical ownership

**Каноническое решение:** browser compaction + resilient first-party ingest + YDB compact recent facts/aggregates + Parquet archive; Supabase not a telemetry lake.

**Отклонено:** Supabase-primary historical telemetry/E2E designs.

**Разрешение:** unified runtime, retention, event catalog and migration inventory created.

### strong actions vs browser clicks

**Каноническое решение:** terminal metrics come from authoritative idempotent receipts.

**Отклонено:** DOM click = success assumptions.

**Разрешение:** product measurement and runtime lanes separate weak observations from strong facts.

### Hero Talk behavior

**Каноническое решение:** versioned chain-first contextual Hero Talk and separate page-end denominator.

**Отклонено:** random/isolated copy experiments.

**Разрешение:** canonical Hero Talk package ported.

### event age rating

**Каноническое решение:** declared-only nullable fact; no default 0+; all-surface parity.

**Отклонено:** inference/default/partial-render variants.

**Разрешение:** public parity contract ported beside existing canonical extraction.

### Autopresenter readiness

**Каноническое решение:** owner-test slice accepted; portable/public remains NO-GO pending target evidence.

**Отклонено:** implementation diary interpreted as public readiness.

**Разрешение:** concise canonical README created.

### dated social brand activation

**Каноническое решение:** no activation claim without current accepted runtime/evidence.

**Отклонено:** fixed 2026-07-30 planned activation document.

**Разрешение:** stale planned document not imported.

## Как читать ledger

- `canonical_*` / `already_canonical` — требование живёт в main/PR #337.
- `conflict_resolved_superseded` / `superseded_*` — более позднее принятое решение победило.
- `historical_evidence` / `implementation_evidence` — материал сохраняет доказательную ценность, но не задаёт TO-BE.
- `backlog_not_accepted` / `not_accepted_*` — proposal не получил достаточного принятия.
- Коммитная дата — только hint; она не переигрывает owner correction и current SOR.

## Постоянный контроль

Workflow строит raw audit, применяет этот ledger и падает, если появляется новая requirement-like branch без disposition. Новый долг поэтому становится видимым в момент появления, а не через месяцы.
