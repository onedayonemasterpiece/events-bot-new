# P0 / SEV-1 LLM-first ingestion recall — execution matrix

## Frozen start state

- Repository: `onedayonemasterpiece/events-bot-new`
- Existing PR: `#494` (draft, unmerged)
- Requested branch: `integration/smart-update-identity-state-machine`
- User-verified head: `8614262f2c2a5489169cf3c7fa5bf8ab19c83b97`
- Fetched PR/origin head at start: `8614262f2c2a5489169cf3c7fa5bf8ab19c83b97`
- Match: **exact**; no newer PR changes existed at the freeze point.
- `origin/main` at start: `f66330f8af81d4b898d137d83356e77914dce90a`
- Integration will preserve the PR history and reconcile latest `main` only after
  the independently reviewable lanes are complete.

## Incident control block

- Incident ID: `INC-2026-08-10-smart-update-identity-terminal-loss`
- Current status: open; severity escalated by the request to P0 / SEV-1 recall
  degradation.
- Affected surfaces: VK discovery and auto-import, raw carrier durability,
  OCR/evidence assembly, event-parse LLM and prompts, cancellation/lifecycle,
  deterministic post-processing, quota admission/backpressure, Smart Update,
  recovery, observability, and production SQLite migration readiness.
- Target behavior: every fetched in-horizon configured-source carrier becomes
  exact replay, a complete-evidence typed LLM outcome, or durable retry; no
  semantic deterministic terminal and no technical terminal failure.
- Mandatory checks: the 76 requested regressions/static gates, incident raw
  replay through the production boundary and Smart Update, negative controls,
  full relevant CI, production-snapshot migration/rollback rehearsal, strict
  read-only acute/historical census and idempotent recovery dry-run.
- Release evidence to collect: exact base/final SHA, immutable timeline and
  graphs, loss/capacity/quota evidence, local/CI receipts, DB before/after
  proof, runbook and PR comment/body update.
- Follow-up boundary: no production write, recovery apply, deploy, merge, new
  PR, GitHub issue, provider or unbenchmarked model in this task.

## Requirements

| ID | Requirement (original section) | Area | Likely files/artifacts | Dependencies | Conflict risk | Primary lane | Parallelizable | Done when |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| R01 | Классификация инцидента | incident | incident record, integration artifacts | none | low | ORCH | yes | carrier/event counts are separated and all eight requested quantities are evidenced or explicitly unavailable |
| R02 | Неизменяемые продуктовые приоритеты | product contract | canonical LLM/ingestion docs | R01 | high | ORCH | lexicographic recall/identity priorities govern code and tests |
| R03 | Главный LLM-first контракт | semantic architecture | parse/discovery boundaries, docs | R02 | high | SEMANTIC-PARSE | after mapping | deterministic signals are hints/verification only |
| R04 | Что считать «все посты проходят LLM» | discovery contract | VK crawler/queue | R02 | high | RAW-DISCOVERY | after mapping | every eligible configured-source post is replay, LLM, or retry |
| R05 | Первый артефакт — точная временная шкала | forensics | git/Fly/env/model/quota artifacts | R01 | low | MAP-TIMELINE | yes | acute cause and chronic filters are dated and tied to deploy/env evidence |
| R06 | Полный control-flow inventory | code mapping | AST graphs and caller inventory | R03–R04 | medium | MAP-TIMELINE | yes | both production graphs annotate LLM/evidence/durability/retry/side effects |
| R07 | Read-only census всех потерь | production forensics | audit scripts/artifacts | R05–R06 | high | CENSUS-RECOVERY | yes/read-only | every available carrier is assigned exactly one loss class A–T with requested measures |
| R08 | Восстановить raw-first VK discovery | discovery/schema | VK fetch/cursor/raw queue/schema | R04,R06 | high | RAW-DISCOVERY | after mapping | durable save precedes semantic selection and caps create continuations |
| R09 | Полностью удалить pre-LLM semantic prefilter | discovery/parse | VK auto-import boundary | R03,R06 | high | RAW-DISCOVERY | after mapping | production path has no semantic prefilter API or equivalent relocation |
| R10 | Обеспечить полноту evidence для LLM | evidence | prompt builder/OCR/ledger | R03,R06 | high | SEMANTIC-PARSE | after mapping | complete manifest and incomplete-evidence negative-outcome guard exist |
| R11 | Один типизированный LLM source verdict | source parse | schemas/adapters/callers | R10 | high | SEMANTIC-PARSE | after mapping | events/actions/disposition are typed and backward-compatible |
| R12 | Убрать regex cancellation bypass | lifecycle | VK auto-import/lifecycle | R11 | high | SEMANTIC-PARSE | after mapping | lifecycle decisions originate in LLM; no-match remains durable |
| R13 | Убрать post-LLM shadow classifier | postprocess | draft validation/persistence | R11 | high | SEMANTIC-PARSE | after mapping | deterministic checks cannot delete a positive child or decide no-event |
| R14 | Conditional LLM verification вместо veto | verification | parse/verifier/prompt | R10–R13 | high | SEMANTIC-PARSE | after mapping | one normal call; only enumerated contradictions invoke existing model/config |
| R15 | Решить TPM на уровне capacity и queue | quota/backpressure | Google client, queue, scheduler, audit | R05–R06 | high | TPM-BACKPRESSURE | after mapping | project-scoped admission, calibrated reservations and durable fair retry pass 1.5x p99 replay |
| R16 | Обновить LLM prompt под maximum recall | prompts | canonical prompt family | R10–R14 | high | SEMANTIC-PARSE | after mapping | prompt covers full evidence, mixed/lifecycle/multi-occurrence recall |
| R17 | Сохранить и завершить Smart Update remediation | Smart Update | typed reasons/identity/callers | R11 | high | SMART-UPDATE-GATES | after mapping | closed reason enums, stable identity and technical retry invariants pass |
| R18 | Типизированные итоги | carrier/child states | verdict/queue/Smart Update adapters | R11,R17 | high | SEMANTIC-PARSE | after mapping | only enumerated carrier and child outcomes remain production-reachable |
| R19 | Recovery всех потерянных данных | recovery | recovery CLI/audit/replay | R07,R08–R18 | high | CENSUS-RECOVERY | serial after behavior | all requested selectors support read-only idempotent full-payload replay planning |
| R20 | Ретроспективная проверка стабильного периода | historical audit | audit scripts/artifacts | R05,R07 | medium | CENSUS-RECOVERY | yes/read-only | Feb–Jul stratified sample and safe horizon recommendation are evidenced |
| R21 | Observability | durable ledger/metrics | schema, queue, audit | R08,R10,R11,R15 | high | TPM-BACKPRESSURE | after mapping | carrier/child balances and zero-forbidden-terminal metrics are durable |
| R22 | Обязательные тесты | verification | tests/CI/replay fixtures | R03–R21 | high | ORCH | serial aggregation | T01–T76 have deterministic receipts and incident replay has a negative control |
| R23 | Production-snapshot migration rehearsal | DB readiness | snapshot artifacts, Database.init | schema lanes | high | CENSUS-RECOVERY | serial after schema | init twice, conflicts, counts, recovery, rollback and quick_check all pass or fail closed |
| R24 | Структура коммитов | delivery | git history | implementation lanes | medium | ORCH | serial integration | six coherent reviewable blocks or fewer justified coherent equivalents |
| R25 | Артефакты в PR #494 | evidence | canonical docs/artifacts/PR body | all | medium | ORCH | serial | all 24 requested artifacts are linked or explicitly blocked with proof |
| R26 | Ограничения | governance | entire diff/PR | all | high | ORCH | continuous | no forbidden product, provider, model, production or GitHub action occurred |
| R27 | Критерий готовности | closure | final checklist | all | high | CLOSURE-REVIEW | serial final | all 16 gates are independently verified; no readiness claim from CI alone |

## Mandatory test ownership

The wording and numbering are the authoritative T01–T76 list in section 22 of
the attached request; no test is intentionally omitted.

| Test IDs | Primary lane | Scope |
| --- | --- | --- |
| T01–T14 | RAW-DISCOVERY | discovery/pre-LLM durability, hints, OCR admission, continuation/cursor |
| T15–T20 | SEMANTIC-PARSE | complete source/OCR evidence and incomplete-evidence handling |
| T21–T31 | SEMANTIC-PARSE | typed source verdict, provider/schema/truncation retry, multi-event/lifecycle |
| T32–T38 | SEMANTIC-PARSE | conditional verification and one-call normal path |
| T39–T44 | SEMANTIC-PARSE | post-LLM no-veto and typed reason behavior |
| T45–T55 | TPM-BACKPRESSURE | quota scopes, leases, fairness, deduplicated calls, calibration, 1.5x p99 |
| T56–T65 | SMART-UPDATE-GATES | replay/update/distinct/technical/side-effect/occurrence identity |
| T66–T70 | CENSUS-RECOVERY | complete recovery inventory, idempotence and unchanged production DB |
| T71–T76 | SMART-UPDATE-GATES | AST/static forbidden-path gates across the integrated production graph |

## Dependency graph

`R01/R02 -> R05/R06 -> {R07,R08,R09,R10,R15,R17}`

`R10 -> R11 -> {R12,R13,R14,R16,R18}`

`{R08,R10,R11,R15} -> R21`

`{R07,R08..R18} -> R19 -> R23`

`{all implementation + audits} -> R22/R24/R25/R26 -> R27`

