# Antigravity-primary Festival Web Research — preproduction project

Статус: `implemented preproduction / provider eligibility blocked` (2026-07-31).
Collect-only runtime, shared quota accounting, A+B(+C), host validation,
operational persistence, manual CLI и безопасный queue seam реализованы.
Production apply отсутствует и feature flag по умолчанию выключен.

Контур относится только к `festival_queue.source_kind=url` и другим
несоциальным web/document-источникам. Telegram/VK intake остаётся отдельной
системой.

Каноническая модель семи discovery-топологий, programme subject→Event и
`festival-edition-v2` определена в
[`../../../festivals/data-model-v2.md`](../../../festivals/data-model-v2.md).
Датированный набор будущей проверки — в
[`antigravity-primary-evaluation.md`](antigravity-primary-evaluation.md).
Промпты/checkpoints — в
[`../../../../llm/antigravity-festival-research.md`](../../../../llm/antigravity-festival-research.md).

## Исправленная целевая роль

1. **Antigravity — основной collector новой системы с первой реализации.**
   Eligible non-social group не проходит сначала через Kaggle.
2. До acceptance feature выключена; ручные прогоны `collect-only` и требуют
   operator review. Это ограничение качества/apply, а не shadow за другим
   parser.
3. Построенный Kaggle+Gemma Universal Festival Parser никогда не запускался на
   production и не считается production-контуром, baseline или standby.
4. Kaggle+Gemma не отлаживается в текущем проекте. В целевом end-state это
   fallback, но его adapter/conformance/live acceptance — отдельная будущая
   работа.
5. Пока fallback не реализован, техническая ошибка Antigravity приводит к
   checkpoint recovery/retry/review. Она не включает legacy Kaggle direct
   writer и не стирает последнюю approved revision.

## Цель

По группе URL одного фестивального выпуска получить проверяемый
`festival-edition-v2` candidate, который:

- отделяет серию от выпуска и текущий выпуск от архива;
- классифицирует одну из семи discovery-топологий;
- хранит secondary topology, discovery/time/space/access/mechanics и programme
  structure раздельно;
- сохраняет полный inventory programme/catalogue/spatial subjects;
- обоснованно решает, что становится отдельным Event, а что остаётся schedule,
  programme block, temporal anchor, zone, participant, work, route point или
  product;
- содержит hash-bound claims, exact quotes, conflicts and unknowns;
- после отдельного approval может атомарно обновить festival projection и
  отправить только разрешённые Event candidates в Smart Update.

## Фактическая отправная точка

### Очередь на `2026-07-31`

Read-only production SQL показал:

- `1318` pending rows: `tg=536`, `vk=694`, `url=88`;
- после фильтра по дате события, а не queue timestamps, остаётся `31`
  defensibly-current non-social URL row;
- они группируются примерно в `22` edition targets;
- все 31 имеют `attempts=0`, `last_error IS NULL`, пустой `result_json`;
- URL rows не являются доказательством фестивальной identity: current
  false-positive cases (`Три кота: День Варенья`) специально остаются в review
  cohort;
- ambiguous `80 историй о главном` исключён из auto-target из-за периода
  `2026-03-28/2026-07-19 or 2026-09-01` и отдельного false-queue backup signal.

Freshness rule:

```text
status=pending AND source_kind=url
-> parse explicit event/edition period from signals/source text
-> accept only one unambiguous interval/single date with effective end >= cutoff
-> do not use created_at/updated_at/next_run_at as event freshness
-> semantic festival identity remains an Antigravity/review decision
```

### Antigravity

Tracked runtime теперь включает:

- `google_ai/interactions.py`: strict explicit-key-pool Interactions transport,
  background polling, continuation, cancellation, environment snapshot и
  request lease на каждый создающий POST;
- `festival_web_research/`: seven-topology contracts, source/evidence safety,
  exact-quote validation, programme→Event gates, A+B(+C) coordinator,
  reconciliation, artifacts и collect/review service;
- четыре additive operational tables для parent run, lane attempt, queue item
  membership и source ledger;
- `scripts/run_festival_web_research.py` для ручного collect-only запуска;
- opt-in `ENABLE_FESTIVAL_WEB_RESEARCH=1` queue seam. При включённом flag и
  отсутствующем strict-limiter service URL row fail-closed завершается ошибкой,
  а не попадает в legacy direct writer. Успешный candidate получает queue
  status `review`, не `done`.

Manual handler и scheduler создают strict service через
`festival_web_research.runtime` только при включённом flag. Перед provider calls
URL rows проходят explicit ISO period, public-DNS preflight и grouping полного
выпуска; safety limit применяется к группам, а не разрезает multi-URL edition.
Missing/ambiguous dates переходят в operator review без provider call.

Candidate принимается только с hash-chain checkpoint manifest. Все непустые
festival facts и topology/programme decisions связаны с accepted claims;
Event disposition дополнительно требует identity/logistics claims и семь pass
gates, включая accepted title/date/time/place claims одного и того же
programme-item subject (`local_subject_id == item_id`). Agent не может объявить
себя operator/host actor, а итоговое fact value обязано совпасть с cited
normalized claim value. A/B programme items сопоставляются по evidence-derived
semantic signature, а unresolved inventory запрещает approval. C ограничен 12k,
не получает search/URL tools и может переключить host candidate на целую
валидную lane только если все конфликты однозначно выбрали одну lane; иначе
результат остаётся review.

Миграция `007_google_ai_interaction_accounting.sql` разносит provider terminal
и semantic status в shared ledger. До её применения production runtime обязан
fail closed. Ручной canary может явно использовать
`--allow-legacy-accounting`: reservation/RPD/TPM остаются в shared limiter, но
semantic verdict хранится только в operational DB. Это не production mode.
Миграция `008_google_ai_atomic_reserve.sql` добавляет transaction advisory lock
на key/model, чтобы concurrent check+increment не превышал internal caps.

Проба `2026-07-29` доказала Interactions API, agent tools, сохранение/download
environment и actual usage. Но A/B/C дали `150950` tokens суммарно и `0/3`
terminal semantic results; обязательные checkpoints отсутствовали. Три
известные factual ошибки были исправлены manual/local review сырого evidence,
а не автономным готовым результатом. Это baseline, не acceptance.

Live debug `2026-07-31` затем выполнил ровно по одному quota-accounted probe на
каждом из пяти зарегистрированных ключей. На всех ключах create вернул
`in_progress` и remote environment, но первый poll завершился одинаковым
provider `403 permission_denied: The caller does not have permission`. Поэтому
полноценный A/B результат «Балтийской Уханы» не получен: одинаковый ответ
совместим с provider project/key eligibility problem; quota exhaustion и
prompt/schema rejection данными ответа не подтверждаются.
Shared daily counters после прогона: `RPD=1` на каждом ключе; код дополнительно
финализирует non-retryable poll rejection, чтобы не оставлять TPM reservation
зависшей. Повторять запросы этими же ключами до исправления eligibility нельзя.

### Kaggle+Gemma

RDR/UDS code построен, но production-run отсутствует. Его потенциальный URL
path имеет известные config/schema/Smart Update/direct-write разрывы, однако их
исправление намеренно вынесено из текущего Antigravity-проекта.

## End-to-end flow

```text
current pending URL rows + explicitly supplied official non-social sources
  -> deterministic currentness prefilter
  -> group series + explicit edition
  -> freeze target manifest, queue ids, normalized seed snapshots, input hash
  -> Antigravity A: evidence + subject inventory + topology + candidate
  -> Antigravity B: independent topology/Event checker + counter-evidence
  -> host compare and inventory conservation
       compatible -> host candidate
       valid conflict -> optional no-network Antigravity C
       missing evidence/unknown -> review
       technical failure -> checkpoint recovery/retry
  -> schema/evidence/topology/Event gates
  -> collect-only candidate + operator review
  -> [future approved apply]
       one immutable effective revision
       approved Event candidates -> Smart Update
       compatibility + atomic index/detail/manifest projections
```

## 1. Grouping before provider calls

### Target key

```text
series_candidate + explicit edition identity
```

Strong edition identity, in order:

1. explicit year/ordinal/season from current official evidence;
2. exact bounded date range in one edition source;
3. source-declared edition label;
4. unresolved temporary group requiring review.

A target group contains:

- all current queue row IDs believed to describe the same edition;
- all seed URLs and source roles;
- normalized content hashes;
- known aliases only as leads, never truth;
- target cutoff/retrieval time;
- schema/taxonomy/prompt/normalizer versions and hashes.

Multiple day-ticket URLs for one three-day festival are one group, not three
festival runs. A direct child-event URL stays in the same group but carries
`seed_subject_hint=possible_child_event`; it must not redefine the edition.

### Input fingerprint

Fingerprint binds:

```text
target identity candidate
sorted queue ids
canonical seed urls + normalized snapshot hashes
schema/taxonomy/prompt/normalizer versions + hashes
```

Unchanged input with a reusable schema/evidence-valid candidate consumes zero
provider calls. `needs_review`, incomplete or failed input may create a new
attempt; an operator-forced rerun always stays under the same parent target
rather than hiding history.

## 2. Source preflight

### Roles

```text
official_home
official_edition
official_program
official_document
official_organizer
official_venue
ticket_single_item
ticket_day
ticket_festival
ticket_pass_or_subscription
regional_official
media_or_aggregator
ambiguous
rejected
```

Role is source- and scope-specific. A ticket subscription page cannot prove an
individual Event ticket; a regional calendar can prove an edition lead/date but
not missing programme details.

### Adapter order

1. safe HTTP + metadata;
2. browser-rendered official page;
3. linked PDF/document extraction plus OCR when needed;
4. bounded known ticket adapter;
5. generic browser fallback;
6. Antigravity discovery only if seed sources lack credible current evidence.

Host fetches seed snapshots where feasible; Antigravity receives bounded
snapshots and may research only within its limits. Social links discovered on a
site are stored as contact/source leads but are not opened or treated as
non-social evidence by this contour.

### Safety

- public `http/https` only;
- DNS rebinding/private/reserved/credential URL rejection;
- redirect/byte/time/MIME limits;
- no browser profile/cookies/auth mounts;
- raw documents, HTML, screenshots and logs stored outside SQLite with hashes;
- prompt-injection text is source content, never instruction authority.

## 3. Operational storage project

All operational metadata is provider-neutral enough for a future collector, but
only `lane=antigravity` is schedulable in this design version.

### `festival_web_research_run`

```text
id, run_uid, target_key, series_candidate, edition_candidate
state, mode                       # collect_only | approval | apply
input_fingerprint UNIQUE
orchestration_version, contract_version, taxonomy_version/hash
primary_queue_item_id, candidate_sha256, candidate_json
quality_json, artifact_manifest_json
lease_owner, lease_expires_at, created_at, updated_at
```

### `festival_web_research_lane_run`

```text
id, run_id
lane                              # antigravity; reserved future values disabled
attempt_no
state                             # running | checkpoint_valid | complete |
                                  # incomplete | quota_blocked | failed
interaction_ids_json              # A/B/C with role and remote environment
model_id, prompt_version, contract_version, taxonomy_version/hash
input_fingerprint, artifact_manifest_json, usage_json, validation_json
candidate_sha256, started_at, completed_at
UNIQUE(run_id, lane, attempt_no)
```

### Queue/source links

```text
festival_web_research_item
  run_id, queue_item_id, original_status, source_role, decision, decision_reason

festival_web_research_source
  lane_run_id, source_id, requested/resolved/canonical_url
  source_role, edition_status, content_sha256, snapshot_ref
  normalizer_version, quote_index_ref, fetched_at, decision/exclusion
```

Large artifacts:

```text
festival-parsing/web-research/<target_slug>/<run_uid>/antigravity/<role>/...
```

Operational candidate is never canonical truth. After approval, durable
snapshot/claim/decision references are copied into the edition revision graph.

## 4. State machine

```text
pending
-> grouped
-> input_frozen
-> collecting_a
-> collecting_b
-> [adjudicating_c]
-> evidence_validating
-> reconciling_inventory
-> candidate_collect_only
-> needs_review | ready_for_approval
-> approved
-> applying
-> applied

collection state -> retryable | quota_blocked | failed
```

Rules:

- collector never writes Festival/Event/Telegraph/static artifacts directly;
- same fingerprint/candidate hash is idempotent;
- incomplete is recoverable only with all mandatory semantic checkpoints;
- technical and semantic failure remain distinct;
- no automatic fourth interaction;
- no automatic Kaggle route;
- last approved revision stays serving through any failure;
- queue rows become done only after a future approved apply and required public
  projection sync.

## 5. Evidence/checkpoint contracts

### Source ledger

Each source records canonical URL, requested/resolved URL, role, current edition
status, snapshot hash/reference, normalizer, retrieval time and decision.

### Atomic claim

```json
{
  "claim_id": "A-C0001",
  "source_id": "A-S001",
  "subject_kind": "festival_edition",
  "subject_key": "edition:target",
  "field_path": "/dates/start_date",
  "raw_value": "7 августа 2026",
  "normalized_value": "2026-08-07",
  "normalization": "iso_date",
  "verbatim_quote": "...",
  "quote_start": 100,
  "quote_end": 115,
  "normalizer_version": "festival-text-normalizer-v1"
}
```

Host reruns offset/hash/reference validation. Bare substring match is
insufficient.

### Subject inventory

Every found source-local subject receives stable local signature, entity role,
claims and one of:

```text
accepted | duplicate_of | rejected | unresolved
```

Accepted programme items additionally receive one canonical action disposition
from the data model. Participants, works, route points, products and zones
remain conserved subjects even when they are not Event candidates.
Every item also retains the explicit Event-gate ledger; model stages may assess
semantic/evidence gates, while `operator_approval` and `smart_update` are
host-owned states that remain pending during collect-only.

### Candidate

Every source-derived semantic fact is claim-backed. Stable keys, schema/hash
metadata, decision-backed enums and deterministic quality/serving fields may be
unwrapped but must have pinned derivation/reference rules.

## 6. Antigravity A/B/C project

### A — primary researcher

Fresh environment. Before first network call write `state.json`. Inspect grouped
seed sources independently; at most one discovery query and six accepted
sources. Checkpoint after each source, then build:

```text
source ledger
source reviews + atomic claims
source-local subject inventories
edition identity/currentness
primary + secondary topology and discovery facets
programme structure
programme item roles/dispositions
candidate_a + run summary
```

A receives taxonomy/schema/validator but no legacy Kaggle result or prior public
narrative.

### B — independent checker

Separate environment receives the same frozen target/snapshots, not A output.
At most one alternative query and four accepted pages. B independently checks:

- festival/not-festival and current edition;
- seven-value primary topology, route subtype and discovery unit;
- complete critical subject inventory;
- child Event gate vs schedule/programme/continuous/service/reject;
- stale edition, unsupported title modifier, offer scope and source authority.

B omission is unresolved.

### Local comparison

Proceed without C only when:

- edition identity/currentness compatible;
- primary topology compatible and secondary differences non-blocking;
- every subject in `A ∪ B` is accepted/rejected/unresolved;
- every possible Event has compatible disposition or blocking review;
- source/claim/hash/quote/reference validators pass;
- there is no stale-edition, ticket-scope or false-festival conflict.

Agreement is compatible evidence, not identical prose.

### C — conditional adjudicator

Only for two locally valid conflicting alternatives. Receives compact claim
diff with exact quotes and hashes. No URLs, raw pages, full candidate,
search/fetch/network. Chooses an existing alternative or `unknown|conflict`.
C cannot be used as retry/recovery researcher.

## 7. Taxonomy and Event gates

The data model owns the normative contract. Required summary:

- seven primary topologies:
  `series_season|lineup|grid_showcase|territory|market|route_promenade|network_pass`;
- topology answers the first user choice; topic, duration, access and Event count
  do not choose it directly;
- programme structure is a different representation axis;
- every programme item has entity role + disposition;
- standalone Event requires current-edition identity, independent public choice,
  event-grade occurrence, meaningful identity, access compatibility,
  topology compatibility, validated evidence and approval;
- shared-admission artist slots, zones, participants/products, institutions,
  works and permanent route objects are not Events;
- a programme block can be Event while constituent works remain nested;
- the Festival edition is not automatically duplicated into Event.

Any missing mandatory gate produces non-Event/review, not guessed promotion.

## 8. Apply boundary

### Collect-only acceptance

- no Festival/Event/Telegraph/static index mutation;
- operator sees source decisions, topology rationale, subject conservation,
  candidate diff and every Event/non-Event decision;
- collecting as primary does not grant write authority.

### Future approved apply

1. build one immutable edition revision from accepted evidence/decisions;
2. activate it atomically;
3. create Event apply plan only for approved event dispositions;
4. pass every Event candidate through Smart Update and media gate;
5. record terminal results;
6. build legacy compatibility projections;
7. atomically publish festival index/detail/manifest and sync compatibility
   surfaces;
8. only then finalize linked queue rows.

## 9. Operator surface project

```text
/fest_queue web --collect --limit 1
/fest_queue web --info
/fest_queue web --review <run_uid>
/fest_queue web --approve <run_uid>
/fest_queue web --reject <run_uid> <reason>
/fest_queue web --retry <run_uid>
```

Review must show:

- grouped queue IDs and edition target;
- A/B/C interaction/environment IDs and actual usage;
- accepted/rejected/stale/ambiguous sources;
- primary/secondary topology + discovery rationale;
- full conserved subject inventory;
- Event candidates and exact failed/passed gates;
- conflicts/unknowns and candidate diff;
- immutable artifact links/hashes;
- explicit candidate hash in callback.

## 10. Quotas and wrapper requirements

Design defaults:

| Setting | Value |
|---|---:|
| feature daily cap | 12 interactions |
| concurrency | 1 |
| calls/group | normally 2, maximum 3 |
| A target/reservation | 20k / 50k tokens |
| B target/reservation | 12k / 30k tokens |
| C target/reservation | 8k / 20k tokens |
| A/B accepted sources | 6 / 4 |

The registered shared safe cap is `54 RPM / 96000 TPM / 90 RPD` under
`antigravity-preview-05-2026`; feature 12 RPD is a stricter planned limit and is
not yet runtime-enforced.

The future wrapper must:

- use Interactions API without unsupported `labels` or structured-output
  assumptions;
- persist an idempotency record before/with remote create to prevent paid
  duplicate work after crashes;
- separate provider/accounting terminality from semantic lane state;
- finalize actual usage on success, incomplete and error;
- enforce wall deadline/cancellation, poll/resume and download integrity;
- hash/redact/checkpoint the environment manifest;
- use actual finalized usage before the next reservation;
- treat best-effort token budgets as non-authoritative because the probe
  overshot them materially.

## 11. Observability

Per parent run/lane/interaction:

```text
run_uid, target_key, input_fingerprint
state, attempt_no, interaction_id, environment_id
prompt/schema/taxonomy/normalizer versions + hashes
search/fetch/source counts
requested/reserved/actual tokens, RPD/RPM/TPM outcome
checkpoint names/hashes/validation
primary/secondary topology and A/B compatibility
subject counts by entity role/disposition
Event gate failures and review reasons
latency, incomplete recovery, retry outcome
candidate/revision hashes
```

Alert on missing mandatory checkpoint, token overshoot, quote/hash mismatch,
false-festival candidate, unresolved current-edition conflict, third-call rate,
RPD exhaustion, repeated incomplete or public mutation attempt during
collect-only.

## 12. Evaluation and acceptance

The dated cohort contains 31 current URL rows / about 22 groups plus
«Балтийская Ухана». It intentionally covers all seven topologies, a route
subtype, multi-URL grouping, shared-admission schedules, networks/passes,
markets/catalogues, continuous territories, series, grid/showcases, likely
false-positive festival rows and child-source rows.

Required gates:

- topology exact match `100%` on reviewed gold;
- child Event precision and recall `1.00` against reviewed gold;
- disposition macro precision `>=0.98`;
- source-grounded subject loss `0`;
- stale/unsupported/ticket-scope critical errors `0`;
- accepted critical exact-quote coverage `100%`;
- terminal-or-mandatory-checkpoint recovery `>=0.95`;
- typical 2 calls, hard max 3;
- five manual diverse live successes, then seven scheduled collect-only days;
- no public mutation during acceptance.

## 13. Implementation and rollout phases

### Phase 0 — wrapper/contracts/offline fixtures — implemented

Interactions wrapper, limiter lease, taxonomy/schema, checkpoint validators,
grouping and reviewed fixture pack.

### Phase 1 — manual Antigravity-primary collect-only — blocked at provider eligibility

One group/run; A+B(+C); five diverse live groups including «Балтийская Ухана»;
operator review of every topology and disposition.

### Phase 2 — scheduled approval-gated canary — not started

At most two changed groups/run, concurrency one, seven days, no auto apply,
quota/checkpoint/quality reports.

### Phase 3 — approved unified apply — intentionally not implemented

Immutable revision, Smart Update only for approved Event candidates, atomic
index/detail/manifest projections. Auto-apply remains a later gate.

### Future separate phase — Kaggle+Gemma fallback

Not designed as an operational dependency here. It must be implemented and
accepted separately before any automatic fallback can exist. После этого
целевой routing: Antigravity technical failure или rejection его candidate по
quality gates → Kaggle collect-only fallback; disagreement/повторный low-quality
result → operator review, а не автоматический выбор. Оба collectors используют
единый v2 contract и не пишут public data напрямую.

## Current operational gate

- Antigravity-primary and no-current-fallback policy accepted;
- correct seven discovery topologies and route subtype pinned;
- one normative child Event gate accepted;
- fresh evaluation cohort and «Балтийская Ухана» expected decisions reviewed;
- Interactions wrapper/checkpoint/quota boundaries accepted;
- collect-only and operator approval mandatory;
- apply migrations 007 and 008 to the limiter project with a database-write credential;
- obtain at least one API key whose Antigravity background execution reaches a
  terminal provider state instead of project-level `permission_denied`;
- rerun «Балтийская Ухана» with normal A+B (third C only on a real conflict),
  review exact evidence and then continue the five-festival Phase 1 cohort.

Manual command:

```bash
python scripts/run_festival_web_research.py \
  --name 'Балтийская Ухана' --edition 2026 \
  --url https://uhana.ru/ --url https://uhana.ru/contest/

python scripts/review_festival_web_research.py 1 approve \
  --operator operator-name --db artifacts/codex/festival-web-research.sqlite

python scripts/resume_festival_web_research.py 3 \
  --db artifacts/codex/festival-web-research.sqlite
```

`resume_festival_web_research.py` принимает ID уже сохранённой A/B lane,
продолжает polling по последнему interaction handle и не выполняет новый
creating POST, поэтому восстановление после падения процесса не расходует RPD.
Несколько различных строк `Дата:` или противоречие диапазона с
`Дата окончания:` считаются ambiguous и уходят в review до provider call.

Without migration 007 the command fails closed. `--allow-legacy-accounting` is
only for a bounded diagnostic and is never set by the queue/scheduler.
