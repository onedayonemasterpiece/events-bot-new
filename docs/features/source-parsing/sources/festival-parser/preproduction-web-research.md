# Festival Web Research: preproduction design

Статус: `proposed preproduction design`, реализация и rollout ещё не выполнены.

Область: `festival_queue.source_kind=url` и все найденные из таких страниц
несоциальные источники. Telegram/VK intake, Telegram Monitoring и VK auto
import этим контуром не меняются.

Canonical target JSON, the seven structural festival profiles, orthogonal
classification facets and item-disposition semantics are defined once in
[`../../../festivals/data-model-v2.md`](../../../festivals/data-model-v2.md).
This document owns collection/runtime, not a parallel festival taxonomy.

## Цель

Получать из нескольких сайтов и документов один evidence-backed пакет выпуска
фестиваля, который:

- не смешивает программы разных лет;
- различает официальный сайт, отдельное событие, билет, абонемент, СМИ и
  агрегатор;
- не добавляет неподтверждённые номера/статусы в название;
- сохраняет точные claims и цитаты;
- разделяет самостоятельные события и program-only активности;
- до ручного approval новый unified/Antigravity candidate ничего не меняет в
  `festival`, `event`, Telegraph и публичных индексах; существующий Kaggle
  continuity path продолжает текущее поведение на shadow-фазе;
- после approval создаёт/обновляет события только через Smart Update.

## Scope

### Входит

- обычные и динамические HTML-страницы;
- официальный сайт/лендинг и страницы программы;
- страницы организаторов и площадок;
- ticketing: single-event, subscription, pass, registration;
- региональные туристические афиши, СМИ и агрегаторы;
- PDF/DOC с программой;
- изображения/афиши с OCR, если они связаны с несоциальной страницей;
- JSON-LD, embedded JSON, RSS/Atom, iCal и sitemap как дополнительные
  машиночитаемые источники;
- redirects и canonical URLs.

### Не входит

- `t.me`, `telegram.me`, `vk.com`, `m.vk.com` и другие social sources;
- чтение постов/комментариев/сторий;
- автоматический public apply;
- замена существующих TG/VK pipelines;
- deterministic keyword-классификация смысла фестиваля или события.

Если сайт содержит social link, контур может сохранить его как
`unfetched_social_reference`, но не загружает и не использует как evidence.

## Фактическая отправная точка

Read-only production probe `2026-07-29`:

- `festival_queue`: `88` pending URL rows;
- после нормализации существующих name hints: `54` группы;
- `15` групп содержат несколько URL, всего `49` rows;
- крупнейшие URL-домены:
  - `kaliningrad.tretyakovgallery.ru` — 21;
  - `tickets.sobor-kaliningrad.ru` — 17;
  - `visit-kaliningrad.ru` — 16;
  - `kaliningrad.qtickets.events` — 13;
  - `dramteatr39.ru` — 5;
  - `filarmonia39.ru` — 3.

Текущий `_festival_group_key()` группирует только `vk/tg`; каждый URL row идёт
в Universal Festival Parser отдельно. Это приводит к повторным Kaggle runs,
потере cross-source evidence и риску принять event/ticket page за полный
festival source.

Текущий Universal Festival Parser на Kaggle + Gemma остаётся активным
production-контуром до acceptance Antigravity, а после acceptance должен
остаться регулярно проверяемым hot standby/fallback. Его UDS v1 нельзя сразу
использовать как authority для общей v2-модели:

- один URL за запуск;
- `GREEDY` prompt максимизирует извлечение, а не precision;
- officiality частично определяется наличием `fest` в домене;
- второй LLM-pass вызывается только при missing fields;
- нет claim/source/edition ledger;
- model confidence является self-grade;
- UDS напрямую upsert-ится до внешнего evidence gate.

Кроме того, текущая реализация ещё не доказана live E2E и имеет блокирующие
разрывы, которые нужно исправить независимо от Antigravity:

- URL/run config формируется launcher'ом, но общий Kaggle runner инжектит
  config только в `.ipynb`, тогда как Universal Festival Parser — script
  kernel и ожидает `FESTIVAL_URL` либо `/kaggle/input/run-config/config.json`;
- prompt и `UDSFestival`/`UDSActivity` расходятся по типу, ticket, participant,
  work и image fields; schema failure сейчас допускает raw fallback;
- URL programme сохраняется в `activities_json`, но не проходит через Smart
  Update и не появляется как Event;
- URL rows не группируются по выпуску и запускают отдельные тяжёлые runs;
- текущий parser напрямую пишет Festival/Telegraph до revision/approval gate.

## Главные решения

1. **Не создавать вторую intake-очередь.** `festival_queue` остаётся
   каноническим входом.
2. **Группировать URL rows до fetch/LLM.** Одна группа выпуска — один research
   target.
3. **Research state хранить в core Fly SQLite.** Это canonical operational
   state, не personalization Supabase и не YDB.
4. **Большие snapshots хранить в существующем Supabase Storage bucket
   `festival-parsing`.**
5. **Роль primary зависит от rollout phase.** До acceptance Kaggle+Gemma
   остаётся активным writer, а Antigravity работает collect-only shadow/canary.
   После acceptance Antigravity становится primary для eligible non-social
   web-групп, Kaggle+Gemma — регулярно проверяемым hot standby/fallback.
6. **Локальный код только ограничивает вход и проверяет доказательства.** Он
   fetch/mount-ит seed sources, хранит hashes/checkpoints и валидирует
   JSON/references/quotes, но не подменяет агентское смысловое исследование.
7. **Оба коллектора приводятся к одному provider-neutral contract.** Ни
   Antigravity, ни Kaggle+Gemma не являются canonical truth и не могут быть
   параллельными writer'ами. Общий host-side gate собирает одну revision.
8. **Local evidence gate является authority.** Модели не выставляют
   publishable/confidence самостоятельно.
9. **Preproduction всегда shadow и approval-gated.**

## End-to-end flow

```text
festival_queue URL rows
  -> group by series + explicit edition
  -> freeze target manifest, source snapshots and input fingerprint
  -> provider-neutral lane router

  before Antigravity acceptance:
     Kaggle+Gemma -> current apply path
     Antigravity A+B(+C) -> independent collect-only shadow

  after Antigravity acceptance:
     Antigravity A+B(+C) -> primary candidate
     Kaggle+Gemma -> fallback on runtime failure + scheduled hot-standby sample

  -> common evidence validation and programme-inventory reconciliation
  -> host-built festival-edition-v2 candidate
  -> shadow diff + operator approval
  -> one immutable effective revision / one apply lock
  -> Smart Update accepted Event candidates
  -> compatibility + festival-index-v2 + festival-detail-v2 projections
```

Runtime failure (`429/5xx`, unavailable model/runtime, invalid/missing mandatory
checkpoints) may route to the healthy standby. Semantic `unknown`, low evidence
coverage or A/B conflict is **not** availability failure: it goes to C/operator
review and must not be silently replaced by a more convenient Gemma answer.

## 1. Grouping

### Target key

```text
series_key
+ explicit edition year/number/season when present
+ date-cluster only when explicit edition label is absent
```

Inputs:

- `festival_name`, `festival_series`, `festival_full`;
- explicit source URLs and `dedup_links_json`;
- explicit dates/year already present in `signals_json`;
- existing `festival` aliases/source URLs;
- source-local Antigravity series/edition decision when deterministic evidence
  недостаточно.

Hard rules:

- differing explicit years never merge;
- `Pianissimo` and `PIANISSIMO` may group by normalized series;
- event/ticket pages with the same series remain separate sources inside one
  group, not separate festival runs;
- unknown edition cannot silently merge into a known edition; it becomes an
  ambiguous member until reviewed;
- `bot:*`, invalid URLs and social domains are excluded before fetch.

Production snapshot expectation: current `88` URL rows should become roughly
`54` initial targets instead of `88` independent Kaggle jobs. Exact count is
versioned and may change after LLM edition review.

## 2. Source preflight and routing

### Source roles

```text
official_home
official_program
official_organizer
official_event
ticket_single_event
ticket_subscription
festival_pass
registration
regional_tourism
media
aggregator
document_pdf
document_image
machine_feed
social
other
```

`social` may be preserved as referenced provenance but is not fetched by this
non-social lane.

Role/officiality is not inferred from a `fest` substring. It requires:

- curated domain/series relationship; or
- explicit self-identification/organizer link inside current source evidence;
  or
- LLM source-role decision with exact quote, downgraded by local checks.

### Adapter order

1. Known structured adapter:
   - `visit-kaliningrad`;
   - `tickets.sobor` / `sobor39`;
   - Tretyakov ticket/catalog;
   - Qtickets;
   - Philharmonia/theatre/venue parsers already present in repo.
2. Plain HTTP fetch + metadata/JSON-LD/anchor extraction.
3. Playwright only for JS shell or materially incomplete static response.
4. PDF text extraction; OCR only if text layer is absent/insufficient.
5. Generic browser fallback.

Ticket JS reverse engineering is adapter-owned. Antigravity не исследует
произвольные bundle/API endpoints.

### Fetch safety

- only `http/https`;
- reject loopback, link-local, private network and metadata endpoints;
- resolve/recheck DNS across redirects;
- at most 5 redirects;
- connect/read timeout 10/20 seconds;
- default max response 2 MB, document override 15 MB;
- MIME allowlist;
- no credentials/cookies from production bot;
- canonical URL and resolved URL stored separately;
- HTML/text normalized once and hashed;
- identical `(canonical_url, content_sha256)` reuses snapshot.

## 3. Storage model

These provider-neutral tables belong to core Fly SQLite.

### `festival_web_research_run`

```text
id
run_uid UNIQUE
target_key
series_hint
edition_hint
state
mode                    # shadow | approval | apply
input_fingerprint UNIQUE
contract_version
orchestration_version
primary_queue_item_id
planned_primary_lane    # antigravity | kaggle_gemma
effective_lane_run_id
fallback_reason
candidate_json          # bounded summary only
quality_json
artifact_manifest_json
verdict                 # ready | needs_review | rejected | failed
last_error
lease_owner
lease_expires_at
started_at
completed_at
created_at
updated_at
```

### `festival_web_research_lane_run`

```text
id
run_id
lane                    # antigravity | kaggle_gemma
lane_role               # active_writer | shadow | canary | primary | fallback | audit
attempt_no
state                    # not_scheduled | running | checkpoint_valid | complete |
                         # incomplete | quota_blocked | failed
model_id
contract_version
prompt_version
taxonomy_version
taxonomy_sha256
input_fingerprint
kernel_or_agent_sha
artifact_manifest_json
usage_json
validation_json
candidate_sha256
fallback_capability     # continuity_only | full_v2
started_at
completed_at
UNIQUE(run_id, lane, lane_role, attempt_no)
```

Antigravity A/B/C interaction IDs/checkpoints belong to one Antigravity lane
run. Kaggle kernel/output IDs belong to the separate Kaggle+Gemma lane run.
Fallback never overwrites or hides the failed primary lane record.

### `festival_web_research_item`

```text
run_id
queue_item_id
selected
input_role_hint
PRIMARY KEY (run_id, queue_item_id)
```

### `festival_web_research_source`

```text
lane_run_id
source_id                # lane-local; A/B/Gemma namespaces do not collide
global_snapshot_id       # host mapping by URL/hash/normalizer
requested_url
resolved_url
canonical_url
domain
content_type
fetch_mode
fetch_status
http_status
content_sha256
artifact_path
source_role
edition_status
edition_year
authority_status
decision_json
excluded_reason
fetched_at
PRIMARY KEY (lane_run_id, source_id)
```

Full source text, HTML, screenshots, documents, claims, LLM logs,
counter-evidence and adjudication packets remain in:

```text
festival-parsing/web-research/<target_slug>/<run_uid>/<lane>/
```

SQLite stores only bounded manifests/hashes and the final candidate summary.
The final candidate is built host-side from validated lane evidence; a lane
candidate is not canonical truth by itself.

Existing `festival_queue.result_json` receives only:

```json
{
  "web_research_run_uid": "...",
  "web_research_verdict": "ready|needs_review|rejected|failed",
  "web_research_input_fingerprint": "...",
  "shadow": true
}
```

## 4. State machine

```text
pending
-> grouped
-> input_frozen
-> routed
-> collecting_primary
-> collecting_check
-> [adjudicating]
-> evidence_validating
-> reconciling
-> candidate_shadow
-> needs_review | ready_for_approval
-> approved
-> applying
-> applied

Any collection state -> retryable | degraded_fallback | failed
```

Rules:

- shadow run never changes queue `status`;
- same input fingerprint reuses terminal shadow run;
- the fingerprint binds target identity, grouped queue IDs, normalized source
  hashes, normalizer, schema/contract and taxonomy version/hash; a forced
  operator rerun creates a new lane attempt rather than a duplicate parent;
- content hash change creates a new run;
- stale lease is recoverable without duplicating provider requests;
- agent `incomplete` with valid checkpoints is not automatically `failed`;
- no automatic fourth Antigravity interaction;
- provider switch creates a new lane run bound to the same input fingerprint;
- only a technical/runtime failure may trigger automatic fallback;
- last approved revision remains serving through retry/fallback/failure;
- queue rows become `done` only after approved apply, Smart Update terminal
  outcomes and atomic index/detail projection sync.

## 5. Evidence contracts

### Source ledger

Per source:

```json
{
  "source_id": "S001",
  "canonical_url": "https://...",
  "content_sha256": "...",
  "source_role": "official_program",
  "edition_status": "accepted|rejected|ambiguous",
  "normalizer_version": "host-pinned string",
  "decision_quotes": [{
    "quote": "verbatim",
    "quote_start": 0,
    "quote_end": 0
  }],
  "artifact_path": "..."
}
```

### Atomic claim

```json
{
  "claim_id": "C0001",
  "source_id": "S001",
  "local_subject_id": "festival|event:1",
  "field": "title|date|time_start|venue|ticket_url|...",
  "raw_value": "...",
  "normalized_value": "...",
  "normalization": "none|trim|iso_date|iso_time|canonical_url",
  "verbatim_quote": "...",
  "quote_start": 0,
  "quote_end": 0,
  "normalizer_version": "host-pinned string"
}
```

Local validator requires that the recorded offsets reproduce the quote under
the pinned normalizer against the exact `content_sha256` snapshot. A bare
substring match is insufficient when the same name/date occurs repeatedly.

### Candidate

Every **source-derived semantic fact** is either represented by this claim
wrapper or belongs to a typed object that carries the required
`claim_ids`/`decision_ids` for that fact:

```json
{
  "value": "...",
  "claim_ids": ["C0001"],
  "status": "supported|conflict|unknown"
}
```

Candidate cannot contain a source-derived value absent from claims. Stable
keys, schema/taxonomy/hash metadata, host-normalized ISO components, controlled
enums selected by an evidence-backed decision and deterministic serving/quality
fields may remain unwrapped; the JSON Schema pins their derivation and required
references. Thus fields such as `disposition` or `programme_profile` cite a
decision, while `serving.index_ready` is recomputed by the host rather than
quoted from a page.

### Taxonomy and programme disposition

The candidate pins:

```json
{
  "schema_version": "festival-edition-v2",
  "taxonomy_id": "kenigevents-festivals",
  "taxonomy_version": "1.0.0",
  "taxonomy_sha256": "...",
  "classification": {
    "programme_profile": {
      "value": "identity_only|single_compound_event|standalone_events|schedule_only|hybrid|continuous_experience|distributed_cycle|unknown",
      "claim_ids": ["C001"],
      "decision_ids": ["D001"],
      "status": "supported|conflict|unknown"
    }
  },
  "programme_sections": [{
    "section_key": "...",
    "items": [{
      "item_key": "...",
      "disposition": "link_existing_event|create_event_candidate|schedule_slot|programme_only|continuous_activity|service_information|reject",
      "decision_ids": ["D002"]
    }]
  }],
  "decisions": [{
    "decision_id": "D002",
    "evidence_claim_ids": ["C002"]
  }]
}
```

The calendar branch's free-text `category`, research status, date precision,
source role and programme profile remain different axes. Unknown taxonomy
labels are quarantined and never become production categories automatically.

### Counter-evidence

Independent Antigravity checker receives target/seed only, not candidate:

```json
{
  "challenges": [{
    "field": "festival.title",
    "challenger_value": "...",
    "source_id": "B-S001",
    "source_url": "...",
    "content_sha256": "...",
    "quote": "...",
    "quote_start": 0,
    "quote_end": 0,
    "reason": "stale_edition|unsupported_modifier|ticket_scope|other"
  }]
}
```

It does one search query, fetches at most four pages and checkpoints each page
immediately.

### Adjudication packet

Only conflicting claim values, exact quotes and hashes. No URLs to reopen, full
HTML, raw candidate, broad search or network tool.

## 6. Dual-lane runtime

### Kaggle + Gemma collector/hot standby

The existing Render–Distill–Reason kernel is retained. Before Antigravity
acceptance it remains the active URL path while Antigravity is shadow-only.
After acceptance it runs on primary technical failure, operator request and a
bounded health/audit sample.

The current UDS v1 output supports only `continuity_only` fallback because it
lacks hash-bound claims and full programme taxonomy. A target `full_v2`
adapter must add, without discarding RDR/Gemma:

```text
strict UDS validation (no raw apply fallback)
-> source snapshots + quote/offset claims
-> seven-profile classification + programme inventory/dispositions
-> festival-edition-v2 candidate
-> collect-only result, no direct Festival/Telegraph mutation
```

Until that adapter passes the common gate, a Gemma fallback result is
`needs_enrichment/needs_review`; it may preserve current service continuity but
cannot automatically claim detail-page readiness.

### Call A — primary researcher

Каждая festival group получает отдельную свежую Antigravity environment.
Агент видит target manifest и bounded snapshots исходных URL, но не результат
Kaggle+Gemma. Cross-lane comparison выполняется только host-side, поэтому
ошибка standby не закрепляется внутри primary prompt.

Контракт:

1. до первого network call записать `/workspace/state.json`;
2. разобрать seed sources по одному;
3. если среди них нет credible current source — выполнить ровно один
   discovery query;
4. использовать не более шести источников суммарно;
5. после каждого успешного fetch сразу сохранить snapshot и обновить source
   ledger;
6. для каждого source определить role/edition и выписать atomic claims с
   точными quotes;
7. классифицировать каждый programme subject и сохранить disposition decision;
8. вывести семь taxonomy axes/programme profile только из принятых
   claims/decisions;
9. собрать candidate только из этих claims.

Так как Antigravity не поддерживает structured output, JSON-файлы агента —
предложение, а не доверенный результат. Их принимает только local validator.

### Call B — independent checker

Вторая свежая environment получает только target manifest и исходные seed
URLs/snapshots. Ей не передаются candidate, claims или решения Call A, чтобы
не закреплять первую ошибку.

Контракт:

1. использовать альтернативную формулировку ровно одного search query;
2. открыть не более четырёх страниц;
3. независимо проверить актуальность edition, модификаторы названия и роль
   ticket URL (`single_event|subscription|festival_pass`);
4. независимо определить programme profile и disposition каждого критического
   programme subject по pinned taxonomy;
5. checkpoint-ить каждый источник немедленно;
6. вернуть собственный source/claim/disposition ledger и counter-evidence, а
   не оценку уверенности первого агента.

Два вызова Antigravity — нормальный путь для каждой группы, а не исключение.

### Call C — optional adjudicator

Вызывается только если local compare нашёл критический конфликт A/B.
Получает компактный claim diff: значения, exact quotes, hashes и перечисленные
локально валидные alternatives. Полные страницы, URLs, исходные candidates и
broad search не передаются. Search/network/fetch выключены.
Для каждого `conflict_id` результат содержит только
`choice=<existing alternative_id>|unknown|conflict` и supporting claim IDs.
Call C не возвращает и не пересобирает candidate, не создаёт новых фактов.

Никакого автоматического четвёртого вызова нет. `status=incomplete` у любого
агента допустим, если обязательные checkpoints уже записаны и проходят
локальную проверку.

### Semantic ownership

Inside the Antigravity lane A/B/C отвечают за:

- festival vs event vs program-only;
- source role and edition match;
- atomic claim extraction;
- event identity/reconciliation;
- explicit conflict classification.

Kaggle+Gemma независимо извлекает те же семантические объекты через свой RDR
contract. Ни одна lane не «побеждает» по имени модели или голосованию. Existing
UDS values без source-bound claims являются proposals/leads; после перехода на
общий evidence envelope валидные claims обеих lanes имеют одинаковые правила
проверки. Host reconciler выбирает source-backed facts по field-specific
authority и сохраняет conflict/unknown.

Field authority is not a single global source score:

- current official home/organizer evidence owns edition identity;
- current official programme/PDF and direct official event pages own
  programme/occurrence facts;
- the matched direct single-event ticket page owns sale/price for that item;
- pass/subscription owns only edition/pass scope;
- a newest explicit organizer/venue cancellation or change may supersede an
  older schedule;
- media/aggregators are fallback evidence and never silently override current
  official evidence;
- text disappearing from a page is not proof of cancellation: a volatile field
  becomes stale/unknown until explicit new evidence exists.

### Deterministic code

May:

- validate JSON/references/hashes/quotes;
- canonicalize URLs and exact ISO dates/times;
- enforce explicit year mismatch floor;
- enforce ticket role/identity compatibility;
- reject source leakage and unsupported candidate values;
- compute confidence;
- route ambiguous semantics to review.

May not:

- decide broad festival/event meaning by keywords;
- invent/repair titles/descriptions;
- merge events on title similarity alone;
- infer free admission from missing price;
- choose a winner in an unresolved semantic conflict.

## 7. Fail-closed gates

`ready_shadow` requires:

1. title has accepted current-edition claim;
2. no unsupported ordinal/status/modifier;
3. period has accepted evidence or remains explicitly unknown;
4. each program event has source-local identity;
5. each event ticket URL comes from `ticket_single_event` or an official
   program anchor tied to the same event/date;
6. subscription/pass never becomes event ticket;
7. rejected/ambiguous source contributes no final scalar;
8. all exact quotes pass hash-bound lookup;
9. incompatible accepted values are listed in `conflicts`;
10. source count/authority/coverage metrics are computed locally;
11. all strong event candidates have date + meaningful title; time/venue
    requirements follow the existing festival program rules;
12. program-only activities are not silently promoted to Event.
13. A/B agree on identity kind and programme profile, or C explicitly resolves
    the bounded conflict;
14. every subject from the union of Antigravity A/B and any scheduled
    Kaggle+Gemma inventory has an accepted, rejected or unresolved disposition;
15. taxonomy version/hash match the host registry and unmapped primary labels
    remain quarantined.
16. `continuity_only` Gemma fallback cannot auto-apply a v2 revision;
17. runtime fallback preserves the primary failure and uses the same frozen
    target/input fingerprint;
18. only one approved revision/apply lock may feed public projections.

Any critical failure => `needs_review`, never guessed repair.

Confidence is computed per field and overall is the minimum across critical
fields. Models do not return final confidence.

## 8. Apply boundary

### Shadow

- Antigravity lane changes no `festival`/`event`, makes no Smart Update calls
  and rebuilds no Telegraph/index;
- existing Kaggle+Gemma production behaviour remains active until the unified
  approval/apply coordinator is accepted;
- operator sees the independent Antigravity candidate and diff against current
  DB/Kaggle output;
- after the unified coordinator lands, neither collector writes public state
  directly; the distinction becomes lane role rather than separate writers.

### Approved apply

1. create one immutable approved festival edition revision from accepted claims;
2. persist source/provenance/decision manifests and activate it atomically;
3. create an apply plan for approved Event dispositions;
4. send every strong occurrence through `smart_event_update`;
5. send images through the existing event-media ingest/gate;
6. wait for terminal Smart Update outcomes;
7. generate legacy Festival/`activities_json` compatibility projections;
8. atomically generate festival index/detail/manifest projections and sync
   Telegraph compatibility surfaces;
9. only then mark linked queue rows `done`.

Partial apply:

- successful events are recorded;
- unresolved events remain `needs_review`;
- queue group is not `done` until required page/index state exists;
- rerun is idempotent by approved candidate hash.

## 9. Operator UX

Proposed commands reuse `/fest_queue`:

```text
/fest_queue web --shadow --limit 1
/fest_queue web --info
/fest_queue web --review <run_uid>
/fest_queue web --approve <run_uid>
/fest_queue web --reject <run_uid> <reason>
/fest_queue web --rerun <run_uid>
```

Review card:

- series/edition target and grouped queue IDs;
- accepted/rejected/ambiguous sources;
- field coverage;
- conflicts and unknowns;
- candidate diff against current `festival`;
- event create/update/program-only plan;
- Antigravity role/code, interaction/environment IDs, actual calls/tokens and
  limiter status;
- links to source ledger, claims, candidate and validation artifacts;
- explicit `Approve` / `Reject` actions.

No approval through a text coincidence; callback must carry `run_uid` and
candidate hash.

## 10. Quotas and concurrency

Preproduction defaults:

| Variable | Default |
|---|---:|
| `FESTIVAL_WEB_RESEARCH_ENABLED` | `0` |
| `FESTIVAL_WEB_RESEARCH_SHADOW` | `1` |
| `FESTIVAL_WEB_RESEARCH_AUTO_APPLY` | `0` |
| `FESTIVAL_WEB_RESEARCH_MAX_GROUPS_PER_RUN` | `2` |
| `FESTIVAL_WEB_RESEARCH_CONCURRENCY` | `1` |
| `FESTIVAL_WEB_RESEARCH_MAX_SOURCES` | `6` |
| `FESTIVAL_WEB_RESEARCH_CHECK_SOURCES` | `4` |
| `FESTIVAL_WEB_RESEARCH_AGENT_DAILY_CAP` | `12` |
| `FESTIVAL_WEB_RESEARCH_MAX_AGENT_CALLS_PER_GROUP` | `3` |
| `FESTIVAL_WEB_RESEARCH_PRIMARY_AGENT_TOKENS` | `20000` |
| `FESTIVAL_WEB_RESEARCH_CHECK_AGENT_TOKENS` | `12000` |
| `FESTIVAL_WEB_RESEARCH_ADJUDICATOR_TOKENS` | `8000` |
| `FESTIVAL_WEB_RESEARCH_PRIMARY_RESERVATION_TOKENS` | `50000` |
| `FESTIVAL_WEB_RESEARCH_CHECK_RESERVATION_TOKENS` | `30000` |
| `FESTIVAL_WEB_RESEARCH_ADJUDICATOR_RESERVATION_TOKENS` | `20000` |

All calls reserve/finalize through canonical
`antigravity-preview-05-2026` shared limiter. Feature cap `12 RPD` is inside
the global safe `90 RPD`. The table is a preproduction implementation contract:
the feature-local `12 RPD` cap and the new per-role reservations are not
runtime-enforced until this lane is implemented.

No parallel Antigravity calls. Scheduler uses actual finalized usage before
the next reservation. `max_total_tokens` remains best-effort and cannot replace
TPM accounting.

При feature cap `12 RPD` схема обрабатывает до шести обычных групп в день
(`A + B`) либо четыре группы, если каждой понадобился adjudicator (`A + B + C`).
Это осознанный отдельный бюджет внутри общего безопасного лимита `90 RPD`, а
не попытка израсходовать все 100 запросов на один фестиваль.

### Routing and hot-standby health

Before acceptance the route is `Kaggle active + Antigravity shadow`. After
acceptance the default is `Antigravity primary`; Kaggle+Gemma is scheduled only
for technical fallback, operator request and health/audit sampling. Unchanged
input fingerprint consumes no model requests.

After the unified coordinator cutover, automatic fallback is allowed only when
standby health is `green` **and** `fallback_capability=full_v2`:

```text
green   # fresh live canary, current kernel/schema/taxonomy adapter, artifacts
yellow  # manual fallback only; canary stale or partially degraded
red     # routing prohibited; credentials/model/config/runtime gate failed
```

`fallback_capability` is tracked separately:

```text
continuity_only   # collect current flat proposal for review; never direct-write after cutover
full_v2           # common evidence/profile/programme/detail contract passes
```

`continuity_only` is useful during migration and as an operator-visible
emergency research result, but it is not a functional automatic reserve for
the unified pipeline. It never bypasses the coordinator, never mutates the old
Festival/Telegraph surfaces directly after cutover and cannot activate a v2
revision. If no green `full_v2` standby exists, the last approved revision
keeps serving and the new queue group becomes `needs_review/retryable`.

Proposed drills:

- daily cheap config/render/distill/offline-schema smoke;
- weekly full Kaggle+Gemma live canary on a pinned source bundle;
- during the first 30 stable days, mirror every eligible target;
- steady state: bounded changed-target sample (initial policy: 20%, at least
  one successful live group per week);
- forced Antigravity-off failover before each canary expansion.

Antigravity may become primary only after the golden 14-bundle pack, five
manual live shadow targets, seven scheduled shadow days, a successful forced
failover through a green **`full_v2`** Kaggle standby, `0`
unsupported/stale/ticket-scope critical errors, `>=0.98` item-disposition
precision and `>=0.95` terminal-or-checkpoint recovery. A green
`continuity_only` canary does not satisfy the primary-switch gate.

### Refresh triggers and freshness

A new immutable research run is triggered by a new queue URL, source content
hash change, operator rerun or risk-window poll. Unchanged fingerprint reuses
the terminal result with zero provider calls.

Initial proposed cadence, subject to production measurements:

- dates-only/far future: about every 14 days;
- 60–14 days before start: about every 3 days;
- under 14 days or programme/sales actively changing: daily;
- final 72 hours for affected official/ticket sources: every 6–12 hours;
- after completion: one closure capture, then archive.

Identity/static claims may carry forward with provenance. Volatile time, venue,
ticket availability and lifecycle facts have separate freshness TTL. A changed
snapshot creates a new candidate/revision; collection failure never erases the
last approved data shown by the index/detail projections.

## 11. Observability

`ops_run.kind = festival_web_research`.

Required metrics:

```text
queue_rows_selected
targets_grouped
sources_fetched/reused/blocked
adapter_hits
playwright_renders
documents_parsed
source_accepted/rejected/ambiguous
claims_total/critical
taxonomy_a_b_agreement/conflicts/unmapped
programme_profile
programme_items_by_disposition
programme_inventory_unresolved
candidate_conflicts
stale_edition_blocks
ticket_role_blocks
antigravity_primary_calls
antigravity_checker_calls
antigravity_adjudicator_calls
agent_incomplete
agent_checkpoint_recovered
agent_actual_tokens
kaggle_gemma_lane_calls/tokens/schema_rejects
primary_lane_runtime_failures
fallback_attempts/successes/capability
standby_health/last_live_canary_age
ready_shadow/needs_review/rejected/failed
apply_events_create/update/program_only/skipped
```

Structured logs include `run_uid`, target key, queue IDs, source ID, model,
requested/final tokens, interaction/environment IDs and artifact paths. Never
log API keys, full credentials or personal data.

## 12. Preproduction eval pack

At least 14 real source bundles, two for each programme profile:

1. identity/dates only;
2. one compound festival visit with internal timetable;
3. independently ticketed multi-event programme;
4. one-ticket schedule-only stage programme;
5. hybrid event + schedule/programme-only source;
6. continuous fair/exhibition/zones;
7. distributed seasonal cycle.

Each pair contains an adversarial variant such as stale year, subscription vs
single ticket, generic JS shell, conflicting times, incomplete PDF/OCR, same
title on different dates, a current page with a separate historical section or
an ordinary event mislabeled as a festival.

Real names/contents stay in fixtures/artifacts and must not become reusable
prompt examples.

### Quality gates

- unsupported critical claims: `0`;
- stale-edition leakage: `0`;
- subscription/single-ticket mismatch: `0`;
- every non-null critical scalar has valid claim/quote: `100%`;
- differing explicit years never merge: `100%`;
- false-positive ordinary events never auto-create festival: `100%`;
- programme-profile exact match against reviewed gold: `100%`;
- item-disposition precision in shadow: `>= 0.98`;
- source-grounded programme inventory loss: `0`;
- rerun with same fingerprint creates no provider calls/writes;
- approved strong events enter only through Smart Update.

### Operational gates

- typical Antigravity calls/group: `2`, hard cap `3`;
- median agent tokens/group: `<= 60k`, p95 `<= 90k`;
- workspace state written before first network call;
- every successful fetch checkpointed immediately;
- checkpoint recovery succeeds for `status=incomplete`;
- group p95 wall time `<= 12 min`;
- zero limiter bypass;
- zero public DB/Telegraph mutations in shadow;
- no `/healthz`, `/webhook` or bot responsiveness regression.

The `2026-07-29` Antigravity 2+1 probe (`150,950` tokens, `0/3` terminal
outputs, but `3/3` known grounding errors caught from checkpoints) fails the
token/completion gate and is baseline evidence for the narrower roles above.
More precisely, B/C preserved raw source checkpoints, not completed semantic
ledgers; the errors were recovered through local/manual source review. A
structured run is not accepted until per-source decision/claim/disposition
checkpoints validate outside the sandbox.

## 13. Rollout

### Phase 0 — stabilize the existing Kaggle lane

- fix script-kernel run-config transport and run-ID round trip;
- align prompt with strict UDS schema; remove raw-unvalidated apply fallback;
- give festival kernel its own telemetry namespace/heartbeat/timeout;
- add live render→Gemma→artifact E2E and URL-programme→Smart Update coverage;
- keep current production writer behaviour until the unified coordinator is
  accepted.

### Phase 1 — common contracts and collectors

- migrations/tables;
- grouping and input fingerprint;
- adapters/fetch safety;
- evidence schemas/gates;
- Google gateway/Antigravity wrappers;
- collect-only UDS-v1→v2 adapter and Kaggle lane manifest;
- operator review UI;
- fixtures/tests.

### Phase 2 — offline replay

- run saved snapshots only;
- no network/provider calls where artifacts suffice;
- compare Kaggle+Gemma and Antigravity candidates against reviewed gold;
- freeze golden verdicts.

### Phase 3 — manual live shadow

- flag enabled only in local/preprod runtime;
- `/fest_queue web --shadow --limit 1`;
- five targets, operator review of every source/claim/diff;
- Kaggle+Gemma remains active production path; Antigravity makes no apply.

### Phase 4 — scheduled shadow and failover drill

- maximum two targets/run, concurrency one;
- seven consecutive days;
- no auto apply;
- daily summary and quota/token report;
- forced Antigravity-off run must succeed through a green Kaggle standby for
  early continuity; before Phase 7 the drill is repeated through `full_v2`.

### Phase 5 — unified staging apply

- separate DB snapshot and Telegraph token;
- approve at least five targets;
- verify Smart Update, program-only split, media gate and atomic index/detail
  projections from one effective revision.

### Phase 6 — production canary

- `AUTO_APPLY=0`;
- one manually approved target/day;
- Kaggle+Gemma remains active outside canary; inside the unified canary it is
  automatic fallback only when green and `full_v2`, otherwise the candidate
  remains unapplied for review/retry;
- only after quality/operational gates may approval-gated production apply
  expand.

### Phase 7 — primary switch and steady hot standby

- route eligible non-social URL groups to Antigravity primary;
- require Kaggle+Gemma `fallback_capability=full_v2` at cutover and keep it
  green through scheduled canaries/samples and failover drills;
- `continuity_only` or `yellow/red` standby cannot receive automatic fallback;
- monitor primary error/quota/checkpoint recovery and fallback rate;
- failback occurs only for a new target run after Antigravity health is stable,
  never by switching provider in the middle of one lane run.

Автоматический apply не входит в первый production rollout. Primary switch is
an explicit acceptance decision, not a consequence of merely enabling the
Antigravity flag.

## 14. Implementation map

```text
festival_web_research/
  contracts.py
  grouping.py
  preflight.py
  fetch.py
  adapters/
  snapshots.py
  taxonomy.py
  antigravity_primary.py
  antigravity_checker.py
  antigravity_adjudicator.py
  reconcile.py
  gates.py
  apply.py
  service.py
  reporting.py

alembic/versions/<...>_festival_web_research.py
models.py
festival_queue.py
source_parsing/festival_parser.py        # retained Kaggle collector + v2 adapter
main_part2.py                            # operator commands/callbacks
scheduling.py                            # shadow job behind flag
```

Required tests:

```text
tests/test_festival_web_grouping.py
tests/test_festival_web_preflight.py
tests/test_festival_web_evidence.py
tests/test_festival_web_ticket_roles.py
tests/test_festival_web_state_machine.py
tests/test_festival_web_apply.py
tests/test_festival_web_quota.py
tests/e2e/features/festival_web_research.feature
```

## Definition of Ready for implementation

- scope remains URL/non-social only;
- storage contour accepted: Fly SQLite + existing parser object storage;
- existing Kaggle+Gemma blocking config/schema/E2E gaps are owned explicitly;
- Antigravity preproduction starts shadow-only while Kaggle remains active;
- Antigravity A+B and optional C roles plus common Kaggle v2 adapter accepted;
- primary-switch and green `full_v2` hot-standby/failover contract accepted;
- operator approval required for every apply;
- the new dual-lane scheduler and unified production apply remain disabled
  until rollout gates pass; existing queue behaviour is not disabled by this
  design document.
