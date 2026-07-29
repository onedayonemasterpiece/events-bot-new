# Festival Web Research: preproduction design

Статус: `proposed preproduction design`, реализация и rollout ещё не выполнены.

Область: `festival_queue.source_kind=url` и все найденные из таких страниц
несоциальные источники. Telegram/VK intake, Telegram Monitoring и VK auto
import этим контуром не меняются.

## Цель

Получать из нескольких сайтов и документов один evidence-backed пакет выпуска
фестиваля, который:

- не смешивает программы разных лет;
- различает официальный сайт, отдельное событие, билет, абонемент, СМИ и
  агрегатор;
- не добавляет неподтверждённые номера/статусы в название;
- сохраняет точные claims и цитаты;
- разделяет самостоятельные события и program-only активности;
- до ручного approval ничего не меняет в `festival`, `event`, Telegraph и
  публичных индексах;
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

Текущий Universal Festival Parser остаётся baseline, но его v1-контракт нельзя
сразу использовать как authority для нового контура:

- один URL за запуск;
- `GREEDY` prompt максимизирует извлечение, а не precision;
- officiality частично определяется наличием `fest` в домене;
- второй LLM-pass вызывается только при missing fields;
- нет claim/source/edition ledger;
- model confidence является self-grade;
- UDS напрямую upsert-ится до внешнего evidence gate.

## Главные решения

1. **Не создавать вторую intake-очередь.** `festival_queue` остаётся
   каноническим входом.
2. **Группировать URL rows до fetch/LLM.** Одна группа выпуска — один research
   target.
3. **Research state хранить в core Fly SQLite.** Это canonical operational
   state, не personalization Supabase и не YDB.
4. **Большие snapshots хранить в существующем Supabase Storage bucket
   `festival-parsing`.**
5. **Antigravity — основной semantic research runtime.** Первый независимый
   агент строит source/claim ledger, второй в новой песочнице ищет
   контрдоказательства, третий вызывается только для реального конфликта.
6. **Локальный код только ограничивает вход и проверяет доказательства.** Он
   fetch/mount-ит seed sources, хранит hashes/checkpoints и валидирует
   JSON/references/quotes, но не подменяет агентское смысловое исследование.
7. **Legacy Gemma 3 Universal Parser — только comparison baseline.** Gemma 4
   не входит в candidate critical path этого Antigravity-first контура.
8. **Local evidence gate является authority.** Antigravity не выставляет
   publishable/confidence самостоятельно.
9. **Preproduction всегда shadow и approval-gated.**

## End-to-end flow

```text
festival_queue URL rows
  -> group target edition
  -> deterministic URL preflight
  -> bounded adapter/fetch/render/document snapshots
  -> Antigravity A: primary research + atomic claims + candidate
  -> Antigravity B: independent fresh-sandbox counter-evidence
  -> local compare
       agreement -> deterministic final gate
       conflict  -> Antigravity C: optional compact adjudicator
  -> shadow diff + operator review
  -> approve/reject
  -> approved Festival update + Smart Update event plan
  -> Telegraph/index sync
```

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
other
```

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

All three tables belong to core Fly SQLite.

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
prompt_version
primary_agent_code
checker_agent_code
primary_queue_item_id
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
run_id
source_id
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
PRIMARY KEY (run_id, source_id)
```

Full source text, HTML, screenshots, documents, claims, LLM logs,
counter-evidence and adjudication packets remain in:

```text
festival-parsing/web-research/<target_slug>/<run_uid>/
```

SQLite stores only bounded manifests/hashes and the final candidate summary.

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
discovered
-> grouped
-> fetching
-> researched_primary
-> challenged
-> validating
-> ready_shadow
-> approved
-> applying
-> applied

Any active state -> retryable | needs_review | rejected | failed
```

Rules:

- shadow run never changes queue `status`;
- same input fingerprint reuses terminal shadow run;
- content hash change creates a new run;
- stale lease is recoverable without duplicating provider requests;
- agent `incomplete` with valid checkpoints is not automatically `failed`;
- no automatic fourth Antigravity interaction;
- queue rows become `done` only after approved apply, Smart Update terminal
  outcomes and festival page/index sync.

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
  "decision_quotes": ["verbatim"],
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
  "verbatim_quote": "..."
}
```

Local validator requires that quote exists inside the exact
`content_sha256` snapshot.

### Candidate

Every scalar:

```json
{
  "value": "...",
  "claim_ids": ["C0001"],
  "status": "supported|conflict|unknown"
}
```

Candidate cannot contain a value absent from claims.

### Counter-evidence

Independent Antigravity checker receives target/seed only, not candidate:

```json
{
  "challenges": [{
    "field": "festival.title",
    "challenger_value": "...",
    "source_url": "...",
    "quote": "...",
    "reason": "stale_edition|unsupported_modifier|ticket_scope|other"
  }]
}
```

It does one search query, fetches at most four pages and checkpoints each page
immediately.

### Adjudication packet

Only conflicting claim values, exact quotes, hashes and at most two cited URLs.
No full HTML, no full raw candidate, no broad search.

## 6. Antigravity-first runtime

### Call A — primary researcher

Каждая festival group получает отдельную свежую Antigravity environment.
Агент видит target manifest и bounded snapshots исходных URL, но не результат
legacy parser.

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
7. собрать candidate только из этих claims.

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
4. checkpoint-ить каждый источник немедленно;
5. вернуть собственный source/claim ledger и counter-evidence, а не оценку
   уверенности первого агента.

Два вызова Antigravity — нормальный путь для каждой группы, а не исключение.

### Call C — optional adjudicator

Вызывается только если local compare нашёл критический конфликт A/B.
Получает компактный claim diff: значения, exact quotes, hashes и не более двух
цитируемых URL. Полные страницы, исходные candidates и broad search не
передаются. Search выключен; допустимо максимум два reopen указанных URL.
Результат — `supported|unknown|conflict`, без создания новых фактов.

Никакого автоматического четвёртого вызова нет. `status=incomplete` у любого
агента допустим, если обязательные checkpoints уже записаны и проходят
локальную проверку.

### Semantic ownership

Antigravity A/B/C отвечают за:

- festival vs event vs program-only;
- source role and edition match;
- atomic claim extraction;
- event identity/reconciliation;
- explicit conflict classification.

Legacy Gemma 3 result может быть создан отдельно только для shadow comparison
и не смешивается с Antigravity candidate.

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

Any critical failure => `needs_review`, never guessed repair.

Confidence is computed per field and overall is the minimum across critical
fields. Models do not return final confidence.

## 8. Apply boundary

### Shadow

- no changes to `festival`/`event`;
- no Smart Update calls;
- no Telegraph/index rebuild;
- queue items stay pending;
- operator sees diff against current DB.

### Approved apply

1. create/update festival edition from accepted claims;
2. persist source/provenance manifest;
3. store weak/program-only entries in `activities_json`;
4. create an apply plan for strong occurrences;
5. send every strong occurrence through `smart_event_update`;
6. send images through the existing event-media ingest/gate;
7. wait for terminal Smart Update outcomes;
8. sync festival page and index;
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
| `FESTIVAL_WEB_RESEARCH_PRIMARY_AGENT_TOKENS` | `25000` |
| `FESTIVAL_WEB_RESEARCH_CHECK_AGENT_TOKENS` | `18000` |
| `FESTIVAL_WEB_RESEARCH_ADJUDICATOR_TOKENS` | `12000` |

All calls reserve/finalize through canonical
`antigravity-preview-05-2026` shared limiter. Feature cap `12 RPD` is inside
the global safe `90 RPD`.

No parallel Antigravity calls. Scheduler uses actual finalized usage before
the next reservation. `max_total_tokens` remains best-effort and cannot replace
TPM accounting.

При feature cap `12 RPD` схема обрабатывает до шести обычных групп в день
(`A + B`) либо четыре группы, если каждой понадобился adjudicator (`A + B + C`).
Это осознанный отдельный бюджет внутри общего безопасного лимита `90 RPD`, а
не попытка израсходовать все 100 запросов на один фестиваль.

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
candidate_conflicts
stale_edition_blocks
ticket_role_blocks
agent_primary_calls
agent_checker_calls
agent_adjudicator_calls
agent_incomplete
agent_checkpoint_recovered
agent_actual_tokens
baseline_parser_calls/tokens/schema_rejects
ready_shadow/needs_review/rejected/failed
apply_events_create/update/program_only/skipped
```

Structured logs include `run_uid`, target key, queue IDs, source ID, model,
requested/final tokens, interaction/environment IDs and artifact paths. Never
log API keys, full credentials or personal data.

## 12. Preproduction eval pack

At least 12 real source bundles:

1. current official program + stale organizer archive;
2. multi-day program + subscription + three single tickets;
3. multiple ticket occurrence URLs for one series;
4. official homepage + separate venue event pages;
5. regional tourism page only;
6. Qtickets event that is a true festival;
7. false-positive ordinary performance/person birthday;
8. explicit prior-year tourism page;
9. JS-only ticket shell;
10. PDF-first program;
11. image/OCR-first program;
12. same series with two explicit years.

Real names/contents stay in fixtures/artifacts and must not become reusable
prompt examples.

### Quality gates

- unsupported critical claims: `0`;
- stale-edition leakage: `0`;
- subscription/single-ticket mismatch: `0`;
- every non-null critical scalar has valid claim/quote: `100%`;
- differing explicit years never merge: `100%`;
- false-positive ordinary events never auto-create festival: `100%`;
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

## 13. Rollout

### Phase 0 — implementation

- migrations/tables;
- grouping and input fingerprint;
- adapters/fetch safety;
- evidence schemas/gates;
- Google gateway/Antigravity wrappers;
- operator review UI;
- fixtures/tests.

### Phase 1 — offline replay

- run saved snapshots only;
- no network/provider calls where artifacts suffice;
- compare current Universal Festival Parser vs candidate;
- freeze golden verdicts.

### Phase 2 — manual live shadow

- flag enabled only in local/preprod runtime;
- `/fest_queue web --shadow --limit 1`;
- five targets, operator review of every source/claim/diff;
- no apply.

### Phase 3 — scheduled shadow

- maximum two targets/run, concurrency one;
- seven consecutive days;
- no auto apply;
- daily summary and quota/token report.

### Phase 4 — staging apply

- separate DB snapshot and Telegraph token;
- approve at least five targets;
- verify Smart Update, program-only split, media gate, page/index.

### Phase 5 — production canary

- `AUTO_APPLY=0`;
- one manually approved target/day;
- rollback is feature flag off; baseline URL parser remains available;
- only after quality/operational gates may approval-gated production apply
  expand.

Автоматический apply не входит в первый production rollout.

## 14. Implementation map

```text
festival_web_research/
  contracts.py
  grouping.py
  preflight.py
  fetch.py
  adapters/
  snapshots.py
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
source_parsing/festival_parser.py        # baseline adapter/reuse only
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
- preproduction starts shadow-only;
- Antigravity-first two-pass research and optional adjudicator roles accepted;
- operator approval required for every apply;
- scheduler and production apply remain disabled until rollout gates pass.
