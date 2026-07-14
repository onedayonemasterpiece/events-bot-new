# External consultant review pack: product E2E personalization

> **Purpose:** дать следующему eligible external agent один вход, правильный порядок чтения и проверяемый deliverable. Этот pack не заменяет самостоятельный audit кода/ветки.
>
> **Target branch:** `feature/personalization-product-e2e-design`.

> **Review status (2026-07-14):** accepted eligible reviews: **0 / 2**. A
> `Gemini 3.1 Pro (High)` Antigravity attempt was run against
> `b90cdaf0d77e67ba097a771f07122930a5a3a4da`, but rejected because its claimed
> reading ledger did not match the transcript and no single response inspected
> the mandatory docs, schemas, implementation, migrations and intake. Treat the
> [attempt record](reviews/product-e2e/supplementary-gemini-pro-audit-attempt-2026-07-14.md)
> only as supplementary probe material.

## 1. Eligibility и provenance

Полноценным external consultant review считается только ответ:

- `gemini-3-pro-preview` или `gemini-3.1-pro-preview`; либо
- Opus через `a-opus`/Antigravity; либо
- project Claude Code alias `Opus`.

В review обязательно записать provider, exact model ID/class, дату, reviewed branch и commit SHA, prompt/pack version, доступ к репозиторию, ограничения, raw-capture hash/path и список реально открытых файлов. Flash/Lite/Gemma/OpenAI и ответ без подтверждаемой provenance могут быть только `supplementary probe material`.

## 2. Reading order с прямыми GitHub-ссылками

Repository branch root: [feature/personalization-product-e2e-design](https://github.com/onedayonemasterpiece/events-bot-new/tree/feature/personalization-product-e2e-design).

### A. Правила и задача

1. [AGENTS.md](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/AGENTS.md)
2. [docs/README.md](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/docs/README.md)
3. [docs/routes.yml](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/docs/routes.yml)
4. [Product E2E research brief](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/docs/features/unsigned-personalization/product-e2e-research-brief.md)

### B. Канонический Phase A design и новые project decisions

5. [Phase A audit/design](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/docs/features/unsigned-personalization/product-e2e-design.md)
6. [Golden personas v0 + strict real-data protocol](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/docs/features/unsigned-personalization/golden-personas-real-data-v0.md)
7. [Database sustainability E2E gate](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/docs/features/unsigned-personalization/database-sustainability-e2e.md)
8. [Golden-persona JSON Schema](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/docs/features/unsigned-personalization/schemas/product-e2e/golden-persona-v0.schema.json)
9. [Real catalog timeline JSON Schema](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/docs/features/unsigned-personalization/schemas/product-e2e/catalog-timeline-v0.schema.json)

### C. Data/runtime boundaries

10. [Personalization data ownership ADR](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/docs/architecture/personalization-data-ownership.md)
11. [Feature family README/current status](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/docs/features/unsigned-personalization/README.md)
12. [Database draft and retention intent](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/docs/features/unsigned-personalization/database.md)
13. [Production integration/thin-runtime gates](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/docs/features/unsigned-personalization/production-integration.md)
14. [Event-detail related contract](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/docs/features/unsigned-personalization/event-detail-related.md)
15. [Personal-feed architecture](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/docs/features/unsigned-personalization/personal-feed-architecture.md)
16. [Semantic vector retrieval](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/docs/features/unsigned-personalization/semantic-vector-retrieval.md)
17. [Bot/automation exclusion contract](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/docs/features/unsigned-personalization/bots-and-automation.md)

### D. Executable truth, а не только документы

18. [Actual Astro event layout](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/site/src/layouts/EventLayout.astro)
19. [Reference personalization client](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/static_site/personalization/personalization.js)
20. [Current Playwright contract](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/tests/playwright/static_personalization_contract.spec.ts)
21. [Draft Gherkin, currently without executable steps](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/tests/e2e/features/static_site_personalization.feature)
22. [Supabase migrations](https://github.com/onedayonemasterpiece/events-bot-new/tree/feature/personalization-product-e2e-design/supabase/migrations)
23. [Vector sync path](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/scripts/sync_event_search_vectors_to_supabase.py)

### E. Предыдущий intake — читать последним и критически

24. [User-supplied supplementary research intake, provenance pending](https://github.com/onedayonemasterpiece/events-bot-new/blob/feature/personalization-product-e2e-design/docs/features/unsigned-personalization/reviews/product-e2e/supplementary-research-intake-2026-07-14-ru.md)

Этот memo не исследовал текущую feature-ветку напрямую и не имеет подтверждённой model provenance. Его target numbers, probabilities, maturity counts, synthetic catalog examples и verdict — proposals, а не accepted project facts.

## 3. Неподлежащие ослаблению constraints

1. Golden personas — контролируемые actors без реального PII; их profile должен возникать из UI-действий, а не direct injection.
2. Event/catalog data — только реальные frozen production records. Нельзя придумывать события, факты, даты, цены, города, accessibility, отмены и lifecycle transitions.
3. Future modeling разрешает менять `as_of` поверх captured real records и смотреть, что реально остаётся eligible; time-travel не предсказывает будущий ingest.
4. Fly SQLite остаётся canonical event owner и read-only input. Supabase — bounded current personalization state. YDB — eventual de-identified analytics/history с TTL, не второй профиль.
5. `NO_RELEVANT_CATALOG_SUPPLY`, `INSUFFICIENT_REAL_SNAPSHOT`, candidate miss, ranking miss и presentation miss — разные outcomes.
6. Static site/CTA остаются usable при отказе personalization paths.
7. Ни один новый KPI threshold, behavior probability или release gate не считается финальным без evidence и accept/adapt/reject synthesis.

## 4. Вопросы review

External agent должен дать конкретную взаимную критику, а не пересказ:

1. Достаточна ли 12-actor panel по age/life-context/interests; где остаются stereotypes или пропущенные cohorts/access needs?
2. Какие persona fields допустимо использовать evaluator, но запрещено ranker, чтобы избежать leakage?
3. Как строго построить sealed holdouts и hard negatives только из real event snapshots при небольшом региональном каталоге?
4. Верен ли primary denominator от canonical catalog supply; как разделить supply, eligibility, candidate, rank и presentation blame?
5. Encounter или meaningful action должен быть primary endpoint; как учитывать abandonment/competing risk?
6. Какие maturity rules минимальны и не подгоняют outcome; что нельзя фиксировать до consented-log calibration?
7. Как разделить pure evaluator, process simulator, DB integration и Playwright, чтобы browser test не стал дорогой evaluation platform?
8. Как доказать time-travel fidelity, когда snapshot не содержит исторической availability/cancellation chronology?
9. Какие anti-bubble/novelty metrics actionable для малого regional catalog, а какие создают metric theater?
10. Достаточны ли Supabase/YDB growth formulas, attribution, cleanup/TTL evidence и 365-day projection; какие failure gates пропущены?
11. Как не переобучиться на golden actors и spent holdouts после многократного tuning?
12. Какой минимальный implementation slice создаст первый честный closed loop от UI actions до paired product outcome без преждевременного canary?
13. Какие предложения supplementary memo следует `accept`, `adapt`, `reject` или `defer`, с указанием точного project evidence?
14. Какие claims в текущем Phase A design противоречат коду/миграциям на reviewed SHA?

## 5. Обязательный deliverable

Сохранить review как:

```text
docs/features/unsigned-personalization/reviews/product-e2e/
  consultant-<provider>-<exact-model>-<yyyy-mm-dd>.md
```

Минимальная структура:

1. provenance, eligibility, reviewed SHA и список открытых файлов;
2. executive verdict с чёткой границей implemented/design-only/missing;
3. persona-panel critique и предложенный diff;
4. strict real-data snapshot/time-travel/holdout protocol critique;
5. KPI/maturity/statistical critique;
6. Supabase/YDB sustainability and ownership critique;
7. first implementation slice и dependency order;
8. `accept/adapt/reject/defer` table для material proposals;
9. risks/blockers и вопросы второму consultant;
10. exact doc/schema/test diffs, которые должны последовать.

Review не должен менять production code, schema, ranking weights или deploy. Два eligible reviews затем сводятся в `reviews/product-e2e/synthesis.md`; Phase B начинается только после разрешения blocking decisions.

## 6. Готовый prompt

```text
Проведи независимый deep architecture/product-E2E review ветки
feature/personalization-product-e2e-design репозитория
onedayonemasterpiece/events-bot-new. Прочитай все материалы в порядке из
external-consultant-review-pack.md, проверь actual code/migrations/tests на
конкретном SHA и только затем прочитай supplementary intake.

Жёсткие условия: golden personas — controlled non-PII actors; все event/catalog
facts и lifecycle states — только frozen real production records; future replay
только сдвигает as_of поверх доказуемых записей; Fly SQLite остаётся canonical
source, Supabase — bounded current personalization state, YDB — de-identified
TTL analytics/history. Не принимай synthetic catalog worlds, direct final-profile
injection, unproven KPI thresholds или provider quota assumptions.

Верни provenance-qualified review в требуемой структуре, с exact file/line/code
evidence, critique 12-persona panel, holdout/time-travel protocol, KPI/maturity,
Supabase/YDB 30/90/365 sustainability gates и concrete first implementation
slice. Для каждого material proposal supplementary memo выставь
accept/adapt/reject/defer. Не изменяй production и не называй design-only
контракт реализованным.
```
