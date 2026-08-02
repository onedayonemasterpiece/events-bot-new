# Подборки статического сайта: план исправления качества и передачи кодовому агенту

Статус: **PR A IMPLEMENTED / PR B–E REQUEST CHANGES**, 2026-08-02.
Целевая ветка: `agent/static-collections-quality-e2e`.
Исходный аналитический пакет: `agent/static-collections-review/curation` @
`e1b854f032a4593e5b3603396e6d61358315ad23`.

Этот документ является конкретным заданием следующему кодовому агенту. Он не
разрешает публикацию semantic-подборок и не заменяет owner/editor review.

PR A status (2026-08-02): **implemented; review contract PASS; publication
blocked**. Provisional seed, ontology v2, source-bound receipts и occurrence
families теперь проверяются промежуточным `review` mode. Owner gold/all-event
scores (PR B), grounded adjudicator/manifests (PR C), browser candidate (PR D)
и публичное включение (PR E) не выполнены. Optional `active_experiences` не
добавлен без отдельного owner product decision.

## 1. Цель следующей итерации

Нужно довести контур подборок до состояния, в котором:

1. продуктовые значения labels непротиворечивы;
2. review seed отделён от принятого owner gold;
3. каждая ручная метка имеет проверяемый source provenance и occurrence family;
4. BGE используется как recall/disagreement detector, а не как недоказанная
   publication truth;
5. метрики воспроизводятся из зафиксированного score artifact;
6. Astro принимает только ID-only manifests и не переопределяет membership;
7. PR, scheduled и browser E2E проверки ловят деградацию до публичного релиза;
8. любой неполный, устаревший или некалиброванный label остаётся fail-closed.

## 2. Неизменяемые ограничения

- Один существующий StaticSiteBuilder после quiet window Smart Update.
- Один immutable Fly SQLite snapshot на build.
- Один shared BGE artifact; отдельный encoder/notebook для каждой подборки
  запрещён.
- Никакого BGE/LLM при открытии страницы.
- Никакого повторного чтения core events из Supabase ради подборок.
- Cinema sources и festival extraction/pages остаются вне этого scope.
- Failed candidate не заменяет last-good public tree пустой или disabled-выдачей.
- Semantic label не появляется в navigation/sitemap без owner gold и quality
  pass.
- Текущая ветка curation является evidence, а не merge-base. Реализация ведётся
  от свежего `main`.

## 3. Обязательные work packages

### Q0. Сохранить fail-closed до конца работ

Проверить и оставить:

- `publication = blocked` для `unusual`, `science`,
  `strong_impressions`, `medieval` и BGE audience-candidate heads;
- `publication_eligible = false` для provisional seed;
- отсутствие blocked labels в navigation и sitemap;
- last-good manifests не стираются из-за provider/model failure;
- Astro не строит membership из title, description, topics или similarity.

**Acceptance**

- unit test доказывает, что `compute_status=pass` не меняет
  `publication_status=blocked`;
- browser E2E доказывает отсутствие blocked route в navigation/sitemap;
- direct blocked route либо 404, либо noindex page с явным
  `data-publication-status="blocked"`.

### Q1. Разделить review seed и owner gold

Legacy-файл до PR A:

```text
tests/fixtures/static_collections_gold_v1.json
```

не является gold. Выполнить миграцию:

```text
docs/review-data/static_collections_review_seed_v1.json
schema_version = static-collections-review-seed-v1
status = provisional_agent_seed_not_owner_approved
publication_eligible = false
```

После независимого owner/editor review создать отдельный immutable файл:

```text
tests/fixtures/static_collections_owner_gold_v1.json
schema_version = static-collections-owner-gold-v1
status = owner_reviewed
publication_eligible = false
```

`owner gold` сам по себе также не публикует label. Он только разрешает
калибровку. Publication разрешает отдельный quality receipt, связанный с hashes
gold, score artifact, policy, prototypes и catalog snapshot.

Legacy-файл удалён после миграции workflow/imports/tests. Нельзя превращать
provisional seed в gold заменой одного поля.

**Acceptance**

- ни один production module не импортирует review seed;
- review seed используется только reviewer tooling;
- owner gold используется только evaluation/calibration;
- изменение owner gold требует отдельного review diff;
- hashes обоих файлов отражены в evaluation receipt.

### Q2. Исправить онтологию labels до новой калибровки

#### Q2.1. Детская и семейная аудитория

Внутренние factual labels:

```text
child_directed
family_suitable
joint_family_activity
```

Определения:

- `child_directed`: ребёнок является прямым целевым зрителем **или**
  участником программы;
- `family_suitable`: взрослые и дети прямо приглашены на совместное посещение;
- `joint_family_activity`: взрослый и ребёнок выполняют общую практику/задание.

Публичная `/detyam/` — объединение принятых `child_directed` и
`family_suitable`, дедуплицированное по occurrence family. Внутри страницы
допустим facet «делать вместе» из `joint_family_activity`.

Возрастной рейтинг, детские авторы, детская тема, слово «семейный», встреча
только для родителей и «семейная атмосфера» не являются достаточным evidence.

#### Q2.2. Наука

Не смешивать:

```text
science_pop
research_in_action
```

- `science_pop`: доказательное объяснение науки, научная лекция, научная
  документалистика, демонстрация объяснённого явления;
- `research_in_action`: научный метод, первичные данные/источники, полевая работа,
  устройство эксперимента, исследовательская практика.

Публичный first release — `science_pop`, если supply и quality подтверждены.
Узкая `research_in_action` остаётся shadow до достаточного supply. Название
публичной полки «Наука» нельзя использовать для обычной исторической выставки
только потому, что в ней есть архивные предметы.

#### Q2.3. Сильные впечатления

Сохранить строгий label:

```text
strong_impressions
```

Positive требует минимум одного основания:

- редкий доступ, недоступный обычному посетителю;
- иммерсивная смена роли зрителя;
- физически или эмоционально интенсивный маршрут/испытание;
- редкая site-specific практика с доказанным личным участием.

Обычный мастер-класс, йога, публичный спортивный день, концерт со свечами,
рекламные слова «яркий/незабываемый» и любое действие посетителя сами по себе
недостаточны.

Если нужен широкий пользовательский intent, создать отдельный label:

```text
active_experiences
```

с публичным названием «Попробовать самому». Не размывать ради него
`strong_impressions`.

#### Q2.4. Средневековье

`medieval` означает, что Средневековье, рыцарская культура, замок как предмет
программы, викинги или living history являются содержанием события.

Недостаточно:

- событие просто проходит в старом замке;
- фэнтези использует рыцарские образы;
- один номер концерта относится к Средневековью;
- историческая программа посвящена другому периоду.

При недостатке независимых families label остаётся seasonal/shadow и не
попадает в основную navigation.

**Acceptance для Q2**

- definitions находятся в одном versioned policy contract;
- prototypes и adjudicator schema ссылаются на те же definitions;
- fixture rows используют новые labels;
- tests содержат positive, easy negative и adversarial negative для каждой
  границы;
- изменение definition повышает policy version и инвалидирует старые decisions.

### Q3. Исправить data-quality defects до обучения порогов

Обязательно вручную перепроверить source rows и canonical descriptions:

| Event | Проверка |
|---:|---|
| 5757 | description смешивает «Собачье сердце» и экскурсию по закулисью |
| 6696 / 6766 | возможный дубль и конфликт venue для пинхол-выставки |
| 6878 | title про травничество, excerpt преимущественно про обложку блокнота |
| 7307 | `family_laser_tag_tournament` не подтверждается сохранённым evidence |
| 7326 | family/children reason не подтверждается сохранённым excerpt |
| 7333 / 7344 | одна festival family с разным BGE outcome |
| 5781 / 7238 | одна экскурсионная family |
| 7373 / 7374 | одна drakkar family |

Добавить deterministic coherence report:

- title ↔ source title;
- canonical description ↔ source snippets;
- venue ↔ source venue;
- event type ↔ source content;
- reason code ↔ quoted source evidence;
- linked occurrences ↔ family identity;
- duplicate title/date/venue/source candidates;
- generated text contamination между соседними событиями.

Нельзя автоматически менять canonical event только из-за heuristic warning.
Результат: `pass | needs_source_review | corrected`, с before/after receipt.

**Acceptance**

- перечисленные event IDs имеют отдельные review receipts;
- `needs_source_review` не допускается в owner gold;
- исправление canonical data проходит обычный Smart Update/data integrity path;
- review seed пересобран только после исправлений.

### Q4. Сделать review rows воспроизводимыми

Каждая positive/hard-negative строка должна иметь:

```json
{
  "event_id": 123,
  "family_id": "explicit-or-reviewed-family-id",
  "title": "...",
  "occurrence_date": "2026-08-01",
  "expected": "positive_candidate",
  "confidence": "high",
  "review_decision": "keep",
  "reason_code": "...",
  "source_refs": [
    {
      "source_type": "telegram|vk|official_site|parser|db",
      "source_url": "...",
      "source_id": "..."
    }
  ],
  "source_quote": "...",
  "model_document_hash": "sha256",
  "positive_score": 0.0,
  "negative_score": 0.0,
  "margin": 0.0,
  "winning_positive_prototype_id": "...",
  "winning_negative_prototype_id": "...",
  "bge_selected": false
}
```

На верхнем уровне:

```text
catalog_hash
snapshot_id + snapshot_sha256
batch_generated_at
model_id + revision + encoder contract
policy_sha256
prototype_bank_sha256
vector_artifact_sha256
score_artifact_sha256
generator_repo_sha
generator_command
```

`source_quote` должен происходить из raw/source-bound evidence, а не только из
LLM-generated canonical description.

**Acceptance**

- любой reviewer может пересчитать `margin`;
- score для невыбранных событий также сохранён;
- report показывает winning positive/negative prototypes;
- artifact mismatch завершает evaluation с ошибкой, а не warning.

### Q5. Перейти на family-weighted evaluation

Occurrence rows не считаются независимыми примерами.

Порядок identity:

1. accepted reciprocal `linked_event_ids`;
2. reviewed canonical family override;
3. exact source/organizer production identity;
4. только для review queue — conservative duplicate candidate;
5. совпадение title само по себе не создаёт accepted family.

Сохранять отдельно:

```text
occurrence_count
family_count
mutual_explicit_component_count
unresolved_family_candidates
```

Positive и hard-negative одной family в одном label запрещены.

**Acceptance**

- `7373/7374`, `5781/7238`, `7333/7344` не увеличивают размер evaluation как
  независимые examples;
- тест падает при family leakage;
- UI calendar может показывать per-date, collection page — per-family, detail
  page — selector других дат.

### Q6. Пересчитать модели и метрики без подмены понятий

Не называть ошибку на curated hard negatives production FPR. Использовать:

```text
hard_negative_challenge_error_rate
```

Отдельно считать:

1. `family_recall` на owner-approved positives;
2. `hard_negative_challenge_error_rate`;
3. `precision_at_page_size` на полном candidate output;
4. `random_output_precision` на стратифицированной ручной выборке;
5. temporal holdout по новым event families;
6. coverage/unknown/adjudication rate;
7. duplicate-family rate.

Минимум до первого semantic public label:

- calibration: не менее 30 positive families и 60 hard-negative families;
- temporal holdout: не менее 15 positive и 30 negative новых families;
- `family_recall >= 0.85`;
- `precision@20 >= 0.90`;
- `hard_negative_challenge_error_rate <= 0.10`;
- `duplicate_family_rate = 0`;
- все ошибки top-20 разобраны owner/editor;
- threshold выбирается на calibration set, итоговая оценка — на holdout.

Для `medieval`, если supply не достигает минимума, корректный результат —
`blocked_insufficient_supply`, а не расширение смысла.

Для audience BGE:

- `candidate_recall >= 0.95`;
- publication truth остаётся source-grounded factual decision;
- BGE-only строка уходит в adjudication, но не публикуется.

### Q7. Добавить компактный grounded adjudicator

Для субъективных semantic labels использовать:

```text
deterministic signals + BGE candidate union
  -> cached source-grounded adjudicator
  -> accepted | rejected | unknown
  -> ID-only manifest
```

Ограничения:

- только новые/изменившиеся candidate families;
- input hash включает source evidence, definition/policy и family identity;
- строгий JSON schema;
- обязательная `evidence_quote`;
- provider failure/abstention => `unknown`;
- `unknown` не стирает last accepted decision;
- ручной override versioned и выше автоматического решения;
- LLM не вызывается для всего каталога на каждой сборке;
- BGE score не становится publication truth.

### Q8. Зафиксировать HTML/E2E contract для Astro

Каждая collection page должна иметь:

```html
<main
  data-static-collection-page
  data-collection-label="science_pop"
  data-publication-status="public|shadow|blocked"
  data-catalog-hash="..."
  data-manifest-hash="..."
>
```

Каждая карточка:

```html
<article
  data-event-card
  data-event-id="123"
  data-family-id="family-..."
>
```

Navigation:

```html
<nav data-static-collection-nav>
```

Состояния:

```html
data-collection-state="ready|empty|last-good|blocked|degraded"
```

Требования:

- один `event_id` на странице не дублируется;
- одна family не создаёт несколько карточек;
- blocked label отсутствует в navigation/sitemap;
- shadow route имеет noindex и отсутствует в navigation/sitemap;
- public route indexable и присутствует там, где требует route contract;
- card URL ведёт на существующую canonical event page;
- empty/degraded не маскируется под успешную пустую подборку;
- last-good явно отражён в manifest receipt, но не обязан показывать
  технический текст пользователю.

### Q9. Включить CI и scheduled E2E по этапам

Добавленный workflow:

```text
.github/workflows/static-collections-quality-e2e.yml
```

Контуры:

1. **contract** — каждый PR/push и ежедневно:
   - policy/seed fail-closed validation;
   - schema, IDs, counts, disjointness;
   - warning о legacy gold naming/provenance;
   - Node behavior tests browser checker;
   - JSON/Markdown report artifact.

2. **browser** — schedule/manual и после готовности URL:
   - Playwright Chromium;
   - public/shadow/blocked route contract;
   - navigation/sitemap/indexability;
   - duplicate event/family;
   - links, images, console/page errors;
   - desktop/mobile overflow;
   - screenshots/report on failure.

3. **gate** — единый required result:
   - contract обязан пройти всегда;
   - browser обязателен после
     `STATIC_COLLECTIONS_E2E_REQUIRED=true`.

До настройки URL browser job помечается явным `not configured`, но не выдаётся
за выполненный E2E. После подключения URL обязательность должна быть включена
в тот же PR.

## 4. Конкретный порядок реализации

### PR A — contracts and data repair

1. Rebase/merge current `main`.
2. Перенести legacy provisional fixture в review-data.
3. Добавить schema validator и migration test.
4. Исправить ontology v2.
5. Провести Q3 source review по перечисленным событиям.
6. Добавить family IDs и provenance.
7. Не менять public routes.

**Gate:** contract CI green, semantic labels blocked.

### PR B — scorer and evaluation

1. Сохранять all-event raw scores, margins и winning prototypes.
2. Добавить family-weighted evaluator.
3. Создать owner-review interface/output.
4. Получить owner-reviewed gold.
5. Freeze calibration + temporal holdout.
6. Выпустить hash-bound evaluation receipt.

**Gate:** метрики воспроизводимы; publication всё ещё blocked.

### PR C — grounded decisions and manifests

1. Candidate-only adjudicator.
2. Durable cache/history/manual override.
3. ID-only accepted manifests.
4. last-good/failure behavior.
5. Cold + warm real StaticSiteBuilder run.

**Gate:** complete batch, zero unknown IDs, exact hash receipts.

### PR D — Astro routes

1. Реализовать HTML contract Q8.
2. Сначала exact/grounded pages.
3. Shadow candidate build.
4. Browser E2E против immutable candidate.
5. Отдельное owner product acceptance.

**Gate:** route-by-route promotion; blocked labels не меняются автоматически.

### PR E — public promotion

1. Обновить route contract state.
2. Включить sitemap/navigation только для принятого label.
3. Установить `STATIC_COLLECTIONS_E2E_REQUIRED=true`.
4. Сделать scheduled E2E required operational check.
5. Проверить rollback и last-good.

## 5. Tests, которые обязан добавить агент

### Python/unit

- provisional seed не может разрешить публикацию;
- owner gold не импортируется production exporter;
- policy version invalidates stale decisions;
- all rows имеют source provenance;
- family positive/negative disjointness;
- raw margin совпадает с `positive_score-negative_score`;
- hashes score/vector/policy/prototypes совпадают;
- threshold recalculation deterministic;
- `compute pass` без quality receipt остаётся blocked;
- provider failure сохраняет last accepted decision;
- unknown не публикуется;
- occurrence family projection не дублирует карточку;
- no event ID outside frozen catalog.

### Data-quality regression

- отдельные fixtures/tests для IDs 5757, 6696/6766, 6878, 7307, 7326;
- title/description/venue contamination;
- asymmetric/dangling `linked_event_ids`;
- same family on both benchmark sides;
- generated source quote вместо raw evidence запрещён.

### Node/Astro

- manifest membership не переопределяется из prose;
- blocked/shadow/public route rendering;
- noindex/canonical/sitemap;
- navigation inclusion;
- duplicate event/family;
- last-good/degraded UI state;
- card URL and event existence.

### Browser E2E

- desktop `1440x900`;
- mobile `390x844`;
- no horizontal overflow;
- no console/page errors;
- all visible images decode or have accepted fallback;
- heading and collection state visible;
- card links resolve;
- blocked route absent from navigation/sitemap;
- shadow noindex;
- public route indexable;
- collection page contains no duplicate event/family IDs.

## 6. Definition of Done

Работа считается завершённой только когда одновременно выполнено:

- review seed и owner gold физически разделены;
- product definitions утверждены owner/editor;
- data defects Q3 закрыты receipts;
- family-weighted gold и holdout существуют;
- score/evaluation воспроизводимы по hashes;
- semantic decisions source-grounded;
- manifests ID-only;
- contract CI green;
- browser E2E реально запускается против candidate URL;
- scheduled workflow имеет успешный run;
- `STATIC_COLLECTIONS_E2E_REQUIRED=true`;
- каждый public label отдельно принят;
- rollback/last-good проверены.

`10 passed` на структурном fixture test не является Definition of Done.

## 7. Команды для локальной проверки

Bootstrap regression legacy seed (только unit fixture):

```bash
python3 -m unittest discover -s tests \
  -p 'test_static_collection_quality_validator.py'
node --test site/scripts/static-collections-e2e.behavior.test.mjs
```

После PR-A migration (обычный contract CI):

```bash
python3 scripts/validate_static_collections_quality.py \
  --mode review \
  --policy site/scripts/static_collection_policy.v2.json \
  --seed docs/review-data/static_collections_review_seed_v1.json \
  --source-review-index docs/review-data/static-collections-source-reviews-v1/index.json
```

После PR B:

```bash
python3 scripts/validate_static_collections_quality.py \
  --mode strict \
  --policy site/scripts/static_collection_policy.v2.json \
  --seed docs/review-data/static_collections_review_seed_v1.json \
  --owner-gold tests/fixtures/static_collections_owner_gold_v1.json
```

Live/candidate E2E:

```bash
cd site
npm ci
npx playwright install chromium
node scripts/check-static-collections-e2e.mjs \
  --base-url "$STATIC_COLLECTIONS_E2E_BASE_URL" \
  --contract scripts/static-collections-e2e.contract.v1.json \
  --report /tmp/static-collections-e2e.json \
  --artifact-dir /tmp/static-collections-e2e-artifacts
```

## 8. Точный prompt следующему кодовому агенту

```text
Работай в onedayonemasterpiece/events-bot-new.

Начни с ветки agent/static-collections-quality-e2e и draft PR, созданного для
quality/E2E handoff. Прочитай:

1. docs/features/static-site-pages/static-collections-quality-plan.md
2. docs/features/static-site-pages/static-collections-e2e-runbook.md
3. docs/features/static-site-pages/podborki-analyst-review.md из ветки
   agent/static-collections-review/curation
4. docs/features/static-site-pages/podborki-to-be.md
5. docs/features/static-site-pages/release-plan.md

Не публикуй semantic labels и не ослабляй fail-closed. Выполняй work packages
Q0–Q9 последовательно. Сначала исправь ontology, legacy gold naming,
source provenance, data-quality defects и family identity. Только после
owner-reviewed gold меняй prototypes/thresholds.

Сохрани один StaticSiteBuilder, один Fly snapshot и один shared BGE artifact.
BGE остаётся candidate/disagreement detector; publication truth для
неоднозначных labels должна быть source-grounded и cached. Astro потребляет
ID-only manifests.

Развивай добавленный workflow и E2E checker, не создавай второй параллельный
browser framework. До реального candidate URL browser job не считать
выполненным. После подключения repository secret
STATIC_COLLECTIONS_E2E_BASE_URL установи repository variable
STATIC_COLLECTIONS_E2E_REQUIRED=true и приложи ссылку на успешный scheduled
workflow run.

Каждый PR заверши exact commands, reports, hashes, unresolved risks и
route-by-route GO/NO-GO. Не объединяй PR A–E в один большой merge.
```
