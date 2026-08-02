# Smart Update facts v3 для подборок: точная задача реализации

Статус: **implementation handoff**.

Кодовая реализация подготовлена в отдельной stacked-ветке
`integration/static-collection-facts-v3`; это не изменение docs-only PR #226.
Unit/contracts проходят, однако обязательный primary-only real-data Gate B от
2026-08-02 имеет статус **NO-GO**, поэтому production apply/deploy и публикация
не выполнялись. Фактическая приёмка и выявленные provisional-data противоречия:
[integration report](../../../.codex/integration/static-collection-facts-v3-INTEGRATION_REPORT.md).

После коррекции PR-A seed добавлен отдельный offline Gate-B evaluator. Он не
меняет prompt или model route, не вызывает LLM и не публикует labels: он
fail-closed связывает primary-only report с точными seed/index/receipt/SQLite/git
hashes и считает family-weighted recall только по high/keep/sufficient rows при
минимуме `0.80`. В обычном Smart Update и backfill recall-router симметрично
распознаёт нормализованную фразу `всей семьей` (включая исходное `ё`), но
`приходите` и возрастной рейтинг сами по себе остаются unrouted.

Stack base: `agent/static-collections-quality/pr-a-ontology` @
`3164e984d04208fcff5618c49271a4633d304eab`.

Эта задача является следующим небольшим production-инкрементом после PR A.
Она не реализует PR B scoring/owner gold, semantic manifests, Astro routes или
публичное включение подборок.

## 1. Решение по архитектуре

### Новых LLM-запросов не добавлять

В `smart_event_update.py` уже есть отдельный bounded вызов
`collection_candidate_adjudication`. Он вызывается только для routed candidate
и одним JSON-ответом сейчас проверяет:

- admission;
- один legacy `audience_decision`;
- appearances людей.

Нужно расширить **этот же единственный вызов**. Запрещено:

- добавлять отдельный вызов для `child_directed`;
- добавлять отдельный вызов для `family_suitable`;
- добавлять отдельный вызов для `joint_family_activity`;
- раздувать основные create/merge/writer prompts;
- запускать LLM на открытии сайта.

Инвариант после реализации:

```text
unrouted source -> 0 collection LLM calls
routed source   -> не более 1 collection LLM call
warm same hash  -> 0 provider calls и 0 DB writes
```

### Модель в этом PR не менять

Для `label="collection_candidate_adjudication"` текущий
`_resolve_smart_update_model()` попадает в default Smart Update lane:

```text
primary: gemma-4-31b-it
fallback: существующий GPT-4o fallback внутри _ask_gemma_json
```

В этом PR:

- оставить primary `gemma-4-31b-it`;
- не переводить stage на `gemini-3.1-flash-lite`;
- не добавлять новую модель;
- не менять общий Google key pool;
- не расширять fallback policy.

Причина: сначала изолированно меняется контракт фактов, а не одновременно
контракт и модель. Отдельный shadow benchmark Gemini Lite допустим после
приёмки facts v3, но не входит в эту работу.

Primary-only quality run выполняется с:

```text
SMART_UPDATE_4O_FALLBACK=0
```

Отдельный failure/fallback drill выполняется после него. В отчёте обязательно
разделить primary Gemma results и GPT-4o fallback results.

## 2. Размер изменения

Это **ограниченно-инвазивная доработка**, а не новый pipeline.

Основные изменения:

1. один schema contract;
2. один prompt;
3. strict validator;
4. independent merge/apply keys;
5. cache version;
6. existing backfill coverage;
7. unit/integration/real-data canary.

Основные production writer, event identity, title/description generation,
festival parsing, cinema parsing, StaticSiteBuilder и Astro не меняются.

## 3. Facts v3 contract

Повысить версии:

```text
STATIC_COLLECTION_FACTS_POLICY_VERSION = "static-collection-facts-v3"
STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION = "static-collection-adjudication-v2"
```

Admission и people оставить семантически без изменений.

Вместо одного LLM-owned `audience_decision` запрос должен вернуть три
независимых решения:

```json
{
  "child_directed_decision": {
    "value": "confirmed|denied|unknown",
    "confidence": 0.0,
    "evidence_quote": "",
    "reason_code": "..."
  },
  "family_suitable_decision": {
    "value": "confirmed|denied|unknown",
    "confidence": 0.0,
    "evidence_quote": "",
    "reason_code": "..."
  },
  "joint_family_activity_decision": {
    "value": "confirmed|denied|unknown",
    "confidence": 0.0,
    "evidence_quote": "",
    "reason_code": "..."
  }
}
```

`denied` разрешён только при явном отрицательном source evidence. Отсутствие
положительной фразы — это `unknown`, а не `denied`.

### 3.1. child_directed

`confirmed`, когда ребёнок является прямым целевым зрителем или участником
конкретного события.

Достаточные reason codes:

```text
explicit_child_audience
explicit_child_spectators
explicit_child_participants
```

`denied` допустим только с явным evidence, например adults-only:

```text
explicit_adults_only
explicit_age_exclusion
```

Недостаточно:

- 0+/6+/12+ без прямого назначения детям;
- дети являются авторами выставленных работ;
- детская тема, детский автор или слово «школа»;
- артист популярен у детей;
- FAMILY/KIDS topic или BGE score.

### 3.2. family_suitable

`confirmed`, когда источник прямо приглашает взрослых и детей/родителей с
детьми на совместное посещение конкретного события.

Reason codes:

```text
explicit_family_invitation
explicit_children_and_adults
explicit_family_format
```

Недостаточно:

- слово «семейный» вне названия/формата события;
- «семейная атмосфера»;
- тема семьи, беременности или социальной поддержки;
- только детская программа с сопровождающим взрослым;
- только parents-only встреча.

### 3.3. joint_family_activity

`confirmed`, когда взрослый и ребёнок выполняют общую практику, задачу или
участвуют одной семейной командой.

Reason codes:

```text
explicit_joint_task
explicit_parent_child_team
explicit_joint_practice
```

Недостаточно:

- совместное присутствие в зале;
- «для всей семьи» без совместного действия;
- детский мастер-класс, на котором взрослый только сопровождает;
- «семейный турнир» без доказательства состава команды.

### 3.4. Логические ограничения

Решения сохраняются независимо. При этом validator должен reject весь v3
payload при внутренне невозможном сочетании:

```text
joint_family_activity=confirmed
и child_directed!=confirmed

joint_family_activity=confirmed
и family_suitable!=confirmed
```

Это не подмена доказательств: одна и та же exact quote либо отдельные exact
quotes должны подтверждать все три утверждения.

## 4. Prompt

Изменить только prompt внутри `adjudicate_collection_candidate()`.

Prompt обязан:

- дать определения ontology v2 дословно по смыслу;
- потребовать три независимых решения;
- объяснить `denied` versus `unknown`;
- повторить, что age/topics/BGE — routing signals, не evidence;
- потребовать exact continuous quote из `source_corpus` для любого
  `confirmed` или `denied`;
- запретить выводить joint activity из одного слова «семейный»;
- при сомнении вернуть `unknown`;
- не создавать публичный текст.

Увеличить `max_tokens` только настолько, насколько требуется расширенной JSON
схеме. Целевой предел: `900–1100`; не создавать retry-by-token-expansion.

## 5. Legacy compatibility без второго вызова

Существующий `Event.collection_decisions.audience_decision` нельзя считать
publication truth ontology v2, но текущие потребители не должны внезапно
сломаться.

После strict validation v3 создать **детерминистическую compatibility
projection**, не второй LLM output:

```text
family_suitable=confirmed -> legacy value=family
иначе child_directed=confirmed -> legacy value=kids
явное adults-only отрицание -> legacy value=none
иначе legacy unknown/без замены
```

Compatibility projection:

- имеет `derived_from_facts_v3=true`;
- хранит `policy_version=static-collection-facts-v3`;
- использует source/input provenance фактических v3 решений;
- не может публиковать ontology-v2 labels;
- не заменяет новые независимые keys.

Существующие v2 decisions в DB не удалять и не массово мигрировать по одному
старому значению.

## 6. Merge/apply semantics

В `deep_merge_collection_decisions()` обрабатывать независимо:

```text
admission_decision
child_directed_decision
family_suitable_decision
joint_family_activity_decision
legacy audience compatibility projection
people_appearances
```

Правила:

- `unknown` никогда не стирает accepted truth;
- `confirmed`/`denied` конкурируют по текущему manual-lock/trust/time contract;
- более слабый source не заменяет более сильный;
- same `input_hash` — no-op;
- JSON reassignment остаётся whole-value;
- audience-only apply не меняет `is_free`;
- title, description, event_type, topics, identity, links и posters не меняются.

## 7. Router

Сохранить один reason `audience`; не создавать три reason/call lanes.

`route_collection_adjudication_reasons()` должен route audience, когда есть:

- KIDS/FAMILY topic;
- новые BGE recall signals;
- существующий legacy или v3 audience decision;
- широкий deterministic text signal из source corpus.

Text signal используется **только для recall routing**, не для решения. Он
может учитывать фразы вида:

```text
для детей
детский спектакль/шоу/занятие
для всей семьи
детям и взрослым
родители и дети
семейная команда
вместе с ребёнком
```

Возрастной рейтинг без других сигналов не route-ит событие сам по себе.

## 8. Backfill

Обновить `scripts/backfill_static_collection_facts.py` без создания второго
backfill script.

Требуется:

- `--reason audience` проверяет coverage трёх v3 keys;
- v2 legacy decision не считается v3 coverage;
- report показывает по каждому source:
  - input hash;
  - provider called/cached/deferred;
  - три v3 outcomes;
  - legacy projection;
  - changed fields;
- первый apply может писать решения;
- второй identical warm apply обязан дать `provider_calls=0`, `writes=0`;
- provider/schema failure даёт deferred/unknown и не стирает accepted truth;
- plan остаётся полностью read-only;
- `--apply` остаётся обязательным для записи.

Повысить report schema version, если структура отчёта меняется.

## 9. Файлы реализации

Ожидаемый основной diff:

```text
smart_event_update.py
scripts/backfill_static_collection_facts.py
tests/test_smart_event_update.py
tests/test_event_update_merge.py
tests/test_static_collection_backfills.py
docs/features/smart-event-update/README.md
site/scripts/static_collection_policy.v2.json
CHANGELOG.md
```

Допустим новый real-case fixture и integration report.

Не изменять:

```text
site/src/**
Astro routes
navigation/sitemap
static_collection_prototypes*
science/research/strong/medieval thresholds
cinema sources
festival extraction/pages
main event writer prompts
```

## 10. Unit и integration tests

Обязательно покрыть:

1. schema принимает три independent facts;
2. exact quote обязателен для confirmed/denied;
3. age-only, child-authors и family-atmosphere cases fail closed;
4. 7326-like case: family confirmed, joint unknown;
5. 7307-like case: family/joint не подтверждаются одним «семейный турнир»;
6. child theatre: child confirmed;
7. parents-only: child/family/joint denied либо unknown строго по quote;
8. joint confirmed требует child+family confirmed;
9. unknown не стирает accepted truth;
10. v2 legacy row не считается v3 coverage;
11. policy/schema bump меняет input hash;
12. routed candidate вызывает provider максимум один раз;
13. unrouted candidate не вызывает provider;
14. warm same hash не вызывает provider;
15. audience-only apply не меняет event prose/type/topics/is_free;
16. provider failure сохраняет DB без изменений;
17. deterministic legacy projection соответствует v3 facts;
18. manual lock и source trust precedence работают отдельно для каждого key.

## 11. Definition of Done реализации

Кодовая часть готова только если одновременно:

- новых LLM calls на routed source не появилось;
- model lane не изменён;
- facts v3 source-grounded и независимы;
- legacy compatibility получена детерминистически;
- v2 data не публикует v3 labels;
- targeted tests и основной CI green;
- real-data acceptance из соседнего документа пройдена;
- bounded production canary имеет warm no-op;
- semantic publication, routes и sitemap остаются blocked.

## 12. Точный prompt кодовому агенту

```text
Работай в onedayonemasterpiece/events-bot-new.

Stack base: agent/static-collections-quality/pr-a-ontology @
3164e984d04208fcff5618c49271a4633d304eab.

Реализуй только Smart Update static-collection facts v3 по документам:
1. docs/features/static-site-pages/static-collections-smart-update-facts-v3-implementation.md
2. docs/features/static-site-pages/static-collections-smart-update-facts-v3-real-data-acceptance.md

Ключевое решение: НЕ добавляй новые LLM-запросы. Расширь существующий один
collection_candidate_adjudication JSON-call. Primary model оставь
Gemma 4 31B, существующий GPT-4o — только fallback. Не переводи lane на Gemini
Lite в этом PR.

Добавь три source-grounded independent decisions: child_directed,
family_suitable и joint_family_activity; bump schema/policy; strict quote
validation; independent merge; deterministic legacy audience projection;
обнови существующий backfill и tests.

Сначала unit/integration, затем primary-only replay реальных source rows, затем
apply/warm replay на fresh production DB copy, затем bounded Fly canary и
реальные новые Telegram/VK posts через штатный Smart Update. Все artifacts,
commands, source IDs, call counts, models, hashes и DB before/after diff приложи
в integration report.

Не начинай PR B, не создавай owner gold/scores/thresholds/manifests/Astro routes
и не включай publication. При любом расхождении real-data gate — NO-GO.
```
