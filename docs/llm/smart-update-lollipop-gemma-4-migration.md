# Smart Update Lollipop / Gemma 4 Migration

Статус: `planning`

Назначение: канонический reference-doc для перевода **реального Smart Update** с `Gemma 3` на `Gemma 4` с сохранением корректного merge-механизма и качеством публичного текста не хуже текущего Smart Update.

Важно:

- `Smart Update G4 candidate` означает **полное удаление живой Gemma 3 из candidate path**. Все Gemma-backed стадии Smart Update должны перейти на `Gemma 4`: extraction, match/create/merge LLM stages, fact-first auxiliary stages, `short_description`, `search_digest`, revise/reflow/shrink/full-rewrite stages. Финальный public writer при этом может быть отдельным `4o` writer lane, если это явно зафиксировано и измеряется.
- `baseline` = текущий Smart Update на `Gemma 3`; он используется только как внешний benchmark reference для сравнения фактов, merge/result fields, текста, структуры и скорости. Baseline facts/text запрещены в payload любых `Smart Update G4 candidate` stages.
- Текущий выбранный путь для следующей работы: **вариант 2 — G4 Smart Update parity + lollipop-light для description**.
- `4o` в конце не запрещён: допустимы и `Gemma 4 final writer`, и `Gemma 4 upstream + final 4o`. Benchmark обязан явно показывать `writer_model`, fallback usage и сравнивать writer lanes отдельно, чтобы не смешивать качество Gemma 4 upstream с качеством финального writer-а.
- Ранние документы/артефакты, где `lollipop g4` означал `Gemma 4 upstream + final 4o`, остаются релевантным research track. Для текущей forced migration цель шире: убрать `Gemma 3` из Smart Update candidate path и доказать, что итоговый Smart Update result не хуже текущего baseline.
- Live-пробы, fixture-ы и benchmark-логи вынесены в отдельный eval-doc:
  [smart-update-lollipop-gemma-4-eval.md](/workspaces/events-bot-new/docs/llm/smart-update-lollipop-gemma-4-eval.md).

## Каноническая цель миграции

Перевести production-relevant Smart Update path на `Gemma 4`, сохранив текущий product contract и добавив лучшие lollipop-наработки там, где они реально улучшают public output.

Current Smart Update surface:

```text
source post / parser text / OCR
-> EventCandidate
-> Smart Update match/create/merge
-> event_source_fact / canonical facts
-> fact-first description
-> short_description + search_digest
-> Telegraph / daily / VK / event pages
```

Target candidate for the selected path:

```text
source post / parser text / OCR
-> EventCandidate
-> Gemma 4 match/create/merge stages
-> Gemma 4 fact extraction
-> Smart Update fact buckets
-> lollipop-light prioritize/layout
-> deterministic writer_pack
-> final public writer (Gemma 4 lane or 4o lane, reported explicitly)
-> Gemma 4 short_description + search_digest
-> existing render/publication surfaces
```

Практический смысл:

- текущий Smart Update на `Gemma 3` остаётся benchmark baseline, а не частью нового pipeline;
- lollipop используется не как отдельная абстрактная система, а как набор уже проверенных Smart Update улучшений: salience, section planning, writer_pack, literal lists, heading discipline;
- успех измеряется итоговым Smart Update result: merge correctness, public facts, `description`, `short_description`, `search_digest`, Telegraph/daily safety and latency.

## Варианты внедрения

| Вариант | Суть | Плюсы | Минусы | Статус |
| --- | --- | --- | --- | --- |
| 1. `G4 parity Smart Update` | Перевести текущие Smart Update LLM stages на `Gemma 4`, архитектуру почти не менять | Минимальный production diff, проще локализовать regressions | Может сохранить текущие слабости writer-а и не использовать lollipop-наработки | Обязательная база, но не выбран как финальный quality path |
| 2. `G4 parity + lollipop-light description` | Весь Smart Update candidate на `Gemma 4`; для description добавить lollipop `prioritize/layout/writer_pack` поверх Smart Update facts | Лучший баланс: сохраняет Smart Update merge/fact-first contract и возвращает структуру, semantic headings, literal lists | Есть риск потерять часть baseline writer-наработок, если writer_pack заменит prompt-contract вместо дополнения к нему | **Выбран для следующей итерации** |
| 3. `Full lollipop Smart Update G4` | Встроить полный cascade `source.scope -> facts.extract -> facts.dedup -> facts.merge -> facts.prioritize -> editorial.layout -> writer_pack -> writer` | Потенциально максимальное качество на сложных multi-source cases | Слишком дорогой и сложный первый rollout; трудно диагностировать, какая stage ухудшила результат | Research-only до успеха варианта 2 |
| 4. `Adaptive G4 router` | Простые events идут быстрым G4 parity path; dense/list-heavy/mixed-phase events идут через lollipop-light/full stages | Лучший production latency/quality баланс | Требует уже принятых быстрых и дорогих путей плюс routing criteria | Следующий шаг после принятого варианта 2 |
| 5. `Big-bang G4 + full lollipop` | Сразу заменить весь Smart Update на полный lollipop G4 | Теоретически быстро прийти к целевой архитектуре | Очень высокий риск качества, скорости и merge regressions | Не использовать |

## Выбранный вариант 2

Вариант 2 должен быть реализован как extension текущего Smart Update, а не как параллельный writer-product.

Инварианты:

- Candidate path не вызывает `Gemma 3`; финальный writer может быть `Gemma 4` или `4o`, но его модель всегда явно записывается в benchmark/result metadata.
- Baseline G3 не попадает в generation payload; он существует только в benchmark.
- `EventCandidate`, deterministic shortlist/anchor guards, `event_source`, `event_source_fact`, title/venue merge rules и public render path остаются Smart Update-owned.
- Lollipop-light добавляется только после того, как факты уже приведены к Smart Update-compatible buckets.
- Baseline writer-наработки должны быть перенесены как обязательный writer contract, а не потеряны:
  - эпиграф из grounded quote/fact, если применимо;
  - lead одним абзацем;
  - 2-3 смысловых `###` headings для dense cases;
  - списки для program/literal items;
  - запрет логистики в narrative;
  - style C: `сцена -> смысл -> детали`;
  - запрет CTA, report/promo formulas, `посвящ...`, `это ... не ..., а ...`;
  - фактический budget от объёма facts, а не только от baseline chars.
- Lollipop-light отвечает за то, что baseline writer делал слабо или нестабильно: salience, section boundaries, exact heading plan, literal list survival, writer_pack coverage.

Главный известный риск варианта 2: при подключении `layout/writer_pack` можно случайно заменить baseline-style public writer более сухим pack-following writer-ом. Поэтому benchmark обязан отдельно измерять **baseline feature parity**: эпиграф, heading count, semantic heading quality, lead richness, list preservation, length/density, style regressions.

### Writer lane decision rule

Выбор между `Gemma 4 final writer` и `Gemma 4 upstream + final 4o` решается только итоговым качеством Smart Update result.

Правило:

- если `Gemma 4 final writer` даёт текст не хуже текущего Smart Update baseline и проходит скорость/стабильность, он может быть выбран как более цельный G4 path;
- если `Gemma 4 upstream + final 4o` даёт заметно лучшее качество текста при приемлемой скорости, этот lane допустим и не считается нарушением миграции, потому что главный запрет касается `Gemma 3` и baseline leakage;
- если `4o` используется только после timeout/error Gemma 4 writer-а, benchmark обязан показывать это как fallback, а не как обычный writer lane;
- если оба lanes проходят hard gates, выбирается тот, где выше итоговое public quality: fact coverage, structure, style, grounded richness, short/search quality, render safety.

При этом промежуточные стадии обязаны сохранять всё, что уже работает в текущем Smart Update:

- корректный match/create/merge без новых дублей;
- event-local venue grounding и title grounding;
- сохранение `event_source_fact` как audit surface;
- strict fact-first narrative без логистики в описании;
- bounded coverage/revise без semantic regex repair;
- сохранение `short_description` и `search_digest` contracts.

## Non-goals

Вне канонической области этой миграции:

- giant-prompt redesign вместо stage-oriented Smart Update / lollipop-light;
- rollout по одному красивому writer-case без upstream family audit;
- включение `thinking` по умолчанию на финальном public prose stage;
- любые варианты, где `Smart Update G4 candidate` тихо использует `Gemma 3` или baseline facts/text.

## Почему Gemma 4 имеет смысл stage-oriented

Для Smart Update ценность `Gemma 4` прежде всего в этом:

- нативная `system`-роль вместо legacy `Gemma 3` prompt style;
- более сильный long-context профиль для multi-source fact packs;
- управляемый `thinking`, который полезен для планирования и disambiguation, но не обязан жить в финальном writer;
- нативный `tool`-протокол как задел под future retrieval-interleaving;
- более удобный transport/runtime contract для stage-oriented structured work.

То есть основной выигрыш ожидается в:

- лучшем выделении и сохранении grounded facts;
- более устойчивом salience/planning;
- меньшем prompt drift между family stages;
- лучшем контроле над schema-following и validator-driven reruns.

## Ключевой контракт Smart Update G4

При любой реализации Smart Update G4 должны одновременно сохраняться инварианты:

1. Candidate generation path полностью свободен от `Gemma 3`; Gemma-backed stages работают на `Gemma 4`, а финальный public writer может быть `Gemma 4` или явно измеряемый `4o`.
2. Structured stages работают как маленькие self-contained requests с явным schema contract.
3. Успех миграции измеряется качеством итогового Smart Update result относительно текущего Smart Update baseline, а не субъективной "силой" отдельного Gemma-stage.
4. Regression guards из `INC-2026-05-02-pre-daily-event-quality` обязательны для любых changes в extraction/merge/title/venue surfaces: event-local venue grounding, placeholder literal ban (`location_address` и подобные), canonical title guidance.

### Structured output rollout note (`2026-05-06`)

Latency probes on the current three production-snapshot Smart Update fixtures showed that native `response_schema` can remove a large part of Gemma 4 structured-call overhead without switching away from `gemma-4-31b-it`.

Accepted safe step:

- `event_topics` now uses `gemma-4-31b-it` with native `response_schema` by default and falls back to the old prompt-schema JSON contract if the native call fails or returns invalid JSON.

Experimental step, not production-default:

- Smart Update JSON stages can opt into native `response_schema` with `SMART_UPDATE_GEMMA_NATIVE_SCHEMA=1`.
- The default experimental stage allowlist is `SMART_UPDATE_GEMMA_NATIVE_SCHEMA_STAGES=facts_extract,create_bundle`.
- This remains gated because the first latency probe showed speed gains for `facts_extract` and `create_bundle`, but `facts_extract` could return fewer facts on dense cases. Full-surface benchmark parity is required before enabling it by default.

## Сравнение Gemma 3 и Gemma 4 по ключевым атрибутам

| Атрибут | Gemma 3 | Gemma 4 | Что это означает для `lollipop` |
| --- | --- | --- | --- |
| Контекст | до `128K` для больших text-моделей | до `256K` для `31B` и `26B A4B` | Больше пространства для multi-source payload, но растут требования к token budgeting и prompt discipline |
| System role | официально не first-class для `IT`-моделей | нативная `system`-роль | Prompt-family можно разделить на policy (`system`) и payload (`user`) |
| Thinking | не оформлен как отдельный протокол | есть отдельный thought-channel и `thinking` policy | Нужно проектировать, где reasoning полезен, а где его надо выключить |
| Tool use | в основном внешняя обвязка | нативный tool protocol | Открывает future path для retrieval-aware stages, но не обязателен в phase 1 |
| Prompt formatting | legacy turn-format | новый turn/token contract | Нельзя просто переиспользовать Gemma-3 prompt style без stage audit |
| Лицензия и экосистема | более жёсткая / старый стек | `Apache 2.0`, очень свежий стек | Проще юридически, но выше риск ранней нестабильности SDK/runtime tooling |

## Что именно должно поменяться в `lollipop g4`

Ниже перечислены family changes для выбранного варианта 2. Они применимы и к lane `Gemma 4 upstream + final 4o`, и к lane `Gemma 4 upstream + Gemma 4 final writer`; различие writer lane должно оставаться видимым в benchmark.

### `source.scope`

Назначение при миграции:

- лучше отделять target event scope от source noise;
- устойчивее работать с mixed-phase и multi-event contamination;
- жёстче маркировать `in_scope / background / uncertain`.

Что менять:

- перевести stage prompts на `system + user`;
- сократить prose-instructions и усилить target JSON contract;
- явно передавать scope objective, target date/event anchor и expected evidence ledger;
- держать `thinking = off` по умолчанию;
- разрешать `LOW thinking` только для реально ambiguous phase-selection cases.

Сигнал успеха:

- меньше ложных future/past leakage;
- меньше downstream rescue-логики из-за плохого scope split.

### `facts.extract`

Это главный кандидат на выигрыш от `Gemma 4`.

Что менять:

- переписать все extract-family prompts под `system`-policy + compact `user` payload;
- tighten JSON schema и field-by-field contracts;
- прямо запрещать premature prose synthesis;
- требовать, чтобы meaningful source facts либо сохранялись, либо явно маркировались как weak/uncertain/background;
- держать family как набор узких prompts, а не объединять их в один universal extractor.

Режим reasoning:

- `thinking = off` по умолчанию;
- `LOW thinking` допустим точечно на dense cases, где extractor должен удержать несколько смысловых пластов, а не только поверхностную карточку.

Сигнал успеха:

- richer grounded pack до `facts.merge`;
- меньше потерь curator/history/context facts;
- меньше downstream "writer looks dry because upstream pack is thin".

### `facts.dedup`

Что менять:

- жёстче фиксировать relation labels (`covered`, `reframe`, `enrichment`, `conflict`);
- вынести инструкцию "не терять новый смысл ради агрессивного collapse";
- явно разделять semantic overlap и source-unique enrichment.

Режим reasoning:

- `thinking = off`;
- если нужен reasoning, он должен быть очень узким и не превращать stage в essay.

Сигнал успеха:

- меньше silent fact loss;
- меньше ложных collapse в cases со схожими, но не идентичными описаниями.

### `facts.merge`

Что менять:

- сохранить canonical pack shape, но перепроверить prompt/style под Gemma 4;
- жёстче закрепить bucket contracts;
- явно передавать provenance expectations и запрет на invented bridging text;
- не смешивать merge с prioritization или layout planning.

Режим reasoning:

- `LOW thinking` допустим только если merge реально упирается в multi-source reconciliation;
- по умолчанию stage должен оставаться structured and bounded.

Сигнал успеха:

- более чистый canonical pack без потери provenance;
- меньше manual/heuristic rescue before prioritization.

### `facts.prioritize`

Что менять:

- переработать salience prompts под `system`-policy;
- явно разделять `must_keep`, `support`, `suppress`, `uncertain`;
- усилить contract для opaque-title / format-anchor / narrative-policy signals.

Режим reasoning:

- `LOW thinking` допустим, если stage реально принимает salience decisions между competing facts;
- reasoning не должен превращаться в hidden second writer.

Сигнал успеха:

- лучшее lead selection;
- меньше dry or taxonomy-heavy openings в final text, потому что pack уже лучше приоритизирован.

### `editorial.layout`

Это второй сильный кандидат на выигрыш от `Gemma 4`.

Что менять:

- использовать нативную `system`-роль для layout policy;
- усилить contract для section boundaries, heading permissions и body split;
- чётче передавать `title_is_bare`, `title_needs_format_anchor`, `non_logistics_total`, `body_cluster_count`;
- запрещать stage писать публичную прозу вместо structure plan.

Режим reasoning:

- `LOW thinking` здесь наиболее оправдан;
- если где-то в `lollipop g4` и нужен limited planning mode, то прежде всего здесь, а не в финальном writer.

Сигнал успеха:

- более устойчивые structure plans;
- меньше collapsed one-blob outputs в list-heavy и dense narrative cases.

### `writer_pack.compose` / `writer_pack.select`

Канонический статус не меняется:

- остаются deterministic;
- не мигрируются на Gemma 4;
- только принимают более сильный upstream payload.

### Final writer lanes

Финальный public writer выбирается по качеству:

- `writer.final_4o` остаётся допустимым final public writer lane;
- `Gemma 4 final writer` может тестироваться как отдельный lane, если он не ухудшает итоговое качество;
- acceptance измеряется на итоговом Smart Update тексте, но benchmark обязан показывать `writer_model` и не смешивать обычный `4o` lane с fallback после Gemma 4 timeout;
- если `writer.final_4o` выбран как quality lane, upstream/merge/fact stages всё равно должны быть `Gemma 3`-free.

## Prompt-contract deltas для Gemma 4

Независимо от family, переход на `Gemma 4` требует одинаковых базовых правил.

### 1. `system` и `user` должны быть разделены

В `system` живут:

- stage objective;
- anti-filler / anti-invention policy;
- schema rules;
- allowed/forbidden behaviors.

В `user` живут:

- stage payload;
- source excerpts;
- current pack;
- explicit task-local variables.

### 2. Нельзя переносить legacy Gemma-3 prompt style как есть

Нужно переписать:

- turn formatting;
- role separation;
- examples;
- failure instructions;
- correction prompts.

Иначе `Gemma 4` будет формально "работать", но stage contracts останутся не оптимизированными под её реальный protocol shape.

### 3. `Thinking` должен быть stage-scoped, а не глобальным

Рабочее правило для `lollipop g4`:

- по умолчанию `thinking = off`;
- `LOW thinking` допускается только на planning/disambiguation-heavy upstream stages;
- если final public prose stage идёт через `4o`, он не зависит от Gemma 4 thought-channel; если тестируется `Gemma 4 final writer` lane, `thinking` для него должен быть `off`.

### 4. Structured output важнее prose cleverness

`Gemma 4` в `lollipop` нужна не для "красивого текста" upstream, а для:

- удержания сложного source pack;
- аккуратного split по buckets;
- лучшего schema following;
- более сильного planning.

Если stage начинает "писать за downstream", это регрессия.

### 5. Примеры должны быть короткими и family-local

Для Gemma 4 полезнее:

- `1-2` коротких positive/negative examples на stage;
- чем один длинный abstract prompt с большой редакторской философией.

Это особенно важно для `source.scope`, `facts.extract` и `editorial.layout`.

## Transport и runtime-дельты

Это часть миграции, а не факультативная обвязка.

### Thought handling

Если выбранный transport отдаёт separate thought channel, нужны жёсткие гарантии:

- thought content не попадает в public output;
- thought content не попадает в persisted multi-turn history;
- thought content не попадает в operator-facing final preview по умолчанию;
- при debug-mode thoughts хранятся отдельно от final answer.

### SDK / API path

Нужно использовать поддерживаемый transport path, а не проектировать `lollipop g4` на deprecated integration assumptions.

### Token budgeting

Из-за нового context profile и другой tokenization нужно пересмотреть:

- per-stage payload caps;
- long-source chunking;
- conservative TPM budgeting;
- retry policy на dense multi-source cases.

## Почему tool-calling пока не обязателен

`Gemma 4` делает tool interleaving более естественным, но для `lollipop g4 phase 1` это не обязательное условие.

Правильный порядок:

1. Сначала довести `Gemma 4` prompts и transport для существующих upstream stages.
2. Потом отдельно решать, нужен ли `tool_call` хотя бы для части retrieval-heavy stages.

То есть `tool use` здесь рассматривается как future expansion path, а не как blocker для первой версии `lollipop g4`.

## План миграции

### Phase 0. Каноника и eval frame

- зафиксировать правильную цель: `Gemma 3` отсутствует в Smart Update candidate path; финальный writer lane (`Gemma 4` или `4o`) выбирается и измеряется явно;
- держать reference-doc отдельно от eval-log;
- зафиксировать benchmark protocol, где writer lane не меняется внутри одной строки сравнения и всегда явно указан.

### Phase 1. Family audit

- пройти по всем Gemma-backed upstream stages;
- для каждой family выписать prompt deltas, schema deltas, retry/validator deltas;
- определить, где `thinking` запрещён, а где допускается в `LOW` режиме.

### Phase 2. Prompt rewrite

- переписать prompts под `system + user`;
- сократить oversized instructions;
- добавить family-local positive/negative examples;
- обновить validators, если schema contract ужесточается.

### Phase 3. Runtime hardening

- подтвердить supported transport;
- внедрить thought filtering;
- обновить token budgeting и pacing;
- проверить logging/debug contracts.

### Phase 4. Canonical eval

- взять свежий synthetic benchmark с несколькими multi-source fixtures;
- прогнать `baseline`, `lollipop`, `lollipop g4`;
- сравнивать итоговый Smart Update result, а не локальную красоту upstream fragments;
- отдельно показывать `Gemma 4 upstream + final 4o` и `Gemma 4 upstream + Gemma 4 writer`, если оба writer lanes участвуют в прогоне.

### Phase 5. Rollout gate

`lollipop g4` может считаться готовым кандидатом только если:

1. итоговый текст стабильно лучше `baseline`;
2. нет явной регрессии относительно текущего `lollipop`;
3. transport/runtime contract чистый: без thought leakage и без сломанного latency profile.

## Acceptance criteria

Минимальная цель для реализации:

- `lollipop g4 > baseline` по качеству итогового public текста на серии canary fixtures.

Более строгая цель для реального прод-замещения текущего `lollipop`:

- `lollipop g4 >= current lollipop` по качеству текста при сопоставимой стабильности.

При этом успешная миграция должна давать не просто "другой текст", а лучшее сочетание:

- grounding;
- format clarity;
- preservation of meaningful facts;
- structure quality;
- отсутствие infoblock leakage;
- отсутствие dry card-style collapse.

## Чек-лист по компонентам

| Компонент | Что менять в рамках `lollipop g4` | Почему |
| --- | --- | --- |
| Prompt families | Переписать Gemma-backed upstream prompts под `system + user` | `Gemma 4` лучше работает на role-separated contracts |
| Validators | Уточнить schema checks там, где prompt contract становится строже | Иначе нельзя честно сравнивать `Gemma 3` и `Gemma 4` |
| Token budgeting | Пересчитать stage caps и pacing | Long-context profile и tokenization меняют бюджет |
| State / history | Гарантировать strip thoughts из persisted history | Это обязательное runtime правило для Gemma 4 |
| Logging | Логировать transport-aware debug traces без thought leakage в public path | Иначе тяжело локализовать regressions |
| Final writer | Поддержать явно выбранный writer lane: `writer.final_4o` или `Gemma 4 final writer`; не смешивать их в отчёте | Иначе нельзя понять, что улучшило результат: upstream, layout или writer |

## Связанные документы

- Канонический funnel: [smart-update-lollipop-funnel.md](/workspaces/events-bot-new/docs/llm/smart-update-lollipop-funnel.md)
- Writer pack contract: [smart-update-lollipop-writer-pack-prompts.md](/workspaces/events-bot-new/docs/llm/smart-update-lollipop-writer-pack-prompts.md)
- Final 4o contract: [smart-update-lollipop-writer-final-prompts.md](/workspaces/events-bot-new/docs/llm/smart-update-lollipop-writer-final-prompts.md)
- Live eval log: [smart-update-lollipop-gemma-4-eval.md](/workspaces/events-bot-new/docs/llm/smart-update-lollipop-gemma-4-eval.md)
