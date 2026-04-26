# Smart Update Lollipop / Gemma 4 Eval Log

Статус: `research`

Назначение: live-пробы, benchmark fixtures, промежуточные выводы и все experiment-specific результаты по `Gemma 4` migration track.

Важно:

- этот документ не является каноническим migration contract;
- канонический target architecture описан в
  [smart-update-lollipop-gemma-4-migration.md](/workspaces/events-bot-new/docs/llm/smart-update-lollipop-gemma-4-migration.md);
- acceptance-relevant сравнения для `lollipop g4` должны сохранять `writer.final_4o` как неизменный финальный writer.

## Live availability check (`2026-04-06`)

На проектном Google key были подтверждены как доступные:

- `models/gemma-4-26b-a4b-it`
- `models/gemma-4-31b-it`

Практические сигналы:

- `Gemma 4` уже доступна для исследовательских прогонов;
- raw responses могут нести separate thought-channel (`parts[].thought = true`);
- значит любой production-like path для Gemma 4 обязан явно фильтровать thought content.

## Early benchmark fixture: `KALMANIA-2885`

### Fixture scope

- `event_id = 2885`
- title: `Кальмания`
- date: `2026-04-03`
- event_type: `концерт`
- source mix:
  - `parser:muzteatr` -> `https://muzteatr39.ru/spektakli/koncerty/kalmaniya/`
  - `telegram` -> `https://t.me/muztear39/9293`
  - `vk` -> `https://vk.com/wall-131136967_21590`

### Почему fixture сохранён

Кейс полезен как аккуратный multi-source sample:

- `site + telegram + vk`;
- без тяжёлого mixed-phase contamination;
- хорошо подходит для writer-sensitive comparison.

Но его ранний прогон был поставлен в некорректной архитектурной рамке.

## Важное исправление интерпретации

Ранний эксперимент сравнивал:

- `baseline`
- `lollipop`
- вариант, где `Gemma 4` использовалась как final writer

Это не канонический `lollipop g4`, потому что:

- поменялся не upstream only path, а финальный writer;
- сравнение перестало быть совместимым с текущим `lollipop` contract;
- результат нельзя использовать как rollout criterion для `Gemma 4 upstream + final 4o`.

Поэтому этот ранний проход нужно читать только как:

- exploratory writer-swap probe;
- signal о стиле и grounding `Gemma 4`;
- не как финальный verdict по `lollipop g4`.

## Что всё же полезно из раннего прогона

Даже при неверной рамке он дал несколько реальных инженерных сигналов:

- `Gemma 4` уже доступна и отвечает на проектном ключе;
- `gemma-4-31b-it` выглядит сильнее по quality, чем `gemma-4-26b-a4b-it`, если нужен quality-first stage;
- `gemma-4-26b-a4b-it` быстрее, но суше;
- `Gemma 4` без дополнительной настройки легко уходит в dry/compact structured prose;
- transport-aware thought filtering обязателен.

## `lollipop_g4_fast` quality reset (`2026-04-26`)

Live KALMANIA benchmark artifact:
[lollipop_g4_benchmark_20260426T121919Z.md](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_benchmark_20260426T121919Z.md)

Verdict по этому artifact: `lollipop_g4_fast` в прежнем contract был быстрее полного `lollipop_g4`, но quality-failed относительно baseline. Главная причина не в финальном `4o`, а в upstream/pack contract:

- extractor был фактически summary-mode (`3-6 facts/source`);
- merge был summary-mode (`5-9 final facts`);
- deterministic pack дополнительно мог отбрасывать второй `event_core` с тем же `hook_type`;
- final writer prompt принудительно сжимал output в фиксированную форму `lead -> Программа -> Участники`.

Этот результат не считается rollout evidence. Он зафиксирован как regression fixture для следующего fast-контракта.

Next contract: `lollipop_g4_fast.v2`.

- `fast.extract_per_source.v1`: coverage-first, no fixed fact cap, all distinct event-facing facts from each source.
- `fast.merge_pack.v1`: coverage-first semantic dedup, no fixed final-fact cap.
- deterministic plumbing: no semantic fact deletion for compactness; only ordering/layout/guardrails.
- `writer.final_4o.v1`: prompt additions are dynamically assembled from `meta.writer_profile` (`rich_case`, rarity, atmosphere, quote candidate, literal program, named roles, must-cover count).
- quality gate is fact coverage first: “не хуже baseline” means no meaningful fact loss versus baseline, then stronger hook/liveness/anti-cliche voice.

## Non-canonical writer-swap result on `KALMANIA-2885`

### Summary table

| Variant | Что реально сравнивалось | Статус |
| --- | --- | --- |
| `baseline` | current prod-style path | допустимый reference |
| `lollipop` | structured upstream + final `4o` | допустимый reference |
| `writer-swap Gemma 4 probe` | structured pack + `Gemma 4` as final writer | неканонический эксперимент |

### Historical quality signal

Локальный вывод того раннего прогона был таким:

1. `lollipop`
2. `writer-swap Gemma 4 probe`
3. `baseline`

Но этот ranking нельзя читать как:

- `lollipop g4` vs `lollipop`;
- `Gemma 4 upstream` vs `Gemma 3 upstream`;
- rollout-ready доказательство.

Он показывает только, что:

- writer-swapped `Gemma 4` уже может давать лучше `baseline` на одном clean case;
- этого недостаточно для канонического решения по `lollipop g4`.

## Канонический benchmark protocol на следующий проход

Следующие сравнения должны быть поставлены так:

### Варианты

- `baseline`: как сейчас на проде генерируется текст
- `lollipop`: текущий `lollipop`
- `lollipop g4`: `Gemma 4` только в upstream stages, final `writer.final_4o` тот же самый

### Fixture requirements

Нужны свежие synthetic cases, где у одного события есть несколько источников:

- `Telegram`
- `VK`
- желательно ещё `site`

### Что именно оценивается

Главная ось сравнения:

- итоговый public text после одного и того же final `4o`

Вторичные оси:

- сохранность meaningful facts;
- format clarity;
- richness без dry collapse;
- отсутствие infoblock leakage;
- стабильность validator results.

### Что больше не допускается

Нельзя считать каноническим benchmark-ом проход, где:

- меняется final writer;
- сравнивается только один красивый case;
- Gemma 4 оценивается только по raw prose, а не по влиянию на конечный `4o` output.

## Candidate canary set for the next pass

Минимальный пакет:

- `1` clean concert/list-heavy case;
- `1` opaque-title + format-bridge case;
- `1` dense multi-block narrative case;
- `1` mixed-source screening/presentation case;
- `1` case с частичным literal list (`literal_list_is_partial = true`).

## Runnable benchmark rerun (`2026-04-06`)

### Canonical local artifact

Результат runnable benchmark сейчас фиксируется локальным harness:

- harness: [benchmark_lollipop_g4.py](/workspaces/events-bot-new/scripts/inspect/benchmark_lollipop_g4.py)
- checkpoint artifact: [lollipop_g4_benchmark_checkpoint_20260406T071525Z.json](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_benchmark_checkpoint_20260406T071525Z.json)
- markdown snapshot: [lollipop_g4_benchmark_checkpoint_20260406T071525Z.md](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_benchmark_checkpoint_20260406T071525Z.md)

Fixture:

- `fixture_id = KALMANIA-2026-04-03`
- `site`: `https://muzteatr39.ru/spektakli/koncerty/kalmaniya/`
- `telegram`: `https://t.me/s/muztear39/9421`
- `vk`: `https://vk.com/wall-131136967_21590`

### Benchmark caveats

Этот rerun уже runnable и качественно полезен, но он не равен идеальному future prod-benchmark `1:1`:

- `baseline` здесь зафиксирован как `prod-style first-pass proxy`:
  - source-level `smart_event_update` fact extraction оставлен продовым;
  - но полный baseline tail (`fact_first_cov/revise`) в живом прогоне упирался в существующий baseline bug:
    - `4o fallback` на coverage-stage падает из-за invalid `response_format.json_schema.name`, когда label несёт `:fact_first_cov`;
  - поэтому для comparison использован первый public-writer pass того же baseline path, без нестабильного coverage/revise tail.
- reference `lollipop` и `lollipop g4` здесь идут через reconstructed minimal harness, потому что historical family scripts отсутствуют в tracked workspace.

То есть этот rerun пригоден как migration evidence, но не как окончательный rollout certificate.

### Final live outputs

Unified artifact:

- json: [lollipop_g4_benchmark_20260406T073303Z.json](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_benchmark_20260406T073303Z.json)
- markdown: [lollipop_g4_benchmark_20260406T073303Z.md](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_benchmark_20260406T073303Z.md)

| Variant | Chars | Headings | Bullets | Main read |
| --- | --- | --- | --- | --- |
| `baseline` | `1635` | `3` | `0` | richest prose, но с `UGC leak` |
| `lollipop` (`Gemma 3 upstream`) | `447` | `0` | `0` | generic stub с validation error, непригоден |
| `lollipop g4` (`Gemma 4 upstream`) | `838` | `3` | `6` | clean structured event-card, но пока суховат и беднее baseline |

Дополнительные runtime сигналы:

- `lollipop`: `extract_errors = ['fact_count_out_of_band:0', 'missing_source_coverage:site,tg,vk']`
- `lollipop g4`: `extract_errors = []`
- `lollipop g4`: `0` hard writer validation errors

### Final ranking

Канонический итог после unified final rerun (`2026-04-06T07:33Z`):

1. `baseline`
2. `lollipop g4`
3. `lollipop`

Коротко почему:

- `Gemma 3` upstream почти не передал факты вниз, и identical final `4o` не смог восстановить event copy из пустоты;
- `Gemma 4` upstream уже собрал жизнеспособный fact pack и радикально поднял качество относительно current `lollipop`;
- но в unified canary `baseline` оказался сильнее по итоговому public-text quality:
  - выше information density;
  - лучше narrative flow;
  - сильнее event-facing usefulness;
- при этом `baseline` всё ещё несёт явный pipeline defect: `UGC leak` в редакторский голос.

### Final Opus verdict

Отдельный final `Opus` pass на тех же трёх текстах дал тот же ranking:

1. `baseline`
2. `lollipop g4`
3. `lollipop`

Короткий смысл verdict:

- `baseline` — strongest text по readability и editorial usefulness, но с серьёзным `UGC leak`;
- `lollipop g4` — strongest as-is по factual cleanliness, но пока слишком похож на structured factsheet и не дотягивает до уровня сильного public announcement;
- `lollipop` на `Gemma 3` остаётся непригодным к публикации;
- strongest delta от `Gemma 4` идёт не из final writer swap, а из upstream extraction/planning quality.

### What exactly improved in `Gemma 4`

На этом кейсе practical uplift от `Gemma 4` выглядит так:

- исчез English leak / empty-facts collapse, который сломал `Gemma 3` harness;
- сохранился полный list-heavy repertoire в bullet form;
- upstream дожил до production team и usable performer set;
- final text не протащил source-review residue вроде `Ирина отметила ...`, который протёк в baseline;
- writer validation на `lollipop g4` прошла clean (`0` hard errors), в отличие от broken `lollipop`.

### Remaining issues before any rollout talk

Даже после явного win против current `lollipop`, этот `lollipop g4` ещё не выглядит финальным production candidate:

- lead всё ещё суховат (`посвящен творчеству ...`) и может быть живее;
- `Атмосфера` — слабая one-line section;
- в literal list есть подозрительное `Баядерка` vs canonical `Баядера` и это нужно отдельно проверить по source packet;
- baseline в этом unified canary всё ещё сильнее по плотности и мотивации к посещению;
- single-canary evidence недостаточно для rollout gate.

## Fast variant design (`2026-04-26`)

После обсуждения полного `lollipop_g4` cost profile добавлен отдельный lab-вариант `lollipop_g4_fast`.

Ключевая гипотеза не считается доказанной заранее: Opus-рекомендация `source-local fast Gemma 4 extractor -> deterministic pack/merge/layout -> writer.final_4o` принята как сильный candidate design, но с явными ограничениями. Её плюс — call-count порядка `N source Gemma calls + 1 final 4o` вместо десятков Gemma stages в full cascade. Её слабое место — риск, что Python layout начнёт принимать смысловые решения. Поэтому fast-вариант оценивается только при соблюдении LLM-first contract: смысловые поля `bucket`, `salience`, `hook_type`, `literal_items`, `dedup_key` выставляет Gemma, а deterministic layer не делает regex-semantic rescue.

Current implementation after the `lollipop_g4_fast.v2` reset:

- `fast.extract_per_source.v1`: один coverage-first source-local Gemma 4 call на источник, native provider-compatible `response_schema`, capped at `max_tokens=2000`; extractor owns `role_class` for people blocks (`production_team/cast/ensemble/none`), groups long cast lists into dense facts instead of one record per role, and preserves plot/character premise for stage titles as narrative facts rather than letting credits dominate;
- default merge/pack: Python pass-through of relevant LLM-owned records plus exact grouping by LLM-owned `dedup_key`. It drops only `drop/infoblock_only/suppress` and does not own semantic dedup, summarization, or fact rescue;
- `fast.merge_pack.v1`: optional compact Gemma 4 merge/dedup fallback behind `LOLLIPOP_G4_FAST_LLM_MERGE=1`; live KALMANIA/VIVAT showed that making this stage mandatory breaks the speed target without enough quality gain;
- `literal_items` are safety-constrained against extractor-owned `literal_items/text/evidence` and source-local excerpt substring gate; this is not regex extraction from raw source and not repair, but refusal to publish provider-noise literals;
- `fast.layout_assemble`: deterministic assembly из уже размеченных LLM fields, без удаления event-core facts ради compactness;
- source-local extraction runs in parallel by default, потому что источники независимы по смыслу и latency не должна оплачиваться потерей фактов;
- source mismatch is handled inside the extractor prompt: if a source is primarily about another event, it may emit only a `drop` mismatch fact, not facts about the other event;
- `fast.layout_planner.v1`: optional fallback только при `LOLLIPOP_G4_FAST_PLANNER=1`, default off;
- `writer.final_4o.v1`: тот же final writer, с fast-specific prompt additions через `meta.writer_profile`; `narrative_fact_count` now influences `rich_case`, so people-heavy outputs with enough non-people facts must keep narrative body before cast/credits; fast path не имеет deterministic repair/rescue и не дожимается отдельным correction loop.

Benchmark command shape:

```bash
python scripts/inspect/benchmark_lollipop_g4.py \
  --fixtures kalmania,vivat \
  --variants baseline,lollipop_g4_fast
```

Если локальный `.env` намеренно содержит только `GOOGLE_API_KEY2` как isolated local benchmark key, baseline path можно запустить с явным opt-in, не меняя глобальную key policy:

```bash
python scripts/inspect/benchmark_lollipop_g4.py \
  --fixtures kalmania \
  --variants baseline,lollipop_g4_fast \
  --allow-google-key2-for-baseline
```

Acceptance evidence для fast должно включать:

- `speed_ratio_vs_baseline.target_pass = true` (`<= 1.5x`) как основной latency target или `speed_ratio_vs_baseline.pass = true` (`<= 2.5x`) как временный quality-first hard cap;
- default `gemma_calls` равно числу источников fixture; optional LLM merge/planner calls are allowed only with explicit flags and count against latency budget;
- `validation.errors = []`;
- `literal.title_mutation` отсутствует: fast pack/optional merge не может выдавать literal title, которого не было в extractor `literal_items` или extractor-owned text/evidence;
- reviewer pairwise vote против baseline по 7 измерениям: fact coverage, event clarity, rarity/atmosphere, literal fidelity, named roles, natural voice, no promo/report leakage. Итог `FAST_WINS` или `TIE` обязателен; `BASELINE_WINS` по fact coverage или event clarity блокирует rollout.

Fast-вариант пока не является rollout evidence сам по себе: `KALMANIA` и `VIVAT-MUNCHHAUSEN` now pass validation and the `<=1.5x` target on the current code, but mixed-phase case, opaque-title screening/presentation case, sparse single-source case and reviewer pairwise votes are still required before any production default.

Live KALMANIA evidence (`2026-04-26T15:25Z`, default no-LLM-merge path with `dedup_key` pass-through grouping):

- artifact: `artifacts/codex/lollipop_g4_benchmark_20260426T152508Z.json` / `.md`;
- models: fast upstream on `gemma-4-31b-it`; no `26b` use in fast path;
- calls: baseline `4 Gemma / 0 4o`; fast `3 Gemma / 1 4o` (`N=3` source-local extract calls + `writer.final_4o`), with source-local extract calls parallelized;
- merge owner: `default_no_llm_merge_passthrough`;
- fact flow: `13` extracted records -> `12` merged/pass-through records;
- latency: reused baseline `47.879492s`, fast `52.036041s`, ratio `1.0868`; target `1.5x` passed;
- validation: `errors=[]`, `warnings=[]`;
- manual note: fast keeps rarity (`раз в сезоне`), `два вечера подряд`, atmosphere/program, exact literal bullets from source excerpt, named soloists and ballet roles, while avoiding report/promo/audience-template validation hits. Remaining quality risk: source typo `«Фиалки Монматра»` is preserved as source-local literal, so cross-source canonicalization remains a reviewer decision, not a deterministic fix.

Live VIVAT-MUNCHHAUSEN evidence (`2026-04-26T15:23Z`, current default path with grouped cast and `role_class` layout split):

- artifact: `artifacts/codex/lollipop_g4_benchmark_20260426T152348Z.json` / `.md`;
- models: fast upstream on `gemma-4-31b-it`; no `26b` use in fast path;
- calls: baseline `3 Gemma / 0 4o`; fast `2 Gemma / 1 4o` (`N=2` source-local extract calls + `writer.final_4o`);
- merge owner: `single_relevant_source_passthrough`;
- source-scope note: Telegram source was primarily about `Кальмания`, so extractor emitted a `drop` mismatch instead of contaminating `Виват, Мюнхгаузен!`;
- fact flow: `8` extracted records -> `6` merged/pass-through records;
- latency: reused baseline `28.392617s`, fast `39.909695s`, ratio `1.4056`; target `1.5x` passed;
- validation: `errors=[]`, `warnings=[]`;
- manual note: grouped cast extraction fixed the previous latency/structure problem while preserving named role coverage; writer kept the local-context hook and avoided baseline's unrelated `Кальмания` contamination plus promo/audience leakage. Remaining quality issue from this artifact: the narrative body is still too thin before the cast/credits sections. The follow-up contract now asks the extractor to preserve protagonist/story-engine facts and makes people-heavy + narrative-rich cases `rich_case`, requiring body prose before role sheets. This needs a fresh live run once a valid `writer.final_4o` token is available.

## Full-cascade retune iteration (`2026-04-06`, afternoon)

После первого full-cascade rerun были внесены только prompt/schema-level изменения, без regex rescue и без схлопывания каскада:

- `Gemma 4` extract-family stages получили native `response_schema`;
- `source.scope.select` перестал автоматически выкидывать `mixed`, если источник несёт уникальный cast / production / staging detail;
- `literal_items` были ужаты до настоящих program/repertoire lists;
- `writer_pack.compose` перестал трактовать non-program facts как literal-list coverage;
- `facts.dedup` был разложен на `chunked passes + final reconcile`;
- `facts.merge` получил native `response_schema`;
- `writer.final_4o` получил более жёсткий named-list contract;
- `facts.extract_support` получил явный scarcity/frequency reminder.

### Intermediate full-cascade benchmark (`2026-04-06T13:42Z`)

Artifact:

- json: [lollipop_g4_benchmark_20260406T134207Z.json](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_benchmark_20260406T134207Z.json)
- markdown: [lollipop_g4_benchmark_20260406T134207Z.md](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_benchmark_20260406T134207Z.md)

Key deltas vs previous full-cascade `lollipop g4`:

- `selected_sources`: `tg,vk` -> `site,tg,vk`
- `extract_records`: `12` -> `66`
- `stage_errors`: many `Invalid JSON` -> `[]`
- `writer validation errors`: `9` -> `0`
- `must_cover`: `6` -> `14`
- `chars`: `592` -> `871`

Practical meaning:

- `Gemma 4` перестала ломаться на structured extract family;
- rich `site` packet наконец доехал в downstream;
- `lollipop g4` стал заметно богаче и чище текущего `lollipop`;
- но текст всё ещё оставался слишком сухим и сворачивал named casts в `и другие`.

### Opus follow-up consultation on refreshed `lollipop g4`

На этом обновлённом benchmark был сделан дополнительный `Opus` pass с жёсткими рамками:

- только `LLM-first` / `fact-first`;
- без regex/heuristic semantic rescue;
- без схлопывания каскада;
- `final writer = 4o` не меняется.

Что `Opus` подтвердил:

- ranking всё ещё: `baseline > lollipop g4 > lollipop`;
- главный remaining gap уже не в scope/extract health, а в over-compression downstream;
- лучший baseline hook — это не UGC, а legitimate official atmosphere line, которую current `g4` pipeline пока не сохраняет в usable writer form.

Что из follow-up рекомендаций принято:

- отдельная защита для scarcity/frequency facts (`раз в сезоне`, `два вечера подряд`);
- запрет `writer.final_4o` сокращать grounded named lists до `и другие`;
- больший character budget для rich fact pack;
- фокус на atmospheric / rarity hooks как на следующем quality delta.

Что остаётся на следующий implementation pass:

- выделить отдельный тип/route для `rarity_signal`;
- выделить protected official-source `atmosphere_characterisation`;
- не давать `facts.dedup` схлопывать atmospheric line в dry overview;
- научить `facts.prioritize.lead` выбирать atmosphere / rarity hook раньше dry summary.

### Cached-fixture benchmark after final writer/support retune (`2026-04-06T13:55Z`)

Повторный live benchmark упёрся не в pipeline, а в timeout на refetch `muzteatr39.ru`, поэтому финальный same-input rerun был выполнен на кэшированном source packet из предыдущего `13:42Z` artifact.

Artifact:

- json: [lollipop_g4_benchmark_cachedfixture_20260406T135548Z.json](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_benchmark_cachedfixture_20260406T135548Z.json)
- markdown: [lollipop_g4_benchmark_cachedfixture_20260406T135548Z.md](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_benchmark_cachedfixture_20260406T135548Z.md)

This cached-fixture rerun is useful because:

- source excerpts are identical to the prior `13:42Z` run;
- therefore changes in `lollipop g4` output come from prompt-family changes, not from source drift.

Key deltas vs `13:42Z` `lollipop g4`:

- `chars`: `871` -> `1380`
- `validation errors`: `0` -> `0`
- `must_cover`: `14` -> `19`
- writer stopped collapsing named casts into `и другие`
- programme block expanded from only operetta titles to titles + explicit `дуэты / арии / терцеты / марши / песенки`

Observed text shift:

- better factual density;
- full soloist and ballet lists now survive into public text;
- stronger production-team surface;
- still no good atmospheric opening;
- scarcity/frequency facts still did not surface into final copy on this pass.

Current honest ranking after the latest cached-fixture rerun:

1. `baseline`
2. `lollipop g4`
3. `lollipop`

But the gap is now materially narrower:

- current `lollipop g4` is no longer a dry mini-card;
- it is a full structured announcement with clean validation and much better fact retention;
- baseline still wins on emotional lead, narrative flow, and attendance motivation.

### Deep stage-loss analysis for the missing richness signals

Отдельный audit по cached-fixture artifact был сделан уже не на уровне whole-text impression, а как прямой trace:

- `source -> extract_runs -> dedup_input_records -> merge_raw -> fact_pack -> weight_result -> layout_payload -> writer_pack -> final text`

Ключевой вывод:

- проблема не одна;
- часть сигналов вообще не стала fact-records;
- часть была извлечена, но была схлопнута в более сухой canonical fact;
- writer dryness в этом кейсе в основном downstream symptom, а не первопричина.

#### Что из примеров пользователя уже не потеряно

На latest cached-fixture rerun два примера уже доживают до финала:

- `сценический дым`
- `дуэты / арии / терцеты / марши / песенки`

Они проходят все стадии:

- extract
- dedup
- merge
- writer_pack
- final text

Это важно, потому что их проблема теперь не в выпадении, а в editorial prominence:

- `сценический дым` остаётся low-support tail line в самом конце;
- programme forms рендерятся как literal list, а не как richer musical promise.

#### Где реально пропадают атмосферные и rarity-сигналы

| Source signal | Есть в source | Extracted | Survives dedup/merge | Final text | Где теряется |
| --- | --- | --- | --- | --- | --- |
| `нежная / страстная / полная надежд / одиночества / радости / удивления` | `tg` | `no` | `no` | `no` | уже на `facts.extract.*` |
| `романтические истории из оперетт И. Кальмана` | `tg` | `no` | `no` | `no` | уже на `facts.extract.*` |
| `искрящийся счастьем, любовью и наполненный легкой, волшебной музыкой` | `tg` | `no` | `no` | `no` | уже на `facts.extract.*` |
| `концерт слишком редкий, очень долгожданный гость в афише` | `vk` | `no` | `no` | `no` | уже на `facts.extract.*` |
| `лишь раз в сезоне, зато два вечера подряд` | `vk` | `no` | `no` | `no` | уже на `facts.extract.*` |
| `волшебную атмосферу интриги и игры создали ...` | `site` | `yes` | `no` | `no` | на `facts.dedup` |
| `сценический дым` | `site` | `yes` | `yes` | `yes` | не теряется |
| `дуэты / арии / терцеты / марши / песенки` | `site` + `vk` | `yes` | `yes` | `yes` | не теряется |

#### Concrete trace by signal

1. `TG` emotional block

   Source text:

   - `Нежная или страстная, ... полная надежд или одиночества, радости или удивления`
   - `романтических историй из оперетт И. Кальмана`
   - `Искрящийся счастьем, любовью и наполненный легкой, волшебной музыкой`

   Trace result:

   - source packet: `present`
   - `baseline` proxy per-source facts: `present`
   - `lollipop g4` `extract_runs`: `absent from every specialized extract stage`
   - `dedup/merge/prioritize/layout/writer`: `not recoverable`, because no fact object was created

   Diagnosis:

   - current extract family does not have an explicit owner for grounded atmosphere/emotional framing;
   - combined multi-source excerpt makes the long `site` text dominate attention, while short `tg` atmosphere lines lose salience;
   - current stage contracts bias Gemma toward compact denotative facts, so it treats this block as promo tone rather than as source-grounded event characterisation.

2. `VK` rarity block

   Source text:

   - `концерт слишком редкий, и поэтому очень долгожданный гость в нашей афише`
   - `Эту невероятную концертную постановку можно посмотреть лишь раз в сезоне, зато два вечера подряд`

   Trace result:

   - source packet: `present`
   - `baseline` proxy per-source facts: `present`
   - `lollipop g4` `extract_runs`: `absent everywhere`
   - downstream stages: `absent`, because extract never emitted rarity facts

   Diagnosis:

   - despite the current support-stage rule about scarcity/frequency, the model still did not lift these lines;
   - the real issue is not only wording of the support prompt, but also lack of source-local extraction discipline;
   - `vk` uniqueness is visible to the human reader, but the current extract pass sees one large blended excerpt and defaults to repeated safer facts instead of unique rarity hooks.

3. `site` atmosphere line

   Source text:

   - `Волшебную атмосферу интриги и игры создали заслуженные художники России И. Нежный и Татьяна Тулубьева`

   Trace result:

   - `baseline_fact_extractor`: `present` as `BF06`
   - `facts.extract_support.v1`: `present`
   - `dedup_input_records`: `present`
   - `facts.dedup`: dropped with `canonical_record_id = BF13`, relation = `covered`
   - `merge_raw`: only the dry personnel facts survive:
     - `Художники-постановщики ...`
     - `... являются заслуженными художниками России`
   - final text: no atmosphere line

   Diagnosis:

   - this is a true dedup failure, not an extract failure;
   - dedup incorrectly treats `atmosphere created by X` as covered by the simpler personnel credit `X are production designers`;
   - semantically these are different facts:
     - one is role attribution;
     - the other is event-characterisation.

#### Why baseline still keeps more richness than the cascade

The current gap is now explainable in concrete terms:

1. `baseline` is effectively source-local before it becomes stylistic.
   `per_source_facts` keeps `site`, `tg`, and `vk` signals separate long enough for rarity and atmosphere to survive.

2. `lollipop g4` extract family is currently source-blended too early.
   Every extract stage sees one concatenated excerpt instead of explicit source-local uniqueness obligations.

3. `lollipop g4` has a type-ownership hole.
   There is still no strong owned route for:
   - `official atmosphere characterisation`
   - `rarity / scarcity / anticipation value`

4. `dedup` is still semantically over-aggressive for atmospheric support facts.
   It can collapse `why this feels special` into `who worked on it`.

5. writer is mostly downstream-constrained.
   In the latest artifact `support_context = 1`, and that single fact is only `SC01 = сценический дым`.
   So the writer literally has no atmospheric or rarity fact IDs to turn into a richer lead.

#### Prompt-level next pass that follows from this audit

This should stay strictly `LLM-first / fact-first`:

- no regex rescue;
- no heuristic post-hoc emotional patching;
- no collapse of the cascade into a giant universal prompt.

The concrete next pass should be:

1. Make extract family source-local before global merge.
   Keep the cascade, but each extract family pass should either:
   - run per selected source, or
   - receive an explicit requirement to emit at least the unique grounded facts carried by each selected source block.

2. Add explicit ownership for atmospheric official-source facts.
   Best options:
   - strengthen `facts.extract_theme.challenger.v1` so it owns grounded event-characterisation lines;
   - or add a dedicated small family for `atmosphere_characterisation`.

   Allowed examples should explicitly include:

   - `романтические истории`
   - `истории о любви, надежде, одиночестве, радости, удивлении`
   - `наполненный легкой, волшебной музыкой`

   but only when they are framed as official source description of the event experience.

3. Add explicit ownership for rarity signals.
   Best options:
   - strengthen `facts.extract_support.v1`;
   - or add a dedicated `rarity_signal` route.

   The prompt must explicitly treat as extractable event facts:

   - `редкий гость в афише`
   - `долгожданный`
   - `раз в сезоне`
   - `два вечера подряд`

4. Tighten dedup so atmospheric support cannot be covered by dry role credit.

   New dedup rule needed:

   - if one record describes `effect / atmosphere / experience / why it feels special`
   - and another record only describes `role / attribution / person credit`
   - relation cannot be `covered`

5. Update lead-selection priorities after the upstream pack is fixed.

   Once rarity/atmosphere facts exist in pack:

   - `lead_fact_id` may still stay event-core;
   - but `lead_support_id` should prefer rarity or atmosphere over secondary credits when such support exists.

6. Update layout/writer to surface the restored support facts in a visible place.

   If a canonical pack contains:

   - one strong rarity fact, or
   - one strong atmosphere fact,

   they should not be buried as a final tail sentence.
   Preferred destinations:

   - lead-support sentence;
   - first body paragraph;
   - a short dedicated narrative block when support cluster is dense enough.

## Evaluation gate

Следующий проход можно считать promising только если одновременно выполняются условия:

1. `lollipop g4 > baseline` на серии canary fixtures.
2. `lollipop g4` не ломает текущий `writer.final_4o` contract.
3. Нет thought leakage и transport-side regressions.

Для замены текущего `lollipop` планка выше:

1. `lollipop g4 >= current lollipop` по качеству текста.
2. Нет ощутимой регрессии по стабильности или latency.

Current status after the unified canonical rerun:

- `lollipop g4 >= current lollipop`: `yes`
- `lollipop g4 > baseline`: `not yet`

## Opus prompt-family audit (`2026-04-06`)

После unified benchmark был проведён отдельный `Opus` prompt-audit именно в рамках `LLM-first / fact-first`:

- без замены `writer.final_4o`;
- без regex/heuristic semantic rescue;
- с требованием дать concrete replacement texts для prompt family.

### Stage-loss diagnosis

`Opus` подтвердил ту же общую картину, что показал локальный stage audit:

- current `lollipop` (`Gemma 3 upstream`) ломается уже на `facts.extract`:
  - `extracted_facts = []`;
  - downstream stages бегут по пустому pack;
  - `writer.final_4o` начинает фантазировать generic copy вместо fail-closed поведения.
- current `lollipop g4` теряет факты не в одном месте, а в двух:
  - `facts.extract` всё ещё недобирает часть контента относительно baseline-style pack;
  - `writer.final_4o` слишком агрессивно сжимает dense fact pack в safe summary.

Конкретно по `KALMANIA`:

- `SC01` (`сценический дым`) был извлечён `Gemma 4`, но был suppressed на `facts.prioritize`;
- полный performer/cast pack (`PR08`, `PR09`) дожил до `writer_pack`, но в final text схлопнулся в `Илья Крестоверов, Ольга Литвинова и другие`;
- baseline оказался богаче не потому, что upstream строго лучше, а потому что downstream там охотнее разворачивает материал, хотя и с `UGC leak`.

### Opus proposals

Ключевые предложения `Opus` по prompt family:

1. `facts.extract.multi_source.v1`
   - добавить жёсткий bucket guide;
   - явно считать `frequency/scarcity` (`раз в сезон`, `два вечера подряд`) частью `event_core`, а не logistics;
   - усилить rule на exact title fidelity (`Баядера`, а не `Баядерка`);
   - усилить rule на full-name preservation без обрезания списков;
   - добавить mini example + explicit `raw JSON only, no markdown fences, no commentary`, чтобы снизить вероятность empty extract на `Gemma 3`.
2. `facts.prioritize.v1`
   - добавить explicit weight guide;
   - не suppress-ить scheduling/scarcity facts;
   - не использовать `suppress` как удобный способ выкинуть полезный support fact.
3. `editorial.layout.plan.v1`
   - если people-heavy block становится слишком плотным, переводить его в `style = structured`, а не оставлять narrative blob;
   - не смешивать long cast block и прочие support facts в один абзац.
4. `writer.final_4o.v1`
   - сделать prompt explicit по `must_cover_fact_ids`;
   - запретить схлопывание explicit name lists в `и другие`;
   - обязать сохранять honorifics / affiliations;
   - объяснить, как рендерить `style = structured`;
   - добавить fail-closed rule для empty pack вместо generic filler prose.

### My acceptance read

Что я принимаю как основной next pass:

- `extract`: bucket guide, exact-title fidelity, full-list preservation, JSON-only/no-fences, mini example;
- `prioritize`: stronger non-suppress rule для scarce/scheduling facts и явный anti-over-suppress carry;
- `layout`: dense people blocks должны уметь уходить в `structured`;
- `writer`: explicit fact-coverage rule, no `и другие`, structured rendering, fail-closed on empty pack.

Что я принимаю только частично:

- reviewer / audience reactions как отдельный richness-owner.
  Полный carry этого предложения я не принимаю, потому что current baseline уже показывает, как легко это выливается в `UGC leak`.
  Допустим richer `support_context`, но не опора финального public text на случайные отзывы.

Что я не принимаю:

- любые regex/heuristic semantic recovery предложения;
- giant-prompt merge вместо small-stage `lollipop`;
- замену `4o` writer на другую модель.

## Full-Cascade Rerun (`2026-04-06T12:36Z`)

Этот rerun supersedes earlier minimal-harness benchmark для любых решений по каноническому `lollipop g4`.

### Artifacts

- unified benchmark json: [lollipop_g4_benchmark_20260406T123611Z.json](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_benchmark_20260406T123611Z.json)
- unified benchmark markdown: [lollipop_g4_benchmark_20260406T123611Z.md](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_benchmark_20260406T123611Z.md)
- row snapshot for audit: [lollipop_g4_benchmark_20260406T123611Z_row.json](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_benchmark_20260406T123611Z_row.json)
- full-cascade `lollipop g4` debug artifact: [lollipop_fullcascade_g4_kalmania_debug.json](/workspaces/events-bot-new/artifacts/codex/lollipop_fullcascade_g4_kalmania_debug.json)
- full-cascade `Opus` consultation: [smart-update-lollipop-g4-fullcascade-consultation-opus-2026-04-06.md](/workspaces/events-bot-new/artifacts/codex/reports/smart-update-lollipop-g4-fullcascade-consultation-opus-2026-04-06.md)

### What Changed Relative To The Earlier Rerun

- `baseline` не ререндерился и был reused как fixed reference;
- current `lollipop` тоже был reused как fixed full-cascade `Gemma 3` reference;
- live ререндерился только `lollipop g4`;
- benchmark harness теперь работает поверх reconstructed tracked full family cascade, а не поверх historical minimal approximation;
- для `Gemma 4` были добавлены native structured-output contracts на `facts.prioritize.weight`, `facts.prioritize.lead` и `editorial.layout`;
- follow-up live probes также подтвердили, что `facts.extract_subject` начинает держать JSON, если дать native `response_schema`.

### Final Outputs

| Variant | Chars | Headings | Bullets | Validation errors | Main read |
| --- | --- | --- | --- | --- | --- |
| `baseline` | `1635` | `3` | `0` | `n/a` | richest editorial text, но с `UGC leak` |
| `lollipop` | `750` | `0` | `0` | `89` | overstuffed / validation-broken blob |
| `lollipop g4` | `592` | `3` | `6` | `9` | cleaner structure, но upstream pack пока слишком thin |

### Ranking

1. `baseline`
2. `lollipop g4`
3. `lollipop`

Причины ранжирования:

- `baseline` всё ещё выигрывает по плотности, cast coverage и мотивации к посещению, несмотря на явный `UGC leak`;
- `lollipop g4` уже заметно healthier как full cascade, но проигрывает baseline по richness, потому что upstream pack обедняется до финального writer;
- current `lollipop` теряет fidelity иначе: он не empty, а перегружен дубликатами, wrong literal coverage и downstream overload.

### Stage-Loss Diagnosis From The Full-Cascade Artifact

Новый rerun показал, что `lollipop g4` теряет факты сразу в трёх местах.

1. `scope.select`
   `Gemma 4` выкинула `site` в `background_source_ids`, оставив только `tg` и `vk` в `selected_source_ids`.
   Именно `site` нёс:
   - полный состав солистов;
   - список артистов балета;
   - постановочную группу;
   - сценический дым;
   - richer repertoire framing.

2. Specialized extract family
   Во fresh full-cascade rerun `Gemma 4` всё ещё не вернула JSON для:
   - `facts.extract_subject`
   - `facts.extract_card`
   - `facts.extract_agenda`
   - `facts.extract_profiles`
   - `facts.extract_performer`
   - `facts.extract_participation`
   - `facts.extract_stage.tightened`
   - `facts.extract_theme.challenger`

   Фактически отработали только:
   - `baseline_fact_extractor`
   - `facts.extract_support`

3. `literal_items` contract
   В `baseline_fact_extractor` и `facts.extract_support` поле `literal_items` сейчас захватывает не только настоящие program items, но и:
   - title;
   - dates;
   - composer name;
   - rarity markers вроде `раз в сезоне`;
   - collective categories вроде `солисты`, `оркестр`, `хор`, `балет`.

   Это создаёт ложный downstream contract, где `writer_pack.compose` и validator ведут себя так, будто все эти элементы должны рендериться как literal-list coverage.

### Concrete Runtime Signals

- `lollipop g4`: `selected_sources = tg,vk`
- `lollipop g4`: `extract_records = 12`
- `lollipop g4`: `dedup_input_records = 10`
- `lollipop g4`: `kept_after_dedup = 8`
- `lollipop g4` fact pack:
  - `event_core = 3`
  - `program_list = 1`
  - `people_and_roles = 1`
  - `support_context = 1`
  - `must_cover_fact_ids = 6`
- `lollipop` fact pack in том же rerun:
  - `extract_records = 61`
  - `must_cover_fact_ids = 39`
  - `validation_errors = 89`

То есть full-cascade rerun показал два разных failure modes:

- current `lollipop g4`: **pack слишком бедный**
- current `lollipop`: **pack слишком noisy / overloaded**

### Prompt-Contract Signal From Follow-Up Probes

После full-cascade rerun были сделаны маленькие live probes.

Подтверждено:

- `facts.prioritize.weight` стабилизируется на `Gemma 4`, если дать native `response_schema`;
- `facts.prioritize.lead` стабилизируется на `Gemma 4`, если дать native `response_schema`;
- `editorial.layout` стабилизируется на `Gemma 4`, если дать native `response_schema`;
- `facts.extract_subject` тоже начинает возвращать валидный JSON, если дать native `response_schema`.

Практический смысл:

- проблема extract family сейчас не выглядит как "Gemma 4 semantic mismatch";
- она выглядит как `Gemma 4 structured-output contract mismatch`;
- значит следующий pass должен усиливать native schema discipline и stage-specific prompt contracts, а не уходить в regex rescue.

## Full-Cascade `Opus` Audit (`2026-04-06`)

Отдельный `Opus` pass на full-cascade artifacts дал тот же top-level verdict:

1. `baseline`
2. `lollipop g4`
3. `lollipop`

Ключевые предложения `Opus`, которые я принимаю как direct next pass:

- для `source.scope.select`:
  - `mixed` source не должен автоматически уходить в background;
  - если source несёт unique cast/production/program richness, он должен оставаться в `selected_source_ids`.
- для extract family:
  - дать `Gemma 4` native `response_schema` на все extract stages;
  - прямо зафиксировать, что `literal_items` допустимы только для настоящих explicit program/repertoire lists;
  - не класть в `literal_items` title/date/name/scarcity/ensemble categories.
- для `facts.prioritize.weight`:
  - scarcity/frequency facts вроде `раз в сезоне` и `два вечера подряд` не должны падать в `low`;
  - эти facts являются attendance-relevant, а не flavour-only.
- для `editorial.layout`:
  - ensemble / people blocks без named cast нужно держать narrative, а не автоматически переводить в pseudo-literal coverage;
  - list/structured style должен оставаться привилегией настоящих repertoire lists или реально dense named blocks.
- для `writer.final_4o`:
  - пока не делать radical rewrite;
  - сначала починить upstream fact richness и `literal_items` contract;
  - только потом точечно убрать filler phrases и сделать stricter anti-invention layer.

Что я не принимаю и после fresh `Opus` audit:

- regex/heuristic semantic recovery;
- giant universal extractor вместо family cascade;
- замену final `writer.final_4o`.

### Immediate Next Experiment

Следующий точный experiment после этого rerun:

1. дать native `response_schema` всем `Gemma 4` extract stages;
2. переписать extract-family contract так, чтобы `literal_items` были только у real program lists;
3. ослабить `scope.select` fail-closed для `mixed` sources с unique factual richness;
4. rerun тот же `KALMANIA` full-cascade benchmark без изменения `baseline` и current `lollipop`.

Ожидаемый критерий успеха:

- `site` возвращается в `selected_source_ids`;
- stage-level `Invalid JSON` уходит хотя бы из большинства extract families;
- `extract_records` и `must_cover_fact_ids` растут, но без current `lollipop`-style overload;
- validator errors падают к `0..2`;
- итоговый public text становится богаче baseline-style fact coverage без `UGC leak`.

## Downstream Retune After Rich-Pack Recovery (`2026-04-06T16:21Z`)

После предыдущего full-cascade rerun стало видно, что главный bottleneck уже downstream:

- rich `Gemma 4` fact pack доезжает до `writer_pack`;
- но `lead` съедает list-heavy `program_list` fact;
- `writer.final_4o` потом растворяет repertoire list в прозе вместо отдельного section.

Чтобы проверить именно эту гипотезу, baseline и current `lollipop` не пересчитывались.

Был сделан live rerun только для:

- `facts.prioritize.lead`
- `editorial.layout`
- `writer.final_4o`

на уже богатом weighted pack из [lollipop_g4_benchmark_20260406T153139Z.json](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_benchmark_20260406T153139Z.json).

Новые artifacts:

- local json: `artifacts/codex/lollipop_g4_downstream_retune_20260406T162145Z.json`
- local markdown: `artifacts/codex/lollipop_g4_downstream_retune_20260406T162145Z.md`

### Что было изменено

Только prompt/contract + structural carry, без regex rescue и без схлопывания каскада:

- `facts.prioritize.lead`
  - list-heavy `program_list` facts с `literal_items` теперь резервируются для downstream `program` section, если есть grounded non-list support fact;
- `editorial.layout`
  - cleaner может detatch-ить repertoire list из `lead` / mixed blocks и реинжектить его как отдельный `program` block;
- `writer.final_4o`
  - prompt теперь явно требует real markdown bullet list для `literal_items`;
  - prose mention list items больше не считается достаточным coverage.

### Result

Новый downstream-only `lollipop g4` дал:

- `chars = 1740`
- `headings = 4`
- `bullets = 6`
- `validation_errors = 0`
- `validation_warnings = 0`

Практически это означает:

- lead снова стал event-facing:
  - `Концерт «Кальмания» ... является редким и долгожданным гостем в афише`
- repertoire list вернулся как отдельный `### Программа` section;
- atmosphere block больше не конкурирует с literal list в первом абзаце;
- named cast / team block сохранился без collapse в `и другие`.

### New Ranking Signal

Это не новый full benchmark, а focused downstream rerender.

Но он уже даёт важный quality signal:

- новый `lollipop g4` выглядит сильнее предыдущего `lollipop g4` rerun `2026-04-06T15:31Z`;
- по narrative richness и structure он существенно ближе к `baseline`;
- главное: теперь `lollipop g4` использует преимущество каскада не только для fact retention, но и для более сильного `4o` prompt.

Пока ещё остаются слабые места:

- финальный tail всё ещё тащит age/smoke/scarcity block немного как service note, а не как органичную концовку;
- section `Особенности программы` пока звучит суше baseline;
- для окончательного verdict нужен следующий unified benchmark artifact, где кэшированные `baseline` и current `lollipop` будут сравнены уже с этим новым downstream-retuned `lollipop g4`.

## Style / Salience Follow-Up (`2026-04-06`, evening)

После ручного чтения downstream artifact стало ясно, что проблема уже не только в retention:

- literal repertoire list и cast coverage были восстановлены;
- но итоговый текст всё ещё звучал слишком чеканно и отчётно;
- strongest hook часто оказывался в конце абзаца, а не в начале;
- writer свободно пропускал phrases вроде `посвящен`, `характеризуется`, `представлены истории`, если literal coverage уже была валидна.

Это противоречит канонике из:

- [smart-update-lollipop-writer-final-prompts.md](/workspaces/events-bot-new/docs/llm/smart-update-lollipop-writer-final-prompts.md)
- [fact-first.md](/workspaces/events-bot-new/docs/features/smart-event-update/fact-first.md)

где уже зафиксированы:

- живой культурный register вместо report/card voice;
- сильный hook в первом абзаце;
- запрет на `посвящ...`-style dry lead;
- запрет на age/access leakage;
- запрет на collapse explicit named lists в `и другие`.

### Что было добавлено в contract

- lead/layout/writer prompts стали явно требовать:
  - frontload strongest grounded hook;
  - не открывать narrative section generic ensemble line, если в нём есть более vivid atmosphere / rarity fact;
  - не уводить текст в report-style formulas.
- writer validator теперь блокирует:
  - `lead.cliche_posvyash`
  - `poster.leak`
  - `age.leak`
  - `named_list.collapsed_to_and_others`
  - `style.report_formula:*`
  - `style.promo_phrase:*`
- correction retry теперь получает не только raw error codes, а конкретные rewrite instructions.

### Latest focused rerenders

Локальные артефакты:

- `artifacts/codex/lollipop_g4_downstream_retune_20260406T171835Z.md`
- `artifacts/codex/lollipop_g4_downstream_retune_20260406T172042Z.md`

Практический вывод:

- style-aware retry действительно вытягивает текст из сухого report-like режима;
- но без дополнительных anti-promo guardrails `4o` охотно переходит в рекламные формулы вроде `уникальная возможность` / `настоящий праздник`;
- после добавления promo validator это уже не считается acceptable output.

То есть следующий remaining gap сузился:

- не `fact loss`;
- не `literal list loss`;
- а баланс между живым hook и anti-promo discipline.

## Full Benchmark + Profile (`2026-04-07`)

Новый full benchmark был прогнан уже после prompt-family retune, но:

- с reuse canonical cached fixture;
- без пересчёта `baseline`;
- без пересчёта current `lollipop`;
- с новым live rerun только для `lollipop g4`.

Artifacts:

- full benchmark json: [lollipop_g4_benchmark_20260407T082914Z.json](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_benchmark_20260407T082914Z.json)
- full benchmark markdown: [lollipop_g4_benchmark_20260407T082914Z.md](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_benchmark_20260407T082914Z.md)
- downstream-only spot rerender after extra writer guardrails: [lollipop_g4_downstream_retune_20260407T083152Z.md](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_downstream_retune_20260407T083152Z.md)

### Result

По честному top-level verdict всё ещё:

- `baseline > lollipop g4 > lollipop`

Но `lollipop g4` теперь уже не сухой failure-case, а полноценный comparable variant:

- `baseline`
  - `chars = 1635`
  - `3` headings
  - no generic style flags in current quality profile
- `lollipop`
  - `chars = 750`
  - `89` validation errors
  - poster/promo leakage remains
- `lollipop g4`
  - `chars = 1664`
  - `4` headings
  - `6` bullets
  - `lead_hook_signals = ['rarity', 'atmosphere']`
  - `validation_errors = 0`
  - `validation_warnings = 1` (`title.keep_overridden_by_model`)
  - no `report_formula_hits`
  - no `promo_phrase_hits`

### Quality read

`lollipop g4` улучшился по сравнению с предыдущими `2026-04-06` passes:

- hook наконец вернулся в lead;
- literal program section выжил как реальный markdown list;
- cast/team block держится без collapse в `и другие`;
- финальный текст уже длиной не уступает `baseline`.

Но `baseline` всё ещё сильнее как public copy:

- lead у `baseline` живее и менее схематичен;
- `lollipop g4` всё ещё любит meta-формулы вроде `редкое музыкальное событие` и абстрактные atmosphere sentences;
- upstream `program_list` fidelity всё ещё шумит:
  - `PL02` дошёл до writer как `«Баядеры»`, `«Марицы»`, `«Фиалки Монматра»`, хотя это хуже canonical title fidelity;
- current writer может быть clean по validator, но всё ещё stylistically weaker than baseline.

### Profiling

Новый benchmark впервые сохранил полный runtime profile для `lollipop g4`:

- `wall_clock_sec = 920.334139`
- `model_active_sec = 776.121102`
- `sleep_sec = 144.1838`
- `gemma_calls = 41`
- `four_o_calls = 2`

Top slowest stage families:

- `facts.extract_support.v1 = 199.358783`
- `facts.extract_participation.v1 = 184.181984`
- `facts.dedup = 149.081026`
- `sleep.gemma_gap = 144.1838`
- `baseline_fact_extractor.v1 = 44.380899`
- `facts.merge = 37.34006`

Практический смысл:

- каскад уже даёт measurable quality uplift относительно current `lollipop`;
- но его latency cost сейчас очень высок;
- bottleneck сидит не только в writer, а прежде всего в `extract_support`, `extract_participation` и `dedup`.

### Reliability notes

Этот full run всё ещё был не идеально clean по transport/runtime:

- `source.scope.extract:500 Internal error encountered.`
- `source.scope.select:500 Internal error encountered.`
- `baseline_fact_extractor.v1[vk]:500 Internal error encountered.`
- `facts.extract_card.v1[tg]:500 Internal error encountered.`
- `facts.extract_support.v1[site]:`
- `facts.extract_participation.v1[site]:`

Несмотря на это, pipeline дошёл до валидного final output. Но как rollout-risk это важно: текущий latency / failure surface у `lollipop g4` существенно выше, чем у `baseline`.

### Follow-up after profile run

После profile benchmark был сделан ещё один дешёвый downstream-only rerender на том же свежем `writer_pack`:

- [lollipop_g4_downstream_retune_20260407T083152Z.md](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_downstream_retune_20260407T083152Z.md)

Он показал важную вещь:

- даже после новых anti-meta guardrails `4o` всё ещё тяготеет к `приглашает вас`, `для ценителей`, `настоящий праздник`, `обещает стать`.

То есть next pass нужно направить уже не на raw fact retention, а на более жёсткий final-writer register:

- no reader-address;
- no invite/promise language;
- no `X — это ...`;
- stronger positive examples of event-led cultural-digest prose;
- отдельно вернуть canonical title fidelity для program items upstream.

## Opus Follow-Up Consultation (`2026-04-07`)

После profile run был получен ещё один предметный `Opus` pass уже не про architecture, а про точечный next retune.

### Что `Opus` подтвердил

- `baseline` выигрывает не по объёму facts, а по public voice;
- biggest remaining gap — не empty pack, а voice-quality loss на `extract -> dedup -> writer`;
- `lollipop g4` уже достаточно структурирован и grounded, чтобы дожимать его через prompt-family retune, а не через новый redesign.

### Что я принимаю напрямую

- source-local uniqueness obligation для extract-family;
- literal title fidelity для repertoire/program items;
- explicit protection of rarity/scarcity as first-class attendance facts;
- dedup rule `atmosphere/effect != covered by dry role credit`;
- lead-support priority `rarity -> atmosphere -> event action -> secondary credit`;
- positive writer exemplars + expanded anti-promo / anti-report banlist;
- retry instruction with explicit `third register` requirement instead of oscillation between promo and report.

### Что я не принимаю

- any regex/heuristic semantic patching after the fact;
- collapse of the cascade into one universal prompt;
- per-source full cascade reruns as the default path;
- final writer swap away from `4o`;
- validator softening to let `soft promo` pass through.

### Implementation note

Следующий implementation pass поэтому был ограничен именно prompt/contract changes:

- `facts.extract`
- `facts.prioritize.lead`
- `editorial.layout`
- `facts.dedup`
- `writer.final_4o`

без изменения общей архитектуры `Gemma 4 upstream + final 4o`.

## Post-Opus Prompt Pass Benchmark (`2026-04-07T22:20Z`)

После прямого применения Opus-guided prompt changes был сделан ещё один live rerun только для `lollipop g4`:

- baseline: reused from [lollipop_g4_benchmark_20260407T082914Z.json](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_benchmark_20260407T082914Z.json)
- current `lollipop`: reused from the same artifact
- new `lollipop g4`: [lollipop_g4_benchmark_20260407T222032Z.json](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_benchmark_20260407T222032Z.json)
- human-readable snapshot: [lollipop_g4_benchmark_20260407T222032Z.md](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_benchmark_20260407T222032Z.md)

### What improved for real

Upstream richness is now visibly better than in the morning `2026-04-07T08:29Z` run:

- `extract_records` выросли до `71` (было `64`);
- `kept_records_after_dedup` выросли до `41` (было `27`);
- в canonical `support_context` теперь явно живут:
  - `SC01`: `искрящийся счастьем, любовью и наполненный легкой, волшебной музыкой...` from `tg`
  - `SC07`: `слишком редкий и очень долгожданный гость...` from `vk`
  - `SC08`: `волшебная атмосфера интриги и игры` from `site`
- `lead_support_id` реально переключился на rarity hook `SC07`, а не на dry secondary credit.

То есть главный риск earlier passes теперь частично снят: upstream уже не starving writer of the best hooks.

### What is still wrong

Итоговый `lollipop g4` text пока всё ещё не rollout-ready:

- `validation_errors = 2`
  - `lead.meta_opening`
  - `style.report_formula:program_filled`
- new prose is richer, but still weaker than `baseline`
- repertoire title fidelity осталась проблемой:
  - `Баядеря`
  - `Фиалка Монматра`
- tail still drifts into service/admin material because `allergy / rows 1–3` notice survives too late in the pack.

Практический вывод:

- `Opus`-guided extract / lead / layout pass сработал;
- bottleneck теперь уже очень узкий и почти целиком writer-facing.

## Writer-Only Spot Rerender On Fresh Pack (`2026-04-07T22:23Z`)

Чтобы не пересчитывать снова весь cascade, на свежем `writer_pack` из `20260407T22:20Z` был сделан отдельный writer-only rerender:

- json: [lollipop_g4_writer_only_rerender_20260407T222326Z.json](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_writer_only_rerender_20260407T222326Z.json)
- markdown: [lollipop_g4_writer_only_rerender_20260407T222326Z.md](/workspaces/events-bot-new/artifacts/codex/lollipop_g4_writer_only_rerender_20260407T222326Z.md)

### Spot result

Этот pass подтвердил, что current bottleneck действительно narrow:

- `lead.meta_opening` был снят;
- но text всё ещё упирается в:
  - `poster.leak`
  - `style.report_formula:program_filled`

То есть writer уже начинает использовать restored rarity/atmosphere hook correctly, но всё ещё:

- тянет literal rarity wording обратно к `афише`;
- легко срывается в `концерт наполнен ...`.

Следующий pass поэтому должен быть уже почти чисто final-writer:

- stronger anti-`афиша` rewrite discipline for rarity hooks;
- stronger ban + retry rewrite for `концерт/вечер/программа наполнены ...`;
- separate decision on whether `allergy / rows 1–3` belongs in public prose at all.
