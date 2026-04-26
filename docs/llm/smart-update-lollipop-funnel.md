# Smart Update Lollipop Funnel

Статус: `dry-run`

`lollipop` — исследовательская shadow-ветка для `Smart Update`. Она не меняет baseline runtime и нужна для сборки более качественного fact-first pipeline.

## Каноническая схема

```text
source.scope
-> facts.extract
-> facts.dedup
-> facts.merge
-> facts.prioritize
-> editorial.layout
-> writer_pack.compose
-> writer_pack.select
-> writer.final_4o
```

## Принципы

- `Smart Update` baseline остаётся эталоном.
- Все grounded facts должны доживать до `facts.merge.emit` или явно классифицироваться как background/uncertain.
- Стадии маленькие и одноцелевые.
- Для `Gemma` используются компактные self-contained prompt'ы с явной JSON-схемой и короткими примерами.
- Для full-family lab reruns с одним Gemma key нужно соблюдать TPM-aware execution discipline:
  - upstream families должны поддерживать `EVENT_IDS` subset reruns и safe run-label/input overrides;
  - shared Gemma caller должен иметь proactive pacing (`LOLLIPOP_GEMMA_CALL_GAP_S`), а не только reactive `429/tpm` retries.
  - Gemma-heavy canary reruns допустимо переносить в Kaggle через `kaggle/execute_lollipop_canary.py`, а не продолжать локальный монолитный batch, если local provider path режется по TPM или location policy.
- Финальный `4o` допускается один раз и только в самом конце.

## Gemma 4 migration track

- Каноническое исследование по возможному переходу `lollipop` с `Gemma 3` на `Gemma 4`: [smart-update-lollipop-gemma-4-migration.md](/workspaces/events-bot-new/docs/llm/smart-update-lollipop-gemma-4-migration.md)
- Live eval / benchmark log: [smart-update-lollipop-gemma-4-eval.md](/workspaces/events-bot-new/docs/llm/smart-update-lollipop-gemma-4-eval.md)
- Канонический target для `lollipop g4`: `Gemma 4` только в upstream Gemma-backed stages; `writer.final_4o` остаётся неизменным.
- Любой benchmark, который меняет final writer вместо upstream-only migration, считается non-canonical exploratory experiment и не может быть rollout gate.
- Этот трек пока не меняет текущую схему funnel автоматически: rollout должен опираться на отдельные проектные измерения качества, latency и cost.
- profiling contract для `lollipop g4` теперь обязан фиксировать не только итоговый текст, но и:
  - `wall_clock_sec`
  - `model_active_sec`
  - `sleep_sec`
  - `gemma_calls / four_o_calls`
  - top slowest stage families, чтобы quality gain можно было сравнивать с latency cost.

## `lollipop_g4_fast`

Статус: `lab variant`, не production default.

Цель: проверить компактный `Gemma 4 upstream + final 4o` путь, который по итоговому Telegraph-тексту должен быть лучше `baseline`. Latency target остаётся `<= 1.5x` от baseline, но на текущей quality-first итерации rollout-gate допускает hard cap `<= 2.5x`; качество важнее попыток ускоряться за счёт более слабой модели.

Критическая оценка выбранной архитектуры после live-прогонов:

- default path: `parallel source-local fast extractor -> deterministic pass-through pack/layout from LLM-owned facts -> writer.final_4o`;
- сильная сторона: call-count остаётся `N source Gemma calls + 1 final 4o`, поэтому fast может реально приблизиться к baseline latency, не отказываясь от source-local uniqueness для rarity/atmosphere/literal coverage;
- главный риск: deterministic pack/layout не должен становиться владельцем смысла. Python имеет право только убрать `drop/infoblock_only/suppress`, сохранить LLM-owned `bucket/salience/hook_type/literal_items/dedup_key`, собрать writer pack и применить safety guardrails;
- LLM merge (`fast.merge_pack.v1`) оставлен только как explicit fallback `LOLLIPOP_G4_FAST_LLM_MERGE=1`: он полезен для сложного semantic dedup, но live KALMANIA/VIVAT показали, что обязательный merge ломает speed target;
- второй риск: single rich extractor может стать too broad и вернуть сухой baseline-like summary. Prompt contract поэтому явно требует source-local rarity/atmosphere/local context, literal title fidelity, named lists и anti-report/anti-promo voice;
- optional planner допустим только как fallback за `LOLLIPOP_G4_FAST_PLANNER=1`, default off. Его наличие не отменяет speed-gate и не должно становиться новым обязательным Gemma stage.

Stage contract:

| Stage | Runtime | Owner | Fail policy |
| --- | --- | --- | --- |
| `fast.extract_per_source.v1` | `Gemma 4 31b`, один компактный call на источник, native `response_schema`, coverage-first без fixed fact cap, `max_tokens=2000`, parallel by default | source-local facts + `bucket/salience/hook_type/literal_items/dedup_key/role_class`; source mismatch emits only `drop`; long cast lists stay dense instead of one-record-per-role; plot/character premise for stage titles is preserved as narrative material, not collapsed into cast credits | empty source payload allowed, but benchmark flags fact loss |
| `fast.merge_pack.v1` | optional compact `Gemma 4` call, `LOLLIPOP_G4_FAST_LLM_MERGE=1` only | fallback semantic merge/dedup for hard duplicate cases | default off; any use counts toward latency budget |
| `fast.layout_assemble` | Python-only default | deterministic pass-through/block assembly from LLM labels | may reserve literal program facts for program block; must not invent, rewrite, summarize, or drop semantic facts for compactness |
| `fast.layout_planner.v1` | optional `Gemma 4`, `LOLLIPOP_G4_FAST_PLANNER=1` | lead/layout fallback only | default off; any use counts toward latency budget |
| `writer.final_4o.v1` | one `gpt-4o` final writer call, no fast-specific retry/repair | public Telegraph prose | same validation as lollipop; fast-specific prompt is dynamically assembled from `meta.writer_profile` |
| `validate` | Python | style, literal coverage, infoblock leak, speed gate | hard gate blocks rollout evidence |

Fast writer не имеет deterministic repair/rescue слоя и не дожимается вторым fast-specific correction pass. Если final output требует восстановления фактов, переписывания lead или удаления рекламного filler после writer stage, это считается провалом основного prompt/stage contract, а не поводом добавлять regex cleanup.

Source-local extraction is parallel by default for `lollipop_g4_fast`: each source is still handled by its own Gemma 4 call and owns its own semantic labels, but independent source calls do not wait on each other. Set `LOLLIPOP_G4_FAST_SERIAL_EXTRACT=1` only for provider debugging or conservative pacing. Default merge is pass-through over relevant LLM-owned facts; the optional LLM merge runs only after all source-local facts are available and only with `LOLLIPOP_G4_FAST_LLM_MERGE=1`.

Чтобы writer не склеивал карточные fragments через `X — это ...`, fast extractor обязан отдавать `event_core` / `rarity` facts как законченные event-facing clauses с действием/глаголом, а не как именные группы.

Текущий контракт `lollipop_g4_fast.v2` после неуспешного KALMANIA-прогона `2026-04-26T12:19Z` больше не трактует fast как summary mode:

- extractor не имеет лимита `3-6 facts/source`; rich source обязан вернуть все distinct event-facing facts, включая формат, редкость, атмосферу, цитатно-точные official phrases, роли и literal program;
- default pack не имеет лимита `5-9 final facts`; он пропускает relevant LLM-owned facts и может склеивать только exact/near duplicates по LLM-owned `dedup_key`, не выбрасывая event-defining facts ради краткости;
- deterministic pack не удаляет второй `event_core` только из-за совпавшего `hook_type`;
- writer получает `meta.writer_profile` (`rich_case`, `lead_strategy`, `has_rarity`, `has_atmosphere`, `has_quote_candidate`, `has_literal_program`, `has_named_roles`, `must_cover_fact_count`, `narrative_fact_count`) и на его основе получает разные prompt-additions для разных типов сильного текста;
- фиксированный output shape `lead -> Программа -> Участники` запрещён: section order остаётся из writer pack, но текст может разворачиваться настолько, насколько нужно для fact coverage и живого городского анонса.
- независимые source-local extractor calls выполняются параллельно по wall-clock, чтобы latency не оплачивалась потерей фактов.

`2026-04-26` quality follow-up after VIVAT evidence: fast extractor explicitly keeps protagonist/story-engine facts for musicals and family stage titles (`учёный, предприимчивый и весёлый мечтатель`, `приключения в обычных ситуациях`, `герой для взрослых и детей`) as event-facing narrative material. Writer profile now marks people-heavy cases as `rich_case` when there are at least three non-people narrative facts, so the final 4o prompt asks for real narrative before cast/credits headings instead of turning the description into a short lead plus role sheet.

`literal_items` проходят структурный allowlist по extractor-owned `literal_items/text/evidence` и source-local excerpt substring gate: pipeline не извлекает названия regex-ом из raw source и не придумывает замену, а только не пропускает literal value, которого не было в LLM-owned upstream fields / исходном excerpt.

`role_class` — LLM-owned routing label для `people_and_roles`: `production_team`, `cast`, `ensemble`, `none`. Python layout может разделять блоки участников по этому label (`Постановочная группа` vs `Действующие лица и исполнители`), но не определяет эти классы сам.

Для people-heavy theatre pages fast extractor не должен превращать каждую роль в отдельный record: фактологическая полнота сохраняется плотной строкой cast/team list, а не количеством JSON records. Это снижает latency и не является semantic compression, если все имена/роли остаются внутри LLM-owned fact text.

Все Gemma-backed stages fast family используют quality-first `31b` path. `26b` не используется ни для extraction, ни для merge, ни для planner fallback.

Hard gates for `lollipop_g4_fast` benchmark evidence:

- `latency.target_missed`: `fast.wall_clock_sec / baseline.wall_clock_sec > 1.5`, допустимо только если reviewer vote показывает качество выше baseline;
- `latency.hard_budget_exceeded`: `fast.wall_clock_sec / baseline.wall_clock_sec > 2.5`, блокирует rollout evidence;
- `gemma_call_count_overrun`: default fast path should use `N source Gemma calls + 1 final 4o`; optional LLM merge/planner count only when explicitly enabled;
- `literal.title_mutation`: fast pack/optional merge must not emit a literal title that is absent from source-local extractor `literal_items` and extractor-owned text/evidence, and required repertoire/work titles must survive as exact markdown bullets;
- `fact_loss_vs_baseline`: manual/reviewer gate when baseline covers event-defining facts absent from fast output;
- existing writer gates: `poster.leak`, `age.leak`, `infoblock.leak`, report-style formulas, promo phrases, missing literal bullets, collapsed named lists.

Implementation surface:

- [fast_extract_family.py](/workspaces/events-bot-new/smart_update_lollipop_lab/fast_extract_family.py) owns extractor/planner prompt text and provider-compatible schemas;
- [fast_cascade.py](/workspaces/events-bot-new/smart_update_lollipop_lab/fast_cascade.py) owns deterministic fast cascade plumbing;
- [writer_final_4o_family.py](/workspaces/events-bot-new/smart_update_lollipop_lab/writer_final_4o_family.py) has one `meta.variant == "lollipop_g4_fast"` prompt branch;
- [benchmark_lollipop_g4.py](/workspaces/events-bot-new/scripts/inspect/benchmark_lollipop_g4.py) accepts `--variants lollipop_g4_fast` and records speed ratio against baseline.
- Для локального benchmark допускается explicit opt-in `--allow-google-key2-for-baseline` / `LOLLIPOP_BENCHMARK_ALLOW_GOOGLE_KEY2_BASELINE=1`, если локальный `.env` специально хранит isolated benchmark key в `GOOGLE_API_KEY2`; это process-local alias и не меняет production key routing.

## Активные families

### `source.scope`

Назначение: отделить нужный event scope от шума, multi-event и mixed-phase contamination.

Базовые стадии:

- `scope.extract`
- `scope.select`

Отдельный риск-класс:

- mixed-phase series post
  Документ: [smart-update-lollipop-casebook.md](/workspaces/events-bot-new/docs/llm/smart-update-lollipop-casebook.md)
  Prompt pack: [smart-update-lollipop-mixed-phase-prompts.md](/workspaces/events-bot-new/docs/llm/smart-update-lollipop-mixed-phase-prompts.md)

### `facts.extract`

Текущий рабочий bank:

- `baseline_fact_extractor`
- `facts.extract.subject`
- `facts.extract.card`
- `facts.extract.agenda`
- `facts.extract.support`
- `facts.extract.performer`
- `facts.extract.participation`
- `facts.extract.stage.tightened`
- `facts.extract.theme.challenger`

Текущая каноника:

- после консультационного owner-audit `2026-03-11` следующий richness-owner подтверждён как `facts.extract`, а не `writer.final_4o` / `editorial.layout`
- для `выставка` extract prompt теперь жёстче сохраняет curatorial/history/collection-detail facts как first-class evidence:
  - `facts.extract.card` может поднимать название экспозиции, размер коллекции, эпоху и институциональную связку
  - `facts.extract.profiles` может сохранять maker/designer/item-level detail даже без named people
  - `facts.extract.theme` / `facts.extract.concept` должны предпочитать исторический контекст и кураторскую рамку, а не только общий `выставка посвящена ...`
  - `facts.extract.performer` не должен вытаскивать bare subject-name из названия выставки; performer stage для выставок остаётся пустым без явной role/status/credibility evidence
- `facts.extract.support` больше не должен выводить широкую аудиторию / возраст / accessibility из friendly title или marketing tone; такие visitor facts допустимы только при явном source evidence
- `iter9` carry after the `2026-04-07` benchmark/profile pass:
  - Gemma-family extract prompts теперь явно предпочитают source-shaped natural Russian вместо сухих rewrites;
  - `subject/card` stages должны предпочитать event-facing action wording (`звучат`, `собраны`, `идёт`, `показывают`) вместо автоматического `посвящен`;
  - `theme/support` stages должны сохранять official mood/context ближе к source wording, а не flatten-ить его в `характеризуется` / `наполнена` / `представлены`.
- `iter10` carry after the 2026-04-07 Opus-guided prompt pass:
  - каждый extract-stage теперь явно несёт `source-local uniqueness obligation`: если конкретный source block несёт уникальный grounded atmosphere / rarity / staging fact, он должен стать first-class record;
  - rarity/scarcity signals (`раз в сезоне`, `два вечера подряд`, `редкий гость`, `долгожданный`) закреплены как attendance facts, а не promo filler;
  - extract contract теперь жёстче требует literal title fidelity для named works (`Баядера`, `Фиалки Монмартра` и т.п.) без склонения/нормализации.

### `facts.dedup`

Назначение: различать `covered / reframe / enrichment` без потери meaningful facts.

### `facts.merge`

Назначение: собрать canonical fact pack:

- `event_core`
- `program_list`
- `people_and_roles`
- `forward_looking`
- `logistics_infoblock`
- `support_context`
- `uncertain`
- `provenance`

Текущая каноника:

- `facts.merge iter5` может гидрировать старый `bucket.v2` trace только если состав `record_id` совпадает с текущим `merge_records`
- если upstream `facts.extract` / `facts.dedup` дал новый record-set, `bucket.v2` должен пересчитываться заново на актуальном payload, а не падать на stale hydrated decisions

### `facts.prioritize`

Назначение: расставить salience для последующих editorial steps.

Текущая каноника:

- full `12`-event family-lab `iter3` уже прогнан на `facts.merge iter5`
- weight stage по-прежнему сохраняет полный grounded pack, но теперь может добавлять узкий deterministic rescue для exhibition/history context из `raw_facts`, если upstream evidence уже существует
- `lead` cleaner теперь знает про title opacity:
  - для bare/opaque `presentation` lead должен вытаскивать event-action fact вроде `на презентации расскажут ...`, а не открываться только описанием проекта;
  - для bare/opaque `кинопоказ` secondary lead fallback теперь может уходить из `people_and_roles` в более событийный / film-defining fact из `support_context`, если чистого screening anchor upstream не хватает
- сам `lead` prompt теперь тоже жёстче фиксирует этот contract:
  - в input/prompt явно передаются `title_is_bare` и `title_needs_format_anchor`;
  - prompt содержит positive/negative examples для `screening`, `presentation`, `lecture`;
  - biography/cast/project-definition openings считаются wrong lead, если есть более событийный event-facing fact
- `iter7` carry after the 2026-04-06 `Gemma 4` downstream retune:
  - list-heavy `program_list` facts с `literal_items` больше не должны жить в lead/support по умолчанию, если есть grounded non-list support fact;
  - deterministic lead cleaner может резервировать такой repertoire list для downstream `program` section, чтобы final writer не терял literal coverage в первом абзаце
- `iter8` carry after the 2026-04-06 editorial pass:
  - lead prompt for concert/list-heavy cases explicitly prefers an announcement-like hook over dry `событие посвящено ...`;
  - rarity / atmosphere support should help prevent catalog-style openings
- `iter9` carry after the 2026-04-07 Opus-guided prompt pass:
  - lead_support selection order теперь явно фиксируется как `rarity/scarcity -> atmosphere/emotional characterisation -> distinguishing event action -> secondary credit`;
  - deterministic cleaner может заменить secondary people/role support на более сильный narrative hook из `support_context` / `forward_looking`, если такой hook уже grounded в pack.
- после weighting применяется deterministic `narrative_policy = include|suppress`
- `suppress` используется для:
  - cross-promo schedules и других `other events` spillovers
  - low-specificity support fillers, когда событие уже закрыто более сильными `high/medium` facts
  - hospitality/service-detail lines (`печенье`, `чай`, подобные visitor-comfort notes), если они не несут narrative value
  - generic audience-pitch lines вроде `мероприятие будет интересно ...`, если pack уже закрыт более содержательными facts
  - age/access restriction lines, которые не должны попадать в public narrative prose
- post-iter4 cleanup добавил ещё один narrow deterministic carry без нового stage split:
  - для `кинопоказ`, где upstream не дал `event_core/forward_looking`, но в `support_context` есть synopsis / adaptation / plot facts, до `editorial.layout` они поднимаются из `low` в `medium`, чтобы screening copy не схлопывался в cast-only reference note
- downstream `editorial.layout` должен потреблять только `include` facts; suppressed items остаются audit-only
- audit layer дополнительно считает `lead_format_anchor_present`, чтобы opaque-title cases можно было мерить не только вручную

### `editorial.layout`

Текущая каноника:

- один `Gemma` stage `editorial.layout.plan.v1`
- deterministic `precompute`
- deterministic `validate`
- full `12`-event family-lab `iter2` уже прогнан на `facts.prioritize iter3`
- post-run `Gemini` verdict: `GO` для перехода к deterministic `writer_pack.compose`
- carry из post-run review уже вшит в prompt contract:
  - `title_is_bare` подаётся прямо в prompt input
  - `all_fact_ids` подаётся прямо в prompt input как явный checklist
- follow-up clarity retune после `iter2 vs baseline` добавил ещё два deterministic carries:
  - `title_needs_format_anchor` считается до `Gemma`
  - `non_logistics_total` и `heading_guardrail_recommended` теперь тоже передаются в prompt, чтобы dense cases не схлопывались в один blob
  - semantic headings теперь можно сохранять не только при `rich`, но и на opaque-title `presentation` / `кинопоказ`, а также вообще при `non_logistics_total >= 4`, если event реально разваливается на смысловые блоки; сами heading labels снова выбирает `Gemma`, а не deterministic cleaner
  - dense cases с `non_logistics_total >= 5` без headings не переписываются детерминированно, но получают явный audit flag `missing_headings_for_dense_case`
- `iter6` carry after the 2026-03-11 rerun:
  - precompute now explicitly carries `body_cluster_count`, `body_block_floor`, and `multi_body_split_recommended`, so rich post-lead material can ask for two narrative sections without deterministic heading labels
  - deterministic cleaner may split one oversized body block at a bucket-cluster boundary as a safety floor, but still leaves second-block heading selection to the model / downstream prose rather than inventing labels in Python
- `iter7` carry after the 2026-04-06 `Gemma 4` downstream retune:
  - deterministic cleaner may detach list-heavy repertoire facts from `lead` / mixed body blocks and reinsert them as a dedicated `program` block;
  - practical goal: keep literal repertoire coverage alive for `writer.final_4o` while leaving atmosphere / rarity in narrative sections
- `iter8` carry after the 2026-04-06 editorial pass:
  - layout prompt now explicitly tells Gemma not to open a narrative block with generic ensemble/service detail when the same block contains a stronger atmosphere / rarity hook;
  - rarity hooks should not be stranded in the weakest tail block if they can support lead or first body
- `iter9` carry after the 2026-04-07 Opus-guided prompt pass:
  - narrative body cleaner may reorder mixed `body` refs so atmosphere / rarity / event-core hook opens before generic people-category detail;
  - dense split heuristic now respects bucket transitions after that reorder, so mixed hook-heavy blocks still split into two readable body sections instead of collapsing back into one blob.
- current `iter2` aggregate:
  - `events_with_flags = 0`
  - `missing_fact_total = 0`
  - `duplicate_fact_total = 0`
  - `auto_fixed_total = 1`

Prompt pack:
- [smart-update-lollipop-editorial-layout-prompts.md](/workspaces/events-bot-new/docs/llm/smart-update-lollipop-editorial-layout-prompts.md)

### `writer_pack.compose`

Текущая каноника:

- deterministic `writer_pack.compose.v1`
- deterministic `writer_pack.select.v1` как identity/no-op
- full `12`-event family-lab `iter2` уже прогнан поверх `editorial.layout iter2`
- post-run `Gemini` verdict: `GO` для перехода к `writer.final_4o`
- current `iter2` aggregate:
  - `events_with_flags = 0`
  - `missing_fact_total = 0`
  - `duplicate_fact_total = 0`
  - `events_with_literal_items = 3`
  - `absorbed_by_list_total = 1`
- literal program items now survive through explicit `literal_items` + `coverage_plan`
- post-baseline retune carry, подтверждённый в `iter2` run:
  - suppressed facts не должны попадать в `sections` или `must_cover_fact_ids`
  - sections с `literal_items` теперь могут нести `literal_list_is_partial = true`
- `iter4` carry after the 2026-03-11 rerun:
  - selected pack now explicitly carries `event_type` into `writer.final_4o`, so final prose can restore format clarity even when lead facts sound like film/project reference notes
- `iter7` carry after the 2026-04-06 `Gemma 4` downstream retune:
  - if layout exposes a dedicated `program` block, literal repertoire items should survive there instead of being absorbed by the lead;
  - downstream pack therefore separates `event-facing lead` from `literal program coverage`, which is critical for list-heavy concerts

Prompt/contract pack:
- [smart-update-lollipop-writer-pack-prompts.md](/workspaces/events-bot-new/docs/llm/smart-update-lollipop-writer-pack-prompts.md)

### `writer.final_4o`

Текущая каноника:

- один final `writer.final_4o.v1` call на `gpt-4o`
- deterministic validator после call
- Python-side apply rule: `title_strategy = keep` всегда принудительно возвращает `original_title`
- full `12`-event family-lab `iter2` уже прогнан поверх `writer_pack.select iter2`
- current `iter2` aggregate:
  - `attempt_total = 13`
  - `retry_event_total = 1`
  - `events_with_errors = 0`
  - `events_with_warnings = 0`
  - `infoblock_leak_total = 0`
  - `literal_missing_total = 0`
  - `literal_mutation_total = 0`
- final post-run `Gemini 3.1 Pro Preview` verdict: `GO`
- post-baseline retune carry, подтверждённый в `iter2` run:
  - partial literal lists должны подаваться как примеры, а не как полный перечень
  - validator блокирует partial list без явного non-exhaustive intro-marker
  - `2498` больше не тащит cross-promo
  - `2657` снова несёт сильный исторический контекст
  - `2734` вводит список через non-exhaustive framing
- текущий safety retune после quality consultation усилил только prompt contract, без нового downstream split:
  - prompt получает explicit structure plan по `sections` и exact headings;
  - для bare/opaque titles есть отдельный `title_needs_format_clarity` signal;
  - prompt держит rough length band, чтобы rich cases не схлопывались в короткую справку;
  - прямо запрещены openings вида `Режиссёр фильма — ...` / `Проект представляет собой ...`, если они не объясняют формат события
- `iter4` rerun on 2026-03-11 подтвердил ещё один рабочий carry:
  - final writer теперь получает `event_type` и вычисляет `lead_needs_format_bridge`, чтобы для screening/presentation cases явно назвать показ/презентацию в первом предложении, если lead facts сами не дают format anchor
  - practical result: headings вернулись во всех `12/12` текстах, а `2673/2659/2747` перестали открываться как чистая справка о проекте/фильме
- `iter6` carry after the 2026-03-11 full rerun:
  - prompt now treats every `section` boundary as a paragraph boundary, so extra body sections from `editorial.layout` survive into public prose even when the later block has `heading = null`
  - practical result: `writer.final_4o iter6` stayed clean (`0 errors`, `0 warnings`, `0 retries`) while average description length recovered from `449.6` to `471.2`
- `iter7` carry after the 2026-04-06 `Gemma 4` downstream retune:
  - prompt now states explicitly that `literal_items` require a real markdown bullet list in that exact section;
  - prose mention of the same items no longer counts as valid coverage;
  - if a dedicated `program` section exists, the lead should stay compact and reserve the exhaustive repertoire list for that section
- `iter8` carry after the 2026-04-06 editorial pass:
  - final prompt now explicitly targets a live cultural announcement rather than a report/card;
  - strongest grounded hook should open the lead and each narrative section before dry summary lines;
  - report-style formulas (`характеризуется`, `посвящен`, etc.) are now explicitly discouraged, as are age/admin notes in narrative prose
- `iter9` carry after the 2026-04-07 profile + rerender pass:
  - final prompt now explicitly bans lead meta-openings of the form `X — это ...`;
  - final prompt also bans audience-template prose like `зрители смогут насладиться ...`;
  - retry prompt must rewrite the same factological hook through event action / stage / music rather than through promo or report meta-language.
- `iter10` carry after the 2026-04-07 Opus-guided prompt pass:
  - final writer prompt now includes positive register exemplars for the target voice (`живой сдержанный дайджест`), not just negative bans;
  - banned formula list расширен под recurring drift (`приглашает вас`, `для ценителей`, `программа состоит из`, `обещает стать настоящим праздником`);
  - retry contract now explicitly forbids replacing report-language with promo-language or vice versa: correction must stay in the third register of a restrained cultural announcement.

Prompt/contract pack:
- [smart-update-lollipop-writer-final-prompts.md](/workspaces/events-bot-new/docs/llm/smart-update-lollipop-writer-final-prompts.md)

## Mixed-phase class

Для источников вида `past recap + future anchor` используется узкий interceptor:

```text
scope.extract.phase_map.v1
-> scope.select.target_phase.v1
-> facts.extract.phase_scoped.v1
```

Смысл:

- прошедшая фаза уходит в `background_context`;
- будущая фаза становится target;
- прошлые venue/time facts не должны протекать в будущую карточку;
- при слабом future anchor pipeline должен работать по принципу `fail closed`.
