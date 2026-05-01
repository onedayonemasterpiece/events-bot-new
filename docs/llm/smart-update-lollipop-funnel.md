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

## `lollipop_legacy`

Статус: `lab variant`, не production default.

Цель: получить baseline-equivalent public text на Gemma 4 (с 4o final-writer fallback при сбое Gemma 4), не возвращая baseline facts/draft в legacy generation payload и не вводя сложный intermediate planning.

Текущая рабочая версия в коде: `lollipop_legacy.v14`.

- Version owner: `smart_update_lollipop_lab/legacy_writer_family.py::LEGACY_CONTRACT_VERSION`
- Статус: fact-extraction layer **partial, substantially recovered** по audit `2026-05-01` после исправления benchmark fixture/source evidence. Writer/text quality **в этой итерации специально не тюнился** — accepted/partial по text quality остаётся вторичным сигналом (`v13` summary).
- Latest full-source fact-extraction evidence: `artifacts/codex/lollipop_g4_benchmark_20260501T212029Z.{md,json}`. Предыдущие `T095915Z`, `T105522Z`, `T201451Z`, `T202856Z`, `T204112Z`, `T205422Z`, `T210751Z` являются iteration evidence: сначала fixture-ы содержали укороченные excerpts, затем добивались extraction timeout/reviewer timeout/prompt regressions. Текущий full-source результат: `55/56` grounded baseline facts covered, `0` critical/major losses, `3/5 accepted`, `2/5 partial` только по suspicious/minor fact issues.
- Latest writer evidence (carry-over): `artifacts/codex/lollipop_g4_benchmark_20260430T201038Z.{md,json}`
- Baseline в benchmark — только comparison reference (chars, quality delta, speed ratio). Baseline text/facts не попадают в Gemma 4 extraction, writer, fallback или 4o payload.

Контракт `lollipop_legacy.v14` (Stage 1+2 как в v13 + benchmark-only fact-coverage reviewer):

- Stage 1 — Gemma 4 31b extraction per source: один JSON-call на каждый source, который возвращает `public_facts[]` и `logistics_facts[]`. Только `source_text` + event metadata в payload, никаких baseline draft/fact передач.
- Stage 2 — единственный Gemma 4 31b writer-call: получает merged `public_facts`, `logistics_facts`, `source_excerpt`, advisory `length_contract` (вычисленный от reference baseline length, но без передачи baseline текста), и возвращает финальный `description_md` + честные `covered_*_fact_indexes`.
- Final-writer fallback: если Gemma 4 writer падает по timeout / exception / возвращает пустой `description_md`, один раз вызывается gpt-4o с тем же writer payload (через `FOUR_4O_TOKEN`, OpenAI structured output с lowercase JSON-Schema). Никаких baseline facts/draft в 4o-payload.
- Никаких intermediate enrichment / planning stages, никаких repair pass'ов, никакого baseline fallback. Никаких regex/post-processing fixes текста — валидатор только репортит.
- `generation_uses_baseline`, `uses_baseline_fact_floor`, `includes_baseline_stage`, `baseline_assisted`, `writer_fallback_to_baseline` всегда остаются `false`. Видимость fallback — `writer_model` (`gemma-4` или `4o`), `writer_fallback_to_4o`, `writer_failure_reasons`.
- Validator (`legacy_writer_family.validate_writer_output`) — read-only signal: пустой текст / явные duplicate word / repeated cluster / prompt leak — errors; `style.direct_address`, `age.leak`, `text.english_word`, `length.below_baseline_ratio`, `length.long`, и `latency.3x_exceeded:N` — warnings.
- Quality delta (`legacy_writer_family.compare_to_baseline`) — read-only signal для отчёта (narrator-frame, source-fidelity, length, lost-headings, lost-epigraph). Не запускает retry/repair.
- Speed gate: целимся в `<=3x` от Gemma 3 baseline. Превышение остаётся как warning, не блокирует output, потому что non-empty стабильность сейчас приоритетнее.
- `--legacy-g4-extract` остаётся как deprecated no-op для совместимости CLI; v14 всегда использует staged Gemma 4 extract+write flow.
- **Stage 3 (benchmark-only)**: после writer в variant добавлен LLM-first fact-coverage reviewer. Получает `source_excerpt`, event metadata, baseline facts (Gemma 3) и Gemma 4 facts; возвращает `baseline_facts_review[]` (`grounded_in_source`, `covered_by_g4`, `loss_severity`), `g4_facts_review[]` (`grounded_in_source`, `useful_new_fact`, `suspicious_reason`), и `coverage_summary` с `public/logistics/named_entity/format_topic_program/overall_verdict`. Reviewer — benchmark-only: baseline facts разрешены в его payload, но extractor/writer payload остаются чистыми (регрессионно проверяется тестом `test_lollipop_legacy_reviewer_payload_uses_baseline_facts_but_generation_does_not`). Primary reviewer uses Gemma 4; if that read-only reviewer times out, the benchmark may fall back to 4o for reviewer judgement only and records it in `fact_coverage.warnings` / `timings.fact_coverage_four_o_calls`. Markdown evidence дополнительно показывает raw baseline `per_source_facts`, baseline `facts_text_clean`, filtered-before-writer facts, metadata anchors и exact Gemma 4 public/logistics facts, чтобы ручная сверка не зависела от reviewer-пересказа.
- **Deterministic verdict floor**: дополнительно к LLM `overall_verdict` benchmark считает свой floor (`accepted`/`partial`/`rejected`). Critical loss любого grounded baseline fact или 3+ ungrounded G4 facts ⇒ `rejected`. Финальный `verdict` — минимальный из двух (LLM judge не может перевернуть deterministic floor).
- Source fixture policy: static Telegram fixtures must be full post snapshots. Public Telegram extraction uses exact post pages (`https://t.me/<channel>/<post>?embed=1&mode=tme`) / `data-post` selection; using the first message on `t.me/s/...` is invalid for benchmark evidence.

Benchmark command:

```bash
python scripts/inspect/benchmark_lollipop_g4.py \
  --variants baseline,lollipop_legacy \
  --fixtures audio_walk,peter_fleet_lecture,sacred_lecture,world_hobbies,red_cosmos \
  --gemma-call-gap-s 0
```

Implementation surface:

- [legacy_writer_family.py](/workspaces/events-bot-new/smart_update_lollipop_lab/legacy_writer_family.py) owns `lollipop_legacy` prompt/schema/validation/objective quality delta;
- [benchmark_lollipop_g4.py](/workspaces/events-bot-new/scripts/inspect/benchmark_lollipop_g4.py) owns variant routing, five real-post fixtures, timing, and markdown report rendering;
- [test_lollipop_legacy.py](/workspaces/events-bot-new/tests/test_lollipop_legacy.py) covers v14 contract: Gemma 4 extract+write, no-baseline-leakage guard for extractor and writer (with reviewer carve-out), 4o fallback on writer timeout, `FOUR_4O_TOKEN` env handling, narrator-frame and source-fidelity quality signals, full `PETER-FLEET` source snapshot, and the fact-coverage reviewer/reporting surface (schema shape, payload assembly, normalization clamping, deterministic verdict floor — `critical loss → rejected`, `useful added → tracked`, `ungrounded g4 → suspicious + partial floor`, exact input texts preserved over reviewer echo).

## `baseline_g4`

Статус: `benchmark probe`, **not accepted**.

Цель: отдельно от `lollipop_legacy` проверить прямой перевод baseline contract на
Gemma 4 в LLM-first форме.

Текущий контракт:

- Gemma 4 native-schema fact extraction from source/metadata;
- Gemma 4 writer stage;
- Gemma 4 native-schema reviewer as semantic acceptance gate;
- Gemma 3 baseline только для comparison: chars, quality delta, speed ratio;
- no baseline fallback, no Gemma 3 facts/text in generation payload, no deterministic
  semantic rewrite/repair.

Latest useful evidence:

- `AUDIO-WALK-QUARTER-971` can pass the current gate (`767/834` chars, reviewer
  `accepted`, `errors=0`, speed ratio about `1.98x`);
- latest full five-fixture artifact:
  `artifacts/codex/lollipop_g4_benchmark_20260430T190623Z.{json,md}`;
- full five-fixture benchmark is **failed**, not merely weak: `AUDIO-WALK` and
  `RED-COSMOS` currently hit writer `TimeoutError`, `PETER-FLEET` / `WORLD-HOBBIES`
  have reviewer-blocked prose errors, and only `SACRED-LECTURE` is accepted.

Acceptance bar before this probe can be called working:

- full five-fixture run must complete without provider timeout;
- `validation.errors=0` and reviewer verdict accepted/no-worse for every fixture;
- no English/mixed-language artifacts, repeated clusters, prompt leaks, or JSON/list
  syntax leakage in public prose;
- speed ratio must remain within `<=3x` of Gemma 3 baseline.

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
