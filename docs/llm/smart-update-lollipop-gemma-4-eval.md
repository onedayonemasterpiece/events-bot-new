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

## `lollipop_legacy` recovery benchmark (`2026-04-29`)

После обнаружения, что tracked repo не содержит отдельного `lollipop legacy` variant, добавлен lab-вариант `lollipop_legacy.v1`.

Design:

- baseline run остаётся текущим prod-style Gemma 3 proxy и выполняется первым внутри `lollipop_legacy`;
- `lollipop_legacy` не запускает full `lollipop g4` cascade и не использует final `4o`;
- Gemma 4 31b используется для одного bounded final-writer pass поверх `baseline_description`, полного baseline fact floor и source excerpt;
- обязательный coverage floor равен полному baseline fact set, включая logistics/ticket/date/start-point facts;
- final text должен быть не короче baseline; validation/timeout failures откатываются к baseline text с warning, чтобы legacy не мог ухудшить baseline.

Важный negative result:

- прямой перенос baseline-style fact extraction на `gemma-4-31b-it` через текущий Smart Update extractor path был проверен первым;
- на `AUDIO-WALK-QUARTER-971` один короткий G4 extraction call занял около `177s`;
- поэтому `--legacy-g4-extract` оставлен как explicit experimental opt-in и не входит в default legacy path.

Superseded benchmark:

- `artifacts/codex/lollipop_g4_benchmark_20260429T211304Z.{json,md}` был некорректно интерпретирован: legacy timing не включал baseline prose stage, coverage floor был event-facing вместо полного baseline fact set, а output length мог быть сильно ниже baseline.

Latest five-fixture benchmark:

- json: `artifacts/codex/lollipop_g4_benchmark_20260429T224021Z.json`
- markdown: `artifacts/codex/lollipop_g4_benchmark_20260429T224021Z.md`
- command:

```bash
LOLLIPOP_GEMMA_DIRECT_TIMEOUT_SEC=45 LOLLIPOP_GEMMA_WRITER_TIMEOUT_SEC=12 \
python scripts/inspect/benchmark_lollipop_g4.py \
  --variants baseline,lollipop_legacy \
  --fixtures audio_walk,peter_fleet_lecture,sacred_lecture,world_hobbies,red_cosmos \
  --reuse-baseline-artifact artifacts/codex/lollipop_g4_benchmark_20260429T223829Z.json \
  --reuse-fixture-artifact artifacts/codex/lollipop_g4_benchmark_20260429T223829Z.json \
  --gemma-call-gap-s 0
```

Results:

| Fixture | Baseline chars | Legacy chars | Fact floor | Speed ratio | Validation |
| --- | ---: | ---: | ---: | ---: | --- |
| `AUDIO-WALK-QUARTER-971` | `554` | `590` | `5/5` | `1.5368` | `errors=0`, `warnings=0` |
| `PETER-FLEET-LECTURE-5600` | `1050` | `1050` | `4/4` | `2.0913` | `errors=0`, fallback warning after duplicate-tail typo |
| `SACRED-LECTURE-ZYGMONT-3170` | `1156` | `1218` | `7/7` | `2.1593` | `errors=0`, detector-only hook warning |
| `WORLD-HOBBIES-5505` | `1078` | `1181` | `5/5` | `2.2308` | `errors=0`, `warnings=0` |
| `RED-COSMOS-7902` | `1060` | `1060` | `6/6` | `2.4086` | `errors=0`, fallback warning after writer timeout |

Interpretation:

- `lollipop_legacy` now passes the stricter `1.0 < speed_ratio <= 3.0` latency gate on all five fixtures while counting the baseline stage;
- output length is guaranteed non-regressive in this run: min ratio `1.000`, average ratio `1.043`;
- full baseline fact floor is preserved on every fixture;
- style detectors report no report-formula hits, no promo phrase hits, and no poster/age leaks;
- two writer failures were contained by baseline fallback: one duplicate-tail typo (`трудностиности`) and one 12s writer timeout;
- quality still needs human pairwise review before rollout: the current evidence is a strong lab recovery candidate, not a production default.

## `lollipop_legacy.v2` public-quality recovery pass (`2026-04-30`)

После human review стало ясно, что `v1` был слишком буквальным: full baseline fact floor заставлял writer тащить logistics/date/ticket facts в public narrative, а требование `>= baseline length` превращало legacy в baseline-copy или раздувало текст. `v2` меняет acceptance contract:

- non-logistics baseline facts стали mandatory public floor;
- date/time/address/ticket/start-point facts остаются `logistics_context` и используются только компактно и по необходимости;
- Gemma 4 31b работает как baseline editor: сохранить concrete baseline substance, улучшить hook/register, не делать short summary;
- objective guard `quality_delta_vs_baseline` ловит measurable regressions: new report/promo/meta leaks, lost baseline hook, too-short output, validation failures;
- при regression запускается один bounded repair pass; baseline fallback остаётся последним no-worse guard.

Latest five-fixture benchmark:

- json: `artifacts/codex/lollipop_g4_benchmark_20260430T060900Z.json`
- markdown: `artifacts/codex/lollipop_g4_benchmark_20260430T060900Z.md`
- command:

```bash
LOLLIPOP_GEMMA_DIRECT_TIMEOUT_SEC=45 LOLLIPOP_GEMMA_WRITER_TIMEOUT_SEC=16 \
python scripts/inspect/benchmark_lollipop_g4.py \
  --variants baseline,lollipop_legacy \
  --fixtures audio_walk,peter_fleet_lecture,sacred_lecture,world_hobbies,red_cosmos \
  --reuse-baseline-artifact artifacts/codex/lollipop_g4_benchmark_20260429T224021Z.json \
  --reuse-fixture-artifact artifacts/codex/lollipop_g4_benchmark_20260429T224021Z.json \
  --gemma-call-gap-s 0
```

Results:

| Fixture | Baseline chars | Legacy chars | Length ratio | Quality delta | Speed ratio | Fallback | Validation |
| --- | ---: | ---: | ---: | --- | ---: | --- | --- |
| `AUDIO-WALK-QUARTER-971` | `554` | `488` | `0.8809` | `improved` | `1.4127` | `false` | `errors=0`, `warnings=0` |
| `PETER-FLEET-LECTURE-5600` | `1050` | `954` | `0.9086` | `improved` | `1.8390` | `false` | `errors=0`, `warnings=0` |
| `SACRED-LECTURE-ZYGMONT-3170` | `1156` | `828` | `0.7163` | `improved` | `1.8086` | `false` | `errors=0`, `warnings=0` |
| `WORLD-HOBBIES-5505` | `1078` | `1089` | `1.0102` | `improved` | `2.1928` | `false` | `errors=0`, `warnings=0` |
| `RED-COSMOS-7902` | `1060` | `985` | `0.9292` | `improved` | `2.1427` | `false` | `errors=0`, `warnings=0` |

Interpretation:

- `5/5` fixtures pass objective no-worse gate as `improved`;
- `4/5` fixtures are shorter than baseline while preserving public fact floor;
- `0` writer fallback and `0` repair-pass usage in this stable run; repair remains a guard for non-deterministic short/invalid outputs observed during iteration;
- `0` validation errors/warnings, `0` report/promo/poster/age leaks;
- latency remains within the requested `<=3x` envelope while counting the baseline stage.

## `lollipop_legacy.v2` prompt + guard tuning (`2026-04-30`)

Follow-up human review targeted two text-quality issues that the first `v2` guard did not model well enough:

- stock narrator-frame leads such as `Погружение в ...`, `Знакомство с ...`, `Путешествие в мир ...`;
- lost scannability when a structured baseline with `###` thematic blocks was collapsed into plain paragraphs.

Changes:

- writer prompt now bans narrator-frame openings and gives positive lead patterns for object / actor / format-texture / event-action / named-quote leads;
- `compare_to_baseline` treats a new narrator-frame opening as a regression, rewards narrator-frame avoidance, and emits soft warnings for dropped baseline `###` structure or leading `>` epigraphs;
- validator rejects the recurring Gemma 4 double-comma artifact as `text.double_comma`, allowing the repair pass to fix it before fallback.

Latest five-fixture benchmark:

- json: `artifacts/codex/lollipop_g4_benchmark_20260430T115455Z.json`
- markdown: `artifacts/codex/lollipop_g4_benchmark_20260430T115455Z.md`

| Fixture | Baseline chars | Legacy chars | Length ratio | Quality delta | Speed ratio | Repair | Fallback | Validation |
| --- | ---: | ---: | ---: | --- | ---: | ---: | --- | --- |
| `AUDIO-WALK-QUARTER-971` | `554` | `521` | `0.9404` | `improved` | `1.4630` | `0` | `false` | `errors=0`, `warnings=0` |
| `PETER-FLEET-LECTURE-5600` | `1050` | `1019` | `0.9705` | `improved` | `2.8412` | `1` | `false` | `errors=0`, `warnings=0` |
| `SACRED-LECTURE-ZYGMONT-3170` | `1156` | `938` | `0.8114` | `improved` | `1.9114` | `0` | `false` | `errors=0`, `warnings=1` (`quality.lost_baseline_epigraph`) |
| `WORLD-HOBBIES-5505` | `1078` | `1063` | `0.9861` | `improved` | `2.1839` | `0` | `false` | `errors=0`, `warnings=1` (`quality.lost_baseline_epigraph`) |
| `RED-COSMOS-7902` | `1060` | `987` | `0.9311` | `improved` | `2.1291` | `0` | `false` | `errors=0`, `warnings=1` (`quality.lost_baseline_epigraph`) |

Interpretation:

- `5/5` fixtures remain `quality_delta_status=improved`;
- narrator-frame openings are removed from the Peter Fleet and Red Cosmos outputs;
- dropped heading structure is restored on `audio_walk`, `peter_fleet_lecture`, and `sacred_lecture`;
- one observed `text.double_comma` artifact was repaired without falling back to baseline;
- all outputs remain within the `1.0 < speed_ratio <= 3.0` gate, with max observed ratio `2.8412`.

## `lollipop_legacy.v3` Gemma 4-only correction attempt (`2026-04-30`)

Audit после `v2` показал, что previous "legacy" evidence был baseline-assisted: Gemma 3 baseline facts/description входили в generation path. `v3` исправляет contract boundary:

- baseline остаётся только benchmark reference для length/quality/speed comparison;
- Gemma 4 `source_facts.v3` извлекает public/logistics facts, lead hooks, structure hints и emergency source draft из source excerpts;

## `baseline_g4` direct baseline migration probe (`2026-04-30`)

Задача: проверить самый простой путь "baseline, но на Gemma 4" без `lollipop_legacy`
и без baseline-assisted writer. Это отдельный benchmark variant, не rollout contract.

Contract:

- `baseline` на Gemma 3 остаётся только comparison reference;
- `baseline_g4` generation path использует Gemma 4 stages: native-schema fact extraction,
  writer, native-schema reviewer;
- Gemma 3 facts/text не входят в generation payload;
- no hidden fallback to baseline;
- no deterministic semantic text repair. Deterministic code may only reject syntax/prose
  artifacts such as prompt leaks, English words, repeated clusters, too-short/too-long output.

Negative direct-swap evidence:

- `artifacts/codex/lollipop_g4_benchmark_20260430T174227Z.{json,md}`
- direct Gemma 4 baseline prompt on `AUDIO-WALK-QUARTER-971` leaked prompt/instruction
  material and produced a huge unusable response (`28838` chars, `424s`), so Gemma 4 is
  not a drop-in replacement for the Gemma 3 baseline prompt.

Optimized staged evidence:

- `artifacts/codex/lollipop_g4_benchmark_20260430T182208Z.{json,md}`:
  `AUDIO-WALK-QUARTER-971` passed locally with reviewer `accepted`, `767/834` chars,
  `quality_delta=improved`, `errors=0`, speed ratio about `1.98x`.
- `artifacts/codex/lollipop_g4_benchmark_20260430T182444Z.{json,md}`:
  full five-fixture run completed but was **not accepted**: `AUDIO-WALK` and
  `PETER-FLEET` passed, while `SACRED`, `WORLD`, and `RED` failed reviewer gates
  (generic lecture copy / English word artifacts / English mixed token).
- Subsequent Red-focused probes improved timeout behavior with a separate object-list
  writer contract but are still **not accepted**:
  - `lollipop_g4_benchmark_20260430T184811Z`: fast but dry report/list style;
  - `lollipop_g4_benchmark_20260430T185040Z`: cut-off text artifact;
  - `lollipop_g4_benchmark_20260430T185158Z` and `185319Z`: no timeout, but repetitive
    filler on the thin source;
  - later direct-text object writer probes timed out on `gemma-4-31b-it`.
- Current full-run artifact after harness fail-open:
  `artifacts/codex/lollipop_g4_benchmark_20260430T190623Z.{json,md}`.
  It is **failed**: `AUDIO-WALK` and `RED-COSMOS` hit `TimeoutError`, `PETER-FLEET`
  has English/typo/unsupported-role errors, `WORLD-HOBBIES` has typo error, and only
  `SACRED-LECTURE` is accepted by the LLM reviewer.

Current conclusion:

- `baseline_g4` is correctly LLM-first and benchmark-visible, but **not a working result yet**.
- The hard remaining blocker is thin object-list exhibition text (`RED-COSMOS-7902`):
  forcing baseline-like length makes Gemma 4 pad/repeat; making the object-list writer compact
  avoids timeout but can become catalog-like. The next prompt iteration must solve this inside
  the LLM contract, not with deterministic text rewriting.
- Gemma 4 writer/repair получает только source-derived facts/context; `baseline_description` в generation payload пустой;
- baseline fallback удалён. Timeout/error может использовать только Gemma 4 source draft.

Latest five-fixture benchmark:

- json: `artifacts/codex/lollipop_g4_benchmark_20260430T141614Z.json`
- markdown: `artifacts/codex/lollipop_g4_benchmark_20260430T141614Z.md`

| Fixture | Baseline chars | Legacy chars | Length ratio | Quality delta | Speed ratio | Repair | Source draft | Validation |
| --- | ---: | ---: | ---: | --- | ---: | ---: | --- | --- |
| `AUDIO-WALK-QUARTER-971` | `554` | `426` | `0.7690` | `improved` | `2.1190` | `0` | `false` | `errors=0`, `warnings=0` |
| `PETER-FLEET-LECTURE-5600` | `1050` | `836` | `0.7962` | `improved` | `2.3424` | `1` | `false` | `errors=0`, `warnings=1` (`quality.lost_baseline_headings`) |
| `SACRED-LECTURE-ZYGMONT-3170` | `1156` | `853` | `0.7379` | `improved` | `4.7624` | `1` | `true` | `errors=1` (`latency.3x_exceeded`), `warnings=2` |
| `WORLD-HOBBIES-5505` | `1078` | `634` | `0.5881` | `regressed` | `3.3164` | `1` | `false` | `errors=3` (`length.below_baseline_ratio`, `quality.too_short_vs_baseline`, `latency.3x_exceeded`) |
| `RED-COSMOS-7902` | `1060` | `0` | `0.0000` | `regressed` | `8.5645` | `1` | `false` | `errors=6` (`source_facts.timeout`, writer timeout/empty output, latency) |

Interpretation:

- `v3` fixes the hidden Gemma 3 problem: every row records `generation_uses_baseline=false`, `uses_baseline_fact_floor=false`, `includes_baseline_stage=false`, and `writer_fallback_to_baseline=false`.
- `v3` is **not accepted** as quality replacement yet: only `3/5` fixtures improve, `2/5` regress, and latency exceeds `3x` on `3/5`.
- Source-only extraction is fact-sparse on short Telegram posts (`peter_fleet_lecture`, `world_hobbies`) compared with the old Gemma 3 baseline-derived text; this is an actual product constraint, not a benchmark formatting issue.
- Next iteration should either make the final writer path shorter/more stable on Gemma 4 or use the explicitly allowed final-writer fallback to `4o` while keeping Gemma 4 as the source-facts extractor.

## `lollipop_legacy.v7` Gemma 4-only source-fidelity pass (`2026-04-30`)

После prompt-family audit через проектный `Opus` alias предыдущие `source draft`, baseline fallback и repair-подходы были сняты из legacy path. `v7` проверяет более строгий LLM-first contract:

- Gemma 4 `facts_v7` извлекает только source-derived `public_facts` и `logistics_facts`;
- Gemma 4 `writer_v7` получает только эти facts, `title`, `event_type` и advisory `target_chars`;
- `source_excerpt`, `baseline_description` и baseline facts в writer payload не передаются;
- baseline используется только как comparison reference для quality/speed metrics;
- text repair, source-draft fallback и baseline fallback отсутствуют; writer failure остаётся visible failure;
- objective guard сравнивает source fidelity: invented named-token reduction and retained source named tokens count as improvements, while raw length vs baseline is warning-only;
- отдельный `length.below_min` gate не даёт принять слишком тонкий public copy.

Implementation:

- `LEGACY_CONTRACT_VERSION = "lollipop_legacy.v7"`;
- benchmark records `legacy_g4_extract_mode="facts_v7.writer_v7"`;
- invariant flags remain `generation_uses_baseline=false`, `uses_baseline_fact_floor=false`, `includes_baseline_stage=false`, `writer_fallback_to_baseline=false`;
- `writer_retry_count=0` on all rows.

Latest five-fixture benchmark:

- json: `artifacts/codex/lollipop_g4_benchmark_20260430T151737Z.json`
- markdown: `artifacts/codex/lollipop_g4_benchmark_20260430T151737Z.md`
- baseline reused from fresh Gemma 3 artifact `artifacts/codex/lollipop_g4_benchmark_20260430T144616Z.json`
- command:

```bash
LOLLIPOP_GEMMA_DIRECT_TIMEOUT_SEC=22 LOLLIPOP_GEMMA_WRITER_TIMEOUT_SEC=22 \
python scripts/inspect/benchmark_lollipop_g4.py \
  --variants baseline,lollipop_legacy \
  --fixtures audio_walk,peter_fleet_lecture,sacred_lecture,world_hobbies,red_cosmos \
  --reuse-baseline-artifact artifacts/codex/lollipop_g4_benchmark_20260430T144616Z.json \
  --reuse-fixture-artifact artifacts/codex/lollipop_g4_benchmark_20260430T144616Z.json \
  --gemma-call-gap-s 0
```

Results:

| Fixture | Baseline chars | Legacy chars | Source facts | Quality delta | Speed ratio | Repair/fallback | Validation |
| --- | ---: | ---: | ---: | --- | ---: | --- | --- |
| `AUDIO-WALK-QUARTER-971` | `834` | `172` | `4` | `improved` | `1.3138` | `0/false` | `length.below_min:172/280` |
| `PETER-FLEET-LECTURE-5600` | `665` | `173` | `4` | `improved` | `0.7170` | `0/false` | `length.below_min:173/280` |
| `SACRED-LECTURE-ZYGMONT-3170` | `1126` | `266` | `7` | `improved` | `1.7108` | `0/false` | `length.below_min:266/420` |
| `WORLD-HOBBIES-5505` | `1040` | `283` | `5` | `improved` | `1.1909` | `0/false` | `length.below_min:283/350` |
| `RED-COSMOS-7902` | `1077` | `194` | `7` | `improved` | `0.7872` | `0/false` | `length.below_min:194/420` |

Interpretation:

- `v7` successfully removes hidden Gemma 3 leakage and all repair/fallback crutches from the generation path.
- `5/5` fixtures improve on the current objective quality delta because they invent fewer named tokens than the Gemma 3 baseline and retain more source-grounded named material where available.
- `5/5` fixtures pass the `<=3x` latency gate, and two are faster than the fresh baseline reference.
- `v7` is **not accepted** as public-copy replacement: `5/5` outputs fail `length.below_min`; the writer is faithful but too thin/dry for production-quality event copy.
- Next work must improve `writer_v7` richness/texture inside the same Gemma 4-only contract, not by reintroducing baseline text/facts, source-draft fallback, or repair passes.

## `lollipop_legacy.v7` paragraph-bound writer tightening (`2026-04-30`)

После дополнительного `Opus` prompt-family audit `writer_v7` был переведён с flat `description_md` на paragraph records:

- Gemma 4 facts stage теперь просит `source_span` для каждого fact; normalizer проверяет span against `source_excerpt`;
- Gemma 4 writer возвращает `paragraphs[]`, где каждый lead/body paragraph обязан ссылаться на `public_fact_indexes`, а logistics paragraph — на `logistics_fact_indexes`;
- финальный `description_md` собирается детерминированно из paragraph records;
- добавлены reject-only guards для filler phrases, adjacent-stem artifacts (`указам указаниям`, `лектор лекторий`) и слишком пустого public copy (`length.below_public_min`);
- baseline по-прежнему comparison-only; writer не получает baseline text/facts/source draft и repair/fallback отсутствуют.

Latest five-fixture benchmark:

- json: `artifacts/codex/lollipop_g4_benchmark_20260430T153849Z.json`
- markdown: `artifacts/codex/lollipop_g4_benchmark_20260430T153849Z.md`
- baseline reused from fresh Gemma 3 artifact `artifacts/codex/lollipop_g4_benchmark_20260430T144616Z.json`

Results:

| Fixture | Baseline chars | Legacy chars | Public facts | Quality delta | Speed ratio | Validation |
| --- | ---: | ---: | ---: | --- | ---: | --- |
| `AUDIO-WALK-QUARTER-971` | `834` | `58` | `1` | `improved` | `0.7050` | `length.below_public_min:58/180` |
| `PETER-FLEET-LECTURE-5600` | `665` | `222` | `2` | `improved` | `0.8762` | `errors=0`, thin-copy warnings |
| `SACRED-LECTURE-ZYGMONT-3170` | `1126` | `240` | `2` | `improved` | `1.8565` | `text.filler:доступны на сайте` |
| `WORLD-HOBBIES-5505` | `1040` | `227` | `2` | `improved` | `1.4081` | `errors=0`, thin-copy warnings |
| `RED-COSMOS-7902` | `1077` | `102` | `3` | `improved` | `0.4944` | `public_fact.uncovered:1/2`, `length.below_public_min:102/180` |

Interpretation:

- Paragraph-index binding removes the most dangerous writer artifacts from the previous run: no baseline leakage, no text repair, no fallback, no source draft, no visible invented object texture like `глиняные` or broken phrase `указам указаниям`.
- The stricter source-span extraction overcorrects: public fact recall drops to `1-3` facts on all fixtures, and the resulting copy is source-faithful but not rich enough for public descriptions.
- This pass is **not accepted**. It is a safer contract boundary and a better regression harness, not a quality replacement for Gemma 3 baseline.
- Next iteration should improve Gemma 4 source-fact recall and writer texture inside the paragraph-bound contract, or explicitly test passing `source_excerpt` to writer as Gemma 4-only grounding input; neither path may reintroduce baseline facts/text, repair, or fallback.

## `lollipop_legacy.v14` fact-coverage reviewer pass (`2026-05-01`)

### Why we re-focused on the fact layer

After `v13` made the variant stable (5/5 non-empty, 0 baseline leakage) but evaluated mostly on writer text, the next iteration's goal was honest: judge Gemma 4 fact extraction against the existing Gemma 3 baseline before judging final prose. The constraint was LLM-first: no string-overlap or keyword-regex matching, no repair pass, no baseline facts/text in the legacy generation payload (extractor / writer / 4o fallback).

### Implementation

- `smart_update_lollipop_lab/legacy_writer_family.py` (now `v14`) gained a benchmark-only fact-coverage reviewer surface:
  - `fact_coverage_response_schema()` — JSON schema with `baseline_facts_review[]`, `g4_facts_review[]`, `coverage_summary` (per-axis statuses + `overall_verdict`).
  - `build_fact_coverage_system_prompt()` — Russian-output reviewer prompt: match by meaning, mark each baseline fact as grounded/false/unclear, decide `covered_by_g4` and `loss_severity` (none/minor/major/critical), tag G4 facts as grounded + useful_new_fact, set `suspicious_reason` when ungrounded/fragmentary/duplicate/leak.
  - `build_fact_coverage_payload()` — Gemma-4 user payload that flattens public + logistics with category labels and indexes; baseline facts are explicitly carried (reviewer-only).
  - `normalize_fact_coverage_payload()` — clamps invalid indexes, normalizes enum values (loss_severity, grounded_in_source, *_status, overall_verdict).
  - `summarize_fact_coverage()` — computes counts, `lost_baseline_facts[]`, `added_g4_facts[]`, `suspicious_g4_facts[]`, and a deterministic verdict floor that takes the more conservative of (LLM verdict, deterministic floor); a single critical loss of a grounded baseline fact, or three+ ungrounded G4 facts, force `rejected`.
- `scripts/inspect/benchmark_lollipop_g4.py` runs a separate Gemma 4 reviewer call inside `_run_lollipop_legacy_variant` after the writer. Reviewer timeout is now `max(_gemma_direct_timeout_sec(), 180.0)` because full-source structured comparison can exceed the extraction budget. If the read-only Gemma 4 reviewer times out/errors, the benchmark may use a 4o reviewer fallback and records that as reviewer warning/timing; this never changes the generation payload. The result is exposed as `result["fact_coverage"]` and rendered in the markdown report under `### Fact Extraction Coverage`. `_baseline_fact_list_for_review()` flattens Gemma 3 `per_source_facts` into the reviewer payload only. The report also renders raw baseline `per_source_facts`, baseline `facts_text_clean`, filtered-before-writer facts, metadata anchors, and exact Gemma 4 public/logistics facts so manual fact audit does not depend on reviewer echo text.
- `tests/test_lollipop_legacy.py` covers schema shape, payload assembly (baseline facts allowed only in reviewer), normalization clamping, deterministic verdict floor (`critical_loss → rejected`, `useful_added → tracked`, `ungrounded_g4 → suspicious + partial floor`), full `PETER-FLEET` source snapshot, exact input texts preserved over reviewer echo, rendered raw fact surfaces, and end-to-end reviewer routing (no baseline leakage in extractor + writer, baseline facts present in reviewer payload, fact_coverage with verdict in the variant result).

### Latest full-source fact-extraction evidence (`artifacts/codex/lollipop_g4_benchmark_20260501T212029Z.{md,json}`)

The earlier `T095915Z` / `T105522Z` artifacts are superseded for fact-layer acceptance because multiple fixtures used shortened manual excerpts while displaying real Telegram URLs. The static five-fixture pack now carries full Telegram post snapshots (`AUDIO` 1274 chars, `PETER` 855, `SACRED` 1374, `WORLD` 1628, `RED` 932), and public Telegram extraction selects the exact `data-post` via `?embed=1&mode=tme` rather than the first message on `t.me/s/...`.

Run command:

```bash
.venv/bin/python scripts/inspect/benchmark_lollipop_g4.py \
  --variants baseline,lollipop_legacy \
  --fixtures audio_walk,peter_fleet_lecture,sacred_lecture,world_hobbies,red_cosmos \
  --gemma-call-gap-s 0
```

| Fixture | baseline raw | baseline writer | grounded covered | g4 public/logistics | suspicious | verdict |
| --- | --- | --- | --- | --- | --- | --- |
| `AUDIO-WALK-QUARTER-971` | 14 | 11 | 14 / 14 | 15 / 10 | 5 | partial |
| `PETER-FLEET-LECTURE-5600` | 10 | 9 | 10 / 10 | 11 / 6 | 1 | accepted |
| `SACRED-LECTURE-ZYGMONT-3170` | 12 | 10 | 12 / 12 | 16 / 7 | 4 | accepted |
| `WORLD-HOBBIES-5505` | 8 | 8 | 8 / 8 | 17 / 4 | 0 | accepted |
| `RED-COSMOS-7902` | 12 | 12 | 11 / 12 | 15 / 2 | 0 | partial |

Verdict: **partial but substantially recovered** for the fact layer after correcting the source evidence.

- `55/56` grounded Gemma 3 baseline facts are covered by Gemma 4 extraction. There are `0` critical and `0` major lost facts.
- Remaining fact loss is one minor RED detail: `Жостовские подносы отличаются разнообразием и уникальностью.` Gemma 4 still covers Жостово, подносы, 1825, букет, лак, Каргопольскую and Дымковскую игрушки, but drops that qualitative subfact.
- `AUDIO` covers `14/14` baseline facts but is `partial` because the reviewer marks several G4 facts as suspicious/fragmentary; manual inspection is required there because some suspicious marks are over-strict service facts, while one English/foreign-word drift (`text`-like helper output in the extractor family during iteration) remains a prompt smell to keep watching.
- Writer prompt was deliberately not tuned in this iteration. The benchmark still records text metrics and latency, but acceptance here is fact-layer only. Latency is not accepted (`8.7x..11.4x` vs reused Gemma 3 baseline) after raising extraction/reviewer budgets for full-source reliability.

Next step: keep the full-source fixtures as the regression pack, tighten extractor prompt against suspicious/fragmentary fact surfaces, and then optimize latency separately without reducing fact recall.

## `lollipop_legacy.v13` simplification: extract+write+4o fallback (`2026-04-30`)

### Why we simplified

After the `v7..v12` multi-stage pass (per-source extract + enrichment + plan + paragraph-bound writer + repair) showed thin / fragile public copy and Gemma 4 writer timeouts on sparse-source fixtures, the goal was reset: stop trying to win quality through a lollipop intermediate stack, and instead get a stable **baseline-equivalent** Gemma 4 path with a clearly-reported 4o final-writer fallback for cases where Gemma 4 cannot deliver non-empty output.

Hard constraints carried into v13:

- no baseline text or baseline facts in any legacy generation payload;
- no Gemma 3 inside the legacy generation path;
- no repair pass; no source-draft fallback; no baseline fallback;
- no regex/post-processing edits to writer output (validator is read-only);
- 4o fallback only for the **final writer** when Gemma 4 writer times out / errors / returns empty;
- 4o receives the same source-derived `public_facts`, `logistics_facts`, and `source_excerpt` payload, never baseline text/facts.

### Implementation

- `smart_update_lollipop_lab/legacy_writer_family.py` rewritten to a tight surface: `build_extraction_system_prompt`, `extraction_response_schema`, `normalize_extraction_payload`, `merge_extraction_facts`, `build_writer_system_prompt`, `writer_response_schema`, `build_writer_payload`, `apply_writer_output`, `validate_writer_output`, `compare_to_baseline`. All `enhancement`/`enrich_v8`/`plan_v8`/`writer_v7`/`source_writer`/`source_fact` v12 stages removed.
- `scripts/inspect/benchmark_lollipop_g4.py::_run_lollipop_legacy_variant` rewritten as: per-source Gemma 4 extraction → single Gemma 4 writer → 4o final-writer fallback on timeout/error/empty. `_baseline_fact_list`, `_legacy_event_fact_floor`, `_legacy_required_fact_floor` (relics of the old baseline-fact-floor approach) removed.
- `_ask_4o_json` updated to read `FOUR_4O_TOKEN` (with `FOUR_O_TOKEN` legacy fallback) and to convert Gemma-style uppercase JSON-Schema types (`STRING`, `INTEGER`, `OBJECT`, `ARRAY`) to OpenAI lowercase form before calling structured-outputs.
- `tests/test_lollipop_legacy.py` rewritten to cover v13: extract+write contract, no-baseline-leakage guard for the variant, 4o fallback path on simulated Gemma 4 writer timeout, `FOUR_4O_TOKEN` env handling, narrator-frame and source-fidelity quality signals.

### Latest benchmark (`artifacts/codex/lollipop_g4_benchmark_20260430T201038Z.{md,json}`)

Run command:

```bash
LOLLIPOP_GEMMA_DIRECT_TIMEOUT_SEC=35 LOLLIPOP_GEMMA_WRITER_TIMEOUT_SEC=30 \
.venv/bin/python scripts/inspect/benchmark_lollipop_g4.py \
  --variants baseline,lollipop_legacy \
  --fixtures audio_walk,peter_fleet_lecture,sacred_lecture,world_hobbies,red_cosmos \
  --reuse-baseline-artifact artifacts/codex/lollipop_g4_benchmark_20260430T200617Z.json \
  --reuse-fixture-artifact  artifacts/codex/lollipop_g4_benchmark_20260430T200617Z.json \
  --gemma-call-gap-s 0
```

| Fixture | baseline chars | legacy chars | writer model | quality_delta | speed | verdict |
| --- | --- | --- | --- | --- | --- | --- |
| `AUDIO-WALK-QUARTER-971` | 834 | 885 | 4o (Gemma 4 timeout) | regressed (`promo_phrase_regression`); improvements: `score_improved`, `lead_hook_improved`, `invented_named_tokens_reduced`, `source_named_tokens_improved` | 3.10x (warning) | non-empty + grounded; mild CTA register from 4o |
| `PETER-FLEET-LECTURE-5600` | 665 | 801 | 4o (Gemma 4 timeout) | improved (`source_named_tokens_improved`); warnings: `lost_baseline_lead_hook`, `longer_than_baseline` | 3.20x (warning) | non-empty + grounded; lecturer/topic/logistics covered |
| `SACRED-LECTURE-ZYGMONT-3170` | 1126 | 924 | gemma-4 | improved (`score_improved`, `lead_hook_improved`, `more_compact`, `invented_named_tokens_reduced`); warning: `lost_baseline_epigraph` | 2.57x (pass) | clean accept |
| `WORLD-HOBBIES-5505` | 1040 | 909 | gemma-4 | improved (`more_compact`, `invented_named_tokens_reduced`, `source_named_tokens_improved`); warning: `lost_baseline_epigraph` | 1.83x (pass) | clean accept |
| `RED-COSMOS-7902` | 1077 | 677 | gemma-4 | improved (`score_improved`, `lead_hook_improved`, `invented_named_tokens_reduced`); warnings: `shorter_than_baseline`, `lost_baseline_epigraph` | 0.97x (pass) | clean accept; thin but factually grounded |

Verdict: **partial accepted**.

- 5/5 non-empty legacy outputs (was 0/5 on AUDIO-WALK and RED-COSMOS in `T190623Z`).
- 0 critical validation errors across all five fixtures.
- 0 baseline leakage in any generation payload (the test `test_lollipop_legacy_variant_does_not_send_baseline_to_generation` pins this contract).
- 4o fallback was used on AUDIO-WALK and PETER-FLEET after Gemma 4 writer timed out; both produced grounded, complete public copy. `writer_model`, `writer_fallback_to_4o`, and `writer_failure_reasons` are reported per row.
- 3/5 fixtures stayed at `<=3x` speed gate; 2/5 are slightly over (3.10x and 3.20x) because the 4o fallback was added on top of the timed-out Gemma 4 writer attempt. Speed gate violations are recorded as warnings, not errors, since the user explicitly prioritised non-empty stability over latency in this iteration.
- Known weakness: 4o-fallback prose tends to use mild CTA / direct-address phrasing (`предлагает уникальную возможность`, `вы сможете`), which the read-only validator flags as `style.direct_address` warning and `quality.promo_phrase_regression` (AUDIO-WALK). This is a register-only issue; facts and structure are correct.

Next step (deferred): tighten the 4o fallback prompt to avoid promo register, and find a Gemma 4 writer-tuning pass that gets AUDIO-WALK and PETER-FLEET to first-pass success without inflating per-source extraction.

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
