# Region Talk: review ложного отсева изображений

Status: **external-consultant handoff / implementation review**. Audit date: **2026-07-14 UTC**. Review branch: [`integration/region-talk-image-compliance-review-20260714`](https://github.com/onedayonemasterpiece/events-bot-new/tree/integration/region-talk-image-compliance-review-20260714). Base: `origin/agent/region-talk/R04-live-canary` at `f7abc1c29d2522831e768d8af5d94b94033be210`.

## Короткий вывод

Текущий visual gate даёт доказанные ложные отказы. Оператор уже подтвердил четыре хороших поста среди 18 постов, отклонённых **именно** из-за image score. Это минимум `4/18 = 22.2%` ошибочных решений внутри rejection cohort. Это нижняя граница, а не оценка общего FNR: остальные 14 отказов ещё не имеют полной слепой разметки.

Проблема не сводится к слишком высокому порогу:

1. Telegram/VK-альбом фактически представлен одним anchor-кадром.
2. `postcardness_score` фактически равен некалиброванной CLIP softmax mass по 5 positive и 8 negative prompts.
3. `overall_media_score` — среднее несовместимых и некалиброванных CV/CLIP/LAION/NIMA сигналов; при недоступности модели меняется знаменатель.
4. Порог `0.66` и узкий override не опираются на зафиксированный labelled holdout.
5. Документированный VLM/safety этап отсутствует в runtime-контракте.
6. После трёх постов средний raw score ниже `0.55` может исключить весь источник.

Поэтому **нельзя просто снизить `0.66`**. До калибровки безопасный recall guardrail — не превращать спорный low score в terminal tombstone: альбомы, высокий model disagreement и сильный отдельный сигнал должны идти в `needs_visual_review`/VLM.

## Что проверено в live evidence

Срез live YDB на 2026-07-14:

- 127 уникальных строк в image queue;
- 47 постов с `actual_scored`;
- все `47/47` до image scoring имели подтверждённый text gate:
  `text_region_confirmation_status=text_confirmed_ko_only_for_image_analysis`,
  `kaliningrad_oblast_only_scope=true`, `vector_gate_status=vector_accept_candidate`,
  `text_vector_fusion_status=fused_e5_bge_m3`,
  `image_product_gate_status=accepted_for_image_analysis`;
- 18 постов сейчас воспроизводимо отклоняются только с причиной
  `overall_media_score_below_threshold`;
- все 18 сохранены как terminal `tombstoned_reject / eligibility_reject_tombstone`;
- 12 из 18 — альбомы, но в каждой строке оценён ровно один image;
- ровно один источник сейчас имеет воспроизводимое source-level исключение только из-за среднего image score: `telegram:catwithbag`, 4 изображения, среднее `0.396` при source threshold `0.55`. Его четыре anchor-изображения (баннер/инфографика/самолёт/графика) не являются доказанными false rejects.

Следовательно, ответ на вопрос «скорятся ли фото только после подтверждённого КО?» для этого среза — **да, 47 из 47 actual-scored прошли подтверждённый KO-only text/vector gate**. Это свойство текущего среза и контракт очереди, а не доказательство корректности visual score.

## Locked operator-positive regressions

Эти четыре решения не обсуждаются как модельные предположения: это операторские labels, которые новый контракт обязан сохранить.

| Пост | Альбом | Текущий score signature | Операторский label |
|---|---:|---|---|
| [Архитектурные излишества / 65432](https://t.me/arkhlikbez/65432) | 10 | overall `0.384`; CLIP/postcard `0.002`; LAION `0.406`; NIMA `0.521`; CV `0.606`; technical `0.706` | `ACCEPT_STRONG`: очень хороший архитектурный альбом, не отклонять |
| [Архитектурные излишества / 65489](https://t.me/arkhlikbez/65489) | 6 | overall `0.501`; postcard `0.512`; LAION `0.361`; NIMA `0.495`; CV `0.637`; technical `0.732` | `ACCEPT_STRONG`: хороший зимний архитектурный альбом |
| [ROUTE / 1342](https://t.me/routecommunity/1342) | 10 | overall `0.518`; postcard `0.240`; LAION `0.689`; NIMA `0.471`; CV `0.672`; technical `0.701` | `ACCEPT_STRONG`: сильные фото; поздние кадры содержат дюны, море, берег и маяк |
| [Фигаро / 7491](https://t.me/figarotravel/7491) | 5 | overall `0.604`; postcard `0.923`; LAION `0.475`; NIMA `0.436`; CV `0.582`; technical `0.679` | `ACCEPT_MIXED_DIVERSE`: часть кадров слабее, но альбом даёт ценный разнообразный взгляд на регион |

Машиночитаемый regression seed без копирования чужих изображений: [`tests/fixtures/region_talk_image_scoring_review_cases.json`](../../../tests/fixtures/region_talk_image_scoring_review_cases.json).

## Полный exact image-only rejection cohort

В таблице показаны только посты, для которых текущая воспроизводимая причина отказа — плохой image score. Local/source/compliance/unknown отказы сюда не подмешаны.

| # | Пост | Overall | Postcard | Aesthetic | Technical |
|---:|---|---:|---:|---:|---:|
| 1 | [arkhlikbez/65432](https://t.me/arkhlikbez/65432) | 0.384 | 0.002 | 0.406 | 0.706 |
| 2 | [imnotbozhena/63601](https://t.me/imnotbozhena/63601) | 0.406 | 0.003 | 0.579 | 0.688 |
| 3 | [meduzalive/144244](https://t.me/meduzalive/144244) | 0.434 | 0.007 | 0.435 | 0.746 |
| 4 | [VK wall-211445468_273](https://vk.com/wall-211445468_273) | 0.436 | 0.283 | 0.391 | 0.641 |
| 5 | [arkhlikbez/65489](https://t.me/arkhlikbez/65489) | 0.501 | 0.512 | 0.361 | 0.732 |
| 6 | [rodinargru/24369](https://t.me/rodinargru/24369) | 0.516 | 0.568 | 0.455 | 0.610 |
| 7 | [routecommunity/1342](https://t.me/routecommunity/1342) | 0.518 | 0.240 | 0.689 | 0.701 |
| 8 | [figarotravel/7922](https://t.me/figarotravel/7922) | 0.528 | 0.669 | 0.443 | 0.655 |
| 9 | [kakotdyhaesh/3342](https://t.me/kakotdyhaesh/3342) | 0.541 | 0.350 | 0.659 | 0.745 |
| 10 | [hotostay/15012](https://t.me/hotostay/15012) | 0.596 | 0.536 | 0.702 | 0.671 |
| 11 | [figarotravel/7491](https://t.me/figarotravel/7491) | 0.604 | 0.923 | 0.475 | 0.679 |
| 12 | [figarotravel/7747](https://t.me/figarotravel/7747) | 0.609 | 0.905 | 0.393 | 0.709 |
| 13 | [rodinargru/24341](https://t.me/rodinargru/24341) | 0.612 | 0.770 | 0.515 | 0.697 |
| 14 | [ted_ns/4652](https://t.me/ted_ns/4652) | 0.617 | 0.735 | 0.538 | 0.685 |
| 15 | [pomoriya/2403](https://t.me/pomoriya/2403) | 0.631 | 0.855 | 0.469 | 0.811 |
| 16 | [VK wall-229036531_271](https://vk.com/wall-229036531_271) | 0.633 | 0.917 | 0.478 | 0.735 |
| 17 | [krasivoOrussia/6344](https://t.me/krasivoOrussia/6344) | 0.636 | 0.678 | 0.751 | 0.712 |
| 18 | [TeamTravelme/4638](https://t.me/TeamTravelme/4638) | 0.640 | 0.773 | 0.833 | 0.614 |

Строки 2–3 сохранены для аудита прежнего решения, но после compliance gate они **не должны потреблять image resources и не должны становиться negative labels для visual model**.

## Юридическая сверка и no-spend gate

Проверка выполнена по официальным источникам 2026-07-14. Важно не смешивать точный legal match и редакционный blacklist.

### `telegram:meduzalive`

- текущий реестр Минюста, snapshot `lastModified=2026-07-10 14:27 UTC`, возвращает точную запись №219 `SIA «Medusa Project»`, registration number `40103797863`;
- в resource field прямо указан `https://t.me/meduzalive`;
- дата включения `23.04.2021`, дата исключения пуста;
- Генпрокуратура 26.01.2023 признала деятельность `SIA «Medusa Project»` нежелательной;
- в актуальном на 2026-07-14 перечне экстремистских организаций Минюста точного совпадения нет; в публичном поиске списка Росфинмониторинга на редакции 2026-07-13 точного совпадения также не найдено.

Корректный label: **active foreign-agent exact resource + undesirable organization; not established as extremist**.

Official evidence:

- [current filtered Minjust export](https://reestrs.minjust.gov.ru/rest/registry/39b95df9-9a68-6b6d-e1e3-e6388507067e/export?search=meduzalive);
- [Minjust registry landing page](https://minjust.gov.ru/ru/pages/reestr-inostryannykh-agentov/);
- [Genprokuratura undesirable decision](https://epp.genproc.gov.ru/ru/gprf/mass-media/news/main/e5996596/);
- [current Minjust extremist-organizations list](https://minjust.gov.ru/ru/documents/7822/).

### `telegram:imnotbozhena`

- точные запросы `Небожена`, `imnotbozhena`, а также известные реквизиты коммерческого оператора канала не дали совпадения в текущем foreign-agent registry snapshot;
- в актуальном перечне экстремистских организаций и публичном поиске Росфинмониторинга точного совпадения нет;
- канал называет себя пародийным аккаунтом, поэтому нельзя приписывать ему личность по игре слов в названии;
- обнаруженный commercial operator не доказывает авторство/владение.

Корректный label: **manual editorial source block; no exact official foreign-agent/extremist match in the checked current snapshot**. Называть канал иноагентом или экстремистом по имеющимся данным нельзя.

Official evidence:

- [empty exact-handle Minjust export](https://reestrs.minjust.gov.ru/rest/registry/39b95df9-9a68-6b6d-e1e3-e6388507067e/export?search=imnotbozhena);
- [current Minjust extremist-organizations list](https://minjust.gov.ru/ru/documents/7822/).

### Что реализовано в этой ветке

Exact-match source gate выполняется до source/post fetch и повторно используется перед E5/BGE, image queue и publication eligibility:

- `@meduzalive` → `rejected_compliance_source`, `deny_no_spend`, legal reason based on exact resource URL;
- `@imnotbozhena` → тот же terminal no-spend status, но reason строго `manual_editorial_source_block`;
- fuzzy title/person-name matching запрещён;
- decision row хранит version, checked/snapshot dates, match basis и official evidence URLs;
- unit tests доказывают terminal exact-post rejection до fetch и image disqualification.

Это **не полноценный автоматический sync всех текущих реестров**. Следующий production-safe шаг — отдельный registry snapshot/sync job с TTL, hash/effective date, active/inactive transitions и `REVIEW_NO_SPEND` при stale/unavailable/ambiguous identity. Нельзя делать append-only вечный blacklist или присваивать legal status по fuzzy name.

## Подтверждённые технические причины

### 1. Один кадр вместо альбома

`RegionTalkImageDiagnostic` для Telegram получает один message по `message_id` и один раз вызывает `download_media(message)`. Для VK код собирает attachments, но загружает только `photos[0]`. После этого `actual_image_count` и `images_scored_actual_count` фиксируются как 1.

Это напрямую объясняет ROUTE/1342: anchor содержит автомобили, а сильные дюны/побережье/маяк находятся дальше в альбоме. Но это не единственная причина: anchor 65432 сам является сильной архитектурной фотографией и всё равно получил CLIP `0.002`; Figaro/7491 получил postcardness `0.923`, но был отклонён по overall.

### 2. Postcardness не является calibrated probability

В `apply_image_queue_status()` используется `visual_consensus_score or clip_postcardness_score`, но `visual_consensus_score` нигде не формируется. CLIP postcardness — softmax mass по пяти positive и восьми negative English prompts. При равных logits baseline равен `5/13 = 0.385`, поэтому число нельзя интерпретировать как абсолютную вероятность качества. Оно зависит от числа и состава prompts.

### 3. Overall смешивает разные шкалы

`finalize()` берёт простое среднее:

```text
cv_overall_media_score
clip_postcardness_score
laion_aesthetic_raw / 10
nima_quality_raw / 10
```

CV уже повторно смешивает technical/aesthetic/postcard/low-noise. Доступность модели меняет число слагаемых и смысл порога. Нормализация raw score делением на 10 не является calibration.

### 4. Hard gate опирается на непроверенную шкалу

Основной проход: `overall >= 0.66` и затем `postcardness >= 0.55`. Narrow override: `overall >= 0.63`, `postcardness >= 0.85`, `aesthetic >= 0.52`, `technical >= 0.68`. Зафиксированного golden set/holdout, ROC/PR calibration или source-disjoint validation для этих границ нет.

### 5. Design/runtime mismatch и safety risk

`image-postcardness.md` описывает VLM, region relevance, publication safety и explanation. Фактический runtime заканчивается CV+CLIP+LAION+NIMA; `publication_safety_score` у части строк отсутствует и отдельным hard gate не проверяется. Поэтому смягчение overall без выделения safety/rights blockers небезопасно.

### 6. Source-level penalty усиливает ошибку

После минимум трёх scored posts весь источник может получить `exclude_low_image_quality`, если средний некалиброванный overall ниже `0.55`. Source scheduling нельзя терминализировать средним raw score модели, которая ещё не прошла post-level validation.

## Предлагаемая рамка redesign

### Safe immediate guardrail

До offline calibration:

- не менять `0.66` как единственную меру;
- low composite переводить в `needs_visual_review`, а не terminal reject, если это album, model disagreement велик или есть сильный независимый signal;
- отключить новые source-level terminal exclusions по среднему raw score;
- re-open существующие image tombstones только под новой model/gate version и idempotent backfill.

### Album-first cascade

1. Telegram: разрешить `grouped_id` и соседние сообщения того же альбома; VK: взять все photo attachments.
2. Выполнить дешёвый decode/CV/duplicate/text-overlay/safety prepass для всех frames.
3. Удалить near-duplicates и выбрать diversity-aware top K, например 3–5, для дорогих моделей.
4. Хранить отдельную media row на кадр и post/album aggregate с selected media IDs.
5. Eligibility: `any strong reusable image`/best calibrated frame с подтверждением, а не среднее альбома.
6. Защититься от noisy-max: strong top frame + второй medium frame **или** VLM verification top frame.
7. Diversity — ranking/value signal, не hard quality gate.

### Разделить оси

- technical usability;
- aesthetic/editorial value;
- negative-format likelihood: screenshot/banner/map/document;
- safety/rights;
- contextual fit;
- mixed/diverse editorial value.

Текст уже подтвердил KO, поэтому visual model не обязан повторно узнавать Калининград. Карта/архивный кадр может быть редакционно полезен, хотя он не postcard.

### Calibrated decision

Не усреднять доступные signals. Зафиксировать exact model set, prompts, preprocessing и calibrator version. Raw model signals можно использовать как features для simple logistic/monotonic calibrator или ranker, обученного на human post/album labels. Если required model недоступна, status должен быть retry/degraded contract, а не новая шкала со скрыто изменившимся знаменателем.

## Golden set и validation protocol

Unit of evaluation: post/album плюс selected frame indices.

Labels:

- `ACCEPT_STRONG` — минимум один сильный пригодный frame;
- `ACCEPT_MIXED_DIVERSE` — смешанный, но редакционно ценный альбом;
- `REVIEW`;
- `REJECT_ALL_WEAK`;
- `REJECT_UNSAFE_OR_RIGHTS`.

Стартовый набор:

- все 47 current actual-scored posts;
- все 18 exact image-only rejects;
- четыре locked positive regressions;
- расширение до 300–500 stratified posts/albums: Telegram/VK, 1/2–5/6–10 frames, architecture/coast/aerial/interior/museum/people, maps/archive/screenshots/banners, seasons/aspect ratios, disagreement/score bands;
- oversampling boundary и model-disagreement cases.

Разметка:

- два независимых blind raters + adjudication;
- model scores скрыты;
- split по source, а не image/post, чтобы исключить source leakage;
- holdout замораживается до threshold fitting.

Metrics:

- post-level recall/FNR отдельно для `ACCEPT_STRONG` и `ACCEPT_MIXED_DIVERSE`;
- admitted/review precision, PR-AUC, per-stratum recall;
- unsafe false-pass count;
- disagreement/manual-review volume;
- images/model calls/runtime per post;
- Brier/ECE только если output заявлен как probability.

Proposed acceptance gates, которые консультант должен подтвердить или обоснованно изменить:

- locked regressions: `4/4` не terminally rejected;
- point recall для high-quality positives `>=95%` с 95% confidence interval;
- mixed-album recall `>=90%`;
- zero false pass на locked unsafe set;
- review workload и p95 compute вписываются в явно заданный budget;
- shadow old/new минимум на 100 новых eligible posts;
- manual audit всех `old-reject/new-accept` плюс random agreement sample.

## Запрос внешнему консультанту

Нужен deep technical review production-фильтра изображений Region Talk с приоритетом recall: существенно снизить ложный отсев хороших постов, не размывая safety/compliance и не раздувая стоимость без контроля.

Разрешённый класс внешнего консультанта по project policy: Gemini Pro (`gemini-3-pro-preview`/`gemini-3.1-pro-preview`) либо Opus (`a-opus` или project alias `Opus`). Flash/Lite/Gemma могут быть только supplementary probes и не закрывают review.

Пожалуйста:

1. Подтвердите или опровергните каждую root-cause гипотезу по коду и данным; для каждого locked кейса разделите album acquisition failure и scorer/calibration failure.
2. Дайте точный Telegram `grouped_id`/VK attachment acquisition, frame selection и post aggregation algorithm: max frames, dedupe/diversity, mixed albums, noisy-max control, retries.
3. Определите label ontology под правила «одной сильной фотографии достаточно» и «разнообразный взгляд тоже ценен»; разделите hard blockers и soft ranking signals.
4. Предложите 2–3 реалистичных cascade alternatives для Kaggle с точными inputs/outputs и benchmark protocol. Не изобретайте веса/пороги без golden-set evidence.
5. Решите, нужно ли удалить CLIP prompt mass, заменить на balanced pairwise cosine margins или сменить модель; укажите, как калибровать LAION/NIMA raw scores.
6. Спроектируйте fixed/versioned post-level decision contract, включая поведение при недоступности component model.
7. Уточните sample size, stratification, annotation/adjudication, source-disjoint split, metrics и acceptance thresholds, достаточные для заявления о существенном снижении false rejects.
8. Спроектируйте shadow/canary, idempotent tombstone/source-exclusion backfill и rollback без duplicate resource spend.
9. Назовите точные code/schema/test/doc hotspots и риски, особенно safety/rights gate и дублирование threshold logic между CandidateReport/orchestrator.
10. Разделите рекомендации на immediate safe guardrails и изменения, которые нельзя делать до offline calibration.
11. Отдельно review compliance architecture: legal sync должен идти до post/media/vector/LLM spend, использовать exact URL/registration/stable-ID matching, хранить snapshot/TTL/active transitions и никогда не присваивать legal label по fuzzy name.

## Ожидаемый deliverable от консультанта

Не общий architecture advice, а:

- decision record с выбранным вариантом и отклонёнными альтернативами;
- псевдокод или code-level diff plan acquisition/aggregation/scoring;
- versioned schema/fields/status transitions;
- annotation guide и executable evaluation plan;
- proposed tests с четырьмя locked regressions и must-reject controls;
- shadow/canary/backfill/rollback plan;
- cost estimate в images/model calls/GPU minutes per post;
- список недостающих доказательств и stop/go criteria.

## Code map

- `kaggle/RegionTalkImageDiagnostic/region_talk_image_diagnostic.py`
  - `fetch_tg_media`, `fetch_vk_media` — single-anchor acquisition;
  - `score_clip` — prompt softmax mass;
  - `finalize` — arithmetic mean;
  - `apply_image_queue_status` — one-image post projection;
  - source visual rollup/exclusion.
- `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py`
  - `_publication_candidate_base_ok` — hard score gate;
  - `image_queue_source_disqualification_reason` — pre-image source gate;
  - `source_compliance_terminal_fields` — new exact no-spend source gate;
  - source visual rollup and terminal cleanup.
- `scripts/region_talk_orchestrator.py` — duplicated simplified threshold visibility.
- `docs/features/region-talk-channel/image-postcardness.md` — desired cascade/runtime contract.
- `tests/fixtures/region_talk_image_scoring_review_cases.json` — locked labels and compliance non-negatives.

## Scope boundary of this branch

Ветка реализует ранний exact compliance deny для двух указанных источников, сохраняет операторские labels и готовит проверяемый redesign brief. **Image thresholds/formula в ветке намеренно не изменены**: изменение до external review и labelled shadow evaluation было бы новой некалиброванной догадкой.
