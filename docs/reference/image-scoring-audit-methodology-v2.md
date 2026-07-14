# Region Talk: аудит и методология image scoring v2

**Статус:** consultant decision record / implementation-ready methodology
**Дата:** 2026-07-14
**Целевая ветка:** `integration/region-talk-image-compliance-review-20260714`
**Основной исходный материал:** `docs/features/region-talk-channel/image-scoring-false-negative-review.md`
**Приоритет:** существенно уменьшить terminal false rejects хороших постов и альбомов без ослабления source compliance, publication safety и rights controls.

---

## 0. Решение

Рекомендуемый вариант — **album-complete, recall-first cascade с selective decision**:

1. Exact source compliance остаётся отдельным no-spend gate до загрузки поста, изображений, embeddings и LLM/VLM.
2. Единица acquisition и решения меняется с anchor-изображения на **post/album**.
3. Все изображения альбома проходят дешёвый decode/identity/dedupe/technical/format/safety prepass.
4. Для дорогого quality scoring сначала выбирается diversity-aware набор `K`, причём `K` определяется по кривой `best-frame capture@K`, а не задаётся как необоснованный постоянный порог. Если selected `K` не находит основания для accept, потенциальный terminal reject проходит **reject-confirmation по всем оставшимся distinct frames** либо уходит в review: existential accept и universal reject требуют асимметричного compute.
5. CLIP prompt softmax mass удаляется из роли probability/hard gate. Сохраняются versioned pairwise cosine margins или raw embeddings/features; все model outputs проходят supervised calibration на human-labelled post/album set.
6. Arithmetic mean несовместимых доступных scores удаляется из decision path. Недоступность компонента не меняет знаменатель и смысл итогового числа.
7. Решение имеет три quality-исхода: `AUTO_ACCEPT`, `NEEDS_VISUAL_REVIEW`, `AUTO_REJECT_ALL_WEAK`. Safety/rights/compliance имеют отдельные terminal outcomes и не смешиваются с quality.
8. До завершения offline calibration low image score не должен создавать глобальный tombstone для альбома, если acquisition неполный, есть model disagreement, сильный независимый сигнал или mixed/diverse pattern.
9. Source-level terminal exclusion по среднему raw image score отключается. Допустим только versioned, TTL-limited scheduling hint, основанный на adjudicated labels, но не deny.
10. CandidateReport и orchestrator потребляют один versioned decision contract; дублированная threshold logic удаляется.

Это не «снижение порога `0.66`». Это замена некорректной единицы наблюдения, некалиброванной формулы и необратимого решения на проверяемый post-level контракт с abstention.

---

## 1. Scope, доказательства и границы вывода

### 1.1. Что считается установленным

По открытому handoff-review на срезе 2026-07-14:

- 47 постов фактически дошли до image scoring после подтверждённого KO-only text/vector gate;
- 18 постов воспроизводимо отклоняются только по `overall_media_score_below_threshold`;
- 12 из этих 18 являются альбомами, но фактически оценён один image;
- четыре поста из rejection cohort имеют locked operator-positive labels;
- текущий `postcardness_score` образован как CLIP softmax mass по пяти positive и восьми negative prompts;
- `overall_media_score` — среднее CV, CLIP, LAION и NIMA signals, причём доступность моделей меняет число слагаемых;
- основные quality thresholds не опираются на frozen labelled holdout;
- низкий средний score после нескольких постов может привести к source-level exclusion;
- задокументированный VLM/safety contract не совпадает с фактическим runtime.

### 1.2. Важная поправка к терминологии

`4/18 = 22.2%` — это **подтверждённая минимальная доля ложных отказов внутри наблюдаемого image-only rejection cohort**, а не общий false-negative rate.

Общий FNR считается так:

```text
FNR = terminally_rejected_true_positive_posts / all_true_positive_posts
```

Для его оценки нужен prevalence-preserving holdout, содержащий как отклонённые, так и пропущенные системой положительные посты. Oversampled rejection cohort полезен для поиска причин, но не даёт несмещённой оценки production FNR без sampling weights.

### 1.3. Требуемая проверка перед merge

Code-level plan ниже привязан к функциям и hotspots, названным в handoff-review. Перед реализацией необходимо сверить точные сигнатуры, table writers и transaction boundaries в текущем HEAD. Методологические выводы от этого не зависят; конкретные названия полей и import paths могут потребовать адаптации.

---

## 2. Аудит текущей методики

| ID | Finding | Severity | Как возникает ложный отсев | Решение |
|---|---|---:|---|---|
| F-01 | Единица решения — один anchor frame вместо post/album | P0 | Сильные кадры позже в альбоме невидимы scorer-у | Полный album acquisition, per-frame rows, post-level aggregation |
| F-02 | Telegram/VK acquisition молча считается полным при одном скачанном изображении | P0 | Partial input превращается в terminal quality verdict | Явный `acquisition_status`; partial/transient никогда не равен reject |
| F-03 | CLIP positive prompt mass зависит от числа и состава prompts | P0 | Score меняется при редактировании prompt list без изменения качества изображения | Pairwise margins/raw features + versioned prompt manifest + calibration |
| F-04 | Arithmetic mean смешивает несовместимые шкалы | P0 | Сильный сигнал размывается другими scores; raw `/10` выдаётся за calibration | Supervised post-level model; raw signals остаются features |
| F-05 | Missing component меняет знаменатель | P0 | Одинаковое изображение имеет другой decision scale при outage | Fixed model bundle; retry/degraded status или отдельный обученный degraded contract |
| F-06 | Hard thresholds не зафиксированы labelled holdout | P0 | `0.66`, `0.55` и override не имеют измеренного recall/safety trade-off | Frozen source-disjoint calibration/holdout; constrained threshold fitting |
| F-07 | Soft uncertain score создаёт terminal tombstone | P0 | Ошибку модели сложно исправить после улучшения scorer-а | Quality terminality scope = decision-contract version; review/abstain для uncertainty |
| F-08 | Средний raw score терминализирует источник | P0 | Несколько ошибок постов усиливаются до потери всех будущих кандидатов | Удалить deny; максимум TTL scheduling hint по adjudicated labels |
| F-09 | Safety/rights не являются отдельным обязательным runtime gate | P0 | Простое смягчение quality может увеличить unsafe false passes | Независимый safety/rights outcome; required model unavailable → no auto-accept |
| F-10 | Threshold logic распределена между Diagnostic, CandidateReport и orchestrator | P1 | Drift логики, разные причины решений, сложный rollback | Один versioned decision module/contract |
| F-11 | Diagnostic set смешивается с validation claim | P1 | Oversampled rejects дают оптимистичную/смещённую оценку | Разделить regression, development-enriched и prevalence holdout |
| F-12 | Недостаточная наблюдаемость по кадрам, версиям и стоимости | P1 | Нельзя установить, где потерян лучший кадр и почему изменилось решение | Media manifest, component statuses, versions, cost counters, reason codes |

### 2.1. Почему простое снижение `0.66` неверно

Снижение одного threshold:

- не исправляет отсутствие остальных кадров альбома;
- не делает CLIP mass, LAION, NIMA или CV calibrated probabilities;
- не устраняет изменение шкалы при model outage;
- может пропустить unsafe/rights cases, потому что quality и safety сейчас недостаточно разделены;
- не устраняет source-level amplification;
- не создаёт доказательства улучшения на unseen sources.

Порог можно менять только после фиксации input contract, model bundle, prompt/preprocessing versions и frozen labels.

---

## 3. Разбор locked positive regressions

| Case | Текущий сигнал | Primary failure | Secondary failure / uncertainty | Обязательный regression outcome |
|---|---|---|---|---|
| `arkhlikbez/65432`, album 10 | overall `0.384`, postcard `0.002`, CV `0.606`, technical `0.706` | **Scorer/calibration failure**: anchor сам является сильной архитектурной фотографией, но CLIP почти обнуляет решение | Single-anchor acquisition также нарушает post contract, но не объясняет весь отказ | Не может стать terminal quality reject даже при scoring только anchor; полный album обязан быть acquired |
| `arkhlikbez/65489`, album 6 | overall `0.501`, postcard `0.512`, technical `0.732` | **Post-level calibration/aggregation failure** | Полный album может добавить более сильные кадры; точный вклад acquisition требует per-frame labels | `ACCEPT_STRONG`; selected set содержит human-best frame(s) |
| `routecommunity/1342`, album 10 | overall `0.518`, postcard `0.240`, aesthetic `0.689` | **Acquisition/selection failure**: anchor с автомобилями не представляет дюны, море, берег и маяк | Score сильных поздних кадров ещё надо измерить; нельзя заранее считать scorer исправным | Full album acquired; human-best late frames captured in `top K`; пост не terminally rejected |
| `figarotravel/7491`, album 5 | overall `0.604`, postcard `0.923`, aesthetic `0.475`, technical `0.679` | **Aggregation/gate failure**: сильный independent signal проигрывает mean/threshold | Mixed/diverse semantics не представлены current label | `ACCEPT_MIXED_DIVERSE`; низкий score части кадров не обнуляет ценность альбома |

Эти cases должны быть разделены в tests на:

1. acquisition correctness;
2. best-frame selection recall;
3. frame scorer behavior;
4. post aggregation/decision behavior.

Один интеграционный assert «post accepted» недостаточен: он может скрыть сохранённую ошибку acquisition за более мягким threshold.

---

## 4. Decision record: выбранный и отклонённые варианты

### 4.1. Выбранный вариант A — album-complete calibrated selective cascade

```text
exact source compliance
        ↓
full media acquisition + manifest
        ↓
all-frame cheap prepass / identity / dedupe / safety routing
        ↓
diversity-aware top-K for expensive quality models
        ↓
fixed raw feature bundle
        ↓
calibrated post/album utility model
        ↓
accept found? ── yes → AUTO_ACCEPT
        │ no
        ↓
all-frame reject confirmation or REVIEW
        ↓
NEEDS_VISUAL_REVIEW | AUTO_REJECT_ALL_WEAK
```

VLM применяется только как versioned adjudicator для ambiguous/noisy-max/mixed cases либо как отдельный contextual-fit component. Он не должен быть единственным safety mechanism и не должен вызываться для exact no-spend source deny.

**Почему выбран:**

- исправляет основной structural FN mechanism;
- сохраняет стоимость под контролем через all-frame cheap pass и bounded `K`;
- допускает supervised calibration на 300–500 development examples без обучения большой vision model с нуля;
- даёт reason codes и возможность rollback;
- не заставляет quality model повторно определять Калининград после подтверждённого text gate;
- поддерживает правила «одной сильной фотографии достаточно» и «смешанный альбом может быть редакционно ценен».

### 4.2. Вариант B — score every frame всеми current models

**Плюсы:** простая baseline-реализация; максимальный шанс увидеть лучший кадр; полезен offline как oracle/full-compute benchmark.
**Минусы:** до 10× больше expensive image evaluations на альбом; не исправляет calibration и decision semantics; всё равно нужен post aggregator.
**Решение:** реализовать как offline benchmark, но не как обязательный production path.

### 4.3. Вариант C — VLM-first по contact sheet

**Плюсы:** хорошо понимает mixed albums, карты, архивные материалы и контекст; одна request на альбом.
**Минусы:** model/version drift, нестабильность structured output, стоимость, resolution loss в contact sheet, сложнее воспроизводимость и safety assurance.
**Решение:** использовать только как adjudication/shadow alternative до накопления evidence.

### 4.4. Отклонённые shortcuts

- **Просто снизить overall threshold.** Не исправляет input и calibration.
- **Заменить mean на raw max.** Уменьшит часть FN, но создаст noisy-max false passes.
- **Пере-взвесить current four scores вручную.** Веса останутся guesses на несовместимых шкалах.
- **Только заменить CLIP на другую backbone.** Model swap не решает post unit, missingness, safety и terminality.
- **Сделать VLM единственным gate для всех frames.** Неприемлемо без cost/reproducibility/safety benchmark.

---

## 5. Нормативный decision contract

В этом разделе `MUST`, `MUST NOT`, `SHOULD` и `MAY` имеют нормативный смысл.

### 5.1. Единица решения

- Quality eligibility **MUST** рассчитываться для `post_id`/`album_id`, не для случайно выбранного frame.
- Per-frame scores **MUST** сохраняться отдельно.
- Post decision **MUST** ссылаться на `input_media_manifest_hash` и `selected_media_ids`.
- Изменение состава media **MUST** инвалидировать старый quality decision для данной contract version.

### 5.2. Порядок gate-ов

1. `SOURCE_COMPLIANCE_NO_SPEND` — exact stable identity / URL / registration match; до fetch.
2. `MEDIA_ACQUISITION` — формирование полного manifest.
3. `DECODE_AND_TECHNICAL_HARD_BLOCKERS`.
4. `PUBLICATION_SAFETY_AND_RIGHTS` — отдельно от aesthetic/editorial quality.
5. `QUALITY_AND_EDITORIAL_UTILITY`.
6. `REVIEW_OR_TERMINAL_DECISION`.

Quality score **MUST NOT** отменять safety/compliance reject. Safety/rights score **MUST NOT** использоваться как negative training label для aesthetic model.

### 5.3. Допустимые final statuses

| Status | Terminal | Scope | Условие |
|---|---:|---|---|
| `REJECTED_SOURCE_COMPLIANCE` | да | compliance snapshot/policy | exact no-spend match |
| `REJECTED_UNSAFE_OR_RIGHTS` | да | safety policy version | подтверждённый hard blocker |
| `ACQUISITION_RETRY` | нет | — | transient/partial media acquisition |
| `SCORING_RETRY` | нет | — | required component unavailable |
| `NEEDS_VISUAL_REVIEW` | нет | — | uncertainty, disagreement, mixed album, incomplete evidence |
| `AUTO_ACCEPT` | да для данной version | decision-contract version | calibrated quality pass + safety pass |
| `AUTO_REJECT_ALL_WEAK` | да только для данной version | decision-contract version | complete acquisition + calibrated all-weak evidence + no positive guardrail |

`AUTO_REJECT_ALL_WEAK` **MUST NOT** создавать вечный глобальный tombstone. При новой `decision_contract_version` разрешён idempotent rescore/backfill.

### 5.4. Selective decision вместо одного threshold

Калиброванная модель выдаёт `p_editorially_usable` только если calibration validation пройдена. Используются два learned thresholds:

```text
p >= T_accept  → AUTO_ACCEPT
p <= T_reject  → candidate for AUTO_REJECT_ALL_WEAK
otherwise      → NEEDS_VISUAL_REVIEW
```

Для `AUTO_REJECT_ALL_WEAK` дополнительно обязательны:

- `acquisition_complete = true`;
- все required components `ok`;
- нет hard safety/rights ambiguity;
- нет strong independent signal;
- нет high model disagreement;
- нет mixed/diverse rule activation;
- выполнен reject-confirmation pass по **всем distinct frames**, которые не были покрыты initial top-K expensive scoring, либо case отправлен в review. Статистически высокий `best-frame capture@K` сам по себе не доказывает, что конкретный альбом all-weak.

Логика намеренно асимметрична:

```text
ACCEPT_STRONG: существует хотя бы один подтверждённый strong frame.
REJECT_ALL_WEAK: подтверждено, что каждый distinct frame weak/unusable.
```

Поэтому система может рано завершать вычисление после надёжного accept, но не должна рано завершать его terminal reject-ом после просмотра только подмножества кадров.

`T_accept` и `T_reject` выбираются на calibration split под recall/precision/review constraints; их запрещено назначать по интуиции или на locked regression cases.

### 5.5. Compliance architecture: non-regression contract

Source compliance остаётся отдельной подсистемой и **MUST** выполняться до post/media/vector/LLM/VLM spend.

- matching только по exact resource URL, registration number, stable platform ID или другому доказанному идентификатору;
- fuzzy title/person-name similarity не создаёт legal status;
- legal reason и manual editorial block хранятся раздельно, например `foreign_agent_exact_resource`, `undesirable_organization`, `manual_editorial_source_block`;
- snapshot хранит `snapshot_id`, source URL, fetched/checked/effective dates, hash, TTL и active/inactive transition;
- stale/unavailable/ambiguous registry state → `REVIEW_NO_SPEND`, а не бессрочный guessed blacklist;
- изменение active status создаёт новую decision version и audit event;
- compliance-blocked posts не становятся visual-quality negatives;
- quality redesign не имеет права отменять source compliance, но и source title не должен загрязнять image scorer.

---

## 6. Media acquisition

### 6.1. Общий manifest

Каждый post должен формировать immutable manifest:

```json
{
  "platform": "telegram|vk",
  "source_key": "...",
  "post_id": "...",
  "album_key": "...",
  "acquisition_version": "...",
  "expected_media_count": null,
  "fetched_media_count": 0,
  "acquisition_status": "complete|partial|retry|no_supported_media",
  "media": [],
  "manifest_hash": "sha256:..."
}
```

Для каждого media item сохраняются stable platform ID, ordinal, source URL, MIME/type, width/height, byte size, content SHA-256, perceptual hash, download status и error code.

### 6.2. Telegram algorithm

```python
def fetch_telegram_album(client, chat, anchor_message_id, cfg):
    anchor = fetch_message_exact(chat, anchor_message_id)
    if anchor is None:
        return retry("anchor_not_found_or_transient")

    if not anchor.grouped_id:
        return manifest_from_single_supported_media(anchor)

    grouped_id = anchor.grouped_id
    matches = {anchor.id: anchor}

    # Исторический API не даёт count прямо в queue row, поэтому сканируем
    # ограниченную окрестность, фильтруя ТОЛЬКО exact grouped_id.
    for message in scan_history_around(
        chat=chat,
        anchor_id=anchor.id,
        max_messages_each_direction=cfg.max_neighbor_scan,
    ):
        if message.grouped_id == grouped_id and is_supported_image_media(message):
            matches[message.id] = message

    ordered = [matches[mid] for mid in sorted(matches)]

    edge_truncated = same_group_seen_at_scan_edge(ordered, cfg)
    if edge_truncated or not stable_on_bounded_refetch(ordered, cfg):
        return partial_retry(grouped_id, ordered)

    if len(ordered) > cfg.max_album_items:
        return partial_retry("album_exceeds_contract_cap", ordered)

    return complete_manifest(
        album_key=f"{chat}:{grouped_id}",
        messages=ordered,
    )
```

Требования:

- фильтрация только по exact `grouped_id`; соседние сообщения другого альбома не смешиваются;
- сортировка по original message ID/ordinal;
- фото и image-documents обрабатываются согласно explicit supported-media policy;
- `grouped_id != null` и один найденный frame не считаются автоматически complete;
- FloodWait/network/CDN/decode errors → retry/partial, не quality reject;
- bounded scan cap и max album size versioned/configured и наблюдаемы;
- повторная загрузка использует cached media по stable ID/hash.

### 6.3. VK algorithm

```python
def fetch_vk_post_images(vk, owner_id, post_id, cfg):
    post = wall_get_by_id_exact(owner_id, post_id)
    attachments = flatten_visible_photo_attachments(
        post,
        include_copy_history=cfg.include_copy_history,
    )

    expected = len(attachments)
    items = []
    for ordinal, photo in enumerate(attachments):
        best = choose_largest_supported_size(photo.sizes)
        items.append(download_or_reuse(photo, best, ordinal))

    if any(item.status != "ok" for item in items):
        return partial_retry(expected, items)

    return complete_manifest(expected, items)
```

Требования:

- запрещено брать только `photos[0]`;
- `copy_history` policy фиксируется, а не меняется неявно;
- выбирается максимальный подходящий resolution variant;
- `expected_media_count` известен из attachment list;
- duplicate attachment IDs не создают лишние scorer slots;
- access-key/permission/transient failures не превращаются в `all_weak`.

---

## 7. Dedupe и frame selection

### 7.1. Dedupe

Последовательно применяются:

1. exact platform media ID;
2. exact content SHA-256;
3. perceptual near-duplicate grouping;
4. embedding-based near-duplicate grouping как secondary signal.

Near-duplicate thresholds должны быть validated на отдельном duplicate set. Внутри duplicate cluster representative выбирается по resolution/technical usability, но original IDs сохраняются.

### 7.2. Cheap prepass для всех frames

Все acquired frames проходят:

- decode/orientation validation;
- dimensions/aspect/byte size;
- blur/exposure/compression/technical features;
- photo/illustration/screenshot/banner/map/document/text-overlay features;
- image embedding для diversity и CLIP-margin features;
- required safety precheck согласно publication contract;
- duplicate identity.

`map`, `archive`, `document` и `banner` — не автоматические aesthetic negatives. Это format axis; editorial utility оценивается отдельно.

### 7.3. Diversity-aware selection без круговой ошибки

Нельзя выбирать top K только по current overall score: тот же ошибочный scorer снова потеряет нужные кадры.

Рекомендуемый deterministic selector:

1. исключить только confirmed hard blockers;
2. схлопнуть duplicate clusters;
3. включить по одному Pareto candidate с лучшим rank по независимым axes:
   - technical usability;
   - raw aesthetic signal;
   - balanced CLIP editorial/photo margin;
   - low negative-format likelihood / contextual utility;
4. оставшиеся slots заполнить greedy max-min embedding diversity;
5. зарезервировать coverage slot для minority format/content cluster, если альбом mixed;
6. tie-break: original ordinal, затем stable media ID.

`K` выбирается offline:

```text
K* = минимальный K, для которого lower CI(best-frame capture@K)
     достигает заданного target при допустимой стоимости.
```

На первом benchmark сравниваются `K ∈ {3, 4, 5}`. Production value не фиксируется до измерения. Все четыре locked regressions обязаны иметь `best-frame capture@K = 1`.

### 7.4. Noisy-max control и reject-confirmation

Raw `max(score_i)` не является final decision. Один top frame может привести к accept только если выполнено одно из условий:

- calibrated top-frame evidence находится в high-confidence accept region;
- либо top frame подтверждён независимым model family/VLM;
- либо есть второй non-duplicate medium-or-better frame;
- иначе → `NEEDS_VISUAL_REVIEW`.

Это сохраняет правило «одной сильной фотографии достаточно», но не допускает случайный высокий outlier как безусловный pass.

Обратное решение строже. Если initial `K` не содержит accept evidence, pipeline **не** делает вывод, что весь альбом слабый. Перед `AUTO_REJECT_ALL_WEAK` он выполняет reject-confirmation для остальных distinct frames дешёвой high-recall моделью плюс, при необходимости, expensive scorer/VLM. Если полный coverage не достигнут в budget, исход — `NEEDS_VISUAL_REVIEW`, а не reject.

### 7.5. Decision pseudocode

```python
def decide_post_image_eligibility(job, contract):
    compliance = contract.source_compliance.check_exact(job.source_identity)
    if compliance.no_spend:
        return terminal("REJECTED_SOURCE_COMPLIANCE", compliance.reason)

    manifest = contract.acquisition.fetch_manifest(job)
    if manifest.status in {"partial", "retry"}:
        return nonterminal("ACQUISITION_RETRY", manifest.reason)

    frames = contract.prepass.run_all(manifest.media)
    if frames.required_component_missing:
        return nonterminal("SCORING_RETRY", frames.missing_components)

    safety = contract.safety.evaluate_required_coverage(frames)
    if safety.hard_reject:
        return terminal("REJECTED_UNSAFE_OR_RIGHTS", safety.reason)
    if safety.ambiguous:
        return nonterminal("NEEDS_VISUAL_REVIEW", safety.reason)

    distinct = contract.dedupe.representatives(frames)
    selected = contract.selector.select(distinct, k=contract.initial_k)
    selected_scores = contract.quality.score(selected)

    partial_post = contract.calibrator.predict(
        all_frame_prepass=frames,
        expensive_scores=selected_scores,
        coverage="selected",
    )

    if contract.policy.confirmed_strong_accept(partial_post, selected_scores):
        return terminal_for_version(
            "AUTO_ACCEPT",
            reason="strong_frame_confirmed",
            selected_media_ids=selected.ids,
        )

    if contract.policy.mixed_diverse_candidate(partial_post, frames):
        mixed = contract.adjudicator.verify_mixed_album(job.text, selected, frames)
        if mixed.accept:
            return terminal_for_version("AUTO_ACCEPT", reason="mixed_diverse_confirmed")
        if mixed.uncertain:
            return nonterminal("NEEDS_VISUAL_REVIEW", mixed.reason)

    remaining = distinct.minus(selected)
    confirmation = contract.reject_confirmation.score_all_or_abstain(remaining)
    if not confirmation.complete:
        return nonterminal("NEEDS_VISUAL_REVIEW", "reject_confirmation_incomplete")

    full_post = contract.calibrator.predict(
        all_frame_prepass=frames,
        expensive_scores=selected_scores + confirmation.scores,
        coverage="all_distinct",
    )

    if contract.policy.confirmed_all_weak(full_post):
        return terminal_for_version("AUTO_REJECT_ALL_WEAK", reason="all_frames_weak_confirmed")

    return nonterminal("NEEDS_VISUAL_REVIEW", full_post.reason_codes)
```

---

## 8. Scoring features и calibration

### 8.1. Что удалить из decision path

- `postcardness = softmax mass(5 positive, 8 negative)` как probability;
- `laion_raw / 10` и `nima_raw / 10` как якобы calibrated `[0,1]`;
- arithmetic mean только доступных components;
- hard decision по одному `overall_media_score`;
- изменение decision scale при component outage.

Raw values сохраняются для audit/backward comparison.

### 8.2. CLIP replacement

Текущий prompt-pool mass имеет вид:

```text
P_pos = sum(exp(s_i / T), i in positive prompts)
      / sum(exp(s_j / T), j in all prompts)
```

Если logits равны, `P_pos = |positive| / (|positive| + |negative|) = 5/13 ≈ 0.385`. Следовательно, число зависит от cardinality prompt lists и не является абсолютной probability качества. Добавление или удаление prompt-а меняет baseline даже при неизменных image embeddings.

Для каждого semantically matched prompt pair:

```text
m_j = cosine(image_embedding, positive_prompt_j)
    - cosine(image_embedding, negative_prompt_j)
```

Предпочтительно сохранять vector `m_1 ... m_J` как features. Если нужен один diagnostic scalar, используется равновесное robust aggregation по concept groups, например median pairwise margin. Он всё равно называется `raw_margin`, а не probability.

Prompt manifest обязан включать:

- model ID и weights hash;
- tokenizer/preprocessing version;
- positive/negative pairs;
- language;
- prompt group weights, если они есть;
- temperature/logit scale;
- manifest SHA-256.

Unit test: изменение числа prompts или добавление duplicate prompt не должно молча менять production contract; требуется новая version.

### 8.3. Не заставлять vision повторять text gate

Text/vector pipeline уже подтвердил региональную релевантность. Image quality model не должен hard-reject:

- архитектуру не «открыточного» стиля;
- зимнюю/пасмурную погоду;
- карту или архивный кадр, если они редакционно полезны;
- mixed album, содержащий разные типы визуального материала.

Visual contextual-fit может быть soft feature/VLM input, но не дублирующий KO classifier без отдельного доказательства пользы.

### 8.4. Baseline calibrator

Для небольшого labelled set рекомендуется начать с regularized, interpretable post-level model, а не с deep MIL training.

Frame-level inputs:

```text
technical features
negative-format features
CLIP pairwise margins
LAION raw
NIMA distribution-derived features/raw score
safety/rights outcome (не как quality feature, а separate gate)
model-status/missing flags
```

Post-level MIL-style summary features:

```text
top1 and top2 frame utility features
best/median/lower-quantile technical features
count of non-duplicate usable frames
number of visual clusters / diversity summary
top1-top2 gap
album size
mixed-format indicators
acquisition completeness
component status flags
platform
```

Первый baseline — L2-regularized logistic model или другой простой model, дающий воспроизводимый feature contribution report. Monotonic constrained boosting сравнивается как challenger. Более сложный model принимается только при статистически подтверждённом улучшении на frozen holdout.

### 8.5. Calibration protocol

- Model fitting, probability calibration и final holdout evaluation используют разные source-disjoint subsets.
- При малом calibration set используется parametric calibration; isotonic допускается только при достаточном объёме и стабильности.
- Thresholds выбираются под constrained objective: recall/FNR first, затем review volume и admitted precision.
- Oversampled diagnostic cases получают sampling weights либо исключаются из prevalence metric.
- Если output не проходит Brier/ECE/reliability validation, поле называется `decision_score`, не `probability`.
- Calibration version привязана к exact feature/model/prompt/preprocessing bundle.

### 8.6. Поведение при model outage

Initial safe contract:

```text
required component unavailable → SCORING_RETRY
retry exhausted             → NEEDS_VISUAL_REVIEW_DEGRADED
```

Запрещено:

```text
mean(available_components)
```

Degraded auto-decision допускается только как отдельный `model_bundle_version`, обученный и validated на искусственно/реально missing components. Нельзя скрыто использовать full-bundle threshold.

---

## 9. Post/album label ontology

### 9.1. Per-frame axes

Каждый frame размечается независимо по осям:

- `technical_usability`: `usable | marginal | unusable`;
- `editorial_visual_value`: `strong | medium | weak`;
- `format`: `photo | illustration | map | archive | screenshot | banner | document | mixed | other`;
- `safety_rights`: `pass | review | hard_block`;
- `contextual_value`: `high | medium | low | unknown`;
- `duplicate_cluster_id`;
- `human_best_frame`: boolean.

Не следует превращать все не-фотографии в один negative visual-quality class.

### 9.2. Post-level labels

#### `ACCEPT_STRONG`

Минимум один non-duplicate frame является сильным, технически пригодным и publishable; остальные слабые кадры не отменяют accept.

#### `ACCEPT_MIXED_DIVERSE`

Альбом в целом редакционно ценен, хотя отдельные frames неоднородны. Типичные основания:

- два и более distinct medium-or-better frames;
- сочетание сильного контекстного материала и визуального вида;
- разные ракурсы/сезоны/детали дают полезный взгляд на регион;
- слабые кадры могут быть исключены из publication selection.

#### `REVIEW`

Недостаточно evidence, acquisition partial, model disagreement, права/контекст неясны или raters расходятся.

#### `REJECT_ALL_WEAK`

Acquisition complete, и все distinct frames после blind review не дают достаточной технической или редакционной ценности.

#### `REJECT_UNSAFE_OR_RIGHTS`

Независимый hard blocker. Не используется как visual-quality negative.

### 9.3. Compliance/non-quality exclusions

Посты, остановленные source compliance, local/source policy или недоступностью данных:

- не включаются как negatives в quality model;
- сохраняются в отдельном audit stratum;
- могут использоваться только для regression tests соответствующего gate.

---

## 10. Annotation guide

### 10.1. Два прохода разметки

**Pass A — image-only:** technical, format, aesthetic/editorial visual value без source handle, model scores и current decision.
**Pass B — post context:** полный album в original order + текст поста; post-level utility и best-frame indices.

Так отделяется способность модели оценить изображение от редакционного контекста.

### 10.2. Blindness

Raters не видят:

- current score и threshold;
- old/new decision;
- source-level quality history;
- legal/editorial label источника, если он не нужен для rights decision.

### 10.3. Raters и adjudication

- два независимых raters на каждый post/album;
- обязательная adjudication при различии post label, best-frame set или hard blocker;
- confidence `high|medium|low` и reason codes;
- agreement metric по post label и frame axes;
- low-agreement strata возвращаются на уточнение guide до model fitting.

### 10.4. Минимальная annotation row

```json
{
  "case_id": "...",
  "platform": "telegram|vk",
  "source_group_id": "hashed/stable",
  "post_id": "...",
  "media_manifest_hash": "...",
  "album_size": 0,
  "frame_annotations": [],
  "best_frame_indices": [],
  "post_label": "ACCEPT_STRONG|ACCEPT_MIXED_DIVERSE|REVIEW|REJECT_ALL_WEAK|REJECT_UNSAFE_OR_RIGHTS",
  "reason_codes": [],
  "rater_id": "...",
  "rater_confidence": "high|medium|low",
  "adjudication_status": "pending|agreed|adjudicated"
}
```

---

## 11. Golden set и validation design

### 11.1. Три разных набора

#### A. Locked regression set

- четыре operator-positive cases;
- must-reject safety/rights controls;
- source-compliance controls;
- acquisition edge cases.

Он используется для CI regression, но **не** для выбора thresholds и не для performance claim.

#### B. Development-enriched set

300–500 stratified posts/albums:

- Telegram/VK;
- single, 2–5, 6–10 frames;
- score bands, boundary и model-disagreement cases;
- architecture/coast/aerial/interior/museum/people;
- maps/archive/screenshots/banners/documents;
- seasons, aspect ratios, resolutions;
- current accept/review/reject;
- oversampled image-only rejects.

Этот размер подходит для failure discovery, feature/model selection и pilot calibration, но не гарантирует точность всех subgroup claims.

#### C. Frozen prevalence-preserving confirmation holdout

Последовательная или корректно случайная выборка eligible production posts, не обогащённая только rejects. Split замораживается до final threshold fitting. Нужен отдельный temporal holdout после model selection.

### 11.2. Split rules

- split по source group, не по image/post;
- near-duplicate media не должны попадать в разные splits;
- same repost/copy chain группируется;
- final temporal holdout состоит из более новых posts;
- calibration и test не переиспользуются для prompt editing;
- все dataset manifests versioned и hashed.

### 11.3. Отдельная проверка acquisition и scoring

Обязательные метрики:

1. `album_acquisition_complete_rate`;
2. `human_best_frame_capture@K`;
3. scorer recall при oracle human-best frames;
4. scorer recall при production selected frames;
5. final post decision recall.

Разница между пунктами 3 и 4 измеряет selection loss. Разница между 4 и 5 измеряет aggregation/threshold loss.

---

## 12. Метрики и статистические критерии

### 12.1. Primary quality metrics

- `terminal_recall_strong` — доля `ACCEPT_STRONG`, не получивших terminal quality reject;
- `terminal_recall_mixed` — то же для `ACCEPT_MIXED_DIVERSE`;
- `terminal_FNR` отдельно по двум positive labels;
- `false_reject_share_among_quality_rejects`;
- `auto_accept_precision`;
- `manual_review_rate`;
- `final_precision/recall_after_review`;
- paired old-vs-new absolute FNR reduction.

Для recall-first gate `REVIEW` не считается terminal false reject, но учитывается как workload/cost.

### 12.2. Safety and rights metrics

- unsafe/rights false-pass count;
- false-pass rate с confidence interval;
- coverage required safety component;
- missing/degraded safety status count;
- manual audit всех `old-reject/new-accept` до canary relaxation.

Zero failures на маленьком locked set — regression guarantee, но не population risk estimate.

### 12.3. Calibration metrics

Только если output заявлен как probability:

- Brier score;
- reliability curve/ECE;
- calibration by platform, album size и format strata;
- probability drift by model bundle version.

PR-AUC полезен для model comparison, но не заменяет recall/FNR at operating thresholds.

### 12.4. Acquisition/selection/cost metrics

- frames expected/fetched/decoded;
- partial acquisition rate;
- duplicate-slot waste;
- best-frame capture@K;
- frames prepassed;
- frames expensive-scored;
- VLM requests;
- GPU milliseconds by stage;
- p50/p95 latency and compute per post.

### 12.5. Sample-size correction к исходному proposal

Для двухстороннего 95% Clopper–Pearson lower bound при наблюдаемом числе terminal false rejects:

| Target lower bound | 0 errors | 1 error | 2 errors |
|---|---:|---:|---:|
| Recall `>= 95%` | 72 positives | 110 positives | 142 positives |
| Recall `>= 90%` | 36 positives | 54 positives | 70 positives |

Поэтому practical confirmation target:

- минимум **150 adjudicated `ACCEPT_STRONG`**;
- минимум **80 adjudicated `ACCEPT_MIXED_DIVERSE`**;
- достаточное число distinct sources; source-cluster bootstrap обязателен как основной uncertainty estimate;
- exact binomial interval публикуется как sensitivity analysis, поскольку posts одного source не полностью независимы.

Для заявления «unsafe false-pass rate <1%» при нуле наблюдаемых ошибок требуется примерно 299 независимых controls для односторонней 95% границы; маленький must-reject set такого заявления не поддерживает.

### 12.6. Acceptance gates

До production auto-decision:

1. `4/4` locked positives не terminally rejected.
2. Все locked must-reject controls остаются blocked.
3. Lower 95% source-clustered CI для `terminal_recall_strong` достигает `>=95%`.
4. Lower 95% CI для `terminal_recall_mixed` достигает `>=90%`.
5. `best-frame capture@K` не теряет locked cases и достигает заранее установленного CI target.
6. Нет silent partial acquisition, превращённого в quality reject.
7. Review workload и p95 compute находятся в утверждённых budgets.
8. Все `old-reject/new-accept` из shadow вручную проверены.
9. Thresholds, prompt manifest, model bundle и preprocessing frozen до final holdout.

---

## 13. Versioned schema

### 13.1. Post decision row

```json
{
  "decision_id": "...",
  "platform": "telegram|vk",
  "source_key": "...",
  "post_id": "...",
  "album_key": "...",
  "input_media_manifest_hash": "sha256:...",

  "decision_contract_version": "region-talk-image-v2.x",
  "acquisition_version": "...",
  "selection_version": "...",
  "preprocessing_version": "...",
  "model_bundle_version": "...",
  "calibrator_version": "...",
  "prompt_manifest_hash": "sha256:...",
  "safety_policy_version": "...",
  "source_compliance_snapshot_id": "...",

  "expected_media_count": null,
  "fetched_media_count": 0,
  "distinct_media_count": 0,
  "selected_media_ids": [],
  "acquisition_status": "...",
  "component_statuses": {},

  "quality_decision_score": null,
  "quality_probability": null,
  "uncertainty": null,
  "model_disagreement": null,
  "mixed_diverse_signal": null,

  "safety_decision": "pass|review|reject",
  "rights_decision": "pass|review|reject",
  "quality_decision": "accept|review|reject_all_weak",
  "final_status": "...",
  "terminal": false,
  "terminal_scope": "none|policy|decision_contract_version",
  "reason_codes": [],

  "cost_counters": {
    "images_downloaded": 0,
    "frames_prepassed": 0,
    "frames_expensive_scored": 0,
    "vlm_calls": 0,
    "gpu_ms_by_stage": {}
  },

  "created_at": "...",
  "result_hash": "sha256:..."
}
```

### 13.2. Per-frame row

```json
{
  "media_id": "platform-stable-id",
  "post_decision_id": "...",
  "ordinal": 0,
  "source_url": "...",
  "content_sha256": "...",
  "perceptual_hash": "...",
  "duplicate_cluster_id": "...",
  "width": 0,
  "height": 0,
  "mime": "...",
  "decode_status": "ok|error",
  "hard_blocker": null,
  "technical_features": {},
  "format_features": {},
  "raw_model_features": {},
  "component_statuses": {},
  "selected_for_expensive_scoring": false,
  "selection_reasons": [],
  "frame_decision_score": null,
  "frame_probability": null
}
```

### 13.3. Idempotency key

```text
(platform, source_key, post_id, input_media_manifest_hash, decision_contract_version)
```

Повторный run с тем же key обязан вернуть тот же materialized result либо безопасно завершить already-complete operation. Новая version создаёт новую decision row, не перезаписывая audit history.

---

## 14. Shadow, canary, backfill и rollback

### 14.1. Shadow sequence

#### Shadow 0 — regression only

Locked positives, must-reject controls, acquisition tests. Publication behavior не меняется.

#### Shadow 1 — dual decision, no effect

Old и new contracts выполняются на одном eligible stream. Сохраняются:

```text
old_status
new_status
old_reason
new_reason
selected_media_ids
cost delta
disagreement category
```

Минимум: 200 новых eligible posts **или** до накопления 50 adjudicated `old-reject/new-accept` disagreements — что наступит позже. Все rescue disagreements проверяются вручную.

#### Shadow 2 — recall guardrail

New contract может переводить old image-only reject в `NEEDS_VISUAL_REVIEW`, но не auto-publish. Compliance/safety rejects не меняются.

#### Canary

После offline gates новая система получает bounded traffic. Любой unsafe false pass, silent partial→reject, locked regression или version mismatch останавливает canary.

### 14.2. Backfill

Порядок:

1. четыре locked positives;
2. все 18 exact image-only rejects;
3. все 47 actual-scored posts;
4. posts affected by `exclude_low_image_quality`;
5. далее broader historical cohort по budget.

Backfill filter включает только quality reasons. Compliance/local/source/unknown decisions не переопределяются quality migration.

Media и embeddings переиспользуются по stable ID/hash/model version. Повторный fetch разрешён только при incomplete manifest или policy-required refresh.

### 14.3. Source exclusions

- существующие exclusions с единственной причиной `exclude_low_image_quality` помечаются stale для новой contract version;
- они не удаляются без audit trail;
- новые raw-score-based source denies запрещены;
- допустим scheduler hint с TTL, decay и reason `adjudicated_low_yield`, но он не влияет на legal/editorial allowlist/denylist.

### 14.4. Rollback

Rollback меняет active `decision_contract_version` pointer. Rows новой version сохраняются. Не выполняются destructive delete/overwrite. Publication pipeline проверяет explicit active version и final status, а не «последнюю строку» без version filter.

---

## 15. Cost model

Без фактических timings конкретного Kaggle accelerator абсолютная оценка GPU-minutes будет недоказанной. До canary требуется benchmark instrumentation; ниже — обязательный расчётный contract.

Пусть:

- `n` — acquired distinct frames, capped acquisition policy;
- `k` — selected frames для expensive quality scoring;
- `q` — доля posts, дошедших до reject-confirmation;
- `r` — доля posts, отправленных в VLM review;
- `C_pre`, `C_embed`, `C_safety`, `C_quality`, `C_reject_confirm`, `C_vlm` — измеренные stage costs.

```text
E[C_post] = n × (C_pre + C_embed + C_safety)
          + k × C_quality
          + q × (n - k) × C_reject_confirm
          + r × C_vlm
```

Асимметрия выгодна: strong post может завершиться после обнаружения подтверждённого positive frame; дополнительный full-album spend несут прежде всего потенциальные rejects, для которых ошибка наиболее опасна.

Рекомендуемый benchmark envelope:

| Alternative | Cheap all-frame | Expensive quality | VLM | Роль |
|---|---:|---:|---:|---|
| Current | 1 frame | 1 frame × current bundle | 0 | Baseline |
| A, recommended | `n <= acquisition cap` | initial `k` = candidate values 3/4/5; remaining `n-k` только для reject-confirmation | 0–1 contact-sheet/full-frame adjudication | Production candidate |
| B, full-frame oracle | `n` | `n` | 0 | Offline recall upper baseline |
| C, VLM-first | decode `n` | 0–2 | 1 | Shadow challenger |

Обязательные counters:

```text
images_downloaded
bytes_downloaded
frames_decoded
frames_prepassed
frames_embedded
frames_expensive_scored
safety_frames_scanned
vlm_requests
stage_gpu_ms
stage_wall_ms
cache_hits/misses
```

`k` выбирается по Pareto curve `best-frame capture@K` versus p95 compute. Business budgets для review rate, GPU-minutes и latency должны быть зафиксированы до go/no-go; при отсутствии budget система не должна silently расширять `K` или VLM usage.

---

## 16. Code-level change plan

### 16.1. `kaggle/RegionTalkImageDiagnostic/region_talk_image_diagnostic.py`

#### `fetch_tg_media`

- заменить single-media return на `MediaManifest`;
- exact `grouped_id` acquisition;
- partial/retry statuses;
- stable media IDs, ordinals, hashes;
- cache-aware downloads.

#### `fetch_vk_media`

- пройти все photo attachments, а не `photos[0]`;
- explicit `copy_history` policy;
- expected/fetched counts и partial status.

#### `score_clip`

- сохранить raw embeddings и balanced pairwise margins;
- удалить prompt-pool mass из hard decision;
- добавить prompt manifest/model/preprocessing versions;
- добавить invariant/versioning tests.

#### `finalize`

- перестать формировать arithmetic mean доступных components;
- materialize per-frame raw features;
- вызвать shared post-level decision component;
- missing required model → retry/review, не новый знаменатель.

#### `apply_image_queue_status`

- использовать post/album decision;
- не project один frame на весь post;
- version-scoped terminality;
- reason codes и selected media IDs.

#### source visual rollup

- убрать terminal `exclude_low_image_quality` по raw average;
- оставить observability/scheduler hint только после отдельной policy.

### 16.2. `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py`

#### `_publication_candidate_base_ok`

- читать `final_status`, `decision_contract_version`, `safety_decision`, `acquisition_status`;
- удалить повторное вычисление `overall >= ...` / `postcardness >= ...`;
- fail closed для missing active version, но через retry/review, не quality tombstone.

#### `image_queue_source_disqualification_reason`

- оставить exact compliance/manual editorial source policy;
- убрать image-average source deny.

#### `source_compliance_terminal_fields`

- сохранить разделение exact legal match и manual editorial block;
- snapshot/TTL/active-state fields;
- no fuzzy identity.

#### source rollup/cleanup

- не удалять/терминализировать posts новой version из-за старого source average;
- version-aware backfill.

### 16.3. `scripts/region_talk_orchestrator.py`

- удалить duplicated simplified threshold logic;
- orchestrator показывает/маршрутизирует canonical decision, но не пересчитывает его;
- active decision-contract version задаётся один раз;
- retries различают acquisition, model, review и terminal policy outcomes.

### 16.4. Shared module

Создать один importable component, условно:

```text
region_talk_image_scoring/
  acquisition.py
  selection.py
  features.py
  decision.py
  schema.py
  evaluation.py
```

Kaggle kernels и orchestrator используют один versioned package artifact/hash. Если packaging constraints требуют vendoring, CI обязан сравнивать content hash и запрещать divergent copies.

### 16.5. Documentation

`docs/features/region-talk-channel/image-postcardness.md` должен стать runtime contract, а не aspirational design:

- actual stages;
- model and prompt versions;
- statuses and terminality;
- missingness behavior;
- safety/rights separation;
- evaluation metrics;
- active thresholds/calibrator provenance;
- rollback procedure.

### 16.6. Fixture

`tests/fixtures/region_talk_image_scoring_review_cases.json` расширить полями:

```text
expected_album_size
expected_acquisition_status
human_best_frame_indices
expected_selected_frame_indices_or_capture
post_label
primary_failure_layer
must_not_terminal_reject
must_reject_reason
```

---

## 17. Tests

### 17.1. Acquisition unit tests

- Telegram single image.
- Telegram `grouped_id` album with all members returned in order.
- Neighboring albums/messages do not contaminate group.
- Partial network/download failure → retry, not reject.
- Album scan cap edge → partial/review.
- VK all photo attachments acquired.
- VK duplicate attachments do not consume duplicate slots.
- Unsupported media has explicit reason.

### 17.2. Selection tests

- deterministic output for same manifest/version;
- exact/near duplicates collapsed;
- high-quality but non-anchor frame selected;
- mixed album reserves diversity/format coverage;
- all four locked cases capture human-best frame(s);
- increasing `K` cannot reduce capture for same selector version;
- selection does not depend solely on old overall score.

### 17.3. Scoring/calibration tests

- CLIP equal-logit prompt-count baseline is not treated as quality probability;
- prompt manifest change requires version change;
- raw LAION/NIMA values are not divided and averaged as calibration;
- component outage does not change denominator;
- full and degraded bundle versions cannot share thresholds;
- calibration split and test split sources do not overlap;
- probability field absent when calibrator validation absent.

### 17.4. Decision tests

- `4/4` locked positives not terminally rejected;
- `routecommunity/1342` fails old anchor path but passes acquisition/selection regression;
- `arkhlikbez/65432` detects scorer failure even when anchor-only test is used;
- `figarotravel/7491` activates mixed/diverse semantics;
- strong top frame with no confirmation → review, not unsafe auto-pass;
- complete consensus all-weak case may reject;
- incomplete acquisition cannot reject all weak;
- safety/rights hard blocker overrides quality accept;
- compliance no-spend occurs before media/model calls.

### 17.5. State/idempotency tests

- same idempotency key produces one materialized decision;
- new contract version creates new row;
- old quality tombstone does not block new version backfill;
- rollback switches active version without deleting rows;
- source image-average cannot create terminal exclusion;
- cached frames/model outputs reused only under matching versions.

### 17.6. Shadow/evaluation tests

- old/new decisions emitted for same manifest;
- all disagreement categories counted;
- sampling weights applied to enriched set metrics;
- source-disjoint and near-duplicate leakage checks fail CI on violation;
- confidence intervals reproducible from frozen manifest;
- compute counters sum to measured stage timings.

---

## 18. Immediate safe guardrails

Эти изменения допустимы до offline calibration, потому что они не auto-accept спорные материалы и не ослабляют safety/compliance.

1. **Album guard:** если source metadata показывает несколько attachments/grouped media, а scored count равен одному, `overall_media_score_below_threshold` не может быть terminal; status → `NEEDS_VISUAL_REVIEW`/`ACQUISITION_RETRY`.
2. **No silent missingness:** required model unavailable → retry/review.
3. **Disable new source quality exclusions:** запретить `exclude_low_image_quality` по raw mean.
4. **Version fields:** записывать active scoring/model/prompt/preprocessing versions и component statuses.
5. **Quality tombstone scoping:** старые score rejects остаются audit history, но не блокируют новую version.
6. **Locked regressions in CI:** четыре positive cases — обязательные non-terminal asserts.
7. **Safety/compliance unchanged:** rescue guardrail переводит только в review, а не bypass hard gates.
8. **Backfill exact image-only rejects:** только versioned, idempotent, без повторного spend при cache hit.

### Что запрещено до calibration

- снижать `0.66` и считать это исправлением;
- вводить новые manual weights;
- auto-accept по raw max одного model;
- называть CLIP/LAION/NIMA output probability;
- auto-reject при partial acquisition;
- запускать degraded scorer под full-bundle threshold;
- делать source deny по среднему model score;
- использовать compliance-blocked cases как negative quality labels.

---

## 19. Stop/go criteria

### STOP

- любой locked positive снова terminally rejected;
- любой locked unsafe/rights case auto-accepted;
- один frame снова выдаётся за complete album;
- required component missing, но overall пересчитан по меньшему знаменателю;
- old/new code paths используют разные thresholds;
- holdout использован для prompt/threshold tuning;
- source-disjoint или duplicate leakage обнаружены;
- review/GPU budgets не определены;
- active decision version не записывается в publication row;
- quality backfill затрагивает compliance/local/source rejects.

### GO TO SHADOW

- acquisition manifest и per-frame rows стабильны;
- locked acquisition/scoring/decision tests разделены и проходят;
- safety/compliance gates не изменены;
- source quality deny отключён;
- cost instrumentation работает;
- model/prompt/preprocessing/calibrator versions фиксированы.

### GO TO CANARY

- frozen holdout gates выполнены;
- all `old-reject/new-accept` shadow cases adjudicated;
- no unsafe false pass;
- best-frame capture и terminal recall targets достигнуты с CI;
- review and compute budgets соблюдены;
- rollback проверен end-to-end.

---

## 20. Недостающие доказательства

До final implementation sign-off нужно получить:

1. Полный blind label для оставшихся 14 current image-only rejects.
2. Per-frame media manifests и human-best indices для всех 47 actual-scored posts.
3. Must-reject safety/rights set достаточного размера.
4. Distribution production album sizes и platform mix.
5. Actual Kaggle timings по каждой model stage и batch size.
6. Operator review capacity: posts/day и допустимый SLA.
7. Exact publication behavior: публикуется один selected frame или весь album; от этого зависит safety coverage.
8. Точные schema/table writers и transaction semantics для versioned rows.
9. Текущий model IDs, weights hashes, prompts, preprocessing и outage frequency.
10. Source grouping rules для reposts/cross-platform duplicates.

Отсутствие этих данных не мешает внедрить immediate guardrails, но блокирует доказательное изменение auto-accept/auto-reject thresholds.

---

## 21. Краткий implementation checklist

```text
[ ] Preserve exact source no-spend compliance gate
[ ] Add plural Telegram/VK acquisition and media manifest
[ ] Add acquisition_complete/partial/retry states
[ ] Add per-frame rows and identity/dedupe
[ ] Add diversity-aware selector and best-frame capture evaluation
[ ] Remove CLIP prompt mass from hard decision
[ ] Remove arithmetic mean of available components
[ ] Add fixed model bundle and missingness contract
[ ] Add separate safety/rights decision
[ ] Add post-level calibrated selective classifier
[ ] Add versioned statuses/reason codes/idempotency
[ ] Remove duplicated CandidateReport/orchestrator thresholds
[ ] Disable raw-score source exclusion
[ ] Build regression/development/confirmation datasets separately
[ ] Run source-disjoint + temporal validation
[ ] Run shadow, adjudicate disagreements, then canary
[ ] Backfill old image-only rejects under new version
[ ] Verify rollback without destructive cleanup
```

---

## 22. Источники и методологические опоры

### Project evidence

- [Review ветки и live evidence](https://github.com/onedayonemasterpiece/events-bot-new/blob/integration/region-talk-image-compliance-review-20260714/docs/features/region-talk-channel/image-scoring-false-negative-review.md)
- [Целевая ветка](https://github.com/onedayonemasterpiece/events-bot-new/tree/integration/region-talk-image-compliance-review-20260714)

### Primary research

- Radford et al., **Learning Transferable Visual Models From Natural Language Supervision (CLIP)**, 2021: https://arxiv.org/abs/2103.00020
- Talebi & Milanfar, **NIMA: Neural Image Assessment**, 2017: https://arxiv.org/abs/1709.05424
- Guo et al., **On Calibration of Modern Neural Networks**, 2017: https://arxiv.org/abs/1706.04599
- Ilse, Tomczak & Welling, **Attention-based Deep Multiple Instance Learning**, 2018: https://arxiv.org/abs/1802.04712
- Bates et al., **Distribution-Free, Risk-Controlling Prediction Sets**, 2021: https://arxiv.org/abs/2101.02703

Эти работы поддерживают три принципа документа: raw model confidence не следует автоматически считать calibrated probability; album/post естественно моделируется как bag of instances; abstention/review может использоваться для явного контроля decision risk.
