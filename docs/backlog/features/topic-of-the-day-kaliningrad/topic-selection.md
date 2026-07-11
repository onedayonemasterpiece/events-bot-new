# Поиск темы, роли авторов, метрики и антиповторы

## 1. Временное окно и eligible corpus

Канонический `source_day` считается в `Europe/Kaliningrad`:

```text
[D-1 00:00:00, D 00:00:00)
```

В корпус входят оригинальные посты, репосты и видеопубликации активных локальных источников. Перед анализом для каждой строки обязательны:

- `source_key`, `ownership_group`, platform post id и canonical URL;
- время публикации и время наблюдения;
- полный доступный текст с сохранёнными rich-text links и media credits;
- список media descriptors;
- видимые метрики и признаки их доступности;
- `text_hash`, признаки forward/repost и original-source link.

Официальные ведомства, муниципальные/региональные учреждения и организаторы входят в eligible corpus наравне с блогерами и пабликами и могут дать теме один голос. Их статус может усиливать роль `primary_source` или фактическую проверку, но не даёт дополнительных голосов и не отменяет ownership/repost dedupe.

Пустой/недоступный показатель записывается как `null + unavailable_reason`, а не как ноль.

Run вычисляет:

- `sources_expected` — активный каталог на cutoff;
- `sources_succeeded`, `sources_empty`, `sources_failed`;
- `posts_total`, `posts_eligible`;
- coverage по числу источников и ownership-групп.

Если source coverage ниже калиброванного порога, можно построить shadow report, но нельзя автоматически утверждать, что найдена тема всего дня.

## 2. Схлопывание дублей до голосования

Последовательно применяются:

1. exact platform id / canonical URL;
2. normalized `text_hash`;
3. forward/repost attribution;
4. pHash для одинакового визуала;
5. near-duplicate recall по vectors;
6. LLM pair verifier для неоднозначных top pairs.

Несколько сообщений одного источника об одном сюжете образуют `source_topic_contribution`. Несколько каналов одного владельца или зеркала образуют одну `ownership_group`. Для cluster support такая группа даёт не больше одного голоса; остальные посты сохраняются как evidence и могут дать лучшие факты/медиа.

## 3. Два embedding-представления

Для каждого eligible document на immutable corpus считаются:

- `intfloat/multilingual-e5-base`;
- `BAAI/bge-m3` dense representation; sparse/lexical output BGE можно добавить после отдельной оценки.

Обязательны `model_id`, revision/checksum, input policy, truncation flag, `text_hash` и document version. E5 и BGE scores нельзя просто усреднять: распределения моделей различаются. Каждый score сначала калибруется на локальном размеченном corpus либо переводится в neighbor rank/percentile.

Fusion v0:

```text
accepted neighbor edge =
  dual_model_consensus
  OR (one_model_very_high AND entity/time overlap)
  OR (one_model_high AND lexical overlap AND LLM pair approval)
```

`model_disagreement` остаётся audit signal. Clustering не стартует при stale `text_hash` или неполной паре, кроме явно помеченного manual degraded run.

Технические основания:

- multilingual E5 заявлен как general-purpose representation для retrieval/clustering: <https://arxiv.org/abs/2402.05672>;
- BGE-M3 даёт multilingual dense/sparse/multi-vector representations и длинный context: <https://arxiv.org/abs/2402.03216>;
- HDBSCAN умеет оставлять noise и искать кластеры разной плотности, но его пригодность для небольшого дневного corpus надо валидировать: <https://scikit-learn.org/stable/modules/clustering.html#hdbscan>.

## 4. Как решается «о чём говорило большинство»

Задача решаема как **source-aware clustering + ranking**, но буквальное `>50% всех источников` не является хорошим условием: в обычный день региональные авторы пишут о многих независимых темах.

### Baseline v0

1. Пост — вершина similarity graph.
2. Принятый fused neighbor — ребро с evidence по обеим моделям.
3. Слабые bridge edges, которые склеивают два разных события через общие слова, удаляются.
4. Graph community/agglomerative clustering строит candidate clusters; HDBSCAN идёт как shadow comparator.
5. LLM получает только top clusters и выполняет `split|merge|accept|reject`, сверяя сущности, место, дату/период, действие и центральное утверждение.
6. После схлопывания ownership-групп считается независимая поддержка темы.

### Topic fingerprint

LLM возвращает строгое JSON-представление:

```json
{
  "topic_type": "event|decision|incident|phenomenon|public_discussion|other",
  "canonical_subject": "...",
  "action_or_change": "...",
  "entities": ["..."],
  "places": ["..."],
  "time_scope": "...",
  "confirmed_facts": [{"fact": "...", "evidence_post_ids": ["..."]}],
  "attributed_claims": [{"claim": "...", "source_post_id": "..."}],
  "conflicts": [{"claim_a": "...", "claim_b": "...", "post_ids": ["..."]}],
  "keywords": ["..."],
  "cluster_decision": "accept|split|merge|reject",
  "confidence": 0.0
}
```

Deterministic code не переопределяет смысл этого fingerprint; оно валидирует schema, ссылки, даты, counts и evidence coverage.

## 5. Выбор одной темы

Сначала hard gates:

- не меньше калиброванного числа независимых ownership-групп (для probe стартовая гипотеза — `3`, не production constant);
- cluster coherence и LLM confidence выше порога;
- ни одна группа не доминирует evidence;
- факты достаточно подтверждены или аккуратно атрибутированы;
- пройден anti-repeat gate;
- source coverage позволяет редакционную формулировку;
- нет safety/legal blocker.

Затем ранжирование top clusters:

```text
topic_score =
  0.35 * independent_source_support
+ 0.15 * semantic_coherence
+ 0.10 * cross_source_fact_coverage
+ 0.10 * normalized_engagement
+ 0.10 * media_and_video_value
+ 0.05 * discussion_signal
+ 0.05 * recency
+ 0.10 * novelty_or_material_development
- dominance_penalty
- conflict_uncertainty_penalty
- repetition_penalty
```

Веса — initial hypothesis для shadow eval, не неизменный product contract. Нельзя искусственно добавлять посты к кластеру ради прохождения минимума.

Публичная формулировка зависит от силы результата:

- strong support: «Тема дня»;
- plurality, но не большинство: «Чаще других вчера обсуждали…»;
- insufficient: `no_topic`, без автопубликации.

## 6. Метрики постов

### Что собираем

На первичном fetch и ещё раз для finalist posts перед публикацией:

```text
views_count
likes_or_reactions_count
reposts_or_forwards_count
comments_count
metrics_observed_at
metrics_age_hours
metric_availability / unavailable_reason
```

Telegram reactions не следует без оговорки называть лайками. Forward count, linked-discussion comments и VK counters могут быть недоступны в конкретном acquisition mode; это должно быть явно видно.

### Как сравниваем

Для каждого доступного показателя нужен baseline того же источника/platform и близкого age bucket. Базовый normalized signal:

```text
metric_lift = clamp(
  log1p(value) - log1p(source_age_bucket_median),
  lower=-2,
  upper=3
)
```

Если per-source sample мал, fallback идёт на source-size cohort/platform, но с меньшей confidence. Итоговый `engagement_normalized_score` ограничивается, чтобы один вирусный пост не победил тему, поддержанную многими независимыми авторами.

Raw counts остаются в evidence report и могут попасть в подпись «эту публикацию обсуждали активнее других», но только если сравниваемая метрика доступна у достаточной доли finalist posts.

## 7. Роли авторов внутри темы

Роль назначается **опционально** и только с evidence. Один источник может получить несколько ролей в data pack, но публичный текст ограничивает повторы и выбирает наиболее различимые вклад/авторов.

| Role id | Как оценивается | Ограничение формулировки |
|---|---|---|
| `primary_source` | первичный документ, организатор, очевидец с проверяемым основанием | не путать с первым найденным репостом |
| `most_factual` | число уникальных grounded facts, охват вопросов кто/что/где/когда/почему, ссылки на первичные данные | факты должны иметь evidence ids |
| `best_explainer` | ясность причин/последствий и контекста | безопаснее, чем «самый компетентный» |
| `most_professional` | подтверждённая релевантная экспертиза автора/организации + качество разбора | при неизвестной экспертизе роль запрещена |
| `most_constructive` | конкретные решения, варианты действий, практические последствия | не равно просто позитивному тону |
| `most_emotional` | выраженность личной реакции, образность, intensity | не выдавать эмоцию за факт |
| `most_detailed` | полнота и глубина без повторов | длина текста сама по себе не достаточна |
| `best_photo` | лучший calibrated media score среди допустимых к reuse | обязательно указать конкретный media id |
| `best_video` | доступное содержательное видео, technical/safety pass | thumbnail не доказывает качество ролика |
| `most_media_rich` | число разнообразных качественных media после dedupe | альбом дублей не выигрывает |
| `most_discussed` | normalized comments при достаточном coverage | если comments недоступны — роль пропускается |
| `most_popular` | capped normalized multi-metric lift | не означает «самый достоверный» |
| `practical_guide` | адреса, расписание, инструкции, полезные действия | данные проверяются на актуальность |
| `historical_context` | проверяемый исторический/справочный контекст | спорные тезисы атрибутируются |
| `alternative_view` | содержательная отличающаяся позиция | не усиливать трэш ради баланса |

Для каждой роли LLM возвращает `candidate_post_id`, `score`, `confidence`, `evidence_fact_ids`, `reason_short`. Deterministic roles (`best_video`, raw media count, metric leader) перепроверяются кодом. Победитель публикуется только при достаточной confidence и заметном margin; иначе категория опускается.

## 8. Сводка фактов и writer cascade

Writer получает не сырой безграничный corpus, а verified topic pack:

1. **Cluster verifier** — границы темы и независимые источники.
2. **Fact extractor** — grounded facts, attributed claims, conflicts.
3. **Role judge** — категории с evidence.
4. **Draft writer** — краткий Telegram/VK текст.
5. **Critic** — проверяет каждый факт, ссылку, превосходную степень, повтор автора и соответствие anti-repeat решению.

Факт из одного источника формулируется как «по данным/словам X». Факт без evidence id удаляется. Расхождения не усредняются и не скрываются: либо кратко указываются, либо блокируют автопубликацию.

## 9. Антиповтор / published-topic memory

Полное сканирование истории нового Telegram-канала делается при bootstrap и затем как reconciliation. Каноническая память — publication ledger, а не повторный full scan каждый день.

Для уже опубликованного выпуска хранятся:

- platform message/post ids и canonical URLs;
- title/summary и normalized text hash;
- topic fingerprint: entities, places, action/change, time scope, key facts;
- member source URLs/hashes;
- E5 и BGE centroid либо ссылка на компактный vector blob;
- publication date и policy/model versions.

Проверка кандидата:

1. exact URL/text/media duplicate;
2. top-K E5 history neighbors;
3. top-K BGE history neighbors;
4. entity/place/time overlap;
5. LLM `same_topic` + `material_newness` decision.

```json
{
  "same_topic": true,
  "material_newness": "none|minor|substantial",
  "new_facts": ["..."],
  "new_phase_or_outcome": "...",
  "decision": "block_repeat|allow_update|needs_review",
  "reason": "..."
}
```

Повтор разрешается только при `substantial`: новый результат, решение, стадия, дата/выпуск, последствия или существенно новые подтверждённые факты. Новый набор авторов при том же содержании сам по себе не делает тему новой.

Это новый, пока не проверенный контур. До golden-set нельзя превращать similarity threshold в безусловный блокер без LLM/human review.
