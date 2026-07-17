# LLM-first contract: выявление необычных событий

Статус: design / shadow-mode required. Это канонический semantic contract для
сценария typed briefing `unusual_event`. Продуктовая доставка и дайджесты:
[artist-arrivals-and-unusual-events.md](../backlog/features/static-typed-briefing/artist-arrivals-and-unusual-events.md).

## Что именно классифицируем

Цель — найти в массе будущих событий **конкретный отличительный факт**, ради
которого человек остановится и откроет карточку. Это не синоним качества,
популярности, известности, дефицита билетов или рекламного слова «уникальный».

Разделяем два публичных уровня:

1. `unusual_public` — корректно сказать «необычный для нашей афиши формат»:
   есть source-grounded факт и достаточный версионированный baseline;
2. `distinctive_fact_only` — факт любопытный, но статистической редкости не
   доказано. Говорим сам факт без ранжирования: «В воскресенье — День
   клубники», а не «самое необычное событие недели».

«День клубники», цветение маковых полей и прогулка с фонарщиком — только
исторические иллюстрации типов сигналов, переданные пользователем. Они не
считаются текущими событиями и не являются reusable positive few-shot с
названиями. Production prompt использует domain-generic examples.

## Признаки-кандидаты

Semantic extractor рассматривает независимо:

| Dimension | Вопрос | Иллюстративный тип факта |
|---|---|---|
| `format_rarity` | Редка ли механика участия, а не слово в заголовке? | ночная экскурсия с исполняемой ролью |
| `theme_specificity` | Есть ли узкая тема/объект, резко отличающий событие от peer group? | отдельный тематический день продукта/ремесла |
| `place_or_access` | Необычен ли доступ или сочетание формата с местом? | normally closed site, необычный маршрут |
| `temporal_natural_window` | Есть ли короткое сезонное/природное окно, явно указанное источником? | наблюдение сезонного цветения |
| `interaction_mechanic` | Что делает посетитель помимо просмотра/прослушивания? | совместный эксперимент, маршрут, диалог |
| `participant_geography` | Есть ли редкое подтверждённое происхождение/состав участников? | отдельный сигнал; не равен известности |
| `one_off_structure` | Есть ли подтверждённая разовая комбинация программы? | специальная однодневная программа |

Одного высокого dimension недостаточно для публикации «редкое»: требуется
baseline. Но он может дать `distinctive_fact_only`.

## Pipeline

### U0. Eligibility и source bundle

До LLM остаются только canonical active/future events региона. Bundle содержит
source text/OCR с fragment IDs, event fields и provenance, dates, venue,
category/format, organizer, duplicate group, cancellation/status. Past,
cancelled, stale и неразрешённые дубликаты исключаются.

### U1. Semantic signature extraction (маленький LLM-запрос)

Модель не решает «необычно ли», а извлекает факты и цитируемые evidence IDs:

```json
{
  "event_id": "E123",
  "signature": {
    "public_format": ["guided_walk"],
    "activity_mechanics": ["role_led_route"],
    "themes": ["..."],
    "setting_access": ["..."],
    "temporal_natural_condition": null,
    "participant_geography_claims": []
  },
  "candidate_dimensions": [
    {
      "dimension": "interaction_mechanic",
      "fact": "...",
      "evidence_fragment_ids": ["src:4"],
      "explicit_or_inferred": "explicit"
    }
  ],
  "missing_evidence": [],
  "confidence": 0.0
}
```

Правила U1:

- только смысл текущего source bundle, без world-knowledge claims;
- рекламные прилагательные не становятся facts;
- природное/сезонное условие допустимо только при прямом source claim и
  ограниченном периоде; модель не предсказывает цветение/погоду;
- названный артист не становится `participant` без роли;
- uncertainty сохраняется в `missing_evidence`, не заполняется догадкой.

### U2. Baseline packet (детерминированная аналитика, не семантика)

После U1 система считает частоты по версионированной semantic taxonomy:

- rolling catalog window (например 365 и 730 дней);
- season-aware comparison с теми же неделями/месяцами прошлых периодов;
- peers минимум по region + broad category, дополнительно по city/format только
  при достаточном объёме;
- `peer_event_count`, `matching_signature_count`, `distinct_occurrence_days`,
  `distinct_venue_count`, coverage долю, nearest semantic neighbours;
- дедупликация повторов, дат одной программы и перепубликаций до подсчёта.

Vector distance и nearest neighbours — только recall/diagnostic. Они не
доказывают новизну. Если baseline ниже versioned coverage/min-sample gates,
результат `baseline_sufficient=false`.

### U3. Grounded adjudication (отдельный маленький LLM-запрос)

Вход: U1 signature, U2 packet, source fragments и negative controls. Выход:

```json
{
  "decision": "unusual_public|distinctive_fact_only|ordinary|indeterminate",
  "dimension_scores": {
    "format_rarity": 0,
    "theme_specificity": 0,
    "place_or_access": 0,
    "temporal_natural_window": 0,
    "interaction_mechanic": 0,
    "participant_geography": 0,
    "one_off_structure": 0
  },
  "decisive_dimensions": [],
  "evidence_fragment_ids": [],
  "baseline_version": "...",
  "reason_internal": "...",
  "public_fact": "...",
  "public_rarity_claim_allowed": false,
  "confidence": 0.0,
  "expires_at": "..."
}
```

Шкала dimension `0..4` нужна для аудита и калибровки, но сумма не заменяет
LLM decision. `unusual_public` допустим только если:

- есть хотя бы один explicit decisive fact с fragment ID;
- baseline достаточен и действительно поддерживает rarity именно этого
  signature;
- public fact проходит отдельный entailment validator;
- событие остаётся active/future, а decision не истёк.

Если факт есть, а baseline слаб, U3 обязан выбрать `distinctive_fact_only`.

### U4. Writer и deterministic validators

Offline writer получает только approved `public_fact`, event/link token и
разрешённый уровень claim. Он строит 1–3 строки без новых фактов.

Validators fail closed при:

- отсутствии/несовпадении evidence IDs;
- словах `единственный`, `самый`, `впервые`, `уникальный`, если именно этот
  claim не был разрешён;
- подмене «необычный формат» на «лучшее событие»;
- невалидной ссылке, past/cancelled status или истёкшем decision;
- превышении viewport budget.

## Hard blockers / negative controls

Следующие сигналы сами по себе дают `ordinary` или `indeterminate`:

- «уникальный», «невероятный», «эксклюзивный» только в рекламном тексте;
- обычный концерт, спектакль, выставка, лекция или экскурсия без отличительной
  механики;
- знаменитость, большое число комментариев, лайков, продаж или sold-out;
- первое появление в нашем источнике либо отсутствие похожего title;
- редкое слово, опечатка, экзотичное изображение без text/OCR evidence;
- venue rarity, полученная из пустого/ошибочного venue;
- сезонное массовое событие, которое выглядит редким в коротком несезонном
  окне;
- несколько карточек одной программы, ошибочно посчитанные как редкая
  комбинация;
- world knowledge модели без текущего источника.

Negative-control eval обязательно включает: generic concert, standard city
walk, seasonal fair, celebrity tour, marketing-heavy workshop, repeated
festival program, screening/tribute with a famous name и новое событие из
source с короткой историей.

## Public copy policy

Предпочтительный приём — любопытство через предметный факт:

- `unusual_public`: «Такой формат редко появляется в нашей афише: {fact}.»;
- `distinctive_fact_only`: «На выходных — {fact}. Посмотрим?»;
- natural window: «На этой неделе можно {grounded activity}. Условия — у
  организатора.»;
- no safe decision: сценарий не создаётся; generic navigation лучше догадки.

Запрещено: «мы нашли самое необычное», «такого вы ещё не видели», «точно стоит
пойти», приписывание интереса пользователю и скрытое сравнение со «всей сетью».

## Хранимые поля и freshness

Минимум: `event_id`, source revision/hash, U1/U3 prompt+model versions,
signature taxonomy version, baseline version/window/coverage/counts, neighbour
IDs, decision, dimension scores, evidence IDs, public fact, confidence,
`decided_at`, `expires_at`, human override/reason. При изменении источника,
event status, date, distinctive facts или taxonomy decision инвалидируется.

## Evaluation и rollout gate

1. Human-labeled set минимум 200 future/historical events, стратифицированный
   по category, city/oblast, season, organizer и source quality; отдельная
   oversample-часть candidate-rich событий.
2. Два редактора независимо ставят `unusual_public`,
   `distinctive_fact_only`, `ordinary`, `indeterminate` и отмечают decisive
   source spans; disagreement adjudicated.
3. Главный gate — precision `unusual_public >= 0.90`; grounding/entailment
   public facts = 100%; critical unsupported superlative = 0. Recall вторичен,
   abstention считается нормальным.
4. Отдельные slices: природные окна, гастрономия, прогулки/экскурсии,
   фестивальные программы, знаменитости, rural/oblast events, one-word titles,
   duplicate programs.
5. Shadow mode минимум полный каталогный цикл; сравнение с editorial finds,
   false-positive review и drift baseline coverage.
6. Canary показывает только human-approved decisions. Auto-publication
   разрешается после gate на свежей выборке и остаётся kill-switchable по
   model/taxonomy version.
