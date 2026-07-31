# Antigravity: evidence-first исследование фестивалей

Статус: `probe contract`, не production default.

Этот контракт нужен для исследования актуального выпуска фестиваля через
`antigravity-preview-05-2026`. Он заменяет монолитный запрос «найди источники,
склей всё и сам оцени уверенность» на маленькие стадии с независимой
проверкой доказательств.

Он заменяет только старый монолитный **Antigravity interaction contract**, а
не существующий Kaggle + Gemma Universal Festival Parser. До acceptance этот
prompt pack работает collect-only shadow/canary; после acceptance Antigravity
может стать primary для non-social web-групп, а Kaggle+Gemma сохраняется как
проверяемый hot standby/fallback. Cross-lane reconciliation выполняется
host-side; ни одна модель не пишет canonical Festival напрямую.

Канонические оси классификации, programme profiles, item dispositions и
выходной `festival-edition-v2` определены в
[`../features/festivals/data-model-v2.md`](../features/festivals/data-model-v2.md).
Этот документ задаёт prompt/evidence protocol и не вводит параллельную
таксономию.

## Почему одного усиленного prompt недостаточно

В монолитном запросе один и тот же агент одновременно:

1. ищет страницы;
2. решает, к какому году они относятся;
3. сопоставляет события;
4. выбирает URL;
5. пишет итог;
6. оценивает собственную уверенность.

Из-за общего контекста правдоподобная деталь из старого выпуска легко
переезжает в текущий. Требование `source_ids` не предотвращает ошибку: модель
может привязать уже придуманное значение к релевантной странице. Поле
`confidence` также не является проверкой, если его выставляет тот же вызов,
который подготовил результат.

Правильная граница — не «более строгий единый prompt», а:

```text
Antigravity discovery
-> source ledger
-> per-source edition/role review
-> deterministic contradiction floor
-> per-source claim extraction
-> deterministic quote/reference validation
-> entity/event reconciliation
-> independent skeptic
-> conditional adjudicator
-> deterministic final gate and confidence
```

Это логические стадии, а не требование делать отдельный Antigravity interaction
на каждую стадию или страницу.

## Quota-aware topology

Внутри одного запланированного Antigravity lane default — primary research и
независимый узкий counter-evidence check; третий Antigravity-вызов нужен только
при доказанном конфликте:

```text
Call A — primary researcher, fresh environment
  immediate state/source checkpoints
  staged discovery -> source ledger -> claims -> candidate A

Call B — independent skeptic, another fresh environment
  тот же target/seed, но не получает candidate A
  one search query, <= 4 pages
  независимо классифицирует critical fields, programme profile и items,
  затем пытается найти опровержения
  -> taxonomy/item ledger B и counter-evidence, без второго полного candidate

Local deterministic comparison
  quote/source/year/URL validation
  strict agreement on critical fields?

yes -> final result from supported intersection
no  -> Call C — Antigravity adjudicator
       получает compact claim diff A/B и короткие exact quotes
       не получает полный raw candidate/pages
       не имеет search/fetch/network tools
       -> conflicts or evidence-backed final result
```

Таким образом:

- обычный расход — **2 Antigravity RPD на фестиваль**;
- hard cap — **3 Antigravity RPD на фестиваль**;
- при `100 RPD` это до 33 полностью спорных или до 50 обычных исследований в
  сутки, что выше текущего объёма фестивальной очереди;
- Call B запускается в свежем environment и не видит вывод Call A, чтобы
  контроль не превратился в согласие с уже показанным ответом;
- Call B не повторяет полный rich candidate/reconciliation: один узкий search
  query, не более четырёх страниц, немедленный source checkpoint после каждой
  fetch, но programme profile и critical item dispositions он определяет
  независимо, иначе отсутствие записи ошибочно выглядит как согласие;
- Call C запускается только если A и B дают две локально валидные конфликтующие
  alternatives по critical fields или edition/item identity; schema failure
  идёт в technical fallback, а низкое покрытие — сразу в review, не в C;
- Call C получает только conflict packet: target, спорные values, claim ids,
  короткие exact quotes и source hashes. Полные HTML/raw candidate и
  открываемые URL запрещены как token-amplification;
- `status=incomplete` не создаёт автоматический четвёртый запрос: schema-valid
  mandatory checkpoints можно восстановить локально; без них Antigravity lane
  является technical failure и маршрутизируется в healthy standby/review, а C
  не превращается в свободный recovery researcher;
- все deterministic checks выполняются без provider requests;
- вызовы идут последовательно через shared limiter. `100000 TPM` не позволяет
  бездумно запускать три длинных interaction одновременно, даже если RPD
  остаётся.

Полный logical stage split выполняет Call A. Calls B/C имеют отдельные
одноцелевые contracts и checkpoint-файлы в `/workspace`; они не повторяют все
стадии и не означают отдельный HTTP-вызов на каждую страницу.

Семантика выпуска, типа страницы и тождества событий остаётся LLM-first.
Детерминированные проверки не угадывают смысл страницы: они только запрещают
использовать отсутствующую цитату, отвергнутый источник, несовместимый год или
не тот класс билетной ссылки.

## Общие правила

- Все reusable prompt'ы domain-generic: названия, даты и артисты из конкретного
  probe не становятся few-shot примерами.
- Один логический extraction job работает только с одним источником; несколько
  таких jobs последовательно исполняются внутри одного Antigravity interaction.
- Reconciler не видит web и raw-страницы: только уже проверенный claim ledger.
- Rejected/ambiguous source не может подтверждать финальное поле.
- Любой значимый scalar в результате имеет `claim_ids`.
- Модель не выставляет итоговый `confidence`.
- При недостатке доказательств результат — `needs_review`, а не догадка.
- Промежуточные и финальные файлы сохраняются под `/workspace/...`, не `/tmp`.

## Stage 1. Discovery и source ledger — Call A

На этой стадии запрещено составлять программу или итоговую карточку. Агент
только находит ограниченный набор страниц и сохраняет наблюдаемые признаки.
В полном Call A этот блок является первым checkpoint, после которого тот же
interaction переходит к source-local stages 2–4. Отдельный ответ
`ledger_saved` используется только в диагностическом `discovery_only` режиме.

### Prompt

```text
Ты выполняешь только DISCOVERY для исследования выпуска фестиваля.
Ты НЕ составляешь итоговые факты, НЕ объединяешь сведения разных страниц и
НЕ оцениваешь итоговую уверенность.

TARGET:
{{target_json}}

Задача:
1. Найди не более {{max_sources}} релевантных публичных страниц.
2. Приоритет: официальный сайт выпуска/организатора; прямые страницы отдельных
   событий и билетов; затем одно-два независимых СМИ. Агрегаторы — только если
   без них не хватает покрытия.
3. Для каждой страницы сохрани final resolved URL и наблюдаемые сигналы
   выпуска: точные цитаты с годом, датами, названием выпуска, программой и
   участниками. Не делай вывод о соответствии target edition.
4. Не считай годом страницы дату загрузки, copyright в footer, текущий год
   браузера или год в URL без подтверждения в основном содержимом.
5. Не считай билетную страницу ссылкой на отдельное событие, пока на самой
   странице не видны его название и дата; только зафиксируй наблюдаемые поля.
6. Остановись, когда найдены:
   - одна наиболее прямая официальная страница текущей программы;
   - прямые страницы билетов для найденных событий;
   - не более двух независимых cross-check источников;
   либо достигнут лимит {{max_fetches}} загрузок.
7. Сохрани UTF-8 JSON в
   /workspace/festival_research/source_ledger.json и программно проверь
   json.loads. Если execution_mode=discovery_only, верни только краткий JSON
   {"status":"ledger_saved","path":"...","source_count":N}. Если
   execution_mode=full_pipeline, не завершай interaction: переходи к
   source-local stages 2–4, используя сохранённые файлы как immutable input.

Схема source_ledger.json:
{
  "schema_version": "festival-source-ledger-v2",
  "target": {
    "name_hint": "string",
    "edition_year": 0,
    "seed_urls": ["string"]
  },
  "sources": [{
    "source_id": "S001",
    "requested_url": "string",
    "resolved_url": "string",
    "canonical_url": "string|null",
    "page_title": "string|null",
    "publisher_visible": "string|null",
    "published_at_visible": "string|null",
    "retrieved_at_utc": "ISO-8601",
    "content_path": "/workspace/festival_research/sources/S001.txt",
    "content_sha256": "lowercase hex",
    "normalizer_version": "host-pinned string",
    "observed": {
      "explicit_year_quotes": [{"value": 0, "quote": "verbatim", "quote_start": 0, "quote_end": 0}],
      "date_quotes": [{"value": "verbatim", "quote": "verbatim", "quote_start": 0, "quote_end": 0}],
      "edition_label_quotes": [{"value": "verbatim", "quote": "verbatim", "quote_start": 0, "quote_end": 0}],
      "event_quotes": [{"value": "verbatim", "quote": "verbatim", "quote_start": 0, "quote_end": 0}],
      "ticket_identity_quotes": [{"value": "verbatim", "quote": "verbatim", "quote_start": 0, "quote_end": 0}]
    },
    "fetch_status": "ok|partial|blocked",
    "fetch_notes": "string|null"
  }],
  "discovery_notes": ["string"]
}

Инварианты:
- quote — дословная непрерывная подстрока сохранённого нормализованного текста;
- `quote_start:quote_end` воспроизводит quote в тексте с указанным
  `content_sha256`; normalizer/version задаёт host и не меняется внутри run;
- пустой/blocked контент не подтверждает ни одного факта;
- разные resolved URL получают разные source_id;
- не добавляй edition_status, officiality, confidence или финальные поля.
```

Рекомендуемые границы одного probe:

- `max_sources=6`;
- `max_fetches=8`;
- один search round без свободного retry;
- целевой `agent_config.max_total_tokens=18000..20000`, а reservation shared
  limiter остаётся консервативным `45000..50000`;
- никаких «до 15 источников» и полного финального JSON в этом interaction.

`max_total_tokens` у Antigravity является best-effort, поэтому внешний consumer
всё равно обязан учитывать фактический `usage.total_tokens`, принимать
`status=incomplete` как штатный budget outcome и скачивать snapshot до
continuation.

## Stage 2. Edition и source role — source-local job внутри interaction

Логический job получает target и ровно один source record с его текстом.
Antigravity выполняет такие jobs последовательно и пишет отдельный review для
каждого `source_id`; quote validator проверяет ответ только против текста этого
source.

### Prompt

```text
Ты классифицируешь РОВНО ОДИН источник относительно target edition.
Не извлекай итоговую программу и не сравнивай этот источник с другими.

TARGET:
{{target_json}}

SOURCE METADATA:
{{source_json}}

SOURCE TEXT:
<source_text>
{{source_text}}
</source_text>

Верни только JSON:
{
  "source_id": "S001",
  "edition_status": "accepted|rejected|ambiguous",
  "edition_year": 0|null,
  "source_role":
    "official_home|official_program|official_organizer|official_event|ticket_single_event|ticket_subscription|festival_pass|registration|regional_tourism|media|aggregator|document_pdf|document_image|machine_feed|social|other",
  "role_evidence": {
    "quote": "verbatim",
    "quote_start": 0,
    "quote_end": 0
  },
  "decision_evidence": [{
    "quote": "verbatim",
    "quote_start": 0,
    "quote_end": 0
  }],
  "reasons": [
    "explicit_target_year|exact_target_dates|edition_label_match|explicit_other_year|date_set_mismatch|program_identity_mismatch|no_edition_evidence|ticket_identity_match|ticket_scope_is_subscription|other"
  ]
}

Правила:
- accepted требует положительного evidence внутри SOURCE TEXT именно для target
  edition; дата retrieval, footer и URL сами по себе не подходят;
- явный другой год либо несовместимый набор дат/программы => rejected;
- отсутствие года при недостаточной идентичности => ambiguous, не likely;
- ticket_single_event допустим, только если сама страница однозначно называет
  одно событие и его дату/время;
- абонемент, подписка, пакет или общий проходной билет — не
  ticket_single_event;
- каждый evidence offset range должен воспроизводить дословную цитату в SOURCE
  TEXT; при отсутствии цитаты используй null/ambiguous.
```

### Deterministic floor

После LLM-классификации caller обязан:

1. проверить дословное наличие всех цитат в сохранённом source text;
2. перевести ответ в `rejected`, если span, относящийся именно к извлекаемой
   программе/выпуску, однозначно задаёт другой год и не содержит совместимого
   target-edition evidence; отдельный исторический раздел страницы сам по себе
   не отклоняет текущую страницу;
3. перевести ответ минимум в `ambiguous`, если нет ни одного валидного
   `decision_evidence`;
4. запретить `ticket_single_event`, если нет доказательств события и его даты;
5. считать все `fetch_status != ok` непригодными для критических полей.

Неоднозначное семантическое противоречие floor не разрешает сам: оно отправляет
источник в `ambiguous`.

## Stage 3. Atomic claim extraction — source-local job внутри interaction

### Prompt

```text
Извлеки атомарные утверждения ТОЛЬКО из одного accepted source.
Не дополняй общеизвестными сведениями, не нормализуй название редакционно и
не объединяй разные события.

TARGET:
{{target_json}}

SOURCE REVIEW:
{{source_review_json}}

SOURCE TEXT:
<source_text>
{{source_text}}
</source_text>

Верни только JSON:
{
  "source_id": "S001",
  "subjects": [{
    "local_subject_id": "festival|event:1|venue:1|organizer:1",
    "subject_kind": "festival|event|venue|organizer"
  }],
  "claims": [{
    "local_subject_id": "event:1",
    "field":
      "title|edition_label|description_fact|start_date|end_date|date|time_start|timezone|venue_name|venue_address|city|organizer_name|organizer_role|participant_name|participant_role|ticket_url|price_text|registration_url|canonical_url",
    "raw_value": "JSON scalar",
    "normalized_value": "JSON scalar|null",
    "normalization": "none|trim|iso_date|iso_time|canonical_url",
    "verbatim_quote": "non-empty verbatim substring",
    "quote_start": 0,
    "quote_end": 0,
    "normalizer_version": "host-pinned string",
    "context_quote": "verbatim|null"
  }],
  "unresolved": ["string"]
}

Правила:
- один claim = одно поле одного local subject;
- никакого «IX», «международный», статуса организатора или другого модификатора,
  если он буквально не поддержан quote этого source;
- normalized_value может отличаться от raw_value только перечисленной
  механической нормализацией;
- разные даты/время/названия создают разные local event subjects;
- ticket_url извлекается как ticket_url только при
  source_role=ticket_single_event; для subscription/pass используй
  registration_url и сохрани scope в unresolved;
- quote должен буквально содержать raw_value либо однозначное исходное
  выражение, из которого получена ISO-нормализация; offsets должны точно
  воспроизводить quote в hash-bound normalized source text;
- если факт подразумевается, но не написан, не извлекай его.
```

Caller детерминированно присваивает `claim_id`, проверяет offsets/цитаты, URL,
hash/normalizer version и разрешённые нормализации. Claim из
rejected/ambiguous source не попадает в accepted ledger.

## Stage 3b. Programme inventory и item disposition

После source-local claims Call A сохраняет source-local inventory, не
склеивая одинаково выглядящие пункты между источниками:

```json
{
  "source_id": "S001",
  "items": [{
    "local_item_id": "item:1",
    "identity_claim_ids": ["C..."],
    "logistics_claim_ids": ["C..."],
    "disposition":
      "link_existing_event|create_event_candidate|schedule_slot|programme_only|continuous_activity|service_information|reject",
    "decision_id": "D...",
    "reason_codes": ["host-vocabulary value"],
    "alternatives_rejected": ["create_event_candidate"]
  }]
}
```

Disposition выбирает LLM по evidence из одного источника. Детерминированный
caller только проверяет enum, ссылки и полноту inventory. Межисточниковый
reconciliation не имеет права потерять source-local item: каждый элемент
union A/B должен быть связан, сохранён, отвергнут с evidence или отмечен
`unresolved`.

## Stage 4. Entity/event reconciliation — LLM над claim ledger

Reconciler не получает web, raw pages и rejected claims. Он только группирует
local subjects и выбирает уже существующие значения.

### Prompt

```text
Сопоставь сущности по ACCEPTED_CLAIMS. Не создавай новых фактов и строковых
значений. Любое выбранное значение должно быть ТОЧНО одним из normalized_value
(или raw_value при normalized_value=null) перечисленных claim_id.

TARGET:
{{target_json}}

ACCEPTED_CLAIMS:
{{accepted_claims_json}}

Верни только JSON:
{
  "festival_cluster": {
    "member_subjects": ["S001:festival"],
    "fields": {
      "field_name": {
        "value": "existing claim value|null",
        "claim_ids": ["C..."],
        "status": "supported|conflict|unknown"
      }
    }
  },
  "classification": {
    "taxonomy_version": "host-pinned string",
    "taxonomy_sha256": "host-pinned lowercase hex",
    "identity_kind": {
      "value": "controlled value|unknown",
      "claim_ids": ["C..."],
      "decision_ids": ["D..."],
      "status": "supported|conflict|unknown"
    },
    "programme_profile": {
      "value": "controlled value|unknown",
      "claim_ids": ["C..."],
      "decision_ids": ["D..."],
      "status": "supported|conflict|unknown"
    },
    "primary_topic_id": {
      "value": "controlled id|null",
      "claim_ids": ["C..."],
      "decision_ids": ["D..."],
      "status": "supported|conflict|unknown"
    },
    "secondary_topic_ids": {
      "values": ["controlled id"],
      "claim_ids": ["C..."],
      "decision_ids": ["D..."],
      "status": "supported|conflict|unknown"
    },
    "raw_topic_labels": [{"value": "source value", "claim_ids": ["C..."]}],
    "unmapped_topic_labels": ["source value"],
    "temporal_profile": {"value": "controlled value|unknown", "claim_ids": ["C..."], "decision_ids": ["D..."]},
    "spatial_profile": {"value": "controlled value|unknown", "claim_ids": ["C..."], "decision_ids": ["D..."]},
    "access_profiles": {"values": ["controlled value"], "claim_ids": ["C..."], "decision_ids": ["D..."]},
    "lifecycle_state": {"value": "controlled value|unknown", "claim_ids": ["C..."], "decision_ids": ["D..."]}
  },
  "event_clusters": [{
    "cluster_id": "E001",
    "member_subjects": ["S001:event:1"],
    "identity_claim_ids": ["C..."],
    "fields": {
      "field_name": {
        "value": "existing claim value|null",
        "claim_ids": ["C..."],
        "status": "supported|conflict|unknown"
      }
    }
  }],
  "conflicts": [{
    "scope": "festival|E001",
    "field": "string",
    "alternatives": [{"value": "existing claim value", "claim_ids": ["C..."]}]
  }],
  "decisions": [{
    "decision_id": "D001",
    "decision_kind": "edition_classification_bundle|programme_profile|programme_item_disposition|entity_match",
    "subject_refs": ["S001:festival"],
    "selected_value": "existing controlled scalar/object",
    "alternatives_rejected": [],
    "evidence_claim_ids": ["C..."],
    "reason_codes": ["host-vocabulary value"],
    "status": "supported|conflict|unknown"
  }],
  "unresolved": ["string"]
}

Жёсткие правила:
- не конструируй title из частей разных claims;
- используй минимальное подтверждённое название без добавочных порядковых
  номеров, статусов и эпитетов;
- события можно объединять только при совместимых дате/времени/площадке и
  семантическом тождестве; одно название без логистики недостаточно;
- subscription/pass URL не может стать event.ticket_url;
- при двух несовместимых accepted values ставь value=null, status=conflict и
  заполняй conflicts; не выбирай «более красивый» вариант;
- используй только mounted taxonomy version/hash и её значения; неизвестную
  тематическую метку сохраняй в `unmapped_topic_labels`, не изобретай новый id;
- `programme_profile` выводится из полного accepted item inventory и
  dispositions, а не из тематической категории или желаемого типа страницы;
- каждый `decision_id` разрешается в `decisions`, а его evidence claims
  существуют в accepted ledger;
- unknown лучше догадки.
```

## Stage 5. Independent skeptic и условный adjudicator

### Call B: independent skeptic

Call B получает тот же target и seed URLs, но не candidate/ledger Call A. Его
prompt не повторяет stages 1–4 и не составляет второй полный candidate:

```text
Ты независимый skeptical researcher. Проведи исследование в свежем контексте.
Сначала создай /workspace/.../state.json. Сделай ровно один узкий search query,
открой не более четырёх страниц и после каждой fetch немедленно сохрани source
text. Не исследуй JavaScript/API ticket shell.
Не предполагай, что наиболее подробная или высоко ранжируемая страница относится
к target edition. Для каждого critical field ищи прямую цитату и пытайся найти:
- страницу другого выпуска с похожим названием;
- несовместимые даты или состав;
- различие между single-event ticket и subscription/pass;
- неподтверждённый порядковый номер или статус в названии.

Не угадывай, какой результат получил другой исследователь: он тебе не дан.
Независимо классифицируй identity kind, programme profile и каждый найденный
programme item по mounted taxonomy/disposition vocabulary. Сохраняй sources,
taxonomy_b.json, item_dispositions_b.jsonl и counter-evidence ledger под
/workspace/festival_research_skeptic/. Отсутствующий item/field отмечай
unknown/unresolved: omission не является согласием.
```

После локальной валидации candidate A и counter-evidence B сравниваются по
critical fields. Совпадением считается не похожий текст, а совместимое
нормализованное значение с валидными claims из accepted target-edition sources.

### Call C: adjudicator

Call C — третий Antigravity interaction. Он получает только локально
валидированный compact conflict packet A/B. Он запускается при разрешимом
конфликте, не получает полные raw pages/candidates и не повторяет discovery.
Низкое покрытие без двух доказуемых альтернатив отправляется сразу на ручную
проверку: adjudicator не может восстановить отсутствующее evidence.

### Prompt

```text
Ты evidence adjudicator. У тебя есть bounded conflicts между двумя независимо
полученными результатами. Не выбирай по большинству, подробности или стилю.
Для каждого conflict_id можно выбрать только один уже перечисленный
alternative_id либо unknown/conflict. Нельзя создавать новый value, claim,
source, taxonomy node, programme item или итоговый candidate.

CONFLICT_PACKET:
{{bounded_conflict_packet_json}}

Каждый conflict содержит allowed_alternatives и exact quote packet с локально
валидированными claim/source hashes.

Верни только JSON:
{
  "schema_version": "festival-adjudication-v1",
  "decisions": [{
    "conflict_id": "CF001",
    "choice": "existing alternative_id|unknown|conflict",
    "supporting_claim_ids": ["C..."],
    "reason_code":
      "stronger_target_edition_evidence|direct_item_identity|ticket_scope_match|insufficient_evidence|incompatible_evidence"
  }]
}

Обязательно:
1. вернуть ровно одно решение для каждого входного conflict_id;
2. existing alternative можно выбрать только по его accepted claims target
   edition;
3. supporting_claim_ids должны принадлежать выбранной alternative;
4. недостаточный packet => unknown, несовместимые сильные evidence => conflict;
5. не переносить rejected/ambiguous source через косвенный claim.

У тебя нет Google Search, browser, URL fetch или иных network tools. Не
открывай cited URLs.
```

## Stage 6. Deterministic final gate

Финальный renderer выполняется после локального A/B comparison либо
adjudicator и повторяет критические проверки независимо:

- JSON парсится и соответствует локальной схеме;
- все ссылки `source_id`, `claim_id`, `local_subject_id` существуют;
- каждая цитата найдена в тексте с совпадающим `content_sha256`;
- offsets воспроизводят цитату при совпадающем host-pinned
  `normalizer_version`;
- каждый финальный scalar равен значению одного из его claims;
- все claims финального результата происходят из `accepted` source;
- `claim.edition_year`, если задан, совпадает с target;
- critical fields (`festival.title`, период, event date/time/venue/ticket_url)
  не имеют скрытого конфликта;
- `event.ticket_url` подтверждён claim из `ticket_single_event` и совпадает с
  тем же event cluster;
- для согласованных A/B adjudication не нужен; для каждого спорного результата
  Call C выбрал существующий allowed alternative;
- rejected/ambiguous source никогда не считается cross-check;
- `conflicts=[]` допустим только после сравнения всех accepted claims одного
  поля;
- taxonomy version/hash совпадают с mounted registry, значения входят в
  vocabulary;
- union A/B programme inventories полностью conserved: каждый item сопоставлен,
  сохранён, доказуемо rejected либо явно unresolved;
- A/B согласны по identity kind, programme profile и critical dispositions
  либо Call C выбрал существующий allowed alternative для каждого конфликта;
- любой `unknown|conflict` Call C оставляет revision в `needs_review`.

Любой сбой даёт:

```json
{
  "status": "needs_review",
  "publishable": false,
  "violations": ["machine-readable codes"]
}
```

Gate не редактирует семантические значения и не запускает автоматический
«repair prompt»: после ошибки нужно исправить конкретную upstream-стадию.

## Confidence без self-grading

Модель вообще не возвращает `quality.confidence`. Caller вычисляет confidence
по проверенным claims:

### Для одного поля

- `high`: есть accepted direct/official claim текущего выпуска и нет
  противоречий; либо два действительно независимых accepted источника, один из
  которых direct/official;
- `medium`: согласны два независимых indirect источника, либо есть один
  accepted direct source без cross-check;
- `low`: один indirect claim;
- `unknown`: нет claim или есть неразрешённый конфликт.

`direct/official` считается только из проверенного registry отношений
доменов/организаторов или явного evidence, а не из модельного самоназвания.
Один домен считается одним источником; syndication/copy не создаёт
независимое подтверждение.

### Для результата

Overall confidence — минимум по critical fields, не среднее. При
`adjudicator=needs_review`, любом critical `unknown/conflict`, stale-source
leakage или ticket-role mismatch статус всегда `needs_review`; значение `high`
невозможно.

## Acceptance pack

Тесты используют вымышленные названия и не попадают в prompts как
положительные production-факты.

1. **Stale edition.** Официальный источник target year даёт даты A–C. Другая
   страница без явного target year даёт даты B–D и иной состав, но имеет свежий
   footer. Ожидание: второй source `rejected` или `ambiguous`; ни один его claim
   не попадает в final candidate.
2. **Unsupported ordinal.** Accepted source называет «Фестиваль Северный
   ветер», а media — «X Фестиваль…». Media не подтверждён как target edition.
   Ожидание: итоговое title не содержит `X`; любое добавление ловится как
   `claim_value_mismatch` или `unsupported_title_modifier`.
3. **Subscription vs single ticket.** Одна страница продаёт абонемент на три
   дня, три другие — отдельные события. Ожидание: абонемент остаётся
   `registration_url`/pass; каждый `event.ticket_url` ссылается только на
   matched single-event claim.
4. **Silent conflict.** Два accepted источника дают разные времена одного
   события. Ожидание: `time_start=null`, status=`conflict`, обе альтернативы
   перечислены; `conflicts=[]` блокируется.
5. **Generic redirect.** Seed URL редиректит на ticket shell без названия и
   даты. Ожидание: URL не подтверждает событие и не становится ticket_url.
6. **Same title, different day.** Два события имеют одинаковое название, но
   разные даты. Ожидание: разные event clusters; название само по себе не
   является достаточным identity evidence.
7. **Current page with history.** Страница target edition содержит отдельный
   исторический блок с прошлыми годами. Ожидание: historical claims не
   поддерживают current programme, но валидные target-edition spans не
   отклоняются только из-за присутствия старых лет на той же странице.

## Вывод из первого probe

Первый raw result должен был быть отклонён сразу тремя независимыми gates:

- неподтверждённый модификатор title не равен ни одному accepted claim;
- страница другого выпуска не проходит edition gate;
- URL абонемента не проходит `ticket_single_event` role/identity check.

Поэтому основное улучшение — внешняя проверяемая provenance-цепочка, а не
добавление фразы «не выдумывай» в исходный монолитный prompt.

## Live acceptance: independent check 2026-07-29

Артефакт:
`artifacts/codex/antigravity-double-check-20260729/evaluation_report.md`
(не коммитится).

Три последовательных interaction были проведены через shared limiter:

| Call | Role | Status | Actual tokens | Google Search | Snapshot |
|---|---|---:|---:|---:|---:|
| A | full primary retry | `incomplete` | 78,948 | 4 | 4,096 B |
| B | compact independent skeptic | `incomplete` | 27,198 | 1 | 42,496 B |
| C | cited-source adjudicator | `incomplete` | 44,804 | 0 | 263,168 B |

Итого: `150,950` actual tokens; `0/3` interaction вернули terminal output.
Shared ledger дошёл до `4/90 RPD`, потому что до трёх agent runs один
preflight request получил HTTP 400 из-за неподдерживаемого Gemini API поля
`labels` уже после reservation.

Несмотря на `incomplete`, B/C сохранили raw source/state artifacts, из которых
последующий ручной/local analysis восстановил достаточно evidence. Это не были
валидные source/claim/taxonomy checkpoints и не подтверждает checkpoint
recovery contract. Восстановленный local gate:

1. удалил неподтверждённое `IX Международный`;
2. исключил `sobor39.ru/.../3930/` из evidence 2026 из-за программы
   `29–31 августа` и несовместимого состава против явной current program
   `28–30 августа 2026`;
3. заменил URL абонемента первого дня прямым single-event URL
   `2026-08-28`, связанным с событием на current official page.

Factual acceptance: `3/3` известных ошибок пойманы. Operational acceptance:
**failed** — полный staged pipeline в каждом из 2+1 calls слишком тяжёл и не
доживает до output при best-effort budget. Поэтому канонический runtime выше
использует полный pipeline только в A, узкий counter-evidence collector в B и
compact claim-diff adjudicator без network tools в C. Local renderer может
завершить результат при `status=incomplete` только из schema-valid semantic
checkpoints; raw source/state artifacts требуют review и сами по себе не
считаются результатом.
