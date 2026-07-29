# Antigravity: evidence-first исследование фестивалей

Статус: `probe contract`, не production default.

Этот контракт нужен для исследования актуального выпуска фестиваля через
`antigravity-preview-05-2026`. Он заменяет монолитный запрос «найди источники,
склей всё и сам оцени уверенность» на маленькие стадии с независимой
проверкой доказательств.

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
-> adversarial verifier
-> deterministic final gate and confidence
```

Семантика выпуска, типа страницы и тождества событий остаётся LLM-first.
Детерминированные проверки не угадывают смысл страницы: они только запрещают
использовать отсутствующую цитату, отвергнутый источник, несовместимый год или
не тот класс билетной ссылки.

## Общие правила

- Все reusable prompt'ы domain-generic: названия, даты и артисты из конкретного
  probe не становятся few-shot примерами.
- Один LLM-вызов извлекает факты только из одного источника.
- Reconciler не видит web и raw-страницы: только уже проверенный claim ledger.
- Rejected/ambiguous source не может подтверждать финальное поле.
- Любой значимый scalar в результате имеет `claim_ids`.
- Модель не выставляет итоговый `confidence`.
- При недостатке доказательств результат — `needs_review`, а не догадка.
- Промежуточные и финальные файлы сохраняются под `/workspace/...`, не `/tmp`.

## Stage 1. Discovery и source ledger — Antigravity

На этой стадии запрещено составлять программу или итоговую карточку. Агент
только находит ограниченный набор страниц и сохраняет наблюдаемые признаки.

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
   /workspace/festival_research/source_ledger.json, программно проверь
   json.loads и затем верни только краткий JSON:
   {"status":"ledger_saved","path":"...","source_count":N}.

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
    "observed": {
      "explicit_year_quotes": [{"value": 0, "quote": "verbatim"}],
      "date_quotes": [{"value": "verbatim", "quote": "verbatim"}],
      "edition_label_quotes": [{"value": "verbatim", "quote": "verbatim"}],
      "event_quotes": [{"value": "verbatim", "quote": "verbatim"}],
      "ticket_identity_quotes": [{"value": "verbatim", "quote": "verbatim"}]
    },
    "fetch_status": "ok|partial|blocked",
    "fetch_notes": "string|null"
  }],
  "discovery_notes": ["string"]
}

Инварианты:
- quote — дословная непрерывная подстрока сохранённого нормализованного текста;
- пустой/blocked контент не подтверждает ни одного факта;
- разные resolved URL получают разные source_id;
- не добавляй edition_status, officiality, confidence или финальные поля.
```

Рекомендуемые границы одного probe:

- `max_sources=8`;
- `max_fetches=12`;
- один search round и один узкий retry только для blocked official URL;
- `agent_config.max_total_tokens=30000..40000`;
- никаких «до 15 источников» и полного финального JSON в этом interaction.

`max_total_tokens` у Antigravity является best-effort, поэтому внешний consumer
всё равно обязан учитывать фактический `usage.total_tokens`, принимать
`status=incomplete` как штатный budget outcome и скачивать snapshot до
continuation.

## Stage 2. Edition и source role — малый LLM-вызов на один source

Вызов получает target и ровно один source record с его текстом. Он не видит
другие страницы, поэтому не может «подтянуть» более правдоподобную программу
из соседнего выпуска.

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
    "official_program|official_organizer|ticket_single_event|ticket_subscription|festival_pass|media|aggregator|social|other",
  "role_quote": "verbatim|null",
  "decision_quotes": ["verbatim"],
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
- каждая decision_quote и role_quote должна быть дословной подстрокой SOURCE
  TEXT; при отсутствии цитаты используй null/ambiguous.
```

### Deterministic floor

После LLM-классификации caller обязан:

1. проверить дословное наличие всех цитат в сохранённом source text;
2. перевести ответ в `rejected`, если источник содержит однозначный явный год,
   отличный от target, а модель вернула `accepted`;
3. перевести ответ минимум в `ambiguous`, если нет ни одной валидной
   `decision_quote`;
4. запретить `ticket_single_event`, если нет доказательств события и его даты;
5. считать все `fetch_status != ok` непригодными для критических полей.

Неоднозначное семантическое противоречие floor не разрешает сам: оно отправляет
источник в `ambiguous`.

## Stage 3. Atomic claim extraction — малый LLM-вызов на один accepted source

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
  выражение, из которого получена ISO-нормализация;
- если факт подразумевается, но не написан, не извлекай его.
```

Caller детерминированно присваивает `claim_id`, проверяет цитаты, URL и
разрешённые нормализации. Claim из rejected/ambiguous source не попадает в
accepted ledger.

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
- unknown лучше догадки.
```

## Stage 5. Adversarial verifier — отдельный LLM-вызов

Это не self-review в том же контексте. Verifier получает candidate, полный
source review ledger и claims, но не имеет права переписывать результат.

### Prompt

```text
Ты adversarial evidence verifier. Ищи причины ОТКЛОНИТЬ candidate.
Не исправляй его и не предлагай более правдоподобные факты.

TARGET:
{{target_json}}
SOURCE_REVIEWS:
{{source_reviews_json}}
CLAIMS:
{{all_claims_json}}
CANDIDATE:
{{candidate_json}}

Верни только JSON:
{
  "verdict": "pass|fail",
  "violations": [{
    "code":
      "missing_claim|claim_value_mismatch|quote_missing|rejected_source_leak|ambiguous_source_leak|edition_mismatch|unsupported_title_modifier|event_identity_mismatch|ticket_role_mismatch|silent_conflict|unknown_claim_id",
    "json_path": "string",
    "claim_ids": ["C..."],
    "details": "short string"
  }]
}

Обязательно проверь:
1. каждый non-null финальный scalar подтверждён существующим claim;
2. выбранное значение равно значению claim, а не пересказу модели;
3. claim принадлежит accepted source нужного выпуска;
4. в title нет неподтверждённого номера/статуса/эпитета;
5. ticket_url принадлежит ticket_single_event того же event cluster;
6. несовместимые accepted values отражены как conflict;
7. rejected/ambiguous sources не просочились через косвенный claim.

Если есть хотя бы одно нарушение, verdict=fail.
```

## Stage 6. Deterministic final gate

Финальный renderer выполняется только после verifier и повторяет критические
проверки независимо от него:

- JSON парсится и соответствует локальной схеме;
- все ссылки `source_id`, `claim_id`, `local_subject_id` существуют;
- каждая цитата найдена в тексте с совпадающим `content_sha256`;
- каждый финальный scalar равен значению одного из его claims;
- все claims финального результата происходят из `accepted` source;
- `claim.edition_year`, если задан, совпадает с target;
- critical fields (`festival.title`, период, event date/time/venue/ticket_url)
  не имеют скрытого конфликта;
- `event.ticket_url` подтверждён claim из `ticket_single_event` и совпадает с
  тем же event cluster;
- `verifier.verdict == pass`;
- rejected/ambiguous source никогда не считается cross-check;
- `conflicts=[]` допустим только после сравнения всех accepted claims одного
  поля.

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
`verifier=fail`, любом critical `unknown/conflict`, stale-source leakage или
ticket-role mismatch статус всегда `needs_review`; значение `high` невозможно.

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

## Вывод из первого probe

Первый raw result должен был быть отклонён сразу тремя независимыми gates:

- неподтверждённый модификатор title не равен ни одному accepted claim;
- страница другого выпуска не проходит edition gate;
- URL абонемента не проходит `ticket_single_event` role/identity check.

Поэтому основное улучшение — внешняя проверяемая provenance-цепочка, а не
добавление фразы «не выдумывай» в исходный монолитный prompt.
