# Hero-talk — управление владельцем через EventsBot MCP

> 6 сентября 2026. Часть [MVP по #642](autofill-mvp.md), не уже доступные MCP methods. Владелец может прочитать текущее наполнение, увидеть статистику, добавить тему/события/картинку, задать дословный текст и цепочки, опубликовать или остановить их.
> Источник текущего MCP: [private-events-mcp.md на проверенном main](https://github.com/onedayonemasterpiece/events-bot-new/blob/6fddf14aeb983f97bde96e5963e1c9a9ddf72590/docs/operations/private-events-mcp.md). Сохранить существующие OAuth, resource/client binding, redaction, query/egress budgets и exact-seven read-only Codex surface.

## 1. Что владелец делает обычной фразой

| Запрос владельца | Ожидаемая операция |
|---|---|
| «Что сейчас рассказывает Hero сверху и внизу?» | Inventory active/ready/draft/expired + representative deterministic previews по выбранным page contexts. Не выдумывать единый текст для всех посетителей |
| «Покажи статистику за неделю и почему кампания не показывается» | Aggregate statistics с denominator/coverage и отдельный eligibility trace: facts, dates, placement, caps, pause, readiness |
| «Расскажи про эту лекцию; вот фото» | Разрешить точный event через existing events tools; создать assisted brief, сохранить выбранный asset и связи; W+R до публикации |
| «Вот три фразы. Не меняй ни слова, показывай до воскресенья» | Verbatim chain с exact text, order, timezone/deadline; никакой скрытой модели-переписчика |
| «Добавь шутку и мем; ссылка на выходные» | Editorial chain с редакционной картинкой, не фиктивное событие в Event DB |
| «Расскажи про новую функцию» | Capability binding; не публиковать обещание до реальной готовности функции/маршрута |
| «Останови эту цепочку только внизу» | Изменить placement policy конкретной программы, не ставить всю кампанию на паузу во всех каналах |
| «Выключи эту кампанию везде» | Existing promo owner operation; её Hero activity немедленно перестаёт быть eligible вместе с остальными зависимостями |
| «Верни прошлую формулировку» | Новая revision из указанной старой, сегодняшние facts/rights/targets проверяются заново |

Не создаётся обязательная отдельная админ-панель. Preview и readback доступны в MCP; preview link/визуальное представление добавляются через существующий review mechanism, без публикации private draft в публичный CDN.

## 2. Минимальная модель программы

`program_id` — identity рассказа; `revision` — immutable содержательная версия. Programme status и desired/active revisions различаются. Author origin и source binding неизменяемы без явной новой операции. Кампанийная цепочка не превращается в organic простым удалением строки `campaign_id`.

```text
program
  id, revision, title
  origin: system | catalog_signal | editorial_program | promo_campaign | user_state
  author_mode: automatic | assisted | verbatim
  publication_mode: draft | scheduled | active | paused | archived
  placements: home_hero / typed page_end allowlist
  intent, topic_anchor, context predicates
  starts_at, ends_at, timezone=Europe/Kaliningrad
  campaign_id/activity_id?; capability_id?; entity_refs[]
  priority_band, frequency_policy, exact_copy/media policy
  chains[]: chain_id, nodes[], topic_anchor, primary_destination
  dependencies[]: canonical_ref, revision, evidence_kind, required_until
```

Для initial chain 1–3 узла, один topic anchor, граф без циклов/потерянных links. Допустимы несколько отдельно выбираемых цепочек/эпизодов с явными окнами; длина одной видимой цепи не ограничивает всю редакционную библиотеку. Не добавлять скрытую обязательную длинную межвизитную историю.

```text
node
  node_id, intent, bridge_from?, bridge_kind?
  fragments[]: text | fact_token | link_token
  normal_variant, short_variant?  (same meaning and object identities)
  media_ref?, media_role?, alt?, crop_ref?
  required_fact_ids[], next_node_id?
```

Event/capability/program IDs, цены, даты, возраст и ссылки разрешает deterministic resolver из canonical packet. Ссылки — labels + allowlisted link tokens, не HTML. Primary CTA может сопровождаться связанными inline links; compiler проверяет количество/geometry без подмены смысла. Модель не присылает исполняемый JS/CSS/SVG.

### Дословный режим

Текст сохраняется как точная последовательность Unicode code points; хранится UTF-8 content hash. Escaping при выводе не меняет отображаемую формулировку. Нельзя незаметно менять кавычки, пунктуацию, порядок, исправлять «неудачную шутку», переводить или придумывать short variant. Normal и short могут быть отдельно заданы владельцем. При отсутствии short renderer переносит строки; если контракт не помещается, prepare сообщает конкретную проблему, а не обрезает текст.

Verbatim не отменяет safety/facts/rights gates. Если literal «бесплатно» стало неверным, эта revision перестаёт показываться. Нельзя исправить его на «от 500 ₽» без новой owner change. У однозначного fact-token grounding модель не нужна. Новое спорное factual claim требует evidence/semantic review; иначе `needs_evidence`, не автоматическое одобрение авторитета владельца. Шутка/fiction маркируется как редакционный смысл, не как canonical факт о событии или посетителе.

### Схематический пример — только fixture, не публикация

```json
{
  "title": "Слишком серьёзная неделя",
  "origin": "editorial_program",
  "author_mode": "verbatim",
  "placements": ["home_hero", "collection_page_end"],
  "topic_anchor": "weekend-planning",
  "nodes": [
    {
      "node_id": "fixture-n1",
      "fragments": [{"text": "Неделя была серьёзная. Выходным это необязательно."}],
      "next_node_id": "fixture-n2"
    },
    {
      "node_id": "fixture-n2",
      "bridge_kind": "consequence",
      "fragments": [
        {"text": "Поищем планы без галстука? "},
        {"link_token": "weekend-route", "label": "Посмотреть выходные"}
      ]
    }
  ],
  "media_ref": "fixture:owner-drawing",
  "media_role": "editorial_image",
  "media_policy": "optional_text_fallback"
}
```

Fixture не содержит реального uploaded asset, timeframe или проверенного route receipt. Production prepare обязан их разрешить; ни одного фактического показа этого примера в исследовании не было. Claim про погоду/популярность/распроданность не подмешивается для «улучшения» шутки.

## 3. Семь tools, один предметный сервис

Названия ниже — предлагаемый новый contract. Typed schemas с `additionalProperties=false`, bounded strings/arrays, strict enum и action-specific `oneOf`; не один произвольный `execute`/SQL API.

| Tool | Основные параметры | Возвращает / side effects |
|---|---|---|
| `hero_talk_get` | `program_id?`, status/placement filters, `cursor?`, `limit<=25` | Inventory или точная версия; current desired/active hashes, available placements, source bindings, freshness, operator reasons. Read-only, 0 LLM |
| `hero_talk_preview` | `program_id`, `revision`, fixture/public route context, viewport/reduced-motion | Полный served plan, copy/links/media refs, deterministic eligibility trace и отличия от active. Не пишет exposure/кап, не публикует, не делает генерацию |
| `hero_talk_stats` | interval (24h/7d/30d или bounded dates), program/campaign/placement/version, bounded groupings | Aggregate + denominator/coverage/data_as_of; operational status отдельно от product analytics. Read-only, 0 LLM |
| `hero_talk_asset_stage` | один authenticated `file` или точно выбранный разрешённый asset ref, role, alt, rights basis | Opaque principal-bound durable asset receipt либо operation_ref проверки. Не публикует сцену. Повтор exact bytes идемпотентен |
| `hero_talk_prepare` | typed action, programme/expected revision, frozen payload, `idempotency_key` | Exact diff/preview, dependency check, proposed schedule/budget, `preparation_ref`, `action_digest`, expiry, errors. Сохраняет подготовку, но не активирует программу |
| `hero_talk_commit` | `preparation_ref`, `action_digest` | Атомарно применяет разрешённую подготовку; `operation_ref`, durable state и next action. Повтор не создаёт второй publish/generation |
| `hero_talk_operation_get` | `operation_ref` | Точный прогресс: accepted/queued/generating/validating/ready/active/blocked/failed/superseded, hashes и sanitized reason; reconcile без повторной записи |

Actions prepare: `upsert_draft`, `generate_draft`, `publish_revision`, `schedule_revision`, `pause`, `resume`, `archive`, `rollback_revision`, `set_placement_policy`, `set_autofill_enabled`. Для каждого — отдельная schema. `set_autofill_enabled` включает только ранее разрешённые source families и существующий budget; не даёт права повышать provider лимиты, включать paid promo или публиковать из нового непроверенного источника.

Основные writer paths:

```text
verbatim: stage asset? → prepare draft → commit → preview
          → prepare publish/schedule exact revision → commit → operation_get/readback

assisted: prepare generate draft → commit → operation_get
          → preview resulting immutable revision
          → prepare publish/schedule exact revision → commit → readback

automatic: one owner-approved autofill policy
           → worker generates/reviews changed briefs
           → automatic publication only inside approved scope
```

Нельзя подтверждать несуществующий будущий текст: assisted publication следует после готовой preview revision. Это не ежедневное ручное одобрение автонаполнения — automatic scope заранее разрешается policy; owner-created assisted draft по умолчанию отдельный управляемый материал. Preview-only/read запрос никогда не включает generation или publication.

Если пользователь уже явно приказал применить точное допустимое изменение, не требуется повторно задавать тот же вопрос в чате: prepare/commit можно выполнить последовательно. При этом обязательные существующие server-side approval gates сохраняются; broad «посмотри» не разрешает write. При изменении содержания между preview и commit нужна новая preparation.

## 4. Авторизация и безопасность изменений

Новая family требует явного server-side разрешения. Предлагаемые scopes: `hero:read`, `hero:write`, `hero:publish`, `hero:analytics`. Owner principal определяется существующим OAuth/resource policy, не параметром `owner=true`. Token для `telegram:publish`, `operations:read` или Codex не получает Hero write автоматически. Partner delegation в этом MVP отсутствует; последующее расширение использует уже принятый partner/organization policy, не особый обход.

Tool annotations readOnly/idempotent/destructive — описательные hints, не механизм защиты. Это согласуется с [MCP Tools](https://modelcontextprotocol.io/specification/2026-07-28/server/tools); права, filters и лимиты проверяет сервер на каждом запросе.

Preparation связывается с actor, tenant/resource/client, action digest, expected programme revision, используемыми facts/assets, public release channel и сроком (предложено 10 минут). Commit повторно проверяет rights/dependencies; stale base даёт `revision_conflict`, не last-write-wins. Для pause/kill новый scope состояния становится эффективным в transaction без ожидания сети. Operation status читается по той же principal binding.

Idempotency key scoped к actor/action; повтор с тем же payload возвращает исходную operation, с другим payload — conflict. После network timeout сначала `operation_get`, не повтор новой logical operation. `processing/unknown` не означает retry-safe. Durable CAS и source fingerprints защищают от гонок owner edit ↔ automatic regeneration; auto не перезаписывает owner-locked programme.

Pause programme, pause Hero activity и pause всей promo campaign — разные действия. Preview явно показывает масштаб; последние выполняются через существующий promo service, а не копией campaign rows. `resume` не снимает expiry/readiness/caps. Delete history/провайдерных публикаций/исходного события не является Hero action.

External source text, картинки и содержимое phrase pack — данные, не инструкции инструментам. Prompt injection в brief не даёт модели доступ к MCP/provider credentials. Publish raw HTML, script URL, произвольный file path или arbitrary outbound HTTP запрещены. Source URL обрабатывается только имеющимся allowlisted fetcher с SSRF/redirect checks; в первом MVP достаточно file и already-approved managed refs.

Asset stage принимает декодируемые поддерживаемые raster images, с явным size/pixel budget и удалением чувствительных metadata. Непроверенный SVG/GIF/video не внедряется как исполняемое/анимированное содержимое; готовый брендовый SVG — только из проверенного registry. Private pending asset не становится публичным до разрешённой публикации. Event photo не записывается напрямую в EventPoster мимо Smart Update. Owner editorial image не получает fake event_id.

## 5. Что означает «сейчас показывается»

`hero_talk_get` должен различать:

- `stored_revision`: сохранено в БД;
- `ready_revision`: pack/asset прошли проверки, но ещё не выбраны active;
- `active_revision`: разрешённая текущая версия программы;
- `eligible_contexts`: где она допустима сейчас;
- `representative_plan`: результат выбора для явно заданного context fixture;
- `observed_exposures`: реально поступившие consented измерения, с задержкой/coverage;
- `delivery_state`: readback подтверждён либо pending/failed.

Нельзя написать «уже у всех на сайте», получив только DB commit. Таймаут readback возвращает `delivery_pending`, не false success; повторная проверка не перезапускает Writer.

Публичный permit endpoint из основного проекта также возвращает `inventory_revision` и hash-ref актуального **публичного** immutable index. Так браузер узнаёт о новой программе без нового Astro build. Устаревший index не является правом на показ: candidate refs повторно проверяются. Черновики, private preview links, actor identities, author briefs и operator rejection details не входят в public index/pack. Basic public eligibility не требует логина или MCP OAuth; owner API и browser-serving API не смешиваются.

Минимальный public pack:

```text
schema_version, renderer_min_version
program_id, program_revision, chain_id, pack_hash
placement_allowlist, intent, origin, topic_anchor
source_revision_refs, canonical entity refs, fact/link slots
nodes with full readable variants, media exact ref/hash/role/crop/alt
campaign/activity binding when applicable, capability requirements
generated_at, safe_until, generic_fallback_id
```

Поле `expires_at` на файле не заставляет HTTP cache удалить его. Доступность скачанного объекта не означает допустимость в current served plan. На mismatch запрещается целая зависимая цепочка либо заранее проверенный независимый fallback, не произвольная склейка узлов разных revisions.

## 6. Статистика, которую можно использовать для решений

Минимальный weekly response по программе:

```text
programme/revision/placement; interval; data_as_of; measurement_status
eligible sampled sessions; page-end reached; qualified chain exposures
node reached/completed; CTA clicks; dismissals
engaged_event_detail / saved / calendar / share / registration actions
feature attempts and authoritative successes
assisted conversion numerator + denominator (not causal uplift)
repetition/suppression, reason coverage, technical fallback rate
model attempts/tokens, generation rejection reasons, publish freshness lag
```

Доступные разрезы: home/page-end, page family, chain/node/version, campaign, mobile/desktop/PWA. Небольшие cohort slices подавляются/агрегируются согласно общему analytics policy; нет раскрытия отдельных посетителей/списка их интересов. Контент статистики не становится новым обучающим taste signal.

Три разных ответа обязательны: `0` — измерено и действительно ноль; `unavailable` — нет такого измерения/сервиса; `insufficient_data` — выборка недостаточна для вывода. Все ratios имеют denominator. Отказ от analytics не запрещает пользоваться Hero, и доля согласившихся не выдаётся за всю аудиторию. Local suppression и observed suppression агрегаты имеют разные scope.

Для диагностики причины непоказа допускается simulation: «при этом route/context эта кампания отсечена по expiry». Это не доказательство, сколько реальных людей её не увидели. Ни `GET pack`, ни qualified visit сайта в целом не подменяют Hero exposure. Общая authority: [analytics §§4–5, 13](https://github.com/onedayonemasterpiece/events-bot-new/blob/6fddf14aeb983f97bde96e5963e1c9a9ddf72590/docs/features/static-site-pages/analytics/README.md).

## 7. Контракт ошибок

Обязательные machine-readable причины: `revision_conflict`, `permission_denied`, `capability_not_ready`, `route_not_published`, `missing_fact`, `fact_revision_changed`, `needs_evidence`, `campaign_paused`, `activity_disabled`, `expired`, `cap_reached`, `cap_accounting_unsupported`, `media_not_approved`, `media_rights_unknown`, `invalid_chain`, `verbatim_overflow`, `budget_deferred`, `provider_unavailable`, `unsupported_renderer`, `delivery_pending`, `telemetry_unavailable`.

Каждая ошибка содержит affected field/node/dependency, retry policy и безопасное следующее действие. Не выдавать пользователю secret URLs, токены, raw provider stacktrace или private source body. `budget_deferred` не меняет public last-valid revision; `fact_revision_changed` напротив может немедленно лишить её eligibility. Эти состояния нельзя сводить к одной кнопке «повторить».

## 8. Приёмка владельцем

Owner через ChatGPT должен пройти один наблюдаемый цикл без ручного SQL/правки JSON в bucket:

```text
прочитал current inventory
→ загрузил свой рисунок
→ создал дословные две реплики со ссылкой и сроком
→ увидел точный preview
→ опубликовал
→ прочитал active version и проверил страницу
→ получил корректный статус статистики
→ изменил одну реплику, не потеряв identities/историю
→ приостановил и увидел generic в пределах fresh-permit SLA
→ восстановил допустимую revision через новую проверку
```

Второй цикл — automatic brief и own campaign: новые пригодные события программы входят сами; pause существующей кампании снимает Hero без редактирования chain; stale job её не оживляет. Обе проверки входят в [implementation acceptance](autofill-implementation-prompt.md). Прохождение схемы tools/list без этого цикла не означает готовый продукт.
