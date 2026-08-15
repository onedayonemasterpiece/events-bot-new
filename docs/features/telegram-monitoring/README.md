# Telegram Monitoring

## P0 typed LLM-first producer/consumer contract

Канонический producer сохраняет для каждого message/album один
`source_parse_decision` (`source-parse-v1`) с closed disposition, всеми event
children, lifecycle actions и `evidence_manifest`. No-keyword/no-date/historical
hints не завершают carrier до LLM. `CONFIRMED_NO_EVENT` допустим только при
complete evidence и валидном structured response; неполные video/media/OCR,
empty/malformed/truncated response и technical/provider error дают
`RETRY_REQUIRED`.

```text
configured Telegram source -> durable message/album source revision
  -> discovery hints only -> attachment/OCR EvidenceManifest
  -> automatic typed SourceParseDecision
  -> optional conditional verification -> Smart Update
  -> typed accepted outcome or visible terminal error
```

Album aggregation объединяет typed decisions, сохраняя siblings и actions;
ordinal не используется как единственная identity. Consumer обрабатывает каждый
доставленный carrier линейно: complete typed no-event закрывается успешно, а
untyped/schema/technical/incomplete-evidence результат закрывается как видимый
`terminal_error` (или `partial_terminal_error`, если positive siblings уже
приняты). Любая legacy force-row удаляется при таком terminal receipt; semantic
force/retry loop не создаётся. Smart Update `diagnostic_event_id` не
увеличивает imported counters и не запускает publication. Legacy payload reader
существует только для fail-closed диагностики/replay старых артефактов; он не
является producer contract или нормальным terminal workflow.

`CONFIRMED_NO_EVENT` обязан содержать ровно один `no_event_reason` из общего
семизначного `SourceNoEventReason`; у всех остальных dispositions поле должно
быть `null`. Missing/unknown/misplaced reason превращает response в
`RETRY_REQUIRED/SCHEMA_MISMATCH` до cursor, receipt, metric или terminal update.

Telegram не имеет отдельной копии contradiction-логики: service staging
доставляет в Kaggle точные `source_parse_contract.py` и
`source_contradiction_facts.py`, а producer импортирует общий pure collector.
Семь типов фактов совпадают с Fly/VK/direct/parser callers, collector не меняет
вердикт, и на один carrier допускается максимум один conditional verifier.
Positive children сохраняются, а исчерпанная inline verification фиксируется
как typed terminal error для оператора, не как вечная очередь.
Канонический prompt contract и закрытые reason definitions:
[`../../llm/prompts.md`](../../llm/prompts.md).


Ежедневный мониторинг публичных Telegram‑каналов/групп с автоматическим импортом событий в БД бота через Smart Event Update.

## Что делает

- По расписанию запускает Kaggle‑kernel `TelegramMonitor`.
- Kaggle читает сообщения источников, делает OCR и извлекает события; афиши по умолчанию грузятся в managed storage:
  - **Yandex Object Storage** (`https://storage.yandexcloud.net/<bucket>/<path>`), если в runtime есть `YC_SA_BOT_STORAGE[_KEY]`;
  - legacy **Supabase Storage** остаётся fallback/backend для старых URL и для окружений без `YC_*`;
  - Catbox используется только в `fallback/off` режимах.
  - Инвариант: extractor **не должен придумывать дату события** из даты публикации поста.
    - Дата/период должны быть явно в тексте/афише (или в виде относительных слов типа «сегодня/завтра», которые разрешено резолвить от даты поста).
    - Для выставок/ярмарок typed parser не подставляет `message_date` или первое число месяца вместо недостающей даты. Teaser/pre-announcement без точного дня разрешается только типизированным complete-evidence LLM verdict; untyped compatibility result не подтверждает no-event и остаётся due.
    - `open call` / «конкурсный отбор» / «приём заявок» и post-event recap — не deterministic filters. Эти признаки передаются как evidence: только complete typed LLM decision может подтвердить no-event/product exclusion, а любой future child или lifecycle action сохраняется.
    - Официальные уведомления администрации Калининграда о **разводе/разводке мостов** считаются событиями городской повестки: `@klgdcity` входит в мониторинг, а bridge-specific rescue работает как conditional verification typed decision, не как разрешение принять пустой legacy verdict. Для источника также включён `bridge_notice_daily`: после успешного `event_id` сервер отправляет notice в `/daily` каналы.
- Для афиш (постеров) по умолчанию использует **managed storage** для стабильных URL:
  - `TG_MONITORING_POSTERS_SUPABASE_MODE=always` (default): upload в managed storage всегда включён; Catbox используется только если storage недоступен;
  - `fallback`: приоритет Catbox, managed storage — только если Catbox‑загрузка не удалась;
  - `off`: только Catbox.
- Если настроен Yandex runtime, bucket для афиш берётся из `YC_STORAGE_BUCKET` (default `kenigevents`).
- Если Yandex не настроен, legacy Supabase fallback берёт bucket из `SUPABASE_MEDIA_BUCKET` (не из `SUPABASE_BUCKET`), чтобы медиа не смешивались с ICS.
- При загрузке афиш в managed storage:
  - объект сохраняется **в WebP** (только WebP, без JPEG) для экономии объёма;
  - новый ключ объекта content-addressed по SHA-256 **закодированных WebP-байтов**:
    - `supabase_path`: `<prefix>/image/v2/<first2>/<encoded_sha256>.webp` (prefix по умолчанию `p`, настраивается через `TG_MONITORING_POSTERS_PREFIX`);
    - dHash остаётся только признаком похожести/дедупликации в payload и никогда
      не определяет публичный immutable URL; старые `<prefix>/dh16/**` доступны
      только для чтения;
  - качество WebP: `TG_MONITORING_POSTERS_WEBP_QUALITY` (default `82`).
- Empty-caption poster-only posts remain part of the normal typed LLM-first path:
  when Telegram text/caption is empty but OCR contains event facts (title/date/time/venue/price/registration),
  TelegramMonitor passes the OCR text as primary source evidence; an empty caption is not a terminal shortcut.
  The server import also preserves that OCR as event/source text and may derive a narrow `tel:+...` booking contact
  from explicit `Запись/регистрация по телефону` evidence before any group post-author fallback is considered.
  Regression contract: `INC-2026-06-30-kraftmarket317-poster-only-zero-events`.
- Сервер скачивает `telegram_results.json` и импортирует события через Smart Update:
  - создаёт новые события;
  - мерджит существующие;
  - добавляет источники в `event_source`.
  - запускает основной event-pass в стандартном scheduling-режиме Smart Update: созданные/обновлённые события
    должны получить обычные `JobOutbox` задачи, включая `vk_sync`, если само событие не `silent`/не отменено.
    Это обязательный контракт для промо-событий, которые дальше должны попасть в VK-публикацию и VK/promo surfaces.
    При повторном импорте уже созданного события результат `skipped_nochange` также re-arm'ит стандартные задачи,
    чтобы catch-up мог чинить старое состояние "event есть, VK job отсутствует".
    Forced single-event replay всегда пропускает свежий payload через Smart Update, даже если точный
    `event_source.source_url` уже есть в БД: это нужно, чтобы исправлять данные события (например конкретную
    hidden/entity registration ссылку вместо широкого landing-page `ticket_link`) и уже затем re-arm'ить публикации
    при `skipped_nochange`.
    Smart Update сохраняет более конкретную same-host registration ссылку из Telegram candidate даже если LLM merge
    не вернул `ticket_link`; при смене ссылки старый `vk_ticket_short_url` сбрасывается, чтобы VK-пост не переиспользовал
    короткую ссылку на broad landing page.
    Обычный `/tg` импорт re-arm'ит только события, затронутые текущим `telegram_results.json`; глобальный catch-up
    старых Telegram-origin событий отключён по умолчанию, чтобы single-source E2E не создавал неожиданные старые VK
    посты. Исторический широкий reconcile доступен только как явный операторский режим через
    `TG_MONITORING_GLOBAL_VK_RECONCILE=1`.
    `vk_sync` в outbox имеет высокий глобальный priority, но всё ещё ждёт свои per-event prerequisites; готовые
    Telegram imports не должны стоять за чужим Telegraph/page backlog.
    Re-arm `tg_event_publish` при открытом дневном publish window не должен сдвигаться на завтра из-за старого pending
    anchor на следующий день; если pending job уже существует, его `next_run_at` должен заменяться на слот текущего
    цикла. Forced single-source E2E должен доводить затронутые события до видимого `@kldevents` поста в текущем цикле
    после Telegraph/VK prerequisites.
    Telegraph prerequisite не должен сам превращаться в far-future `stale` blocker на нормальном LLM-first render пути:
    runtime budget `telegraph_build` должен быть достаточно длинным для Gemma/Gemini fallback и повторный import обязан
    re-arm'ить ошибочный Telegraph job через стандартный Smart Update path.
    Если Telegram-origin событие доходит до `vk_sync` без renderable афиши/фото (`event.photo_urls`/`eventposter` пусты
    и Telegraph fallback не даёт картинок), VK publication fail-closed: managed `klgdevents` пост не создаётся текстом.
    Это regression contract для `INC-2026-06-04-tg-monitoring-media-and-digest-quality.md`: чинить нужно media
    ingestion/source parsing, а не принимать silent text-only публикацию.
    Venue/location semantics остаются LLM-first: regex/OCR helpers may only provide narrow structural hints
    (address normalization, explicit `адрес, студия/зал` parsing) or fail-closed safety gates. They must not
    override an LLM-extracted/source-default known venue with a free-text comma fragment unless a separate LLM-owned
    venue-review stage confirms that offsite venue from source-grounded evidence. Regression contract:
    `INC-2026-06-18-tg-location-prose-still-extracted`.
    Telegraph/source-media rehydration must also be event-local: if one source URL is attached to multiple event rows,
    the rehydrate pass must not attach all source images to each row; media recovery needs event-local assignment/OCR
    evidence instead of broad deterministic source-context reuse.
  - обрабатывает Telegram-посты в хронологическом порядке (старые → новые), чтобы старые посты не перезатирали более свежие обновления того же события.
  - во время импорта в `/tg` показывает live-прогресс по каждому посту (`X/Y`, ссылка на пост, `Smart Update: ✅/🔄`, `event_ids`, иллюстрации, `took_sec`), чтобы оператор видел, что импорт не завис.
  - отправляет подробный блок `Smart Update (детали событий)` сразу после обработки конкретного поста (не дожидаясь завершения всего импорта).
    В event-блоке строка `Посты:` обязана показывать оба publication surface: `VK` со ссылкой на managed `klgdevents` пост и `TG` со ссылкой на пост в `@kldevents`.
    Если Telegram-анонс уже поставлен в `JobOutbox`, но отложен до утреннего окна, строка показывает `TG ⏳`, чтобы оператор видел, что это расписание, а не потеря публикации.
  - в интерактивном режиме (`/tg`) финальный отчёт **не повторяет полный список созданных/обновлённых событий**, чтобы не дублировать ленту (подробности уже пришли per-post).
    - переопределение: `TG_MONITORING_FINAL_EVENT_LIST=1` (вернуть полный список в финале) или `=0`.
  - в `Smart Update (детали событий)` дополнительно показываются операторские блоки:
    - `🔥 Популярные посты` (⭐/👍) с метриками внутри канала; если пост уже привязан к событиям, рядом даётся ссылка на Telegraph страницы этих событий.
    - `📈 Метрики обновлены (без Smart Update)` для уже сканированных постов (обновление views/likes без повторного импорта).
    - `📌 Частично/пропущено` с причинами (past/invalid/rejected/nochange и т.п.), чтобы было понятно, почему «извлечено != создано».
  - перед отправкой per-post отчёта (best-effort) синхронно «дренит» JobOutbox задачи `ics_publish` + `telegraph_build` для затронутых `event_id`, чтобы ссылки Telegraph/ICS были актуальны сразу и DEV-снапшоты с чужим Telegraph token корректно пересоздавали страницы при `PAGE_ACCESS_DENIED` (см. `TG_MONITORING_DRAIN_EVENT_JOBS`).
    - Важно: inline‑drain ограничен по времени (`TG_MONITORING_INLINE_DRAIN_TIMEOUT_SEC`, default `10`) и не должен останавливать импорт `/tg` (если outbox занят/завис или задачи уже выполнены).
  - в per-post блоке `Источник:` Telegram-посты (`t.me/<channel>/<id>`) рендерятся с preview-friendly `href` (`?single`), чтобы ссылка лучше открывалась через web preview в клиентах Telegram; канонический `source_url` в БД при этом не меняется.
  - fallback на афиши из публичной страницы `t.me/s/...` теперь выполняется независимо от наличия `bot`-объекта и для single-event, и для multi-event постов, если upstream payload потерял `posters[]`; для multi-event fallback дополнительно прогоняет OCR по scraped-картинкам, чтобы сохранить безтекстовые фото у всех split-событий, но не размазывать расписательные постеры по чужим карточкам. В логи пишется явный результат (`tg_monitor.poster_fallback ... posters=N`) и ошибки fallback (debug), чтобы пропуски медиа диагностировались по логам.
  - fallback полного текста из публичной страницы `t.me/s/...` остаётся single-event only.
  - если в fallback сломалась загрузка poster media в Catbox/Supabase, импорт не обнуляет иллюстрации: используется прямой CDN URL целевого Telegram media (`cdn*.telesco.pe`) как последний аварийный fallback.
  - `linked_source_urls` теперь обогащают медиа события: сервер пытается подтянуть афиши из linked Telegram постов (сначала из того же `telegram_results.json`, затем через `t.me/s/...` fallback) и добавляет их в candidate до Smart Update.
- `linked_source_urls` также обогащают факты: для single-event постов сервер (best-effort) скачивает текст linked Telegram постов (payload-first, затем `t.me/s/...`) и прогоняет Smart Update по каждому linked источнику, чтобы в source log были факты по всем ссылкам. Linked-pass не считает location-name доказанным только потому, что в linked text присутствует тот же адрес: неподтверждённое имя площадки уходит в LLM `location_grounding_review`, а fuzzy reference match не может считать `Советский 1` и `Советский 12` одним домом (`INC-2026-07-27-icae-casting-wrong-venue`). Эти вспомогательные linked-pass вызовы подавляют `vk_sync`, потому что публикационную задачу должен ставить только основной источник события.
- Публичные Telegram URL принимаются с host `t.me` и `telegram.me` на source-add, linked-post,
  Smart Update/source identity, media recovery, festival queue и operator lookup границах.
  Любой принятый alias перед сохранением канонизируется в `https://t.me/...`; producer и public
  output также пишут только `t.me`, поэтому смена host не создаёт дубль источника/события.
- Перед вызовом Smart Update candidate build дополнительно проверяет площадку по `source_text` и OCR афиши:
  - если extractor отдал venue, которого нет в тексте/OCR, а в том же посте явно виден другой venue, сервер подменяет extractor guess на подтверждённый venue;
  - если producer уже пометил venue как подозрительный и LLM-review оставил поле пустым, сервер может восстановить площадку из `default_location`, `docs/reference/locations.md` / `docs/reference/location-aliases.md`, адреса или OCR/text fallback; это reference/grounding layer, а не semantic phrase dictionary.
  - если extractor разложил соседнюю прозу между `location_name` и `location_address`, сервер отбрасывает prose-like address-фрагмент и восстанавливает структурные `location_name/location_address/city` из единственной известной площадки в исходном тексте/алиасах.
- если афиша явно содержит несколько дат/времён одного и того же события (например «12 июня 19:00» и «13 июня 15:00»), а extractor их схлопнул в одну дату, сервер (best-effort) расширяет карточку до нескольких событий по OCR афиши.
  Dotted tokens that can also be dates (`9.08`, `26.07`) are not accepted as times unless nearby OCR context says it is a time (`начало`, `в`, `часов`, etc.); this is a guardrail for `INC-2026-06-07-future-event-quality-recurrence.md`.
  - сохраняет `source_title`/`sources_meta[].title` в `telegram_source.title` (человекочитаемое название канала/группы).
  - сохраняет метаданные источника из `sources_meta[]`: `about`, `about_links_json`, `meta_hash`, `meta_fetched_at`.
  - сохраняет подсказки серии/сайта (`suggested_*`) в `telegram_source` и показывает их в UI `/tg` отдельной кнопкой принятия (без автоперезаписи ручного `festival_series`).
  - если Kaggle вернул неполную карточку события (например только `DD.MM | Title` без `location_name`/`ticket_link`),
    сервер делает best-effort восстановление из текста сообщения:
    - локация/адрес: по строкам вида `📍 ...` или `Площадка, улица/дом ...`;
    - контакт/ссылка для записи:
      - `@username` в контексте «запись/бронь/напиши» → `ticket_link=https://t.me/username`;
      - если в Kaggle‑payload пришли `messages[].links` (кнопки/hidden URL entities типа “More info”, “билеты”, “здесь”) и `ticket_link` пустой, сервер может best-effort выбрать один «сильный» registration/ticket URL.
    - заголовок: если extractor вернул мусор вроде `(4 места)`, заголовок берётся из первой содержательной строки поста. Short contentful titles returned by the LLM (`Идиот`, `Гараж`, `№ 13`) are valid and must not be overwritten only because they are short; umbrella/service lines such as `завтра в театре`, `афиша`, `анонс`, `в продаже репертуар` are skipped by this fallback.
- В Kaggle используются только модели Gemma (текст/vision); 4o там не участвует.
- Актуальный Kaggle runtime для LLM-stage теперь строится из [telegram_monitor.py](/workspaces/events-bot-new/kaggle/TelegramMonitor/telegram_monitor.py:1), а [telegram_monitor.ipynb](/workspaces/events-bot-new/kaggle/TelegramMonitor/telegram_monitor.ipynb:1) синхронизируется из него перед push.
- Kaggle producer переведён на shared `GoogleAIClient`/`google_ai` runtime с native `response_schema` для Gemma 4 structured stages вместо direct `google.generativeai` calls.
- Primary Kaggle key isolation для этого surface: `GOOGLE_API_KEY3` / `GOOGLE_API_LOCALNAME3`. Если `GOOGLE_API_KEY3` ещё не зарегистрирован в Supabase quota registry, gateway не должен молча брать общий key pool: он переходит на process-local limiter и всё равно вызывает provider через выбранный `GOOGLE_API_KEY3`.
- Kaggle secrets для Telegram Monitoring не передают unrelated `GOOGLE_API_KEY*` pools: `GOOGLE_API_KEY` внутри notebook является legacy alias на выбранный monitoring key, а дефолтный fallback env тоже указывает на `GOOGLE_API_KEY3`.
- Provider calls ограничены таймаутом: `TG_MONITORING_LLM_TIMEOUT_SECONDS` (default `45`) выставляет `GOOGLE_AI_PROVIDER_TIMEOUT_SEC`, чтобы retryable Gemma 4 `500/504` или зависшие calls fail-open на уровне поста/стадии, а не съедали весь Kaggle window.
- Дефолтные Kaggle text/vision модели для этого surface: `models/gemma-4-31b-it`.
- `Gemma 4` prompt hardening для source metadata запрещает сохранять social/profile links (`Telegram`, `Telegra.ph`, `Instagram`, `VK`, `YouTube`, `Linktree`, `Taplink`, `Boosty`, `Patreon`) как `suggested_website_url`; туда должен попадать только standalone website самого фестиваля/проекта/источника.
- `Gemma 4` extract prompt для Telegram text+OCR явно требует мерджить venue/date/time facts из OCR в event object, заполнять `location_name`/`location_address`, избегать whitespace-only strings и не придумывать `end_date` для single-date событий.
- Historical-date contract explicitly treats interviews, memoirs, museum chronicles and anniversary articles as non-events: an old opening/acquisition/employment day-month cannot be rolled into the current year without a separate future attendee-facing announcement.
- Typed source prompt различает явные work-hours notices и события в музеях/библиотеках: `график/режим/часы работы`, `санитарный день`, `не работает/закрыто` могут дать только complete-evidence `CONFIRMED_NO_EVENT`, но лекции, шоу, мастер-классы, экскурсии и фестивальные слоты с датой/временем должны стать positive children даже при venue/address словах вроде `Библиотека ...` или `Музейная аллея`.
- После `INC-2026-07-31-false-kgd80-festival-link` принадлежность к кампании
  «80 историй о главном» требует literal anchor в текущем source text/OCR/link:
  точного названия (separator-style hashtag тоже допустим), домена `kgd80.ru`
  или явной curated KGD80 series у festival-source. Общие формулировки
  «80-летие Калининградской области» / «80 лет области» таким anchor не
  являются. Prompt остаётся semantic owner, а Telegram import и центральный
  Smart Update применяют узкий fail-closed grounding guard до merge и
  festival queue.
- После `INC-2026-07-10-zoo-ticket-validity-non-event` schedule-like сообщения сначала проходят отдельный LLM screen `event_timetable | institution_hours_or_ticket_terms | other` с `date_role` и короткими evidence spans. Сообщения только о нормальном режиме площадки, часах посетителей/касс, покупке или сроке действия входного билета могут закрыться typed `CONFIRMED_NO_EVENT`: `билет действителен до 31 декабря` — это `ticket_valid_until`, не event date, а часы площадки/кассы — не event time. Deterministic `schedule_like` остаётся только роутером; если настоящих date-header blocks нет, whole-message schedule rescue не запускается.
- `Gemma 4` producer-level contract for `location_name`: поле должно быть реальным venue/place name, а не соседней прозой, биографией спикера, schedule commentary, non-location emoji/list bullet, discussion-topic строкой (`о концертах` / `об итогах ...`), film metadata, ticket instruction, описанием события или temporal/date fragment. Запрещены и decorated формы вроде `🤗Завтра` / bullet-prefixed `Сегодня`: producer schema и venue-review prompt обязаны отправлять такие значения в LLM review / empty+fallback path до server import safety-net. Одна prose/list sentence не должна раскладываться между `location_name` и `location_address`. Для расписаний schedule-rescue передаёт в каждый day-block prompt общий контекст поста, чтобы хвостовые venue-линии вроде `📍Остров Канта` были видны LLM для всех строк расписания.
- После `INC-2026-05-17-future-event-quality-regressions` этот contract дополнен профилактикой для реальных May 17 форм:
  - очевидные имена персон в `location_name` (например `ТАТЬЯНА БОРИСОВА`) являются только триггером LLM venue-review; смысловую площадку выбирает review-pass по source text/OCR/source default, а серверный импорт держит такой же fail-closed safety-net;
  - compact theatre lines вида `17.05 | GROZA` трактуются как date marker + title, не как `time=17:05`;
  - service/digest headings вроде `неделя в театре`, `афиша`, `репертуар`, `анонс` не считаются attendee-facing title, если рядом есть реальное название события;
  - если primary typed decision не содержит positive child для structurally clear single-event post с датой, временем и ticket/venue evidence, запускается узкий conditional LLM verifier. Сервер сохраняет typed retry/diagnostic evidence, чтобы DB-аудит видел “possible producer false negative”, а не “пост не сканировался”; carrier остаётся доступен для автоматической переобработки.
- После `INC-2026-07-14-ecodvor-unknown-start-time-cursor` parent/programme window не становится временем вложенной активности: если у мастер-класса/лекции прямо сказано, что время начала уточняется, producer и Smart Update возвращают неизвестное время, даже когда тот же пост даёт часы всего фестиваля. Подтверждённый LLM anchor-review также может очистить уже сохранённое неверное время при source-anchor merge. Это один дополнительный anchor-role LLM review только для exact TBD-кандидата, а не новый вызов на каждое событие; узкий server/merge guard лишь fail-closed применяет подтверждённое решение и не извлекает смысл.
- После `INC-2026-05-02-pre-daily-event-quality` prompt contract дополнительно закрепляет **event-local venue grounding**: в multi-event/digest/repost постах площадка, адрес и город берутся из ближайшего блока конкретного события, а source/default location используется только когда event-local блок не называет свою площадку. Литералы имён полей (`location_address`, `address`, `location_name`, `venue`, `city`, `адрес`, `город`) считаются синтаксическими placeholders и должны стать пустыми строками, не публичными значениями.
- После `INC-2026-06-24-future-event-date-default-venue-regressions` contract дополнительно закрепляет real-source date/default-location guard: русские числовые даты в мониторинге всегда трактуются как `DD.MM` (`10.05` = 10 мая, не 10 сентября), даты словами/хэштегом (`26 июля`, `#13_июня`, `#21_июня`) являются authoritative event date и не переносятся в месяц публикации, а `гейт 2.6`, этажи, адреса, координаты, цены, телефоны и номера домов не могут становиться датой/временем. Для single-event output producer может узко исправить LLM-date drift только если в тексте/OCR есть ровно одна явная дата; это safety-net, не классификатор eventness. Если пост явно даёт offsite `Место:`/`📍`/адрес, эта event-local площадка/адрес выигрывает у `source.default_location` даже когда extractor изначально оставил venue пустым.
- `Gemma 4` venue-review stage: если extracted `location_name` имеет широкий плохой shape (слишком длинная фраза, schedule row, короткий section label вроде `Кинозал:`, короткое предложение с точкой, non-location emoji/list bullet вроде `📩 ...`, topic fragment вроде `о концертах`) или источник имеет `default_location`, Kaggle делает отдельный LLM-pass только по `location_name/location_address/city` на original message + OCR + source context + `default_location`. После `INC-2026-05-09-event-location-alias-free-dup-regressions` stage также запускается, когда извлечённая площадка не grounded в тексте/OCR/source context, а рядом есть venue/address cues, или когда адрес есть в источнике, но не попал в structured fields. Детерминированная часть решает только “нужна проверка”; смысловую площадку выбирает LLM через быстрый Gemma 4 native-schema response. Это canonical fix path для произвольных фрагментов вроде случайно попавшей фразы из соседнего предложения и для похожих venue-alias drift случаев вроде `Дворец спорта «Янтарный»` vs `Дворец спорта «Юность»`.
- После `INC-2026-05-27-dachniki-prose-venue-duplicates` title prompt дополнительно закрепляет приоритет caption/source event name над poster OCR slogan/CTA: если сообщение называет событие/проект (`Живой сундук`), а афиша несёт лозунг вроде `Читайте бумажные книги!`, extractor должен взять название события из caption/source и оставить лозунг в `raw_excerpt/search_digest`, а не переименовывать карточку.
- Второй hardening wave по реальным Gemma 4 outputs (`run_id=48fa98294333486d94dd0e14785d774f`) точечно лечит наблюдавшиеся регрессии: prompt явно запрещает inline `//`/`#` комментарии и markdown (`**`/`__`) внутри JSON-значений, запрещает ghost-события с пустыми `title` и `date`, запрещает литерал `"unknown"` в любом поле, требует lowercase русский `event_type` (`концерт`/`выставка`/`лекция`/...) вместо английских токенов вроде `"exhibition"`/`"meetup"`, не даёт копировать город из parenthetical origin notes (`"(Санкт-Петербург)"` в описании коллекции ≠ место события), и скипает fundraiser/video-recap/book-review посты без приглашения на будущее событие. Те же правила дублированы в exhibition fallback prompt и в json-fix retry, чтобы retry не пропускал те же классы ошибок.
- Schema `EVENT_ARRAY_SCHEMA` получил `description` по ключевым полям (`title`/`city`/`event_type`/`date`/`time`/`location_*`), которые Gemini structured output уважает как дополнительный канал подсказок; hard constraints остаются в prompt text, чтобы не нарушать schema-совместимость Gemma 4.
- Добавлен LLM-output safety-net `_sanitize_extracted_events` (детерминированный post-LLM хелпер без semantic rewriting): срезает leaked inline `//`/`#` хвосты, снимает оставшиеся markdown-маркеры, нормализует placeholder-литералы (`unknown`/`n/a`/`none`) до пустой строки и дропает ghost-события, где пусты и `title`, и `date`. Это не заменяет LLM — просто не даёт известным Gemma 4 failure modes доехать до Smart Update и Telegraph.
- Iter2 safety-net extensions по результатам local-only Gemma 4 eval ([artifacts/codex/tg-g4-opus-local-eval/](/workspaces/events-bot-new-tg-g4-sU9xCP/artifacts/codex/tg-g4-opus-local-eval/), `GOOGLE_API_KEY2`, **не production-equivalent** — production Telegram Monitoring остаётся на `GOOGLE_API_KEY3`, который в локальной среде отсутствует): `_sanitize_extracted_events` теперь дополнительно стрипает HTML-подобные теги (`</strong>`, `<br>`, `<em>` — Gemma 4 изредка эмитит их внутрь structured JSON string values) и отрезает trailing meta-commentary вида `own title:` / `own id:` / `own field:` (наблюдались в iter1 local eval у `@barn_kaliningrad/971`). Это syntax-only cleanup и не подменяет LLM-решение о смысле события.
- Следующий точечный prompt pass для exhibition-постов добавил title/cardinality guardrail: если пост говорит о разделе внутри выставки (`"в разделе X на выставке Y"`), title должен оставаться названием основной выставки `Y`, а не subsection label `X`; если один и тот же пост анонсирует и открытие выставки, и её run-window, по умолчанию лучше вернуть одну exhibition-card с opening datetime + `end_date`, а не два раздельных attendable события. Local-only re-eval на `GOOGLE_API_KEY2` вернул positive control `TG-G4-EVAL-10` из `2` событий обратно к `1`.
- LLM-first local tuning pass по `TG-G4-EVAL-01..10` добавил staged Gemma prompts вместо semantic regex/fallback extraction: single invited lecture rescue, named ongoing exhibition rescue, museum spotlight rescue/repair и chunked schedule rescue. На полном локальном `extract_events` пути это закрыло `TG-G4-EVAL-02` (`Космос красного`, без `unknown`), `-03` (лекция Amber Museum с OCR date/time), `-04` (museum spotlight как exhibition-card), `-07` (одна лекция без дубля/venue-only row) и `-10` (positive-control exhibition остаётся одной строкой). `TG-G4-EVAL-08` теперь извлекает реальные zoo schedule rows без garbage placeholder row, но из-за provider `500`/timeout на отдельных schedule chunks recall может быть частичным; production-equivalent smoke через `GOOGLE_API_KEY3` всё ещё обязателен.
- Controlled Kaggle smoke `tg_g4_key2_as_key3_forced_eval_70b4fc14` и focused extraction-only smoke `tg_g4_key2_as_key3_focused_eval_90e527f5` запускались с локальным `GOOGLE_API_KEY2`, замапленным в env-name `GOOGLE_API_KEY3` по тому же encrypted-dataset Kaggle path. Smoke подтвердил отсутствие старых leak/ghost/unknown/event_type drift классов на `@barn_kaliningrad/971`, `@domkitoboya/3170`, `@kldzoo/7089`, `@koihm/5505` и `@kaliningradartmuseum/7902`, но выявил новый prompt-contract риск: когда OCR не даёт дату/время, одиночная лекция `@ambermuseum/5600` не должна датироваться `message_date`. Prompt теперь явно запрещает использовать `message_date` как fallback event date для не-выставочных single events; он остаётся только контекстом для явных relative anchors (`сегодня`/`завтра`/`послезавтра`) и для museum/exhibition as-of merge cases. Дополнительный post-LLM guardrail `_lacks_supported_non_exhibition_date` только enforcing-уровня: он не извлекает и не переписывает смысл, а отбрасывает не-выставочные rows без поддержанной даты или с подставленным `message_date` без anchor.
- Forced A/B regression gate на тех же 16 постах из ночного prod output `095e32fd497442258fb5675f65f43731` показал: legacy Gemma 3 notebook (`g3cmp095e0785bc`) извлёк `10` событий и имел `empty_date=1`, `english_event_type=4`; промежуточный Gemma 4 producer (`abfull095ef8e2d2`) извлёк `12` событий без leak/ghost/empty-date/bad-date/English-city smell-классов, но с quality regression на `@signalkld/10512`, где poster OCR heading `НАЧАЛО В 19:00` стал `title` вместо caption event name `Второй Большой киноквиз`. Prompt теперь явно закрепляет title-audit: OCR service headings/date/time/price/venue labels не должны заменять named event из message text, а используются только для date/time/venue/ticket facts. Targeted smokes `sig10512c3c25072`, `sig10512b518272c` и `sig10512r6cb27e5` показали, что prompt-only guidance и full-event repair было недостаточно, поэтому добавлен компактный LLM title-review stage: deterministic код только замечает service-heading title, а Gemma 4 возвращает replacement `title/event_type/search_digest` по original caption+OCR; event count/order сохраняются. Targeted validation `sig10512u8402a5b` подтвердил исправление (`title="Второй Большой киноквиз"`, `event_type="квиз"`), а полный повтор gate `abfinal095edeb15` извлёк `14` событий на тех же 16 постах и дал `0` smell-регрессий по проверяемым классам: thought/markdown leak, ghost row, empty title/date, bad date shape, English city/event_type, `unknown` literal и service-heading title. Это regression evidence для ветки hardening; production deploy/catch-up должен отдельно подтвердить импортный контур.
- Историческая запись: в первом Gemma 4 migration commit `open_call_re`/`anchor_re` гварды приводили к silent drop. В актуальном producer path эти сигналы ушли в typed primary/verifier evidence и не фильтруют positive child; legacy `extract_events` не вызывается production scan.
- Для следующего prompt-quality pass собран компактный eval pack из реального full-run evidence: [tests/fixtures/telegram_monitor_gemma4_eval_pack_2026_04_23.json](/workspaces/events-bot-new-tg-g4-sU9xCP/tests/fixtures/telegram_monitor_gemma4_eval_pack_2026_04_23.json). В нём 10 именованных кейсов из `run_id=48fa98294333486d94dd0e14785d774f`: thought leak + ghost row (`@barn_kaliningrad/971`), `unknown` placeholders, city drift (`Saint Petersburg` вместо Калининграда), English `event_type`, markdown tail, retrospective/non-event posts, same-day anchor regression, schedule post with garbage placeholder row и один positive control. Этот fixture нужен для A/B prompt tuning и второго Opus-pass, чтобы оценивать изменения на одной и той же базе.
- Kaggle notebook embed-ит в generated `.ipynb` **полное детерминированное Python-дерево** пакета `google_ai` (включая `limiter_supabase.py`, `interactions.py` и будущие вложенные модули), без ручного allowlist. Runner дополнительно ищет bundled package в kernel root, `/kaggle/working` и `/kaggle/input`; это нужно, потому что plain extra files рядом с notebook не гарантированно попадают в Kaggle runtime. Изолированный generated-notebook test обязан импортировать публичный package API и shared limiter до deploy.
- Generated Kaggle `.ipynb` вырезает script-only tail `asyncio.run(main())` / `already running event loop` guard и запускает `main()` отдельной notebook-cell через `nest_asyncio`; иначе Papermill падает в уже запущенном event loop.

Live validation (`2026-04-22`):

- Run `tg_g4_live_smoke_subset_20260422g` на Kaggle `zigomaro/telegram-monitor-bot` завершил producer stage и выгрузил `telegram_results.json` (`schema_version=2`, `sources_total=3`, `messages_scanned=2`, `messages_with_events=1`, `events_extracted=4`).
- Kaggle log подтвердил `text_model=models/gemma-4-31b-it`, `vision_model=models/gemma-4-31b-it`, `requested_model/provider_model/invoked_model=models/gemma-4-31b-it`; Gemma 3 fallback не использовался.
- Server import/recovery по этому output зафиксирован в `ops_run`: `id=797`, `trigger=recovery_import`, `status=success`, `errors_count=0`; повторный import-only `id=798` тоже завершился `success`, `errors_count=0`.
- Scheduled full run `48fa98294333486d94dd0e14785d774f` после key-pool hardening прошёл через Kaggle на 45 источниках: `messages_scanned=177`, `messages_with_events=69`, `events_extracted=84`; server recovery import `ops_run id=803` завершился `success`, `errors_count=0`, `events_imported=14`.
- Full-run log подтвердил `GOOGLE_API_KEY3`, отсутствие `GOOGLE_API_KEY2`, отсутствие `gemma-3`, `requested_model/provider_model/invoked_model=models/gemma-4-31b-it`, но также показал, что старый `180s` provider timeout слишком длинный для scheduled window; default снижен до `45s`.
- Post-timeout smoke `tg_g4_45s_smoke_20260423a` завершился без recovery через primary `ops_run id=807` (`status=success`, `sources_scanned=3`, `messages_processed=3`, `messages_with_events=2`, `errors_count=0`, `duration_sec=279.22`). Log evidence: `GOOGLE_API_KEY3`, `GOOGLE_API_KEY2=0`, `gemma-3=0`, `Traceback=0`, `AuthKeyDuplicatedError=0`; два `45s` timeout на source metadata fail-open и не сорвали run.

## Multi-event, multi-session и zero-event

Один message/album может вернуть несколько событий/сеансов и несколько lifecycle
actions. Producer не останавливается на первой дате, отличает historical recap
от будущего анонса и возвращает `MIXED`, если отмена/перенос соседствует с новым
event. Каждый child получает стабильный occurrence/candidate key и независимо
проходит Smart Update; terminal error одного child не удаляет принятых siblings.

`Событий извлечено` — occurrence count, не carrier count. Разница между
extracted/imported обязана иметь typed terminal/product/exact receipt. Legacy
force replay закрывается тем же вызовом и удаляется из force-таблицы; technical
или identity uncertainty остаётся видимой в receipt и требует операторского
решения, а не бесконечного semantic replay. Complete typed
`CONFIRMED_NO_EVENT` может продвинуть cursor без Event; untyped compatibility
payload без children не может. Старые сообщения, прочитанные только для метрик после уже успешного exact
revision, не вызывают LLM повторно.

Operator report и `ops_run.metrics_json` раздельно показывают
`messages_new_raw`, `messages_forced_replay`, `messages_metrics_only` и
`messages_typed_candidates`; первые три — взаимоисключающие carrier buckets,
последний — число carriers с event children. `messages_terminal_errors` отдельно
показывает незакрытые по смыслу/технике terminal receipts.

Forum/multithread-группа остаётся одним source с общим message-id cursor; topic
root сам по себе не event, но это решает typed source parse, а не local semantic
filter. Point replay старого message выполняется по `message_id`, без расширения
всего scan horizon.

## Метрики постов и популярность (⭐/👍)

Цель: собирать динамику `views/likes` у постов и подсвечивать «популярные» анонсы в отчётах Smart Update.

Каноника (общая для TG/VK): `docs/features/post-metrics/README.md` (таблицы, медианы, уровни `⭐/👍`, retention, ENV).

## Retention (очистка старых метрик)

Снапшоты метрик не хранятся вечно:

- по умолчанию оставляем только последние `90` дней (по publish timestamp);
- очистка выполняется scheduler job `post_metrics_cleanup` раз в сутки;
- настройка: `POST_METRICS_RETENTION_DAYS` (по умолчанию = `POST_POPULARITY_HORIZON_DAYS`).

## Ссылки на другие Telegram-посты (linked posts)

- Если в исходном посте найден URL вида `t.me/.../<message_id>`, мониторинг может добавить его в `linked_source_urls` конкретной карточки события.
- Цель: не потерять факты, которые могут быть разнесены между “коротким” и “полным” постами про одно и то же событие.
- На сервере linked посты обрабатываются так:
  - linked URL сохраняется в `event_source` рядом с основным источником;
  - для single-event постов импортёр (best-effort) подтягивает афиши и полный текст linked поста (payload-first, затем через публичный `t.me/s/...`);
  - затем выполняется дополнительный Smart Update-pass по linked URL: это нужно, чтобы в `🧾 Лог источников` были видны факты и их статусы по linked источнику (а не “без извлечённых фактов”).
- Ограничения (защита от рекурсии/лимитов):
  - только 1 уровень обхода ссылок (без цепочки);
  - обрабатываются только ссылки на посты (`t.me/<channel>/<id>`);
  - лимит linked-текста на карточку: `TG_MONITORING_LINKED_SOURCES_TEXT_LIMIT` (default `2`, max `5`);
  - отключение: `TG_MONITORING_LINKED_SOURCES_TEXT=0` (или `false/no/off`).

## Точки входа

- `/tg` — управление источниками и ручной запуск мониторинга (есть пагинация списка источников).
- `/tg` -> `Только @kraftmarket39` — emergency/containment запуск того же Telegram Monitoring + Smart Update pipeline, но с Kaggle config scope ровно для `@kraftmarket39`. Это временный обходной путь для отладки фестивальных источников без полного multi-source прогона; он не создаёт отдельный импортёр и не обходит Smart Update. Канонический техдолг: `docs/backlog/features/festival-monitoring-debt/README.md`.
- Bot API `channel_post` on-demand — fast-path для allowlisted каналов (v1 default `@kraftmarket39`): новый пост coalesce'ится в durable очередь, после 10-минутного debounce ставится source-specific запуск того же Telegram Monitoring pipeline. Если `_RUN_LOCK`/global lock/remote Telegram session заняты, очередь retry'ится через 10 минут; scheduled monitoring остаётся catch-up. Детали: `docs/features/tg-monitoring-on-demand/README.md`.
- `/tg` → `♻️ Импорт из JSON` — debug/import-only режим: позволяет выбрать один из последних локальных `telegram_results.json` (по умолчанию показываются 4, newest → older) и повторить server-import без нового запуска Kaggle.
  - После выбора файла показывается выбор режима:
    - `Импорт (обычно)` — обычный `run_telegram_import_from_results(...)`.
    - `DEV: Recreate + Reimport` — доступно только при `DEV_MODE=1`, сначала показывает preview (сколько событий/marks будет очищено), затем по подтверждению:
      - удаляет события детерминированно по `event_source(source_type='telegram', source_url IN links из JSON)`;
      - удаляет `joboutbox` по найденным `event_id` (без FK cascade);
      - очищает `telegram_scanned_message` по парам `(source_username, message_id)` из JSON;
      - запускает повторный импорт из того же файла.
  - В `DEV_MODE!=1` DEV-режим не показывается в UI и отклоняется на уровне callback/task, даже если callback вызван вручную.
- Планировщик (`scheduling.py`) — ежедневный запуск по ENV.

Канонический список источников (prod/test) и их настройки: `docs/features/telegram-monitoring/sources.yml` (см. также `docs/features/telegram-monitoring/sources.md`). В список входит официальный `@ecodvor39` с `high` trust и без `default_location`, чтобы площадка оставалась source-grounded.

## Основные модули

- `source_parsing/telegram/commands.py` — UI/команды `/tg`.
- `source_parsing/telegram/service.py` — оркестрация Kaggle и загрузка результатов.
- `source_parsing/telegram/handlers.py` — разбор `telegram_results.json`.
- `smart_event_update.py` — Smart Event Update.

## Надёжность Kaggle polling

- Статус Kaggle kernel опрашивается с интервалом `TG_MONITORING_POLL_INTERVAL` (по умолчанию 30s) до динамического лимита ожидания (или фиксированного, если включён `fixed` mode).
- Транзиентные ошибки сети/SSL при опросе Kaggle API (например `UNEXPECTED_EOF_WHILE_READING`) **не валят прогон**: мониторинг продолжает опрос до получения `COMPLETE/FAILED` или таймаута, а в UI этап показывается как «временная ошибка сети».
- Если status API продолжает отдавать транзиентные `HTTP 429/5xx`, сервер параллельно пробует скачать Kaggle output.
  `telegram_results.json` принимается как завершение только если его `run_id` совпадает с текущим запуском; после этого
  обычный server-import продолжается без рестарта Fly machine.
- Перед `push` сервер теперь дополнительно проверяет общий `kaggle_registry`: если другой remote Telegram Kaggle job (`guide_monitoring`, `tg_monitoring`, `telegraph_cache_probe`, `kenigsberg_story`) с тем же `remote_telegram_auth_scope` ещё жив или его status lookup закончился неопределённо, `tg_monitoring` обязан завершиться `skipped` с `remote_telegram_session_busy`, а не запускать вторую удалённую Telethon session поверх той же auth key. Jobs с разными explicit scopes могут идти параллельно; unknown scope считается конфликтующим.
- Для fresh `UNKNOWN` status lookup guard остаётся fail-closed. Если же registry-запись старше `REMOTE_TELEGRAM_SESSION_UNKNOWN_STALE_MINUTES` (default `390`) и lookup падает только транзиентно (`HTTP 5xx`, сеть, SSL, timeout), guard помечает её как `stale_transient_status_lookup_failure` и больше не считает владельцем remote Telegram session. Это предотвращает вечный `remote_telegram_session_busy` от старых Kaggle refs, но не разрешает запускать вторую сессию поверх свежего неизвестного run.
- Отменённые Kaggle runs со статусом `CANCEL_ACKNOWLEDGED` считаются terminal для shared remote Telegram session guard: такой job не должен блокировать следующий компенсирующий `/tg` catch-up после ручной отмены.

## Recovery после рестарта бота

- `tg_monitoring` регистрирует Kaggle kernel в общем `kaggle_registry` сразу после успешного `push`.
- Scheduler `kaggle_recovery` на старте/по интервалу проверяет незавершённые `tg_monitoring` kernels:
  - если kernel ещё работает в Kaggle, запись остаётся в реестре и будет проверена позже;
  - если kernel завершился `complete`, бот заново скачивает `telegram_results.json` из Kaggle и запускает обычный server-import;
  - если kernel рано сообщает `failed/error/cancelled`, запись не удаляется мгновенно: recovery ещё несколько часов перепроверяет output, потому что Kaggle иногда дозавершает `telegram_results.json` уже после раннего terminal-status; только после истечения `TG_MONITORING_RECOVERY_TERMINAL_GRACE_MINUTES` (default `360`) запись удаляется как окончательно невосстановимая.
- Recovery пропускает job, принадлежащий текущему PID, только пока текущий процесс реально держит Telegram Monitoring `_RUN_LOCK`.
  После cancellation/restart stale registry entry с тем же `pid` обязан снова импортироваться, иначе хвост `telegram_results.json`
  может остаться без prod-side `event_source` / `telegram_scanned_message` evidence (`INC-2026-06-04-kraftmarket271`).
- Локальный poll-timeout в сервере тоже не считается окончательной потерей результата: recovery продолжает проверять kernel в фоне и подхватывает поздно дозавершившийся output без ручного пересканирования.
- Это значит, что для восстановления **не требуется** сохранять `telegram_results.json` в `/data`: источником истины остаётся Kaggle output, а локальный `/tmp` используется только как временный download/cache путь.

## Статусы `ops_run` для `tg_monitoring`

- `success` — результаты Kaggle скачаны, `telegram_results.json` разобран, import завершён, `messages_scanned > 0`.
- `empty` — результаты Kaggle скачаны и разобраны, но реальный отчёт пустой (`messages_scanned = 0`).
- `partial` — отчёт разобран, но во время import накопились ошибки в `TelegramMonitorReport.errors`.
- `error` — results не были получены/разобраны или run был прерван до завершения import.
- Важно: `empty` выставляется **только** когда бот реально прочитал `telegram_results.json`. Пустой in-memory `TelegramMonitorReport` после рестарта/отмены больше не считается `success`.
- Scheduled entrypoint теперь создаёт bootstrap `ops_run` ещё до резолва superadmin и до входа в `run_telegram_monitor()`. Если bootstrap-слой падает раньше основного runner'а, запись закрывается как `error` с `scheduler_entrypoint/fatal_error` в `details_json`; если run стартовал нормально, он переиспользует ту же запись вместо создания второй строки.
- Если APScheduler задержал, потерял или deploy/restart оборвал слот до регистрации Kaggle kernel, общий `critical_scheduler_watchdog` после grace-окна сверяет последний локальный плановый слот с `ops_run`. `running/success/partial/empty` считаются доставкой, а `crashed/error/skipped` — нет; поэтому deploy-killed run, вроде `INC-2026-06-12-tg-monitoring-deploy-crash-no-watchdog`, будет запущен заново как scheduled catch-up.
- Watchdog вычисляет именно последний локальный слот `TG_MONITORING_TIME_LOCAL`, включая предыдущий день после полуночи, и запускает `telegram_monitor_scheduler()` с `run_id` вида `catchup-tg-monitoring-...`. Такой catch-up проходит через тот же remote Telegram session guard и `heavy_operation`, что и обычный scheduled run.
- Если в `kaggle_registry` уже есть незавершённая запись `tg_monitoring`, watchdog не запускает новый catch-up поверх неё: он откладывает повтор на `TG_MONITORING_REMOTE_BUSY_RETRY_SECONDS` (default `300`) и даёт `kaggle_recovery` скачать/import'нуть output или убрать terminal запись. Это важно для `TELEGRAM_AUTH_BUNDLE_S22`: даже когда старый server-side `ops_run` был отменён рестартом, Kaggle kernel может всё ещё держать Telethon session.
- Если catch-up материализовался как `status='skipped'` из-за `remote_telegram_session_busy`, watchdog тоже ставит короткий retry-hold вместо повторного тика каждую минуту.

## Надёжность импорта (SQLite lock)

- Если на этапе server-import возникает `sqlite3.OperationalError: database is locked`, мониторинг не падает сразу:
  - импорт `telegram_results.json` автоматически ретраится (`TG_MONITORING_IMPORT_RETRY_ATTEMPTS`, default `4`);
  - backoff между попытками: `TG_MONITORING_IMPORT_RETRY_BASE_DELAY_SEC` (default `2.0`, exponential).
- Во время ретрая оператор получает сообщение в `/tg`, что импорт повторяется.
- Для ORM-сессий SQLite на каждый новый connection применяются те же PRAGMA, что и для raw-connection (`journal_mode`, `busy_timeout`, `synchronous`, `foreign_keys`, `cache_size`), чтобы снизить вероятность lock-конфликтов под длительной нагрузкой.
- Telegraph job `telegraph_build` делает `commit()` перед сетевыми вызовами к Telegraph API (edit/create), чтобы не держать SQLite write-lock во время HTTP запросов (это снижает вероятность `database is locked` при параллельной работе импортов и воркеров).
- Тонкая настройка SQLite ожидания блокировок:
  - `DB_TIMEOUT_SEC` (default `30`);
  - `DB_BUSY_TIMEOUT_MS` (если задан, приоритетнее `DB_TIMEOUT_SEC`).
- Malformed optional fields from Kaggle payload (например `festival=true/false` вместо строки) не должны валить весь server-import:
  - importer нормализует такие значения до `None` на границе данных;
  - safety-net в `smart_event_update` не должен падать на non-string diagnostic fields во время logging/debug helper paths.

## Защита от параллельных запусков (global lock)

Проблема: если по ошибке запустить мониторинг/импорт из нескольких процессов бота одновременно (например, два polling-инстанса),
то операторский UI начнёт “дублировать” прогресс и отчёты, а SQLite чаще будет падать с `database is locked`.

Решение: сервер ставит cross-process lock на время `run_telegram_monitor` и `run_telegram_import_from_results` (включая DEV `Recreate + Reimport`).

- По умолчанию lock-файл создаётся в `tempdir` (обычно `/tmp`) и включает `BOT_CODE`, чтобы prod/test не блокировали друг друга.
- При попытке параллельного запуска второй процесс получает понятное сообщение в UI `/tg` и прогон пропускается.
- Можно переопределить путь через `TG_MONITORING_GLOBAL_LOCK_PATH`.

## Данные

- `telegram_source` — список источников (username, title, trust, defaults).
- `telegram_source.filters_json` — server-side фильтры на источник (см. `docs/features/telegram-monitoring/sources.yml`).
- `telegram_source.festival_source/festival_series` — признак фестивального канала и название серии.

## Иллюстрации (афиши)

Монитор (Kaggle) может прикреплять к постам список `posters[]` (URL + sha256 + OCR). На сервере эти афиши
переносятся в `event.photo_urls`/`event_poster` через Smart Update.

Важный нюанс:
- OCR используется как evidence для дат/времени только после узких safety‑проверок. Метаданные пластинок/альбомов
  вроде `LP 33 1/3 RPM`, `33⅓ RPM`, `45 RPM`, каталожных номеров и похожих музыкальных обозначений не считаются
  датами или временем события; этот контракт закреплён инцидентом `INC-2026-07-03-event-6045-static-defect`.
- Для постов с одним событием мы переносим **все** фото из поста (dedupe по `sha256`). OCR используется только
  для приоритизации (первое изображение как обложка), а не для удаления фото.
- Для постов, где извлечено несколько событий (расписания/альбомы), мы стараемся **не** прикреплять “чужие”
  афиши ко всем событиям: используем event-level assignment от Kaggle или строгий OCR-матчинг.
- Если у multi-event поста `posters[]` потерялись на upstream, server-side public-page fallback повторно забирает
  картинки из `t.me/s/...`: безтекстовые фото могут попасть во все split-события, а постеры с читаемым OCR
  всё равно проходят через event-level фильтрацию.
- Нестандартный кейс: иногда канал публикует **текст** и сразу отдельным следующим сообщением пересылает афишу
  (forward из другого чата/канала). Если у текстового сообщения нет фото, а у следующего есть `posters[]`, сервер
  (best-effort) прикрепляет афишу к событию из предыдущего поста (poster-bridge) и не считает метрики второго сообщения
  “постом с событием” для популярности.
  - Safety: poster-bridge включается только при короткой подписи и малом временном дельта‑окне, и прикрепляет афиши
    только если OCR уверенно матчит `title/date/time` события (иначе лучше не прикреплять вовсе, чем прикрепить “чужую” афишу).
- Smart Update не “вымывает” уже прикреплённые афиши, если новая выборка `posters[]` оказалась пустой
  (защита от ложного prune).
- Для multi-event текстовых постов без приложенных афиш допустимы события без media в БД, но они не должны silently
  становиться текстовыми VK-постами: публикационная граница требует хотя бы одно renderable VK attachment для
  Telegram-origin managed post.
- Если в payload мониторинга `posters[]` отсутствуют из-за upstream media сбоев, сервер может сделать best-effort
  fallback: вытащить фото из публичной HTML страницы `t.me/s/<username>/<message_id>`.
  Этот fallback извлекает **только** медиа‑изображения из самого поста (photo wrap + video thumbnail) и **не** должен подхватывать
  аватар канала или картинки из соседних постов. При сборке Telegraph страницы дополнительно есть safety‑net:
  слишком маленькие картинки (avatar‑like) удаляются, если в наборе есть полноценный постер.
- При сборке Telegraph страницы события есть дополнительный repair для уже привязанных источников: если у события несколько
  `event_source`, но сохранена только одна картинка, rebuild best-effort повторно забирает изображения из Telegram public page
  и VK wall API по самим source URLs, дедуплицирует их и дописывает в `eventposter`/`event.photo_urls`. Это чинит старые
  source-only/идемпотентные merges без повторного semantic import.
- Если `message.text` в payload выглядит обрезанным (часто заканчивается на `…`/`...`), сервер может (best-effort)
  забрать полный текст поста из публичной HTML страницы `t.me/s/<username>/<message_id>` и использовать его как `source_text`
  для Smart Update (чтобы не терять строки про состав/поддержку/участников).
- `telegram_source.about/about_links_json/meta_hash/meta_fetched_at` — метаданные канала/группы, полученные в Kaggle через Telethon.
- `telegram_source.suggested_festival_series/suggested_website_url/suggestion_confidence/suggestion_rationale` — best-effort подсказки для оператора.
- `telegram_scanned_message` — идемпотентность сообщений.
- `telegram_post_metric` — снапшоты `views/likes` по дням после публикации (для аналитики и ⭐/👍).
- `event_source` — источники события (много на одно событие).
- `ticket_site_queue` — очередь обогащения событий по ticket‑ссылкам из постов (см. `docs/features/ticket-sites-queue/README.md`).
- `eventposter.phash` — опциональный перцептивный хеш.
- `eventposter.supabase_url/supabase_path` — legacy имена полей для managed-storage URL/путей афиш
  (могут хранить как Supabase, так и Yandex URL для надёжного preview и контролируемой очистки).

## Оценённые вертикальные видео (Yandex CDN)

Каноническая рубрика, thresholds и structured-output contract:
[`video-quality.md`](video-quality.md).

- Producer рассматривает видео только **после** получения хотя бы одного
  подтверждённого event candidate и только при фактическом размере строго
  `< 10 MiB`; non-event/oversize/non-vertical пост не открывает video model call.
- До download source username должен быть явно разрешён в
  `TG_MONITORING_VIDEO_REPUBLICATION_ALLOWED_SOURCES`; wildcard не используется.
- Один unique raw SHA анализируется `gemini-3.1-flash-lite` не более одного раза.
  Accepted и rejected решения немедленно сохраняются в permanent
  **Fernet-encrypted** Yandex sidecar
  `v/analysis/v1/<first2>/<sha256>.json`; публичный bucket отдаёт только
  ciphertext, а cache hit не пересматривает байты.
- Все запросы идут только через `GoogleAIClient` и общий atomic Supabase limiter.
  Dedicated normal pool не включает unrelated keys, fail-closed при отсутствии
  registry/RPC, требует минимум два declared keys и имеет hard ceiling `6`
  video calls на весь monitoring run.
- Только `auto_accept` загружается **прямо из Kaggle** в Yandex Object Storage:
  `v/video/v1/<first2>/<sha256>.<ext>`. Rejected/review bytes в CDN не попадают,
  поэтому Fly не проксирует тяжёлый внутренний трафик.
- Core SQLite разделяет global `video_asset` (описание, search text, intrinsic
  scores, CDN state) и M:N `event_video_link` (event relevance/ranking). Один SHA
  может быть связан с несколькими событиями, а повторный import не создаёт
  дубликаты.
- Static export отдаёт `video_assets[]`, отсортированные по link-level rank,
  для будущего click-to-play UI. Сам UI в этот rollout намеренно не входит.
- После удаления последней event-связи начинается 24-часовой grace period;
  затем cleanup ставит только video binary в durable managed-storage delete
  queue. Analysis sidecar/global SHA result остаётся, а повторная связь до
  flush отменяет stale delete.

## Seed источников (prod/test)

- Канонический список: `docs/features/telegram-monitoring/sources.yml`.
- Автосинхронизация отсутствующих источников выполняется при старте (SQLite seed).
- Ручная синхронизация: `/tg` → «🧩 Синхронизировать источники».

## OCR

- OCR выполняется **внутри Kaggle‑ноутбука** для сообщений с афишами, даже если в тексте поста уже есть описание.
- Результаты OCR сохраняются в `telegram_results.json`:
  - `messages[].posters[].ocr_text` и `messages[].posters[].ocr_title`;
  - агрегированный `messages[].ocr_text` (для удобства дебага).
- Дополнительно Kaggle (best-effort) сохраняет `messages[].links`:
  - URL, найденные в тексте;
  - URL из `MessageEntityTextUrl`/`MessageEntityUrl` (hidden links);
  - URL из кнопок (`reply_markup`) типа “More info”/“билеты”.
  Если extractor вернул широкий landing URL, а hidden/entity link на том же
  домене имеет ticket/registration label и более конкретный path/query, импорт
  должен уточнить `ticket_link` до этой конкретной registration/ticket ссылки.
- В UI (`/events` → Edit) OCR виден в блоке **Poster OCR**.
- Проверка OCR в UI: см. `tests/e2e/features/telegram_monitoring.feature` (сценарий «Полный пользовательский поток мониторинга (UI)»).
- Для каналов с заданным `default_location` это значение считается **сильным prior** (защита от контекстных городов вроде «(г. Москва)» в описании участников), но это не «жёсткий игнор»:
  - если extractor извлёк явную **off-site площадку/адрес**, подтверждённые текстом поста, candidate сохраняет эту площадку вместо слепой подмены на `default_location`;
  - если extractor извлёк город, противоречащий `default_location`, Smart Update делает короткую LLM‑проверку и может переключить `city/location_*` на извлечённые значения (после чего сработает регион‑фильтр и out‑of‑region пост будет корректно отвергнут);
  - если extractor выдал неподтверждённую off-site площадку или prose-фрагмент, сервер больше не заменяет её слепо на `default_location`: записывается только known/reference/text-grounded площадка, иначе candidate fail-closed без публичного venue-default drift.
- Для постов-расписаний (несколько спектаклей в одном сообщении) применяется строгая фильтрация афиш по фактам события; если она неуверенна, но Kaggle уже выдал `event_data.posters` для конкретного события, используется event-level fallback (чтобы не терять релевантную афишу при отсутствующем времени в Telegram).

## Санитаризация и semantic boundary

Custom emoji/transport escapes и URL формы нормализуются детерминированно.
Giveaway, rental, congratulations, promo, recap, date/time и venue detectors
передаются как neutral evidence или verification triggers. Они не удаляют
положительный LLM child и не превращают carrier в `skipped/rejected`. Giveaway
mechanics могут быть исключены из public prose внутри LLM, не теряя отдельно
описанное событие. Objective impossible schema вызывает verification/retry.

Poster/OCR assignment не имеет eventness authority. Если часть media
недоступна, manifest запрещает semantic no-event и сохраняет message для
enrichment; event-level positive children обрабатываются независимо.

## UI (/tg) — настройка источников без «параметров в сообщении»

Формат вида `@channel trust=low` поддерживается как расширенный, но операторский флоу — через кнопки:

- `/tg` → `📋 Список источников`
  - `Trust → ...` — циклически: low → medium → high
  - `📍 Локация → ...` — задать/очистить `default_location`
  - `🎟 Ticket → ...` — задать/очистить `default_ticket_link`
  - `🎪 Фестиваль → ...` — пометить источник как фестивальный и задать серию (очистка через `-`)
  - `✅ Принять подсказку` — появляется, если `festival_series` пустой и есть `suggested_festival_series`; копирует suggested в `festival_series` и включает `festival_source=1`.
  - `🌐 Suggested website` — ссылка на suggested `website_url` (без автосохранения в фестивальные сущности).
  - `♻️ Сбросить отметки ...` — очистить `telegram_scanned_message` и `last_scanned_message_id` для перескана
  - `🗑️ Удалить ...` — удалить источник

Если источники были удалены массово в тестовой БД, восстановление можно сделать без UI:
- `python scripts/restore_telegram_sources.py --db <DB_PATH> @username1 @username2 ...` (не удаляет/не трогает существующие настройки, только upsert + `enabled=1`).

- `/tg` → `♻️ Импорт из JSON`
  - После выбора файла: `Импорт (обычно)` или (только в `DEV_MODE=1`) `DEV: Recreate + Reimport`.
  - `DEV: Recreate + Reimport` использует 2-step confirm и очищает события/marks перед повторным импортом для детерминированного отладочного прогона Smart Update.

Канонический список источников (prod/test) и их настроек (trust/festival/defaults): `docs/features/telegram-monitoring/sources.md`.

## ENV

Минимум:

- `ENABLE_TG_MONITORING=1`
- `TG_MONITORING_TIME_LOCAL=23:40`
- `TG_MONITORING_TZ=Europe/Kaliningrad`
- `TELEGRAM_AUTH_BUNDLE_S22`, `TG_API_ID`, `TG_API_HASH`
- `GOOGLE_API_KEY`
- `KAGGLE_USERNAME`

Выбор auth bundle для мониторинга:

- по умолчанию используется `TELEGRAM_AUTH_BUNDLE_S22`;
- для ручной отладки можно явно переопределить источник через `TG_MONITORING_AUTH_BUNDLE_ENV=<ENV_KEY>` (например `TELEGRAM_AUTH_BUNDLE_E2E`).
- даже при явном override оператор обязан держать session boundary: remote run не должен стартовать параллельно с другим remote Telegram kernel, а shared guard намеренно переводит такие коллизии в `skipped`, чтобы не доводить до `AuthKeyDuplicatedError`.

Дополнительно:

- Yandex Object Storage (primary poster backend в текущем rollout):
  - `YC_SA_BOT_STORAGE`, `YC_SA_BOT_STORAGE_KEY`
  - optional: `YC_STORAGE_BUCKET` (default `kenigevents`), `YC_STORAGE_ENDPOINT` (default `https://storage.yandexcloud.net`)
- Supabase (legacy poster fallback и глобальный rate-limit RPC, если включено):
  - `SUPABASE_URL`, `SUPABASE_KEY` (или `SUPABASE_SERVICE_KEY`), `SUPABASE_SCHEMA`, `SUPABASE_DISABLED`
  - bucket'и: legacy `SUPABASE_BUCKET` (default `events-ics`); плановое разделение: `SUPABASE_ICS_BUCKET`, `SUPABASE_MEDIA_BUCKET`
    (см. `docs/operations/supabase-storage.md`)
- Poster fallback настройка (Kaggle):
  - `TG_MONITORING_POSTERS_SUPABASE_MODE=off|fallback|always` (default `always`)
  - `TG_MONITORING_POSTERS_PREFIX` (default `p`)
  - `TG_MONITORING_POSTERS_WEBP_QUALITY` (default `82`)
- Оценка и CDN видео (Kaggle + import):
  - `TG_MONITORING_VIDEOS_SUPABASE_MODE=off|always` (default `always`)
  - `TG_MONITORING_VIDEO_MAX_MB` (default `10`; сравнение строго `<`)
  - `TG_MONITORING_VIDEO_MODEL` (default `gemini-3.1-flash-lite`)
  - `TG_MONITORING_VIDEO_GOOGLE_KEY_ENVS` (default `GOOGLE_API_KEY3,GOOGLE_API_KEY5`)
  - `TG_MONITORING_VIDEO_MAX_MODEL_CALLS_PER_RUN` (default и hard maximum `6`
    физических provider sends; app/SDK retry, model fallback и provider-429
    rotation отключены; legacy Google SDK fail closed)
  - `TG_MONITORING_VIDEO_REPUBLICATION_ALLOWED_SOURCES` (обязательный явный
    comma-separated allowlist без wildcard)
  - `TG_MONITORING_VIDEO_ANALYSIS_CACHE_KEY` (обязательный постоянный Fernet key)
  - `TG_MONITORING_VIDEO_ANALYSIS_VERSION`
  - `VIDEO_ASSET_ORPHAN_GRACE_HOURS` (default и production minimum `24`, server cleanup)
  - portrait envelope: `TG_MONITORING_VIDEO_MIN_WIDTH_HEIGHT_RATIO`,
    `TG_MONITORING_VIDEO_MAX_WIDTH_HEIGHT_RATIO`, minimum width/height and
    duration envs documented in `.env.example`
- `TG_MONITORING_KERNEL_REF`
- `TG_MONITORING_KERNEL_PATH`
- `TG_MONITORING_CONFIG_CIPHER`
- `TG_MONITORING_CONFIG_KEY`
- `TG_MONITORING_POLL_INTERVAL`
- `TG_MONITORING_LOCAL_RESULTS_GLOB` — glob для поиска локальных результатов import-only кнопки (по умолчанию `tg-monitor-*/telegram_results.json` в системном temp-dir; в UI показываются 4 последних).

Таймаут ожидания Kaggle (на стороне бота, polling):

- `TG_MONITORING_TIMEOUT_MODE=dynamic|fixed` (default `dynamic`)
- `TG_MONITORING_TIMEOUT_MINUTES` — базовый/минимальный таймаут (default `90`)
- `TG_MONITORING_TIMEOUT_BASE_MINUTES` — базовая прибавка для dynamic (default `15`)
- `TG_MONITORING_TIMEOUT_PER_SOURCE_MINUTES` — baseline прибавка на источник (default `3.64`)
- `TG_MONITORING_TIMEOUT_SAFETY_MULTIPLIER` — safety multiplier для baseline (default `1.3`)
- `TG_MONITORING_TIMEOUT_MAX_MINUTES` — верхняя граница для dynamic (default `360`)

В режиме `dynamic` итоговый таймаут считается так:
`max(TG_MONITORING_TIMEOUT_MINUTES, TG_MONITORING_TIMEOUT_BASE_MINUTES + ceil(sources * TG_MONITORING_TIMEOUT_PER_SOURCE_MINUTES * TG_MONITORING_TIMEOUT_SAFETY_MULTIPLIER))`,
но не больше `TG_MONITORING_TIMEOUT_MAX_MINUTES`.

Скан лимиты (в Kaggle):

- `TG_MONITORING_LIMIT` — максимум сообщений **на источник** (по умолчанию 50).
- `TG_MONITORING_DAYS_BACK` — глубина по дням (по умолчанию 3).

Live E2E multi-source (VK+TG): `tests/e2e/features/multi_source_vk_tg.feature` (рекомендуемо запускать с `TG_MONITORING_LIMIT=10`).
- `TG_MONITORING_DAYS_BACK` — сколько дней сканировать назад. Для E2E держите дефолт `3`; для старых кейсов не расширяйте окно глобально, а добирайте конкретный `message_id` точечно.
- `TG_MONITORING_LIMIT` — лимит сообщений на источник за запуск.
- `TG_MONITORING_MEDIA_MAX_PER_SOURCE` — лимит скачиваний медиа на источник (снижает шанс FloodWait).
- `TG_MONITORING_MEDIA_DELAY_MIN/MAX` — дополнительные задержки перед скачиванием медиа (снижает шанс FloodWait).
- `EVENT_TOPICS_LLM=gemma` — чтобы классификация тем не использовала 4o (Gemma-only).
- `EVENT_TOPICS_MODEL` — модель Gemma для классификации тем (по умолчанию `TG_MONITORING_TEXT_MODEL`).
- `TELEGRAPH_TOKEN_FILE` — путь к токену Telegraph. В dev среде автоматически фолбэкается на `artifacts/run/telegraph_token.txt`, если `/data` недоступен на запись.

## Контракт результата

Сервер принимает `telegram_results.json`:

- `schema_version=2`: `messages[]` + top-level `sources_meta[]`; каждый новый message содержит `source_parse_decision` и `evidence_manifest`. Это единственный канонический producer contract.
- `schema_version=1` (legacy): только fail-closed reader для диагностики/replay ранее сохранённых `messages[]` без `sources_meta`; positive children можно адаптировать, zero-event/technical result не подтверждается и cursor не продвигается.

- Producer (Kaggle): `kaggle/TelegramMonitor/telegram_monitor.py` -> sync в `telegram_monitor.ipynb`
- Consumer (server): `source_parsing/telegram/handlers.py`

## FloodWait (Telegram rate limits)

Если в Kaggle логах появляется `FloodWaitError` или строки вида `Sleeping for Xs on GetHistoryRequest flood wait`, Telegram ограничил скорость запросов.

Типовые причины:

- Слишком большой объём сканирования: много источников и/или большой `TG_MONITORING_LIMIT`, `TG_MONITORING_DAYS_BACK` (особенно после очистки отметок мониторинга).
- Слишком агрессивные задержки (`TG_MONITORING_DELAY_*`, `TG_MONITORING_SOURCE_PAUSE_*`).
- Параллельные запуски мониторинга (ручной и scheduled) с одной и той же Telegram-сессией.

Митигации (ENV, пробрасываются в Kaggle):

- Увеличить “human-like” задержки: `TG_MONITORING_DELAY_MIN/MAX`, `TG_MONITORING_SOURCE_PAUSE_MIN/MAX`.
- Ограничить и замедлить скачивание медиа (частая причина FloodWait): `TG_MONITORING_MEDIA_MAX_PER_SOURCE`, `TG_MONITORING_MEDIA_DELAY_MIN/MAX`.
- Настроить поведение Telethon при FloodWait:
  - `TG_MONITORING_FLOOD_SLEEP_THRESHOLD` (по умолчанию 600) — авто-sleep при FloodWait до N секунд.
  - `TG_MONITORING_FLOOD_WAIT_MAX` (по умолчанию 1800) — максимум ожидания на один FloodWait.
  - `TG_MONITORING_FLOOD_MAX_RETRIES` (по умолчанию 4) — сколько раз подряд терпеть FloodWait на одном участке.
  - `TG_MONITORING_FLOOD_WAIT_JITTER_MIN/MAX` — небольшой джиттер к ожиданию.

Примечание: на сервере есть lock, который не даёт запустить два мониторинга одновременно в одном процессе (manual vs scheduler), но лучше всё равно избегать ручных запусков рядом с scheduled окном.

## E2E и старые посты

- Для регрессий по конкретному старому посту используйте point-fetch по `message_id` вместо расширения `TG_MONITORING_DAYS_BACK`.
- Базовый E2E профиль: `TG_MONITORING_DAYS_BACK=3`, умеренный `TG_MONITORING_LIMIT`.
- Причина: широкий перескан резко увеличивает время прогона, FloodWait-риск и количество лишних запросов в Gemma (лимиты ограничены).

## Очистка (DB + Supabase)

- Ежедневная очистка удаляет события, завершившиеся более 7 дней назад (по `end_date`, либо по `date` если `end_date` пуст).
- В рамках той же очистки (best-effort) удаляются связанные объекты из Supabase Storage:
  - ICS файлы события;
  - fallback афиши по `eventposter.supabase_path`.

## Acceptance (Gherkin)

Канонические сценарии (UI): `tests/e2e/features/telegram_monitoring.feature`.

Если нужно добавить/уточнить сценарий — правим `.feature` и шаги в `tests/e2e/features/steps/bot_steps.py`.

## Отложенное обновление страниц

Telegram Monitoring может обновлять/создавать много событий за один запуск, поэтому обновления month/weekend страниц делаются **отложенно и накопительно** (debounce 15 минут после последнего изменения). Каноническое описание механизма — в `docs/features/smart-event-update/README.md` («Отложенное обновление страниц (debounce)»).

### Source-grounded admission links (2026-07-14)

Telegram Monitor may emit `ticket_link` only when the source labels the URL as registration,
ticket purchase, booking, or an equivalent attendee-admission action. A sole external URL is
not evidence of admission. Links labelled donation/support/help/fundraising (including a bank
recipient/payment page) must remain ordinary source links unless the same source explicitly
labels that exact URL as attendee payment. The server import repeats this fail-closed check so
a producer mistake cannot turn organizer donations into “registration”. The generated Kaggle
notebook must remain synchronized with `telegram_monitor.py` after this contract changes.
