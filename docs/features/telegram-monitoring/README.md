# Telegram Monitoring

Ежедневный мониторинг публичных Telegram‑каналов/групп с автоматическим импортом событий в БД бота через Smart Event Update.

## Что делает

- По расписанию запускает Kaggle‑kernel `TelegramMonitor`.
- Kaggle читает сообщения источников, делает OCR и извлекает события; афиши по умолчанию грузятся в managed storage:
  - **Yandex Object Storage** (`https://storage.yandexcloud.net/<bucket>/<path>`), если в runtime есть `YC_SA_BOT_STORAGE[_KEY]`;
  - legacy **Supabase Storage** остаётся fallback/backend для старых URL и для окружений без `YC_*`;
  - Catbox используется только в `fallback/off` режимах.
  - Инвариант: extractor **не должен придумывать дату события** из даты публикации поста.
    - Дата/период должны быть явно в тексте/афише (или в виде относительных слов типа «сегодня/завтра», которые разрешено резолвить от даты поста).
    - Посты вида `open call` / «конкурсный отбор» / «приём заявок» считаются **не‑событиями** (это не «куда пойти») и должны отфильтровываться на стороне Kaggle (server-side guard существует как safety-net).
    - Посты‑отчёты о уже прошедшем мероприятии (`приняли участие`, `встреча прошла`, `педагоги отметили`, `администрация выразила благодарность`) не должны создавать event card; серверный Smart Update режет их как `skipped_non_event:completed_event_report`, если в тексте нет invite/registration/ticket сигналов.
    - Официальные уведомления администрации Калининграда о **разводе/разводке мостов** считаются событиями городской повестки: `@klgdcity` входит в мониторинг, Gemma 4 извлекает их как `Развод мостов`; если общий extractor вернул пустой или непригодный bridge-ответ, запускается узкий Gemma rescue-pass, и только затем структурный fallback сохраняет карточки по явно grounded дате/тексту. Для источника также включён `bridge_notice_daily`: после успешного `event_id` сервер отправляет notice в `/daily` каналы.
- Для афиш (постеров) по умолчанию использует **managed storage** для стабильных URL:
  - `TG_MONITORING_POSTERS_SUPABASE_MODE=always` (default): upload в managed storage всегда включён; Catbox используется только если storage недоступен;
  - `fallback`: приоритет Catbox, managed storage — только если Catbox‑загрузка не удалась;
  - `off`: только Catbox.
- Если настроен Yandex runtime, bucket для афиш берётся из `YC_STORAGE_BUCKET` (default `kenigevents`).
- Если Yandex не настроен, legacy Supabase fallback берёт bucket из `SUPABASE_MEDIA_BUCKET` (не из `SUPABASE_BUCKET`), чтобы медиа не смешивались с ICS.
- При загрузке афиш в managed storage:
  - объект сохраняется **в WebP** (только WebP, без JPEG) для экономии объёма;
  - ключ объекта content‑addressed по перцептивному хешу (dHash16), чтобы одно и то же изображение (даже при разном разрешении/реэнкоде) не загружалось повторно:
    - `supabase_path`: `<prefix>/dh16/<first2>/<dhash>.webp` (prefix по умолчанию `p`, настраивается через `TG_MONITORING_POSTERS_PREFIX`);
    - качество WebP: `TG_MONITORING_POSTERS_WEBP_QUALITY` (default `82`).
- Сервер скачивает `telegram_results.json` и импортирует события через Smart Update:
  - создаёт новые события;
  - мерджит существующие;
  - добавляет источники в `event_source`.
  - обрабатывает Telegram-посты в хронологическом порядке (старые → новые), чтобы старые посты не перезатирали более свежие обновления того же события.
  - во время импорта в `/tg` показывает live-прогресс по каждому посту (`X/Y`, ссылка на пост, `Smart Update: ✅/🔄`, `event_ids`, иллюстрации, `took_sec`), чтобы оператор видел, что импорт не завис.
  - отправляет подробный блок `Smart Update (детали событий)` сразу после обработки конкретного поста (не дожидаясь завершения всего импорта).
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
- `linked_source_urls` также обогащают факты: для single-event постов сервер (best-effort) скачивает текст linked Telegram постов (payload-first, затем `t.me/s/...`) и прогоняет Smart Update по каждому linked источнику, чтобы в source log были факты по всем ссылкам.
- Перед вызовом Smart Update candidate build дополнительно проверяет площадку по `source_text` и OCR афиши:
  - если extractor отдал venue, которого нет в тексте/OCR, а в том же посте явно виден другой venue, сервер подменяет extractor guess на подтверждённый venue;
  - если producer уже пометил venue как подозрительный и LLM-review оставил поле пустым, сервер может восстановить площадку из `default_location`, `docs/reference/locations.md` / `docs/reference/location-aliases.md`, адреса или OCR/text fallback; это reference/grounding layer, а не semantic phrase dictionary.
  - если extractor разложил соседнюю прозу между `location_name` и `location_address`, сервер отбрасывает prose-like address-фрагмент и восстанавливает структурные `location_name/location_address/city` из единственной известной площадки в исходном тексте/алиасах.
- если афиша явно содержит несколько дат/времён одного и того же события (например «12 июня 19:00» и «13 июня 15:00»), а extractor их схлопнул в одну дату, сервер (best-effort) расширяет карточку до нескольких событий по OCR афиши.
  - сохраняет `source_title`/`sources_meta[].title` в `telegram_source.title` (человекочитаемое название канала/группы).
  - сохраняет метаданные источника из `sources_meta[]`: `about`, `about_links_json`, `meta_hash`, `meta_fetched_at`.
  - сохраняет подсказки серии/сайта (`suggested_*`) в `telegram_source` и показывает их в UI `/tg` отдельной кнопкой принятия (без автоперезаписи ручного `festival_series`).
  - если Kaggle вернул неполную карточку события (например только `DD.MM | Title` без `location_name`/`ticket_link`),
    сервер делает best-effort восстановление из текста сообщения:
    - локация/адрес: по строкам вида `📍 ...` или `Площадка, улица/дом ...`;
    - контакт/ссылка для записи:
      - `@username` в контексте «запись/бронь/напиши» → `ticket_link=https://t.me/username`;
      - если в Kaggle‑payload пришли `messages[].links` (кнопки/hidden URL entities типа “More info”, “билеты”, “здесь”) и `ticket_link` пустой, сервер может best-effort выбрать один «сильный» registration/ticket URL.
    - заголовок: если extractor вернул мусор вроде `(4 места)`, заголовок берётся из первой содержательной строки поста.
- В Kaggle используются только модели Gemma (текст/vision); 4o там не участвует.
- Актуальный Kaggle runtime для LLM-stage теперь строится из [telegram_monitor.py](/workspaces/events-bot-new/kaggle/TelegramMonitor/telegram_monitor.py:1), а [telegram_monitor.ipynb](/workspaces/events-bot-new/kaggle/TelegramMonitor/telegram_monitor.ipynb:1) синхронизируется из него перед push.
- Kaggle producer переведён на shared `GoogleAIClient`/`google_ai` runtime с native `response_schema` для Gemma 4 structured stages вместо direct `google.generativeai` calls.
- Primary Kaggle key isolation для этого surface: `GOOGLE_API_KEY3` / `GOOGLE_API_LOCALNAME3`. Если `GOOGLE_API_KEY3` ещё не зарегистрирован в Supabase quota registry, gateway не должен молча брать общий key pool: он переходит на process-local limiter и всё равно вызывает provider через выбранный `GOOGLE_API_KEY3`.
- Kaggle secrets для Telegram Monitoring не передают unrelated `GOOGLE_API_KEY*` pools: `GOOGLE_API_KEY` внутри notebook является legacy alias на выбранный monitoring key, а дефолтный fallback env тоже указывает на `GOOGLE_API_KEY3`.
- Provider calls ограничены таймаутом: `TG_MONITORING_LLM_TIMEOUT_SECONDS` (default `45`) выставляет `GOOGLE_AI_PROVIDER_TIMEOUT_SEC`, чтобы retryable Gemma 4 `500/504` или зависшие calls fail-open на уровне поста/стадии, а не съедали весь Kaggle window.
- Дефолтные Kaggle text/vision модели для этого surface: `models/gemma-4-31b-it`.
- `Gemma 4` prompt hardening для source metadata запрещает сохранять social/profile links (`Telegram`, `Telegra.ph`, `Instagram`, `VK`, `YouTube`, `Linktree`, `Taplink`, `Boosty`, `Patreon`) как `suggested_website_url`; туда должен попадать только standalone website самого фестиваля/проекта/источника.
- `Gemma 4` extract prompt для Telegram text+OCR явно требует мерджить venue/date/time facts из OCR в event object, заполнять `location_name`/`location_address`, избегать whitespace-only strings и не придумывать `end_date` для single-date событий.
- `Gemma 4` extract prompt различает явные work-hours notices и события в музеях/библиотеках: `график/режим/часы работы`, `санитарный день`, `не работает/закрыто` возвращают `[]`, но лекции, шоу, мастер-классы, экскурсии и фестивальные слоты с датой/временем должны извлекаться даже при venue/address словах вроде `Библиотека ...` или `Музейная аллея`.
- `Gemma 4` producer-level contract for `location_name`: поле должно быть реальным venue/place name, а не соседней прозой, биографией спикера, schedule commentary, film metadata, ticket instruction или описанием события. Для расписаний schedule-rescue передаёт в каждый day-block prompt общий контекст поста, чтобы хвостовые venue-линии вроде `📍Остров Канта` были видны LLM для всех строк расписания.
- `Gemma 4` venue-review stage: если extracted `location_name` имеет широкий плохой shape (слишком длинная фраза, schedule row, короткий section label вроде `Кинозал:`) или источник имеет `default_location`, Kaggle делает отдельный LLM-pass только по `location_name/location_address/city` на original message + OCR + source context + `default_location`. Детерминированная часть решает только “нужна проверка”; смысловую площадку выбирает LLM. Это canonical fix path для произвольных фрагментов вроде случайно попавшей фразы из соседнего предложения.
- Второй hardening wave по реальным Gemma 4 outputs (`run_id=48fa98294333486d94dd0e14785d774f`) точечно лечит наблюдавшиеся регрессии: prompt явно запрещает inline `//`/`#` комментарии и markdown (`**`/`__`) внутри JSON-значений, запрещает ghost-события с пустыми `title` и `date`, запрещает литерал `"unknown"` в любом поле, требует lowercase русский `event_type` (`концерт`/`выставка`/`лекция`/...) вместо английских токенов вроде `"exhibition"`/`"meetup"`, не даёт копировать город из parenthetical origin notes (`"(Санкт-Петербург)"` в описании коллекции ≠ место события), и скипает fundraiser/video-recap/book-review посты без приглашения на будущее событие. Те же правила дублированы в exhibition fallback prompt и в json-fix retry, чтобы retry не пропускал те же классы ошибок.
- Schema `EVENT_ARRAY_SCHEMA` получил `description` по ключевым полям (`title`/`city`/`event_type`/`date`/`time`/`location_*`), которые Gemini structured output уважает как дополнительный канал подсказок; hard constraints остаются в prompt text, чтобы не нарушать schema-совместимость Gemma 4.
- Добавлен LLM-output safety-net `_sanitize_extracted_events` (детерминированный post-LLM хелпер без semantic rewriting): срезает leaked inline `//`/`#` хвосты, снимает оставшиеся markdown-маркеры, нормализует placeholder-литералы (`unknown`/`n/a`/`none`) до пустой строки и дропает ghost-события, где пусты и `title`, и `date`. Это не заменяет LLM — просто не даёт известным Gemma 4 failure modes доехать до Smart Update и Telegraph.
- Iter2 safety-net extensions по результатам local-only Gemma 4 eval ([artifacts/codex/tg-g4-opus-local-eval/](/workspaces/events-bot-new-tg-g4-sU9xCP/artifacts/codex/tg-g4-opus-local-eval/), `GOOGLE_API_KEY2`, **не production-equivalent** — production Telegram Monitoring остаётся на `GOOGLE_API_KEY3`, который в локальной среде отсутствует): `_sanitize_extracted_events` теперь дополнительно стрипает HTML-подобные теги (`</strong>`, `<br>`, `<em>` — Gemma 4 изредка эмитит их внутрь structured JSON string values) и отрезает trailing meta-commentary вида `own title:` / `own id:` / `own field:` (наблюдались в iter1 local eval у `@barn_kaliningrad/971`). Это syntax-only cleanup и не подменяет LLM-решение о смысле события.
- Следующий точечный prompt pass для exhibition-постов добавил title/cardinality guardrail: если пост говорит о разделе внутри выставки (`"в разделе X на выставке Y"`), title должен оставаться названием основной выставки `Y`, а не subsection label `X`; если один и тот же пост анонсирует и открытие выставки, и её run-window, по умолчанию лучше вернуть одну exhibition-card с opening datetime + `end_date`, а не два раздельных attendable события. Local-only re-eval на `GOOGLE_API_KEY2` вернул positive control `TG-G4-EVAL-10` из `2` событий обратно к `1`.
- LLM-first local tuning pass по `TG-G4-EVAL-01..10` добавил staged Gemma prompts вместо semantic regex/fallback extraction: single invited lecture rescue, named ongoing exhibition rescue, museum spotlight rescue/repair и chunked schedule rescue. На полном локальном `extract_events` пути это закрыло `TG-G4-EVAL-02` (`Космос красного`, без `unknown`), `-03` (лекция Amber Museum с OCR date/time), `-04` (museum spotlight как exhibition-card), `-07` (одна лекция без дубля/venue-only row) и `-10` (positive-control exhibition остаётся одной строкой). `TG-G4-EVAL-08` теперь извлекает реальные zoo schedule rows без garbage placeholder row, но из-за provider `500`/timeout на отдельных schedule chunks recall может быть частичным; production-equivalent smoke через `GOOGLE_API_KEY3` всё ещё обязателен.
- Controlled Kaggle smoke `tg_g4_key2_as_key3_forced_eval_70b4fc14` и focused extraction-only smoke `tg_g4_key2_as_key3_focused_eval_90e527f5` запускались с локальным `GOOGLE_API_KEY2`, замапленным в env-name `GOOGLE_API_KEY3` по тому же encrypted-dataset Kaggle path. Smoke подтвердил отсутствие старых leak/ghost/unknown/event_type drift классов на `@barn_kaliningrad/971`, `@domkitoboya/3170`, `@kldzoo/7089`, `@koihm/5505` и `@kaliningradartmuseum/7902`, но выявил новый prompt-contract риск: когда OCR не даёт дату/время, одиночная лекция `@ambermuseum/5600` не должна датироваться `message_date`. Prompt теперь явно запрещает использовать `message_date` как fallback event date для не-выставочных single events; он остаётся только контекстом для явных relative anchors (`сегодня`/`завтра`/`послезавтра`) и для museum/exhibition as-of merge cases. Дополнительный post-LLM guardrail `_lacks_supported_non_exhibition_date` только enforcing-уровня: он не извлекает и не переписывает смысл, а отбрасывает не-выставочные rows без поддержанной даты или с подставленным `message_date` без anchor.
- Forced A/B regression gate на тех же 16 постах из ночного prod output `095e32fd497442258fb5675f65f43731` показал: legacy Gemma 3 notebook (`g3cmp095e0785bc`) извлёк `10` событий и имел `empty_date=1`, `english_event_type=4`; промежуточный Gemma 4 producer (`abfull095ef8e2d2`) извлёк `12` событий без leak/ghost/empty-date/bad-date/English-city smell-классов, но с quality regression на `@signalkld/10512`, где poster OCR heading `НАЧАЛО В 19:00` стал `title` вместо caption event name `Второй Большой киноквиз`. Prompt теперь явно закрепляет title-audit: OCR service headings/date/time/price/venue labels не должны заменять named event из message text, а используются только для date/time/venue/ticket facts. Targeted smokes `sig10512c3c25072`, `sig10512b518272c` и `sig10512r6cb27e5` показали, что prompt-only guidance и full-event repair было недостаточно, поэтому добавлен компактный LLM title-review stage: deterministic код только замечает service-heading title, а Gemma 4 возвращает replacement `title/event_type/search_digest` по original caption+OCR; event count/order сохраняются. Targeted validation `sig10512u8402a5b` подтвердил исправление (`title="Второй Большой киноквиз"`, `event_type="квиз"`), а полный повтор gate `abfinal095edeb15` извлёк `14` событий на тех же 16 постах и дал `0` smell-регрессий по проверяемым классам: thought/markdown leak, ghost row, empty title/date, bad date shape, English city/event_type, `unknown` literal и service-heading title. Это regression evidence для ветки hardening; production deploy/catch-up должен отдельно подтвердить импортный контур.
- Закрыт latent regex-баг, появившийся в первом Gemma 4 migration commit: `open_call_re`/`anchor_re` гварды использовали double-escaped `\\b`/`\\s`/`\\d`/`\\w` в raw string и поэтому никогда не матчили реальный текст; это роняло `has_anchor=False` на большинстве постов и приводило к тому, что валидные события с `date == message_date` тихо отбрасывались. Теперь гварды реально фильтруют open-call посты и правильно распознают `сегодня`/`завтра`/`DD.MM`/`DD месяца` anchors.
- Для следующего prompt-quality pass собран компактный eval pack из реального full-run evidence: [tests/fixtures/telegram_monitor_gemma4_eval_pack_2026_04_23.json](/workspaces/events-bot-new-tg-g4-sU9xCP/tests/fixtures/telegram_monitor_gemma4_eval_pack_2026_04_23.json). В нём 10 именованных кейсов из `run_id=48fa98294333486d94dd0e14785d774f`: thought leak + ghost row (`@barn_kaliningrad/971`), `unknown` placeholders, city drift (`Saint Petersburg` вместо Калининграда), English `event_type`, markdown tail, retrospective/non-event posts, same-day anchor regression, schedule post with garbage placeholder row и один positive control. Этот fixture нужен для A/B prompt tuning и второго Opus-pass, чтобы оценивать изменения на одной и той же базе.
- Kaggle notebook теперь embed-ит `google_ai` sources прямо в generated `.ipynb`, а runner дополнительно ищет bundled package в kernel root, `/kaggle/working` и `/kaggle/input`; это нужно, потому что plain extra files рядом с notebook не гарантированно попадают в Kaggle runtime.
- Generated Kaggle `.ipynb` вырезает script-only tail `asyncio.run(main())` / `already running event loop` guard и запускает `main()` отдельной notebook-cell через `nest_asyncio`; иначе Papermill падает в уже запущенном event loop.

Live validation (`2026-04-22`):

- Run `tg_g4_live_smoke_subset_20260422g` на Kaggle `zigomaro/telegram-monitor-bot` завершил producer stage и выгрузил `telegram_results.json` (`schema_version=2`, `sources_total=3`, `messages_scanned=2`, `messages_with_events=1`, `events_extracted=4`).
- Kaggle log подтвердил `text_model=models/gemma-4-31b-it`, `vision_model=models/gemma-4-31b-it`, `requested_model/provider_model/invoked_model=models/gemma-4-31b-it`; Gemma 3 fallback не использовался.
- Server import/recovery по этому output зафиксирован в `ops_run`: `id=797`, `trigger=recovery_import`, `status=success`, `errors_count=0`; повторный import-only `id=798` тоже завершился `success`, `errors_count=0`.
- Scheduled full run `48fa98294333486d94dd0e14785d774f` после key-pool hardening прошёл через Kaggle на 45 источниках: `messages_scanned=177`, `messages_with_events=69`, `events_extracted=84`; server recovery import `ops_run id=803` завершился `success`, `errors_count=0`, `events_imported=14`.
- Full-run log подтвердил `GOOGLE_API_KEY3`, отсутствие `GOOGLE_API_KEY2`, отсутствие `gemma-3`, `requested_model/provider_model/invoked_model=models/gemma-4-31b-it`, но также показал, что старый `180s` provider timeout слишком длинный для scheduled window; default снижен до `45s`.
- Post-timeout smoke `tg_g4_45s_smoke_20260423a` завершился без recovery через primary `ops_run id=807` (`status=success`, `sources_scanned=3`, `messages_processed=3`, `messages_with_events=2`, `errors_count=0`, `duration_sec=279.22`). Log evidence: `GOOGLE_API_KEY3`, `GOOGLE_API_KEY2=0`, `gemma-3=0`, `Traceback=0`, `AuthKeyDuplicatedError=0`; два `45s` timeout на source metadata fail-open и не сорвали run.

## Multi-event посты (несколько событий в одном сообщении)

Требование: если один Telegram‑пост содержит **несколько будущих событий**, мониторинг должен:

- создать/обновить **каждое** событие отдельной записью (по `title + date + time + location` якорям);
- **не создавать** события, которые уже в прошлом (по дате);
- перед созданием нового события сначала попытаться найти матч в БД, чтобы не плодить дубли;
- на странице Telegraph конкретного события не оставлять строки расписания/названия других событий из того же поста.

Проверки этого поведения зафиксированы в E2E сценариях: `tests/e2e/features/telegram_monitoring.feature`.

## Почему «извлечено много, а создано мало»

В отчёте мониторинга `Событий извлечено` — это количество кандидатов, которые Kaggle нашёл в сообщениях.
Дальше на сервере часть кандидатов **не импортируется** и попадает в `Пропущено`, например:

- событие уже в прошлом (по `date`, выставки/ярмарки — по `end_date` если он есть);
- не хватает якорей для Smart Update (чаще всего нет `location_name` даже после восстановления из текста);
- Smart Update нашёл матч, но изменений нет (`skipped_nochange`).

Чтобы отчёт был объясним, бот выводит разбиение `Пропущено` по основным причинам (прошедшие/невалидные/без изменений и т.д.).
Дополнительно бот отправляет оператору список **пропущенных/частично обработанных постов** (с ссылкой на пост, кратким фрагментом текста и breakdown причин), чтобы можно было вручную проверить “почему не импортировалось”.

Также мониторинг может сканировать сообщения «на несколько дней назад» (для обновления просмотров/лайков).
Такие сообщения **не прогоняются через Smart Update повторно** (идемпотентность по `message_id`), а учитываются как `Посты только для метрик` в отчёте.

Исключение из этой идемпотентности: legacy/incomplete scan rows, где `telegram_scanned_message.status`
равен `skipped`/`partial`/`error`, `events_extracted > events_imported`, `error` пустой, в новом payload есть
будущее/актуальное событие и `event_source` ещё не содержит URL поста. Такие строки считаются
неполной попыткой импорта и переобрабатываются, чтобы валидный Telegram-пост не залипал навсегда как
`metrics_only`. Новые неполные пропуски сохраняют компактный `skip_breakdown` в `telegram_scanned_message.error`;
это делает намеренные/постоянные skip-решения диагностируемыми и не запускает бесконечный retry.
Regression contract: `docs/reports/incidents/INC-2026-04-27-tg-monitoring-sticky-skipped-post.md`.

Для Telegram payload с противоречивыми price/free полями сервер трактует `ticket_price_min=0`
и пустой/нулевой `ticket_price_max` как бесплатный вход даже если extractor ошибочно вернул
`is_free=false`. Это защищает посты, где Telegram custom emoji `🆓` были удалены при экспорте,
но LLM всё же сохранил числовой факт нулевой цены.

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

Канонический список источников (prod/test) и их настройки: `docs/features/telegram-monitoring/sources.yml` (см. также `docs/features/telegram-monitoring/sources.md`).

## Основные модули

- `source_parsing/telegram/commands.py` — UI/команды `/tg`.
- `source_parsing/telegram/service.py` — оркестрация Kaggle и загрузка результатов.
- `source_parsing/telegram/handlers.py` — разбор `telegram_results.json`.
- `smart_event_update.py` — Smart Event Update.

## Надёжность Kaggle polling

- Статус Kaggle kernel опрашивается с интервалом `TG_MONITORING_POLL_INTERVAL` (по умолчанию 30s) до динамического лимита ожидания (или фиксированного, если включён `fixed` mode).
- Транзиентные ошибки сети/SSL при опросе Kaggle API (например `UNEXPECTED_EOF_WHILE_READING`) **не валят прогон**: мониторинг продолжает опрос до получения `COMPLETE/FAILED` или таймаута, а в UI этап показывается как «временная ошибка сети».
- Перед `push` сервер теперь дополнительно проверяет общий `kaggle_registry`: если другой remote Telegram Kaggle job (`guide_monitoring`, `tg_monitoring`, `telegraph_cache_probe`) ещё жив или его status lookup закончился неопределённо, `tg_monitoring` обязан завершиться `skipped` с `remote_telegram_session_busy`, а не запускать вторую удалённую Telethon session поверх той же auth key.
- Отменённые Kaggle runs со статусом `CANCEL_ACKNOWLEDGED` считаются terminal для shared remote Telegram session guard: такой job не должен блокировать следующий компенсирующий `/tg` catch-up после ручной отмены.

## Recovery после рестарта бота

- `tg_monitoring` регистрирует Kaggle kernel в общем `kaggle_registry` сразу после успешного `push`.
- Scheduler `kaggle_recovery` на старте/по интервалу проверяет незавершённые `tg_monitoring` kernels:
  - если kernel ещё работает в Kaggle, запись остаётся в реестре и будет проверена позже;
  - если kernel завершился `complete`, бот заново скачивает `telegram_results.json` из Kaggle и запускает обычный server-import;
  - если kernel рано сообщает `failed/error/cancelled`, запись не удаляется мгновенно: recovery ещё несколько часов перепроверяет output, потому что Kaggle иногда дозавершает `telegram_results.json` уже после раннего terminal-status; только после истечения `TG_MONITORING_RECOVERY_TERMINAL_GRACE_MINUTES` (default `360`) запись удаляется как окончательно невосстановимая.
- Локальный poll-timeout в сервере тоже не считается окончательной потерей результата: recovery продолжает проверять kernel в фоне и подхватывает поздно дозавершившийся output без ручного пересканирования.
- Это значит, что для восстановления **не требуется** сохранять `telegram_results.json` в `/data`: источником истины остаётся Kaggle output, а локальный `/tmp` используется только как временный download/cache путь.

## Статусы `ops_run` для `tg_monitoring`

- `success` — результаты Kaggle скачаны, `telegram_results.json` разобран, import завершён, `messages_scanned > 0`.
- `empty` — результаты Kaggle скачаны и разобраны, но реальный отчёт пустой (`messages_scanned = 0`).
- `partial` — отчёт разобран, но во время import накопились ошибки в `TelegramMonitorReport.errors`.
- `error` — results не были получены/разобраны или run был прерван до завершения import.
- Важно: `empty` выставляется **только** когда бот реально прочитал `telegram_results.json`. Пустой in-memory `TelegramMonitorReport` после рестарта/отмены больше не считается `success`.
- Scheduled entrypoint теперь создаёт bootstrap `ops_run` ещё до резолва superadmin и до входа в `run_telegram_monitor()`. Если bootstrap-слой падает раньше основного runner'а, запись закрывается как `error` с `scheduler_entrypoint/fatal_error` в `details_json`; если run стартовал нормально, он переиспользует ту же запись вместо создания второй строки.
- Если APScheduler задержал или потерял слот до входа в `telegram_monitor_scheduler()` (`JOB_SUBMITTED`/`JOB_MISSED` без строки `ops_run`), общий critical-run watchdog после grace-окна сверяет последний плановый слот с `ops_run` и запускает тот же scheduled entrypoint как catch-up. Catch-up использует `run_id` с префиксом `startup_catchup_tg_monitoring_...` или `watchdog_tg_monitoring_...`, поэтому такие восстановления видны в обычных логах `tg_monitoring`.

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
- Если в payload мониторинга `posters[]` отсутствуют из-за upstream media сбоев, сервер может сделать best-effort
  fallback: вытащить фото из публичной HTML страницы `t.me/s/<username>/<message_id>`.
  Этот fallback извлекает **только** медиа‑изображения из самого поста (photo wrap + video thumbnail) и **не** должен подхватывать
  аватар канала или картинки из соседних постов. При сборке Telegraph страницы дополнительно есть safety‑net:
  слишком маленькие картинки (avatar‑like) удаляются, если в наборе есть полноценный постер.
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

## Видео (Supabase)

- Kaggle (producer) может экспортировать `messages[].videos[]` с `sha256/size_bytes/mime_type/supabase_url/supabase_path` (часть полей опциональна).
- Режим загрузки видео: `TG_MONITORING_VIDEOS_SUPABASE_MODE=off|always` (default: `always`).
- Дедуп видео делается по `sha256` (content‑addressed ключ):
  - canonical `supabase_path`: `v/sha256/<first2>/<sha256>.<ext>`;
  - перед upload producer проверяет existence в Supabase Storage и при наличии не загружает видео повторно.
- Дополнительно есть fast‑path: если удаётся получить Telegram `document.id`, producer перед скачиванием проверяет
  legacy‑путь `v/tg/<document.id>.<ext>`; если объект уже есть — повторно не скачивает и использует его URL.
- Сервер прикрепляет такие видео в `event_media_asset`, когда сообщение мапится ровно на одно событие (включая `skipped_nochange`: событие уже существует, но прикрепление медиа всё равно полезно).
- Если из одного поста импортировано несколько событий, видео не мапится (статус для UI: `skipped:multi_event_message`), чтобы не прикрепить видео к неверному событию.
- В `/tg` per-post отчёте выводится явный статус: `🎬 видео (supabase)` или `🎬 видео (skipped: <reason>)`.
- Если видео прикреплено к событию, оно отображается на Telegraph-странице события:
  - как встроенное видео (preview) для прямых файлов (`.mp4/.webm/.mov`);
  - иначе как ссылка (🎬).
- После прикрепления новых видео сервер requeue-ит `telegraph_build`, чтобы Telegraph страница гарантированно отразила новые ссылки (защита от гонок с уже запущенной сборкой страницы).
- Очистка старых событий (`cleanup_old_events`) подхватывает `event_media_asset` и ставит объекты в `supabase_delete_queue` до удаления событий из БД.

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
- В UI (`/events` → Edit) OCR виден в блоке **Poster OCR**.
- Проверка OCR в UI: см. `tests/e2e/features/telegram_monitoring.feature` (сценарий «Полный пользовательский поток мониторинга (UI)»).
- Для каналов с заданным `default_location` это значение считается **сильным prior** (защита от контекстных городов вроде «(г. Москва)» в описании участников), но это не «жёсткий игнор»:
  - если extractor извлёк явную **off-site площадку/адрес**, подтверждённые текстом поста, candidate сохраняет эту площадку вместо слепой подмены на `default_location`;
  - если extractor извлёк город, противоречащий `default_location`, Smart Update делает короткую LLM‑проверку и может переключить `city/location_*` на извлечённые значения (после чего сработает регион‑фильтр и out‑of‑region пост будет корректно отвергнут);
  - если уверенности нет — остаётся `default_location`.
- Для постов-расписаний (несколько спектаклей в одном сообщении) применяется строгая фильтрация афиш по фактам события; если она неуверенна, но Kaggle уже выдал `event_data.posters` для конкретного события, используется event-level fallback (чтобы не терять релевантную афишу при отсутствующем времени в Telegram).

## Фильтры и санитаризация

- **Custom emoji** (Telegram `MessageEntityCustomEmoji` / `<tg-emoji>`) вычищаются из текста перед публикацией в Telegraph (обычные Unicode‑эмодзи остаются).
- **Розыгрыши билетов** (giveaway): Smart Update не делает детерминированного “вырезания” текста. LLM получает исходный текст и по инструкции игнорирует механику розыгрыша (условия участия, «подпишись/репост/коммент» и т.п.), сохраняя факты о событии. Если после разбора не остаётся признаков события — пост скипается.
- **Поздравления** (не‑ивент контент) не импортируются как события и не должны становиться источниками события (например посты «Поздравляем…» со списком ближайших спектаклей).
- **Акции/промо**: промо‑фрагменты (скидки/промокоды/«акция») должны игнорироваться/удаляться **внутри LLM** по инструкциям промпта (детерминированного regex‑стрипинга нет). Если в посте есть полноценный анонс события (дата/время/место), он импортируется/мерджится.
- OCR‑подсказка времени стала устойчивее: поддерживаются диапазоны (`10:00–18:00`), выбор времени при множественных упоминаниях на афише и защита от ложных совпадений типа `05.02` → `05:02` (дата на афише не должна становиться временем).
- Инференс года для дат без года ограничен границей года (декабрь → январь): посты февраля не должны превращать январские даты в `YYYY+1`.

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
- Видео в Supabase (Kaggle + import):
  - `TG_MONITORING_VIDEOS_SUPABASE_MODE=off|always` (default `always`)
  - `TG_MONITORING_VIDEO_MAX_MB` (default `10`)
  - `TG_MONITORING_VIDEO_BUCKET_SAFE_MB` (default `430`; для видео отдельный safe guard)
  - для фото/постеров остаётся safe-порог `490MB` (default общего helper-а bucket guard)
  - `SUPABASE_BUCKET_USAGE_GUARD_CACHE_SEC` (default `600`)
  - `SUPABASE_BUCKET_USAGE_GUARD_ON_ERROR=deny|allow` (default `deny`, рекомендуется `deny`)
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

- `schema_version=1` (legacy): только `messages[]` (без `sources_meta`);
- `schema_version=2`: `messages[]` + top-level `sources_meta[]` с метаданными источников и подсказками.

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
