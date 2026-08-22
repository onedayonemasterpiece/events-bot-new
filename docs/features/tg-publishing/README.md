# Telegram Event Publishing

Каноника для исходящих Telegram-публикаций отдельных событий после Smart Update.

## Event Posts

- После Smart Update событие получает редакционный пост в Telegram-канале `https://t.me/kldevents`.
- Telegram-публикация не заменяет VK, но не зависит от VK-публикации: для publish-eligible события VK (`vk_sync`) и Telegram (`tg_event_publish`) являются независимыми public surfaces. VK media/API/captcha failures must not block Telegram event announcements.
- Публикация запускается через `JobTask.tg_event_publish` и зависит только от Telegram-required prerequisites: `telegraph_build` и `tg_ics_post` при наличии валидного календарного времени. `vk_sync` не входит в `depends_on` для `tg_event_publish`.
- Если dependency ещё retry'ится с bounded `next_run_at`, ожидающий `tg_event_publish`/`tg_ics_post` не должен стареть до `expired`: он переносится к ближайшему retry dependency. Terminal Telegraph/ICS dependency failure остаётся blocker и требует requeue/repair; terminal VK failure не должен блокировать Telegram.
- Календарное время означает, что у события парсятся и `date`, и `time`; произвольные строки времени вроде `по расписанию`/`уточняется` не должны создавать новую ICS-проекцию. Если календаря ещё нет, случай завершается как `skipped_invalid_schedule`, а не уходит в бесконечный retry. Если Smart Update удалил ранее опубликованное ошибочное время, existing `ics_publish`/`tg_ics_post` переармируются в cleanup-режиме: storage object попадает в durable `supabase_delete_queue`, поля/shortlink очищаются, а старый документ удаляется из calendar-канала до повторного Telegraph/VK/TG fanout. Публичные rebuild jobs зависят от cleanup, поэтому старый календарный CTA не может вернуться из stale DB state.
- Повторный Smart Update/reimport должен переармировать `tg_event_publish` и `tg_ics_post` с текущим набором зависимостей, а не накапливать старые. Для события без времени и без прежней calendar-проекции dependency `tg_ics_post:<event_id>` убирается; при существующей проекции dependency временно сохраняется как cleanup barrier и исчезает после удаления stale calendar surface. Старые зависимости `vk_sync:<event_id>` также не должны попадать в `tg_event_publish`.
- Storage ICS и Telegram calendar document имеют отдельные content hashes (`ics_hash` и `ics_post_hash`). Обновление storage-файла не должно ошибочно помечать старый Telegram document актуальным. При изменении расписания `tg_ics_post` редактирует прежний channel document через `editMessageMedia`; новый message создаётся только если прежний действительно не найден, а прочие edit-ошибки fail closed вместо публикации дубля.
- Если у события уже есть pending `tg_event_publish`, повторный Smart Update/reimport должен заменить его `next_run_at` на слот, рассчитанный для текущего цикла. Отложенные page rebuild jobs могут сохранять будущий слот, но Telegram event announcement не должен оставаться на завтрашнем stale slot, когда текущий publish window открыт.
- `telegraph_build` является обязательной dependency для Telegram event announcement и может включать несколько LLM-first шагов с fallback. Его runtime budget должен покрывать нормальный Telegraph render путь; slow-but-healthy render не должен помечаться `stale`, иначе `tg_event_publish` получает вечный dependency blocker.
- Runtime target по умолчанию: `@kldevents`; в production не нужно выставлять `TG_EVENT_CHANNEL=@kldevents` отдельным env. `TG_EVENT_CHANNEL` / `TG_EVENT_CHANNEL_ID` — только явный override для миграции или изолированной отладки; `ENABLE_TG_EVENT_PUBLISHING=0` выключает job handler без изменения Smart Update.
- Cancelled/postponed, silent и полностью прошедшие события не публикуются.
- Идемпотентность хранится на `event`: `tg_event_post_url`, `tg_event_post_id`, `tg_event_post_mode`, `tg_event_source_hash`. Для события с применимыми curated-медальонами организатора, площадки, фестиваля, программы или Пушкинской карты режим равен `rich_message`; медальоны канала-источника/агрегатора в Telegram запрещены. Хеш включает текст, deduped media signature, набор slug медальонов, renderer version и SHA-256 фактических assets. Изменение текста, галереи, manifest-match или файла логотипа поэтому переармирует публикацию. Существующий `rich_message` редактируется через `editMessageText(rich_message=...)`; переход с legacy `text`/`photo_caption`/`album_caption` создаёт новый RichMessage и только после успешной отправки best-effort удаляет прежний post id.
- Если Bot API даёт timeout/`Request timeout error` именно на операции отправки нового Telegram event post (`sendRichMessage`, `sendMessage`, `sendPhoto`, `sendMediaGroup`), результат считается неопределённым: Telegram мог уже принять сообщение, но ответ с `message_id` потерялся. Такой `tg_event_publish` не должен автоматически retry'иться, потому что retry может создать публичный дубль. Job остаётся в `error` с дальним `next_run_at`; оператор должен проверить канал через Telethon, удалить/сверить дубль или вручную reconcile/requeue.
- Чтобы не слать ночной поток уведомлений, `tg_event_publish` ставится в отложенный `JobOutbox` слот: по умолчанию не раньше `07:00` и не позже `23:00` локального `Europe/Kaliningrad`, с интервалом минимум `10` минут после последней queued/done Telegram event публикации. Runtime knobs: `TG_EVENT_PUBLISH_START_HOUR`, `TG_EVENT_PUBLISH_END_HOUR`, `TG_EVENT_PUBLISH_INTERVAL_MINUTES`.
- Spacing игнорирует `error` jobs и pending/running anchors дальше `TG_EVENT_PUBLISH_SPACING_HORIZON_HOURS` (по умолчанию `24`) от текущего времени: это защищает новые публикации от ручных cleanup-marker rows вроде `next_run_at=2036-...`. Планировщик выбирает ближайший свободный слот внутри publish window, а не слот после самого позднего pending anchor: вечерний или завтрашний backlog не должен оставлять дневную дыру в `@kldevents`, если после последней фактической публикации уже прошёл стандартный интервал. Если текущий локальный publish window уже закрыт и новый кандидат нормализован на завтрашнее `07:00`, stale anchors того же следующего дня, стоящие намного позже утреннего старта, тоже не должны сдвигать свежие импорты в вечерний кластер; нормальные соседние anchors продолжают задавать стандартный интервал.
- `tg_event_publish` различает новые объявления и edit/reconciliation jobs только для порядка очереди: no-post announcements идут раньше existing-post edit rows. Но execution spacing остаётся жёстким для каждого `tg_event_publish`, потому что existing-post job при смене режима `text`/`photo`/`album` тоже может отправить новый public message id; обход 10-минутного gate запрещён.
- Если один исходный пост/афиша (`event.source_post_url` или внешний `event_source.source_url`) породил несколько отдельных событий, `tg_event_publish` дополнительно разводит эти события минимум на `SAME_SOURCE_EVENT_PUBLISH_INTERVAL_HOURS` (default `12`) внутри `SAME_SOURCE_EVENT_PUBLISH_SPACING_HORIZON_HOURS` (default `168`). Обычная очередь при этом остаётся общей: стандартный 10-минутный spacing учитывает такие отложенные `JobOutbox` anchors, а source-specific правило только увеличивает разрыв между событиями из той же афиши.
- Пока Telegram-пост ждёт утренний слот, Smart Update-отчёты должны показывать строку `Посты: VK ... · TG ⏳`; после публикации `TG ⏳` заменяется ссылкой на пост в канале.

## Promo Campaign Surfaces

- `promo_activity.surface='tg_event_publish'` is an explicit campaign slot in a
  Telegram event-flow channel such as `@kldevents`. It is separate from the
  ordinary `JobTask.tg_event_publish` pipeline above: campaign slots may publish
  an extra full event post for selected targets and record
  `promo_exposure.publish_status='TG_PUBLISHED'`.
- The promo activity target channel is configured per activity through
  `config_json.target_chat`; it is not controlled by the global
  `TG_EVENT_CHANNEL` runtime setting.
- `promo_activity.surface='tg_repost'` forwards an already published source
  channel post, normally `@kldevents -> @kenigevents`. It uses
  `config_json.source_chat`, `config_json.target_chat`, active-window settings,
  and `dedup_hours`. If no source post exists yet (`event.tg_event_post_url` or
  a recent `tg_event_publish` exposure), the activity skips the tick instead of
  creating a new post in the daily/digest channel.
- `tg_repost` must preserve the channel-role split: event-flow channels can
  receive full event posts, while daily/digest channels receive only selected
  forwards or editorial daily blocks. A repost activity by itself must not
  change buttons on the source `@kldevents` post; button highlighting is
  controlled only by the separate `tg_button_highlight` marker activity.
- Compensation/manual repair publications in `@kldevents` must use the ordinary
  event-post publisher (`job_publish_tg_event_post` /
  `publish_tg_event_announcement`) or a fully equivalent path. A repair is not
  complete if it renders a promo-only body that drops the registration/ticket
  link or bypasses the graphical-medallion RichMessage resolver. For legacy media groups,
  do not attach inline buttons; the registration/ticket CTA must be preserved
  as a link entity inside the caption/text.
- Canonical `tg_event_publish` does **not** enqueue or invoke the Premium/custom-emoji editor for `rich_message` posts. The delayed editor may still normalize unrelated labels on legacy posts, but receives `medallion_html_block=None`; using it to place event medallions again is forbidden. A stale already-enqueued editor job also checks the current stored mode and exits before Telethon when it has become `rich_message`.

## Формат

- Telegram-пост не наследует VK-капс: заголовок остаётся нормальным регистром и рендерится как `<b>...</b>`.
- Explicit promo activity posts (`promo_activity.surface='tg_event_publish'`) publish the full event body, but Markdown service markup from Smart Update descriptions must be normalized before send: section headings such as `### О спикере` render as bold Telegram HTML headings, bullets render as `•`, and raw `###`, `**`, or `*` markers must not be visible in the public post.
- Explicit promo activity posts with `event.photo_urls` must not silently degrade to text-only output because the full body is longer than Telegram's media caption limit. When the full promo body does not fit a media caption, the publisher sends the image/album with a concise caption and `Подробнее` button instead of dropping media; long bullet-list bodies are summarized in the caption rather than dumped as raw thesis lists.
- Инфоблок содержит дату/время, площадку/адрес/город, Пушкинскую карту и билетную строку. Если у события есть `end_date` позже `date`, дата в инфоблоке и календарной кнопке выводится диапазоном (`12–15 июня`, `30 июня–2 июля`), чтобы long-running события не выглядели как однодневные стартовые посты.
- Smart Update description не публикуется целиком: для каждого поста строится короткий Telegram intro через зафиксированный public-writer **`gemini-3.1-flash-lite`**. Это отдельный public-copy writer, а не стадия обработки/сопоставления события; Gemma допустима в processing-контрактах, но не должна становиться автором Telegram intro. Модель не переопределяется runtime-env, hook выполняет ровно одну Lite-попытку и не наследует глобальный `GOOGLE_AI_FALLBACK_MODELS`. При ошибке или невалидном Lite-тексте разрешён только строгий `gpt-4o` writer с атомарным persisted UTC-day бюджетом **не более 100 запросов**; `gpt-4o-mini`, Gemma и детерминированная сборка narrative запрещены. Если 4o-бюджет исчерпан или обе LLM недоступны/невалидны, публикация fail-closed и остаётся на retry, а не выходит с автоматически собранным текстом. Intro может быть цепляющим вопросом, коротким полезным абзацем или friendly-вступлением вроде `Друзья, ...`; вопрос не обязателен, если событие утилитарное/сервисное и важнее объяснить пользу. Writer не должен начинать серией одинаковых шаблонов вроде `Хотите...`, `Готовы...`, `Что здесь стоит увидеть?`: если вопрос уместен, он должен быть конкретным к событию. Любые обещания посетителю (`увидите`, `сможете`, `пообщаетесь`), программа, участники и свойства экспонатов должны быть прямо подтверждены future-event контекстом; детали recap нельзя переносить в teaser. При бедном источнике Lite пишет консервативно о подтверждённом формате, а не домысливает активности. Текст не должен повторять дату, время, место, цену, ссылки и хештеги из инфоблока, а собственные имена и названия должен брать без изменения написания.
- Public writer возвращает grounded JSON `sentences[].text + evidence_quote`: schema обязана жёстко ограничивать массив одним–тремя элементами (`minItems=1`, `maxItems=3`), а `evidence_quote` — динамическим `enum` максимум из шести дословных фрагментов organizer-source corpus длиной до 160 символов каждый. Это делает перефразированную «цитату» невыразимой в structured output, не превышая provider schema-complexity limit; application parser всё равно повторно проверяет диапазон и exact-source/claim grounding. При построении enum сохраняются границы строк и абзацев organizer-source: соседние строки одного абзаца можно упаковать в одну дословную цитату до 160 символов, но нельзя пересекать пустую строку/границу двух источников. Это не даёт точке внутри названия (`Дюна. Империя`) оторвать заголовок от непосредственно предшествующего обозначения формата. Output ceiling учитывает JSON framing и повторённые evidence quotes, а не только видимые 330/500 символов: ordinary lane использует `768`, promo lane — `1024` токена; тот же mode-specific budget и та же динамическая schema передаются строгому 4o fallback. Изменять эти границы или делать bulk rearm можно только с regression contract `INC-2026-08-21-tg-event-public-writer-max-tokens` и планом catch-up для событий без Telegram post URL.
- Если событие покрыто любой активной promo-кампанией (`promo_campaign` + `promo_target`), Telegram intro получает promo-режим по умолчанию для канала: до `500` символов вместо обычных `330`, 1-3 предложения и prompt на более богатый выбор 2-3 конкретных причин/деталей без рекламной воды. Для такого режима LLM получает объединённый editorial/source контекст (`description`, `short_description`, `search_digest`, `source_text`) после тех же guardrail-проверок, а не только первый доступный текст. Для этого не нужна отдельная `promo_activity.surface='tg_event_publish'`, channel target или новый env; promo-флаг входит в `tg_event_source_hash`, поэтому включение/выключение кампании переармирует пост.
- Для пунктов приёма, сбора, переработки, волонтёрских и других городских полезных акций intro должен объяснять практическую пользу, действие и важные ограничения. Если `event.description` явно противоречит utility/source text (например обещает музыкальные номера, театральную программу или билеты для события `Приём шин`), Telegram publisher использует `event.source_text` как более надёжный источник для intro.
- Та же конфликтная `event.description` не участвует в подборе type-хештегов Telegram-поста, чтобы в utility/service caption не попадали теги вроде `#спектакль` из галлюцинированного описания.
- Строка локации выводит город как хештег (`📍 Площадка, адрес, #Калининград`), но не повторяет его, когда `location_name` уже равен городу: city-level fallback рендерится как `📍 Янтарный`, а не `📍 Янтарный, #Янтарный`. Та же общая композиция используется managed VK header. Нижняя hashtag line оставляет дату, компактный городской тег афиши (`#афишакалининград`), до нескольких ключевых type-тегов (`#лекция`, `#спектакль`, `#показ`, `#концерт`, ...), фестивальный тег.
- Обычный non-promo footer сохраняет текстовую ссылку `🔎 Подробнее`, а
  после неё в той же строке добавляет постоянные ссылки `Max` и `Вконтакте`
  (`https://max.ru/channel_kenigevents`,
  `https://vk.ru/im/channels/-239844596`) с визуальным отступом в 12 пробелов: `🔎 Подробнее            Max · Вконтакте`.
  В RichMessage HTML эти 12 пробелов сериализуются как 12 `&nbsp;`, поэтому
  клиент не схлопывает семантический разрыв; все три ссылки остаются в одной строке.
  Inline-кнопка `✨ Подробнее` управляется отдельной promo-активностью
  `promo_activity.surface='tg_button_highlight'` /
  `profile_key='kldevents:details-button'`: если активность включена для
  кампании, покрывающей событие, ссылка `Подробнее` уходит из текста в кнопку;
  если активности нет или она выключена, `🔎 Подробнее` остаётся текстовой
  ссылкой даже при promo-intro. Это позволяет оставить богатый promo intro от
  кампании, но отключить громоздкую кнопку для широких механик вроде
  `tg_repost`. `Подписаться` не добавляется в event captions. Social footer
  `Max · Вконтакте` подавляется только для явных button-highlight постов, где
  `Подробнее` уже вынесен в inline-кнопку `✨ Подробнее`; широкий
  `promo_highlight`/богатый intro сам по себе не убирает footer-ссылки.
- Если у события есть календарь, пост получает inline-кнопку вида `📅 <дата или диапазон> <время> · Добавить в календарь`. Публичный CTA должен вести на открываемую аудиторией ссылку: публичный Telegram calendar-post URL (`https://t.me/<username>/<id>`) предпочтительнее, но приватные внутренние `https://t.me/c/...` ссылки asset-канала не считаются публичными и заменяются на `event.ics_url`. Новые `tg_ics_post` записи сохраняют username-ссылку, если asset-канал зарегистрирован с `username`.
- Source posts that look like third-party ticket giveaways (`розыгрыш` / `разыгрываем` / `выиграть` + `билет` / `пригласительный`) are fallback content for managed Telegram/VK event publication only when Smart Update has not produced substantial cleaned non-giveaway event copy. If Smart Update extracted a real event body after removing raffle mechanics, the event still receives the normal Telegram event post and managed VK dependency even when the raw source mentioned a giveaway.
- Для бесплатных Telegram event posts в hashtag line добавляется `#бесплатно`, чтобы событие оставалось поисковым независимо от визуального оформления admission-строки.
- Хештеги в Telegram event posts должны быть навигационными и поисковыми, а
  не пересказом названия события. Канонический набор: дата (`#1июля`,
  `#1_июля`), городской афишный тег (`#афишакалининград`), до трёх коротких
  type-тегов (`#лекция`, `#концерт`, ...), `#бесплатно` для бесплатных
  событий и только короткий фестивальный/брендовый тег. Presentation guardrail:
  один tag body после `#` — максимум 28 символов, строка — максимум 8 тегов и
  около 140 видимых символов. Длинные слитные title-like slug tags вроде
  `#ПоодёжкевстречаютНародныйкостюмтрадицииисмыслы` не публикуются.
- Если структурной цены, билетной ссылки, sold-out статуса и явного признака бесплатности нет, билетная строка не выводится.
- Telegram использует исходный `event.ticket_link`. `event.vk_ticket_short_url` — VK-only analytics ссылка и не должна попадать ни в текст, ни в entities/buttons Telegram-поста.
- Если `event.ticket_link` содержит телефон (`tel:+...`) или в narrative/body есть телефонный контакт, Telegram-пост должен делать его кликабельным явной Telegram `phone_number` entity при отправке/редактировании caption/text. Для самого Telegram caption видимый номер нормализуется в компактный `+74012463635`, потому что Bot API принимает `phone_number` entity только на таком phone-like payload; человекочитаемая группировка сохраняется в промежуточной HTML-сборке/Telegraph, но не должна ломать actionability в Telegram. Уже существующие HTML-ссылки не перелинковываются повторно.
- Smart Update не должен заменять уже известный canonical `event.ticket_link` на VK shortener URL (`vk.cc`, `vk.link`, `go.vk.com`, `l.vk.com`). Short URL допустим в `event.ticket_link` только если источник не дал более прямой ticket/registration URL; последующий parser/site URL обязан заменить short URL.

## Медиа

- Перед отправкой в Bot API publisher берёт уже материализованные Smart Update URL из `event.photo_urls`, скачивает их и отправляет как `BufferedInputFile`. Remote URL strings не являются штатным способом доставки медиа в Telegram event posts.
- Перед публикацией `event.photo_urls` уже является производной `approved` projection единого [Smart Update event-media gate](../event-media/README.md). Source aggregation выполняет `event_media_review` job (только specific multi-source gap), не Telegraph renderer. TG publisher не принимает perceptual-решений: он оставляет exact-URL safety guard и берёт максимум 9 approved изображений; managed/source mirror, crop/re-encode и semantic conflicts разрешаются до fanout.
- Если late media rehydration всё же произошёл после первого public fanout (например, video popular-review восстановил фото из исходного VK-поста), URL сначала materialize'ятся в managed storage, а затем обычные `tg_event_publish`/`vk_sync` автоматически re-arm'ятся. Сохранить `event.photo_urls`, оставив старые text-only Telegram/VK posts, запрещено.
- Если resolver нашёл хотя бы один curated-медальон, канонический пост — `sendRichMessage`: все approved event images идут отдельными верхними media blocks, затем текст, затем самостоятельная нижняя полоса медальонов и footer. Полоса рендерится локально Pillow в opaque brand graphite `#202830`, `1300×330`, с медальонами до `260px`; OpenAI image generation не используется.
- Автоматическая смена одиночного legacy `text`/`photo_caption` на RichMessage выполняется send-first/delete-after-success. Существующий `album_caption` автоматически не заменяется: историческая схема хранит только id первого элемента media group, поэтому без полного операторского ledger удалить весь старый альбом безопасно невозможно. Такой пост остаётся альбомом до аудируемой миграции всех message ids; новые публикации с curated-медальонами сразу создаются как RichMessage.
- Resolver читает `site/src/data/organizerMedallions.json`, опциональный `festivalMedallions.json`, соответствующие local runtime assets и Pushkin-card composite. Venue/organizer aliases сопоставляются только с location/source identity fields, festival/program marks — со структурным `event.festival`; `event.festival=80 историй о главном` всегда даёт отдельные `KGD80` и `Знание`. Поэтому event `6811` в КОНБ обязан разрешаться в три медальона: `КОНБ + 80 историй + Знание`. Аватары `MEOW Афиша` и других source/агрегатор-каналов в этот resolver не входят, даже если их assets используются на static event page.
- Presentation cap по умолчанию — 5 source-grounded medallions (`TG_GRAPHIC_MEDALLION_MAX_ITEMS`), при переполнении все выбранные assets пропорционально уменьшаются в пределах safe area. Пушкинская карта добавляется как program medallion после organizer/festival identities.
- Если применимого graphical medallion нет, сохранён legacy fallback: одно изображение публикуется через `sendPhoto`, несколько — через `sendMediaGroup`; это не разрешает custom-emoji medallions.
- В production event announcement всегда требует хотя бы одно approved CDN-изображение. Пустая gallery или любой non-CDN URL fail-closed: `tg_event_publish` остаётся в retry/error, ожидает `event_media_review`, не отправляет text-only пост и не удаляет существующий media post. Это запрещает и первую text-only публикацию, и downgrade `photo_caption/album_caption -> text`.
- Caption всегда удерживается в `1000` видимых символов: если LLM hook или инфоблок всё ещё длинные, deterministic safety-net режет только narrative body; заголовок, инфоблок, хештеги и footer links сохраняются.

## Проверки

- Unit: `tests/test_tg_event_publish.py`.
- Live E2E для фичи удобно гонять через `/vk_auto_import --limit=1`: событие с curated identity должно появиться в `@kldevents` как RichMessage с approved poster block(s), обычным event text, самостоятельной нижней graphical-medallion strip, публично открываемой календарной кнопкой, датным/городским/type/festival hashtag line и footer `Подробнее            Max · Вконтакте`. Проверка должна подтвердить `event.tg_event_post_mode='rich_message'`, отсутствие custom-emoji mosaic и тот же managed VK-пост в `klgdevents` через `wall.getById`.

### Grounded public intro contract (2026-07-14)

The event-announcement intro is a public text and remains isolated from cheap bulk-processing
model routing. It uses one Gemini Lite attempt; only the existing persisted `gpt-4o` emergency
lane (maximum 100 calls per UTC day) may write a fallback. Gemma, mini models, and deterministic
narrative fallbacks are prohibited.

Both approved writers return structured sentences with an exact contiguous `evidence_quote`
from the organizer source corpus. The publisher verifies that the quote occurs in the raw
source and supports the whole sentence, including every number. Invalid JSON, a missing quote,
or an unsupported sentence fails closed (or proceeds to the bounded approved emergency writer)
instead of publishing fluent filler. Canonical description text can be supplied as draft
context, but it is not evidence and cannot authorize a claim.
