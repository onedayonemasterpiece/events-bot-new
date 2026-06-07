# VK Publishing

Каноника для исходящих публикаций в VK: посты отдельных событий после Smart Update, компактный ежедневный анонс и отдельные feature-owned дайджесты.

## Event Posts

- После Smart Update событие получает отдельный VK-пост в событийном сообществе `VK_EVENTS_GROUP_ID`; если переменная не задана, используется исторический `VK_AFISHA_GROUP_ID`. Это относится и к событиям, пришедшим из VK wall: исходный VK-пост не считается заменой редакционного анонса в `klgdevents`.
- Telegram-анонс того же события живёт в отдельной фиче [Telegram Event Publishing](../tg-publishing/README.md): он не наследует VK plain-text/caps формат и публикуется через отдельный `tg_event_publish` job.
- Планировщик задач (`schedule_event_update_tasks`) запускает `vk_sync` для актуального события, у которого `source_vk_post_url` пуст или указывает на внешнее сообщество. Только когда `source_vk_post_url` уже указывает на пост в `VK_EVENTS_GROUP_ID`, `vk_sync` пропускается — управляемый klgdevents-пост уже существует. Это значит, что актуальное событие, импортированное из VK wall (с внешним `source_vk_post_url`), всё равно получает свой пост в `klgdevents`.
- Полностью прошедшие события (`end_date` строго раньше сегодняшней локальной даты, либо `date` строго раньше сегодняшней локальной даты, если `end_date` пуст) не получают новый managed `klgdevents` пост. Guard стоит и при постановке `vk_sync`, и в самом `job_sync_vk_source_post`, чтобы уже накопленные pending jobs после deploy не дошли до `wall.post`. Ongoing long events с `end_date >= today` остаются publish-eligible.
- Если попытка `wall.edit` для управляемого klgdevents-поста отклонена VK как «окно редактирования истекло» (старше `VK_POST_MAX_EDIT_AGE` или `can_edit=0`), редактор пишет предупреждение в лог и тихо завершается. Superadmin-чат больше не получает уведомление: Smart Update не должен зависеть от возможности редактировать произвольно старые VK-посты.
- Hash идемпотентности для управляемого VK-поста включает версию формата поста, `event.title` и body/description: title-only repairs и изменения hashtag/footer policy должны приводить к `wall.edit`, иначе исправленный публичный формат останется только в БД/Telegraph.
- После успешного `wall.post`/`wall.edit` `vk_sync` обязан дожать запись `event.source_vk_post_url` и `event.vk_source_hash` в БД с коротким retry на transient SQLite lock. Внешняя VK-публикация уже произошла, поэтому lock на финальном `commit()` не должен превращаться в повторный `wall.post` и публичный дубль.
- VK weekly/weekend «навигационные» wall-посты выведены из эксплуатации 2026-05-17. `JobTask.week_pages` больше не ставится в очередь нигде в коде; `update_week_pages_for`, `sync_vk_week_post`, `sync_vk_weekend_post` оставлены как no-op стабы для дренажа уже стоящих в `JobOutbox` записей. Перестроение Telegraph weekend-страницы (`sync_weekend_page`) больше не дёргает `wall.post`/`wall.edit`. Помощники `build_week_vk_message`, `build_weekend_vk_message`, `_build_month_vk_nav_lines` и связанные дебаунсы удалены. Навигация в VK ограничивается компактным VK daily.
- Целевое production-сообщество для событийных анонсов: `https://vk.com/klgdevents`.
- Все исходящие community wall-посты через общий `post_to_vk` публикуются с `owner_id=-<group_id>`, `from_group=1`, `signed=0` независимо от actor token (`group` или `user`). Это обязательный contract: VK должен записывать `from_id=-<group_id>`, иначе пост выглядит созданным личным пользователем и теряет нормальный wall/community forward.
- Все новые исходящие community wall-посты через `post_to_vk` ставятся в отложенную публикацию (`publish_date`), а не публикуются сразу. Расчёт идёт по `Europe/Kaliningrad`: минимум через 10 минут от текущего времени, минимум через 10 минут после последней уже отложенной записи этого сообщества, и не раньше 06:00, если ближайший слот попадает на ночные часы до 06:00. Это распространяется на daily, событийные посты и poll-посты, которые идут через общий `post_to_vk`; уже опубликованные старые записи не меняются.
- Для отложенных постов VK может вернуть `post_id`, который фактически является `postponed_id`, а реальный `wall.get` item получает другой `id`. `post_to_vk` обязан сразу после успешного `wall.post` резолвить `postponed_id -> id` через `wall.get` user actor (с коротким retry, потому что VK иногда показывает actual item не мгновенно) и сохранять URL с реальным wall id; `vk_sync` также lazy-resolves уже сохранённые stale `postponed_id` URLs перед `wall.edit`. Иначе последующие `wall.getById`/`wall.edit` будут видеть пустой/удалённый пост.
- Пост события поддерживает медиагруппу: все доступные `event.photo_urls` загружаются как `photo-<group_id>_<id>` и прикладываются к `wall.post` в рамках `VK_MAX_ATTACHMENTS`.
- Если VK возвращает captcha (`code=14`) на `photos.getWallUploadServer` / `photos.saveWallPhoto`, `vk_sync` должен fail closed / pause before `wall.post`: нельзя публиковать событийный пост без картинок только потому, что загрузка фото требует ручного captcha step.
- Для нового managed `klgdevents` поста Telegram-origin события (`source_post_url=t.me/...`, Telegram chat/message fields или `event_source.source_type=telegram`) silent text-only fallback запрещён. Если после `event.photo_urls`, Telegraph fallback и upload попыток нет ни одного `photo...` attachment, `sync_vk_source_post` должен вернуть ошибку `vk_sync_missing_media_for_telegram_event`, а не создавать `wall.post` с `attachments=0`. Emergency override: `VK_REQUIRE_MEDIA_FOR_TG_SOURCE_POSTS=0`.
- Тот же fail-closed contract обязателен для promo `vk_publication`, но отсутствие картинки у промо не считается успешным решением. Runner сначала делает best-effort Telegraph recovery для пустого `event.photo_urls`; если после recovery/upload нет `photo...` attachments, он пишет `FAILED_NO_MEDIA` audit row с `event_id`, `campaign_id`, `activity_id`, `source_post_url`, `photo_urls_count`, `attachments_count` и action для расследования, затем завершает действие `vk_sync_missing_media_for_telegram_event`, а не создаёт text-only promo wall post. Компенсация должна восстанавливать media исходного промо-события и публиковать replacement с картинкой, а не просто удалять/замещать его нерелевантным событием.
- `event.photo_urls` должен быть уже очищен на уровне Smart Update: `_apply_posters` сравнивает текущие и новые poster/photo URL по perceptual hash (`dh16` / `SMART_UPDATE_POSTER_NEAR_DUP_HAMMING`, default 20 for the 256-bit hash) и заменяет legacy site/CDN duplicate на preferred managed-storage poster URL. VK publish boundary дополнительно отбрасывает near-duplicates перед upload, но это только последний предохранитель; первичный contract — не хранить визуальные дубли в `Event.photo_urls`.
- VK не поддерживает Markdown-заголовки в wall text. Markdown/HTML headings из Smart Update перед публикацией превращаются в plain text: заголовок капсом и пустая строка ниже. Inline `###` markers from older/failed writer output are split at the VK boundary before `wall.post`/`wall.edit`, so raw Markdown markers must not leak into public VK text.
- В конце событийного поста перед редакционным футером добавляется VK hashtag line: базовые `#анонс #анонс39 #кудапойтиКалининград #афишакалининград`, город события, две даты события (`#17мая` и `#17_мая`), компактный городской тег афиши (`#афишасветлогорск`, `#афишагусев` и т.п.), до нескольких ключевых type-тегов из заголовка/body/source (`#лекция`, `#спектакль`, `#показ`, `#концерт`, `#выставка`, `#мастеркласс`, `#экскурсия`, ...), и, если событие привязано к фестивалю, отдельный хештег из canonical `Festival.name` без пробелов/разделителей (например `#80историйоглавном`). Raw `event.festival` используется только как fallback, если canonical `Festival` не найден; падежные формы в событии (`Кантаты`) не должны попадать в публичный hashtag вместо canonical search tag (`#Кантата`).
- API-соавторство для managed wall-постов не используется. Локальная проверка VK API и UI 2026-06-07 показала, что `copyright`, `coauthors` и `coauthor_ids` могут приниматься `wall.post`, но в форме редактирования «пригласить соавторов» никто не выбран и уведомление соавтору не приходит. Поэтому публичную атрибуцию нужно делать видимым текстом/репостом, а не скрытыми coauthor-параметрами.
- Сетка медиагруппы управляется клиентом VK: бот публикует все фото одним `wall.post` photo-attachment списком, без отдельного link/video attachment в этой медиагруппе.
- При редактировании существующего поста частичная неудача re-upload не должна сжимать медиагруппу: если загрузились не все новые фото, редактор сохраняет уже прикреплённые к посту изображения.
- **Telegraph fallback для медиагруппы.** Если `event.photo_urls` пуст, но у события есть `telegraph_url`, `sync_vk_source_post` тянет картинки со страницы Telegraph (`extract_telegraph_image_urls`) и публикует их вместо отсутствующих `photo_urls`. Фолбэк срабатывает только когда у управляемого VK-поста ещё нет фото-вложений (`wall.getById` возвращает 0 photo attachments) — это защищает от повторной загрузки тех же файлов на каждом Smart Update sync и от появления дубликатов в фотоальбоме сообщества.
- **Multi-photo посты публикуются как сетка, а не карусель.** `post_to_vk`, `edit_vk_post` и shortpost-публикация передают `primary_attachments_mode=grid` в `wall.post`/`wall.edit`, когда фото-вложений больше одного. Без этого современные клиенты VK по умолчанию рендерят несколько фото как горизонтальную карусель.
- Фото-публикация включена по умолчанию через `VK_PHOTOS_ENABLED_DEFAULT=true`. Команда `/vkphotos` остаётся ручным рубильником и записывает явное значение в БД; явное `0` в БД сильнее дефолта.
- Для загрузки фото/видео нужен пользовательский VK token. Runtime читает `VK_USER_TOKEN`, а локально также принимает fallback `VK_ACCESS_TOKEN4`.
- Внешние ссылки на билеты/регистрацию продолжают проходить через существующий VK shortener (`utils.getShortLink` / `vk.cc`). Если shortener недоступен, публикация не должна падать: текст сохраняет исходную ссылку.
- Group token сам по себе не годится для upload/edit-flow событийных постов: загрузчик фото и редактор постов в `VK_EVENTS_GROUP_ID` должны использовать user actor, если он доступен, и не тратить ретраи на group-token `code=27`.

## Past-Event Cleanup

- Каноника фичи автоудаления: [`autodeletevkposts.md`](autodeletevkposts.md).
- Дважды в сутки (`vk_post_prune_scheduler`, по умолчанию `02:30,14:30`
  Europe/Kaliningrad) бот удаляет управляемые `klgdevents` посты для событий,
  которые уже в прошлом и не набрали репостов/шерингов в историю
  (`reposts.count == 0`) и комментариев (`comments.count == 0`), чтобы лента не
  рекомендовала прошедшие события.
- Удаляются только посты, чей `Event.source_vk_post_url` указывает на
  `-VK_EVENTS_GROUP_ID`; внешние стены VK-импортов, закреплённые посты и события
  с непустым `reposts.count` или `comments.count` не трогаются. Прошлость
  берётся из БД, поэтому daily, опросы и промо-репосты не сопоставляются и не
  удаляются.
- Джоб входит в общий heavy-ops gate, так что не конкурирует с VK-write/Kaggle
  джобами: при занятом gate проход пропускается с уведомлением `ADMIN_CHAT_ID`.

## Compact Daily

- VK daily остаётся отдельным расписанием от Telegram daily:
  - `/vktime today HH:MM` управляет утренним постом `НЕ ПРОПУСТИТЕ СЕГОДНЯ` (дефолт `08:00`);
  - `/vktime added HH:MM` управляет вечерним постом `N ДОБАВИЛИ В АНОНС` (дефолт `20:00`).
- Формат события в VK daily — одна строка, близкая к компактной Telegram-секции `ДОБАВИЛИ В АНОНС`: дата перед названием, затем короткие маркеры, заголовок и ссылка на VK-пост события, если она уже есть.
- Если новых событий больше девяти, VK daily группирует строки по городам так же, как компактная Telegram-секция для большого списка.
- VK daily не добавляет тяжёлую навигацию по дням, выходным и месяцам. Ссылкой для перехода служит VK-пост события; если `vk_repost_url` отсутствует, строка остаётся без ссылки.
- Партнёрские события не получают автоматическую ссылку на сохранённый partner/source post в daily-строке: строка остаётся текстовой, чтобы не уводить трафик в чужой исходник вместо редакционного VK-анонса.

## Feature-Owned Digests

- Guide excursions VK digest принадлежит фиче [Guide Excursions Monitoring](../guide-excursions-monitoring/README.md), но обязан использовать общий VK wall contract из этого документа.
- Для `https://vk.com/uhtykaliningrad` нужен отдельный target group id/env, независимый от `VK_EVENTS_GROUP_ID` (`klgdevents`) и `/vkgroup` daily-настройки.
- Такие дайджесты публикуются через `post_to_vk`, поэтому по умолчанию попадают в отложку: минимум через 10 минут от текущего времени, с теми же `VK_POSTPONED_*` правилами и community-author contract.
- Feature-owned digest не должен молча наследовать Telegram formatting. Перед вызовом `wall.post` он должен быть plain text, с VK-safe ссылками, без HTML/Markdown и без Telegram-only caption/media mechanics.
- Guide excursions VK digest в `uhtykaliningrad` указывает `klgdevents` видимой строкой `Совместно с Полюбить Калининград Афиша: https://vk.com/klgdevents`; API-соавторство для этого не применяется, потому что VK UI smoke не подтвердил приглашение соавтора.
- Guide excursions VK digest должен загружать materialized local media assets в VK через user-token photo upload и передавать полученные `photo...` attachments в тот же `wall.post`, что и текст. Silent text-only fallback запрещён, если у issue есть media items.

## Promo VK

- Promo campaign activity `vk_publication` can create additional event posts in
  a configured community when organic Smart Update posts for the same campaign
  target did not reach the rolling 24-hour minimum. It uses the same outgoing
  wall contract as Smart Update event posts: `post_to_vk`, community author,
  postponed queue, and source-style event text.
- The promo VK runner checks every 30 minutes by default. It starts new promo
  VK actions only during the activity's local active window (default
  09:00-21:00 Europe/Kaliningrad) and spreads the daily target into even
  due-slots, so two daily posts are normally attempted around midday and early
  evening rather than as one batch.
- Promo campaign activity `vk_repost` reposts a recent source-community event
  post into a configured target community. The repost caption is the short
  rewrite text only (`build_short_vk_text`), without the title/logistics
  infoblock and without hashtags. Promo-publication URLs are eligible for
  repost only after VK `wall.getById` reports a publish date that is no longer
  in the future; scheduled source posts are not reposted immediately. If VK
  changed the id after postponed publish (`postponed_id -> live id`), the promo
  runner must lazy-resolve both promo exposure URLs and organic
  `event.source_vk_post_url` values before deciding that no source post exists.
- Promo campaign activity `vk_story` publishes a caption-free image story into
  a configured community from a recent source-community event post. It uploads
  the source wall image/poster without passing the source wall URL as VK
  internal `link_url`, because VK renders wall links as a white post/caption
  card under the image. It treats upload as successful only after
  `stories.save` returns a saved story. It must not render title/date/venue into
  a white text card under the image. `stories.getPhotoUploadServer` is called
  with a user actor; group-token-only story delivery is not valid.
- VK promo evidence is stored in `promo_exposure`: `details_json.target_url`
  for each created post/repost/story and `details_json.source_url` for reposts
  and stories. The `/promo report` output must show those concrete links.
- `vk_repost` must call VK `wall.repost` with a user actor first. VK rejects
  `wall.repost` under group authorization (`code=27`), so group-token-only
  retries are not a valid fallback for promo repost delivery.
- The built-in `80 историй о главном` campaign is configured for
  `https://vk.com/klgdevents` (minimum two festival event posts in the last
  24 hours) plus one repost from `klgdevents` to
  `https://vk.com/kenigeventsofficial` when a source post exists in the same
  window. It also publishes two story cards per day from recent `klgdevents`
  festival event posts into each target community:
  `https://vk.com/klgdevents` and `https://vk.com/kenigeventsofficial`.

## Operational Checks

- `vk_source.owner_type` distinguishes community walls (`group`, negative owner id) from personal pages (`user`, positive owner id). Operator-seeded personal sources such as `ivsguide` and `natakkaz` must keep `owner_type='user'` so crawl/review/repost URLs use `wall<user_id>_<post_id>` instead of `wall-<group_id>_<post_id>`.
- Перед production-проверкой убедиться, что заданы `VK_USER_TOKEN` или `VK_ACCESS_TOKEN4`, `VK_EVENTS_GROUP_ID` и целевой `/vkgroup` для daily.
- После VK captcha / замены `VK_USER_TOKEN` проверять не только наличие секрета, но и прямой `photos.getWallUploadServer` для `VK_EVENTS_GROUP_ID`: `ok` без `error_code=14`. Если процесс уже поймал captcha, нужен restart/redeploy для сброса in-memory `_vk_captcha_needed`.
- Для события с несколькими картинками проверять не только наличие `vk_repost_url`, но и attachments в самом VK-посте.
- Для daily проверять два независимых слота: утренний `today` и вечерний `added`; отсутствие событий в одном слоте не должно блокировать второй.
- Для нового daily/event smoke проверять через VK API не только URL, но и авторство: `from_id` должен быть `-<group_id>`, а `likes.can_publish` должен быть `1`. Новые smoke-посты ожидаемо появляются в postponed queue; проверять `publish_date`/`date` и удалять из отложки после проверки, если smoke не должен выйти публично.
- Для guide digest smoke в `uhtykaliningrad` дополнительно проверять, что первая строка содержит count + точные даты/диапазон, пост один, `publish_date` стоит минимум на 10 минут вперёд, есть photo attachments, а Telegram registration/source links либо сокращены через `vk.cc`, либо явно залогированы как shortener fallback.
- Runtime-параметры отложки: `VK_POSTPONED_ENABLED` (default `true`), `VK_POSTPONED_TZ` (default `Europe/Kaliningrad`), `VK_POSTPONED_MIN_INTERVAL_SECONDS` (default `600`), `VK_POSTPONED_START_HOUR` (default `6`).
- Runtime-параметры promo VK: `ENABLE_PROMO_VK_SCHEDULER` (default `true`) и
  `PROMO_VK_INTERVAL_MINUTES` (default `30`).
