# Сценарии для отладки статического сайта

Идентификаторы нужны для стабильных ссылок из багов, Playwright/Gherkin-сценариев и release evidence. Сценарии пользователя сохранены отдельно от сценариев, добавленных по результатам анализа.

## Сценарии, добавленные пользователем

- **USR-01 — Лайк события.**
- **USR-02 — Поделиться событием на десктопе.**
- **USR-03 — Поделиться событием на мобильном устройстве.**
- **USR-04 — Поиск по жанру и времени.** Поискать события, например по названию любимого жанра и признаку времени.
- **USR-05 — Поиск уличного спектакля.**
- **USR-06 — Добавление в календарь и избранное.** Проверить, что событие после добавления видно в разделе «Моё» / «Избранное» или в окончательно выбранном разделе.
- **USR-07 — Привязка почты.** Протестировать привязку почты с использованием ссылки и/или кода подтверждения. Для теста использовать собственную `info@kenigevents.ru` и автоматически проверить поступление письма.
- **USR-08 — Индикатор новых интересных событий.** Проверить, что при появлении в категории новых событий, которые могут быть интересны пользователю, у соответствующего пункта меню появляется выделение в виде красной точки.
- **USR-09 — Визуальная целостность списочных элементов.** Проверить, что списочные элементы не разваливаются.
- **USR-10 — Накопление персонализации.**
- **USR-11 — Нахождение интересного события.** При накопленной персонализации интересное пользователю событие должно встретиться не позднее просмотра 30 карточек.
- **USR-12 — Сброс персонализации.**
- **USR-13 — Почтовый дайджест.** Проверить дайджест интересных событий на почту при развитой персонализации.
- **USR-14 — Подтверждение успешности поиска.** Проверить сценарий, в котором пользователь подтверждает, что нашёл искомое.
- **USR-15 — Проверка транспорта.**
- **USR-16 — Маршрут «Как добраться».** Проверить загрузку маршрута до события в карты от текущей локации пользователя. Должна открыться именно найденная именованная локация события по корректным координатам, а не случайная или безымянная точка.
- **USR-17 — Визуальная проверка изображений.** Проверить, что изображения не обрезают лица в `hero talk`, hero-блоках и каруселях.
- **USR-18 — Desktop/mobile coverage.** Все применимые сценарии проверить как минимум на десктопе и мобильном viewport/device-профиле.

## Дополнительные сценарии по результатам анализа

Эти сценарии не заменяют `USR-*` и не меняют их смысл. Они закрывают найденные пробелы вокруг identity, персонализации, жизненного цикла события, статической публикации, почты, accessibility, SEO/GEO и деградаций.

### Identity, авторизация и почта

- **ADD-ID-01 — Единое состояние identity на всех страницах.** Вход, выход, добавление и забывание email должны одинаково работать на главной, листингах, поиске, странице события, похожих, персональной странице и в избранном; поиск не должен быть единственным владельцем identity UI.
- **ADD-ID-02 — Email вводится один раз.** После подтверждения email сохраняется в разрешённом device-local состоянии, переживает reload/переходы и не запрашивается повторно при каждом добавлении события в календарь.
- **ADD-ID-03 — «Забыть почту на этом устройстве».** Действие очищает локальный email/session state и обновляет интерфейс во всех открытых вкладках, но не выдаёт себя за удаление аккаунта, отзыв согласия или отписку от писем.
- **ADD-ID-04 — Яндекс login/logout и склейка профиля.** Анонимные действия до входа не теряются и не удваиваются после входа; logout не показывает данные другого пользователя, а повторный вход восстанавливает именно его данные.
- **ADD-ID-05 — Email verification edge cases.** Проверить неверный, истёкший и повторно использованный код/ссылку, лимит повторной отправки, смену email и безопасное восстановление после reload/back.
- **ADD-ID-06 — Яндекс без доступного email.** Вход остаётся успешным, но email-зависимое действие явно предлагает отдельную верификацию почты и не обещает несуществующее уведомление.
- **ADD-ID-07 — Переключение аккаунтов.** После выхода пользователя A и входа пользователя B не должны протекать избранное, email, профиль, поисковые теги или персональная выдача пользователя A.

### Персональные страницы, согласие и приватность

- **ADD-PRIV-01 — Публичная секретная ссылка.** Персональная статическая страница открывается без авторизации любому владельцу ссылки, включая другого пользователя, и не переназначается на его текущий профиль.
- **ADD-PRIV-02 — `noindex` персональной страницы.** Страница по секретной ссылке имеет корректный `noindex`, отсутствует в sitemap и внутренних публичных индексах, но остаётся доступной по самой ссылке.
- **ADD-PRIV-03 — Отсутствие персональных утечек.** В HTML, URL, метаданных, share-card, analytics и source map не появляются email, account id, скрытые scores, сырой профиль или история действий.
- **ADD-PRIV-04 — Consent denied/revoked.** При отказе или отзыве согласия удалённая персонализация/telemetry не включается; статическая выдача, поиск, ICS и базовая навигация продолжают работать.

### Персонализация и discovery

- **ADD-PERS-01 — Cold start.** Новый пользователь без истории получает полезную разнообразную статическую выдачу без пустого блока, выдуманных предпочтений и требования авторизации.
- **ADD-PERS-02 — Полная цепочка сигнала.** Один correlated сценарий доказывает: действие записалось в localStorage → принято backend/DB → изменило ожидаемый профиль → повлияло на следующую выдачу.
- **ADD-PERS-03 — Дедупликация сигналов.** Reload, back/forward, повторный клик и повторная доставка telemetry не создают несколько одинаковых действий или искусственно завышенный интерес.
- **ADD-PERS-04 — «Не интересно» и отмена.** Карточка превращается в понятное ненавигационное состояние, `Отменить` восстанавливает её, а сигнал корректно отражается в localStorage, backend и следующей выдаче.
- **ADD-PERS-05 — Межвкладочная согласованность.** Like, favorite, dislike, identity и сброс профиля синхронизируются между вкладками без гонок и возврата устаревшего состояния.
- **ADD-PERS-06 — Отсутствие жёсткого инфопузыря.** У зрелой golden-персоны выполняется целевой лимит нахождения интересного, но в пределах принятого окна остаются контролируемые новые/разнообразные события вне уже известных интересов.
- **ADD-PERS-07 — Нет релевантного предложения в каталоге.** Система не должна считать KPI проваленным или подставлять слабое событие как «точно интересное», если в актуальном inventory нет подходящего предложения.
- **ADD-PERS-08 — Сброс без восстановления из кэша.** После сброса профиль и персональный порядок не возвращаются из старого cache, service worker, history/back или другой вкладки.

### Поиск, теги, похожие и популярное

- **ADD-DISC-01 — Пустой и ошибочный поиск.** Пустой запрос, опечатка, отсутствие результатов, timeout/quota и offline дают понятные состояния и не ломают базовый статический листинг.
- **ADD-DISC-02 — Сохранение поиска как публичного тега.** Intent проходит нормализацию, проверку минимального результата и novelty; практически совпадающий поиск переиспользует существующий тег, действительно новый получает стабильный публичный URL.
- **ADD-DISC-03 — Идемпотентная генерация тега.** Повторный запуск с тем же нормализованным intent/result hash не создаёт дубль страницы или навигационного пункта; rejected/pending тег не попадает в sitemap и public navigation.
- **ADD-DISC-04 — Актуализация сохранённого тега.** После изменения каталога страница тега обновляет результаты одним штатным static rebuild и не сохраняет завершённые/отменённые события как актуальные.
- **ADD-DISC-05 — Качество «Похожих событий».** Список действительно семантически похож, не содержит исходное событие, дубль или ту же программу из блока «Другие даты», не повторяет карточки и имеет безопасный fallback при отсутствии vectors/LLM verdict.
- **ADD-DISC-06 — Автоматическое обновление похожих.** После effectful Smart Update нового/изменённого события, debounce и Kaggle-прохода новый verified related graph попадает в опубликованный static manifest; reverse-affected старые события также обновляются.
- **ADD-DISC-07 — «Популярное» из общей статистики.** Порядок использует единую консолидированную статистику источников и сайта; проверить source-only, site-only и смешанный пример, freshness/last-good и отсутствие отдельной скрытой формулы страницы.

### Избранное, календарь и уведомления

- **ADD-FAV-01 — Идемпотентное сохранение.** Повторное добавление одного события не создаёт дубль в избранном, ICS или reminder; badge показывает число уникальных сохранённых событий только при `N > 0`.
- **ADD-FAV-02 — Полный список «Моё избранное».** Список открывается из общего меню на всех страницах, показывает все сохранённые события и явно различает upcoming, перенесённые, отменённые и уже завершившиеся.
- **ADD-FAV-03 — Удаление и отмена.** Удаление из карточки, detail и избранного приводит к одному состоянию; undo не создаёт второй объект или второй reminder.
- **ADD-FAV-04 — Корректность ICS.** Проверить title, именованную локацию, координаты/адрес, timezone, начало/конец, canonical URL, перенос и отмену в реально импортированном календаре.
- **ADD-FAV-05 — Видимое обещание D-1 письма.** После добавления интерфейс явно показывает, будет ли письмо за день, на какой masked email и что нужно сделать, если email отсутствует/не подтверждён или consent не дан.
- **ADD-FAV-06 — D-1 идемпотентность и изменение события.** Повторный scheduler run не отправляет второе письмо; reschedule пересчитывает уведомление, отмена его подавляет, слишком позднее сохранение следует принятой catch-up policy.
- **ADD-FAV-07 — Раздельность like/favorite/transport.** Like, скачанный ICS и отдельно сохранённый транспортный участок не должны ошибочно увеличивать badge сохранённых событий.

### Почтовые подборки и deliverability

- **ADD-MAIL-01 — Ровно три предложения и рабочая персональная ссылка.** Письмо содержит три актуальных неповторяющихся события, а его страница соответствует тому же snapshot/пользователю без PII в URL или HTML.
- **ADD-MAIL-02 — Suppression и bounce.** Hard bounce, complaint, unsubscribe и действующая suppression исключают следующую отправку; provider event применяется идемпотентно и не понижает репутацию другого mail stream/поддомена.
- **ADD-MAIL-03 — Ссылки и представление письма.** Проверить canonical/UTM links, plain-text fallback, изображения-disabled режим, mobile/desktop mail clients и отсутствие preview/secret leakage.
- **ADD-MAIL-04 — Устаревшее событие между enqueue и send.** Отменённое, перенесённое или завершившееся событие не уходит со старой датой; применяется revalidation или безопасная отмена отправки.

### Связанные даты и жизненный цикл события

- **ADD-EVENT-01 — Несколько дат/времени одной программы.** Карточка явно показывает наличие альтернатив, открывает полный список на detail и сохраняет именно выбранный occurrence с правильными URL/ICS/reminder.
- **ADD-EVENT-02 — Same-slot duplicate не является альтернативой.** Дубли одного времени объединяются/исключаются, а не показываются как «другая дата».
- **ADD-EVENT-03 — Перенос, отмена и завершение.** Изменение статуса согласовано на главной, листингах, поиске, тегах, похожих, избранном, detail, share и calendar/reminder surfaces.
- **ADD-EVENT-04 — Возрастная маркировка.** Подтверждённое `0+|6+|12+|16+|18+` одинаково видно на каждой event-bearing surface; неизвестное значение остаётся без выдуманного `0+`.
- **ADD-EVENT-05 — Критические факты совпадают.** Название, дата/время, timezone, город, площадка, адрес, ticket/free status и canonical link совпадают между карточкой, detail, JSON-LD, ICS, письмом и share payload.

### Изображения, медальоны и share

- **ADD-MEDIA-01 — Нет дублей изображений внутри события.** Проверить exact SHA, re-encode и визуально одинаковые crop/overlay варианты; содержательно разные постеры/программа/фото сохраняются.
- **ADD-MEDIA-02 — Умный фокус во всех форматах.** Лицо, заголовок афиши и главный объект не теряются в mobile/desktop card, hero, carousel, related, email и share-card; OCR-постер не обрезается как фотография.
- **ADD-MEDIA-03 — Битое/отсутствующее изображение.** Broken CDN URL, slow image и отсутствие media дают чистый fallback без layout shift, пустой alt, бесконечного loader или потери ссылки на событие.
- **ADD-MEDIA-04 — Медальоны на всех static-site surfaces.** Playwright снимает фактические места на mobile/desktop; artwork, ring и shadow не обрезаны и не загрязнены. Telegram-медальоны в этот сценарий не входят.
- **ADD-SHARE-01 — Adaptive share одного события.** На mobile вызывается системный share с корректным fallback; на desktop выполняется принятый clipboard-сценарий. Обе версии используют canonical URL и правильную карточку именно этого события.
- **ADD-SHARE-02 — Поделиться самим сервисом.** Одна кнопка доступна в раскрытом mobile menu и footer, на desktop работает как copy; используется заранее отрендеренная актуальная карточка, а не on-demand генерация.
- **ADD-SHARE-03 — Отмена и недоступность системного share/clipboard.** Cancel, permission denial, insecure context и unsupported API дают честное ненавязчивое состояние и рабочий fallback, а не ложное «отправлено».

### Транспорт

- **ADD-TR-01 — Eligibility транспорта.** Транспортная подсказка показывается только для поддерживаемого события вне Калининграда и не появляется с нерелевантным маршрутом для городского события.
- **ADD-TR-02 — Туда/обратно и достижимость.** Рейс действительно позволяет прибыть до начала и, если обещан обратный путь, уехать после завершения с допустимым запасом времени.
- **ADD-TR-03 — Provider stale/partial failure.** При недоступности КППК или автобусов показывается last-good с честной свежестью либо безопасное отсутствие блока; данные одного provider не затирают исправные данные другого.
- **ADD-TR-04 — Транспортная карточка в галерее.** Если опция включена, «Как добраться» является отдельной понятной карточкой, не выдаёт себя за афишу и совпадает с detail route data.
- **ADD-TR-05 — Избранный транспортный участок.** Повторное сохранение идемпотентно, относится к выбранному occurrence/направлению и не увеличивает event-favorite badge.

### Социальные действия и обратная связь по событию

- **ADD-SOC-01 — Консолидированные просмотры, лайки и share.** Действия на сайте учитываются вместе со статистикой Telegram/VK через один общий event-level contract; retry/reload не удваивает действие, stale source не превращается в ноль, а все consumers видят совместимые totals.
- **ADD-SOC-02 — Честные публичные счётчики.** Нулевые значения скрыты по принятому UI-контракту, displayed total совпадает с принятой проекцией, а пользовательский интерфейс не называет reach-наблюдения уникальными людьми.
- **ADD-FEEDBACK-01 — Проверенные факты из комментариев.** На странице показываются только актуальные event-bound факты с допустимым source evidence; вопрос, слух, отрицание, неопределённость, ответ неавторитетного пользователя или комментарий к другому событию не становятся утверждением.
- **ADD-FEEDBACK-02 — Исправление и протухание факта.** Correction, retraction и TTL удаляют/заменяют публичный факт одним rebuild, не переписывают критические date/time/location поля и не оставляют старую формулировку в HTML/cache.
- **ADD-FEEDBACK-03 — «Активно обсуждают».** Если post-release medallion включён, он появляется только при выполнении принятого human-discussion threshold, не учитывает ботов/официальные ответы/дубли/engagement bait и не влияет на ranking без отдельного решения.

### Статическая публикация, CDN и устойчивость

- **ADD-BUILD-01 — Debounce после Smart Update.** Несколько effectful обновлений в пределах окна приводят к одному build через 15 минут после последнего эффекта; no-op update не запускает лишнюю сборку.
- **ADD-BUILD-02 — Atomic promotion и rollback.** Пользователь не видит смесь HTML/assets разных releases; failed check не меняет current, а rollback полностью возвращает последний проверенный manifest.
- **ADD-BUILD-03 — Catalog parity и отсутствие устаревших страниц.** Все eligible события присутствуют, ineligible/quarantined не опубликованы, удалённые slug получают принятую redirect/410 policy и не остаются в sitemap.
- **ADD-BUILD-04 — CDN asset contract.** Canonical HTML/assets/ICS реально обслуживаются через принятый CDN path; runtime raster assets легковесные WebP, векторные — безопасные SVG, нет случайных тяжёлых PNG/JPEG или origin-only зависимостей.
- **ADD-BUILD-05 — Degraded network.** Slow 3G, offline after first load, CDN image failure и временная недоступность personalization/search API не ломают статический контент, навигацию, detail и ICS.
- **ADD-BUILD-06 — Cache/back/version transition.** После публикации новой версии reload/back/forward/service cache не возвращают несовместимые JS/data или состояние старого пользователя.

### UI, accessibility и browser matrix

- **ADD-UI-01 — Единая навигационная модель.** Mobile tag/menu и desktop header сохраняют одинаковые destinations, labels, порядок, active state, избранное и identity semantics.
- **ADD-UI-02 — Граничные viewport.** Помимо обычных mobile/desktop проверить узкий телефон, landscape, tablet и ширины непосредственно до/после breakpoint; исключить horizontal overflow и скачки layout.
- **ADD-A11Y-01 — Keyboard/focus.** Все ссылки, меню, карусель, modal/gallery, like/share/calendar, email form и undo доступны с клавиатуры; focus видим, не заперт и возвращается к trigger.
- **ADD-A11Y-02 — Screen reader semantics.** У controls корректные accessible names/state/live announcements; карточка не создаёт конфликтующих вложенных ссылок/кнопок, возраст и альтернативные даты читаются понятно.
- **ADD-A11Y-03 — Reduced motion.** `prefers-reduced-motion` отключает parallax/автодвижение без потери контента или управления; autoplay не мешает чтению.
- **ADD-A11Y-04 — No-JS.** Основные страницы, факты, ссылки, похожий static fallback, navigation и SEO-content остаются доступными без JS; enhancement не оставляет пустые обязательные блоки.
- **ADD-UI-03 — Длинный и экстремальный контент.** Проверить длинные названия, площадки/адреса, несколько медальонов, отсутствие цены, пять возрастных вариантов, много дат и 1/10+ изображений.
- **ADD-UI-04 — Browser/device matrix.** Финальный RC проверить минимум в актуальных Chrome/Edge/Firefox на Windows, Safari/Chrome/Firefox на macOS и Safari iOS/Chrome Android; native share/clipboard/calendar проверять на реальных ОС, не только Playwright mocks.

### SEO/GEO, безопасность и наблюдаемость

- **ADD-SEO-01 — Canonical/robots/sitemap.** Каждая публичная page family имеет корректные status, canonical, indexability и sitemap membership; preview, secret и personal URLs не индексируются.
- **ADD-SEO-02 — Structured-visible parity.** JSON-LD не расходится с видимыми title/date/location/status/age/ticket facts и не сохраняет устаревший occurrence после rebuild.
- **ADD-SEO-03 — Share/search preview.** Open Graph и остальные preview metadata ведут на canonical страницу, используют доступное CDN-изображение и не содержат персональные/preview URL.
- **ADD-SEC-01 — XSS и URL safety.** Source title/description/address/comment facts/search input и generated tags не могут внедрить script/HTML; внешние URL используют допустимые схемы и безопасные атрибуты.
- **ADD-SEC-02 — Auth/admin boundaries.** Обычная авторизация, знание event id или `noindex` не открывают admin report/history; повторный report защищён от двойного запуска и показывает реальный статус/результат нескольких incidents.
- **ADD-OBS-01 — Ошибки видимы и коррелируются.** Build/search/share/email/transport/personalization failure оставляет run/request correlation evidence без PII; UI не сообщает успех, если backend действие не принято.
- **ADD-OBS-02 — Чистая клиентская сессия.** На каждой page family нет неожиданных console errors, unhandled rejections, failed first-party requests, hydration mismatch и повторяющегося telemetry spam.

## Правило исполнения

- `USR-*` — исходные сценарии пользователя; их нельзя тихо удалить, переименовать по смыслу или закрыть только unit-тестом.
- `ADD-*` — дополнительные сценарии аудита; при переносе в Gherkin/Playwright идентификатор сохраняется в названии scenario/test и в release evidence.
- Для каждого применимого сценария фиксируются URL/page family, viewport/device/OS/browser, test identity/persona, catalog/build SHA, входное состояние, ожидаемый результат и фактическое evidence.
- Автоматизация не заменяет native/manual evidence там, где проверяется системный share, clipboard/paste, email client, calendar/maps handoff или реальное визуальное качество.

### Дизайн-система и недопущение визуального drift

- **ADD-DS-01 — Каталог собирается из runtime-компонентов.** `/lab/design-system/` существует в immutable preview, содержит один H1, foundations, primitives, feedback states, product components и registry; карточки/hero/CTA импортированы из production source, а не скопированы как demo HTML.
- **ADD-DS-02 — Полные состояния primitives.** Для Button видимы default/hover/focus/pressed/loading/disabled/selected; для Field — default/focus/filled/error/disabled; loading/error доступны screen reader через `aria-busy`, `aria-invalid` или live region.
- **ADD-DS-03 — Token-only shared primitives.** Shared primitives не содержат raw hex и получают цвет, spacing, radius, elevation, motion и min target из `--ke-*`; пары текста/фона проходят AA contract check.
- **ADD-DS-04 — Responsive component catalog.** На `320/360/390/430/768/1024/1366/1440/1920` нет page-level horizontal overflow; таблица registry имеет явно ограниченный focusable overflow; product fixtures не разваливаются.
- **ADD-DS-05 — Promotion governance.** Новый утверждённый паттерн имеет status/owner/runtime path, все состояния, immutable preview/SHA sign-off, contract check, docs и changelog; page-local утверждённый дубль считается release blocker.
- **ADD-DS-06 — Deprecated variant is explicit.** `EventCard overlay-controls` помечен deprecated и не возвращается на обычные event pages; `split-actions` остаётся release baseline до нового зафиксированного решения.
- **ADD-DS-07 — Reduced motion and keyboard.** Все catalog actions доступны с клавиатуры, focus видим, порядок логичен; `prefers-reduced-motion` отключает shimmer и необязательное движение без потери статуса/контента.
- **ADD-DS-08 — Icon-only copy action.** Видимое значение остаётся обычным выделяемым текстом, рядом одна copy-icon кнопка не меньше `44×44px` с точным accessible name. Клик кладёт в буфер точное значение; success меняет copy на check и озвучивает live-status, error даёт символ и recovery status, при этом bounding box кнопки и соседнего текста не меняется. Clipboard API и fallback проверяются отдельно на light/secondary и dark/inverse fixtures.
- **ADD-DS-09 — Versioned component migration.** Материальная переработка `vN` создаёт видимую `vN+1`; `vN` помечена deprecated и содержит replacement, все production consumers переведены на `vN+1`, а незавершённое сосуществование имеет feature flag, owner, consumer inventory и срок удаления. Source/preview audit не допускает старых callers после статуса migration complete.

### Списочные страницы по датам

- **ADD-LIST-01 — Точное время и перенос.** События одного точного времени находятся в одной группе и заполняют доступную ширину intrinsic-карточками; `18:30` не смешивается с `18:00`, длинная группа переносится на следующие строки и не создаёт page-level horizontal overflow на `1366/1440/1920`.
- **ADD-LIST-02 — Гибридная навигация.** Sticky-бар показывает периоды и количества. Точные часы раскрываются только у плотного периода, ведут на существующий заголовок и передают ему keyboard focus; Escape и клик снаружи закрывают disclosure. После фильтрации нулевой период остаётся на месте disabled, нулевой точный час скрывается.
- **ADD-LIST-03 — Сегодня, завершение и текущее время.** В `Завершились` попадают только события с явным `end_at <= build/runtime now`; один лишь start time не делает идущее событие прошедшим. Секция закрыта по умолчанию и стоит выше `Сейчас · HH:MM`; past-state обесцвечивает только media, но не снижает контраст текста. `Сейчас` обновляется без перемещения групп по одному start time.
- **ADD-LIST-04 — Завтра без ложной срочности.** На `/zavtra/` отсутствуют `Сейчас` и `Начались ранее`; все группы идут по времени, а точная навигация появляется только при достаточной плотности.
- **ADD-LIST-05 — Плотные выходные.** Десятки событий остаются на одной странице: DOM и visual order одинаковы `время слева → суббота → воскресенье`; одна union-ось печатает точное время один раз, две непрерывные lane-поверхности не разбиваются на клетки/строчные подложки/границы. Высота строки равна более высокой day-cell, пустая сторона не схлопывает композицию, крупное время остаётся видимым внутри длинного часа, sticky-заголовки дней не перекрывают общий бар. На реальных 62 карточках document height ≤7600px при 1440px, плотные 11:00/19:00 ≤720px и ничего не скрывается пагинацией.
- **ADD-LIST-06 — Городской intersection.** Реальные города и counts постоянно видимы direct chips на desktop без dropdown: default `Все`, первый город изолирует выбор, следующие добавляются/убираются, `Все` сбрасывает. Выбранность имеет check-marker, а не только tint; wrap сохраняет ровное начало строк и 44px targets. Фильтр синхронно пересчитывает общий итог, периоды, точные часы, `Начались ранее` и обе day-surface; неизвестный сохранённый выбор не оставляет новую страницу пустой.
- **ADD-LIST-07 — Полный список и персональный режим.** Без совместимого consented-профиля статический полный список остаётся видимым, переключатель не обещает несуществующую персонализацию. При реальной разнице `Для меня / Полный список` v2 меняет только client-enhancement visibility и повторно запускает единый пересчёт counts.
- **ADD-LIST-08 — Семантический выбор медиа V15.** Wide candidate выигрывает только внутри допустимого existing event inventory; `unknown` wide source может быть выбран в собственном natural ratio без crop (control `6875`: 1280×960). `ocr_text`, `unknown`, identity poster, schedule и attendee document сохраняют natural ratio/vertical retention `1.0`; отсутствие OCR не становится разрешением на crop. Adaptive 3:2/4:3 получает только classified event photo с `safe_crop`, focal evidence и reviewed/high-confidence role. `no_event_relevance` (control `6904`) не возвращается generic fallback-ом. `3794` не содержит event-id override/V13 `bb778...`, но рендерит существующий canonical 300×174 как no-upscale last resort. Новый crawler в acceptance не входит. Skeleton занимает финальную рамку до load/decode.
- **ADD-LIST-09 — Дубли не маскируются интерфейсом.** Exact identity dedup не скрывает разные сеансы. Семантический/time-drift дубль исправляется через incident-owned LLM-first merge flow; preview reconciliation имеет evidence, а production route не получает title-regex или id blacklist.
- **ADD-LIST-10 — Общая дизайн-система.** Today/Tomorrow/Weekend используют зарегистрированные runtime-компоненты и глобальные `--ke-content-listing-max`/`--ke-listing-*` из общей DS; page-local копии shell/card/navigation geometry блокируют check.
- **ADD-LIST-11 — Responsive media и быстрый scroll.** Listing card использует доступные WebP derivatives через `srcset/sizes`, задаёт intrinsic `width/height`, regular slot объявлен как 332px (396px на wide desktop), Weekend как 245/291px; для проверенных 3:2 source crops Weekend объявляет фактический адаптивный слот 267/297px, чтобы браузер не выбирал превью ниже rendered width. Если inventory заканчивается на 512w, encoded original остаётся последним DPR safety candidate: браузер выбирает его только когда derivative физически меньше rendered frame × DPR. Первые две карточки первого видимого часа имеют eager/high priority; остальные получают `src/srcset` через near-viewport observer (`200px`) и сохраняют `<noscript>` fallback. На матрице DPR ограничивающий raster density ≥1; skeleton снимается после decode/error, CLS <0.05.
- **ADD-LIST-12 — Общая левая линия и полное название V17.** Canonical title целиком остаётся в DOM. Каждый singleton начинается от левого края своего flow: card left и visual left равны flow left с допуском 1px, включая controls `6685`, `6953`, `6590`; copy envelope получает до 420px без центрирования. В multi-card ряду enhancement получает только фактическая последняя видимая карточка при tail ≥72px и available ≥280px. После filter/resize/fonts actual rows пересчитываются; >4 строк, collision или overflow откатывают enhancement.
- **ADD-LIST-13 — Одна стабильная discovery plane V18.** На desktop opaque sticky site header имеет 57px и переиспользует общий компонент. Под ним cities и time/daypart находятся в одной light `52px` grid-plane, не в двух последовательных подшапках. До и после scroll должны совпадать height/class/attributes/font; `IntersectionObserver`, `ResizeObserver`, `data-stuck` и delayed compact state отсутствуют. Counts скрыты на 1536 и видимы на 1920; dropdown и document X-overflow запрещены.
- **ADD-LIST-14 — Weekend visual scale.** На 1366/1440/1536/1920 Weekend media height равен 178…198px, title 20px, meta 17px; нет искусственной min-height/пустых строк. При 1536×768 первая карточка целиком заканчивается не ниже y=720. При 200% text spacing selective full-title может отключиться, но controls/content не пересекаются.
- **ADD-LIST-15 — Desktop adaptive matrix V18.** Playwright проверяет 1366×768, 1440×900, 1536×864 (FHD@125% CSS viewport) и 1920×1080. `6762`/`6867` остаются reviewed adaptive фото без полей; `3794` использует canonical `300×174`, не содержит replacement `bb778...`, но его media box имеет общую regular-height `221px` с bounded scale ≤1.30. Protected controls `6899`, `6876`, `6893`, `3262`, `5204`, `6732`, `6546`, `6849`, `6944` не входят в `visual-crop` и имеют vertical retention `1.0`. Меряются 51…64px quiet medallions без universal border/shadow, no-overflow cities, sticky stack, Weekend heads и counts. Row count при 1920 не ухудшается; фактические 3-up группы существуют.
- **ADD-LIST-16 — Behavioral Popular V18.** `/populyarnoe/` рендерит `PopularBehaviorRows@1` и общие `ListingEventCard@8`, а не deterministic event-type filter. Должно быть 4–5 полок в приоритетном порядке `fast_growth` → `multi_source` (если ≥3) → `discussed` → `frequently_shared` → fallback, максимум пять карточек в одной desktop-строке. Ни event id, ни normalized title+type+venue/city family не повторяются между полками; `scrollWidth == clientWidth` на 1536.
- **ADD-LIST-17 — Contextual identity/social-proof rail V18.** До трёх listing-ready identity/Free медальонов сохраняются и при proof. При regular-height `221px` event `6811` упаковывает три `51px` identity и Share/Like в один `64px` rail; при Weekend-height `178px` используется split rail `56px + 36px = 96px`. Overlay разрешён системно только selected source-reviewed `classified event_photo`, confidence ≥0.9, `visual_only`, `safe_crop`, focal, ratio ≥1.2; genuine no-media fallback может содержать identity. Controls `6950`/`6957` имеют overlay ровно `10px` справа/снизу. OCR/unknown/rejected raw media fail closed. Calendar/Share/Like buttons отсутствуют; только ненулевые Share/Like — muted detail links, нули не занимают DOM/ширину.
- **ADD-LIST-18 — Sticky exact-time context.** На `/segodnya/` и `/zavtra/` при прокрутке длинной группы маркер часа фиксируется непосредственно под discovery surface и остаётся видимым, пока следующий час не вытолкнет его. Проверка якоря `#tomorrow-time-17:00`: после последовательной прокрутки внутри группы top маркера равен sticky-stack bottom + `12px` (±2px), а следующий маркер не перекрывает предыдущий.
- **ADD-LIST-19 — No preview fields V18.** Natural OCR/unknown cards сохраняют source ratio в media width при общей density-height и `object-fit:cover` без meaningful crop; vertical retention равен `1.0`. Низкоразрешённый control `3794` допускает bounded upscale до общей `221px` height, но не asset replacement, `contain` в несовпадающей рамке, UI side/top fields, repeated edges или backdrop filler. Safe adaptive cards обязаны иметь `source-reviewed` crop evidence.
- **ADD-LIST-20 — Weekend complete sticky context V17.** Marker top равен `site header + measured discovery + Weekend day heads + 12px` (±2px). Прямая загрузка `#weekend-time-17:00` повторно выравнивается после fonts и каждого изменения discovery geometry; последовательные прогоны/scroll не дают промежуточного сдвига, верх media не обрезан.
- **ADD-LIST-21 — Weekend day counts.** Под временем рендерится только маленький filled weekday chip `сб`/`вс`; число и грамматически корректная подпись (`1 событие`, `2 события`, `5 событий`) идут рядом обычной светлой типографикой. Нулевой день скрыт. После city/personal filter значения пересчитываются.
- **ADD-LIST-22 — Unknown-end elapsed starts.** Явный `end_at <= now` получает completed state. Для текущей даты start `<= now - 1h` без end получает только `Началось ранее`: media muted, readable title/meta/proof, без утверждения «завершилось/недоступно». Событие с известным будущим end не мутится независимо от start.
- **ADD-LIST-23 — Weekend range cloud V17.** Над и после расписания присутствуют шесть direct weekend range anchors. Текущий range имеет нецветовую selected-state; локальная длинная стрелка идёт отдельным `aria-hidden` non-anchor continuation sibling после облака, а не внутри terminal link. Ссылки ведут на существующие static routes.
- **ADD-LIST-24 — Truthful calendar proof.** Listing не рендерит calendar aggregate из ICS URL/download/click. Календарик допустим только при ненулевом privacy-safe deduplicated durable `saved_event_count`; при нуле/отсутствии поля нет иконки/плейсхолдера/зарезервированной ширины. Save action на detail остаётся отдельным контрактом.
- **ADD-LIST-25 — Reviewable personalization V17.** Immutable `/preview-*` автоматически показывает контролируемую реальную разницу `Для меня / Полный список`; `?personalization=off` выключает fixture. Full list остаётся default. Radio semantics (`aria-checked`, roving tabindex, arrows) и переключение пересчитывают city/daypart/exact/day counts; personal set <5 fail closed. Stable production URL не получает fixture.
- **ADD-LIST-26 — Measured within-hour packing V18.** Только внутри одного day + exact time допускается best-fit permutation по фактическому `flow.clientWidth`, и только если projected row count уменьшается. Она не пересекает day/time; original order хранится для repeat/resize. На 1536 lane ≈641.6px и большинство совместимых групп используют 2-up; на 1920 присутствуют 3-up группы. Listing container растёт до `1720px`, без X overflow.
- **ADD-LIST-27 — Floating personalization research V18.** `ListingPersonalFilter@3` существует одним bottom-center radio group вне discovery plane; full list выбран по умолчанию, control полностью помещается в viewport, исчезает при footer visibility и остаётся preview-only fixture. Mobile использует нижнее safe-area размещение, но явный `Комфортно / Компактно` density control и native-zoom acceptance остаются будущим отдельным pass.
