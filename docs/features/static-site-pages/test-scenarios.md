# Сценарии для отладки статического сайта

Идентификаторы нужны для стабильных ссылок из багов, Playwright/Gherkin-сценариев и release evidence. Сценарии пользователя сохранены отдельно от сценариев, добавленных по результатам анализа.

> **Статус:** канонический acceptance inventory, а не журнал уже пройденных
> тестов. Наличие ID не означает, что функция реализована, автоматизирована или
> принята в production. Full release scope определяет
> [Stage 0–12 readiness checklist](../../reports/static-personal-announcements-release-readiness-2026-07-11.md);
> [release-plan.md](release-plan.md) владеет только platform/Telegraph slice.

## Уровни доказательства

- **Automated release** — выполняется против clean main-reachable production
  candidate/public surface и выдаёт сохраняемое evidence.
- **Automated component** — unit/build/fixture contract; полезен, но один не
  закрывает release scenario.
- **Demo-only** — исполняемый stand/mocked client, не текущий Astro/public site.
- **Draft** — Gherkin/spec без полного runnable step/runtime path.
- **Manual/native** — требуется реальная ОС, browser/app/email/calendar/maps или
  визуальный review; mocks не заменяют evidence.
- **Planned** — product/implementation ещё отсутствует в `origin/main`.

## Карта текущего покрытия на 2026-07-17

| Surface | Сценарии | Уровень сейчас | Канонический test/command | Что не доказано |
|---|---|---|---|---|
| Preview build, files, routes, JSON-LD, ICS, media/static related | `ADD-BUILD-*`, `ADD-SEO-*`, часть `ADD-EVENT/MEDIA-*` | Automated component | `npm --prefix site run build:preview && npm --prefix site run check:preview` | production root, full live catalog, interaction, promotion/rollback |
| Public projection eligibility/media safety | `ADD-BUILD-03/04`, `ADD-MEDIA-*` | Automated component | `pytest -q tests/test_static_site_public_gate.py` | whole-catalog reconciliation and live HTTP |
| Kaggle command handoff | `ADD-BUILD-08` | Automated component | `pytest -q tests/test_static_site_build_handoff.py` | real Kaggle → promotion/catch-up; ID must be added to the runnable test name |
| Anonymous personalization | `USR-10..12`, `ADD-PERS-*` | Demo-only + Draft | `tests/playwright/static_personalization_contract.spec.ts` (9 mocked demo tests); `tests/e2e/features/static_site_personalization.feature` (`@draft`, no Behave steps) | current Astro/public E2E and backend persistence |
| Share/calendar/maps/email/native browser | `USR-02/03/06/07/13/15/16`, `ADD-SHARE/FAV/MAIL/TR-*` | Manual/native or Planned | per-scenario evidence | real target applications and not-yet-implemented durable flows |
| Atomic release and Telegraph cutover | `ADD-BUILD-*`, `ADD-CUTOVER-*` | Planned | future production publisher/resolver suite | весь production cutover contract |

### Release evidence row contract

При автоматизации или ручном приёмочном прогоне к ID добавляется отдельная
evidence row/report со следующими полями: release tier, implementation status,
automation level, canonical test/command, fixture/persona, URL/page family,
viewport/device/OS/browser, production SHA, snapshot/build/manifest id, expected
result, actual result, artifact/log link и reviewer. Эти поля не подменяются
словом `passed` без привязки к конкретной сборке.

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

### Regression-сценарии изменений 15–17 июля

- **ADD-DS-01 — Каталог собирается из runtime-компонентов.** `/lab/design-system/` содержит foundations, primitives, feedback states, product components и registry; product components импортированы из runtime source, а не скопированы как demo HTML.
- **ADD-DS-02 — Полные состояния primitives.** Button/Field и feedback components показывают accepted default/hover/focus/pressed/loading/disabled/selected/error states с корректными ARIA semantics.
- **ADD-DS-03 — Token-only shared primitives.** Shared primitives используют `--ke-*` tokens, не локальные raw-значения, а text/background pairs проходят AA contract.
- **ADD-DS-04 — Responsive component catalog.** Acceptance widths `320/360/390/430/768/1024/1366/1440/1920` не дают page-level overflow или сломанных product fixtures.
- **ADD-DS-05 — Promotion governance.** Новый shared pattern имеет status/owner/runtime path, states, immutable preview/SHA sign-off, contract check, docs и changelog; утверждённый page-local дубль блокирует release.
- **ADD-DS-06 — Deprecated variant is explicit.** `EventCard overlay-controls` остаётся deprecated/regression-only, а `split-actions` — baseline до нового зафиксированного решения.
- **ADD-DS-07 — Reduced motion and keyboard.** Catalog actions доступны с клавиатуры; focus видим; `prefers-reduced-motion` отключает необязательное движение без потери статуса/контента.

- **ADD-RECENT-01 — Interest-club projection gate.** В public manifest и sitemap
  попадают только approved/fresh club identities и accepted event relations;
  deferred/review/festival-only/устаревшие identities не протекают, а failed club
  projection не блокирует базовый event build.
- **ADD-RECENT-02 — Desktop Editorial media-role contract.** На desktop строгий
  `event_identity_poster` получает полный poster companion, photo сохраняет
  принятую Editorial composition, а service/schedule/document/OCR/unknown media
  не превращаются в crop-cover из-за размеров или наличия текста.
- **ADD-RECENT-03 — Owned social popularity без двойного счёта.** Метрики всех
  управляемых TG/VK event surfaces собираются, но repost одного события внутри
  owned audience схлопывается по max-компонентам, а независимые внешние источники
  продолжают учитываться отдельно.
- **ADD-RECENT-04 — Возрастная маркировка.** Публично показывается только
  подтверждённое declared значение `0+|6+|12+|16+|18+`; assessed-only/unknown не
  превращается в выдуманное ограничение, а видимый факт совпадает с export и
  structured data.

### Статическая публикация, CDN и устойчивость

- **ADD-BUILD-01 — Debounce после Smart Update.** Несколько effectful обновлений в пределах окна приводят к одному build через 15 минут после последнего эффекта; no-op update не запускает лишнюю сборку.
- **ADD-BUILD-02 — Atomic promotion и rollback.** Пользователь не видит смесь HTML/assets разных releases; failed check не меняет current, а rollback полностью возвращает последний проверенный manifest.
- **ADD-BUILD-03 — Catalog parity и отсутствие устаревших страниц.** Все eligible события присутствуют, ineligible/quarantined не опубликованы, удалённые slug получают принятую redirect/410 policy и не остаются в sitemap.
- **ADD-BUILD-04 — CDN asset contract.** Canonical HTML/assets/ICS реально обслуживаются через принятый CDN path; runtime raster assets легковесные WebP, векторные — безопасные SVG, нет случайных тяжёлых PNG/JPEG или origin-only зависимостей.
- **ADD-BUILD-05 — Degraded network.** Slow 3G, offline after first load, CDN image failure и временная недоступность personalization/search API не ломают статический контент, навигацию, detail и ICS.
- **ADD-BUILD-06 — Cache/back/version transition.** После публикации новой версии reload/back/forward/service cache не возвращают несовместимые JS/data или состояние старого пользователя.

### Telegraph coexistence и D10 cutover

- **ADD-CUTOVER-01 — Dual-run создаёт оба event-detail артефакта.** В D0–D9
  eligible событие попадает в current static release и в Telegraph shadow path;
  отчёт связывает оба URL с одним event revision.
- **ADD-CUTOVER-02 — Readiness до выдачи static URL.** Resolver возвращает static
  URL только если current promoted manifest содержит нужные `event_id` и revision;
  до D10 допускается явный existing-Telegraph fallback, после D10 — ожидание/catch-up,
  но не создание Telegraph.
- **ADD-CUTOVER-03 — D10 запрещает create и recreate.** В `existing_only` и `off`
  новый event не вызывает Telegraph create, а failed edit старой страницы не
  провоцирует fallback-recreate; счётчики попыток после cutoff равны нулю.
- **ADD-CUTOVER-04 — Legacy Telegraph URLs сохраняются.** Cutover, rebuild,
  rollback и cleanup не очищают `telegraph_url/path`; старые опубликованные ссылки
  остаются доступными и не меняют canonical нового static event page.
- **ADD-CUTOVER-05 — Все outbound surfaces используют resolver.** Telegram/VK
  event posts, importer waits, digests и admin reports не зависят от успешного
  `telegraph_build`, когда static revision ready; в dual report разрешены оба URL.
- **ADD-CUTOVER-06 — Rollback обратим и ограничен.** До D10 outward mode можно
  вернуть в Telegraph/dual и восстановить last-good static pointer; после D10
  emergency `create_edit` включается только явно и выполняет bounded backfill без
  удаления legacy URL/evidence.
- **ADD-CUTOVER-07 — Aggregate scope не выключается случайно.** Event-detail D10
  не останавливает month/weekend/festival Telegraph pages; их отдельный switch
  разрешён только после static parity и собственного acceptance pack.

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

## Release-orchestration scenarios из полного Stage 0–12 checklist

Эти IDs восстанавливают требования, которые не помещались в прежние широкие
`ADD-*` families. Они остаются `Planned`, пока ID не связан с runnable test/manual
evidence row на frozen SHA.

### Scope, quality и RC

- **ADD-RC-01 — Scope freeze.** Mandatory scope F1–F13/F15–F18, H1 decision, M1–M6 и post-release exclusions зафиксированы без тихого сокращения.
- **ADD-RC-02 — Main reachability ledger.** Каждый candidate commit достижим из `origin/main` либо имеет явный `reject|supersede|clean-port` disposition.
- **ADD-RC-03 — Immutable versions and sign-off.** UI, data/schema versions, preview/build ID и owner sign-off относятся к одному SHA.
- **ADD-RC-04 — Evidence pack completeness.** Manifest, commands, checks, screenshots/native evidence, production probes, reviewer и rollback target присутствуют и взаимно согласованы.
- **ADD-RC-05 — Limited canary and quality window.** Canary выдерживает полные 14 дней без critical event defect или recurrence закрытого root cause.
- **ADD-RC-06 — Launch closure.** Rollback/on-call drill, noindex removal, 72-hour hypercare и 14-day review привязаны к точному release.
- **ADD-QUALITY-01 — Audit cadence.** Все новые/изменённые события проверяются ежедневно, active/future inventory — по принятому полному cadence.
- **ADD-QUALITY-02 — Closure-grade replay.** Исправление проходит реальный import boundary → Smart Update → DB → static/Telegram/VK/Telegraph/ICS surfaces.
- **ADD-QUALITY-03 — Incident regression pack.** Канонические duplicate/location/date/recurrence/venue incidents воспроизводятся и не регрессируют.
- **ADD-QUALITY-04 — Zero-critical stability.** Четырнадцать последовательных дней имеют ноль новых critical и ноль recurrence/reopen; нарушение перезапускает окно.

### Production build, URLs и related/search

- **ADD-BUILD-07 — Production profile isolation.** Production build indexable/canonical/full-catalog, preview остаётся noindex и не смешивает lab/fixture routes.
- **ADD-BUILD-08 — Immutable Kaggle handoff.** Snapshot → unique Kaggle input → heartbeat/lease → checked artifact коррелируются одним build ID.
- **ADD-BUILD-09 — Release manifest parity.** Manifest хранит repo/snapshot/schema/schedule hashes, page/event counts, checks, freshness и asset hashes; фактический tree совпадает.
- **ADD-BUILD-10 — Staging/current/rollback.** Failed candidate не меняет current; promotion reader-atomic; one-command rollback возвращает полностью согласованный last-good tree.
- **ADD-BUILD-11 — Canonical delivery.** TLS/CDN/MIME/cache проверены для HTML/JSON/sitemap/robots/ICS/assets; raster budget соблюдён WebP, SVG безопасен.
- **ADD-BUILD-12 — Capacity preflight.** Disk/volume/Object Storage/Kaggle/Supabase budgets и retention позволяют build, last-good и rollback без eviction нужных артефактов.
- **ADD-BUILD-13 — Freshness alert and catch-up.** Missed/stale build видим, max-staleness alert срабатывает, bounded catch-up публикует newest eligible snapshot.
- **ADD-BUILD-14 — Operator readiness scorecard.** Expected slot → run → checked artifact → delivery отображаются с owner/SLO/reason/evidence; отсутствие evidence не бывает green.
- **ADD-URL-01 — Persisted canonical slug.** Изменение title/location не ломает существующий canonical event URL.
- **ADD-URL-02 — Alias and lifecycle.** Rename/merge/delete/ineligible переходы создают принятые alias/redirect/410/retention без потери старых ссылок.
- **ADD-URL-03 — Collision and sitemap identity.** Slug collision/idempotent rebuild безопасны; sitemap содержит только текущие canonical eligible URLs.
- **ADD-RELATED-01 — Correlated related pipeline.** Effectful Smart Update автоматически достигает vector sync, full graph, LLM verification, checked build и atomic promotion.
- **ADD-RELATED-02 — Hash barrier and reverse anchors.** Публикация ждёт нужную vector revision и пересчитывает события, для которых изменённое событие было reverse candidate.
- **ADD-RELATED-03 — Model/provider fail-closed.** Vector/LLM timeout, invalid output и partial coverage сохраняют честный static last-good и не публикуют неверные связи.
- **ADD-RELATED-04 — Reconciliation and expiry.** Periodic job чинит missed triggers/drift и обновляет lifecycle/time-only expiry без ручного Smart Update.
- **ADD-RELATED-05 — Retrieval quality.** Current coverage `>=95%`, golden/hard-negative pack и verified labels проходят accepted thresholds.
- **ADD-SEARCH-01 — Live authorized search.** Production mobile login → quota → search → useful result/fallback → logout работает с реальным auth/backend и текущим catalog.
- **ADD-TAG-01 — Public saved-tag curation.** Save/normalize/merge/reject idempotent; LLM-first golden evaluation и current-catalog static generation не публикуют private query data.

### Supabase, personalization, reminders и homepage

- **ADD-CAP-01 — Storage baseline and Green band.** Текущие размеры и рост держат personalization project ниже принятой доли 500 MB с измеримым запасом.
- **ADD-CAP-02 — Compact current state.** Один current row/aggregate заменяет unbounded raw event history там, где история не нужна продукту.
- **ADD-CAP-03 — Retention and compaction.** TTL/compaction/reconciliation удаляют disposable telemetry, но сохраняют durable favorites/consent и audit minimum.
- **ADD-CAP-04 — Fail-safe shedding.** Near-cap отключает disposable writes до user-critical state; статический сайт и ICS продолжают работать.
- **ADD-CAP-05 — Capacity simulation.** Synthetic 1k/10k profiles, cleanup dry-run и restorable snapshot доказывают бюджет/rollback.
- **ADD-PERS-09 — Strict RC relevance KPI.** Для eligible mature golden personas `cards_to_first_relevant <=20`; исходный `USR-11 <=30` остаётся отдельным пользовательским ceiling.
- **ADD-PERS-10 — Real correlated personalization E2E.** Browser local state → authorized DB/profile evidence → generation → next feed доказаны на isolated real backend одним correlation ID.
- **ADD-REM-01 — Calendar-to-reminder delivery.** Explicit save/consent → due scheduler → Postbox MessageId/provider event → delivered/suppressed/cancelled/rescheduled/rollback проверены end-to-end.
- **ADD-HOME-01 — H1 decision.** До F5 freeze owner фиксирует `ship|defer`; defer полностью исключает experimental code/manifest из RC.
- **ADD-HOME-02 — Static first scene.** Accepted H1 полезен без JS, не выталкивает categories/feed за budgets и не создаёт пустой hero.
- **ADD-HOME-03 — Semantic motion safety.** Stable hitboxes, `CLS=0`, interruptibility и reduced-motion contract проходят mobile/desktop/keyboard.
- **ADD-HOME-04 — Grounded deterministic claims.** Briefing manifest имеет source/provenance/`safe_until`; просроченные/неподтверждённые claims fail closed.
- **ADD-HOME-05 — Discovery experiment.** Control/V1/V2 сравниваются по downstream discovery, performance, novelty и accessibility до ship decision.

### Exact Stage 6 gates

- **ADD-MEDALLION-01 — P0 shortlist and provenance.** Accepted organizer/venue/festival medallions имеют source, alias boundaries, current gap audit и owner disposition.
- **ADD-MEDALLION-02 — Exhaustive target inventory.** Каждая фактическая medallion surface/renderer имеет URL inventory и captures на 390/1440 плюс критические breakpoints.
- **ADD-MEDALLION-03 — Zero visual defects.** Нет clipping, alpha matte, dirty shadow, overflow, unreadable artwork или capture failure.
- **ADD-MEDIA-05 — Full-catalog duplicate closure.** Active/future ledger имеет ноль confirmed duplicates и ноль unreviewed visual clusters после rebuild всех public surfaces.
- **ADD-MEDIA-06 — Focus metadata producer.** Face/focal/saliency producer, confidence/fail-closed, manual override и golden crop corpus согласованы со всеми consumers.
- **ADD-AGE-01 — Age source reconciliation.** Source→canonical/backfill/conflict ledger не теряет reliable declared values и не принимает programme/door/audience age за event rating.
- **ADD-AGE-02 — Age all-renderer RC matrix.** Пять accepted values плюс unknown проверены на каждой card/list/detail/search/related/personal/favorite/festival/transport/share/structured derivative.
- **ADD-OCC-01 — Canonical linked graph parity.** Symmetric no-self/no-dangling core graph точно совпадает со static artifact и не использует title-only inference.
- **ADD-OCC-02 — Occurrence-specific actions.** URL/ICS/favorite/reminder/ticket/transport относятся к выбранной дате, а не к случайному sibling.
- **ADD-OCC-03 — All-card occurrence matrix.** Альтернативы доступны на всех card families/states/timezones/no-JS; same-slot duplicate не становится другой датой.
- **ADD-TR-06 — Provider fan-in.** Независимые KPPK/bus jobs → одна versioned schema → validation/last-good fan-in → один changed-hash rebuild, zero unchanged rebuild.
- **ADD-TR-07 — Exact-date and bounded ICS.** Route calendar соответствует дате, filename type-prefix верен, event caps 4/6 соблюдены, orphan ICS отсутствуют.
- **ADD-TR-08 — Stable real-event fixtures.** Presentation fixtures 6510/6397/6710/Yantarny и unsupported Kaliningrad проверяют расписание, no-return, conflicts и eligibility.
- **ADD-ADMIN-01 — Report queue safety.** Admin allowlist, unique active key, idempotent enqueue и atomic poller claim не создают двойной repair.
- **ADD-ADMIN-02 — Repair lifecycle.** Crash/retry/structured result → canonical repair → rebuild → immutable history проходят без ложного success.
- **ADD-ADMIN-03 — Real ArtKodex owner E2E.** Реальный accepted report доходит до правильного owner/worker и возвращает проверяемый result на public surface.
- **ADD-EVENT-SHARE-04 — Stable event assets.** 1200×630, 1080×1350 и 1080² assets deterministic/hash-bound/CDN-ready, CORS/freshness/rollback проверены.
- **ADD-EVENT-SHARE-05 — Native event-share matrix.** Telegram/VK/MAX на реальных mobile/desktop targets получают правильный event URL/image/text; API success не равен paste/send success.
- **ADD-SERVICE-SHARE-01 — Service share placements.** Mobile menu и footer присутствуют на всех page families; mobile native share и desktop copy behavior не смешиваются.
- **ADD-SERVICE-SHARE-02 — Service-card manifest.** WebP/PNG одного payload deterministic, metric/copy versions и hashes записаны, asset загружен до atomic promotion.
- **ADD-SERVICE-SHARE-03 — Native clipboard matrix.** Final RC проходит Windows/macOS × accepted browsers × plain/image/Telegram/VK/MAX targets с 2/2 repeatability.
- **ADD-SERVICE-SHARE-04 — Claim evidence.** Catalog metrics и обещания на service card имеют dated reproducible evidence; unsupported superlatives не публикуются.

### Final SEO/security/performance and Telegraph schedule

- **ADD-SEO-04 — Frozen neutral audit pack.** Crawl/render/status/canonical/robots/sitemap/schema/link/no-JS/mobile/media/performance evidence снято с immutable feature-complete SHA.
- **ADD-SEO-05 — Three independent audits.** Codex, approved Gemini Pro и Opus получают один evidence pack; failed/low-class lane не подменяет review.
- **ADD-SEO-06 — Reconciled zero-high ledger.** Findings воспроизведены/опровергнуты, Critical/High закрыты, visible/schema conflicts отсутствуют, final exact-SHA rerun принят.
- **ADD-SEO-07 — GEO transparency.** What/where/when/who/price/status, source/freshness/entity clarity, crawler policy и representative regional query pack доказаны без AI-only filler.
- **ADD-SEC-03 — Release security review.** RLS/grants/auth callbacks/tokens/admin/webhooks/secrets/abuse limits проходят review на точном RC.
- **ADD-PERF-01 — Full-catalog load.** CDN, Edge search, telemetry, provider fan-in и atomic promotion выдерживают принятый каталог/traffic budget.
- **ADD-CUTOVER-08 — Exact T0 record.** UTC/Kaliningrad timestamps, mode flags, SHA/snapshot/build/manifest и rollback target записаны до dual-run.
- **ADD-CUTOVER-09 — Ramp evidence.** D0/D2/D4/D6 percentages, resolver parity и реальные Telegram/VK/MAX unfurl probes проходят без broken outward links.
- **ADD-CUTOVER-10 — D7–D9 soak.** 72 часа на 100% static outward links имеют ноль release-critical errors и полную reconciliation.
- **ADD-CUTOVER-11 — D10 no-create audit.** Mode становится static/existing-only, create/recreate counters после cutoff равны нулю, legacy fields/URLs сохранены.
- **ADD-CUTOVER-12 — Aggregate Telegraph separation.** Month/weekend/festival Telegraph выключаются только собственным switch после отдельного static parity/acceptance pack.

### Separate post-release release scenarios

- **ADD-COMMENT-01 — F14 clean-port boundary.** Exact-SHA reuse/adapt/reject matrix сохраняет authority/thread/session/privacy boundaries и не merge-ит divergent Region Talk wholesale.
- **ADD-COMMENT-02 — 30-day fact shadow.** Daily collection по current/future events сохраняет parent/reply/source-owner metadata и классифицирует fetch failures.
- **ADD-COMMENT-03 — Precision and product GO.** Accepted-fact precision `>=0.99`, zero wrong-author/event/question/uncertainty failures и доказанная decision usefulness.
- **ADD-COMMENT-04 — Typed fact lifecycle.** Exact-span authority/scope/TTL/correction/retraction ledger идемпотентен и не переписывает critical event fields.
- **ADD-COMMENT-05 — F14 own RC.** Deterministic «Важно знать»/medallion проходит own static preview, privacy, a11y, canary and rollback without ranking effect.
- **ADD-FEST-01 — Festival queue safety.** Atomic claim/idempotency/retry/quarantine/stale recovery и live source E2E закрыты.
- **ADD-FEST-02 — Official-site monitoring.** Registry/cadence/fingerprint/fetch/LLM-first extraction/last-good/freshness alerts доказаны.
- **ADD-FEST-03 — Stable edition identity.** Rename/merge redirects и versioned event↔edition projection сохраняют permanent identity.
- **ADD-FEST-04 — Festival pages and relations.** Index/detail/archive, distinct card и bidirectional programme links согласованы с event cards.
- **ADD-FEST-05 — Festival own RC.** Standard build/promotion/rollback, UI/a11y/no-JS and final SEO/GEO pass for the new page family.
- **ADD-OPS-01 — Versioned check registry.** Owner/criticality/slot/freshness/SLO/attempt/success/delivery/status/reason/evidence нормализованы.
- **ADD-OPS-02 — Cross-pipeline reconciliation.** Ingestion/video/promo/transport/static/media/quality/email/capacity дают согласованный score, missing evidence не green.
- **ADD-OPS-03 — Protected read-only dashboard.** Admin access, redacted drill-down, bounded history и no PII/secrets проверены.
- **ADD-OPS-04 — Mutating controls separate.** Retry/catch-up/kill-switch появляются только после отдельного confirmation/idempotency/audit acceptance.
- **ADD-CLUB-02 — Club taxonomy/time split.** Stable community отделена от linked occurrence/festival/duplicate/venue series на past→future validation.
- **ADD-CLUB-03 — False merge/split closure.** Canary имеет zero confirmed false public merge, no dangling relation и accepted split/redirect behavior.
- **ADD-CLUB-04 — Club freshness.** Новая встреча accepted club появляется в SLO, stale/deferred identity fail closed и не блокирует base event build.
- **ADD-CLUB-05 — Club stable release.** Семидневное observation, rollback, UI/a11y/no-JS/SEO and owner decision закрывают отдельный canary release.
- **ADD-VIDEO-GUIDE-01 — Stable production basis.** Ролик привязан к canonical production SHA/build/manifest после D10 и не показывает experimental/preview/lab flow.
- **ADD-VIDEO-GUIDE-02 — Быстрый старт.** Новый пользователь по гайду открывает актуальное событие через `Сегодня`/`Завтра`/`Выходные` или категорию без лишнего обхода сайта.
- **ADD-VIDEO-GUIDE-03 — Точный поиск.** Показанный запрос по жанру/времени/месту/цене воспроизводится на текущем catalog и приводит к корректному событию, а не к смонтированному успеху.
- **ADD-VIDEO-GUIDE-04 — Правильный occurrence и сохранение.** Detail URL, дата/время, favorite/calendar и остальные показанные действия относятся к выбранному occurrence.
- **ADD-VIDEO-GUIDE-05 — Формат и доступность.** Mobile/desktop вариант соответствует реальному flow, subtitles/transcript читаемы, taps/cursor и labels различимы без одной только озвучки.
- **ADD-VIDEO-GUIDE-06 — Privacy and publication evidence.** В кадре нет PII/secrets/private URL/admin state; для каждого target сохранены owner approval, public URL и hashes source/export.
- **ADD-VIDEO-GUIDE-07 — Freshness and withdrawal.** После изменения navigation/search/action contract зависимые guide ids отправляются на review, а критически устаревшая версия снимается до проверенной замены.


## Правило исполнения

- `USR-*` — исходные сценарии пользователя; их нельзя тихо удалить, переименовать по смыслу или закрыть только unit-тестом.
- `ADD-*` — дополнительные сценарии аудита; при переносе в Gherkin/Playwright идентификатор сохраняется в названии scenario/test и в release evidence.
- Для каждого применимого сценария фиксируются URL/page family, viewport/device/OS/browser, test identity/persona, catalog/build SHA, входное состояние, ожидаемый результат и фактическое evidence.
- Автоматизация не заменяет native/manual evidence там, где проверяется системный share, clipboard/paste, email client, calendar/maps handoff или реальное визуальное качество.
