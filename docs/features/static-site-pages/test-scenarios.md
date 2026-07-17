# Сценарии для отладки статического сайта

Идентификаторы нужны для стабильных ссылок из багов, Playwright/Gherkin-сценариев и release evidence. Сценарии пользователя сохранены отдельно от сценариев, добавленных по результатам анализа.

> **Статус:** канонический acceptance inventory, а не журнал уже пройденных
> тестов. Наличие ID не означает, что функция реализована, автоматизирована или
> принята в production. Current release scope и gates определяет
> [release-plan.md](release-plan.md).

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
| Static build orchestration and immutable Kaggle handoff | `ADD-BUILD-01`, `ADD-BUILD-08/13`, `ADD-OBS-01` | Automated component | `pytest -q tests/test_static_site_release.py tests/test_static_site_build_handoff.py` | real Kaggle/status-ledger and enabled Fly flags |
| Production/preview/secret artifact profiles | `ADD-BUILD-07/09/11/12` | Automated component | `npm --prefix site run test:static-release`; production/secret build+check commands from the runbook | live candidate HTTP and root promotion |
| KAUP timetable A/B/C | `ADD-TR-06..10` | Automated component + manual visual | `pytest -q tests/test_static_site_bus_boarding.py tests/test_static_site_transport_experiment.py` | consented focus-group/live outcomes and statistical decision |
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
- **ADD-TR-06 — Три лаконичных варианта расписания.** Для одной и той же
  транспортной проекции доступны `departure_board_v1`, `route_strips_v1` и
  `next_departure_queue_v1`; вариант меняет только представление, но не рейсы,
  остановку посадки, маршруты или CTA.
- **ADD-TR-07 — Стабильное распределение A/B/C.** Один browser subject получает
  один вариант SHA-256-распределением и сохраняет его между событиями, rebuild и
  secret-prefix; в каждой сессии отрисован ровно один arm.
- **ADD-TR-08 — QA override не загрязняет эксперимент.** Query override позволяет
  визуально проверить каждый arm, но forced/QA impression и action не попадают в
  доверенную telemetry; production-root по умолчанию работает в `off`.
- **ADD-TR-09 — Квалифицированная экспозиция и исход.** Impression засчитывается
  только после не менее 50% видимости блока в течение одной секунды; outcome —
  только осмысленный клик `transfer`, `map`, `walk` или `car`, связанный с тем же
  experiment/variant/subject.
- **ADD-TR-10 — Посадка на Северном вокзале.** Если рейс проходит остановку
  «Калининград Северный», расчёт и видимая транспортная цепочка начинаются там,
  а не с конечной остановки маршрута.

### Социальные действия и обратная связь по событию

- **ADD-SOC-01 — Консолидированные просмотры, лайки и share.** Действия на сайте учитываются вместе со статистикой Telegram/VK через один общий event-level contract; retry/reload не удваивает действие, stale source не превращается в ноль, а все consumers видят совместимые totals.
- **ADD-SOC-02 — Честные публичные счётчики.** Нулевые значения скрыты по принятому UI-контракту, displayed total совпадает с принятой проекцией, а пользовательский интерфейс не называет reach-наблюдения уникальными людьми.
- **ADD-FEEDBACK-01 — Проверенные факты из комментариев.** На странице показываются только актуальные event-bound факты с допустимым source evidence; вопрос, слух, отрицание, неопределённость, ответ неавторитетного пользователя или комментарий к другому событию не становятся утверждением.
- **ADD-FEEDBACK-02 — Исправление и протухание факта.** Correction, retraction и TTL удаляют/заменяют публичный факт одним rebuild, не переписывают критические date/time/location поля и не оставляют старую формулировку в HTML/cache.
- **ADD-FEEDBACK-03 — «Активно обсуждают».** Если post-release medallion включён, он появляется только при выполнении принятого human-discussion threshold, не учитывает ботов/официальные ответы/дубли/engagement bait и не влияет на ranking без отдельного решения.

### Regression-сценарии изменений 15–17 июля

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
- **ADD-RECENT-05 — Полная матрица event-detail v11.** Preview обязан собрать
  каждый сценарий из `eventTemplateContract.json`: горизонтальное фото,
  горизонтальное фото с отдельной OCR-афишей, OCR/document contain,
  одиночные/серийные вертикальные изображения, low-resolution и
  quality-fallback. Все сценарии и реальные event pages используют один
  `DesktopEventPage`/`buildDesktopEventPresentation`, а не legacy desktop DOM.

### Статическая публикация, CDN и устойчивость

- **ADD-BUILD-01 — Debounce после Smart Update.** Несколько effectful обновлений в пределах окна приводят к одному build через 15 минут после последнего эффекта; no-op update не запускает лишнюю сборку.
- **ADD-BUILD-02 — Atomic promotion и rollback.** Пользователь не видит смесь HTML/assets разных releases; failed check не меняет current, а rollback полностью возвращает последний проверенный manifest.
- **ADD-BUILD-03 — Catalog parity и отсутствие устаревших страниц.** Все eligible события присутствуют, ineligible/quarantined не опубликованы, удалённые slug получают принятую redirect/410 policy и не остаются в sitemap.
- **ADD-BUILD-04 — CDN asset contract.** Canonical HTML/assets/ICS реально обслуживаются через принятый CDN path; runtime raster assets легковесные WebP, векторные — безопасные SVG, нет случайных тяжёлых PNG/JPEG или origin-only зависимостей.
- **ADD-BUILD-05 — Degraded network.** Slow 3G, offline after first load, CDN image failure и временная недоступность personalization/search API не ломают статический контент, навигацию, detail и ICS.
- **ADD-BUILD-06 — Cache/back/version transition.** После публикации новой версии reload/back/forward/service cache не возвращают несовместимые JS/data или состояние старого пользователя.
- **ADD-BUILD-07 — Preview/production/secret isolation.** Preview и secret
  candidate остаются `noindex`; production profile формирует root canonical,
  indexable robots/sitemap и не включает preview/lab/fixture routes.
- **ADD-BUILD-08 — Immutable Kaggle handoff.** Kaggle получает отдельный
  read-only SQLite snapshot с `quick_check`, SHA-256, размером, max revision и
  уникальными `snapshot_id/run_id/build_id`; неверный hash/manifest останавливает
  запуск.
- **ADD-BUILD-09 — Manifest/tree/catalog parity.** Checked manifest доказывает
  равенство eligible catalog, event pages, sitemap/ICS и фактического дерева,
  отсутствие ineligible leaks, orphan references и dangling occurrence links.
- **ADD-BUILD-10 — Immutable candidate и rollback boundary.** Failed build/check
  не пишет ни одного объекта; accepted candidate загружается create-only только
  в новый secret prefix. Изменение `root`, `current` и stable `/ics/` запрещено.
  Reader-atomic root promotion/rollback остаётся отдельным blocked gate и не
  подменяется последовательным копированием файлов.
- **ADD-BUILD-11 — CDN/MIME/assets.** Все objects candidate имеют manifest hash,
  корректный MIME/cache policy, допустимый CDN asset URL и после публикации
  проверяются authenticated HEAD/GET; root/current не меняются.
- **ADD-BUILD-12 — Capacity и privacy preflight.** До upload проверяются bounded
  result/archive, свободное место/лимиты и запрет anonymous bucket listing;
  secret token не попадает в sitemap, canonical, логи или внутренние ссылки.
- **ADD-BUILD-13 — Freshness/retry/catch-up.** Feature runtime допускает до 5400
  секунд; retry bounded и классифицирован, stale/missed/deferred request после
  restart получает ровно один catch-up, а update во время running build — ровно
  один follow-up с более новым snapshot.
- **ADD-BUILD-14 — Template provenance.** Production и secret manifest фиксируют
  `static-event-detail-v11` и accepted source SHA `3b17e536…`; каждая страница
  несёт те же markers и один из разрешённых `editorial|split` результатов.
  Real-event specimens привязаны к стабильному event id, а не к изменяемому
  после Smart Update slug/title.
  Отсутствие хотя бы одного marker, lab-сценария или real-event family
  останавливает build до Kaggle artifact publication.

### Related/vector barrier

- **ADD-RELATED-01 — Revision barrier.** При включённом related mode manifest
  фиксирует ожидаемые `search_v3/related_v1` revisions/hashes и не публикует
  устаревшую verified projection.
- **ADD-RELATED-02 — Reverse-affected anchors.** Изменение кандидата обновляет не
  только его страницу, но и старые anchors, в related-выдачу которых он входит.
- **ADD-RELATED-03 — Last-good или честное отсутствие.** Provider/vector failure
  сохраняет совместимую last-good projection либо убирает optional block; raw
  candidates не маркируются проверенными.
- **ADD-RELATED-04 — Optional barrier не блокирует base pages.** При выключенной
  related/personalization части базовый full-catalog export/build/check остаётся
  работоспособным и явно записывает disabled revision state.

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

## Правило исполнения

- `USR-*` — исходные сценарии пользователя; их нельзя тихо удалить, переименовать по смыслу или закрыть только unit-тестом.
- `ADD-*` — дополнительные сценарии аудита; при переносе в Gherkin/Playwright идентификатор сохраняется в названии scenario/test и в release evidence.
- Для каждого применимого сценария фиксируются URL/page family, viewport/device/OS/browser, test identity/persona, catalog/build SHA, входное состояние, ожидаемый результат и фактическое evidence.
- Автоматизация не заменяет native/manual evidence там, где проверяется системный share, clipboard/paste, email client, calendar/maps handoff или реальное визуальное качество.
