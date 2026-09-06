# Новое окно ChatGPT: сквозная система Floating Island для KenigEvents

Дата: 2026-09-05, расширенная постановка после восстановления «Плана релиза». Самостоятельная продуктовая, архитектурная и системно-дизайнерская работа в ChatGPT, не сырой тикет Codex и не новый оркестратор.

## Результат и способ работы

Спроектируй полноценную систему Floating Island **для всего публичного сайта**, восстановив прежние концепции и их связи с надёжным подключением, статистикой, продуктовыми решениями и персонализацией. Нужна сохраняемая спецификация: применимость по архетипам, роли, композиции, состояния, геометрия, поведение, интеграция, миграция и автотесты. Не заканчивай аудитом референсов или списком того, что кому-то предстоит придумать.

Работай лично в ChatGPT. Не делегируй исследование/проектирование Codex/DevCoveer. Небольшие документальные и детерминированные source/schema/test изменения допустимы; реальную runtime-интеграцию выдели в последующий законченный пакет с критериями проверки. Это не разрешение на deploy, реальные provider runs, изменения production/чужих foundations/STATUS или перезапись canonical Penpot components.

GitHub — источники и сохранение. Браузер и Penpot — проверка реальных страниц и состояний, если доступны. Сначала discovery и реальный read; предыдущая ошибка доступа не доказывает нынешней недоступности. Не подменяй Penpot Figma, screenshot чтением исходника и tool description успешным действием. Ограничения доступа не обходить. Без браузера/Penpot документальная работа продолжается, но визуальное evidence честно остаётся непроверенным. При невозможной записи выдай полный пакет файлами и точную границу несохранённого, не выдуманный commit.

## 1. Продуктовая цель и принятые направления

Floating Island — управляемая система, а не одна кнопка поверх страницы и не частная функция голосового поиска. Минимальные роли: часть шапки; подходящие одиночные полки/контекстные панели; нижняя навигация; голосовой/текстовый composer. Заголовки последовательных ответов поиска — потребители той же контекстной роли.

Несколько островов могут сосуществовать. Не нужен запрет «четвёртого острова» или universal pill для всех смыслов. Не каждый прямоугольник и не каждый архетип обязательно превращается в остров. Surface, composition, control semantics и layout/runtime behavior проектируются отдельно; composer, navigation dock и persistent-state dock не сливаются по сходству радиуса.

Цель — сохранить понятный контекст и управление, не перекрыть афишу, не потерять действие/ввод и не заставить контент прыгать. Карточки/сетка/медиа/CTA остаются общими. «Бесплатно» — обычная фильтрованная выборка, а не ошибочный канон с двумя карточками в desktop-строке и произвольным отдельным блоком «бесплатные выставки». Технический счётчик не заменяет осмысленный заголовок.

Владелец хочет генерировать визуальные варианты и делать подробное, в том числе голосовое, ревью. Подготовь реальные page-context/anatomy/state материалы для такого review, а не абстрактные телефоны. Не закрепляй случайные новые шрифты/цвета/радиусы или красивый чужой рендер как уже принятые tokens.

## 2. Сначала восстанови общий план, затем проектируй оболочку

Это обязательная часть анализа, не необязательное приложение. В конце июля/начале августа уже были увязаны выдача, доставка действий, статистика, продуктовые решения и профиль. Голос/острова не создают независимый новый продукт.

В `onedayonemasterpiece/events-bot-new` прочитай:

- `docs/features/static-site-pages/release-plan.md` — «План релиза», его scope, data progression, current/historical evidence и другие связанные gates;
- `docs/features/static-personal-announcements/README.md`, `global-product-decisions.md` и **`release-integration.md`** — верхнеуровневый F1–F17 scope и восстановленные стыки с нынешней работой;
- `docs/features/static-site-pages/presentation-release-checklist.md` — разграничение visual review, functional acceptance и регулярного readout;
- `docs/features/unsigned-personalization/production-integration.md` — общий BackendClient/OperationCatalog, маршруты, acknowledgement/replay и budget;
- `docs/operations/yandex-dependency-resilience.md` — независимые capabilities и аварийные состояния;
- `docs/features/static-site-pages/analytics/README.md` — определения/consent/exposure/metrics/delivery, особенно §24 reverse bridge, §29–33 tests/DoD/фактическая граница;
- `docs/features/static-site-pages/personalizaion/requirements.md` и `personalization-to-be.md` — ручные требования, activation, surface policies, hides, visible-prefix freeze, local projection и materializer;
- `docs/architecture/personalization-data-ownership.md` — владелец данных и permission/localization boundaries;
- `docs/product-model/README.md` и релевантные существующие analysis/MeasurementQuestion записи — путь от наблюдения к решению, не новая бюрократия.

Старый `NO-GO`, missing RPC или исторический PASS не выдавай за свежий production verdict без проверки. Сохраняй совместимые прежние требования, обновляй только конкретно устаревшую часть. Ручной requirements.md и первичные исследования не переписывать автоматически. Legacy two-root ALB и host fallback из старых этапов не оживлять как обязательный текущий build path: #621 использует единый Kaggle builder и существующий bucket. Инварианты свежести, целостности, privacy и проверяемого rollback при этом не отменяются.

### Сети действительно работают в двух направлениях

```text
Product/Auth/Search: browser → Supabase
                 OR browser → Yandex relay → тот же Supabase

Optional analytics: browser → Yandex ingest → YDB
                 OR browser → Supabase Edge blind bridge → тот же ingest/YDB
```

Это не двусторонняя репликация БД и не универсальная fallback-цепочка. Product route health отделён от analytics. Shared upstream outage не лечится вторым proxy, 200 gateway не доказывает downstream commit. Selected-once не повторяется после ambiguous dispatch; safe-read и proven idempotent replay имеют свои правила. Не рисуй один общий «интернет/Яндекс работает» indicator.

В UI предусмотри working/queued/local-only/committed/partially-committed/unknown/failed состояния именно для затронутого действия; sidecar failure не отменяет primary success. Audio media и маленькие JSON operations имеют разные route caps. Availability manifest — публичная подсказка, не personal quota и не гарантия соединения конкретного браузера. Общая система островов не становится ещё одним transport client.

## 3. Найденные прежние материалы Floating Island

### LoveKGD Design System

Репозиторий `onedayonemasterpiece/lovekgd-design-system`:

1. PR #47, ветка `docs/floating-control-islands-reference`; проверенный ранее HEAD `774bcf0659915dffa16431847d408b2a6a6f2302`. Читать current remote HEAD/comments, не фиксироваться на исторической отметке.
2. `docs/research/floating-control-islands-2026-08/README.md`, `planned-design-pattern.md`, `planned-pattern.json`, `source-manifest.json`, `screen-observations.json`, `assets/reference-board.svg`, `assets/anatomy.svg`. Отдели ранние non-source-faithful эскизы от source-informed пакета.
3. Существующая identity: **`pattern.detached-chrome-control-islands`**. Не создавать одноимённый второй паттерн.
4. PR #39 и `docs/design-system-planned-patterns-checklist.md` в фактической ветке PR; default-branch search не видит все незамерженные материалы.
5. Активная нормализация: `integration/launch-normalized-sot-penpot-20260902`, `docs/launch-normalization/README.md`, `STATUS.md`, `PARALLEL-WINDOWS.md`, `contracts/launch-normalized-ui.v1.yaml`. Разреши актуальную версию перед изменениями.
6. `docs/product-governance/astro-sot-penpot-conformance.md` — действующее правило A=S=P, не пересказ по памяти.

Шесть прежних референсов показывали разные leading/context/utility islands и bottom architectures. Это источник принципов, не наш финальный дизайн. Сырые скриншоты с личным/сторонним содержимым намеренно не публиковались в Git. Не реконструируй неизвестный оригинал и не переноси private reference в public artifact без основания.

### Astro и текущая работа

В `events-bot-new`:

- `AGENTS.md`, `docs/README.md`, `docs/routes.yml`, применимые skills;
- #621: текущее тело, релевантные owner corrections/comments, actual integration branch из текущего STATUS. Комментарий `5550659316` содержит прежний bounded island handoff; последующие комментарии могут уточнять его;
- `docs/features/static-site-pages/mobile-shell.md`; реальные EventLayout/header/drawer/BottomNav/Toast, ListingDiscoveryRail, keyboard/navigation, AdaptiveEventCardGrid/MediaFrame, route-to-archetype map. Найди actual paths/versions/consumers, не угадывай по названиям;
- PR #587, `docs/agent-assisted-event-discovery-20260826`: `docs/features/static-site-pages/smart-vector-search/agent-assisted-event-discovery.md` и `voice-search-solution-v1.md`. Читай current HEAD с последним release-integration расширением, не старую редакцию `62c54ce` как завершённое видение;
- `docs/operations/static-site-autotest-strategy.md`, `docs/testing/static-site-autotest-scenarios.v1.yml`, existing browser harness/CI.

### Исходные голосовые

В `onedayonemasterpiece/idea-hub`, папка `inbox/voice/2026/09/`, прочитай полные расшифровки, не только auto-summary:

`voice-20260905-002114-a0677098.md`, `voice-20260905-003237-8a342775.md`, `voice-20260905-092949-7ab7703f.md`, `voice-20260905-094341-67f72ad8.md`, `voice-20260905-094737-dfa8cdc1.md`.

Адресно расширь поиск на более ранние floating/detached chrome/header/shelf/dock/voice решения обеих репозиторных веток и idea-hub. Короткая карта: источник → принято / гипотеза / отклонено / устарело → целевой владелец. Не расширяй работу на все проекты пользователя.

## 4. Корректировка старого sequencing gate

Старый planned-design-pattern откладывал само проектирование до общего AS-IS/parity PASS. Нынешнее поручение разрешает сейчас сбор решений, документальную архитектуру, mapping и изолированные review-предложения.

Зафиксируй «было → стало» адресно. Принятие визуальных variants, реализация и A=S=P claims требуют доказанного baseline соответствующих целевых consumers; весь сайт нельзя объявлять нормализованным заранее. Нельзя остановить документальную работу лишь из-за незавершённого общего gate. При этом не переписывай чужой STATUS и не снимай остальные guards.

## 5. Глубина проектирования

### Применимость по всему публичному сайту

Из реальной карты составь consumer matrix: home/HeroTalk, дата/выходные, обычные/фильтрованные подборки, одиночные и много-полочные страницы, Search/answer feed, event detail/gallery/CTA, personal/favorites/hidden recovery и прочие найденные пользовательские архетипы. Число не угадывай по старым «17». Служебные, lab/admin/diagnostic страницы не включай в пользовательскую нормализацию; они могут быть только test/evidence surfaces.

На каждый consumer: Job/outcome, affected regions, current owner, применимо/неприменимо/unresolved, desktop/mobile, сценарии, fixtures, measured/accepted constraints и migration boundary. Одна полка на странице и несколько полок — разные композиции. «Бесплатно» не становится единственным приёмочным архетипом.

### Геометрия, роли и компоненты

Раздели четыре слоя: material/radius/elevation; композиция; семантика кнопок/chips/ввода/nav; runtime anchoring/scroll/keyboard/occlusion. Reuse/new-version decision основано на actual family mapping, не округлости.

Опиши top/bottom/side slots, measured occupied rectangles, safe-area, compact/expanded, слой/приоритет, минимально полезную площадь контента, скрытие/замену/возврат. Existing event CTA vs nav exclusivity сохраняется до explicit совместимого решения. Референс mini-player не создаёт обязательную новую функцию сайта.

Проверь portrait/landscape, desktop, маленькую effective-height, zoom/large text, drawer/modal/auth/consent, gallery, toast, keyboard/VisualViewport, SSR/hydration и вложенные scroll containers. Last-card actions и stop recording достижимы; отсутствуют невидимые full-screen hit planes, layout loops и controls, уезжающие под пальцем. Предпочтение CSS-first с небольшим shared controller, не generic оконному менеджеру.

### Состояния, профиль и движение контента

Явные переходы flow/sticky/fixed, expanded/compact, focus, route/Back, loading/degraded/error/unknown, activation/projection/local-only/committed. Надёжное подключение — обязанность существующего транспорта, но его правдивые состояния отображает эта система.

Календарь сохраняет хронологию, тематическая подборка eligibility и слабый ниже-viewport rerank, «Для меня» сильную персонализацию, Search явный запрос выше профиля. Общий exact hide/undo работает даже в прошлых voice sections; нельзя восстановить скрытое из immutable history как обычную карточку. Карточка с текущим взаимодействием и видимый префикс не прыгают при обновлении projection. Actual unobscured viewport важен и для rerank boundary, и для exposure. Informational activation notice не заменяет постоянную ссылку на Правила и не превращается в новый обязательный checkbox.

### Голосовой потребитель

Capture и processing независимы, догон принимается. Search владеет intent/history/parent/result-set, сетью и прикладными receipts; shell — расположением и слоями. Просматриваемый раздел не является автоматически базой уточнения. Заголовки разделов становятся sticky в своих границах, заменяют друг друга, при обратной прокрутке восстанавливаются. Весь вопрос/ответ не приклеивается.

По explicit submit — один переход к новому заголовку; чтение истории не перехватывается поздним ответом. Ошибка/лимит не удаляет composer/контекст. При необходимости исправь минимальный Search↔Shell contract вместе с owning doc; не создавай второй профиль, ASR или limiter ради layout.

### Статистика и её продуктовый смысл

Для затронутых capabilities сформулируй несколько конкретных MeasurementQuestions: сохраняется ли контекст, достигаются ли CTA, мешают ли острова просмотру, помогает ли продолжение поиска. На каждый: eligible population, actual exposure, engagement, conversion, denominator, guardrails, факт/receipt, owner/query/readout и решение, которое можно принять.

Дедупликация: переход heading flow→sticky не новая page view; возврат карточки в viewport и повтор в разных answers не добавляют уникальных событий к общему cards-to-value. Section-local observations не суммируются как глобальные уникальные. Рендер≠видимость: shared shell сообщает occupied rectangles, actual served-list order согласуется с bounded presentation receipt. Оpaque section_id не подменяет served_list_id. Одна CTA голосом/клавиатурой/мышью — один факт с modality, не три conversions.

Optional analytics требует своего consent, не блокирует UX и не активирует профиль. Raw speech/query/LLM response/DOM text/profile/keystrokes/coordinates не уходят в общий поток. Weak summaries входят в shared budgets, не новый POST на каждый scroll/audio tick. Primary commit и analytics projection различаются. Dashboard/query должен показывать coverage/lag/недостаточность выборки; hotspot/assisted path не causal uplift. Existing action map остаётся отдельной default-OFF campaign с настоящим zero-cost OFF, не always-on частью островов.

### A=S=P и owner review

Один frozen Event Corpus + sanitized synthetic profile/activation/projection/consent/UI-state/viewport fixtures + versioned components. Реальные частные profiles в Penpot не экспортируются. Visual parity, network/behavior tests и physical-device certification — отдельные доказательства.

Подготовь малое репрезентативное множество real page variants, покрывающее все классы конфликтов в consumer matrix, paired baseline/candidate и материалы для генераций/голосового owner review. При доступном Penpot исследовательские материалы — на явно обозначенной review-page из проверенных источников; canonical components не менять на этом этапе. Без доступа — воспроизводимый materialization input и честные missing bindings, не придуманные IDs.

### Автотесты и release

Given/When/Then с уровнем и ожидаемым evidence: geometry/occlusion/sticky succession, keyboard/editing/focus/Back/zoom, notifications/modal, async response anchors, profile refresh без скачков, hide/undo между поверхностями, consent-off нулевые optional writes, distinct network failures/partial ACK, served-list/DOM reconciliation, aggregate denominator/test-pollution и action-map OFF. Не ограничиваться grep токена. Не требовать полный Cartesian product: выбери явные high-risk combinations.

Используй existing scenario registry и GitHub-hosted CI, real fault-injection server/test sink и test DB где это предмет проверки. Qwen frozen dialogues — материал Search, не отдельный analytics framework. Mobile viewport не native iPhone. Self-hosted runner не добавлять. Все published previews — существующий единый Kaggle builder/bucket path; local diagnostic не получает owner-review/A=S=P credit.

## 6. Сохраняемый результат и маршрутизация

Продолжи #47 и существующий pattern owner, если fresh-read не обнаружил новый authoritative successor. Один компактный пакет: система/consumer-state mapping/acceptance и конкретный integration handoff; machine-readable поля только когда их будет читать validator/materializer. Не дублируй каждый абзац в YAML и не создавай новый управляющий контур.

Роли данных: product meaning в events-bot-new; общие visual/component contracts в lovekgd-design-system; native Penpot — проекция; transport, analytics, profile и release остаются у прежних владельцев. Индексы/#39/#621/voice docs получают нужные ссылки и короткие compatibility notes, не копии всей спецификации. Сохранённые ранее решения не выбрасываются только потому, что новый документ короче или новее.

Проверь actual remote commit/current branch, ссылки и что новые requirements явно отличаются от engineering proposals и проверенных implementation facts. Не объявляй схему runtime test, документ — реализацией или source commit — production release.

## 7. Критерий завершения окна

Из результата независимый разработчик понимает, где система применяется, кто чем владеет, как острова сосуществуют, как сохраняются ввод/действие/контекст при скролле/клавиатуре/сетевом сбое, как они взаимодействуют с персонализацией, какие наблюдения допустимы и как доказать полезность. Есть ясный первый вертикальный implementation slice и его regression gates, а не требование потом заново спроектировать всю логику.

Численные недоказанные настройки и необходимые owner visual choices перечислены конкретно, с рабочим вариантом для остальной работы. Обязательные environment/authorization gates сохраняются, но не служат поводом остановить независимую документальную работу.

В финале: canonical package/commit, ключевые решения/исправленные расхождения, реальные проверки и границы, первый законченный пакет реализации. Не заканчивай вопросом, продолжать ли уже порученную работу, или обещанием фонового выполнения.
