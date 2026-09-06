# Hero-talk: автонаполнение home/page-end — MVP-проект

> 6 сентября 2026. Исследование по [#642](https://github.com/onedayonemasterpiece/events-bot-new/issues/642), продолжение Hero-talk [#291](https://github.com/onedayonemasterpiece/events-bot-new/pull/291) и главной [#641](https://github.com/onedayonemasterpiece/events-bot-new/pull/641).
> **Статус: конкретное предложение TO-BE для реализации и review, не работающий runtime и не разрешение на production rollout.** Новые интервалы, бюджеты и ограничения ниже — стартовые проектные настройки, не результаты эксперимента и не автоматически принятые решения владельца.
> Владелец механизма: [Hero-talk](README.md). Этот документ владеет только его наполнением, доставкой и актуальностью. [MCP-контракт владельца](owner-mcp-mvp.md) и [постановка на реализацию](autofill-implementation-prompt.md) — части того же проекта.

## 1. Выбранное решение

**Одна библиотека коротких связанных цепочек, два placement и три способа наполнения.**

1. Автоматический редактор выбирает допустимые темы из уже обработанного каталога, собственных кампаний и реестра действительно работающих возможностей. Нейросеть заранее пишет и проверяет новые цепочки.
2. Владелец через EventsBot MCP задаёт тему, события, иллюстрацию и желаемый смысл: «расскажи об этом». Используется тот же редактор.
3. Владелец передаёт готовые формулировки и порядок реплик: «покажи дословно». Они сохраняются без нейросетевой перезаписи и без скрытого сокращения.

Публичная сцена — смысловые фрагменты текста, ссылки и, когда уместно, мозаика **одного конкретного изображения**. Home и page-end используют один renderer, общий выбор и общий контроль повторов. Никаких новых Campaign Talk, Meme Talk, отдельного page-end CMS или LLM на открытие страницы.

```text
Smart Update / campaigns / released capabilities / owner MCP
                  ↓
      canonical brief + locked facts, links, media
                  ↓
  Writer → independent Reviewer   |   owner verbatim
                  ↓
       deterministic validator + compiler
                  ↓
       immutable content-addressed phrase pack
                  ↓
       lightweight publication, no full site rebuild
                  ↓
   fresh eligibility permit → deterministic context resolver
                  ↓
     home_hero / page_end → existing product analytics
```

Ключевое разделение: **«текст подготовлен» не означает «текст сейчас разрешено показывать»**. Изменение фактов и пауза кампании отзывают разрешение без ожидания Writer, генерации фотографии или StaticSiteBuilder.

## 2. Что проверено, а что ещё предстоит построить

Исследовательский runtime baseline — `main@6fddf14aeb983f97bde96e5963e1c9a9ddf72590`. На него же указал read-only `eventsBot.operations_snapshot` 2026-09-06 17:31 UTC; этот вызов сообщил `provider_calls=0`. Его SQLite quick-check закончился `QueryBudgetExceeded`, поэтому проверка подключения **не** является заключением о здоровье БД.

| Область | Найдено в источниках | Следствие для MVP |
|---|---|---|
| Главная | `HomeHeroTalk.astro` — статический текст, два маршрута и одно событие; полного редактора/цепочек в этом компоненте нет [S2] | Нужен общий renderer и подключение пакетов, а не только cron для написания фраз |
| Hero-talk owner | #291: chain-first, home/page-end, факты и ссылки вне модели, optional mosaic, собственные редакционные кампании [S3] | Продолжаем этот механизм; старая ветка — документационный donor, не готовая runtime-база |
| Home MVP | #641, SHA `aab23a276e4da13ca2ad63d49083fd2d14aae3cf`: постоянный Search, единый бюджет 30, общие caps [S1] | Промо не вытесняет поиск и не повторяется снизу; конкретное hero-event входит в общий учёт предложений |
| Promo | Реальны campaign/target/activity/exposure и `promo.py`; в прочитанном перечне surfaces нет `hero_talk` [S4] | Добавить activity к существующему владельцу, не создавать параллельные кампании |
| Медиа | `EventPoster`, approved projection, CDN, semantic roles, exact SHA и миниатюры уже описаны/реализованы [S5] | Повторно не анализировать каждое фото для Hero; approved poster не считать автоматически фотографией |
| Сборка | Durable secret-preview build request и `enqueue_static_site_build_request`; allowlisted projection, отдельный root release [S6] | Shell проходит обычную сборку; последующие тексты публикуются лёгким контуром. Наличие preview не доказывает доступность ссылки в production |
| MCP | Сервис прикрепляется к существующему aiohttp app; OAuth/social store отделён от event-domain [S7] | Новые owner tools — внутри EventsBot MCP. Не использовать `social_action_*` как Hero ledger |
| Статистика | Общий сайт-wide TO-BE частично реализован; Hero exposure относится к product analytics [S8] | Не выдавать HTTP-запросы пакетов или TG/VK views за показы Hero |
| Погода | Consumer prototype default-off; producer/public rollout и стабильный API ещё gated [S9] | Не включать weather narration в первый автоматический выпуск |

В открытом каталоге EventsBot MCP во время исследования не было Hero tools. Live Hero inventory, показы и browser-поведение не проверялись. PR #587 открыт и draft, прочитан на `a8f009b0a3cec54bebc5ba7a5872dae15f9620c6`: его существование не служит release receipt голосового поиска. Запрошенный путь `docs/editorial/` в проверенном main вернул 404; нельзя ссылаться на якобы прочитанный общий style corpus. Начальный голос берётся из Hero/onboarding owner examples и отдельного утверждённого набора фраз.

Чтение было предметным: полностью прочитан связанный с #642 prompt, Hero README/release/testing; проверены PR metadata и релевантные разделы текущих promo, media, MCP, analytics, LLM, site docs и перечисленные исходники. Это не аудит всех файлов репозитория или всей production-инфраструктуры.

## 3. Границы первого результата

Обязательно: автоматическое наполнение; ручные и assisted цепочки; мозаика фото и разрешённых редакционных ассетов; own-editorial `hero_talk` promo activity; home и контекстный page-end; отзыв устаревшего; MCP чтение/изменение/статистика; реальный generic fallback.

В первой видимой цепочке 1–3 сцены об одной задаче. Это уже используемый стартовый бюджет Hero, не ограничение размера всей библиотеки. Владелец может хранить несколько цепочек/эпизодов с собственными окнами показа. Длинный сценарий не обрезается и не автоматически сериализуется: инструмент показывает нарушение бюджета и предлагает явно разделить эпизоды. Автоматического бесконечного продолжения нет.

Не входят: video mosaic, внешний автономный meme-crawler, генерация картинок, обязательная cross-device narrative memory, новый профиль, собственная погодная интеграция, paid/partner rollout без отдельного разрешённого контура. Мемы и шутки **входят** как owner/editorial content; их не надо выдавать за события каталога.

Return delta, голос, артефакты, клубы и напоминания включаются условно, если их реальный owner подтверждает готовность, маршрут и состояние. Это не исключение этих смыслов из продукта, а запрет рекламы отсутствующей функции. Нет отдельного переключателя «согласен включить персонализацию», вводимого самим Hero; действуют актуальные общие activation/purpose contracts.

## 4. Источники, нейросетевые шаги и актуальность

`W+R` ниже — два заранее выполняемых модельных вызова: Writer и независимая проверка. Источники TG/VK/web сначала проходят существующий intake/Smart Update. Hero не читает весь сырой интернет заново и не определяет отмену или бесплатность по ключевым словам.

| Тип | Авторитетный вход | Когда готовить; LLM input → output | Контроль; срок пригодности | Когда снять/заменить |
|---|---|---|---|---|
| Приветствие, ориентация | Утверждённые примеры Hero; timezone `Europe/Kaliningrad`; рабочие базовые маршруты | При смене style/schema: W+R → библиотека daypart/коротких связок. Смена времени суток — код | Ни чисел каталога, ни сведений о пользователе. Generic текст не требует ежедневной генерации; локальная шутка — cooldown | Изменён voice contract/маршрут; неподходящее время суток |
| Ключевое событие | Canonical event projection, lifecycle, admission/age, approved photo; существующие editorial/scoring признаки | Новая пригодная тема/значимое изменение; после shortlist W+R → normal/short одной мысли | Locked ID, название/дата/цена/CTA, фото того же объекта. Content validity не более 24 ч до повторной проверки и не позже границы события | Отмена, перенос, скрытие, identity merge, недопустимый ticket state, изменение используемых фактов, неготовый destination |
| Фестиваль/программа | Живая festival/program identity и пригодные участники программы | Изменился состав/смысл программы: W+R → связный рассказ; числовой счётчик вычисляет код | Membership fingerprint, явные сегменты, один доминирующий путь. Не более 24 ч и до конца допустимого окна | Кампания/программа закончилась, состав/маршрут изменился, выбранные события недопустимы |
| Собственная промо-кампания | Существующие campaign + target + activity; редакционный brief | Создание/изменение активной `hero_talk` activity: W+R или owner verbatim | Дополнительно campaign revision, schedule, cap, target, disclosure policy; `safe_until` не позже campaign end | Pause/archive/disable activity — немедленная серверная неготовность, без LLM |
| Функции, включая текстовый/голосовой Search | Onboarding capability registry + release/build readiness + route/platform requirements | Только новая подтверждённая возможность/смена объяснения: W+R; обычный показ использует готовую фразу | Готовность проверяется при выборе. Hint suppressed после mastery/dismiss. Пересмотр текста раз в 30 дней, это не продление готовности | Flag off, потеря маршрута/доступа, несоответствие платформе, mastery/cooldown |
| Первый артефакт, коллекция, клуб | Canonical artifact/club programme и точный доступный hint anchor | Изменение принятой подсказки/программы: W+R или редакционная фраза | После основной event value, только реальная первая коллекция; никаких придуманных находок/призов | Anchor/rules/feature unavailable, уже найдено/освоено, срок программы |
| «Новое с прошлого визита» | Готовый `served_delta_id`, meaningful watermark, тот же destination cohort | Runtime подстановка count/link в проверенную фразу; 0 LLM | Count и список из одного набора после hide/lifecycle. Срок определяется самим delta owner | Нет watermark/cohort, он истёк, count/list расходятся: без числа либо другая сцена |
| Page-end после действия | Page packet и подтверждённый product outcome | 0 LLM на действие: заранее проверенные result/recovery templates | «Сохранено» только после реального success receipt; не включать Push автоматически | Pending/error/undo, другой event/route/account, устаревший результат |
| Пустой поиск/конец списка | Фактический search status, ограничения и page family | 0 LLM на этот статус; библиотека recovery фраз | Один явный следующий шаг; query не уходит в общий generation pipeline | Новые результаты/другая задача; не называть ошибку пустой выдачей |
| Мем, шутка, локальная реплика, новость сервиса | Owner brief/дословный текст, editorial image и права; фактические утверждения отдельно | Verbatim — 0 Writer; assisted — W+R. Без автономного копирования чужих мемов | Маркер editorial/fiction, не поддельный event fact. По умолчанию окно 7 дней, owner может задать другое; необоснованные факты не публикуются | Deadline, owner pause, проблема с правами/смыслом, зависимая функция/event недоступны |
| Погода | Только будущий проверенный publisher из существующего weather track | В MVP не генерируется; позже использовать его snapshot, не новый погодный запрос | Свой weather TTL и attribution, не календарь генерации Hero | Нет producer receipt/истёк snapshot → никакой погодной реплики |

Различаются `source_observed_at`, `catalog_checked_at`, `copy_generated_at` и `eligible_until`. Чтение старой записи БД не превращается в «организатор подтвердил сейчас». SLO отзыва измеряется **от принятого canonical изменения**, а не от неизвестной системе публикации организатора. Проблемы исходного мониторинга остаются видимыми отдельно.

## 5. Автоматический редактор без лишних агентов

### 5.1. Отбор до Writer

Сначала применяются lifecycle/identity/age/admission/route/media/campaign gates. Затем из полного допустимого каталога строится небольшой разнообразный shortlist: текущие и ближайшие события, значимое обновление программы, собственные кампании, работающие возможности, редакционные темы. Не брать просто `feed[0]` и не считать популярность доказательством «главного события».

Горизонт organic spotlight по умолчанию 14 дней; ongoing событие допустимо по своему canonical интервалу. Explicit owner/campaign window может быть шире. Редакционный выбор, новизна, покрытие города/формата и существующие scoring признаки — основания выбора, но не придуманные факты в тексте. Диверсификация не проталкивает нерелевантное событие через hard filters.

Начальный вычислительный бюджет: до 8 новых/изменённых semantic briefs за один drain, последующие остаются pending. Это лимит работы worker, **не** предел каталога, типов сообщений или всех цепочек. Один brief создаёт normal/short варианты с одинаковыми object/chain/node identities. Нет декартова произведения «каждое событие × все персоны × все страницы».

### 5.2. Модельные роли

Предлагаемый первичный model ID для Writer и Reviewer — зарегистрированный в проекте `gemini-3.1-flash-lite` [S10]. Это предложение для новой публичной поверхности, не автоматическое наследование approval Telegram writer. Перед включением отдельный Hero sample должен пройти owner editorial review; существующая миграция limiter не доказывает текущую provider entitlement. Actual model/version записываются в receipt. При недоступности разрешённой модели — pending/last-valid/generic, не молчаливый переход на Gemma или другой публичный writer.

Все вызовы через `GoogleAIClient.generate_content_async`, общий project/model ledger, `reserve → mark_sent → provider → finalize`; проверяются `google_ai_project_model_atomic_v1` и `rolling_60s_pacific_day_v2`. Model fallback/новый key pool не добавлять. Внешняя консультативная оценка Pro/Opus не заменяется Lite-review: здесь Reviewer — внутренняя стадия продукта, не «external expert approval».

**Writer input:** task/placement, один topic anchor, разрешённые intents, fact slots и link tokens, точный media ref, запреты, исходный owner brief, short/normal ограничения. Никаких токенов OAuth, private notes, полного профиля или пользовательского search text.

**Writer output:** JSON chain, fragments, token references, bridge kinds, варианты длины, объяснение используемых fact IDs. Может вернуть `abstain`. Нельзя менять IDs/URL/суммы/время или добавлять популярность, дефицит, срочность, «мы знаем ваш вкус».

**Reviewer:** отдельный запрос с исходными facts и кандидатом. Проверяет grounding, отсутствие смыслового расширения, естественный русский текст, связанность, честный payoff, уместность юмора и соответствие задаче. Возвращает `accept | repair | reject`, findings по node/field и evidence refs. Проверка не равна доказательству безошибочности модели.

После `repair` максимум одна исправляющая попытка Writer и повторный Reviewer. Итого базово 2 вызова, максимум 4 на brief. Неудача остаётся наблюдаемой; система продолжает работать на других принятых цепочках. Не просить владельца ежедневно разбирать каждый провал генерации.

### 5.3. Детерминированная компиляция

JSON/schema, references, точные значения, route readiness, строки/ссылки/overflow, допустимые media role/crop, expiry и граф проверяет код. Семантическую истинность нельзя свести к проверке цифр: за неё дополнительно отвечает Reviewer, а спорное не публикуется.

Значение цены в fact slot можно перевязать без Writer, если semantic policy и весь окружающий смысл остались прежними; новая версия всё равно проходит validators. Превращение «бесплатно» в «платно», смена объекта или разрушенный сюжет требуют повторной семантической проверки. Verbatim никогда не редактируется таким способом внутри буквального текста: новая несовместимая цена блокирует его до правки владельца.

Fingerprint включает используемые canonical revisions, intent/brief, разрешённый pack, prompt/schema/style/compiler/model policy и media SHA. Повтор идентичного входа: **0 model calls, 0 новых content writes**. Операционный heartbeat не считается изменением content. Last-good разрешён только при всё ещё совместимых фактах и свежем permit; «последний удачный» не оправдывает отменённое событие.

## 6. Расписание и триггеры

Выбран **гибрид**: scheduled-only слишком поздно снимает отмену; event-only не замечает течение времени и теряет изменения при пропущенном hook. Дорогая генерация и дешёвый отзыв разделены.

Все продуктовые часы — `Europe/Kaliningrad`; provider RPD day остаётся у общего limiter и не переопределяется.

| Когда | Действие | Model calls |
|---|---|---|
| 06:00 ежедневно | Обновить shortlist и запас цепочек на день/ближайшие даты. Повторно проверить зависимости | Только новые semantic fingerprints |
| 16:00 ежедневно | Дневной delta: новые ключевые события, изменения программ, дефицит пригодного контента | Только изменившееся; при отсутствии изменений 0 |
| 00:05 ежедневно | Переключить календарный контекст, исключить истёкшее, подготовить следующее окно | 0; готовые daypart/date templates |
| После принятого event/program/media update | Durable dirty marker; debounce 5 мин, максимальное ожидание 15 мин; объединить IDs/reasons | 0 на hook; позже W+R только при необходимости |
| После owner/campaign/capability change | Commit desired revision и priority dirty; пауза/отзыв действует сразу в eligibility | 0 на отзыв; generation только для нового смысла |
| Каждые 5 мин и после restart | Bounded reconciliation активных program dependencies и pending operations, восстановление пропущенных hooks | Сам sweep 0 |
| При достижении `eligible_until` | Renderer перестаёт использовать сцену по уже выданному сроку | 0 |

APScheduler существующего процесса запускает drain/reconcile; отдельный daemon/Celery/Kaggle notebook для коротких текстов не нужен. Сам worker single-flight, lease сохраняется; после рестарта job получает актуальный source fingerprint. Квота не доступна — durable `next_attempt_at`, без busy loop. Модель/сеть вне SQLite transaction.

Первый operational cap: 64 физических LLM attempts за UTC day для Hero, включая retries и ручную assisted generation; параллельно действует более строгий общий limiter. Это отдельный **потолок**, не бронь 64 запросов и не обход приоритета intake. Пауза, чтение, verbatim без спорных фактов и serving не требуют этого бюджета. Настройка меняется адресно владельцем инфраструктуры; публикуемый control API не может повышать её.

## 7. Хранение, публикация и гонки

Не создаётся новая campaign/profile database. Предлагаются две небольшие domain-таблицы в canonical event DB:

- `hero_talk_program`: stable ID, source/origin, schedule/placements, desired/active revision, status, campaign binding, dirty fingerprint, bounded claim/retry fields.
- `hero_talk_change_log`: version/operation, actor scope, frozen request/preview/digest, idempotency, base/result revisions, source fingerprint, output refs, terminal state и reason. Одна история служит версионированием, prepare/commit и аудитом, без отдельного workflow engine.

Actor/private brief/authorization не экспортируются в публичный pack. Assets и принятые пакеты — content-addressed objects в существующем managed storage/CDN. Pure media validators можно переиспользовать; short-lived social asset ref нельзя просто вставить в публичную страницу. Для editor image нужен долговечный Hero-owned asset receipt; если общий media registry уже обеспечивает это к реализации, переиспользовать его, а не создавать ещё один.

Не добавлять несовместимые значения `JobTask`/`JobStatus` в первый rolling release [S7]. Pending revisions дренируются существующим scheduler через эти domain rows; текущий static-build outbox остаётся у StaticSiteBuilder. Миграции additive, без startup backfill/hot-table rebuild. Перед реализацией сверить параллельный MCP release: уже появившийся подходящий domain ledger можно адаптировать, а не дублировать.

Порядок публикации:

1. Прочитать согласованный source packet и desired revision.
2. Вне transaction создать/проверить pack, materialize assets, получить точные hashes.
3. Проверить готовность нужных **публичных** targets и совместимость со schema установленного renderer.
4. В короткой transaction заново проверить dependencies/status и CAS active revision. При несовпадении — `superseded`, не возврат старой кампании в active.
5. Проверить pack readback и live control projection; различать `stored`, `ready`, `active`, `delivery_pending/failed`. Только `active` с проверенной версией разрешает выбор.

Новые страницы/URL по-прежнему требуют обычного public build receipt. Уже существующий совместимый URL не требует rebuild для каждой смены текста. Public runtime release и secret preview — разные каналы. Rollback создаёт новую revision из старого контента с повторной проверкой сегодняшних фактов, не оживляет старое разрешение.

## 8. Свежесть поверх CDN, PWA и открытых вкладок

### 8.1. Два разных вида данных

**Immutable pack** хранит copy/CTA/media/facts/safe_until по hash URL. Его можно кэшировать долго. **Live permit** — небольшое разрешение для известного набора кандидатов, проверенное против текущих canonical revisions, состояния программ/кампаний и capability readiness.

Предлагается bounded read-only endpoint в существующем aiohttp/runtime transport, а не новый сервер. Exact route выбирается при привязке к действующему OperationCatalog. Он не вызывает LLM и не получает raw profile/query. За один запрос — максимум 16 candidate refs, общий response budget 12 KiB. Неподдерживаемая version/неизвестная ref не раскрывает черновик.

Permit выдаётся максимум на 60 секунд, но не дольше самого раннего dependency deadline. Сервер проверяет актуальные authority rows, а не продлевает время на старом materialized manifest. Для permit: `Cache-Control: no-store`, transport fetch `cache: no-store`, Service Worker **network-only**. На permit запрещены stale-while-revalidate и stale-if-error. Публичный CDN используется для immutable packs, а не как единственный владелец права на показ.

Причина: HTTP cache headers не гарантируют revalidation при Back/BFCache, Cache API требует собственной cache policy, настройки CDN могут давать собственный TTL [W1–W4]. Поэтому простого «добавить Cache-Control» недостаточно.

### 8.2. Поведение renderer

First HTML — полноценная evergreen generic scene со стабильными обычными ссылками, без актуальной цены, campaign claims, daypart-заявления из вчерашней сборки или персонального числа. После разрешённого enhancement выбирается готовая совместимая цепочка. Generic остаётся fallback внутри нового renderer; не возвращаем старый большой HomeHeroTalk как отдельную архитектуру.

Пока сцена видима в foreground, общий page-level coordinator обновляет permit раз в 30 секунд; home/page-end не открывают независимые polling loops. Невидимый page-end не загружает изображение и не создаёт exposure. При hidden/pagehide анимация останавливается; transient scene очищается до generic для восстановления. На pageshow/visibility resume — проверка before enhancement. BFCache, долгий sleep, смена системных часов и expiry тестируются отдельно; lease считается с server time и монотонным elapsed, долгий разрыв требует нового ответа.

Известный отзыв/expiry — снять весь согласованный комплект copy/CTA/media, не оставлять старую картинку с новым названием. Pointer/focus не должны превращаться в первый холостой клик; не подменять назначение ссылки во время pointer sequence. Safety removal может завершить сцену, но не запускает другой рекламный CTA под пальцем.

При offline/контрольном timeout/new load без permit — generic. Уже выданный lease не продлевается молча. Замороженный браузером кадр нельзя удалённо стереть из памяти или сделать нечитаемым в уже снятом screenshot; обещание «везде мгновенно» некорректно.

**Acceptance SLO:** после canonical commit сервер больше не выдаёт новое разрешение несовместимой версии; в выполняющейся активной вкладке старое разрешение живёт не более 60 секунд, обычно до следующего refresh. На возобновлении generic до нового permit. Это проектный предел при исправном renderer/clock handling, не измеренный production результат. Если transport не умеет обеспечить его, launch остаётся на generic, а не на бессрочной рекламе.

## 9. Детерминированный выбор и page-end

Порядок: hard truth/access/expiry → текущая задача и помощь → capability/utility → подходящая narrative/editorial тема → eligible promo → разнообразие среди равноправных. Campaign priority и owner pin действуют **внутри** допустимого множества. Явный запрос/город/дата и exact hide сильнее исторического профиля.

| Page context | Разрешённая следующая задача | Что запрещено |
|---|---|---|
| Home | Ориентация, подходящий spotlight/кампания, действительная новая возможность | Убрать постоянный Search или повторить hero-event первым в feed |
| Event detail | Действительная festival/club programme, уместное действие после прочтения | Ещё одна лента «похожих»; ссылка на то же событие как новое открытие |
| Подборка/дата | Уточнить выбор или явно перейти к соседнему маршруту | Вставка внутри хронологии; тайно снять ограничения |
| Search empty/error | Recovery по фактическому статусу | Реклама вместо помощи, «ничего нет» при сетевой ошибке |
| For Me / новое | Управление выбором, объяснение, доступный следующий маршрут | Возврат в уже открытый тот же delta state |
| Saved/action context | Подтверждённый result echo и доступное следующее действие | «Сохранено» по клику до ACK; повтор save после success; согласие на Push из save |

Служебные/административные страницы исключены. Page-end после основного содержания и существующего continuation, перед NPS/footer, без конфликта с bottom navigation. Расширение списка переносит page-end, но не создаёт новую identity показа. По умолчанию page-end не autoplay-ит всю цепь: первый полезный текст сразу, продолжение осознанное.

Одна и та же кампания — максимум один qualified exposure за общую сессию home/page-end; anonymous session semantics берутся из общего сайта. Для local daily frequency стартово один показ кампании в день на installation, если более строгая политика не задана. Anonymous installations не называются уникальными людьми. Local cap/dismiss работает и без analytics consent; при недоступном необходимом coordination state промо консервативно подавляется, а не бесконтрольно повторяется. Cross-tab использует общий существующий coordinator, не новый identity service.

**Не смешивать единицы caps.** Нынешний `promo_exposure` учитывает прежде всего доставленные публикации, а Hero qualified exposure — видимость у посетителя. Нельзя прибавлять browser impressions к числу видео-публикаций. Общие ограничения проверяются существующим promo owner; для Hero задаются typed `session/daily_frequency` и отдельная unit-qualified policy. Если campaign требует точного глобального viewer goal/cap, а атомарного подходящего accounting нет, вернуть `cap_accounting_unsupported` и не обслуживать её. Точные paid reach guarantees не являются MVP; асинхронная consented аналитика не может притворяться атомарным cap ledger.

В библиотеке могут быть и органические, и owner, и campaign цепочки. Привязанную к кампании копию нельзя переименовать в editorial, чтобы обойти pause/caps. Campaign click/exposure и artifact hint не обучают вкус; только реальные event actions по общему контракту. Один event spotlight учитывается в общем home budget 30, мозаичные тайлы не являются карточками.

## 10. Мозаика, ссылки и формулировки

Для событий: approved `event_photo` и точные role/geometry/crop/rights refs из Event Media. Festival image требует отдельного подтверждённого festival provenance: event gate не объявляется владельцем всей festival media. Изображение с программой/афишей нельзя произвольно обрезать как фотографию.

Дополнение по текущему решению владельца: для мемов, локальной редакционной мысли и функций допускаются `editorial_image` / `capability_illustration`. Это иллюстрация конкретной цепочки, **не** фото якобы происходящего события. Владелец передаёт файл/разрешённый source ref, origin/rights basis, alt и при необходимости locked crop. Нет автоматического скачивания случайной картинки по URL, исполнения SVG/HTML, использования private attachment без явного выбора или перезаписи чужого source asset. Проверки MIME/размера/декодирования/метаданных/прав обязательны; сомнение → text-only либо reject согласно owner media policy.

Одновременно декодируется одна картинка, а не запрос на каждый тайл. Tile layout — общий DS contract. Text/CTA/age/media переключаются атомарно; terminal image остаётся. Normal/short варианты семантически эквивалентны. Стартовый payload target: pack до 32 KiB gzip, выбранная картинка до 200 KiB на mobile / 400 KiB на desktop, увеличение — только с browser evidence. Не ухудшать уже принятый более строгий бюджет.

В сцене один доминирующий CTA, допустимы связанные inline links с ясными метками; это не запрет всех ссылок кроме одной. Ссылки только из locked tokens, без raw HTML/JS. Reveal не обрезает ссылку, первый полезный фрагмент и действие доступны сразу. Reduced-motion/no-JS — полный стабильный текст. Никакого кода от модели или owner в браузер.

## 11. Статистика и операторское чтение

Подробные tools описаны в [owner MCP](owner-mcp-mvp.md). `hero_talk_get` отвечает, какие программы active/ready/draft/expired, на каких страницах допустимы, что показывает deterministic preview и почему иной кандидат подавлен. Это не утверждение, что все посетители видят один и тот же hero.

Две независимые плоскости:

- эксплуатационная: accepted/rejected/expired counts, source lag, superseded jobs, last successful compile, actual model attempts/tokens, budget denied, publish/readback failures;
- продуктовая: eligible/reached/qualified exposure, node completion, CTA, dismiss, useful event action, feature success — по существующему consented bounded session pipeline [S8].

Hero-specific visibility predicate вносится в общий analytics registry: предложено не менее 50% **текстовой части** в foreground непрерывно 2 секунды после доступности первой полной смысловой единицы. Это измеримая возможность прочтения, не доказательство чтения. Применяется более строгий общий threshold, если он уже установлен. Scene/node и chain/program exposure считаются отдельно; один chain не становится тремя campaign показами. Render/pack download/hidden-tab/BFCache/sticky copy не увеличивают счётчик.

Статистика несёт `data_as_of`, telemetry coverage, scope/denominator, excluded test/preview/bot state и suppression semantics. Server fact rejection и sampled client suppression не складываются как «все непоказы». Без pipeline/consent/достаточных данных — `unavailable` или `insufficient_data`, не нулевые counts. Raw text, profile, email, токены и private draft в analytics не попадают. Нет синхронной SQLite строки на каждое browser impression.

Primary readout: полезное обнаружение/действие после eligible Hero exposure; secondary CTR, page-end continuation, feature success, fatigue. Assisted conversion — последовательность, **не causal uplift**. Не включать автоматическую оптимизацию текстов по CTR в MVP; сначала стабильная измеряемая поставка и owner review. Analytics outage не ломает показ допустимой generic/editorial сцены.

## 12. Один день и стоимость — расчётный пример, не production log

Все события, названия и ситуации этого раздела синтетические; реальные публикации не выполнялись.

| Время 7 сентября, Калининград | Что происходит | Новые вызовы |
|---|---|---:|
| 00:05 | Смена даты, истёкшее исключено; generic и готовые date templates остаются | 0 |
| 06:00 | В shortlists четыре действительно новых semantic briefs; приняты W+R | 8 |
| 08:00 | Smart Update принял новую встречу в уже известном фестивале | 0 на ingest |
| 08:05 | Одна обновлённая festival chain, W+R | 2 |
| 09:10 | Цена выбранного события изменилась; literal owner «бесплатно» несовместимо. Permit больше её не разрешает | 0 на отзыв |
| 09:15 | Одна автоматическая replacement chain по новой admission policy | 2 |
| 12:00 | Владелец приостановил campaign; новые permits не выдаются, сверху/снизу общий cap | 0 |
| 13:00 | Владелец прислал дословную шутку и свой рисунок, без внешних factual claims; validators и публикация | 0 |
| 16:00 | Две новые темы; одна потребовала repair и повторного Reviewer | 6 |
| Весь день | Открытия сайта, save result echo, recovery, generic/no-JS | 0 |
| Итого | Восемь первоначальных briefs + одна repair-пара | **18 attempts** |

Расчётная нагрузка одного brief: Writer 1 500 input + 500 output tokens; Reviewer 2 000 input + 250 output. Базовый день: 28 000 input + 6 000 output; одна такая же repair-пара даёт итог **31 500 input + 6 750 output tokens**. Это гипотеза размера prompts, не измеренный tokenizer расход; actual thinking/output/reserve учитывает gateway.

Стоимость = `31 500 / 1e6 × P_input + 6 750 / 1e6 × P_output`. Например, **при условных, не заявленных как тариф поставщика** $0.25/$1.50 за миллион это около $0.018/день, $0.54 за 30 таких дней. Free-tier availability и реальные ставки не подтверждены этим исследованием; они проверяются перед включением, отдельно от registry application limits. Порог 64 attempts не означает обязанность их потратить. Типичный warm день без новых смыслов — 0 model calls.

Serving cost также не ноль: при условных 1 000 активных Hero visits в день, двух минутах видимости и refresh раз в 30 секунд — примерно 5 000 control reads. При 2 KiB среднем ответе это около 10 MB/day полезных данных, без protocol overhead. 1 000 уникальных загрузок mobile image по 200 KiB — около 195 MiB. Это расчёт нагрузки, не проверка тарифа CDN и не capacity benchmark Fly. Перед rollout нужен load test; quota pressure отключает enhancement, но не афишу.

## 13. Конкретные отличия от старого проекта

| Было | Предлагается / уточняется | Основание |
|---|---|---|
| Writer + отдельные semantic/style/chain critics | Writer + один независимый Reviewer, deterministic gates отдельно | Для MVP сохранить все виды проверки, но не четыре постоянно оплачиваемые роли. Изменение требует согласования companion testing/release |
| Старый release-plan местами возвращает legacy HomeHeroTalk | Generic scene внутри нового renderer | Уже исправлено owner-corrected onboarding v0.4 и PR #291 body; историческую ошибку не канонизировать |
| Mosaic только в основном event-photo framing | Event photo сохраняется; owner editorial/capability image — отдельные явные роли | Дополнение владельца в #642: мемы, шутки, рассказ о функциях и конкретные формулировки |
| Last-good без достаточно конкретного механизма отзыва | Immutable content + live bounded permit; no-JS evergreen | Нельзя обеспечить pause/cancel полным rebuild и старым CDN cache |
| Несколько видов сложной narrative memory | First release: конечные coherent chains и bounded session state; return/cross-device только с готовым owner evidence | Не задерживать автонаполнение ожиданием всей персонализации, не выдумывать историю посетителя |
| Promo publication counters | Явные units; session frequency отдельно от exact global reach caps | Browser impression не равен опубликованному видео; consented статистика не атомарный delivery ledger |

Другие принятые owner contracts не ослабляются. После review согласованные изменения должны попасть в канонические документы `main` обычным PR, а не навечно остаться в комментариях. Эта работа продолжает документационный #291; **не предписывает merge всей августовской runtime-ветки в современный main**.

## 14. Источники и граница доказательств

Все GitHub ссылки ниже — прочитанные источники, а не заявление об их полном production внедрении.

- [S1: полный исходный prompt #642](https://github.com/onedayonemasterpiece/events-bot-new/blob/aab23a276e4da13ca2ad63d49083fd2d14aae3cf/docs/features/static-site-pages/home-hero-talk-autofill-research-prompt.md); [home MVP](https://github.com/onedayonemasterpiece/events-bot-new/blob/aab23a276e4da13ca2ad63d49083fd2d14aae3cf/docs/features/static-site-pages/home-mvp.md).
- [S2: реальный HomeHeroTalk](https://github.com/onedayonemasterpiece/events-bot-new/blob/6fddf14aeb983f97bde96e5963e1c9a9ddf72590/site/src/components/HomeHeroTalk.astro).
- [S3: Hero owner](https://github.com/onedayonemasterpiece/events-bot-new/blob/52f3afe73acb4bde2a6983500a534fd74d5a4116/docs/features/hero-talk/README.md), [release](https://github.com/onedayonemasterpiece/events-bot-new/blob/52f3afe73acb4bde2a6983500a534fd74d5a4116/docs/features/hero-talk/release-plan.md), [tests](https://github.com/onedayonemasterpiece/events-bot-new/blob/52f3afe73acb4bde2a6983500a534fd74d5a4116/docs/features/hero-talk/testing.md), [onboarding v0.4](https://github.com/onedayonemasterpiece/events-bot-new/blob/52f3afe73acb4bde2a6983500a534fd74d5a4116/docs/features/static-site-onboarding/README.md).
- [S4: promo owner](https://github.com/onedayonemasterpiece/events-bot-new/blob/6fddf14aeb983f97bde96e5963e1c9a9ddf72590/docs/features/promo-campaigns/README.md), [promo.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/6fddf14aeb983f97bde96e5963e1c9a9ddf72590/promo.py).
- [S5: Event Media](https://github.com/onedayonemasterpiece/events-bot-new/blob/6fddf14aeb983f97bde96e5963e1c9a9ddf72590/docs/features/event-media/README.md).
- [S6: build request script](https://github.com/onedayonemasterpiece/events-bot-new/blob/6fddf14aeb983f97bde96e5963e1c9a9ddf72590/scripts/request_static_site_build.py), [projection contract](https://github.com/onedayonemasterpiece/events-bot-new/blob/6fddf14aeb983f97bde96e5963e1c9a9ddf72590/static_site_release.py), [site owner](https://github.com/onedayonemasterpiece/events-bot-new/blob/6fddf14aeb983f97bde96e5963e1c9a9ddf72590/docs/features/static-site-pages/README.md).
- [S7: private MCP](https://github.com/onedayonemasterpiece/events-bot-new/blob/6fddf14aeb983f97bde96e5963e1c9a9ddf72590/docs/operations/private-events-mcp.md), [safe implementation](https://github.com/onedayonemasterpiece/events-bot-new/blob/6fddf14aeb983f97bde96e5963e1c9a9ddf72590/docs/operations/private-events-mcp-event-operations-safe-implementation.md), [actual app integration](https://github.com/onedayonemasterpiece/events-bot-new/blob/6fddf14aeb983f97bde96e5963e1c9a9ddf72590/private_events_mcp/integration.py).
- [S8: analytics owner](https://github.com/onedayonemasterpiece/events-bot-new/blob/6fddf14aeb983f97bde96e5963e1c9a9ddf72590/docs/features/static-site-pages/analytics/README.md), особенно §§4–5, 13–16.
- [S9: weather readiness](https://github.com/onedayonemasterpiece/events-bot-new/blob/6fddf14aeb983f97bde96e5963e1c9a9ddf72590/docs/features/static-site-pages/weather-calendar.md).
- [S10: LLM request policy](https://github.com/onedayonemasterpiece/events-bot-new/blob/6fddf14aeb983f97bde96e5963e1c9a9ddf72590/docs/llm/request-guide.md), [gateway](https://github.com/onedayonemasterpiece/events-bot-new/blob/6fddf14aeb983f97bde96e5963e1c9a9ddf72590/docs/features/llm-gateway/README.md), [historical registered model](https://github.com/onedayonemasterpiece/events-bot-new/blob/6fddf14aeb983f97bde96e5963e1c9a9ddf72590/migrations/005_gemini_flash_lite_limits.sql).
- [W1: MDN Cache-Control](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control): no-store, immutable, history-navigation caveat.
- [W2: MDN Cache API](https://developer.mozilla.org/en-US/docs/Web/API/Cache): lifecycle/cache management belongs to application; HTTP headers не заменяют SW policy.
- [W3: MDN pageshow](https://developer.mozilla.org/en-US/docs/Web/API/Window/pageshow_event) и [Page Visibility](https://developer.mozilla.org/en-US/docs/Web/API/Page_Visibility_API): restore/foreground handling.
- [W4: Yandex CDN caching](https://yandex.cloud/en/docs/cdn/concepts/caching): origin/custom caching modes and TTL.

Нейросетевые оценки качества фраз, browser/load tests, live quota probe, CDN/SW fault drills, новые MCP methods и runtime rollout в этом исследовании **не выполнялись**. Их проверяемый результат определён в постановке, а не объявлен PASS заранее.
