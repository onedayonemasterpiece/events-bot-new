# KenigEvents — завершить видимую унификацию и вернуть материалы в Penpot

Обновлено: 2026-09-05 после уточнения владельца и принятия нижней навигации
в формате floating island на мобильном и десктопе. Постановки для ручного
запуска, не отчёт о выполнении. Исходный immutable-пакет393a8e8 предшествует
уточнению,64784c01 — принятию навигации; задачи продолжаются, не создаются заново.
Исправлена двусмысленность поставки: Astro и executable UI SoT меняются вместе;
отложенная native-материализация Penpot не разрешает отложить SoT.

Выполняй общую часть и только выбранную роль: CODE, VISUAL, DATA, MATERIALS
или REVIEW. CODE — ручной кодовый агент в VS Code/терминале. Остальные —
личная работа ChatGPT через реально доступные инструменты, без Codex MCP.

## Продуктовый смысл и единственный источник требований

Владелец не увидел нормализации в самом интерфейсе: заголовки, блоки реакций,
отступы и цветовые сочетания всё ещё выглядят как набор независимо сделанных
страниц. Цель — устранить эти различия в работающем продукте. Раздать старым
значениям имена токенов, а затем обосновать исключение для каждого consumer,
не является этой задачей. Существующая инфраструктурная работа сохраняется,
но отдельно от её реального визуального результата.

Нормативный владелец — `onedayonemasterpiece/lovekgd-design-system`, ветка
`integration/launch-normalized-sot-penpot-20260902`:
- `AGENTS.md`, `docs/product-governance/astro-sot-penpot-conformance.md`;
- `contracts/launch-normalized-ui.v1.yaml` v1.14.0 или проверенный successor;
  особенно `owner_visible_normalization_clarification`, review stage и build topology;
- `docs/launch-normalization/STATUS.md`;
- `docs/launch-normalization/owner-audioreview-20260905.md`, теперь AR-01–AR-18;
- для Free: `contracts/page-profiles/free-collection.owner-review.v1.yaml`
  v1.2.1 или проверенный successor, включая `source_delivery`.

DS checkpoint после принятия навигации: `b31653708a878040d9415b26679aa4c1a2f5b888`.
Уточнение парной поставки Free: `e92a4504004ddf551a29748c85c7f5a66b980061`.
Открой current heads: точки выше не разрешают reset или игнорирование новых
решений. Требования не переопределять этим пакетом/комментарием/планом.

## Продолжение, не новый старт

Координация — `events-bot-new#621`; прочитай comment5549769770 и всё новее,
включая owner clarification. Runtime-ветка прежняя:
`work/owner-audioreview-card-geometry-20260905`.
Исторический стартовый UI — `d6fd10919f39e2689a85388ebd6cfd53639fec61`;
его clean verification head — `97c507be2521a244820caca5f7f5d641efca8bbc`.
Это не актуальный кандидат: CODE уже продвинул исходники и публикацию;
их точные текущие refs и evidence разрешай по #621 и remote heads.
Позднейшие изменения постановок — не новая UI-сборка.
Integration target — `agent/static-site-single-kaggle-contract`;
исходный отвергнутый trunk — `8b1bb81298bfca2fe2aaa3ceb7e5f654748b301f`.

Отвергнутый before-reference:
https://kenigevents.ru/preview-real-8b1bb8129-normalized-20260904-v1/__preview/
Manifest:
https://kenigevents.ru/preview-real-8b1bb8129-normalized-20260904-v1/preview-build.json
Snapshot `issue621-audioreview-20260904T205339Z`,
clock `2026-09-04T22:53:39+02:00`, real slice300, не весь каталог.

Точные три расшифровки и source IDs находятся в AR-register. Новые записи
idea-hub отбирай по содержанию; чтение текста не называй прослушиванием аудио.
Для сравнения с30 июля сначала найди точный source/artifact; такого измеренного
сравнения пока нет. Не подменяй его различием данных или современной production.

Сохранённая диагностика: run33947585260 / artifact9963838251 / source97c507b.
Это committed fixtures, не исправленный real snapshot; не наследуй blanket PASS.

## Разделение работы

CODE — единственный владелец объединения runtime/foundations/SoT и текущего
STATUS/AR-register. VISUAL/DATA возвращают предметные результаты в#621, без
конкурентных изменений этих файлов. MATERIALS — один явно назначенный
исполнитель существующей роли R0.PENPOT; он меняет только согласованные native
страницы/их bindings/evidence. REVIEW не принимает собственную реализацию.

Не добавляй оркестратор, registry, checker, дизайн-систему или build pipeline.
У CODE максимум2 внутренних работника при непересекающихся owning paths.
Разделяй по общим зависимостям, не по страницам: карточки/медиа/runtime и
shell/actions/keyboard/overlays — возможная граница, не навязанная архитектура.
Общие foundations, generated outputs и интеграция остаются у ведущего.
Пересечение одного owner решай последовательной работой, не локальными копиями.
Старые R0-runtime-only и W0–Wn схемы не отменяют актуальную CODE-задачу.

## CODE — комплексная инженерная задача

Лично продолжи реализацию до одного согласованного real-кандидата. Это не
только запуск/деплой и не новый аудит. Fresh-read инструкций, применимых
project skills и текущих heads, рабочего дерева, полного diff от merge-base,
действующих PR/чужих изменений и#621 обязателен. Сохрани уже сделанную работу.

Проверь фактическую возможность runtime/browser/git/Kaggle. Отказ метода не
равен отказу системы; неизвестный исход записи сначала проверяй ref/blob/run.
Actions не превращай в новый редактор кода. Запиши короткую границу owning
paths и первый исполняемый пакет, затем реализуй, а не заканчивай на плане.

### Первая видимая проверка и весь обязательный scope

Не уходи снова в большую волну переименований. Первым законченным пакетом
покажи на конкретных product consumers изменения обычных ролей или карточных
tracks, с before/after и измерениями. Это промежуточное доказательство верного
направления, не запрос owner-подтверждения каждого коммита и не конец задачи.
Полный scope определяется существующим product/consumer graph и AR-register,
не только11 ранее проверенными маршрутами.

Закрой четыре owner-visible направления из контракта. Исключения минимальны
и основаны на работе пользователя/состоянии/доступности, не на старом CSS.
Одинаковая семантическая роль должна реально сходиться по виду и поведению.
Different entity adapters не освобождают reaction UI от общего языка размеров,
иконок, отступов, counters и selected/loading/error/disabled states.
Не объявляй каждую комбинацию цветов отдельным законным вариантом задним числом.
VISUAL даёт целевой набор существующих ролей и основания; пока он работает,
продолжай бесспорные исправления карточек/поведения. Существенный новый выбор
оформляй по контракту, но не останавливай из-за него независимые исправления.

Карточки: устранить разные внутренние media/body tracks, framing/density и
неполные ряды во всех причастных flow/packed/static/hydrated/client-created
consumers. Сохранять admission, допустимый порядок, содержание и текст афиш;
нельзя достигать равенства удалением событий, unsafe crop, обрезкой текста
или растягиванием singleton. Локальный профиль/фабрика не заменяет organic feed.

Поведение: event5370/Tretyakov/actions/semantic keyboard reading, cold/warm/
loading/return, truthful badges и entity adapters по конкретным AR-cases.
DATA даёт фактические dispositions; не округляй/правь production данные по догадке.

### Общие нижние окна и навигация — теперь часть реализации

AR-17 — миграция всех app-owned modal/notification consumers через существующих
owners, не отдельный новый компонент без подключённых процессов. Проверь
имеющиеся dialog/sheet roots и `MobileToastRegion.astro` / `KenigEventsToast`:
там уже есть timer/progress, паузы, очередь и persistent action/error; в исходном
review toast был привязан к верху. Сначала проверь текущую реализацию CODE,
переиспользуй правильное и убери параллельные реализации.

Нижнее размещение и общий visual/lifecycle contract применяются и к desktop,
и к mobile. Интерактивные формы/подтверждения/ошибки не исчезают во время ввода;
пассивные сообщения могут иметь реальный видимый countdown. Это семантические
режимы одной системы, не оправдание различного оформления. Проверяй pause,
manual close, очереди/дедупликацию, ошибки, отмену, фокус, длинный текст,
background tab, reduced motion, повторный вызов и отсутствие потери состояния.
Не имитируй native OS/browser permission/share/auth screens.

AR-18 — решение принято владельцем: нижняя основная навигация в формате
floating island на мобильном и десктопе. Больше не ожидай выбора «нужна ли
нижняя навигация на desktop». Реализуй общий состав/назначение/labels/icons/
current-state с адаптивной шириной и отступом острова от краёв; desktop-вариант
компактный, мобильный — тоже остров, не приклеенная к краям полоса.
Primary vs contextual navigation и размещение согласуются с floating context
и notifications; не делай три перекрывающихся независимых нижних панели.
Навигация не исчезает по notification timer. Сейчас реализуются структура,
действия, состояния и доступность; окончательный skin остаётся для отдельного
генеративного ревью владельца. Принятое решение не равно выполненной миграции.

### Замкнутый цикл и итог

Для каждого UI-изменения: воспроизведение → owner/reverse impact → согласованная
правка Astro/consumers И executable UI SoT → проверки A=S и регрессии → личный
просмотр → сохранённая связанная поставка и remote read-back.
SoT не является задачей «после готового Astro», независимо от размера правки.
Соблюдай существующий conformance contract; для Free его применение уточнено
в `source_delivery` профиля v1.2.1. Исправленный профиль/STATUS/комментарий
сами по себе не обновляют executable SoT.

Меняются причастные canonical owners, variants/states, composition, точные
asset/geometry/token bindings, interaction semantics и actual consumers.
Производные данные регенерируются существующими средствами и проверяются;
не создавай вторую ручную копию CSS или определений компонентов. В разных
репозиториях допустимы связанные коммиты, но до согласованной пары A/S это WIP,
а не законченная поставка или кандидат с якобы отложенной только P-частью.

Проверяй видимые consumers, same-role styles/colors/spacing, внутренние tracks,
длинный контент, loaded/missing/broken media, negative override/singleton/
occlusion controls, три заданных viewport и причастные breakpoint seams.
Ожидания не получай из текущего экрана как его собственного эталона.
Native mobile-system checks выбирай по существующему scenario registry;
desktop viewport не закрывает OS integration. Дешёвые L0/L1 по риску, не полный
каталог на эмуляторах. Focused диагностика — существующий путь.

Актуализируй executable SoT и данные структурной проекции в той же поставке,
используя существующие owners/exporter/validator. Проверка опубликованного
кандидата позднее дополняет evidence этой пары, а не впервые создаёт S.
До передачи готовности укажи в существующем receipt/evidence точные A/S revisions,
сценарий/состояние и результаты проверок, включая отклонение stale/missing S.
Пять экспортированных карточек не заменяют описание всей композиции и её
initial/personalized/load-more/error/empty states.

Готовые совместимые A+S изменения интегрируй в executable trunk по repo rules,
без обхода release-side effects. Production автоматически не переключать.
Полный опубликованный кандидат — только единый существующий Kaggle-путь.
Первый причинный before/after сохраняет snapshot/clock/slice300/page scope;
недоступный snapshot не подменяй незаметно свежим. Исправления upstream-данных
проверяй отдельно с собственной идентичностью, не маскируя смену baseline.

Передай MATERIALS уже согласованную пару A+S и изменившиеся bindings для native
материализации. Старые каталоги/packets не доказывают новый source. Полная
материализация всех страниц Penpot может быть отложена, executable SoT — нет;
четыре каталога остаются отдельным обязательным результатом.
Нативную работу не приписывай себе по Git JSON.

В#621: одна актуальная ссылка/manifest, точные A/S SHA и snapshot/clock,
фактическое покрытие, тесты/скриншоты, состояния AR-01–AR-18, четыре native page
links либо точный неустранённый blocker. STATUS/docs/changelog согласованы;
никакого completed при stale SoT или открытом обязательном видимом расхождении.
Передай кандидата независимому REVIEW; его замечания исправляй в этой же задаче.
Готовность к owner review не означает owner approval или A=S=P без P.

Внешние пробелы делегируй коротким пакетом ChatGPT: exact вопрос/refs/evidence,
ожидаемый выход, место в#621, запрещённые owning paths. Не дублируй работу,
не запускай Codex скрыто из окна ChatGPT, не строй polling-сервис; завершённые
окна сами не просыпаются. Нет внешнего результата — не выдумывай его.

## VISUAL — решения, ведущие к видимой унификации

Прочитай общую часть, contract clarification и AR-register. Работай лично
через GitHub/browser или собственные сценарии на существующем Actions пути.
Не повторяй широкий аудит и не меняй runtime/общий STATUS параллельно с CODE.

Сначала коротко разложи уже сделанное: structural-only / visible-code /
integrated / published / personally-verified, с точными refs. До какого слоя
дошли, что реально увидел владелец, какие расхождения сохранились? Не выдумывай
проценты, не объявляй всю работу бесполезной и не затягивай новые исправления.
30 июля — только после разрешения точного исторического source/artifact.

Дай CODE компактную матрицу целевых ролей на product scope: роль/состояние →
consumers → подтверждённое расхождение → общее существующее target family/
style → measurable expected result. Приоритет: headings, reactions, spacing,
semantic color combinations. Смысл — устранить, а не классифицировать все
различия как допустимые. Отличай найденное основание от собственного предложения.

Для палитры выбери согласованные семантические роли из существующего продукта,
а не новый бренд. Разбери Exhibitions: тёмная поверхность сама по себе не баг,
но одна её историческая реализация не оправдывает всю отдельную палитру.
Не выдавай массовое сохранение исходных значений за нормализацию.
Для reactions различай presentation и entity action/count semantics.
Согласуй нижние окна с AR-17 и DATA, а нижнюю floating-island навигацию на обоих
устройствах считай уже принятым AR-18. Не открывай заново разрешённый выбор.
Другие неразрешимые существенные решения вынеси конкретно, не отправляй владельца
анализировать компоненты. Финальный skin Floating Island не проектируй.

Результат#621: конкретная матрица и рекомендуемые минимальные изменения,
source/screenshot identity, принятое отдельно от awaiting approval; затем
проверка первого видимого пакета CODE. Это не финальная независимая приёмка.

## DATA — доказательные факты и семантика

Прочитай общую часть и точные расшифровки. Отдельно от VISUAL/CODE разбери:
8737/19:09;8702 venue/content/no-media;8589 no-source; weekend coverage slice300;
5370 occurrences; withheld Unusual; exhibition unseen-count; local bookmark
vs public reaction/count для фестивалей/клубов и других consumers.

Цепочка: symptom → записанные snapshot values → первичный источник → слой
причины → проверенное действие/точная нехватка доказательства. Разные entity
API не оправдывают разный visual; передавай требуемые truthful states/labels
общему UI. Проверь также смысл итоговых уведомлений/ошибок для AR-17: что
нельзя терять по таймеру, где подтверждение и где просто информация.

Сверяй upstream/adapter, не только HTML; отсутствие в300 не означает отсутствия
в базе. При необходимости запроси один ограниченный read-only export через
CODE/существующий путь, без PII/нового exporter. Не округляй, не удаляй сходные
записи, не придумывай изображения/нули, не обходи quality gate и не пиши prodDB.
AR-15 остаётся withdrawn/undecided. В#621 факты/dispositions и точные adapter/
test requirements; приватные транскрипты целиком туда не копировать.

## MATERIALS — восстановить четыре страницы в существующем Penpot

Нужны GitHub и реальные методы Penpot. Это личная работа ChatGPT, не Code
MCP и не Figma-замена. Выполняй bounded catalog authorization из контракта,
согласовав единственного writer существующей роли R0.PENPOT; не создавай
параллельный writer или новый файл. Имя файла и старый ID из памяти недостаточны:
fresh-read actual file/pages/revision/components и сверка актуального owner
binding. При противоречии IDs сначала разреши его, не мутируй случайный файл.

Восстанови по одной удобной странице: Иконки, Медальоны, Артефакты,
Окна и уведомления. Сначала найди существующие страницы/компоненты/известные
источники и точечно восстанови их; не пересобирай исторические115 страниц и
не проси повторно загрузить уже известные материалы до их поиска.

Покажи продуктовые и восстановленные owner материалы полно, а не выборку
красивых примеров. Native linked instances и exact SVG/asset identity, подписи,
роль/variant/state/owner/usage и active/disabled/deprecated статус. Для медальона
отрази реальную композицию artwork/frame; для артефактов состояния не активируют
саму функцию в продукте. Для нижних окон — реальные short/long, success/error,
interactive/timed/persistent, focus/paused/loading states и desktop/mobile.
Планы будущих состояний явно отделяй от существующей реализации. Нижняя
навигация на обоих устройствах принята в формате floating island; согласуй
её соседство с окнами по source CODE, без выбора нового финального skin.

Не рисуй похожие значки/глифы и не выдавай стену PNG за восстановленный каталог.
Донор восстанавливает материал, но не делает старый вариант новой authority.
Стабильные материалы можно материализовать из уже согласованного A+S;
последующие изменения поступают как новая проверенная пара A+S, и MATERIALS
обновляет её native P. Он не должен задним числом создавать отсутствующий S
по изменённому Astro. Старая P-проекция помечается как принадлежащая старой
паре, а не как текущая. Нельзя объявить завершение на preliminary-страницах.

Проверь native exports визуально, linkage и duplicate-free повторный проход.
Сохрани source pins/receipt по действующему контракту и реальные page/frame
IDs; в#621 четыре прямые ссылки и экспортированные previews. При реальной
недоступности native tool сообщи конкретный blocker, а не restored/PASS.
Не трогай unrelated pages, production, switches или полную A=S=P приёмку.

## REVIEW — независимая приёмка нового real-кандидата

После готового кандидата прочитай current#621, contract/STATUS/AR-register и
принятые решения VISUAL/navigation. Сам разреши URL/manifest и проверь SHA,
snapshot/clock/slice. Нет successor — нет приёмки по rejected build.
Проверь соответствующую executable S revision и данные всей причастной
композиции/состояний: новый Astro со старым S не проходит, даже если экран
выглядит правильно и обновлены Markdown-профиль или только sample-карточки.

Лично выполни собственный browser scenario при1440×900/390×844/1920×1080:
product archetypes, shared role consistency, reaction states, color combinations,
spacing/wraps, internal media/body geometry/remainders/media cases, event5370,
loading/return/Home/keyboard, нижние окна/timer/forms/focus и весь navigation/
island/notification stack. Отдельно проверь принятую floating-island навигацию
на desktop и mobile: общие действия и состояния, адаптивная геометрия и отсутствие
перекрытий. Проверяй реальные применимые hydrated consumers;
fixtures и persistence не смешивать. Не создавать публичные реакции или
почту вне предусмотренного тестового режима.

Проверь четыре native Penpot страницы и их exact source bindings/exports,
а не только запись об их существовании в Git. Пересказ qa-summary автора или
выбранных им картинок не независимая проверка. При отказе bridge допустимы
собственные сценарии на существующем Actions пути с личным просмотром.
Смотри на рабочем масштабе, не только contact sheet; правильный marker/HTTP/
overflow не отменяет видимый дефект. Подтверди фактические data dispositions.

В#621: exact target, выполненные действия/evidence, что действительно стало
единообразным по четырём направлениям, покрытие AR-01–AR-18, оставшиеся дефекты.
Верни их той же CODE/MATERIALS-задаче; не правь runtime/эталоны сам.
Успех — готовность к owner review, не принятие владельцем и не A=S=P без P.

## DATA follow-up — actual residual protected-text evidence (2026-09-05)

Continue the existing DATA lane; do not duplicate CODE layout work or start a
new exporter/service. GitHub plus browser/image observation are required.
Do not delegate to Codex/DevCover. Read #621/5551500203 and newer results first.

CODE now groups the entire 111-event Free pool before pagination. Ninety-three
cards fit natural rows; the residual 18 below lack usable protected-text proof.
This is a bounded source-image investigation, not a request to crop unknown OCR
blindly or to modify production data. First reuse any existing exact-source
review; do not repeat completed DATA work. Inspect original images at readable
resolution, identify all important text/face/value regions, and return positive
or negative evidence tied to exact URL, dimensions and verified content hash.
Do not label a path-embedded hash as a measured pixel hash. Normalized boxes use
{x,y,w,h} in [0,1]; include every protected region, confidence and methodology.
Human observation is not an automatic `geometry_status=classified` declaration.

For each proposed row, test **vertical-only** crop to its maximum natural aspect
ratio; do not use a geometric midpoint that cuts source sides. Maximum20% source
area is a ceiling, never a substitute for text safety. If text cannot be retained,
return INCOMPATIBLE and explain the exact region, not a fabricated safe result.
Look for existing verified visual-only classification of the same exact source;
do not infer no-OCR from a title or event category. Do not change event facts,
source selection, card widths, visible order, or delete events to pass geometry.

Return all18 dispositions and source-bound boxes/evidence in one #621 result,
with machine-readable attachment/file in your existing DATA lane if available.
CODE owns importing validated evidence and the shared rendering contract.
No independent V0 verdict, no publication or production mutation by DATA.

| Proposed row IDs | Vertical target W/H | Exact sources (dimensions) |
|---|---:|---|
| 8575, 8742, 8524 | 0.70747218 | 8575: [533×800](https://static.kenigevents.ru/p/image/v2/77/774020c577f54b29f2850b59537da53c969ffb9505bf4f1ed1761a0347e1c733.webp); 8742: [874×1280](https://static.kenigevents.ru/p/image/v2/26/26a18d01792da6c222328f5e4f0ad8a4f37291a47bb5b4af9b24f75cd244929c.webp); 8524: [445×629](https://static.kenigevents.ru/p/image/v2/53/53529f224a773bfaec3486b94cb56a48ba56106a033fe950df65298cf6a64789.webp) |
| 8375, 8549, 8434 | 0.72398190 | 8375: [1812×2560](https://static.kenigevents.ru/p/image/v2/e3/e373c546a94ccdc398f2a3fe40b8ef3ac0707f7a363365b16968bef16048383e.webp); 8549: [1778×2507](https://static.kenigevents.ru/p/image/v2/09/0975f15120d196b32a8637fc1187f0bc62e0d4f065d2f25d0b06e6db071ba301.webp); 8434: [1280×1768](https://static.kenigevents.ru/p/image/v2/72/726ae7ec5da755a6b6546bfda7d75fa9edb3d8ff341b8a457e128fe43d353a4b.webp) |
| 8664, 8745, 8515 | 0.79986658 | 8664: [1360×1851](https://static.kenigevents.ru/p/image/v2/32/32b5b3954613babe6d62883ea87c1501ac623163eccb5d78ac4b35f4608c1360.webp); 8745: [960×1280](https://static.kenigevents.ru/p/image/v2/66/66eab753606e59a2c055db150a32955946fdddbcd70a3f2ad29f73343138c00b.webp); 8515: [1199×1499](https://static.kenigevents.ru/p/image/v2/a8/a8336e00cc028a43c5509770136e2e964141e29fb61ab5c3f174d006775b6858.webp) |
| 8413, 8673, 8339 | 1.00750000 | 8413: [863×1078](https://static.kenigevents.ru/p/image/v2/af/af47f183c5241c827b5d0d2cbbc0483986e05fc1dfd1555bddae33c68fe09bf7.webp); 8673: [1080×1080](https://static.kenigevents.ru/p/image/v2/3e/3e9a51c582f3808bea78d4c095cb2cbf7ef0cdf843bf3f0b70b19e54d3f7f07f.webp); 8339: [2418×2400](https://static.kenigevents.ru/p/image/v2/3f/3f86ba9728347079294cb45116b4d00c57958d2564cfbd49227a0ffab7a56808.webp) |
| 8486, 8715, 8667 | 1.49882904 | 8486: [1034×843](https://static.kenigevents.ru/p/image/v2/d6/d6d8d4ec36738505bfbbc4c43075a6a374472b408d5b1fd8ffa003762885472f.webp); 8715: [2236×1521](https://static.kenigevents.ru/p/image/v2/13/130770e978ef315ad290f42af2f8fe7942a8edd6c5794acd6c8305f1faa3d0f7.webp); 8667: [1280×854](https://static.kenigevents.ru/p/image/v2/ee/eeaf6b0da33ff8920b7935641458f681c6933ee7c5a4444c517098f2175266db.webp) |
| 8449, 8364, 8631 | 1.77798683 | 8449: [1708×961](https://static.kenigevents.ru/p/image/v2/e7/e7cba3bd6fb63c05d8628ae86e7f4db0501003b820e1088d3bbb58d99fc1bfdc.webp); 8364: [1920×1080](https://static.kenigevents.ru/p/image/v2/67/672a50a2c8d00fd304fd418b86c93864d4d1cdfcd2b3ac25e1ceb99ab2c5e3d6.webp); 8631: [1890×1063](https://static.kenigevents.ru/p/image/v2/15/1566514a25b334983acd54f9de94b80d2fb661e984b736e1712379e1f57e5135.webp) |
