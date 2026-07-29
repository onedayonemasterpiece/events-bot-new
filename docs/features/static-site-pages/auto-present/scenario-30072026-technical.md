# Технический сценарий показа 30 июля 2026

Статус: `working_owner_test_candidate`, public demo остаётся `NO-GO` до финальной репетиции.

Авторский источник — [`scenario-30072026-base.md`](scenario-30072026-base.md).
Реализация остаётся простой: набор явных сцен, без DSL, редактора, новой БД
или новой инфраструктуры.

## Принцип быстрых итераций

Сцена после принятия получает статус `frozen` в
`SCENE_ACCEPTANCE_CONTRACT` (`tools/autopresenter/agent/presentation-contract.mjs`).
SHA-256 выбранных method/markup-сегментов проверяется отдельным
`accepted-scene-freeze.test.mjs`, поэтому случайная правка замороженной сцены
роняет gate; осознанная правка требует явно переоткрыть acceptance.
В черновой итерации проверяются только новые и явно переоткрытые сцены. Полный
регрессионный прогон выполняется один раз перед финальной передачей. В итерации C адресно переоткрыты intro, лекция, PWA, 03.2–03.10, 03.13,
03.15, `tomorrow-rail-like`, `weekend-amber-artifact`, `weekend-desktop` и
визуальный фон outro. После текущей проверки `tomorrow-mobile`,
`tomorrow-rail-like`, `weekend-amber-artifact` и `outro-qr` снова закрыты
новыми hashes; intro, лекция, PWA и сервисные сцены остаются открытыми до
owner acceptance.

## Непрерывная среда

- один Windows agent, browser, BrowserContext, page и fullscreen-окно 1920×1080;
- следующий Run меняет сцену в том же окне; статические кадры остаются видимыми
  сколько нужно спикеру;
- Stop/Reset нетерминальны; только «Закрыть презентацию» завершает всё;
- пульт — PWA «Пульт презентации» со sticky-таймером, стабильной геометрией
  статуса, временем начала intro и полем запроса для Smart Search;
- существующие Fly app и Yandex CDN переиспользуются; новые ресурсы не создаются.

## Сцены

### Intro

`intro-loop` воспроизводит 50-минутный логичный цикл. Hero Talk перенесён как
семантическое раскрытие четырёх фрагментов: активный cursor следует за текущим
фрагментом и исчезает после завершения; паузы между фрагментами варьируются
детерминированно по фразе и пунктуации, без повторяющегося modulo-ритма, держится 5,2 секунды, без длинного пустого кадра. При заданном на
пульте времени начала в цикл периодически добавляется «Начнём через N минут».
Музыка и знак «Знание» сохраняются, Stop снимает таймеры и останавливает audio.

### Лекция

Лекция — **семь отдельных кнопок и сцен** `lecture-01`…`lecture-07`, а не
автопроигрываемая колода. Каждый кадр остаётся на экране до следующей команды.
Композиции чередуют светлую/тёмную тему и разные layout: split, horizontal,
poster, portrait, cinema. Горизонтальные изображения показываются целиком,
под ними есть смысловая подпись. Знак «Знание» без белой плашки: светлый на
тёмном фоне и тёмный на светлом.

### Презентация сервиса

Явные полноэкранные сцены:

1. `service-wordmark` — обычная надпись переходит в точный фирменный SVG;
   отдельный exact vector-path буквы «о» расширяется сильнее логотипа и
   ease-in-out возвращается в итоговый фирменный размер;
2. `service-needs` — найти, поделиться, добавить в календарь;
3. `weekend-amber-artifact` — принятый живой сценарий артефакта;
4. `service-medallions`, `service-medallions-desktop`,
   `service-medallions-mobile` — объяснение и реальная страница события;
   desktop сначала показывает отдельную мысль, затем страницу события `6153`
   той же focus-сборки с горизонтальным hero, top-slot и минимум двумя
   медальонами; mobile использует предоставленную актуальную страницу `6865`;
5. `service-joke` — фраза Максиму, пауза 7 секунд, ответ;
6. `service-search-concept`, `service-search-live` — поиск по смыслам и
   видимый ввод запроса с пульта на телефоне;
7. `service-disruption`, `service-taste`, `service-feedback`,
   `service-focus-group`, `service-nps`, `service-future-celebrity` —
   последовательность тезисов из авторского документа.

Новые live-вставки используют предоставленную immutable focus-preview сборку.
Сцена фокус-группы показывает крупный QR иерархии «Присоединяйтесь к
фокус-группе» на точный URL; onboarding-сцены дожидаются принятия invite-fragment
в `kenigevents:focus-participation:v1`
`/preview-20260729-focus-simple-r15-a5cc0256/fokus-gruppa/priglashenie/#invite=focus-group-2026-announcements`.
Tomorrow/rail и остальные live-сцены работают на focus-preview. Единственное
исключение — artifact: в focus build feature flag `off`, поэтому 03.3 использует
проверенный R15 candidate с `tail` и событием `7164`. Сброс агента сохраняет
`kenigevents:focus-participation:v1`. 03.3 и 04.2 используют более медленный
smootherstep-жест, видимые выдержки результата и подробные статусы на пульте.

### Desktop и outro

`weekend-desktop` теперь двухфазный: сначала полноэкранная мысль, через 4,2
секунды — живой сайт во весь FHD и естественная прокрутка. `outro-qr` не
изменялся и остаётся видимым до следующей команды.

## Визуальная система и медиа

Presenter stage поставляет Cygre Book/SemiBold/ExtraBold и использует только
реальные веса 400/600/800. Точная буква «о» извлечена из фирменного SVG в
`announcements-o-expanded.svg` и используется вместо круглых декоративных
колец. Image/QR entrances используют выраженную кинематическую S-кривую с
коротким overshoot/settle; медальоны перемещаются из центра области сетки в
ячейки FLIP-анимацией.

Lecture, intro music, знак «Знание» и QR используют уже загруженные immutable
content-addressed URL `static.kenigevents.ru`; точные SHA-256 находятся в
`presentation-contract.mjs` и `outro-contract.mjs`. Логотип «Анонсы», share
icon и аватар Татьяны Удовенко берутся из существующих static-site assets.

## Известный блокер

03.9 уже требует авторизацию, настоящий submit и реальные event cards, но
готовой demo-сессии нет. Нужен однократный вход отдельного непривилегированного
аккаунта именно в Windows BrowserContext; состояние сохраняется только в
`%LOCALAPPDATA%\KenigEvents\Autopresenter\cache-v1\browser-state-v1.json`.
Секреты не входят в Git/ZIP/relay.

## Verification

Во время разработки: contract/unit tests и выборочные 1920×1080 screenshots
только новых/переоткрытых сцен. Перед финальным handoff: один полный
agent/relay/site regression, live session switch test, Windows ZIP smoke и
финальная репетиция. M0 compatibility tree не изменяется.
