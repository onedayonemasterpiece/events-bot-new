# CrumpleVideo

Фича генерации видео-анонсов `/v` (intro + афиши + outro) через пайплайн
CrumpleVideo/Blender. Этот документ собирает требования и проблемы тестового рендера
в одну точку.

## Контекст запуска

### Тестовый запуск (`/v - Тест завтра`)

- Состав: intro + до 12 афиш + outro.
- Афиши берутся из базы анонсов (без тестовых подстановок).
- Быстрый запуск сначала готовит ручной preflight: показывает `INPUT/SELECTED`,
  даёт проверить дубли и только после явного подтверждения запускает рендер.
- Выборка и генерация ограничены 12 афишами.
- Если на завтра мало афиш, окно подбора расширяется волнами:
  завтра → завтра+послезавтра → завтра+послезавтра+послепослезавтра → ...
  (лимит: 5 дней включая завтра).

### Боевой запуск (`/v - Запуск завтра`)

- Состав: intro + 2-12 афиш + outro.
- Афиши берутся из базы анонсов (без тестовых подстановок).
- Быстрый запуск сначала готовит ручной preflight: показывает `INPUT/SELECTED`,
  даёт проверить дубли и только после явного подтверждения запускает рендер.
- При нехватке афиш окно подбора расширяется волнами:
  завтра → завтра+послезавтра → завтра+послезавтра+послепослезавтра → ...
  (лимит: 5 дней включая завтра).

## Проблемы и наблюдения (последний тестовый прогон)

- Рендер занял `7314.6s` (ускорение требуется только для тестового запуска).
- В тестовом запуске появилось 2 афиши, и это тестовые афиши, а не из БД.
- Intro не соответствует актуальному макету (похоже на старый паттерн).
- Логи показывают `Using legacy dataset: /kaggle/input/afisha-dataset-2`,
  `Posters to render: 4`, и таймауты Blender по 30 минут на каждый рендер.

## Требования

### Производительность (только тестовый запуск)

- Цель: ускорение рендера тестового запуска минимум в 10 раз.
- Недопустимо: таймаут Blender 30 минут на один постер.

### Аудио

- использовать только `The_xx_-_Intro.mp3`.
- начало аудио с `1:17` от начала аудиозаписи.
- правило одинаковое для test и prod: server-side session dataset и Kaggle notebook не должны подменять этот трек на альтернативный mp3.

### Story UX: обучение жесту паузы/перемотки

- Status: `Not confirmed by user` (`2026-04-12`) until the rendered rollout is manually validated in a real Telegram story; CTA copy and visual treatment are approved for implementation.
- Текущий story-ритм допускает ручное чтение афиши через Telegram gesture UX (`hold to pause`, при необходимости `drag to seek`), но этот сценарий сейчас не объясняется зрителю явно.
- Для `CrumpleVideo` нужно добавить лёгкий обучающий CTA именно для Telegram stories, чтобы зритель понял механику чтения без изменения длины ролика и без пересборки набора кадров.
- Утверждённая CTA-последовательность:
  - `Нажми и держи` / `чтобы читать`
  - `Держишь палец` / `афиша на паузе`
  - `Веди по экрану` / `чтобы промотать`
- Ограничения:
  - итоговая длина ролика не меняется;
  - состав кадров и порядок сцен не меняются;
  - CTA можно показывать только в промежутках, где бумага ещё свёрнута и сцена не перекрыта разворачиваемой или уже развёрнутой афишей;
  - обучение должно быть распределено по нескольким таким промежуткам, а не сведено в один длинный экран;
  - текст должен быть предельно лаконичным, легко читаться и оставаться вторичным относительно самой афиши;
  - подача не должна визуально давить, перетягивать всё внимание или раздражать при повторных просмотрах;
  - базовый акцент: объяснить `нажми и держи`, вторичный акцент: при необходимости подсказать, что во время удержания можно вести палец по экрану для перемотки;
  - целевой шрифт для этого слоя — современный `Cygre`; допускается деликатный визуальный cue в виде стилизованного касания/пальца, если он не ломает сцену.
- Runtime contract:
  - CTA распределяется по первым трём folded-paper interstitials;
  - сам текст остаётся статичным, а “живым” остаётся только очень тихий touch cue;
  - CTA композится под слоем бумаги, поэтому при unfold бумага естественно перекрывает подсказку и она не остаётся поверх сцены.

### Scheduled Recovery (`/v tomorrow`)

- Status: `Not confirmed by user` (`2026-04-12`) until the next real scheduled slot proves the recovery path without manual intervention.
- Same-day startup catch-up и live watchdog для `video_tomorrow` не должны считать delivery завершённым только потому, что scheduler entrypoint успел записать `ops_run=success`.
- Если scheduled `/v tomorrow` уже стартовал в этот локальный день, но его единственная matching-сессия по `profile_key + target_date` завершилась ранним recoverable `FAILED`, runtime должен разрешить один автоматический rerun того же scheduled path после рестарта/догоняющего watchdog.
- Текущий allowlist recoverable early failures:
  - `missing video output`
  - `kaggle push failed`
- Это recovery намеренно одноразовое для данной local-day / profile / target-date комбинации: если matching failed attempts уже больше одной, дальнейший rerun требует ручного вмешательства, чтобы не уйти в бесконечный цикл.

### Состав и источник афиш

- Тестовый запуск:
  - до 12 афиш;
  - из базы анонсов (реальные, выбранные на завтра).
- Боевой запуск:
  - 2-12 афиш;
  - все из базы анонсов;
  - отсутствие тестовых картинок в финальном видео.
- Для `/v - Запуск завтра` и `/v - Тест завтра` события с распроданными билетами не попадают в выборку.
- Для `/v` ярмарка считается «идущей сейчас» только если `end_date` подтверждён источником. `end_date_is_inferred=1` не расширяет выборку на будущие дни и не рисуется как `по ...` на афише.

### Качество афиш (OCR / полнота данных)

- В видео попадают только афиши с непустым `EventPoster.ocr_text` (пустые/пунктуация считаются пустыми).
- На афише должны присутствовать: название события, дата+время, место проведения.
- Если `ocr_text` есть, но на афише не хватает части данных, пайплайн добавляет плашку с недостающей
  информацией (best-effort размещение в зоне с низкой «плотностью текста»).
- Проверка полноты `ocr_text` делается через `Gemma-3-27b` (Google AI клиент + общий rate-limit фреймворк).

### Подпись поста

- Формат: `Видео-анонс #{номер} на завтра {дата или диапазон дат}`.
- Дата берётся из выбранных событий: одна дата или диапазон.

### Intro: актуальный макет

#### Intro ref (weekend)

```css
position: relative;
width: 1080px;
height: 1572px;

background: #F1E44B;

/* 24-25 */
position: absolute;
width: 945px;
height: 308px;
left: 55px;
top: 270px;

font-family: 'Benzin-Bold';
font-style: normal;
font-weight: 400;
font-size: 224px;
line-height: 308px;
text-align: right;

color: #100E0E;

/* января */
position: absolute;
width: 476px;
height: 200px;
left: 850px;
top: 541px;

font-family: 'Bebas Neue';
font-style: normal;
font-weight: 400;
font-size: 200px;
line-height: 200px;

color: #100E0E;

transform: rotate(-90deg);

/* КАЛИНИНГРАД СВЕТЛОГОРСК ЗЕЛЕНОГРАДСК */
position: absolute;
width: 357px;
height: 267px;
left: 435px;
top: 1058px;

font-family: 'Oswald';
font-style: normal;
font-weight: 400;
font-size: 60px;
line-height: 89px;
text-align: right;

color: #100E0E;

/* ВЫХОДНЫЕ */
position: absolute;
width: 710px;
height: 279px;
left: 82px;
top: 779px;

font-family: 'Druk Cyr';
font-style: normal;
font-weight: 700;
font-size: 220px;
line-height: 279px;

color: #100E0E;
```

#### Intro ref (day)

```css
position: relative;
width: 1080px;
height: 1572px;

background: #F1E44B;

/* 19 */
position: absolute;
width: 324px;
height: 308px;
left: 676px;
top: 270px;

font-family: 'Benzin-Bold';
font-style: normal;
font-weight: 400;
font-size: 224px;
line-height: 308px;
text-align: right;

color: #100E0E;

/* января */
position: absolute;
width: 476px;
height: 200px;
left: 850px;
top: 541px;

font-family: 'Bebas Neue';
font-style: normal;
font-weight: 400;
font-size: 200px;
line-height: 200px;

color: #100E0E;

transform: rotate(-90deg);

/* КАЛИНИНГРАД СВЕТЛОГОРСК ЗЕЛЕНОГРАДСК */
position: absolute;
width: 357px;
height: 267px;
left: 435px;
top: 1058px;

font-family: 'Oswald';
font-style: normal;
font-weight: 400;
font-size: 60px;
line-height: 89px;
text-align: right;

color: #100E0E;

/* ПОНЕДЕЛЬНИК */
position: absolute;
width: 724px;
height: 228px;
left: 73px;
top: 827px;

font-family: 'Druk Cyr';
font-style: normal;
font-weight: 700;
font-size: 180px;
line-height: 228px;
text-align: right;

color: #100E0E;
```

#### Intro ref (different months)

```css
/* intro ref (different monhes) */

position: relative;
width: 1080px;
height: 1572px;

background: #F1E44B;


/* КАЛИНИНГРАД СВЕТЛОГОРСК ЗЕЛЕНОГРАДСК */

position: absolute;
width: 357px;
height: 267px;
left: 435px;
top: 1058px;

font-family: 'Oswald';
font-style: normal;
font-weight: 400;
font-size: 60px;
line-height: 89px;
text-align: right;

color: #100E0E;



/* ФЕВРАЛЯ */

position: absolute;
width: 480px;
height: 228px;
left: 317px;
top: 827px;

font-family: 'Druk Cyr';
font-style: normal;
font-weight: 700;
font-size: 180px;
line-height: 228px;

color: #100E0E;



/* ЯНВАРЯ — */

position: absolute;
width: 504px;
height: 228px;
left: 317px;
top: 637px;

font-family: 'Druk Cyr';
font-style: normal;
font-weight: 700;
font-size: 180px;
line-height: 228px;

color: #100E0E;



/* 31 */

position: absolute;
width: 107px;
height: 228px;
left: 157px;
top: 637px;

font-family: 'Druk Cyr';
font-style: normal;
font-weight: 700;
font-size: 180px;
line-height: 228px;
text-align: right;

color: #100E0E;



/* 1 */

position: absolute;
width: 44px;
height: 228px;
left: 220px;
top: 827px;

font-family: 'Druk Cyr';
font-style: normal;
font-weight: 700;
font-size: 180px;
line-height: 228px;
text-align: right;

color: #100E0E;
```

## Критерии приемки (готово для нового тестирования)

- Тестовый запуск `/v - Тест завтра`:
  - intro соответствует одному из макетов (weekend/day);
  - до 12 афиш из БД;
  - события с распроданными билетами исключены;
  - подпись соответствует шаблону;
  - авто-расширение окна подбора не превышает 5 дней;
  - нет таймаутов Blender.

## Текущее состояние реализации

- Фича оформлена как canonical feature: основной документ здесь, роутинг добавлен в `docs/routes.yml` как `crumple_video`.
- У `/v` уже есть ручной session flow: список кандидатов, переключение `READY/SKIPPED`, разворачивание полного списка, ручная сортировка сцен и превью `payload.json`.
- В меню есть быстрые кнопки `🎬 Завтра: проверка перед запуском` и `🧪 Тест Завтра`; ручной flow остаётся основным operator-mode.
- Обе быстрые кнопки теперь работают через ручной preflight: подбор не уходит в Kaggle сразу, сначала оператор видит `INPUT/SELECTED`, текущий лимит сцен и может отменить запуск.
- В `scheduling.py` теперь есть optional job для полностью автоматического `/v` на завтра:
  - основной env route: `ENABLE_V_TOMORROW_SCHEDULED=1`, `V_TOMORROW_TIME_LOCAL`, `V_TOMORROW_TZ`, `V_TOMORROW_PROFILE`;
  - legacy env `ENABLE_V_TEST_TOMORROW_SCHEDULED` / `V_TEST_TOMORROW_*` пока остаются fallback alias;
  - по умолчанию запускается в `16:00 Europe/Kaliningrad`, чтобы даже при `VIDEO_KAGGLE_TIMEOUT_MINUTES=225` успеть закончить render + story fanout до вечернего окна и не пересекаться с `guide_excursions_full` в `20:10`;
  - по умолчанию использует production path `VideoAnnounceScenario.run_tomorrow_pipeline(... test_mode=False)`;
  - при `V_TOMORROW_TEST_MODE=1` тот же slot можно временно вернуть в legacy test-render на `12` сцен (`TOMORROW_TEST_MIN_POSTERS`);
  - если для профиля не выбран `test`-канал, итоговый ролик уходит в операторский chat/superadmin chat.
- После завершения рендера результат всегда отправляется в `test`-канал, а если для профиля настроен `main`-канал, бот сейчас автоматически дублирует ролик и туда.
- Если `VIDEO_ANNOUNCE_STORY_ENABLED=1`, story publish выполняется внутри `Kaggle` notebook, а не после локального скачивания:
  - notebook читает `story_publish.json` из session-dataset;
  - auth для Telethon передаётся в Kaggle через encrypted split-datasets (`story_publish.enc` + `story_publish.key`);
  - production order лучше задавать явно через `VIDEO_ANNOUNCE_STORY_TARGETS_JSON`; если он задан, именно этот ordered list целиком определяет target fanout (например `@kenigevents`, затем `@lovekenig` через `600` секунд);
  - `main`-канал профиля + `VIDEO_ANNOUNCE_STORY_EXTRA_TARGETS_JSON` остаются только как legacy fallback, если explicit ordered list не задан;
  - перед долгим video-render notebook делает `CanSendStoryRequest` preflight; если текущая Telegram-сессия не умеет публиковать stories (например, user account без `Telegram Premium`) или target не принимает stories, run останавливается до рендера и пишет понятный `story_publish_report.json`;
  - `story_publish_report.json` записывается в JSON-safe виде даже если Telethon возвращает `datetime`/TL-object поля в `result`;
  - для story-video cover/preview принудительно ставится на `0` секунд, то есть CrumpleVideo использует первый кадр ролика как preview frame;
  - перед story-upload notebook должен готовить отдельную story-safe копию `1080x1920`, в которую исходный `1080x1572` ролик вписывается целиком с вертикальным паддингом без дополнительного zoom/crop;
  - production story publish должен отправляться с `pinned=true`, чтобы история попадала в Telegram surface со списком опубликованных stories, тогда как smoke/image-only runs не должны туда добавляться;
  - story lifetime зависит от охвата дат: `12h`, если выбранные события покрывают только одну дату (`завтра`), и `24h`, если ролик охватывает две и более дат;
  - для story-video действует отдельный guard `30 MB`: если финальный mp4 больше, notebook считает story publish failed и пишет это в report;
  - notebook пишет `story_publish_report.json` в output и считает run failed, если story publish был включён, но любой target завершился ошибкой.
  - production env дополнительно может включать `VIDEO_ANNOUNCE_STORY_REQUIRED=1`; тогда `/healthz` обязан считать runtime unhealthy, если stories неожиданно выключены или story-auth/story-target path повреждён, чтобы branch/config drift не оставлял `/v` “успешным” без story fanout.
- Отдельно от story publish:
  - primary path для самих афиш остаётся обычный HTTP download poster assets; отсутствие Telethon auth само по себе не должно валить render;
  - при этом Telegram/Telethon lookup остаётся реальным rescue-path для poster URLs, которые уже перестали отдаваться по `catbox`/прямому HTTP, но всё ещё могут быть найдены по filename через ту же Telegram session;
  - если notebook уже получил working Telegram auth/runtime для story publish через encrypted datasets, poster cache обязан переиспользовать этот auth и не зависеть только от `kaggle_secrets` API;
  - Kaggle Secrets API может оставаться legacy fallback для poster cache, но его недоступность не должна отключать Telegram rescue-path, если encrypted story auth datasets уже присутствуют;
  - Telegram cache search остаётся best-effort и не должен валить render, если cache search не сработал или local cache path невалиден; в таком случае pipeline продолжает обычный HTTP path и уже потом решает судьбу конкретной сцены по фактической доступности изображения.
- Для быстрого smoke-check перед долгим рендером есть отдельный image-only runner: `kaggle/execute_crumple_story_smoke.py`.
- Дефолтный runtime timeout для `/v` поднят до `225` минут (`VIDEO_KAGGLE_TIMEOUT_MINUTES`), чтобы длинные Kaggle runs успевали не только дорендерить mp4, но и отдать output на download path.
- Live preflight on `2026-04-07` уже проходил для обоих production targets `@kenigevents` и `@lovekenig` на premium-сессии, поэтому актуальный rollout-risk для stories лежит в code/config path, а не в старом `BOOSTS_REQUIRED`.
- Инцидент `2026-04-10`: если очередной prod-deploy уводит runtime на ветку без story-layer или возвращает `VIDEO_ANNOUNCE_STORY_ENABLED=0`, это считается release/branch incident, а не “нормальным mp4-only режимом” для production `/v`.

## Продовый rollout

- Боевой roadmap и TODO ведутся в [tasks/README.md](/workspaces/events-bot-new/docs/features/crumple-video/tasks/README.md).
- До включения продового режима `/v` нужно ориентироваться на этот roadmap, а не на исторические заметки из корневого `README.md`.
