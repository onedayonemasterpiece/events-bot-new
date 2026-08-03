# Погода и температура воды на календарных листингах

> **Дата решения:** 2026-08-02
> **Статус:** consumer prototype реализован default-off; producer и public rollout заблокированы release gates
> **Решение:** `GO_TO_PROTOTYPE`, но `NO-GO` для public rollout до закрытия freshness,
> attribution, browser и failover gates
> **Scope:** `/segodnya/`, `/zavtra/`, `/date-YYYY-MM-DD/` и `/vyhodnye/`; без
> изменения event-detail, ранжирования событий, sitemap и Event JSON-LD

## 1. Итоговое продуктовое решение

Погодный контекст полезен именно на странице выбора событий по дате: он помогает
быстро решить, оставаться ли в городе, выбирать ли уличные события и имеет ли
смысл планировать поездку на побережье. Для Калининградской области это не
декоративная информация, а часть решения «куда поехать в этот день».

В MVP принимается следующий вариант:

1. Показывать **один компактный погодный блок на дату**, а не повторять погоду в
   каждой карточке события.
2. Сохранять две понятные пользователю локации из «Котопогоды»:
   **«Калининград»** и **«Побережье»**.
3. Для Калининграда показывать состояние погоды и дневной диапазон температуры;
   для Побережья — состояние погоды, дневной диапазон и, при выполнении порога,
   температуру воды.
4. Температуру воды показывать только тогда, когда **отображаемое значение
   строго выше `+16,0 °C`**. Значение `+16,0 °C` и ниже не показывается.
5. Погода остаётся вторичным контекстом: она не меняет порядок событий, не
   скрывает события и не становится фильтром в первой версии.
6. Не запускать полный StaticSiteBuilder каждые 30 минут. Погода обновляется
   отдельным лёгким publisher-контуром и подхватывается страницей как небольшой
   first-party JSON.
7. При недоступности или устаревании прогноза блок исчезает целиком или частично;
   основной список событий продолжает работать без изменений.

Это решение намеренно не превращает сайт анонсов в погодный сервис: блок должен
занимать не больше одной компактной строки на десктопе и двух строк на мобильном.

## 2. Что уже есть в «Котопогоде» и что можно переиспользовать

Каноническое описание текущей реализации находится в
[`cat-weather-new/docs/weather.md`](https://github.com/onedayonemasterpiece/cat-weather-new/blob/main/docs/weather.md).

Уже реализованы:

- получение городской погоды из Open-Meteo;
- получение морских условий из Open-Meteo Marine API;
- `sea_surface_temperature`, высота волн и периодные значения;
- обновление примерно раз в 30 минут;
- локальный SQLite-кэш;
- повтор запроса до трёх раз при сбое;
- WMO-коды и их нормализация для интерфейса;
- сущности `cities`, `seas`, `weather_cache_*` и `sea_cache`;
- отдельная логика для городского и морского снимков.

Важные ограничения текущего состояния:

- production-координаты и фактические записи «Калининград»/«Побережье» живут в
  базе «Котопогоды», а не в проверяемом Git-конфиге; перед реализацией нужен
  точный read-only export, а не приблизительные координаты из памяти;
- текущие запросы в коде «Котопогоды» ограничены `forecast_days=2`; этого
  недостаточно, чтобы в понедельник показать прогноз на следующие выходные;
- существующий `sea_cache` удобен для текущего дня и частей суток, но не является
  готовым датированным контрактом для календарных страниц сайта;
- публичного стабильного weather API-контракта для сайта анонсов сейчас нет;
- прямое чтение SQLite «Котопогоды» из сайта или browser-call к Fly runtime
  создаст хрупкую runtime-зависимость и не принимается.

Следовательно, переиспользовать нужно не Telegram-представление, а проверенную
цепочку provider → retry → normalization → cache и точные production-локации.

## 3. Где показывать

### 3.1. Страница конкретной даты, десктоп

Текущая последовательность в `DateListingSurface.astro`:

```text
ListingPageHeader
ListingDiscoveryRail
time navigation / timeline
```

Целевая последовательность:

```text
ListingPageHeader
WeatherDateContext            ← новое место
ListingDiscoveryRail
time navigation / timeline
```

Макет:

```text
8 августа
События на 8 августа                                      24 события

┌────────────────────────────────────────────────────────────────────┐
│ Калининград  🌤  +19…+24°     Побережье  ⛅  +17…+21° · вода +17,4° │
│ Прогноз на субботу · обновлён 07:32 · Open-Meteo                   │
└────────────────────────────────────────────────────────────────────┘

[Утро] [День] [Вечер]                          [Вся область ▾]
```

Блок не должен визуально конкурировать с H1 и количеством событий. Вторая строка
может быть скрыта за доступным `details`/tooltip на узком десктопе, но время
актуальности и источник остаются доступны assistive technology.

### 3.2. Страница даты, мобильный

Текущий `MobileListingRailSurface.astro` уже разделяет крупный `.page-head` и
строку `.feed-head`. Погодный блок размещается между ними и не становится sticky.

```text
События на 8 августа
8 августа · суббота · 24 события

┌──────────────────────────────────┐
│ Калининград   🌤  +19…+24°       │
│ Побережье     ⛅  +17…+21°        │
│                вода +17,4°       │
└──────────────────────────────────┘

По времени                 [Вся область]
10:00  …
```

При воде `+16,0 °C` или ниже третья часть строки отсутствует, но высота блока не
должна оставлять пустую «дырку».

Погода не добавляется в фиксированный заголовок после прокрутки: пользователь уже
получил контекст, а 64-пиксельная sticky-зона нужна дате и навигации.

### 3.3. «Выходные»

На десктопе прогноз относится к каждому дню отдельно и входит в шапку субботы и
воскресенья, а не выводится одним неоднозначным средним значением.

```text
┌────────────────────────────┬────────────────────────────┐
│ Суббота, 8 августа         │ Воскресенье, 9 августа     │
│ 🌤 +19…+24°                │ 🌦 +16…+20°                │
│ Побережье · вода +17,4°    │ Побережье · вода +16,3°    │
├────────────────────────────┼────────────────────────────┤
│ события по времени         │ события по времени         │
```

В примере справа вода `+16,3 °C` показывается, потому что значение строго выше
порога. Если значение равно `+16,0 °C`, строка воды отсутствует.

На мобильном погодная строка находится в заголовке соответствующей группы
«Суббота»/«Воскресенье». Один общий блок над обеими группами запрещён.

### 3.4. Где не показывать в MVP

| Поверхность | Решение | Причина |
|---|---|---|
| Каждая карточка события | Нет | визуальный шум, дублирование и ложная привязка общей погоды к indoor-событию |
| Полный календарный bottom sheet | Нет | слишком плотная сетка; иконки погоды ухудшат выбор даты и доступны лишь на коротком горизонте |
| Горизонтальный date rail | Нет | маленькие chips уже несут weekday/date/weekend state |
| Event detail | Нет | сначала требуется надёжная классификация outdoor/coastal и точное время события |
| Поиск, «Популярное», подборки | Нет | там нет одного однозначного date context |
| Sitemap, Event JSON-LD, OG description | Нет | прогноз изменчив и не является фактом о мероприятии |

## 4. Правила данных и отображения

### 4.1. Временная зона и дата

- Все расчёты выполняются в `Europe/Kaliningrad`.
- Ключ прогноза — календарная дата листинга, а не UTC-дата запроса.
- Для `/segodnya/` сначала применяется уже существующий runtime guard актуальной
  даты; погодный блок не должен показывать прогноз «сегодня» поверх архивной
  static-сборки вчерашнего дня.
- Для прошедших дат прогноз не показывается.
- Product horizon MVP — **не более 7 календарных дней**, даже если provider
  технически отдаёт более длинный прогноз. Дальние даты остаются без блока.

### 4.2. Температура воздуха

- Отображается дневной диапазон для целевой даты, а не ночной минимум суток.
- Базовый интервал агрегации: `09:00–21:00 Europe/Kaliningrad`.
- Значения округляются до целого градуса для компактности.
- Для сегодняшнего дня допустимо добавить текущее значение, но оно не заменяет
  диапазон и не должно называться прогнозом на весь день.
- Ветер показывается только при продуктовом пороге «заметный/сильный»; постоянная
  третья цифра перегружает MVP.

### 4.3. Температура воды

- Используется `sea_surface_temperature` для точной production-точки
  «Побережье».
- Для будущего дня берётся медиана значений `10:00–18:00` по местному времени;
  для текущего дня — свежий current либо та же дневная медиана.
- Отображается один знак после запятой.
- Порог применяется к **округлённому отображаемому значению**:

```text
show_water_temperature = round_half_up(value_c, 1) > 16.0
```

Так пользователь никогда не увидит строку «вода +16,0°», которая формально была
разрешена из-за скрытых сотых долей.

- Порог отвечает только за видимость показателя. Он не означает автоматически,
  что купание безопасно или комфортно.
- Значение снабжается доступным текстом «температура поверхности воды», а не
  только эмодзи.

### 4.4. Состояние моря

Высота волны уже есть в «Котопогоде», но в основном погодном блоке MVP не
показывается. Она используется как quality/safety signal и позже — как один из
ворот промо-маршрута. Формулировки «шторм»/«сильный шторм» нельзя смешивать с
туристическим CTA без отдельного визуального и редакционного решения.

### 4.5. Частичные данные

- Есть Калининград, нет Побережья: показывается только Калининград.
- Есть Побережье, но нет sea temperature: показывается погода Побережья без воды.
- Есть только температура воды, но нет валидной погодной строки Побережья:
  допускается строка `Побережье · вода +…`, если sea snapshot свежий.
- Нет свежих данных ни по одной локации: весь блок отсутствует.
- `null`, `NaN`, provider-коды и технические ошибки никогда не выводятся в UI.

## 5. Рекомендуемая архитектура

### 5.1. Почему не полный static build

«Котопогода» обновляет данные примерно каждые 30 минут, а production static-site
строится тяжёлым проверяемым контуром через snapshot/Kaggle/candidate gates.
Запускать полный каталог из-за изменения одной температуры дорого, создаёт
очереди и повышает риск вытеснить действительно важный Smart Update.

### 5.2. Producer → immutable snapshot → pointer

Рекомендуемый контур:

```text
Open-Meteo Weather + Marine
  -> существующий refresh/cache «Котопогоды»
  -> новый weather-calendar exporter
  -> schema validation + freshness/quality checks
  -> immutable first-party JSON snapshot
  -> atomic current.json pointer
  -> progressive WeatherDateContext на календарной странице
```

Предпочтительный владелец producer — `cat-weather-new`, потому что там уже есть
расписание, retry, кэш и точные production-локации. Сайт анонсов не читает его
SQLite и не вызывает bot runtime на page view; границей становится долговечный
версионированный artifact.

Пути (ориентир):

```text
/data/weather/v1/snapshots/<sha256>.json   # immutable, long cache
/data/weather/v1/current.json              # маленький atomic pointer
```

Размещать лучше под first-party same-origin `kenigevents.ru`. Если используется
`static.kenigevents.ru`, нужны явный CORS, отдельный cache contract и browser gate.
Прямые запросы браузера к `api.open-meteo.com` и
`marine-api.open-meteo.com` запрещены.

### 5.3. Почему не прямой API «Котопогоды»

Runtime endpoint создал бы зависимость доступности календаря от Fly-сервиса,
CORS, rate limiting и версии bot deployment. Immutable snapshot сохраняет
последнее корректное состояние, легко проверяется, кэшируется CDN и не раскрывает
внутреннюю БД.

### 5.4. Предлагаемый контракт `weather-calendar-v1`

```json
{
  "schema": "weather-calendar-v1",
  "snapshot_id": "weather-20260802T053000Z-a1b2c3",
  "generated_at": "2026-08-02T05:30:00Z",
  "valid_until": "2026-08-02T08:30:00Z",
  "timezone": "Europe/Kaliningrad",
  "provider": {
    "name": "Open-Meteo",
    "attribution_url": "https://open-meteo.com/"
  },
  "location_revision": "kotopogoda-production-locations-20260802",
  "days": [
    {
      "date": "2026-08-08",
      "kaliningrad": {
        "status": "fresh",
        "temperature_day_min_c": 19.1,
        "temperature_day_max_c": 24.2,
        "weather_code": 1,
        "wind_day_max_m_s": 4.7,
        "source_updated_at": "2026-08-02T05:00:00Z"
      },
      "coast": {
        "status": "fresh",
        "temperature_day_min_c": 17.0,
        "temperature_day_max_c": 21.1,
        "weather_code": 2,
        "wind_day_max_m_s": 5.2,
        "sea_surface_temperature_c": 17.4,
        "wave_height_day_max_m": 0.3,
        "show_water_temperature": true,
        "source_updated_at": "2026-08-02T05:00:00Z"
      }
    }
  ],
  "errors": []
}
```

`show_water_temperature` вычисляет producer по каноническому правилу, а Astro не
переопределяет порог из float. При этом consumer всё равно валидирует диапазоны и
не доверяет произвольному JSON.

Точный JSON Schema должен храниться в `events-bot-new` рядом с consumer tests и
использоваться exporter-тестами в `cat-weather-new`. Изменение семантики требует
новой schema version, а не молчаливого расширения старого поля.

#### Точный atomic pointer contract

`current.json` не содержит прогноз и не является mutable snapshot. Его допустимый
формат зафиксирован в `site/src/lib/weather-calendar-pointer-v1.schema.json`:

```json
{
  "schema": "weather-calendar-pointer-v1",
  "snapshot_id": "weather-20260803T180000Z-a1b2c3",
  "snapshot_url": "/data/weather/v1/snapshots/<sha256>.json",
  "sha256": "<64 lowercase hex>",
  "updated_at": "2026-08-03T18:00:01Z"
}
```

Consumer принимает только same-origin URL без query/fragment, требует совпадения
имени immutable-файла с `sha256`, проверяет SHA-256 до JSON parse и сверяет
`snapshot_id`. Лимиты: pointer `<=4 KiB`, snapshot `<=64 KiB` uncompressed;
целевой compressed budget по-прежнему `<=20 KiB`. Pointer загружается
`no-store`, immutable snapshot — `force-cache`. Прямой browser request к
Open-Meteo или runtime «Котопогоды» запрещён.

### 5.5. Freshness и last-known-good

- Producer запускается после успешного weather refresh, ориентировочно каждые
  30 минут.
- `current.json` сдвигается только после schema, range, timezone и completeness
  checks.
- Последний корректный immutable snapshot не удаляется при provider failure.
- Consumer сверяет `generated_at`, `valid_until`, дату и per-location status.
- Начальные пороги для прототипа:
  - today: `fresh <= 90 min`, `degraded <= 3 h`, после 3 h скрыть;
  - future day: `fresh <= 3 h`, `degraded <= 6 h`, после 6 h скрыть;
  - точные значения подтвердить на canary, но они не могут быть бесконечными.
- Service Worker/PWA не должен бессрочно возвращать cached `current.json`:
  network-first + проверка `valid_until`; stale cache не становится «погодой».
- Частичный provider failure не обнуляет last-good другой локации, но каждый
  фрагмент сохраняет собственный timestamp.

### 5.6. Детерминированный review

Immutable secret candidate не должен зависеть от меняющейся погоды во время
Playwright-run. Для проверки используются:

1. pinned snapshot/fixture для visual and contract gates;
2. отдельный live read-only smoke producer → pointer;
3. production consumer, который после rollout читает current pointer.

В mandatory fixtures должны быть как минимум состояния:

- вода `+16,0°` — скрыта;
- вода `+16,1°` — показана;
- только Калининград;
- только Побережье;
- partial sea data без температуры воды;
- stale snapshot;
- future date вне горизонта;
- суббота и воскресенье с разной погодой;
- archived `/segodnya/` после смены локальной даты;
- malformed/unknown schema.

## 6. UI/UX-подводные камни

| Риск | Почему опасно | Решение |
|---|---|---|
| Погода повторяется в каждой карточке | шум и ложная связь с конкретным событием | один route-level блок |
| Один прогноз на весь weekend | суббота и воскресенье могут различаться | отдельный day context |
| Текущая погода на будущей странице | пользователь принимает «сейчас» за прогноз даты | date-keyed manifest и явная подпись |
| Дальние календарные даты | низкая достоверность и отсутствующие данные | product horizon 7 дней; дальше скрыть |
| Текущий Kotopogoda horizon 2 дня | следующий weekend часто не покрыт | отдельный 7-day exporter/query, не менять старый Telegram output молча |
| Микроклимат побережья | Калининградская температура не описывает море | отдельная точка «Побережье» |
| Модельная SST у берега | это поверхность модельной ячейки, не замер пляжа | точная подпись, без обещания безопасности; не для навигации |
| Порог мерцает около 16° | +16,0 может появляться из-за скрытых сотых | gate по округлённому значению |
| Смена дня на static `/segodnya/` | прогноз может относиться к новой дате, список — к старой | weather mount после existing Today guard |
| Provider outage | пустые/устаревшие цифры снижают доверие | immutable LKG + hard expiry + partial hide |
| Полный rebuild каждые 30 минут | очередь Kaggle и риск stale event catalog | отдельный маленький publisher |
| Прямой third-party browser fetch | privacy/CORS/rate limit/нестабильность | first-party artifact |
| Layout shift | блок появляется после загрузки и сдвигает события | bounded slot/early preload; Playwright CLS gate |
| Эмодзи без текста | непонятно screen reader и при монохроме | текстовые condition labels, decorative emoji |
| Цвет как единственный сигнал | accessibility regression | состояние всегда выражено текстом |
| Weather SEO/JSON-LD | изменчивая информация смешивается с Event facts | не включать в Event structured data |
| Лицензия/атрибуция | использование в другом продукте не наследуется автоматически | проверить режим Open-Meteo, дать видимую attribution |

Проверка официальных условий 2026-08-03 показала: данные Open-Meteo
распространяются по CC BY 4.0 с обязательной атрибуцией, а free API предназначен
только для некоммерческого использования и имеет rate limits. До production
нужно письменно зафиксировать выбранный режим: подходящая commercial
subscription/customer endpoint либо отдельное решение о self-hosting. Проверка
должна быть повторена непосредственно перед rollout; ключ/endpoint провайдера в
browser не передаётся. Видимая attribution сопровождается указанием, что сайт
агрегирует и округляет исходные данные:

- <https://open-meteo.com/en/docs>
- <https://open-meteo.com/en/docs/marine-weather-api>
- <https://open-meteo.com/en/terms>
- <https://open-meteo.com/en/pricing>
- <https://open-meteo.com/en/license>
- <https://open-meteo.com/>

Marine API отдельно предупреждает об ограниченной точности некоторых данных в
прибрежных зонах. Значения нельзя позиционировать как измерение конкретного
участка Западного пляжа или как информацию для навигации/безопасности.

## 7. Дополнительная промо-идея: «Поехать на море с комфортом»

> **Статус:** только backlog/product hypothesis. Не входит в weather MVP, не
> является реальным событием и не разрешена к автоматической публикации.

Идея: в хороший пляжный выходной показать отдельную редакционную карточку
**«Поехать на море с комфортом»** — поездка на Западный пляж Зеленоградска на
сдвоенной «Ласточке».

### 7.1. Продуктовая форма

Карточка не должна маскироваться под организованное мероприятие. Рекомендуемый
тип — `editorial_route` / «Идея дня»:

```text
Идея дня
Поехать на море с комфортом
Западный пляж, Зеленоградск · на сдвоенной «Ласточке»
[Посмотреть маршрут и актуальное расписание]
```

Она:

- не увеличивает счётчик событий;
- не входит в time timeline;
- не получает Event JSON-LD;
- не создаёт event ICS;
- не попадает в sitemap как самостоятельное событие;
- может располагаться после погодного блока и до списка «По времени»;
- ведёт на проверяемую страницу маршрута/расписания либо на официальный источник.

### 7.2. Предварительные eligibility gates

Все условия должны выполняться одновременно для конкретной субботы или
воскресенья:

1. дата является выходным днём в `Europe/Kaliningrad`;
2. прогноз Побережья свежий и относится к этой дате;
3. отображаемая вода строго выше `+16,0 °C`;
4. дневная температура воздуха на Побережье не ниже `+20 °C`;
5. нет грозы, тумана, сильного/продолжительного дождя;
6. начальные research-пороги: precipitation probability max `<=20%`, дневная
   сумма осадков `<1 mm`, устойчивый ветер `<=6 m/s`, порывы `<=10 m/s`;
7. максимальная высота волны `<0.5 m`;
8. transport manifest из официального источника подтверждает актуальные прямые
   рейсы туда и обратно;
9. факт **сдвоенного состава** подтверждён структурированным расписанием или
   официальным сообщением — его нельзя выводить из слова «Ласточка»;
10. расписание свежее, нет отмены, ремонта или service alert.

Пороги являются гипотезой для canary, а не утверждённым стандартом пляжной
погоды. Их нужно проверить на исторических данных и вручную оценить минимум на
20 положительных и 20 отрицательных днях.

### 7.3. Риски промо

- «С комфортом» может обещать свободные места, которых сервис не знает.
  Сдвоенный состав увеличивает вместимость, но не гарантирует посадку/сидячее
  место. Если нет надёжной capacity evidence, безопаснее copy
  «На море на Ласточке».
- Погода и температура воды не доказывают безопасность купания, санитарное
  состояние, флаги спасателей и доступность пляжа.
- Нельзя создавать фальшивое время «начала события»: это маршрут/идея, а не
  организованная поездка.
- Изменение расписания должно немедленно подавлять карточку; stale transport
  last-good здесь недопустим.
- Автоматическая карточка не должна вытеснять сильное реальное событие без
  эксперимента и product-owner approval.

### 7.4. Метрики canary

- impression → open route CTR;
- переход к расписанию;
- сохранение/поделиться маршрутом;
- дальнейший просмотр реальных событий после карточки;
- bounce после клика;
- hide/report и жалобы на недостоверность;
- разница против holdout без промо;
- доля автоматических suppression по weather/transport freshness.

## 8. Метрики погодного MVP

Основная гипотеза: погодный контекст уменьшает неопределённость при выборе даты и
увеличивает полезное взаимодействие со списком, а не просто просмотры виджета.

Primary:

- переход из date/weekend listing в event detail;
- сохранение события/добавление в календарь;
- доля пользователей, открывших второе событие в той же сессии;
- weekend coastal-event engagement при показанном тёплом море.

Guardrails:

- weather JSON success/fresh/partial/stale rate;
- доля дат с ошибочно показанным блоком вне horizon;
- mismatch между listing date и weather date — строго `0`;
- прямые browser calls к внешнему provider — строго `0`;
- page JS errors — `0`;
- горизонтальный overflow на `320/390/720/1366` — `0`;
- дополнительный CLS от погодного блока — `<=0.05`;
- compressed pointer + snapshot transfer — целевой бюджет `<=20 KB`;
- влияние на LCP p75 — не более `+50 ms` на canary;
- жалобы на неверную/устаревшую погоду.

Пассивный блок не требует отдельного клика. Не следует добавлять искусственную
интерактивность только ради метрики.

## 9. Этапы реализации

### M0 — документация и contract freeze

- [x] продуктовая целесообразность;
- [x] размещение desktop/mobile/weekend;
- [x] правило воды `> +16,0 °C`;
- [x] архитектурная граница без полного rebuild;
- [x] промо-идея записана отдельно;
- [ ] точный production export двух location records из «Котопогоды»;
- [x] JSON Schema и fixture inventory в `site/src/lib/weather-calendar-v1.schema.json` и `site/tests/fixtures/weather-calendar/`.

### M1 — producer

- расширить forecast horizon до 7 дней в отдельном exporter path;
- не менять существующую Telegram-семантику «Котопогоды» без regressions;
- получить air forecast для обеих точек и marine forecast для Побережья;
- реализовать date/timezone aggregation и threshold rule;
- immutable upload + atomic pointer + retention;
- provider attribution, call accounting, retry и alerts;
- cold/warm fixture tests, malformed provider payload, partial failure.

### M2 — consumer/UI

- [x] `WeatherDateContext.astro` и маленький runtime loader;
- [x] интеграция в `DateListingSurface.astro`;
- [x] отдельная day-level интеграция в `WeekendListingSurface.astro`;
- [x] mobile placement между `.page-head` и `.feed-head`, для weekend — после заголовка своей day-group;
- [x] no-JS/core fallback без погоды, но с полноценным списком событий;
- [x] PWA остаётся network-only, а pointer читается с `cache: no-store`;
- [x] accessibility и reduced-motion/no-animation contract.

### M3 — immutable review candidate

- pinned fixtures для всех состояний;
- Playwright `320/390/720/1366`;
- date mismatch, threshold, stale/partial/timeout, weekend parity;
- no external browser requests;
- visual owner acceptance;
- live producer → pointer read-only smoke;
- attribution и usage-plan verification.

### M4 — bounded canary

- feature flag default off;
- ограниченный процент production traffic или отдельная accepted page family;
- минимум 7 дней наблюдения, включая одни выходные;
- stop при mismatch, stale leak, layout regression или provider/license issue;
- отдельное решение `ship / narrow / stop`.

### M5 — promo research, отдельно

Только после принятого weather MVP и актуального transport contract. Никакой код
«Поехать на море с комфортом» не должен случайно попасть в M1–M4.

## 10. Release acceptance

Weather track остаётся `NO-GO`, пока не доказано всё ниже:

- точные production coordinates/IDs «Калининград» и «Побережье» экспортированы и
  зафиксированы revision/hash;
- JSON Schema версионирован и валидируется producer/consumer;
- horizon/date/timezone contract проходит boundary tests;
- `+16,0°` скрыто, `+16,1°` показано во всех форматах;
- current pointer атомарен, immutable snapshot readback/hash проверены;
- full static build не запускается на weather refresh;
- provider failure сохраняет last-good, но hard expiry скрывает stale UI;
- `/segodnya/` weather mount согласован с runtime date guard;
- weekend отображает два независимых дня;
- no weather data не ломает HTML, фильтры, timeline и personal feed;
- browser не обращается к third-party weather endpoints;
- Open-Meteo attribution, отметка об агрегации/округлении и актуальный
  commercial/self-host usage plan приняты;
- marine limitations сформулированы без обещания пляжной безопасности;
- Playwright/visual/a11y/performance gates зелёные;
- rollout flag default off и rollback сводится к отключению consumer без rebuild
  event catalog.

## 11. Зафиксированные решения

| Вопрос | Решение 2026-08-02 |
|---|---|
| Нужна ли погода на date listing | Да, как компактный contextual layer |
| Нужна ли вода зимой/при холодной воде | Нет; только отображаемое значение `> +16,0 °C` |
| Локации | «Калининград» и «Побережье», точные production records из «Котопогоды» |
| Погода в каждой event card | Нет |
| Влияние на ranking/filter | Нет в MVP |
| Дальние даты | Без погоды; product horizon 7 дней |
| Обновлять полным static build | Нет |
| Browser → Open-Meteo | Нет |
| Источник данных | reuse collector/cache/locations «Котопогоды» через versioned artifact |
| Weekend | отдельный прогноз для субботы и воскресенья |
| Event detail | вне MVP |
| «Поехать на море с комфортом» | отдельная backlog-гипотеза `editorial_route`, не событие |

## 12. Consumer prototype и producer handoff (2026-08-03)

### Реализованный consumer

- rollout-флаг `PUBLIC_WEATHER_CALENDAR_ENABLED=1`; отсутствие переменной или
  любое другое значение означает `off`;
- `site/src/lib/weatherCalendar.ts` — строгий parser, WMO vocabulary,
  `Europe/Kaliningrad` date/horizon/freshness guards и правило воды;
- `site/src/lib/weatherCalendarRuntime.ts` — один deduplicated load на pointer для
  всех mounts, same-origin/integrity checks и fail-closed partial rendering;
- `WeatherDateContext.astro` подключён только к date/today/tomorrow/weekend
  листингам и не влияет на ranking, counts, JSON-LD или карточки;
- pinned fixtures покрывают `+16,0/+16,1`, city-only, coast-only, sea-only,
  два разных weekend days, stale, out-of-horizon и malformed schema.

### Иконки

Визуально проверен contact sheet семейства
[Weather And Forecast Icons](https://www.svgrepo.com/collection/weather-and-forecast-icons/).
Выбраны SVGRepo IDs `384328`, `384312`, `384313`, `384304`, `384311`,
`384318`, `384323`, `384327`, `384308`: у них одинаковые 32-unit rounded
outline geometry и stroke weight, они читаются и при 24 px. Лицензия — CC0.
В durable assets геометрия не менялась; чёрный stroke адаптирован к
`currentColor`. Полная provenance — `site/public/assets/weather/manifest.json`.

### Почему producer не изменён в этой ветке

Read-only inspection `cat-weather-new@82e834a7` подтвердил: текущие city/marine
requests используют `forecast_days=2`, `sea_cache` не date-keyed, а точные
production records двух локаций находятся только в runtime DB. В checkout нет
безопасного first-party atomic upload adapter и нет подтверждённого
location revision/export. Поэтому добавление producer «по приблизительным
координатам» или публикация из тестовой ветки запрещены.

Producer PR в `cat-weather-new` должен:

1. получить read-only exact records «Калининград» и «Побережье», записать их
   canonical hash/revision без вывода секретов;
2. отдельным exporter path запросить семь календарных дней, не меняя Telegram
   output и его двухдневные cache contracts;
3. агрегировать air `09:00–21:00` и SST median `10:00–18:00` в
   `Europe/Kaliningrad`, вычислить `show_water_temperature` half-up правилом;
4. валидировать payload обеими схемами из этого репозитория, сначала atomic
   загрузить immutable `<sha256>.json`, проверить readback/hash, затем заменить
   `current.json`;
5. при любой partial/provider/upload ошибке не двигать pointer; отправить
   sanitized metric/alert без координат, токенов и provider payload;
6. пройти cold/warm/partial/malformed/retention tests и live read-only smoke до
   включения consumer-флага.

До выполнения этих пунктов статус release track остаётся `NO-GO`; consumer код
безопасно остаётся выключенным и не инициирует сетевых запросов.
