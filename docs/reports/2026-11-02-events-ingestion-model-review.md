# Events Bot: модели Google, расход и деградация импорта — пакет для ChatGPT Pro

Подготовлено 2 ноября 2026, Europe/Kaliningrad. Автор: Codex, по запросу владельца.
**Статус: расследование продолжено в новой сессии; узкий source fix проверен локально. Прод пока не изменён, ChatGPT Pro не запущен.**
Инцидент inc_3d23e0008f860b634306a59b / [основная запись](incidents/INC-2026-11-02-intake-quota-vk-media.md).

## Краткий вывод

**Подтверждённый дефект: source parser теряет обе Lite-модели при двойном построении аварийной цепочки.** VK настроен на gemini-3.1-flash-lite, но реальный путь получает только gemini-3.6-flash → gemini-2.5-flash. В разрешённых им проектах обе Flash исчерпали зарегистрированный RPD. При этом обе Lite имеют запас; 3.5 Flash-Lite действительно существует в Google и успешно используется другими потребителями.

Сегодня VK-import обработал 132 позиции: **106 технических ошибок, 6 успешных импортов, 20 отклонений, 2 новых события**. Technical failure rate — 80,3%. Это не 106 доказанно потерянных уникальных событий: carrier может содержать несколько событий, быть повторным источником или не-событием. Raw posts продолжают сохраняться.

По текущему контракту technical terminal не возвращается автоматически в импорт. Поэтому восстановить новые запросы недостаточно: нужен отдельный ограниченный catch-up. Но бесконечные и identity background retries ранее явно отменялись владельцем — их нельзя незаметно вернуть под видом hotfix.

Не всё исправится маршрутизацией: Lite получают реальные 503, timeout и обрезанные ответы; отдельно 67 будущих VK-проекций заблокированы media gate. **Предпочтительное направление для обсуждения: исправить normal path, сохранить limiter и существующий sibling fallback, проверить replay, затем ограниченно восстановить carriers. Без новой очереди, router или watchdog.**

## 1. Задача и ограничения для ChatGPT Pro

Прочитай пакет целиком. Предложи минимальное изменение, которое заметно уменьшит пропуски актуальных событий, без переделки системы.

Нужно:

1. Проверить причинную цепочку; отделить подтверждённые ошибки от гипотез.
2. Сравнить узкую правку model chain, снятие существующего emergency override и выбор 3.1-first / 3.5-first.
3. Назвать предпочтительный вариант, затронутых consumers, один отличающий regression test и live canary.
4. Отдельно предложить ограниченный catch-up с reuse receipts, Smart Update и защитой от дублей.
5. Указать, что можно сделать срочно, что требует продуктового решения, а какие проверки реально нужны до deploy.
6. Проверить влияние лимита физических sends: pre-send denials не должны тратить sends, но два неудачных provider sends могут исчерпать cap до Lite.

Ограничения:

- Production — **RunCoveer**, container deployment-events-1, DB /data/db.sqlite. Fly — не текущий prod.
- Все Google generation calls через общий reserve → mark_sent → provider → finalize. Не обходить ledger сырыми ключами и не повышать quotas без данных Google.
- Сохранить raw-first, immutable parse receipts, idempotency, Smart Update и LLM-first семантику. Не разгружать систему broad keyword-отсевом событий.
- Не возвращать снятый background identity retry consumer. Не добавлять новую retry-очередь, общий gateway или watchdog без конкретной необходимости.
- Не требовать переписывания main.py, полной смены моделей или миграции GenerateContent → Interactions для исправления локальной ошибки.
- Предпочтителен diff в десятки строк, если этого достаточно. Для любого нового state/branch/retry объяснить наблюдаемый сбой и почему существующий механизм его не покрывает.
- [Обязательная политика простоты](https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/main/skills/devserver-simplicity/SKILL.md).
- Тест, PR и healthy контейнер не означают восстановленный продукт: нужны реальные source→event outcomes и отдельно проверенная публикация.

## 2. Источники, период и ограничения измерений

Runtime image: runcoveer/events:guide-private-f9b50fbb9.
Immutable SHA: f9b50fbb9066e1f85d96d56529a1990256bf2844.
Он является предком проверенного origin/main 2b9a653e6727ec780d27fd83b953e19615cb2d02.

Снимки:

- Live health, read-only SQLite, очереди, file logs: около 19:57–20:01 UTC / 21:57–22:01 Калининграда.
- Google Models API и nonsecret registry/config/counters: **2026-11-02T20:18:21.418539+00:00**.
- Google request ledger cutoff: **2026-11-02T20:18:35.696557+00:00**.
- Период расхода: 2026-10-24T22:00:00Z → cutoff, т.е. локальные 25 октября → неполное 2 ноября.
- Полностью прочитаны **17 810 строк google_ai_requests**, постранично по локальным дням.
- Ledger общий для нескольких продуктов; встречается street-story, а consumer=kaggle не всегда позволяет определить продукт. **Общий расход нельзя назвать расходом только Events Bot.**
- Request row — логический вызов gateway с финальным outcome, не обязательно одна физическая попытка. Pre-send reservation denials могут не создавать request row.
- Tokens — сумма имеющихся usage receipts; null usage не является нулевой реальной стоимостью. Финальный request receipt не обязательно содержит расход всех его предыдущих retries.
- Попытка широкого JOIN к physical attempts получила SQL timeout. Она исключена из выводов; использовано полное индексированное чтение request ledger по дням.
- Сырые prompts/private payloads не запрашивались. В этом исследовании не было Google generation-canaries, публикаций, production writes, restart или deploy.
- Прямые Google calls здесь — только GET метаданных Models API, не генерация.

**Сутки различаются:** RPD сбрасывается в полночь Pacific, отчёт продукта — Europe/Kaliningrad. 2 ноября новый quota day начался в **10:00 Калининграда**. Локальные дневные 1 038 успешных 3.1 Lite-вызовов не противоречат меньшим текущим Pacific-day counters.

## 3. Что есть непосредственно у Google, а не в нашем лимитере

### 3.1. Lite-модели

Выполнен GET https://generativelanguage.googleapis.com/v1beta/models с paging.
На всех шести ключах: HTTP 200, **60 моделей, одинаковые множества, complete=true**.

| ID Google | Назначение | Для source text parse |
|---|---|---|
| gemini-3.1-flash-lite | Текстовая/мультимодальная Lite | Реально работает, есть receipts |
| gemini-3.5-flash-lite | Следующая текстовая/мультимодальная Lite | Реально работает; уже предусмотренный sibling |
| gemini-flash-lite-latest | Подвижный alias | Не считать новым независимым budget |
| gemini-2.5-flash-lite | Legacy Lite | List не доказывает generation access/долговечность |
| gemini-3.1-flash-lite-preview | Старый preview ID | Не использовать вместо stable только потому, что list его вернул |
| gemini-3.1-flash-lite-image | Генерация изображений | Не замена текстового extractor |
| gemini-3.8-flash-lite-tts | Синтез речи | Не замена текстового extractor |

**gemini-3.6-flash-lite в Google list отсутствует.** Нужная взаимозаменяемая на уровне нашей routing policy пара — **3.1 Flash-Lite / 3.5 Flash-Lite**. 3.5 Flash и 3.6 Flash — другие модели.

Google описывает 3.5 Flash-Lite как модель для массового разбора/извлечения документов, с текстовым выводом и structured outputs. Это аргумент рассматривать её как sibling, а не доказательство одинакового extraction quality на наших постах.
[Официальная карточка 3.5 Lite](https://ai.google.dev/gemini-api/docs/models/gemini-3.5-flash-lite).

### 3.2. Более широкий каталог и новые варианты

| Семейство | Примеры, возвращённые Google | Значение для срочного исправления |
|---|---|---|
| Новые general text | gemini-3.8-flash, gemini-4-argon | Рассмотреть позже: нет canary, проверенной quota/params compatibility |
| Другие general text | gemini-3.7-flash, gemini-3.6-flash, gemini-3.5-flash, gemini-3-flash-preview | Не путать с Lite; сейчас emergency Flash имеют маленький budget |
| Pro | gemini-3.1-pro-preview, customtools, gemini-2.5-pro | Не включать в массовый importer без бюджета |
| Gemma | gemma-4-31b-it, gemma-4-26b-a4b-it | 31b уже используется и имеет существенные failures |
| Images | gemini-nano-banana-2.1, gemini-3-pro-image, gemini-3.1-flash-image, Lite Image | Не лечит text routing |
| Speech / Live | gemini-3.5-transcribe, gemini-3.8-live, Flash TTS, Lite TTS | Вне этого ingestion fix |
| Embeddings | gemini-embedding-001, gemini-embedding-2, preview | Отдельный identity/search этап |
| Специализированные | Omni, Antigravity, Deep Research, robotics, Lyria | Не предлагать как простую замену extractor |

[Каталог Google](https://ai.google.dev/gemini-api/docs/models) и [официальный migration reference](https://github.com/google-gemini/gemini-skills/blob/main/skills/gemini-api-dev/references/migration.md) подтверждают новые семейства и рекомендуют 3.5 Lite для старых Lite. Для 3.5 Lite/3.8 Flash есть изменения sampling/thinking parameters; это нужно учитывать при реальной миграции, но API migration не является обязательным лекарством от нашего chain bug.

В Gemini Developer API доступ к 2.5 ограничен existing users; официальный каталог сообщает автоматическую маршрутизацию gemini-2.5-flash в 3.5 Lite с 1 декабря 2026. Не смешивать это с иными сроками Vertex/Enterprise.
[Google Models: 2.5](https://ai.google.dev/gemini-api/docs/models#gemini-25-flash).

**List ≠ health/доступная quota:** каталог содержит stable, aliases, legacy и preview. Успешный GET не доказывает generation на конкретном ключе. Новые модели в этом исследовании не генерировали ответы.

Полный список 60 моделей — в приложении A.

## 4. Израсходованное и оставшееся

### 4.1. Текущий quota day: исчерпаны emergency lanes, а не весь Google

Таблица — **наш registered limit / counters**, не квоты, выгруженные из Google AI Studio. Источник каталога выше независим от limiter metadata.

| Ключ | 3.1 Lite /500 | 3.5 Lite /500 | 3.6 Flash /20 | 2.5 Flash /20 |
|---|---|---|---|---|
| GOOGLE_API_KEY | 235 | 0 | 0 | 20 |
| GOOGLE_API_KEY2 | 73 | 1 | 20 | 0 |
| GOOGLE_API_KEY3 | 16 | 0 | 20 | 0 |
| GOOGLE_API_KEY4 | 16 | 0 | 0 | 20 |
| GOOGLE_API_KEY5 | 84 | 0 | 0 | 0 |
| GOOGLE_API_KEY6 | 80 | 0 | 20 | 0 |

0 означает отсутствие записанного расхода в выбранном day counter, а не гарантию отсутствия стороннего Google traffic.

Production GOOGLE_AI_MODEL_KEY_ENVS_JSON намеренно ограничивает модели:

- gemini-2.5-flash → GOOGLE_API_KEY и GOOGLE_API_KEY4;
- gemini-3.6-flash → GOOGLE_API_KEY2, GOOGLE_API_KEY3 и GOOGLE_API_KEY6.

Все эти пять разрешённых project/model lanes — **20/20**. Обычный pool включает все шесть env. Для каждой Lite зарегистрирован RPD 500 на scope/model. Суммарно текущие counters 3.1 Lite — 504, 3.5 Lite — 1. Запас зарегистрированного бюджета есть.

Это не гарантия ёмкости Google и не независимая проверка key→Cloud project mapping: registry хранит шесть scopes, но для окончательной верификации нужны identities в Google. Увеличивать общий budget только по числу alias нельзя.

В 18:00–около 20:08 UTC были успешные 3.1 Lite calls на ключах 1, 2, 5, 6. На №3 и №4 в том окне были 503, не RPD. За локальные сутки успешные 3.5 Lite receipts есть на всех шести ключах, большинство до нового Pacific day.

По Google quotas действуют на Cloud project, не на ключ, RPD reset — Pacific midnight; фактическая capacity может отличаться от опубликованной.
[Google rate limits](https://ai.google.dev/gemini-api/docs/rate-limits).

### 4.2. Сегодня: запросы, исходы, tokens в общем ledger

Локальные сутки 2 ноября до около 22:18, **все consumers общего Google ledger**:

| Модель | Запросы | Успех | failed_provider | Input | Output | Всего tokens | Без usage |
|---|---|---|---|---|---|---|---|
| gemini-3.1-flash-lite | 1271 | 1038 | 233 | 1560863 | 208977 | 1769840 | 201 |
| gemma-4-31b | 256 | 146 | 110 | 157959 | 13554 | 171513 | 110 |
| gemini-3.5-flash-lite | 85 | 79 | 6 | 95077 | 33343 | 128420 | 3 |
| gemini-3.6-flash | 59 | 55 | 4 | 253108 | 16171 | 269279 | 3 |
| gemini-2.5-flash | 39 | 37 | 2 | 122292 | 11649 | 133941 | 1 |
| gemini-embedding-2 | 16 | 15 | 1 | 5241 | 0 | 5241 | 1 |
| gemini-3.5-flash | 3 | 1 | 2 | 1910 | 565 | 3727 | 2 |

Успех gateway не равен принятому событию. Дальше возможны schema/grounding/Smart Update outcomes. failed_provider включает наш отказ считать truncated response полноценным semantic result, не только outage Google. Пропуски usage означают неполноту измерения.

Tokens — не invoice. Для ориентира standard paid API: 3.1 Lite $0.25/M input text/image/video и $1.50/M output; 3.5 Lite $0.30/M input и $2.50/M output, включая thinking. Billing tier и счета каждого проекта не проверены; суммы в долларах не выдумываются.
[Google pricing](https://ai.google.dev/gemini-api/docs/pricing).

### 4.3. Обе Lite работают; workload не равен контролируемому A/B

| Локальный день | 3.1 Lite успех / ошибка | 3.5 Lite успех / ошибка | Tokens 3.1 / 3.5 |
|---|---|---|---|
| 2026-10-25 | 1135 / 170 | 38 / 0 | 1904292 / 55238 |
| 2026-10-26 | 549 / 374 | 167 / 13 | 968782 / 325878 |
| 2026-10-27 | 1106 / 515 | 352 / 7 | 1826913 / 672209 |
| 2026-10-28 | 1413 / 290 | 73 / 6 | 2340443 / 137940 |
| 2026-10-29 | 1632 / 92 | 28 / 4 | 2612543 / 84032 |
| 2026-10-30 | 1685 / 77 | 43 / 6 | 2633011 / 97090 |
| 2026-10-31 | 1281 / 346 | 77 / 5 | 2082596 / 137012 |
| 2026-11-01 | 930 / 248 | 66 / 3 | 1535815 / 112840 |
| 2026-11-02 | 1038 / 233 | 79 / 6 | 1769840 / 128420 |

За весь период у 3.5 Lite **923 succeeded / 50 failed_provider**, у 3.1 Lite **10 769 / 2 345**, плюс одна старая sent-строка. Разные inputs/consumers/primary-fallback роли не позволяют сделать из этих общих rates вывод, что 3.5 всегда лучше. Но гипотеза «3.5 не работает» опровергнута.

Сегодня крупнейшие сочетания model/consumer:

| Модель | Consumer | Запросы | Успех | Ошибка | Tokens |
|---|---|---|---|---|---|
| gemini-3.1-flash-lite | smart_update | 293 | 282 | 11 | 470429 |
| gemini-3.1-flash-lite | poster_ocr_fallback | 198 | 131 | 67 | 173633 |
| gemini-3.1-flash-lite | kaggle | 155 | 79 | 76 | 98734 |
| gemini-3.1-flash-lite | event_media_role | 120 | 120 | 0 | 213249 |
| gemini-3.1-flash-lite | event_parse | 105 | 71 | 34 | 68516 |
| gemini-3.1-flash-lite | event_media_review | 100 | 98 | 2 | 277241 |
| gemini-3.1-flash-lite | guide_scout_ocr | 84 | 77 | 7 | 110832 |
| gemini-3.5-flash-lite | kaggle | 76 | 70 | 6 | 117720 |
| gemma-4-31b | guide_scout_screen | 72 | 45 | 27 | 62394 |
| gemma-4-31b | video_partner_filter | 64 | 53 | 11 | 52400 |
| gemini-3.1-flash-lite | route_weaver_enrich | 58 | 39 | 19 | 69389 |
| gemma-4-31b | smart_update_image_geometry | 51 | 3 | 48 | 2104 |
| gemini-3.1-flash-lite | guide_scout_tier1_extract_block | 51 | 51 | 0 | 92406 |
| gemini-3.1-flash-lite | guide_occurrence_enrich | 46 | 44 | 2 | 92927 |
| gemma-4-31b | event_topics | 30 | 26 | 4 | 35443 |
| gemini-3.6-flash | smart_update | 30 | 29 | 1 | 72771 |
| gemini-2.5-flash | smart_update | 28 | 26 | 2 | 49909 |
| gemini-3.6-flash | event_parse | 27 | 24 | 3 | 193409 |

В event_parse есть успехи 3.1 Lite в других ingress/стадиях, но за этот локальный день **нет event_parse rows для 3.5 Lite**. Это не обвинение всех Telegram/Kaggle parsers: маршруты и их telemetry отличаются. Ключевая проверенная проблема — конкретный source-parser path.

### 4.4. Что реально ломается у Lite сегодня

Финальные request errors за локальные сутки:

- 3.1 Lite: 166 HTTP 503 high demand, ещё четыре HTTP 503 deadline expired.
- 3.1 Lite: 32 output token ceiling результата; 22 timeout по 45s, один timeout по 120s; семь cancellations после send и один HTTP 408.
- 3.5 Lite: три output token ceiling, два timeout по 45s, один provider 503.
- Финальных provider-429 rows для этих двух Lite за выбранные сутки нет. Это не исключает прошлых 429 или промежуточных retries: финальный request ledger не равен полному attempts ledger.
- Gemma 31b: 110 failed_provider, включая 43 HTTP 500, 39 timeout по 90s и 22 HTTP 503. «Перевести всё на Gemma» не является доказанным быстрым лечением.

503, RPM/TPM, дневной RPD, schema/truncation и identity/grounding — разные причины; generic TECHNICAL_ERROR скрывает различия.

### 4.5. OpenAI до смены VK route

Дополнительно прочитан application token_usage для bot=announcements, с локального 25 октября. Все GPT-4o rows, tagged и untagged, **не только VK**:

| День | GPT-4o вызовы / tokens | GPT-4o-mini вызовы / tokens |
|---|---|---|
| 25 октября | 99 / 754138 | 109 / 3170821 |
| 26 октября | 111 / 877695 | 128 / 3757274 |
| 27 октября | 130 / 876376 | 130 / 3944370 |
| 28 октября | 79 / 459398 | 74 / 1913038 |

После 28 октября в этой выборке GPT/4o rows нет. Это отсутствие записей, не проверенный нулевой invoice. Старый incident считал untagged parse calls отдельно, поэтому его 79/93 за 25/26 октября не должны совпадать с totals 99/111.

[История VK 4o spend](incidents/INC-2026-10-27-vk-import-4o-spend.md): RunCoveer route изменён с 4o на Lite 28 октября 16:58 UTC. Эта дата важна для деградации, но сама по себе не доказывает все причины.

## 5. Подтверждённая причинная цепочка

### 5.1. Двойной transform теряет исходный model string

В реальном контейнере pure-function reproduction, без генерации:

~~~python
configured = "gemini-3.1-flash-lite"
first = monitoring_text_primary(configured)
# first == "gemini-3.6-flash"

monitoring_text_model_chain(first)
# ["gemini-3.6-flash", "gemini-2.5-flash"]

monitoring_text_model_chain(configured)
# ["gemini-3.6-flash", "gemini-2.5-flash",
#  "gemini-3.1-flash-lite", "gemini-3.5-flash-lite"]
~~~

В [main.py:10403](https://github.com/onedayonemasterpiece/events-bot-new/blob/f9b50fbb9066e1f85d96d56529a1990256bf2844/main.py#L10397) исходная модель заменяется первой emergency model. Внутренний _generate_once [на main.py:10318](https://github.com/onedayonemasterpiece/events-bot-new/blob/f9b50fbb9066e1f85d96d56529a1990256bf2844/main.py#L10315) повторно строит chain уже из замены. Исходный Lite больше неоткуда восстановить.

~~~text
VK_AUTO_IMPORT_PARSE_GEMMA_MODEL=gemini-3.1-flash-lite
EVENT_PARSE_GEMMA_MODEL=gemma-4-31b-it
MONITORING_TEXT_RESERVE_MODELS=gemini-3.6-flash,gemini-2.5-flash
TG_MONITORING_TEXT_MODEL=gemini-3.1-flash-lite
TG_MONITORING_TEXT_MODEL_FALLBACK=gemini-3.5-flash-lite
EVENT_PARSE_ENABLE_4O_FALLBACK=0
~~~

Терминальный adjudicator по умолчанию запрашивает 3.5 Lite, но проходит тот же transform. «Ремонтный» semantic path поэтому может снова прийти в исчерпанный Flash pool, а не стать независимым спасением.

[Shared chain builder](https://github.com/onedayonemasterpiece/events-bot-new/blob/f9b50fbb9066e1f85d96d56529a1990256bf2844/google_ai/client.py#L68).
Новый router не нужен: потеряна информация в уже существующем пути.

### 5.2. Local limiter denial — не превышение квоты Google

В файловых логах с UTC 00:00 до около 19:59 2 ноября: 307 primary-provider/schema technical failure tracebacks. 302 заканчиваются RateLimitError:rpd, пять — provider_429. RPD-denial возникает при reserve_result.ok=false **до provider send**.

Эти 302 ошибки показывают защиту исчерпанного выбранного budget, а не 302 нарушения внешней квоты. Отдельные настоящие Google 429 с quota text в logs есть. В local-day request ledger одна финальная 3.6 Flash 429; counts отличаются по единице и временным границам. Причина каждого расхождения internal/Google counter ещё не установлена. Возможны иной traffic в том же проекте, фактический limit, особенности учета попыток. Утверждать массовый bypass atomic limiter нельзя.

В работающем client уже есть quota_admission_blocks: pre-send RPD/RPM/TPM denial не расходует cap физических sends. Это [PR #712](https://github.com/onedayonemasterpiece/events-bot-new/pull/712), commit 4172be897, уже в проде. Не предлагать эту правку заново.

### 5.3. Technical terminal сохраняет долг даже после восстановления провайдера

[vk_auto_queue._terminalize_vk_auto_failure](https://github.com/onedayonemasterpiece/events-bot-new/blob/f9b50fbb9066e1f85d96d56529a1990256bf2844/vk_auto_queue.py#L1029) записывает FAILED_TECHNICAL без durable retry. Raw packet остаётся, но carrier уже не selectable автоматически.

Это **явное решение владельца от 15 августа**, зафиксированное в [каноническом VK contract](../features/vk-auto-queue/README.md). В октябре unsolicited identity retry consumer тоже был снят [owner-directed rollback](https://github.com/onedayonemasterpiece/events-bot-new/commit/bd8a1ee97). Возврат workers — изменение политики, не обычная починка.

При этом в vk_review.schedule_retry осталась документация о бесконечно retryable technical errors. Наличие функции не доказывает, что она участвует в текущем auto-import path. Перед её удалением/переделкой нужны callers и tests; не следует «чинить» legacy слой вместо реального пути.

~~~mermaid
flowchart LR
  A["Source fetched"] --> B["Raw packet committed"]
  B --> C["Admission / inbox"]
  C --> D["Source parse"]
  D --> E["Two exhausted emergency Flash lanes"]
  E --> F["Limiter refuses RPD"]
  F --> G["FAILED_TECHNICAL"]
  G --> H["No automatic re-drive"]
  D --> I["When successful: Smart Update"]
  I --> J["Public projection / media gate"]
~~~

### 5.4. Масштаб ухудшения

| Локальный день | Runs | Processed | Imported | Technical | Rejected | New events |
|---|---|---|---|---|---|---|
| 25 октября | 5 | 60 | 7 | 14 | 39 | 4 |
| 26 октября | 5 | 60 | 14 | 17 | 29 | 10 |
| 27 октября | 5 | 60 | 5 | 19 | 36 | 5 |
| 28 октября | 10 | 120 | 8 | 87 | 25 | 10 |
| 29 октября | 12 | 132 | 2 | 117 | 13 | 2 |
| 30 октября | 14 | 156 | 4 | 138 | 14 | 2 |
| 31 октября | 14 | 168 | 5 | 150 | 13 | 4 |
| 1 ноября | 11 | 132 | 5 | 104 | 23 | 2 |
| 2 ноября, неполный | 11 | 132 | 6 | 106 | 20 | 2 |

25–27 октября technical 50/180 = **27,8%**; 29–31 октября 405/456 = **88,8%**, +61,0 п.п. Это сопоставимые трёхдневные интервалы, но число/состав обработанных позиций тоже изменились. Не чистая causal оценка config change. Processed позиции не равны уникальным событиям; один carrier может создавать несколько events.

Runs стало больше, но продуктивный outcome не восстановился. Дополнительные scheduled attempts без исправления route не заменяют работающую модель.

Сегодня все ingress дали 15 новых DB events: 13 около 00:53–01:19, два — 10:24 и 12:05 Калининграда. VK сохранил 226 packets от 82 источников, последний около 21:33. Source collection не выключен.

Site parse: последний проход 261 кандидат, 104 failures, 154 unchanged, 3 updated, 0 new. Предыдущие 259/102 failures могут перекрывать те же candidates, складывать их как unique losses нельзя.

Telegram scan ledger за local day: 13 terminal_error, шесть partial_terminal_error; успешные/частичные строки дали 33 imported occurrences. Это не 33 новых DB events.

### 5.5. Backlog и различие «кандидат» / «доказанная потеря»

Всего в vk_inbox: 1 973 failed_technical и 3 892 pending — исторические состояния, не столько актуальных валидных событий. Среди созданных за local day 2 ноября 48 rows уже failed_technical.

Кандидаты для replay, свежие technical terminals с future date hints:

- [Программа недели «Локация»](https://vk.com/wall-214027639_12159), inbox 87001.
- [Агропарк, новогодние программы](https://vk.com/wall-221969169_53108), inbox 86986.
- [Третьяковская галерея, дополнительные билеты](https://vk.com/wall-212760444_6357), inbox 86977.
- [«Онегин», 15 ноября](https://vk.com/wall-100137391_167662), inbox 86966.

Это не доказательство отсутствия связанных Events: возможны повторный источник, update или несколько occurrences. Нужны parse + обычный Smart Update и source→event mapping.

### 5.6. Отдельная проблема публикации

67 будущих vk_sync jobs имеют vk_sync_missing_media: **62 события без eventposter rows**, пять с poster rows, пригодность которых ещё не доказана.

Все сегодняшние 15 новых событий уже имеют vk_sync=done и stored VK URLs. Поэтому нельзя объяснять их малое число именно этими 67 historical blocks. Часть registry rows missing/ambiguous расходится с done/stored URL; authenticated VK readback не был успешно выполнен через подключённый social workspace. Не объявлять посты удалёнными и не повторять публикацию вслепую.

Не отключать media gate ради зелёной очереди: известная афиша не должна превратиться в текстовый пост без продуктового решения.

## 6. Уже существующие механизмы и возможные конфликты

| Механизм | Назначение | Наблюдение / что делать |
|---|---|---|
| Atomic project/model reserve | Quota protection | Сохранить; не лечит потерянную caller model |
| Normal key pool и rotation | Допустимый project/key | Сохранить; alias не доказывает отдельный Cloud project |
| Explicit model/key pools | Legacy model access | Не расширять наугад, Flash pools заданы намеренно |
| Emergency model override | Спасение прежнего Lite outage | Сейчас double transform теряет normal route |
| Lite sibling chain | 3.1 ↔ 3.5 в gateway | Уже реализована, надо сохранить достижимость |
| quota_admission_blocks | Отделить denial от send | Уже исправлено и deployed |
| Bounded wait внутри claim | Краткий Retry-After | При RPD может бесполезно ждать; измерить, не расширять |
| Conditional verifier / adjudicator | Semantic/schema contradictions | Не независимый резерв: тоже попадает в transform |
| Immutable parse receipt / exact replay | Reuse результата без повторной оплаты | Сохранить, использовать в catch-up |
| Crawl continuation retry | Доставить ещё не сохранённые raw pages | Не смешивать с semantic re-drive |
| VK technical terminal | Линейный outcome по решению владельца | Сохранить контракт; explicit catch-up отдельно |
| Старый schedule_retry | Прежний retry-контракт | Проверить reachable callers до изменения |
| Scheduler watchdog / catch-up | Не потерять slot | Уже есть; healthy job registration не равен успешному импорту |
| Media review / publication gate | Подтверждённые картинки | Отдельный backlog |
| Identity retry worker | Был добавлен и снят | Не возвращать без изменения согласованного scope |

Проблема не в числе слов retry в репозитории, а в том, что слои вместе теряют requested model и оставляют устойчивый terminal. Не нужно ни ещё одно спасение поверх старого, ни массовое удаление safety/semantic checks.

## 7. Основные гипотезы

| Гипотеза | Оценка | Что доказывает / что различит |
|---|---|---|
| H1: double chain transform теряет Lite | **Подтверждена** | Pure reproduction активного кода; нужен caller-level regression test |
| H2: все ключи/модели исчерпаны | **Опровергнута** | Lite budget и successful receipts |
| H3: emergency route усиливает source-parse outage | **Сильно подтверждена** | Выбранные pools 20/20, tracebacks; live canary подтвердит эффект исправления |
| H4: 3.5 Lite нет / не работает | **Опровергнута** | Google list + 923 успешных receipts |
| H5: transient 503/timeouts/truncation сохранят неполноту после H1 | **Есть отдельные доказательства** | Ошибки Lite; replay обычных и тяжёлых sources |
| H6: terminal policy не восстанавливает уже пропущенное | **Подтверждена** | Contract, handler и backlog; нужен отдельный catch-up |
| H7: фоновые consumers отбирают время/квоту у критичного ingress | **Возможна, вклад не измерен** | Общий pool и расход media/guide/video; нужен contention analysis, не новая priority queue |
| H8: часть candidates теряется на identity/grounding | **Есть отдельные случаи** | Smart Update failed outcomes; предметный LLM-first replay |
| H9: media gate объясняет два новых events сегодня | **Не подтверждена** | Новый intake и старые blocked projections — разные множества |
| H10: limiter массово допускает превышение RPD | **Не доказана** | Большинство ошибок pre-send; sparse Google 429 требуют exact project/request audit |

Важная история: transform появился с commit 3cba8086de **28 сентября**, не 28 октября. Переход с прямого OpenAI на Google 28 октября, вероятно, проявил существовавший дефект. Совпадение дат не заменяет replay causal проверки.

## 8. Как уменьшить пропуски оперативно

Это **варианты для анализа, не выполненные изменения**.

### 8.1. Восстановить Lite в existing normal path

**Вариант A — сохранить original requested model и строить chain один раз.** Убрать/исправить ранний monitoring_text_primary(model), использовать original model при chain construction. Самый узкий root-level code change; использовать существующий sibling fallback.

Ограничение: emergency Flash останутся первыми. Два реальных неудачных sends на них способны исчерпать per-call cap до Lite. Local quota denials уже не расходуют cap. Нужен осмысленный выбор порядка chain и единственного владельца policy, не ещё один fallback вокруг неё.

**Вариант B — снять существующий emergency override в env.** Пустой MONITORING_TEXT_RESERVE_MODELS возвращает Lite caller к обычной паре без нового флага. Но env общий: сайт-парсер с default Gemma, Smart Update и другие consumers тоже поменяют поведение. Требуется scoped consumer impact и rollback, это не «безопасная кнопка только VK».

**Вариант C — только поменять VK model на 3.5 Lite. Недостаточно:** тот же transform снова заменит его на 3.6 Flash.

3.1-first / 3.5-first выбирать на одинаковом наборе retained sources по typed decision/Smart Update success и latency/budget. Общие rates §4 не доказывают превосходство модели. Обе уже есть — новая routing state machine не нужна.

**Один отличающий regression test:** configured Lite + реальная emergency env → source-parser caller не теряет Lite; при exhausted Flash pre-send reservations достигает доступной разрешённой Lite, соблюдая physical-send cap. Тест caller+builder, а не только helper, который сам по себе и сейчас работает.

### 8.2. Не ждать дневную квоту как краткий Retry-After

Проверить existing bounded wait: RPD без meaningful Retry-After не станет доступным через 1–60 секунд. Использовать existing chain, а при полном отказе сохранить точную quota reason в имеющемся receipt/log без нового state.

Не превращать RPD/provider/schema error в CONFIRMED_NO_EVENT или product exclusion. Измерить до/после carrier duration, число reservations и provider sends. Величина wasted waits сегодня не посчитана; это отдельное улучшение, не замена H1.

### 8.3. После canary — ограниченный explicit catch-up

1. Отобрать точные failed_technical carriers с retained packet и актуальным horizon. Свежесть/близость даты — приоритет, не semantic verdict.
2. У carriers с successful immutable parse receipt переиспользовать receipt; без него — обычный parse.
3. Провести через normal Smart Update: merge/no-op для повторов, не manual insert Event.
4. Зафиксировать run_id, inbox/packet IDs, source→event mapping и все outcomes.
5. Перед повтором публичного эффекта reconcile stored URL/provider receipt/readback. Не переигрывать done jobs вслепую.
6. Отчитаться: packets обработано, new/merged/no-op, product exclusions, unresolved technical. Не обещать восстановить все 1 973 historical terminals.

Использовать existing CLI/import boundary. Если ограниченного re-drive действительно нет, назвать минимальный недостающий кусок. Не подменять это постоянным скрытым worker.

### 8.4. Проверить устойчивость и отделить дальнейшую работу

Нужны ограниченный реальный ingress после fix и следующий штатный batch, actual model receipts и DB/source links. Existing ops_run уже даёт processed/imported/rejected/technical, created/updated/duration — новый monitoring daemon не обязателен.

Negative controls: past/non-event не становится Event; повторный source не создаёт дубль; provider error не маскируется под no-event; known poster не теряется ради быстрой публикации.

Позже, отдельно:

- 67 media-blocked projections: восстановление источника/approval media, не отключение gate.
- Output ceiling: replay тяжёлых inputs; общий рост max_tokens повышает TPM/latency и не универсален.
- Actual quotas в Google AI Studio, other project traffic, key→project mapping.
- Снять просроченный emergency override после нормализации policy, вместо следующего emergency слоя.
- Новые 3.8 Flash / 4 Argon рассматривать только при задаче, не решаемой проверенной Lite-парой.

## 9. Чего мы не доказали

- Точное число уникальных будущих отсутствующих событий.
- Actual Google quotas/billing tier и независимость project scopes на стороне Google.
- Полный token/currency bill всех физических attempts.
- Причину каждого Google 429 при внутренних counters.
- Одинаковое extraction/grounding quality двух Lite.
- Полноту живой VK-стены и причину missing/ambiguous registry.
- Долю failures, которую исправит только H1: transient/provider/Smart Update остаются.
- Reachability каждого legacy retry path.

Не писать «это восстановит 90% событий» из процента exceptions. Маленький различающий replay лучше уверенной недоказанной оценки.

## 10. Код, tests и release constraints

Все ссылки ниже pinned к работающему SHA f9b50fbb9066e1f85d96d56529a1990256bf2844:

- [main.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/f9b50fbb9066e1f85d96d56529a1990256bf2844/main.py): source model, chain, verifier, adjudication.
- [google_ai/client.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/f9b50fbb9066e1f85d96d56529a1990256bf2844/google_ai/client.py): pools/reserve/send cap.
- [vk_auto_queue.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/f9b50fbb9066e1f85d96d56529a1990256bf2844/vk_auto_queue.py): carrier claims/terminal/import.
- [vk_review.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/f9b50fbb9066e1f85d96d56529a1990256bf2844/vk_review.py): receipts и legacy retry primitives.
- [test_google_ai_client.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/f9b50fbb9066e1f85d96d56529a1990256bf2844/tests/test_google_ai_client.py).
- [test_vk_auto_queue_rate_limit.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/f9b50fbb9066e1f85d96d56529a1990256bf2844/tests/test_vk_auto_queue_rate_limit.py).
- [test_vk_auto_queue_gemma4.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/f9b50fbb9066e1f85d96d56529a1990256bf2844/tests/test_vk_auto_queue_gemma4.py).
- [test_source_parse_contract.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/f9b50fbb9066e1f85d96d56529a1990256bf2844/tests/test_source_parse_contract.py).
- [test_smart_update_linear_terminal_contract.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/f9b50fbb9066e1f85d96d56529a1990256bf2844/tests/test_smart_update_linear_terminal_contract.py).
- [RunCoveer runbook](../../deploy/runcoveer/README.md).
- [Release governance](../operations/release-governance.md).

Это указатели на существующие tests, не заявление, что они запускались в read-only задаче. Выполнены live reads и pure reproduction. Перед будущим deploy: clean merged origin/main, running SHA ancestry, Events-only Compose, rollback, readiness и реальный product outcome. Сейчас healthy контейнер сосуществует с 80% VK technical failures.

## Приложение A. Все 60 моделей от Google Models API

У всех шести ключей одинаковый список на capture time. После тире — advertised methods, не результат generation probe.

- **gemini-2.5-flash** — generateContent, countTokens, createCachedContent, batchGenerateContent
- **gemini-2.5-pro** — generateContent, countTokens, createCachedContent, batchGenerateContent
- **gemini-2.5-flash-preview-tts** — countTokens, generateContent
- **gemini-2.5-pro-preview-tts** — countTokens, generateContent, batchGenerateContent
- **gemma-4-26b-a4b-it** — generateContent, countTokens
- **gemma-4-31b-it** — generateContent, countTokens
- **gemini-flash-latest** — generateContent, countTokens, createCachedContent, batchGenerateContent
- **gemini-flash-lite-latest** — generateContent, countTokens, createCachedContent, batchGenerateContent
- **gemini-pro-latest** — generateContent, countTokens, createCachedContent, batchGenerateContent
- **gemini-2.5-flash-lite** — generateContent, countTokens, createCachedContent, batchGenerateContent
- **gemini-2.5-flash-image** — generateContent, countTokens, batchGenerateContent
- **gemini-3-flash-preview** — generateContent, countTokens, createCachedContent, batchGenerateContent
- **gemini-3.1-pro-preview** — generateContent, countTokens, createCachedContent, batchGenerateContent
- **gemini-3.1-pro-preview-customtools** — generateContent, countTokens, createCachedContent, batchGenerateContent
- **gemini-3.1-flash-lite-preview** — generateContent, countTokens, createCachedContent, batchGenerateContent
- **gemini-3.1-flash-lite** — generateContent, countTokens, createCachedContent, batchGenerateContent
- **gemini-3-pro-image-preview** — generateContent, countTokens, batchGenerateContent
- **gemini-3-pro-image** — generateContent, countTokens, batchGenerateContent
- **nano-banana-pro-preview** — generateContent, countTokens, batchGenerateContent
- **gemini-3.1-flash-image-preview** — generateContent, countTokens, batchGenerateContent
- **gemini-3.1-flash-image** — generateContent, countTokens, batchGenerateContent
- **gemini-3.1-flash-lite-image** — generateContent, countTokens, batchGenerateContent
- **gemini-nano-banana-2.1** — generateContent, countTokens, batchGenerateContent
- **gemini-3.5-flash** — generateContent, countTokens, createCachedContent, batchGenerateContent
- **gemini-3.5-flash-lite** — generateContent, countTokens, createCachedContent, batchGenerateContent
- **gemini-omni-flash-preview** — generateContent, countTokens
- **gemini-omni-1.1-flash** — generateContent, countTokens
- **gemini-3.5-transcribe** — generateContent, countTokens
- **gemini-3.6-flash** — generateContent, countTokens, createCachedContent, batchGenerateContent
- **gemini-3.7-flash** — generateContent, countTokens, createCachedContent, batchGenerateContent
- **gemini-3.8-flash** — generateContent, countTokens, createCachedContent, batchGenerateContent
- **gemini-4-argon** — generateContent, countTokens, batchGenerateContent
- **lyria-3-clip-preview** — generateContent, countTokens
- **lyria-3-pro-preview** — generateContent, countTokens
- **lyria-3.5** — generateContent, countTokens
- **gemini-3.1-flash-tts-preview** — generateContent, countTokens, batchGenerateContent
- **gemini-3.8-flash-tts** — generateContent, countTokens, batchGenerateContent
- **gemini-3.8-flash-lite-tts** — generateContent, countTokens, batchGenerateContent
- **gemini-robotics-er-2-preview** — generateContent, countTokens, createCachedContent, batchGenerateContent
- **gemini-2.5-computer-use-preview-10-2025** — generateContent, countTokens
- **antigravity-preview-05-2026** — generateContent, countTokens
- **antigravity-preview-09-2026** — generateContent, countTokens
- **antigravity-preview-latest** — generateContent, countTokens
- **deep-research-max-preview-04-2026** — generateContent, countTokens
- **deep-research-preview-04-2026** — generateContent, countTokens
- **deep-research-pro-preview-12-2025** — generateContent, countTokens
- **gemini-embedding-001** — embedContent, countTextTokens, countTokens, asyncBatchEmbedContent
- **gemini-embedding-2-preview** — embedContent, countTextTokens, countTokens, asyncBatchEmbedContent
- **gemini-embedding-2** — embedContent, countTextTokens, countTokens, asyncBatchEmbedContent
- **aqa** — generateAnswer
- **gemini-3.5-transcribe-live** — bidiGenerateContent
- **gemini-2.5-flash-native-audio-latest** — countTokens, bidiGenerateContent
- **gemini-2.5-flash-native-audio-preview-09-2025** — countTokens, bidiGenerateContent
- **gemini-2.5-flash-native-audio-preview-12-2025** — countTokens, bidiGenerateContent
- **gemini-3.1-flash-live-preview** — bidiGenerateContent
- **gemini-3.8-live** — bidiGenerateContent
- **gemini-3.8-live-extended-thinking** — bidiGenerateContent
- **gemini-robotics-er-2-streaming-preview** — bidiGenerateContent
- **gemini-3.5-live-translate-preview** — bidiGenerateContent
- **lyria-realtime-exp** — bidiGenerateMusic

## Приложение B. Воспроизводимость

- SQLite: read-only URI file:/data/db.sqlite?mode=ro. ops_run kind=vk_auto_import, day=date(started_at,'+2 hours'), метрики из metrics_json. Для всех девяти дней проверено processed=imported+technical+rejected.
- Google catalog: GET /v1beta/models?pageSize=1000, paging, HTTP/completeness и сравнение множеств names.
- Registry: только nonsecret metadata google_ai_api_keys, model_limits и day counters с minute_bucket IS NULL.
- Request ledger: по каждому local day UTC gte/lt boundaries, order created_at, страницы по 1000, fields model/consumer/key/status/usage/error. Private prompts не читались.
- Error counts — финальные request errors; не складывать их с traceback count.
- OpenAI legacy token_usage: bot=announcements, дата at, GPT/4o model labels; это application telemetry, не invoice.
- Retained evidence и аналитические скрипты на devserver: /home/dev/artifacts/events-bot-new/20261102T195715Z-prod-intake-audit-20261102/. Pro не нужен доступ к этому пути: существенные числа/код/контекст включены в файл.
- Credentials в evidence/Markdown не копировались.

## Продолжение: скриншот владельца и проверка исправления

Сессия-продолжение прочитала результат codex://threads/01a19dd4-8b21-7a33-82f2-c7e7ae2bd411 и сохранила предыдущие evidence. Около 20:44 UTC runtime по-прежнему f9b50fbb9066e1f85d96d56529a1990256bf2844; ready=true.

### Что означает 40 / 20

AI Studio явно подписывает таблицу **Peak usage per model compared to its limit over the last 28 days**. Числители ниже — пики соответствующих метрик в этом окне, не единый текущий день и не объём Events Bot. Точный Cloud project скриншота не указан. 40 при текущем limit 20 требует проверки: лимит мог измениться, в том же проекте могли быть сторонние consumers, либо есть проблема учёта/обход. Скриншот не позволяет выбрать одну из этих причин. Не делить 40 на число наших ключей и не обнулять counters.

Свежий readback из canonical registry через production environment: HTTP 200 около 20:47 UTC. Все представленные конечные положительные лимиты совпадают с присланным владельцем снимком: обе Lite 15/250000/500; 2.5 Flash и 3.6 Flash 5/250000/20; 3.8 Flash 5/250000/20; 2.5 Lite 10/250000/20; Gemma 4 30/16000/14400; Embedding 2 100/30000/1000. TTS/transcribe/robotics и Antigravity также совпадают. **Изменение численных лимитов не требуется: они уже актуальны.** Zero/Unlimited не подменены выдуманным конечным бюджетом. Старые alias rows не доказывают дополнительную независимую квоту.

| Модель из AI Studio | Peak RPM / limit | Peak TPM / limit | Peak RPD / limit |
|---|---|---|---|
| Gemini 2.5 Flash | 5 / 5 | 34.79K / 250K | 40 / 20 |
| Gemini 3.1 Flash Lite | 13 / 15 | 58.36K / 250K | 499 / 500 |
| Gemini 3.6 Flash | 3 / 5 | 30.71K / 250K | 20 / 20 |
| Gemini 3.5 Flash Lite | 4 / 15 | 65.94K / 250K | 64 / 500 |
| Gemini 3.8 Flash | 1 / 5 | 83.03K / 250K | 10 / 20 |
| Gemma 4 31B | 4 / 30 | 5.2K / 16K | 126 / 14.4K |
| Gemini Embedding 2 | 4 / 100 | 452 / 30K | 80 / 1K |
| Antigravity | 0 / 60 | 0 / 100K | 0 / 100 |
| Deep Research Pro Preview | 0 / 0 | 0 / 0 | 0 / 0 |
| Gemini 2 Flash | 0 / 0 | 0 / 0 | 0 / 0 |
| Gemini 2 Flash Lite | 0 / 0 | 0 / 0 | 0 / 0 |
| Computer Use Preview | 0 / 0 | 0 / 0 | 0 / 0 |
| Nano Banana (Gemini 2.5 Flash Preview Image) | 0 / 0 | 0 / 0 | 0 / 0 |
| Gemini 2.5 Flash Lite | 0 / 10 | 0 / 250K | 0 / 20 |
| Gemini 2.5 Flash TTS | 0 / 3 | 0 / 10K | 0 / 10 |
| Gemini 2.5 Pro | 0 / 0 | 0 / 0 | 0 / 0 |
| Gemini 2.5 Pro TTS | 0 / 0 | 0 / 0 | 0 / 0 |
| Gemini 3 Flash | 0 / 5 | 0 / 250K | 0 / 20 |
| Nano Banana Pro (Gemini 3 Pro Image) | 0 / 0 | 0 / 0 | 0 / 0 |
| Gemini 3.1 Pro | 0 / 0 | 0 / 0 | 0 / 0 |
| Nano Banana 2 (Gemini 3.1 Flash Image) | 0 / 0 | 0 / 0 | 0 / 0 |
| Nano Banana 2 Lite (Gemini 3.1 Flash Lite Image) | 0 / 0 | 0 / 0 | 0 / 0 |
| Gemini 3.1 Flash TTS | 0 / 3 | 0 / 10K | 0 / 10 |
| Gemini 3.5 Transcribe | 0 / 3 | 0 / 10K | 0 / 25 |
| Gemini 3.8 Flash Lite TTS | 0 / 3 | 0 / 10K | 0 / 10 |
| Gemini 3.8 Flash TTS | 0 / 3 | 0 / 10K | 0 / 10 |
| Gemini 4 Argon | 0 / 0 | 0 / 0 | 0 / 0 |
| Gemini Embedding 1 | 0 / 100 | 0 / 30K | 0 / 1K |
| Gemini Nano Banana 2.1 | 0 / 0 | 0 / 0 | 0 / 0 |
| Gemini Omni 1.1 Flash | 0 / 0 | 0 / 0 | 0 / 0 |
| Gemini Robotics ER 2 Preview | 0 / 5 | 0 / 250K | 0 / 20 |
| Gemma 4 26B | 0 / 30 | 0 / 16K | 0 / 14.4K |
| Lyria 3 Clip | 0 / 0 | 0 / 0 | 0 / 0 |
| Lyria 3 Pro | 0 / 0 | 0 / 0 | 0 / 0 |
| Veo 3 Fast Generate | 0 / 0 | - | 0 / 0 |
| Veo 3 Generate | 0 / 0 | - | 0 / 0 |
| Veo 3 Lite Generate | 0 / 0 | - | 0 / 0 |
| Gemini 2.5 Flash Native Audio Dialog | 0 / Unlimited | 0 / 1M | 0 / Unlimited |
| Gemini 3 Flash Live | 0 / Unlimited | 0 / 65K | 0 / Unlimited |
| Gemini 3.5 Live Translate | 0 / Unlimited | 0 / 20K | 0 / Unlimited |
| Gemini 3.5 Transcribe Live | 2 / Unlimited | 997 / 20K | 16 / Unlimited |
| Gemini 3.8 Live | 3 / Unlimited | 12.14K / 65K | 42 / Unlimited |
| Gemini 3.8 Live Extended Thinking | 0 / Unlimited | 0 / 65K | 0 / Unlimited |

### Узкая правка normal path

В main._parse_event_via_gemma цепочка строится один раз из исходной caller model; первый элемент становится effective model, остальные передаются existing GoogleAIClient как fallback_models. Не меняются порядок emergency models и предел двух физических sends. Преждевременный monitoring_text_primary и повторный transform удалены. Net production diff: +6/−5 строк. Новых состояний, флагов, retries, очередей, workers и repair paths нет. Это механическая routing-ошибка; prompts и event semantics не менялись.

До правки caller-level test падает на всех трёх configured models (Gemma, 3.1 Lite, 3.5 Lite); после правки parser/client/quota suite: 110 passed. Существующий client test подтверждает: RPD denials обеих Flash не тратят physical-send cap и достигают Lite. Офлайн provider path audit: unapproved=0, allowlisted_debt=0. Это source verification, ещё не доказательство восстановления импорта в production.

Риск сохраняется: две настоящие неудачные отправки на emergency Flash могут исчерпать cap до Lite. Изменять общий emergency order без необходимости в этом патче не требуется. После доставки нужны реальные retained-source controls и bounded same-day catch-up через обычный Smart Update; не новый background retry consumer. Инцидент остаётся открытым до live-проверки.

Для Pro: оцени также этот конкретный diff и остаточный риск порядка моделей. Не предлагай заново уже работающий quota_admission_blocks и не приравнивай healthy scheduler к успешному ingestion.

## Короткий prompt для Pro

> Разбери этот production incident и предложи минимальное исправление без переделки системы. Проверь double model-chain transform и различия между local limiter denial, provider 429 и provider 503. Учитывай обе Lite, physical-send budget, project pools и owner-directed отказ от background identity retries. Сравни code fix с удалением emergency env; выбери узкий вариант, один regression test, safe live canary и bounded catch-up. Не добавляй очереди/watchdogs/repair-workers без доказанной необходимости. Не выдавай failed carriers за количество потерянных событий. Назови неподтверждённые допущения и только действительно необходимые следующие проверки.


---

<a id="critical-analysis-20261103"></a>

## Критический анализ: регулярный импорт без новых слоёв — 3 ноября 2026

**Автор: ChatGPT, по прямому поручению владельца. Статус: анализ исходников и предложений; этот раздел не является отчётом о восстановлении production.**

Прочитаны исходный пакет на `da5dcbbf5440db8add5738f33453cff462599336`, последующее дополнение и код актуального на момент проверки `main` — `cdd56170713448258d81d4e05c9a2d5d3f85a70a`. [PR #761](https://github.com/onedayonemasterpiece/events-bot-new/pull/761) уже merged 2 ноября в 22:56:35 Europe/Kaliningrad. Поэтому исходное предложение исправить double transform в source parser **уже реализовано в проверенном коде**; предлагать его как ещё не сделанную работу нельзя.

Числа production ниже относятся к снимкам 2 ноября из этого отчёта. В рамках настоящего анализа первичные runtime-журналы, SQLite и общий Google ledger заново не выгружались; исполняемый SHA после merge не проверялся. Прочитанные тесты и результаты автора PR отделены от собственных проверок: новых model calls, replay, нагрузочных прогонов или изменений production здесь не выполнялось. Исторические описания Fly/default limits в документации не принимаются за текущую конфигурацию RunCoveer.

### 1. Главный вывод и предлагаемое решение

**Проблема шире выбора модели, но решается в существующей конструкции.** Проверенный parser-fix устраняет конкретную потерю маршрута. Для надёжного регулярного импорта остаются три обязанности, которые сейчас смешиваются:

1. Выбрать пригодную модель в пределах разрешённых попыток и сохранить исходный выбор конкретной стадии.
2. Довести уже принятый источник до содержательного результата, сохранив успешные части при частичном отказе.
3. Восстановить действительно пропущенный запуск по расписанию, не превращая это восстановление в повторную обработку следующих источников при той же аварии.

В проекте уже есть владельцы этих обязанностей: shared Google client, существующий import/Smart Update и существующий scheduler. Нужны исправление их композиции и удаление лишних повторов. Оснований создавать ещё одну очередь, общий router, контроллер восстановления, постоянного проверяющего агента или дополнительный ledger не найдено.

**Предпочтительный путь:** сохранить PR761; закончить то же исправление потери исходной модели в Smart Update; вернуть обычной Lite-паре первые полезные попытки после проверки затронутых consumers; убрать короткое ожидание явного дневного quota-denial; исправить трактовку полностью завершённого неудачного VK batch существующим watchdog. После проверки нормального source→event пути выполнить адресный ограниченный catch-up. Судьбу будущих временных технических отказов явно согласовать с продуктовым контрактом: разовый catch-up не обеспечивает постоянную автоматическую восстанавливаемость.

### 2. Что исходный отчёт доказывает, а чего из него не следует

| Утверждение | Критическая оценка |
|---|---|
| Исходная Lite терялась при двойном преобразовании | Причинный механизм подтверждён сохранённым reproduction и diff PR761. В текущем source parser он исправлен. |
| 106 из 132 позиций завершились технически | Арифметика верна: 80,3%. Это исходы обработанных позиций за неполные сутки, не количество уникальных потерянных событий. |
| 302 из 307 tracebacks заканчиваются RPD | Это сильное свидетельство неудачно выбранного маршрута. Делить эти tracebacks на carriers или приписывать столько же потерянных событий нельзя: отличаются единицы, стадии и окна наблюдения. |
| Возврат Lite полностью восстановит импорт | Не доказано. Даже 25–27 октября technical rate составлял 50/180 = 27,8%; причины этого предшествующего уровня не разобраны. Текущая доля каждого остаточного механизма неизвестна. |
| 3.5 Lite лучше 3.1 Lite | Общие rates этого не доказывают. Из 85 сегодняшних 3.5-запросов 76 относятся к `kaggle`; сравнение с другим набором consumers и ролью primary не является A/B для VK extraction. |
| 226 raw packets против 132 processed означает отставание на 94 события | Неверная единица сравнения: между raw collection и inbox есть admission; revision, carrier, попытка и событие имеют разную кратность. |
| Healthy scheduler и уменьшение pending означают исправление | Нет: pending может просто переходить в `failed_technical`. Требуются содержательные исходы и возраст действительно ожидающей работы. |
| 67 media-blocked jobs объясняют малое число новых событий | По представленным данным это разные множества. Media-проблему нужно сохранять отдельной, без отключения её gate. |

Оговорки исходного отчёта о квотах и оплате правильны: unused registered budget не гарантирует доступность провайдера, request row не равна физической отправке, null usage не равен нулевому расходу. [Официальная документация Google](https://ai.google.dev/gemini-api/docs/rate-limits), повторно прочитанная при этом анализе, подтверждает project-level quotas, Pacific reset RPD и отсутствие гарантии advertised capacity. Число ключей не следует превращать в множитель мощности без проверки project identities.

### 3. Новая существенная находка: тот же дефект остаётся в Smart Update

**Уровень доказательства: механизм в текущем коде; частота в production не измерена.**

В [smart_event_update.py:299–346](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/smart_event_update.py#L299-L346) `_resolve_smart_update_model(label)` сначала вычисляет исходную модель стадии, затем возвращает только `monitoring_text_primary(...)`. `_smart_update_fallback_models(label, model)` получает уже заменённую модель и строит chain повторно.

При включённом emergency override это приводит к двум потерям сразу:

- вместо исходной stage Lite в builder приходит effective Flash;
- проверка `model == SMART_UPDATE_FACTS_MODEL` может перестать выполняться, поэтому facts-specific fallback заменяется глобальным `GOOGLE_AI_FALLBACK_MODELS`.

Так составлены реальные callers: [JSON/native-schema](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/smart_event_update.py#L9681-L9803), [обычный JSON](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/smart_event_update.py#L9838-L9847), [text](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/smart_event_update.py#L10101-L10147) и [direct native JSON](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/smart_event_update.py#L10476-L10517). Успешный parser может передать результат на следующую стадию с той же ошибкой выбора маршрута.

**Сильное ограничение вывода:** глобальный fallback может содержать Lite и случайно возвращать их в цепочку. Актуальный runtime env этой стадии здесь не установлен. Поэтому нельзя утверждать, что весь Smart Update сейчас лишён Lite или что этим объясняются все его ошибки. Доказана потеря исходной stage policy и зависимость результата от постороннего глобального хвоста.

**Минимальное исправление:** у существующего caller один раз получить исходную модель стадии и её fallback; передать их существующему `monitoring_text_model_chain`; из одной полученной цепочки взять primary и tail. Удалить повторную интерпретацию уже выбранной primary. Сохранить специальный контракт `single_primary_send`: отключённый fallback и cap=1 не превращать в дополнительные попытки. Защитная обязанность stage selection остаётся, удаляется только потеря информации между двумя helpers.

[Существующие helper tests](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/tests/test_smart_update_provider_fallback_safety.py#L35-L52) передают в fallback helper исходную модель; production caller передаёт преобразованную. Нужен один различающий caller-level сценарий: facts Lite + emergency env + пустой глобальный fallback сохраняют исходную Lite, sibling и stage tail. Отдельная проверка новой библиотеки маршрутизации не нужна.

### 4. Модель в списке ещё не означает доступную попытку

PR761 сохраняет цепочку `3.6 Flash → 2.5 Flash → configured Lite → sibling Lite`, но [cap остаётся равным двум физическим отправкам на один вызов генерации](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/main.py#L10315-L10326).

| Ситуация | Фактическое следствие |
|---|---|
| Обе Flash отвергнуты локальным limiter до send | Уже существующий `quota_admission_blocks` позволяет дойти до Lite. Этот механизм исправлять повторно не нужно. |
| Обе Flash получили реальные sends и не дали результата | Двух попыток уже нет; Lite остаётся недостижимой, хотя есть в chain. |
| Реальный provider 429 вызвал разрешённую rotation project | Rotation тоже расходует физическую отправку; два sends могут уйти до следующей модели. |
| Первыми стоят обычная Lite и её sibling | Обе попытки доступны штатному маршруту, но общей недоступности Google это не устраняет. |

Источники: [admission и cap](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/google_ai/client.py#L1634-L1723), [provider rotation](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/google_ai/client.py#L1775-L1832). В source parser [same-model и скрытые SDK retries отключены](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/main.py#L10016-L10052): нельзя объяснять этот путь обычным автоматическим двойным retry первой Flash.

**Рекомендация по порядку:** сохранить `3.1 Lite → 3.5 Lite` как исходный intended normal path. Свидетельств для срочной смены на 3.5-first недостаточно. Не увеличивать cap до четырёх только ради прохождения двух аварийных моделей перед штатными: это компенсирует неверный приоритет дополнительными sends и задержкой.

Снятие `MONITORING_TEXT_RESERVE_MODELS` убирает постоянный аварийный приоритет без нового флага. Но это **общее изменение конфигурации, связанное также с бюджетом попыток**, а не отдельная кнопка VK:

| Consumer | Что нужно проверить перед снятием override |
|---|---|
| Source parser, включая сайты | VK вернётся к configured Lite; сайт с default Gemma вернётся к Gemma. У последней в отчёте есть собственные provider failures. |
| Smart Update | Помимо выбора facts/writer/other stage и fallback проверить cap: `monitoring_attempt_cap()` после очистки env возвращает `None`. Прежний предел двух sends автоматически не сохраняется; single-send opt-out должен остаться cap=1. |
| Topics | Chain строится из оригинала правильно, но явные fallback/cap=2 передаются только при включённом override. После очистки действуют обычные defaults клиента; их бюджет требуется проверить. [Код](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/main.py#L11905-L11921). |
| Telegram public-copy writer | Уже использует обычную Lite-пару отдельно от monitoring reserves. Не переносить на него изменение по одному только сходству слова Telegram. [Код](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/main_part2.py#L6412-L6443). |

**Существенная связность настройки:** [monitoring_attempt_cap](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/google_ai/client.py#L86-L87) возвращает `None` без emergency env, а [client трактует `None` как отсутствие этого physical-send cap](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/google_ai/client.py#L1638-L1642). Поэтому конфигурационное снятие override нельзя назвать нейтральным по расходу. В затронутых callers нужно сохранить принятый конечный бюджет через уже существующие аргументы, отдельно от выбора порядка моделей; нового budget-controller или флага для этого не требуется. У parser собственный `min(2, len(model_chain))` сохраняется независимо от env.

У аварийного маршрута была реальная защитная функция: [сентябрьский инцидент](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/docs/reports/incidents/INC-2026-09-28-lite-fallback-overload.md) описывает недоступность Lite. Поэтому предпочтение снимается после короткой проверки normal path затронутых consumers. Если конкретная стадия всё ещё требует Flash, это должно выражаться её существующим выбором модели, а не неявной заменой всех consumers. Не нужны динамический рейтинг моделей, автоматический circuit breaker или новая таблица маршрутов.

### 5. Дневную квоту код ошибочно превращает в короткое ожидание

**Уровень доказательства: механизм в исходниках подтверждён; потерянное wall time не измерено.**

Для RPD [лимитер возвращает `retry_after_ms=NULL`](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/supabase/migrations/20260803074500_google_ai_rolling_windows_pacific_rpd.sql#L248-L250). [Client передаёт причину и значение в RateLimitError](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/google_ai/client.py#L2098-L2111). Внешний [bounded-wait wrapper](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/main.py#L10860-L10927) принимает любой RateLimitError за повод ждать, при отсутствии Retry-After подставляет одну секунду и повторяет весь primary. [VK по умолчанию предоставляет этому ожиданию 60 секунд](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/vk_auto_queue.py#L101-L109).

Если допустимые модели уже отказали по дневному бюджету, короткое повторение не делает квоту доступной. Счётчик wrapper ограничивает сумму sleeps, а не всю длительность повторных вызовов; называть его точным полным wall-clock budget также нельзя.

**Минимальное удаление:** явный RPD без известного скорого reset не должен запускать короткое sleep/reinvoke. Existing chain сначала исчерпывает допустимые альтернативы; после этого сохраняется честный technical outcome. Для RPM/TPM с осмысленным коротким Retry-After полезное ожидание сохраняется. Не нужны новый cooldown ledger или таймер до следующего quota day в открытом carrier claim.

Различающий пример: RPD без Retry-After не вызывает sleep и повтор primary; TPM с кратким Retry-After по-прежнему может закончиться успехом. Уже существующий [TPM test](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/tests/test_event_parse_llm_first_contract.py#L514-L555) защищает вторую половину. Умножать все 106 технических исходов на 60 секунд и объявлять полученное число измеренной потерей нельзя.

### 6. Новая существенная находка: watchdog может усугублять отказ

**Уровень доказательства: связный механизм в коде; происхождение конкретных дополнительных runs 2 ноября не установлено.**

[VK importer](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/vk_auto_queue.py#L2159-L2198) записывает `failed`, если весь пакет неудачен, и `partial` при смеси технических и иных завершённых исходов, когда нет cancel/незавершённого остатка. Scheduler считает доставленными [только `running/success/partial/empty`](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/scheduling.py#L1305-L1334). Его существующий [15-минутный hold](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/scheduling.py#L2832-L2862) знает `error/crashed/skipped`, но не `failed`.

Конкретный контрпример для следующей проверки:

- Scheduled batch обработал 12 строк: `status=failed`, `inbox_processed=12`, `terminal_rows=12`, `unresolved_rows=0`.
- После grace, в допустимом catch-up окне [watchdog не находит delivery или retry hold и запускает importer снова](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/scheduling.py#L2865-L2935).
- Первые 12 уже находятся в `failed_technical`; [selector берёт следующие pending](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/vk_review.py#L486-L521). При прежнем outage они также могут стать terminal.
- Если в исходном пакете было 11 technical и одно корректное содержательное отклонение, статус был бы `partial`, и этот же watchdog остановился бы.

Это расхождение между **запуск состоялся** и **результат успешен**. Нельзя лечить его увеличением числа catch-up slots или выдавать такую обработку следующих строк за восстановление старых ошибок.

**Минимальное упрощение:** в VK-specific predicate существующего watchdog признать полностью законченный batch доставкой scheduled slot по уже записанному terminal balance. Для рассматриваемого случая доказательство завершения — terminal, не canceled, `inbox_processed > 0`, `terminal_rows == inbox_processed`, `unresolved_rows == 0`; отсутствующие исторические metrics не дают такого доказательства через подстановку нулей. При этом оставить честный `failed` для продуктовых метрик и ошибок. Удаляется скрытый повтор пакета после завершённой попытки; сохраняется восстановление настоящего пропуска/crash/незакрытых строк. Не добавлять `failed` безусловно в общий `ok_statuses`: это затронет других consumers и скроет незавершённые batches.

Один различающий тест должен сопоставить полностью завершённый failed batch и реальный crash с незавершённой работой: первый не вызывает новый claim следующих источников, второй сохраняет существующий catch-up. Нынешний [тест pacing](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/tests/test_scheduling.py#L2354-L2398) создаёт `status="error"` и этот случай не покрывает. Добавление только `failed` в 15-минутный hold лишь замедлит ошибочное повторение; оно не устраняет смешение обязанностей.

### 7. Продуктовый выбор: что означает «надёжно» после временного отказа

Текущий [контракт VK](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/docs/features/vk-auto-queue/README.md#L77-L119) намеренно завершает выбранный carrier как `FAILED_TECHNICAL` и разрешает только явное re-drive. [Запись outcome очищает due/lease](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/vk_review.py#L1118-L1172). [Повторное получение неизменной revision сохраняет этот terminal](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/vk_intake.py#L5399-L5421); обычный следующий crawl его не возвращает.

Таким образом, raw-first обеспечивает сохранность источника, но сам по себе не обеспечивает его будущий импорт. Конечный набор inline-попыток иногда будет исчерпан даже при хорошем маршруте. Нельзя одновременно сохранить окончательный technical terminal после такого отказа и обещать автоматическое восстановление каждой актуальной записи после временного сбоя.

| Возможный контракт | Польза и цена |
|---|---|
| Сохранить действующий terminal + явный ограниченный catch-up | Максимально узкая текущая правка, соблюдение прежнего решения владельца. Некоторые временные ошибки остаются долгом до отдельного восстановления. |
| Разрешить конечное автоматическое продолжение временного технического отказа существующим importer | Уменьшает потребность в регулярном ручном восстановлении. Требует явного изменения контракта, конечного ресурса/горизонта и сохранения уже принятых результатов. |

**Рекомендация для постоянного режима:** если цель — регулярная работа без оператора после кратковременных provider failures, выбрать второй контракт для ограниченного класса временных транспортных/квотных отказов. Использовать существующие inbox, due, claims и расписание; не возвращать отдельный identity retry consumer и бесконечный semantic retry. Детерминированная ошибка схемы/grounding или подтверждённое отсутствие события не становятся автоматически retryable лишь потому, что попали в техническую категорию.

Этот выбор не реализован и не объявляется уже разрешённым прежней политикой. Значения горизонта и общего бюджета нельзя назначать произвольными «тремя попытками» без требования к свежести и наблюдений. При сохранении первого контракта нужно честно измерять технический долг, даже если batch формально полностью terminal. Разовый catch-up остаётся устранением накопленного ущерба, а не постоянной моделью надёжности.

### 8. Адресное восстановление: сохранить полезное и не выдать старый CLI за готовое решение

В §8.3 исходного отчёта правильное направление, но готовность конкретного entrypoint требует уточнения. [Существующий `recover_smart_update_identity_losses.py`](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/scripts/ops/recover_smart_update_identity_losses.py#L678-L765) выбирает только `failed` и due `deferred`; guard записи повторяет эти статусы. Текущие `failed_technical` он **не выбирает**. Обычный `/vk_auto_import --limit=N` также не является адресным восстановлением этих terminal rows.

Минимальный план одного catch-up после подтверждения normal path:

1. Зафиксировать конечное окно инцидента, точные inbox/packet revisions и ресурсный бюджет. Future date hint влияет на порядок, но его отсутствие не исключает source: иначе за пределами восстановления останутся photo-only и неизвестные даты. Это соответствует [действующему priority contract](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/docs/features/vk-auto-queue/README.md#L199-L210).
2. Использовать существующий import boundary и актуальный claim. Selector должен явно понимать `failed_technical`; уже изменившиеся, активные или содержательно закрытые записи не перезаписываются старым снимком. Если existing operator entrypoint не умеет этого, нужна его узкая доработка, а не новый repair service. Готовность такой команды этим анализом не подтверждена.
3. Переиспользовать [успешный immutable parse receipt](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/vk_review.py#L617-L694) по exact packet и той же `prompt_version`, как проверяет текущий loader. Смена requested/provider model уже не делает receipt непригодным. Не менять prompt version и не стирать receipt только ради повторного вызова модели.
4. Оставить обычный Smart Update и source→event mapping. [Успешные children связываются до финального technical outcome carrier](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/vk_auto_queue.py#L3130-L3184). Следовательно, technical carrier может уже содержать созданные события: весь пакет нельзя объявлять «не применённым» и вставлять заново.
5. Для публичного эффекта сначала разрешить неопределённость по существующим IDs/receipts/readback; не повторять уже `done` публикации вслепую. Media backlog восстанавливается отдельно с сохранением его проверки.
6. В результате показать exact processed revisions, new/merged/exact-noop, подтверждённые exclusions и оставшиеся technical. Сравнить их с исходным выбранным набором, а не со всеми 1 973 историческими terminal rows.

Исторический [сентябрьский exact-ID replay](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/docs/reports/incidents/INC-2026-09-28-lite-fallback-overload.md) полезен как существующий операционный образец. Он не доказывает совместимость нынешнего legacy selector с новым статусом.

### 9. Как измерять регулярность без нового мониторинга

Существующих `ops_run`, inbox, packet/attempt receipts и source→event mappings достаточно для первого сопоставимого среза. Нужны следующие показатели из одного окна и одной cohort принятой работы:

| Показатель | Что он разрешает утверждать |
|---|---|
| Новые admitted carrier revisions | Реальный приток в дорогой импорт; raw packets учитываются отдельно. |
| Уникальные содержательно завершённые revisions | Полностью разрешённые revisions: imported/merged/exact-noop и подтверждённые product exclusions. Частично успешные carriers и их accepted children показываются отдельно; вся revision не объявляется завершённой. |
| Возраст свежих неразобранных и oldest due pending | Получает ли новый актуальный источник результат вовремя, не маскируется ли голодание истории. |
| Технический долг в выбранной cohort | Pending/незавершённые revisions и failed_technical с неразрешённым остатком, в том числе при уже принятых children. Принятые части учитываются отдельно и не считаются потерянными. |
| Время admitted→содержательный результат и длительность batch | Отличает задержку очереди от model/provider/Smart Update времени. Незавершённые записи не превращаются в нулевую задержку. |
| Actual sends и причина отказа | Physical sends считать по mark_sent/attempt rows общего Google ledger, связанным с логическими вызовами; parser/receipt attempts не заменяют этот счётчик. Отдельно показывать local RPD/RPM/TPM denial, provider 429/503, timeout/truncation и semantic failure. |

Устойчивость требует, чтобы содержательное обслуживание успевало за admitted-притоком и возраст актуальной работы не рос. Здесь нет данных для численного SLA или нового batch size. В существующем selector уже есть fresh/history interleave; создавать ещё одну priority queue преждевременно. Старые документированные лимиты и defaults не заменяют readback actual runtime configuration.

Отдельное ограничение оценки расхода: «две отправки» — предел одной `generate_content_async`, **не всего исходного поста**. [Malformed JSON repair](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/main.py#L10466-L10488), conditional verifier и typed adjudication могут создать другие вызовы. При этом [thrown provider exception/timeout возвращает technical outcome сразу](https://github.com/onedayonemasterpiece/events-bot-new/blob/cdd56170713448258d81d4e05c9a2d5d3f85a70a/main.py#L11009-L11049): не всякая ошибка запускает adjudicator. Эти различия нужно сохранить в анализе, не вводя ещё один общий budget-controller без измеренного основания.

Две Lite — полезный existing fallback, но общий провайдер не даёт доказанной независимости отказов. Из их смешанных rates нельзя вычислить обещание 99,9% доступности или гарантированный процент восстановленных событий.

### 10. Минимальная последовательность работ и различающие проверки

Это предложения для последующей реализации, не выполненные в этой задаче действия. Независимые маленькие правки не должны превращаться в один большой gate, задерживающий полезное исправление.

| Участок | Что изменить/удалить | Минимальное доказательство результата |
|---|---|---|
| Source parser | Сохранить уже merged PR761; повторную реализацию не делать | Существующий caller regression и readback фактической модели при разрешённой проверке source→event. |
| Smart Update routing | Убрать повторное построение policy из effective primary во всех перечисленных callers | Caller с facts Lite и пустым global fallback сохраняет stage chain; single-primary control не получает лишних sends. |
| Normal model order | После проверки consumers снять постоянный emergency preference и сохранить принятые caps через существующие аргументы; 3.1-first оставить исходным выбором | Normal pair получает реальные допустимые sends; source→event outcome проверен; clearing env не снимает утверждённый cap, single-send остаётся single-send. |
| RPD wait | Убрать короткий sleep/reinvoke явного дневного отказа без скорого reset | RPD не вызывает повтор; краткий TPM/RPM wait остаётся работоспособным. |
| VK watchdog | Отделить законченный неудачный batch от отсутствовавшего slot в существующем VK predicate | All-technical, all-terminal batch не забирает следующий carrier; реальный crash сохраняет catch-up. |
| Ограниченный catch-up | Использовать exact revisions, исправный selector и existing import/receipt/mapping | Повтор не создаёт дубль; mixed carrier сохраняет accepted child; unknown-date/photo-only источник не теряется из-за отсутствия hint. |

Для live-подтверждения нужен небольшой разрешённый набор retained sources с заранее проверяемым смыслом: валидное будущее событие, настоящий non-event/прошедший анонс, дубль или update, неоднозначная дата/афиша. Затем нужен **следующий штатный batch**, чтобы отличить разовый ручной успех от работающего расписания. Canary не должен публиковать контент за пределами полномочий; проверка импорта и проверка разрешённого публичного эффекта имеют разные границы.

Сохраняются shared reserve→mark_sent→provider→finalize, project/model quotas, raw-first, semantic grounding, receipt compatibility, Smart Update, ID-based reconciliation и media gate. Не требуется переписывание `main.py`, новая модель, migration API, новая очередь или постоянный оператор. Для предложенных механических исправлений новые persisted states, flags, workers и зависимости не обоснованы. Устаревший `schedule_retry` с формулировкой «forever» нужно разбирать по reachable callers отдельно; его наличие не является разрешением вернуть бесконечный контракт в current importer.

### 11. Критерий результата и статус инцидента

Практический результат для владельца: новые актуальные источники регулярно получают проверяемый содержательный исход; сбой одной записи сохраняет независимые готовые результаты; повтор не создаёт дублей; полная авария провайдера не заставляет watchdog переводить всё новые записи в постоянный technical backlog.

**В этом дополнении исследованы и предложены изменения.** Исходный отчёт сохранён. Подтверждены merged parser-fix и перечисленные оставшиеся механизмы в source `cdd56170713448258d81d4e05c9a2d5d3f85a70a`. Наличие merge не объявляется доказательством его deployment или восстановления импорта. Инцидент `inc_3d23e0008f860b634306a59b` / `INC-2026-11-02-intake-quota-vk-media` не закрывается этим анализом: для closure остаются фактический source→event outcome, ограниченное восстановление выбранного долга и соответствующее release/publication evidence из его существующего контракта.

## Проверенное восстановление и первая проверка прода, 2026-11-03

Это последующие факты выполнения; критический анализ выше сохранён как исходный
review. [PR #763](https://github.com/onedayonemasterpiece/events-bot-new/pull/763)
доставил цепочки моделей Smart Update, независимые от аварийного override границы
отправок, отказ от короткого ожидания суточной квоты и исправление учёта полностью
терминального неуспешного пакета VK. Аварийный Flash-first override отключён.
После этого повтор четырёх постов ещё не восстановил события: доступность модели
не устраняет все последующие отказы.

Канонический журнал identity показал отдельный дефект контракта промпта: модель
уверенно различала концерт и выставку, приводила цитаты и конфликты, но выбирала
`no_candidate_match`, зарезервированный для неопределённости. Шесть строк уточнения
существующих кодов в [PR #764](https://github.com/onedayonemasterpiece/events-bot-new/pull/764)
исправили этот путь без ослабления grounding/identity guards. Две реальные пары
после уточнения дали FINAL_DISTINCT, контроль настоящего дубля — FINAL_MATCH.
Оба PR прошли CI; локальные затронутые наборы дали 284 и 85 passing tests.

RunCoveer фактически работает на `343826c80998e84c6c20adda3d57ddd47c6008dd`,
образ `runcoveer/events:intake-adjudication-20261103`; проверены все 4427 tracked
файлов, readiness, SQLite и scheduler. Kotopogoda не переключалась. Ограниченный
повтор 11554 завершился success: посты 86941 и 86953 импортированы, Events 10342
и 10343 созданы, связи с исходными VK URL проверены. Это два события из двух
постов, не полное восстановление технического долга.

### Первая отложенная проверка, 05:33–05:36 UTC

- Штатный парсер сайтов 11555, начавшийся в 02:30, создал 51 событие из
  dramteatr/muzteatr/sobor/tretyakov/yantarhall; все имеют event_source.
  Это обычный запланированный импорт после релиза, отдельно от ручного recovery.
  Его candidate ledger: 51 CREATED, 2 MERGED и один активный attempt_started.
- VK-слот 04:15 пропущен по heavy_busy: ресурс занят source_parsing.
  Watchdog поставил догоняющий запуск 11557 в 04:47:21, но он пока ждёт тот же
  ресурс и ещё не забрал VK-посты. `running` этого ops_run не означает фактическую
  обработку VK. Результат обычного VK-импорта пока не подтверждён.
- В окне 04:47–05:36: 105 успешных Google-вызовов; Gemma — 42 ответа 500 и
  четыре 503, 3.1 Lite — 19 ответов 503. Ещё по семь обрезанных output_truncated
  ответов у 3.1/3.5 Lite. За это окно нет структурированных quota/block событий
  и provider 429. Это ошибки сервиса и потолка ответа, не доказательство RPD.
- В VK остаются 3898 pending (77 опубликованы за последние 24 часа) и 1970
  failed_technical (41 за последние 24 часа). Это посты, не число упущенных
  событий; исторические и свежие записи нельзя смешивать в оценке восстановления.

Остаточные отдельные случаи: networking child 25812 отвергнут существующим LLM
location_grounding_review (`vk_location_not_in_post`, missing_location), хотя
предварительный extraction заполнил площадку; 86986 остаётся на long-event
anchor, 87005 — на незавершённом typed negative verdict. Эти результаты не
обходились и не объявлены исправленными. Следующая проверка — ещё через три часа,
затем через пять; для обычного Telegram-прогона 21:40 UTC может потребоваться
продление. Инцидент открыт. Подробные release/recovery receipts — в
[канонической записи](incidents/INC-2026-11-02-intake-quota-vk-media.md).


## Live findings, 2026-11-03 08:15 UTC

The official parser run 11555 held the shared resource for 5 hours 19 minutes
across 262 events. It created 89 events but postponed VK claims until 07:49 UTC.
PR #766 is deployed from merged main 14d38eaf: the existing gate now releases
between source events, and parser producers share the existing PARSE_LOCK.
The image passed verification of 4,427 files; 179 import/scheduler tests and
20 OCR tests passed. Production readiness, database and scheduler are healthy.

Normal VK run 11557 then finished with 12 carriers: 5 rejected, 7 technical
failures, and no imports. Primary OCR reported insufficient_quota; its Google
fallback on 3.1 Lite had 19 responses with status 503. The existing OCR fallback
now uses 3.5 Lite after a successful real OCR canary through the shared limiter.

Gemma stages also exhausted their two-send budget on Gemma and 3.1 Lite before
reaching the healthy sibling 3.5 Lite. Reordering the existing fallback list
passed three real decision-only controls: two distinct events and one duplicate.
No event or queue records were changed. This configuration follow-up is separate
from the fairness and OCR release already deployed; call budgets stay unchanged.

Boot completed at 08:15:00 UTC, crossing the 08:15 cron boundary during cutover.
The watchdog waits for the configured 30-minute misfire grace plus 120 seconds,
so this slot becomes eligible around 08:47 UTC.
Its actual import receipts still need verification; health and a running job
are not acceptance. The incident remains open, including provider degradation,
technical backlog and the scheduled VK/TG checks. See the canonical incident
record for confirmed subsequent outcomes.


## Normal VK result and remaining latency, 2026-11-03 09:29 UTC

The existing watchdog automatically processed the missed slot: run 11565 finished
with 12 posts, 1 imported, 8 confirmed no-event and 3 technical failures. The
completed import created Event 10434. A separate programme post verified 12
children but hit the 30-minute row deadline after creating Event 10433 and
merging five existing events. Those six writes have canonical source links but
are absent from final carrier-level metrics, so the two counts must not be mixed.
Two other posts failed identity conflict checks and require source-level review.

Another narrow routing defect was confirmed: topic classification discarded the
configured fallback list and repeated Gemma calls without reaching healthy Lite.
Its fix passes the existing configuration into the existing chain builder, with
the same two-send cap and unchanged prompts. The baseline regression fails; 98
related tests pass after correction. Two real Lite topic controls pass. Image
geometry on Gemma also costs time, but a global model switch would invalidate
model-keyed caches; no such migration or extra recovery worker is added here.
The normal import is active, but residual failures and programme recovery remain
open. See the canonical incident for release and subsequent verification.


## Проверка обычного ВК и доставка резерва тематик, 2026-11-03 11:05 UTC

Обычный плановый запуск 11566 в 10:00 завершился в 10:29:13: 12 постов,
два импортированы, семь подтверждены как не содержащие событий, три завершились
технической ошибкой. По новым связям с исходными постами подтверждены создание
Event 10435 и обновление Event 10251. Исторические связи других постов в этот
результат не включены. ВК работает, но полнота пока не восстановлена.

Реальный ответ 503 выявил второй дефект в классификации тематик: после
восстановления списка резервных моделей клиент всё ещё тратил оба разрешённых
запроса на Gemma. PR #769 задаёт одну попытку на модель существующего клиента,
как уже сделано в Smart Update. После 503 остаётся возможность обратиться к
3.5 Lite в пределах прежнего лимита двух запросов. Промпты, схемы и квоты не
изменены; нового цикла повторов нет. Отличающий регрессионный тест проходит
через настоящую фабрику и общий fallback-клиент: Gemma 503, затем Lite success.
126 связанных тестов и обязательные CI прошли.

Исправления PR #768 и #769 доставлены. Фактический runtime —
`fd4da1b81550d4a9633a22ba173311a95a7138bc`, образ
`intake-topics-retry-20261103`. Версия, настройки моделей и SQLite проверены
явными условиями выполнения; ready=true, БД и scheduler здоровы. Kotopogoda
не изменена. Само здоровье не заменяет проверку следующего обычного импорта.

При переключении были прерваны два прохода партнёрского выбора до создания
видео-сессии, после проверки существующего restart/watchdog-контракта. Импорт
источников и удалённый рендер не прерывались. Штатный watchdog автоматически
запустил новый проход 11571 в 11:02:30; свежие heartbeat и provider calls
подтверждают работу. Это подтверждает возобновление выбора, а не готовность
видео. Gemma в этом отдельном consumer продолжает отвечать 500/503; переход
на 3.5 Lite зафиксирован.

Важное исправление доказательств: production использует PYTHONOPTIMIZE=1,
поэтому assert внутри контейнера не выполняется. Ранняя диагностическая
проверка кандидата и read-only план восстановления не были действующей
проверкой условий. Они не доказывают доставку; старый файл даже содержит
предыдущий SHA. Доставка выше проверена новым скриптом с if/raise. Условия
конечного повтора также заменены явными проверками; отрицательный контроль
версии отклонён под тем же оптимизированным Python до мутаций.

Программа из поста 87457 пока не повторялась. Шесть ранее принятых дочерних
событий сохраняются; число завершённых постов нельзя выдавать за число
созданных событий или оценку всех потерь. Планируется один конечный повтор
через обычный pipeline в свободном интервале, с проверкой exact replay,
оставшихся детей и итоговых ссылок. Массовый план 37 технических отказов
не запускался; конфликты адресов существующих карточек не обходятся.
Остаются проверка совместной работы source parser/VK после 12:15, контроль
около 14:30 и первый обычный TG после исправлений в 21:40 UTC. Инцидент открыт.


## Дополнительный дефект адреса и дедупликации, 2026-11-03

По сообщению владельца проверены две карточки «Плетёного мужа» на 3 ноября,
19:30, в «Синема Парк Европа»: 10242 и 10295. Официальное расписание подтверждает
Гвардейский проспект 3. Ошибочная Театральная 30 присутствует уже в исходном
посте meowafisha/9050. Система нашла существующую карточку, но LLM сочла
расхождение адресов достаточным для FINAL_DISTINCT и создала дубль. Это
отдельная семантическая причина деградации, не ошибка квоты или recall.

Подтверждённый дубль объединён штатным guarded manifest tool с сохранением
источников, постеров и принятого кандидата. Дневная подборка исправлена;
ошибочный календарный пост и ВК-дубль удалены с проверкой владельца и readback,
Telegraph ведёт на основную карточку.

Одного добавления официальной площадки в справочник оказалось недостаточно:
полный shadow replay нормализовал адрес, но LLM продолжила отвергать источник
из-за исходной цитаты. Узкий PR #773 передаёт в тот же identity-запрос
существующую проверенную справочную запись отдельно от текста источника.
Нового запроса, повтора или исключения из merge-защит нет. Реальный Telegram
importer → Smart Update replay после этого даёт один MERGED, ноль CREATED и
верную source-связь; другие сеанс и кинотеатр в реальных контролях не сливаются.

Финальный образ d61a1f6a квалифицирован; доставка ожидает безопасного интервала
после активного парсинга. Не считать это уже доставленным prevention. Подробные
квитанции и незавершённая приёмка: [запись инцидента](incidents/INC-2026-11-03-cinema-europe-address-duplicate.md).
Основной контроль регулярных импортов продолжается.


## Combined release delivered, 2026-11-03 13:58 UTC

Actual RunCoveer Events runtime is now `7d03b15853d3548299aa334418a9a907ae415312`,
image `intake-guide-promo-spacing-20261103`, immutable
`sha256:96896a2b00717740524948c4b49caedc4b6f2a96e6dba4bf23c5482789ade566`.
PRs #777 and #778 passed all required CI before merge. Source parser completed
261/261 before cutover; the final guard found no active ops. Events alone was
recreated. Kotopogoda container ID remained unchanged. Explicit runtime/model
settings checks, SQLite quick_check, effective cinema reference and identity
function hash passed. Live health reports ready=true, database/scheduler ok,
no issues. The existing promo interval is five minutes.

Evidence is retained in the intake recovery directory under
`promo-spacing-cutover.log`, `promo-spacing-live-verification.log`,
`promo-spacing-live-health.json` and `promo-spacing-startup.log`.
This supersedes earlier not-deployed candidate status; it does not close the
incident or substitute for actual scheduled source receipts.

Remaining acceptance: automatic VK recovery of the skipped 13:30 UTC slot,
14:30 UTC verification and ordinary Telegram import at 21:40 UTC. Programme
87457 has never replayed; the guarded candidate interval is 16:35–18:10 UTC,
after terminal normal 16:30 VK, with no concurrent heavy work. Today's two
promo posts remain valid content receipts but failed timing acceptance. Preserve
them without duplicates. Check normal promo dispatch after delivery, daily
Афиша on November 4, and the next eligible two-destination pair on November 6
using actual provider timestamps. All three owner objectives remain active.
