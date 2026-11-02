# Events Bot: модели Google, расход и деградация импорта — пакет для ChatGPT Pro

Подготовлено 2 ноября 2026, Europe/Kaliningrad. Автор: Codex, по запросу владельца.
**Актуальный статус: узкая routing-правка доставлена в RunCoveer из main; live catch-up частичный (1/2). Полный инцидент открыт. ChatGPT Pro не запущен.**
Сначала прочитайте раздел «После выпуска»: он дополняет исходное исследование и меняет приоритет оставшихся вопросов.
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

Это первоначальные варианты для анализа. Впоследствии вариант A доставлен; актуальная проверка и ограничения — в разделе «После выпуска». Другие варианты не применялись.

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

## После выпуска: live outcomes, 2 ноября 2026

### Что действительно доставлено

- [PR #761](https://github.com/onedayonemasterpiece/events-bot-new/pull/761) merged; exact main SHA **cdd56170713448258d81d4e05c9a2d5d3f85a70a**. Все обязательные CI checks passed на финальном PR head.
- Image `runcoveer/events:intake-lite-20261102`, ID `sha256:aba9c71e887cfce663413cbe9bcbef61f1e6b3396f29d2ac5916cd270b3a6f75`. Actual prior runtime f9b50fbb9 — ancestor кандидата. Dockerfile/dependency inputs неизменны; verified все **4 427 tracked files**.
- Изолированный preflight: отдельная пустая SQLite, production volume не подключался, webhook и scheduler не запускались. Затем Events-only cutover с DEPLOY_PREFLIGHT=0; Kotopogoda сохранила прежний container ID.
- Production readback: source SHA совпадает; ready=true, db=ok, scheduler/tasks healthy. Старый образ и Compose сохранены для rollback.
- Негативный контроль через настоящий parser и atomic limiter: recap-only → CONFIRMED_NO_EVENT, 0 events. Обе Flash получили pre-send RPD denial, затем 3.1 Lite успешно ответила (6 158 actual tokens).
- Исправление сохраняет original caller model; новых recovery workers/queues/retries/flags нет. Порядок emergency Flash и send cap не менялись.

### Разовый catch-up через normal path — **частичный**

Run ID `INC-2026-11-02-lite-chain-catchup`, ops_run **11548**, original batch `auto:1793638966`. Выбраны только два сохранённых failed_technical carrier с полным source envelope; атомарные state/batch guards исключали изменённые и ранее успешные строки. Обычные run_vk_auto_import → parse → Smart Update → outbox; существующая terminal policy не менялась. Operator notifications подавлены в отдельном diagnostic process; никаких новых фоновых consumers.

| Inbox | Source | Outcome | Подтверждение |
|---|---|---|---|
| 86966 | [Онегин](https://vk.com/wall-100137391_167662) | imported / EVENTS_RESOLVED | Связан с существующим Event **7234**, обновление без создания дубля; 251,495 s |
| 86977 | [Третьяковская галерея](https://vk.com/wall-212760444_6357) | failed_technical | Parser завершён, затем SMART_UPDATE_UNRESOLVED:identity_final_retry:match_rejected:time_conflict_veto; 26,620 s |

Итого 279,89 s, processed=2, imported=1, failed_technical=1, new events=0, updated events=1. Обе leases освобождены, незавершённых rows этого прогона нет. Нулевое число новых events здесь не означает неработающий parser: первый carrier обновил уже существующее событие, второй остановился на identity. Массовый historical catch-up не выполнялся.

**Фактическое использование обеих Lite:** в этом run event_parse получил два успешных ответа 3.1 Lite. Smart Update получил семь успешных 3.1 и четыре 3.5 Lite; четыре 3.1 вызова вернули provider **503**, а existing sibling fallback успешно ответил через 3.5. Теперь обе модели достижимы и работают в обычном импортном пути.

### Оставшийся семантический конфликт — материал для Pro

Источник Третьяковской галереи объявляет общую «Ночь искусств» 3 ноября: концерт, квиз/кинопоказ, мастер-классы с 18:00, единый билет vk.cc/d39xyT. Parser выдал один umbrella event на 2026-11-03 18:00, evidence_complete=true, STOP, 8 186 actual tokens. Source packet **26015** сохраняет typed receipt; не был помечен как no-event.

Existing deterministic identity gate выдал veto_create `deterministic_same_ticket_slot`, matched Event **10060**: отдельный мастер-класс «Катающаяся живопись», та же площадка, дата, 18:00 и общий ticket URL. Код `smart_update_identity.py:394` допускает veto по related title **ИЛИ location**. Для shared programme ticket совпадение venue/time не доказывает идентичность umbrella и дочернего события. В соседнем same-source guard уже требуется related title; ticket guard пока слабее. Это конкретный кандидат на узкое исправление, которое возвращает решение existing LLM adjudication вместо детерминированного заключения по площадке.

Затем final identity adjudication вернула `match_rejected:time_conflict_veto`. Точный выбранный ею target и происхождение time conflict ещё не реконструированы: нельзя утверждать, что финальный target обязательно тот же 10060 (у него в DB тоже 18:00). **Не отключать time-conflict veto и не принудительно объединять umbrella с мастер-классом.** Нужен source-bound LLM-first replay с shared-ticket sibling и настоящим same-event positive control. Новой retry-очереди это не требует.

### Отдельная цена enrichment и пределы проверки

Для первого carrier provider parse занял 13,017 s, persist/Smart Update — 235,667 s. Во время enrichment event_topics Gemma вернула **три provider 500**, затем один успешный ответ. Не объявлять все 235 s временем именно Gemma: другие Smart Update стадии также входят в эту длительность.

| Consumer/model | Successful physical calls | Errors | Known total tokens |
|---|---:|---:|---:|
| event_parse / 3.1 Lite | 2 | 0 | 16 467 |
| smart_update / 3.1 Lite | 7 | 4 × 503 | 9 238 |
| smart_update / 3.5 Lite | 4 | 0 | 14 591 |
| event_topics / Gemma 4 31B | 1 | 3 × 500 | 1 466 |

Итого log sample: **21 физических generation calls**, 41 762 известных tokens; плюс отдельный negative control 6 158 tokens. У failed calls usage отсутствует: это не доказанная нулевая стоимость. Pre-send quota denials сюда не включены. Это только explicit catch-up, не общий расход продакшена.

Event 7234 имеет vk_sync=done и сохранённый managed VK URL; при проверке telegraph_build=done, tg_event_publish pending и static_site_build pending. Существуют старые media materialization errors и известный vector barrier backlog. Authenticated wall readback в этой сессии не выполнялся; новая публикация всех восстановленных данных не заявляется. **Инцидент остаётся открытым**: normal routing исправлен, полная semantic/publication recovery не доказана.

Retained evidence: `/home/dev/artifacts/events-bot-new/20261102T204526Z-intake-lite-fix-20261102/`; catchup.log SHA-256 `15e0734d9f86e0a45145ad25d182fac3214d1e6c76b42d4a23099ffebd95c7a1`, совпадает с production operation log. Secret values не сохранялись.

Для Pro приоритет теперь: (1) shared-ticket umbrella/sibling identity и точный final target; (2) стоимость/latency enrichment и безопасное сокращение лишней работы существующего пути; (3) bounded reconciliation после исправления semantics. Не предлагать заново уже deployed single-chain fix. Не расширять бюджет или снимать safeguards ради зелёной метрики.

## Короткий prompt для Pro

> Прочитай пакет целиком, начни с live-раздела «После выпуска». Потеря Lite в parser chain уже исправлена и deployed в main cdd561707, обе Lite live-verified. Разбери оставшийся отказ Третьяковской галереи: shared programme ticket, umbrella vs workshop Event10060, deterministic_same_ticket_slot и последующий time_conflict_veto (его точный final target пока неизвестен). Предложи узкую source-bound LLM-first правку существующего identity path с sibling negative control и same-event positive control; не отключай конфликт времени и не добавляй retry worker. Отдельно оцени 251s на первый carrier, provider503/500, enrichment и bounded catch-up. Дай минимальный diff/порядок проверки без переписывания системы, новых очередей/watchdogs/repair-paths. Отдели подтверждённое от гипотез и actual import от публикации.
