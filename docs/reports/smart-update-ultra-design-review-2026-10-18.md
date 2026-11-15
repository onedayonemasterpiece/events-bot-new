# Smart Update Ultra: отчёт по исследованию и выбору решения

Дата: **18 октября 2026 года, Europe/Kaliningrad**. Статус: **исследование и проектное предложение**. Новые inference, replay, нагрузочные испытания, shadow processing, deployment и изменения runtime в рамках этого отчёта не выполнялись.

Основной проект: [Smart Update Ultra — целевая реализация](../architecture/smart-update-ultra-targeted-implementation.md).

## 1. Вывод

Рекомендуется реализовать Ultra ограниченным самостоятельным исполнителем на Kaggle, сохранив действующий Python bot, его БД, доменную логику и publication adapters. Начальный исполнитель — компактный Python runner с существующим OpenCode; Go не является обязательным условием. Выбор языка следует пересматривать только для измеренного участка, на котором новый runtime даст существенный выигрыш без повторения доменной логики.

Главная оптимизация здесь — короткий путь **полученный материал → законченная модельная оценка → немедленная запись в каноническую БД сервиса → использование сохранённого результата**. Самые убедительные свидетельства Street Story относятся к устранению повторного чтения, лишних модельных переходов, потери готовых результатов и бесконтрольного роста контекста. Они не доказывают, что замена Python на Go ускорит удалённый inference.

Отладку стоит разделить на быстрый prompt lab, проверку настоящей целевой модели через OpenCode на DevCoveer и затем ограниченную проверку Kaggle. Каждый этап отвечает на свой вопрос. Успешный ответ субагента сам по себе не подтверждает canonical save, работу Google fallback или полную продуктовую приёмку.

## 2. Версии и границы доказательств

Каноническая документация Street Story прочитана на `61783a3e9092d43958bd647db566501d3949cf8d`. После синхронизации 12 октября `main` содержит требования и документацию; его SHA не является доказательством версии приложения. Актуальный исследованный продуктовый код — draft [PR #257][PR257], `bd700adef8cb4e8d1c21ddc30c3c8151fd99c9f2`. Сохранённый [отчёт 16 октября][S01] подтверждает установку и local/public health для runtime `8542673cb8adcf2ea4daa0e03a9fe38fb27d0901`; сегодняшний health отдельно не проверялся.

Для events-bot использован код `d431b9f86b87999cf27f74b99857d23aedc28f16`. Выводы о его интеграционных границах получены параллельным разбором этого revision и сведены в основной проект. Проектные предложения ниже не выдаются за уже существующие возможности.

В отчёте различаются четыре уровня: **наблюдение в сохранённом запуске**, **механизм в конкретном коде**, **предлагаемое изменение** и **гипотеза об эффекте**. Исторические испытания разных версий не объединены в accuracy или benchmark текущего Ultra.

## 3. Что именно означает качественная работа с фактами

### Самостоятельное утверждение с необходимыми условиями

В Street Story атомарность означает одно самостоятельно выбираемое утверждение, а не одно предложение. Архитектор, высота и дата строительства остаются разными фактами, даже если источник перечисляет их в одной строке. Дата, приблизительность, затронутая часть и существенное условие одного события остаются вместе с этим событием. Разрезать квалификаторы ради увеличения счётчика фактов нельзя. Это явно закреплено в актуальном `ATOMIC_PROPOSITION_CHECKS`. [S02]

Для афиши переносится смысл этого правила. «Концерт состоится 20 октября в 19:00 в указанном зале» описывает событие с необходимыми атрибутами. История организатора не датирует нынешний фестиваль; публикация о программе фестиваля не доказывает проведение каждого отдельного сеанса. «Планируется», «предположительно», «от 500 рублей» и «последние места» нельзя превращать в более сильные безусловные утверждения.

Модель должна различать Event, occurrence, фестивальную программу, площадку и организацию. Код сохраняет и проверяет адресацию этого решения, но не устанавливает смысл по регулярным выражениям. Street Story уже использует декларации физического предмета, отношения к выбранному объекту, временного режима и поддержки конкретного утверждения. [S03]

### Источник должен поддерживать именно этот факт

В `fact_ledger.py` источник связан с observation, assertion, source version, chunk, границами фрагмента и его hash. Поэтому общий URL не превращается в универсальное подтверждение всех тезисов страницы. Новая версия того же URL не заменяет историческое evidence молча. [S04]

При этом актуальный путь не требует безошибочно перепечатывать длинную цитату как условие истины. Сервер сохраняет реально полученные неизменяемые chunks; краткий `confirmed_fragment` служит объяснением и представлением. Механическая нормализация эквивалентной формы допустима, создание отсутствующего evidence — нет. [S03]

Для Ultra нужны revision сообщения или документа, исходный материал, фактически полученный контекст, отдельно publication time и fetch time, ссылки на поддержку существенных полей. При объединении сохраняются разные фрагменты одного URL и источники разных происхождений. Пять перепечаток не становятся пятью независимыми подтверждениями. Множественность источников обогащает evidence, но не голосует за истину. [S05]

Не следует переносить буквально исторический book scoring, expert routing, POI graph или целевую длину исторической карточки: это другие продуктовые обязанности.

## 4. Немедленная каноническая запись и сохранённая полезность

В актуальном `agent_evidence.py` одна SQLite transaction сохраняет fact/assertion, собственные evidence, accepted basis, необходимые conflict records, POI projection и durable outcome callback. Ответ с `fact_id` возвращается после этой записи. Если ответ потерян, повтор восстанавливает тот же outcome. Если outcome не записан, связанные изменения откатываются, а закрытая модельная оценка остаётся доступной для повторного локального применения. [S03]

`native_fact_contract.py` требует вызывать `save_facts` сразу после готовности полезного знания и затем продолжать исследование в том же turn. Финальное сообщение модели не является единственной точкой доставки. [S06]

Это соответствует требованию Ultra: **каждый законченный fact/provenance/model proposal/writer result должен достигнуть канонической БД сервиса сразу по готовности**. Итоговый `output.json` Kaggle, checkpoint только в object storage или накопление пакетов в памяти до конца запуска этого не обеспечивают.

Каноническое сохранение результата и его допуск к публичному Event — разные переходы. Неподтверждённый proposal можно сохранить, не перезаписывая принятые дату или площадку. Сбой writer сохраняет пригодные факты; сбой Astro сохраняет Event и работу на последующую публикацию. Завершение callback save не закрывает весь внешний inference. Полезные сохранённые результаты переживают UNKNOWN финального ответа без ложного объявления операции завершённой.

### Новая поддержка не должна отзывать прежнюю

`accept_fact_basis` сохраняет первоначальную принятую основу при совместимом расширении поддержки. `accepted_basis_valid` проверяет утверждение, предмет и собственные evidence. Само открытие recheck не обнуляет пригодный результат. `poi_memory.py` переиспользует исходную admission без синтетического нового verdict. [S04], [S07]

Но явно withheld утверждение нельзя вернуть обычным refresh: требуется новое адресное положительное решение. Реальная смена смысла, предмета или существенное противоречие блокирует затронутый факт. Для Ultra это защищает от пустой страницы при фоновом recheck и одновременно запрещает воскрешать отменённое событие старым положительным receipt.

## 5. Практические уроки отказов

В историческом broad run Street Story было 18 eligible candidates, но консервативная независимая проверка признала полностью атомарными и поддержанными 6 из 18. Это конкретный старый корпус и версия, **не текущая accuracy**. Успешный SQL commit и статус eligible не доказывают семантическую правильность. [S08], [S09]

Отчёт 16 октября сохраняет более свежие контрпримеры: у кейса 105 адресно отозваны три неверных passage basis; у 133 оставалась история другого корпуса той же гимназии, а две исходные Google операции сохраняли UNKNOWN. Общая продуктовая приёмка оставалась открытой. [S01] Новый язык не исправляет такую ошибку предметной привязки.

Ретроспектива также фиксирует длинную choreography: 15 повторных чтений незавершённого core, premature continuation, ошибку acceptance harness и local token-admission отказ, ошибочно принятый за provider outage. Избыточный coverage review и reconciliation начали удерживать весь результат, хотя полезные факты уже существовали. [S08]

В Ultra следует удалить именно дублирующие обязанности: второй hop ради сохранения готового факта, host continuation после каждого предложенного URL, повторный download прочитанного материала, обязательный whole-inventory review перед выдачей независимых результатов и normal-path repair/supersession. Адресные semantic checks, provenance, quota, leases, Stop и recovery сохраняются.

Partial означает локальность ущерба. Ошибка одного источника не уничтожает соседние результаты; malformed candidate не разрушает независимые карточки. При этом зависимости учитываются: нельзя применить перенос неизвестного сеанса только потому, что JSON корректен. В Street Story такое адресное разбиение уже существует в `fact_page_boundary.py`. [S10]

## 6. Конкретная граница интеграции с events-bot

Сохранение Python application не означает, что Ultra можно подключить простым вызовом старой функции. Текущая `smart_event_update` принимает `db`, `candidate`, параметры source check/scheduling и lease owner; отдельного входа для уже законченных extraction, merge и writer stages у неё нет. `_SMART_UPDATE_LOCK` охватывает LLM implementation; завершение candidate находится отдельно от доменного commit. [E01], [E02], [E03]

Существующий source-fact log нельзя объявлять полноценным immutable facts ledger. `_normalize_fact_item` по умолчанию ограничивает текст 200 символами; create-path использует `extracted_facts[:18]`; `_record_source_facts` удаляет и создаёт записи заново. Эти представления полезны для нынешней функции, но недостаточны как единственное хранилище полного результата Ultra. [E04], [E05], [E06]

**Предлагается:** небольшой prepared-result entry в существующем Python service, который переиспользует нынешние domain apply, identity, dedup и публикационные правила. Законченные stage payloads и replay outcomes сохраняются в добавочной таблице той же сервисной БД. Существующий mutable candidate payload не подменяет журнал результатов разных попыток и revisions.

Это адресное расширение. Не требуется второй canonical writer на Go, отдельная knowledge DB или переписывание всего `smart_event_update`. Запись применённого результата и его точного outcome нужно согласовать внутри существующей транзакционной границы; remote LLM await не должен входить в SQLite transaction. Конкретный контракт описан в основном проекте.

## 7. Prompt lab: быстрый метод с честной границей результата

Сохранённый progress Street Story подтверждает применение direct subagents. Сначала проводилась guided диагностика, затем автономный повтор; отдельные задачи проверяли исправленный prompt и адресное чтение. В `direct_paginated_review_rev45` зафиксировано 11 чтений без предварительной передачи всего корпуса, но только in-memory state и без mutation callbacks. Некоторые direct children не имели `retain_native_source/save_facts`; их отчёт не выдавался за canonical admission. Для части запусков actual model metadata не раскрыта, несмотря на запрошенную Luna. [S11]

| Сохранённый опыт | Что действительно установлено | Ограничение |
|---|---|---|
| Direct prompt/review children | Можно быстро обнаруживать неоднозначный subject scope, неверную адресацию и лишнюю choreography | Не квалификация фактического provider/runtime и не запись в БД |
| Real native save component | Actual `gpt-6-luna/medium`; save через 21,57 с, terminal через 32,70 с | Подтверждался прежний факт; новых знаний 0 |
| Поздний current-profile control | Две карточки записаны до завершения turn; первая самостоятельно пригодная через 44,71 с, terminal 56,07 с | Независимо атомарной признана одна из двух; полнота не доказана |

Источник всех трёх строк — сохранённый progress; это разные условия, а не benchmark ускорения. [S11]

Для Ultra порядок следующий: замороженные DEV/HOLDOUT source packets → быстрая отладка semantic prompt → тот же контракт на реальной целевой модели через OpenCode → отдельная проверка save/apply/replay → ограниченный автономный DevCoveer запуск → Kaggle. Acquisition не нужно повторять для каждого prompt edit. Assisted успех нельзя считать blind holdout, а ответ сильного Codex — доказательством качества MiMo или Groq.

Независимый анализ ошибок полезен в лаборатории. Он не требует постоянного второго judge в production. Нормальный путь должен извлекать, нормализовать и оценивать собственную поддержку одним законченным semantic pass; отдельная проверка допустима там, где она закрывает установленный дефект или обязательство прежнего контракта.

## 8. Что известно о разных моделях внутри субагентов

Актуальный `native_fact_research.py` использует native Luna и разрешает адресную помощь `gpt-6.1-sol` с `fork_turns="none"`, без дальнейшего делегирования. Код проверяет actual model и binding дочернего thread. Это не свидетельство поддержки MiMo как native Codex child. [S12]

Конкретный Street Story OpenCode profile отключает `task`, стандартные `general/explore` и другие capabilities. Поэтому нужная пользователю nested схема не возникает в нём автоматически. Для лаборатории нужен явный ограниченный profile либо оркестрация независимых OpenCode задач существующим интерфейсом DevCoveer. [S13]

Отчёт 5 октября подтверждает реальный MiMo search с 26 URLs и provider receipt `cost=0`. Он также фиксирует qualification failure: из 2 048 output tokens 2 024 ушли в reasoning, видимому ответу осталось 24. Это урок настройки и наблюдаемого ответа, не сегодняшняя гарантия бесплатности, доступности или достаточности модели. [S14]

### Подключённый каталог DevCoveer в этой сессии

Дополнительно выполнено read-only чтение `list_models(verified_only=false)`, без inference: `probesPerformed=false`. Возвращено 165 entries: Codex 7, OpenCode 84, Groq 16, NVIDIA 57, Atria 1. Это переписанный в отчёт минимальный диагностический срез подключённого каталога, не рекламный список и не benchmark. Секреты и полный аккаунтный dump не публикуются.

| Route | Наблюдаемые metadata | Практический вывод |
|---|---|---|
| Native `gpt-6.1-sol`, provider `codex` | Live native catalog | Другой backend, чем OpenCode-строка ниже |
| `opencode/gpt-6.1-sol` | Connected, `free:false` | Не включать автоматически в бесплатный lab |
| `opencode/mimo-v2.6-flash-free` | Connected, active, free, agent eligible | Exact ID есть; actual execution сегодня не проверялся |
| `opencode/nemotron-3-ultra-free` | Connected, active, free, default | Кандидат для текстовых ролей, без нового quality/latency измерения |
| `opencode/big-pickle`, `opencode/glm-5.3-flash-free`, `opencode/longcat-2.5-preview-free` | Connected/free | Альтернативы для отдельных ролей, не три обязательных вызова |
| `opencode/nemotron-3.5-super-vl-free` | Connected/free | Поддержку actual image input ещё нужно квалифицировать |
| `opencode/deepseek-v4.1-flash-free` | `probe_due`, provider unavailable observation | Не называть доказанно рабочим сейчас |
| `groq/openai/gpt-oss-120b`, `groq/openai/gpt-oss-20b`, `groq/qwen/qwen3.8-27b` | `free:false`, `renewable_key_pool`, tool-capable | Account/resource policy требует проверки; это не утверждение обо всех Groq speech/compound entries |

Каталог предупреждает: inference-verification receipt недоступен. Provider-level OpenCode имеет `degraded_observation`, последнюю категорию `provider_unavailable`, время **2026-10-17T22:12:15.619Z**, или **18 октября 00:12:15.619 Калининград**. Это timestamp сохранённого provider observation, не время нового модельного запроса и не доказательство отказа всех Zen-моделей. У перечисленных connected-active моделей отдельного свежего `verifiedAt` нет.

В generic routing обнаружены `policy=zen_then_groq_then_nvidia_then_metered`, `automaticFallback=true`. Для Ultra предлагается явный разрешённый resource profile: отказ бесплатного route не разрешает скрытый платный fallback. Проверяется также merged config и вспомогательные модели. OpenCode поддерживает override модели субагента; это capability продукта, не результат квалификации установленного DevCoveer profile. [W01], [W02]

Исторический Ultra pilot от 5 октября зафиксировал OpenCode 1.18.31, пять native tasks и 21 unittest диагностического кода. Установленная версия сейчас отдельно не проверялась. Direct-session эксперимент тогда получил `403 FreeTierError`; готового принятого batch runner этот эксперимент не дал. В том же пилоте три коротких текста потребовали у Nemotron 16 122 input / 17 400 total tokens — это основание исследовать размер контекста, не измерение чистой inference latency. Пилот не подтверждает 21 успешно обработанное событие, blind holdout, shadow CREATE/MERGE или Kaggle parity. [E07]

## 9. Эффективность, критерии и следующий самостоятельный участок

Историческая compact projection уменьшила model-facing discovery response с 8 618 до 2 976 символов — примерно на 65%, сохранив durable evidence. Поздние диагностики показывают обратный риск: payload 754 197 символов остановился до send; отдельный успешный save component имел prompt 179 809 символов и 221 379 совокупных token units, включая cached input. [S08], [S11] Эти разные серии не позволяют обещать процент экономии Ultra, но обосновывают компактные проекции вместо повторной передачи полного ledger.

Следует измерять время до первой канонической записи и до готового Event, новые пригодные результаты отдельно от recheck/reuse, фактические model sends отдельно от claims, token usage и UNKNOWN отдельно от нулевых значений. Нужны peak RSS/CPU исполнителя и bot, объём stage payloads, возраст незавершённой работы и повторные send/save. Существующая наблюдаемость расширяется адресно; новая платформа не нужна.

Первый самостоятельный участок — один реальный source packet, Ultra и Google fallback с одинаковым результатным контрактом, немедленный canonical save, подготовленный apply существующим bot и восстановление после потери callback. Writer failure должен оставлять факты, а повтор — возвращать прежние IDs. После этого расширяются источники и фестивальная обработка. Сокращение runtime footprint всего бота до 2 ГБ данным исследованием не доказано.

## 10. Принципиальные вопросы

1. Допускается ли культурный ежедневный выпуск из готовой части при продолжительном отсутствии ресурсов, с последующим дополнением, или требуется задерживать весь выпуск? Partial research сам по себе не задаёт эту редакционную политику.
2. Какую глубину хранения исходных source revisions и raw материалов считать обязательной для повторной обработки и расследования? Принятые факты, их опора и незакрытые обязательства должны переживать очистку воспроизводимых временных артефактов.

Эти вопросы не мешают проектированию общего save/apply контракта и лабораторной проверке на замороженных материалах.

## Источники

- Street Story: [последний изученный PR][PR257], [проверенные результаты 16 октября][S01], [историческая ретроспектива][S08], [сохранённая приёмка][S09].
- Семантика и хранение: [atomic claims][S02], [canonical save][S03], [accepted basis][S04], [source multiplicity][S05], [ранняя доставка][S06], [повторное использование][S07], [локальность отказов][S10].
- Лаборатория и модели: [сохранённый progress][S11], [native parent/child contract][S12], [OpenCode profile][S13], [исторические MiMo результаты][S14].
- Events-bot: [действующий entry][E01], [lock boundary][E02], [candidate completion][E03], [нормализация фактов][E04], [create-path projection][E05], [source-fact recording][E06].

[PR257]: https://github.com/onedayonemasterpiece/street-story/pull/257
[S01]: https://github.com/onedayonemasterpiece/street-story/blob/bd700adef8cb4e8d1c21ddc30c3c8151fd99c9f2/docs/reports/street-story-codex-continuation-20261016.md
[S02]: https://github.com/onedayonemasterpiece/street-story/blob/bd700adef8cb4e8d1c21ddc30c3c8151fd99c9f2/backend/street_story/review_packets.py
[S03]: https://github.com/onedayonemasterpiece/street-story/blob/bd700adef8cb4e8d1c21ddc30c3c8151fd99c9f2/backend/street_story/agent_evidence.py
[S04]: https://github.com/onedayonemasterpiece/street-story/blob/bd700adef8cb4e8d1c21ddc30c3c8151fd99c9f2/backend/street_story/fact_ledger.py
[S05]: https://github.com/onedayonemasterpiece/street-story/blob/61783a3e9092d43958bd647db566501d3949cf8d/docs/poi-knowledge-base.md
[S06]: https://github.com/onedayonemasterpiece/street-story/blob/bd700adef8cb4e8d1c21ddc30c3c8151fd99c9f2/backend/street_story/native_fact_contract.py
[S07]: https://github.com/onedayonemasterpiece/street-story/blob/bd700adef8cb4e8d1c21ddc30c3c8151fd99c9f2/backend/street_story/poi_memory.py
[S08]: https://github.com/onedayonemasterpiece/street-story/blob/61783a3e9092d43958bd647db566501d3949cf8d/docs/retrospectives/facts-live-first-retrospective-2026-10-04.md
[S09]: https://github.com/onedayonemasterpiece/street-story/blob/61783a3e9092d43958bd647db566501d3949cf8d/docs/acceptance/facts-research-finish-2026-10-03.md
[S10]: https://github.com/onedayonemasterpiece/street-story/blob/bd700adef8cb4e8d1c21ddc30c3c8151fd99c9f2/backend/street_story/fact_page_boundary.py
[S11]: https://github.com/onedayonemasterpiece/street-story/blob/bd700adef8cb4e8d1c21ddc30c3c8151fd99c9f2/docs/reports/street-story-pr257-progress-20261014.json
[S12]: https://github.com/onedayonemasterpiece/street-story/blob/bd700adef8cb4e8d1c21ddc30c3c8151fd99c9f2/backend/street_story/native_fact_research.py
[S13]: https://github.com/onedayonemasterpiece/street-story/blob/bd700adef8cb4e8d1c21ddc30c3c8151fd99c9f2/backend/street_story/opencode_research.py
[S14]: https://github.com/onedayonemasterpiece/street-story/blob/61783a3e9092d43958bd647db566501d3949cf8d/docs/reports/identity-facts-orchestration-20261005.md
[E01]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/smart_event_update.py#L17344
[E02]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/smart_event_update.py#L17416
[E03]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/smart_event_update.py#L17496
[E04]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/smart_event_update.py#L8510
[E05]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/smart_event_update.py#L21091
[E06]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/smart_event_update.py#L23652
[E07]: https://github.com/onedayonemasterpiece/events-bot-new/blob/621e7b38659d50c4005dee84439d6cd6a0c05b79/docs/reports/smart-update-ultra/devcoveer-pilot-2026-10-05.md
[W01]: https://opencode.ai/docs/agents/
[W02]: https://opencode.ai/docs/config/
