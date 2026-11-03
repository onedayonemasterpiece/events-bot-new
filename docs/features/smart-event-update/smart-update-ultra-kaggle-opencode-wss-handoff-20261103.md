# Smart Update Ultra — автономный поиск через Kaggle + OpenCode + WSS

**Дата:** 3 ноября 2026 года  
**Назначение:** документ для архитектурной проработки, декомпозиции и последующей реализации в EventsBot.  
**Статус:** **proposal / implementation handoff; НЕ внедрено и НЕ прошло live acceptance**.  
**Владелец продуктовой границы:** EventsBot на RunCoveer.  
**Связанный проект:** [my-daep](https://github.com/onedayonemasterpiece/my-daep) — распределённая *разработка*, не обязательная зависимость Smart Update Ultra.

> Этот файл описывает один из возможных способов выполнить поисковый этап Smart Update Ultra автономно и надёжно. Он **не изменяет действующий контракт Smart Update**, не разрешает production deploy и не означает, что OpenCode уже запущен в Kaggle. Решение нужно проверить на свежем коде и фактическом runtime перед реализацией.

## 1. Результат, который нужен продукту

EventsBot по собственному расписанию или явному запросу запускает **длительный поиск по источникам** в Kaggle. OpenCode использует структурированный поиск, доступные инструменты чтения страниц и Playwright там, где требуется полноценный браузер. По мере нахождения источников возвращает **source-grounded кандидатов событий** на RunCoveer. EventsBot применяет кандидатов через **существующую границу Smart Update** с его правилами identity, dedup, правдивости, медиа, transaction receipts и доставки.

Обязательные свойства:

- автономность: RunCoveer способен запустить/продолжить работу и принять результат **при выключенном DevCoveer и закрытом ChatGPT**;
- регулярность: расписание и сведения об источниках принадлежат EventsBot; очередной запуск не зависит от ручного запуска ноутбука;
- полезные частичные результаты: найденные кандидаты доставляются по мере готовности, не только после завершения всего обхода;
- достоверность: точные URL/даты/время/место/исходный текст/при необходимости screenshot + provenance; модель не додумывает отсутствующие факты;
- надёжность: потерянный ACK, повторный callback, отключение WSS или окончание notebook не создают событие второй раз;
- управляемая стоимость: внешние бесплатные модели — ресурс с квотой, а не обещанная безлимитная инфраструктура;
- минимальная архитектура: не создавать второй Smart Update, вторую бизнес-очередь, глобальный gateway или отдельный сервис под каждую операцию.

**Не цель первого этапа:** замена всего текущего source parsing, миграция Smart Update на новый LLM stack, автономный merge в production, объединение нескольких CPU/GPU Kaggle в одну модель, развёртывание полного my-daep, запуск всех существующих Kaggle-сценариев на новом runtime.

## 2. Подтверждённые предпосылки и недоказанные части

| Область | Что подтверждено на 03.11.2026 | Чего это НЕ доказывает |
|---|---|---|
| OpenCode v2 | В исходном коде при отсутствии ключа предусмотрены `apiKey: "public"` и включение бесплатных моделей; есть соответствующие тесты. Официальный каталог перечисляет временно бесплатные модели. | Успешный **анонимный** inference именно из Kaggle, конкретная квота, ежедневная доступность и качество поиска. Документация разных версий о signed-out режиме расходится; нужен live probe закреплённой версии. |
| Kaggle | В EventsBot есть Kaggle запуск/статусы, callback и heartbeat, а в РКБ — работающий пример исходящего WSS, reconnect и передачи результата. | Готовый универсальный тёплый Kaggle OpenCode worker или возможность держать неограниченное число одновременных notebook. |
| EventsBot | Production работает на **RunCoveer**; уже существуют планировщик, source parsing, Smart Update и журналы приёмки/исходов. | Что внешний структурированный EventCandidate можно без адаптации направить через raw-text `event_create_adapter`: тот adapter сейчас повторно вызывает парсер текста. |
| my-daep | Есть согласованный проект 1–4 OpenCode coding workers с Git checkpoints и интеграцией. | Наличие готового DAEP supervisor и завершённого live Kaggle coding-pilot. |
| Общий runtime | В исследовании 29.10 согласован независимый `controller + worker` с WSS на **каждом** сервере-потребителе. | Что новый компонент уже реализован, установлен или принят в production. |

Ориентиры в коде:
- [EventsBot Smart Update — канонический документ](README.md);
- [Kaggle Status Framework](../kaggle-status-framework/README.md);
- [RunCoveer deployment](../../../deploy/runcoveer/README.md);
- [source_parsing/handlers.py](../../../source_parsing/handlers.py);
- [private_events_mcp/event_create_adapter.py](../../../private_events_mcp/event_create_adapter.py);
- [RKB WSS transport](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/f13bbd63881e45fb9b849fcba20bbbf684288f3f/scripts/production/bge_wss_transport.py);
- [Dataset Loop Kaggle bootstrap](https://github.com/onedayonemasterpiece/dataset-loop-mcp/blob/d67ffe18ff75c8cc8a582e16e2f57c826f467440/src/dataset_loop_mcp/providers/kaggle_worker.py).

Проверка продукта при подготовке этого документа — **read-only анализ исходников и документации**, не live/production E2E.

## 3. Предлагаемая архитектура

```text
EventsBot / RunCoveer                         [НЕ зависит от DevCoveer]
  existing scheduler / ручной trigger
       |
       v
  Events-owned runtime controller
  - Kaggle launch / warm pool / run identity
  - durable job -> worker -> OpenCode session
  - собственный WSS endpoint + короткие credentials
       ^                     |
       | исходящее WSS       | задачи, отмена, model choice
       |                     v
  Kaggle Events worker (CPU по умолчанию)
  - pinned OpenCode v2 server на 127.0.0.1, native HTTP/SSE
  - deterministic bootstrap / allowed search tools
  - Playwright + Chromium при необходимости
  - поиск, извлечение, evidence/candidate batches
       |
       | candidate/evidence + повторная доставка до ACK
       v
  EventsBot product adapter
  - проверяет источник, shape, identity, grounding
  - возвращает существующий EventCandidate/продуктовый input
       |
       v
  Existing Smart Update adjudication
  - source decision / child scope / dedup / identity
  - facts -> text / media gate / event receipt
       |
       v
  RunCoveer production DB (единственный продуктовый writer)

Параллельно и независимо:
DevCoveer -> собственный controller -> свои Kaggle Dev workers
```

Один и тот же **переиспользуемый код** controller/worker может устанавливаться на DevCoveer и RunCoveer, но у них **разные процессы, WSS, secrets, задачи, пулы и durable state**. Отказ DevCoveer не должен мешать RunCoveer запускать новый notebook. Не подключать один активный worker сразу к двум контроллерам.

**Первый объём:** один Events controller и один worker в пуле, один активный task на worker. Затем проверить два одновременно работающих Kaggle workers. Масштаб 1–4 из my-daep — ориентир на будущее для coding, **не подтверждённая ёмкость Kaggle-аккаунта и не норматив для Events**.

Внутри Kaggle HTTP OpenCode доступен только с loopback. Внешне доступен HTTPS/WSS у RunCoveer; **не открывать публичный inbound API notebook**.

## 4. Разделение обязанностей и защита качества Smart Update

**В Kaggle:** чтение источников, поиск ссылок/страниц, извлечение вероятных событий, аккуратное различение объявления/новости/прошедшего/рекламы, сбор текстовых и визуальных свидетельств и доставка результата. Ответ LLM — **предложение с доказательствами**, не подтверждённое событие и не SQL-команда.

**В EventsBot:** выбор due-источников, source identity, текущие policy/allowlist, нормализация входа, подтверждённый EventCandidate/child scope, существующее LLM-first identity adjudication, применение событий, receipt, публикационные side effects и recovery. Ни одна часть этой бизнес-логики не переносится «заодно» в Kaggle.

Особенно важно:

1. Не вызывать повторно сырой `add_events_from_text` для уже структурированных результатов, если это заставляет ещё раз извлекать те же факты и терять исходный child scope. Сделать **узкий product-owned adapter к существующей внутренней candidate boundary**; перед кодингом определить актуальный native shape/entrypoint по текущему HEAD.
2. Не считать найденное совпадение по title/date достаточным разрешением на merge или create. Применить действующие правила occurrence identity, quote grounding, negative evidence и `RETRY_SCHEDULED` при неопределённости.
3. Для каждого найденного child сохранять original source URL, canonical source identity, текст/выдержку, точный locator и capture time. Визуальная информация (poster OCR, screenshot) должна иметь связь с конкретным источником. Не придумывать дату, площадку, билетный URL или цитату из недоступной страницы.
4. Продуктовый accepted outcome (`CREATED`/`MERGED`/`NOOP_EXACT_REPLAY` и др.) определяется только после реальной приёмки Smart Update. Нельзя считать `worker_completed`, доставленный batch или ACK равнозначным committed event.
5. После подтверждённого DB commit не преобразовывать принятый исход в технический failure из-за необязательного observer. Сохранять существующий retry/fail-closed contract; **не включать скрыто выключенные consumer/identity background retries**.
6. Отдельно защитить product-state от внешнего текста: сайты, документы, HTML и screenshots являются **untrusted content**. Команды из страницы не дают модели права вызывать произвольные инструменты, менять настройки, выполнять SQL или обращаться к secrets.

### Минимальная передача кандидата

До реализации изучить существующий `EventCandidate` и соответствующий продуктовый adapter; **не утверждать новый JSON как каноническую схему**. Логически понадобятся:

- `job_id / task_id / attempt_id / candidate_ref` (стабильные для replay);
- `source_id` и точный исходный URL/locator;
- candidate facts, привязанные к verbatim excerpts или poster/screenshot evidence;
- обнаруженные child/occurrence anchors, если страница включает несколько событий;
- timestamps для наблюдения, версия prompt/model/tool, ошибки неполного извлечения;
- immutable content/reference fingerprint, а не выдуманный «похожий дубль».

Уже существующие продуктовые ключи и receipts — авторитетные. Не заводить независимый «свой event_id» в Kaggle и не помещать основные данные в новый бизнес-реестр.

## 5. OpenCode без собственного API-ключа: подтверждение и ограничения

На текущей ветке официального OpenCode v2 код включает public-доступ к моделям с бесплатным входом, когда нет `OPENCODE_API_KEY` или подключённого аккаунта. В тестах проверен режим `keeps free models without credentials`. В официальном каталоге указаны `glm-5.3-flash-free`, `deepseek-v4.1-flash-free`, `mimo-v2.6-flash-free` и другие **временно** бесплатные модели.

Это **не** означает гарантированную бесконечную квоту и не разрешает обходить provider/account/IP limits. `FreeUsageLimitError`/429 нужно считать обычным ожидаемым результатом и обрабатывать ограниченным fallback или паузой. Новая Kaggle VM **не обнуляет автоматически** квоту. Бесплатные chat models могут требовать запуска через сам OpenCode, а не использоваться как открытый API произвольного клиента.

Проверка *именно из Kaggle* обязательна до проектирования платных резервов и модельных цепочек:

```bash
# В Kaggle CPU notebook с включённым интернетом.
npm install -g @opencode/cli
opencode --version
opencode models
opencode run --standalone --model opencode/glm-5.3-flash-free \
  "Ответь одним словом: OK"
```

Запускать в чистой среде, не передавая `OPENCODE_API_KEY` и не включая `OPENCODE_DISABLE_FREE_MODELS`. Зафиксировать exact binary/version, model ID, HTTP outcome, ответ, время и видимость доступной бесплатной модели. **Эти команды приведены как acceptance probe, а не как уже исполненный тест.**

Отдельные проверки: разрешён ли этот способ использования текущей политикой сервиса, есть ли доступ к поисковым инструментам, способен ли конкретный model ID читать изображения и работать с tools на выбранной версии. Браузерные screenshots сами по себе **не доказывают** multimodal inference.

Для первого canary: одна явно выбранная бесплатная модель, без платного автоматического fallback; если она временно недоступна — честный `WAITING_MODEL` с bounded retry/cooldown. Модельные маршруты и стоимость фиксировать по факту, а неизвестный расход показывать как unknown, а не «0».

Источники:
- [OpenCode provider implementation](https://github.com/anomalyco/opencode/blob/dev/packages/core/src/plugin/provider/opencode.ts);
- [OpenCode keyless free tests](https://github.com/anomalyco/opencode/blob/dev/packages/core/test/plugin/provider-opencode.test.ts);
- [OpenCode v2 installation](https://opencode.ai/v2/docs);
- [OpenCode free models](https://opencode.ai/v2/docs/console/models);
- [OpenCode CLI commands](https://opencode.ai/v2/docs/cli/commands);
- [OpenCode server API](https://opencode.ai/v2/docs/server).

## 6. Транспорт, восстановление и жизненный цикл

### Минимальный controller

Один небольшой процесс на RunCoveer, подключённый локально к EventsBot:

- принимает `submit/follow/cancel` и конфигурацию источников из product-owned scheduler;
- сохраняет ключ дедупликации запуска, `job_id`, `attempt_id`, `worker_id`, `provider_run_ref`, `opencode_session_id` и этап;
- инициирует новый Kaggle notebook только если нет подходящего тёплого worker и есть подтверждённая доступная ёмкость;
- ведёт свой технический journal, **не заменяя** `kaggle_run_ledger` и продуктовые receipts;
- принимает WSS от назначенного worker, маршрутизирует команды только туда;
- сообщает реальные статусы `requested / queued_at_provider / running / connected / working / checkpointed / blocked` без выдуманного «процента готовности».

Не строить отдельный scheduler, Redis, брокер, LLM gateway, универсальный Kubernetes или систему автономного пересоздания архитектуры. Разделение процесса controller обосновано независимым restart и WSS endpoint, но внутри него не нужны отдельные микросервисы.

### Worker

- Пинованная версия OpenCode v2; `opencode serve` на loopback; native API/SSE для session control.
- Bootstrap/Playwright/Chromium и provider settings устанавливаются детерминированно; health OpenCode проверяется отдельно от успешного LLM inference.
- Исходящее `wss://` соединение с bounded reconnect; heartbeat ≠ полезное продвижение; idempotent command IDs и ACK после durable записи.
- Смена модели/продолжение/abort не должны сами по себе перезапускать notebook.
- Для поиска достаточно сохранённого task input, cursor/source state, подтверждённых кандидатских результатов и evidence refs. Полный LLM transcript — полезная диагностика, но не единственная точка восстановления.
- После потери VM новая физическая попытка получает тот же логический job и последнее подтверждённое состояние; подтверждённые кандидаты не отправляются как новые.
- **30 минут полезного простоя** — начальный настраиваемый idle timeout, не гарантированная возможность бесконечно продлевать Kaggle-сессию. У Kaggle есть собственный hard lifetime/availability.

### Секреты и авторизация

- RunCoveer владеет Kaggle API credential, server TLS/WSS secret и при необходимости GitHub App private key. Ни один master secret/PEM не попадает в публичный notebook или модельный prompt.
- Notebook получает только scoped, expiring bootstrap/attempt credential; lease/attempt generation запрещает позднему старому worker принимать новые задания или повторно применять результат.
- Для **поиска** EventsBot GitHub write token обычно не требуется. Если нужен Git для установки версии worker — read-only source/bundle; право записи отдельно обосновывать.
- Доступ к production DB остаётся на RunCoveer. **Kaggle не подключается к SQLite напрямую и не делает SQL writes**, хотя может прислать proposal/candidate на применение.
- Не переносить credentials DevCoveer в Events runtime, не делать Dev обязательным broker для RunCoveer.

## 7. Порядок проработки и реализации

| Этап | Узкий объём | Gate для перехода |
|---|---|---|
| **P0 — инвентаризация** | Fresh-read текущих `docs/features/smart-event-update/README.md`, `source_parsing/handlers.py`, candidate shape, receipts, scheduler, Kaggle Status Framework и RunCoveer deployment. Определить точную source → candidate → Smart Update границу и owner существующей очереди. | Карта consumer/producer и точная точка адаптера; исключены параллельные writers/повторная идентификация. |
| **P1 — анонимный OpenCode smoke** | Один CPU Kaggle notebook, deterministic install v2, `models`, реальный free model inference и tool call. Без WSS/production data. | Сохранены exact версии, вывод, статус, квота/error; либо явный blocker, а не предположение о бесплатном доступе. |
| **P2 — автономный WSS warm worker** | Минимальные controller + worker, привязка Run к Kaggle, health, event/ACK, session control, model switch, reconnect, idle-stop, один consumer task. | С одним worker: один submit → результат; pause/resume/cancel; отключение WSS и рестарт Run controller не создают второй job. |
| **P3 — browser/search profile** | Playwright Chromium, URL search/read, HTML/DOM, screenshot/vision при фактической поддержке модели, bounded source crawling. | 2–3 известных источника, точное evidence, обработка dynamic/blocked сайта без галлюцинации, корректные ошибки. |
| **P4 — Events adapter** | Преобразование source-grounded candidate в существующий внутренний input, delivery в Smart Update, durable receipt reconciliation. | На тестовой БД candidate → accepted outcome; exact replay не добавляет второй Event/child; неопределённость не становится create. |
| **P5 — RunCoveer isolated canary** | Под контролем владельца один due-source и один повтор, сначала preflight/без внешних публикаций; затем разрешённая live acceptance. | При выключенном DevCoveer Run создаёт Kaggle task, получает candidate, применяет через Smart Update и независимо читает DB receipt. |
| **P6 — масштабирование при необходимости** | Второй worker, routing и capacity test, необязательный shared runtime package для DevCoveer. | Два notebook/разные задания без перепутывания сессий, команды/отмена попадают строго в свой worker. |

**Рекомендуемый первый PR реализации:** P1+P2 без модификаций фактов/идентичности Smart Update. Второй PR — узкий адаптер и продукто-специфичные regression tests. Миграция всего текущего source parsing недопустима как побочный эффект.

## 8. Проверяемые сценарии приёмки

Не заменять реальный результат зелёными mock-тестами. Для каждого проверяемого эффекта сохранять version, `run_id`, exact источник, `candidate_ref`, journal/receipt, относящиеся тесты и исход состояния.

1. **Free model / без ключа:** новый Kaggle notebook успешно вызывает модель без `OPENCODE_API_KEY`; при отказе ошибку отличают от ошибок установки/сети/несовместимой версии.
2. **Run независим:** отключить/изолировать DevCoveer; EventsBot на RunCoveer сам запускает Kaggle, принимает исходящее WSS и доводит кандидат до продукта.
3. **WSS lost ACK:** candidate доставлен и принят, ответ потерян; повторная передача с тем же идентификатором не повторяет создание.
4. **Worker умер в середине:** восстановить подтверждённый cursor/result; не повторять уже accepted children и не терять подтверждённые результаты соседа.
5. **Quota exceeded:** `FreeUsageLimitError`/429 не порождает бесконечную цепочку Kaggle notebooks и не стирает уже полученные результаты. Бounded fallback либо явное ожидание.
6. **Cancel/model switch:** сохранить ту же notebook-сессию, если это поддерживается; не считать `abort` уничтожением VM и не выдавать команду соседнему worker.
7. **Два workers:** одновременные задачи разных источников, корректная sticky routing и отдельные WSS identities; отсутствие гонки за один слот аккаунта.
8. **Web evidence:** для JS-страницы Playwright предоставляет подтверждаемый текст/снимок; неподдерживаемая vision model возвращает `needs_vision / unavailable`, а не сочинённые факты.
9. **Smart Update identity:** одно событие в двух источниках и два разных child в одном источнике: соответственно безопасный merge/replay и раздельные occurrences по действующему контракту.
10. **Неопределённая семантика:** противоречивая дата/место/название — retry/review/fail-closed; никакой произвольный `CREATED`.
11. **Product commit:** отдельно читается фактический event/receipt на RunCoveer; `worker_completed` без успешного commit не считается end-to-end success.
12. **Откат/отключение:** выключение feature flag возвращает прежний источник/Smart Update путь; существующие scheduled workflows не были скрыто переведены на новый сервис.

## 9. Риски и продуктовые решения

- **Квоты:** доступность бесплатных моделей, лимит одновременно работающих Kaggle notebooks и срок жизни VM — внешние ограничения. Использовать реально наблюдаемую capacity и ограниченное ожидание, не создавать много notebooks ради обхода лимита.
- **Качество поиска:** стоимость ошибочного пропуска события важнее технической скорости. Нужны тестовые источники с проверяемыми событиями, несколькими occurrences, постерами и отрицательными примерами.
- **Лицензии и сайты:** уважать разрешённые способы доступа, robots/terms/rate limits; не обходить CAPTCHA/авторизацию. Наличие Playwright не является разрешением на любой обход.
- **Данные:** адреса источников/скриншоты могут содержать персональные данные. Передавать минимально необходимое, ограничивать инструменты модели, не прикладывать secrets или cookies к evidence.
- **Смена моделей:** при настоящей недоступности free провайдера нужна явно разрешённая policy. Платный fallback не включать по умолчанию и не объявлять «нулевую стоимость» без receipts.
- **CI vs продукт:** тест, установленный OpenCode, тёплый notebook и доставленный candidate не доказывают качество и корректное изменение production events.

Вопросы владельцу перед расширением P4–P5: список/приоритет проверяемых источников, допустимость автоматических платных моделей и разрешённый контур live canary. **P0–P3 можно прорабатывать без включения production writes и платных моделей.**

## 10. Источники обсуждения и связанные решения

1. [29.10 — исходная голосовая идея, полная расшифровка](https://github.com/onedayonemasterpiece/idea-hub/blob/main/inbox/voice/2026/10/voice-20261029-191818-995cc4a1.md): warm Kaggle OpenCode, WSS, Playwright, Smart Update Ultra, независимость Run.
2. **29.10 — `kaggle-opencode-runtime-architecture-20261029.md` (Library artifact):** архитектурное предложение с двумя независимыми controllers, профильными workers, маршрутизацией и acceptance. Исследование не создало исполнительного кода.
3. [30.10 — исследование Kaggle CPU/GPU, бесплатного OpenCode и WSS](https://github.com/onedayonemasterpiece/idea-hub/blob/main/ideas/venture.new-product/research-20261030-kaggle-cpu-gpu-llm-opencode-rkb.md): разделяет независимых OpenCode workers и эксперимент по распределённому по слоям инференсу; для Smart Update Ultra нужен **первый** подход.
4. [my-daep — действующий дизайн](https://github.com/onedayonemasterpiece/my-daep/blob/master/docs/distributed-mode.md): Git checkpoints, попытки, ограниченные retry, будущая распределённая разработка. Это полезный donor, но **не обязательный runtime dependency**.
5. [РКБ — WSS worker](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/f13bbd63881e45fb9b849fcba20bbbf684288f3f/docs/operations/bge-wss-worker.md): существующий transport precedent; BGE/PG state machine не переносить целиком.
6. [OpenCode v2 docs и исходники](https://opencode.ai/v2/docs), [server API](https://opencode.ai/v2/docs/server), [keyless provider](https://github.com/anomalyco/opencode/blob/dev/packages/core/src/plugin/provider/opencode.ts): точные технические механизмы требуют version-pinned live probe.

---

**Критерий завершения работы над этим предложением:** команда должна превратить P0–P5 в небольшие проверяемые задачи с владельцами, выполнить source review и реальную приёмку. Пока нет фактического RunCoveer → Kaggle WSS → OpenCode → source-backed candidate → Smart Update receipt, статус остаётся **«архитектура обоснована, интеграция не доказана»**.
