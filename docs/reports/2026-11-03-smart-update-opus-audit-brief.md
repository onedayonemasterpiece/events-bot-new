# Smart Update: задание на критический аудит надёжности и упрощения

Дата среза: 3 ноября 2026 года, около 19:55 UTC (21:55 Калининград). Это **задание аудитору**, а не утверждённый проект переделки. Проверяй наши объяснения по коду и доказательствам, не принимай их за готовый диагноз.

## Что просит владелец

Найти хрупкость, ненадёжность, переусложнение и причины пропусков событий в Smart Update. Дать практически выполнимые рекомендации по упрощению **без переписывания всей системы**. Предпочтительны удаление дублирующих решений, исправление существующих контрактов и небольшие изменения с понятной проверкой. Не проектировать новую платформу, универсальный repair framework или ещё один worker.

Нужен отдельный файл `smart-update-opus-critical-review-YYYYMMDD.md`: сначала выводы и приоритеты, затем доказательства и конкретные предложения. Аудит не разрешает аудитору менять прод, данные или требования продукта. Выполнение исправлений остаётся у Codex с обычной проверкой и доставкой.

## Координация и требования

- [Координация аудита и исправлений](https://mira-projects.slack.com/archives/C0C9LS6L3J8/p1793735038385739): channel `C0C9LS6L3J8`, parent `1793735038.385739`. Сюда передавать ссылку на Markdown с критическими замечаниями и обсуждать проверяемые изменения. Пользователь сообщил, что аудитор будет включаться по расписанию; этот файл не подтверждает, что его расписание уже создано.
- [Продуктовые требования владельца](https://mira-projects.slack.com/archives/C0C9LS6L3J8/p1793735123652469): тот же channel, parent `1793735123.652469`. Перед каждой итерацией читать новые сообщения. На момент среза есть правило владельца: в тред пишет только он; короткий отчёт о выполнении допустим после подтверждения требования **как минимум сутками production runtime**. Не писать туда предложения, промежуточные статусы или «реализовано» по одному тесту.
- Рекомендация аудитора — гипотеза/предложение, не новое решение владельца. При конфликте требований явно указывать конфликт, продолжая независимую разрешённую работу.
- Codex перечитывает оба треда при продолжении работы и по текущему циклу проверок. Не создавать ради координации новые службы, каналы или транспорт.

## Версии и границы среза

Репозиторий: [onedayonemasterpiece/events-bot-new](https://github.com/onedayonemasterpiece/events-bot-new).

| Что | Подтверждённое состояние на момент среза |
| --- | --- |
| Фактический production | RunCoveer, service `events`, runtime `9f14c503f977b467ed6ad963e2de59008bc0a435` |
| Immutable image | `sha256:8b8875452f667264b279f0a10459b92e3556d57b6ec2a02ccebf72590ef51e24` |
| `main` после документации | `2b01705165d74df44ba2a73b72b51ef21421ef75`; документация не требует перезапуска |
| Незавершённое исправление | [PR #791](https://github.com/onedayonemasterpiece/events-bot-new/pull/791), head `f3832b86c50a059b7845b3290564a5342bb6c129`: постоянный отказ редактирования VK не должен превращаться в успех. **Не доставлено на момент среза** |
| Другие задачи | Контроль обычных импортов VK/TG, исправленных дублей, медиабюджетов и промокампании продолжается параллельно |

При повторном аудите сначала перечитай actual runtime и актуальный `main`. Не смешивай проблему старой версии с уже доставленным исправлением и не считай наличие кода в PR доказательством его работы в проде.

Fly — выведенный из эксплуатации writer. Не предлагать операции с Fly как исправление текущего production. Не затрагивать соседний сервис Kotopogoda.

## Карта системы, которую нужно проверить

Это ориентир для чтения, а не доказательство, что каждый путь действительно соблюдает контракт.

1. Producers: официальные source parsers, TG Monitoring, VK inbox, ticket sites, festival queue, ручной/forwarded ввод, typed MCP create.
2. Source parser и, когда требуется, conditional verifier формируют кандидата/дочерние события и доказательства из текста/OCR.
3. Публичный facade `smart_event_update` фиксирует идентичность пакета/попытку и вызывает существующую обработку кандидата.
4. Нормализация и проверки области события, якорей, места, eventness; recall/match/create bundle; identity gate и final dedup adjudicator.
5. Принятый результат создаёт или меняет Event и источник. Извлечение фактов, fact-first описание и media evidence участвуют в подготовке публичного представления.
6. JobOutbox распространяет изменение в VK, TG, Telegraph, календарь и сайт. Это отдельная граница: accepted Event ≠ опубликованная актуальная ревизия.
7. Candidate/source/child/carrier/operation/job записи отражают разные уровни исполнения; их взаимное согласование — отдельный предмет аудита.

### Где читать код

Ниже ссылки закреплены на **фактически доставленном** SHA, чтобы строки не уплывали. Изменения в более новом `main` сравнивать отдельно.

- [smart_event_update.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/9f14c503f977b467ed6ad963e2de59008bc0a435/smart_event_update.py): `EventCandidate` около 606; `SmartUpdateResult` около 1876; scope/anchor/location reviews около 6926/7123/7485; `_llm_dedup_adjudicator` около 13535; `_llm_merge_identity_gate` около 14208; facade `smart_event_update` около 17360; `retry_due_smart_update_candidates` около 17715. На срезе файл содержит 23 774 строки — это ориентир масштаба, не само по себе доказательство плохой архитектуры.
- [smart_update_identity.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/9f14c503f977b467ed6ad963e2de59008bc0a435/smart_update_identity.py): identity subjects, deterministic veto, merge verdict и подтверждение grounded distinct.
- [smart_update_state.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/9f14c503f977b467ed6ad963e2de59008bc0a435/smart_update_state.py): enum результатов, `begin_candidate_attempt`, `finish_candidate_attempt`, ack failure, claiming.
- [source_parse_contract.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/9f14c503f977b467ed6ad963e2de59008bc0a435/source_parse_contract.py): source/child evidence, frozen receipts и cache codec.
- [vk_intake.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/9f14c503f977b467ed6ad963e2de59008bc0a435/vk_intake.py), [source_parsing/telegram/handlers.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/9f14c503f977b467ed6ad963e2de59008bc0a435/source_parsing/telegram/handlers.py), [source_parsing/handlers.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/9f14c503f977b467ed6ad963e2de59008bc0a435/source_parsing/handlers.py): не ограничиваться core-файлом — проверить адаптеры и подсчёт успехов.
- [private_events_mcp/event_create.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/9f14c503f977b467ed6ad963e2de59008bc0a435/private_events_mcp/event_create.py), [event_create_adapter.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/9f14c503f977b467ed6ad963e2de59008bc0a435/private_events_mcp/event_create_adapter.py): внешние операции и отображение причин результата.
- [main.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/9f14c503f977b467ed6ad963e2de59008bc0a435/main.py): `add_events_from_text`, `schedule_event_update_tasks`, JobOutbox worker, `job_sync_vk_source_post` около 23726.
- [main_part2.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/9f14c503f977b467ed6ad963e2de59008bc0a435/main_part2.py): `sync_vk_source_post`, `edit_vk_post`; отдельно сравнить PR #791.
- [google_ai/client.py](https://github.com/onedayonemasterpiece/events-bot-new/blob/9f14c503f977b467ed6ad963e2de59008bc0a435/google_ai/client.py): действующий общий limiter/provider boundary. Не проектировать второй limiter.

### Канонические контракты и история

- [Smart Update](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/smart-event-update/README.md), [identity state machine](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/smart-event-update/identity-state-machine.md), [caller inventory](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/smart-event-update/caller-inventory.md), [fact-first](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/features/smart-event-update/fact-first.md).
- [Основной текущий инцидент](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/reports/incidents/INC-2026-11-02-intake-quota-vk-media.md), [исходный расширенный отчёт](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/reports/2026-11-02-events-ingestion-model-review.md).
- [Кинотеатр: опечатка адреса и дубль](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/reports/incidents/INC-2026-11-03-cinema-europe-address-duplicate.md).
- Сначала открыть [индекс инцидентов](https://github.com/onedayonemasterpiece/events-bot-new/blob/main/docs/reports/incidents/README.md), затем связанные regression records: `INC-2026-08-04-smart-update-identity-source-replay-corruption`, `INC-2026-08-10-smart-update-identity-terminal-loss`, `INC-2026-08-22-sos-dedup-veto-location-tyunin-farm`. Они объясняют, почему нельзя просто удалить identity gates и считать любой отказ разрешением CREATE.
- [Принцип простоты](https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/main/skills/devserver-simplicity/SKILL.md): для каждой добавляемой сущности объяснить реальный сбой и почему существующего механизма недостаточно.

Документация может отставать от runtime. В частности, enum/commentary говорят о provisional `RETRY_SCHEDULED` и линейном завершении, а `finish_candidate_attempt` отдельно сохраняет некоторые identity retry; caller inventory описывает default-off worker. **Проверь фактическую достижимость переходов**, а не выбирай удобный текст документа за истину.

## Подтверждённые наблюдения, которые аудит должен объяснить

### A. «Мелодии любви», 14 ноября: отказ не равен отсутствию полей

- Проверенная текущая афиша организатора: [VK wall-194927034_5002](https://vk.com/wall-194927034_5002), опубликована 3 ноября 2026. На изображении: 13 ноября «Кино в главной роли», 14 ноября «Мелодии любви», 27 ноября «Песни о любви», начало 18:00, Зеленоградск, Железнодорожная 1. Год берётся из контекста текущей публикации, на самой афише он не напечатан.
- В базе уже были 9775 «Мелодии любви» на 13 ноября и 9815 «Кино в главной роли» на 14 ноября. У них есть источники из производных Telegram-публикаций, включая собственные каналы.
- Scoped request `evt_op_hw6PvnyBvxh_UtG4U2y6FsTOK_U1zhl2` прошёл parser и full Smart Update; candidate 25994, decision 19099. Финал: rejected, Event не принят, jobs не созданы.
- Внешний код ошибки: `EVENT_REQUIRED_FIELDS_MISSING`. Реальная причина в candidate receipt: `identity_final_retry`; final decision: `distinct_not_grounded`.
- Raw LLM verdict предлагал `create/distinct_occurrence`, но одновременно записал конфликт дат 13/14 ноября в `blocking_conflicts`. Guard преобразовал это в `FINAL_RETRY`.
- Candidate ledger сохранил `RETRY_SCHEDULED`, `next_retry_at`; это **не доказывает**, что кто-то его автоматически обработает. Identity retry worker не включался и не разрешён как способ обхода проблемы.
- В подготовленном кандидате город стал «Калининград», хотя во входе явно «Зеленоградск». Для принятой карточки 9779 город остался правильным. Причина этого отдельного расхождения пока не установлена.

Вопросы: где ошибка промпта/схемы, где честная неопределённость источника, где адаптер искажает отказ? Может ли один механизм разрешения источников заменить каскад повторных трактовок? Не делать вывод «надо форсировать создание» или «надо перенести старую карточку» без доказательств. Официальная программа повторяется на разных датах; различие дат само по себе не доказывает перенос.

### B. 27 ноября: тот же штатный путь успешно обновил существующую карточку

Request `evt_op_oMI4xiGzlVrNd502ZMu49M9VoV7c6f-Q` принят как `MERGED` в 9779. «Верни мне музыку» обновлено до «Песни о любви», дата 27 ноября/18:00 и Зеленоградск сохранены. Есть source anchor 13915467 на исходную программу. Точная проверка даты/площадки нашла одну карточку, новый дубль не создан. Это положительный контроль для сравнения с A, а не основание переносить весь прежний каталог.

Original poster добавлен через существующий typed media edit `evt_op_OtTmrBIzM6ve8t5ehHnkbhDlzUadqiXo`, poster 35057. Automated review ждёт обычного feature budget reset 4 ноября 00:05 UTC. Само accepted edit не означает, что новая афиша уже появилась в публичных проекциях.

### C. Потеря доказательств при cached replay

Программа carrier 87457 / source packet 26103 была повторена **один раз**, ops 11601: 0 CREATED, 6 MERGED, четыре технических identity-отказа и два missing_location среди 12 детей. Шесть прежних источников и carrier links сохранены. Старый cached receipt потерял poster metadata/OCR, и exact replay превратился в повторный семантический проход. Codec исправлен PR #783 и доставлен; старые потерянные данные это автоматически не восстановило.

Проверить: что действительно является неизменным входным пакетом, где меняются fingerprints и stable child keys, какие данные нужны для exact replay и что можно убрать из вторичных копий. Не предлагать повторить весь пакет: среди детей уже есть принятые события и конфликты исторических данных.

### D. Ошибочная гранулярность события

Один концерт Trio Sveta в Бастионе: адрес Судостроительная 6/1, двери в 20:00, выступление в 21:00. Hint принял 6/1 за возможную дату, conditional verifier разбил один концерт на две сущности, Smart Update обновил 10418 и создал дубль 10451. Дубль объединён и лишние публичные проекции очищены. PR #785 изменил существующее правило verifier: hints — гипотезы, адреса не даты, admission/start одного представления не отдельные сеансы. Реальные before/after и separate-session/date controls прошли, полный изолированный VK → Smart Update replay дал MERGED 10418 и 0 CREATED.

Проверить ответственность producer/verifier/Smart Update за occurrence scope. Не возвращать широкое regex-склеивание разных показов или семантические решения по ключевым словам.

### E. Ошибка адреса и семантическая идентичность

Для Cinema Park Europe посты одного показа содержали «Гвардейский проспект 3» и «Театральная 30». Исправлены reference/alias и передача проверенной venue evidence существующему identity LLM; дубль объединён, публикации исправлены. Это положительный контроль: проверенные адресные данные могут разрешать typo, но не должны сливать действительно разные площадки/сеансы.

### F. Деградация моделей и потеря резерва в адаптерах

В этом инциденте несколько потребителей заново собирали model chain и теряли настроенный список резервов; исправлены текущие parser/topic маршруты. Реальный source parser получил Gemma HTTP 500, затем успешный Gemini 3.5 Flash Lite в пределах существующих двух физических отправок. Другой реальный Gemma parse занял около 101 секунды. Нет основания называть каждую 500/503 исчерпанием RPD всех ключей.

На текущем проде fallback list: `gemini-3.5-flash-lite,gemini-3.1-flash-lite,gemma-4-31b-it`; OCR fallback — `gemini-3.5-flash-lite`, emergency reserve override пуст. Stage budgets различаются; не предполагать универсальный лимит двух отправок на весь Event или на каждый stage. Remote guide scan отдельно потерял два поста из-за 503; его `_secrets_payload` не передаёт общий fallback list, а документация намеренно оставляет screen на Gemma. Это повод проверить последовательность контракта, не разрешение молча менять все стадии.

Проверить: кто единственный владелец маршрута/attempt budget, где вложенные retries и последовательные LLM-проверки съедают время, какая минимальная централизация действительно удаляет дублирование. Не проектировать второй limiter и не обходить общий `reserve → mark_sent → provider → finalize`.

### G. Принятие Event и успешная публикация сейчас могут расходиться

Для 9779 canonical acceptance подтверждено, но live VK [wall-231920894_12213](https://vk.com/wall-231920894_12213) всё ещё содержит старое название. `edit_vk_post` определил старый пост как uneditable, вернул False/None; caller проигнорировал различие и записал новый hash/успех. Publication status затем показал текущую ревизию как applied. Это подтверждённая механическая ошибка, не неопределённость семантики. PR #791 передаёт существующую постоянную ошибку в существующий scoped outbox handler, не создавая новый пост или retry loop. На срезе исправление ещё не доставлено.

Медиа feature budgets отдельно от Google RPD удерживают часть media review/Telegraph/calendar до 00:05 UTC. Не скрывать media dependency под «Event опубликован». Участок JobOutbox важен как граница Smart Update, хотя аудит не должен перерасти в переделку всех publisher-ов.

## Что именно проверить на упрощение

1. **Единый результат и его смысл.** Enum, legacy `status`, booleans `created/merged`, `event_id`, `diagnostic_event_id`, typed reason enums, candidate ledger и MCP error mapping. Где представлений одной истины больше, чем необходимо? Составить таблицу достижимых исходов, кто их создаёт и кто интерпретирует.
2. **Линейность и liveness.** Что реально происходит после `FINAL_RETRY`, technical failure и ack failure? Где есть лишь durable запись без исполнителя? Где ошибка одного ребёнка блокирует другие? Предложить замкнутый путь через существующие средства, не always-on identity worker как автоматический ответ.
3. **Владение семантическим решением.** Producer hints, conditional verifier, scope/anchor/location/eventness review, match/create, merge gate, final adjudicator. Какие решения повторяются или отменяют друг друга? Для удаления каждого stage назвать проверенный инвариант, который всё равно будет защищён.
4. **Grounding и источник истины.** Source text, OCR, reference venue, legacy description, fact log, generated summaries, собственные Telegram/VK публикации. Где производный текст начинает подтверждать сам себя? Как разрешать более свежий первичный источник и старую производную карточку без новой универсальной системы trust scores?
5. **Exact replay и частичные программы.** Stable child/occurrence identity, fingerprints, source binding, cached input, partial accepted children. Сохранить корректную идемпотентность без повторной semantic работы по идентичному пакету и без ложного exact match после изменения доказательств.
6. **Транзакции и границы эффектов.** Domain write, candidate acknowledgement, EventSource binding и JobOutbox. При сбое между ними можно ли доказать принятый Event? Какие существующие транзакции/constraints достаточны вместо дополнительных проверок/таблиц?
7. **Стоимость фактического прохода.** Для нескольких реальных source→terminal traces составить последовательность model calls, physical sends, ожиданий и токенов по stage. Не выдавать накопительный массив timing за длительность одного ребёнка, один trace — за дневные расходы, backlog rows — за число потерянных событий.
8. **Тесты и диагностика.** Какие тесты проверяют наблюдаемое поведение, а какие закрепляют случайную структуру? Нужен короткий набор различающих regression controls, плюс production acceptance; не сотни тестов, повторяющих реализацию. Отметить существующие baseline failures отдельно.

## Что нельзя потерять при упрощении

- Семантика идентичности/eventness/venue/dates остаётся LLM-first с grounded evidence. Не заменять смысл широкими regex/keyword rails.
- Неуверенность модели, отсутствие match или diagnostic Event ID не разрешают CREATE и не считаются успехом.
- Разные реально посещаемые сеансы и разные площадки нельзя склеивать ради уменьшения дублей.
- Повтор принятого пакета не создаёт новый Event; частичный успех детей не затирается ошибкой соседа.
- Явные product exclusions отличаются от технической недоступности. Нельзя «улучшить метрику» переименованием ошибок в no-event.
- У продукта нет ручного Event-review оператора. Не предлагать новую очередь «нужен человек» как нормальный успешный финал. Не включать ранее отключённый identity retry worker без отдельного решения владельца.
- Один действующий shared limiter и существующий JobOutbox. Не обходить RPD/TPM, feature budgets, provider restrictions или source ownership.
- Green tests/merged PR/accepted Event/публичная актуальная ревизия — разные утверждения. Для каждого нужна подходящая проверка.
- Не прерывать здоровые импорты, не запускать process-local CLI параллельно обычному heavy job, не изменять чужой сервис или незавершённый worktree.

## Формат результата аудитора

Начать с 5–10 главных выводов. Для каждого:

| Поле | Что требуется |
| --- | --- |
| Приоритет / уверенность | P0/P1/P2, подтверждено / гипотеза / не хватает данных |
| Наблюдаемый ущерб | Конкретный путь, событие, источник; если влияние не измерено, так и написать |
| Доказательство | Commit + файл/функция/строка; для live-утверждения — trace/receipt/время |
| Корневая причина | Почему её создаёт действующий контракт, а не только описание симптома |
| Минимальное изменение | Что убрать/объединить/исправить в существующем пути |
| Сохранённые гарантии | Как не получить replay corruption, дубли, неверную склейку или потерю provenance |
| Размер и риск | Оценка файлов/строк, миграции, rollout/rollback; без выдуманной точности |
| Одна различающая проверка | Что падает до правки и проходит после; нужный отрицательный контроль |
| Live acceptance | Как понять, что пользовательский результат изменился в проде |

Отдельно выдать:

- карту реально достижимых переходов и список расхождений кода/документации;
- **список удаления**: код/ветви/повторные решения, которые можно убрать, и почему;
- 3–5 небольших последовательных PR вместо общего refactor; сначала операционное уменьшение потерь, затем упрощение;
- где достаточно честного исправления результата/диагностики, а где это не решит пропуски событий;
- что сознательно **не менять сейчас** и какие данные ещё нужны;
- таблицу «новая сущность → конкретный наблюдаемый сбой → проверенный существующий механизм → оставить/удалить» для любых предлагаемых новых состояний, флагов, retries, queues, checks или cache.

Не надо соглашаться с гипотезами Codex из этого файла. Лучше опровергнуть их конкретным кодом/контрпримером. Если доступа к репозиторию или live-доказательствам нет, явно ограничить выводы: по этому брифу можно сформулировать вопросы, но нельзя выдать неподтверждённый диагноз как факт.

## Доступ к доказательствам

Активные incident receipts на devserver хранятся в retained-каталоге `/home/dev/artifacts/events-bot-new/20261102T233947Z-intake-recovery-20261103/`. Этот локальный путь не означает, что удалённый аудитор имеет к нему доступ. Недостающие точечные обезличенные выдержки запрашивать в координационном треде; production DB целиком, `.env`, приватные MCP endpoints, signed preparation refs, токены и cookies туда не отправлять.

Полезные имена evidence: `programme-melodii-v3-terminal.json`, `programme-melodii-v3-decision.json` (санитизирован), `programme-existing-exact-source-1925.json`, `programme-pesni-accepted.json`, `programme-pesni-canonical-1937.json`, `programme-pesni-vk-live-stale.json`, `programme-pesni-publication-1942.log`, `phase2/programme-receipts-1624.json`, `trio-shadow-replay.log`, `vk-permanent-edit-before.log`, `vk-permanent-edit-boundary-final.log`.
