# Голосовой разговорный поиск — техническая спецификация v1

Дата: 2026-09-05. Статус: **спроектированное решение для implementation review; runtime не реализован этим документом**. Владелец продуктового поведения — [agent-assisted-event-discovery.md](agent-assisted-event-discovery.md); обычного Search — [README.md](README.md). Этот файл владеет техническими деталями голоса/диалога, а не вторым поиском, профилем, общим UI-shell или лимитером.

Документ закрывает переход от гипотезы к решению: конкретный путь обработки, поля операций, правила конкурентности, данные/сроки, ошибки, интеграция, тесты и последовательность реализации. Имена новых HTTP endpoints/модулей ниже — выбранный проект v1, не утверждение об их текущем существовании. Технические defaults помечены как предлагаемые; изменение существующих общих правил требует адресного совместимого diff, а не скрытого обхода.

## 1. Принятый продукт и границы v1

Голос и текст — два входа одного разговорного поиска. Полезный результат — лента самостоятельных разделов: название выборки → исходный вопрос → краткий/раскрываемый ответ → обычные EventCards. Последующий завершённый ответ не стирает предыдущий. Фактический вопрос об адресе/транспорте может дать explanation-раздел без карточек. Во время обработки пользователь может добавлять речь.

V1 включает: интерпретацию естественного запроса; последовательные уточнения и исправления; ограничения и отрицания; ленту ответов с независимой пагинацией; выбор старого раздела для уточнения; восстановление после сетевого/процессного сбоя; понятные состояния; короткую память и opt-in возврат к истории; динамическое управление доступом к ресурсу.

V1 не включает: автономную регистрацию/покупку, произвольные инструменты/MCP-петли, фоновое бесконечное прослушивание, обязательную озвучку ответов, новую подписочную платформу, профиль по одному голосовому упоминанию, открытый web research. Сохранение/календарь/реакции используют существующие CTA. Голосовые изменяющие команды — последующий пакет с отдельными подтверждениями, а не ещё один необъявленный action executor.

Auth-only остаётся текущей политикой. Гость видит возможность и обычную навигацию, но не запускает audio upload/ASR/Search; после входа нужен явный submit. Иные голосовые лимиты не означают неявное разрешение гостевых запросов.

## 2. Прочитанная база и точные места интеграции

Продуктовая correction-версия: PR #587, `bce0a4ae06d75651aff09ef3657d8272113b2267`. Existing runtime/source pointers проверены на `b8f463f5c35fa62befcfed171a7a8a0886af20f7`; перед кодовой интеграцией читать current main и active integration branch, не откатывать их к этим SHA.

| Существующее место | Использование в v1 |
|---|---|
| `site/src/components/AuthorizedEventSearch.astro` | Существующий Search entry/Auth/result presentation. Новый режим не обрезает речь по legacy maxlength 180. |
| `site/src/lib/staticSiteAuth.ts` | Единственная identity/session; без отдельного login-клиента ассистента. |
| `site/src/lib/backendOperationCatalog.ts` | Добавить явные voice operations/capability; неизвестные операции по-прежнему fail closed. |
| `site/src/lib/resilientDataClient.ts`, `resilientSupabaseTransport.ts` | Переиспользовать семантику selected-once/safe-read; текущий transport специфичен для Supabase, значит новый origin требует явного route adapter, а не произвольной подстановки URL. |
| `supabase/functions/event-search/index.ts` | Выделить общий domain retrieval/validation/presentation adapter. Старый endpoint продолжает работать с прежним wire contract. |
| `supabase/functions/event-search/google-quota.ts` | Тот же project/model ledger и reserve→mark_sent→finalize. При выделении shared owner оставить compatibility re-export, не копию реализации. |
| `supabase/functions/event-search/occurrence-families.ts` | Стабильная группировка повторов/сеансов до отображения и логической пагинации. |
| `docs/architecture/personalization-data-ownership.md` | Supabase владеет identity/consent/current profile; YDB не второй профиль; аудио не raw telemetry. |
| `docs/operations/static-site-autotest-strategy.md`, `docs/testing/static-site-autotest-scenarios.v1.yml` | Единые уровни и реестр; план из §15 переносится сюда по мере реализации. |
| `.github/workflows/ci.yaml`, `search-production-health.yml`, `site/e2e/search/` | Расширение действующих тестов, не новая QA-платформа. |

Уже существующий `operation.get` в my-data-hub нельзя автоматически считать пользовательским API сайта: это иной control plane и иной permission scope. Для публичной функции нужны описанные здесь owner-scoped receipts.

## 3. Выбранная архитектура

```text
Static Astro + existing Auth + shared shell
  → selected first-party voice origin
  → authenticated bounded intake
  → durable utterance receipt + temporary audio spool
  → ordered worker in that same service
       → shared limiter → approved Lite interpretation
       → validated IntentPatch
       → shared Search domain / Supabase pgvector + filters
       → optional bounded grounded Lite presenter
       → atomic answer-section commit
  ← buffered JSON receipts / bounded delta reads / canonical card projections
```

Размещение — отдельный небольшой сервис на Devstand, при условии проверки его публичной доступности и эксплуатации. Это обычный first-party сервис, не Codex/DevCoveer/MCP agent. Не размещать audio/LLM обработку в Fly web. Kaggle используется для offline fixtures и действующего published-preview build, не как синхронный backend пользовательского запроса.

Предпочтительный implementation shape: TypeScript service и worker с инъекцией Auth/DB/limiter/provider/retrieval adapters; один deployment unit, без Redis/Kafka, второго Postgres на Devstand или новой оркестрационной платформы. Worker claim хранится в уже используемом Supabase, не только в памяти процесса. Common Search domain извлекается из Edge-обработчика с явными dependencies вместо Deno.env внутри pure logic; Edge и service вызывают его, а не HTTP-сами-себя.

Auth проверяется до privileged DB. Сервис использует существующий first-party JWT-validation путь; обязательны signature/issuer/audience/expiry, проверенный subject, точный Origin/CORS allowlist, TLS, request size/time limits. Никаких service-role/provider keys в браузере. Cookie-путь, если появится, требует той же CSRF/origin политики; CORS не является проверкой владельца.

### API Search нельзя переиспользовать слепо

Нельзя отправить весь transcript в legacy `event-search` и молча обрезать его до 180 символов. Semantic query для embedding может быть компактным представлением цели; строгие условия передаются отдельным типизированным объектом и применяются доменным слоем. Извлечение общего сервиса сохраняет старые consumers; новая policy `voice_search` не должна дважды списываться как обычный пользовательский Search и как голосовой ход. Actual embedding/model attempts учитываются отдельно в общем ledger.

## 4. Доменная модель

### Идентификаторы и версии

| Поле | Смысл |
|---|---|
| `conversation_id` | Серверный UUID; owner определяется только Auth. |
| `epoch` | Монотонная граница reset/delete/закрытия текущей управляющей сессии; старые результаты не могут применяться после неё. |
| `writer_id`, `client_sequence` | Серверно выданная identity текущего writer и последовательность ввода; не identity пользователя. |
| `utterance_id` | Стабильный UUID одной принятой реплики; одновременно ключ её receipt. |
| `previous_utterance_id` | Зависимость от предыдущего ещё обрабатываемого фрагмента; допускает приём до его расшифровки. |
| `draft_id` | Один строящийся раздел текущей цепочки уточнений. |
| `context_revision` | CAS-версия структурированного намерения. |
| `accepted_through`, `processed_through` | Непрерывно принятый/интерпретированный префикс последовательности, не максимум всех увиденных номеров. |
| `section_id`, `parent_section_id` | Самостоятельный ответ и база уточнения; история хронологическая, не tree UI. |
| `result_set_id`, `membership_revision` | Зафиксированная логическая выборка и её состав. |
| `catalog_revision`, `corpus_revision`, `policy_version` | Версии данных и исполнения; входят в evidence/cache signatures. |

Различать `viewed_section_id`, `refinement_base_section_id` и `pending_draft_id`. Первый меняется при прокрутке; второй — по понятному действию пользователя; третий — состоянием обработки. Они не заменяют друг друга.

### IntentState

Один bounded объект содержит `semantic_goal`, `when`, `where`, `budget`, `formats`, `themes`, `audience`, `exclusions`, `soft_preferences`, `rejected_event_ids` и provenance по каждому изменённому полю. Бюджет различает `free`, `max_amount` и `unconstrained`; unknown price не free. Время — ISO границы с timezone и исходным anchor высказывания. Locality/venue IDs разрешает host по существующей taxonomy, не произвольный ID от LLM.

IntentPatch допускает только разрешённые поля и операции `set`, `clear`, `add`, `remove`. Для одиночного поля `set` заменяет прежнее значение. «Не Светлогорск, а Калининград» — замена, не невозможный AND. «Можно платно до пятисот» снимает free и задаёт сумму. «И без концертов» добавляет исключение. В одном patch дубли/конфликтующие операции для одного singleton поля отвергаются, а не решаются случайным порядком ключей JSON.

Scope запроса: `new_search`, `refine_selection`, `continue_draft`, `explain_selection`, `explain_event`. Определённый пользователем explicit target приоритетнее догадки модели. «Начнём заново» сбрасывает текущие ограничения, но не удаляет исторические разделы; «Удалить историю» — отдельное явное действие.

### AnswerSection

`kind=results|explanation|mixed`; `status=draft|ready|empty|error`. Ready explanation не получает пустую grid и статус zero results. Поля: title, исходный user_query, effective intent, answer blocks, parent, ordered membership, own pagination cursor, versions, created_at, expiry, generation/degraded markers. Failure state не маскируется под пустую успешную выборку.

Допустимые блоки: `text`, `event_group`, `event_annotation`, `location`, `map_actions`, `suggested_replies`, `uncertainty`. Text — безопасный ограниченный Markdown subset; HTML/произвольные ссылки запрещены. ID события/места/action принадлежит allowlist фактов текущей операции. Factual annotations ссылаются на host-provided `fact_id`; ссылка сама по себе не доказывает, что предложение верно, поэтому числа/цены/время/адрес рендерятся из typed facts, а смысловое качество проходит независимые fixtures/live checks.

## 5. HTTP contract v1

Префикс: `/v1/assistant`. Это новый explicit capability в существующем OperationCatalog. Точные origin/route bindings выбираются из deployment config; произвольный client URL не принимается. Version header/body — `voice-search.v1`. Все ответы private/no-store, кроме публичного status manifest. Разделы не публикуются как индексируемые URL; query/transcript не помещаются в query string.

| Операция | Вход/ответ | Повторы |
|---|---|---|
| `POST /conversations` | request UUID, retention choice; возвращает conversation, epoch, writer, effective policy | Идемпотентное создание с Auth-owned key, 0 providers. |
| `POST /conversations/{id}/utterances` | multipart: metadata JSON + один audio blob ИЛИ JSON text; 202 receipt после durable acceptance | `selected-once`; тот же ID/body возвращает тот же receipt, не новый запуск. |
| `GET /conversations/{id}/utterances/{uid}` | receipt/status, stage, safe error, applicable revision, changed text if requested | safe-read; предназначен в том числе для ambiguous POST. |
| `GET /conversations/{id}/updates?after_revision=N` | bounded delta всех pending операций и новых section headers, not-modified marker | safe-read; один poll на активную беседу, не по запросу на каждую карточку/реплику. |
| `GET /conversations/{id}/sections?cursor=C` | страница исторических разделов, без полной истории за один запрос | safe-read; owner/epoch-aware cursor. |
| `GET /conversations/{id}/sections/{sid}/events?cursor=C` | следующая страница данного result set, canonical card projections | safe-read; cursor нельзя применить к другому section/user. |
| `POST /conversations/{id}/control` | request UUID + enum `cancel_draft|start_new_task|resume_section|reset_context` | Идемпотентные host actions, не LLM tool executor. |
| `DELETE /conversations/{id}` | request UUID; logical deny immediately + purge receipt | Идемпотентно, 0 providers; поздние ответы не воскрешают удалённое. |

Metadata обязательна: utterance_id, epoch, writer_id, client_sequence, previous_utterance_id/null, target (new/section/draft ID), client_created_at, locale, input kind. Для audio: SHA-256 bytes, declared MIME, byte size, duration, sample rate/channels. Всё сверяется сервером по содержимому и decoder probe с ограничением ресурсов. Хэш тела для idempotency — серверный HMAC нормализованной metadata и фактических audio/text bytes; секрет в аналитике не раскрывается. Подмена того же ID другим payload даёт 409.

`client_created_at` помогает понять относительную дату, но не авторизует запрос и не назначает TTL. Серверное received_at авторитетно; неправдоподобный сдвиг часов фиксируется и уточняется для значимой даты. Effective timezone по умолчанию Europe/Kaliningrad; иной явно выбранный контекст хранится отдельно от timezone компьютера/CI.

Receipt содержит: utterance_id, receipt_revision, accepted bool, phase, conversation epoch, accepted/processed prefix, draft/section refs, optional retry_after_ms, safe error code. `accepted=true` означает durable intake, НЕ найденные события и НЕ гарантию будущей доступности провайдера.

### Ошибки

| HTTP/code | Поведение UI/сервера |
|---|---|
| 401 `auth_required|session_expired` | Вход/восстановление; никаких автоматических платных повторов после него. |
| 404 `not_found` | Чужой и отсутствующий ресурс не раскрываются по-разному. Один 404 не является доказательством, что прошлый запрос никогда не был отправлен провайдеру. |
| 409 `payload_conflict|writer_conflict|epoch_conflict|base_conflict` | Получить состояние; сохранить локальное дополнение; не overwrite чужой revision. |
| 410 `context_expired|selection_expired|receipt_expired` | Видимый resume/new-search выбор, а не тайное применение старых условий. |
| 413/415/422 | Слишком большой/неподдержанный/битый ввод; до provider; исходник не изображается обработанным. |
| 429 `user_capacity|global_capacity` | Различать причины, не сбрасывать историю; next retry только по валидному admission. |
| 503 `limiter_unavailable|service_unavailable|provider_policy_blocked` | Provider fail closed; обычная навигация остаётся доступной. |
| receipt `outcome_unknown` | Отдельное состояние неопределённого внешнего исполнения. Не бесконечный spinner и не автоматический второй provider call. |

При timeout после отправки клиент читает receipt. Повтор upload того же UID допускается только после authoritative состояния `upload_incomplete`/`never_dispatched` и атомарной проверки сервера. Глобальное переключение транспортов ради rescue POST запрещено. Explicit повтор после unknown создаёт новое осознанное действие с `retry_of`; UI не обещает, что первая попытка не стоила ресурса.

## 6. State machine, догон и перезапуск

```text
receiving → queued → interpreting → interpreted → retrieving → presenting → completed
                 ↘ waiting_capacity ↗
terminal alternatives: rejected / failed / outcome_unknown / cancelled
```

У `completed` отдельно указано `presentation_applied`; ранее интерпретированная реплика может быть superseded для выдачи, не будучи потерянной из контекста. Cancellation до dispatch и после dispatch различаются в receipt и limiter evidence.

1. Intake проверяет Auth, owner, epoch, writer и capacity; создаёт receiving receipt до длительной обработки тела, ограниченно пишет временное аудио. После закрытого файла/контрольной суммы и короткого DB commit переводит в queued и только тогда отвечает 202. Незавершённое тело — не queued.
2. Один writer на беседу по умолчанию. Две вкладки получают разные conversation IDs. Открытие старой беседы read-only безопасно; для продолжения новый writer явно приобретает ownership epoch/CAS, старый writer прекращает mutations. Не связывать пользователя голосовым отпечатком.
3. Out-of-order U2 может ожидать U1, но `accepted_through` продвигается только по непрерывному префиксу. Gap даёт видимое missing-input состояние; сервер не пропускает потерянное «бесплатно» и не применяет отдельно «из них». Отменить отдельную реплику — явный skip marker, а не исчезновение sequence.
4. Worker берёт короткую DB lease с fencing token; внешние вызовы идут вне транзакции. Lease expiry сама по себе не разрешает повтор provider call. Для каждой стадии хранится общий limiter request UID и outcome.
5. Interpret U1, затем U2 с правильной базой. Неотправленные соседние дополнения можно batch-обработать в одном Lite-запросе с сохранением UID/order. Уже отправленный контент не пересылается на каждую паузу.
6. После interpretation берётся последний непрерывный префикс текущего draft. Retrieval/presenter получают именно его intent revision. Новое принятое дополнение отменяет применимость старого результата, но не его provider accounting и не текст U1.
7. Section commit — короткая транзакция: owner/epoch/fencing совпадают, processed_through покрывает требуемый accepted prefix, draft не superseded, context_revision и data revision проверены. Unique draft publication гарантирует один section. Если U2 атомарно принято до commit, U1-result не публикуется; если после commit — возникает следующий раздел. Это точная граница коалесценции, а не зависимость от времени перерисовки DOM.
8. Клиент применяет только монотонные receipt revisions своего epoch; section_id dedupe исключает повторный append после reconnect. Server commit уже сохраняет ответ даже при потере HTTP-доставки. Поисковая секция не определяется видимостью spinner.

### Контрольные гонки

- U1 ещё в ASR; U2 принята: два receipts, один draft; смысл U1 не теряется.
- U1 в retrieval; U2 принята: U1 retrieval может завершиться, но не примениться; новый поиск использует полный intent.
- U1 завершён; U2 — уточнение: S1 immutable, S2 с parent=S1.
- S1/S2 видны; пользователь выбирает уточнение S1: S3 в конце, parent=S1; scroll не меняет target.
- Во время записи выбран иной раздел: target фиксируется в момент начала фрагмента; UI показывает старую базу до завершения/отмены либо предлагает явное переключение, не переадресует произнесённое молча.
- Ошибка ASR U1, U2 зависит от неё: U2 остаётся принятой, но blocked_on_previous; пользователь исправляет недостающий текст. Нет догадки о потерянном условии.
- Редактирование уже опубликованного вопроса создаёт новый answer с `revision_of`, не переписывает историю/потомков без спроса.

### Crash semantics

После receiving/spool crash: проверка закрытого файла и hash, незавершённый intake допускает повтор того же UID только до provider dispatch. После pure retrieval crash: безопасный повтор чтения с сохранением revision. После mark_sent/до сохранённого provider outcome: unknown; не освобождать лимит как unsent и не делать blind rerun. После полученного и сохранённого provider outcome: продолжить последующую стадию без повторного ASR. После section commit/до ответа клиенту: вернуть уже существующий section.

Exactly-once внешний вызов при аварии между провайдером и DB не обещается. Гарантируются durable local identity, не более одного применения результата и консервативный учёт неопределённого dispatch. Удаление беседы/смена epoch всегда побеждает поздний callback.

## 7. Поиск, полнота выборки и достоверность

Порядок: validated intent → hard-filter eligibility → semantic ranking в согласованном корпусе → occurrence-family collapse → result membership → page projection. Если текущий retrieval сначала ограничивает top-K, доказать отсутствие потери hard-filter результатов на контрольных примерах; не утверждать полноту всего каталога по случайным K candidates.

`result_set_id` фиксирует полную логическую выборку текущего поиска, включая ещё не отрисованные страницы. Хранится ordered membership всех найденных подходящих canonical occurrence/family IDs плюс минимальные факты, по которым принято решение, policy/catalog/taxonomy revisions. Не копируется весь Event, медиа, embedding или canonical event DB.

`refine_selection` означает подмножество этого membership. Простое новое ограничение фильтрует весь parent set, не только первый экран. `new_search` может искать шире. «А можно и платно?» при снятии free — расширение: показать новую формулировку и искать по изменённым условиям, а не ограничиваться старым free-only membership. Host валидирует вид операции; неоднозначное расширение не делается тайно.

Если snapshot отсутствует/устарел либо новое semantic условие невозможно корректно проверить по сохранённым фактам, дать явный «Обновить поиск по этим условиям». Не обещать воспроизведение прежней выборки через текущую изменяемую БД. Safety ceiling членства — configurable, стартовый ориентир 4096 IDs; достижение потолка помечает `membership_complete=false`, а не выдаёт top-K за всю выборку. В этом случае «из них» ограничено честно описанной найденной частью или запускает явное расширение. Это технический размер результата, не лимит диалога.

Отображение истории сохраняет её состав и факты ответа. Card projections могут быть актуализированы отдельно с меткой изменения; отменённое/удалённое событие получает tombstone/status overlay, а не исчезает без следа. Любое текущее действие проверяет live state. Старое «бесплатно» после исправления цены не показывается как актуальная гарантия: важное изменение сразу отмечается; для новых рекомендаций нужен согласованный свежий snapshot.

Explanation выбирает факты только из canonical event/location/transport evidence. Для адреса не ждать полного геокаталога, если trusted address уже есть. Название, stop ID, расписание, время пути и расстояние — разные поля; модель не выводит неизвестное из знакомого названия города. «Рекомендует Культурная чайка» допустимо только по подтверждённому endorsement, не mention/advertisement. Незавершённый source directory не блокирует основной voice-search v1.

### Модельные стадии

Interpreter получает роль и closed schema, текущую structured base, небольшую историю, locale/time anchor и входное text/audio. Возвращает transcript, scope proposal, IntentPatch, нужное уточнение/unsupported. Не получает admin tools, SQL или provider secrets. Event descriptions всегда данные, не system instructions.

Presenter получает только validated input и bounded retrieved facts, allowlisted IDs/fact IDs. Для стандартной выборки достаточно шаблонного резюме; для содержательного совета/транспорта допустим отдельный Lite-вызов. Не принуждать к одному вызову, если это означает выдумывание ещё не полученных фактов. Не наследовать Gemma overflow старого verifier в voice policy без owner decision; embedding остаётся существующей отдельной policy.

Structured output проверяется как синтаксически, так и семантически: JSON schema не доказывает фактическую точность. Provider поддерживает только свою часть JSON Schema; canonical runtime validator полнее provider prompt schema. Невалидный ответ не запускает бесконечную repair chain: полезные canonical results показываются с честным degraded status; незавершённый intent просит одно необходимое уточнение.

## 8. Capture и пользовательские состояния

Два автомата независимы: `capture=off|permission|listening|speech|paused|error`; `work=idle|uploading|queued|interpreting|searching|answering|ready|error|unknown`. Слушаю не означает «модель уже распознала»; queued не означает готово.

Основной режим v1: явное нажатие запускает видимую foreground voice-session. VAD отмечает речь и конец фразы; после отправки фразы микрофон может принимать догон без ожидания сети. Видимая кнопка stop завершает сессию и tracks. При inactivity, hidden/pagehide, logout или потере permission захват прекращается; сервис не обещает, что браузер даст flush при каждом системном уничтожении процесса. Уже принятый сервером контекст восстанавливается независимо от микрофона.

Предпочтительный минимальный capture backend: getUserMedia → AudioWorklet bounded PCM ring → проверяемый локальный VAD → самостоятельные WAV-фразы, mono PCM16, 16 kHz после проверенного resampling. Так pre-roll не теряет начало «не», каждая фраза декодируема и длинная тишина не передаётся. Raw sample rate устройства нельзя только переименовать в 16 kHz. VAD/переходы одинаковы для тестового WAV и микрофона; energy gate сам по себе не считать доказанным качественным VAD.

Это инженерный выбор для первого проверяемого пути, не запрет более экономного native Opus/AAC: при наличии уже пригодного web-recording модуля предпочесть его после тех же boundary tests. Native MediaRecorder adapter должен доказать MIME negotiation и декодируемость целого utterance; произвольные timeslice chunks не всегда независимые файлы. Не добавлять одновременно два обязательных capture backend. Клиентская компрессия после доказанного capture рассматривается по uplink/CPU measurements, не как причина переносить transcoding в Fly.

Стартовые калибровочные значения: pre-roll 250 ms, post-roll 350 ms, конец фразы после 1200 ms без речи, завершение бездействующей mic-session после 30 s с явным статусом. Они конфигурируемы и проверяются на отрицаниях/паузах, не являются принятыми владельцем SLA. VAD не определяет смысловое окончание вопроса; последующая речь присоединяется по правилам draft.

Технические ceilings v1 для rehearsal: 180 s voiced audio и 8 MiB на самостоятельный upload; 16 MiB pending browser audio. При подходе к границе — закрыть валидный сегмент и продолжить цепочку, если capacity есть; ни слово, ни blob не обрезать молча. На фоне заполненной очереди ясно остановить mic и сохранить уже принятое. In-memory blobs живут отдельно от 64 KiB общего persistent browser-state бюджета; долговечная offline audio очередь по умолчанию отсутствует.

WAV имеет цену uplink: mono PCM16 16 kHz = 32 000 B/s, 30 s ≈ 0,96 MB до headers/base64. Это расчёт, не замер. Аудио идёт напрямую в выбранный voice origin, не в Supabase Storage/Realtime и не через Fly. Встроенные caption/конечная расшифровка допускают ручное исправление; 8192 characters text ceiling отличается от legacy 180 и даёт явную ошибку, не truncation.

Первый ответ использует общие skeletons; уточнение сохраняет старые sections. Статус обновляется по фактической стадии. Нет fake percent, притворного streaming transcript или дублирующих aria-live announcements. Типичный текст: «Ищу события. Можно дополнить», «Дополнение принято», «Не удалось распознать первую фразу», «Ответ готов ниже».

## 9. Граница с системой островов

Сквозной владелец — `pattern.detached-chrome-control-islands` в lovekgd-design-system, исходный PR #47. Полный design handoff: [20260905-floating-islands-system-design.md](../design-system/window-prompts/20260905-floating-islands-system-design.md). Этот Search не определяет глобальные координаты, visual tokens или порядок всех chrome layers.

Минимальный adapter contract, имена предлагаемые:

```text
Search → Shell:
  role=voice_composer / answer_context
  instance_id, scope_section_id, expanded, input_focused
  recording, stop_action_available, status_kind
  measured_size, preferred_placement, restore_focus_target

Shell → Search:
  effective_top_inset, effective_bottom_inset
  occupied_rects, viewport_rect, layout_mode
  layer_role, permitted_expansion, overlay_interaction_state

Semantic events:
  select_refinement_base(section_id)
  request_reveal_section(section_id, reason=submit|explicit_jump)
  announce_status(text, category)
```

Только общая система разрешает конфликт с nav/header/CTA/drawer/consent/toast/keyboard. Не вводить универсальный глобальный event bus с произвольными payloads: это маленький typed adapter над фактическим shell API. Названия согласует окно островов без изменения смысла доменных полей.

Заголовок section-contained sticky ниже занятой верхней области; следующий заменяет предыдущий, обратный scroll восстанавливает старый. Pin только компактный title/context, не длинный вопрос/ответ. Один semantic heading, без focusable дубля. Composer не закрывает последний CTA и не перемещает stop под пальцем.

По submit создаётся draft boundary и один переход к его заголовку. Scroll anchor — `section_id + element_id + relative_offset`, не только абсолютный Y. Если после submit пользователь прокрутил историю, автоматическое следование отменяется до explicit jump; новый ответ даёт «Новый ответ ↓». Поздние изображения/Подробнее/пагинация старого section и Back не сбрасывают выбранное место. Чтение истории не меняет refinement target.

Обычно достаточно 2–4 содержательных предложений и раскрытия подробностей, но нельзя скрывать важную неопределённость/условие регистрации ради compactness. Не уменьшать шрифт вместо правильного ограничения композиции.

## 10. Хранилище, владение, память

Выбран primary — существующий Supabase personalization owner. Предлагаются четыре логические сущности в private schema: conversation; utterance/operation receipt; answer section; result members. При наличии равнозначной уже действующей receipt/entity таблицы расширить её, задокументировав mapping, а не добавлять дубль.

- Conversation: owner, epoch, writer, high-watermarks, latest section, active intent/revision, activity/expiry/retention choice.
- Utterance: UID, conversation/writer/seq, predecessor, payload HMAC, phase, lease/fence, attempt refs, safe error, temporary audio pointer, transcript на разрешённый срок.
- Section: committed answer/intent/parent/versions, protected original question, optional revision_of, pagination metadata/expiry.
- ResultMember: section, canonical occurrence/family IDs, rank, bounded eligibility facts, trusted card reference. Unique(section, member identity), без полного Event/media/vector.

Unique constraints: (conversation, writer, client_sequence), UID, one committed publication per draft. Индексы owner+updated_at, queued next_attempt_at, expiry, section+rank. Foreign ownership и ancestry проверяются транзакционно; нельзя указать чужую parent section даже со своим conversation_id. Нет межпользовательских branch-links.

Raw tables не выдаются browser напрямую. Для exposed RPC/views — явные grants, RLS и security boundary. Privileged role обходит RLS, поэтому она требует собственного owner check; security-definer функции имеют фиксированный search_path и restricted execute. DB security тестируется реальными ролями, не grep исходников.

### Предлагаемая retention policy v1

| Данные | Выбранный default для реализации/проверки |
|---|---|
| Неявный active task | 30 min бездействия или смена локального дня — переход в needs_resume, не уничтожение видимой истории. |
| Без включённой истории | Server conversation доступна только в active session; expiry не позднее 24 h от создания; после inactivity удаление по retention worker. |
| Включённая история | Committed sections/исходные вопросы/минимальные membership facts до 7 суток; явное resume обновляет задачу, а не бесконечно продлевает жизнь каждой старой реплики. |
| Внутренний хвост реплик | Последние 6 реплик либо 12 KiB model-context, с сохранением structural constraints; эти пределы не урезают видимую историю. |
| Аудио | Удалить после durable transcript или отмены; absolute safety TTL 1 h для error/spool; без opt-in архивирования. |
| Dedupe evidence | Компактные UID/seq/digest/attempt refs до завершения retention conversation. После очистки full receipt high-watermark всё ещё запрещает повторно исполнить старый seq. |
| Профиль | Existing lifecycle, только явные подтверждённые preferences/действия; общий reset/delete не копируется. |

Это предлагаемые числовые defaults для review, не уже применённые/утверждённые лимиты. UI consent/history control должен точно отражать, что исходный вопрос хранится вместе с section; удаление processing transcript при сохранённой копии вопроса не называется полным удалением текста.

Logical expiry/deletion отсекает чтение немедленно, purge выполняется bounded job; target lag ≤1 h с alert. Backup/провайдер retention описывается отдельно: удаление local spool не доказывает удаления у внешнего сервиса. На logout mic/локальная доступная проекция и writer прекращаются; opt-in история аккаунта не уничтожается, но старый callback не появляется в новой identity. На delete отзываются pointers и очищаются live caches; anonymized aggregates следуют существующей политике.

«Завтра» сохраняет исходный absolute anchor. Возобновление через день показывает дату и проверяет актуальность; нет автоматического сдвига на ещё один день. История≠active context≠постоянный профиль. Не нужен vector memory index или голосовой биометрический идентификатор.

## 11. Ресурсная политика и egress

Один shared provider ledger; отдельная `voice_search` product allowance допускает щедрый динамический доступ. Две цели одновременно: использовать свободный разрешённый ресурс и не дать одному клиенту захватить весь interactive queue.

Порядок admission: cheap abuse/input/auth check → duplicate receipt lookup → пользовательский token bucket по рабочим cost units → fair queue/lease → reserve конкретного provider stage → mark_sent → call → finalize actual. Дубликат не создаёт новый product charge. Согласованные technical fragments одной принятой реплики не становятся отдельными «поисками». Actual sent calls учитываются всегда; сообщения UI о remaining не подменяют серверное решение.

Начальная реализация policy — pure decision function над версиями config и snapshots: remaining RPM/TPM/RPD, spend headroom, active users/queue, estimated stage cost, oldest wait. `effective_user_burst = min(approved_user_ceiling, base_burst + fair_share_of_spare_capacity)`. При pressure прекращается заимствование свободной ёмкости, deferred discretionary batch/test jobs уступают interactive на границах admission. Новые пользователи получают обслуживание через round-robin/aging, не только старые долгие диалоги.

Hysteresis: переход в pressure требует нескольких плохих наблюдений, выход — устойчивого восстановления; исходные windows/thresholds задаются versioned config и fake-clock tests. Стартовые user/RPM/spend numbers заполняются из фактического headroom и разрешённого бюджета перед enablement — не из придуманного числа «три поиска». Нельзя резервировать неограниченное будущее диалога. Lowering policy не откатывает accepted text/context или уже выданную lease, кроме явной emergency safety остановки с честным status.

Stage timeouts и queue deadline ограничены; при превышении work становится failed/waiting user action, не висит вечно. Cost-bearing retry после неизвестного исхода не автоматический. CI concurrency ограничивает CI, но не заменяет cross-system limiter. Qwen CPU slots/runtime — другой измеритель ресурса; общий policy layer не притворяется, что CPU расходует Gemini RPM.

Supabase egress бюджетируется по actual response bytes всех service classes, в том числе DB→voice service. Не отправлять полный history, membership, vectors или карточные медиа на каждый статус. Delta polling только активной беседы: начальный интервал 1,5 s с backoff до 6 s, jitter, остановка в hidden/terminal; лёгкий process cache обслуживает повторные version reads, не меняя owner boundary. При потере процесса polling продолжает читать authoritative receipt через bounded DB path.

Пример стоимости памяти — расчёт, не измерение: 50 000 ходов × 8 KiB исходящего состояния ≈0,41 GB; ×50 KiB карточной выдачи ≈2,56 GB. К этому добавляются receipt reads, Auth, cache/прочий трафик. Перед release нужны замеры bytes/turn, p95 payload, retention row counts, purge lag и cache hits. Небольшой текст не повод обещать отсутствие расхода.

## 12. Кэш и доступность

Cache key Search включает effective normalized intent, parent-membership signature для subset, catalog/corpus/taxonomy/prompt/policy versions, locale/time anchor и profile revision при персонализации. Query hash — HMAC, не угадываемый plain SHA. Общий cache не содержит чужих вопросов/личных answer sections; private result cache partitioned по проверенному owner. Transient degraded cache отдельно маркирован, чтобы outage не размножал одинаковые попытки.

Предлагаемый новый status object: `/data/assistant-availability.v1.json` в существующем CDN/bucket. Поля: schema_version, enabled, state available/degraded/unavailable/unknown, checked_at, valid_until, minimum_client_contract, optional coarse retry_after. Никаких JWT, raw errors, key aliases, personal quota.

Лёгкий service/health observer публикует status, не запускает Astro rebuild. Стартовый update/cache интервал 30 s, valid_until 90 s; потеря publisher превращает состояние в unknown, а не сохраняет green. Shallow process health отдельно от возраста последнего deep canary; manifest не изображает каждую проверку настоящим ASR запросом. Existing protected health workflow проверяет глубокий путь реже через общий лимитер. Не использовать GitHub cron как точный 30-second realtime ticker.

Браузеры читают CDN с кэшем, без cache-busting и per-tab direct Devstand ping; личный допуск проверяется сервером при submit. На активном разговоре outage не удаляет composer/историю; доступен текст либо обычные фильтры. Vector-only fallback обещается только когда сам retrieval действительно доступен, не при любом provider outage.

## 13. Security, privacy и эксплуатационные prerequisites

Публичное enablement отдельно требует проверки provider eligibility, возрастной/региональной применимости, режима обработки реального аудио и data-flow/localization обязательств. Предыдущий обзор не заменяет проверку конкретного договора/проекта. Не обходить ограничения перемещением URL, чужими ключами или скрытой сменой модели. Разработка на synthetic/mocked данных не доказывает public admission.

Threat cases: чужой conversation/section UID; forged owner; replay sequence; payload substitution; raw Markdown/HTML injection; prompt instructions из события; rogue map URL; giant/decompression audio; resource exhaustion; leaked client logs; сохранённая история на общем устройстве; compromised privileged RPC. Ограничить decoder CPU/wall time, MIME sniffing, CORS, content security, upstream destinations и вывод внутренних ошибок. VAD не DDoS-защита.

Deployment config manifest перед включением обязан содержать verified service origin/health, Auth issuer/audience, existing DB/limiter bindings, model policy capability, voice allowance ceilings, exact code/prompt/schema/corpus revisions и purge/alert настройки. Отсутствующий обязательный binding держит voice flag OFF. Это конкретная эксплуатационная проверка, не необходимость заново придумать архитектуру.

Наблюдаемость: stage duration, queue, accepted/processed gap, duplicate/rejected counts, stale result suppressions, provider actual usage, outcome_unknown, egress, purge lag, end-to-end first useful event. Без raw audio/text/токенов в public logs/общей аналитике. Prompt/corpus/version IDs позволяют воспроизвести тест, не публикуя персональные данные.

## 14. Пакеты реализации и границы записи

| Пакет | Содержание | Где и кто | Definition of done |
|---|---|---|---|
| A — contract core | Type definitions, IntentPatch validator, timeline reducer, fake-clock policy decision, fixtures | ChatGPT, feature branch; будущие `site/src/lib/assistant/`/shared domain files | Реальные unit/property tests, в том числе отрицательные; без провайдеров. |
| B — durable intake | Owner-scoped endpoints, DB migrations/RPC/lease/receipts, queue/restart, existing limiter adapter | Кодовый агент, proposed `services/site-assistant/` + current Supabase migration conventions | Test DB grant/RLS/CAS/crash tests; zero bypass; signed-in text dialogue работает в test environment. |
| C — audio/UI vertical slice | Один capture backend/VAD, actual upload, transcript editing, dogn, canonical renderer, shared-shell adapter | ChatGPT для bounded source; агент для browser/runtime integration | WAV fake mic проходит настоящий capture; delayed/duplicate paths; никакого page-local card/layout fork. |
| D — acoustic quality | Frozen Qwen corpus и маленький human holdout; protected live provider tests | ChatGPT+my-data-hub для Qwen; existing CI live lane | Exact artifacts, verified words/slots, provider accounting; no budget ≠ PASS. |
| E — system/release | Согласованный island contract, SoT/native Penpot states, mobile lifecycle, staged deploy/rollback | Текущие family owners/#621 и integration agent | Один Kaggle published preview, exact release tuple, applicable L0–L3 evidence. |

Это не разрешение текущему окну запустить B–E, применить миграции или изменить STATUS. Source work не зависит от полного redesign островов: до готового общесистемного адаптера модуль работает с существующим inline Search, сохраняя те же semantics. Новый временный floating mock не становится baseline.

Миграции additive: private tables/RPC/grants, default-OFF capability, backfill не нужен. Сначала backend compatible/readable старым UI, затем новый client flag; не удалять старые Search contracts. Rollback выключает новое intake, завершает/честно останавливает уже принятое, оставляет receipt/history read/delete и старую навигацию; не drop DB и не переносить provider работу в Fly. Published candidate rollback — существующий release process.

## 15. Автотестовый контракт

Это тест-план решения, а не утверждение о существующих тестах. Scenario IDs ниже — stable proposals для existing registry; до реализации статус planned. Given/When/Then обязателен в исполняемых тестах, а не только в YAML.

| ID | Given → When → Then | Уровень |
|---|---|---|
| voice.auth_gate | Guest → submit → 0 uploads/providers/Search; login не auto-submit | L0/L1 |
| voice.patch_semantics | free/Svetlogorsk → «до пятисот, в Калининграде» → replace, не AND/unknown-free | L0 + quality |
| voice.sequence_gap | U2 приходит раньше U1 → worker → не обходит gap и не теряет условия | DB integration |
| voice.dogon_asr | U1 ASR задержана → U2 принята → оба receipts, correct ordered context | L0/DB/L1 |
| voice.dogon_search | U1 retrieval задержан → U2 accepted до commit → старый result не применяется | DB/L1 |
| voice.commit_race | U2 и commit конкурентны → линейный порядок → один корректный draft/новый section, без дублей | DB integration |
| voice.history_append | S1 ready → refine → S2 с parent=S1; S1 content unchanged | L0/L1 |
| voice.old_base | S1/S2 → scroll к S1 → target прежний; explicit refine S1 → S3 parent=S1 | L1 |
| voice.ordinal_binding | Сказано «второе» по S1 → завершилась S2 → выбран ID из reference set S1 | L0/DB |
| voice.parent_complete | Подходящий coast event на второй странице S1 → refine → включён | Retrieval integration |
| voice.expand_vs_subset | free-only S1 → «можно платно» → explicit new universe; не fake subset | L0/quality |
| voice.stale_membership | Expired/incomplete parent → refine → честный bounded/refresh path | DB/L1 |
| voice.current_actions | Historical card изменилась/отменена → CTA → current validation/notice | Integration |
| voice.idempotency | Same UID+body twice → один intake/effect; другой body →409 | DB |
| voice.unknown_dispatch | Crash после mark_sent → restart → unknown, 0 blind provider repeats | Integration |
| voice.spool_recovery | Incomplete/closed audio, DB crash → restart → bounded recovery/orphan purge | Integration |
| voice.epoch_and_delete | Reset/delete/logout → late completion → нет воскресшего ответа/identity leak | DB/L1 |
| voice.writer_conflict | Две вкладки к одной беседе → concurrent write → CAS/explicit takeover | DB/L1 |
| voice.retention | Expiry/control delete → read/purge/replay → deny, purge bounded, старый seq не перезапускается | DB |
| voice.capture_negation | Тихое «не», пауза/начало слова → VAD/capture → отрицание в real audio сохранено | L1/live |
| voice.capture_lifecycle | Permission revoke/hidden/offline → capture → tracks stopped/честный partial state | L1/L2/L3 |
| voice.codec_and_size | Invalid MIME/giant file/wrong declared rate → intake → reject до provider | Unit/integration |
| voice.elastic_capacity | Spare/pressure/recovery fake clock → policy → borrow/fairness/hysteresis, не ложный tiny cap | L0/integration |
| voice.shared_accounting | Site+test contend same project → dispatch → общий hard ceiling, cancel-sent не unsent | Integration |
| voice.manifest | Publisher пропал/личный limit → CDN UI → expiry unknown, нет personal data/global outage lie | L0/L1 |
| voice.status_truth | Capture+queue+ASR одновременно → UI → независимые честные labels, no fake percent | L1 |
| voice.scroll_anchor | Submit, затем чтение истории/late images → completion → position retained/new answer control | L1 |
| voice.island_geometry | Header+context+nav+composer+keyboard → interaction → stop/CTA видны, sticky succession корректна | L1/L2 |
| voice.explanation_only | Вопрос адреса → factual response → ready explanation, не пустой grid/zero result | L0/L1/live |
| voice.grounding_security | Foreign ID/HTML/source prompt injection/unknown price → output → reject/unknown, no arbitrary tools | Unit/integration/live |
| voice.same_corpus | Один frozen corpus+state → Astro/SoT/Penpot → совпадающие canonical content/components | Existing parity gates |
| voice.real_quality | Frozen audio/independent labels → actual permitted Lite+retrieval → slot/negation/event outcome scored | Protected live |

PR lane в GitHub-hosted Actions: pure tests + isolated actual Postgres/RPC roles + Playwright. Mocks только у provider boundary; Media capture/upload/DOM идут по реальному коду. Chromium fake WAV input доказывает браузерный capture, не iPhone microphone. Firefox/WebKit проверяют поддержанные пути отдельно; L2/L3 отличают native permissions/PWA/lock от viewport emulation. Никакого self-hosted runner.

Набор устойчивых trace fixtures замораживает now/timezone, IDs, corpus, input и контролируемый порядок завершения сетевых запросов. L0 проверяет reducer/policy; L1 проверяет actual DOM/geometry. Search operations security требует реального test DB с ролями и negative rows. Исходные grep/schema checks не заменяют их.

Live lane — existing protected workflow/manual release qualification, synthetic test users, checked-out trusted SHA, minimum permissions, pinned fixture hashes, shared limiter и bounded batch. Fork PRs без live secrets; pull_request_target не исполняет недоверенный PR с секретами. Cache smoke и cold-ASR path считаются отдельно. `SKIPPED_NO_BUDGET`, `BLOCKED_PROVIDER_POLICY` и failed capture не PASS.

### Qwen corpus

Reuse `idea-hub/skills/voice-file-qwen3-tts/` и его BASELINE/LONGFORM-BASELINE/RUNBOOK; подтверждённый ранее resource `zigomaro/yazyki-rossii-qwen3-tts-cpu-0901-r2/2` — donor, не новая Search-run запись. Для Search fixtures одна notebook family/стабильный slug, CPU baseline, versioned outputs. Не перезаписывать чужую лекционную задачу и не менять dependency stack без причины.

Первая матрица: примерно 30 смысловых сценариев, 2 разрешённых голоса, 3 акустических условия; сначала небольшой smoke, затем полный batch. Это размер покрытия, не обязательный расход квоты. Expected intent/result constraints размечаются до синтеза; отдельно проверяется, что аудио действительно произнесло заданное, особенно «не», суммы и города. Не использовать output тестируемого ASR как единственный эталон. Real-human holdout не публиковать без consent; speaker cloning не подразумевается разрешённым автоматически.

Fixture manifest: clip SHA/duration/codec, source/model/notebook version, transcript intended+verified, expected IntentPatch и negative constraints, reference time/timezone, corpus revision, dialogue predecessor/timing и consent provenance. Noise/compression derivatives помечаются и не заменяют clean baseline. В Git manifest+малый smoke; большие binaries в versioned artifact storage, не ephemeral latest Actions artifact. Qwen не перегенерируется на каждый PR.

## 16. Приёмка, ограничения доказательств и конкретные оставшиеся bindings

Документальная приёмка: API/данные/ошибки/переходы/реализационные пакеты согласованы и связаны с единственными владельцами. Runtime release дополнительно требует implemented unit/DB/browser tests, разрешённого live provider subset, mobile-critical evidence, same-corpus UI states, actual source+backend+prompt+policy+corpus release tuple. Нет выдуманного SLA или PASS из факта сохранения Markdown.

До enablement заполнить только конкретные environment/quality bindings: фактический service origin и supervisor; caller доступа к shared limiter; проверенную модель/capabilities и provider/data-processing admission; реальные numeric allowance/headroom; approved Event Corpus selection для acoustic fixtures; общий island adapter и visual variants. Архитектура не оставляет эти пункты поводом заново исследовать весь продукт: packages A/B и mock integration могут выполняться независимо, public voice остаётся OFF до своих gates.

Ключевые метрики: existing discovery/intent success и time-to-first-value; дополнительно lost utterances=0, stale applied answers=0, duplicate user effects=0 в тестовом корпусе, unknown factual claims, correction/abandonment, queue/latency, tokens/audio seconds/egress на успешный выбор, capacity left unused при ошибочных отказах. Количество отправленных ИИ сообщений не является продуктовой победой.

### Первичные внешние технические источники

Проверены 2026-09-05; перед изменением зависимостей/provider deployment проверять текущую документацию снова:

- [AudioWorklet и secure context](https://developer.mozilla.org/en-US/docs/Web/API/AudioWorklet).
- [MediaRecorder dataavailable и ограничения timeslice/lifecycle](https://developer.mozilla.org/en-US/docs/Web/API/MediaRecorder/dataavailable_event).
- [Gemini audio input](https://ai.google.dev/gemini-api/docs/audio) и [structured output](https://ai.google.dev/gemini-api/docs/structured-output).
- [Supabase RLS/grants/privileged-role boundary](https://supabase.com/docs/guides/database/postgres/row-level-security).
- [GitHub Actions token permissions](https://docs.github.com/en/actions/tutorials/authenticate-with-github_token).

Спецификация не запускает эти API, не применяет SQL и не создаёт audio fixtures. Она задаёт проверяемое решение и границы следующей реализации.
