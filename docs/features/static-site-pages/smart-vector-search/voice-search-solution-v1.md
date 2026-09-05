# Голосовой разговорный поиск — техническая спецификация v1

> 2026-09-05, редакция после восстановления связей «Плана релиза». **Implementation-review specification, не runtime/deployment PASS.**
> Продуктовый владелец: [agent-assisted-event-discovery.md](agent-assisted-event-discovery.md). Действующий Search: [README.md](README.md).
> Обязательная общесистемная зависимость: [release-integration.md](../../static-personal-announcements/release-integration.md). Она связывает транспорт, статистику, продуктовые решения, профиль и текущий релиз без создания второго владельца этих систем.

## 0. Изменение относительно первого v1

Первый v1 на `62c54ce42786eecc5b380ea3dba002af78df8fd0` задавал полезные API/receipts/ordered-dogon/history boundaries, но недостаточно связывал их с июльско-августовской общей архитектурой. Эта редакция сохраняет их и уточняет:

- отдельный worker origin не означает отдельный transport client; browser access подключается к existing capability/route policy;
- JSON/control и audio media имеют разные route envelopes; прежние 8 MiB нельзя считать стандартным relay-compatible request;
- voice section/result set не заменяет фактический served list, analytics session или durable профиль;
- уже принятые activation, exact-hide/undo, surface policy, local projection и scheduled materializer действуют и здесь;
- статистика имеет producer → accepted sink → aggregate → readout → reviewed decision, а не только перечень метрик;
- static/voice/analytics success, main commit, deployed feature и измеренный outcome не смешиваются.

Новые имена/поля/значения ниже — проект реализации, не утверждение об existing deployed endpoints. Числовые defaults допускают проверку/изменение без новых искусственных ограничений естественной речи. Shared requirements, security и ресурсные пределы не обходятся.

## 1. Продукт и границы

Голос и текст — два входа одного разговорного поиска. Результат: самостоятельный раздел «название выборки → исходный вопрос → полезный краткий/раскрываемый ответ → обычные EventCards». Завершённый раздел сохраняется при новом ответе. Factual адрес/транспорт может дать explanation без карточек. Пока идёт сеть, можно добавлять речь.

V1 включает корректировки/отрицания, сужение и расширение поиска, независимую пагинацию разделов, возврат к старому разделу для уточнения, восстановление обработки, понятную доступность, короткую память, разрешённую историю и динамический admission.

Не включены универсальные tools/MCP loops, покупки/регистрация агентом, гарантированное background listening, озвучка каждого ответа, новая подписочная система или профиль из каждой поисковой фразы. Save/calendar/reactions используют existing CTA/domain commands. Голосовые изменяющие команды — следующий отдельный безопасный срез.

Текущий Search остаётся для eligible авторизованных пользователей: проверять принятую site-user-identity eligibility, не только наличие JWT. Focus anonymous Supabase session не становится автоматически verified account. Гость не выполняет cost-bearing upload/ASR/Search; после login нет autosubmit. Account login не активирует interest profile и не выдаёт optional analytics consent.

## 2. Источники и места интеграции

Source review: `main@b8f463f5c35fa62befcfed171a7a8a0886af20f7`, product correction `bce0a4ae06d75651aff09ef3657d8272113b2267`, первый v1 `62c54ce42786eecc5b380ea3dba002af78df8fd0`. Перед coding integration читать current main/active branches, не откатывать их к этим указателям.

| Existing owner/source | Использование |
|---|---|
| `site/src/components/AuthorizedEventSearch.astro` | Search entry, Auth и canonical results; legacy maxlength 180 не применяется молча к transcript. |
| `site/src/lib/staticSiteAuth.ts` | Единственная identity/session; eligibility/anonymous-upgrade не дублируются. |
| `site/src/lib/backendOperationCatalog.ts` | Явные assistant operations/caps/replay semantics; unknown operations fail closed. |
| `resilientDataClient.ts`, `resilientSupabaseTransport.ts` рядом | Единые правила route health и retry; новый origin требует явного фиксированного adapter, не произвольной подстановки URL. |
| `supabase/functions/event-search/index.ts` | Shared domain retrieval/validation/presentation выделяется с injected dependencies; старый endpoint/wire contract сохраняется. |
| `supabase/functions/event-search/google-quota.ts` | Existing project/model reserve→mark_sent→provider→finalize. При выделении общего owner — compatibility export, не копия ledger. |
| `supabase/functions/event-search/occurrence-families.ts` | Canonical repeats/family collapse и pagination. |
| [Transport contract](../../unsigned-personalization/production-integration.md), [Yandex resilience](../../../operations/yandex-dependency-resilience.md) | Capability health, exact relay routes, ACK, selected-once и proven replay. |
| [Personalization blueprint](../personalizaion/personalization-to-be.md), [ручные требования](../personalizaion/requirements.md) | Activation, query priority, current hide/undo, profile projection и sensitive/campaign guards. |
| [Data ownership](../../../architecture/personalization-data-ownership.md) | Supabase current profile/identity, YDB analytics sidecar, shared storage/permission/localization boundaries. |
| [Analytics](../analytics/README.md), [Product model](../../../product-model/README.md) | Actors/sessions/facts/metrics, independent purposes, MeasurementQuestion и reviewed decision. |
| [Release umbrella](../../static-personal-announcements/README.md), [План релиза](../release-plan.md) | Полнота F1–F17, current-data/candidate/release dependencies, не isolated voice launch. |
| [Autotest strategy](../../../operations/static-site-autotest-strategy.md), [registry](../../../testing/static-site-autotest-scenarios.v1.yml), existing CI/Search harness | Общие L0–L3 и исполняемые сценарии; без второго framework. |

`my-data-hub.operation.get` — control-plane API с иной identity, не готовый пользовательский receipt endpoint сайта. Название analytics adapter или RPC в документации не доказывает, что он уже развёрнут.

## 3. Архитектура исполнения и подключения

```text
Static Astro / existing Auth / shared shell and operation transport
 → selected healthy assistant route
 → bounded authenticated intake + durable receipt + temporary audio spool
 → ordered worker
      → shared limiter → allowed Lite interpreter
      → validated IntentPatch + current explicit-state/profile policy
      → shared Search domain / Supabase corpus and hard filters
      → optional grounded Lite presenter
      → atomic section + result membership commit
 ← bounded JSON receipt/delta + canonical card projections

User CTA → existing primary command/ACK
       ├→ immediate overlay / later profile materialization
       └→ eligible async analytics projection
```

Предпочтительное размещение worker — отдельный небольшой first-party TypeScript service на Devstand, после проверки доступности/эксплуатации; один deployment unit с worker, без Redis/Kafka/второго Postgres или coding-agent вызова на каждый запрос. Lease/receipt authority — существующий approved primary, не только process RAM. Внешние вызовы вне DB transactions. Общая domain extraction сохраняет старые consumers и не делает server HTTP call самому себе.

Fly web остаётся thin. Kaggle — offline Qwen fixtures и existing published-preview builder, не online voice backend. Сервис проверяет JWT signature/issuer/audience/expiry и eligibility до privileged DB; owner никогда не принимается из client user_id. TLS, exact origin/CORS, fixed upstream allowlist, bounded decoder/body/timeouts; CORS не заменяет authorization/CSRF. Provider/service keys не в браузере.

### 3.1. Общие маршруты не заменяются голосовым клиентом

Product/Auth/Data/Search могут использовать direct Supabase либо Yandex relay к тому же upstream. Analytics имеет другое направление: direct Yandex ingest либо Supabase Edge blind bridge к тому же YDB analytics ingest. Это не репликация/универсальное кольцо прокси. Detailed contract — [release integration §3](../../static-personal-announcements/release-integration.md#3-стабильное-подключение-действительно-два-направления).

Новые `assistant.control` и `assistant.audio-upload` capabilities регистрируются в существующей policy с actual routes/caps и no-side-effect nonce/body probes. Direct-first-party и выбранный fixed alternate требуют отдельной проверки, а не объявления из схемы. Один маленький health GET не доказывает upload/download/decode. Analytics failure не quarantine-ит Search/Auth. 429 даёт cooldown, не alternate bypass; upstream outage не лечится повтором через тот же upstream.

Selected-once путь выбирается до dispatch; ambiguous response не отправляет тело второй раз. Safe-read и idempotent replay имеют свой contract. Последний реальный успех обновляет capability health, idle per-tab pings не нужны. Публичный CDN status — coarse hint; personal quota и локальная доступность проверяются отдельно.

### 3.2. Audio envelope — исправление первого v1

[Yandex API Gateway](https://yandex.cloud/en/docs/api-gateway/concepts/limits) на проверке 2026-09-05 ограничивает request/response **2.5 MB**. Прежние 8 MiB допустимы лишь как separately proven direct-only ceiling/aggregate spool, не стандартный relay request. Первоначальный безопасный target transport segment — ≤1 MiB **итогового wire body**, включая envelope/multipart/base64 и минимальный downstream limit. Actual adapter gate проверяет bytes, не только MIME/длительность.

Длинная речь не обрезается по пределу gateway: recording формирует independently decodable segments с общей logical-utterance identity, порядком и audio timeline. Технические части не считаются новыми пользовательскими поисками. Control submission запускает processing только по полностью принятому validated segment manifest. Не делать ASR-вызов на каждую transport-часть лишь из-за её размера.

При direct-only media path он явно классифицирован и тестируется как direct-only; текст/receipts могут иметь working alternate независимо. Supabase analytics blind bridge не становится audio proxy автоматически: permission/data-flow и server caps должны быть приняты отдельно. Heavy transcoding/ASR в relay не размещается; [Edge limits](https://supabase.com/docs/guides/functions/limits) и timeout всего dialogue нельзя игнорировать.

## 4. Доменная модель и версии

| Поле | Смысл |
|---|---|
| `conversation_id` | Server UUID, owner из проверенной identity. |
| `epoch` | Reset/delete/управляющая граница; late replies не применяются в новом epoch. |
| `writer_id`, `client_sequence`, `previous_utterance_id` | Проверенный writer, порядок и зависимость; это не identity человека. |
| `utterance_id`, `draft_id` | Логическая реплика и один строящийся ответ её revision chain. |
| `audio_manifest_id`, `segment_id`, `segment_index` | Транспортные части реплики; idempotent input identity, не дополнительные ходы. |
| `context_revision`, `accepted_through`, `processed_through` | CAS и непрерывные high-watermarks, не максимум полученных sequence. |
| `section_id`, `parent_section_id`, `revision_of` | Хронологическая история и явная база уточнения/исправления. |
| `result_set_id`, `membership_revision` | Зафиксированная логическая выборка; не то же, что фактически увидено. |
| `served_list_id`, `presentation_receipt_id` | Existing bounded record фактического порядка/контекста представления; не новый профиль и не public raw history. |
| `catalog_revision`, `corpus_revision`, `profile_revision`, `model_version`, `policy_version` | Совместимость данных/ранжирования/исполнения и cache evidence. |

Различать viewed section, refinement base и pending draft. Scroll меняет только первое. Existing analytics session/actor definitions не зависят от conversation IDs/TTL; новая беседа не обязательно новая аналитическая сессия.

### IntentState / IntentPatch

Bounded object: semantic goal, when, where, budget/currency, formats/themes/audience, exclusions, soft preferences, rejected IDs, scope/provenance полей. Hard conditions проверяются host. Time — absolute ISO anchor + timezone исходной реплики; locale машины не определяет даты события. ID места разрешается trusted taxonomy, не сочиняется моделью.

Closed patches: set/clear/add/remove по allowlisted fields. Singleton set заменяет прежнее значение; конфликтующие операции не разрешаются случайным JSON key order. «Не Светлогорск, а Калининград» заменяет город; «можно платно до пятисот» снимает free; «без концертов» добавляет исключение. Unknown price не free. Scope: new_search/refine_selection/continue_draft/explain_selection/explain_event. Explicit target приоритетнее model guess. Start-new-task не равен delete-history.

### AnswerSection

Kind results/explanation/mixed; state draft/ready/empty/error. Fields title, user_query, effective intent, parent, answer blocks, result membership/cursor, versions, timestamps/expiry/degraded reason. Ready explanation без событий не empty search. Safe blocks text/event_group/event_annotation/location/map_actions/suggested_replies/uncertainty; prose не arbitrary HTML/URL. Card facts и actions — trusted projections. fact_id помогает проверке, но JSON/schema/наличие ссылки не доказывает истинности предложения.

## 5. HTTP и операция принятия ввода

Проект prefix `/v1/assistant`, contract `voice-search.v1`. Origin/routes — разрешённый config, не client URL. Private responses no-store; transcript/history не в query string и не индексируемом публичном URL. Body/response/request time budgets заданы по capability.

| Операция | Результат и semantics |
|---|---|
| POST `/conversations` | Idempotent request UUID → conversation/epoch/writer/effective policy; 0 providers. |
| PUT `/conversations/{id}/audio/{manifest_id}/segments/{segment_id}` | Proven idempotent bounded input upload; immutable same-ID/same-bytes, mismatch409; durable part receipt; 0 providers. Fixed owner/epoch/TTL/caps. |
| POST `/conversations/{id}/utterances` | Logical text ИЛИ immutable accepted audio-manifest reference; selected-once → 202 durable queued receipt; не ASR success. Small inline audio разрешён только как тот же envelope/capability, не bypass. |
| GET `/conversations/{id}/utterances/{uid}` | Safe-read receipt/stage/version/error/section refs; для reconciliation ambiguous submit. |
| GET `/conversations/{id}/updates?after_revision=N` | Один bounded delta reader активной беседы, not-modified marker; не poll каждой карточки. |
| GET `/conversations/{id}/sections?cursor=C` | Пагинация private history, не full history на каждый запрос. |
| GET `/conversations/{id}/sections/{sid}/events?cursor=C` | Страница именно этого result set, current visibility overlay и valid projections; cursor owner/scope-bound. |
| POST `/conversations/{id}/control` | Idempotent request+enum cancel_draft/start_new_task/resume_section/reset_context; не LLM action executor. |
| DELETE `/conversations/{id}` | Immediate logical deny + bounded purge receipt, idempotent, 0 providers. |

Audio part metadata: manifest/segment IDs, index, codec/rate/channels, declared/actual bytes, exact capture start/end offsets, checksum; manifest закрывается с ordered segment hashes/total duration. Неполные, пересекающиеся без разрешённого overlap, foreign или изменённые parts не queued. Exact audio overlap/pre-roll policy удаляет только дубли границы, не короткие слова/отрицания. Closing/reusing manifest после dispatch не меняет уже принятое содержимое. PUT replay безопасен из-за server uniqueness и **0 provider effects**, не потому что запрос маленький. Unreferenced parts очищаются по TTL, общий actor/spool admission предотвращает storage abuse.

Utterance metadata: UID/epoch/writer/sequence/predecessor, explicit target new/section/draft, client time/locale, input kind, text или closed audio manifest. Server HMAC normalized payload, не user-supplied checksum как security proof. Same UID/same payload возвращает existing receipt; другая body409. Server received_at владеет retention; client clock помогает интерпретации даты, значимый неправдоподобный сдвиг требует уточнения. Default event timezone Europe/Kaliningrad.

Receipt: UID/revision/accepted/phase/epoch/high-watermarks/draft or section refs/optional retry_after/safe code. `accepted` только durable intake. Rendering receipt не доказывает exposure. `presentation_applied` означает применение версии, не product success.

401 auth required/expired; чужой/отсутствующий resource404 без identity leak; 409 payload/writer/epoch/base conflict; 410 context/selection/receipt expired; 413/415/422 input envelope/codec/validation; 429 user/global capacity; 503 limiter/service/policy unavailable. `outcome_unknown` — видимый отдельный receipt state.

После потерянного POST response читать receipt, не rescue POST другим origin. Одиночный404 не доказывает отсутствие отправки. Cost-bearing replay допускается только по authoritative never-dispatched proof/атомарному existing contract либо как явное новое действие `retry_of` после объяснения unknown. Клиент не обещает, что первая попытка ничего не стоила.

## 6. Догон, конкуренция и crash recovery

```text
receiving → queued → interpreting → interpreted → retrieving → presenting → completed
                 ↘ waiting_capacity ↗
alternatives: rejected / failed / outcome_unknown / cancelled
```

1. Auth/owner/epoch/capacity до privileged work. Audio parts считаются принятыми только после durable closed bytes/hash; complete logical manifest + short transaction переводит utterance в queued. Открытый stream не accepted.
2. Один writer per conversation по умолчанию; обычные вкладки имеют разные conversations. Read-only просмотр истории не writer takeover. Продолжение в новой вкладке/устройстве требует проверенного writer/CAS transition, не голосовой биометрии.
3. U2, пришедшая раньше U1, ждёт missing predecessor; high-watermark продвигается только по непрерывному префиксу. Skip/cancel отдельной реплики — явный marker, не молчаливая потеря.
4. Worker claims короткую lease с fencing token. Внешние вызовы не в transaction. Lease expiry не разрешает повтор неизвестного provider dispatch; stage attempt UID/outcome в общем ledger.
5. Interpret последовательно с правильной base revision. Несколько ещё не отправленных logical additions можно coalesce; транспортные segments одного manifest сначала собираются/валидируются и не порождают отдельные поисковые смыслы.
6. Retrieval/presenter принадлежат последнему полному intent prefix/revision. Новое accepted дополнение делает старую выдачу неприменимой, но не удаляет transcript/условия/usage прежних реплик.
7. Atomic section commit проверяет owner/epoch/fence, accepted/processed prefix, draft/current context и совместимые data versions. Unique draft publication исключает дубли. U2 принято до commit → уточняется один draft; после commit → новая section. Это линейная server граница, не скорость DOM repaint.
8. Клиент принимает монотонные revision только своего epoch, section ID dedupe сохраняет один append после reconnect. Lost network delivery после commit возвращает existing section.

Контрольные cases: догон во время ASR и retrieval; два completion в обратном порядке; duplicate payload; U1 failure блокирует dependent U2 без потери её accepted input; old-section refinement даёт новый chronological S3 с parent=S1; target во время записи фиксируется и не переключается молча при scroll; правка уже опубликованного вопроса создаёт revision_of, не переписывает потомков.

Crash до queued допускает повтор idempotent input без provider. После pure read можно перечитать compatible snapshot. После mark_sent и до saved provider outcome — unknown, no blind rerun/refund-as-unsent. После persisted model result продолжить следующую стадию без повторного ASR. После section commit/до client ACK — read existing result. Epoch/delete побеждает late callbacks. Exactly-once внешнего провайдера не обещается; local effects дедуплицируются и uncertainty учитывается консервативно.

## 7. Retrieval, история и факты

Validated intent → hard-filter eligibility/current exclusions → semantic ranking → occurrence-family collapse → stable membership → pages. Legacy top-K нельзя объявлять полной выборкой без проверки покрытия. Compact embedding query не обрезает hard conditions: они отдельные typed fields.

`result_set_id` включает ещё не отрисованные страницы; хранит ordered IDs, минимальные eligibility facts и versions, не full Event/media/vector copy. «Из них на побережье» фильтрует все parent members; «можно платно» — явное расширение universe и новое название/выборка. Просроченный/невоспроизводимый snapshot требует visible refresh по прежним условиям, не тихой подмены текущей БД. Предлагаемый technical ceiling 4096 members допускает изменение; при его достижении membership_complete=false и честное описание ограниченного набора, не полнота по умолчанию.

Immutable record хранит решение/состав прошлого ответа, **не замораживает пользовательские права/скрытия/жизненный цикл события**. Current exact-hide/undo применяется ко всем sections; скрытая карточка не возвращается из history/exploration. Допустимо компактное скрытое состояние без новой card exposure, restore через существующий recovery action. Отмена/изменение цены/удаление события помечается актуальным overlay/tombstone; CTA перепроверяет current state. Профиль не переписывает задним числом сгенерированную прозу; полный refresh создаёт новую объяснимую выдачу.

Interpreter получает closed schema, active structured base, маленький recent tail, input/time/locale, не весь raw profile и не admin tools. Presenter получает фактические candidates/IDs/fact IDs. Простой ответ может быть deterministic; содержательное объяснение после retrieval может требовать ещё один Lite call. One-call target не запрещает полезный ответ и не разрешает описывать ещё неизвестные факты. Existing verifier/presenter переиспользуется без двойного списания product Search+voice. Embedding и каждый фактический model attempt отдельно через общий limiter. Lite policy не наследует Flash/Pro/Gemma escalation скрыто.

Malformed/invented IDs/URLs/claims отвергаются. Structured output не truth proof; цены/адреса/времена/маршрутные actions из trusted fields. Нет infinite repair chain. Unknown price не free, unknown transport не придуманное расписание. Known address/проезд можно дать без ожидания полного геокаталога, с existing provenance. Mention/advertisement не endorsement «Культурной чайки». Evidence gaps не блокируют основной discovery, но ограничивают соответствующий ответ.

## 8. Capture, UX и отсутствие информационного голода

Независимые автоматы capture off/permission/listening/speech/paused/error и work idle/uploading/queued/interpreting/searching/answering/ready/error/unknown. Visible foreground mic-session включается явно; VAD boundary может отправить полную фразу, пока capture продолжает принимать догон. Stop останавливает tracks. Inactivity/hidden/pagehide/logout/revoked permission прекращают capture; flush при системном уничтожении процесса не гарантируется. Accepted server state восстанавливается независимо от микрофона.

Первый проверяемый backend: getUserMedia → AudioWorklet bounded ring → локальный VAD → независимые WAV/PCM16 mono16kHz segments с правильным resampling. Не переименовывать raw rate устройства. Допустимо переиспользовать уже пригодный MediaRecorder/Opus/AAC adapter после тех же тестов; timeslice blob не считается самостоятельным декодируемым audio автоматически. Не строить одновременно два обязательных recording engine и не переносить transcoding в Fly.

Калибровочные proposals: pre-roll250ms, post-roll350ms, end-of-phrase1200ms, inactive mic30s. VAD не semantic end; сохранить тихое «не» важнее агрессивного вырезания всех пауз. Endpoint limit не sentence limit: segment size по §3.2, logical voiced group ориентир180s/aggregate8MiB, pending audio RAM16MiB. При approaching capacity закрыть валидную часть и продолжить chain, если admission позволяет; при невозможности принять новое — честно остановить запись, не потерять принятое. Значения проверяемые, не уже принятые жёсткие allowances.

PCM16/16k mono = 32 000 bytes/s; 30s ≈0,96MB до envelope. Wire size определяет более раннюю границу; base64/multipart учитываются фактически. Audio не в Supabase Storage/Realtime и не в generic12KiB text outbox; transport adapter/data-flow явно описан. Persisted browser audio очередь не добавляется молча. Text limit proposal8192 символов даёт явную validation error, не silent180char truncation.

First result — общие skeletons; при уточнении completed sections остаются. Labels: «Ищу события. Можно дополнить», «Дополнение принято», «Не удалось распознать первую фразу», «Ответ готов ниже». No fake percentages/pretend streaming ASR. Aria/live/keyboard/reduced motion согласуются с shared shell; duplicate announcements исключаются. Небольшой полезный ответ раскрывается; важная оговорка не прячется мелким шрифтом.

## 9. Общая оболочка и персонализированная выдача

System owner — `pattern.detached-chrome-control-islands`, DS #47, [отдельное проектирование](../design-system/window-prompts/20260905-floating-islands-system-design.md). Search не задаёт global z-index, brand tokens и второй layout engine.

Предлагаемый adapter: Search передаёт role voice_composer/answer_context, instance/section IDs, expanded/focused/recording, reachable stop/status, measured size и focus restore target; shell возвращает occupied rects, effective insets/viewport, layout/layer/expansion policy. Semantic intents select_refinement_base/reveal_section/announce_status типизированы, не generic произвольный event bus.

Section heading sticky только внутри своего раздела ниже actual top occupancy, следующий заменяет предыдущий; flow/sticky один semantic heading, не pin всей прозы. One reveal нового draft-heading по submit; пользовательский scroll к истории отменяет auto-follow, late result даёт «Новый ответ ↓». Anchor section+element+relativeoffset сохраняется после image load/Подробнее/old pagination/Back. Stop и последний CTA доступны, controls не уезжают под пальцем. Scrolling не меняет refinement base.

### 9.1. Existing personalization policy — обязательный вход

Use current explicit overlay и последнюю совместимую `profile_projection`, а не second assistant profile. Activation и localization eligibility проверяются по existing primary contract. `personalization_started_at` может возникнуть от разрешённых meaningful actions; обычный voice query/scroll/share не первый activation. «Запомни, что люблю джаз» — предложение explicit interest-profile-change, не автоматическая запись из любого упоминания жанра. Profile/analytics/history/communications имеют разные разрешения; optional analytics denial не выключает уже eligible personalization.

Порядок приоритетов: lifecycle/security/explicit hides → surface eligibility и текущий явный query/context → разрешённые мягкие preference/ranking. Calendar chronology не переделывается; theme collections слабее ForMe; voice не исключение из hard-hide. Personalization может быть выключена/холодная/compatible/stale/degraded; эти состояния видимы без выдуманной фразы «я учёл интересы», когда projection не применялась.

Explicit current action применяется быстро своим shared command; derived profile считается scheduled/threshold materializer, не LLM при открытии и не после каждого сообщения. Opportunistic ETag/next_refresh refresh не блокирует initial render/local scoring и не создаёт новый профиль на каждую беседу. Pending actions проходят existing primary/replay guards; объявление idempotent дизайна не меняет фактический selected-once saved-state endpoint без server-proof.

Уже видимый префикс/активная карточка не пересортировываются при новой projection. Offscreen update работает по usable viewport с actual occlusion, stable anchor и общий served-list record. Global hide/undo §7 остаётся immediate overlay даже для прошлых sections. Sensitive topics и artifact/promo hunting не превращаются в долговременные facets; short/mid/long interest horizons принадлежат model contract, не history TTL.

## 10. Хранилище и память

Primary — существующий approved personalization/identity store с сохранением localization gate. Логические entities: conversation; utterance/receipt; answer section; result members. Дополнительные audio-part receipts могут быть ограниченной частью receipt/spool manifest, не обязательной пятой большой БД. При наличии равнозначных existing entities расширить их, не дублировать.

Conversation owner/epoch/writer/revisions; utterance UID/sequence/predecessor/hash/phase/fence/attemptrefs/error/transcript/audio pointer; section parent/intent/prose/versions/expiry; members IDs/rank/minimal eligibility facts/card references. Unique UID, writer-sequence и one-publication-per-draft; section membership/cursor owner-safe. Indexed queued work/expiry/ownerupdated/sectionrank. Ни full profiles, ни eventmedia/vectors здесь не дублируются.

Raw tables private, browser direct DML нет; exposed views/RPC restricted grants/RLS, security-definer fixedsearch_path, service-role route own owner checks. Payloaduser_id/user_metadata не authority. Crossuser parent/cursor, malformed keys и raw logs — negative tests реальными ролями. Authkeys вне 64KiB app storage eviction. Temporary audio private restricted spool+TTL; deletion нет публичных raw URLs.

Proposed defaults для проверки, не новая общая retention policy:

| Слой | Срок/поведение |
|---|---|
| Implicit active intent | 30min inactivity/смена локального дня → needs_resume, не стирание видимой ленты. |
| История выключена | Active-session data, absolute expiry не позднее24h; конкретный inactivity purge не должен неожиданно уничтожить pending accepted work. |
| История включена | Sections/questions/minimum membership до7days; resume не продлевает любую старую реплику бесконечно. |
| Model context | Последние6 реплик либо12KiB, сохраняя structured constraints; это не видимый history cap. |
| Audio | Удалить после durable transcript/cancel, absolute safety TTL1h для spool/error; не implicit archive. |
| Dedupe | UID/seq/digest/attempt refs до завершения conversation retention; compact high-watermark запрещает повтор oldseq после compaction. |
| Profile/purpose data | Существующий профильный lifecycle/горизонты и отдельные purpose policies, не этот history TTL. |

Logical expiry/delete блокирует чтение немедленно; bounded purge target≤1h с alert, backups/provider retention отдельно. Сохранённая user_query в section всё ещё текст: удаление processing transcript не равно удалению всех копий. Logout останавливает capture/writer и доступную локальную проекцию, но не отменяет opt-in account history; newidentity не получает late callbacks. Reset profile, new search и delete history — разные controls. Старое «завтра» anchored к исходной дате; resume перепроверяет дату/актуальность, не сдвигает молча.

Existing shared browser outbox16records/12KiB/24h/5attempts и total64KiB не увеличивается из-за analytics legacy7day proposal; применить пересечение применимых budgets. Browser delivery TTL не равен server analytics/history retention. Нельзя вытеснить явное несинхронизированное state без честного terminal/recovery состояния.

## 11. Ресурсы, egress и кэш

Один provider ledger, отдельная generous dynamic voice product policy. Cheap auth/abuse/size → duplicate lookup → fair logical-action admission → lease → reserve конкретного provider stage → mark_sent→call→finalize. Segments одного input и retries receipt не новые user questions; все реальные provider attempts учитываются. Accepted bytes/utterances не уничтожаются из-за снижения policy. Emergency stop честный и отдельно от refund.

Policy pure function над approved config/headroom RPM/TPM/RPD/spend/queue/activeusers/stagecost. `effective_user_burst=min(approved_user_ceiling,base_burst+fair_share_of_spare_capacity)`. Hysteresis не даёт мигания из-за одного slowcall; borrowing снижается при pressure, discretionary batch/tests уступают interactive на safe boundaries, aging/round-robin сохраняют вход новым людям. Numericceilings из actualsharedusage, не произвольных «трёх поисков». Queue/stage deadlines не оставляют бесконечный spinner. Qwen CPU slots/runtime отдельный resource dimension, не GeminiRPM.

Supabase egress включает DB→service; no fullhistory/membership/vectors на каждый poll. Propose delta poll1.5s→6sbackoff+jitter только pending foreground conversation, bounded cache/no sensitive sharedentries. Pageidle не постоянный healthpoll. 50kturns×8KiBstate≈0.41GB и ×50KiBresults≈2.56GB — только иллюстративный расчёт; добавить receipts/Auth/bridge/profile/stats и actual responsebytes. Freequota не presumed sparecapacity.

Cache identity: intent + parentmembership signature + catalog/corpus/taxonomy/prompt/policy + timeanchor/locale + applied profile/explicit-state revision. Query hashHMAC. Private output partitionedпоowner, publiccache не rawquestions или personalprose. Degradedshortcache снижает повторный расход при outage. Profile change не неявный historyrewrite; exacthideoverlay всегда current.

## 12. Статистика и критерии результата

Владелец definitions/consent/envelope/retention — [Analytics](../analytics/README.md). Feature измеряется только после actual terminal readout, не по названию отправленного события. [Release integration §§4–5](../../static-personal-announcements/release-integration.md#4-статистика-что-именно-доходит-до-владельца) задаёт общие стыки, no competing pipeline.

| Вопрос | Наблюдение и denominator | Решение |
|---|---|---|
| Помог голос найти событие? | eligible/exposed actor population → event_value/intent_action; отказ в capacity, correction/abandonment/latency отдельно | Развивать либо исправлять actual bottleneck, не максимизировать число реплик. |
| Работает уточнение? | accepted refinement → rendered/seen result → useful action; сравнение при сопоставимых corpus/eligibility | Менять intent/retrieval/presentation. Causal uplift только зарегистрированный holdout. |
| Не мешает история/остров? | real exposed controls/sections, accidental jumps/errors, downstream CTA; QAocclusion рядом | Менять navigation/layout, не countsticky transitions. |
| Применение профиля полезно? | compatible/static coverage, cards-to-value, diversity/hide; registered experiment | Менять surface/model policy, не выводить эффект из selfselection. |
| Надёжно сохранено и учтено? | primary ACK → optionalprojection → sinkreceipt → aggregate/readout coverage | Исправлять конкретный маршрут/этап без ложного productfailure. |

Единицы: записан звук ≠ accepted utterance ≠ model response ≠ rendered answer ≠ seen answer ≠ полезное действие. Denied opportunities не исключаются молча из картины доступности; essential aggregates без actor tracking не смешиваются с consented conversion denominator.

Cardexposure по общему session×surface×event правилу, не новый uniqueevent для каждой section. Section-local analysis отдельно. Flow→sticky однойшапки, повтор viewport и Back не pageview. Фактический servedorder/scorepolicy/profile/corpus/experiment привязан к existing served-list/presentationreceipt; section_id не подменяет его. Occlusion от sharedshell учитывает visibleportion без rawcoordinate tracking.

Strongstate/actionфакты из canonical store, weakvisibility/hints/depth из consented compact summaries, operationalrouting/usage из отдельного минимального evidence. Одна voice/touch/keyboard CTA + retry = одинprimaryfact с modality. Save/calendar durable state един; ICSexport не внешний import, ticketclick не purchase/attendance. Суммарные TG/VK/siteobservations не uniquepeople. `/general_stats` не заменяет productdashboard.

Optionalstats: существующий accumulator/outbox → Yandexingest либо Supabaseblindbridge → одинidempotentsink/YDB → dailyaggregates/privateownerreport. BridgeнеrawSupabaserows и не analyticsSOR. PrimaryACK/asyncoutbox отделены от YDBдоступности; failureprojection не повторяет productcommand. End-to-end terminaltests доказываютsinkreceipt, не просто HTTP200proxy.

WeakUIsummary входит в общий≤3batches/session/bytebudget; no request на VADframe, каждуюreplica,scrolltick,stickytransition. Publicanalyticsне rawaudio/query/modelanswer/DOMtext/fullprofile/tokens/privateURLs/precisecoordinates. Productanalyticsconsentотдельный: без него0optionalobservations, но Searchиeligibleprofile работают. Stableanalyticsactor не прячется подessential. Test/preview/bots/synthetic отделены поexistingenvelope; многократныеQwen/CIruns не улучшают userretention.

Каждый readout имеет query/expectednumerator/denominator/coverage/version/purpose/guardrails. Missingorlatebatch — gap/INSUFFICIENT_DATA, не нулевойинтерес. Assistedsequence не causal. Reviewed finding/options/decision/followup идут в existingProductAtlasanalysis, не rawstream/profile. Optionalactionmap остаётся default-OFFcampaign и не обучаетпрофиль.

## 13. Доступность, privacy и эксплуатация

Proposed `/data/assistant-availability.v1.json` в existingCDN/bucket: schema/flag/coarsestate/checked_at/valid_until/clientcontract/globalretryhint, безpersonalquota/secrets. Lightpublisher/observer не Astrorebuild; initial30supdate/cache,90svalidity — тестовыепараметры. Expiredpublisher→unknown; deepliveASRcanaryотдельно отshallowhealth. GitHubcronне30secondheartbeat.

BrowserCDNcache безcachebusting/directper-tabpings. Activecomposer/история не исчезают наoutage; correctfallbackпоcapability. Vector-onlyнеобещается при недоступномupstreamembedding/retrieval. Локальный сбой не глобальныйYandexoutage.

Publicenablement требует actual serviceorigin/supervisor/health, Auth/eligibility, DB+limiterbindings, approvedmodelcapabilities/provider/data-processing/region/audience/localization, routecaps/probes, actualbudget, schema/prompt/corpus/UIversions, purge/alert. Незавершённыйbinding держитflagOFF; mockeddevelopment неblocked целиком. Не обходитьprohibitionключами/proxy/geography или hiddenmodel. History/voiceprivacy отдельно отpublicpersonalpageforwardablelink.

Threats: foreignconversation/parent/cursor, payloadsubstitution/replay, anonymousprivilegeupgrade, sourcepromptinjection/HTML/rogueURL, decompression/giantaudio, spoolabuse, rawlogs/generaldeviceleaks. BounddecoderCPU/wallclock, redactlogs, checktruecodec/bytes, scopefixedrelay. VAD/CORS неantiabuseproof. Sourcecode/blockdiagramsнеlegalcomplianceevidence.

## 14. Пакеты реализации и rollout

| Пакет | Результат | Исполнение и доказательство |
|---|---|---|
| A — core contracts | IntentPatch/timeline/resource-policy, sourceowner mapping, measurement fixtures | ChatGPT; actual unit/property/fake-clock/aggregate oracle, no providers. |
| B — durable paths | Registeredcontrol/media routes, owner-scoped intake/DBleases/receipts, limiter/sharedsearch integration | Кодовый агент; realtestDB/HTTPfaultmatrix, oneeffect, no rawkeys/bypasses. |
| C — useful UI journey | Capture/VAD/transcript/dogon/history + canonicalcards/sharedshell + currenthide/profileoverlay | Boundedsource ChatGPT и actualbrowserintegration агент; realfakeaudio→upload, orderedrace, stabletargets. |
| D — acoustic and measurement | FrozenQwen/humanholdout, permittedliveASR, sink→aggregate→readout onsynthetictestidentity | ExistingprotectedCI/my-data-hub; actualcounts/versions, nobudget≠PASS. |
| E — system/release | Samecorpus/profile/state SoT/nativePenpot, mobilecritical, oneKagglepreview, releasebindings | Currentfamilyowners/#621/release; evidenceдляцелевогосреза, неслучайногоdemo. |

Это не разрешение данного документа на live calls/migrations/deploy. Sourcecore может работать внутри existinginlineSearch до принятия полногоislandlayout, без канонизации временногоfloatingmock. Commonfoundations/STATUS не редактируются параллельно.

Additivemigrations/flags; старыйSearchсовместим, backfillголосовойистории ненужен. Сначалаподготовленныйbackend иvalidroutes, затемclientflag. Rollbackостанавливаетnewintake, завершаетлибо честнотерминирует acceptedwork, сохраняетreceipt/read/delete иstaticnavigation; неdropDB/повторproviders/переносLLMвFly. Publishedcandidate — currentoneKagglepath, нестарыйfutureALB автоматически. F1–F17 и datafreshness/primaryaction/releaseguardsне исчезают.

Первый вертикальный slice: eligibleuser ищет «бесплатно» → уточняет «из них на побережье» → скрываетсобытие → открываетобычнуюподборкубезвозвратаскрытого → получает durableaction/profileoverlay и воспроизводимыйtestreadout. При analyticsdenied продуктработает с0optionalwrites; приYDBdownprimaryуспехнеотменён; приlostACKнетduplicate.

## 15. Автотестовый контракт

**План, не пройденные тесты.** Existing registry — единственный executable список. Ниже32 ранее выбранных ID сохраняются; новые seams используют mapping [release-integration §8](../../static-personal-announcements/release-integration.md#8-автотесты-стыков), не второйQAframework.

| ID | Given → When → Then |
|---|---|
| voice.auth_gate | Guest/anonymoussession vs eligibleaccount → submit/login → onlyallowedcalls, noautosubmit. |
| voice.patch_semantics | free/Svetlogorsk → maxprice/Kaliningrad → replace, notAND. |
| voice.sequence_gap | U2beforeU1 → worker → no missingconstraintskip. |
| voice.dogon_asr | U1slowASR→U2accepted→bothreceipts/correctorderedcontext. |
| voice.dogon_search | U1slowretrieval→U2beforecommit→staleresultnotapplied. |
| voice.commit_race | concurrentU2/commit→one linear draft/newsection outcome, noduplicates. |
| voice.history_append | S1ready→refine→S2parentS1, originaldecisionunchanged. |
| voice.old_base | scrollS1→baseunchanged; explicitrefineS1→S3parentS1. |
| voice.ordinal_binding | «второе» referencedS1→S2ready→IDизS1. |
| voice.parent_complete | qualifyingeventnextpage→subset→included, notfirstscreenonly. |
| voice.expand_vs_subset | freeparent→«платно»→explicitexpansion. |
| voice.stale_membership | expired/incompleteparent→refine→boundednotice/refresh. |
| voice.current_actions | historicalcancelled/hidden/changedprice→render/CTA→currentoverlay/validation, nohide resurrection. |
| voice.idempotency | UID+samebody→oneeffect; changedbody→409; mediapartreplay→0provider. |
| voice.unknown_dispatch | crashaftermark_sent→restart→unknown, noblindrerun. |
| voice.spool_recovery | partial/closedparts/missingmanifest→restart→safeinputrecovery/purge, neverprematureASR. |
| voice.epoch_and_delete | reset/delete/logout→latecompletion→noleak/resurrection. |
| voice.writer_conflict | twowriters→CAS→explicitconflict/takeover. |
| voice.retention | expiry→read/purge/replay→deny/purge/nooldseqexecution. |
| voice.capture_negation | quiet«не»/boundary/pause→capture→wordpreserved; transportchunks donotaltermeaning. |
| voice.capture_lifecycle | revoked/hidden/offline→tracksstop/honestpartialstate. |
| voice.codec_and_size | badcodec/giantbody/crossroutecap→rejectbeforeprovider; validsegmentsrecoverwholeinput. |
| voice.elastic_capacity | spare/pressure/recovery→fairborrow/hysteresis, nounnecessarytinycap. |
| voice.shared_accounting | website+CIcontend→singleproviderceiling; cancel-sentnotunsent. |
| voice.manifest | stale/localfailure→unknown/coarsehint, noprivatequota/globalfailurelie. |
| voice.status_truth | capture+pending→independenthonestlabels, nofalseACK/percentage. |
| voice.scroll_anchor | submit+historyreading+lateimage→anchorretained, newanswercontrol. |
| voice.island_geometry | header/context/nav/composer/keyboard→reachableCTA/stop/stickysuccession/frozenvisibleprefix. |
| voice.explanation_only | addressquery→readyfacts, noemptygridfalsezero. |
| voice.grounding_security | foreignID/rawHTML/sourceinstruction/unknownprice→reject/unknown, notools. |
| voice.same_corpus | frozenEventCorpus+sanitizedprofile/activation/consent/state→Astro/SoT/Penpotconsistent. |
| voice.real_quality | frozenaudio+independentoracle→permittedLite/retrieval→slots/negations/outcomes scored. |

Обязательные дополнительные seam assertions: actualdirect/relay/bothdown/upstreamfaults; reverseanalyticsbridge→same sinkreceipt; disposaltelemetrynotroutehealth; consentoff+activationindependence; currentglobalhide/calendar/surfacepolicy; ETagrefreshwithoutUIjump; rendered-vs-seen/sectiondedupe/servedorder; authoritativeCTAprojectionno duplicate; aggregateoracle/denominator/coverage/testpollution; sharedbudgetTTLintersection; zero-costactionmapOFF; exactrelease/corpusconsumption. Reuse existing IDs/gates гдеониужеесть, newIDsтольковcanonicalregistry.

GitHub-hosted PRlane: unit/property/schema/PII/fakeclock плюс isolatedactualPostgresroles/CAS и faultinjectionHTTPserver; Playwrightнаactualcapture/upload/DOM/geometry. Mockтолькоproviderboundary, не alluserjourney. ChromiumfakeWAVнеphysicaliPhone; Firefox/WebKitпроверятьподдержанныеadapterpaths, L2/L3поnativepermissions/PWA/lock/hardware. Никакогоself-hostedrunner.

Protectedlive: trustedSHA+sessionfixture+testidentity/sink+boundedsharedlimiter, no rawsecretsfromfork/pull_request_target. ColdASRотдельноcachedsmoke. SKIPPED_NO_BUDGET/BLOCKED_PROVIDER_POLICY/failedcaptureнеPASS. Terminal evidence связываетcode/schema/prompt/model/profile/corpus/fixture/UIversions, receivedoutcomes и readout; docgrepнеDBsecurity/liveacceptance.

### Qwen corpus

Reuse idea-hub `skills/voice-file-qwen3-tts/{SKILL.md,BASELINE.yaml,LONGFORM-BASELINE.yaml,RUNBOOK.md}` и проверенныйраньше donor `zigomaro/yazyki-rossii-qwen3-tts-cpu-0901-r2/2`. Дляsearch однаstable notebookfamily сversionedCPUoutputs, неoverwriteлекционнойjob/сменазависимостейбезоснования. Новыеrunsданнымдокументомнезапущены.

Пример стартовойматрицы30semanticcases×2разрешённыхголоса×3условия, сначалаsmoke; этоcoverageproposal, необязательныйрасход. Independentexpectedmeaning **до**TTS, verifiedspokenwords после. Quietnegation/numbers/localnames/continuoussplit/dogon/oldsection/transportquestions. TestASRнеединственныйoracle. Humanholdoutсconsent, nocloning/publicvoicesimplicitly.

Manifest: exactclipSHA/codec/duration/rate, intended+verifiedtranscript, expectedpatch/negativeconditions, timestamps/predecessors, frozennow/timezone/corpus/profile/activation/consent, model/notebook/sourceversion иrights. Marknoisy/compressedderivatives, сохранятьclean. Gitmanifest+smallsmoke, largeimmutableartifactsнеephemerallatestActionsartifact. НеперегенерироватьприкаждомPR.

## 16. Приёмка и реальные границы

Документальнаяприёмка — совместимыеAPI/данные/статистика/профиль/ошибки/пакеты сединымивладельцами. Runtime требуетреальныхtests иvalidenvironmentbindings. Interfaceavailability/sourcecommit/model200/видимаякарточка поотдельностинедоказывают end-to-end discovery.

Осталосьпроверитьконкретно: serviceorigin/supervisor, actualcontrol/mediaalternate/envelopes, Auth/limiter/DBprivileges, permittedprovider/dataflow, numericcapacity, existingprofileprojection/materializer/sourcebindings, permittedanalyticsingest/readout, approvedfixturecorpus, общаяislandAPI/visualstates. Необъявлятьmissingplatformdeployed; одновременноpurecore/fixtureworkможетидтибезpublicenablement.

Ключевыеguards вacceptance: nolostacceptedutterance, nostaleappliedanswer, noduplicateprimaryeffect, noprivacy/foreignidentityleak, noglobalhideresurrection, correctfacts/denominators/coverage. Productoutcome — successfuldiscovery/intentaction иtime/cards-to-value, рядомcost/queue/egress/diversity. ЧислорепликИИнеNorthStar.

### Технические первичные ссылки

[AudioWorklet](https://developer.mozilla.org/en-US/docs/Web/API/AudioWorklet), [MediaRecorder lifecycle/chunks](https://developer.mozilla.org/en-US/docs/Web/API/MediaRecorder/dataavailable_event), [Gemini audio](https://ai.google.dev/gemini-api/docs/audio), [structured output](https://ai.google.dev/gemini-api/docs/structured-output), [Supabase RLS](https://supabase.com/docs/guides/database/postgres/row-level-security), [Yandex gateway limits](https://yandex.cloud/en/docs/api-gateway/concepts/limits), [Edge limits](https://supabase.com/docs/guides/functions/limits). Проверятьактуальностьприизмененииadapter/provider. ЭтадокументациянеприменяетSQL, непубликуетсайт и незапускаетмодели.
