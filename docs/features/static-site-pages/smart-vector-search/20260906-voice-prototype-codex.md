# Codex: интегрировать и опубликовать проверенный голосовой прототип KenigEvents

Дата: 2026-09-06. Это исполняемая интеграция и исправление конкретного кода, не новый широкий аудит. Работай в `onedayonemasterpiece/events-bot-new`, продолжай PR **#587**, ветку **`docs/agent-assisted-event-discovery-20260826`**. Все исходники уже обычные Git-файлы в репозитории. Архивы и повторный перенос из ChatGPT не нужны.

## Результат и границы разрешения

Нужен HTTPS/noindex preview через существующий **один Kaggle builder / current bucket path**, с проверенным путём: настоящий микрофон → локально сохранённое аудио → настоящий разрешённый ASR → исправляемый и явно подтверждённый текст → интерпретация → текущие обычные карточки → уточнение/история → существующее действие → подтверждённое измерение результата. Доводи найденные дефекты до исправления и повторной проверки, не заменяй ремонт дополнительным логированием.

Не выполняй production/root/current promotion, не сливай PR в main ради демонстрации. Встраивание изменений актуальной базы в рабочую ветку допускается после проверки конфликтов и сохранения параллельной работы; force-push запрещён. Не создавай второй publisher, runtime, индекс, Auth, профиль, quota ledger, транспорт, shell или липкий этаж.

Персонализация предусмотрена Пользовательским соглашением: отдельного включателя или шага согласия на неё **нет**. Не восстанавливай старый `Настроить рекомендации?`. Optional analytics/research/email/push permissions не расширять. Наличие Google API key не заменяет одобренную policy/model boundary.

## 1. Точные входы и состояние

Первый kernel: `d913010ea0de9d157b2d64763846495d4b482bec`; его исторический CI `34001133025` не является приёмкой этой реализации.

Новый основной source checkpoint: **`8f349f0fa1756d06cbda6807c5c84cbb8b388dd1`**. `fe35a525b29804e9d837dbdf858fc094fd134233` сохраняет тот же runtime и устраняет конфликт места записи CHANGELOG для запуска новых PR checks. Дальнейший фактический HEAD и terminal receipts прочитай в #587 и `20260906-voice-prototype-evidence.md`, если этот отчёт уже добавлен. Не откатывай более новые исправления к приведённому checkpoint.

Прочитай применимые AGENTS/skills, `20260906-voice-prototype-chatgpt.md`, текущие `voice-search-solution-v1.md`, `agent-assisted-event-discovery.md`, canonical `docs/operations/e2e-scenarios.md`. Ограниченно прочитай используемые Search/Auth/profile/analytics контракты и актуальный #621. Не открывай новый аудит всей release umbrella F1–F17.

Острова: заново прочитай `../design-system/floating-islands.md` и указанный там актуальный DS #47. Последний проверенный здесь DS HEAD — `e989d9169eefc51d6bd259098a710a79f6a500a2`; это не предписание отката параллельного исполнителя. Адаптер текущего кода: `window.KenigEventsSearchAdapterV1`, событие `kenigevents:search-adapter-ready`, context-change event, `beforeOverlayOpen()`. Это integration port, а не заявление о завершённой FI-16/FI-17 интеграции.

## 2. Уже написанный код — продолжать, не переписывать с нуля

**Browser:** `site/src/lib/assistant/`, `ConversationalSearch.astro`, подключение из `AuthorizedEventSearch.astro`, `site/public/voice/pcm-capture-worklet.js`. Continuous foreground PCM без самодельного energy VAD: тихое «не» не выбрасывается. Независимые WAV parts с настоящими sample rate/frame offsets; transport part не означает границу реплики. Остановка не ждёт сеть. IndexedDB `kenigevents-voice-v1` version 2 хранит части, команды, accepted state и ответы с owner keys; version 1 не удаляется. Confirmed text/CAS сохраняются до сети. Поздние результаты архивируются, но не заменяют новый draft. Есть отдельная база уточнения, история и bounded working state.

**Cards/actions:** используется `KenigEventsCreateEventCard` и общий media resolver, не новый макет карточки. `EventLayout.astro` предоставляет adapter обычного feedback/saved-event owner; global hide overlay обновляет исторические карточки, текущая карточка сохраняет undo. Заголовок H2 остаётся в разделе. Adapter не владеет sticky/z-index/nav shell. Текущий layout показывает inline composer; окончательная композиция с островами ещё требует интеграции и визуальной проверки.

**Backend:** существующая Edge Function `event-search`, новые `assistant-{handler,intent,provider,repository,media,dialogue,audio}.ts`. Браузерные прежние kernel entrypoints переэкспортируют общий доменный код, а не содержат вторую копию. Три fixed routes: `/assistant/control`, `/assistant/audio`, `/assistant/status`. Existing Auth проверяет ordinary registered JWT до privileged client. Control создаёт immutable receipt; отправленная операция не запускается повторно после потери ACK; status/history выполняют reconcile/read. Part upload идемпотентен по точным байтам и manifest. Вызов ASR — один на принятую полную реплику после всех частей, не один на part. Canonical facts и structured hard filters применяются до UI pagination. Factual/ordinal explanation не изобретает факты.

**Provider:** один existing shared Google limiter/URL builder/attempt boundary. Результат сохраняется до завершения quota accounting; сбой finalize не приводит к повторной генерации. Подход перенесён из проверенных `my-data-hub` inference/checkpoint модулей (`12c330a96e5db7d781a9283d38f6bc0069d8f89d`), но не account `record-idea-hub`, owner-only ACL или IdeaHub publisher. Дополнительный Python service не нужен.

**Data/routes:** additive `supabase/migrations/20260906013000_event_search_assistant_receipts.sql`, service-only RPC/grants/RLS, immutable request/audio/outcome; fixed relay paths в `infra/yandex/supabase-relay/{openapi.yaml,desired-state.json}`, existing operation catalog. Ничего из этого здесь не задеплоено.

**Measurement:** `measurement.ts` различает rendered/visible/committed, делает preview-only bounded aggregate и дедуплицирует события. Это НЕ завершённый внешний analytics sink/dashboard. Search metadata помечает voice preview как исключаемый тестовый трафик; потребители метрик должны действительно применять исключение.

## 3. Воспроизводимые проверки до live

Из root после установки pinned site dependencies:

```sh
cd site && npm ci --no-audit --no-fund && cd ..
node --experimental-strip-types --test site/tests/assistant-*.test.mjs
npm exec --yes --package typescript@5.8.3 -- tsc \
  --noEmit --strict --skipLibCheck --allowJs --target es2023 \
  --module esnext --moduleResolution bundler --lib es2023,dom,dom.iterable \
  --allowImportingTsExtensions site/src/lib/assistant/*.ts \
  supabase/functions/event-search/assistant-*.ts
node scripts/generate_event_search_revision.mjs --check
python -m py_compile scripts/ops/voice_pwa_device_probe.py
git diff --check
cd site
npx playwright install --with-deps chromium
node --test tests/voice-browser.integration.mjs
PUBLIC_EVENT_SEARCH_ASSISTANT_ENABLED=1 PREVIEW_BUILD_ID=preview-voice-local npm run build:preview
```

Existing CI содержит отдельные voice steps, затем прежние release gates. Existing Postbox SQL workflow на PostgreSQL 17 применяет voice migration и `supabase/tests/event_search_assistant_contract.sql`; тест rollback-only. Не применяй CI bootstrap к рабочей БД. Проверь весь `index.ts` штатным Deno/Edge check, а не только `assistant-*.ts`: локальная модульная проверка его не заменяет.

Границы исходного ChatGPT evidence: 78 deterministic tests PASS; strict module typecheck PASS; Astro preview build 436 pages PASS. Native Chromium локально блокировался `net::ERR_BLOCKED_BY_ADMINISTRATOR`, поэтому перенесён в GitHub CI. Истинный terminal результат смотри в отчёте и Actions. HTTP tests используют реальные Request/Response, но injected provider/repository. Chromium capture использует synthetic device. Ни один из этих результатов не подтверждает настоящее ASR, deployed DB или Android.

Текстовый корпус `site/tests/fixtures/voice-dialogues.v1.json` содержит десять независимых ожидаемых диалогов; это не десять сгенерированных аудио и не десять успешных ASR. Ранее спроектированные 32 quality cases тоже не являются PASS.

## 4. Подключить разрешённое preview окружение

Сначала прочитай текущие deployed revision/config/quotas, ничего не печатай из секретов. Используй существующий secret/config механизм Search. Имена новых переменных:

```text
PUBLIC_EVENT_SEARCH_ASSISTANT_ENABLED=1
EVENT_SEARCH_ASSISTANT_ENABLED=1
EVENT_SEARCH_ASSISTANT_POLICY_REF
EVENT_SEARCH_ASSISTANT_ORIGINS
EVENT_SEARCH_ASSISTANT_PREVIEW_USER_IDS
EVENT_SEARCH_ASSISTANT_MODEL
EVENT_SEARCH_ASSISTANT_APPROVED_MODELS
EVENT_SEARCH_ASSISTANT_AUDIO_MAX_BYTES
```

`POLICY_REF` — ссылка/идентичность реального принятого решения, не строка-заглушка для обхода gate. `PREVIEW_USER_IDS` — закрытая allowlist разрешённых ordinary registered пользователей для ограниченного прототипа, не перенос авторизации владельца донора. Модель выбирается из действующего разрешённого списка и бюджета. Кодовый default модели — не рекомендация blindly enable. Прежние Supabase publishable URL/key, server service key и Google pool не дублировать; ничего секретного в PUBLIC env.

Проверь schema compatibility и текущую миграционную историю. Примени только эту additive migration в разрешённое preview/staging окружение, проверь actual grants/owner rejection и прочитай deployed schema. Разверни новый код существующей Edge Function и fixed relay paths через действующий deployment runbook; ordinary Search должен остаться совместимым. Canary с выключенным голосовым gate обязан подтвердить отсутствие регрессии существующего поиска. Включай gate лишь для перечисленных preview пользователей/origins. Allowlist origin сама по себе не ограничивает URL path: дополнительно проверь noindex, build mode и отсутствие voice UI/флага в production artifact.

Rollback: сначала disable assistant server flag/preview entry, вернуть предыдущую Edge/relay revision по действующему механизму. Не drop/truncate новую БД, не очищать client IndexedDB, не удалять частичные записи и receipts. Не отзывать существующую Auth-сессию для ремонта.

## 5. Конкретные остаточные работы и ловушки

1. **Recovery/UI.** Проверь повтор сохранения после IndexedDB quota failure: исходный `retryUnsaved()` переносит байты, но состояние receipt/UI требует отдельного подтверждённого обновления. Не объявляй interrupted/background запись complete после локального retry. После потери ACK reconciliate тот же operation ID; после `outcome_unknown` автоматический повтор модели запрещён. Revoke/hidden/pagehide/lock должны оставлять честный partial/stopped статус. Проверь двойной stop, stop во время permission prompt, несколько retry кликов и смену аккаунта. Полученные части не удалять.
2. **Длина и полноценная выдача.** Сейчас ASR materialization имеет явный server byte bound (default 16 MiB), а первичная logical candidate выборка ограничена 60 перед показом по 12. Это технические границы slice, не заявленная щедрая продуктовая квота и не полная выборка. Для длинных записей нужен bounded multipart/continuation без потери хвоста и нарушения слов; для поиска — canonical pagination/full logical parent membership, особенно refine старого ответа. Не скрывать caps фиктивным «всё найдено».
3. **Общий профиль.** Карточки используют existing actions/hide owner, но полный current profile materializer и согласование query-over-profile/surface policy должны быть проверены с реальным аккаунтом. После hide событие не появляется в старом ответе, новой выдаче и обычной подборке; undo возвращает только допустимое. Не перестраивай уже прочитанный видимый префикс молча. Favorite/calendar success определяется durable owner receipt, не click.
4. **Общий limiter.** Ledger/attempt checkpoint уже общие; fair scheduling, динамическая allowance/headroom/hysteresis и per-user concurrency ещё требуют реализации/проверки против реального общего account pool. Не заводи второй счётчик или произвольную малую дневную квоту. Недоступный budget/policy — явный blocked, не fake ASR/fallback-send.
5. **Stats end-to-end.** Подключи preview contract к существующему допустимому analytics/test sink и проверь receipt → aggregate → denominator/readout для сценария search/refine/hide/favorite. Локальный `diagnostic()` не заменяет эту проверку. Rendered ≠ seen ≠ committed; несколько answer sections с тем же событием и locator не увеличивают denominator. Analytics denied/outage не блокирует product. Synthetic/Qwen/CI/owner QA исключаются всеми downstream readers, а не только флагом клиента.
6. **Острова и A=S=P.** Свяжи existing port с актуальным общим контроллером; viewed section, refinement base и draft независимы. Overlay opening ждёт `beforeOverlayOpen()`, stop доступен при клавиатуре/нижнем dock. Semantic H2 остаётся на месте; locator не клонирует H2. Десктоп не компактируется без причины. Синхронизируй canonical Astro/SoT и существующую Penpot проекцию с владельцем островов, не канонизируй локальный interim layout как финальный P. Не трогай параллельные STATUS/foundations без установленного ownership.

## 6. Реальная приёмка и fixture discipline

Первый live slice: разрешённый ordinary user → «бесплатные события в выходные» → «на побережье, но не концерт» → обычные актуальные карточки → явный старый раздел как база → уточнение → hide → обычная подборка без скрытого → favorite/calendar durable receipt → допустимый sink readout. Дополнительно factual address/ordinal question с canonical фактами или честным unknown; пустая выдача не заполняется выдуманными событиями.

Для ASR: короткое/тихое «не», длинные паузы без окончания мысли, исправление суммы/даты, местные названия, фраза через part boundary, длинная речь и дата около полуночи по Europe/Kaliningrad. Ожидания из независимого corpus/original text; не оценивай ASR только тем же ASR. Qwen audio можно заранее версионировать на существующем Kaggle CPU пути/том же slug, с actual sample rate, SHA и независимой проверкой произнесённого; голосовые образцы пользователя не публиковать автоматически.

Faults: direct-down/relay-up и наоборот, real-sized media upload, both-down, 429, timeout headers/body/decode, response loss after provider success, reload between all stages, ordinary JWT/anonymous/invalid JWT/foreign UID. Докажи число model sends, immutable input hash, stage receipts и отсутствие двойного списания. Маленький control health probe не доказывает upload. Уменьшение original audio или тишины ради зелёного теста запрещено.

## 7. PWA на телефоне через OpenCode/ADB

APK не нужен. Используй установленный браузер/PWA, авторизованное владельцем устройство и existing preview launch механизм. Не выполняй uninstall, `pm clear`, clear browsing data/site storage, unregister-all service workers, logout/reset, queue/audio deletion. Не отключай TLS/security и не публикуй ADB/DevTools в Интернет. Сначала прочитай manifest `id/start_url/scope`, asset base, auth return URL и SW policy, чтобы preview не заменил установленную production PWA.

В разрешённом OpenCode окружении:

```sh
python scripts/ops/voice_pwa_device_probe.py \
  --serial '<явно выбранное авторизованное устройство>' \
  --preview-url 'https://kenigevents.ru/<фактический-preview-путь>/'
```

Путь должен начинаться `/preview-`; placeholder выше заменить фактическим URL из release receipt. Скрипт read-only, не запускает PWA и всегда честно сообщает `asr_tested=false`, `pwa_tested=false`. Не считать preflight приёмкой. Далее подключись к существующему Chrome/PWA по разрешённому локальному remote-debugging механизму OpenCode/ADB; не выбирай первое попавшееся устройство, не создавай public forwarding.

Проверить настоящие permission deny/allow/revoke, клавиатуру и stop, экранную блокировку, сворачивание, смену Wi-Fi/mobile, возвращение, history/Back/reload и обновление SW без потери сессии/очереди/аудио. Если устройство не подключено, зафиксировать `BLOCKED_DEVICE_NOT_CONNECTED`, не рисовать Android PASS по desktop viewport.

## 8. Публикация и отчёт

Прочитай свежий #621 и существующий Kaggle static-site runbook. Собери exact-commit candidate на том же разрешённом one-builder/current-bucket пути, опубликуй только immutable noindex preview, проверь исходный manifest/source/model/policy/schema identity, same-preview Auth return, MIME/worklet loading, CSP/CORS и cache revision. Не менять root/current pointer, не разворачивать production gate.

В #587 и при необходимости #621 сохранить: exact code/tree и test SHA, terminal CI/native browser/Postgres receipts, реальные ASR fixture identities/usage, состояние canonical corpus/profile/sink, скриншоты desktop/mobile preview без приватных данных, фактическую HTTPS ссылку, результат Android или точный blocker, rollback identity. Diagnostic bundle содержит только неприватные версии, opaque operation/section IDs, интервалы, error codes и audio format/bytes; JWT/cookie/raw speech/query/profile/device serial отсутствуют.

Acceptance — рабочий адрес и воспроизведённый полный путь, а не только 78 unit tests или модельный mock. При реальном external blocker исправь и проверь всё независимое, сохрани это в GitHub и назови конкретный незавершённый gate. Не объявляй production GO и не заканчивай новым общим планом вместо исполняемой интеграции.


## Read-only integration discovery — 2026-09-06

Исходные исправления `b54c4a622daaf58956f8d9be5268a58d03a2bef2` и локальный
handoff `b92b55d6b94cf56055c22f286af2239a2b295fbf` сохранены обычным fast-forward
push в эту же ветку PR #587, после fresh fetch от `d1e845cde`. GitHub HEAD и
`git ls-remote` совпали; force-push/production merge не выполнялись.

### Реально прочитанное окружение, не предположения

- Supabase Management API доступен через существующий
  `~/.supabase/access-token`. Проектный `PERSONALIZATION_SUPABASE_ACCESS_TOKEN`
  из `.env` вернул `401 Unauthorized`; действующий CLI credential дал HTTP200.
  Секреты не менялись, их значения в evidence не выводились.
- Текущая shared Edge Function `event-search`: **ACTIVE, version 79**, timestamp
  обновления `1786301691654` (2026-08-09 UTC). Список server secrets содержит
  59 имён и **не содержит ни одного `EVENT_SEARCH_ASSISTANT_*`**.
- Branch inventory вернул `[]`; отдельный existing Supabase preview branch
  не обнаружен. Migration history: 52 записи, voice migration отсутствует.
  Настоящий `/database/query/read-only` (HTTP201) подтвердил:
  `to_regclass(public.event_search_assistant_operations)=null`,
  `to_regclass(public.event_search_assistant_audio_parts)=null`,
  `event_search_assistant_admit_v1` отсутствует.
- Реальный shared model registry содержит `gemini-3.1-flash-lite` и
  `gemini-3.5-flash-lite` (для каждой 15 RPM / 250000 TPM / 500 RPD на момент
  чтения). Это capacity configuration, **не policy-разрешение KenigEvents**
  и не доказательство текущего свободного бюджета; reserve/provider не запускались.
- Живой donor my-data-hub: deployed `12c330a9`, `control_plane_ready=true`,
  `master_state=CHECKPOINT_FAILED`. Sanitized running-container config
  подтверждает voice model/allowlist `gemini-3.1-flash-lite` и configured v2
  intake. Это другой owner-device flow с IdeaHub publisher. Его токен,
  разрешение и private словарь нельзя переносить на website Search.
- Телефонный путь проверен не только через local PATH: canonical
  record-idea-hub `docs/ADB_HANDOFF.md` описывает ADB на компьютере владельца,
  а `docs/operations/private-events-mcp.md` §Connect OpenCode on Windows —
  Windows OpenCode с loopback OAuth, **не серверный ADB bridge**. На этом
  сервере `opencode-devcoveer.service` — local `127.0.0.1:4097`, OpenCode
  1.18.15, MCP config содержит только my-data-hub; remote-phone MCP/SSH alias
  не обнаружен. Стандартные user/SDK/opt пути не содержат adb, USB видит только
  HID Tablet/root hubs. Это не утверждение, что телефон владельца отключён
  от его Windows: его актуальный bridge/device selector здесь неизвестен.

### Где preview, а где изменение production runtime

Этот handoff §4 явно предписывает «Разверни новый код **существующей Edge
Function** и fixed relay paths ... Canary с выключенным голосовым gate ...
Включай gate лишь для перечисленных preview пользователей/origins». Исходная
постановка `20260906-voice-prototype-chatgpt.md` §9 допускает «применить только
разрешённые staging migrations/config», §8 требует существующий preview-путь.
Это реальный описанный guarded deployment, а не требование нового backend.

Но физически замена `event-search` v79 даже при assistant OFF меняет **общий
действующий production endpoint**, а additive migration затрагивает его общую
personalization DB. В текущем обнаруженном окружении нет самостоятельной
staging DB/branch. Поэтому запрет production/root promotion нельзя обходить,
называя backend write «только preview». Трактовку допуска такого guarded shared
runtime rollout должен согласовать основной исполнитель с владельцем задачи;
эта read-only lane его **не выполняла** и не вводит выдуманный обязательный
новый сервис/branch. Изолированная статическая публикация immutable noindex
prefix сама по себе не меняет root/current, но не даст live ASR без backend.

### Минимальный следующий шаг и оставшиеся границы

1. Зафиксировать реальный **KenigEvents** policy/model binding:
   `EVENT_SEARCH_ASSISTANT_POLICY_REF`, разрешённый model/allowlist, ordinary
   preview users и origins. Не заполнять POLICY_REF заглушкой и не брать
   разрешение от owner-only donor. Public env содержит только UI flag/публичные
   existing Search bindings.
2. Согласовать конкретный permitted runtime target/границу: описанный выше
   guarded shared endpoint + additive DB rollout либо уже существующий
   разрешённый target, если владелец его укажет. Не создавать cloud branch или
   второй сервис ради обхода. Затем штатные Deno/schema/relay canary checks,
   assistant OFF → ограниченная allowlist, и только после этого paid live cases.
3. В **существующем Windows/OpenCode**, где реально подключён телефон, получить
   разрешённый device selector/bridge и выполнить только preflight:
   `python scripts/ops/voice_pwa_device_probe.py --serial <authorized-device>
   --preview-url https://kenigevents.ru/<real-preview-prefix>/`.
   До фактического опубликованного prefix команда остаётся шаблоном. Не
   выполнять uninstall/pm clear/logout/очистку аудио/очередей, не открывать
   ADB/DevTools в Интернет. Donor ADB handoff команды reinstall/purge не являются
   разрешением для этой задачи.
4. После этих bindings: existing builder/prefix, настоящий microphone → ASR →
   explicit confirmation → обычные Search cards → refinement/history/action,
   sink readout и physical PWA. Исторические unit/synthetic результаты не
   переносятся в эту live матрицу.

**Статусы:** исходники/локальные проверки — Done; интеграция исходников в PR —
Done; live ASR/Auth/cards/refinement/sink/public prototype — Partial/Blocked
на перечисленных bindings; physical phone — Blocked на реальном bridge/selector.
В этой итерации provider calls, cloud/DB writes, phone mutations, full catalog
или Kaggle build **не выполнялись**.

Push `b92b55d6b` создал run **34018201563** для постороннего
`.github/workflows/region-talk-research-remediation-export-20260802.yml`:
`failure`, `jobs=[]`. Это **не CI PASS** и не провал voice runtime теста.
У собственного нового top-of-Unreleased insertion был конфликт с актуальным
main; запись voice перенесена ниже в той же Unreleased без изменения текста
или чужих записей. Сторонний invalid workflow этой lane не исправляется.
Свежий terminal CI по итоговому SHA следует проверить отдельно.

Sanitized local evidence: `artifacts/codex/voice-integration-discovery/`
(`management-access.json`, `runtime-metadata.json`, `schema-readonly.json`,
`shared-model-registry.json`, `donor-runtime-config.json`, PR/readback/Actions).
В Git сохраняется этот сводный readout, не credentials, raw речи или аудио.

### Desktop capture-only slice (2026-09-06; not ASR acceptance)

Отсутствие телефона **не блокирует desktop**. В существующий Search добавлен
preview-only режим `PUBLIC_EVENT_SEARCH_ASSISTANT_CAPTURE_ONLY=1` вместе с
`PUBLIC_EVENT_SEARCH_ASSISTANT_ENABLED=1`. Оба флага browser-safe; capture-only
проверяется loader-ом как `0|1` (также разрешён `STATIC_SITE_PUBLIC_*` alias).
SSR выводит `data-assistant-capture-only="true"` и явный дисклеймер: локальная
запись/сохранение/прослушивание, распознавание ещё не подключено. Обычный Auth
не обходится: до входа запись disabled; смена аккаунта скрывает чужие записи.
AudioWorklet/owner-scoped IndexedDB не заменены новым recorder/store.

В этом режиме отсутствуют **все assistant network calls**, включая status
сохранённой истории при Auth initialize, ручную историю, интерпретацию и ASR.
Два слоя защиты: UI не создаёт ConversationController и не включает server
controls; AssistantClient отказывает до getSession/transport. Обычные Search и
Auth запросы этим режимом не запрещаются. Это не обещание offline-входа.
Аудио остаётся в текущем browser profile/origin; не очищать сессию, site data,
очереди или аудио. Mic запускается только явной кнопкой пользователя.

**Реальная desktop preflight:** full Chromium `143.0.7499.4` без fake-device
flag: secure context, MediaDevices и AudioWorklet доступны; audio input/output
count = **0/0**, `/dev/snd` без PCM capture nodes, Pulse/PipeWire socket нет.
`getUserMedia`/запись не запускались, чужие аудиофайлы не читались. Это ограничение
данного dev-host, не браузера пользователя. Существующий remote browser bridge
доступен, но обнаруженные профили не подтверждают обычную KenigEvents сессию или
микрофон; сторонние профили не заимствованы. Нужен пользовательский Chrome/Edge:
обычный вход → «Записать голосом» → Stop → раскрыть сохранённые записи →
«Прослушать» → reload и повторное прослушивание. Публичный prefix готовит parent;
до HTTP/SSR readback публикация и реальная пользовательская запись не PASS.

**Честное разделение проверок:**
- `site/tests/assistant-capture-only.test.mjs`: 2 unit PASS; все server routes
  отклонены до Auth/session/network, public flag validated.
- `site/tests/voice-capture-only.integration.mjs`: full Chromium PASS, реальный
  DOM mount/IndexedDB/WAV decoding/reload; **Auth snapshot и сохранённый WAV —
  test fixtures**, не реальная речь/вход. Зафиксированы 0 assistant requests
  browser-side и server-side, 0 session reads, disabled submit/history/ASR,
  локальное аудио после reload, signout блокирует record и скрывает записи.
- Ранее 5 capture tests используют synthetic microphone; их PASS не становится
  real ASR, физической записью или phone acceptance.
- Live ASR/interpretation/Search/dialogue остаются на описанном выше отсутствии
  KenigEvents policy/model binding и assistant schema/runtime; телефон к этому
  desktop blocker не относится. Local запуск existing handler допустим как host,
  но без bindings реальная обработка потребовала бы тех же shared DB/limiter
  writes. Dummy repo/policy или donor owner auth не добавлены.

**Focused build, не full catalog:** из уже существующей islands ветки
`960c90ef5aced6b2e51b1f65fd08041efa055f9e` переиспользованы без изменений
`site/scripts/page-class-build-filter.mjs`, versioned page-class JSON и его 8
behavior tests; стандартная integration добавлена в `astro.config.mjs`.
Default `all` сохраняет прежнее поведение; новый builder/entrypoint не создан.
Для existing `build:preview` parent использует `STATIC_SITE_PAGE_CLASSES=personal`
и `STATIC_SITE_FOCUSED_ROUTES` с `/poisk/`, `/__preview/`, `/robots.txt`,
`/manifest.webmanifest`, `/pwa-sw.js`. Registry относит Search/manifest/SW к
personal; preview/robots остаются обязательной поддержкой. Auth redirectTo
использует `cleanStaticAuthUrl()` текущего `/poisk/`; отдельного callback route
нет. `PUBLIC_PWA_START_URL` должен указывать на `/poisk/` **в том же prefix**.
Публикуется только immutable static prefix, не shared Edge/DB и не root.
Source lane не запускает build и не меняет `site/dist`; публикацию/readback
выполняет parent из clean exact SHA. Sanitized dev preflight сохранён локально в
`artifacts/codex/voice-desktop-real/`, не коммитится с raw media/credentials.

Read-only Auth redirect verification (Management API HTTP **200**, no changes):
`site_url=https://kenigevents.ru`,
`uri_allow_list=https://kenigevents.ru/**,https://www.kenigevents.ru/**`.
The requested `/preview-voice-desktop-capture-20260906/poisk/` matches the existing
`**` rule per [Supabase redirect wildcard contract](https://supabase.com/docs/guides/auth/redirect-urls).
Only these public URL fields were retained; no raw auth config or secrets.
This verifies configuration, **not an OAuth login or authenticated capture**.
Local sanitized receipt: `artifacts/codex/voice-desktop-real/auth-redirect-readonly.json`.

### Published desktop capture-only receipt (2026-09-06)

User-test-ready static preview:
[desktop Search / local voice capture](https://kenigevents.ru/preview-voice-desktop-capture-20260906/poisk/).
**Built source SHA:** `f0dd8cca6e4dc07352eebbfb1e64b68be4191b42`.
This later documentation receipt does not change the published build identity.
Existing `build:preview` produced only two pages (Search + preview index) and
three support routes; existing publisher wrote the new immutable prefix only.
No full catalog/Kaggle build, shared Edge/DB mutation or root promotion.

Parent verified 7 exact byte readbacks (five routes, preview-build manifest,
worklet), public Chromium signed-out desktop 1440/1280 plus manifest scope
(3 checks PASS), configured ordinary Auth, visible capture-only notice, disabled
signed-out controls, correct worklet MIME, zero assistant calls/page errors;
local/public screenshots were inspected. Source lane independently read back
HTTP200, SSR capture-only marker and notice. Link delivered and read back in
[Telegram message 1440](https://t.me/c/4337049383/1440).
Sanitized publication/browser/TG receipts:
`artifacts/codex/voice-desktop-preview-20260906/` in the primary checkout.

**Current source CI:** [run 34019001634](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/34019001634),
exact `f0dd8cca6e4dc07352eebbfb1e64b68be4191b42`, completed/success with all three
jobs (`smart-update-identity-state-machine`, `static-browser-release-gate`,
`python-ci`) success. This is actual CI PASS, unlike the historical empty-jobs
run above, but still **not real microphone, Auth login or ASR proof**.

Done: source integration, deterministic/native-fixture checks, focused static
publication, anonymous public desktop/render/network checks, existing redirect
allowlist verification, user link delivery. Partial: authenticated capture on
user's desktop (ordinary login + explicit microphone action + playback/reload
awaiting user execution). Blocked: live ASR/interpretation/dialogue/cards/actions
acceptance on the still-unbound assistant runtime; policy/model and shared
runtime/schema boundary remain as documented. Phone is a separate remaining
PWA acceptance scenario, not a prerequisite for the published desktop slice.

## Product correction: floating voice entry (2026-09-06)

The recorder dashboard in the desktop-capture preview is **superseded as product
UI**, not accepted as conversational Search. Product answer history means
previous query/result sections, not a user-managed file-saving workflow.
The original product/v1 specification was reread with the implementation worker.

- Entry is a floating microphone on desktop/mobile. Existing `EventLayout` owns
  its position relative to the existing bottom navigation; Search does not
  introduce a second navigation shell or sticky heading. The existing adapter
  opens the same composer, and results remain ordinary in-flow sections with
  semantic H2, explanation, common cards and refinement actions. Full FI-16/17
  occupied-rectangle routing/native component certification remains incomplete.
- The composer opens on explicit action rather than occupying the initial page.
  Local audio protection stays automatic; playback/recovery is secondary and
  appears only when audio exists. Previous selections are not a “save answer” step.
- Guest microphone clicks open a visible sign-in explanation. The same Auth
  controller handles login without invoking classic Search's draft/auto-run
  click handler. OAuth errors (including a resolved `false`) appear in the
  composer. Login never automatically starts recording or sends a request.
- Complete user-stopped capture in an enabled environment now requests ASR
  directly, without a separate “save → recognize” journey. The transcript is
  editable before Search submission. A revision guard prevents late ASR from
  replacing typed input/new recording/new search/refinement context. Partial or
  interrupted capture is not auto-sent; unknown provider outcome is not retried.
- Close/Escape awaits recording stop and returns focus to the mic. Missing mic,
  denied permission, AudioContext construction failure, storage failure and
  mount failure have explicit feedback; startup failure never leaves a silent
  disabled launcher. Browser dialog behavior follows the
  [native dialog contract](https://developer.mozilla.org/en-US/docs/Web/API/HTMLDialogElement/showModal).

**Confirmed reproduction:** anonymous Chromium on the old public preview:
`data-assistant-record.disabled=true`; a real mouse click changed neither status
nor Auth prompt. This does not identify the user's authenticated-session state.

**Scope of this correction:** the preview still uses capture-only safeguards.
ASR/AI/cards from speech remain unavailable until the already-documented actual
model-policy/runtime/schema integration is resolved. The microphone UI is not
proof of live conversational Search. No root promotion, production Edge/DB
change, fabricated AI results, deleted sessions/audio or Kaggle build.

Checks/evidence are recorded below after rendered-preview verification. Local
artifacts: `artifacts/codex/voice-floating-composer-20260906/` in the voice worktree.

Correction validation before publication: 82 assistant unit tests PASS; 6 native
Chromium synthetic-device capture cases PASS; 1 mount/reload/false-OAuth fixture
PASS; 2 composer close/focus/failure cases PASS; 3 capture→transcript orchestration
fixtures PASS (normal, late result after edit, interrupted capture not sent).
The latter explicitly inject capture/provider/controller, not live model output.
Strict TypeScript, Astro compile, revision check, source-surface gate and diff
whitespace check PASS. Anonymous local rendered checks at 1440/1280/390px PASS;
page/composer screenshots inspected. An initial harness-only login failure was
fixed by removing the fixture's unintended disabled attribute from login.
The shared-shell selector review also caught and corrected an accidental H1
selector grouping before publication. No new live ASR/Auth/phone claim.

### Published correction and real donor-reuse verification

Published UI source: `68452a2a6be5700bf46b09b4905d4e29a2f26048`.
[Direct Search preview](https://kenigevents.ru/preview-voice-floating-20260906/poisk/),
[Telegram delivery/readback 1441](https://t.me/c/4337049383/1441).
Seven public files match local SHA256 exactly. Public anonymous Chromium
1440/1280/390px: 3/3 PASS, zero assistant network calls/errors; keyboard
close/reopen/reload, guest feedback, H1 non-floating, mic/nav separation checked.
Final local build has the same 3/3 PASS. Production/root untouched.

**Wonderful Lections is a working donor, not an unimplemented concept.** Its
`src/runtime/transcription.mjs` → `gemini-gateway.mjs` →
`scripts/gemini-gateway-worker.py` uses pinned `vibepublish/google_ai/` public
`GoogleAIClient.generate_content_async`, server-only credentials and shared
quota. No donor owner bearer or IdeaHub publishing is needed for an ASR probe.
The active model is `gemini-3.1-flash-lite`. The verified synthetic fixture is
`mixed-notes.wav`, 8.51573696s mono PCM16 22050Hz, SHA256
`2e03de2625a7dcb2e32040cc5518505a42902c1d4690c6b8ad17fe6783ad619b`;
provenance: Wonderful Lections `docs/product/review-reliability-and-speaker-notes.md`
Observed validation (source `f8ee3d982b49d70ecb6041bb1aa22cbf5047fc2e`).
No private owner speech was read or sent. Prior ASR output is not an independent
quality oracle.

Two **real model calls**, no mocks/fallback or automatic provider retry:

1. Existing pinned WL GoogleAI client, credentials from **events-bot-new** env,
   consumer `kenigevents.voice.asr.probe.v1`, account `kenigevents`: Russian
   transcript returned; usage 243 input +32 output =275 tokens.
2. **Existing KenigEvents `assistantGenerator`**, its vocabulary/schema and
   strict project/model atomic limiter on the existing personalization/Search
   DB: dispatched → completed at 2885ms → accounted at 2944ms; valid
   `{text, uncertain:[]}`; 431 input +48 output =479 tokens. Explicit diagnostic
   key lane `GOOGLE_API_KEY6` (active registered key), not an assertion that the
   published Edge uses that non-default pool. Request UID
   `60741a72-f3eb-4644-825b-4150dc163f1b`. Raw transcript remains in ignored artifacts.

**Configuration correction:** a first local TS diagnostic mistakenly paired the
new strict quota adapter with the donor's legacy quota project. It stopped before
provider dispatch with `42703` (missing `quota_scope` column). This was a probe
configuration error, not proof that the ASR implementation or credentials were
broken. Actual runtime pairs must stay distinct:
- WL gateway: `SUPABASE_URL` + `SUPABASE_KEY` (legacy shared quota schema).
- Existing Search adapter: `PERSONALIZATION_SUPABASE_URL` +
  `PERSONALIZATION_SUPABASE_SECRET_KEY` (project/model atomic quota schema).
Never mix a URL from one pair with a key from the other or weaken strict quota to
make the wrong DB work. Both registries contain the tested Lite model. The earlier
blanket “ASR not available” assessment is narrowed: **provider availability is now
verified**, while public route/schema/user bindings are still not enabled.

Remaining: actual browser speech → deployed `/event-search/assistant/*` → durable
Search receipts → current retrieval/ordinary cards → explanation/refinement/
history/actions. That requires the already-prepared additive migration and the
existing shared Edge update; no isolated staging branch exists. Parent asked for
an explicit decision on this protected-preview shared runtime update, as required
by source specification §9; no production DB/Edge writes were made while waiting.
Phone and real user's desktop microphone/login acceptance remain unverified.

Artifacts in the same ignored correction directory: `live-asr/` contains scoped
probe scripts, metadata/usage/checkpoint receipts and private provider output;
`public-readback.json`, `public-browser.log`, screenshots and Telegram receipt.
No raw audio/transcript, credential or session is committed. Latest code push had
only unrelated invalid workflow run `34020265628` (`jobs=[]`); this is **not** a
CI pass or a voice test failure. The current PR's own changelog insertion was moved
next to its existing voice entry to avoid a conflict with parallel main entries.

### Nonmodal microphone and bounded local checkpointing (2026-09-06)

The modal correction above is itself **superseded**: the owner tried real
recordings after Yandex login and rejected the window interaction. Successful
owner login is user feedback, not a controlled Auth/device test.

Read actual donor implementations rather than only ASR modules:
- Wonderful Lections `site/public/review/review.css` (`.microphone`, `.mic-halo`,
  1.8s ease-in-out breath and reduced-motion), `review.js` microphone toggle,
  `starting/stopping` gates, durable `chunkWrites`/seal before saved state.
- record-idea-hub `android/app/src/main/java/com/onedayonemasterpiece/recordideahub/`
  `MainActivity.kt` (`animateMode`, capture vs upload/transcription status),
  `RecordingService.kt` and `M4aChunkWriter.kt` (local commit and recovery).
  Reused interaction/reliability rules, not owner Auth, IdeaHub publication,
  deletion policy or native VAD. No speculative silence trimming is introduced.

Existing shared EventLayout positions the same Search control: circular 64px
(desktop)/56px (mobile), stationary hit target, halo-only pulse, stop square,
recording timer and short adjacent status. Clicking the mic toggles capture;
no modal/backdrop, focus trap, auto-scroll or new navigation floor. Escape stops
capture. The same composer is an explicit in-flow `Запрос и записи` surface;
starting a recording hides it, and successful local capture does not reopen it.
Guest sign-in remains through the existing Auth controller. Microphone denial,
startup failure and missing storage keep explicit feedback. Processing/persistence
is not falsely presented as ASR completion.

Reliability changes:
- StreamingPcm16 accepts an optional max-part frame bound. Real capture uses one
  second at the actual sample rate (or smaller if required by wire budget).
  Every part uses the existing strict IndexedDB transaction and hash checks;
  no resampling, silence removal, new DB name/version or user-data cleanup.
- The stop indicator remains `saving` until the recording receipt commits.
  Repeated clicks cannot create another recording during that commit.
- Failed final receipt retains the same audio and exposes local save retry;
  it does not show “saved”. Already committed parts survive an abruptly closed
  page as unfinished recovery data. This is not zero-loss assurance: the current
  uncommitted tail, worklet messages or pending storage transaction can be lost
  on OS/browser termination. Periodic checkpoints reduce that window.
- Conditional beforeunload warning covers active/pending/unsaved capture. It is
  not a guaranteed mobile shutdown hook; existing visibility/pagehide stop stays.
  [Browser lifecycle limitation](https://developer.mozilla.org/en-US/docs/Web/API/Window/beforeunload_event).

Focused validation: 83 L0 unit checks, 7 native Chromium synthetic-device capture
cases (including durable parts before Stop and abrupt page closure), compositor
and full mount tests for repeated capture/no panel, terminal-save delay, local
retry, unchanged audio after reload, and zero assistant network. Injected Auth
snapshots and synthetic microphone are explicitly not the user's Yandex session
or real phone. Public/rendered viewport checks cover 1440/1280/390px, circular
hit geometry, nonmodal guest route, halo motion and reduced-motion. Recording
screenshots in this suite are labelled render fixtures, not physical capture.

Scope tags: voice/microphone/indexeddb/recovery/keyboard/animation. The skill path
required by `site/AGENTS.md`, `.codex/skills/static-site-autotest/SKILL.md`, is absent
in this checkout and the active primary checkout; used its named canonical
strategy, scenario registry and release gates directly instead. Android/iOS
system evidence remains unavailable/not run; this is an isolated review preview,
not mobile-system certification or production promotion. No new server/model
call or runtime enablement in this UI correction. Existing user session and audio
remain on the same origin and unchanged owner-scoped DB.

Artifacts: `artifacts/codex/voice-orb-20260906/` in the voice worktree.

Donor source snapshots inspected: Wonderful Lections `cceac0c1fbab6cdca881f642accf5f87cf802487`; record-idea-hub `294c3485f377570505800516e2e86e58a6141781`. Full-mount capture-only suite: 3/3 PASS, including failed final receipt and same-record local retry.
