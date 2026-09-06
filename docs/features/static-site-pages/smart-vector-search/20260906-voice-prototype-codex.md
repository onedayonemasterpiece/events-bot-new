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
