# ChatGPT: довести голосовой разговорный поиск KenigEvents до работающего прототипа

Дата: 2026-09-06. Класс задачи: самостоятельная продуктовая разработка и проверка в ChatGPT, затем конкретный integration/debug пакет для ручного Codex. Это не новое исследование с нуля и не задача только на написание ещё одного плана.

## 1. Поручение и результат

Продолжи existing events-bot-new PR #587, ветку `docs/agent-assisted-event-discovery-20260826`. Владелец поручил закончить проектирование и лично в ChatGPT написать максимально работоспособный код голосового поиска. Затем Codex должен интегрировать, подключить инфраструктуру и помочь отладить, а не заново изобретать продукт. Острова уже проектируются/реализуются в другом окне: не дублируй их работу, читай текущий adapter и совместимость.

Цель: на реальной странице сайта/PWA пользователь записывает вопрос, получает исправляемую расшифровку, понятное состояние обработки и обычные карточки; уточняет голосом/текстом без потери контекста; видит историю самостоятельных ответов; возвращается к старому разделу; скрывает событие, и оно не появляется снова на другой поверхности. Один общий Auth, транспорт, limiter, профиль, статистика и публикация. Вопрос об адресе может дать полезное пояснение без карточек.

Не заканчивай после добавления пары тестов или scaffolding. Выполняй законченные source-пакеты последовательно, сохраняй небольшие commits с readback. Если реально недоступны нужные runtime-инструменты, заверши всё независимое source/test work, точно обозначь границу и подготовь исполняемую постановку Codex. Не выдавай mocked response за распознанную речь, local harness за опубликованный сайт или unit PASS за live prototype.

## 2. Что уже сделано, что НЕ сделано

Проверенный кодовый старт: `d913010ea0de9d157b2d64763846495d4b482bec`.

- `site/src/lib/assistant/conversationState.ts` — чистые неизменяемые переходы текстового/расшифрованного диалога, порядок и дедупликация, version tickets для подавления устаревших результатов, history/parent, subset/expansion, explicit visibility overlay.
- `site/src/lib/assistant/audioSegments.ts` — PCM16 WAV parts с настоящим sample rate, непрерывными frame offsets и учётом binary/base64 wire envelope. Это НЕ microphone capture, НЕ VAD, НЕ ASR. Слова на границах не обрезаются; каждый сегмент декодируем самостоятельно.
- `site/tests/assistant-conversation.test.mjs`, `assistant-audio.test.mjs` — **49 локально выполненных тестов, 0 failed, 0 skipped** на Node 22.16.0; отдельно strict TypeScript 5.8.3 check двух source modules.
- Existing `.github/workflows/ci.yaml` получил одну команду этих тестов в `static-browser-release-gate` перед npm ci. Новый workflow/framework/runner не создан. Terminal GitHub Actions PASS пока НЕ получен.

Exact Git blob readback совпал с локально проверенными байтами:

| Файл | Git blob |
|---|---|
| conversationState.ts | `df7c05354d1b25834b0d173d08cbd0ebaa920a75` |
| audioSegments.ts | `4758123ba240b4ad1f90276875da55ff1737ffd1` |
| assistant-conversation.test.mjs | `3637ea6800babcfc690bf90b3b2d4e34aea27e3d` |
| assistant-audio.test.mjs | `27444f6d8b4f254cf761ba6c8e18b461cfa5bf79` |

Проверка из root:

```sh
node --experimental-strip-types --test site/tests/assistant-*.test.mjs
tsc --noEmit --strict --target es2023 --module esnext --moduleResolution bundler --lib es2023,dom site/src/lib/assistant/*.ts
```

Границы старта: source-модули ещё не подключены к Astro, API, Auth, БД, реальному микрофону, provider, профилю и sink аналитики. `Intent` пока содержит только goal/locality/excluded formats/free/price; добавь реальные typed dates/timezone, аудиторию и прочие необходимые поля по спецификации, не обходи их свободной прозой. `acceptInput` работает только с непрерывным префиксом: durable intake должен сохранять out-of-order upload/реплики и передавать их сюда в порядке, а не терять U2 из-за sequence conflict. CAS здесь — требование к хосту, не реализованная транзакция БД. Сохранённый provider outcome можно безопасно повторно применить после проверки базы, не повторяя внешний запрос.

Текущие memory caps в ядре — защитные пределы локального проверяемого slice, не продуктовые квоты. Для длинной истории реализуй bounded pagination/compaction без удаления принятой речи; целый State нельзя класть в ограниченный 64 KiB localStorage. `failDraft` сейчас защищает failure текущего retrieval ticket; отдельные ошибки capture/intake/ASR потребуют соответствующих стадийных receipts. Код не проверяет Auth, ownership, подписи или достоверность текста ответа: это отдельные реальные границы, а не повод считать ядро безопасным сервером само по себе.

Source-сохранение пока не является завершённым release-пакетом: обнови основной CHANGELOG.md адресной вставкой в Unreleased и canonical scenario registry, сохранив всё чужое содержимое. В предыдущем окне они не правились, чтобы не заменять огромные общие файлы неполным прочитанным текстом. Не создавать вместо них новый параллельный changelog/реестр.

## 3. Первое чтение — ограниченное и предметное

Прочитай актуальные HEAD/comments #587, ветки интеграции и применимые AGENTS/skills; не доверяй PR body как текущему HEAD. В этой ветке параллельно меняется routing островов: не force-push и не затирать чужие пути. Точные SHA выше — evidence, не указание откатить ветку.

Основные документы в events-bot-new:

- текущие `agent-assisted-event-discovery.md`, `voice-search-solution-v1.md` в этой папке;
- `docs/features/static-personal-announcements/{README.md,release-integration.md,global-product-decisions.md}`;
- `docs/features/static-site-pages/design-system/floating-islands.md` — **актуальный routing другой работы**, не старое описание sticky из первого voice v1;
- canonical Search, `unsigned-personalization/production-integration.md`, `operations/yandex-dependency-resilience.md`, analytics, personalization-to-be и ручные requirements, ownership;
- actual AuthorizedEventSearch, EventCard/renderer, backendOperationCatalog, Search Edge/retrieval/quota modules, существующие profile/action/analytics adapters, existing test harness;
- #621 и current release/build contract, только для интеграции и принятия готовых family APIs. Не повторять широкий аудит всего F1–F17.

Последний прочитанный routing островов указывает DS #47 и `eb3309591be368d729ea52c90b6ef99d1acbad6b`: top-row v1.1. Semantic H2 остаётся в разделе, общей верхней строке принадлежит non-heading locator/section action. Поэтому не создавать ещё один отдельный sticky этаж или клон H2. Viewed section, refinement base и draft независимы. Прочитай более новый HEAD, если появился.

## 4. Переиспользование — конкретные кандидаты

В `onedayonemasterpiece/my-data-hub` проверен source `12c330a96e5db7d781a9283d38f6bc0069d8f89d`:

- `src/my_data_hub/voice_intake_v2/inference.py`: AggregateGeminiInference, checkpoint, bounded HTTP, общий limiter, structured output и учёт стадий;
- `voice_intake_v2/runtime.py`: compose worker/media/store; `media.py`, `checkpoint.py`, `store.py`, `worker.py` нужно дочитать по реальной зависимости;
- `src/my_data_hub/google_ai/` и `voice_intake/gemini.py`: reusable provider/limiter boundary.

**Не использовать целый voice-intake pipeline как website Search:** сейчас он привязан к авторизации владельца, терминологии, сводке и публикации в IdeaHub. В частности, `account_name="record-idea-hub"`, prompt про сессию владельца и publisher не переносятся в публичный сайт. Извлекай/используй узкую общую provider/media/checkpoint часть через явный API; не вызывай private `_generate` как якобы стабильный контракт и не копируй raw-key provider client.

В `record-idea-hub` изучи актуальные recorder/VAD/segment/recovery tests. Android native WebRTC VAD нельзя просто импортировать в PWA: переиспользуются проверенные правила паузы, сохранения начала фразы и fail-open, а не обещание бинарной совместимости Kotlin с браузером. Непроверенный energy threshold не должен вырезать тихое «не». Для первого прототипа допустим явно обозначенный continuous foreground capture вместо некачественной «оптимизации» речи.

В events-bot-new `audio_transcription/service.py` существует AudioTranscriptionService, но он связан с Telegram/Kaggle orchestration. Не добавляй Kaggle cold-start или Telegram-сессию в интерактивный запрос сайта. Переиспользование конкретных guards/типов возможно; wholesale hot-path reuse не предполагается.

Qwen: existing idea-hub `skills/voice-file-qwen3-tts/`, BASELINE/LONGFORM/RUNBOOK и проверенный ранее donor `zigomaro/yazyki-rossii-qwen3-tts-cpu-0901-r2/2`. Перед запуском проверь actual notebook/config/status через my-data-hub. Не перезаписывай лекционную job, не клонируй реальные голоса без разрешения, не запускай синтез на каждый PR.

## 5. Завершение технических решений без повторного проектирования продукта

Выбери минимальный reuse-состав по текущему коду и закрепи адресный diff в voice v1. Предпочтение — уже работающий service/worker, а не отдельный процесс ради прежнего предположения «обязательно TypeScript-сервис». Но shared Search нельзя скопировать в другой язык и объявить независимой реализацией. Зафиксируй один практический вариант размещения и узкие typed adapters после чтения кандидатов; не оставляй два обязательных конкурирующих backend.

Порядок результата:

1. Рабочий текстовый vertical slice через реальные Auth/receipt/retrieval/card paths с fake provider boundary только в тестах.
2. Capture → bounded segments → durable receipt → разрешённый ASR/intent → те же результаты. Типизированное interpretation не заменять regex-парсером нескольких демонстрационных фраз.
3. Догон, история/parent, поправки, subset/expansion, current global hide/profile, короткое grounded explanation, recovery.
4. Реальные DB/HTTP/browser tests и согласованный UI adapter; protected permitted live subset, затем опубликованный owner preview/PWA.

Не ждать завершения всех красивых вариантов островов: сначала существующий inline composer/common components, затем adapter к текущей общей системе. Временный макет не объявлять A=S=P. Core no-UI code не требует рисовать новые Penpot components.

Раздели три вещи: готовый исходный код, работающий локальный интеграционный прототип и опубликованный прототип с реальным provider. Не остановись на первом, когда доступен следующий. Невозможность public provider enablement не должна останавливать независимые mock/source/browser работы, но разрешённость модели не обходится VPN, чужими ключами или автоматической сменой модели.

## 6. Продуктовые инварианты

- Персонализация предусмотрена соглашением; нет отдельного обязательного включателя, consent-token или второго шага после лайка. Техническое начало профиля не opt-in UI. Query context не равен долгосрочному вкусу. Purpose analytics/email/research остаются независимыми.
- Уточнение не выбрасывает прежний текст. Capture и network work независимы, есть видимый stop и честный pending/unknown/error. Обновление выдачи не обнуляет прошлые ответы.
- Принятые U1/U2/U3 сохраняются, а поздний результат не отменяет актуальные условия. Отмена браузерного fetch не доказывает отмену внешнего вызова. Между provider dispatch и сохранённым outcome возможна неопределённость; не создавать повторное списание автоматически.
- Одинаковый ID с другим содержимым — конфликт. Две вкладки/устройства не смешивают владельцев. Reset/delete/logout и stale results проверяются реально.
- «Из них» фильтрует полную логическую parent selection; расширение «можно платно» не ограничивается старым free-only membership. Unknown price не бесплатно. Даты anchored к высказыванию/Europe/Kaliningrad, не timezone тестовой машины.
- Старые разделы соблюдают текущий exact hide и lifecycle; видимые карточки не прыгают после profile refresh. Query выше общего профиля. «Второе» относится к записанному реально видимому списку.
- Один общий limiter, разные динамические allowances; не душить spare capacity маленьким фиксированным cap. Все real Google calls проходят approved reserve/mark_sent/finalize; тесты тоже. Бюджет/CPU Qwen учитывается в соответствующем resource policy, не как Gemini RPM.
- Direct/relay имеют разные фактические caps и health. Маленький JSON probe не доказывает upload; control и media независимы. Сбой optional analytics не ломает основной путь.
- Stats: rendered ≠ seen ≠ action committed; stable served-list/section IDs различны; duplicate cards/headers/test traffic не раздувают метрики. Доведи test sink до expected aggregate/readout, не только до записи console.log. Без optional analytics consent продукт работает.

## 7. Тестирование и Qwen

Existing GitHub-hosted CI уже запускает первый core suite. Расширяй existing registry/harness, не добавляй self-hosted runner или отдельную QA-платформу. Нужны strict typecheck, unit/property/fake-clock, actual DB grants/RLS/CAS, HTTP faults (headers/body/decode/timeout), real browser capture и canonical DOM. Provider mock нужен для детерминированности, не для заявления ASR quality.

Сначала около десятка frozen диалогов с независимыми expected slots/negative constraints; затем расширять корпус. Qwen генерирует версии аудио заранее, с hashes, verified spoken content и фактическим sample rate. Несколько человеческих записей — отдельная разрешённая проверка. Нельзя оценивать качество только тем же ASR, который тестируем. Synthetic runs маркируются и не попадают в product dashboards.

Обязательные live/quality случаи: короткое «не»; сумма и исправление цены; местные названия; дата около полуночи; догон до ASR и после retrieval; тихая/длинная речь; потерянный ACK; 429; чужой UID; no-results; unknown facts; history/old parent; global hide; optional analytics off; настоящий microphone deny/revoke; reload/standalone/Back. No budget или policy blocked — не PASS.

## 8. PWA и телефон / OpenCode

Владелец разрешает отдельный тест прототипа на своём телефоне, в том числе через подключённый OpenCode. Это дополнительный канал реального feedback, не замена автотестам и не разрешение стирать приложение, browser profile, очередь, аудио или существующую Auth-сессию.

Подготовь owner-only noindex preview через **существующий один Kaggle builder/current bucket path**. Не нужен APK ради PWA. Проверить HTTPS, существующий manifest start_url/scope/id, asset base path, SW version/cache и возврат Auth на тот же preview. Не ломать уже установленную production PWA. Предпочесть существующий принятый debug/preview launch механизм; не заводить второй publisher или публичный административный обход.

Для отладки — настоящий Android Chrome/PWA и разрешённое USB/ADB pairing/remote debugging; не выставлять DevTools/ADB порты в Интернет. Локальный HTTP localhost тест не доказывает микрофон на LAN HTTP телефона. Не отключать TLS/security ради демонстрации.

Собери безопасный diagnostic bundle: build/code/schema/model/policy/corpus versions, opaque operation/section IDs, этапы и интервалы, результат доставки/ошибка, формат/bytes аудио. Без JWT, cookie, raw voice/query/profile в публичных логах или GitHub. Запись с телефона отдельно согласуется как private fixture; нельзя публиковать её автоматически.

Проверь экранную клавиатуру, разрешение микрофона, паузу/stop, смену сети, сворачивание/блокировку, возвращение, повторное открытие после обновления SW, доступность stop/CTA. При background browser restrictions нужен честный stopped/partial state и сохранённое подтверждённое состояние, не обещание непрерывного прослушивания.

## 9. Когда и что передать Codex

Не делегируй повторный широкий анализ. После максимально завершённого source-пакета сохрани в GitHub отдельный коротко запускаемый handoff Codex: точные repo/PR/SHAs, изменения и сохранённые constraints, реальные команды, env **имена без секретов**, существующие services/routes, миграции/rollback, оставшиеся конкретные integration/debug пункты, acceptance и diagnostic paths.

Codex: integrate/rebase безопасно, установить зависимости, поднять выбранный existing runtime, применить только разрешённые staging migrations/config, прогнать tests, выполнить allowed live path, собрать/проверить один preview и исправить найденные дефекты. Не только написать отчёт о диагностике. Для production/root promotion требуется собственное явное решение; owner prototype не подразумевает массовое включение.

Модель/уровень рассуждения выбираются минимально достаточные, не default дорогое sol/high. Не смешивай prompt для самостоятельного ChatGPT с жёстким deployment ticket. Владелец передаёт только ссылку/короткий текст, не архивы исходников.

## 10. Критерий окончания

Готовый прототип означает проверяемый адрес/локальный запуск и реальный путь «микрофон → подтверждённый ввод → допустимая модель → текущая выборка → уточнение/история → действие», с обозначенным режимом данных и actual evidence. Истинно недоступный этап назови точно, не выдавай stub за исполнение. Code/test/doc changes сохранены, readback совпал, existing unrelated functionality не сломана, CHANGELOG и registry синхронизированы.

В финале: фактически работающий результат и ссылка/команда, exact source и выполненные проверки, реальные остаточные blockers, готовый Codex handoff при необходимости. Не заканчивай обещанием потом написать код или вопросом, продолжать ли уже порученную разработку.
