# INC-2026-09-06-voice-preview-startup — зависший запуск микрофона в preview

Status: open
Severity: sev3
Service: preview-only `/preview-voice-devcoveer-20260906/poisk/`
Opened: 2026-09-06
Closed: —
Owners: voice-search PR #587
Related docs: `docs/features/static-site-pages/smart-vector-search/voice-search-solution-v1.md`

## Summary / User Impact

На Android внутри Telegram владелец видит вошедший аккаунт, но нажатие микрофона
оставляет «Поиск подключается. Попробуйте через несколько секунд». Запись не
начинается; нижняя форма может показывать устаревшее приглашение войти.
Это ограниченный пользовательский preview, **не утверждение об аварии production**
и не разрешение продвигать ветку или менять production/service configuration.

## Detection / Timeline (UTC)

- 2026-09-06 10:38: screenshot Telegram message 1446 (12:38 локального телефона).
- Parent read-only проверка: public `/healthz` HTTP 200, user service active,
  source `923915eb`; последние принятые QA operations 09:31:55, новых операций
  в момент пользовательской попытки нет. Journal после 10:20 без записей.
  Пользовательское аудио/содержимое запросов не читались.
- Fresh published-preview probe: ordinary QA `session_fixture`, mobile Chromium
  Pixel 7 emulation: без инъекции микрофон начинает native запись; с задержанным
  единственным callback `VoiceStore` IndexedDB open — **точная исходная фраза**,
  Auth signed_in, capture idle, ноль page errors и assistant requests.

## Root Cause

Mechanical/lifecycle, не semantic/event meaning.

1. `mountConversationalSearch` ждёт неограниченный `VoiceStore.open()` **до**
   `presentation.bind` и подписки на Auth. Mic остаётся с default loading handler.
2. Нельзя утверждать, почему именно физический WebView не завершил IndexedDB:
   screenshot не содержит device storage trace. Доказаны воспроизводимый путь
   frontend-зависания и отсутствие timeout/retry, не конкретная поломка Android.
3. Обычный `renderAuthState` не заменяет guest status при signed_in. Очистка
   случайно зависит от quota RPC; ошибка/задержка RPC оставляет ложный guest prompt.
4. Auth может включить submit раньше восстановления локальной conversation;
   проверка controller readiness входит в регрессионный контракт этого исправления.

## Automation Contract

### Treat as regression guard when

VoiceStore open/upgrade, assistant mount/presentation, Auth-to-search readiness,
conversation initialization и обычный Search auth status.

### Mandatory checks before closure / preview publication

- IndexedDB delayed/blocked/late success: bounded visible outcome, retry only by
  user intent, late connection closes; никакой очистки DB/сессий/очередей.
- Mic never auto-starts after storage retry/Auth recovery; explicit next gesture.
- Readiness/Auth owner changes не разрешают submit до restore; старый callback
  не включает контролы для другой identity.
- Signed-in обычный Search не показывает guest prompt при quota failure.
- Native capture/PCM/compressed recovery regression suite; mounted mobile UI.
- Exact authorized preview smoke после публикации. Physical Telegram Android
  остаётся отдельной проверкой; эмуляция не является real-device PASS.

## Immediate Mitigation

Изменения сервиса, production, пользовательского хранилища и provider не выполнялись.

## Corrective Actions / Prevention

Implemented locally: bounded 8s open, allowlisted startup/error diagnostic,
manual retry without reload/clear/auto-mic, late connection cleanup and
current-owner controller-ready guard. Ordinary Search guest copy clears on
signed_in even if quota fails; repeated Auth snapshots preserve Search output.

## Release And Closure Evidence

- Base: `origin/docs/agent-assisted-event-discovery-20260826` `2485c273a`.
- Reproduction: isolated worktree `voice-startup`,
  `artifacts/codex/voice-startup/mobile-auth-reproduction.json` and
  `mobile-auth-{normal,idb-stall}.png`; synthetic QA identity only.
- Fixed commit / preview publication: pending parent integration; no deployment.
- Local regressions: 22 unit checks passed (`voice-startup-store`,
  `search-auth-startup`, `search-initial-state`, `search-recovery`,
  `static-site-auth`); 24 browser checks passed (`voice-startup`,
  `voice-capture-only`, `voice-flow`, `voice-compression`, `voice-browser`).
  Logs: `artifacts/codex/voice-startup/browser-tests.log`.
- Exact public-page HTML/CSS + **browser-local replaced JS**, real ordinary QA
  session fixture, Pixel 7 viewport, quota503 fault: normal native mic records;
  delayed storage -> timeout -> manual reconnect -> ready/idle without auto mic;
  ordinary form shows signed-in ready. Both scenarios: zero page errors and
  zero assistant requests. `patched-mobile-auth-readback.json`,
  `patched-mobile-auth-{normal,idb-stall}.png`, `patched-storage-error.png`.
  Screenshots inspected; these are not deployed-bundle/physical-device proofs.
- This record remains open until preview readback; no production rollout is implied.

## Follow-up Actions

- [ ] Parent: integrate committed fix into PR #587 and publish only authorized preview.
- [ ] Parent/user: verify the same Telegram Android surface, without clearing data.
