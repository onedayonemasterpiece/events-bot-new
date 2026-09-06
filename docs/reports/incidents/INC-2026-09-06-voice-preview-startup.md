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
- Worker commit `1ff8979bcd1d9d8dcc49da75ff850153f9955b62`; integrated/pushed in PR #587 as `f4a92b19bd010255b06821bdf8e494d62d72195d`.
- Published preview only: https://kenigevents.ru/preview-voice-startup-20260906/poisk/ . Existing DevCoveer backend source remains `923915eb`; no backend/production change.
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
- Published exact-bundle readback PASS: 7 route/support files byte-identical to
  source `f4a92b19b`; focused 2-page build, 5 parent startup tests, source-surface
  contract and 3 anonymous rendered viewport checks PASS.
- Real ordinary QA Auth fixture, actual public JS (no replacement), Pixel7
  emulation: normal native capture and durable reload PASS (203652B PCM/8074B
  Opus, 101760frames); **injected** IDB callback stall -> timeout with zero mic
  calls -> explicit retry closes one late connection -> ready/idle -> next
  gesture records. Separately injected quota503 retains signed-in Search copy.
  Zero page errors/assistant requests in these startup tests; intentionally
  interrupted stop avoided ASR. This is NOT a new end-to-end ASR acceptance.
  Fixture cleanup PASS. Worker evidence: `voice-startup/artifacts/codex/voice-startup/public-readback/`;
  parent publication/layout: `voice-search-implementation/artifacts/codex/voice-startup-20260906/`.
  Parent inspected timeout screenshot; visible error/retry and existing nav fit.
- CI status at delivery: no PR check runs were attached to `f4a92b19b`.
  Push run34028857535 is a different existing `region-talk-research-remediation-export`
  workflow failing before jobs (jobs/check-runs empty, no logs); not a failed
  startup test and not represented as CI PASS. Relevant local/public checks above
  were run independently. No unrelated workflow edits.
- Telegram direct-link delivery/readback: https://t.me/c/4337049383/1448 .
- Mitigation published; record remains open for physical Telegram Android
  confirmation. The triggering physical storage condition is still unknown;
  no production rollout or physical-device PASS is implied.

## Follow-up Actions

- [x] Parent: integrate committed fix into PR #587 and publish only authorized preview.
- [ ] Parent/user: verify the same Telegram Android surface, without clearing data.

## 11:13 UTC follow-up: confirmed timeout and diagnostic-only investigation

- Telegram screenshot 1450 is **Chrome Android**, not Telegram WebView. It shows
  the updated «Открываю локальные записи…» toast and signed-in Search quota copy.
  The user then explicitly confirmed the local-storage timeout after 10–15s;
  lack of a repeated microphone permission prompt is not evidence of denial.
- Previous working schema was v2; compression integration `d4175e695` added
  `compressedParts` by opening the same DB at v3. The old source already had
  `onversionchange => close()`. We must not claim a missing handler or a frozen
  tab as the established physical cause. Main Auth can work independently.
- Native Chromium evidence: an intentionally held v2 connection blocks a first
  v3 upgrade; another v3 open for that name receives **no event** before timeout,
  while an independent v1 DB opens successfully. Releasing only the fixture's
  v2 connection lets both v3 opens finish. This is a genuine native connection
  queue, not delayed `onsuccess` injection. Legacy audio bytes `[17,29]` remain.
  The mechanism matches the [IndexedDB connection queue contract](https://w3c.github.io/IndexedDB/#connection-queue): requests for the same storage key/name
  run sequentially; waiting on preceding requests happens before `blocked`.
- [Chrome lifecycle guidance](https://developer.chrome.com/docs/web-platform/page-lifecycle-api)
  recommends closing IndexedDB connections on freeze. However our CDP
  `Page.setWebLifecycleState(frozen)` probe did **not** establish a frozen tab:
  headless document stayed `visible`, no freeze event fired, and the real old
  `onversionchange => close()` handler allowed upgrade immediately. This is
  negative/inconclusive evidence, not proof of the user's physical trigger.
  Artifacts: isolated `voice-regression/artifacts/codex/voice-regression/`
  `native-idb-queue.json`, `frozen-idb.json`, scripts and diagnostic screenshot.
- User rejected timeout/retry as a product fix. No capture journal, alternate
  kernel, volatile-success bypass, provider/service change or data cleanup was
  implemented. The immediate release scope is **diagnosis only**: existing UI
  details export a privacy-safe event report, with an explicit isolated probe;
  retry cannot amplify a pending same-DB queue. Report contract is canonical in
  the feature document, not duplicated here.
- Local diagnostics acceptance: 28 relevant unit checks and 6 native browser
  startup checks PASS. The browser suite includes actual queued upgrade,
  unchanged legacy bytes, explicit probe cleanup, no automatic probe/mic/network,
  report privacy/download, bounded ring and previous normal native capture.
  Rendered mobile fixture details inspected; actual published CSS/bundle readback
  remains parent release work. Trigger tags: `forms`, `connectivity`, `auth`;
  Auth mode `mocked_ui` for this native-browser fixture, not live Auth acceptance.
  L2 physical-device storage cause remains unknown and incident stays open.

Next: publish only the diagnostic preview, obtain the user's manually exported
report on the affected device, then choose a cause-specific repair. Do not ask
the user to clear site data, recordings, queues or sessions.

### Diagnostic-only publication/readback — 2026-09-06

- Worker `2c4523df023551b5441ef5e58b4892cf5b1415e5` integrated and pushed to
  PR587 as `87ca55d1346256ce1252b624787b7df6992db6da`.
- Published https://kenigevents.ru/preview-voice-diagnostics-20260906/poisk/
  with the existing create-only publisher, clean source and focused 2-page
  build. Backend/source/service, production and user recordings unchanged.
- Exact public bytes: 7 route/support files match. Parent 6 focused unit checks
  and source-surface contract PASS; worker 28 unit + 6 native startup PASS.
- Actual published bundles, anonymous isolated Chromium390x844: normal and
  held-v2 **test fixture** cases PASS. Expanded diagnostic panel, explicit
  independent empty-DB probe, automatic cleanup of that exact disposable DB,
  native JSON download verified. Blocked report identifies schema2 + blocked
  upgrade; independent probe succeeds and original fixture sentinel remains
  unchanged. No Auth, microphone or ASR calls in this diagnostic acceptance.
  Parent inspected the real rendered report/buttons; no horizontal overflow.
- Parent harness bootstrap hit ESM module resolution and stale global browser
  package issues before running. Node official ESM resolution reference was
  consulted; existing installed site Playwright was used, no dependencies or
  browser cache installed/deleted. These were harness startup errors, not page
  failures. Actual page scenarios completed with zero page errors.
- Evidence: `voice-search-implementation/artifacts/codex/voice-diagnostics-20260906/`
  including `public/receipt.json`, exported reports and rendered screenshots.
- Telegram1452 direct link/instructions sent and read back:
  https://t.me/c/4337049383/1452 . No automatic diagnostic upload; user chooses
  whether to copy/send the metadata-only report.
- **Still open:** awaiting report from the affected physical phone. Diagnostic
  delivery does not restore recording or establish the user's root cause.

### First physical-phone diagnostic — Telegram1453

User-provided report from Chrome152, diagnostic bundleDQ8l1o0V, received
2026-09-06 11:39UTC. `open_requested`187ms (schema3, attempt1) ->
`open_timeout`8188ms. No blocked/upgrade/error/success event. Page was hidden
at3873ms, freeze5463ms, visible5634ms, resume5641ms; pageshow was not persisted.
The open was already pending before freeze, and the recorded freeze lasted
about178ms, so that lifecycle event alone does not explain the8second wait.
Queue blockage is compatible with the trace but not established. No standalone
probe or database metadata events were in this first report; user asked to run
the explicit isolated probe and resend. No data reset, schema bypass or second
recording/search kernel was introduced. Physical report artifact:
`artifacts/codex/voice-diagnostics-20260906/phone/telegram-1453.json`.

### Physical isolated probe and restart recovery — 2026-09-06

- Correct report arrived in Telegram1454 after user clarified earlier copies
  were pasted into Saved Messages, not topic1030. The diagnostic UI was not
  shown to be broken; both agent and parent native-touch/copy checks passed.
  Additional diagnostic-entry UX work is superseded, not deployed.
- Physical Chrome152: main attempt still had no terminal event at146321ms;
  metadata reports voice DB **version2**, isolated v1 probe succeeds in34ms
  (146321→146355), exact disposable cleanup complete at146360ms. This rules out
  blanket IndexedDB unavailability in this measured context, and localizes the
  wait to the existing voice DB/version transition. Saved unchanged as
  `artifacts/codex/voice-diagnostics-20260906/phone/telegram-1454.json`.
- Native supplementary causal reproduction: raw CDP in headed Chrome143,
  synthetic isolated profile, genuine hidden/freeze events on a v2 connection
  that DOES have `onversionchange => close()`: v3 waits silently for8.26s;
  independent v1 opens; resume dispatches versionchange/close then upgrade and
  success; sentinel bytes17,29 unchanged. Unlike the earlier focus-emulated
  Playwright attempt, actual freeze was observed. Worker artifacts:
  `voice-regression/artifacts/codex/voice-regression/raw-cdp-freeze-8s.{json,mjs}`.
  Spec opening step10.3 waits for versionchange dispatch before deciding blocked:
  https://w3c.github.io/IndexedDB/#opening . This reproduces the mechanism but
  does not identify which earlier context on the user's phone held a connection.
- User independently rebooted the phone and reported recording restored, with
  ready-to-confirm transcript. No agent deletion/reset/uninstall performed.
  DevCoveer read-only metadata corroborates two new ASR operations completed:
  11:49:22→11:49:25UTC and11:49:57→11:50:00UTC. No audio/transcript content read.
- Immediate user impact recovered; no permanent prevention claim yet. Await
  user card/search/refinement confirmation. Do not replace the kernel or create
  a second recording database merely to bypass this diagnosed upgrade lock.


## Permanent prevention integration (2026-09-06)

Worker `03a640eaf96b8cd4c963f790c53998f5935e91d7` is integrated as
`ceb07e97d`: versionless open, v2-compatible typed negative compressed-key range,
legacy v3 compressed store preserved, same owner/PCM/controller/CAS. 20 unit and
19 sequential native browser cases passed in the worker; native CDP frozen-v2
read/write completed in 11 ms without a versionchange or resume of the old tab.
A prior concurrent run exhausted host disk; sequential rerun passed, and that
infrastructure failure is not represented as a product pass. Physical restart
containment reported earlier remains distinct from prevention acceptance. No user
app/session/audio/queue was cleared. Publication/readback follows in voice preview
delivery evidence; incident stays open pending physical-device prevention review.
