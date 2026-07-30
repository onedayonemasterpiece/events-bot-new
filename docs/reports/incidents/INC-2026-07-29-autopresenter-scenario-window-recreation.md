# INC-2026-07-29 Autopresenter scenario window recreation

Status: open
Severity: sev1
Service: Autopresenter owner-only Internet first test
Opened: 2026-07-29
Closed: —
Owners: Autopresenter implementation / release
Related incidents: `INC-2026-07-29-autopresenter-windows-bootstrap-startup`
Related docs:
`docs/features/static-site-pages/auto-present/README.md`,
`docs/features/static-site-pages/auto-present/scenario-30072026-technical.md`,
`docs/operations/release-governance.md`

## Summary

При запуске следующего сценария некоторые сцены закрывали единственный
BrowserContext и создавали новый. В headed Windows это визуально закрывало
presentation window и открывало другое, разрушая контракт единой непрерывной
среды.

## User / Business Impact

- ведущий видел закрытие окна между сценами;
- длинная часовая презентация не могла восприниматься как единый показ;
- безусловный 30-секундный whole-scenario timeout блокировал будущие длинные
  текстовые, медиа- и лекционные сцены.

## Detection

Дефект обнаружен владельцем в первом Windows-тесте при переходе к следующему
сценарию. Telegram message `820` дополнительно показал точную ошибку
`tomorrow-rail-like exceeded 30000ms`. Read-only разбор локализовал normal path
`freshContext -> context.close -> newContext/newPage`.

## Timeline

- 2026-07-29 11:55 UTC — владелец сообщил о закрытии окна при следующем Run.
- 2026-07-29 12:05 UTC — root cause локализован в `freshContext` для второго и
  третьего сценария и общем 30-секундном timeout.
- 2026-07-29 12:10 UTC — начат минимальный persistent-session fix и QR-аутро.
- 2026-07-29 12:15 UTC — после завершения загрузки Telegram screenshot
  подтверждён exact timeout `tomorrow-rail-like exceeded 30000ms`.
- 2026-07-29 12:40 UTC — первый public exact-source прогон подтвердил, что
  сетевой `tomorrow-mobile` тоже может обоснованно перейти границу 30 секунд;
  live-site policy унифицирована на 120 секунд до повторного deploy.
- 2026-07-30 09:59 UTC — Fly deploy перезапустил in-memory relay; уже
  работающий Windows-агент сохранил старый sequence cursor и перестал получать
  новые команды после краткого HTTP 502.
- 2026-07-30 10:30 UTC — owner-прогон также выявил рассинхронизацию
  control/relay allowlist: 34 кнопки отклонялись до доставки агенту.

## Root Cause

1. `tomorrow-rail-like` и `weekend-amber-artifact` запрашивали
   `prepareScenarioStage(..., { freshContext: true })`.
2. `prepareScenarioStage()` закрывал единственный BrowserContext до создания
   нового.
3. BrowserContext владел единственным видимым page/window, поэтому обычная
   смена сцены стала process-like lifecycle transition.
4. `PACING.scenarioMaxMs = 30_000` использовался как безусловный лимит любой
   будущей сцены.
5. Relay начинал sequence с `1` после каждого process restart, но Windows-агент
   продолжал poll со старым большим `after_seq`; новые команды становились
   невидимыми до искусственного превышения старого cursor.
6. Relay allowlist поддерживался отдельно от control/agent и отстал на 34
   сценария: 02.3, 02.10, 02.11, 03.18 и все 30 ручных страниц.

## Contributing Factors

- clean-state изоляция тестовых сценариев была реализована самым широким
  способом — пересозданием контекста;
- regression suite проверял финальные UI-состояния, но не идентичность
  context/page/window между последовательными Run;
- первый vertical slice ошибочно трактовал сценарий как короткий одноразовый
  browser journey, а не как сцену внутри долгой presentation session.

## Automation Contract

### Treat as regression guard when

- меняется agent lifecycle, Run/Stop/Reset/Shutdown;
- добавляется новая сцена или per-scenario preparation;
- меняются timeout, recovery или fullscreen/window handling.

### Affected surfaces

- `tools/autopresenter/agent/agent.mjs`;
- `tools/autopresenter/agent/scenario-contract.mjs`;
- control Run selection and relay command lifecycle;
- Windows first-test launcher;
- hosted presenter stage.

### Mandatory checks before closure or deploy

- последовательные Run разных сцен используют один browser/context/page;
- второй Run может переключить активную сцену без `already running`;
- Stop и Reset не закрывают context/browser;
- только Shutdown закрывает context/browser и завершает agent с exit code 0;
- hour-capable explicit scene timeout policy не ограничен 30 секундами;
- все существующие сценарии и QR-аутро проходят exact-source E2E;
- работающий Windows-агент после relay restart получает и подтверждает первую
  новую Run, scroll и D-pad команду без повторных нажатий;
- каждая `data-action="run"` кнопка control входит в relay allowlist;
- `tools/autopresenter/m0/**` неизменён;
- Windows bootstrap regression suite из связанного incident проходит.

### Required evidence

- source SHA и Fly image;
- lifecycle/unit suite;
- публичный последовательный scenario-switch E2E с постоянным
  context-generation/page identity;
- public CDN QR status/hash;
- refreshed Windows ZIP hash;
- owner Windows retry;
- reachability from `origin/main` before closure.

## Immediate Mitigation

- убрать normal-path `freshContext`;
- очищать только известное тестовое browser-local state внутри существующего
  page/context;
- оставить terminal close исключительно Shutdown.

## Corrective Actions

- добавить persistent-session lifecycle contract;
- заменить общий 30-секундный лимит простой per-scenario policy;
- добавить fullscreen QR-аутро как явную сцену того же stage.

## Follow-up Actions

- [ ] Owner: повторить переключение сцен на Windows и подтвердить отсутствие
  закрытия окна.
- [ ] Release owner: вернуть fix в `origin/main` до incident closure.
- [ ] Product: добавлять сцены из сценария 30 июля небольшими итерациями на том
  же runtime, без premature DSL/editor.

## Release And Closure Evidence

- owner-test deployed SHA: `dddc98fc0824c59333cfc45c275034d1e6f08c3a`
- deploy path: manual Fly deploy from the clean, already-pushed integration
  worktree to the existing `kenigevents-autopresenter` app; no Fly/Yandex
  resource was created
- Fly image:
  `kenigevents-autopresenter:deployment-01KYPYAHH0EF1JMXNVN1KRKQW9`,
  manifest
  `sha256:1cfa72243279e141b58dc234db6b6c656f7ac9ce5acf04ffa311cad88d8c5c46`
- machine `2879209fd9e998`: version 10, started, 1/1 HTTP checks passing,
  unchanged one shared CPU / 512 MB
- local regression checks: agent `25/25`, relay `13/13`, PWA auth `2/2`,
  Windows bootstrap `4/4`, presenter-stage `3/3`; full immutable Astro preview
  build produced 465 pages without errors
- visual QA: 1920×1080 stage remained exactly viewport-sized; `outro-qr`
  rendered «Как вам?» with a 504×504 visible QR, intrinsic image width 1155,
  exact immutable CDN URL and zero browser console/page errors
- CDN evidence: public asset returned `200 image/png`, immutable one-year cache
  headers and exact SHA-256
- first public deploy probe: active Run → `outro-qr` switch passed in the same
  agent generation; `tomorrow-mobile` then reproduced the remaining
  per-scene 30-second bound and was corrected before the final candidate
- refreshed 13-entry Windows ZIP SHA-256:
  `ecb0b467ce0b72b068ab2d27b6612b1302544c06af63a55a998ac63ef871e252`;
  archive includes `outro-contract.mjs` and the 120-second live-site policy
- final public exact-source E2E: an active `tomorrow-rail-like` was
  cooperatively switched to `outro-qr`; then `tomorrow-mobile`,
  `tomorrow-rail-like`, `weekend-amber-artifact`, and `outro-qr` each produced
  their successful 1920×1080 capture
- the final agent log contains exactly one
  `browser context ready {"generation":1}` line, stderr is 0 bytes, and
  confirmed Shutdown produced durable `closed` and agent exit code 0
- Telegram handoff: reply `827` to thread message `803`, verified through the
  approved E2E human session
- remaining closure blockers: owner retry with the fresh Windows ZIP and
  reachability from `origin/main`

## Prevention

Каждая новая сцена обязана доказывать не только собственное UI-состояние, но и
сохранение presentation session. Context/browser close является terminal
операцией и проверяется отдельно от scene completion.

### Seven-scene owner-test candidate — 2026-07-29

- final source SHA: `0e9ba9d87f10342e2f8f614065dc3724ef1497b6`, pushed to
  `origin/feature/autopresenter-design` before deploy;
- existing Fly app only: image
  `kenigevents-autopresenter:deployment-01KYQ2K51H6C3YA4WB62FR8JCE`, manifest
  `sha256:5e1295218dfd6e371ba86ad262b7287a800b580275db52e18a3839e4ac9b83c6`;
  machine `2879209fd9e998` version 12 remained one shared vCPU / 512 MB with
  1/1 HTTP check passing;
- no new Fly/Yandex resource was created; the intro logo/music and seven
  lecture frames were uploaded content-addressed beneath the existing
  `assets/autopresenter/scenario-20260730/` CDN prefix;
- regression suites: agent `28/28`, relay `13/13`, Windows bootstrap `4/4`,
  presenter stage `6/6`; exact Astro build completed 465 pages;
- the 1920×1080 visual review covered intro, lecture frames 01/04/07 and
  Weekend desktop top/bottom; desktop iframe bounding box was exactly
  `0,0,1920,1080` with zero console/page errors;
- exact downloaded 14-entry Windows ZIP SHA-256:
  `bdca1a1ce249100f1100200d8f48f9f7b3a21c12860c011313e9700815396989`;
- full exact-package sequence against the deployed public stage completed
  intro → lecture → intro → tomorrow-mobile → tomorrow-rail-like →
  weekend-amber-artifact → weekend-desktop → outro-qr → Shutdown in one
  context generation; stderr was empty and every scenario produced a
  1920×1080 capture;
- after removing the passive desktop debug overlay, the final exact ZIP reran
  `weekend-desktop` against the deployed stage: natural scroll reached the
  footer, the final frame had no adjacent/overlay copy, and Shutdown exited 0;
- the owner’s already-running older Windows agent was not interrupted to seize
  the public single-agent relay. Telegram reply `831` to message `803` explains
  that the fresh ZIP must be downloaded after closing the old presentation;
- `tools/autopresenter/m0/**` remained unchanged. Incident stays open for owner
  retry on Windows and reachability from `origin/main`.

### Relay restart and blocked-scene recurrence — 2026-07-30

- deployed source SHA:
  `95269bb5a21e29369c421824df9e831c20174b0c`, pushed to
  `origin/integration/autopresenter-expanded-20260729-e` before deploy;
- existing Fly app only: release `v24`, image
  `kenigevents-autopresenter:deployment-01KYSASV8V0ZX5SS2K2DKVYHWR`;
  machine `2879209fd9e998` version 24 remains one shared vCPU / 512 MB with
  `1/1` HTTP check passing;
- production `/healthz`, authorized state, control markers and stage markers
  passed after deploy. Relay state exposes `boot_id` and a timestamp-based
  sequence, eliminating sequence rollback after process restart;
- downloaded production Windows ZIP SHA-256:
  `cc5af1d83a7fa9465a5491de87a008eb42d1b42c9628c8a3fe90a48619a156da`;
- exact local regressions passed for 02.3, 02.10, 02.11, split 03.2, D.1M,
  D.1D, 03.8.4, 03.12.1 and 03.18; the persistent-agent relay-restart
  regression delivered Run, scroll and D-pad after restart;
- automated suites: agent `39/39`, relay `19/19`, PWA control auth `2/2`,
  presenter-stage `10/10`, Windows bootstrap `4/4`; Astro built 465 pages;
- no new Fly app, machine, volume, bucket, CDN, database or queue was created;
  `tools/autopresenter/m0/**` remained unchanged;
- closure remains blocked on owner retry with the fresh Windows ZIP and
  reachability of the fix from `origin/main`. The authenticated 03.10.2
  search must also be confirmed with the owner-prepared Yandex session.
