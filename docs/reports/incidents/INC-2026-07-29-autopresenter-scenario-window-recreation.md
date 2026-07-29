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

## Root Cause

1. `tomorrow-rail-like` и `weekend-amber-artifact` запрашивали
   `prepareScenarioStage(..., { freshContext: true })`.
2. `prepareScenarioStage()` закрывал единственный BrowserContext до создания
   нового.
3. BrowserContext владел единственным видимым page/window, поэтому обычная
   смена сцены стала process-like lifecycle transition.
4. `PACING.scenarioMaxMs = 30_000` использовался как безусловный лимит любой
   будущей сцены.

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

- deployed SHA: pending
- deploy path: pending
- local regression checks: agent `25/25`, relay `13/13`, PWA auth `2/2`,
  Windows bootstrap `4/4`, presenter-stage `3/3`; full immutable Astro preview
  build produced 465 pages without errors
- visual QA: 1920×1080 stage remained exactly viewport-sized; `outro-qr`
  rendered «Как вам?» with a 504×504 visible QR, intrinsic image width 1155,
  exact immutable CDN URL and zero browser console/page errors
- CDN evidence: public asset returned `200 image/png`, immutable one-year cache
  headers and exact SHA-256
- post-deploy verification: pending

## Prevention

Каждая новая сцена обязана доказывать не только собственное UI-состояние, но и
сохранение presentation session. Context/browser close является terminal
операцией и проверяется отдельно от scene completion.
