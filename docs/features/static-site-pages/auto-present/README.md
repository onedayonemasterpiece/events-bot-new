# Автопрезентатор статического сайта

> **Статус:** локальный owner-test vertical slice принят; portable/public release
> остаётся `NO-GO` до target Windows 10 evidence, rehearsal и fallback proof.

Автопрезентатор показывает настоящий KenigEvents UI в сценической композиции и
принимает команды с отдельного защищённого mobile PWA-пульта. Это presentation
tooling, а не новая публичная page family сайта.

## Source of truth

- [`scenario-30072026-base.md`](scenario-30072026-base.md) — продуктовый
  сценарный baseline;
- [`requirements.md`](requirements.md) — исходные требования;
- `scenario-30072026-technical.md`, когда присутствует в integration branch, —
  implementation diary/evidence, а не самостоятельное продуктовое решение;
- [`playwright_autopresenter_prompt.md`](playwright_autopresenter_prompt.md) —
  historical implementation prompt; напрямую не исполнять как актуальную
  спецификацию.

## Принятые инварианты

- настоящий browser UI и реальные Playwright actions;
- телефон и Windows-agent общаются только через исходящий HTTPS relay;
- control/agent tokens различаются; token fragment не попадает в access logs;
- Run/Stop/Reset нетерминальны; Shutdown закрывает environment;
- browser/context/window не пересоздаются между сценами;
- static frame остаётся до следующей команды;
- public show имеет заранее проверенный fallback recording;
- одна и та же immutable site build используется для сцен и evidence;
- presentation tooling не меняет production content/state.

## Release gates

### M0 — target machine

- 20/20 cold loopback cycles;
- 5/5 live smoke;
- headed browser, self-test, orphan kill;
- zero hidden manual bootstrap dependency.

### Portable/public

- hermetic Windows 10 package without runtime downloads;
- exact dependency/browser hashes;
- Internet interruption and relay restart recovery;
- fallback video parity and operator rehearsal;
- phone PWA permission reset and token redaction;
- full scenario timing against actual talk;
- owner sign-off on the exact package/build.

До этих доказательств локальный owner test не называется portable release или
разрешением публичного показа.
