# Resolution внешнего review автопрезентатора — 2026-07-28

- **Входной вердикт:** `GO_TO_M0_COMPATIBILITY_SPIKE_ONLY / NO_GO_FOR_PUBLIC_DEMO`.
- **Результат:** принят; канонический [README.md](README.md) усилен и остаётся в статусе `design`.

## Disposition

| Review item | Статус | Resolution |
|---|---|---|
| Windows 10 не поддерживается актуальным Playwright | Applied | M0 стал первым blocking gate; добавлены два exact candidate и 20-run contract |
| Термин «Chromium» неточен после 1.57 | Applied | до выбора версии используется `Playwright-managed browser binary` |
| Today vs Tomorrow | Adjusted | актуальный owner input требует «Завтра»; `/zavtra/` подтверждён как отдельный Astro route, старый `today` superseded |
| Нужен deterministic scenario | Applied | фиксируются build/date/event/Saturday/timezone и unique site hooks |
| Stage оставлен между iframe/direct page | Applied | normative MVP — iframe 430×932 в stage 1920×1080 |
| Stop не гарантирован | Applied | parallel poll/runner, cooperative 500 ms, hard teardown, ack ≤ 2 s |
| Long-poll без протокола | Applied | versioned envelope, sequence, TTL, idempotency, lease, durable agent cursor |
| Relative «Завтра» нестабильно | Applied | immutable presentation dataset, `Europe/Kaliningrad`, exact markers |
| Portable ZIP неполон | Applied | exact tree, manifests, hashes, self-test и path matrix |
| Relay не является Astro | Applied | dynamic `aiohttp` boundary, one instance, no cache/scale-to-zero/deploy |
| Нужны local hotkeys | Applied | `Esc`, `R`, `F`, `Space/Right Arrow` |
| Backup должен быть готов заранее | Applied | reviewed offline MP4 + manifest/checksum входит в exact release |
| Runtime/test isolation | Applied | `tools/autopresenter`, its tests, separate site contract test |
| Раздельные токены | Applied | admin/agent separation и строгий command allowlist |
| Старый PoC опасен | Applied | machine-readable superseded header; routes mark it non-normative |
| Не ставить `mvp` | Applied | status remains `design`, gate `m0_win10_compatibility` |
| Перестроить M0–M3 | Applied | compatibility → stage → remote → exact release/rehearsal |

## Проверенные внешние факты

- Текущие [Playwright system requirements](https://playwright.dev/docs/intro#system-requirements) указывают Windows 11+ / Windows Server 2019+, без Windows 10.
- [Release notes 1.57](https://playwright.dev/docs/release-notes#version-157) фиксируют переход headed browser на Chrome for Testing.
- Package metadata `playwright-core@1.61.1` содержит browser revision `1228`, version `149.0.7827.55`; `1.54.2` — revision `1181`, version `139.0.7258.5`. Это candidate inputs, а не доказательство совместимости.
- В репозитории `site/src/pages/zavtra/index.astro` является отдельным route; «Завтра» не нужно изобретать как client-side state.

## Следующее разрешённое действие

Только реализация M0 compatibility spike и сохранение evidence. Phone control, relay и декоративный stage не начинаются до M0 PASS.
