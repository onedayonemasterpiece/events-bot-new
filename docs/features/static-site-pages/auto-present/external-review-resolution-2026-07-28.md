# Resolution внешних review автопрезентатора — 2026-07-28

- **Входной вердикт:** `GO_TO_M0_COMPATIBILITY_SPIKE_ONLY / NO_GO_FOR_PUBLIC_DEMO`.
- **Результат:** оба review приняты; канонический [README.md](README.md) усилен, M0 разрешён к реализации, target execution ещё не выполнен.

## Disposition второго review

Второй критический review коммита `981aebd9` подтвердил
`DESIGN_APPROVED_FOR_M0` и сохранил `NO_GO_FOR_M1_M2_M3` /
`NO_GO_FOR_PUBLIC_DEMO`. Его четыре обязательных M0-уточнения применены:

1. 20 запусков теперь означают 20 полных cold Node+browser cycles на кандидата:
   10 fresh-profile и 10 persistent-profile;
2. compatibility использует deterministic loopback HTTP fixture и считается
   отдельно от последующих 5/5 live `/zavtra/` smoke;
3. exact candidate manifests фиксируют Node/package lock/browser
   revision/executable/path/hash и запрещают channel, download и machine cache;
4. PASS требует строго 20/20 + 5/5, zero install/admin/system-browser/orphans и
   полный evidence package с target-laptop system/run records.

Подробные normative правила не дублируются здесь: они находятся в разделах
`M0 candidate matrix`, `Exact candidate manifest`, `M0 test contract` и
`Артефакты и evidence` канонического README. Ограничение review на недоступный
GitHub web-cache не принято как доказательство файлов; локальный diff
проверяется отдельно.

## Disposition первого review

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

Только реализация M0 compatibility spike и запуск на целевом Windows 10
ноутбуке с сохранением evidence. Phone control, relay, iframe/decorative stage,
recording и final portable release не начинаются до M0 PASS. Linux/CI
validation не заменяет target execution.
