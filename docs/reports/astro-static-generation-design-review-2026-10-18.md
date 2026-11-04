# Astro: отчёт исследования для самостоятельной оптимизации

Дата: **18 октября 2026**, Europe/Kaliningrad. Результат: [проект реализации](../architecture/astro-static-generation-optimization.md). Статус: **исследовано и спроектировано; runtime не изменён и ускорение новой версии не измерено**.

## Главный вывод

Сильное ускорение следует начинать в существующих Astro/TypeScript и Python-компонентах. В коде найдены повторные сортировки и lookup полного каталога, три release-прохода Astro, широкий bootstrap зависимостей, повторные exporter/media операции и последовательная публикация тысяч объектов. Эти механизмы дают конкретные небольшие границы изменения. Общая переработка бота или обязательный переход на Go увеличили бы область отладки без доказанного дополнительного выигрыша.

Первый самостоятельный результат — быстрый локальный цикл на DevCoveer с HMR и reusable данными. Следующие — адресная сборка, ускорение подготовки и existing publisher. Production compute остаётся на Kaggle. Интеграция с Ultra в одном kernel имеет смысл после независимой приёмки обоих участков.

## 1. Версия, источники и ограничения

Проверен `main@d431b9f86b87999cf27f74b99857d23aedc28f16`. Прочитаны instructions, canonical static-site документы, exporter, runtime scripts, BGE, catalog code и выбранные incidents. Уточнения владельца взяты из обновлённого PR #746; его полный Go-проект не считается планом обязательного переноса текущего сервиса. [Owner review][R01].

Источники разделены на сохранённые наблюдения, механизм кода и ожидаемый эффект. Live logs/production состояние на 18 октября не проверялись; старые incident counts не являются текущим состоянием сервиса. Новые npm/build/LLM/Kaggle/probe runs не выполнялись, benchmark новой реализации отсутствует.

`package.json` разрешает Astro `^6.0.5`, но lockfile фиксирует **6.4.8**; Node требуется `>=22.12.0`. Общее дерево lockfile содержит 1,428 package entries. Это не размер установленного `node_modules`. [Manifest][R02], [lockfile][R03].

## 2. Что подтверждено измерениями

В [astro-preview.md][R04] сохранён локальный замер от 28 июня:

| Срез | Events / pages | Files / bytes | Wall / Max RSS |
|---|---|---|---|
| v43 preview | 80 / 95 | 261 / 28 MiB | 6.57 s / ≈387 MiB |
| Полный активный snapshot | 386 / 403 | 1,185 / 128 MiB | 20.16 s / ≈522 MiB |

Это старый код/каталог, а не современная ёмкость DevCoveer; media mirroring/transforms исключены. Измерение опровергает лишь автоматическое отождествление многоминутного pipeline с CPU-стоимостью Astro.

Августовский accepted candidate содержит **3,305 objects / 674,467,502 B**. Exact run стартовал в 20:41 UTC, kernel/gates/report завершились к 21:28, final readback/CAS — в 22:51 после timeout и adoption. Интервал включает несколько стадий и восстановление; его нельзя объявлять временем рендера или обещанной экономией Go. [Incident 3 августа][R05].

Другой сохранённый срез показывает tree 642,764,482 B / 3,289 files и 232,841,551 B скачанных archives. Указанный рядом старый DB snapshot 310,312,960 B не характеризует нынешний compact projection. Сохранённые 377-event BGE/QA PASS с нулём provider calls доказывают ограниченный compute path; тогда отдельный browser defect блокировал общий candidate. [Collection report][R06].

## 3. Проверенные механизмы и рекомендуемые изменения

| Механизм актуального кода | Где подтверждён | Минимальное изменение |
|---|---|---|
| `getEventDetailEvents()` заново строит Map и sort; ID/slug lookup вызывает его снова | `events.ts`, около 80–87 и 202–207 | Один immutable catalog context, Map по ID/slug |
| Related и occurrence многократно обращаются к каталогу; mobile/desktop отдельно получают 10/30 candidates | `events.ts` 284, 546–608; detail route 44–45 | Переиспользовать family/related projection одного контекста |
| Три Astro builds: preview contract, production root, secret candidate | Kernel 1281–1317; candidate script | Dev выходит из release-цепи; route exclusions до рендера |
| Общий npm bootstrap включает mobile/mail QA tooling | package manifest; kernel 1243–1261 | Разделить build/required gates и отдельные E2E dependencies |
| Per-event schema/media SQL и синхронные dimension probes | Exporter 866–894, 1443–1717 | Batch metadata и reuse сохранённых immutable asset facts |
| PUT всех objects, затем GET всех objects последовательно | `static_site_release.py` 2811–2836 | Bounded parallel I/O и streaming hash с полным readback |

Источники таблицы: [каталог][R07], [route][R08], [kernel][R09], [candidate][R10], [exporter][R11], [publisher][R12].

Есть важные контрдоказательства. Popularity median пересчитывается только в fallback: положительный materialized score возвращается сразу. Participants/video export уже batched. PUT уже использует file handle; весь tree не загружается в RAM. Поэтому проект не приписывает себе эти готовые оптимизации и не заявляет одинаковый масштаб проблемы для каждого события.

Image probe защищает hero от маленького/некачественного первого media. Его удаление требует сохранённых dimensions/quality и корректного unknown fallback, а не простого отключения функции. Три release passes проверяют разные contracts; нельзя заменить их одним build, не сохранив root/candidate canonical, robots, prefix и browser obligations.

## 4. Переиспользование уже работающей надёжности

`static_site_projection_sqlite_v1` уже копирует разрешённые таблицы **и столбцы**, исключает operational tables, связывает rows/hashes и закрывает live read transaction до сети. Это актуальный механизм вместо старой передачи полной DB. Используются существующие queue, claim, fingerprint/no-op, Kaggle ledger, exact lease, success receipt и adoption. Новая control plane не нужна. [Release code][R12], [runbook][R13].

BGE module уже переиспользует unchanged text hashes, проверяет pinned model/document/normalization и отделяет prototype compatibility. BGE dense 1024 и Gemini Search 768 несовместимы. UI-only изменение не требует повторного encoding; lifecycle может менять eligibility без изменения текста embedding. Static builder не получает второго writer поисковой проекции. [BGE][R14], [runbook][R13].

Передача только requested IDs в exporter не является готовой single-page оптимизацией. `--include-ids` меняет приоритет preview slice, а затем добирает rows; full mode эту приоритизацию сбрасывает. Целевой render set должен ограничить HTML, сохранив полный совместимый retrieval corpus. Новое событие способно изменить old anchors, которых ещё нет в reverse-link inventory. [R11], [linked-events contract][R15].

## 5. Инцидентные уроки

**3 августа:** 27 запусков и 27 failures в аудированном окне не имели одной доказанной причины. Для последних попыток установлены fixture-specific Popular assertion и неверная трактовка fallback-image geometry; ранние 22 failures не приписаны им автоматически. Успешный output затем переживал timeout/adoption. Вывод — проверять смысл contract на текущем каталоге, восстанавливать exact output, не запускать заново все вычисления. [R05].

**18–19 июля:** накопление snapshots и generated outputs создавало disk pressure. По документам это GiB на volume/overlay, а не доказательство утечки RAM. Нынешние compact projection, scratch cleanup, hard-link staging и active-handoff retention надо сохранять. Уменьшение памяти через смену языка не устраняет оставленные на диске tarballs. [Snapshot incident][R16], [overlay incident][R17].

**Strict trailing:** поздняя поправка в incident 20 июля заменяет временный 30-minute cap на owner-requested `latest_effect_at + 15m`. Ранний runbook ещё содержит старый абзац. Новый проект сохраняет последнее правило и immediate operator/calendar/startup. Recovery уже готового exact output не должен заново ждать quiet window — августовское наблюдение показало такое starvation. [Debounce incident][R18], [R05].

## 6. Предлагаемые цели и проверка эффективности

Цели проекта: warm UI edit→visible ≤2 секунды; адресный build 1–10 страниц ≤10 секунд после готовых inputs/dependencies; zero re-encode unchanged compatible BGE documents; zero candidate RPC при valid related cache hit; zero Kaggle push для accepted no-op. Это **targets**, не выполненные проверки.

Исходный и новый код сравниваются на одном современном frozen snapshot с одинаковыми runtime/clock, количеством событий и routes. Отдельно фиксируются export, BGE, Astro passes, gate, transport и public-ready. CPU/RSS измеряются для дерева процессов, bytes — по фактической передаче. Маленькая выборка даёт значения/диапазон, а не убедительный p95. Холодный bootstrap не смешивается с HMR.

Первыми проверяются эквивалентность order/family/related, media reuse, изменение соседа без собственной revision, midnight, stale full build после fresh page и потерянный ACK. Действующая стратегия release tests сохраняется; новая всеобщая E2E-программа не вводится. [Release plan][R19], [gates][R20].

Общий бот в 2 GB RAM этим отчётом не обещается. Для DevCoveer разумен один тяжёлый build/gate за раз; локальные BGE, Chromium и несколько Astro instances не становятся обязательным фоном UI-отладки. Production compute остаётся на Kaggle.

## 7. Что ещё требует решения и реализации

Текущий publisher умеет immutable `_review/<token>`, а не автоматически готовую публичную landing. Адресный public prefix, freshness, CDN readback и защита от старого full build — следующий отдельный участок. Нужные KenigEvents aggregates и полный отказ от active Telegraph остаются конечным требованием, но не блокируют первые пять optimization PR. Экскурсии/дайджесты, Edge, promo, notifications и существующие video runtimes сохраняются.

Единственный принципиальный вопрос для landing этапа: **может ли корректная core page появиться раньше нового проверенного блока похожих событий?** Рекомендовано да; совместимые принятые связи сохраняются. Ответ не нужен для HMR/index/export/publisher acceleration.

Официальные документы подтверждают HMR и route selection. Experimental incremental build появляется в Astro **7.2**, `--timings` — **7.4**; они не выдаются за возможности locked 6.4.8. Upgrade можно исследовать позже, если собственно render останется существенным ограничителем. [CLI][W01], [routing][W02], [incremental][W03].

Go не исключается навсегда, но дальнейший перенос потребует измеренного hotspot и достаточной выгоды крупного компонента. Сейчас наиболее обоснован первый PR в существующем TypeScript-каталоге и локальном dev workflow; полный [проект](../architecture/astro-static-generation-optimization.md) задаёт независимый порядок A–G.

[R01]: https://github.com/onedayonemasterpiece/events-bot-new/blob/57638a89014a839b071d7d6268b17200495dc0f8/docs/architecture/events-bot-2.0-owner-requirements-review-20261017.md
[R02]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/site/package.json
[R03]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/site/package-lock.json
[R04]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/docs/features/static-site-pages/astro-preview.md
[R05]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/docs/reports/incidents/INC-2026-08-03-static-site-builder-failure-storm.md
[R06]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/docs/features/static-site-pages/podborki-to-be.md
[R07]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/site/src/lib/events.ts
[R08]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/site/src/pages/sobytiya/[slug].astro
[R09]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/kaggle/StaticSiteBuilder/static_site_builder.py
[R10]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/site/scripts/build-secret-candidate.mjs
[R11]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/site/scripts/export-production-preview-data.py
[R12]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/static_site_release.py
[R13]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/docs/operations/kaggle-static-site-builder.md
[R14]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/site/scripts/static_event_bge.py
[R15]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/docs/features/linked-events/README.md
[R16]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/docs/reports/incidents/INC-2026-07-18-static-snapshot-disk-pressure.md
[R17]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/docs/reports/incidents/INC-2026-07-19-static-builder-root-overlay-recurrence.md
[R18]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/docs/reports/incidents/INC-2026-07-20-static-claim-lost-health.md
[R19]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/docs/features/static-site-pages/release-plan.md
[R20]: https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/docs/features/static-site-pages/release-autotest-gates.md
[W01]: https://docs.astro.build/en/reference/cli-reference/
[W02]: https://docs.astro.build/en/reference/routing-reference/
[W03]: https://docs.astro.build/en/reference/experimental-flags/incremental-build/
