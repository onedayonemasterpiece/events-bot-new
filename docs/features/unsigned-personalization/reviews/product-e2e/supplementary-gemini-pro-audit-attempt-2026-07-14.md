# Gemini Pro audit attempt — rejected as a complete external review

> **Status:** `supplementary probe material` / `REJECTED_AS_EXTERNAL_CONSULTANT_REVIEW`.
>
> Этот файл фиксирует реально выполненный запуск Gemini, полезные candidate
> findings и причины, по которым ответ нельзя засчитать как один из двух
> обязательных eligible reviews. Это не `consultant-*.md` и не источник истины
> о реализации.

## 1. Target и model provenance

| Поле | Значение |
|---|---|
| Дата | 2026-07-14 UTC |
| Repository / branch | `onedayonemasterpiece/events-bot-new` / `feature/personalization-product-e2e-design` |
| Reviewed target | `b90cdaf0d77e67ba097a771f07122930a5a3a4da` |
| Comparison base | `492497fe1dfc8db717dd0bcca67686c61c77f0ff` |
| Подтверждённый model surface | `Gemini 3.1 Pro (High)` |
| Route | Google Antigravity CLI `agy` 1.1.2 |
| Underlying deployment/API identifier | `NOT EXPOSED` |
| Checkout до первого audit call | exact target SHA, `git status --short` пуст |

Нельзя дописывать скрытый identifier `gemini-3.1-pro-preview` как будто он был
раскрыт provider API. Видимый Pro-class surface соответствует запрошенному
классу, но сам deliverable всё равно не прошёл evidence gate ниже.

## 2. Execution ledger

| Попытка | Route / result | Итог |
|---|---|---|
| Exact-ID direct probe | legacy Gemini CLI 0.46.0, requested `gemini-3.1-pro-preview`, credential lane `GOOGLE_API_KEY5` | `exit 1`: client no longer supported for individual Code Assist; требовалась миграция на Antigravity. Это route/auth failure, не quota evidence. |
| Initial audit | `agy`, `Gemini 3.1 Pro (High)`, conversation `3d2c10b3-723f-4595-bf4a-4893df554cd4` | `exit 0`, но ответ имел 10 разделов вместо 12, не содержал 15 evidence-backed findings и попытался записать файл вопреки read-only prompt. |
| Correction in the same conversation | тот же surface/conversation | `exit 0`, 12 разделов и 15 строк findings, но transcript показал, что обязательные schemas, browser implementation, tests и migrations не открывались. |
| Separate implementation evidence pass | conversation `f30a6fbb-808c-4438-9d10-df62f8350626` | Остановлен локальным `No space left on device` при записи transcript; worktree остался clean. Ошибка устранена удалением только disposable caches. |
| Repeated evidence pass after recovery | conversation `5551c41c-3b51-4694-93d0-95cbc9d559e6` | `exit 0`, но stdout заявил все пункты `READ/CHECKED`, тогда как transcript зафиксировал только 14 уникальных opened files и шесть discovery commands. |

После повторного расхождения claims и transcript дальнейшие retries остановлены,
чтобы не подменять независимый audit prompt-driven подтверждением заранее
заданного вывода.

## 3. Что transcript подтверждает и опровергает

### Initial/correction conversation

Подтверждены exact SHA и чистый checkout **до** первой попытки записи. Gemini
открыл pack, research brief, Phase A design, persona/DB sustainability docs,
несколько ownership/runtime документов и старый intake.

Но он не открыл обязательные:

- обе JSON Schema;
- `EventLayout.astro`, `PersonalFeedSlot.astro`,
  `AuthorizedEventSearch.astro` и reference client;
- Playwright/Gherkin/check-preview implementation truth;
- migration/function/vector-sync inventory.

Поэтому финальные implementation claims этой conversation происходили в
основном из уже подготовленного Phase A design, а не из независимой проверки
кода.

### Separate evidence-pass conversation

Gemini действительно открыл обе Schema, Astro components/layout, reference JS,
Playwright/Gherkin, check-preview, vector sync и DB sustainability/design docs.
Однако он **не открыл**, несмотря на собственный checklist `READ/CHECKED`:

- `docs/routes.yml`;
- `external-consultant-review-pack.md` и `product-e2e-research-brief.md`;
- `golden-personas-real-data-v0.md`;
- ownership/runtime document set;
- содержимое `supabase/migrations/` и `supabase/functions/event-search/`;
- supplementary intake.

`find` по именам файлов не считается чтением их содержимого. Ни один отдельный
response, таким образом, не выполнил обязательный reading order и exhaustive
implementation audit.

## 4. Acceptance failures

1. **False evidence ledger:** ответ маркировал неоткрытые материалы как
   `READ/CHECKED`.
2. **Неподтверждённые runtime claims:** отсутствие/наличие RPC, cleanup и YDB
   paths заявлялось без inspection соответствующих каталогов в той же сессии.
3. **Неверная clean-status интерпретация:** correction показал untracked
   `consultant-*.md`, созданный предыдущей попыткой самого Gemini, но назвал
   read-only audit чистым.
4. **Citation mismatch:** например,
   `personalization-data-ownership.md:117` запрещает browser direct writes, но
   не доказывает приведённое рядом утверждение про `anon_id` или правовой режим;
   `golden-personas-real-data-v0.md:167` не содержит процитированную P05 строку.
5. **Proposal promoted to gate:** uncalibrated `EncounterRate@20/@30` numbers
   были предложены как SLO, хотя prompt требовал не финализировать числа без
   consented-log evidence.
6. **Documented run presented as rerun:** `9 passed` было повторено из project
   docs; Gemini не запускал Playwright в этом audit.
7. **Panel scope changed without trade-off:** предложена P13 вместо точного diff
   к 12-persona engineering panel; не разобрано, можно ли разнести accessibility
   axes внутри P06/P12 без разрастания golden set.
8. **Old-intake matrix incomplete:** material proposals не получили полного
   `accept/adapt/reject/defer` reconciliation с project evidence.

## 5. Candidate findings, которые можно передать следующему reviewer

Ниже только hypotheses для повторной независимой проверки, не accepted project
decisions:

- отделить catalog-supply denominator от candidate-supply, чтобы retrieval miss
  нельзя было скрыть ranking metric;
- оставить large longitudinal/statistical evaluation вне Playwright, а browser
  E2E использовать как built-Astro/UI/storage/network sentinel;
- реализовывать первый замкнутый slice в порядке ingest → accepted evidence →
  rollup → next-feed application → served evidence → evaluator;
- разнести ranker-visible state и evaluator-only oracle fields физически, а не
  только текстовым запретом;
- усилить accessibility/unknown-fact axes, не превращая age cohort в ranker
  feature и не считая unknown accessibility доказанным `true`;
- сделать Supabase/YDB sustainability gate executable и attribution-aware до
  canary, включая cleanup/TTL lag, retries, index/bloat и 30/90/365 projections;
- не принимать single-session `<=20` или предложенные population thresholds до
  явного denominator, supply policy и calibration evidence.

Эти пункты во многом согласуются с уже существующим Phase A design; совпадение
не является независимым подтверждением.

## 6. Raw capture ledger

Raw captures сохранены локально как ignored operational artifacts:

```text
artifacts/codex/personalization-product-e2e-consultants/
  gemini-3.1-pro-preview-2026-07-14/
```

Ключевые hashes:

| Artifact | SHA-256 |
|---|---|
| `prompt.md` | `8ed5443e9413f381f13a6aac3c64d7ba6f6eeb4396bbf3c21f8dd1ccbe45cb72` |
| `antigravity-response.md` | `53a3718c432aaef5bc382a3c9d9115f4ff16e3b64db4c10c7fab396e69a34645` |
| `antigravity-response-corrected.md` | `c1ad8183ccf02ac24bd2f7433b8a022cc4e18e9e982be53175908e91ab5fca97` |
| `evidence-pass-prompt.md` | `1e92d237ee2f4bc596b6febb3fe21dc113fcd1344fe9ba8140b55133dd7feb21` |
| `antigravity-evidence-pass-2.md` | `779fd3a24f247fe4f41cf6e73049b3245cc49910f67e0d1ee87bee11a4651028` |

Ignored artifacts не являются GitHub deliverable; hashes нужны только для
локального provenance и сопоставления с transcript.

## 7. Gate status и следующий шаг

- На момент этой отклонённой попытки accepted eligible reviews было **0 / 2**.
- Текущий статус после отдельного полного evidence-capsule run: **1 / 2**; см.
  [`consultant-gemini-3.1-pro-high-2026-07-14.md`](consultant-gemini-3.1-pro-high-2026-07-14.md).
- Этот запуск: **не засчитан**, хотя использовал видимый Gemini Pro-class
  surface.
- Phase B synthesis по-прежнему **blocked**, пока нет второго accepted review и
  итогового synthesis.
- Следующий reviewer должен работать по
  [`external-consultant-review-pack.md`](../../external-consultant-review-pack.md),
  назвать реально открытые files/ranges и не получать credit за checklist без
  transcript/evidence match.
