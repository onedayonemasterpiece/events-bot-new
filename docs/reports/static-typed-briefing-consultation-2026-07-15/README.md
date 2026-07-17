# Typed briefing consultation evidence

> **Evidence status:** committed research provenance, not a production contract.
> **Consultant:** Antigravity/agy display model `Gemini 3.1 Pro (High)` through the local `a-gemini` wrapper.
> **Runs:** 2026-07-15 08:18:32–08:20:34 UTC and 08:21:26–08:22:49 UTC; both exited with status `0`.
> **Thread/session:** agy brain `e01102a9-57e2-4b1d-ba48-35f271156ffd`; Part II continued Part I and is not an independent review.
> **Tooling:** `/home/dev/.local/bin/a-gemini` over agy `1.1.2`; display alias `Gemini 3.1 Pro (High)`. The exact provider model ID and sampling parameters were not exposed by the wrapper.
> **Invocation:** `--print-timeout 20m`; Part II additionally used `--continue`.
> **Input repository SHA:** `926dad8a91fc7f1070126d32a05281aa92ff1666` (`origin/main` at consultation start).

## Committed evidence

| File | Role | SHA-256 |
|---|---|---|
| [`prompt-v1.md`](prompt-v1.md) | Original product/design/motion/scenario consultation prompt with supplied prior analyses appended. | `0b9be70dca677b97f39a0b462f860f33d42f0462ce01ef11a350687f85d7920c` |
| [`gemini-part1.md`](gemini-part1.md) | Full primary consultant document saved from the agy brain output. | `6ad4b2f632dca8a8a500d873f4eb7b7c71e6a10657f4f49d26f34cb47f45de9e` |
| [`prompt-v2.md`](prompt-v2.md) | Corrective follow-up requesting missing depth and correcting SSG/provider assumptions. | `5c16cb06a87f331fbc8a4d29f4abd0cc8e2758ce0173220ea7dd3a37654318af` |
| [`gemini-part2.md`](gemini-part2.md) | Full follow-up consultant document. | `8a8124b65d2af816cadc3c70ed30084d0839433b321d39a3039b03cd8e045be8` |

The hashes above are for the committed byte-for-byte copies and can be checked with:

```bash
sha256sum docs/reports/static-typed-briefing-consultation-2026-07-15/{prompt-v1.md,gemini-part1.md,prompt-v2.md,gemini-part2.md}
```

The local run directory remains ignored as operational evidence:

```text
artifacts/codex/static-typed-intro-consultation-20260715/
```

The committed copies are the durable review surface; the ignored directory is not required to audit the product decision.

## Conditional-motion and crop-regression corrective gate

The `2026-07-17` correction did not inherit the previous media-deck approval.
Gemini **3.1 Pro (High)** received the exact four desktop screenshots, two
WebM recordings, extracted entry/terminal frames, the accepted older contact
sheet and the implementation/tests. The first complete review returned
`FAIL`: it correctly accepted the conditional motion state machine but found
that the three-source collage left ambient columns. It also made two factual
mistakes—treating `mosaicColumns` as shell width and treating restored crop
values as new broad edits. Those statements were not silently accepted.

The collage was changed to allocate every active column among contiguous
cover panels (`7/7/6` at 20 columns), pause cancellation was tightened, and the
second prompt explicitly required pixel/code verification of the disputed
claims. The recheck returned strict overall `PASS`, all nine contracts
`PASS`, and `Можно публиковать`. This sequence is retained as
`FAIL → concrete fix/fact correction → PASS`; the final response is approval
for isolated user review only, not production-home rollout or desirability.

| File | Role | SHA-256 |
|---|---|---|
| [`motion-crop-corrective-gate-prompt.md`](motion-crop-corrective-gate-prompt.md) | Strict first gate with screenshots, WebM, baseline and nine contracts. | `29529c3c7edbd4af11306e121daa364da8d7ee4ea88cb3fddc5d882db5ab4949` |
| [`motion-crop-corrective-gate-gemini.md`](motion-crop-corrective-gate-gemini.md) | Initial overall `FAIL`; motion passed and collage/crop concerns were challenged. | `fddb3ac2cbc6a6b3fa2309d65f461fad534266450afe7ce0c910d80ec9a41be0` |
| [`motion-crop-corrective-recheck-prompt.md`](motion-crop-corrective-recheck-prompt.md) | Corrective recheck with refreshed collage and factual constraints. | `7dad7ebeb1aded55a18d933607198936299ddf45a9224bcca915d3362c4c9931` |
| [`motion-crop-corrective-recheck-gemini.md`](motion-crop-corrective-recheck-gemini.md) | Final strict `PASS`, nine of nine contracts accepted. | `7ff12c8acb6ec8b7c5af332832657bbd1218cfdc075b9aa6c81108b8fb5ddf4c` |

## Focused media-correction gate

After user review rejected the first `wide-media` composition as a nested
frame and marked public meta/pause/pace as lab chrome, the exact rejected
desktop screenshot and annotated mobile screenshot were sent to the same
Antigravity/agy display model `Gemini 3.1 Pro (High)`.

- corrective review: `2026-07-15 18:24:55–18:25:44 UTC`, status `0`, verdict
  `FAIL`;
- post-change acceptance: `2026-07-15 18:45:10–18:45:46 UTC`, status `0`,
  verdict `PASS WITH CONDITIONS` for desktop and `PASS` for mobile;
- the acceptance explicitly confirmed that `frame inside frame` was removed;
  its remaining stripe/terminal-action spacing conditions were applied before
  publication.

| File | Role | SHA-256 |
|---|---|---|
| [`media-correction-prompt.md`](media-correction-prompt.md) | Exact screenshot-based rejection prompt. | `155e0b5c818a3f33e6791b7317634934eeab5b761ed376265e5a34f5b26321d3` |
| [`media-correction-gemini.md`](media-correction-gemini.md) | `FAIL` critique and replacement geometry/motion checks. | `e6ed161e1667755848bb56494439164ee537ebf08832ddfbeb63e8745ffdb248` |
| [`media-acceptance-prompt.md`](media-acceptance-prompt.md) | Before/after acceptance prompt with measured geometry. | `4c70115b57caa837eaa205413f085e3e3fcd865e4d0c56caf5aa483b236e5143` |
| [`media-acceptance-gemini.md`](media-acceptance-gemini.md) | Post-change acceptance verdict. | `0827822a809d0743120c988360bf0cc29f31aca26d451299e3cc28f857778282` |

The local screenshots and run logs remain under
`artifacts/codex/static-typed-briefing-media-correction/` and are intentionally
not committed.

## Exact small-media/weather visual-harmony gate

The earlier wide-media/mobile acceptance did **not** review the exact
`anticipated_person_named` and `weather_water_demo` desktop states later shown
by the user. Those states received a new screenshot-based gate rather than
inheriting the earlier pass:

- exact-state critique: `2026-07-15 19:39:40–19:40:36 UTC`, status `0`,
  `FAIL / FAIL / overall FAIL`;
- post-fix six-viewport critique: `2026-07-15 19:53:45–19:54:59 UTC`, status
  `0`, `PASS WITH CONDITIONS`; the only remaining blocker was the 1366×768
  short/wide poster crop;
- focused 4:5 crop recheck: `2026-07-15 19:58:49–19:59:10 UTC`, status `0`,
  blocker `CLOSED`, named scene `PASS` at 1366/1440/1920 and final
  `PUBLISH PASS`.

All three were run through `/home/dev/.local/bin/a-gemini`, resolved by agy to
the display model `Gemini 3.1 Pro (High)`, with empty stderr. The correction
restores the complete approved header lockup, makes small media a flat 4:5
editorial grid element without radius/shadow, strengthens the named-scene
typography, keeps weather copy within the line budget and demotes terminal
`Показать следующее` to a ghost action below the semantic CTA hierarchy.

| File | Role | SHA-256 |
|---|---|---|
| [`visual-harmony-exact-states-prompt.md`](visual-harmony-exact-states-prompt.md) | Exact user-state rejection prompt. | `3d46f7c54776e2b94d493c689b0d8ef389a6a7841e13a8f2deff929f0a7e0014` |
| [`visual-harmony-exact-states-gemini.md`](visual-harmony-exact-states-gemini.md) | Initial `FAIL` critique. | `4597f7f5d6bf023f1a763cb4991689bcc31f3664960a1746c019e296473b50ac` |
| [`visual-harmony-postfix-prompt.md`](visual-harmony-postfix-prompt.md) | Six-capture post-fix prompt. | `d19dce82a8e51e8b3708cb997fb4c2e2235e553887d45b147f88410c9807f318` |
| [`visual-harmony-postfix-gemini.md`](visual-harmony-postfix-gemini.md) | `PASS WITH CONDITIONS` response. | `8ec64e4e3238090c7f2b09baa0cbb4fec17ce80b54f9a595c98538b486f32ade` |
| [`visual-harmony-cropfix-prompt.md`](visual-harmony-cropfix-prompt.md) | Focused 1366 crop gate. | `50d8cae90a38b52354f258e11de33a2b1ba6f75f28179396c13ac5143d101a4f` |
| [`visual-harmony-cropfix-gemini.md`](visual-harmony-cropfix-gemini.md) | Final `PUBLISH PASS`. | `f15709160aa388dbfc0e937ba58f49568ac6c86cd84d72e0404684ac9670e92e` |

Operational screenshots, geometry JSON and run receipts remain ignored under
`artifacts/codex/static-typed-briefing-artist-unusual/`.

## Exact text-state continuity and cursor gate

The exact `weekend_count`, `weather_water_demo`, `frequently_forwarded` and
`festival_demo` states later challenged by the user received their own gate;
the earlier named-person/poster pass was explicitly not reused:

- initial exact-state review: `2026-07-15 20:49:50–20:50:58 UTC`, status `0`,
  all four scenes `FAIL`, overall `FAIL`; the 1180px background seam was an
  explicit publish blocker;
- corrective continuation: `2026-07-15 20:51:42–20:52:08 UTC`, status `0`;
  it corrected two inaccurate observations in the first response (the existing
  muted ghost Next was not solid, and `24 идеи` was already linked) while
  retaining the seam, grounding and bottom-anchor requirements;
- post-fix review of five desktop states, two mobile states and the real cursor
  motion recording: `2026-07-15 21:45:40–21:46:41 UTC`, status `0`, empty
  stderr, all five scenes `PASS`, overall `PASS`, final verdict
  `LAB PUBLISH PASS`.

The post-fix gate confirms the full-viewport wash, desktop bottom anchor,
specific forwarded/festival targets, regional weather copy, sequential
storm-to-lecture chain and the horizontal pending-transition cursor with
terminal retirement. It is acceptance of the isolated lab build only, not a
production-home approval or product desirability result.

| File | Role | SHA-256 |
|---|---|---|
| [`visual-harmony-text-states-prompt.md`](visual-harmony-text-states-prompt.md) | Exact four-state failure gate. | `53e571df79242bc666d75d2319f0ed5ba768076b5cbc8ec350a1e07ff73b9d79` |
| [`visual-harmony-text-states-gemini.md`](visual-harmony-text-states-gemini.md) | Initial four-scene `FAIL` response. | `54b6c13f6f0056ebd796b4f3f7a17822e69e2a940ae148abdc27770ab629cf10` |
| [`visual-harmony-text-states-correction-prompt.md`](visual-harmony-text-states-correction-prompt.md) | DOM-fact correction and exact chain/CTA/vertical-rule request. | `fea445965f60d14ef0e1b2cd5c67b1c4b5dcea7fc8fe1fe08a174688052785cf` |
| [`visual-harmony-text-states-correction-gemini.md`](visual-harmony-text-states-correction-gemini.md) | Corrective continuation. | `d08f2711177b198747702e1aa938432d54f83d085da5a47589261d2bf0a2a919` |
| [`visual-harmony-text-states-postfix-prompt.md`](visual-harmony-text-states-postfix-prompt.md) | Five-state desktop, mobile and cursor post-fix gate. | `572889668af27393f5063628787f8f941b72c76151b4273554b7e86f70c15ade` |
| [`visual-harmony-text-states-postfix-gemini.md`](visual-harmony-text-states-postfix-gemini.md) | Final `LAB PUBLISH PASS`. | `458fe2978e0a72fa36b4c5cd31b0485264474dbc0b072e0ce5ba874375cdb6dc` |

### Storm grounding correction after merge review

An independent merge reviewer then found that the visually accepted storm
chain was not safely grounded: events `3592` and `5077` had past display dates,
and their broad end dates did not establish future lecture occurrences. The
first immutable prefix containing that chain was therefore superseded before
Telegram/user handoff. The lab now uses conditional copy with no live forecast
claim and a two-screen chain to genuinely future event `5803` on 24 July.

The focused recheck ran `2026-07-15 22:06:44–22:07:57 UTC`, status `0`, empty
stderr. It correctly returned `LAB PUBLISH FAIL` because the supplied WebM was
stale and still showed the pre-fix quoted title with a dangling `».` line,
while the PNGs already showed the corrected no-quotes version. A new WebM was
captured from the exact current build; the corrective continuation ran
`2026-07-15 22:09:00–22:09:19 UTC`, status `0`, empty stderr, closed that sole
visual blocker and returned `LAB PUBLISH PASS`. The stale WebM is retained only
as local provenance and is not a review/send artifact.

| File | Role | SHA-256 |
|---|---|---|
| [`visual-harmony-storm-grounding-recheck-prompt.md`](visual-harmony-storm-grounding-recheck-prompt.md) | Focused grounding and exact-state recheck. | `4432f9122724ab9b9204eda507107e5513662f595fac15a08af227fdab302d45` |
| [`visual-harmony-storm-grounding-recheck-gemini.md`](visual-harmony-storm-grounding-recheck-gemini.md) | Correct `FAIL` on the stale-video mismatch. | `20bc103490f33f04261904872fcdc848759b7f647b18b24fa3d51e670af608c0` |
| [`visual-harmony-storm-grounding-correction-prompt.md`](visual-harmony-storm-grounding-correction-prompt.md) | Corrective continuation with exact new WebM/state receipt. | `b8d577706d2bf2d413fd00bbb9a55b0368fabf1d42dea9cc54418697429d9631` |
| [`visual-harmony-storm-grounding-correction-gemini.md`](visual-harmony-storm-grounding-correction-gemini.md) | Final focused `LAB PUBLISH PASS`. | `5bb0b74765e29bb1905e3655d1a93356f04d666545d2d8853a713cff247ae55d` |

### Desktop mosaic exact-state gate

Новая пользовательская идея `8×3` mosaic прошла отдельный, а не перенесённый
с wide-media, visual/motion gate. Gemini **3.1 Pro (High)** получил exact
`1366×768`/`1440×900` final screenshots, три промежуточные фазы, полный
`12.5s` slow WebM, `320×568`/`390×844` text-only mobile и измерения geometry,
request count, alpha/rank matrices и 404 degradation. Запуск
`2026-07-15 22:54:57–22:55:50 UTC`, provider status `0`, stderr empty.

Вердикт: `MOSAIC LAB GATE: PASS`, blockers отсутствуют,
`PUBLISH FOR USER REVIEW: YES`. Это разрешение опубликовать изолированный lab
для пользовательского просмотра, не production rollout и не доказательство
product desirability. Предложения про более тёплый stripe и дополнительные
desktop spacing/gradient оставлены polish, потому что не исправляют дефект и
могли бы снова изменить уже принятую композицию до пользовательской проверки.

| File | Role | SHA-256 |
|---|---|---|
| [`mosaic-acceptance-prompt.md`](mosaic-acceptance-prompt.md) | Exact screenshots/video, measurements and strict failure request. | `8b06ddba0471f23f48923f30f1f91b64b0f1f433651f69f940d79ff62ab98f2f` |
| [`mosaic-acceptance-gemini.md`](mosaic-acceptance-gemini.md) | Final `MOSAIC LAB GATE: PASS`. | `5d5ad43ae83da05044bbdab2f72f23b5c01fcc28859d2e642bedbe6f9d90b87a` |

### Mosaic majority/anchor/irregularity follow-up gate

После пользовательского отклонения единственного `8×3` примера follow-up
получил отдельный exact-state gate. Gemini **3.1 Pro (High)** проверил три
fully-revealed desktop-сценария, partial entry, `20.5s` WebM трёх
последовательных mosaic-сцен, mobile `320/390`, точную `12×4` alpha-матрицу,
фиксированный text anchor и viewport crop. Финальный запуск после long-name
типографического regression fix: `2026-07-16 05:38:42–05:39:26 UTC`, status
`0`, stderr empty.

Первоначальный вердикт был `MOSAIC FOLLOW-UP GATE: PASS`, но последующий
пользовательский screenshot review доказал, что этот gate **невалиден и
superseded**. Prompt заранее сообщил reviewer ошибочные success-метрики и
требовал отличия каждого соседа; ответ повторил эти числа, но не заметил
идеальный parity-checkerboard, тусклый правый край и принудительное растяжение
каждого source raster до `3:1`. Этот ответ сохраняется только как provenance
failed-review-process, а не acceptance evidence.

| File | Role | SHA-256 |
|---|---|---|
| [`mosaic-followup-acceptance-prompt.md`](mosaic-followup-acceptance-prompt.md) | Exact rejected requirements, screenshots/video and measured contract. | `7d49f3ffd066c4ce08e0305062329b95cc776450fcca02b0102b0210821ccd3a` |
| [`mosaic-followup-acceptance-gemini.md`](mosaic-followup-acceptance-gemini.md) | Final `MOSAIC FOLLOW-UP GATE: PASS`. | `280d0e22c01d217f8e1a0a026b81889b7373809a3778acb7f1c3759d69159735` |

### Dramatic mosaic corrective gate

Коррекция получила blind-first exact visual gate: Gemini **3.1 Pro (High)**
сначала сравнил rejected/candidate pixels, затем отдельно проверил right edge,
checkerboard/parity, локальные reversals, portrait/horizontal crop, шесть
entry/exit фаз, `20.5s` WebM и text-only mobile. Prompt намеренно не сообщил
модели ни alpha-матрицу, ни результаты Playwright, ни утверждение о готовности.

Запуск `2026-07-17 07:49:21–07:50:27 UTC`; consultant response complete,
stderr empty. После полного сохранения response локальный wrapper не смог
записать metadata из-за host `ENOSPC`; очищен только npm `_npx` cache, metadata
реконструирована, сам consultant output не повторялся и не изменялся.

Вердикт: `MOSAIC DRAMATIC CORRECTION: PASS`; все шесть требований `PASS`;
`PUBLISH FOR USER REVIEW: YES` для isolated lab. Reviewer визуально подтвердил
непрозрачный правый anchor, отсутствие шахматности, драматичные острова,
покрытие правых 3/4, source-faithful face/object geometry и независимый
entry/exit rhythm. Это не production approval и не desirability evidence.

| File | Role | SHA-256 |
|---|---|---|
| [`mosaic-dramatic-correction-acceptance-prompt.md`](mosaic-dramatic-correction-acceptance-prompt.md) | Blind-first rejected/candidate screenshot and motion gate. | `cea95059edd10870a257d36c681626d811e9aa517d256b3cb001bbe5c7f8b35e` |
| [`mosaic-dramatic-correction-acceptance-gemini.md`](mosaic-dramatic-correction-acceptance-gemini.md) | Final `MOSAIC DRAMATIC CORRECTION: PASS`. | `3f9042ff0402189c4594f17527b106dbcefb1605387ec770dd1563a3c2e7ca2f` |

The verdict above is now **invalid and superseded**. A later user pixel review
identified three publish blockers that the prompt and reviewer did not reject:
opaque overlapping paper slabs/double horizontal bands around linked copy,
low-resolution `360×450` and `478×317` sources enlarged across the hero, and
hero copy competing with raster OCR/poster typography. It is retained only as
failed-review-process provenance. The next acceptance prompt treats stripe
occlusion, visible raster text and actual cover-upscale as independent
hard-fail criteria and does not inherit any earlier PASS.

### Stripe / source-quality / OCR corrective gate

После нового пользовательского отклонения прежний dramatic-mosaic PASS не
переиспользовался. Финальный blind-first gate получил rejected baseline,
exact candidate PNG на `1366/1440/1920`, text-only abstention states,
`390×844`, `18s` WebM и browser geometry. Hard-fail критерии независимо
запрещали opaque/overlapping/double stripe, плохую читаемость, raster OCR,
видимый upscale/stretch, smooth/checkerboard alpha, inner-frame seam,
mobile raster и потерю horizontal pending cursor.

Запуск через `/home/dev/.local/bin/a-gemini`, display model Gemini **3.1 Pro
(High)**: `2026-07-17 09:48:55–09:50:39 UTC`, provider status `0`, stderr
empty. Вердикт: `R01 STRIPE: PASS`, `R02 QUALITY/CROP: PASS`, `R03 TILE DRAMA:
PASS`, `R04 OCR/ABSTENTION: PASS`, `MOBILE/MOTION: PASS`, `OVERALL: PASS`,
`PUBLISH FOR USER REVIEW: YES`, `BLOCKERS: none`. Acceptance относится только
к публикации isolated lab для пользовательского ревью.

| File | Role | SHA-256 |
|---|---|---|
| [`stripe-media-correction-acceptance-prompt.md`](stripe-media-correction-acceptance-prompt.md) | Blind-first stripe/OCR/upscale/motion hard-fail gate. | `6f96b5e2794e2b707fa718157c06b7995a30a4814aeaec8a6159f608b8bb8ecd` |
| [`stripe-media-correction-acceptance-gemini.md`](stripe-media-correction-acceptance-gemini.md) | Final all-PASS answer. | `16c4dc384961f3c4bd411c82e71e09aba572c3324fa633ac5fd202576f670967` |

## Decision trace

### Accepted from Gemini

- Treat the surface as a navigational/editorial briefing, not an AI assistant.
- Keep the first useful text static-first and useful without JS.
- Prefer a flat editorial treatment over a terminal/video metaphor.
- User interaction interrupts motion immediately.
- Keep LLM out of the page-view path.

### Corrected or deferred

- The project is Astro **SSG**, not SSR, and publishes through Yandex Object Storage/CDN rather than Cloudflare/Vercel.
- The two runs are one correlated iterative consultation, not independent product validation.
- The consultant did not prove user desirability or metric impact.
- Its 33-scenario, personalization and Gemini Lite system is retained only as post-validation research, not MVP scope.
- The initial lab uses at most eight deterministic messages plus a neutral fallback, no Gemini Lite and no personalization.
- Mobile layout is governed by first-event visibility. `12–18svh` and `160px` are challenger hypotheses, not hard acceptance limits.

## External audit application

A later external audit correctly distinguished design completeness from product evidence. Its resulting decision is:

```text
desk_research_synthesis: complete
decision: GO_TO_PROTOTYPE_ONLY
product_desirability: unvalidated
user_validation: false
metric_validation: false
production_approval: no
```

The isolated `/lab/briefing/` prototype is the only approved implementation scope. Production routes, deploy, Gemini Lite, personalization, runtime APIs and wordmark animation remain excluded.

## Later external audit provenance

The corrective audit supplied by the user was read from an IDE attachment and is not copied into this report because it was not a consultant invocation made by this branch. Its source SHA-256 was `d0755feab8065e2ce5872c8c5d62e264fa307f157ba8587581489e0c405873d5`; requirements R01–R10 are resolved in the canonical feature document.

### Twelve-image manual review deck gate

Пользовательская претензия «три автоматических сценария, одна картинка» оформлена
как отдельный reviewability gate, а не как расширение обычной narrative queue.
Gemini **3.1 Pro (High)** сначала получил exact 12-image contact sheet, отдельные
problem slides, mobile `390×844`, six-scene WebM и measured source/crop facts.
Запуск `2026-07-17 10:25:53–10:27:43 UTC` вернул честный `OVERALL: FAIL`:
сценарии 04/05/07 теряли контраст на тёмных клетках, а mobile rail обрезал
состояния 10–12. Этот ответ сохранён как обязательный rejected gate, а не
acceptance.

После исправления клетки, геометрически пересекающие фактические line boxes
текста, получили alpha cap `.24` без opaque paper slab; остальные клетки
сохранили нерегулярный контраст. Mobile rail стал переноситься в две строки без
горизонтального clipping. Corrective запуск `2026-07-17 10:34:25–10:35:17 UTC`,
provider status `0`, stderr empty, закрыл оба блокера и вернул все пять критериев
`PASS`, `OVERALL: PASS`, `PUBLISH FOR USER REVIEW: YES`, `BLOCKERS: none`.
Это разрешение только на пользовательскую проверку isolated lab, не production
approval и не доказательство product desirability.

| File | Role | SHA-256 |
|---|---|---|
| [`media-review-deck-acceptance-prompt.md`](media-review-deck-acceptance-prompt.md) | First exact 12-image/manual-reviewability gate. | `8dd464fdbcbf9e8bcea7d16011fff24b35e3d1f65436bff8b9acf05bce7756a5` |
| [`media-review-deck-acceptance-gemini.md`](media-review-deck-acceptance-gemini.md) | Required first `OVERALL: FAIL`. | `3af4bc48ab954babb1c4a0c15c6ed97a4e081096c62d25ac492f867bdcfd37ef` |
| [`media-review-deck-postfix-prompt.md`](media-review-deck-postfix-prompt.md) | Exact blocker-closure prompt. | `943175ae3b12958406100dea85248ee7bc7e8d5591ec84aa2894bc38f9fad058` |
| [`media-review-deck-postfix-gemini.md`](media-review-deck-postfix-gemini.md) | Final all-PASS answer. | `b89c3afa1d8ede921a17787fd6756e68eabaf3fa6eb575e08d78abb2db9908e3` |

### Crop-interval model probe

The follow-up crop experiment is recorded in
[`crop-interval-gemma4-probe-2026-07-17.md`](crop-interval-gemma4-probe-2026-07-17.md).
It is supplementary production-model evidence, not a Gemini Pro consultant
verdict. A direct focal-point prompt failed the safety boundary; the accepted
minimal contract asks Gemma 4 only for a semantic vertical interval. Gemma 4
31B retained both tested scenes, while 26B A4B omitted a principal stage element
and cannot be the sole author. Deterministic geometry retains final authority.

### Narrative lifecycle / portrait corrective gate

Gemini 3.1 Pro High was run as an adversarial visual gate over the persistent
manual state, single portrait, three-source portrait collage, mobile controls
and a six-state sequential WebM. The first two rounds correctly returned
`OVERALL: FAIL`: first for media/copy desynchronization and a head-boundary
crop, then for a perceptible transition blank. The implementation was not
published on either verdict.

The final round re-opened regenerated evidence after exact-source preload/decode
plus atomic state commit and a contained group-portrait cluster. The prompt
also clarified the already user-approved requirement that per-square alpha
must remain irregular: contiguous means one source owns a contiguous
macro-panel, not that every cell has equal opacity. Final verdict:
`R01–R07 PASS`, `OVERALL: PASS`, `PUBLISH ISOLATED LAB FOR USER REVIEW: YES`,
`BLOCKERS: none`. This is an isolated-lab publication gate only, not production
approval or product-desirability evidence.

| File | Role | SHA-256 |
|---|---|---|
| [`crop-cycle-portrait-acceptance-prompt-2026-07-17.md`](crop-cycle-portrait-acceptance-prompt-2026-07-17.md) | First strict lifecycle/crop/portrait gate. | `06d82fd90dd0c7e4bf152804cd32b5c92ce54dd30e0c17fef209be69c93bb4e8` |
| [`crop-cycle-portrait-acceptance-gemini-2026-07-17.md`](crop-cycle-portrait-acceptance-gemini-2026-07-17.md) | First `OVERALL: FAIL`. | `919a5d9765c0f2b54077e3f824d83ff410c338e332aea042bb5c35f730667220` |
| [`crop-cycle-portrait-corrective-acceptance-prompt-2026-07-17.md`](crop-cycle-portrait-corrective-acceptance-prompt-2026-07-17.md) | Second blocker-closure gate. | `d2c0efe60874ac6bf49318f6fded960f007958989a0f521d6aa5539ac584b6d9` |
| [`crop-cycle-portrait-corrective-acceptance-gemini-2026-07-17.md`](crop-cycle-portrait-corrective-acceptance-gemini-2026-07-17.md) | Second `OVERALL: FAIL`. | `a68f69b974b4569a1473b31e4eb2a4fc80614ae70594adb3d48f81997714f510` |
| [`crop-cycle-final-clarified-acceptance-prompt-2026-07-17.md`](crop-cycle-final-clarified-acceptance-prompt-2026-07-17.md) | Final gate with explicit user-approved alpha semantics. | `9cac54e6d1cf5971298a4dec9be5b1b03ea2441e574ed3676b17d941d6d9678b` |
| [`crop-cycle-final-clarified-acceptance-gemini-2026-07-17.md`](crop-cycle-final-clarified-acceptance-gemini-2026-07-17.md) | Final all-PASS answer. | `2fdb3f55490d9fbb67f3445889cae38fbe9bc039dfe8612f4824e30150efaf00` |
