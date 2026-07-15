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
