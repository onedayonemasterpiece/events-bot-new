# Городской обзор на главной: минимальный prototype gate

> **Status:** shareable isolated lab built and locally accepted on the feature branch; public versioned preview evidence is recorded below.
> **Implementation:** dedicated one-route build for `/lab/briefing/`; no production homepage integration.
> **Production effect:** none; the lab is not linked from production navigation and is published only under an immutable preview prefix.
> **Decision:** `GO_TO_PROTOTYPE_ONLY`.
> **Product desirability:** unvalidated.
> **Validated by users:** false.
> **Validated by metrics:** false.
> **Owner:** unassigned.
> **Review after:** production canary + baseline listing funnel + lab usability/telemetry evidence.

## Решение после внешнего аудита

Первое исследование подробно ответило на вопрос «как могла бы работать полноценная briefing-система», но не доказало, что она нужна пользователям. Два прохода одного Gemini Pro — полезное итеративное design review, а не две независимые консультации и не product evidence.

Поэтому прежний `Conditional Go` отменён:

```text
research: accept with corrected provenance/status
minimal isolated prototype: approved
production implementation: not approved
Gemini Lite: deferred
personalization overlay: deferred
extended 33-scenario platform: deferred
```

Качественный отзыв о слишком быстрых видеоанонсах подтверждает общий риск фиксированного темпа и потери «что, где, когда», но относится к другому media format. Он не доказывает desirability текстового briefing и не определяет его оптимальную высоту или скорость.

## Место в roadmap

Typed briefing не разблокирует текущий production static-site rollout и не является P0/P1 до появления базовой воронки. Правильная последовательность:

```text
production export
→ production canary
→ baseline listing funnel
→ выявление конкретной потери discovery
→ minimal static briefing experiment
→ только после выигрыша motion/personalization/platform work
```

Lab-прототип допустим сейчас только как дешёвый research artifact: он не меняет production routes, публикуется только в изолированном versioned preview prefix с `noindex` и не претендует на приоритет над data quality, export/canary или telemetry write path.

## Проверяемая гипотеза

**Question:** помогает ли один короткий статичный редакционный факт быстрее открыть подходящее событие, не отодвигая ленту?

**Не проверяется в V0:**

- персонализация;
- «с прошлого визита»;
- Gemini Lite или другой LLM;
- runtime API/manifest fetch;
- настройка темпа;
- длинная auto-sequence;
- анимация wordmark/wide-`о`;
- production homepage integration.

## Изолированный lab

Route:

```text
/lab/briefing/
```

Variants:

| Variant | Contract | Purpose |
|---|---|---|
| `A · control` | briefing отсутствует; сразу категории и feed | baseline first-event discovery |
| `B · static` | весь короткий editorial briefing виден сразу | проверить ценность содержания |
| `C · reveal` | тот же текст один раз проявляется строкой/блоком за `≤1200ms` | проверить incremental motion только после B |

Все варианты находятся на одной QA-странице, переключаются без production routing и используют одинаковые категории/feed fixtures. Нельзя менять copy и event order между B/C: иначе невозможно отделить motion от content value.

## Минимальный deterministic inventory

V0 содержит максимум восемь scenario IDs и один universal fallback, по одному editorially approved copy на scenario.

| ID | Eligibility fact | Copy | CTA token |
|---|---|---|---|
| `today_count` | verified active-today count > 0 | `Сегодня в городе — {{count}} событий.` | `route:today` |
| `tomorrow_count` | verified tomorrow count > 0 | `На завтра запланировано {{count}} событий.` | `route:tomorrow` |
| `weekend_count` | verified weekend count > 0 | `На выходные собрано {{count}} событий.` | `route:weekend` |
| `exhibitions_count` | verified active exhibition count > 0 | `Сейчас идут {{count}} выставок.` | `route:exhibitions` |
| `free_count` | canonical `is_free=true` count > 0 | `Вход свободный на {{count}} событий.` | `route:search_free` |
| `tonight_count` | grounded later-today events in Kaliningrad time | `На вечер осталось {{count}} событий.` | `route:today` |
| `newly_added_count` | canonical created/public window count > 0 | `В свежих анонсах — {{count}} событий.` | `route:search_new` |
| `catalog_generic` | valid build, no stronger eligible scenario | `Выберите событие на сегодня, завтра или выходные.` | `route:today` |
| `neutral_fallback` | missing/stale/zero-invalid facts | `Афиша города уже открыта — начните с удобной даты.` | `route:today` |

`free_count`, `tonight_count` and `newly_added_count` remain fixture-only until the exporter exposes explicit validated counts and route tokens. A zero/stale count never appears in copy.

### Minimal manifest shape

```json
{
  "schema_version": "briefing-lab-v0",
  "id": "today_count",
  "priority": 100,
  "eligible": true,
  "facts": {"count": 42},
  "headline_template": "Сегодня в городе — {{count}} событий.",
  "supporting_text": "Выберите выставку, концерт или встречу.",
  "cta_label": "Смотреть события",
  "cta_token": "route:today",
  "fact_source": "static_event_export",
  "generated_at": "2026-07-15T08:00:00Z",
  "expires_at": "2026-07-16T00:00:00Z"
}
```

Contract:

- deterministic and build-time validated;
- facts and copy stored separately;
- `generated_at` and `expires_at` required;
- allowlisted route token, not raw model-generated URL;
- no network request in lab render;
- stale, missing or invalid fact selects `neutral_fallback`;
- no Gemini-generated output is admitted automatically.

## Layout gate

Height is judged by discovery outcome, not by the former permissive `≤50svh` ceiling.

```text
P0 outcome: event title and a meaningful part of its card are visible at initial render
mobile challenger hypothesis: 12–18svh
mobile challenger ceiling: 160px
short-mobile rule: content and first-card visibility win over the numeric hypothesis
desktop challenger hypothesis: 18–24svh
CLS after first render: 0
```

`12–18svh` and `160px` are test inputs, not acceptance mandates: at `320×568` they may be too short for useful copy. The measured lab with the real header, categories and first card determines whether either budget is viable; the first-event outcome remains the hard gate.

The shareable lab no longer imitates the product shell: it imports the actual `EventLayout`, `.page-shell`, `.source-links` and `EventListItem`, with stable production fixtures `6607`, `5373`, `6020`. The briefing is `150px` on mobile and `190px` on desktop; production cards retain their real minimum heights (`154px` mobile, `168px` desktop) and are never shrunk to manufacture a pass. At `320×568` the first production card continues below the fold; this is reported as an honest product finding, not hidden by custom compact cards.

Required control viewports: `320×568`, `375×667`, `390×844`, `1440×900`. All eight scenarios plus neutral fallback are checked in B/C; the longest briefing must satisfy `scrollHeight <= clientHeight` and `scrollWidth <= clientWidth`. The approved header/lockup is never shrunk to make the lab pass.

## Reproducible shareable-lab workflow

The lab uses a separate Astro `srcDir` and output root, so it does not run or publish the full catalog build:

```bash
cd site
PREVIEW_BUILD_ID=briefing-lab-$(git rev-parse --short=12 HEAD) npm run build:lab
PREVIEW_BUILD_ID=briefing-lab-$(git rev-parse --short=12 HEAD) npm run check:lab
PREVIEW_BUILD_ID=briefing-lab-$(git rev-parse --short=12 HEAD) npm run preview:lab
```

Local URL: `http://127.0.0.1:4177/<build-id>/lab/briefing/?variant=a`; replace `a` with `b` or `c`, and optionally add one of the documented `scenario` IDs. The build fails closed unless the artifact contains only the lab HTML, hashed Astro CSS, manifest, favicon and exact wordmark asset.

Public publication is a distinct command and accepts only `preview-YYYYMMDDtHHMM-briefing-lab-<sha8>`. It performs recursive copy only into that new prefix, never `sync`, delete, cache purge, root write, stable ICS mutation or production navigation change. Local telemetry remains capped at 24 records in `window.__briefingTelemetry` and can be downloaded from the on-page debug panel; no beacon, XHR, POST, Supabase or analytics transport is created.

### Published lab evidence — 2026-07-15

Immutable preview built from `0e94a440`:

- [A · control](https://kenigevents.ru/preview-20260715t1241-briefing-lab-0e94a440/lab/briefing/?variant=a)
- [B · static](https://kenigevents.ru/preview-20260715t1241-briefing-lab-0e94a440/lab/briefing/?variant=b)
- [C · reveal](https://kenigevents.ru/preview-20260715t1241-briefing-lab-0e94a440/lab/briefing/?variant=c)

Acceptance: one-route build and five-file allowlist passed; local Playwright passed `3/3`; all eight scenarios plus fallback passed B/C overflow and production-geometry checks at the four required viewports; the public A/B/C × viewport screenshot matrix returned `12/12` HTTP 200 with exact noindex and zero POST/XHR/fetch/beacon/Supabase/analytics/telemetry requests. Evidence is stored locally under `artifacts/codex/static-typed-briefing-shareable-20260715/` and intentionally not committed.

The visual result also preserves the negative finding: at `320×568` the title/decision region is visible, but the unshrunk production card extends below the viewport. This prototype is therefore shareable for moderated research, not accepted for homepage rollout.

### ENOSPC boundary

The earlier full static build failed because the host filesystem was at `99–100%` capacity with only hundreds (and during tests tens) of MiB free; inode usage was about `16%`, so this was block-space exhaustion rather than inode exhaustion or a lab defect. The isolated one-route lab build succeeds under that constraint. This does **not** prove the ordinary full-catalog production build: that gate remains unproven on this host until space is reclaimed and the full build is rerun.

## Motion gate for Variant C

- static content is present and readable in HTML;
- enhancement is one line/block reveal, not character typing;
- total reveal `≤1200ms`;
- run at most once per session/day in the lab;
- `prefers-reduced-motion` disables it completely;
- `pointerdown`, keyboard focus or scroll completes it immediately;
- BFCache restore does not replay it;
- no autoplay sequence, cursor, terminal metaphor or wide-`о` motion;
- B must establish content value before C is considered for any later experiment.

The previously documented pace profiles are intentionally outside V0. If C later wins, discrete `manual/calm/normal/fast` may be researched; a slider remains unsupported without evidence. See the [post-validation backlog](../../backlog/features/static-typed-briefing/README.md).

## Experiment sequence

Do not launch A/B/C/D simultaneously without traffic/MDE evidence.

### Experiment 1

```text
A: no briefing
B: static editorial briefing
```

Only if B wins without harming first-event visibility/performance:

### Experiment 2

```text
B: static briefing
C: one short reveal
```

Only after a general briefing wins:

### Experiment 3

```text
general deterministic briefing
vs
coarse/personalized briefing
```

Before any experiment, define baseline eligible sessions, event-open rate, unit of randomization, MDE/sample size, duration, returning-user assignment, deep-link/search exclusions and multiple-comparison policy.

## Metric and telemetry contract

**Primary production outcome (future experiment):**

```text
event_detail_open_rate =
  unique eligible_listing_sessions with >=1 destination-confirmed event_detail_open
  / unique eligible_listing_sessions
```

An `eligible_listing_session` starts on the eligible home/general listing surface, has a stable experiment assignment, becomes visible and has at least one active event target. The denominator is not conditioned on briefing impression: otherwise control sessions and failed treatment delivery disappear from the comparison.

The isolated lab cannot prove `event_detail_open`: a source-page click shows activation, not successful destination rendering. V0 therefore uses only an in-memory debug sink and records `event_detail_activate`. A future public experiment may call it `event_detail_open` only after the destination event page confirms a visible load through a consented, server-accepted, bot-filtered and deduplicated path.

### Logical events

```text
eligible_session
briefing_impression
briefing_complete {completion_kind: static|natural|interrupt|reduced_motion}
briefing_interrupt {reason: pointerdown|focusin|scroll|visibility_hidden}
first_event_visible
event_detail_activate       # lab/source-side proxy only
event_detail_open           # future destination-confirmed outcome
ticket_click
calendar_add
share
```

Delivery semantics:

- `eligible_session`: once per experiment/session for A, B and C; also the intention-to-treat denominator;
- `briefing_impression`: B/C only after at least 50% of the block is visible for 250 continuous visible milliseconds;
- `briefing_complete`: B after qualified static impression; C after natural end or forced completion;
- `briefing_interrupt`: first causal interrupt only; reduced motion is a delivery state, not an interrupt;
- `first_event_visible`: stable first-card title/decision region at least 90% visible for 250 continuous visible milliseconds;
- `event_detail_activate`: source link activation; never silently promoted to the primary outcome;
- `ticket_click`: requested ticket/register/source/phone action, not purchase proof;
- `calendar_add`: ICS request, not proof of calendar import;
- `share`: only after native share or clipboard copy succeeds.

The lab sink is bounded and local:

```text
window.__briefingTelemetry
```

It makes UI/test semantics observable but sends no network request and creates no production telemetry write path. QA query assignments are labeled `assignment_source=qa_query` and excluded from causal aggregates.

Bounded payload: schema version, ephemeral session/page-view IDs, experiment ID, variant, assignment source, scenario ID, copy/build version, viewport class, reduced-motion flag, active-visible time rounded to 100ms, event ID/rank and event-specific reason/outcome. Never store or emit rendered free text, auth token, raw profile, email, exact geolocation, search history or full URL/referrer.

### Session and dedupe

- experimental unit is a session; V0 may use an ephemeral `sessionStorage` UUID;
- assignment remains stable through reload/BFCache inside the session;
- `eligible_session`, impression, complete and first-event-visible emit at most once per session/experiment;
- BFCache restore does not replay reveal or duplicate events;
- hidden time is excluded from visibility timers;
- telemetry failure never delays navigation or other actions.

### Guardrails

- `initial_first_event_visible_rate` including non-viewers in the denominator;
- `time_to_first_event_visible` p50/p75 plus non-view rate;
- mobile binary gate at `320×568` and `375×667`;
- listing scroll depth;
- ticket/calendar/share session rates;
- `briefing_interrupt_rate`;
- LCP, INP, CLS and JS/fallback errors;
- return-7d only when identity/consent and sample size make it valid.

A public experiment still needs baseline eligible sessions/open rate, unit of randomization, MDE/sample size, duration, consent basis, destination instrumentation, bot/monitor exclusion, server dedupe and persistent aggregates. Until then the primary metric is a contract, not a measured result.

## Eight P0 prototype blockers

1. Without JavaScript, B/C show the full useful briefing.
2. On mobile, the first event title and meaningful card area are visible initially.
3. `prefers-reduced-motion` disables reveal completely.
4. Scroll, `pointerdown` and focus complete motion immediately.
5. BFCache restore does not replay the reveal.
6. Missing/stale/invalid facts select the neutral fallback.
7. Relative to control, the briefing adds no network request, has bounded inline JS, causes `CLS=0` and introduces no unexplained lab performance regression.
8. Eligible session, impression, completion/interruption, first-event visibility and source-side event activation are distinguishable in the local lab sink; no activation is mislabeled as destination `event_detail_open`.

P1/P2 platform checks, 33 scenario families, pace controls, personalization, Gemini writer boundaries and the extended risk register are preserved only in the [post-validation backlog](../../backlog/features/static-typed-briefing/README.md).

## Consultation provenance

Committed evidence:

- [evidence README and decision trace](../../reports/static-typed-briefing-consultation-2026-07-15/README.md);
- [prompt v1](../../reports/static-typed-briefing-consultation-2026-07-15/prompt-v1.md);
- [Gemini Part I](../../reports/static-typed-briefing-consultation-2026-07-15/gemini-part1.md);
- [corrective prompt v2](../../reports/static-typed-briefing-consultation-2026-07-15/prompt-v2.md);
- [Gemini Part II](../../reports/static-typed-briefing-consultation-2026-07-15/gemini-part2.md).

The model was Antigravity/agy `Gemini 3.1 Pro (High)`, run twice as one correlated consultation thread from input commit `926dad8a91fc7f1070126d32a05281aa92ff1666`. Checksums and accepted/corrected/deferred decisions are in the evidence README.

## External-audit traceability

| Requirement | Resolution |
|---|---|
| R01 status/evidence | `GO_TO_PROTOTYPE_ONLY`; user/metric validation false |
| R02 no Gemini/personalization MVP | excluded from V0; retained only in appendix |
| R03 ≤8 scenarios + fallback | eight deterministic IDs + one neutral fallback |
| R04 isolated A/B/C lab | `/lab/briefing/` |
| R05 mobile visibility | first-event outcome is the hard gate; `12–18svh`/`160px` are challenger hypotheses only |
| R06 no production/deploy | explicit lab-only scope |
| R07 telemetry contract | bounded local/debug events above |
| R08 one primary metric | `event_detail_open / eligible_listing_session` |
| R09 eight P0 checks | eight blockers above; remainder appendix |
| R10 consultation provenance | committed prompts, outputs, hashes, SHA and decision trace |

## Next decision

The branch may be reviewed as a **minimal lab prototype**, not as an approved homepage feature. Production work remains blocked until:

1. production static-site baseline funnel exists;
2. lab demonstrates first-event visibility and accessibility;
3. Experiment 1 has a feasible sample-size plan;
4. B shows downstream event discovery value without performance or feed-discovery harm.

If B does not win, remove the feature rather than expanding scenarios, motion or personalization.
