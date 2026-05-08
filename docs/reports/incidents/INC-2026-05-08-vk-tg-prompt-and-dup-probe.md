# INC-2026-05-08-vk-tg-prompt-and-dup-probe Hallucinated venue/title and duplicate cards across multiple May 8 surfaces

Status: open
Severity: sev2
Service: VK auto-import / Telegram Monitoring / Smart Update / public Telegraph cards
Opened: 2026-05-08
Closed: —
Owners: events-bot maintainers
Related incidents:
- `INC-2026-05-07-vk-auto-import-merge-regression-gemma4.md` — predecessor (Gemma 4 prompt-family rewrite, wall-clock cap, phone-as-tel ticket_link); pre-create duplicate probe was filed there as a follow-up but never landed.
- `INC-2026-04-20-club-znakomstv-duplicate-event-cards.md` — same merge-guard class.
- `INC-2026-05-01-future-event-quality-audit.md` — duplicate guards / prose-in-venue regression contract.
- `INC-2026-05-05-event-quality-regression.md` — same source/default venue fallback class.
Related docs: `docs/llm/prompts.md` (vk_intake / TG monitoring extract), `docs/features/smart-event-update/README.md`, `docs/reference/locations.md`.

## Summary

The May 8 daily surface (`/daily` Telegram + Telegraph individual cards) shipped with **eight distinct quality regressions** captured in user feedback today. They split into three classes: (a) duplicate event rows that the deterministic + LLM matcher chain failed to merge before `INSERT event`; (b) hallucinated `location_name` / `location_address` that puts ticket-sales points or unrelated prose into the venue field; (c) writer/title regressions where the title fell back to `<event_type> — <venue>` or the description was truncated mid-sentence. Item (h) is a different class: `telegraph_url` is missing for two May 15 events that nevertheless got published into the `/daily` summary with no link.

## User / Business Impact

Concrete events affected (all visible in `/daily` on 2026-05-08):

- **a / Окна Победы (8.05 17:00)** — events `4173` (vk-30777579_15080, `Научная библиотека`) and `4200` (t.me/kaliningradlibrary/2214, `Дом Семьи, Леонова 4`). Two cards for what reads like the same exhibition; the second card placed it at a different venue.
- **b / Ансамбль Балтийского флота (8.05 19:00)** — events `2819` (`Янтарь холл, Ленина 11, Светлогорск`) and `4038` (`Янтарь-холл` + `city=Калининград` + `location_address=ТРЦ «Европа» 2 этаж`). Same event, but second row hallucinated city `Калининград` and used the box-office point as `location_address`.
- **c / Камамбер (8.05 19:00 Зеленоградск)** — three rows: `4350`, `4396`, `4397`. Row `4397` carries `location_address=Потемкина 20 asignatura` — the foreign-language token `asignatura` is pure LLM fabrication.
- **d / Барн «Открытие выставки» (8.05 19:00)** — events `4584` and `4585`. Identical title, identical address, but `location_name` on both rows was a 200-char curator quote (`перетекающие жизненные этапы…`). Two-way duplicate plus prose-in-venue.
- **e / Эдит Пиаф (8.05 19:30)** — events `3824` and `3983`. Row `3983` description starts with `> Спектакль-байопик «Эдит Пиаф.` and is truncated mid-sentence; `location_address=ТРЦ «Европа» 2 этаж` again.
- **f / «Концерт — Янтарь холл» (21.06)** — event `4670`, source `vk-100137391_164499`. The post is a programme of Soviet-stage hits with named performers; the parser fell back to the bare `<event_type> — <venue>` template instead of using the programme theme.
- **g / `/daily` rows without `telegraph_url`** — events `4664` («С чего начинается Родина?») and `4705` («История старого Лиса») landed in the May 15 Telegram daily without a Telegraph URL. The reader cannot click through to find out what the event is. Different class — `joboutbox::telegraph_build` did not run / failed for these events.
- **h / Hallucinated content (16.05)** — event `4645` «Мастер-класс по созданию ёлочных игрушек», description mixes Christmas tree decoration with `выступления диджеев и исполнителей 90-х`. Source post (`t.me/dobro39/5865`) was about something else entirely; Gemma 4 fused unrelated programme fragments into one event.

Auto-import run on 2026-05-08 06:14–08:05 also showed:
- repeated Gemma 4 `500 INTERNAL` provider errors (`event_parse` for `wall-30777579_15153`, `smart_update merge:4484:fact_first_desc`);
- one `TimeoutError: Google AI provider call timed out after 0.0s` (label `telegraph_render_remove_logistics`) — suspicious zero-duration timeout reported by `GoogleAIClient.provider_timeout_seconds`;
- one row hung on `wall-211696971_5569` for the full `VK_AUTO_IMPORT_ROW_TIMEOUT_SEC=1800s`.

The `_ask_gemma_json`/`_ask_gemma_text` wall-clock cap from `INC-2026-05-07` did fire on some stages, but `event_parse` (`main._parse_event_via_gemma`) lives outside the smart_update gateway and has a separate retry path; it does not yet honour the same wall-clock cap.

## Detection

Operator caught the regressions while reviewing the May 8 `/daily` Telegram summary and three Telegraph cards. Issues #g and #h were also visible in the same `/daily` summary. Issues #a–#f are user-visible in production.

## Timeline

- 2026-04-12 .. 2026-05-07 — affected duplicate rows imported across many days (4173/4200/2819/4038/4350/4396/4397/4584/4585/3824/3983/4670/4645/4664/4705).
- 2026-05-07 — INC-2026-05-07 hotfix shipped (Gemma 4 wall-clock cap, vk_intake prompt-family rewrite, phone-as-tel ticket_link); pre-create duplicate probe was filed as follow-up #5 but did not land in that hotfix.
- 2026-05-08 morning — `/daily` 2026-05-08 publication exposes the duplicate / hallucinated rows enumerated above.
- 2026-05-08 06:14–08:05 — auto-import run captures Gemma 4 5xx storm + `TimeoutError 0.0s` + 1800s row timeout in `runtime_logs/events-bot.log`.
- 2026-05-08 — this incident filed; LLM-first prompt patch + deterministic pre-create dup probe land on `hotfix/inc-2026-05-08-prompt-and-dup-probe`.

## Root Cause

1. **No deterministic pre-create dup probe.** `INC-2026-05-07` documented the probe as follow-up #5 but never shipped it. With a flaky Gemma 4 endpoint causing the LLM matcher (`_llm_match_or_create_bundle`) to time out / 500-storm intermittently, the existing deterministic chain (anchor / longrun / single_candidate / strong_match / exact_title / related_title / rescue_bundle_title) leaves blind spots for cross-source reposts whose titles drift slightly (`Янтарь холл` vs `Янтарь-холл`, `Научная библиотека` vs `Научная библиотека, Мира 9, Калининград`). Result: events 4173/4200, 2819/4038, 3824/3983, 4584/4585 all survived as duplicate active cards.
2. **`location_address` had no anti-fabrication contract.** The `location_name` rule was tightened in `INC-2026-05-07`, but `location_address` still accepts any prose-shaped value. Production evidence: `Потемкина 20 asignatura` (foreign-language LLM fabrication), `ТРЦ «Европа» 2 этаж` (ticket-sales point used as venue address for `Янтарь холл`), `перетекающие жизненные этапы…` (curator-quote prose).
3. **Title fallback `<event_type> — <venue>` re-emerged for opaque-titled posts.** Event 4670 source is a quiz-style promo about Soviet-stage hits with named performers; the existing rule said "do NOT downgrade explicit names to template" but did not say what to do when the source has no explicit name. Gemma 4 picked the venue. The right behaviour is either a programme-theme title (`Хиты советской эстрады`) or no event at all.
4. **`event_parse` stage has no wall-clock cap.** `main._parse_event_via_gemma::_generate_with_rate_limit_wait` runs its own retry budget driven by `EVENT_PARSE_GEMMA_RATE_LIMIT_MAX_WAIT_SEC=120` and is not wrapped in `asyncio.wait_for`. Smart Update has the cap (`SMART_UPDATE_GEMMA_JSON_WALL_CLOCK_SEC=90`), but `event_parse` fall-throughs the row timeout (1800s).
5. **Provider 5xx storm + Gemma 4 thinking budget** (carried over from INC-2026-05-07) keeps making affected runs slow even after the prompt is correct; this is upstream Google instability.
6. **Out of scope of this hotfix:** truncated description on 3983 (`> Спектакль-байопик «Эдит Пиаф.`) — likely `_drop_reported_speech_duplicates` or `fact_first_desc` cutting too aggressively. Tracked as follow-up.
7. **Out of scope of this hotfix:** missing `telegraph_url` for 4664 and 4705 — joboutbox / telegraph_build worker failure class. Tracked as follow-up.

## Contributing Factors

- After `INC-2026-05-07` shipped, the immediate quality wins on freshly imported events masked the latent merge-guard hole; older rows from late April / early May persisted into the May 8 daily surface.
- `EVENT_PARSE_GEMMA_MAX_TOKENS=4000` raised in `INC-2026-05-07` reduced thinking-budget truncation in vk_intake but did not cover `_parse_event_via_gemma`'s separate path on the failed wall-30777579_15153 case.

## Automation Contract

### Treat as regression guard when

- any change to `smart_event_update.py::_pre_create_duplicate_probe` or its callers, the deterministic matcher chain (`_match_existing_event_by_*`, `_single_candidate_auto_match_ok`, `_deterministic_*_match`, `_llm_match_or_create_bundle`), or `INSERT event` flow;
- any change to vk_intake / TG monitoring extract prompts in `docs/llm/prompts.md` (especially title / location / `<event_type>` fallback rules);
- any change to `main._parse_event_via_gemma` retry / timeout policy.

### Affected surfaces

- `smart_event_update.py` (matcher chain + new `_pre_create_duplicate_probe`).
- `docs/llm/prompts.md` master prompt (`location_address` anti-fabrication; ticket-sales-point exclusion; title fallback; opaque-source rule).
- public surfaces: Telegraph individual event pages, `/daily` Telegram, `/daily` VK, month/week/day pages.
- production rows for events 4173, 4200, 2819, 4038, 4350, 4396, 4397, 4584, 4585, 3824, 3983, 4670, 4645, 4664, 4705.

### Mandatory checks before closure or deploy

- `tests/test_pre_create_duplicate_probe.py` PASS (7 cases: ticket-link parity + related title; ticket-link parity blocked by unrelated titles; location/time/related-title repost; missing time anchor; unrelated titles at same anchor; parser-source skip; empty inputs).
- regression replay through Gemma 4 on event 4670 source post (`vk-100137391_164499`): title MUST not be `Концерт — Янтарь холл`; either a grounded programme-theme title or `[]`.
- regression replay on event 4397 source: `location_address` MUST NOT contain foreign tokens like `asignatura`, ticket-sales-point text like `ТРЦ "Европа"`, or prose phrases.
- post-deploy: a fresh `/vk_auto_import` on a known cross-source repost (different ticket URL, same venue/date/time, related title) should produce `🔄 updated` not `✅ created`.

### Required evidence

- deployed SHA on `origin/main`;
- pytest output for the new probe tests (PASS);
- prod sqlite snapshot showing remediation status of the 14 listed events (archived / merged / re-imported);
- `runtime_logs/events-bot.log` excerpt with `smart_update.match type=post_decision_dup_probe` lines after the next live cross-source repost.

## Immediate Mitigation

- LLM-first prompt patch + deterministic pre-create dup probe shipped on `hotfix/inc-2026-05-08-prompt-and-dup-probe`.
- (proposed, not yet applied) compensating data remediation: archive duplicate rows (4200 if confirmed wrong-venue, 4038, 4396, 4397, 4585, 3983) keeping the canonical row per event; reset `vk_inbox` rows for 4670 and 4645 to `pending` so re-import picks the prompt-tightened path.

## Corrective Actions

LLM-first / deterministic-safety-net hotfix on `hotfix/inc-2026-05-08-prompt-and-dup-probe`:

1. **Master-prompt: `location_address` anti-fabrication contract.** Forbid foreign-language tokens (`asignatura`/`street`/`building`), ticket-sales points (`ТРЦ "Европа"`/`атриум "Лондон"`/`информационная стойка`), and prose / curator quotes / event programme text in `location_address`. Explicit rule for venues whose box-office sits in a different building (`Янтарь холл` ↔ `ТРЦ "Европа"`): the EVENT happens at the canonical venue, never at the box-office.
2. **Master-prompt: title fallback rule.** Forbid the bare `<event_type> — <venue>` template (`Концерт — Янтарь холл`) when neither name nor programme theme is recoverable; in that case return `[]` instead of inventing a placeholder title.
3. **`smart_event_update.py::_pre_create_duplicate_probe`**: deterministic last-line probe before `INSERT event`, gated to non-`parser:*` sources, with two branches:
   - identical normalised `ticket_link` + overlapping `date` + no explicit time conflict + `_titles_look_related`;
   - identical normalised `location_name` + overlapping `date` + identical non-empty time anchor + `_titles_look_related`.
4. Tests `tests/test_pre_create_duplicate_probe.py` cover both branches plus all six guard rails.

## Follow-up Actions

- [x] events-bot / 2026-05-08 / land prompt-tightening + pre-create dup probe + tests on `hotfix/inc-2026-05-08-prompt-and-dup-probe` (this commit).
- [ ] events-bot / 2026-05-08 / merge to `origin/main` and `flyctl deploy --remote-only`.
- [ ] events-bot / 2026-05-08 / compensating data remediation: archive dup rows (4200?, 4038, 4396, 4397, 4585, 3983) + reset `vk_inbox` for 4670 / 4645 to `pending`; trigger telegraph rebuild for canonical kept rows; rebuild May 8 / May 15 daily / month / week pages.
- [ ] events-bot / 2026-05-09 / wrap `main._parse_event_via_gemma` calls in `asyncio.wait_for(EVENT_PARSE_GEMMA_WALL_CLOCK_SEC)` so the `event_parse` stage cannot consume the full 1800s row timeout on a 5xx storm.
- [ ] events-bot / 2026-05-09 / locate the `_drop_reported_speech_duplicates` / `fact_first_desc` step that truncated event 3983 description to `> Спектакль-байопик «Эдит Пиаф.`; either move the cut to LLM-first prompt rule or add finished-sentence guard.
- [ ] events-bot / 2026-05-09 / investigate `joboutbox::telegraph_build` failure for events 4664 and 4705 (no `telegraph_url`); ensure failed builds either retry or are flagged out of `/daily`.
- [ ] events-bot / 2026-05-09 / investigate `TimeoutError: ... 0.0s` (label `telegraph_render_remove_logistics`) — likely `provider_timeout_seconds` set to `0.0` somewhere in the call path.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks: —
- post-deploy verification: —

## Prevention

- The pre-create dup probe is the deterministic safety net for any future LLM-matcher regression (Gemma upgrade, prompt drift, provider outage). Its 7-case unit test is the regression contract for `_titles_look_related` semantics.
- The widened `location_address` contract removes a concrete prompt-quality blind spot (foreign tokens, ticket-sales points, prose). It should be re-audited every time we add a venue with split box-office geography (Янтарь холл pattern).
- Wall-clock cap parity between `event_parse` and `smart_update` (follow-up) closes the second half of `INC-2026-05-07`.
