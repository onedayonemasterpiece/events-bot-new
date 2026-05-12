# INC-2026-05-11-event-parse-defender-and-escalation-poc Event_parse defender + escalation POC (bare `<event_type> — <venue>` title only)

Status: closed
Severity: sev3
Service: `event_parse` (`main.py::parse_event_via_llm`) → Gemma 4 default → optional escalation to `gemini-3.1-flash-lite` when the output matches a known "principal failure" pattern.
Opened: 2026-05-11
Closed: 2026-05-11
Owners: LLM prompts owner / data quality
Related incidents:
- `INC-2026-05-11-bar-bastion-stochastic-title-fallback-and-semantic-dup` (the regression that motivated the POC).
- `INC-2026-05-11-vasnetsov-30may-stochastic-title-clone` (sister stochastic regression on the same prompt surface).
- `INC-2026-05-08-vk-tg-prompt-and-dup-probe` (master-prompt evolution and `<event_type> — <venue>` title fallback ban).
Related docs: `docs/llm/prompts.md`, `CHANGELOG.md`.

## Summary

`event_parse` now runs a small deterministic defender on the Gemma 4 output and, when the defender flags a known forbidden pattern, re-calls the same stage on the configured escalation model (`gemini-3.1-flash-lite` by default). The first POC defender catches exactly one pattern: a bare `<event_type> — <venue>` title (`Концерт — Бар Бастион`, `Спектакль — Музыкальный театр`, `Лекция — Музей янтаря`). This is the same title shape that is already explicitly forbidden by the master prompt at `docs/llm/prompts.md:58`, but Gemma 4 occasionally falls back to it stochastically (see the Bar Bastion and Vasnetsov incidents).

When the defender flags any event in the response, the entire batch is re-parsed on the stronger model. When the defender does not flag, the original Gemma 4 output is returned unchanged — the path stays cheap for the common case. The escalation is gated by an env var so it can be turned off without redeploy.

## User / Business Impact

- Reduces the rate at which obvious title regressions reach production data:
  - `Концерт — Бар Бастион` (events 4765, 4788);
  - `Спектакль — Музыкальный театр` (events 4791–…);
  - any future `<event_type> — <venue>` shape forbidden by the master prompt.
- Pattern is now extensible: the defender shape can be reused for other principal failures detected in code review (address-as-`location_name`, prose in `location_address`, multi-event digest title clone, etc.) without re-architecting.

## Detection

This is a pre-emptive POC opened off the back of `INC-2026-05-11-bar-bastion-stochastic-title-fallback-and-semantic-dup` operator feedback ("Gemma sometimes makes principal errors we can detect with a defender; re-call the same stage with a stronger model"). It is not a reaction to a new production incident — it's the architectural answer to the existing pattern.

## Timeline

- 2026-05-11: operator review surfaced multiple stochastic Gemma title regressions (Bar Bastion, Vasnetsov, "Спектакль — Музыкальный театр" series).
- 2026-05-11: this incident opened, POC defender + escalation implemented for the bare-title pattern only.

## Root Cause

Pure prompt-side mitigation is insufficient for stochastic Gemma 4 lapses on well-defined forbidden patterns: the prompt already forbids the bare title fallback, but the model still falls into it on certain inputs. A deterministic post-output check + retry on a stronger model is the LLM-first answer that does not require prompt inflation.

## Contributing Factors

- Gemma 4 occasionally violates explicit master-prompt rules (stochastic, low single-digit %).
- `gemini-3.1-flash-lite` is already plumbed through `GOOGLE_AI_FALLBACK_MODELS` and `EVENT_PARSE_LARGE_POST_MODEL` and is the cheapest "stronger" route on this stack.
- The existing rate-limiter and retry chain already handle the Lite route, so no new infra is needed for the escalation call.

## Automation Contract

### Treat as regression guard when

- changing `main.py::_event_parse_title_looks_bare` or `_event_parse_defender_check`;
- changing `main.py::parse_event_via_llm` escalation block (the new code path right after the first Gemma call returns);
- adding new defender reasons or escalation patterns (must extend `_event_parse_defender_check` and add corresponding tests in `tests/test_prompt_json.py`).

### Affected surfaces

- code: [main.py::_event_parse_title_looks_bare](main.py), [main.py::_event_parse_defender_check](main.py), [main.py::parse_event_via_llm](main.py) escalation block.
- env: `EVENT_PARSE_DEFENDER_ESCALATION_MODEL` (default `gemini-3.1-flash-lite`; set empty to disable the escalation step without redeploy).
- tests: `tests/test_prompt_json.py` — 4 new tests pin the defender rule and the escalation flow.

### Mandatory checks before closure or deploy

- `.venv/bin/pytest tests/test_prompt_json.py -q` → all green (9 passed locally on 2026-05-11).
- Live smoke: a clean-output source (e.g. the Скитальцы text) must produce exactly **one** Gemma call (no escalation), with the title `🎸 Концерт «Скитальцы»: Артур Беркут и Сергей Маврин`. Verified 2026-05-11 (~55s, one `google_ai.call_ok`).

### Required evidence

- 2026-05-11 unit tests (4 new): defender flags bare titles, ignores valid quoted-programme titles, escalation re-calls with `gemma_model=gemini-3.1-flash-lite`, no escalation on clean output.
- 2026-05-11 live smoke on Скитальцы source: one Gemma call, no escalation, correct title.

## Immediate Mitigation

- Code change landed on this branch with regression tests.
- Env var `EVENT_PARSE_DEFENDER_ESCALATION_MODEL` is a clean kill-switch.

## Corrective Actions

- Added `_event_parse_title_looks_bare(title)` and `_event_parse_defender_check(events)` helpers in `main.py`.
- Inserted a defender + escalation block in `parse_event_via_llm` right after the first Gemma call returns. Disabled when the caller already pre-routed (`extra["gemma_model"]` set) or when the escalation env var is empty. On escalation timeout the un-escalated original Gemma output is returned (no hard-fail).
- 4 new tests in `tests/test_prompt_json.py`:
  - `test_event_parse_defender_flags_bare_type_dash_venue_title`
  - `test_event_parse_defender_check_returns_reasons_for_each_flagged_event`
  - `test_parse_event_via_llm_escalates_on_defender_flag`
  - `test_parse_event_via_llm_no_escalation_when_output_is_clean`

## Follow-up Actions

- [ ] Owner: LLM owner / no due date / add a second defender (address-shaped `location_name`, e.g. starts with `Ул.`/`улица`/`пр-кт` or matches `\d+[а-я]?$`) after observing the Bar Bastion and Жили они incidents in the same week.
- [ ] Owner: LLM owner / no due date / monitor production logs for `event_parse: defender flagged …; escalating primary=…` lines for the first 7 days; if escalation rate exceeds 20% of imports, investigate whether the Lite model should become the primary instead of the escalation.
- [ ] Owner: LLM owner / no due date / once two or three defenders exist, factor the helpers into a small `event_parse_defenders.py` module so each defender has its own focused test surface.

## Release And Closure Evidence

- deployed SHA: <to be filled at deploy time>.
- deploy path: regular `main` → fly deploy.
- regression checks: `pytest tests/test_prompt_json.py -q` (`9 passed`).
- post-deploy verification: production runtime logs should occasionally show `event_parse: defender flagged … reasons=['events[0].title_bare:Концерт — Бар Бастион'] … escalating primary=gemini-3.1-flash-lite` and the resulting events should have grounded titles, not the bare template.

## Prevention

- Regression tests pin the rule text and the escalation flow.
- This INC sits in the index as the canonical contract for adding more defenders; future contributors should extend `_event_parse_defender_check` and add a corresponding test, not bypass it.
