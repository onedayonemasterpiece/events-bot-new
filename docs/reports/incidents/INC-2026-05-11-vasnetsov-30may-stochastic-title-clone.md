# INC-2026-05-11-vasnetsov-30may-stochastic-title-clone Stochastic title clone from first date-block to second event in multi-event Telegram digest

Status: closed
Severity: sev3
Service: `event_parse` Gemma 4 master prompt / Telegram monitoring → Smart Update create path
Opened: 2026-05-11
Closed: 2026-05-11
Owners: LLM prompts owner / data quality
Related incidents: `INC-2026-05-08-vk-tg-prompt-and-dup-probe` (overlapping prompt surface and dup-probe contract).
Related docs: `docs/llm/prompts.md`, `docs/reports/incidents/INC-2026-05-08-vk-tg-prompt-and-dup-probe.md`, `CHANGELOG.md`.

## Summary

Production event `4761` (date `2026-05-30`, time `16:00`, location `Сигнал`) was created with title `Виктор Васнецов: богатырь, написавший русскую сказку`, identical to the legitimate event `4760` on the previous date-block. The source post `t.me/signalkld/10657` is a multi-event digest by the «Культур-Мультур» project announcing two separate lectures at the same venue:

- 15 мая 19:00 — `Виктор Васнецов: богатырь, написавший русскую сказку`;
- 30 мая 16:00 — `Зинаида Серебрякова: нежность, пережившая катастрофу`.

The parser correctly extracted both dates and times but cloned the Vasnetsov title onto the Серебрякова event. `rich_facts_extract` for event 4761 even noted the regression in a fact: `В тексте упоминается параллельная лекция о Зинаиде Серебряковой под названием «Зинаида Серебрякова: нежность, пережившая катастрофу».`, yet the title and description never reflected it.

A fresh 2026-05-11 probe of `parse_event_via_llm` on the same source text with the unchanged master prompt returned two events with the correct distinct titles (Vasnetsov and Серебрякова). The failure is **stochastic Gemma 4 behaviour**, not a deterministic prompt bug.

## User / Business Impact

- One published Telegraph card for 2026-05-30 advertises the wrong lecturer and the wrong subject. Attendees expecting Васнецов get Серебрякова or vice versa.
- The legitimate Серебрякова lecture has no card at all on `/events 2026-05-30`.
- Trust hit: the bot is the canonical city events surface, mis-attributed cultural events are highly visible.

## Detection

- 2026-05-11 operator review of the Smart Update output for the May 30 slot.
- No alert fired — `event_parse` returned a syntactically valid 2-event list; downstream stages had no signal that one of the titles was a clone.

## Timeline

- 2026-05-09 22:50 and 22:52 Europe/Kaliningrad: two `event_source` rows (ids 1826481, 1826482) were created for the same source `t.me/signalkld/10657` within 1.5 minutes; both produced events with the same Vasnetsov title. This double-import is an additional anomaly noted but not yet root-caused.
- 2026-05-11: operator reported the regression.
- 2026-05-11: probe run against the live `parse_event_via_llm` with the same source text returned correct distinct titles; failure confirmed as stochastic.

## Root Cause

1. Gemma 4 at the original import time produced a 2-event list where the second event reused the first event’s `title` field instead of building it from the second `🎟 30 мая` date-block. The master prompt at `docs/llm/prompts.md` does include a block-locality rule for venues (line 199), but does not explicitly state the same rule for `title` / `short_description` / `programme`.
2. The behaviour is non-deterministic for this exact source: a re-run with the same prompt and same input now returns the correct distinct titles. Therefore this is a stochastic title-clone, not a deterministic prompt gap that requires hardening.

## Contributing Factors

- The same source post produced two `event_source` rows 90 seconds apart; the second event row could be a side-effect of the duplicate import, not an independent failure mode.
- The dup probe `_pre_create_duplicate_probe` (see `INC-2026-05-08-vk-tg-prompt-and-dup-probe`) would not catch this regression because both events here have legitimately different `date`+`time` anchors; the bug is title generation, not duplication.

## Automation Contract

### Treat as regression guard when

- changing the master `event_parse` prompt `docs/llm/prompts.md` block-locality rules (current rule pinned for venue at line 199);
- changing the default `event_parse` model or its provider routing;
- changing how `event_source` rows are de-duplicated at import time (the double-import side-effect remains an open question).

### Affected surfaces

- prompt: `docs/llm/prompts.md` master prompt (multi-event digest section).
- code: `main.py::parse_event_via_llm`, `event_source` write path during Telegram monitoring import.
- data: production events 4760 (correct) and 4761 (wrong title — Vasnetsov on 2026-05-30).

### Mandatory checks before closure or deploy

- If a prompt-side change is made for multi-event digests, replay `t.me/signalkld/10657` source text through `parse_event_via_llm` and assert both returned events have distinct titles matching their dates.
- For this incident specifically: no code-side change was made; closure relies on user-side re-import of event 4761 via the standard delete-and-reimport admin path.

### Required evidence

- 2026-05-11 probe transcript saved in the conversation history showed `events_count=2` with `🎨 Лекция «Виктор Васнецов…»` for 2026-05-15 and `🎨 Лекция «Зинаида Серебрякова…»` for 2026-05-30 — proving the current prompt is structurally correct.
- After user-side re-import: `event` row for 2026-05-30 16:00 at `Сигнал` must carry the Серебрякова title and a description grounded in the Серебрякова paragraph of the source post.

## Immediate Mitigation

- Documented the stochastic nature of the bug and the user-side re-import path.

## Corrective Actions

- None on the code side. The master prompt is already correct enough for this source; adding a defensive title block-locality rule was considered and rejected (extra prompt noise for a stochastic single-run failure).
- User re-imports event 4761 through the bot’s admin delete path so that the Telegram monitoring → Smart Update pipeline parses the source post again and creates the correct Серебрякова event.

## Follow-up Actions

- [ ] Owner: operator / no due date / delete event 4761 from production to trigger reimport.
- [ ] Owner: ingestion path / no due date / investigate why a single Telegram source message produced two `event_source` rows 90s apart (ids 1826481 and 1826482) on 2026-05-09 22:50–22:52.

## Release And Closure Evidence

- deployed SHA: n/a (no code change in this incident).
- deploy path: n/a.
- regression checks: probe of `parse_event_via_llm` on the live source text returns distinct correct titles.
- post-deploy verification: after re-import, the 2026-05-30 card on `Сигнал` carries the Серебрякова title and grounded description.

## Prevention

- Documented the stochastic-title failure mode here, scoped to multi-event digests with multiple `🎟 <date> <time>` blocks at one venue.
- Cross-linked to `INC-2026-05-08-vk-tg-prompt-and-dup-probe` because both incidents live on the same prompt/dup-probe surface; if title-clones recur, hardening should be considered as a small targeted rule there rather than as a new doc.
