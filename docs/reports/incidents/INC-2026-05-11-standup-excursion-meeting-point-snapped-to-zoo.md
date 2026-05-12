# INC-2026-05-11-standup-excursion-meeting-point-snapped-to-zoo Meeting-point landmark on an excursion silently snapped to a nearby Known-venues building by address proximity

Status: closed
Severity: sev3
Service: `event_parse` Gemma 4 master prompt at `docs/llm/prompts.md` / Telegram monitoring → Smart Update create path / public Telegraph card for walking-tour events
Opened: 2026-05-11
Closed: 2026-05-11
Owners: LLM prompts owner / data quality
Related incidents: `INC-2026-05-09-event-location-alias-free-dup-regressions` (venue-grounding family); `INC-2026-05-08-vk-tg-prompt-and-dup-probe` (master-prompt evolution).
Related docs: `docs/llm/prompts.md`, `docs/reference/locations.md`, `CHANGELOG.md`.

## Summary

Production event `4687` (`Стендап-Экскурсия по Калининграду`, 2026-05-11 14:00) was stored with `location_name="Калининградский зоопарк"`, `location_address="просп. Мира 2"`. The source post explicitly says the event is a walking tour and the meeting point is `Скульптура "Борющиеся зубры" просп. Мира, 2 / остановка общ. транспорта "Технический университет"`. The bull sculpture is a landmark on `Мира 2`; the Калининградский зоопарк known venue sits a few blocks away on `пр-т Мира 26` and is **not** mentioned in the post.

The bot snapped the meeting-point address to the geographically closest "Known venues" entry, producing a venue identity that does not match either the source text or the canonical address of that venue. The result is a public card that misleads readers into expecting an event inside the zoo.

A 2026-05-11 probe of `parse_event_via_llm` on the unchanged master prompt reproduced the bug deterministically (same wrong `location_name`/`location_address`), so this is a structural prompt gap, not a stochastic Gemma 4 lapse.

## User / Business Impact

- Public Telegraph card and `/daily` row tell readers the standup excursion happens at the Калининградский зоопарк. In practice the meeting point is the bull sculpture across the avenue and the event is a walking tour through the city centre.
- Pattern risk: any event whose source describes a landmark meeting point on an avenue where a popular Known venue sits (excursions, free walking tours, тематические туры, стендап-экскурсии) was exposed to the same regression.

## Detection

- 2026-05-11 operator review caught the venue mismatch on the public card.
- No alert fired — `event_parse` produced syntactically valid fields and downstream grounding accepted them because the wrong `location_name` happens to be a real Known venue.

## Timeline

- 2026-05-07 22:20 Europe/Kaliningrad (event_source `imported_at`): event 4687 imported from a VK source with the wrong venue.
- 2026-05-11: operator review reported the mismatch alongside the May 11 quality batch.
- 2026-05-11: probe of live `parse_event_via_llm` on the same source text reproduced the bug deterministically.
- 2026-05-11: master-prompt patch landed with a stronger "Meeting-point override" rule; re-probe returned `location_name="Скульптура «Борющиеся зубры»"`, `location_address=""`, `city="Калининград"` — accepted shape per operator framing.

## Root Cause

1. The master prompt at [docs/llm/prompts.md](docs/llm/prompts.md) had two overlapping rules around venue normalisation:
   - line 111: "If the source only describes the place by an oblique reference, copy the canonical Known-venues row."
   - line 112: "If neither source/OCR nor a clear reference match a known venue, return empty strings — do NOT fall back to a 'plausible' Kaliningrad venue from world knowledge."
   Neither rule explicitly handled the case where a meeting-point landmark sits *next to* a Known venue. Gemma 4 used its strong prior that `Мира + low number` maps to the zoo and copied the zoo row, ignoring the fact that the address (`Мира 2`) does not match the zoo's canonical address (`пр-т Мира 26`) and that the post never mentions the zoo.
2. The first attempted prompt fix (a bullet appended to the multi-event block-locality rule deep in the rules list) was not strong enough — Gemma kept producing the zoo even after the rule was added.

## Contributing Factors

- The "Известные venues" prior for popular landmark streets is very strong in Gemma 4.
- The address `просп. Мира 2` is genuinely close to the zoo, so a coincidental proximity bias was easy to fall into.
- The original rule that should have caught this ("do NOT fall back to a 'plausible' venue from world knowledge") was phrased about absent venue mentions, not about meeting-point landmarks with their own explicit text identity.

## Automation Contract

### Treat as regression guard when

- changing `docs/llm/prompts.md` around the venue-grounding rules (currently lines 110–115 and the new "Meeting-point override" bullet);
- changing how the master prompt is assembled in `main.py::_read_base_prompt`;
- adding or rephrasing rules about walking-tour / excursion / standup-excursion handling in any prompt that produces `location_name`/`location_address`/`city` for `event_parse`.

### Affected surfaces

- prompt: `docs/llm/prompts.md` master prompt, specifically the new "Meeting-point override" bullet near line 113.
- tests: `tests/test_prompt_json.py::test_base_prompt_includes_meeting_point_override_rule` pins the rule text.
- data: production event 4687 still carries the wrong `location_name`/`location_address` until re-imported through Smart Update.

### Mandatory checks before closure or deploy

- `.venv/bin/pytest tests/test_prompt_json.py -q` → all green.
- Replay against the 4687 source text via `parse_event_via_llm` must return a meeting-point shape, not `Калининградский зоопарк`. Two shapes are acceptable per the rule:
  - `location_name="Скульптура «Борющиеся зубры»"`, `location_address=""`, `city="Калининград"`; or
  - `location_name=""`, `location_address=""`, `city="Калининград"`.

### Required evidence

- 2026-05-11 probe (before fix): `location_name="Калининградский зоопарк"`, `location_address="Мира 2"` — confirms deterministic prompt gap.
- 2026-05-11 probe (after fix): `location_name="Скульптура «Борющиеся зубры»"`, `location_address=""`, `city="Калининград"` — confirms rule works.
- Final closure depends on operator-side re-import of event 4687 through Smart Update so the production row inherits the corrected output.

## Immediate Mitigation

- Master prompt patched on this branch; pytest green.
- Production event 4687 keeps the wrong venue until re-imported.

## Corrective Actions

- Added a "Meeting-point override" bullet to `docs/llm/prompts.md` (near the venue-grounding rules at line 113) that explicitly forbids snapping a meeting-point landmark to a Known venue purely by address proximity, lists the meeting-point markers (`Встреча:` / `Место встречи:` / `Сбор:` / `Точка старта:` / `Встречаемся у/возле/около/на`), enumerates the landmark family (skulptura, остановка, площадь, ворота, мост, фонтан, угол улиц, парк-entrance), and gives the 4687 example (`Скульптура «Борющиеся зубры»` is NOT `Калининградский зоопарк`).
- Added a regression test `tests/test_prompt_json.py::test_base_prompt_includes_meeting_point_override_rule` that pins the new rule text against the real 4687 ground-truth landmark and known-venue names.

## Follow-up Actions

- [ ] Owner: operator / no due date / re-import production event 4687 via Smart Update so the new prompt produces the correct landmark venue.
- [ ] Owner: prompt maintenance / next pass / watch for `Калининградский зоопарк`-shaped regressions on other excursion events, especially walking tours that meet at landmarks on busy avenues near other Known venues (e.g. Кафедральный собор + Остров Канта, Янтарь холл + ТРЦ "Европа", filharmonia + ...).

## Release And Closure Evidence

- deployed SHA: `a1d48da3` (events-bot-new-wngqia v1061, deployed 2026-05-12 05:48 UTC).
- deploy path: regular `main` → fly deploy.
- regression checks: `pytest tests/test_prompt_json.py -q` (`5 passed`).
- post-deploy verification: re-imported event 4687 must show `location_name` derived from the meeting-point landmark or be empty (with `city="Калининград"`).

## Prevention

- Regression test pins the rule text and the 4687 example, so a future prompt rewrite that loses the meeting-point override will fail CI.
- Cross-linked to `INC-2026-05-09-event-location-alias-free-dup-regressions` because both incidents live on the venue-grounding family.
