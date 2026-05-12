# INC-2026-05-11-bar-bastion-stochastic-title-fallback-and-semantic-dup Stochastic `<event_type> — <venue>` title fallback plus semantic duplicate that escaped the structural dup-probe

Status: closed
Severity: sev3
Service: `event_parse` Gemma 4 master prompt / `smart_event_update._pre_create_duplicate_probe` / public Telegraph + Telegram card
Opened: 2026-05-11
Closed: 2026-05-11
Owners: LLM prompts owner / data quality
Related incidents: `INC-2026-05-08-vk-tg-prompt-and-dup-probe` (forbids the `<event_type> — <venue>` fallback and owns `_pre_create_duplicate_probe`); `INC-2026-05-11-vasnetsov-30may-stochastic-title-clone` (sister stochastic title regression).
Related docs: `docs/llm/prompts.md`, `docs/reports/incidents/INC-2026-05-08-vk-tg-prompt-and-dup-probe.md`, `smart_event_update.py::_pre_create_duplicate_probe`.

## Summary

Production events `4765` (`2026-08-15 20:00`, `Бар Бастион`) and `4788` (`2026-07-04 19:00`, `Бар Бастион`) were both created with the bare title `Концерт — Бар Бастион`, the exact `<event_type> — <venue>` template that is explicitly forbidden by the master `event_parse` prompt at [docs/llm/prompts.md:58](docs/llm/prompts.md#L58):

> Do NOT use the bare `<event_type> — <venue>` template (`Концерт — Янтарь холл`, `Лекция — Музей янтаря`); a venue is not a title.

Both source posts contained obvious title-eligible material (`тур «Скитальцы»`, `Артур БЕРКУТ` + `Сергей МАВРИН`, `«БАСТИОН», опен-эйр ДЕНЬ ГОРОДА`), so the forbidden fallback was a stochastic Gemma 4 lapse, not a structural prompt gap.

Additionally, event 4765 is a semantic duplicate of the legitimate event 4486 (`Скитальцы: Сергей Маврин и Артур Беркут`, also 2026-08-15 at `Бар Бастион`, time 21:00). Both reference the same Скитальцы tour stop. `_pre_create_duplicate_probe` did not merge them because:

- `ticket_link` differed (`qtickets.events/232664-…` for 4486 vs `vk.cc/cXi4Uk` for 4765); and
- the structural time-anchor branch requires identical times, while 4486 has time `21:00` and 4765 has time `20:00` (the source post explicitly says `Двери: 19:00`, `Начало: 20:00`).

A fresh 2026-05-11 probe of `parse_event_via_llm` on event 4765’s source text returned title `🎸 Концерт «Скитальцы»: Артур Беркут и Сергей Маврин`, confirming the prompt is structurally correct.

## User / Business Impact

- Two visible Telegraph cards (`Koncert--Bar-Bastion-05-10`, `Koncert--Bar-Bastion-05-10-2`) advertise a concert and an open-air festival without naming the headliners or programme — the most action-driving piece of information for music events.
- One of those cards (event 4765) is also a duplicate of the legitimate Скитальцы card (event 4486), so the same concert appears twice on the 2026-08-15 daily slot.
- Pattern risk: same stochastic fallback was observed on item-list #11 of the operator report (multiple bare `Спектакль — Музыкальный театр`, `Концерт — Бар Бастион` titles).

## Detection

- 2026-05-11 operator review flagged both the title regression and the dup for the 2026-08-15 slot.
- No alert fired — `event_parse` succeeded structurally and `_pre_create_duplicate_probe` correctly answered "no structural match" given the time/ticket differences.

## Timeline

- 2026-05-10 (event_source `imported_at`): events 4765 and 4788 imported through the VK auto-import → Smart Update create path with the forbidden bare title.
- 2026-05-11: operator reported.
- 2026-05-11: probe of the live `parse_event_via_llm` on event 4765’s source text returned the correct title `Концерт «Скитальцы»: Артур Беркут и Сергей Маврин`; failure confirmed as stochastic.

## Root Cause

1. Gemma 4 at original import time fell back to the forbidden `<event_type> — <venue>` title template despite the explicit `docs/llm/prompts.md:58` rule. A current re-run on the same source text now produces a correct grounded title — therefore stochastic, not deterministic.
2. `_pre_create_duplicate_probe` does not have a semantic branch for `same date + same venue + same programme cue + similar time (±2h)` when ticket links differ. The Скитальцы tour has two announcement posts with different ticket-sale points (the venue’s `qtickets.events` page vs the VK reposted `vk.cc` URL) and a 1-hour difference between the announced concert time (21:00) and the official «Начало» time (20:00). Both structural anchors of the probe miss, so the merge does not happen.

## Contributing Factors

- The bare `<event_type> — <venue>` fallback is rare but recurrent; this is the second observed batch on the same prompt revision.
- Tour-style concerts often have two parallel announcements (artist-side and venue-side) with different ticket URLs and slightly different time framings; the dup-probe was designed for the more common identical-ticket-link case.

## Automation Contract

### Treat as regression guard when

- changing the master `event_parse` prompt forbidden-title rules in `docs/llm/prompts.md` (currently lines 56–63);
- changing `_pre_create_duplicate_probe` in `smart_event_update.py` (its current branches are pinned by `INC-2026-05-08-vk-tg-prompt-and-dup-probe`);
- adding any "semantic dup" branch — the new branch must not collapse legitimately distinct events at the same venue on the same date (e.g. afternoon kids show + evening adult show).

### Affected surfaces

- prompt: `docs/llm/prompts.md` master prompt, especially the title rules.
- code: `smart_event_update.py::_pre_create_duplicate_probe`.
- data: events 4486, 4765, 4788; Telegraph pages `Koncert--Bar-Bastion-05-10` and `Koncert--Bar-Bastion-05-10-2`.

### Mandatory checks before closure or deploy

- For this specific incident: no code change. Closure relies on user-side delete-and-reimport of event 4765 (dup of 4486) and event 4788 (single bad title) via the standard admin path.
- If a semantic dup branch is added in a future change, it must replay this incident’s source texts and assert that:
  - event 4765 source merges into event 4486 (correct);
  - event 4788 stays as its own event (different date — no merge);
  - synthetic afternoon-kids vs evening-adult shows at the same venue do not collapse.

### Required evidence

- 2026-05-11 probe transcript showed `events_count=1` with title `🎸 Концерт «Скитальцы»: Артур Беркут и Сергей Маврин` — proving the prompt is currently correct.
- After user-side re-import: 2026-08-15 daily slot must show only one card for the Скитальцы concert; 2026-07-04 card must carry a non-fallback title grounded in `«БАСТИОН», опен-эйр ДЕНЬ ГОРОДА`.

## Immediate Mitigation

- Documented the stochastic title regression and the dup-probe gap.
- No automated mitigation; user deletes 4765 (the dup) and reimports 4788 (the bad-title event) through the bot’s admin delete path.

## Corrective Actions

- None on the code side. Defensive prompt repeat of the forbidden-template rule was considered and rejected (prompt noise; sister stochastic incident `INC-2026-05-11-vasnetsov-30may-stochastic-title-clone` reaches the same conclusion).
- A future semantic dup-probe branch (same date + same venue + similar time + related programme) remains an open architecture follow-up, not a fix for this incident.

## Follow-up Actions

- [ ] Owner: operator / no due date / delete event 4765 (dup of 4486) via the bot to remove the duplicate card and trigger reimport.
- [ ] Owner: operator / no due date / delete event 4788 to trigger reimport with a non-fallback title.
- [ ] Owner: Smart Update / backlog / consider extending `_pre_create_duplicate_probe` with a semantic same-date+same-venue+related-programme branch with ±2h time tolerance; must include a guard against legitimate parallel-time events.

## Release And Closure Evidence

- deployed SHA: n/a (no code change).
- deploy path: n/a.
- regression checks: probe of `parse_event_via_llm` on 4765 source returned the correct grounded title.
- post-deploy verification: after user-side actions, 2026-08-15 has a single Скитальцы card; 2026-07-04 has a grounded-title День города опен-эйр card.

## Prevention

- Documented the stochastic title fallback pattern and the dup-probe gap.
- Cross-linked to `INC-2026-05-08-vk-tg-prompt-and-dup-probe` for the underlying prompt + dup-probe contract.
- Cross-linked to `INC-2026-05-11-vasnetsov-30may-stochastic-title-clone` for the sister stochastic title regression.
