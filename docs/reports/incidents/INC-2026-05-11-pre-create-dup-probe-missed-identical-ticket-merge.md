# INC-2026-05-11-pre-create-dup-probe-missed-identical-ticket-merge Pre-create dup probe missed a clear identical-`ticket_link` merge between two events of the same play

Status: open
Severity: sev3
Service: `smart_event_update.py::_pre_create_duplicate_probe` (branch 1: identical normalised ticket_link + overlapping date + no time conflict + related titles).
Opened: 2026-05-11
Closed: —
Owners: Smart Update / dup-probe owner
Related incidents: `INC-2026-05-08-vk-tg-prompt-and-dup-probe` (owns the probe contract).
Related docs: `docs/features/smart-event-update/README.md`, `CHANGELOG.md`.

## Summary

Production events `4156` and `4752` are two separate database rows for the same Театр современной драмы «Акт.Опус» performance of «Жили они долго и счастливо» on 2026-05-11 20:00 (Дом молодёжи, Октябрьская 76). Both rows carry the **identical** `ticket_link` `https://actop.us/performances/11-05-zhili-oni-dolgo-i-schastlivo`. Per the probe contract added by `INC-2026-05-08-vk-tg-prompt-and-dup-probe`, branch 1 of `_pre_create_duplicate_probe` should have merged the second candidate into the first event (`identical normalised ticket_link + overlapping date + no time conflict + related titles`). It did not. Two active duplicate cards survive in `/daily` for 2026-05-11.

## User / Business Impact

- The same performance appears twice in the daily list, with two different venue strings (`Театр современной драмы «Акт.Опус»` vs `Дом молодёжи, Октябрьская 76`).
- Erodes trust in the deduplication contract that was tightened only three days earlier.

## Detection

- 2026-05-11 operator review reported the duplicate alongside Bug A (address-shaped `location_name` on a third row 4582 that is a separate near-duplicate from Telegram).

## Timeline

- 2026-04-23 04:59 Europe/Kaliningrad: event 4156 imported from `vk.com/wall-69311452_2713` (the theatre's own VK).
- 2026-05-09 15:51 Europe/Kaliningrad: event 4752 created from `vk.com/wall-69311452_2727` with the **same** ticket_link as 4156. Dup probe should have merged. It did not.
- 2026-05-11: operator review reported the duplicate.

## Root Cause

Unknown without the runtime trace from 2026-05-09 15:51. Three plausible hypotheses, all consistent with the post-state evidence in `db_prod_snapshot.sqlite`:

1. The shortlist `events` passed to `_pre_create_duplicate_probe` at create time did not include event 4156. The probe walks only the shortlist, so a candidate-side filter (date window, source-type window, etc.) could have hidden 4156 even though it is structurally a perfect match.
2. The candidate's `ticket_link` was empty at probe time and was populated only later by a downstream stage. Branch 1 requires `cand_ticket` to be set before walking the shortlist.
3. `_titles_look_related(candidate.title, ev_title)` returned `False` for some reason (e.g. lemma/normalisation gap between `Жили они долго и счастливо` and `🎭 Спектакль «Жили они долго и счастливо»`). Branch 1 requires the related-title check to pass.

The post-state evidence ranks (1) and (2) as more likely than (3): the titles are visually clearly related and `_titles_look_related` is already well-tested for that shape.

## Contributing Factors

- The probe trace is not currently persisted: the only signal at production time is the resulting row in `event`, not "shortlist had/did not have event X" or "candidate.ticket_link was/was not populated when probe ran".
- The Smart Update create path has multiple stages that can each modify `candidate.ticket_link`, and the order in which they run vs. when the probe walks the shortlist is non-obvious from the code alone.

## Automation Contract

### Treat as regression guard when

- changing `_pre_create_duplicate_probe` in `smart_event_update.py`;
- changing the candidate shortlist construction immediately before `INSERT event` in the Smart Update create path;
- changing when `candidate.ticket_link` is populated relative to the probe call.

### Affected surfaces

- code: `smart_event_update.py::_pre_create_duplicate_probe` and the create-path block that calls it.
- data: production events 4156 and 4752 (both `active`); they need to be merged or one of them archived through the bot.

### Mandatory checks before closure or deploy

- Add lightweight runtime logging to the probe path:
  - shortlist size, presence of any event with `_normalize_url(event.ticket_link) == _normalize_url(candidate.ticket_link)`;
  - candidate `ticket_link` value at probe entry;
  - per-branch reject reason when the probe returns `None` despite a ticket-link match (e.g. `_titles_look_related=False`, `_has_explicit_time_conflict=True`, `_date_overlaps=False`).
- Replay this incident's two source posts (`vk.com/wall-69311452_2713` and `vk.com/wall-69311452_2727`) through Smart Update create on a fresh DB and confirm one of the three hypotheses.

### Required evidence

- A runtime trace from a replay or from a production occurrence that explicitly shows which guard fired (shortlist absence, empty ticket_link at probe time, or `_titles_look_related` false) on a known dup-pair.

## Immediate Mitigation

- None on the code side. Operator removes the duplicate row through the bot UI; the production duplicate count for «Жили они» drops to 1.

## Corrective Actions

- None landed yet. Investigation requires runtime evidence. See Follow-up Actions.

## Follow-up Actions

- [ ] Owner: Smart Update / no due date / add structured logging to `_pre_create_duplicate_probe`: log shortlist size, the count of events whose normalised `ticket_link` matches the candidate, and which branch rejected the match (with the named guard reason). Keep the log volume bounded (one line per probe call).
- [ ] Owner: Smart Update / no due date / write a focused replay test in `tests/replays/INC-2026-05-11-pre-create-dup-probe-missed-identical-ticket-merge/` with the two source posts from this incident; run it on each Smart Update touch to prevent silent regressions.
- [ ] Owner: operator / no due date / archive event 4156 or 4752 (whichever has the worse content), keeping 4752 if it already has the canonical Дом молодёжи venue.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks: —
- post-deploy verification: —

## Prevention

- This incident record itself acts as the regression contract until the probe logging follow-up lands and proves which guard fired.
