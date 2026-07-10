# INC-2026-07-10 Future-event semantic/date audit

Status: mitigated
Severity: sev2
Service: canonical event quality / Smart Update / public fanout / vector sidecar
Opened: 2026-07-10
Closed: —
Owners: events-bot maintainer / Codex
Related incidents: `INC-2026-07-10-zoo-ticket-validity-non-event.md`, `INC-2026-07-09-recurring-occurrence-date-drift.md`, `INC-2026-07-07-new-event-quality-degradation.md`, `INC-2026-06-24-future-event-date-default-venue-regressions.md`

## Summary

The Zoo and Knight reports triggered a full source-grounded audit of the exact production inventory of active canonical rows dated `2026-07-10` or later. The frozen pre-repair catalog contained `305` events. Retrieval was vector-first: `193` existing `related_v1` embedding rows were reused and the other `112` rows were embedded locally without writing to the personalization sidecar. Every row then received an LLM source/date-role verdict; vectors were recall evidence only.

The audit reproduced the reported Zoo defect and exposed additional high-confidence instances of the same broader families: deadline/historical text imported as an event, a deadline used as start time, duplicate occurrences with wrong dates/times, operational notices mutating an existing exhibition, wrong-merge residue, and source-default/prose venue drift.

## User impact

- Non-events and wrong duplicate occurrences reached Telegram, VK, Telegraph and sometimes Calendar/ICS.
- Valid canonical events remained available in parallel with several bad duplicates, making the wrong cards look plausible.
- The stale vector sidecar covered only `193/305` exact audit rows; a vector identity allow decision was therefore neither complete quality coverage nor an eventness approval.

## Audit result

- exact catalog coverage: `305/305`;
- vector coverage used by the runner: `193` sidecar embeddings + `112` local fill, zero missing;
- final LLM verdicts: `174 pass`, `82 repair-candidate`, `6 remove-candidate`, `42 needs_review`, `1 indeterminate`;
- provider-error verdicts after retry: `0`;
- fail-closed candidate set: `131` rows. This is a review queue, **not 131 confirmed defects**: common false-positive shapes include a correct current-year inference from a day/month-only source and one valid occurrence extracted from a multi-date series.

Gemma 4 performed the primary audit. Provider `500`/quota failures were removed rather than counted as verdicts and retried per row; the final small retry set used the repository's normal `gemini-3.1-flash-lite` bulk-verification class. That retry is supplementary bulk classification, not an external consultant review. Destructive actions below were based on direct source/OCR and survivor verification, not the raw model label.

## Confirmed production repairs

| Event | Classification | Resolution |
|---|---|---|
| `6783` Zoo 31 December | `non_event`, `ticket_valid_until`, `work_hours_as_event_time` | cancelled and removed/tombstoned on every managed surface; scoped production replay produced zero events |
| `6057` theatre-department recruitment | `non_event`, `campaign_deadline` | cancelled; Telegram/VK removed where present; Telegraph tombstoned |
| `6787` museum staff-history article | `non_event`, `historical_date` | cancelled; Telegram and resolved live VK post deleted; Telegraph tombstoned |
| `2759` Makovetsky at `00:00` | `duplicate_event`, `unsupported_time` | superseded by source-backed `2758` at `19:00`; wrong Telegram/VK/Telegraph/ICS surfaces removed or neutralized |
| `6622` Magomayev/German on 10 July | `duplicate_event`, `wrong_date_time` | superseded by `6510` on 12 July at `17:00`; wrong surfaces removed or neutralized |
| `6771` New Year in summer at `17:00` | `deadline_as_event_time`, `duplicate_event` | superseded by `6720` at `21:00`; wrong surfaces removed |

The reported Knight Tournament `3980` is not in this repair set: the earlier 1-May-to-September season flattening was already repaired by `INC-2026-07-09`; its current `2026-07-10 20:00` occurrence is source-grounded.

Old Telegram Calendar documents `4881` and `7238` exceeded the Bot API delete window and the local human role was not a channel admin. They were therefore fail-closed neutralized in place with `removed.txt` and explicit tombstone captions; the wrong ICS content is no longer attached. This follows the official Telegram constraint rather than silently leaving a live wrong calendar card.

## Confirmed repair queue from the audit

These rows have direct source/OCR contradictions and remain blocked for narrow source-backed repair rather than automatic bulk mutation:

- `4517` — ongoing exhibition `Куплю гараж`: operational/opening-hours updates changed the canonical range to `2026-07-10..2026-08-10`; poster evidence says `13.05–31.10`.
- `3864` — valid Pianissimo concert on `2026-08-07 20:00`, but wrong-merge residue left `event_type=выставка`, `end_date=2026-09-27` and a Telegraph page for an unrelated exhibition.
- `5735` — source-backed concert time `20:00`, canonical `12:00`.
- `6312` — prose/default location conflicts with the event-local factory venue.
- `6517`, `6725`, `6782`, `6798` — source-local venue/city is more specific than or conflicts with the canonical binding.

Other LLM-blocked rows stay a review queue, not confirmed defects, until exact source adjudication. A multi-date source does not by itself invalidate one correct occurrence, and vector similarity never auto-merges or auto-deletes.

## Root-cause classes

1. **Date-role loss:** ticket validity, application deadlines and historical dates were treated as occurrence dates.
2. **Operational-time loss:** visitor/cash-desk hours, transfer deadlines, gate opening or meeting time were promoted to event start time.
3. **Series/occurrence flattening:** a weekly or multi-date program was treated as one continuous interval or one source update overwrote another occurrence.
4. **Identity/merge contamination:** related events or later logistics notices merged into an existing canonical row and retained stale fields/media/Telegraph content.
5. **Venue-prior overreach:** source default venue/city or prose survived despite stronger event-local evidence.
6. **Projection lag:** identity vectors were stale and public fanout had no current hash-bound semantic quality decision.

## Automation contract

- Freeze an exact canonical catalog and report `catalog_count`, vector coverage and LLM verdict coverage separately.
- Use pgvector/prototypes/neighbors for recall only; LLM must verify the current canonical claim against linked source text and poster OCR.
- Carry semantic roles such as `occurrence`, `series_or_program`, `ticket_valid_until`, `deadline`, `work_hours` and `historical` with evidence spans.
- Missing vectors, source evidence, malformed LLM output, provider failure or stale source/hash decision must block publication; none may be counted as pass.
- Never bulk-repair every LLM flag. High-confidence destructive changes require source confirmation, duplicate survivor selection and row-level backups.
- Keep Fly SQLite canonical; Supabase/Postgres is a derived vector/personalization sidecar.
- Before closure, replay Zoo, Knight occurrence/series, deadline, historical-article, valid recurring occurrence and event-local venue controls.

## Corrective actions delivered

- LLM-first Telegram schedule screen with explicit decision/date roles and evidence spans.
- Removed whole-message schedule rescue when no genuine date header exists.
- Smart Update routes operational date-role ambiguity to its LLM eventness review and fails closed.
- Vector-first full-catalog audit runner and source-linked incident evidence.
- Exact Zoo replay plus positive and recurrence controls.

## Follow-up actions

- [ ] Complete source adjudication and narrow repair of the confirmed queue above.
- [ ] Persist an append-only quality decision keyed by canonical hash + source-bundle hash and enforce it before every public projection.
- [ ] Restore the vector sidecar to current core coverage and alert on projection lag.
- [ ] Add first-class date-role/evidence fields to producer/import contracts.
- [ ] Close `INC-2026-07-09` only after its full pipeline replay/whole-series negative control.

## Release evidence

- prevention SHA: `732e34702f68f94b47b3c034d34999d1444a8efd`, reachable from `origin/main`;
- Fly image: `deployment-01KX62P844FWPSCVYCPZDBE8NR`, machine version `1615`;
- focused regressions: `59 passed`; one broader-suite baseline failure reproduced on pre-change `origin/main`;
- Zoo compensating replay: `ops_run=3421`, success, one forced source message, zero extracted/imported events, zero errors;
- post-deploy `/healthz`: ready, DB OK, no issues;
- audit and public-repair evidence: ignored artifacts under `artifacts/codex/INC-2026-07-10-future-date-quality/`.
