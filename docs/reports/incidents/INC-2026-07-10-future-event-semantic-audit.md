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
- post-freeze current-inventory check: nine newly imported rows `6799`–`6807` received local vectors and source-grounded LLM `pass` verdicts. At the explicit cutoff `2026-07-10T14:07:17Z`, after six confirmed cancellations/supersessions, the active-future inventory was `308`, so cutoff coverage was `308/308` (`299` surviving frozen rows + nine append rows); `314` unique rows were examined across the incident window. The scheduled VK importer was still running at cutoff, so this is an exact bounded inventory claim, not a claim that later imports are pre-audited.

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

These source/OCR contradictions were the bounded repair queue for the second audit and were repaired in production on 2026-07-10:

- `4517` — ongoing exhibition `Куплю гараж`: operational/opening-hours updates changed the canonical range to `2026-07-10..2026-08-10`; poster evidence says `13.05–31.10`.
- `3864` — valid Pianissimo concert on `2026-08-07 20:00`, but wrong-merge residue left `event_type=выставка`, `end_date=2026-09-27` and a Telegraph page for an unrelated exhibition.
- `5735` — source-backed concert time `20:00`, canonical `12:00`.
- `6312` — prose/default location conflicts with the event-local factory venue.
- `6517`, `6725`, `6782`, `6798` — source-local venue/city is more specific than or conflicts with the canonical binding.

Other LLM-blocked rows stay a review queue, not confirmed defects, until exact source adjudication. A multi-date source does not by itself invalidate one correct occurrence, and vector similarity never auto-merges or auto-deletes.

## Second full audit — 2026-07-10 22:08Z

A fresh, transaction-bounded compact production export rechecked the complete strict-future catalog after the scheduled VK importer finished:

- predicate: ISO `event.date >= 2026-07-10`, `lifecycle_status=active`, `identity_status=canonical`, `merged_into_event_id IS NULL`;
- cutoff: `2026-07-10T22:08:09.991179Z`;
- production `PRAGMA quick_check=ok`;
- exact coverage: `311/311` rows with source text or poster OCR and `311/311` available identity vectors (availability coverage; the then runner did not yet enforce exact `text_hash` freshness, which the post-repair runner below now does);
- vector recall: `1,557` union top-10 pairs; `71` at cosine `>=0.90`, `188` at `>=0.85`, `950` at `>=0.80`; every same-date `>=0.90` pair received source/OCR adjudication;
- frozen raw export SHA-256: `60f49e611cb323b4f6607566720d32dea3e1f38f4f3bdb563238710c9a130970`.

Confirmed duplicate donors/survivors:

- `6420 -> 4671`, `6609 -> 6395`, `6508 -> 6345`, `6722 -> 6721`,
  `6751 -> 6725`, `4757 -> 5373`, `6312 -> 5204`, `5735 -> 6424`.

Confirmed canonical repair units:

- wrong venue/address: `3803`, `6113`, `6401`, `6476`, `6520`, `6642`, `6725`;
- source-default/reference venue drift found in the append tail or VK comments: `6602`, `6603`, `6604`, `6782`, `6798`, `6808`, `6810`;
- date/range/identity contamination: `4517`, `4782`, `6112`, `6517`, `6721`;
- event-type/source-description contamination: `3864`, `5295` (distinct from film `6688`).

The `ЮНОСТЬ` defect was independently reported in a managed VK comment: events
`6602/6603/6604` say `Дворец спорта «Юность», Маршала Баграмяна 2`, while the
official source group and `groups.getById(fields=addresses,description)` ground
the venue as `Парк аттракционов ЮНОСТЬ, ул. Тельмана 3`, specifically at the
`Звёздный болид` ride. The fresh append row `6808` similarly mapped explicit
`Верхнее озеро, остров Шайба` to unrelated `Остров Канта` through a generic
single-token fuzzy match.

A regression-corpus pass scanned all `174` canonical `INC-*.md` records (`23,831` lines) and mapped them into 14 recurring quality families: eventness, false negatives, date roles, datetime parsing, recurrence, identity/duplicate decisions, location drift, titles, fact/description grounding, ticket/free/link semantics, media/OCR, public projections, provider boundaries, and audit/vector provenance. The repeated signatures are reflected in the root-cause classes below rather than treated as isolated one-off rows.

Authenticated VK comment coverage scanned `2,762` unique managed wall items,
all `103` comment-bearing posts and `301` comments/replies. Other correctness
remarks referred to already repaired `3980`, `6395/6609` city, `6569` and
`4327`; no other live comment-confirmed defect remained at the comment cutoff.

Explicit no-merge controls were retained for `5618/5619`, `6684/6750`,
`2863/2884` and `6364/6770`. Vector similarity remains recall, never deletion
authority. Evidence is stored in ignored artifacts under
`artifacts/codex/INC-2026-07-10-full-future-audit-v2/`.

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

- [x] Complete source adjudication and narrow repair of the confirmed queue above.
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


## Second-audit production mitigation — 2026-07-10 22:27–23:00Z

The full second-audit queue was repaired from row-level backups
`codex_backup_20260710_full_future_v2_*`; production `PRAGMA quick_check` remained
`ok` after the transaction.

- Nine confirmed duplicate donors were merged into the adjudicated survivors:
  `6420 -> 4671`, `6609 -> 6395`, `6508 -> 6345`, `6722 -> 6772`,
  `6721 -> 6772`, `6751 -> 6725`, `4757 -> 5373`, `6312 -> 5204`,
  `5735 -> 6424`. The ninth pair (`6721/6772`) was exposed only after the
  exact-hash replay: the later source explicitly distinguishes doors at `20:00`
  from the concert start at `21:00`, so `6772` is the survivor.
- Canonical date/range/source repairs were applied to `3864`, `4517`, `4782`,
  `5295`, `6112`, `6517`, `6721`; `5295` was detached from film `6688`.
- Source-grounded location repairs were applied to `3803`, `6113`, `6401`,
  `6476`, `6520`, `6602`–`6604`, `6642`, `6725`, `6782`, `6798`, `6808`,
  `6810`. In particular, `6602`–`6604` now use `Парк аттракционов ЮНОСТЬ,
  ул. Тельмана, 3`, and `6808` uses `Верхнее озеро, остров Шайба`.
- All eight donor Telegraph pages were tombstoned; their unique managed VK
  posts were deleted. Four stale/donor `@kldevents` posts were deleted through
  the approved E2E human session and a later duplicate `6604` post was also
  removed. The remaining/current Telegram announcements were verified by
  Telethon for title, schedule and venue.
- Every affected Telegraph survivor page was rebuilt with current canonical
  fields. Managed VK published or postponed cards were repaired; two old
  `ЮНОСТЬ` cards whose VK edit window had expired were deleted and replaced.
  The final authenticated postponed queue contained one repaired card per
  affected event, with the ten transient duplicate queue items removed.
- Current Supabase ICS objects were rebuilt from canonical rows. Event `6517`
  has no synthetic ICS because its source has a multi-day range but no grounded
  start time; this is an intentional fail-closed result.

A fresh post-repair strict-future export at
`2026-07-10T23:10:02.306492Z` contained `301` active canonical unmerged rows
(`311 - 9` merged donors and the repaired ongoing exhibition `4517`, whose
start date is correctly before the strict-future cutoff). The ongoing row was
checked separately. Exact-hash vector coverage was `301/301`; the final union
top-10 set contained `1,488` pairs (`63 >= 0.90`, `159 >= 0.85`,
`511 >= 0.80`). The only same-date pairs at `>= 0.90` were the explicit
no-merge controls `5618/5619` (12:00/16:00 performances) and `6684/6750`
(distinct 14:50/11:00 excursion programmes). The replay rejects sidecar vectors
whose `text_hash` does not equal the current identity-document hash and fills
only exact missing hashes ephemerally; transient embedding provider timeouts
now receive bounded retries instead of aborting the audit.

### Remaining projection blocker

Telegram Bot API deletion is limited to messages younger than 48 hours. The
approved E2E human account is not an administrator of `@kenigeventscalendar`,
and the current bot receives `MESSAGE_DELETE_FORBIDDEN` / `CHAT_WRITE_FORBIDDEN`
for the older Calendar documents. Therefore the canonical rows, Supabase ICS,
Telegram event channel, VK and Telegraph are repaired, but the legacy public
Calendar documents cannot be deleted or tombstoned with the available roles.
The incident remains **mitigated**, not closed, until a Calendar-channel admin
role removes or replaces those immutable historical projections.

### Second-audit release evidence

- prevention SHA: `a0fda9d4664ac0b0073a877f49bc16d5360d640b`, reachable from `origin/main`;
- Fly releases: `v1616`/`v1617`, image
  `deployment-01KX727W4XEP0H733494E5V54J`, digest
  `sha256:56e43c392811fd65d620b93ebdf100cb100b204b1bf625b2153e68704833f903`;
- post-deploy `/healthz`: ready, DB OK, no issues;
- focused location/vector regressions: `13 passed`; relevant incident suite:
  `77 passed` plus one pre-existing baseline failure also reproduced before the
  change; Python compile and vector-runner smoke passed;
- production repair/public verification evidence is stored in ignored artifacts
  under `artifacts/codex/INC-2026-07-10-full-future-audit-v2/`.
