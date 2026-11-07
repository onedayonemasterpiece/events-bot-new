# INC-2026-11-08 giveaway source publishes unsupported zero ticket price

Status: open
Severity: sev3
Service: Telegram source extraction / Smart Update / event ticket display
Opened: 2026-11-08 Europe/Kaliningrad
Closed: —
Owners: events-bot
Registry: `inc_f6f868d4eec9517d8c35ecce`
Related incidents: `INC-2026-04-30-tg-monitoring-event-quality-regressions`,
`INC-2026-06-12-raffle-source-publication-false-skip`,
`INC-2026-11-02-intake-quota-vk-media`

## Summary

Ordinary Telegram import 11726 created Event 10692 from
`https://t.me/locostandup/4389` and merged
`https://t.me/terkatalk/5623` into the same event. Both exact remote outputs
already contain `is_free=false`, `ticket_price_min=0` and no maximum price.
The sources announce the real November 13, 19:00 standup and a raffle of two
tickets, without stating the attendee ticket price. Current canonical data keeps
that zero and the public Telegraph ticket block displays `Билеты 0 руб.`.

Unknown attendee cost is not evidence of zero cost. This does not establish the
actual price of the concert. Preserve the valid event, its exact sources and
clean narrative; do not reintroduce the old blanket raffle-publication skip.

## User / Business Impact

A reader sees an unsupported admission price on an otherwise valid event.
The current narrative retains the comedian, three named projects and programme;
raffle mechanics are excluded. This price defect must not be hidden by calling
the prose compression successful or by relying only on `is_free=false`.

## Detection and evidence

- Run `43e501f8a5d340dfb485dc0ec946c165` started automatically November 7 at
  21:40 UTC. Remote collection finished at 22:53:37 UTC with 57 sources and
  98 exact messages; local import remains active.
- Candidate 27439 CREATED 10692; candidate 27440 MERGED the same card.
  Exact EventSources 14147858 and 14147859 are present.
- Both frozen remote event objects contain numeric zero before local conversion.
  Local `_to_int` preserves `None`; no missing-value-to-zero coercion is proved.
- Current Event 10692: `is_free=0`, `ticket_price_min=0`, no maximum or ticket
  link, ticket trust `high`. Public Telegraph returns HTTP 200 and displays
  `Билеты 0 руб.`. Managed VK URL is stored, but its price text is not yet
  independently inspected.
- Existing Smart Update adds `Semantic hint: extracted price lacks deterministic
  source match` to the fact log and logs `price_evidence_conflict`. It does not
  clear or resolve the numeric value at that boundary.
- Current schema describes minimum cost as optional and says to omit when no
  attendee price is grounded; the prompt's explicit free-entry contract primarily
  addresses `is_free`. A new keyword rule or repeated redundant instruction is
  not a qualified root fix.

Retained evidence under
`/home/dev/artifacts/events-bot-new/20261102T233947Z-intake-recovery-20261103/`:
`tg11726-first-accepted-quality.json`,
`tg11726-event10692-public-first.json`,
`tg11726-frozen-roster.json`, and the registry-open receipt.

## Timeline

- November 8, 00:54–00:56 Europe/Kaliningrad — first source accepted and event
  created through ordinary pipeline.
- November 8, 00:59 — second source merged with the original source retained.
- November 8, 01:03 — canonical zero and public ticket display independently
  confirmed. No Event/Job repair or replay performed.

## Root Cause and open questions

Observed producer boundary: both remote extraction outputs contain an unsupported
numeric zero even though the boolean free flag is false. The downstream evidence
conflict is recorded as a hint and the value reaches the ticket block. Exact
model reasoning and original raw provider response are unretained. Do not claim
an introduction commit or attribute this to the unrelated description regex fix.

Classify prevention as semantic ticket/free interpretation: LLM-first schema or
runtime contract, or narrow existing grounding/routing with distinguishing
controls. No broad deterministic giveaway/free classifier, new state, retry,
worker or price guess. Do not increase timeouts or bypass normal import guards.

## Automation Contract

### Treat as regression guard when

Changing attendee price/free extraction, raffle handling, price evidence checks,
canonical ticket updates or public ticket formatting.

### Affected surfaces

Remote Telegram schema/prompt, `source_parsing/telegram/handlers.py`,
`smart_event_update.py`, Event fields and managed public projections.

### Mandatory checks before closure or deploy

- Exact source without attendee price stays unknown; real underlying event
  remains accepted, preserving cleaned narrative and source links.
- Explicit free attendance remains free, explicit numeric paid price remains
  intact, raffle prize amounts/counts do not become admission price.
- Existing April 30 free/paid/work-hours and June 12 mixed-raffle contracts pass.
- Actual before/after model or runtime boundary evidence for the exact source;
  fixed mock output or schema wording assertion alone is insufficient.
- Native/date/identity guards unchanged; normal source import is not interrupted
  or replayed. Data repair is separate from prevention and must be exact and
  guarded against concurrent import changes.
- If code changes: all required latest-head CI, clean merged-main ancestry,
  full source and initialized isolated qualification, safe idle release and
  actual ordinary post-release receipts.

### Required evidence

Exact field-level before/after, source-based price decision, current public
readback and recurrence prevention in a normal eligible import. Do not close
from a model experiment or canonical-only correction.

## Containment and current status

No production data, public post, prompt, guard, cap or runtime changed during
triage. Do not blindly replay either accepted source or rerun old recoveries.
Preserve healthy local import 11726. Need bounded exact-source price contract
controls and safe existing correction path before any mitigation claim.


## Prevention candidate, November 8 01:17 Europe/Kaliningrad

Candidate reuses the existing failed price-evidence branch. It clears only zero-valued min/max when that check fails and is_free is not true. Nonzero values keep their current behaviour; no new general semantic price classifier. The existing number syntax now accepts one-digit amounts and its value matching includes zero, preserving an explicit zero and rejecting a prize amount as evidence for zero admission. No keyword rule, prompt, model, state, retry, worker, timeout or identity change. Net production lines +4. Existing conflict log adds zero_cleared without private text.

Corrected distinguishing baseline: two failures, five positive contrasts passing. Initial function-name typo selected no tests and is rejected evidence. The first candidate failed the explicit-zero contrast because the old number recognizer required two digits; that candidate was corrected rather than weakening the positive control. Final77 non-event, ticket, fact-grounding and location-review tests pass; provider audit passes. The broad suite initially exposed two unrelated stale location-review fixtures, reproduced on the exact original module. These two eventness-only tests now isolate location review; the fixed-date historical case also isolates wall-clock past-event policy. Production location/date guards remain enabled and their separate suite passes. LLM-disabled price tests do not qualify enabled production identity acceptance.

Existing CI includes the price and ticket modules. Candidate not yet merged, built, pinned, qualified or delivered. Actual runtime remains1e4103bf. Exact baseline, rejected intermediate, original-module fixture baseline, final and provider logs retained.


## Exact canonical mitigation, November 8 01:17 Europe/Kaliningrad

A read-only exact-state plan passed, followed by one apply invocation of zero-price10692-canonical-correction-once.py. Runtime1e, date/time November13 19:00, is_free=false, original zero/min and absent/max, both exact source anchors and original fact213388 were checked again in the transaction. Canonical ticket_price_min is now NULL. The original unsupported price fact is retained with conflict status. Title, date, time, source bindings and description remain unchanged. No accepted source was replayed, no kernel launched, no guessed attendee price written. NEVER rerun this correction after a lost response; reconcile its committed fields and receipt.

Existing schedule_event_update_tasks with refresh_existing_vk=True queued normal updates of current managed projections; no raw provider callback, post deletion or resend. Current Telegraph/media jobs151475/151482 are deferred to November8 00:05 UTC by existing budget rules, and rich Telegram151478 remains due07:00 UTC. No deadline, budget or queue status was bypassed. Public correction is therefore pending and must be read after ordinary workers finish. Receipt zero-price10692-correction-receipt.json; public0rub before receipt remains the last independently verified page. This is mitigation of one canonical value, not delivered recurrence prevention or incident closure.
