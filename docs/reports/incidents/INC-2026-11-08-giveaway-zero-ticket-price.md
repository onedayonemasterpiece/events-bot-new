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


### November 8 01:32 Europe/Kaliningrad — recurrence prevention qualified, delivery pending

PR873 merged after all four latest-head required checks passed. Exact main516b1e51da444b071e03284f0bc0333869d411f9 built once as intake-zero-price-20261108, immutable sha256:cb7bcd3676a8ef2b2433f37266e9b71ee0a44d030ae0d30242e5560d94ceb977. Full4461-file proof, actual1e ancestry and unchanged dependencies verified. Initialized isolated seven exact-price/create contrasts and inherited identity/native/date/receipt/media/DOM/writer controls pass. Price controls use LLM-disabled isolated create and inherited offline Telegram controls retain their previous fixture identity defaults; neither is enabled-production identity acceptance. Wrong SHA rejects before initialization; final HTTP503 ready=false/DBskipped at187.2seconds boot age with only launch bind and tmpfs data proves isolation. Owned preflight removed.

NOT DELIVERED, no cutover attempted. Current1e remains ready=true. Normal TG11726 remote completed57sources98messages; local21fresh receipts at23:30UTC with actual new transitions throughkldevents5330. Preserve healthy import. Do not rebuild or requalify unchanged candidate. Staged host cutover/verify in exact516 release directory expect old1e/bddac immutable and old description-boundary tag. Fresh terminal/idle ops, MCP, background and independently untracked/remote heavy work are mandatory before switching; reconcile newer actual ancestry. Canonical10692 correction already applied once, public budget-governed refresh remains pending00:05UTC; never rerun correction driver. Qualified prevention does not close the public defect. Retained zero-price-qualification.json and exact logs.


### November 8 02:13 Europe/Kaliningrad — partial public correction, normal intake active

Canonical 10692 still has unknown attendee price; original unsupported fact 213388 remains conflict. Correction driver was not rerun. Ordinary vk_sync 151477 completed November 7 at 23:18:25 UTC. Independent authenticated wall.getById now confirms public wall-231920894_13773 preserves the standup and programme with no zero/free ticket claim. Runtime log proves existing postponed-to-live reconciliation from 13772 to 13773 at 23:05:04, before the normal edit; no replacement or duplicate send was performed.

Telegraph independently still displays 0 rubles at 00:11:53 UTC. Its pending job 151475 depends on media 151482, both due after the existing 00:05 budget boundary but not yet executed. Elapsed due time is not acceptance. No dependency clearing, forced build, queue status reset, budget bypass or provider callback. Recent worker cycles report SQLite locked at the job claim commit, then resume ordinary VK work; the exact lock holder and root are not proved. General health reports ready=true, DB and worker ok. Do not attribute these lock errors to the undelivered candidate or assume a new fix from the stack alone.

TG 11726 remote remains completed with the exact 98-message roster. Local processing advanced to 44 fresh messages at 00:13:29 UTC; latest kldevents/5345 completed 00:10:49, /5346 state 27611 began 00:10:50. Read-only exact-roster provisional accepted-state check finds 23 states over 22 Events with source links present, including historical/context states; these are not 23 new anchors or terminal totals. Preserve partial accepted children. Qualified PR873 remains NOT DELIVERED, no rebuild or cutover attempted.

Media Event10041 has no successor after 151133, but that job is still pending attempts0. Its actual next_run_at was already advanced by ordinary code to November 7 23:10:33, rather than the older remembered November 8 00:05. It has not completed after reset, so sustained post-reset prevention is not accepted. No mutation or guessed producer fix. Evidence postreset-0016-read.json, tg11726-local-0016.json, tg11726-accepted-0014.json, zero-price10692-vk-public-0015.json, zero-price10692-telegraph-0012.json and exact runtime excerpts under retained intake artifacts.
