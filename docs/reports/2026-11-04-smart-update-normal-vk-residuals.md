# Smart Update: actual normal VK residuals, 2026-11-04

For the existing Opus audit coordination thread. This supplements the original
brief and the locked-facts reproduction; it is evidence and a request for a
minimal recommendation, not permission to weaken identity/date guards.

## Delivered runtime and actual result

RunCoveer runs merged main `53a7791360ca8308458ce11df313d23c5c7bfd03`.
Ordinary scheduled VK ops 11623 ran 10:00:00–10:28:26 UTC, batch
`auto:1793786400`, scheduler run `abb086c481e343a994be9afa0dfc7858`.
All 12 carriers terminal, none unresolved: one fully imported, nine no-event,
two technical carriers with partially accepted children. Eight actual CREATED
Events 10513–10520 have exact source anchors. No manual replay was involved.

| Carrier / source packet | Children | Accepted Events | Residual |
| --- | --- | --- | --- |
| 88253 / 26387 | 1 CREATED | 10513 | none |
| 88279 / 26393 | 5 CREATED, 1 FAILED_TECHNICAL | 10514–10518 | location grounding response invalid |
| 88264 / 26390 | 2 CREATED, 1 FAILED_TECHNICAL | 10519, 10520 | distinct_not_grounded |

This is normal scheduled ingestion with partial success. A failed carrier is
not equivalent to zero Event writes; never replay the whole source blindly.
The delivered submitted-slot accounting works: watchdog recognizes the existing
submission and defers duplicate catch-up. Actual OCR OpenAI 429s were recovered
by Gemini 3.5 Lite. This run does not prove all Google keys exhausted.

## A. Recurring programme: schema/decision contract mismatch

Public source: https://vk.com/wall-211696971_5862. It explicitly lists
`7, 15, 22.11` for the same Ponart excursion, start 14:50. November 7 and 22
created Events 10519 and 10520; November 15 candidate state 26351 was refused.
Actual final decision 19442:

```json
{
  "action": "FINAL_RETRY",
  "reason": "distinct_not_grounded",
  "raw_action": "create",
  "raw_reason_code": "distinct_occurrence",
  "relation": "distinct_occurrence",
  "evidence": ["7, 15, 22.11", "СЕНТЯБРЬ 5, 12, 19, 26"],
  "blocking_conflicts": ["Разные даты проведения: 2026-11-15 против 2026-09-05"],
  "reviewed_event_ids": [8565]
}
```

Source inspection: DEDUP_ADJUDICATOR_SCHEMA reason_code is a closed enum of
merge/keep codes. `distinct_occurrence` is a relation value, not an allowed
reason_code. `_llm_dedup_adjudicator` accepts the returned string and the final
consumer refuses it. The raw producer answer violates the intended enum;
this does not prove which provider/schema adapter lost enforcement. Inspect
the actual boundary before proposing a fix. Do not silently remap all relation
values into successful reason codes.

The opposite real control is decision 19099: same concert on November 13 versus
14 without proven recurrence/reschedule. The earlier bounded replay proved
that simply dropping reason-code membership changes FINAL_RETRY to FINAL_DISTINCT
while the disputed dates remain. Preserve that refusal. Existing public
counterexample is recorded in PR 797 and the locked-facts reproduction report.

Please recommend the smallest normal producer/consumer contract correction
that distinguishes explicit recurrence from contradictory dates. No new repair
worker, historical queue drain, fallback create or manual acceptance state.

## B. Programme city/venue contamination plus failed grounding

Public source: https://vk.com/wall-35373633_29102. Six programme children: five
created, sixth candidate 26349 failed `location_grounding_review:llm_response_invalid`.
The source puts two final activities in the Marine Exhibition Centre in
Svetlogorsk. The fifth child (lecture) was repaired by the existing grounding
review and created as Event 10518 at that venue/city. The sixth, musical evening
November 4 at 16:00, retained city Svetlogorsk alongside a location containing
Museum of the World Ocean, Peter the Great Embankment 1, Kaliningrad.

Actual logs show the sixth grounding review returned invalid, not a successful
venue decision. No raw invalid provider payload has been captured in this
packet, so its exact format failure is not claimed. Investigate why the normal
child producer retains a contradictory default and why the existing grounding
call could not repair this sibling. Do not create from the mixed-city tuple or
repeat the five already accepted children. Prefer fixing the producer over an
additional special repair route.

## Required comparison for any proposed change

- Explicit Ponart recurrence, above: distinguish the November occurrence from
  the historical September programme using actual source evidence.
- Decision 19099: contradictory dates without proven recurrence must remain
  refused. Do not remove the existing guard to make a positive test pass.
- Same event with venue alias/doors-time variation must still match one owner.
- Multi-city programme must preserve the correct venue per child; already
  accepted siblings must remain intact on any bounded source recovery.

Apply the existing [simplicity policy](https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/main/skills/devserver-simplicity/SKILL.md).
User-visible goal: fewer missing occurrences without extra duplicate cards or
new operational repair paths. Start with a recommendation and one distinguishing
regression; provisional production-code budget at most 15 net lines, explain
if the correct root fix cannot fit. This document itself adds no production
code, model call, retry, state, queue, provider write or Event mutation.

Retained evidence on DevCoveer: vk11623-terminal-1035.json,
vk11623-anchors-and-failures-1037.json and vk11623-two-refusals.log in the existing
intake recovery artifact directory. Exact EventSource anchors 13937509–13937516
were verified. No secrets or private runtime configuration are included here.
