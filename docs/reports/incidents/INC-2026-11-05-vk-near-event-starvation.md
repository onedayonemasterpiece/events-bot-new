# INC-2026-11-05 VK near-event starvation under fresh backlog

Status: open. Severity: sev2. Surface: VK auto-import queue.
Registry continuation: inc_f5d0a79f3a7ffa6073158f3c.

## Evidence and root cause

Boyko post https://vk.com/wall394310405_3914 announces November 6 at 14:00.
Inbox 88166 has been pending with zero attempts since November 4, while normal
batches 11651/11654/11661/11663 process other carriers. There were 166 newer due
publications ahead of it. Its parsed event hint already points to November 6.
The fresh lane sorts publication timestamp before the existing event hint,
which only affects equal publication timestamps. This is mechanical work
ordering, not semantic date interpretation or event eligibility.

## Correction and acceptance

Within the existing fresh lane, prioritize today-or-future hints in ascending order (Kaliningrad day boundary);
use newest-publication order for the remainder. Keep the same history cursor,
claim transaction, limiter, evidence analysis and Smart Update pipeline.

One regression reproduces the near-event source waiting behind newer unknown
and expired hints, then verifies the nearest dated source is claimed first, including a date-only
hint at midnight of the current local day.
Existing fresh-only, history-interleave and manual oldest-first checks must
remain green. Qualify ordering against the real queue on an isolated copy.
Delivery requires exact merged main, runtime ancestry, full source verification,
isolated startup, Events-only cutover and the actual standard import receipt.
Do not change source dates, insert Events manually or rewrite queue statuses
to manufacture successful processing. Current status is not delivered.


## November 6 04:56 UTC — actual ordinary selection verified

The earlier not-delivered status is historical. PR838 was preserved in delivered runtime9ec3247c585ec27de8e243ea4a9aad0bbcb74627. Ordinary04:15 VK batch11673 actually selected formerly starved carrier88166/source26326, wall394310405_3914. The pipeline created10617, the November6 14:00 thematic Sovietsk cinema walk, with an exact EventSource and carrier link. Its other child refused location_grounding_review:llm_keep_not_grounded, leaving the carrier terminal partial failed_technical rather than clean imported. This proves claim-order liveness for the observed formerly waiting post, not success of every child or public publication. No manual callback, claim reset, date change or source replay. Preserve10617 and do not replay the entire post. Evidence vk11673-terminal-summary.json and vk11673-child-anchor-proof.json in the retained intake artifact directory. Residual semantic grounding remains open.


## November 6 — residual normalized-address grounding

Ordinary source child27022 (mural opening) returned semantic KEEP twice, but
`Горького 3` / `ул. Горького, 3` is not a verbatim substring of the authoritative
`дома № 3 по улице Горького`. The prompt permitted semantic KEEP while its
validator required lexical support. This is a semantic reviewer contract issue;
no deterministic address inference or grounding exemption is appropriate.

The existing reviewer prompt now requests REPAIR with a complete verbatim venue
fragment for reordered/reworded addresses. No new call, retry, validator, flag,
state or worker. Production diff is +4 lines, budget +5. Existing negative
controls still reject invented venues and missing locations.

Retained evidence: `/home/dev/artifacts/events-bot-new/20261104T093210Z-guide-digest-vk-quality-20261104`.
`boyko-location-tests.log`: distinguishing test fails before, all18 location tests pass after.
`boyko-shadow.log`: exact stored VK draft passed `persist_event_and_pages` and
Smart Update with real providers in a fresh shadow seeded with accepted walk10617.
The opening was CREATED with source-grounded `фасаде дома № 3 по улице Горького`;
walk10617 remained unchanged. The real-provider negative control with no stated
place was rejected `llm_reject_missing_location`. Shadow event10618 is not a
production receipt. No publication or canonical production writes in this replay.

Treat as regression guard when editing location prompts/validation or VK child replay.
Mandatory checks: all18 location controls, actual VK persistence/Smart Update
shadow replay, missing-place opposite control, preserve accepted walk and exact
source identity. Before closure collect exact-main release and actual failed-child
receipt. Do not replay the already accepted sibling or rewrite carrier status.
