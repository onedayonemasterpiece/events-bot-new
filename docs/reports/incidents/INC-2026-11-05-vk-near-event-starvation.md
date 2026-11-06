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
