# INC-2026-07-17 replay contract

The first fixture is the exact production VK source text and poster identity from
`vk_inbox.id=10337`. Closure requires replay through the real VK auto-import
boundary and Smart Update on a production snapshot/shadow or controlled
production catch-up. The resulting event/source/poster rows and the durable
`image_geometry` outbox job must be recorded.

The second fixture is an opposite control: normal key rotation may restore an
LLM call, but may not weaken eventness or source-grounding decisions.
