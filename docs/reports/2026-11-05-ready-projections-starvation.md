# Ready public projection waits for unrelated media

At05:17:54 UTC, Event9779 Telegraph job100934 remained pending and due since
03:18:40. Its latest declared media dependency144431 was done at03:18:43.
The actual worker continued choosing unrelated media/VK jobs.132 media jobs
were due. These are queue facts, not132 missing Events.

The previous fresh-selection fix is already live in3948742. It ends each
background pass after one claim and reselects. The remaining barrier is the
existing task-priority map: all ordinary media rank ahead of public projections,
even when a projection's own media is complete. Per-event `depends_on` already
provides the correct prerequisite; unrelated pictures need not finish first.

The correction swaps the existing ordinary public and media lanes. Six scalar
priority values change, with no new rank or mechanism. Ready VK/fresh Telegram
and missing-media VK priorities remain stronger. The normal dependency, due-time,
expired-event, feature-budget and Telegram-spacing checks are untouched.

A SQLite replay places a ready Telegraph job beside an unrelated due media job.
Unchanged production chooses the unrelated media and fails the distinction;
the corrected version chooses the ready projection. A second case still blocks
a projection on its own pending media.35 focused worker, dependency, captcha,
lifecycle and coalescing controls pass, including existing VK ordering.
Six broader Telegram failures reproduce with the unchanged original worker:
five quiet-time/spacing expectations and one absent local Telethon package.
They are not successful checks.
Provider-path audit passes. No external provider call or production queue
mutation is used by these controls.

This report describes a proposed correction, not delivery. Source394 remains
production while Philharmonia catch-up11645 and normal3D job11646 run. A new
release requires green mandatory CI, exact-main ancestry, full source proof,
initialized isolated controls and safe Events-only cutover. Actual current
Telegraph/calendar/TG receipts must then prove the effect. Do not force publish,
mark media done, bypass budgets or replay accepted requests.

Retained evidence is under the existing intake recovery directory:
`outbox-ready-projection-plan.md`, `outbox-ready-before.log`,
`outbox-ready-focused.log`, `outbox-ready-original-controls.log` and
`media-exact-0516.json`. Existing canonical incident:
[INC-2026-11-02](incidents/INC-2026-11-02-intake-quota-vk-media.md).


## Live delivery and first acceptance, November5 05:49 UTC

PR826 is delivered on RunCoveer as ae5d459ceeab30e86703ac48c63287a7fbdc9b8e. Actual ordinary worker completed the previously starved Event9779 Telegraph and calendar projections at05:43 UTC, without manual drains or requeue. Public Telegraph body now has the current title and November27 date. Its scheduled rich Telegram card remains due07:50 UTC, so that publication is still a separate acceptance check. Readiness, SQLite, settings and unrelated Kotopogoda service passed live checks. No provider or media budgets were bypassed. See the canonical incident timeline for exact receipts and the separate successful39-row Philharmonia recovery.
