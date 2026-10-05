# Partner EventsBot MCP — release handoff for Codex

Status: **FULFILLED / DEPLOYED_ACCEPTED on 2026-10-05**. The source-ready
branch was merged in PR #725 to canonical main
`e679853bc328aece113163e30667cfd4bbeab70a` and deployed on RunCoveer as
`runcoveer/events:e679853bc`. This file is retained as the independent release
review contract; do not reinterpret it as a remaining manual owner step.
Real partner onboarding and public provider test posts were not used for
deployment acceptance.

## Independent review

1. Fetch the branch/PR and record exact HEAD plus current `origin/main`.
2. Review the diff against
   [Partner event operations](partner-event-operations.md) and the v2 scenario
   registry. Confirm one canonical DB/Smart Update/JobOutbox/promo engine,
   separate `events-partner` OAuth audience, current-policy checks at commit,
   no Social Workspace storage for event operations, and no secret/provider
   configuration exposed as MCP arguments.
3. Run `git diff --check` and the relevant Python 3.12 suites. The
   ChatGPT source-ready checkpoint finished with **505 passed** in the combined
   release regression. At minimum re-run:
   - Partner MCP event/promo/security/application tests;
   - `tests/test_promo.py`, `tests/test_partner_promo.py`,
     `tests/test_partner_promo_menu.py`;
   - migration/repeated-init and worker revision-guard tests.
4. Re-run additive SQLite acceptance: `Database.init()` twice,
   `PRAGMA quick_check`, then prove the previous main binary can initialize
   the upgraded DB without losing existing campaign/activity data.
5. Review that `video_general` and `vk_repost` are the only advertised
   partner promo placements, current availability is server-derived, and the
   VK path reaches the existing runner with sanitized durable outcome readback.

## Merge and release

If independent review and CI are green, merge the reviewed PR. Deploy only from
the clean canonical `main` checkout using the current production release
runbook; never deploy from this development worktree.

Keep all new mutation gates off for the first deploy. Run schema init/readiness,
health and SQLite checks, then refresh OAuth metadata and authenticate all three
resource families:

- owner ChatGPT/OpenCode;
- partner `events-partner`;
- read-only Codex.

Verify authenticated `tools/list`, cross-audience denial and refresh-token
behavior. Roll out in the documented order R0 → R1 → R1b → R2 → R3 → R4.
At R4 use two isolated test tenants and only explicitly approved private test
destinations. Verify create/merge/replay/review/reject, media, lifecycle,
campaign create/add/update/pause/resume/archive, executable video/VK placement,
publication/readback and restart/lost-response recovery.

Stop the rollout on any unexplained external outcome. Do not retry an
`outcome_unknown` provider mutation blindly.

## Rollback

Disable the newest feature gate first and leave the additive SQLite schema in
place. The source acceptance includes compatibility with the previous main
binary, so rollback is code/flags rather than destructive schema reversal.

## Acceptance labels

- `SOURCE_READY`: current branch/PR after green review.
- `ISOLATED_LIVE_VERIFIED`: only after the private provider smoke above.
- `DEPLOYED_ACCEPTED`: only after canonical-main deploy, health/SQLite/OAuth
  readback and controlled acceptance.

Current result: `SOURCE_READY=PASS`, `DEPLOYED_ACCEPTED=PASS`,
`ISOLATED_LIVE_VERIFIED=NOT_RUN`. The latter is not a deployment blocker; it
requires an explicitly private provider destination and must not be simulated
with a public post.

Lifecycle visual badges/public old→new site history, registrations, NFC/QR
check-in and NPS are separate roadmap/release scopes and are not to be invented
or declared complete during this handoff.
