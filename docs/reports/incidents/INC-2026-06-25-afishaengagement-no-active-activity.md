# INC-2026-06-25 Afisha Engagement stopped for general klgdevents posts

Status: mitigated
Severity: sev2
Service: `afishaengagement` public CTA cards for VK `klgdevents`
Opened: 2026-06-25
Closed: —
Owners: events-bot operator / Codex
Related incidents: `INC-2026-06-14-afishaengagement-shadow-fallback-regression`, `INC-2026-06-14-vk-publication-cta-plain-duplicate`, `INC-2026-06-24-vk-past-actuals`
Related docs: `docs/features/afishaengagement/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

Public Afisha Engagement (`Мотивация`) cards stopped appearing on the general
`klgdevents` VK event flow after the broad all-events public canary campaign
expired on 2026-06-20. The VK publication boundary still invoked
`afishaengagement`, but production logs repeatedly showed
`reason="no_active_activity"` for ordinary non-festival events, so the boundary
fell back to plain VK posts.

## User / Business Impact

- VK `klgdevents` posts for ordinary events lost the motivating CTA card layer.
- Only narrower still-active activities, such as the `80 историй о главном`
  public canary, could still match; the general fallback had stopped.
- The product expectation that some ordinary VK event posts get engagement cards
  was not met for at least 2026-06-25 and likely since 2026-06-21.

## Detection

- User reported that `klgdevents` had no visible `afishacard`/motivation cards
  in the last day.
- Runtime file mirror was available: `/data/runtime_logs/events-bot.log*`,
  `ENABLE_RUNTIME_FILE_LOGGING=1`, retention 24h.
- DB evidence showed the last public all-events `afishaengagement` exposure was
  on 2026-06-20 before the broad campaign end date; later rows were either
  narrower 80-stories public cards or deleted/debug shadow rows.

## Timeline

- 2026-06-20 23:59:59 UTC — `promo_campaign.id=10`
  (`afishaengagement public canary · all events · 2026-06-12`) reached
  `ends_at` and stopped matching.
- 2026-06-25 09:44-23:07 UTC — production logs show many
  `afishaengagement.decision` checks for `vk_owner_id=-231920894` ending with
  `reason="no_active_activity"` for ordinary events.
- 2026-06-25 23:08 UTC — investigation confirmed active file logs and DB
  quick-check `ok`.
- 2026-06-25 23:10:59 UTC — production DB mitigation set campaign `id=10`
  `ends_at=NULL` and updated the goal comment.
- 2026-06-25 23:11-23:12 UTC — new log lines no longer show
  `no_active_activity`; candidates resolve to `campaign_id=10/activity_id=27`
  and produce normal `dice_miss` decisions when outside the 15% rollout.
- 2026-06-25 23:14-23:15 UTC — manual compensation generated one public card
  for event `6423` / `Калининградцы на защите Родины`.
- 2026-06-25 23:16 UTC — DB URLs reconciled after VK reassigned the edited
  postponed post from `wall-231920894_4492` to public `wall-231920894_4495`.

## Root Cause

1. The broad all-events public Afisha Engagement campaign was configured with a
   finite `ends_at='2026-06-20 23:59:59'`, although the production behavior
   expected an ongoing fallback for ordinary events.
2. Once it expired, ordinary non-80-stories events had no matching active public
   `afishaengagement` activity.
3. The normal VK publication path correctly failed open to plain posts, but no
   health/config smoke alerted that the broad fallback activity was inactive.

## Contributing Factors

- Existing documentation listed production public rates, but did not explicitly
  state that the all-events fallback campaign must be open-ended while rollout
  remains active.
- The log signal existed (`no_active_activity`) but was not monitored as a
  production regression.
- Manual catch-up through `wall.edit` on a postponed VK post without preserving
  `publish_date` caused VK to publish it immediately with a new post id; the DB
  had to be reconciled to the new URL.

## Automation Contract

### Treat as regression guard when

- changing `afishaengagement` candidate resolution, public/shadow mode,
  campaign date handling, or VK publication preflight;
- creating/updating promo campaigns for public Afisha Engagement;
- changing VK postponed edit/catch-up behavior for managed `klgdevents` posts.

### Affected surfaces

- `afishaengagement.resolve_candidates` active campaign filtering;
- `main_part2.sync_vk_source_post` public CTA preflight;
- production promo tables: `promo_campaign`, `promo_activity`, `promo_target`,
  `promo_exposure`;
- VK wall/postponed API behavior for `wall.getById`, `wall.get`, `wall.edit`;
- runtime log smoke for `afishaengagement.decision`.

### Mandatory checks before closure or deploy

- Production SQL confirms broad all-events public activity is active:
  campaign `id=10` or its successor has `status='active'`, `ends_at IS NULL` or
  a consciously future reviewed date, `target_type='all'`,
  `surface='afishaengagement'`, `publish_mode='public'`, target group
  `231920894`/`klgdevents`.
- Runtime logs after mitigation show `afishaengagement.decision` for ordinary
  `klgdevents` events resolving to an active candidate (`campaign_id` and
  `activity_id`) rather than `reason="no_active_activity"`.
- Authenticated VK API evidence verifies at least one current or next-slot
  Afisha Engagement public card, or a dice miss with an active candidate if no
  dice-selected event appears in the sampled window.
- If manual catch-up edits a postponed VK post, verify whether VK reassigned the
  post id and reconcile `event.source_vk_post_url` / `promo_exposure` URLs.
- Regression checks from related incidents still pass: no debug-shadow public
  side effect, no CTA/plain duplicate pair, and no same-day timed past event is
  published.

### Required evidence

- Runtime log mirror availability and searched patterns.
- DB rows for campaign/activity/target and recent `promo_exposure` rows.
- VK API verification using authenticated API, not public HTML.
- If a code fix is deployed: deployed SHA reachable from `origin/main` and
  `/healthz` after deploy.

## Immediate Mitigation

- Production DB campaign `id=10` was restored by setting `ends_at=NULL` and
  updating its goal comment.
- Manual compensation generated one public Afisha Engagement card for event
  `6423`; VK published it as `https://vk.com/wall-231920894_4495`, and the DB
  was reconciled from the old postponed URL `..._4492` to `..._4495`.

## Corrective Actions

- Restored the all-events public fallback config in production.
- Added this incident contract and clarified the Afisha Engagement docs so the
  broad public fallback is not accidentally configured as an expired temporary
  debug campaign.

## Follow-up Actions

- [ ] Add an automated health/check command or scheduled smoke that alerts when
      recent ordinary `klgdevents` Afisha Engagement decisions are mostly
      `no_active_activity` while VK publications continue.
- [ ] Add a safe catch-up helper for public Afisha Engagement on postponed posts
      that preserves `publish_date` or refuses to edit postponed posts without
      an explicit publish-time strategy.

## Release And Closure Evidence

- deployed SHA: no code deploy for mitigation; current prod image remained
  `deployment-01KW0G6NKKNCVWTZD2TJ2NCCG4` from merge SHA `338f9b11`.
- deploy path: production DB config repair via Fly SSH; docs-only incident PR to
  `main` required for process closure.
- regression checks:
  - DB quick-check `ok`;
  - runtime logs searched for `afishaengagement.decision`, `no_active_activity`,
    `dice_miss`, and `selected`;
  - post-mitigation logs showed `dice_miss` for `campaign_id=10/activity_id=27`
    instead of `no_active_activity`;
  - VK API found compensated public post `wall-231920894_4495`.
- post-deploy verification: `/healthz` remained green before the config repair;
  no code deploy was needed.

## Prevention

- Treat expiry of the broad all-events public Afisha Engagement activity as a
  production regression unless there is an explicit replacement activity.
- Prefer read-only candidate-resolution smoke before changing campaign dates.
- Avoid manual `wall.edit` catch-up on postponed posts until the helper can keep
  the intended publish time or explicitly record the immediate-public outcome.
