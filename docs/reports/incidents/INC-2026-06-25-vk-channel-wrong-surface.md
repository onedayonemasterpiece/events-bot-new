# INC-2026-06-25 VK community Channel wrong surface

Status: mitigated
Severity: sev2
Service: promo campaigns / VK community Channel publishing
Opened: 2026-06-25
Closed: —
Owners: events-bot operations
Related incidents: —
Related docs: `docs/features/promo-campaigns/README.md`, `docs/operations/incident-management.md`, `docs/operations/release-governance.md`

## Summary

The new `vk_channel_publish` promo activity for festival `80 историй о главном`
used VK `messages.send` with an explicit `peer_id`. The compensation run sent a
compact event promo into VK Messenger/Favorites (or a personal dialog), not into
the community Channel surface visible in Messenger → "Каналы". The expected
community Channel post did not appear in the user's Channels tab.

## User / Business Impact

- The requested VK community Channel was not updated.
- A message appeared in the wrong Messenger surface, creating misleading
  delivery evidence.
- `promo_exposure.id=514` was initially recorded as `VK_CHANNEL_SENT`, which
  could make campaign reporting count a non-public/wrong-surface action.

## Detection

The user supplied mobile VK screenshots on 2026-06-25: the message was visible
under Messenger "Все" / "Избранное", while the "Каналы" tab listed other
channels and did not contain the target community channel/post.

## Timeline

- 2026-06-24: `vk_channel_publish` was implemented using `messages.send` and an
  explicit peer id, then a compensation run sent event `6172`.
- 2026-06-24 21:15:46 UTC: production DB recorded
  `promo_exposure.id=514`, `surface='vk_channel_publish'`,
  `publish_status='VK_CHANNEL_SENT'`, public target `https://vk.com/im?...`.
- 2026-06-25 09:28 Europe/Kaliningrad: user screenshots showed no community
  Channel entry.
- 2026-06-25 07:34 UTC: production mitigation disabled
  `promo_activity.id=30`, invalidated exposure `514` as
  `FAILED_WRONG_SURFACE`, cleared `public_targets_json`, and removed the
  `VK_AFISHA_CHANNEL_PEER_ID` Fly secret.

## Root Cause

1. The implementation equated VK community Channels with VK Messenger
   recipients and treated an explicit `messages.send.peer_id` as sufficient
   proof of the target surface.
2. Verification checked only API success/message URL and production exposure
   rows, not the user-visible Messenger → "Каналы" tab.
3. Public VK Open API docs for `messages.send` and `wall.post` were not treated
   as a hard contract boundary before the first production attempt.

## Contributing Factors

- VK uses the word "channel" both in third-party CRM/messenger integrations and
  in the newer community Channels product, which made search results ambiguous.
- The compensation run had no fail-closed guard for an unsupported community
  Channel posting API.
- `VK_CHANNEL_SENT` was counted as a public promo exposure even though the URL
  was an `im` Messenger URL.

## Automation Contract

### Treat as regression guard when

- changing `promo.py` activity surfaces, public exposure statuses, or campaign
  reports for `vk_channel_publish`;
- adding any VK Channel/community Channel transport;
- using `messages.send`, `wall.post`, or private VK web endpoints for a product
  described as a VK community Channel.

### Affected surfaces

- `promo.py`: `PROMO_SURFACE_VK_CHANNEL_PUBLISH`, public exposure statuses,
  initial `80 историй о главном` activity seed, runner branch;
- `main_part2.py`: compact VK Channel copy builder and transport helper;
- production DB: `promo_activity`, `promo_exposure`;
- Fly secrets: `VK_AFISHA_CHANNEL_PEER_ID`;
- external VK Open API and VK mobile/web UI.

### Mandatory checks before closure or deploy

- Targeted test proving `vk_channel_publish` does not call `messages.send`, does
  not create `VK_CHANNEL_SENT`, and creates no public exposure while unsupported.
- Check `PUBLIC_PROMO_EXPOSURE_STATUSES` excludes `VK_CHANNEL_SENT`.
- Production DB verification that the activity is disabled and wrong exposure is
  invalidated (`FAILED_WRONG_SURFACE`, `public_target_count=0`, empty public
  targets).
- VK API research evidence: official `messages.send` and `wall.post` docs do
  not document community Channel posting; any future transport must be verified
  in the actual Channels tab before public exposure is counted.
- Release evidence: deployed SHA and confirmation that the SHA is reachable from
  `origin/main`.

### Required evidence

- Tests: `tests/test_promo.py::test_promo_runner_skips_vk_channel_publish_until_real_channel_api`.
- Production SQL before/after mitigation.
- Fly secret list after mitigation (name absent; no secret values).
- Source research links or saved notes for VK API/community Channel behavior.
- Deployed SHA / PR / merge commit.

## Immediate Mitigation

- Disabled production `promo_activity.id=30` for `vk_channel_publish`.
- Updated `promo_exposure.id=514` from `VK_CHANNEL_SENT` to
  `FAILED_WRONG_SURFACE`, set `public_target_count=0`, and cleared
  `public_targets_json`.
- Removed `VK_AFISHA_CHANNEL_PEER_ID` from Fly secrets so the currently deployed
  wrong `messages.send` path cannot silently resume if the activity is toggled
  before code rollout.

## Corrective Actions

- Code now makes `vk_channel_publish` fail closed with
  `vk_community_channel_post_api_unsupported` and no exposure rows.
- The initial `80 историй о главном` VK Channel activity is seeded disabled and
  documents that `messages.send` is the wrong surface.
- `publish_vk_channel_promo_event_publication()` raises
  `vk_community_channel_post_api_unsupported` instead of falling back to
  Messenger.
- `VK_CHANNEL_SENT` is no longer a public promo exposure status.

## Follow-up Actions

- [ ] Find a documented or verified VK community Channel posting API. Do not use
  private endpoints for production unless the operational risk is explicitly
  accepted.
- [ ] If a transport is found, add an authenticated smoke that verifies the post
  in Messenger → "Каналы" before recording a public exposure.
- [ ] Decide with the operator whether the wrong Messenger/Favorites message
  should be deleted manually from VK.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending manual Fly deploy from clean hotfix worktree
- regression checks: pending
- post-deploy verification: pending

## Prevention

VK community Channel publishing is now treated as a distinct product surface,
not a synonym for Messenger. Any future implementation must prove the target
surface in the actual Channels tab before campaign reporting can count it as a
public promo exposure.
