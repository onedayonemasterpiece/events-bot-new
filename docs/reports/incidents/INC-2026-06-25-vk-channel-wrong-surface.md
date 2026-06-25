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
  described as a VK community Channel. `messages.send` is acceptable only when
  the feature is explicitly labelled as a non-public operator manual-copy draft.

### Affected surfaces

- `promo.py`: `PROMO_SURFACE_VK_CHANNEL_PUBLISH`, public exposure statuses,
  initial `80 историй о главном` activity seed, runner branch;
- `main_part2.py`: compact VK Channel copy builder and transport helper;
- production DB: `promo_activity`, `promo_exposure`;
- Fly secrets: `VK_AFISHA_CHANNEL_PEER_ID` (removed), `VK_AFISHA_CHANNEL_DRAFT_PEER_ID`
  (manual-copy draft only);
- external VK Open API and VK mobile/web UI.

### Mandatory checks before closure or deploy

- Targeted test proving `vk_channel_publish` does not create `VK_CHANNEL_SENT`
  or public exposure. If it uses `messages.send`, it must be explicit
  `delivery_mode='vk_messages_manual_copy_draft'`, record only
  `VK_CHANNEL_DRAFT_SENT`, and keep `public_target_count=0`.
- Targeted test proving the draft CTA prefers `ticket_link`/registration links
  over Telegraph URLs.
- Check `PUBLIC_PROMO_EXPOSURE_STATUSES` excludes `VK_CHANNEL_SENT`.
- Production DB verification that the wrong exposure is invalidated
  (`FAILED_WRONG_SURFACE`, `public_target_count=0`, empty public targets) and
  any current manual-draft exposure remains non-public.
- VK API research evidence: official `messages.send` and `wall.post` docs do
  not document community Channel posting; any future transport must be verified
  in the actual Channels tab before public exposure is counted.
- Release evidence: deployed SHA and confirmation that the SHA is reachable from
  `origin/main`.

### Required evidence

- Tests:
  `tests/test_promo.py::test_promo_runner_sends_vk_channel_manual_draft_nonpublic`,
  `tests/test_promo.py::test_vk_channel_manual_draft_prefers_registration_link_over_telegraph`.
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
- Follow-up behavior (2026-06-25): while true VK community Channel API remains
  unavailable, the activity may send the prepared text to the operator's VK
  Messenger/Favorites as `VK_CHANNEL_DRAFT_SENT`. This is a manual-copy draft,
  not Channel delivery; it must remain non-public in campaign accounting.

## Follow-up Actions

- [ ] Find a documented or verified VK community Channel posting API. Do not use
  private endpoints for production unless the operational risk is explicitly
  accepted.
- [ ] If a transport is found, add an authenticated smoke that verifies the post
  in Messenger → "Каналы" before recording a public exposure.
- [ ] Decide with the operator whether the wrong Messenger/Favorites message
  should be deleted manually from VK.

## Release And Closure Evidence

- deployed SHA: `0de0fbe8` (`fix(promo): fail closed VK channel wrong surface (#10)`), reachable from `origin/main`.
- deploy path: manual `flyctl deploy -a events-bot-new-wngqia --remote-only --detach` from a clean detached `origin/main` worktree.
- Fly release: `v1480`, image `events-bot-new-wngqia:deployment-01KVYVQKQG6N2YY9NA93YH8QFS`, machine `683961db016e28` started.
- regression checks:
  - `python -m pytest -q tests/test_promo.py::test_promo_runner_skips_vk_channel_publish_until_real_channel_api tests/test_partner_promo_menu.py::test_campaign_card_keyboard_has_vk_channel_controls_for_80_campaign` — passed (`2 passed`).
  - Deployed code check: `VK_CHANNEL_SENT` is absent from `PUBLIC_PROMO_EXPOSURE_STATUSES`; initial `vk_channel_publish` activity seeds `enabled=False`; helper contains `vk_community_channel_post_api_unsupported`.
- production mitigation verification:
  - `promo_activity.id=30` has `enabled=0`.
  - `promo_exposure.id=514` has `publish_status='FAILED_WRONG_SURFACE'`, `public_target_count=0`, `public_targets_json=[]`.
  - Fly secrets list no longer includes `VK_AFISHA_CHANNEL_PEER_ID`.
- post-deploy verification: `/healthz` returned `ok=true`, `ready=true`, DB `ok`, scheduler `ok`, `promo_vk=ok`; Fly HTTP service check passing.
- VK API research evidence:
  - Official `https://dev.vk.com/ru/method/messages.send`, `https://dev.vk.com/ru/method/wall.post`, method index, object reference, and sitemap returned no `channel`/`канал`/`Пост в канал` documentation.
  - Authenticated harmless method probes for `channels.*`, `messages.*Channel*`, `wall.post*Channel*`, `newsfeed.getChannels`, and `groups.getChannels` returned VK error code `3` (`Unknown method passed`) via Open API.
  - VK mobile public JS exposes a frontend `ChannelsApi` only for `create/delete/join/leave/getMessagesById/getById/getRecommendations`; no channel post/send method was found in the downloaded channel/posting chunks. This is evidence of product surface/recommendations, not a verified posting API.
  - Public SMM guide confirms community Channels are created/published from the community UI (`Создать пост` → `Пост в канал`) and posts appear in Messenger → `Каналы`.

## Prevention

VK community Channel publishing is now treated as a distinct product surface,
not a synonym for Messenger. Any future implementation must prove the target
surface in the actual Channels tab before campaign reporting can count it as a
public promo exposure.
