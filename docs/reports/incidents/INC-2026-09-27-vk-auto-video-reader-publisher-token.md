# INC-2026-09-27: VK auto video reader used the publishing token

Status: `Not confirmed by user` — mitigation requires production release and observation.
Severity: sev2. Related: `INC-2026-09-24-vk-afisha-publication-recurrence`.

## Live evidence

The Fly production runtime mirror `/data/runtime_logs/events-bot.log*` recorded
113 `vk.actor=user method=video.get` calls from 26 September 18:45 through
27 September 17:20 UTC, including 22 in the 13:00 UTC hour. The running
`/app/vk_auto_queue.py` passed `_force_user_actor=True` at the recurring
video evidence refresh, bypassing the existing service-token read selection
in `main.vk_api`. The September 24 repair moved the Kaggle metrics reader
off the publishing token but missed this reader. The retained Fly window did
not contain a VK API code-9 response. VibePublish's separate token1 received
code 9 during its 27 September scheduled-post preflight; the exact provider
scope linking that restriction to Fly token4 traffic is unproved.

Other user-token calls in that Fly window were 46 `wall.get`, 21
`photos.getWallUploadServer`, 20 `utils.getShortLink`, 13
`photos.saveWallPhoto`, nine `wall.post`, nine `wall.getById`, and six
`wall.edit`. Those require separate call-site review; raw counts do not prove
that each is unnecessary. In the same period VibePublish had no VK operations
between 19 September 08:03 and 27 September 16:55 UTC.

## Mechanism and mitigation

The confirmed events-bot defect is an autonomous read path using a publishing
credential after the prior credential-separation fix. This creates avoidable
publisher-token traffic and can evade the intended service/read isolation.
The hotfix routes the video refresh through `VK_SERVICE_TOKEN` and rejects
video reads when that credential is unavailable or a user-actor override is
requested. The fetcher retains its original wall evidence when video refresh
fails. The service response may omit playable MP4, so preview evidence remains
but full video/audio coverage needs a separate nonpublishing reader.

The immediate VibePublish publication for 27 September 21:00 Kaliningrad was
independently confirmed in VK's native scheduled queue using token2. That
recovery does not establish the original VK anti-flood trigger.

## Verification gate

- Focused tests prove `video.get` uses only the service token and cannot
  silently fall back to the publishing token.
- Deploy a clean SHA reachable from `origin/main` using the manual Fly path.
- Observe a fresh VK auto video refresh in production logs with
  `vk.actor=service method=video.get`, no new user-actor video calls, and a
  healthy VK Afisha publisher lane.
- Monitor native event posts and code-9 errors; a healthy process alone does
  not prove publication continuity.
