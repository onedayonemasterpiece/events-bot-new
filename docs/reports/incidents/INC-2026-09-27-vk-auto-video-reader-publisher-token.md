# INC-2026-09-27: VK auto video reader used the publishing token

Status: `Mitigated in production; observing publication continuity`.
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

The first hotfix was merged as `f65558f225c279fb8b5ceae7e40bf48110f56e13`
and deployed to Fly. A live read of a known auto-queue source post recorded
`vk.actor=service` for both `wall.getById` and `video.get`, and the video
refresh logged `actor=service`; no user-actor video calls appeared after the
release in the observed window. `/healthz` remained ready with no issues and
the volume remained 3 GiB.

## Text-only event publication found during live verification

At 16:20 UTC, the Afisha wall contained managed event post
`wall-231920894_11514` for event 9041 with zero attachments. The production
event row had zero photos and the sync log recorded `no_illustration`, but
`vk_sync` still created the post. The previous missing-media guard only
covered Telegram-origin events, known unmaterialized posters, or failed
uploads when photo candidates existed. It did not cover an event with no
candidate photo. This violates the owner rule that no photo means no VK event
publication. A follow-up patch requires a successful attachment before a new
Afisha event post and classifies `vk_sync_missing_media` for the once-daily
media check. Existing text-only posts are recorded as evidence; the patch
does not silently delete historical wall posts.

## Verification gate

- Focused tests prove `video.get` uses only the service token and cannot
  silently fall back to the publishing token.
- Deploy the follow-up patch from a clean SHA reachable from `origin/main` using
  the manual Fly path.
- Observe a fresh VK auto video refresh in production logs with
  `vk.actor=service method=video.get`, no new user-actor video calls, and a
  healthy VK Afisha publisher lane.
- Monitor native event posts and code-9 errors; a healthy process alone does
  not prove publication continuity.
