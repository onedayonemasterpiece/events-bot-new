# INC-2026-06-26 Telegram story-message forward missing

Status: open
Severity: sev2
Service: CherryFlash / CrumpleVideo Telegram story and channel-feed fanout
Opened: 2026-06-26
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-15-cherryflash-caption-metadata`, `INC-2026-04-24-crumple-story-channel-boosts-required`, `INC-2026-04-26-crumple-story-required-channel-fanout`, `INC-2026-06-14-crumple-vk-transport-drift`
Related docs: `docs/features/cherryflash/README.md`, `docs/features/crumple-video/README.md`, `docs/operations/runtime-logs.md`

## Summary

CherryFlash had a Telegram channel-feed target implemented as a raw `telegram_chat`
MP4 upload to `@kenigevents`. The desired product contract is different: first
publish the video as a Telegram story, then post/forward that already-published
story into the channel body. CrumpleVideo needed the same channel-body
story-message forward after its required `@kenigevents` story repost.

## User / Business Impact

- `@kenigevents` subscribers saw a separate video post rather than a feed post
  whose media is the story itself.
- CherryFlash story publication and channel-feed publication could drift into two
  independent surfaces.
- CrumpleVideo had no channel-body story-message target, so it could publish
  stories without the requested feed forward.

## Detection

- Operator report on 2026-06-26: CherryFlash appears in the Telegram channel as
  a separate post; expected sequence is story publication followed by a forward
  from the story into the channel.
- Production runtime mirror was enabled and checked on 2026-06-26. It showed the
  current CherryFlash session `#759` had `story_targets_override` with a second
  `@kenigevents` target labeled `tg:@kenigevents:post`, `transport=telegram_chat`,
  `mode=upload`.

## Timeline

- 2026-06-26 07:44 UTC: scheduled CherryFlash session `#759` started.
- 2026-06-26 07:45 UTC: runtime log emitted story config targets including
  duplicated `@kenigevents` targets; production DB confirmed the second one was
  `transport=telegram_chat`.
- 2026-06-26: operator reported the target-shape mismatch.
- 2026-06-26: code change prepared to introduce `telegram_story_message` and use
  it for CherryFlash and CrumpleVideo.

## Root Cause

1. The prior caption incident added a `telegram_chat` target to create a visible
   `@kenigevents` channel post with numbered/date caption metadata.
2. `telegram_chat` intentionally calls Telethon `send_file()`, so it uploads the
   rendered MP4 as an independent channel post and does not depend on the story
   that was just published.
3. The shared story helper had story-to-story reposts (`repost_previous`) but no
   transport for “send previous story into a chat/channel feed” via
   `InputMediaStory`.
4. CrumpleVideo production config stopped at story targets (`me` ->
   `@kenigevents` -> `@lovekenig`) and did not include a channel-feed
   story-message step.

## Contributing Factors

- The earlier caption regression tests validated caption text on `telegram_chat`,
  not whether the channel feed media was a story reference.
- Existing docs used “Telegram channel post” language, which made a raw video
  post look acceptable even though the desired UX is story-first.

## Automation Contract

### Treat as regression guard when

- changing `popular_review` / CherryFlash `story_targets_override`;
- changing `VIDEO_ANNOUNCE_STORY_TARGETS_JSON` for CrumpleVideo;
- changing `video_announce/story_publish.py` transport parsing;
- changing `kaggle/CrumpleVideo/story_publish.py` Telegram transports or
  notebook-embedded helper code.

### Affected surfaces

- `video_announce/scenario.py` CherryFlash target generation;
- `video_announce/story_publish.py` server-side story target normalization;
- `kaggle/CrumpleVideo/story_publish.py` shared Kaggle story helper;
- `kaggle/CrumpleVideo/crumple_video.ipynb` embedded helper copy;
- `fly.toml` / `.env.example` CrumpleVideo production target JSON;
- public Telegram channel `@kenigevents` story and feed fanout.

### Mandatory checks before closure or deploy

- CherryFlash config must contain `@kenigevents` story upload followed by a
  `telegram_story_message` target, not `telegram_chat`, for the production
  channel-feed post.
- CrumpleVideo production target JSON must include required
  `telegram_story_message` after the `@kenigevents` story repost.
- Kaggle helper tests must prove `telegram_story_message` uses the previous
  successful story as `InputMediaStory` and calls `messages.SendMediaRequest`,
  not `send_file()`.
- Existing `telegram_chat` tests must remain for intentional test-post targets
  such as KОНБ/kenigsberg test channels.
- Run notebook build/sync check for the CrumpleVideo embedded helper.
- Post-deploy verification must inspect a live story report/log or public
  Telegram evidence showing the channel-feed post was produced from the story.

### Required evidence

- deployed SHA reachable from `origin/main`;
- focused pytest and notebook-build output;
- production config evidence for the CrumpleVideo target JSON;
- CherryFlash or CrumpleVideo story publish report/log showing
  `transport=telegram_story_message`, `source_story_id`, and `message_id`.

## Immediate Mitigation

No destructive repair was attempted during investigation. The incorrect target
shape was isolated to `telegram_chat` raw upload, and the prepared fix switches
future runs to story-message forwarding.

## Corrective Actions

- Add `telegram_story_message` transport to server-side target parsing and
  dedupe.
- Implement Kaggle-side `telegram_story_message` by sending
  `types.InputMediaStory(peer=<previous story peer>, id=<previous story id>)`
  through `functions.messages.SendMediaRequest` to the target channel/chat.
- Change CherryFlash `@kenigevents` channel-feed target from `telegram_chat` to
  required `telegram_story_message` after the primary story target.
- Add required `telegram_story_message` to CrumpleVideo production target JSON
  after the required `@kenigevents` story repost.
- Update feature docs and changelog.

## Follow-up Actions

- [ ] After deploy, collect live evidence from the next CherryFlash/CrumpleVideo
      run and close the incident if `telegram_story_message` succeeds.
- [ ] If Telegram rejects `InputMediaStory` into channel feed for story privacy
      or rights reasons, record the exact RPC error and decide whether a manual
      operator-forward fallback is acceptable.

## Release And Closure Evidence

- deployed SHA: `c1de6a82813659aef506fb2e637610604322f036`, pushed to `origin/main`.
- deploy path: manual `flyctl deploy -a events-bot-new-wngqia --config fly.toml --remote-only` from clean branch `fix/tg-story-forward-channel-20260626`; Fly release version `1492` initially deployed `458484546ddd93de1a1db9cfe8fe239f70ab89c1`, then the no-comment tightening was deployed as image `registry.fly.io/events-bot-new-wngqia:deployment-01KW1JHM7M8FNHTFQQ0CHFRRQD` from `c1de6a82813659aef506fb2e637610604322f036`.
- regression checks:
  - `.venv/bin/python -m pytest tests/test_kaggle_story_publish.py tests/test_video_announce_story_publish.py tests/test_crumple_build_notebook.py -q` -> `38 passed`;
  - `.venv/bin/python kaggle/CrumpleVideo/build_notebook.py`;
  - `.venv/bin/python -m py_compile video_announce/scenario.py video_announce/story_publish.py kaggle/CrumpleVideo/story_publish.py`;
  - `git diff --check`.
- post-deploy verification:
  - `curl https://events-bot-new-wngqia.fly.dev/healthz` returned `ok=true`, `ready=true`, no issues;
  - Fly SSH code/env probe confirmed `/app/video_announce/scenario.py` contains `telegram_story_message`, `/app/kaggle/CrumpleVideo/story_publish.py` contains `functions.messages.SendMediaRequest` and the empty `message=""` story-message send, and production `VIDEO_ANNOUNCE_STORY_TARGETS_JSON` contains required `tg:@kenigevents:story-message`.
- caveat: CherryFlash session `#759` started before this deploy and its persisted/dataset targets still contain the old `transport=telegram_chat` post target; the fix applies to new sessions unless `#759` is manually compensated after its story id is available.

- manual live verification on 2026-06-26:
  - because CherryFlash session `#759` started before the fix and still had the old `telegram_chat` target, an operator-scoped E2E Telethon account with `@kenigevents` admin rights was used for a manual proof;
  - source raw post: `https://t.me/kenigevents/4170`;
  - manual story upload produced `story_id=338` in `@kenigevents`;
  - `messages.SendMediaRequest(media=InputMediaStory(peer=@kenigevents, id=338), message="Видеоанонс #759 · 26 июня")` produced channel message `https://t.me/kenigevents/4172`;
  - verification showed message `4172` has `MessageMediaStory` with `message_media_story_id=338`, and story `338` has caption `Видеоанонс #759 · 26 июня`;
  - operator observation showed this commented story-message started a separate-looking view counter; the implementation was tightened to send `telegram_story_message` with `message=""` so it is a clean story share/forward and not a commented story-message;
  - CrumpleVideo production target timing was tightened to keep `tg:@kenigevents:story-message` 10 minutes (`delay_seconds=600`) after the successful `@kenigevents` story repost, with no comment/message;
  - the test artifact was removed: message `4172` and story `338` were deleted, and `@kenigevents` had no active stories immediately after cleanup.

## Prevention

Keep the story-first/feed-forward distinction explicit: `telegram_chat` is only
for intentional raw MP4 test posts; production CherryFlash/CrumpleVideo channel
feed posts must use `telegram_story_message` unless the product contract is
explicitly changed.
