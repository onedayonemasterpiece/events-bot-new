# RESULTS — vk_official_mapping

- Requirement: R01
- Mode: read-only discovery
- Status: completed

## Evidence

The safe exact mappings for managed VK community `231828790` are:

1. `event.vk_repost_url` written after a successful single-event repost/post.
2. `promo_exposure` with `surface='vk_repost'` and
   `publish_status='PUBLISHED_MAIN'`, using the exact wall URL from
   `details_json.target_url` or `public_targets_json[].url`.

Daily digests, multi-event videos, stories, festival aggregate posts and polls
have no one-post-to-one-event counter contract and must be excluded.

Production discovery on 2026-07-17 found 36 historical exact promo reposts for
17 events, but zero active/future exact mappings at that instant. Therefore the
first active-only run could correctly contain zero official-group targets; a
bounded rolling-retention backfill is safe and avoids scanning the full wall.
