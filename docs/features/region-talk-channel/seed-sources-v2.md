# Seed sources v2 — Region Talk Channel

Status: MVP-1.x expansion seed for dry-run discovery. Machine-readable file: [`seed-sources-v2.csv`](seed-sources-v2.csv).

Purpose: move beyond the 30-row v1 probe toward a broader source frontier without automatically monitoring every catalog hit.

## Counts and composition

- 30 v1 rows promoted for continuity.
- 150 Telegram travel/blogger/route/photo/Russia-travel catalog/profile-probe candidates.
- 120 VK public/group wall discovery candidates.
- 30 web/catalog hubs for link-to-link discovery.

Rows with `monitoring_enabled=false` are **not** monitored automatically. They must pass source profile probe first.

## VK priority

1. VK public/group wall posts.
2. VK wall posts with text + photo albums.
3. VK wall posts with short video + meaningful text.
4. VK comments only for source-link discovery.
5. VK Video is auxiliary only and may help identify cross-platform authors, but it is not the core target.

## Extra v2 columns

`profile_probe_enabled`, `comments_discovery_enabled`, `forward_repost_edge_enabled`, `seed_batch`, `source_profile_url`, `profile_probe_status`.

## Safety

- No Telegram/VK publishing.
- No channel/community creation.
- Comments are only source-discovery evidence and must be redacted.
- Source candidates discovered from links/forwards/reposts are frontier rows, not monitored sources until probed/manual accepted.
