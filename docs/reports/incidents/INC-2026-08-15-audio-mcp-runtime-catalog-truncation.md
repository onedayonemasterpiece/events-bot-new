# INC-2026-08-15-audio-mcp-runtime-catalog-truncation

Status: monitoring
Severity: sev1
Service: private eventsBot ChatGPT MCP
Opened: 2026-08-15
Closed: —
Owners: eventsBot MCP
Related incidents: —
Related docs: `docs/features/audio-transcription/README.md`, `docs/operations/release-governance.md`

## Summary

The production MCP server returned all three audio transcription tools and the
ChatGPT app settings displayed them after Refresh, but a newly opened ChatGPT
conversation materialized only the older bounded tool subset. The audio tools
had been appended at positions 28–30 of a 30-tool, 138,507-byte catalog, outside
the 26-tool runtime view reported by the affected conversation.

## User / Business Impact

- ChatGPT could not call `audio_transcription_start`,
  `audio_transcription_status`, or `audio_transcription_get`.
- The built-in ChatGPT transcription could read the attachment, but that did
  not exercise Telegram-native transcription, durable jobs, or source-relative
  and absolute timeline exports.
- Refreshing the existing MCP app and opening a new chat did not restore the
  production workflow.

## Detection

- The user reported the missing tools after app Refresh and supplied a settings
  screenshot showing `audio_transcription_get` with `telegram:publish` access.
- Production runtime logs showed an authenticated refresh sequence and a
  successful `tools/list` at 2026-08-15 11:15:05 UTC.
- An exact-scope production probe returned 30 tools, including all three audio
  tools, while the conversation reported only 26 callable tools.

## Timeline

- 2026-08-15 10:38 UTC — production process attached the private MCP in
  `read_plus_audio_transcription` mode.
- 2026-08-15 11:08–11:09 UTC — ChatGPT reauthorization retained the existing
  `telegram:publish` scope; no new `audio:transcribe` consent was required.
- 2026-08-15 11:15:05 UTC — ChatGPT Refresh performed authenticated
  `initialize`, `notifications/initialized`, and `tools/list` successfully.
- 2026-08-15 11:24 UTC — user confirmed that a new conversation still omitted
  all `audio_transcription_*` tools despite the settings page displaying them.
- 2026-08-15 11:31 UTC — exact-scope production probe confirmed 30 server tools,
  audio positions 28–30, and a 138,507-byte decoded catalog.

## Root Cause

1. Audio transcription registered its tools by appending them after the full
   private social workspace catalog.
2. The affected ChatGPT conversation materialized a bounded 26-tool runtime
   view even though app settings had scanned the complete 30-tool catalog.
3. Consequently all three appended audio tools fell outside the callable
   runtime subset. OAuth was not the blocker: the refreshed token held
   `telegram:publish`, and each audio descriptor advertised that same scope.

## Contributing Factors

- The pre-release production smoke validated the complete MCP `tools/list`
  response, not the ordering-sensitive ChatGPT conversation runtime view.
- The app settings view and the conversation runtime exposed different
  snapshots, so a successful Refresh looked like end-to-end acceptance.
- The server had no regression assertion that newly added workflow entry tools
  remain at the front of a large catalog.

## Automation Contract

### Treat as regression guard when

- adding or reordering private ChatGPT MCP tools;
- changing audio transcription discovery, OAuth scopes, or descriptors;
- changing the universal social workspace catalog size or order.

### Affected surfaces

- `audio_transcription/mcp.py` tool registration;
- private MCP `tools/list` ordering and catalog size;
- ChatGPT app Refresh and per-conversation tool materialization;
- existing `telegram:publish` OAuth compatibility.

### Mandatory checks before closure or deploy

- focused audio tests, including the discovery-order regression;
- full private MCP regression suite;
- existing remote Telegram session regression suite;
- exact-scope production `tools/list` returns all three audio tools first while
  retaining all existing tools;
- ChatGPT app Refresh followed by a new-chat live call reaches
  `audio_transcription_start` in production logs;
- health check is ready and the deployed SHA is reachable from `origin/main`.

### Required evidence

- PR, merge SHA, deployed SHA, and exact-main ancestry;
- test commands and pass counts;
- sanitized production list receipt with count, order, and no credentials;
- sanitized ChatGPT live-call receipt with an opaque `atr_*` job reference;
- post-deploy `/healthz` result.

## Immediate Mitigation

Prioritize the three audio tools ahead of the existing catalog without deleting
or renaming any existing tool and without changing OAuth consent.

## Corrective Actions

- Added a pure merge guard that rejects name conflicts, preserves every existing
  tool, and places the audio workflow at the beginning of discovery order.
- Added a regression test for the 30-tool catalog shape and audio-first prefix.

## Follow-up Actions

- [ ] Capture the first successful ChatGPT `audio_transcription_start` live call
  and close this record after the full job/result path passes.
- [ ] Evaluate a compact/dynamic MCP catalog if the social workspace continues
  to grow; do not rely indefinitely on client-specific runtime bounds.

## Release And Closure Evidence

- 2026-08-25 existing-connection regression: the deployed 30-tool production
  catalog serialized to 151,699 bytes and retained
  `audio_transcription_start`, `audio_transcription_status` and
  `audio_transcription_get` as positions 1–3. A private Telegram high-level
  read reached the same audio service, returned one ready result among 12 voice
  attachments, and reported cache hits for all 12 on the repeat read without
  logout, reauthorization or manual reconnect. This proves the new social-read
  ingress and result path but does not replace the still-pending standalone
  ChatGPT `audio_transcription_start` acceptance required to close this older
  incident.

- 2026-08-25 integration regression: Telegram high-level read enrichment reuses
  the existing audio service internally without adding a top-level tool; the
  standalone start/status/get descriptors remain the discovery prefix in their
  original order. Focused catalog, private MCP, audio and remote-session tests
  are required again before release; production/live evidence remains pending.

- deployed SHA: `5e86d87583875b240f3cdbac9f198e6742e4b6c0`, reachable
  from `origin/main`
- deploy path: `scripts/deploy_fly_main.sh` from a clean detached worktree at
  exact `origin/main`; PR #510, all three required GitHub checks passed
- regression checks: focused audio `20 passed`; private MCP `454 passed`;
  remote-session `7 passed`; package compileall passed
- post-deploy verification: `/healthz` ready with zero issues; in-container
  immutable SHA matched; exact-scope production `tools/list` retained all 30
  tools and returned start/status/get as positions 1–3 with
  `telegram:publish`. The final real ChatGPT start/job/result acceptance remains
  pending before incident closure.

## Prevention

New workflow entry tools must be checked not only for presence in the complete
MCP response but also for placement in the bounded discovery prefix exercised by
the real ChatGPT conversation runtime.
