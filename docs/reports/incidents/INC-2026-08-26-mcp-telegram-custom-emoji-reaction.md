# INC-2026-08-26 Telegram MCP could not mark ideas added to GitHub

Status: mitigating — implementation and local provider evidence complete; exact-main deployment and refreshed-ChatGPT canary pending
Severity: sev2
Service: private eventsBot ChatGPT MCP / Telegram Social Workspace reactions
Opened: 2026-08-26
Closed: —
Owners: eventsBot MCP / ChatGPT workspace administrator
Related incidents: `INC-2026-08-25-chatgpt-frozen-mcp-actions`
Related docs: `docs/operations/private-events-mcp.md`, `docs/operations/release-governance.md`, [Telegram message reactions](https://core.telegram.org/api/reactions)

## Summary

After GitHub idea intake, ChatGPT tried to mark processed Telegram messages
with ordinary Unicode check marks. The production adapter always compiled the
input as `ReactionEmoji`; Telegram rejected those values for this chat and the
post-attempt secrecy boundary correctly returned `outcome_unknown`. The user
selected a specific GitHub custom emoji instead, but the MCP contract had no
provider-neutral way to request a `ReactionCustomEmoji` and reaction readback
collapsed every custom emoji to the generic string `custom`.

The remediation adds one closed semantic option, `github_added`, with the
reviewed custom-emoji document ID stored only in server configuration. It does
not expose a native document selector and does not automatically mutate a
message merely because it was read or transcribed.

## User / Business Impact

- Processed ideas could not be durably distinguished in Telegram from ideas
  still awaiting GitHub intake.
- Repeated check-mark attempts produced ambiguous `outcome_unknown` receipts,
  increasing the risk of duplicate mutations or lost processing position.
- Even a future successful custom reaction would have appeared only as
  `custom` in MCP readback, preventing semantic reconciliation.

## Detection

- The user supplied the failed ChatGPT action report and the Telegram custom
  emoji picker with the intended GitHub icon selected.
- Production durable audit/operation state showed six Telegram reaction
  operations on 2026-08-26 between 10:33 and 11:49 UTC, all ending
  `outcome_unknown`; the first two retained the sanitized `provider_error`
  classification. Bounded `list_reactions` checks did not observe the requested
  check marks.
- The production runtime file mirror was enabled with 48-hour retention and
  healthy rotated files; the provider-specific exception text was deliberately
  absent from file logs at this secrecy boundary.

## Timeline

- 2026-08-26 10:33–11:49 UTC — ChatGPT prepared and committed six ordinary
  reaction attempts; durable operations remained `outcome_unknown` and
  readback did not confirm the mark.
- 2026-08-26 12:39 UTC — a role-safe local Telethon search for `github`
  returned the same ordered 23-icon picker result; visual matching identified
  the user-selected static GitHub icon.
- 2026-08-26 12:45 UTC — read-only Telethon inspection confirmed the authorized
  target group allows custom reactions and the local human account is Premium.
- 2026-08-26 12:47 UTC — production file-mirror and durable operation/audit
  evidence confirmed the failure window and sanitized classification.
- 2026-08-26 13:00 UTC — failing contract, adapter and configuration regressions
  were added before implementation; the focused MCP suite then passed.

## Root Cause

1. The public action contract offered only a free-form `reaction` string.
2. `_DefaultTelethonTypes.request("reaction")` unconditionally constructed
   `types.ReactionEmoji`, which represents only supported standard reactions.
3. The selected GitHub icon is a Telegram custom emoji and requires
   `types.ReactionCustomEmoji(document_id=...)`.
4. Accepting that numeric document ID directly from ChatGPT would violate the
   provider-neutral opaque-boundary contract, so the missing abstraction was a
   closed server-bound semantic preset rather than a wider native-ID field.
5. Readback recognized only `reaction.emoticon`; every custom-emoji reaction
   therefore degraded to `custom` and could not confirm the business meaning.

## Contributing Factors

- A visually check-mark-like Unicode character is not necessarily an enabled
  Telegram standard reaction for a particular chat.
- Provider errors after the mutation boundary are intentionally redacted and
  treated as unknown outcomes, which is safe but makes an unsupported reaction
  look less specific to ChatGPT.
- The earlier action-refresh incident means a server schema change alone does
  not make the new option visible in an already approved ChatGPT app snapshot.

## Automation Contract

### Treat as regression guard when

- changing Social Workspace reaction inputs or Telegram reaction transport;
- changing the configured GitHub marker or reaction readback normalization;
- publishing refreshed ChatGPT MCP action definitions.

### Affected surfaces

- `private_events_mcp/social_workspace.py` closed action contract;
- `private_events_mcp_telegram_adapter.py` Telethon translation/readback;
- `private_events_mcp/config.py` and production environment binding;
- ChatGPT action-definition refresh/publication.

### Required checks

- contract accepts Telegram `reaction_preset=github_added`;
- validator rejects both `reaction` and `reaction_preset`, neither field, an
  unknown preset, or the preset on VK;
- adapter constructs `ReactionCustomEmoji` from the server binding and never
  from a caller-supplied numeric ID;
- `list_reactions` returns `github_added` for the configured document;
- ordinary standard emoji reaction behavior remains unchanged;
- reading/transcribing an item never adds a reaction or marks media played;
- production exact-main health/SHA pass, ChatGPT actions are refreshed and a
  live controlled reaction is confirmed by readback.

## Remediation

1. Add the closed `SocialReactionPreset.GITHUB_ADDED` contract.
2. Keep the selected native document ID only in strict server configuration.
3. Compile the preset with Telethon `ReactionCustomEmoji` while retaining
   `ReactionEmoji` for ordinary supported reactions.
4. Project the configured custom reaction back to `github_added` in reads.
5. Refresh/publish the ChatGPT action definition and run a controlled live
   mutation/readback canary after exact-main deployment.

## Rollback

Remove the production document-ID setting and roll back the exact-main release.
The preset then fails closed as unconfigured; existing ordinary reaction,
read/transcription and GitHub operations remain available. Do not substitute a
different custom-emoji ID without operator review.

## Closure Evidence

- Pending: merged exact-main SHA and Fly release receipt.
- Pending: production health and immutable in-image SHA.
- Pending: refreshed/published ChatGPT action snapshot.
- Pending: successful live `github_added` mutation plus semantic
  `list_reactions` readback.
