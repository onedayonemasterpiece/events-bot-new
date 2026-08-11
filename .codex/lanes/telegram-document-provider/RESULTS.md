# Telegram document provider lane results

## Scope

- Lane: `telegram-document-provider`
- Requirement IDs: `R07`, `R08`
- Base SHA: `dcd51b7a1dc50eacdaffe5401a808d2c4285eec0`
- Implementation head tested: `3260a40dbb603ca56f095ad5d9f88eb6de5f61cd`
- Branch: `agent/private-mcp-document-send/telegram-provider`

## Outcome and evidence

Implemented the closed Telegram document path while preserving the existing text,
image, and story paths:

- document staging validates the frozen immutable metadata contract and performs no
  Telegram/client/asset-reader I/O;
- only `telegram/send_message` to an existing Saved Messages or user-DM target may
  carry exactly one document; publish, multiple, mixed, and group cases fail before
  a provider mutation;
- commit reopens the owner-bound asset, checks expiry, byte length, and SHA-256, then
  marks the attempt immediately before exactly one `send_file` call;
- `send_file` receives one named `BytesIO` (not a list), `force_document=True`, the
  verified MIME/size, exactly one `DocumentAttributeFilename`, caption/entities,
  and `parse_mode=None`; no separate `upload_file` occurs;
- read-after-write checks the message ID and scoped target, requires a document, and
  compares filename/size when the provider reports them;
- timeout and read-back mismatch persist `outcome_unknown`, `retry_safe=false`, and
  replay the durable result without a second attempt;
- exact-target capability output includes `document` only where the existing
  `send_message` action is present.

## Verification commands

- `/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q tests/test_private_events_mcp_telegram_workspace.py -k 'document'`
  - `15 passed, 45 deselected`
- `/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q tests/test_private_events_mcp_telegram_workspace.py tests/test_private_events_mcp_telegram_media_stories.py tests/test_private_events_mcp_workspace_providers.py`
  - `80 passed`
- `/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q tests/test_private_events_mcp_*.py`
  - `410 passed, 3 warnings` (pre-existing aiohttp `NotAppKeyWarning` sites)
- `/home/dev/.codex/venvs/events-bot-new/bin/python -m compileall -q private_events_mcp private_events_mcp*.py tests scripts main_part2.py`
  - PASS
- `/home/dev/.cache/uv/archive-v0/klptK945vMs2Ma60/bin/ruff check private_events_mcp_telegram_adapter.py tests/test_private_events_mcp_telegram_workspace.py`
  - `All checks passed!`
- `git diff --check`
  - PASS

The installed project venv was also inspected directly and reports Telethon `1.44.0`;
its `send_file` signature contains the required `force_document`, `mime_type`,
`file_size`, `attributes`, `formatting_entities`, and `parse_mode` parameters.

## Changed files

- `private_events_mcp_telegram_adapter.py`
- `tests/test_private_events_mcp_telegram_workspace.py`
- `.codex/lanes/telegram-document-provider/RESULTS.md`

## Integration notes and risks

- The integration lane must merge the document policy/store contract first so
  provider inputs contain mandatory `role`, `display_name`, and `classification`.
- The adapter deliberately retains the existing `asset_reader.open_verified`
  byte-source boundary; the provider-builder integration must bridge the policy
  store's `reverify(...)` result to this closed reader contract.
- The adapter enforces the 64 MiB hard document ceiling; the core/config lane owns
  the lower configured default (48 MiB) and kill switch.
- No config, runtime, workspace-provider, docs, changelog, deployment, or live
  Telegram state was changed in this lane.
