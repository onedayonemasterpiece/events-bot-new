# Provider adapters lane results

## Scope

- Lane ID: `provider_adapters`
- Requirement IDs: generic Telegram read/publish adapter; generic VK read/publish adapter; enabled-only `create_app()` injection; provider environment/documentation/changelog; fake-only unit validation.
- Base SHA: `7d35b5d94355b1041749f2b68ba5357eb191ca96`
- Head SHA at implementation evidence capture: `679b240941606443ba836425fd9a77adbbf16e98`
- Branch: `agent/mcp-multiclient/provider-adapters`

## Outcome

Implemented a provider module outside the MCP core with:

- a lazy Telegram adapter that accepts only `TELEGRAM_AUTH_BUNDLE_EVENTS_BOT_MCP`, uses the existing Telegram API ID/hash names or TG aliases, serializes session use, creates a per-call authorized Telethon human client, and disconnects in `finally`;
- bounded Telegram recent-message scans and new plain-text `send_message` calls with `parse_mode=None` and link previews disabled;
- a fixed-method VK wrapper over injected `main.vk_api`, limited to `wall.get` and `wall.post`, with exact owner/community flags and deterministic SHA-256 `guid` values;
- generic/redacted `SocialAdapterError` boundaries and secret-free adapter `repr` values;
- enabled-only adapter construction in `create_app()`, retaining strict disabled MCP behavior without provider credential parsing or Telethon import;
- canonical environment, operations documentation, and changelog updates.

No bot-token publication, event/outbox/Telegraph/card/media path, raw provider method, edit/delete/forward, MAX, live read, or live send was added or invoked.

## Evidence and commands

- `date -u -d @1786190400` — fixture timestamp verification.
- Official VKCOM schema probe against `https://raw.githubusercontent.com/VKCOM/vk-api-schema/master/wall/methods.json` — confirmed `wall.post.guid` is a string and the fixed owner/from-group/signed/message parameters are present.
- `PYTHONPATH=. /home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q tests/test_private_events_mcp_provider_adapters.py` — **11 passed**.
- `PYTHONPATH=. /home/dev/.codex/venvs/events-bot-new/bin/python -m compileall -q private_events_mcp private_events_mcp_provider_adapters.py tests/test_private_events_mcp_provider_adapters.py main_part2.py` — **passed**.
- `PYTHONPATH=. /home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q tests/test_private_events_mcp_*.py` — **63 passed**, with three pre-existing aiohttp `NotAppKeyWarning` warnings exercised by the new disabled-`create_app` regression test.
- `git diff --check` and `git diff --cached --check` — **passed**.
- Negative source scan confirmed the provider module contains no event publication function or `event_id` path and exposes only literal `wall.get` / `wall.post` VK calls.

## Test coverage

- dedicated Telegram role bundle decoding and optional device metadata;
- absence of E2E, generic session, or S22 fallback;
- Telegram fixed numeric target, bounded scan window, blank skipping, output limit, plain-text send parameters, serialization, authorization checks, and disconnect after provider failures;
- VK fixed methods/parameters, negative group owner, blank skipping, bounded scan/output, stable timestamps, deterministic 64-hex `guid`, and defensive response handling;
- provider exception/target/credential redaction and secret-free adapter representations;
- lazy adapter construction and disabled `create_app()` semantics with malformed provider-only configuration.

## Risks

- No live provider operation was allowed in this lane; production rights/session validity and VK token permissions remain activation-gate checks.
- The Telegram session bundle is intentionally fail-closed and has no recovery fallback. Missing/invalid/unauthorized dedicated credentials yield only a generic provider error.
- The existing `main.vk_api` selects and throttles runtime VK credentials; adapter tests use only injected fakes and do not validate a production actor token.

## Changed files

- `.env.example`
- `CHANGELOG.md`
- `docs/operations/private-events-mcp.md`
- `main_part2.py`
- `private_events_mcp_provider_adapters.py`
- `tests/test_private_events_mcp_provider_adapters.py`
- `.codex/lanes/provider_adapters/RESULTS.md`
