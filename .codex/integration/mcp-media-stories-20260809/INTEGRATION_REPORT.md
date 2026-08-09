# Private Events MCP media/stories integration

- Integration branch: `integration/mcp-media-stories-20260809`
- Initial base: `origin/main` `80f7bc6c31125abba67575dc94d0fa2b730db247`
- Existing ChatGPT OAuth identity: must remain unchanged.
- Codex contract: exactly seven evidence tools; no social scopes or provider calls.

## Baseline

- `compileall`: passed for current MCP package/scripts/adapters.
- Private MCP baseline suite: `257 passed` (3 existing aiohttp warnings).
- The sparse checkout was expanded with the application modules imported by
  `main.create_app()` (`preview_3d` and `serverless`), so the baseline now matches
  the exact merged-main result instead of relying on a sparse-checkout exception.

## Confirmed pre-existing blockers

1. Configuration unconditionally rejects the media/story feature flag.
2. `social_asset_stage` trusts a caller-declared upload handle and never receives
   or verifies bytes.
3. Telegram and VK tests inject fake provider-native media bindings that production
   cannot create.
4. Telegram story items are later treated as ordinary messages for statistics.
5. VK story request/response fakes do not match official API 5.199 shapes.

## Delivery state

| Requirement | State |
|---|---|
| R01 image ingress/publication | In progress |
| R02 story publication | In progress |
| R03 story read | In progress |
| R04 aggregate story statistics | In progress |
| R05 stable ChatGPT / Codex isolation | Preserved by design; final regression pending |
| R06 merge/deploy/live acceptance | Pending |
