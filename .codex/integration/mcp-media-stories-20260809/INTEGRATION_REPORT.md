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

## Integrated lanes

| Lane | Worker head | Integration commits | State |
|---|---|---|---|
| immutable media store | `237553503` | `5769d0a9e`, `ca5c46f5a` | merged |
| core fileParams/runtime | `092e82f5a` | `ec465ac3d`, `ace88d8d4`, `7aaa0bd57` | merged |
| Telegram media/stories | `674800a3e` | `0753c5449`, `528977e9a` | merged |
| VK media/stories | `11fcecf6d` | `352f25ff4`, `d561d1cba` | merged |
| docs/CI/smoke | `4de51fedb` | `6af619cec`, `b5a87f976` | merged |
| provider/runtime integration | integration branch | pending integration commit | validated locally |

## Delivery state

| Requirement | State |
|---|---|
| R01 image ingress/publication | Done in code: immutable JPEG/PNG/WebP ingress and provider materialization; video denied |
| R02 story publication | Done in code: Telegram/VK image story prepare/approval/commit/read-back |
| R03 story read | Done in code: bounded pages plus principal-bound 768px/64KiB MCP image preview |
| R04 aggregate story statistics | Done in code: official provider shapes, viewer identities excluded |
| R05 stable ChatGPT / Codex isolation | Done locally: no OAuth identity changes; Codex exact-seven regression passes |
| R06 merge/deploy/live acceptance | Pending independent exact-head review, PR/CI, merge and live canary |

## Integration validation

- Full private MCP suite after provider wiring and visual preview: `345 passed`
  with three unchanged aiohttp `NotAppKeyWarning` warnings.
- Compileall: passed for the MCP package, top-level Telegram/VK adapters,
  workspace providers/transports, tests, scripts and `main_part2.py`.
- Focused Ruff on all changed MCP/provider/smoke modules: passed.
- `git diff --check`: passed.
- `social_asset_preview` returns a standard MCP image content block containing
  only a stripped JPEG thumbnail; the structured result contains no provider
  URL, native ref, file id/name or local path.
- Production activation and provider calls have not occurred in this branch.
