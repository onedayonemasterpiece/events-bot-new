# Telegram document-send requirement matrix

Base: `dcd51b7a1dc50eacdaffe5401a808d2c4285eec0` (`origin/main` at start).
Baseline: compileall PASS; `tests/test_private_events_mcp_*.py` = 395 passed, 3 existing aiohttp warnings.

| ID | Requirement | Primary lane | Dependencies | Done when |
|---|---|---|---|---|
| R01 | Existing ChatGPT `eventsBot` accepts a real fileParams upload | release_live_acceptance | R02-R14 | Actual ChatGPT UI stage succeeds without identity replacement |
| R02 | `social_asset_stage(telegram, document)` | core_document_runtime | R03-R04 | Dynamic closed schema and runtime stage produce ready `ast_*` |
| R03 | Immutable validation, type/size/SHA/name and prepare/commit recheck | document_policy_store | none | Store plus core reverify and adversarial mutation tests pass |
| R04 | APK/PDF/ZIP/UTF-8/Office allowlist | document_policy_store | none | Structural positive/negative matrix passes without extraction/execution |
| R05 | Telegram `send_message`, one document, no mixed media | core_document_runtime | R03-R04 | Contract/runtime reject every other action/provider/cardinality |
| R06 | Typed stage -> prepare -> commit | core_document_runtime | R02-R05 | Digest/preview/approval/idempotency integrate with verified asset |
| R07 | Exactly one forced-document `send_file` with name/caption/entities | telegram_document_provider | R03-R04 | Telethon 1.44 call-shape/count tests pass |
| R08 | Read-after-write verification | telegram_document_provider | R07 | Message/document/name/size checked; ambiguity is non-retryable unknown |
| R09 | No URL/file-id/path/original-name/access-hash/native-ID leakage | document_policy_store | none | Sentinel tests across output/log/audit/store pass |
| R10 | Separate default-off fail-closed flag | core_document_runtime | R02 | Config, catalogue and post-prepare kill-switch tests pass |
| R11 | Existing text/image/story/TG/VK/OAuth/approval/idempotency unchanged | core_document_runtime | all code lanes | Full private MCP and repo-required gates pass |
| R12 | Canonical docs/env/changelog/smoke synchronized | docs_smoke | R02-R11 | Runbook, E2E index, env, changelog and bounded smoke updated |
| R13 | Full tests/GitHub Actions and negative/mutation controls | exact_head_review | R02-R12 | Exact immutable head passes all local and required CI gates |
| R14 | Independent exact-head audit | exact_head_review | R13 | Read-only reviewer approves the exact PR head |
| R15 | PR ready, merged normally, exact current main deployed | release_live_acceptance | R14 | PR merged and Fly/in-container SHA equals origin/main |
| R16 | Scoped prod config, in-place ChatGPT Refresh/new chat | release_live_acceptance | R15 | Stable OAuth identity preserved and new catalogue observed |
| R17 | Real ChatGPT APK -> Saved Messages live acceptance | release_live_acceptance | R16 | Downloadable document, preview equality, one attempt, negative probe |
| R18 | File-send rollback probe, re-enable, and canary cleanup state | release_live_acceptance | R17 | Capability disappears/reappears without regression; cleanup recorded |
