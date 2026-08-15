# Final live-integration task for the coding agent

Status: completed on 2026-08-15. The default-off canary below passed and its
sanitized receipt is attached to PR #505. The user then explicitly authorized
the subsequent exact-main production rollout; current operational status is
recorded in `README.md`. This file is retained as the historical canary
contract, not as a current instruction to disable the production feature.

The implementation and repository integration are already complete. Do not redesign the architecture, rename the MCP tools, add another ASR, or reuse an existing Telegram session.

Perform only the live canary closure:

1. Create a dedicated Premium-capable Telethon `StringSession` and store it as `TELEGRAM_AUTH_BUNDLE_TRANSCRIPTION`; do not use E2E, S22, STORY, monitoring, or editor sessions.
2. Keep `PRIVATE_EVENTS_MCP_AUDIO_TRANSCRIPTION_ENABLED=0` in steady state. Enable it only for the canary environment, grant/re-consent the `audio:transcribe` OAuth scope, and submit one short Russian `m4a` with independently known recording start time.
3. Verify the exact `atr_*` lifecycle, one guarded Kaggle CPU run, Telegram-native text, absolute-time arithmetic, five export hashes, aggregate temporary-message cleanup with zero failures, registry removal, and deletion of both private datasets.
4. Run the focused audio tests plus existing private-MCP and remote-session tests. Change code only for a concrete live compatibility failure and preserve all security boundaries documented in `README.md`.
5. Attach a sanitized canary receipt to the Draft PR and leave the feature disabled. Do not deploy unrelated changes, merge, or enable automatic batch ingestion.
