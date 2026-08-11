# Task for coding agent: Telegram document sending through private Events MCP

## Repository and branch

Repository: `onedayonemasterpiece/events-bot-new`

Create a fresh linked worktree from current `origin/main` and branch:

```text
feature/private-mcp-telegram-document-send
```

Read and obey `AGENTS.md`, `CODEX.md`, `docs/README.md`, `docs/routes.yml`, `docs/operations/private-events-mcp.md`, and `docs/operations/repository-workflow.md` before editing. Record the actual base SHA in the PR. The production MCP snapshot observed during design reported `55d06e4a0d2622c770093acec379abd7f2fbbddf`; do not assume that is still `origin/main`.

## Goal

Implement **Telegram message document v1** end to end so an explicit ChatGPT request can attach an actual file, including a structurally valid APK, to a Telegram `send_message` action and deliver it to Saved Messages through the existing private Events MCP.

Use the attached `architecture.md`, `acceptance-matrix.md`, and prototype `proposed/private_events_mcp/document_policy.py` as the normative design. Adapt the prototype to repository conventions; do not copy it blindly if the current immutable asset model already provides equivalent safeguards.

## Fixed product scope

Implement only:

- Telegram;
- `SocialAction.SEND_MESSAGE`;
- any resolved target already permitted to receive `send_message`, with Saved Messages as mandatory live case;
- exactly one `MediaRole.DOCUMENT` attachment plus optional caption/rich entities;
- ChatGPT `fileParams` ingress;
- default allowlist: APK, PDF, ZIP, UTF-8 TXT/MD/CSV/JSON, DOCX/XLSX/PPTX;
- default max document size 48 MiB, hard cap 64 MiB;
- new fail-closed kill switch, disabled by default.

Do not add VK documents, publish/schedule/comment/edit/forward document support, multiple files, mixed media, audio/video/animation, arbitrary URL/path/base64 ingress, raw Telethon arguments, generic opaque binaries, antivirus claims, or production activation.

## Required implementation

1. **Separate common asset ingress from image story.**
   Add `PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_FILE_SEND_ENABLED` and derive `asset_ingress_enabled = media_story_enabled or file_send_enabled`. Parse/validate media root, allowed hosts, store size, TTL and download timeout under that derived gate. Add `PRIVATE_EVENTS_MCP_DOCUMENT_MAX_ASSET_BYTES` with default 48 MiB and hard maximum 64 MiB. Preserve disabled-startup inertness and fail closed on invalid enabled configuration.

2. **Add an independent document policy.**
   Do not weaken the existing image validator. Validate regular immutable files, size, detected bytes, SHA-256, safe bounded display filename, MIME consistency, and bounded ZIP/APK/Office inventory without extraction. APK classification requires Android structure; a `.apk` ordinary ZIP must fail. Do not accept unrestricted `application/octet-stream`; it may only be an incoming hint when bytes classify to an allowed type.

3. **Extend asset contracts.**
   Allow `role=document` in the base schema and validator only for Telegram. Accept syntactically valid `text/*` hints. Extend verified asset/manifests with role, sanitized display name and classification; dimensions remain image-only. Never persist/return signed URL, original file ID, unsanitized name, internal path or native provider identifiers.

4. **Make tools and scopes role-aware.**
   Expose stage/status when either story/image or file-send ingress is enabled; preview/stories remain story-only. Document staging requires the existing Telegram `send_message` write scope. Replace the blanket “media requires media_story” rule with role/action/provider checks. Final prepare and commit must re-authorize exact action/target.

5. **Project capabilities correctly.**
   Advertise `document` only when file send is enabled, provider is Telegram, asset ingress is available, and the exact target supports `send_message`. Do not advertise it for VK or read-only/publish-only targets.

6. **Enforce prepare invariants and digest binding.**
   Telegram + `send_message` only; exactly one document; no mixed media; ready/unexpired/principal-bound/provider-bound asset; re-open and re-hash bytes. Bind role, SHA-256, size, detected MIME, sanitized display name and expiry into `verified_assets` before computing the action digest and human preview.

7. **Implement Telegram delivery.**
   Stage a closed binding without uploading. At commit, reopen/revalidate immutable bytes and make exactly one Telethon 1.44 `send_file` attempt with forced document semantics, sanitized filename, optional caption and existing rich entities. Keep provider-specific kwargs internal. Timeout remains `outcome_unknown`, `retry_safe=false`; no blind resend. Read after write and verify target/message/document plus filename/size when available.

8. **Tests and documentation.**
   Implement every applicable row in `acceptance-matrix.md`. At minimum update:
   - `tests/test_private_events_mcp_social_asset_ingress.py`
   - `tests/test_private_events_mcp_social_workspace_contract.py`
   - `tests/test_private_events_mcp_social_workspace_runtime.py`
   - `tests/test_private_events_mcp_telegram_workspace.py`
   - `tests/test_private_events_mcp_workspace_providers.py`
   - `tests/test_private_events_mcp_server.py`
   - `.env.example`
   - `docs/operations/private-events-mcp.md`
   - `CHANGELOG.md` `[Unreleased]`

   Add a bounded document smoke mode/script. Keep CI fixtures synthetic and small.

## Required verification

Run the focused MCP suite, then all repository-required checks for touched surfaces. Preserve exact command output in the PR description or a non-committed artifact. Add mutation/negative controls proving that tests fail when each of these guards is removed:

- document role restriction;
- action restriction;
- principal/provider binding;
- TTL/digest recheck;
- filename sanitization;
- feature kill switch;
- one-attempt timeout behavior;
- no URL/path/native-ID leakage.

Perform a real live acceptance only if the connector/runtime credentials and an isolated operator configuration are available without changing production defaults:

1. generate a deterministic tiny APK-shaped ZIP fixture;
2. stage it through actual ChatGPT fileParams;
3. resolve Saved Messages;
4. prepare/commit one `send_message` with caption;
5. verify the returned read-after-write evidence and the downloadable Telegram document;
6. remove the test message using the existing typed delete flow where permitted.

If live ChatGPT fileParams cannot be exercised from the coding environment, provide a fully reproducible acceptance script and mark only that external acceptance as pending. Do not substitute a direct filesystem call and call it end-to-end.

## Git and delivery

- Keep the worktree isolated and clean.
- Commit only task-related files.
- Push the branch.
- Open a Draft PR with base/head SHA, design summary, changed contracts, exact tests/results, security negative controls, rollout/rollback, and any genuinely unresolved external acceptance.
- Do not merge, deploy, or enable the production flag.
- Do not stop for intermediate confirmation unless blocked by a missing permission/secret or an ambiguity that changes the fixed scope above.

## Definition of done

The PR is ready when the feature is disabled by default, all offline tests pass, existing image/story/text/VK behavior regresses cleanly, and the implementation can deliver one actual APK fixture to Telegram Saved Messages through the normal MCP stage → prepare → commit path with one provider attempt and sanitized evidence.
