# Lane Results: mcp-universal-social-vk

## Scope

- Lane ID: `mcp-universal-social-vk`
- Base SHA: `dd388c4b71d7bb86dbac94e4dbe19347a5ca7e2b`
- Initial implementation SHA: `dec112d4e597c71264797858469e9990c3f11234`
- Review-fix implementation HEAD SHA: `43f907627c0d0b98f6ba6fb590a46c1e49805f79`
- Writable files: `private_events_mcp_vk_adapter.py`, `tests/test_private_events_mcp_vk_workspace.py`, this result file.
- Production calls, credentials, deploys, shared/core/docs/config/integration edits: none.

## Delivered

- Added `VKWorkspaceAdapter` with the stable async `capabilities`, `resolve`, `read`, `execute`, and `reconcile` surface.
- Requires an injected dedicated role-scoped actor transport, opaque-reference store, call governor, captcha/cooldown hook, and sanitation hook; there is no environment/main-token/runtime fallback.
- Pins VK API `5.199`; private fixed call compiler enforces per-operation actor/capability, method allowlist, required keys, and allowed parameter keys.
- Covers exact self/user/community resolution; wall, dialog/conversation, comment, reaction, story, audience, post/story/community stats; bounded wall/newsfeed/community search; deterministic related-community discovery; and 100-item editorial pagination in pages of at most 25.
- Editorial sampling loads community title/description/activity/site/member metadata and excludes ads, reposts, and pinned entries from the schema-compatible owner-post sample.
- Covers exact-user DM with deterministic `random_id` and read-after-write verification; wall publishing/scheduling, comments/reactions, edits/deletes, repost/message-forward analogue, typed media/photo-album/video bindings, and story save.
- Provider content is recursively sanitized and marked untrusted; public results contain opaque refs rather than native IDs. Provider errors are collapsed to stable codes. Captcha invokes cooldown, and action timeouts become non-retryable `outcome_unknown` without blind retry.

## Independent review fixes

- Full canonical `compute_action_digest()` now binds the local idempotency claim. Exact replay returns the original receipt, while the same key with different target/item/destination/content/entities/media/reaction/schedule/revision is rejected before any provider call.
- Exact concurrent action replays are serialized across claim, provider call, and receipt storage. A regression covers concurrent story replay and proves one `stories.save` call and identical receipts.
- `execute(intent, *, operation_ref=...)` accepts and atomically claims a caller-issued opaque operation reference; receipts and `reconcile()` preserve that exact reference and reject conflicting reuse.
- Action compilation and target-scoped capabilities now require exact user/community kinds, matching actor capability, and coherent `group_id`/`owner_id` or `user_id`/`peer_id` bindings.
- Cursors are HMAC-signed and bound to operation, target/item fingerprint, query fingerprint, sample reference, and offset. Cross-target, cross-operation, cross-query, and tampered/unknown cursor reuse fail closed.
- Cooldown check, governor admission, provider invocation, captcha recording, success recording, and governor completion now share one serialized critical section. A concurrent captcha regression proves the second request observes cooldown and never reaches transport.

## Evidence and commands

- `/home/dev/.venvs/events-bot-image-geometry/bin/python -m compileall -q private_events_mcp_vk_adapter.py tests/test_private_events_mcp_vk_workspace.py` — pass.
- `/home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -q tests/test_private_events_mcp_vk_workspace.py` — `12 passed` after review fixes.
- `/home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -q tests/test_private_events_mcp_social_workspace_contract.py tests/test_private_events_mcp_vk_workspace.py` — `42 passed` after review fixes.
- `git diff --cached --check` before implementation commit — pass.
- `git diff --check` after tests — pass.
- Grep safety review found no `main.vk_api`, environment-token lookup, access-token field, filesystem path, base64, or public raw-call method in the implementation.

## Changed files

- `private_events_mcp_vk_adapter.py`
- `tests/test_private_events_mcp_vk_workspace.py`
- `.codex/lanes/mcp-universal-social-vk/RESULTS.md`

## Risks / integration dependencies

- The existing provider-neutral editorial response schema has no reviewed field for separate ad/repost/pinned signals. Per integrator direction, this lane excludes those entries without overloading metrics; an integration-owned `content_flags`/`exclusions` schema extension is still needed if clients must receive the separate counts/flags.
- Opaque refs, durable operation/idempotency storage, actor-token binding, and real VK upload preparation remain integration-owned injected services. This lane intentionally performs no live call and accepts no filesystem path, arbitrary fetch URL, base64 payload, or caller-provided native attachment string.
- The implementation commit is recorded above. The following result-only commit advances branch HEAD; its SHA is supplied in the handoff because a commit cannot contain its own SHA.
