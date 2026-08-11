# One-pass execution prompt: enable Telegram document sending in production Events MCP

Work in `onedayonemasterpiece/events-bot-new` and complete this feature end to end. Do not stop after analysis, local code, a pushed branch, or a Draft PR. The intended final state is that the existing production `eventsBot` ChatGPT connector can receive a real ChatGPT upload through `fileParams` and send the file as a Telegram document to Saved Messages through the normal typed MCP flow.

## Sources of truth

Read before editing:

- `AGENTS.md`
- `CODEX.md` if present
- `docs/README.md`
- `docs/routes.yml`
- `docs/operations/repository-workflow.md`
- `docs/operations/release-governance.md`
- `docs/operations/private-events-mcp.md`
- this handoff directory:
  - `README.md`
  - `architecture.md`
  - `acceptance-matrix.md`
  - `CODE_AGENT_TASK.md`
  - `proposed/private_events_mcp/document_policy.py`
  - `proposed/tests/test_document_policy.py`

`architecture.md` and `acceptance-matrix.md` define the product and security contract. `CODE_AGENT_TASK.md` defines the detailed implementation surface. This execution prompt extends that task through merge, exact-main deployment, production activation, connector refresh, and live acceptance. Where the older task says production activation is excluded, this prompt supersedes that exclusion.

Begin with `git fetch origin --prune`, record the current `origin/main` SHA, and create a clean linked worktree on `feature/private-mcp-telegram-document-send`. Re-check all named code locations against that exact base; do not assume the design-time SHA is current.

## Fixed scope

Implement only Telegram `send_message` with exactly one `MediaRole.DOCUMENT` attachment and an optional caption/rich entities. Mandatory live target: Telegram Saved Messages. Use ChatGPT `fileParams` ingress only.

Allowed v1 types: structurally valid APK, PDF, ZIP, UTF-8 TXT/MD/CSV/JSON, DOCX/XLSX/PPTX. Default document limit: 48 MiB; hard configuration cap: 64 MiB. The feature has its own fail-closed kill switch, is disabled by default in source, and reuses the authenticated immutable asset-ingress boundary.

Do not add VK documents, multiple attachments, mixed media, albums, audio/video/animation, story documents, publish/schedule/edit/forward document support, arbitrary URL/path/base64 ingress, raw Telethon arguments, generic opaque binaries, antivirus claims, or a new connector/OAuth identity.

## Required implementation

Implement every requirement in `CODE_AGENT_TASK.md` and every applicable row of `acceptance-matrix.md`. In particular:

1. Add `PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_FILE_SEND_ENABLED` and derive common asset ingress from `media_story_enabled or file_send_enabled`, without weakening the existing image/story gate.
2. Add `PRIVATE_EVENTS_MCP_DOCUMENT_MAX_ASSET_BYTES`, default 48 MiB and hard maximum 64 MiB. Enabled invalid configuration must fail closed; disabled startup must remain inert.
3. Integrate a repository-native document policy based on the supplied prototype: immutable regular bytes, size, SHA-256, sanitized bounded filename, detected type, declared/detected MIME consistency, bounded ZIP/APK/Office inventory without extraction, APK Android-structure requirement, unsafe/encrypted/bomb-like archive rejection, and no execution.
4. Extend schemas, verified manifests, capability projection, scopes, tool discovery, prepare/commit authorization, digest binding, audit redaction, and provider bindings so `document` is advertised only for Telegram targets that currently support `send_message`.
5. Enforce Telegram + `send_message`, exactly one document, no mixed media, ready/unexpired principal/provider-bound asset, reopen/rehash at prepare and commit, and binding of role, SHA-256, size, detected MIME, sanitized filename, and expiry into the action digest and frozen preview.
6. At commit, make exactly one Telethon 1.44 `send_file` attempt with forced document semantics and a sanitized `DocumentAttributeFilename`; preserve caption/entities. Timeout remains `outcome_unknown`, `retry_safe=false`, with no blind resend. Perform read-after-write verification of target/message/document and filename/size when available.
7. Never expose or persist a signed download URL, original ChatGPT file ID, unsanitized name, internal path, Telegram access hash/native IDs, raw provider kwargs, or credentials.
8. Keep all existing text, image/story, Telegram, VK, OAuth compatibility, approval, idempotency, timeout, and disabled-startup behavior green.
9. Add a bounded document smoke/acceptance script. CI fixtures must be deterministic, synthetic, and small. A filesystem-only call is not the ChatGPT `fileParams` acceptance.
10. Update `.env.example`, `docs/operations/private-events-mcp.md`, relevant routes/indexes, tests, and `CHANGELOG.md` `[Unreleased]`. When complete, mark this handoff as implemented/archive and remove it from the active TODO list; the operational document becomes canonical.

## Verification and adversarial controls

Run at least:

```bash
PYTHONPATH=. python -m compileall -q private_events_mcp private_events_mcp*.py tests scripts main_part2.py
PYTHONPATH=. pytest -q tests/test_private_events_mcp_*.py
git diff --check
```

Also run all repository-required linters, focused suites for touched modules, and every existing GitHub Actions gate. Preserve exact commands and results.

Add negative/mutation controls proving that tests fail when each of these protections is removed: document-role restriction; Telegram/action restriction; one-document/no-mixed-media invariant; principal/provider binding; TTL and digest recheck; filename sanitization; MIME/structure policy; feature kill switch; one-attempt timeout behavior; and URL/path/file-ID/native-ID non-leakage.

Do an independent review against the exact proposed head SHA. Resolve findings rather than relying on the implementation summary or green CI alone.

## PR, merge, deployment, and activation

1. Push the feature branch and open a Draft PR with base/head SHAs, contracts changed, exact tests, negative controls, rollout, and rollback.
2. Bring the branch current with `origin/main`; obtain green required checks and exact-head independent review. Mark ready only when the implementation is releaseable.
3. Do not stop at the PR. Merge through the repository's normal path when permissions and branch policy allow. If an external approval is mechanically required and unavailable, finish every other step and report that single concrete blocker; do not claim the feature is available.
4. From a separate clean exact-main worktree, verify that the merged SHA is the current `origin/main`, then deploy only through `scripts/deploy_fly_main.sh`.
5. Preserve the existing MCP private path, connector name, ChatGPT client ID/secret, OAuth resource/audience, signing key/state, scopes, and provider identity. Do not run `--new-install`, delete/re-add, or rename the connector.
6. Stage production configuration through the existing secure mechanism. Enable `PRIVATE_EVENTS_MCP_UNIVERSAL_SOCIAL_FILE_SEND_ENABLED=1` only after exact-main deployment and preflight checks. Reuse the existing owner-only asset root and observed ChatGPT download-host policy; do not print secrets or signed URLs. Keep the source default disabled.
7. Verify merged SHA = Fly release SHA = in-container immutable SHA, `/healthz` readiness, DB `quick_check=ok`, auth/media-root permissions, disk quota, logs, scheduler/webhook health, and no provider call during staging or prepare.
8. Refresh the existing ChatGPT connection in place and start a new chat so the current tool catalogue is fetched. Never create a replacement connection for this upgrade.

## Mandatory live acceptance

The task is not done until production passes all of the following with sanitized evidence:

1. In a new ChatGPT chat, select a deterministic tiny APK-shaped fixture through the actual upload UI so the connector supplies a real `fileParams` object.
2. `social_asset_stage(platform=telegram, role=document)` returns a ready opaque asset with detected APK MIME, exact size, SHA-256, sanitized `.apk` filename, and expiry; no URL/file ID/path is present anywhere in response, logs, or artifacts.
3. Resolve Telegram Saved Messages through the existing typed target resolver.
4. Prepare one `send_message` with the document and a clear canary caption. The frozen preview and digest include target, filename, size, MIME, digest prefix, and expiry. No Telegram upload occurs before commit.
5. Commit once. Verify one and only one Telethon provider attempt, a successful durable receipt, read-after-write evidence, and an actually downloadable Telegram document whose filename, size, and caption match the frozen preview.
6. Exercise at least one negative live probe that fails before Telegram transport, such as an ordinary ZIP renamed `.apk`, expired asset, or disabled file-send flag.
7. Perform a rollback probe: disable only the file-send switch, redeploy/reload using the repository-approved mechanism, confirm document capability disappears while evidence tools, text sends, and image/story behavior remain intact; then re-enable it and repeat the catalogue plus harmless readiness check so the final production state has file sending enabled.
8. Delete the disposable canary only through the existing typed delete flow after evidence is captured and only if permissions/policy allow; otherwise leave exactly one clearly labeled test document in Saved Messages and report it.

Do not substitute a direct local path, arbitrary HTTP URL, fake `fileParams` dict, filesystem call, unit fixture, or provider SDK call and call it end to end.

## Final report

Return one final completion report containing:

- PR and merge links;
- branch base, final head, merged main, Fly release, and in-container SHAs;
- exact test/CI/independent-review results;
- production flags/config names changed, without values or secrets;
- sanitized catalogue evidence showing `document` only on eligible Telegram `send_message` targets;
- sanitized live Saved Messages receipt/read-back evidence and canary cleanup state;
- regression and rollback-probe results;
- any remaining blocker, stated precisely.

Definition of done: the existing production `eventsBot` connection, refreshed in place, advertises document ingress only where allowed and has successfully delivered one real ChatGPT-uploaded APK fixture to Telegram Saved Messages through `stage -> prepare -> commit`, with immutable validation, one provider attempt, read-after-write verification, and no secret/native/path leakage.
