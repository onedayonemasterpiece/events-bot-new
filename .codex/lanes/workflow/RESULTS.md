# Region Talk external-publication import workflow lane

## Delivered

- Added `.github/workflows/region-talk-external-publication-import.yml`.
- The manual dispatcher accepts a required string but runtime-validates it as one
  regular (not symlinked) file under the reviewed main path
  `docs/features/region-talk-channel/region-talk-external-research-result-*.json`.
  Traversal, directory escapes, and every other path are rejected, so a new
  reviewed JSON merged to `main` is dispatchable without editing the workflow.
- The job checks out `main` only, with persisted credentials disabled; it never
  checks out a dispatcher-provided revision.
- Permissions are minimized to `contents: read` and `id-token: write`; the
  YDB write job is gated by protected environment `region-talk-ydb-import`.
- Validation runs before WIF authentication and before `--execute`. The
  execution uses `--no-publish-registry`, so this narrowly-scoped YDB service
  account does not need static-site registry-publishing permissions. A SHA-256
  addressed receipt plus validation/execution reports are uploaded as an
  artifact.
- Authentication uses the GitHub OIDC request endpoint and RFC 8693 exchange
  at `auth.yandex.cloud/oauth/token` for a short-lived IAM token. It uses
  environment/repository variables for non-secret IDs and stores no service
  account JSON key or long-lived token.

## Required protected-environment variables

- `YANDEX_WIF_OIDC_AUDIENCE` — audience configured on the Yandex OIDC WIF.
- `YANDEX_WIF_SERVICE_ACCOUNT_ID` — service-account ID attached to the
  restricted federated credential.
- `REGION_TALK_YDB_ENDPOINT`
- `REGION_TALK_YDB_DATABASE`
- `REGION_TALK_YDB_NAMESPACE`

The federated credential must bind the exact GitHub OIDC `sub` for this
repository and the protected `region-talk-ydb-import` environment. The service
account must receive only the YDB permissions required by the importer.

## Verification

- YAML parsed with PyYAML; static assertions verify the input regex/path
  guard, minimal permissions, fixed `main` checkout, WIF token exchange,
  `--no-publish-registry`, and absence of a service-account key JSON input.
- Every embedded Bash snippet passed `bash -n`; `git diff --check` passed.
