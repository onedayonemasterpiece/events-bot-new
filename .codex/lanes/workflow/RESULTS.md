# Region Talk external-publication import workflow lane

## Delivered

- Added `.github/workflows/region-talk-external-publication-import.yml`.
- The manual dispatcher is limited by both a `choice` input and a shell allowlist
  to the three approved, committed Region Talk research-result JSON paths.
- The job checks out `main` only, with persisted credentials disabled; it never
  checks out a dispatcher-provided revision.
- Permissions are minimized to `contents: read` and `id-token: write`; the
  YDB write job is gated by protected environment `region-talk-ydb-import`.
- Validation runs before WIF authentication and before `--execute`. A SHA-256
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

The federated credential must bind the exact GitHub OIDC `sub` for this
repository and the protected `region-talk-ydb-import` environment. The service
account must receive only the YDB permissions required by the importer.

## Verification

- YAML parsed with Ruby Psych.
- Static grep confirms the workflow has no `contents: write`, no arbitrary
  checkout expression/ref, and no service-account key JSON input.
