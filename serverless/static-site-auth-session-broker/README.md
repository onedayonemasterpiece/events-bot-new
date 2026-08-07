# Static-site auth session broker

This HTTPS function exchanges a verified GitHub Actions OIDC identity for one
short-lived Supabase email OTP and one-time action link generated through the admin `generate_link`
endpoint. It never calls the product `/auth/v1/otp` endpoint and never sends
mail.

The deployment must configure exact allowlists for repository, ref,
`workflow_ref`, protected environment, event, persona and redirect. Scheduled
runs use the special `github-claim-bound` run policy: the numeric request
`run_id` must equal the signed GitHub claim. The broker then atomically claims
`claim_static_site_auth_session_issue_v1` with a hard limit of one credential
per run/persona before asking Supabase to generate a credential.

Audit records contain only keyed hashes. The OIDC token, email, OTP, action
link, redirect path and raw run id must never be logged.
