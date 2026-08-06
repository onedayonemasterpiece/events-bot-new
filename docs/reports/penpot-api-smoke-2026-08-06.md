# Penpot API smoke — 2026-08-06

> Status: **BLOCKED — Penpot Cloud rejects GitHub-hosted runner egress before PAT authentication**

## Intended product loop

```text
design-system element generated from Git
→ visual specimen in Penpot
→ owner leaves a comment over the specimen
→ integration reads the open comment
→ short reproducible ChatGPT prompt is generated
→ candidate implementation and secret preview
→ owner sign-off
→ versioned release
```

The owner is not expected to edit generated design-system elements in Penpot. Penpot is the visual review and feedback surface. MCP and Codex are not part of this contract.

## What was implemented

Branch: `agent/penpot-api-smoke-20260806`.

- PAT-backed adapter: `scripts/integrations/penpot_api_smoke_test.mjs`;
- isolated target file name: `00 — LoveKGD API smoke test`;
- stable specimen ID: `core.button.smoke`;
- intended write: one frame, one button and one marked comment thread;
- post-write reread and revision verification;
- direct workspace URL and immutable JSON receipt on success;
- workflow: `.github/workflows/penpot-design-system-api-smoke.yml`.

The adapter is idempotent and refuses to write outside its dedicated smoke file.

## Live evidence

The repository secret `PENPOT_INTEGRATION_TOKEN` was present in every run. Offline syntax and contract self-tests passed. Every live attempt was rejected at the first Penpot RPC call, `get-profile`, with an HTTP `403` Cloudflare `Just a moment...` response before the request reached Penpot token authentication.

Attempts:

1. GitHub-hosted Ubuntu, direct Node request — [run 31100428716](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31100428716);
2. Ubuntu with browser request headers — [run 31100796202](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31100796202);
3. Ubuntu with Chromium bootstrap and cookies — [run 31100968024](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31100968024);
4. Ubuntu with the API request executed inside a Chromium page context — [run 31101198304](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31101198304);
5. GitHub-hosted macOS — [run 31101400696](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31101400696).

The macOS runner was also hosted in Azure `westus`, and the same Cloudflare response was returned. The token has therefore not been proven invalid; the hosted-runner egress path is blocked before PAT validation.

## Product result

- Penpot file created: **no**;
- button specimen created: **no**;
- comment thread created: **no**;
- direct Penpot page URL available: **no**;
- token leaked to logs or artifacts: **no**.

No success claim or synthetic Penpot link is permitted while this status remains blocked.

## Minimal next path

Run the same workflow on a short-lived self-hosted GitHub Actions runner located on the existing development server and carrying only the custom label `lovekgd-penpot`. This keeps the current repository secret and workflow model, but moves the Penpot API call away from GitHub/Azure hosted-runner egress. It does **not** require self-hosting Penpot or introducing MCP.

The workflow is now manual-only and targets `runs-on: lovekgd-penpot`. Once the runner is registered, dispatch it with `allow_write=true`. A successful run must create the isolated file, reread the generated frame, upload a JSON receipt and return the exact Penpot workspace URL.

## Acceptance after the write succeeds

1. Open the direct Penpot page.
2. Leave a reply or a new comment over the generated button.
3. Run the comment-pull slice.
4. Verify that it binds the feedback to `core.button.smoke` and emits a short prompt for a new ChatGPT session.
