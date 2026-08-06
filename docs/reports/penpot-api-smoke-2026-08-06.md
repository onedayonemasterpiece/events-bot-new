# Penpot API smoke — 2026-08-06

> Status: **FAIL — no end-to-end Penpot integration has been demonstrated**

## What is proposed, not validated

The following interaction model is a product hypothesis, not a working flow:

```text
design-system element is published to Penpot
→ owner leaves a comment over the element
→ integration reads the open comment
→ integration emits a short prompt for a new ChatGPT session
```

None of those transitions has passed an end-to-end test. In particular:

- no object has been created in Penpot;
- no Penpot comment has been created or read;
- no mapping from a comment to a stable design-system element ID has been proven;
- no prompt has been generated from a real Penpot comment;
- no direct Penpot file/page URL is available.

The proposed rule that the owner comments rather than edits the generated design-system element remains a product requirement, but it must not be described as an implemented workflow.

## Implemented experimental code

Branch: `agent/penpot-api-smoke-20260806`.

- experimental adapter: `scripts/integrations/penpot_api_smoke_test.mjs`;
- reserved smoke-test file name: `00 — LoveKGD API smoke test`;
- proposed stable specimen ID: `core.button.smoke`;
- intended isolated write and reread;
- diagnostic workflow: `.github/workflows/penpot-design-system-api-smoke.yml`.

The adapter has **not** been accepted as a valid Penpot client. Its first versions sent every RPC as `POST`, while Penpot's official PAT example calls `get-profile` with `GET`. Therefore the adapter must not be used for writes until query/mutation HTTP semantics are validated command by command.

## Live evidence

The repository secret `PENPOT_INTEGRATION_TOKEN` was present in every run and was not printed. The following GitHub-hosted attempts all returned an HTTP `403` Cloudflare `Just a moment...` response before Penpot application authentication was observed:

1. direct Node request — [run 31100428716](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31100428716);
2. browser-like request headers — [run 31100796202](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31100796202);
3. Chromium bootstrap and cookies — [run 31100968024](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31100968024);
4. request from a Chromium page context — [run 31101198304](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31101198304);
5. GitHub-hosted macOS — [run 31101400696](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31101400696).

A sixth run used the exact request shape documented by Penpot for PAT profile access: `GET /api/rpc/command/get-profile` with `Authorization: Token …`. It still returned Cloudflare `403`, `Content-Type: text/html`, `Server: cloudflare`, and a challenge page body — [run 31104523319](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31104523319).

This proves only that the tested GitHub-hosted egress paths do not reach a successful PAT profile read. It does **not** prove that the token is valid or invalid, and it does **not** prove that another hosting provider will work.

## Retracted conclusion about ServerSpace

The earlier recommendation to register a self-hosted runner on ServerSpace was unsupported. A different egress IP is a plausible diagnostic variable, but no request from ServerSpace was made and no evidence shows that Cloudflare will treat that IP differently. A hosting-provider IP may receive the same challenge.

ServerSpace must therefore not be presented as the solution or as the minimal next path.

## Correct validation order

1. Use the exact documented read-only `GET get-profile` probe from a candidate egress.
2. Treat `200` as proof of PAT access, `401/403` JSON as an application-layer authentication result, and Cloudflare HTML `403` as an edge-layer block.
3. Only after a `200`, validate read commands and mutation commands with their correct HTTP methods and payload formats.
4. Only after those contract tests, perform one isolated Penpot write and reread.
5. Only after the write succeeds, implement and test the real comment-to-prompt loop.

A self-hosted runner is justified only after the exact read-only probe has already succeeded from that machine or network.

## Current product result

- working Penpot integration: **no**;
- validated owner feedback flow: **no**;
- Penpot file/button/comment created: **no**;
- direct Penpot page URL: **no**;
- ServerSpace suitability: **unknown**;
- token leaked to logs or artifacts: **no**.
