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

- experimental RPC adapter: `scripts/integrations/penpot_api_smoke_test.mjs`;
- Android Chrome/Appium probe: `site/e2e/penpot/pat-browser-probe.mjs`;
- Android Chrome raw-CDP probe without WebDriver: `site/e2e/penpot/pat-raw-cdp-probe.mjs`;
- reserved smoke-test file name: `00 — LoveKGD API smoke test`;
- proposed stable specimen ID: `core.button.smoke`;
- diagnostic workflows under `.github/workflows/penpot-*smoke.yml`.

No write adapter has been accepted as a valid Penpot client. Mutation HTTP methods, payloads and revision semantics have not yet passed contract tests, and no Penpot write was attempted after the read path remained blocked.

## Direct HTTP evidence

The repository secret `PENPOT_INTEGRATION_TOKEN` was present in every run and was not printed. The following GitHub-hosted attempts all returned an HTTP `403` Cloudflare `Just a moment...` response before Penpot application authentication was observed:

1. direct Node request — [run 31100428716](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31100428716);
2. browser-like request headers — [run 31100796202](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31100796202);
3. Chromium bootstrap and cookies — [run 31100968024](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31100968024);
4. request from a Chromium page context — [run 31101198304](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31101198304);
5. GitHub-hosted macOS — [run 31101400696](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31101400696).

A sixth run used the exact request shape documented by Penpot for PAT profile access: `GET /api/rpc/command/get-profile` with `Authorization: Token …`. It still returned Cloudflare `403`, `Content-Type: text/html`, `Server: cloudflare`, and a challenge page body — [run 31104523319](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31104523319).

## Android Chrome evidence

The Android tests reused the repository's existing Android 15 / Pixel 7 / KVM contour. They deliberately retained the same GitHub-hosted egress and changed only the browser execution context.

### Appium browser context

[Run 31108790581](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31108790581) opened the ordinary Penpot login application successfully:

- root URL: HTTP 200;
- final URL: `https://design.penpot.app/#/auth/login`;
- page title: `Penpot | Full-stack design`;
- no Cloudflare challenge remained on the root application.

From that same Chrome session, three same-origin `fetch` probes called `get-profile`:

- without `Authorization`;
- with a deliberately invalid token;
- with `PENPOT_INTEGRATION_TOKEN`.

All three received Cloudflare HTML `403`; the real token never reached an observable Penpot authentication response.

### Top-level API navigation with scoped header injection

[Run 31109395702](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31109395702) navigated the browser itself to the API URL and injected the `Authorization` header only into the exact `get-profile` request. This allowed the Cloudflare challenge page to execute as a top-level document without leaking the PAT to challenge subresources.

Results for no token, invalid token and real token were identical:

- page title: `Just a moment...`;
- HTTP status observed by Navigation Timing: 403;
- content type: `text/html`;
- challenge detected: yes;
- challenge cleared within the bounded wait: no;
- profile detected: no.

### Raw Android Chrome without WebDriver

[Run 31111146181](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31111146181) removed Appium and ChromeDriver from the browser path. Chrome was started directly through ADB and controlled only through its DevTools socket. The evidence explicitly observed:

- `navigator.webdriver = false`;
- root Penpot application loaded with HTTP 200 and no challenge;
- exact API requests were intercepted only for scoped PAT header injection;
- no-token, invalid-token and real-token API navigations all returned the same Cloudflare HTML 403;
- none of the three challenges cleared;
- no profile response was observed.

This rules out `navigator.webdriver` and ChromeDriver alone as the explanation for the tested Android failure. It does not prove that every physical mobile device or every network will fail; it proves that the tested GitHub-hosted Android emulator path does not provide a usable autonomous PAT bridge.

## Established conclusions

1. Penpot Cloud's ordinary web application is reachable from GitHub-hosted Android Chrome.
2. The protected `get-profile` API route receives a separate Cloudflare challenge in that context.
3. Android Chrome did not clear that challenge, with or without WebDriver.
4. The real PAT and deliberately invalid PAT remained indistinguishable at the edge layer.
5. Therefore the token is still neither validated nor invalidated by these runs.
6. GitHub Actions plus the tested Android emulator is **not** a working solution for the Penpot integration.
7. iOS Simulator was not tested; no conclusion is claimed for iOS.

## Retracted conclusion about ServerSpace

The earlier recommendation to register a self-hosted runner on ServerSpace was unsupported. A different egress IP is a plausible diagnostic variable, but no request from ServerSpace was made and no evidence shows that Cloudflare will treat that IP differently. A hosting-provider IP may receive the same challenge.

ServerSpace must therefore not be presented as the solution or as the minimal next path.

## Correct validation order

1. Use the exact documented read-only `GET get-profile` probe from a candidate browser/network context.
2. Treat `200` as proof of PAT access, `401/403` non-HTML as an application-layer authentication result, and Cloudflare HTML `403` as an edge-layer block.
3. Only after a `200`, validate read commands and mutation commands with their correct HTTP methods and payload formats.
4. Only after those contract tests, perform one isolated Penpot write and reread.
5. Only after the write succeeds, implement and test the real comment-to-prompt loop.

A new execution environment is justified only when the exact read-only probe can be shown to reach Penpot authentication from that environment.

## Current product result

- working Penpot integration: **no**;
- validated owner feedback flow: **no**;
- Penpot file/button/comment created: **no**;
- direct Penpot page URL: **no**;
- Android emulator suitability: **failed for the tested GitHub-hosted contour**;
- iOS Simulator suitability: **unknown**;
- ServerSpace suitability: **unknown**;
- token leaked to logs or artifacts: **no**.
