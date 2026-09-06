# INC-2026-09-03 DevCoveer MCP runtime returned 502 for every tool call

Status: closed
Severity: sev2
Service: Codex DevCoveer MCP / OpenAI secure tunnel
Opened: 2026-09-03 09:08 UTC
Closed: 2026-09-03 15:54 UTC
Owners: DevCoveer MCP operator
Related incidents: `INC-2026-07-08-prod-root-overlay-disk-full.md` (disk-full pattern only; different host/service)
Related docs: `/home/dev/.local/libexec/openai-codex-mcp/README.md`, `docs/operations/incident-management.md`

## Summary

MCP schema discovery remained available, but all ChatGPT runtime calls returned HTTP 502. The long-lived `codex-devserver-tunnel.service` process was alive and its `/healthz` and `/readyz` endpoints were green, while its stdio MCP execution channel had stopped delivering requests to the still-running `codex-chatgpt-bridge.py` child.

The immediate recovery was to free host disk headroom and restart `codex-devserver-tunnel.service`. The restart recreated the stdio transport and also activated the already-installed tunnel-client `0.0.14` instead of the stale in-memory `0.0.12` process. No bridge source, model routing, provider fallback, tunnel ID, or upstream URL was changed.

## User / Business Impact

- ChatGPT could discover the eight DevCoveer methods but could not execute `find_projects`, `list_tasks`, `list_models`, or any task lifecycle call.
- Every tested runtime request failed with a connector 502, so the whole remote execution surface was unavailable rather than only one provider catalog.
- Local Codex/OpenCode task execution remained healthy when invoked through a fresh local MCP stdio session.

## Detection

- Reported from ChatGPT after repeated identical 502 responses from independent methods.
- The tunnel journal recorded `failure_source=client_internal`, `upstream_response_received=false`, and HTTP 502 for every `tools/call` beginning at 09:08:45 UTC.
- `/healthz=live` and `/readyz=ready` did not detect the broken execution channel. This is an observability gap: they proved daemon/poller liveness, not a successful MCP tool round trip.

## Timeline

- 06:15 UTC — native Codex thread-store operations logged `No space left on device (os error 28)` while the root filesystem was exhausted.
- 09:07:58 UTC — the native `codex app-server --stdio` child exited with `rc=-15`; the bridge attempted its supported read-path restart.
- 09:08:02 UTC — bridge logged a new native app-server connection.
- 09:08:44 UTC — one tunnel command reached its response deadline and was dropped.
- 09:08:45 UTC — asynchronous thread resume failed; the first common-path remote 502 followed immediately.
- 09:08–15:47 UTC — 467 remote `tools/call` responses were recorded as 502; the bridge child stayed alive but received no new tool-level calls.
- 15:50 UTC — local tunnel `/healthz` and `/readyz` still returned 200; OpenCode remained listening on authenticated `127.0.0.1:4097`.
- 15:51 UTC — a fresh local MCP stdio session successfully ran `find_projects`, `list_tasks`, and native Codex `list_models`, isolating the failure to the long-lived tunnel execution channel.
- 15:53 UTC — obsolete Playwright browser cache was removed, increasing root-disk headroom from 441 MiB to about 950 MiB.
- 15:54 UTC — `codex-devserver-tunnel.service` restarted cleanly; PID changed from `802` to `744951`, bridge PID changed from `909` to `744965`, and tunnel-client changed from in-memory `0.0.12` to installed `0.0.14`.
- 15:56–15:57 UTC — external ChatGPT connector calls for `list_tasks`, `list_models`, and a retried `find_projects` succeeded. The first post-restart `find_projects` attempt encountered one transient connector-side 504 while the remote stream re-established; the immediate retry completed in under one second.
- 15:56 UTC — bounded read-only task `dvt_57ef8b5a032746aeb562ca9285dc7849` started and `read_task` returned terminal text `DEVCOVEER_SMOKE_OK`.
- 16:00 UTC — three consecutive external A/B/C smoke cycles all passed. The remaining reproducible Playwright browser cache was removed after it was confirmed unused, leaving about 1.1 GiB free before worktree cleanup.

## Root Cause

1. The long-lived tunnel-client `0.0.12` instance entered a wedged stdio MCP transport state after the native app-server restart/resume failure. Its process and health server stayed alive, but remote `tools/call` requests failed inside the tunnel client before any upstream bridge response was received.
2. The service's readiness check covered the tunnel daemon/poller, not a semantic MCP round trip, so systemd did not restart the unhealthy execution channel.
3. Host disk exhaustion was a contributing precursor and independently caused native thread-store errors earlier that day. It was not the failing `find_projects` code path: the same bridge code passed through a fresh local MCP session before the service restart.

## Contributing Factors

- Root filesystem was at 100% with only 441 MiB available when investigated.
- The tunnel-client binary had been upgraded at 14:24 UTC, but the service had not restarted and therefore still ran `0.0.12` from memory.
- The DevCoveer installation is operational state under `~/.local`, not a Git checkout, so source/version drift is harder to audit from the project repository.
- Stale task registry rows made many old tasks appear `running`/`cancelling` even though no native child process existed.

## Automation Contract

### Treat as regression guard when

- changing the DevCoveer bridge, tunnel-client profile/service, native app-server lifecycle, task history, or tunnel health/readiness behavior;
- upgrading tunnel-client without restarting the enabled user service;
- changing host disk-retention or cache-cleanup policy.

### Affected surfaces

- `codex-devserver-tunnel.service`
- `/home/dev/.config/tunnel-client/codex-devserver.yaml`
- `/home/dev/.local/bin/codex-mcp-server`
- `/home/dev/.local/libexec/openai-codex-mcp/codex-chatgpt-bridge.py`
- native `codex app-server --stdio`
- authenticated OpenCode backend at `127.0.0.1:4097`
- OpenAI tunnel control-plane poll/response path for `tunnel_6a8afbe7d1c08191a9bf0769ccba7077`

### Mandatory checks before closure or deploy

- verify `systemctl --user status` and `NRestarts` for both DevCoveer units;
- verify root disk bytes and inodes, and search the tunnel journal for `No space left on device`;
- verify local `/healthz` and `/readyz`;
- run a fresh local MCP stdio smoke for `find_projects`, `list_tasks`, and `list_models(provider=codex)`;
- run the same three calls through the ChatGPT connector/tunnel, not only locally;
- start one bounded read-only Codex task and confirm terminal `read_task` output;
- check the post-recovery journal for new 502/504, process exits, and crash loops.

### Required evidence

- exact tunnel-client and bridge PIDs plus tunnel-client version;
- local MCP smoke output;
- external connector result for all three read calls;
- bounded `start_task` task ID and terminal `read_task` result;
- disk free-space evidence and the relevant journal window.

## Immediate Mitigation

- Removed only reproducible, currently unused Playwright browser cache directories for revisions `1208` and `1228`; no project files, task histories, or credentials were deleted.
- Restarted `codex-devserver-tunnel.service`, recreating the stdio MCP child and remote tunnel execution channel.
- Left `opencode-devcoveer.service` running because it was healthy and was not the failing common path.

## Corrective Actions

- Activated tunnel-client `0.0.14` through the controlled service restart.
- Re-established the bridge process and stdio transport without changing the configured tunnel ID, control-plane base URL, provider routing, or bridge source.
- Recorded the incident as a regression contract with mandatory local and external semantic smokes.

## Follow-up Actions

- [ ] Add a semantic watchdog/canary that executes a cheap MCP method such as `find_projects("events")`; daemon-only `/readyz` must not be the sole availability signal.
- [ ] Add bounded disk-retention monitoring/cleanup before free space reaches the thread-store failure threshold.
- [ ] Ensure tunnel-client upgrades restart the enabled user service and record the running version, not only the installed binary version.
- [ ] Reconcile stale task registry statuses after an unclean bridge/app-server lifecycle transition.

## Release And Closure Evidence

- deployed SHA: not applicable; operational service recovery, no bridge or project code change
- deploy path: `systemctl --user restart codex-devserver-tunnel.service`
- regression checks:
  - local MCP: `find_projects("events")` resolved `events-bot`; `list_tasks(limit=5)` returned five rows; `list_models(provider="codex")` returned seven live native models;
  - external connector: the same three methods returned successful MCP results after the tunnel restart;
  - bounded task: `dvt_57ef8b5a032746aeb562ca9285dc7849` completed with `DEVCOVEER_SMOKE_OK`;
  - local health: `/healthz=live`, `/readyz=ready`;
  - units: tunnel PID `744951`, bridge PID `744965`, OpenCode PID `810`, no crash loop (`NRestarts=0`).
- post-deploy verification: no new tunnel-dispatcher 502 was recorded after restart. One transient nested Codex Streamable HTTP 504 appeared while the external stream re-established, but the affected connector retry and the bounded task both completed; three subsequent A/B/C cycles passed. Root disk had about 1.1 GiB available before the temporary documentation worktree was removed.

## Prevention

The incident record now requires a real MCP call on both sides of the tunnel. Health endpoints alone are explicitly insufficient for closure. Permanent semantic watchdog and disk-retention automation remain tracked follow-ups.
