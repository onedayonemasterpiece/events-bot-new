# INC-2026-08-15-mtproto-proxy-desktop-reconnect Telegram Desktop did not reconnect after host reboot

Status: monitoring
Severity: sev3
Service: host-level Telegram MTProto proxy (`vpn-server-mtproto-1`)
Opened: 2026-08-15
Closed: —
Owners: server operator
Related incidents: `INC-2026-07-29-mtproto-proxy-desktop-disconnect`
Related docs: `/home/dev/projects/vpn-server/docs/operations.md`

## Summary

After the VPS rebooted at `2026-08-15 06:37 UTC`, Telegram Desktop remained in
the connecting state for the saved MTProto proxy profile. Server-side checks
excluded a continuing proxy outage: the container started normally after boot,
the listener and Telegram DC handshakes were live, and a protocol-aware DD test
completed through three DCs. The incident matches the prior Desktop saved-profile
collision failure mode; a fresh DD link using a different DNS alias was prepared
to force a distinct client-side proxy identity. User confirmation is pending.

## User / Business Impact

- One reported Telegram Desktop client could not connect through its saved proxy.
- The proxy continued to accept other public client connections and reach Telegram
  DCs, so a server-wide outage was not observed.

## Detection

- Primary signal: user report and Telegram Desktop proxy settings screenshot.
- Server evidence: boot time, Docker state, listener, application log, live
  protocol-aware `mtp_ping`, current-day log inventory and disk usage.

## Timeline

- 2026-08-15 06:37 UTC — VPS booted; all `vpn-server` containers started.
- 2026-08-15 06:48 UTC — container confirmed running with restart count `0`,
  OOM false, and `0.0.0.0:1443` listening; fresh Telegram DC handshakes present.
- 2026-08-15 06:52 UTC — official upstream `mtp_ping` secure/DD checks passed
  against the saved `sslip.io` alias and a fresh `nip.io` alias, DC 1/2/3,
  `3/3 OK` for each hostname.
- 2026-08-15 06:54 UTC — fresh DNS-alias DD link prepared for the user; awaiting
  Desktop confirmation.

## Root Cause

A continuing server-side outage is excluded by the listener, live downstream
traffic, Telegram DC handshakes, and complete DD application handshakes. The
most likely immediate failure is stale/colliding Telegram Desktop proxy-profile
state after the host reboot, consistent with the confirmed 2026-07-31 recurrence.
Final attribution remains pending until the user confirms recovery through the
fresh hostname.

## Contributing Factors

- Telegram Desktop persists proxy identity/state across reconnect attempts.
- The service still lacks a stable dedicated Desktop hostname; recovery currently
  depends on public wildcard DNS aliases to create a distinct profile identity.
- No protocol-aware automated healthcheck alerted independently of the user report.

## Automation Contract

### Treat as regression guard when

- investigating Telegram Desktop MTProto reconnect failures;
- changing the MTProto service, listener, protocol list, Desktop link hostname,
  startup behavior or host reboot recovery.

### Affected surfaces

- `/home/dev/projects/vpn-server/docker-compose.yml`;
- `/home/dev/projects/vpn-server/bot/vpn_manager.py`;
- container `vpn-server-mtproto-1`;
- TCP `1443` and Desktop DD proxy profiles;
- `/home/dev/projects/vpn-server/data/mtproto-logs`;
- root filesystem capacity.

### Mandatory checks before closure or deploy

- `docker compose config --quiet`;
- container running, OOM false, no restart loop;
- listener on `0.0.0.0:1443`;
- fresh Telegram DC `handshake complete` evidence;
- protocol-aware secure/DD `mtp_ping` to DC 1/2/3;
- current-day log retained and logger capped at 25 MiB with two rotations;
- filesystem capacity recorded before and after diagnostics;
- user confirmation from Telegram Desktop through a newly-created profile.

### Required evidence

- container start timestamp/restart count and host boot time;
- listener, client/downstream and Telegram DC evidence;
- redacted `mtp_ping` output;
- log inventory and disk usage;
- user recovery confirmation.

## Immediate Mitigation

- No server restart or secret rotation was performed because the service is healthy.
- Prepared an equivalent DD link through `188.227.84.107.nip.io`, which resolves
  to the same server but creates a new Desktop proxy identity.

## Corrective Actions

- None deployed during initial triage; recovery is client-profile scoped.

## Follow-up Actions

- [ ] Obtain user confirmation through the fresh DD profile and close or continue
      packet-level diagnosis accordingly.
- [ ] Configure a stable dedicated Desktop hostname rather than public wildcard DNS.
- [ ] Add protocol-aware MTProto health monitoring that survives host reboots.

## Release And Closure Evidence

- deployed SHA: none; no code/config change or deploy was required;
- deploy path: none;
- regression checks: Compose config OK; container running, OOM false, restart
  count 0; `0.0.0.0:1443` LISTEN; current protocols include `mtp_secure` and
  `mtp_fake_tls`; DD `mtp_ping` DC 1/2/3 passed `3/3` for both tested hostnames;
  fresh Telegram DC handshakes and active-client downstream migration present;
  logger cap `26214400` bytes with two rotations confirmed;
- post-triage verification: current-day active log retained; MTProto log directory
  `51 MiB`; filesystem returned to `89%` used (`8.5 GiB` available) after removing
  the temporary diagnostic image; user confirmation pending;
- redacted evidence: `artifacts/codex/INC-2026-08-15-mtproto-desktop-disconnect/mtp-ping-20260815T0652Z.txt`.

## Prevention

The prior incident regression contract remains active. Closure additionally
requires a real Desktop recovery confirmation rather than TCP-only evidence.
