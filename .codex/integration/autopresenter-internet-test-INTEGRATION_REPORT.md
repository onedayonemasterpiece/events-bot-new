# Autopresenter internet first-test integration report

| Lane | Requirement IDs | Branch | Status | Evidence |
|---|---|---|---|---|
| discovery-public-relay | R01–R03 | read-only | completed | Separate Fly app, role-scoped bearer tokens, one always-on instance |
| discovery-windows-test | R04 | read-only | completed | Auth-scoped ZIP with one-click Windows bootstrap; explicitly not M3 |
| integration-implementation | R01–R06 | integration/autopresenter-internet-test | completed | Deployed source `2f074038517ca01ceee00425088800fd2db6f47a`; live checks below |
| closure-review | R01–R06 | read-only | completed | R01–R03 Done; R04 Partial pending Windows test; R05 Done for exact-HEAD public relay; R06 Done on secure handoff |

## Requirements

- R01: Public HTTPS phone control usable without LAN.
- R02: Public relay command and agent APIs are authenticated.
- R03: Presenter agent connects outbound to the public relay.
- R04: Provide a clear downloadable Windows first-test demonstrator launcher or equivalent demonstrator entry point.
- R05: Verify the phone/relay/agent path without localhost or a shared LAN.
- R06: Publish clear PHONE and DEMONSTRATOR links with minimal operator instructions, synchronized canonical docs, and CHANGELOG.

## Live release evidence

- Public service: `https://kenigevents-autopresenter.fly.dev/`
- Public phone entry point: `/control/#token=<control token>`; the fragment is moved to session storage and removed from browser history.
- Public demonstrator entry point: `/demonstrator/#token=<control token>`.
- Deployed image: `kenigevents-autopresenter:deployment-01KYPAXY2BEYBGQDYFS6X8HX3W`.
- Fly machine `2879209fd9e998`: started, one of one health check passing.
- `/healthz`: HTTP 200; unauthenticated `/api/state`: HTTP 401; authenticated `/api/state`: HTTP 200.
- Auth-scoped Windows ZIP: 9 entries, stable SHA-256
  `6e9d6c43e4648e449507f08179140d5f4adf8c9e754ba72835a6af52021e7fc1`.
- Exact deployed-HEAD HTTPS relay E2E completed the real `tomorrow-mobile`
  Playwright scenario and verified Run, Reset, and Stop (bounded transcript
  below).
- Phone control was exercised at a 430×932 viewport through the public HTTPS URL.
- M0 compatibility harness diff count: zero.

## Local verification

- Relay unit tests: 8/8 passed.
- Agent unit tests: 8/8 passed.
- Python compile and Node syntax checks passed.
- Exact static-site build completed with 465 pages.

## Sanitized exact-HEAD E2E transcript

Run at `2026-07-29T07:12Z` with deployed source
`2f074038517ca01ceee00425088800fd2db6f47a`; secrets and URL fragments are
excluded:

```text
agent=exact-head-2f074038 status=idle detail=stage ready
sequence=1 action=run
status=running detail=tomorrow-mobile
status=completed detail=tomorrow-mobile: /zavtra/ ready
sequence=2 action=reset
status=idle detail=stage reset
sequence=3 action=run
sequence=4 action=stop
status=idle detail=agent confirmed stopped
EXACT_HEAD_PUBLIC_E2E=PASS
```

This proves the public HTTPS relay and real browser-agent path on exact deployed
HEAD. It does not substitute for the requested first run on the target Windows
computer or independently prove cellular-network provenance.

## Closure review

- R01 **Done** — public HTTPS phone control, no shared LAN required.
- R02 **Done** — live unauthenticated probes return 401 and control/agent roles
  are separated.
- R03 **Done** — the agent is outbound-only and the exact deployed source was
  exercised live.
- R04 **Partial by design** — the live authenticated Windows ZIP and launcher
  are ready; empirical Windows execution is the next owner test.
- R05 **Done for the public relay path** — exact-HEAD Run → Completed, Reset,
  and Stop passed; the owner’s Windows/cellular test remains acceptance
  evidence, not an implementation blocker.
- R06 **Done** — canonical docs and CHANGELOG are synchronized; exact
  token-bearing links are handed off out of Git.

The reviewer confirmed an identical M0 tree, honest `FIRST_TEST_NOT_M3`
labeling, no unrelated diff, and no dropped worker patch. Future browser
evidence must remain sanitized because URL fragments can be captured by
automation snapshots.
