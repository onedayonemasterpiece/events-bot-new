# Search production-health Stage 2 integration report

## Base and safety

- Integration base: `origin/main@66bc0d43e36299417626f992021cfb7299ddf704`;
  it contains merged PR #441 at
  `dd5ffc2eb5327cb52eb62e232e1e927dbe4c9c66`.
- Branch/PR: `integration/search-production-health-stage2-20260809` / #451.
- PR #451 merged to `main` as `ad0a1f3bb12a63805aec65f52489151e3f382b83`.
- Live acceptance workflows so far: `0 / 2` accepted; bounded diagnostic runs
  through `31335122827` supplied browser product proof and isolated the
  remaining Android evidence boundary without making an Android Search POST.
- The post-merge activation migration and exact Edge/Fly deployments are
  complete; their verification is recorded below.
- PR #436: untouched and not a dependency.

## Integrated lanes

| Lane | Implementation | Receipt | Result |
|---|---|---|---|
| workflow/release marker | `d7eb39e97` | `3a55b8995` | two schedules, manual/runtime marker, no generation triggers |
| journey/evidence | `0938baed8` | `967c74c4e` | one query/POST, target pin, bytes, strict evidence |
| mobile transport | `583ea5d0a` | `c27b983c8` | real Appium preflight/scroll/card open/cleanup |
| broker | `a22f01b62` | `1e8188427` | platform identity, typed admission, migration |
| reporter | `70754d633`, `ec02e0fc2` | `a557a6d0a` | platform disposition and REST mutation |
| broker purpose/replay | `b3db5eff1`, `7954b1f98` | `ef00d10d9`, `330e20bcb` | distinct qualification persona, exact SQL identity, bounded replay and pg_cron erasure |
| whole-cell observer | `b72cedeba` | `c07da8303` | pre-Auth through event-route physical POST/bytes/forbidden traffic boundary |
| integration hardening | current branch | this report | Appium secret-log containment, exact Edge source-revision activation gate, reporter history/labels/aggregate and canonical docs |

## Deterministic acceptance

Required before live:

- Search production-health aggregate suite: **144/144 PASS** after the current
  Android Auth byte-observer and disposable-probe regressions;
- legacy Search harness: **32/32 PASS**;
- focused broker/HTTP/SQL/security suite **48/48 PASS**; combined broker,
  security and static source/release regression **135/135 PASS**; Edge contract
  group **26/26 PASS** and Auth
  Node **16/16 PASS**;
- static source-binding/release regression: **87/87 PASS**;
- workflow YAML and shell/node syntax;
- GitHub Actions semantic validation with actionlint 1.7.12, including the
  job/step context boundary for ephemeral Android/iOS Appium homes;
- broker migration plus its canonical SQL replay/expiry contract on ephemeral
  PostgreSQL 17, and diff check;
- independent checklist audit and fresh GitHub Actions.

## Production activation gate

1. Merge #451 into current `main` after green checks.
2. Apply migrations `20260809143602_static_site_auth_broker_platform_claims.sql`
   and `20260809191607_static_site_auth_broker_short_active_claim.sql` in
   order; verify the widened identity, v1 compatibility, two-minute completed
   claim lease and named minute `pg_cron` ciphertext-erasure job.
3. Add legacy, `search-production-health.yml@refs/heads/main` and
   `search-release-qualification.yml@refs/heads/main` to the broker workflow
   allowlist; set the exact event allowlist to
   `workflow_dispatch,schedule,repository_dispatch`; verify the three cached
   health personas plus distinct `search-cold-browser` account.
4. Generate/check the immutable `event-search` source digest, deploy
   `supabase/functions/event-search` from exact merged `origin/main` via the
   pinned Supabase CLI, and verify the side-effect-free HEAD contract exposes
   that exact `X-KenigEvents-Search-Revision` separately from the stable
   contract version.
5. Deploy exact merged `origin/main` through `scripts/deploy_fly_main.sh` with
   validation profile `none`.
6. Run manual `browser_android`, then manual `browser_ios`. Debugging runs are
   allowed by the owner, but every run remains one Search POST per selected
   platform and each failure is investigated rather than blindly retried.
7. Before enabling automation, require both platform sets HEALTHY/PASS and exact
   equality of target fingerprint/immutable tuple, site runtime SHA, backend
   revision and content/index generation ids across the accepted proofs.
8. Set `SEARCH_PRODUCTION_HEALTH_ENABLED=true`, then update the Search incident
   regression record and issue #431 with exact run IDs.

Current disposition: `STAGE2_IMPLEMENTED_LIVE_ACCEPTANCE_PENDING / PRODUCT_HEALTH_UNCONFIRMED`.

Steps 1–5 were completed from exact merged `main`: the v2 broker migration and
minute cleanup are active, broker event/workflow allowlists and distinct
personas were verified, Edge HEAD exposes
`sha256:7ab0bd272925e959531bdcf679e995c65ee672165bd05788d51f7d13d35875d7`,
and Fly reports the baked merge SHA with ready health/DB/disk checks. First
diagnostic run `31330520373` proved one real browser Search POST, five matching
cards, zero LLM/pagination and 9,162 observed bytes, then false-failed on two
decorative subresource failures. Android booted but `/bin/sh` rejected
`pipefail` before Appium or Search. Both causes now have deterministic fixes;
clean live acceptance is still pending.

Runs `31333074131` and `31333753972` each completed the full browser journey as
`HEALTHY/PASS` with one HTTP-200 Search POST, five response IDs equal to five
cards, real scroll, event route HTTP 200, zero LLM/pagination/receipt/storage
and less than 10 KiB observed Supabase traffic. Their Android cells completed
transport preflight and product authorization but stopped before Search on an
unclosed Auth network byte record. Diagnostic run `31334260547` then proved the
exact ChromeDriver boundary was `mixed_request_only`: response/terminal events
were absent even though product Auth completed. The Android adapter now uses an
allowlisted pre-document page observer for received Auth bytes and removes it
after callback; no body, URL, request id, token or session reaches evidence.
The same run's browser failure was one Edge-class plus one RPC-class request;
closed evidence intentionally omitted their paths. Only the three exact losing
disposable capability-probe paths are now excluded from product-network
failure, while final operations remain strict. The next live run must prove
whether this was that expected race. Neither cell from `31334260547` is
terminal acceptance, so the disposition remains unchanged pending a fresh
merged-SHA browser+Android proof.

Merged-SHA run `31335122827` then completed browser `HEALTHY/PASS` with one
HTTP-200 Search POST, five matching cards, real scroll/event HTTP 200, zero
console/network errors and 9,343 bytes. Android stopped before callback/Search:
Appium returned 404 for the legacy ChromeDriver
`/chromium/send_command_and_get_result` extension. The checked Appium base
contract registers `/session/:sessionId/goog/cdp/execute`; the adapter now uses
that route and exposes route/receipt absence only as typed Android
infrastructure. A fresh main-SHA browser+Android run remains required.

Follow-up run `31331011185` then reached neither Search path: browser was
correctly typed `UNKNOWN_AUTH_BROKER` because the completed prior claim still
held the original 20-minute persona lease, and the Android action again split
the inline script into separate `/bin/sh -c` commands. The next patch uses a
single checked-in Bash wrapper and shortens only a successfully completed
claim to the two-minute replay window; uncompleted owners remain fail-closed.

The current hardening pass additionally proves one physical POST is observed
once from pre-Auth through event navigation, accepts bounded cache-write
telemetry outcomes, rejects real skeleton/placeholder UI,
aligns the Appium preflight/diagnostic receipt, preserves failed-journey bytes,
gates cleanup, rereads pointer on failure, suppresses superseded issue mutation,
keeps pre-runner UNKNOWN streaks, refuses missing-artifact BROKEN proof, pins
Appium drivers, makes full qualification synchronous, and provides a bounded
encrypted durable broker idempotency replay window with the full SQL identity
and minute ciphertext erasure. It also verifies the
active exact Edge source revision with a side-effect-free HEAD before
Auth/Search, rechecks the revision on the one already-received Search response
to close the HEAD-to-POST race, enables
the iOS Safari console bucket, rejects mobile redirect chains, and keeps an
adapter-level physical Search observer alive through final event-page
diagnostics so the complete journey proves exactly one Search POST. Unknown
pre-runner cells retain the exact sanitized summary schema with explicit
closed null/zero values. Mobile protocol receipts also ingest CDP
`redirectResponse` for the document chain and correlate terminal
`loadingFinished.encodedDataLength` when Content-Length is absent. Direct and
relay transport probes, discarded retries, Auth/RLS, Search and post-navigation
traffic are counted exactly once; pending measurements and the hard cap are
closed before the one Search dispatch. When Android ChromeDriver supplies only
Auth request starts, a pre-document allowlisted fetch observer closes the
received-byte measurement inside the page and exports only totals/counters.
Losing disposable capability-probe aborts are excluded from product errors but
any received bytes remain metered. These are deterministic results;
live acceptance remains `0 / 2`; activation changed only the explicitly listed
migration, Edge and exact-main Fly release state, while scheduled health remains
default-off.
