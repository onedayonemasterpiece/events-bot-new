# search-stage2-exact-tip-blockers — Results

## Scope

- **Lane:** `search-stage2-exact-tip-blockers`
- **Base SHA:** `dbdb2313233f0e10c4b60f40eeb80f6debeaf8c4`
- **Implementation SHA:** `311e92842a5e955ed0a1a14596dc925fe4897b3a`
- **Requirements:** B1, B2, B3

## Outcome

- **B1 — Done.** Mobile CDP Search accounting now keys each `Network.requestWillBeSent` dispatch by sanitized `requestId` plus CDP monotonic timestamp. Redirect hops that reuse a request ID count as separate physical POSTs, while replayed log entries remain idempotent. No URL, body, or query is retained.
- **B2 — Done.** Failed-cell retention now performs the final diagnostics/driver-log drain before persistent and physical evidence snapshots. A Search POST arriving in that last Appium drain is preserved in authoritative whole-cell evidence rather than reported as zero/stale.
- **B3 — Done.** Missing, unready, and invalid current-accepted receipts are retried with bounded attempts/delay before adapter construction, Auth issuance, or Search. Exhaustion returns terminal `BLOCKED_RELEASE_NOT_ACTIVE`; rejected pins reset only until a valid immutable target is pinned. The live CLI also writes blocked evidence instead of escaping as a top-level UNKNOWN.

## Red-before / green-after evidence

Before implementation, focused regressions reproduced:

- redirect chain: expected 2 physical POSTs, observed 1;
- unavailable initial accepted receipt: first resolver error escaped top level after one read;
- failed mobile evidence: physical count sampled as 0 before final diagnostics revealed count 2;
- explicit initial resolver wait: unready receipt escaped instead of retrying.

After implementation:

```text
node --experimental-strip-types --test --test-name-pattern='explicit release wait|unavailable initial accepted|redirect chain|drains final diagnostics' tests/search-production-health-mobile-preflight.test.mjs tests/search-production-health-journey.test.mjs
# 5 passed, 0 failed

npm run test:search-production-health
# 131 passed, 0 failed

npm run test:search-e2e-harness
# 31 passed, 0 failed

git diff --check
# PASS
```

## Changed files

- `site/e2e/mobile-web/appium-network-receipt.mjs`
- `site/e2e/search/production-health-run.mjs`
- `site/e2e/search/production-health-target.mjs`
- `site/tests/search-production-health-journey.test.mjs`
- `site/tests/search-production-health-mobile-preflight.test.mjs`
- `.codex/lanes/search-stage2-exact-tip-blockers/RESULTS.md`

## Risks / limitations

- The redirect-hop identity relies on the CDP `timestamp` present in real `Network.requestWillBeSent` events. The compatibility fallback distinguishes one timestamp-less redirect from the initial request but intentionally does not retain raw request data.
- No browser, live Supabase, deploy, workflow, broker, reporter, SQL, docs, or CHANGELOG changes/runs were performed, per lane scope.
