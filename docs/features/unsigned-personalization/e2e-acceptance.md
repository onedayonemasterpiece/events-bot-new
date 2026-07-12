# Personalization E2E acceptance and KPIs

> Status: **required release-test design; full integration/live suite is not implemented**.
> Existing assets cover a local MVP contract only: `tests/e2e/features/static_site_personalization.feature` and `tests/playwright/static_personalization_contract.spec.ts`.

## Goal

Make personalization observable and automatically debuggable end to end through Playwright, not only through isolated ranking tests. A release run must prove all four boundaries:

1. browser behavior is captured correctly in `localStorage` after consent;
2. eligible compact telemetry and profile updates reach the personalization Supabase/Postgres project exactly once;
3. the resulting profile represents the expected interests and exclusions;
4. a later feed/list/related surface actually applies that profile and brings a relevant event forward.

Gherkin owns readable behavior scenarios. Playwright owns real browser actions, localStorage/network assertions, screenshots/traces and visible UI results. Service-side test fixtures own DB assertions and cleanup; a Supabase secret/service key must never enter the browser context.

## Email-only browser persistence contract

A user who does not use Yandex must not re-enter the same email on every calendar save or visit.

- The first manual entry writes a versioned `ke_contact_email_v1` localStorage record with normalized email, masked display form, `pending|verified` status, source, and timestamps.
- Before verification, the cached address may prefill/resume the code/link flow but cannot authorize a reminder or mail send.
- After code/link verification, the record becomes `verified`; subsequent same-browser surfaces reuse it and show the masked address without asking for entry again.
- Supabase Auth/email control plane remains the durable identity and verification source of truth. localStorage is a convenience cache, not proof of ownership, consent, send eligibility or a replacement for server state.
- The cache contains no access/refresh token, provider secret, consent grant or email-control row. It is cleared only by explicit `Забыть почту`/profile reset, account deletion, or an incompatible schema migration; ordinary navigation/reload does not clear it.
- On a shared device, the UI must expose `Забыть почту`. A stale/pending/unverified cache never displays the promise that a D-1 reminder will be sent.

## Test layers

| Layer | Environment | What it proves | Must not be accepted as |
|---|---|---|---|
| Deterministic contract | routed Playwright fixture, fixed manifests | localStorage schema, consent, ranking, failure fallback, telemetry payload shape | DB integration or production quality proof |
| Integration E2E | clean local/static build + isolated personalization Supabase test namespace/project | browser → ingest/RPC → DB rows/profile rollup → next browser feed | live provider/CDN proof |
| Release canary | current public canary build + dedicated test identities/personas | real CDN/auth/network/config path and visible personalization outcome | permission to write arbitrary production user data |

Every run records a unique `e2e_run_id`, persona id, build/catalog watermark, algorithm/taxonomy/profile versions and served-list ids. Playwright artifacts go to ignored `artifacts/codex/<run-id>/`; test DB rows are correlated and deleted/expired after evidence capture.

## Required scenario catalogue

| ID | Scenario | Browser/localStorage assertion | DB assertion | Application assertion |
|---|---|---|---|---|
| `PERS-EMAIL-001` | Manual email entered once, code/link verified, page reloaded and revisited | `ke_contact_email_v1` persists as verified; field is not requested again; masked address is shown | one verified identity, no duplicate recipient/profile | calendar reminder UI reuses the address |
| `PERS-EMAIL-002` | Yandex login returns no usable email, then manual verification | Yandex session survives; manual address is cached once | one linked identity/profile, no duplicate favorite/reminder | later surfaces reuse the verified address |
| `PERS-CONSENT-001` | Browse and act without personalization consent | no trusted profile mutation/remote queue | zero accepted personalization rows | static order and CTA remain usable |
| `PERS-COLLECT-001` | Consent, valid impression, dwell, detail open, like/save/calendar/share/hide | expected compact action/profile deltas with stable ids and versions | expected accepted rows once; duplicate resize/retry deduped | local profile changes immediately where allowed |
| `PERS-COLLECT-002` | Quick scroll, hover, invalid impression and bot/preview actor | weak/noisy signals do not gain strong weight | dropped/quarantined per contract | feed is not distorted by invalid signals |
| `PERS-PROFILE-001` | End session and run profile rollup | browser profile remains compatible | short/mid/long snapshot contains expected positive/negative facets and timestamps | profile becomes eligible for next request |
| `PERS-APPLY-001` | New session after profile update | restored local profile and session id are valid | served-list/result records reference current profile/algorithm | relevant candidates move forward; hidden candidates stay absent |
| `PERS-LINK-001` | Anonymous history → email/Yandex identity | local cache/profile is not duplicated | idempotent merge produces one profile/favorite/reminder state | ranking remains stable after login/logout/reload |
| `PERS-FAIL-001` | Supabase timeout/reject during collection | local fallback stays bounded and retry-safe | no partial/duplicate accepted rows | static/local feed and CTA keep working |
| `PERS-STORAGE-001` | Supabase reaches Orange/Red synthetic capacity band | bounded local queue; no retry storm | nonessential telemetry is rejected while consent/favorite/reminder-control writes remain available | static/local personalization and user-control UI keep working |
| `PERS-QUALITY-001` | Mature «Чайковский» persona sees a newly eligible Tchaikovsky concert | local profile contains source-grounded classical/composer affinity, not raw keyword noise | server snapshot reflects expected facets and strong actions | first eligible relevant event appears within 20 inspected cards |
| `PERS-QUALITY-002` | Strong preference plus explicit negative interests | positive and negative axes remain separate | profile weights/reasons are explainable | top 20 preserves relevance, exclusions and diversity without one-category collapse |

The catalogue must become executable Gherkin/Playwright work. Extend the existing `static_site_personalization.feature` for browser/local fallback, and add a clearly marked planned integration feature for DB/profile/feed boundaries only when its step implementation lands. Scenario IDs must be identical in Gherkin, Playwright test titles, DB fixtures and release evidence.

## Golden persona: «Чайковский»

The deterministic scenario forecasts and then verifies behavior rather than injecting the final profile directly:

1. Seed a catalog with source-grounded classical events, Tchaikovsky concerts, other composers and hard-negative non-classical events.
2. Across at least three sessions, simulate valid impressions plus strong positive actions on Tchaikovsky/classical events (detail+dwell, like, calendar/save or ticket click) and explicit negative actions on unrelated candidates.
3. Assert each action in the browser cache and the accepted/deduped DB evidence before running the profile rollup.
4. Assert that the profile gains controlled `classical_music`/composer affinity derived from canonical event features; raw title keyword matching alone is not acceptance evidence.
5. Add or activate a previously unseen Tchaikovsky concert, generate the next candidate set and start a clean session.
6. Count distinct validly inspected cards up to and including the first ground-truth relevant event/action. The count must be `<=20` when an eligible relevant event exists in the candidate pool.
7. Assert that already hidden events remain absent and diversity/fatigue guardrails still operate.

For deterministic release tests, a **mature persona** initially means at least `3` completed sessions, `30` valid impressions, `5` strong positive actions and `2` explicit negative actions with a current profile snapshot. These are versioned test-fixture thresholds, not a claim about the final production segmentation rule.

## KPI framework

### Primary KPI — cards to first relevant event

```text
cards_to_first_relevant =
  count(distinct valid card impressions up to and including
        the first ground-truth relevant event that receives a meaningful action)
```

- Release golden gate: every eligible mature-persona scenario, including «Чайковский», passes `cards_to_first_relevant <= 20`.
- Canary metric: report the distribution and share of eligible mature sessions with `<=20`; the production percentile/SLO is approved only after a baseline, but a regression may not be hidden by averaging cold-start/no-supply sessions together with mature eligible sessions.
- Denominator: sessions where at least one ground-truth relevant active event exists in the candidate pool. `no_relevant_supply` is a separate catalog/coverage metric.

### Primary reliability KPI — personalization collection success

```text
collection_success_rate = accepted valid consented action summaries
                          / emitted valid consented action summaries
```

Deterministic E2E expects `100%` for valid fixtures, `0` duplicate accepted rows on retry, and `0` trusted rows without consent. Canary reports acceptance, drop/quarantine reason and profile-rollup lag separately.

### Primary application KPI — profile-to-feed effectiveness

Measure the share of current profile updates that are reflected by the next eligible feed/list request, plus `MRR`/`precision@20` on the golden-persona pack. A passing storage write without a changed served list is not successful personalization.

### Drivers and guardrails

- drivers: profile rollup latency, compatible-profile restore rate, candidate coverage, relevant-event supply, strong-action capture rate;
- guardrails: `not_interested`/quick-skip rate in top 20, category/venue diversity, hidden-event recurrence, stale/cancelled leakage, fallback rate, page/CTA latency and no-consent privacy failures.

All metrics are segmented by `viewport_class`, `layout_mode`, surface, algorithm/profile/taxonomy version, mature vs cold-start and relevant-supply availability.

## Release evidence

A personalization feature cannot be marked production-verified until one automated evidence bundle contains:

- Gherkin scenario results and mapped Playwright test results;
- before/after localStorage snapshots with email/auth tokens redacted;
- network request/response correlation without secrets or plaintext email in logs;
- service-side DB assertions for accepted/deduped telemetry, profile snapshot and served-list application;
- storage-band/capacity evidence showing that disposable telemetry cannot crowd out durable control state;
- Tchaikovsky and other golden-persona metric calculations, including candidate-supply evidence;
- trace/screenshots for the visible personalized order and failure fallback;
- cleanup result and confirmation that test data cannot train/contaminate ordinary production profiles.
