# Shared Live resources and KenigEvents Live Search

KenigEvents uses the shared `live-interaction` transport and the shared
`ai-resource-control` authority. It does not contain a second WebSocket/audio
implementation or a second quota database.

## Runtime contract

- `live-interaction v0.1.4` is the transport/session dependency. The provider
  checks `resource_guard` before connect/setup/send/receive, pins one key for
  the session/resumption lifetime, treats resource failures as terminal, and
  the Python host owns client-liveness cleanup.
- `ai-resource-control v0.1.5` extends the existing Google AI authority
  Supabase with fenced expiring Live leases. The private wheel is built
  by the trusted Fly deploy script and never committed to this public repo.
- `google_ai/live_resources.py::run_live_search` is the only KenigEvents
  managed Live resource entry. It uses consumer `kenigevents` and a hash of
  the server-authorized Live session as the binding.
- Existing `GoogleAIClient`, ordinary event-search quota, Edge vector search
  and Kaggle paths keep their previous reserve/mark/finalize contracts.

## Browser product

The existing AuthorizedEventSearch UI is preserved. When
`PUBLIC_STATIC_SITE_LIVE_SEARCH_URL` is configured:

1. Yandex/Supabase authentication remains the existing product authentication.
2. Text submit or the microphone starts an authenticated Live session on the
   EventsBot backend.
3. The model must call `search_events`; the tool invokes the existing
   Supabase `event-search` contract and emits `search_results`.
4. The browser renders those results with the existing event-card renderer.
5. `Показать ещё` stays in the same Live session and asks for the next page.
6. After a completed result turn, the session remains available for roughly
   15 seconds for a follow-up, then stops automatically.
7. Page exit sends a keepalive Stop. If that request is lost, the server
   client-liveness watchdog closes the session and the resource lease expires
   fail-closed.

The recording indicator pulses only while the Live session is active/listening.
Reload intentionally discards conversational state. No provider key, model
selector, resource receipt, transcript or user identifier is stored in the
static page.

If the Live URL is absent, the pre-existing direct event-search orchestration
remains available as a compatibility adapter. A configured Live path never
silently falls back to it after a Live failure.

## HTTP surface

EventsBot exposes an authenticated same product boundary:

- `POST /api/live-search`
- `POST /api/live-search/{session_id}/input`
- `GET /api/live-search/{session_id}/events?after=N`
- `POST /api/live-search/{session_id}/stop`

Every route binds the session to the same authorized Supabase token/user.
Allowed browser origins are explicit. The static site remains static; it does
not connect directly to Google.

## Quota authority and rollout state

The canonical shared authority is deployed with migrations 001–008, six verified
Live scopes, bounded retention, server-registry acquire v2 and Vault-backed
lease-wrapped provider-key delivery. KenigEvents must use consumer
`kenigevents`; product Supabase is not a substitute and a second limiter is
forbidden.

Provider facts are not inferred from ordinary Flash quotas. Owner AI Studio
readback records Gemini 3.8 Live and Extended Thinking as RPM Unlimited / TPM
65K / RPD Unlimited while guaranteed provider concurrency remains not exposed.
Finite local safety ceilings remain authoritative.

The backend serving `PUBLIC_STATIC_SITE_LIVE_SEARCH_URL` must run through
`ai-resource-control 0.1.5` with the central authority. The static browser never
receives an authority service credential or a Google key.

### KenigEvents authority-outage fallback

KenigEvents owns exactly one emergency Live alias: `GOOGLE_API_KEY2`. The
server-side Live boundary may pass central authority configuration, optional
ledger id and that one alias to the shared SDK. It must not pass Wonderful
Lections' `GOOGLE_API_KEY`, Street Story's `GOOGLE_API_KEY3`, Projects Hub's
`GOOGLE_API_KEY4`, or shared reserve keys 5–6 as KenigEvents fallback.

The SDK may use `GOOGLE_API_KEY2` only when the initial read-only authority
capability probe returns `RESOURCE_CONTROL_UNAVAILABLE`, before any mutating
acquire. It must not activate after a successful authority probe, for
admission/quota/429/capacity/credential failures, after a lost mutating acquire
response, or after provider ready. Emergency mode uses the same
`live-interaction` resource guard, permits one local KenigEvents fallback
session per process and expires after two hours. If the assigned alias is
unavailable, the outage stays an explicit Live error rather than borrowing
another project's key.

## Daily canary

`.github/workflows/live-search-daily-canary.yml` runs once per day (plus
manual dispatch). It uses the existing no-mail auth-session broker, opens a
real `gemini-3.8-live` session, sends one text request, requires an actual
`search_events` function call and non-empty `search_results`, then stops the
session and stores only a bounded sanitized receipt.

Scheduled daily runs stay disabled until LIVE_SEARCH_DAILY_CANARY_ENABLED=1 after production cutover. Manual dispatch remains available for first acceptance.

The daily canary intentionally does not depend on a virtual microphone. Real
microphone/pulse/Stop acceptance is a release-time browser check; the daily
test targets the provider/session/quota/function-call path that can regress
without UI changes.

## Focused verification

```sh
pytest --noconftest tests/test_static_site_live_search.py -q
pytest --noconftest tests/test_shared_live_resources.py -q
node --test site/tests/live-event-search-controller.test.mjs
node --test site/tests/live-search-build-contract.test.mjs
node --test site/tests/preview-search-env.test.mjs
python scripts/inspect/audit_google_ai_provider_paths.py
```

Owner provenance: `voice-20260926-180531-a07ef741` in IdeaHub.
