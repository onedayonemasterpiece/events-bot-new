# Shared Live resources and KenigEvents Live Search

KenigEvents uses the shared `live-interaction` transport and the shared
`ai-resource-control` authority. It does not contain a second WebSocket/audio
implementation or a second quota database.

## Runtime contract

- `live-interaction v0.1.4` is the transport/session dependency. The provider
  checks `resource_guard` before connect/setup/send/receive, pins one key for
  the session/resumption lifetime, treats resource failures as terminal, and
  the Python host owns client-liveness cleanup.
- `ai-resource-control v0.1.2` extends the existing dedicated Google AI
  limiter Supabase with fenced expiring Live leases. The private wheel is built
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

The application integration is implemented, but the production Live lease SQL
must exist in the existing dedicated limiter Supabase before
`ENABLE_STATIC_SITE_LIVE_SEARCH=1` is enabled. Never use product Supabase or
create a second limiter.

Provider facts are not inferred from ordinary Flash quotas. Owner AI Studio
readback dated 2026-09-24 records Gemini 3.8 Live and Extended Thinking as
RPM Unlimited / TPM 65K / RPD Unlimited. The dashboard does not expose a
guaranteed concurrent-session entitlement, so ai-resource-control records
concurrency as not_exposed rather than guessing a number. Finite local safety
ceilings still apply: v0.1.2 aligns all four consumers to six shared project
slots with one lease per opaque product binding while per-scope policies remain
the real capacity boundary.

Current operator access discovery on 2026-09-26 confirmed the dedicated limiter
service-role credential, the existing six key-to-quota-scope registry rows and
all six corresponding runtime keys. The Live resource migrations are not
present yet (the ai_resource_* REST/RPC surface returns 404). DevCoveer does not
currently have usable management/SQL authorization to that separate Supabase
account, and the current ChatGPT Supabase account must not be used. Therefore
production Live remains fail-closed until migrations 001-004 are applied through
the correct limiter-account SQL/management path.

After migration, read back the generated ledger id and set
`AI_RESOURCE_LEDGER_ID` on the backend. Configure/verify key-state and policy
rows using actual provider evidence; do not reset ordinary counters or registry
seeds.

## Daily canary

`.github/workflows/live-search-daily-canary.yml` runs once per day (plus
manual dispatch). It uses the existing no-mail auth-session broker, opens a
real `gemini-3.8-live` session, sends one text request, requires an actual
`search_events` function call and non-empty `search_results`, then stops the
session and stores only a bounded sanitized receipt.

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
