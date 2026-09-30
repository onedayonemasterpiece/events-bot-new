# Shared Live resources and KenigEvents Live Search

KenigEvents uses the shared `live-interaction` transport and the shared
`ai-resource-control` authority. It does not contain a second WebSocket/audio
implementation or a second quota database.

## Runtime contract

- WSS integration candidate: `live-interaction v0.3.7-rc.1`; exact archive SHA-256
  is recorded in `site/package.json.liveFramework` and Python requirements. The provider
  checks `resource_guard` before connect/setup/send/receive, pins one key for
  the session/resumption lifetime, treats resource failures as terminal, and
  the Python host owns client-liveness cleanup.
- `ai-resource-control v0.1.4` extends the existing Google AI authority
  Supabase with fenced expiring Live leases. The private wheel is built
  by the trusted Fly deploy script and never committed to this public repo.
- `google_ai/live_resources.py::run_live_search` is the only KenigEvents
  managed Live resource entry. It uses consumer `kenigevents` and a hash of
  the server-authorized Live session as the binding.
- Existing `GoogleAIClient`, ordinary event-search quota, Edge vector search
  and Kaggle paths keep their previous reserve/mark/finalize contracts.

## Browser product

When `PUBLIC_STATIC_SITE_LIVE_SEARCH_URL` is configured, `/poisk/` uses one
conversational surface rather than preserving the legacy search form:

1. Yandex/Supabase authentication remains the existing product authentication.
2. A large round microphone and the text composer are equal entry points into
   the same Live session.
3. Provider `input_transcript` is rendered immediately as the user's message;
   `output_transcript` is rendered as the assistant message while the same
   provider audio is played.
4. A pending assistant turn shows a text skeleton and canonical event-card
   skeletons; it is not represented by an isolated spinner.
5. The model must call `search_events` before claiming facts about events. The
   tool invokes the existing Supabase vector-search contract with LLM
   verification enabled and no discovery fallback for this conversational
   surface.
6. The model receives a bounded factual JSON view of verifier-confirmed cards
   and presents the result in text/audio: total result count plus normally 2–3
   particularly relevant choices with a concrete reason grounded in the
   returned fields. The full result is rendered as the canonical EventCard grid
   (three cards per row on desktop, one on mobile).
7. Further text or voice refinements append new messenger-style turns in the
   same session. The browser scrolls to the new user turn / assistant answer
   rather than replacing the previous result.
8. A completed result keeps the Live session open for the bounded follow-up
   window (currently roughly 15 seconds). Turning the microphone off does not
   itself destroy the conversation.
9. Page exit sends a keepalive Stop. If that request is lost, the server
   client-liveness watchdog closes the session and the resource lease expires
   fail-closed.

Reload intentionally discards conversational state in the current implementation.
No provider key, model selector, resource receipt or user identifier is stored
in the static page. Personal interests are not claimed as ranking input until a
real personalization context is explicitly connected.

If the Live URL is absent, the older direct event-search implementation remains
a compatibility path for non-Live builds. A configured Live path never silently
switches to that path after a Live failure.

## Cross-project WSS contract

The migrated browser uses the shared `wl-live-v1` transport: authenticated HTTPS
bootstrap, one-use 15-second subprotocol ticket, binary PCM and pushed events.
Socket URLs resolve against the API origin, not the static-page origin; foreign
origins, query credentials and reused tickets fail closed. Ping/pong maintains
client liveness without events polling. A WSS session cannot fall back to HTTP input.

EventsBot remains one Python/aiohttp service, using the same framework relay as
Street Story/FastAPI. Its `static_site_live_socket.py` is only an authorization/I/O
adapter, not an audio engine. Wonderful Lections keeps the Node framework binding.
No Node sidecar, broker or shared cross-product outage domain is added. See the
pinned framework's `docs/native-wss.md` for the runtime decision and tradeoffs.

Startup capture remains disabled until hello acknowledgement; there is no promise
of seamless audio replay. Real browser/public-TLS/search acceptance is required
before production activation. Historical HTTP acceptance below does not prove WSS.

## HTTP surface

EventsBot exposes an authenticated same product boundary:

- `POST /api/live-search`
- `POST /api/live-search/{session_id}/socket-ticket` (same authorized user)
- `GET /api/live-search/{session_id}/socket` (WebSocket upgrade; one-use ticket)
- `POST /api/live-search/{session_id}/input`
- `GET /api/live-search/{session_id}/events?after=N`
- `POST /api/live-search/{session_id}/stop`

HTTP routes bind the session to the same authorized Supabase token/user.
WSS inherits that binding through its single-use ticket. Legacy HTTP input
and events endpoints remain explicit compatibility paths, not automatic fallback.
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

The backend serving `PUBLIC_STATIC_SITE_LIVE_SEARCH_URL` runs through the
pinned `ai-resource-control v0.1.4` central authority. The static browser never
receives an authority service credential or a Google key.

### Authority outage policy

KenigEvents Live passes only central authority configuration (and the optional
ledger id) to the shared resource controller. Host-local `GOOGLE_API_KEY*`
values and `AI_RESOURCE_CONTROL_FALLBACK_KEY` are deliberately excluded from
the Live resource environment. If the authority is unavailable, a new Live
session fails closed; admission/quota/capacity/credential decisions are never
converted into a direct provider-key path. This keeps the deployed Search on
one resource-control contract rather than maintaining an alternative runtime.

### Technical debt: provider-independent conversational fallback

The current product release has exactly one conversational provider:
`gemini-3.8-live`. A provider outage is therefore a product availability risk.
The required follow-up architecture is already fixed, but is **not implemented
in this release**:

- the browser conversation owns provider-neutral turns
  (`user_transcript`, `assistant_text`, `search_results`, audio/listening
  state and `turn_complete`) rather than Google-specific message shapes;
- a future server adapter may bind the same conversation to another
  Live-capable model;
- a lower-capability fallback may run text dialogue plus the existing verified
  vector search with no realtime audio model, while preserving the same visible
  messenger history and EventCard result contract;
- fallback selection must be explicit and observable. It must not duplicate a
  user request after an outcome-unknown provider failure or silently lower
  relevance guarantees;
- this provider fallback is independent of resource authority. It is **not**
  permission to use a raw local Google key when `ai-resource-control` is down.

Until one of those adapters is implemented and accepted, Google Live outage
remains an explicit unavailable state rather than a hidden degraded mode.

Production activation was accepted on 2026-09-27: the Live HTTP surface was
enabled on EventsBot and the real no-mail canary completed a
`gemini-3.8-live` conversation with an actual `search_events` tool call,
eight returned cards, `has_more=true`, and explicit session release.

## Daily canary

`.github/workflows/live-search-daily-canary.yml` runs once per day (plus
manual dispatch). It uses the existing no-mail auth-session broker, opens a
real `gemini-3.8-live` session, sends one text request, requires an actual
`search_events` function call and non-empty `search_results`, then stops the
session and stores only a bounded sanitized receipt.

Scheduled daily runs are enabled after the 2026-09-27 production acceptance via `LIVE_SEARCH_DAILY_CANARY_ENABLED=1`; manual dispatch remains available for release acceptance and diagnosis.

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

Owner provenance: `voice-20260926-180531-a07ef741` and detailed Search review `voice-20260927-114256-b9508e62` in IdeaHub.
