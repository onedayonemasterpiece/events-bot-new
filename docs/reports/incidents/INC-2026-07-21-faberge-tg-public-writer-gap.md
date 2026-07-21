# INC-2026-07-21 Faberge Telegram Public Writer Gap

Status: open
Severity: sev2
Service: Telegram event publishing (`@kldevents`) / grounded public writer
Opened: 2026-07-21
Closed: —
Owners: Codex / events-bot operator
Related incidents: `INC-2026-06-12-tg-event-utility-hook-quality`, `INC-2026-06-07-tg-event-publishing-media-calendar-dedup`, `INC-2026-07-14-synthetic-thin-source-public-copy`
Related docs: `docs/features/tg-publishing/README.md`, `docs/llm/request-guide.md`, `docs/operations/runtime-logs.md`

## Summary

Canonical event `6991`, the 25 July lecture about Agathon Faberge, reached Telegraph, calendar and the managed VK group but did not reach `@kldevents`. Its `tg_event_publish` outbox job exhausted four attempts because both the normal Gemini Lite writer and the strict `gpt-4o` fallback returned payloads rejected by the exact-source grounding boundary. The boundary correctly failed closed, but the publisher had no final safe recovery path, circuit breaker or targeted operator alert, so a valid current event remained unpublished.

The investigation also found two non-causal projection defects: Telegram Monitoring scanned `kraftmarket39/349` but lost that source attachment during a SQLite lock, and the event DB retained stale managed VK URL `wall-231920894_7780` although the actual live post is `wall-231920894_7784`.

## User / Business Impact

- Subscribers to `@kldevents` did not receive the Faberge lecture announcement.
- The same terminal writer error currently affects ten active canonical events dated today or later, so the impact is systemic rather than event-local.
- The event is present in managed VK at `https://vk.com/wall-231920894_7784`, but the stored URL points to a non-resolving postponed/live predecessor ID.
- Repeated hourly retries consume writer capacity without producing a publishable post or actionable operator notification.

## Detection

- The operator noticed that `https://t.me/kraftmarket39/349` had no corresponding `@kldevents` post and requested a VK check.
- Authenticated Telegram history confirmed no Faberge post in the recent `@kldevents` messages.
- Production DB and runtime-log correlation identified outbox job `39244`, event `6991`, and all four grounding rejections.
- Runtime file logging was enabled (`ENABLE_RUNTIME_FILE_LOGGING=1`); active and rotated files under `/data/runtime_logs` retained the relevant evidence.
- Observability gap: logs preserve the rejected sentence and reason, but not the complete Lite/4o JSON payload and submitted `evidence_quote`, which makes exact replay and prompt diagnosis harder.

## Timeline

- 2026-07-20 14:06:38 UTC — `@kraftmarket39/349` published the lecture announcement.
- 2026-07-21 00:57:50 UTC — canonical event `6991` was imported from `@koihm/5899`; later sources include `@museum39/4231` and `@kulturnaya_chaika/8070`.
- 2026-07-21 01:03:47 UTC — Telegram Monitoring scanned `@kraftmarket39/349`, extracted one event, but duplicate merge/source attachment failed with `sqlite3.OperationalError: database is locked`; recovery run `4297` ended `partial`.
- 2026-07-21 01:50:30 UTC — the managed VK announcement became live as `https://vk.com/wall-231920894_7784`; DB/outbox evidence still referenced `..._7780`.
- 2026-07-21 06:44:52 UTC — first Telegram public-writer attempt was rejected with `quote_not_in_source`.
- 2026-07-21 06:55:44 UTC — second attempt was rejected with `quote_not_in_source`.
- 2026-07-21 06:57:50 UTC — third attempt was rejected with `quote_not_in_source`.
- 2026-07-21 07:19:10 UTC — fourth attempt was rejected with `insufficient_lexical_support`; outbox row remained `error`, attempts `4`.
- 2026-07-21 — investigation confirmed `tg_event_post_url` and `tg_event_post_id` are null and no public repair has yet been attempted, per operator request to establish cause first.

## Root Cause

1. `build_tg_event_hook_text()` asks the normal Lite writer for claims backed by an exact contiguous organizer-source `evidence_quote`; each Lite response for this event failed local grounding validation.
2. The strict `gpt-4o` fallback also returned paraphrased/combined evidence or claims whose lexical support was insufficient. Both provider calls succeeded, so this was not a provider outage or Telegram authorization failure.
3. `source_contains_quote()` / `claim_is_grounded()` correctly rejected unsupported copy, but the two-stage writer path has no bounded repair pass that forces an exact source substring, nor a safe source-grounded terminal fallback.
4. After both writer stages fail, `tg_event_publish` only raises and retries. A valid event can therefore remain unpublished indefinitely while its other public projections succeed.

## Contributing Factors

- The writer prompt and schema state the exact-quote contract, but generation is not constrained enough to prevent paraphrased `evidence_quote` values.
- Full rejected model payloads are not durably recorded, reducing diagnostic precision.
- There is no dedicated alert or backlog health signal for current/future `tg_event_publish` rows repeatedly failing strict grounding.
- A separate SQLite writer lock prevented `kraftmarket39/349` from being attached as an additional source, although event `6991` already had sufficient organizer evidence and this did not cause the public-writer failure.
- VK postponed-to-live ID drift left stale `event.source_vk_post_url`, masking the actual live managed post during a DB-only check.

## Automation Contract

### Treat as regression guard when

- Changing `tg_event_publish`, `build_tg_event_hook_text`, `_tg_event_source_evidence`, `_parse_tg_event_hook_payload`, or `_build_tg_event_hook_via_4o`.
- Changing `source_contains_quote`, `claim_is_grounded`, public-writer prompts/schemas, retry policy, or writer observability.
- Replaying or repairing event `6991` or the current strict-writer failure cohort.

### Affected surfaces

- `main_part2.py` Telegram public-writer and strict fallback path.
- `llm_source_grounding.py` exact-quote and lexical-support validation.
- `tg_event_publish` JobOutbox scheduling/retry/alerting.
- Production event fields `tg_event_post_id`, `tg_event_post_url`, `source_vk_post_url`.
- Telegram `@kldevents`, managed VK group `-231920894`, and Telegram Monitoring source attachment.

### Mandatory checks before closure or deploy

- `pytest tests/test_tg_event_publish.py -q`
- `pytest tests/test_llm_source_grounding.py -q`
- Positive replay of event `6991` through the same public-writer boundary: every emitted claim must pass exact source grounding and reach the publish path.
- Negative control proving that an unsupported combined/paraphrased claim remains rejected.
- Audit and bounded replay of every active canonical today/future event with the same strict-writer terminal error; current investigation denominator is ten.
- Verify the repaired Faberge event has a live `@kldevents` URL and persisted Telegram post mapping.
- Reconcile or explicitly account for managed VK live URL `..._7784` versus stale stored URL `..._7780`.
- Verify source attachment recovery for `kraftmarket39/349` or document why the already attached organizer sources are sufficient.
- Follow release governance, deploy only a SHA reachable from `origin/main`, and verify `/healthz` after deploy.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Test output and positive/negative writer replay artifacts.
- Correlated outbox/runtime evidence showing no remaining current/future strict-writer terminal cohort.
- Live Telegram and VK URLs plus DB mappings for event `6991`.
- Production health and post-deploy log evidence.

## Immediate Mitigation

- None applied yet. Investigation was intentionally completed before repair or requeue.

## Corrective Actions

- Pending implementation and review.

## Follow-up Actions

- [ ] Add a bounded LLM-first repair stage that must select/copy an exact organizer-source quote while preserving fail-closed grounding.
- [ ] Persist redacted rejected writer payload/evidence metadata and emit an actionable terminal-failure alert.
- [ ] Cap unproductive retries and provide an operator-visible recovery state rather than silent hourly repetition.
- [ ] Replay and verify the ten current/future affected events after the writer fix.
- [ ] Publish event `6991` to `@kldevents` and persist its live post mapping.
- [ ] Repair `kraftmarket39/349` source attachment and reconcile the stale managed VK URL.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks: investigation only; no behavior change deployed
- post-deploy verification: —

## Prevention

- Open pending corrective actions above; incident remains active until the public-writer path, affected backlog and event `6991` are repaired and verified.
