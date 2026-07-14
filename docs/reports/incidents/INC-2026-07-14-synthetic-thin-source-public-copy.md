# INC-2026-07-14 Synthetic thin-source public copy

Status: monitoring
Severity: sev2
Service: Smart Update / Telegram event publishing / Telegraph / managed VK / event vector sidecar
Opened: 2026-07-14
Closed: —
Owners: events-bot maintainer / Codex
Related incidents: `INC-2026-06-24-future-event-date-default-venue-regressions.md`, `INC-2026-07-11-event-vector-sidecar-sync-stalled.md`, `INC-2026-07-13-runtime-logging-recurring-event-quality.md`
Related docs: `docs/features/smart-event-update/README.md`, `docs/features/tg-publishing/README.md`, `docs/features/unsigned-personalization/semantic-vector-retrieval.md`, `docs/operations/sqlite-db-init.md`

## Summary

The organizer of `Летний Экодвор` reported that event `6767` had fluent but
uninformative or invented AI copy. The defect reached every managed public projection:

- Telegraph: `https://telegra.ph/Letnij-EHkodvor-07-08`;
- Telegram: `https://t.me/kldevents/2243`;
- VK: `https://vk.com/wall-231920894_7008`;
- the event vector sidecar used the same contaminated facts and also labelled unknown admission
  as `ticketed`.

A Tinkoff support/donation URL was incorrectly presented as registration. Telegram additionally
invented a four-tyre collection limit, and VK accumulated multiple historic AI bodies instead of
replacing them. The canonical source set is `@ecoklgd/3315`, `/3317`, `/3320` and
`@ecodvor39/926`, `/927`.

## User / Business Impact

- The organizer publicly stated that the programme was wrong and that the registration link was
  actually a donation link.
- Visitors received false logistics and low-information prose on three public channels.
- Public output could become circular evidence after a restart, weakening later Smart Update and
  vector decisions.

## Detection

- Detected from the organizer complaint and the user-provided Telegraph link.
- Authenticated VK inspection confirmed comment `7421`; public Telegram/Telegraph inspection and
  production SQLite confirmed the same event and wrong ticket semantics.
- Runtime file logging was enabled at `/data/runtime_logs` with 48-hour retention. The initial
  8/12 July generation had expired, but current repairs, provider calls, jobs and deploys were
  fully observable.
- A final DB verification found that the startup legacy backfill had recreated the managed VK
  projection as `event_source` at `2026-07-14 14:17:25`. The full-catalog audit found `3577`
  such projection rows and no dependent facts.

## Timeline

- 2026-07-14 13:30Z–14:10Z — source/public/DB evidence collected; event `6767`, wrong donation
  admission and unsupported public claims confirmed; narrow production backups created.
- 2026-07-14 14:10Z–14:24Z — canonical event and all public projections repaired; Telegraph,
  Telegram, VK, ICS and vector jobs completed. Telegram's final public hook used one successful
  Gemini Lite request.
- 2026-07-14 14:25Z — organizer complaint acknowledged from the managed VK group after the public
  fix (reply comment `7466`).
- 2026-07-14 14:32Z–14:34Z — vector admission facet fix deployed; compensating vector run `3767`
  completed with no call-cap remainder.
- 2026-07-14 14:41Z–14:42Z — final SQLite startup backfill fix deployed; restart purged `3577`
  managed projection rows while retaining five organizer sources for event `6767`.
- 2026-07-14 14:44Z — August month page catch-up rebuilt and public text verified.

## Root Cause

1. Smart Update rich-fact and merge schemas accepted free-form facts without source quotations.
   A thin teaser therefore acquired inferred purpose/format/series claims which later writers
   treated as canonical.
2. The Telegram public writer returned unstructured prose, so a fluent unsupported sentence could
   pass style/length checks.
3. Telegram Monitor and server link inference treated a sole external URL as a ticket URL; support
   and donation context was not negative evidence.
4. Managed VK edits appended historic bodies. Telegraph and Smart Update backfill guards were
   fixed first, but `Database.init()` had a third legacy backfill path that reinserted every
   `event.source_vk_post_url` on restart.
5. Vector tags used `not free => ticketed`, conflating unknown admission with a positive ticket
   contract.

## Contributing Factors

- The initial organizer teaser intentionally promised details later, so a writer optimized for a
  full card had strong pressure to fill missing content.
- Later source posts contained concrete programme details, but the prior unsupported facts remained
  durable and public.
- VK publication and evidence identities were stored in adjacent legacy fields.
- The August month rebuild exceeded a single Telegraph page and the queued job was marked stale;
  a direct compensating rebuild was required.

## Automation Contract

### Treat as regression guard when

- changing Smart Update rich-fact extraction, merge facts, fact-first description generation or
  sparse-source behavior;
- changing Telegram public event writer models, schema, fallback budget or source bundle;
- changing Telegram/VK admission-link extraction;
- changing VK managed post edit behavior, `event_source` backfill or SQLite startup migration;
- changing event vector admission facets or incremental vector sync.

### Affected surfaces

- `smart_event_update.py`, `llm_source_grounding.py`, `main_part2.py`;
- `source_parsing/telegram/handlers.py` and Telegram Monitor prompt/schema;
- `main.py` Telegraph/VK publication paths and `db.py::Database.init`;
- Fly SQLite `event`, `event_source`, `event_source_fact`, `joboutbox`, `ops_run`;
- personalization Supabase `event_search_documents` / `event_embeddings`;
- Telegraph, `@kldevents`, managed VK and month-page projections.

### Mandatory checks before closure or deploy

- Thin-source and later-source replay must reject unsupported purpose/format/tyre claims and retain
  only facts with an exact contiguous `evidence_quote`.
- Sparse input must be allowed to produce one or two honest paragraphs without forced filler.
- Telegram public sentences must be generated only by Gemini Lite or the persisted `gpt-4o`
  emergency lane capped at 100/day; every sentence must pass exact organizer-evidence validation.
- A sole donation/support URL must not become `ticket_link`; an explicitly labelled registration
  URL is the required positive control.
- Managed VK output must be excluded from every legacy source backfill path and removed on restart;
  external organizer VK sources must remain.
- Existing managed VK body updates must replace, not append, stale copy.
- Unknown admission must not emit the `ticketed` vector tag; explicit ticket/registration/phone
  admission remains a positive control.
- Verify canonical DB, Telegraph, Telegram, authenticated VK, month page and both vector document
  hashes after repair.

### Required evidence

- Deployed SHA reachable from `origin/main` and passing Fly health check.
- `artifacts/codex/INC-2026-07-14-synthetic-thin-source-copy/prod-verify-after.json`.
- Public Telegram/Telegraph/VK final artifacts and organizer reply artifact in the same directory.
- `vector-verify-6767-final.json`, successful `ops_run=3767`, and no provider call-cap remainder.
- Startup purge evidence with `managed_event_source_count=0`, backup count `3577`, and SQLite
  `quick_check=ok`.

## Immediate Mitigation

- Backed up the touched event/source/fact/outbox rows, then repaired event `6767` to `8 August,
  14:00–17:00`, `Железнодорожные ворота`, and removed the donation URL from ticket fields.
- Rebuilt the description from later organizer posts: forest specialist/topic vote, four topic
  choices, reusable-material collection and collection-owner pickup condition.
- Edited the existing Telegraph, Telegram and VK posts in place; retained four source-grounded
  photos and rebuilt ICS/month surfaces.
- Replied to the organizer complaint after verification.
- Backed up all `3577` managed VK projection source rows before the startup cleanup; none had
  dependent source facts.

## Corrective Actions

- Smart Update fact extraction and merge now require `{fact, evidence_quote}` and fail closed through
  a narrow exact/lexical grounding verifier. Sparse descriptions no longer require filler headings.
- Telegram public copy now uses a structured sentence/evidence schema with Gemini Lite as the normal
  writer and only the existing persisted capped `gpt-4o` emergency lane as fallback; no deterministic
  or lower-class model writes public copy.
- Donation/support links are negative evidence in Telegram Monitor and server import; the unsafe
  sole-URL ticket fallback was removed.
- Managed VK projections are excluded from Smart Update, Telegraph and SQLite startup backfill;
  startup also purges historic managed projection rows.
- VK edits replace stale body text.
- Vector admission facets fail closed on unknown admission and the production sidecar was rebuilt.

## Follow-up Actions

- [ ] Add the exact five organizer source artifacts to a closure-grade shadow replay through the
  Telegram import boundary and `smart_event_update.py`, including unsupported-fact and explicit
  registration negative/positive controls.
- [ ] Persist `evidence_quote` alongside accepted `event_source_fact` rows so later audits can prove
  the grounding decision without relying only on runtime retention.
- [ ] Monitor the next thin-source enrichment/import and one production restart for zero managed VK
  source reappearance before moving from `monitoring` to `closed`.

## Release And Closure Evidence

- deployed SHAs (all reachable from `origin/main`):
  - `33bc4115` — source-grounded facts, sparse writer, donation admission and public writer contract;
  - `fdb40682` — Telegraph managed-output exclusion;
  - `e4e9c4bf` — native Google JSON schema contract;
  - `0d4ff178` — VK body replacement;
  - `26c9b84a` — unknown-admission vector facet fix;
  - `7aa00646` — SQLite startup exclusion/purge.
- final deploy: `registry.fly.io/events-bot-new-wngqia:deployment-01KXGH58PCQV4EX0W24X2713HF`,
  machine version `1666`, one passing Fly check, `/healthz ready=true`, SQLite `db=ok`.
- regression checks:
  - incident-focused run: `164 passed`; nine unrelated date-sensitive tests still hard-code now-past
    April/June 2026 dates and fail through existing past-event guards;
  - DB/vector startup subset after the final root fix: `12 passed`;
  - changed-module `py_compile` and `git diff --check`: passed.
- provider evidence:
  - final Telegram public hook: one successful `gemini-3.1-flash-lite` call, 3622 input / 297 output
    tokens; final publication did not use `gpt-4o`;
  - vector catch-up `ops_run=3767`: 276 documents, 41 changed embeddings, 511 unchanged, 41 provider
    calls, `not_embedded_due_call_cap=0`.
- production verification:
  - canonical DB has no ticket link, no bad facts, five organizer sources, no managed projection
    source, and `quick_check=ok`;
  - Telegraph/Telegram/VK have no tyre limit, donation/ticket CTA or old generic body;
  - VK has four attachments and organizer reply `7466`;
  - vector `search_v3` and `related_v1` hashes match their documents and `ticketed` is absent;
  - August month page contains `Летний Экодвор`, `8 августа`, `14:00`, and no bad claim.

## Prevention

The semantic path is LLM-first: small structured extraction/writer calls make the meaning decision,
while deterministic code only validates exact evidence, enforces provider/budget contracts and
prevents publication/evidence identity loops. The vector path remains vector-first for recall and
retrieval, but vector tags and persisted hashes are source-grounded and fail closed on unknown
admission; vector similarity never authorizes semantic invention.
