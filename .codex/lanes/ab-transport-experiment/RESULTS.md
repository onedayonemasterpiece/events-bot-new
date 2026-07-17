# R01 — A/B/C contract for the transport timetable

Status: **implementation-ready design; no product code, canonical docs or database state changed**
Requirement: **R01 / transport timetable experiment**
Branch: `agent/static-site-production-pipeline-secret/ab-transport-experiment`
Worktree: `/home/dev/.codex/worktrees/events-bot-new/static-site-production-ab-transport`
Base and audited input HEAD: `2822a91d` (`origin/main` at lane start)
Result HEAD: the commit containing this file; obtain with `git rev-parse HEAD` after integration handoff.

## Executive decision

All three accepted treatments should be retained under one experiment key:

| Variant id | User-facing pattern | Role in experiment |
|---|---|---|
| `departure_board_v1` | Alternative A: one trip per row in a dense departure board | arm A |
| `route_strips_v1` | Alternative B: a route strip for each trip | arm B |
| `next_departure_queue_v1` | Alternative C: next scheduled trip plus the remaining queue | arm C |

The experiment must randomize the **browser visitor**, not the event, request,
session or build. The same browser therefore sees one treatment on every eligible
transport page for the whole version. Build/release ids are evidence fields and
must never enter assignment.

The production static build does **not** create three URLs or three event-page
designs. It ships one normalized transport dataset and three inert timetable
treatment templates inside the existing transport block. A very small client
controller chooses exactly one template. The surrounding event-detail
composition, transfer block, boarding place, walking leg, no-return warning and
car alternative remain unchanged.

The implementation should initially be enabled only for the secret-prefix
artifact requested in the parent workstream:

```text
mode=off          production root; current timetable remains unchanged
mode=qa           secret noindex prefix; force arms; never trusted telemetry
mode=focus_group  secret noindex prefix; normal allocation + consented ingest
mode=live         later explicit GO only
```

`noindex` and an unguessable preview prefix are discovery controls, not
authentication. Do not describe the secret link as an access-control boundary.

## Source audit

### Current implementation state

`origin/main@2822a91d` does not contain the accepted KAUP component or the V11
research. They remain in the pushed side branch
`integration/static-event-v11-transport-phone-carousel@3b17e536`:

- `site/src/components/KaupTransportSchedule.astro` renders the current public
  bus rows and all invariant safety/action facts;
- `site/src/lib/eventKaupTransport.ts` calculates the two suitable route-119
  options and preserves terminal/North boarding provenance;
- `site/src/data/busTransportSchedules.json` and `busRouteDirectory.json` own
  the static source-backed schedule projection;
- `.codex/lanes/transport-infographic-research/RESULTS.md` records the three
  alternatives, 120 reviewed Pinterest references and authoritative transport
  standards;
- the completed V11 external review ranked A over C over B, while recommending
  A plus shared route context as the production default. The product owner has
  now explicitly accepted all three for user testing, so that earlier ranking is
  prior evidence, not a reason to remove an arm.

Do not merge that integration branch wholesale into the production-pipeline
branch. Selectively port the accepted component/data dependencies first, or
land this experiment after V11 is in `origin/main`.

### Personalization database reality

The redacted DB health probe succeeded against the separate personalization
Supabase/Postgres contour:

- PostgreSQL 17.6, about 37 MB used;
- the required personalization credentials are present in the canonical root
  `.env` (values were not printed);
- `public.personalization_event_reaction_counter` exists;
- **no** `personalization_experiment_event`,
  `personalization_served_list_summary`, generic interaction-event table or
  experiment ingest RPC currently exists.

Therefore:

- secret-preview rendering, deterministic assignment and local QA do **not**
  need Supabase;
- a real user A/B/C decision **does** need the personalization Supabase for
  durable exposure/action evidence;
- Fly SQLite must continue to own events, schedule snapshots, rebuild jobs and
  release metadata. It must not receive browser experiment rows;
- YDB is not needed for assignment or the decision. A de-identified aggregate
  may be projected to analytics later, but it cannot be the experiment control
  plane or a blocking write path.

## Assignment contract

### Identity and persistence

Use a dedicated first-party browser subject, because the existing
`ke_personalization_profile.anon_id` is created only after consent:

```text
localStorage key: ke_experiment_subject_v1
value: UUID v4
assignment key: ke_experiment_assignment:transport_timetable_layout:1
value: { variant, bucket, algorithm, assigned_at, config_hash }
```

Rules:

1. If a saved compatible assignment exists for the exact experiment version
   and config hash, use it.
2. Otherwise create/read `ke_experiment_subject_v1` and calculate the arm.
3. Never derive the subject from IP, user-agent, event id, preview prefix,
   session id or build id.
4. When consent is later accepted, keep the existing experiment subject and
   attach the compatible personalization `anon_id` separately. Do not switch the
   visible arm when consent changes.
5. localStorage failure, unavailable Web Crypto, invalid config or an automation
   actor fails closed to the baseline timetable and creates no trusted exposure.
6. Anonymous cross-device stability is impossible without identity. The honest
   v1 guarantee is stable **within one browser profile across builds and secret
   prefixes on `kenigevents.ru`**. Authenticated cross-device assignment is a
   later server-hydrated design and must not be claimed by this experiment.

### Hash and buckets

Canonical allocation input:

```text
UTF-8("transport_timetable_layout|1|" + experiment_subject_uuid)
  -> SHA-256
  -> first unsigned 32-bit big-endian word n
  -> bucket = floor(n * 10000 / 2^32)
```

Initial allocation:

```text
0..3332     departure_board_v1       (3333 buckets)
3333..6665  route_strips_v1           (3333 buckets)
6666..9999  next_departure_queue_v1   (3334 buckets)
```

Web Crypto is available in target evergreen browsers and may resolve after DOM
parse: the transport block is below the first viewport. If allocation has not
resolved when the block approaches the viewport, render the baseline and do not
count an experimental exposure. Do not use `Math.random()` per page view.

The Postgres ingest function must independently recompute the SHA-256 bucket
from the experiment subject and reject/quarantine variant or bucket mismatch.
This is experiment-integrity validation, not a security identity claim.

### Eligibility shared by all arms

Randomize only when all treatments can honestly render the same facts:

- supported reviewed transport suggestion;
- exact event service date and `Europe/Kaliningrad` timestamps;
- at least two suitable outbound trips;
- source snapshot is accepted and not stale under the transport release gate;
- each trip has a stable id, boarding timestamp, stop/venue arrival estimates
  and estimate/scheduled semantics;
- variant C has at least one not-yet-departed trip when the component becomes
  eligible for exposure.

If any rule fails, show the unchanged baseline, set a bounded local diagnostic
reason, and do not create a valid experiment exposure. Eligibility cannot differ
by assigned arm; otherwise the sample ratio and population are biased.

Variant C must say **`по расписанию`**, never imply real-time vehicle tracking.
It selects the first scheduled boarding timestamp after `now + boarding reserve`
and shows the remaining trips in chronological order. If all trips have passed,
the whole page is outside the experiment rather than silently converting only C
to another treatment.

To exercise the intended “many buses” case, calculation and presentation must
be separated. `getKaupTransportSuggestion()` currently ends with `.slice(-2)`.
The experiment input should instead return the full validated suitable set,
bounded at 20, while treatment components decide that A/C initially show 3–5
and disclose the remainder. Every arm receives the exact same ordered trip ids.

## Exposure and outcome contract

### Valid exposure

A valid exposure is recorded once per
`(experiment_key, experiment_version, experiment_subject_id, event_id)` only
after all are true:

- the assigned treatment was actually mounted;
- at least 50% of the timetable treatment was in the viewport continuously for
  at least 1000 ms;
- `document.visibilityState === 'visible'`;
- no QA override, preview bot, crawler, monitor or Playwright actor;
- local personalization consent is explicitly accepted before remote ingest.

Repeat views may update a bounded repeat count/last-seen timestamp, but analysis
uses the first valid exposure. Persist both `assigned_variant` and
`rendered_variant`. Primary analysis is intention-to-treat by assigned arm.
Delivery mismatch is a release guardrail and blocks a winner decision when it
exceeds the threshold below.

### Product metric: “поехать на мероприятие”

The UI cannot prove physical attendance. The preregistered primary proxy should
be **qualified transport-plan action rate**:

```text
unique exposed subjects with >=1 qualifying action
---------------------------------------------------
unique subjects with a first valid exposure
```

Qualifying action within the same event/session and within 30 minutes of the
first valid exposure:

- `official_transfer_booking_click`;
- `bus_origin_map_click`;
- `walk_route_click`;
- `car_route_click`;
- `transport_calendar_add` when a trip calendar action exists.

Count at most one primary conversion per subject/event/version. The composite
measures making a viable plan, including choosing not to use the bus after
understanding it. Do not optimize only “expand” clicks.

Secondary metrics:

- time to first qualified action;
- each qualifying action separately;
- `departure_select` and `schedule_expand` (diagnostic engagement only);
- transfer-vs-public-bus-vs-car mix;
- conversion by viewport and trip-count bucket (`2`, `3-5`, `6-10`, `11-20`);
- no-action return to the transport block in the same session;
- later optional, explicitly asked `transport_arrival_confirmed` survey. It is
  the only proposed direct attendance signal and is out of this implementation.

Do not add hover/mousemove/scroll firehoses. CTA navigation never waits for
telemetry; use `sendBeacon`/`keepalive` best effort and allow loss.

### Guardrails

All arms must preserve exact source facts and invariant controls:

- boarding point `Северный вокзал` remains visible;
- route number/direction, estimate markers, walk leg, no-return warning,
  transfer booking and car route are not treatment variables;
- no horizontal overflow at 320/390/768/1366 px;
- no missing or nested interactive target; keyboard and screen-reader order is
  meaningful; no colour-only semantics;
- JS exception increase <= 0.5 percentage points versus A;
- LCP p75 regression <= 200 ms and treatment JS remains within the accepted
  static personalization budget;
- assigned/rendered mismatch < 0.5%; telemetry write failures never affect UI;
- stale/invalid time classification is zero in deterministic fixtures;
- source/safety-fact parity across A/B/C is 100% in build checks.

A winner cannot be declared while a safety/parity, delivery mismatch or SRM
guardrail is red.

## Sample-ratio and decision semantics

1. Run an **A/A/A instrumentation stage** first: three assignment labels, but
   all render the current baseline treatment. Require at least one complete
   weekday/weekend cycle. This validates bucket, exposure and action plumbing.
2. For each analysis epoch, take one first valid exposure per subject and run a
   3-cell multinomial/chi-square goodness-of-fit test against
   `3333/3333/3334`.
3. Cut SRM by viewport, event id, trip-count bucket and release id as diagnostics.
4. Before 300 total unique exposures, show counts only; do not alarm on an
   underpowered p-value. At/after 300, page an SRM blocker when **both**
   `p < 0.001` and any arm differs by more than 1.5 percentage points from its
   planned share. Require two consecutive daily evaluations before automated
   pause, but a large delivery mismatch pauses immediately.
5. SRM means all effect estimates are diagnostic until root cause and a clean
   version/epoch are established. Never “rebalance” by mutating weights in
   place.
6. Pre-register baseline, minimum detectable effect, alpha/power and required N
   from the A/A/A/focus-group evidence. Require at least two complete event-week
   cycles. If traffic cannot reach the calculated N, report the experiment as
   inconclusive instead of choosing the largest observed rate.
7. Primary analysis is subject-level intention-to-treat. Per-protocol rendered
   arm is secondary debugging evidence only. Report absolute percentage-point
   lift and confidence interval, not only relative lift.

## Versioning and release-manifest integration

Suggested static definition:

```json
{
  "schema_version": "transport-timetable-experiment-v1",
  "experiment_key": "transport_timetable_layout",
  "experiment_version": 1,
  "allocation_algorithm": "sha256-u32be-bucket-10000-v1",
  "assignment_unit": "browser_subject",
  "status": "off",
  "variants": [
    {"id":"departure_board_v1","from":0,"to":3332},
    {"id":"route_strips_v1","from":3333,"to":6665},
    {"id":"next_departure_queue_v1","from":6666,"to":9999}
  ]
}
```

Rules:

- treatment meaning, eligibility, primary metric, hash algorithm or allocation
  changes start a new `experiment_version`; never edit a running version;
- harmless CSS/token fixes that preserve treatment semantics may keep the
  version, but still produce a new config hash/release id;
- a weight change is a new analysis epoch/version, not an in-place mutation;
- the static release manifest records `experiment_key`, version, status,
  allocation algorithm, definition SHA-256 and treatment bundle SHA-256;
- snapshot/build/release ids are attached to exposure rows for diagnosis but do
  not affect the arm;
- rollback rolls back the definition and bundle together with the site tree.

In the parent pipeline's production manifest, add an `experiments[]` record only
after the checked build has hashed the definition/treatment assets. A secret
release may use `qa`/`focus_group`; production root must assert `off` until an
explicit experiment GO.

## Secret-preview QA controls

Only a build compiled with `mode=qa|focus_group` and a secret-prefix profile may
honour:

```text
?ke-exp-transport=departure_board_v1
?ke-exp-transport=route_strips_v1
?ke-exp-transport=next_departure_queue_v1
&ke-exp-debug=1
```

- Forced assignment lives in `sessionStorage`, not the durable assignment key.
- It is marked `qa_override=true` and never reaches accepted experiment rows.
- The debug panel/badge appears only with `ke-exp-debug=1`, is `data-nosnippet`,
  and shows no secret or user identity.
- Normal secret links without override use deterministic allocation and may
  ingest only in `focus_group` mode after consent.
- The server definition allowlists focus-group release ids/config hashes.
- Preview UAs, `navigator.webdriver`, monitors and Playwright runs produce local
  diagnostics only.
- `mode=off` ignores all override parameters, preventing accidental experiment
  controls at production root.

## Supabase write contract

Recommended new migration:

`supabase/migrations/<timestamp>_transport_timetable_experiment_v1.sql`

Create a generic compact table rather than a transport-specific shadow profile:

```text
public.personalization_experiment_event
  id uuid PK
  client_event_id uuid/text, unique with experiment_subject_id
  experiment_key text
  experiment_version integer
  experiment_subject_id uuid
  anon_id uuid                 -- consented personalization id
  session_id uuid
  event_id bigint
  assigned_variant text
  rendered_variant text
  assignment_bucket smallint
  event_kind text              -- valid_exposure + bounded action allowlist
  occurred_at / received_at timestamptz
  viewport_class text
  release_id text
  config_hash text
  transport_snapshot_hash text
  consent_version text
  actor_class / trust_state    -- server-owned
  metadata jsonb <= 512 bytes
```

Add a partial unique index for first exposure on
`(experiment_key, experiment_version, experiment_subject_id, event_id)` where
`event_kind='valid_exposure'`. Action rows dedupe by client event id.

Expose only a typed
`public.ingest_transport_experiment_event_v1(...)` (or equally narrow generic
`ingest_experiment_event_v1`) RPC:

- table RLS enabled; no `anon`/`authenticated` table grants;
- revoke function execute from `PUBLIC`; grant only the exact RPC to `anon`;
- `SECURITY DEFINER` only if required, with `search_path=''` and fully qualified
  relations;
- hard payload/metadata limits and allowlisted key/version/release/variant/kind;
- server recomputes bucket/variant, timestamps receipt and classifies trust;
- client `actor_class`, `trust_state`, debug status and server timestamps are
  ignored;
- idempotent exposure insert; bounded per-subject/session/IP time-bucket quota;
- QA overrides, unapproved releases, no consent, mismatch and automation are
  dropped or stored only as tiny short-retention quarantine evidence;
- minimal `{ok:true}`/void response; no profile reads.

Use `PUBLIC_PERSONALIZATION_SUPABASE_URL` and the personalization publishable key
only. Never expose the secret key/direct connection and never fall back to the
legacy `SUPABASE_URL`/`SUPABASE_KEY` project.

Retention proposal: raw experiment events 30 days after experiment closure,
subject-level analysis extract 90 days, irreversible daily arm aggregates 12
months, quarantine 7 days. Final retention remains a product/legal decision.

## Exact suggested write scope

This is the minimal future implementation scope; it was intentionally not
modified by this lane.

### Static/UI

- `site/src/data/experiments/transportTimetableLayout.v1.json` — immutable
  definition and bucket ranges.
- `site/src/lib/experimentAssignment.ts` — SHA-256 test-vector assignment,
  storage failure and QA override handling.
- `site/src/lib/eventKaupTransport.ts` — return full bounded ordered trip set and
  exact service timestamps/stable trip ids; no UI choice here.
- `site/src/components/transport/TransportTimetableExperiment.astro` — inert
  template host, baseline fallback and exposure observer.
- `site/src/components/transport/DepartureBoardTimetable.astro` — A.
- `site/src/components/transport/RouteStripsTimetable.astro` — B.
- `site/src/components/transport/NextDepartureQueueTimetable.astro` — C.
- `site/src/components/KaupTransportSchedule.astro` — replace only the current
  `<ol>` timetable subsection with the host; invariant transport sections stay.
- `site/src/lib/transportExperimentTelemetry.ts` — bounded consent-aware beacon/
  RPC client; CTA is never blocked.
- `site/src/layouts/EventLayout.astro` — only shared release/profile/runtime data
  attributes if the component-local island cannot own them cleanly.

### Build/release

- selective clean-port integration with the parent's production build/manifest
  module: record experiment definition and bundle hashes;
- `site/scripts/check-preview.mjs` — all treatment source-fact and safety parity;
- `site/scripts/check-production.mjs` (once clean-ported) — root mode must be
  `off` before GO and QA overrides/routes must not leak;
- `site/scripts/check-transport-experiment.mjs` — definition validation, bucket
  coverage/no overlap, exact same trip ids/facts in all arms.

### Persistence/analytics

- `supabase/migrations/<timestamp>_transport_timetable_experiment_v1.sql`;
- `supabase/tests/transport_timetable_experiment_contract.sql`;
- a small backend/daily report query or script under `scripts/analytics/` for
  unique first exposure, primary conversion, SRM and guardrails. It reads the
  personalization DB only and must not join by copying canonical event bodies.

### Canonical docs/change record when code lands

- `docs/features/event-transport/README.md` — experiment state and invariant
  safety contract;
- `docs/features/static-site-pages/event-transport-schedule.md` after its V11
  selective port — treatment definitions and many-trip behavior;
- `docs/features/unsigned-personalization/database.md` and
  `production-integration.md` — applied schema/RPC and consent/trust boundary;
- `docs/features/static-site-pages/test-scenarios.md` — stable A/B/C acceptance
  IDs;
- parent release-plan/operations docs — secret-prefix mode and manifest fields;
- `CHANGELOG.md` under `[Unreleased]`.

## Required tests and acceptance IDs

Suggested stable IDs (do not reuse the parent's `ADD-BUILD-*` ids):

- `TR-EXP-01`: same subject/config always maps to same bucket/arm; fixed SHA-256
  vectors match browser and SQL.
- `TR-EXP-02`: 0..9999 has full non-overlapping coverage and intended weights.
- `TR-EXP-03`: reload/new build/same origin retains arm; new session does not
  reassign; localStorage failure gets non-experimental baseline.
- `TR-EXP-04`: QA query forces each arm only in secret mode and creates zero
  accepted rows.
- `TR-EXP-05`: A/B/C render exactly the same route, departure/arrival facts,
  estimate flags, source snapshot, walk/no-return warnings and CTAs.
- `TR-EXP-06`: many-trip fixtures at 2/5/10/20 trips stay usable at
  320/390/768/1366 and disclosure has a 44 px target.
- `TR-EXP-07`: variant C selects the correct upcoming scheduled trip before,
  between and after departures in `Europe/Kaliningrad`; all-passed is globally
  ineligible and not an exposure.
- `TR-EXP-08`: IntersectionObserver/visibility timing creates one idempotent
  exposure; <50%, <1 s, hidden tab, bot and webdriver create none.
- `TR-EXP-09`: action beacon failure does not delay transfer/map/calendar
  navigation; primary composite dedupes per subject/event/version.
- `TR-EXP-10`: RPC rejects invalid UUID, unknown key/version/release/variant,
  bucket mismatch, missing consent, oversized metadata, duplicate event and
  quota abuse; tables remain unreadable/unwritable directly by `anon`.
- `TR-EXP-11`: A/A/A synthetic allocation passes SRM; biased fixture raises the
  declared blocker; low-N fixture stays diagnostic.
- `TR-EXP-12`: production manifest has mode `off` before GO; secret manifest has
  config/bundle hashes and rollback restores them atomically.

Playwright must cover every arm at mobile/desktop, keyboard order, no JS
baseline, slow script/blocked storage, focus-group normal allocation, and QA
overrides. SQL tests must run in a transaction and roll back fixtures.

## Risks and blockers

1. **Dependency not in main.** KAUP/V11 transport code and canonical detailed
   schedule doc must be selectively ported before experiment code can compile on
   the parent's fresh-main branch.
2. **No telemetry backend today.** A visual secret preview can ship without it,
   but no user A/B conclusion is valid until migration/RPC/consent/SRM evidence
   exists.
3. **Low traffic.** One KAUP event with two trips may never reach an adequately
   powered three-arm sample. Expand only to other reviewed schedules using the
   same eligibility and parity gates; do not fabricate a winner.
4. **C is clock-sensitive.** It requires exact dated timestamps and accepted
   source freshness. Browser-local string clocks are insufficient.
5. **Anonymous multi-device identity.** V1 cannot keep a person in one arm
   across unrelated browsers/devices. Claim browser stability only.
6. **Secret link leakage.** `noindex`/entropy does not authorize private data.
   The artifact must contain only already-public event facts and no secrets.
7. **Experiment changes safety facts.** The timetable is the only treatment.
   Boarding, walking difficulty and missing return cannot vary; build parity is
   a hard fail, not an analytics guardrail.

## Commands/evidence

Read-only commands used in this lane:

```text
cat AGENTS.md
sed docs/README.md docs/routes.yml
cat .codex/skills/events-bot-dual-db/SKILL.md
sed docs/features/static-site-pages/README.md
sed docs/features/event-transport/README.md
sed docs/features/unsigned-personalization/{README,database,production-integration}.md
git show integration/static-event-v11-transport-phone-carousel:...
git show origin/integration/static-site-production-release-20260715-v2:...
PYTHONPATH=artifacts/codex/tmp_pg_driver \
  python3 .codex/skills/events-bot-dual-db/scripts/check_personalization_db.py \
  --env /home/dev/projects/events-bot-new/.env
```

An additional direct read-only Postgres catalog query listed public/
personalization relations and matching functions; it printed relation/function
names only, not connection values, rows, user data or secrets. No SQL mutation,
deployment, external publication or public-root change occurred.
