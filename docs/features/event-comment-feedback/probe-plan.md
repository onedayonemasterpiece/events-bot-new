# Research and minimal implementation plan — event-comment-feedback

> Status: **approved post-release research plan; implementation not started**. This capability is not a blocker for the first public static-site presentation. It gets a separate post-release RC, canary and rollback decision.
>
> Product priority: **verified decision facts first**, **«Активно обсуждают» medallion second**, broad positive/negative discussion carousel later only if evidence justifies it.

## Decision being tested

Open comments can create value in two different ways and must remain separate:

1. **Decision facts** — a new, concrete answer from the official source that can change attendance, registration or ticket-purchase decisions: duration, transfer, entrance, doors, venue format, accessibility or another allowlisted practical rule.
2. **Discussion attention** — a short-lived, non-editorial signal that an event is actively discussed. The public label is **«Активно обсуждают»**; it is not a quality score, recommendation or popularity rank.

The first post-release implementation must not regenerate the main event description from arbitrary comments. The safe initial public surface is a modular **«Важно знать»** block built from typed verified facts. The medallion uses a separate rolling score and never becomes an event fact.

## Evidence already collected

Fresh live canary on `2026-07-15T22:02:47Z`:

- 20 current/future events, 135 source links and 109 unique source posts;
- 105 comments fetched during the run; 8 comment keys were absent from the preceding output;
- 16/16 site-eligible future events had at least one fetched comment;
- vector routing used `intfloat/multilingual-e5-base` and `BAAI/bge-m3`; no embedding API or LLM call was used;
- 24 source fetch errors remained: 11 invalid Telegram numeric peers, 8 deleted/inaccessible VK posts, 4 invalid Telegram message ids and 1 invalid topic;
- the current prototype reported `official_context_rows=0`, even though direct VK API checks proved official source-owner replies. Authority metadata is therefore a release blocker for extraction, not evidence that official answers are absent.

Real product-positive cases:

| Event | Evidence | Novelty/value | Automatic disposition |
|---|---|---|---|
| `6651/6652`, «Руслан и Людмила. На стыке времен» | official source owner answered that the performance lasts one hour | absent from description/fact ledger; useful for family visit and transport planning | safe fact-family candidate; store as series-scoped until occurrence projection is explicit |
| `6507`, «Откровения Вены» | official source owner named the transfer provider | absent from description/fact ledger; important for a remote venue | stable logistics candidate; omit phone numbers from narrative |
| `2863`, «Территория мира — Территория музыки» | official source owner explained sector-2 subscription-only policy and was uncertain about sector 3 | useful for purchase, but volatile and partly uncertain | accept only the confirmed clause with TTL; suppress uncertainty |
| `4961`, URATSAKIDOGI | official source owner confirmed a large concert hall | venue-format information is useful; «space for slam» is subjective | accept only the literal hall-format slot |
| `6407/6408` | comments say ticket sales started | canonical `ticket_status=available` already says this | duplicate; no update |

Vector routing found the relevant needs in new comments (`duration`: E5 `0.8522`, BGE `0.7856`; `transfer`: E5 `0.8919`, BGE `0.7509`), but the existing bank is question-oriented and did not promote the paired official answers. This validates topic recall, not fact truth. The research must test `Q + A` pairing, speech act, polarity and authority separately.

Artifacts for the live evidence remain under `artifacts/codex/event-comment-feedback/fact-enrichment-20260715/` and are intentionally not committed.

## Non-negotiable architecture

```text
daily source delta fetch
  → preserve post/thread/parent/reply/author-owner metadata
  → BGE + E5 closed-taxonomy routing
  → deterministic exact-span typed slot extraction
  → authority + speech-act + polarity + event/series scope gates
  → novelty/conflict/TTL/retraction reconciliation
  → verified active fact-set hash
  → static «Важно знать» projection and checked rebuild only when hash changes
```

Forbidden:

```text
comment embedding
  → nearest expected meaning
  → assume the whole comment is true
  → feed raw comment text into Smart Update extraction
  → rewrite the main description
```

No LLM participates in comment extraction. If a later Smart Update writer formats already verified typed facts, that is a separate product decision and experiment. A strict zero-LLM delivery remains possible through deterministic templates.

## Research work packages

### R0 — Region Talk clean-port gate

Before opening a production implementation branch:

- bind current main, Region Talk and the stale F14 probe to exact SHAs;
- publish the `reuse|adapt|reject|defer` matrix and data/credential boundary from [the Region Talk reuse audit](region-talk-reuse-audit.md);
- validate the required `region-talk-ydb-funnel-audit` and `event-comment-feedback-pipeline` project skills;
- list only behavior to reimplement/cherry-pick onto current main; never merge the divergent product branches wholesale.

Gate: the audit/skills pass their existing acceptance checklist and no Region Talk frontier, image/publication/writer stage or session lane enters F14.

### R1 — Collection health and authority

Goal: prove that the daily collector can distinguish user questions, official answers and ordinary-user replies.

Work:

- collect 30 consecutive daily shadow runs over at least 30 current/future events with TG/VK discussions;
- retain source owner id, author peer id/type, parent/root keys, reply depth, post id, source URL, fetch cursor and capability state;
- classify every fetch failure as permanent, retryable or source-config defect;
- verify source-owner identity directly from API metadata; organizer identity aliases are allowlisted and versioned, never inferred from text;
- never borrow Region Talk `DISCOVERY1/2`, E2E or S22 session roles for production collection.

Gate:

- 100% of proposed facts have reproducible authority evidence;
- zero ordinary-user replies accepted as official;
- permanent/deleted sources fail closed and do not make the whole run green;
- every run exposes checked posts, accessible posts, new comments, authority-resolved replies and error buckets.

### R2 — Closed taxonomy and Q/A calibration

Initial fact families:

1. `duration` and `intermission`;
2. `transfer_available`, `transfer_direction`, `transfer_provider`;
3. `doors_open`, `entry_point`, `parking`;
4. `seated_or_standing`, `hall_or_stage`;
5. `accessibility_rule`;
6. volatile `ticket_sector_rule` only as a TTL-controlled structured fact.

For every family create prototypes for:

- positive official assertion;
- explicit negative assertion as another slot value;
- question;
- uncertainty/conditional answer;
- correction/retraction;
- request/desire;
- past experience/hearsay;
- adjacent hard-negative family.

Embed both `reply alone` and `Q:<parent>\nA:<reply>`. Split calibration by event and source so near-duplicate posts cannot leak between train/tune and evaluation sets.

Shadow starting thresholds, not production constants:

- E5 positive cosine `>=0.82`, hard-negative margin `>=0.05`;
- BGE-M3 positive cosine `>=0.76`, hard-negative margin `>=0.04`;
- runner-up margin `>=0.03` in both models;
- both models agree on family, speech act and polarity.

Production gate is per-family precision, not global average:

- accepted-fact precision `>=0.99`;
- zero question→fact, uncertainty→fact, wrong-author, wrong-event, lost-negation and stale-current errors;
- recall is secondary; fully automatic mode is intentionally fail-closed.

### R3 — Literal slot extraction and scope

Goal: prove that the output contains only source-grounded values, not embedding-generated prose.

Work:

- implement narrow deterministic parsers for time spans, duration, boolean/directional transfer values, exact venue/stage spans and short access rules;
- preserve the exact redacted evidence span and its hash internally;
- compare against structured event fields, current descriptions and active canonical facts;
- if one source post maps to several occurrences, store `event_series` scope unless the question/answer contains an exact date/title anchor;
- test shared-post, mixed-date, correction and retraction fixtures.

Gate:

- every public value is reconstructable from an exact source span;
- no shared source is blindly copied into every child occurrence;
- duplicate facts produce no public change;
- correction/retraction removes or supersedes stale active state.

### R4 — Product yield and usefulness

For every accepted candidate record:

- `decision_area`: planning, access, ticket purchase, registration or expectations;
- `novelty`: new, duplicate, conflict, clarification;
- `durability`: stable or TTL-controlled;
- `surface`: structured fact, «Важно знать», suppressed;
- whether the information could reasonably change attendance/purchase behavior.

Go gate after the 30-day shadow:

- at least 3 current/future events contain genuinely new, decision-useful official facts;
- at least 2 priority fact families produce accepted facts;
- duplicate/no-op rate is measured and idempotent;
- no accepted fact creates a misleading main-description rewrite.

If the useful yield is below this floor, keep collection for research but do not ship a public block.

### R5 — Fact lifecycle and Smart Update shadow

The existing flat `EventSourceFact(fact,status)` is insufficient for comment facts and must not be reused without authority, scope and retraction semantics.

Required versioned fact state:

- `event_id` or explicit `event_series_key`;
- `fact_type`, typed `value_json` and normalized fingerprint;
- source post/comment/reply keys and evidence hash;
- `source_role=official_comment_fact`;
- authority method and identity version;
- model/policy versions and both score sets;
- `active`, `observed_at`, `expires_at`, `supersedes`/`retracted_by`;
- `verified_fact_set_hash` and `applied_hash`.

Research tests:

- unchanged daily rerun triggers zero Smart Update/static rebuild work;
- one new fact creates exactly one changed-hash handoff;
- failed writer/build retains the last-good public block and old `applied_hash`;
- retraction/expiry removes the fact without reconstructing from previous prose;
- comment pseudo-sources never inflate the public source count.

### R6 — «Активно обсуждают» calibration

The medallion is a separate volatile projection. It never becomes a fact and initially does not affect ranking.

Candidate calculation uses a rolling 7-day window with freshness decay. Before public rollout:

- exclude official/admin answers, bots, link-only replies, duplicates, repeated contest phrases and crossposts;
- suppress giveaway/contest/poll/engagement-bait threads entirely;
- require exact event binding; a shared series post cannot badge every occurrence;
- suppress controversy/fraud/cancellation-dominated threads;
- fail closed when fetch coverage is incomplete.

Initial shadow entry gate:

- at least 6 qualified comments;
- at least 5 qualified authors;
- at least 3 new qualified authors in the last 72 hours;
- qualified/raw-human ratio `>=0.60`;
- source/age-bucket activity percentile `>=P90` after a 30-day baseline.

Exit gate: event ended/cancelled/ambiguous, or score below exit threshold for two daily runs, or no new qualified author for 7 days.

The first public label is **«Активно обсуждают»** with tooltip «За последние 7 дней событие активно обсуждали в открытых источниках». A stronger **«Особо обсуждают»** tier is deferred until real percentile calibration.

### R7 — Operations and privacy

- raw/redacted comments remain backend-only with an approved short retention period;
- static export contains no raw text, names, ids, comment links, phone numbers or verifier/debug payloads;
- daily run has lease, heartbeat, terminal status, retry/cooldown, compact retention and catch-up semantics;
- product funnel is measured as `comments fetched → official Q/A pairs → typed candidates → accepted novel facts → changed public state → checked static promotion`;
- a successful fetch with zero accepted/public delivery is visible as zero delivery, not a false-green product run.

## Minimal post-release implementation

### PFR-1 — Collector and typed shadow ledger

Deliver only:

- EventSource-derived TG/VK source inventory;
- incremental daily fetch with full thread/authority metadata;
- isolated YDB source/comment/typed-fact/run state;
- BGE/E5 closed-taxonomy routing and deterministic slot extraction;
- shadow reports, no public UI and no Smart Update write.

### PFR-2 — «Важно знать» static block

After R1–R5 gates pass:

- export only active, novel, stable/valid-TTL typed facts;
- render at most 1–3 attributed facts under **«Важно знать»**;
- use deterministic templates such as «Организатор уточнил: продолжительность спектакля — около одного часа»;
- trigger one checked static rebuild only when `verified_fact_set_hash` changes;
- hide the block on missing/stale/invalid state;
- do not modify main description, critical event fields, JSON-LD, OG description or public source count.

### PFR-3 — Smart Update integration experiment

Only after the deterministic block is stable:

- add a dedicated `apply_verified_comment_fact_snapshot(...)` ingress that bypasses event matching and LLM fact extraction;
- run writer/coverage in shadow against the typed active snapshot;
- permit public narrative formatting only if it is no worse than the deterministic block, handles retractions and preserves last-good output;
- date/time/place/cancellation changes remain outside this experiment and use their existing repair/incident workflow.

### PFR-4 — «Активно обсуждают» medallion

After R6 baseline:

- publish the medallion only for events passing the strict qualified-human gate;
- keep score/debug fields out of public HTML;
- no ranking boost in the first experiment;
- measure eligible-impression → ticket/registration CTA click, while guarding false badges, churn and organizer/source concentration.

### Deferred beyond the minimal release

- broad positive/negative carousel «Что видно по обсуждению»;
- generated summaries or direct quotes;
- arbitrary open-ended fact extraction;
- automatic critical field repair from comments;
- ranking boost from discussion activity;
- stronger «Особо обсуждают» tier.

## Post-release acceptance checklist

- [ ] 30-day daily shadow completed with reproducible run artifacts.
- [ ] Authority metadata is preserved; ordinary users cannot become canonical sources.
- [ ] Event/source-disjoint calibration meets per-family `>=0.99` accepted-fact precision and zero critical error classes.
- [ ] At least 3 current/future events and 2 fact families prove new decision usefulness.
- [ ] Unchanged, duplicate, correction, retraction, expiry and shared-series cases are idempotent and tested.
- [ ] Public static projection contains no raw comment/PII/debug fields and does not inflate source counts.
- [ ] «Важно знать» remains modular and fail-closed; main narrative/JSON-LD/critical fields are unchanged in PFR-2.
- [ ] «Активно обсуждают» has a real 30-day baseline, giveaway/spam/crosspost suppression and no initial ranking effect.
- [ ] Separate RC, preview, accessibility/no-JS/mobile checks, canary, rollback and owner sign-off exist.
