# P0/SEV-1 LLM-first recall — integration report

Date: 2026-08-11 UTC
Draft PR: `#494`
Branch: `integration/smart-update-identity-state-machine`

## Delivery identity and safety boundary

- User-verified PR head at the start of the investigation:
  `8614262f2c2a5489169cf3c7fa5bf8ab19c83b97`.
- The branch was reconciled without dropping its newer commits and rebased onto
  `origin/main@f66330f8af81d4b898d137d83356e77914dce90a`.
- Last implementation checkpoint before this closure report:
  `02cba2c5416e1a60ed2be2270649c68f0eae0c90`.
- The exact pushed delivery head is authoritative in the PR ref and final PR
  comment; this file intentionally does not pretend that a commit can contain
  its own SHA.
- No merge, deploy, production write, recovery apply, provider/model switch,
  operator queue, new PR, or GitHub issue was performed.

## What is integrated

| Block | Integrated result |
| --- | --- |
| Emergency recall | configured-source VK revisions are durable before semantic selection; keyword/date/history/past/too-far/cancellation signals are hints only; automatic technical failures remain due |
| Evidence + verdict | full available text/OCR manifest, closed source dispositions, multi-child and lifecycle actions, incomplete-evidence negative guard |
| Conditional verification | one normal primary parse and at most one verifier for seven closed contradiction classes; technical/uncertain verification retries |
| TPM/backpressure | richer provider usage/finish receipts, `countTokens` input sizing, model/consumer calibration, durable claims/backoff/fair due selection, no inline long carrier sleep |
| Smart Update | closed reason enums, accepted/diagnostic ID split, durable candidates/attempts, immediate distinct create, stable occurrence identity and exact replay |
| Recovery/observability | A–T census, all-source feature detection, filtered read-only plan, raw packet/attempt funnel, invariants/alerts, production-clone migration rehearsal |
| Telegram | typed producer/consumer boundary, complete album OCR accounting, untyped zero-event cannot advance cursor; legacy semantic extractor is not used by the production scan |

The detailed requirement state is authoritative in
[`EXECUTION_MATRIX.md`](./EXECUTION_MATRIX.md). `Done` is not used to hide an
evidence limitation: R07, R15, R19–R22 and R25 are explicitly Partial/Blocked,
and R27 remains Blocked inside the mandated no-deploy/no-write boundary.

## Incident finding

The observed fall from 30+ additions/day to 2–5/day was not one new filter.
Git and read-only production evidence show:

1. chronic deterministic discovery/evidence losses existed since 2025 and the
   current VK semantic prefilter family was present by April 2026;
2. quota/reservation changes landed on 2026-07-31;
3. Smart Update identity enforcement on 2026-08-04 plus the caller fail-closed
   boundary on 2026-08-05 formed the plausible acute terminal-loss route; and
4. Supabase crawl snapshots separately collapsed from 121 configured sources
   on Aug 1–4 to 33 on Aug 5, 2 on Aug 6, and no retained snapshots Aug 7–9.

The frozen chronology, mandatory `git log -S` receipts and AS-IS/TO-BE graphs
are in
[`TIMELINE_AND_CONTROL_FLOW.md`](./TIMELINE_AND_CONTROL_FLOW.md). The immutable
first production artifact has SHA-256
`7361713debce0431f967757ccec43bc8ac4e7b827f1782dcdbb1015e519185c3`.

## Read-only census: what is and is not countable

- The final evidence pack assigns exactly one A–T class to **1,634 retained,
  non-overlap evidence rows**. This is not a fetched-post or event count: 748
  rows are incomplete sampled VK discovery evidence and 250 are class S.
- Acute retained VK inbox evidence: P=13, Q=114, S=129, T=232.
- Telegram: 398 carriers, 526 extracted children, 147 imported children; the
  observed child gap is 379 across 202 carriers. These are evidence gaps, not
  automatically 379 real events.
- Official parser telemetry contains 2,326 processed, 123 failed and 19 skipped
  repeated observations across four failing sources; it cannot be converted
  honestly into unique carriers.
- Current ticket/festival queues contain 302 active ticket rows and 1,382
  pending festival rows with attempts=0 in the observed snapshot.
- Retained historical discovery evidence starts on 2026-06-12. February–May
  raw carriers/configuration history are unavailable and remain class T rather
  than being estimated from a non-uniform sample.

Final ignored production evidence pack:

- report SHA-256 `b56de401694fbd840dd2ea382998b06c05a40cb3a5341bc4653be76a7eaa4e9b`;
- A–T census SHA-256 `bbdd6a28200c771dccd1eca0b8c9338f9cc1d47fc6577dd36e9996db22306b82`;
- full manifest SHA-256 `8c0550f7f57ad16f871c153c14e2c0d5f58b5c1a015301cf3cfa0bc0a919edad`.

Exact global never-LLM/incomplete-evidence counts and model-derived
event/action/no-event recovery counts are unavailable because the old system
did not retain a lossless raw revision/evidence ledger. No invented
extrapolation is reported.

## Capacity result

Read-only historical usage shows that the previous fixed reservation was much
larger than typical actual use:

| Route | n | input p99/max | candidate output p99/max | actual total p99/max | previous reserved |
| --- | ---: | ---: | ---: | ---: | ---: |
| Gemma 4 | 502 | 6,741 / 7,124 | 3,891 / 5,689 | 11,015 / 11,695 | 14,500 |
| Flash Lite | 411 | 8,473 / 14,410 | 407 / 4,209 | 8,849 / 18,619 | 31,229 p99 / 45,987 max |

The gateway now separates admission reservation from semantic output ceiling,
uses counted input plus persisted model/consumer/prompt-version tail
calibration, and retains output/thought/reservation/finish metadata. A
deterministic 1.5× observed p99 burst fixture (9 carriers) drains without loss.

Capacity report SHA-256:
`7222debc01bed6f4f8505427cf54ee9524146c59028a63860a0f1890351140af`.
Machine summary SHA-256:
`62720a27814a6d7c8a2214a2e6337cadce13bba6e3990fadbd9a3d18e4e964db`.

Six application quota scopes are configured, but this does not prove six
independent provider projects. Billing tier, spend cap, provider-side active
limits/reset timestamps and a live semantic recovery replay were not available;
the capacity/quota acceptance is therefore Partial, not production-ready.

## Migration rehearsal

On a disposable copy of the current production SQLite bundle:

- `PRAGMA quick_check=ok` before and after;
- full `Database.init()` passed twice;
- new candidate/raw packet/attempt/index conflict checks passed;
- census, read-only recovery plan and compatibility rollback passed;
- zero new foreign-key violations were introduced;
- 195 pre-existing orphan references were detected (45 event-source facts to
  Event, 111 event-source facts to EventSource, 37 posters to Event, 2 video
  announce items to Event) and only a deterministic, non-executed repair plan
  was produced.

Rehearsal report SHA-256:
`965bf0b2996c8d24fabe21e1450e5a81d0c4e7a5d688d9b294f3a13596334a7e`.
Run-log SHA-256:
`a035a870b6f6e0bb8d70926afb430f012310bf940c591515cf47c06e8092c6d5`.
The source bundle was opened/read over a sequential DB/WAL copy rather than an
atomic production snapshot; the clone passed integrity checks, but that caveat
must remain visible at release review.

## Verification receipts

- Mandatory T01–T76 mapping:
  [`TEST_MATRIX.md`](./TEST_MATRIX.md). T68 remains Partial because old partial
  child payload/model evidence does not exist; the planner selection is tested.
- Post-rebase integrated focused suite before the last receipt/docs block:
  `467 passed, 17 warnings` in 56.71s; log SHA-256
  `405dccdbb077146f4228584a7473067f51622cb407dd5ef574dec65afec4cf02`.
- Provider-attempt/metrics/rehearsal increment: `70 passed`.
- Final semantic-suppression/typed-source focused increment: `89 passed`.
- Final local relevant aggregate: `489 passed in 70.82s`; log SHA-256
  `07092f11d064f942d54ecd060ff6c9380a019e0d4953584499c05795111c1e0f`.
- Final Google provider path audit: PASS (`1,138` files, `0` unapproved,
  `0` unreadable); log SHA-256
  `16009d8e7922c173cf4ba8fcdcab4e63c40655658918d286648068b557660436`.
- Production-boundary `py_compile` and `git diff --check`: PASS. GitHub CI is
  not claimed green until it actually runs on the exact pushed head.
- The first rebased-head CI exposed four legacy structural duplicate-probe
  fixtures without the new optional explicit-occurrence conflict field. The
  helper now treats an absent set as empty (without weakening an actual
  conflict); the exact 222-test Smart Update CI command passes locally. Log
  SHA-256 `dd426e47232aae2b5f621a27461879ccd2fd45261d76ac2d81f5461df3f59c77`.

The current recovery source was also run twice against the offline production
bundle with `--read-only --since 2026-08-04T00:00:00Z --until
2026-08-12T00:00:00Z --batch-size 10000 --include-discovery-misses`. Both JSON
files are byte-identical (SHA-256
`3ad6810a48720448141e0d003fae5885db31fc8bd2f895b1a2404fb916433699`),
selected 5 Telegram + 119 VK legacy rows, reported `would_change=124` and
`changed=0`, and did not count already-due 17 ticket/42 festival rows as new
changes. The pre-migration durable state and parser recovery-request table are
honestly reported unsupported. Main/WAL/SHM hashes were identical before/after;
the main DB SHA-256 remained
`d0bd007994eb71e8587039da04249bf30af6175b03e2716b94d43076f29a44c1`.
This is a requeue plan, not the unavailable raw/model-derived event replay.

## Requested PR artifact checklist

| # | Artifact | Status / location |
| ---: | --- | --- |
| 1 | exact final HEAD | PR ref + final PR comment after push |
| 2–4 | AS-IS discovery, AS-IS auto-import, TO-BE | `TIMELINE_AND_CONTROL_FLOW.md` |
| 5 | Git/deploy/env timeline | same file + frozen timeline hash above |
| 6 | semantic filter table | same file, old stage/action/new contract |
| 7–8 | full available loss census; carrier vs occurrence | final census/report hashes above; unavailable fields explicitly null |
| 9 | acute incident report | canonical `docs/reports/incidents/INC-2026-08-10-smart-update-identity-terminal-loss.md` |
| 10 | Feb–Jul historical sample | Partial: deterministic sampler; retained June–July only, February–May unavailable |
| 11–13 | capacity, quota map, calibration | capacity receipts and `docs/features/llm-gateway/README.md`; provider entitlements Partial |
| 14 | existing-model routing benchmark | Blocked: no production-equivalent model replay/authoritative tier; no route change made |
| 15 | caller inventory | `docs/features/smart-event-update/caller-inventory.md` + caller AST contract |
| 16 | typed reason map | Smart Update README/state enums/tests |
| 17 | static/AST gate | T71–T76 in `TEST_MATRIX.md` |
| 18 | targeted tests | receipts above and final PR comment |
| 19 | full relevant CI | local 489-pass aggregate and green provider audit; GitHub pending final pushed head |
| 20 | production-snapshot rehearsal | hashes and caveats above |
| 21 | strict read-only recovery dry-run | two byte-identical current-source runs, 124 selected/0 changed; model-derived replay unavailable |
| 22 | DB unchanged | production `mode=ro/query_only`, `total_changes=0`, before/after SHA equal |
| 23 | remaining risks | next section |
| 24 | separate release/deploy/recovery runbook | `docs/operations/release-smoke-smart-update.md` |

## Remaining risks / why this is not a readiness claim

1. Production does not yet have the new raw packet/candidate/attempt schema, so
   the requested live funnel metrics cannot exist before a later approved
   migration/deploy.
2. Historical raw evidence is incomplete. The implementation cannot recreate
   deleted/private/edited carriers or infer missing children honestly.
3. Provider entitlements/reset data and production-like model replay are
   unavailable; deterministic queue/capacity tests do not prove live quota.
4. The production snapshot contains 195 pre-existing FK orphans requiring a
   separate audited repair decision before release.
5. A future approved release must come from `origin/main`, run the migration
   readiness gate, deploy the exact SHA, then separately approve recovery apply
   and perform current-day catch-up. None of those actions belongs to this
   no-write task.

Accordingly the draft PR may be reviewed as an implementation/evidence package,
but P0 recovery and incident closure remain blocked until the missing external
evidence and explicitly prohibited release/recovery phases are completed.
